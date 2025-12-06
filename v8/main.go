package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ============================================================================
// 1. 全局配置与监控
// ============================================================================

var (
	KafkaTopic   = getEnv("KAFKA_TOPIC", "engine.trades")
	KafkaBrokers = getEnv("KAFKA_BROKERS", "localhost:9092")
	KafkaGroupID = getEnv("KAFKA_GROUP", "clearing_cluster_v8")
	DatabaseURL  = getEnv("DATABASE_URL", "postgres://root:123456@localhost:5432/clearing_db?sslmode=disable")

	// 性能调优
	BatchInterval     = 200 * time.Millisecond
	BatchSize         = 5000
	InputBufferSize   = 50000 // Kafka 接收缓冲区
	PersistBufferSize = 50000 // 落盘缓冲区
)

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// Prometheus Metrics
var (
	metricTradesTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "clearing_trades_total"})
	metricDBBatches   = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clearing_db_batches"}, []string{"partition"})
	metricMemLatency  = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "clearing_mem_latency_us", Buckets: prometheus.ExponentialBuckets(1, 2, 10)})
	metricDedupHits   = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clearing_dedup_hits"}, []string{"type"})
)

func init() {
	prometheus.MustRegister(metricTradesTotal, metricDBBatches, metricMemLatency, metricDedupHits)
}

// ============================================================================
// 2. 核心数据模型
// ============================================================================

type AccountKey struct {
	UserID   uint64
	Currency string
}

type Account struct {
	UserID   uint64
	Currency string
	Balance  decimal.Decimal
}

type Position struct {
	UserID     uint64
	Symbol     string
	Size       decimal.Decimal
	EntryValue decimal.Decimal
}

type LedgerItem struct {
	TxID      string
	UserID    uint64
	Currency  string
	Amount    decimal.Decimal
	Type      string
	RelatedID uint64
	CreatedAt int64
}

type TradeEvent struct {
	MatchID      uint64          `json:"match_id"`
	MakerID      uint64          `json:"maker_id"`
	TakerID      uint64          `json:"taker_id"`
	Symbol       string          `json:"symbol"`
	Price        decimal.Decimal `json:"price"`
	Amount       decimal.Decimal `json:"amount"`
	Timestamp    int64           `json:"timestamp"`
	MakerFeeRate decimal.Decimal `json:"maker_fee_rate"`
	TakerFeeRate decimal.Decimal `json:"taker_fee_rate"`
}

// 内部消息：包含 Kafka 元数据
type PartitionMsg struct {
	Trade  TradeEvent
	Offset int64
}

// ============================================================================
// 3. 单分区计算引擎 (Engine Per Partition)
// ============================================================================

// Engine 负责处理【单个 Kafka Partition】的所有逻辑
// 它的所有操作都是单线程的，不需要内部锁 (API读取除外)
type Engine struct {
	partitionID int32

	// 状态数据
	accounts     map[AccountKey]*Account
	positions    map[string]*Position
	processedIDs map[uint64]int64 // Hot Dedup Cache

	// 通道
	inputChan  chan *PartitionMsg // 接收 Kafka 消息
	accChan    chan *Account      // 这里的指针传递实现了内存状态快照
	posChan    chan *Position
	ledgerChan chan *LedgerItem
	offsetChan chan int64
	quitChan   chan struct{} // 停止信号

	dbPool *pgxpool.Pool
	logger *zap.Logger
	mu     sync.RWMutex // 仅用于 API 读，写操作在 loop 中无锁
	wg     sync.WaitGroup
}

func NewEngine(partitionID int32, pool *pgxpool.Pool, logger *zap.Logger) *Engine {
	return &Engine{
		partitionID:  partitionID,
		accounts:     make(map[AccountKey]*Account),
		positions:    make(map[string]*Position),
		processedIDs: make(map[uint64]int64),
		inputChan:    make(chan *PartitionMsg, InputBufferSize),
		accChan:      make(chan *Account, PersistBufferSize),
		posChan:      make(chan *Position, PersistBufferSize),
		ledgerChan:   make(chan *LedgerItem, PersistBufferSize),
		offsetChan:   make(chan int64, PersistBufferSize),
		quitChan:     make(chan struct{}),
		dbPool:       pool,
		logger:       logger.With(zap.Int32("partition", partitionID)),
	}
}

// Start 启动该引擎的 处理协程 和 持久化协程
func (e *Engine) Start() {
	e.logger.Info("Engine Starting...")

	// 1. 预热数据 (Hydrate)
	// 生产环境优化：SELECT * FROM accounts WHERE hash(user_id) % total_parts = this_part
	// 这里为了安全性，全量加载 (如果数据量太大，需改为 Lazy Load 或基于分片的 SQL)
	e.hydrate()

	// 2. 启动业务处理循环 (Processor)
	e.wg.Add(1)
	go e.runProcessor()

	// 3. 启动持久化循环 (Persister)
	e.wg.Add(1)
	go e.runPersister()

	// 4. 启动内存 GC
	go e.runGC()
}

// Stop 优雅停止
func (e *Engine) Stop() {
	e.logger.Info("Engine Stopping...")
	close(e.quitChan) // 广播停止信号
	e.wg.Wait()       // 等待内部协程处理完
	e.logger.Info("Engine Stopped.")
}

// 预热逻辑
func (e *Engine) hydrate() {
	ctx := context.Background()
	// 加载 Accounts
	rows, _ := e.dbPool.Query(ctx, "SELECT user_id, currency, balance FROM accounts")
	defer rows.Close()
	for rows.Next() {
		acc := &Account{}
		rows.Scan(&acc.UserID, &acc.Currency, &acc.Balance)
		e.accounts[AccountKey{acc.UserID, acc.Currency}] = acc
	}
	// 加载 Positions
	rows2, _ := e.dbPool.Query(ctx, "SELECT user_id, symbol, size, entry_value FROM positions")
	defer rows2.Close()
	for rows2.Next() {
		pos := &Position{}
		rows2.Scan(&pos.UserID, &pos.Symbol, &pos.Size, &pos.EntryValue)
		e.positions[fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)] = pos
	}
}

// runGC 定期清理去重缓存
func (e *Engine) runGC() {
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-e.quitChan:
			return
		case <-ticker.C:
			// 因为 runGC 和 runProcessor 并发，需要加锁
			// 为了避免 ProcessTrade 里的频繁加锁，这里使用 Channel 通信或者简单的互斥
			// 鉴于 GC 频率极低，我们在 ProcessTrade 里加一把大锁是可接受的
			e.mu.Lock()
			now := time.Now().Unix()
			for id, ts := range e.processedIDs {
				if now-ts > 3600 {
					delete(e.processedIDs, id)
				}
			}
			e.mu.Unlock()
		}
	}
}

// checkIdempotency 冷热去重
func (e *Engine) checkIdempotency(matchID uint64) bool {
	// Hot Check
	if _, exists := e.processedIDs[matchID]; exists {
		metricDedupHits.WithLabelValues("mem").Inc()
		return true
	}
	// Cold Check
	txKey := fmt.Sprintf("M-%d", matchID)
	var dummy int
	err := e.dbPool.QueryRow(context.Background(), "SELECT 1 FROM ledger_entries WHERE tx_id=$1 LIMIT 1", txKey).Scan(&dummy)
	if err == nil {
		metricDedupHits.WithLabelValues("db").Inc()
		e.processedIDs[matchID] = time.Now().Unix() // 回填
		return true
	}
	return false
}

// runProcessor 核心业务循环 (单线程，无内部锁竞争)
func (e *Engine) runProcessor() {
	defer e.wg.Done()

	for {
		select {
		case <-e.quitChan:
			return // 停止处理新消息
		case msg := <-e.inputChan:
			e.processTradeLogic(msg)
		}
	}
}

func (e *Engine) processTradeLogic(msg *PartitionMsg) {
	t0 := time.Now()
	trade := msg.Trade

	// 加锁主要是为了配合 API 读取和 GC
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. 幂等检查
	if e.checkIdempotency(trade.MatchID) {
		e.offsetChan <- msg.Offset // 即使重复也要推进 Offset
		return
	}
	e.processedIDs[trade.MatchID] = time.Now().Unix()

	// 2. 准备上下文
	tradeVal := trade.Price.Mul(trade.Amount)
	ts := time.Now().UnixNano()
	txKey := fmt.Sprintf("M-%d", trade.MatchID)
	settleCurrency := "USDT"

	// 3. 闭包：处理单边
	processSide := func(uid uint64, isMaker bool, amountDelta decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
		rate := trade.TakerFeeRate
		if isMaker {
			rate = trade.MakerFeeRate
		}
		fee := tradeVal.Mul(rate)

		// Get/New Position
		posKey := fmt.Sprintf("%d-%s", uid, trade.Symbol)
		pos, ok := e.positions[posKey]
		if !ok {
			pos = &Position{UserID: uid, Symbol: trade.Symbol}
			e.positions[posKey] = pos
		}

		pnl := decimal.Zero
		// Flip Logic
		isSameDir := pos.Size.IsZero() || (pos.Size.Sign() == amountDelta.Sign())
		if isSameDir {
			cost := trade.Price.Mul(amountDelta.Abs())
			if amountDelta.IsNegative() {
				cost = cost.Neg()
			}
			pos.Size = pos.Size.Add(amountDelta)
			pos.EntryValue = pos.EntryValue.Add(cost)
		} else {
			if amountDelta.Abs().GreaterThan(pos.Size.Abs()) {
				// Flip
				avg := pos.EntryValue.Div(pos.Size).Abs()
				diff := trade.Price.Sub(avg)
				if pos.Size.IsPositive() {
					pnl = diff.Mul(pos.Size)
				} else {
					pnl = diff.Mul(pos.Size.Abs()).Neg()
				}

				rem := amountDelta.Add(pos.Size)
				pos.Size = rem
				newCost := trade.Price.Mul(rem.Abs())
				if rem.IsNegative() {
					newCost = newCost.Neg()
				}
				pos.EntryValue = newCost
			} else {
				// Reduce
				ratio := amountDelta.Div(pos.Size)
				released := pos.EntryValue.Mul(ratio)
				avg := pos.EntryValue.Div(pos.Size).Abs()
				if pos.Size.IsPositive() {
					pnl = trade.Price.Sub(avg).Mul(amountDelta.Abs())
				} else {
					pnl = avg.Sub(trade.Price).Mul(amountDelta.Abs())
				}
				pos.Size = pos.Size.Add(amountDelta)
				pos.EntryValue = pos.EntryValue.Add(released)
			}
		}

		// Send pointer to Persister
		e.posChan <- pos
		return pnl, fee
	}

	mPnL, mFee := processSide(trade.MakerID, true, trade.Amount.Neg())
	tPnL, tFee := processSide(trade.TakerID, false, trade.Amount)

	// 4. 闭包：更新资金
	updateBal := func(uid uint64, pnl, fee decimal.Decimal) {
		key := AccountKey{uid, settleCurrency}
		acc, ok := e.accounts[key]
		if !ok {
			acc = &Account{UserID: uid, Currency: settleCurrency}
			e.accounts[key] = acc
		}
		acc.Balance = acc.Balance.Add(pnl.Sub(fee))

		e.accChan <- acc

		if !fee.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: settleCurrency, Amount: fee.Neg(), Type: "FEE", RelatedID: trade.MatchID, CreatedAt: ts}
		}
		if !pnl.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: settleCurrency, Amount: pnl, Type: "PNL", RelatedID: trade.MatchID, CreatedAt: ts}
		}

		// Bankruptcy
		if acc.Balance.IsNegative() {
			loss := acc.Balance.Abs()
			acc.Balance = decimal.Zero
			e.ledgerChan <- &LedgerItem{TxID: txKey + "-INS", UserID: uid, Currency: settleCurrency, Amount: loss, Type: "INSURANCE_COVER", RelatedID: trade.MatchID, CreatedAt: ts}

			// System Account Deduct
			sKey := AccountKey{0, settleCurrency}
			sys, ok := e.accounts[sKey]
			if !ok {
				sys = &Account{UserID: 0, Currency: settleCurrency}
				e.accounts[sKey] = sys
			}
			sys.Balance = sys.Balance.Sub(loss)
			e.accChan <- sys
		}
	}

	updateBal(trade.MakerID, mPnL, mFee)
	updateBal(trade.TakerID, tPnL, tFee)

	// Revenue
	totalFee := mFee.Add(tFee)
	if !totalFee.IsZero() {
		sKey := AccountKey{0, settleCurrency}
		sys, ok := e.accounts[sKey]
		if !ok {
			sys = &Account{UserID: 0, Currency: settleCurrency}
			e.accounts[sKey] = sys
		}
		sys.Balance = sys.Balance.Add(totalFee)
		e.accChan <- sys
		e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: 0, Currency: settleCurrency, Amount: totalFee, Type: "REVENUE", RelatedID: trade.MatchID, CreatedAt: ts}
	}

	e.offsetChan <- msg.Offset
	metricTradesTotal.Inc()
	metricMemLatency.Observe(float64(time.Since(t0).Microseconds()))
}

// runPersister 异步持久化 (Batch)
func (e *Engine) runPersister() {
	defer e.wg.Done()
	ticker := time.NewTicker(BatchInterval)
	defer ticker.Stop()

	accDedup := make(map[AccountKey]*Account)
	posDedup := make(map[string]*Position)
	var ledgers []*LedgerItem
	var maxOffset int64 = -1

	flush := func() {
		if len(accDedup) == 0 && len(posDedup) == 0 && len(ledgers) == 0 && maxOffset == -1 {
			return
		}

		batch := &pgx.Batch{}
		ts := time.Now().UnixNano()

		// 1. Accounts Upsert
		for _, acc := range accDedup {
			batch.Queue(`INSERT INTO accounts (user_id, currency, balance, updated_at) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, currency) DO UPDATE SET balance = $3, updated_at = $4`,
				acc.UserID, acc.Currency, acc.Balance, ts)
		}
		// 2. Positions Upsert
		for _, pos := range posDedup {
			if pos.Size.IsZero() {
				batch.Queue("DELETE FROM positions WHERE user_id = $1 AND symbol = $2", pos.UserID, pos.Symbol)
			} else {
				batch.Queue(`INSERT INTO positions (user_id, symbol, size, entry_value, updated_at) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = $5`,
					pos.UserID, pos.Symbol, pos.Size, pos.EntryValue, ts)
			}
		}
		// 3. Ledger Insert (Ignore Duplicates)
		for _, l := range ledgers {
			batch.Queue(`INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id, created_at) 
				VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (tx_id, type, user_id) DO NOTHING`,
				l.TxID, l.UserID, l.Currency, l.Amount, l.Type, l.RelatedID, l.CreatedAt)
		}
		// 4. Checkpoint (Per Partition)
		if maxOffset >= 0 {
			partKey := fmt.Sprintf("%s-%d", KafkaTopic, e.partitionID)
			batch.Queue(`INSERT INTO system_state (topic_partition, last_offset) VALUES ($1, $2)
				ON CONFLICT (topic_partition) DO UPDATE SET last_offset = $2`, partKey, maxOffset)
		}

		// 5. Retry Loop (Crucial for reliability)
		for {
			br := e.dbPool.SendBatch(context.Background(), batch)
			_, err := br.Exec()
			br.Close()
			if err == nil {
				metricDBBatches.WithLabelValues(fmt.Sprintf("%d", e.partitionID)).Inc()
				break
			}
			e.logger.Error("DB Flush Failed, Retrying...", zap.Error(err))
			time.Sleep(1 * time.Second)
		}

		clear(accDedup)
		clear(posDedup)
		ledgers = ledgers[:0]
		maxOffset = -1
	}

	for {
		select {
		case acc := <-e.accChan:
			accDedup[AccountKey{acc.UserID, acc.Currency}] = acc
		case pos := <-e.posChan:
			posDedup[fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)] = pos
		case l := <-e.ledgerChan:
			ledgers = append(ledgers, l)
			if len(ledgers) >= BatchSize {
				flush()
			}
		case off := <-e.offsetChan:
			if off > maxOffset {
				maxOffset = off
			}
		case <-ticker.C:
			flush()
		case <-e.quitChan:
			// Drain Logic on Shutdown
			for {
				select {
				case acc := <-e.accChan:
					accDedup[AccountKey{acc.UserID, acc.Currency}] = acc
				case pos := <-e.posChan:
					posDedup[fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)] = pos
				case l := <-e.ledgerChan:
					ledgers = append(ledgers, l)
				case off := <-e.offsetChan:
					if off > maxOffset {
						maxOffset = off
					}
				default:
					flush() // Final Flush
					return
				}
			}
		}
	}
}

// ============================================================================
// 4. 分区管理器 (Partition Manager) - 解决问题 2: One Process Multi Partitions
// ============================================================================

type PartitionManager struct {
	engines map[int32]*Engine
	pool    *pgxpool.Pool
	logger  *zap.Logger
	mu      sync.Mutex
}

func NewPartitionManager(pool *pgxpool.Pool, logger *zap.Logger) *PartitionManager {
	return &PartitionManager{
		engines: make(map[int32]*Engine),
		pool:    pool,
		logger:  logger,
	}
}

// 路由消息到对应分区的 Engine
func (pm *PartitionManager) Dispatch(msg *kafka.Message) {
	partID := msg.TopicPartition.Partition

	pm.mu.Lock()
	engine, exists := pm.engines[partID]
	if !exists {
		// 懒加载：如果收到这个分区的消息，但 Engine 还没起，就启动它
		// 在 Rebalance 场景下，通常配合 OnAssigned 事件预启动
		engine = NewEngine(partID, pm.pool, pm.logger)
		engine.Start()
		pm.engines[partID] = engine
		pm.logger.Info("Spawned Engine for partition", zap.Int32("part", partID))
	}
	pm.mu.Unlock()

	// 构造内部消息
	var trade TradeEvent
	if err := json.Unmarshal(msg.Value, &trade); err != nil {
		pm.logger.Error("Bad JSON", zap.ByteString("val", msg.Value))
		// 仍需推进 Offset 避免死循环，这里简化处理
		return
	}

	// 发送给 Engine 的输入通道 (非阻塞或带缓冲)
	select {
	case engine.inputChan <- &PartitionMsg{Trade: trade, Offset: int64(msg.TopicPartition.Offset)}:
	default:
		pm.logger.Warn("Engine input channel full", zap.Int32("part", partID))
	}
}

// 停止所有引擎
func (pm *PartitionManager) Shutdown() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for id, eng := range pm.engines {
		pm.logger.Info("Stopping Engine", zap.Int32("part", id))
		eng.Stop()
	}
}

// ============================================================================
// 5. 主程序
// ============================================================================

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// DB
	config, _ := pgxpool.ParseConfig(DatabaseURL)
	config.MaxConns = 100 // 并发高，连接池要大
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		logger.Fatal("DB Init Failed", zap.Error(err))
	}

	// Partition Manager
	pm := NewPartitionManager(pool, logger)

	// Kafka Consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  KafkaBrokers,
		"group.id":           KafkaGroupID,
		"enable.auto.commit": false, // 必须禁用
		"auto.offset.reset":  "earliest",
		// 关键优化：心跳与元数据
		"session.timeout.ms":       6000,
		"go.events.channel.enable": true, // 启用事件通道以处理 Rebalance
	})
	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{KafkaTopic}, nil)

	// API Server (简化版，只读 Partition 0，生产环境需聚合所有 Engine)
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8080", nil)
	}()

	logger.Info("Cluster Node Started. Waiting for assignment...")

	// Main Event Loop
	run := true
	for run {
		select {
		case sig := <-func() chan os.Signal {
			s := make(chan os.Signal, 1)
			signal.Notify(s, syscall.SIGINT, syscall.SIGTERM)
			return s
		}():
			logger.Info("Signal received", zap.String("sig", sig.String()))
			run = false

		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				logger.Info("Partitions Assigned", zap.Any("parts", e.Partitions))
				c.Assign(e.Partitions)
				// 在这里可以做 Seek 操作：遍历 e.Partitions，查 DB，然后 Seek
				for _, p := range e.Partitions {
					key := fmt.Sprintf("%s-%d", *p.Topic, p.Partition)
					var lastOff int64
					err := pool.QueryRow(context.Background(), "SELECT last_offset FROM system_state WHERE topic_partition=$1", key).Scan(&lastOff)
					if err == nil {
						p.Offset = kafka.Offset(lastOff + 1)
						c.Seek(p, 0)
						logger.Info("Seeked", zap.String("key", key), zap.Int64("off", lastOff+1))
					}
				}

			case kafka.RevokedPartitions:
				logger.Info("Partitions Revoked", zap.Any("parts", e.Partitions))
				c.Unassign()
				// 这里应该调用 pm 停止对应的 Engine，为了代码简洁略过动态销毁逻辑
				// 生产环境需在此处销毁被撤销分区的 Engine 实例以释放内存

			case *kafka.Message:
				pm.Dispatch(e)

			case kafka.Error:
				logger.Error("Kafka Error", zap.Error(e))
			}
		}
	}

	pm.Shutdown()
	pool.Close()
	c.Close()
	logger.Info("Bye.")
}
