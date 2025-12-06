package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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
// 1. 配置与常量
// ============================================================================

var (
	KafkaTopic      = getEnv("KAFKA_TOPIC", "engine.trades")
	KafkaGroupID    = getEnv("KAFKA_GROUP", "clearing_engine_prod_v7")
	KafkaBrokers    = getEnv("KAFKA_BROKERS", "localhost:9092")
	DatabaseURL     = getEnv("DATABASE_URL", "postgres://root:123456@localhost:5432/clearing_db?sslmode=disable")
	TargetPartition = 0 // 生产环境通常每个 Pod 负责一个 Partition，这里硬编码为 0

	// 调优参数
	BatchSize         = 2000
	BatchInterval     = 100 * time.Millisecond
	ChannelBufferSize = 100000
)

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

// ============================================================================
// 2. 监控指标
// ============================================================================

var (
	tradesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_trades_total", Help: "Total trades processed"})
	tradesSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_trades_skipped_total", Help: "Duplicate trades skipped"})
	dbBatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_db_batches_total", Help: "DB batches committed"})
	insuranceTriggers = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_insurance_triggers", Help: "Bankruptcy events"})
	currentOffset = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clearing_kafka_offset", Help: "Current committed offset"})
)

func init() {
	prometheus.MustRegister(tradesTotal, tradesSkipped, dbBatches, insuranceTriggers, currentOffset)
}

// ============================================================================
// 3. 数据结构
// ============================================================================

// Account 内存状态
type Account struct {
	UserID   uint64
	Currency string
	Balance  decimal.Decimal
}

// Position 内存状态
type Position struct {
	UserID     uint64
	Symbol     string
	Size       decimal.Decimal
	EntryValue decimal.Decimal
}

// LedgerItem 待落盘的流水
type LedgerItem struct {
	TxID      string
	UserID    uint64
	Currency  string
	Amount    decimal.Decimal
	Type      string // FEE, PNL, REVENUE, INSURANCE...
	RelatedID uint64
	CreatedAt int64
}

// TradeEvent Kafka 消息结构
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

// ============================================================================
// 4. 核心引擎 (In-Memory Engine)
// ============================================================================

type Engine struct {
	accounts  map[uint64]*Account
	positions map[string]*Position

	// 内存去重缓存 (LRU简化版)
	processedIDs map[uint64]int64

	// 异步通道
	accChan    chan *Account
	posChan    chan *Position
	ledgerChan chan *LedgerItem
	offsetChan chan int64 // 传递 Kafka Offset

	dbPool *pgxpool.Pool
	logger *zap.Logger
	mu     sync.RWMutex // 仅用于 API 读锁
}

func NewEngine(pool *pgxpool.Pool, logger *zap.Logger) *Engine {
	return &Engine{
		accounts:     make(map[uint64]*Account),
		positions:    make(map[string]*Position),
		processedIDs: make(map[uint64]int64),
		accChan:      make(chan *Account, ChannelBufferSize),
		posChan:      make(chan *Position, ChannelBufferSize),
		ledgerChan:   make(chan *LedgerItem, ChannelBufferSize),
		offsetChan:   make(chan int64, ChannelBufferSize),
		dbPool:       pool,
		logger:       logger,
	}
}

// StartGC 定期清理内存中的去重记录
func (e *Engine) StartGC() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		e.mu.Lock()
		now := time.Now().Unix()
		deleted := 0
		for id, ts := range e.processedIDs {
			if now-ts > 3600 { // 1小时前的 ID 清理掉
				delete(e.processedIDs, id)
				deleted++
			}
		}
		e.mu.Unlock()
		if deleted > 0 {
			e.logger.Info("GC Cleaned IDs", zap.Int("count", deleted))
		}
	}
}

// Hydrate 从数据库恢复状态
func (e *Engine) Hydrate(ctx context.Context) error {
	e.logger.Info("Hydrating accounts...")
	rows, err := e.dbPool.Query(ctx, "SELECT user_id, currency, balance FROM accounts")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		acc := &Account{}
		if err := rows.Scan(&acc.UserID, &acc.Currency, &acc.Balance); err != nil {
			return err
		}
		e.accounts[acc.UserID] = acc
	}

	e.logger.Info("Hydrating positions...")
	rows2, err := e.dbPool.Query(ctx, "SELECT user_id, symbol, size, entry_value FROM positions")
	if err != nil {
		return err
	}
	defer rows2.Close()
	for rows2.Next() {
		pos := &Position{}
		if err := rows2.Scan(&pos.UserID, &pos.Symbol, &pos.Size, &pos.EntryValue); err != nil {
			return err
		}
		key := fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)
		e.positions[key] = pos
	}
	e.logger.Info("Hydration Complete", zap.Int("accounts", len(e.accounts)), zap.Int("positions", len(e.positions)))
	return nil
}

func (e *Engine) getAccount(uid uint64) *Account {
	if acc, ok := e.accounts[uid]; ok {
		return acc
	}
	acc := &Account{UserID: uid, Currency: "USDT", Balance: decimal.Zero}
	e.accounts[uid] = acc
	return acc
}

func (e *Engine) getPosition(uid uint64, symbol string) *Position {
	key := fmt.Sprintf("%d-%s", uid, symbol)
	if pos, ok := e.positions[key]; ok {
		return pos
	}
	pos := &Position{UserID: uid, Symbol: symbol, Size: decimal.Zero, EntryValue: decimal.Zero}
	e.positions[key] = pos
	return pos
}

// ProcessTrade 主处理逻辑 (全量无删减版)
func (e *Engine) ProcessTrade(trade TradeEvent, kafkaOffset int64) {
	// 1. 内存去重
	e.mu.Lock()
	if _, exists := e.processedIDs[trade.MatchID]; exists {
		e.mu.Unlock()
		tradesSkipped.Inc()
		// 即使跳过，也要更新 Offset，否则持久化层会卡在旧 Offset
		e.offsetChan <- kafkaOffset
		return
	}
	e.processedIDs[trade.MatchID] = time.Now().Unix()
	e.mu.Unlock()

	// 2. 加锁 (为了 API 读取一致性，如果是极致 TPS 可去掉)
	e.mu.Lock()
	defer e.mu.Unlock()

	ts := time.Now().UnixNano()
	tradeVal := trade.Price.Mul(trade.Amount)
	txKey := fmt.Sprintf("M-%d", trade.MatchID)

	// === 内部闭包：处理单边仓位 ===
	processSide := func(uid uint64, isMaker bool, amountDelta decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
		// 费率
		rate := trade.TakerFeeRate
		if isMaker {
			rate = trade.MakerFeeRate
		}
		fee := tradeVal.Mul(rate)

		// 获取持仓
		pos := e.getPosition(uid, trade.Symbol)
		pnl := decimal.Zero

		// 核心逻辑：Flip / Reduce / Open
		isSameDir := pos.Size.IsZero() || (pos.Size.Sign() == amountDelta.Sign())

		if isSameDir {
			// 情况 A: 加仓 (Open/Increase)
			cost := trade.Price.Mul(amountDelta.Abs())
			if amountDelta.IsNegative() {
				cost = cost.Neg() // 做空成本为负
			}
			pos.Size = pos.Size.Add(amountDelta)
			pos.EntryValue = pos.EntryValue.Add(cost)
		} else {
			// 情况 B: 减仓或翻转
			if amountDelta.Abs().GreaterThan(pos.Size.Abs()) {
				// B.1 翻转 (Flip)
				// 先平掉现有仓位
				avgPrice := pos.EntryValue.Div(pos.Size).Abs()
				diff := trade.Price.Sub(avgPrice)
				if pos.Size.IsPositive() {
					// 做多平仓 PnL = (Price - Entry) * Size
					pnl = diff.Mul(pos.Size)
				} else {
					// 做空平仓 PnL = (Entry - Price) * |Size|
					pnl = diff.Mul(pos.Size.Abs()).Neg()
				}

				// 再反向开仓
				remainder := amountDelta.Add(pos.Size) // 剩余数量
				pos.Size = remainder
				newCost := trade.Price.Mul(remainder.Abs())
				if remainder.IsNegative() {
					newCost = newCost.Neg()
				}
				pos.EntryValue = newCost

			} else {
				// B.2 仅减仓 (Reduce)
				// 按比例释放成本
				ratio := amountDelta.Div(pos.Size)    // 负数
				released := pos.EntryValue.Mul(ratio) // 负数

				// 计算 PnL
				avgPrice := pos.EntryValue.Div(pos.Size).Abs()
				if pos.Size.IsPositive() {
					// 做多卖出: (Price - Avg) * SoldAmount
					pnl = trade.Price.Sub(avgPrice).Mul(amountDelta.Abs())
				} else {
					// 做空买入: (Avg - Price) * BoughtAmount
					pnl = avgPrice.Sub(trade.Price).Mul(amountDelta.Abs())
				}

				pos.Size = pos.Size.Add(amountDelta)
				pos.EntryValue = pos.EntryValue.Add(released)
			}
		}

		// 推送持仓变更
		select {
		case e.posChan <- pos:
		default:
			e.logger.Warn("Position channel full")
		}

		return pnl, fee
	}

	// 执行双方
	mPnL, mFee := processSide(trade.MakerID, true, trade.Amount.Neg())
	tPnL, tFee := processSide(trade.TakerID, false, trade.Amount)

	// === 内部闭包：更新余额 ===
	updateBal := func(uid uint64, pnl, fee decimal.Decimal) {
		acc := e.getAccount(uid)
		change := pnl.Sub(fee)
		acc.Balance = acc.Balance.Add(change)

		// 推送账户变更
		select {
		case e.accChan <- acc:
		default:
			e.logger.Warn("Account channel full")
		}

		// 推送流水 (不可丢失)
		if !fee.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: "USDT", Amount: fee.Neg(), Type: "FEE", RelatedID: trade.MatchID, CreatedAt: ts}
		}
		if !pnl.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: "USDT", Amount: pnl, Type: "PNL", RelatedID: trade.MatchID, CreatedAt: ts}
		}

		// 破产检查 (Bankruptcy)
		if acc.Balance.IsNegative() {
			insuranceTriggers.Inc()
			loss := acc.Balance.Abs()
			acc.Balance = decimal.Zero // 重置为0

			// 记录赔付流水
			e.ledgerChan <- &LedgerItem{TxID: txKey + "-INS", UserID: uid, Currency: "USDT", Amount: loss, Type: "INSURANCE_COVER", RelatedID: trade.MatchID, CreatedAt: ts}

			// 扣减保险基金 (ID 0)
			sys := e.getAccount(0)
			sys.Balance = sys.Balance.Sub(loss)
			e.accChan <- sys
		}
	}

	updateBal(trade.MakerID, mPnL, mFee)
	updateBal(trade.TakerID, tPnL, tFee)

	// 平台收入记录
	totalFee := mFee.Add(tFee)
	if !totalFee.IsZero() {
		sys := e.getAccount(0)
		sys.Balance = sys.Balance.Add(totalFee)
		e.accChan <- sys
		e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: 0, Currency: "USDT", Amount: totalFee, Type: "REVENUE", RelatedID: trade.MatchID, CreatedAt: ts}
	}

	// 发送 Offset 给持久化线程
	select {
	case e.offsetChan <- kafkaOffset:
	default:
		e.logger.Warn("Offset channel full")
	}

	tradesTotal.Inc()
}

// ============================================================================
// 5. 持久化器 (The Persister)
// ============================================================================

func (e *Engine) StartPersister(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(BatchInterval)
	defer ticker.Stop()

	accDedup := make(map[uint64]*Account)
	posDedup := make(map[string]*Position)
	var ledgers []*LedgerItem
	var maxOffset int64 = -1

	// 内部函数：刷盘
	flushDB := func() {
		// 空检查：如果没有数据且 Offset 没变，跳过
		if len(accDedup) == 0 && len(posDedup) == 0 && len(ledgers) == 0 && maxOffset == -1 {
			return
		}

		batch := &pgx.Batch{}

		// 1. Accounts (Map天然去重)
		for _, acc := range accDedup {
			batch.Queue(`INSERT INTO accounts (user_id, currency, balance, updated_at) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, currency) DO UPDATE SET balance = $3, updated_at = $4`,
				acc.UserID, acc.Currency, acc.Balance, time.Now().UnixNano())
		}

		// 2. Positions (Map天然去重)
		for _, pos := range posDedup {
			if pos.Size.IsZero() {
				// 即使 Size 为 0 也建议 Update 0 而不是 Delete，防止 Delete 竞争，这里简化为 Update
				batch.Queue(`INSERT INTO positions (user_id, symbol, size, entry_value, updated_at) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = $5`,
					pos.UserID, pos.Symbol, pos.Size, pos.EntryValue, time.Now().UnixNano())
			} else {
				batch.Queue(`INSERT INTO positions (user_id, symbol, size, entry_value, updated_at) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = $5`,
					pos.UserID, pos.Symbol, pos.Size, pos.EntryValue, time.Now().UnixNano())
			}
		}

		// 3. Ledgers (Insert ON CONFLICT DO NOTHING - 关键去重)
		for _, l := range ledgers {
			batch.Queue(`INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id, created_at) 
				VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (tx_id, type) DO NOTHING`,
				l.TxID, l.UserID, l.Currency, l.Amount, l.Type, l.RelatedID, l.CreatedAt)
		}

		// 4. System State (Offset Checkpoint) - 放在同一个事务里
		if maxOffset >= 0 {
			sysKey := fmt.Sprintf("%s-%d", KafkaTopic, TargetPartition)
			batch.Queue(`INSERT INTO system_state (topic_partition, last_offset) VALUES ($1, $2)
				ON CONFLICT (topic_partition) DO UPDATE SET last_offset = $2`, sysKey, maxOffset)
		}

		// 5. 无限重试循环 (Until Success)
		retryCount := 0
		for {
			br := e.dbPool.SendBatch(context.Background(), batch) // 使用 Background 防止关机中断
			_, err := br.Exec()
			br.Close()

			if err == nil {
				dbBatches.Inc()
				currentOffset.Set(float64(maxOffset))
				break
			}

			retryCount++
			e.logger.Error("Persist Failed! Retrying...", zap.Error(err), zap.Int("attempt", retryCount))
			time.Sleep(1 * time.Second) // 固定退避
		}

		// 清空缓冲
		clear(accDedup)
		clear(posDedup)
		ledgers = ledgers[:0]
		maxOffset = -1
	}

	// 主循环
	for {
		select {
		case acc := <-e.accChan:
			accDedup[acc.UserID] = acc
		case pos := <-e.posChan:
			key := fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)
			posDedup[key] = pos
		case l := <-e.ledgerChan:
			ledgers = append(ledgers, l)
		case off := <-e.offsetChan:
			if off > maxOffset {
				maxOffset = off
			}

		case <-ticker.C:
			flushDB()

		case <-ctx.Done(): // 收到停机信号，执行排空逻辑
			e.logger.Info("Persister shutting down, draining channels...")
			// Draining Loop: 把 Channel 里剩下的东西全部读出来
		Drain:
			for {
				select {
				case acc := <-e.accChan:
					accDedup[acc.UserID] = acc
				case pos := <-e.posChan:
					posDedup[fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)] = pos
				case l := <-e.ledgerChan:
					ledgers = append(ledgers, l)
				case off := <-e.offsetChan:
					if off > maxOffset {
						maxOffset = off
					}
				default:
					break Drain // 通道空了
				}
			}
			flushDB() // 最后一次刷盘
			e.logger.Info("Persister finished.")
			return
		}

		// 如果积压过多，提前刷盘
		if len(ledgers) >= BatchSize {
			flushDB()
		}
	}
}

// ============================================================================
// 6. Kafka 辅助函数
// ============================================================================

func GetLastOffset(pool *pgxpool.Pool) int64 {
	var offset int64
	sysKey := fmt.Sprintf("%s-%d", KafkaTopic, TargetPartition)
	err := pool.QueryRow(context.Background(),
		"SELECT last_offset FROM system_state WHERE topic_partition=$1", sysKey).Scan(&offset)
	if err == pgx.ErrNoRows {
		return -1 // 没记录，从头开始
	}
	return offset
}

// ============================================================================
// 7. 程序入口
// ============================================================================

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// DB Init
	config, _ := pgxpool.ParseConfig(DatabaseURL)
	config.MaxConns = 50
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		logger.Fatal("DB Init Failed", zap.Error(err))
	}

	engine := NewEngine(pool, logger)
	go engine.StartGC()

	// 1. Hydrate
	if err := engine.Hydrate(context.Background()); err != nil {
		logger.Fatal("Hydration Failed", zap.Error(err))
	}

	// 2. Start Persister
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go engine.StartPersister(ctx, &wg)

	// 3. Kafka Config
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  KafkaBrokers,
		"group.id":           KafkaGroupID,
		"enable.auto.commit": false, // 必须禁用
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		panic(err)
	}

	// 4. Seek Logic (Crash Recovery)
	lastOffset := GetLastOffset(pool)
	tp := kafka.TopicPartition{
		Topic:     &KafkaTopic,
		Partition: int32(TargetPartition),
		Offset:    kafka.OffsetBeginning,
	}
	if lastOffset >= 0 {
		tp.Offset = kafka.Offset(lastOffset + 1) // 从下一条开始
		logger.Info("Seek to checkpoint", zap.Int64("offset", lastOffset+1))
	}
	// 手动 Assign
	c.Assign([]kafka.TopicPartition{tp})

	// 5. Start API
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/balance", func(w http.ResponseWriter, r *http.Request) {
			uid, _ := strconv.ParseUint(r.URL.Query().Get("user_id"), 10, 64)
			engine.mu.RLock()
			acc := engine.getAccount(uid)
			bal := acc.Balance
			engine.mu.RUnlock()
			fmt.Fprintf(w, `{"user_id": %d, "balance": "%s"}`, uid, bal.String())
		})
		logger.Info("API Listening on :8080")
		http.ListenAndServe(":8080", nil)
	}()

	logger.Info("Engine v7.0 Started")

	// 6. Main Loop
	go func() {
		for {
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			var trade TradeEvent
			if err := json.Unmarshal(ev.Value, &trade); err != nil {
				logger.Error("Bad JSON", zap.ByteString("val", ev.Value))
				// 即使是坏消息，也要推进 Offset，否则会死循环
				engine.offsetChan <- int64(ev.TopicPartition.Offset)
				continue
			}

			engine.ProcessTrade(trade, int64(ev.TopicPartition.Offset))
		}
	}()

	// 7. Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("Stopping...")
	cancel() // 通知 Persister 排空
	wg.Wait()
	c.Close()
	pool.Close()
	logger.Info("Stopped.")
}
