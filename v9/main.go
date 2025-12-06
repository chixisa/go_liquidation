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
// 1. 基础配置
// ============================================================================

var (
	KafkaTopic   = getEnv("KAFKA_TOPIC", "engine.trades")
	KafkaBrokers = getEnv("KAFKA_BROKERS", "localhost:9092")
	KafkaGroupID = getEnv("KAFKA_GROUP", "clearing_cluster_v9_isolated")
	DatabaseURL  = getEnv("DATABASE_URL", "postgres://root:123456@localhost:5432/clearing_db?sslmode=disable")

	TargetPartition = 0
	ShardID         = fmt.Sprintf("clearing-shard-%d", TargetPartition)

	BatchInterval     = 200 * time.Millisecond
	BatchSize         = 5000
	InputBufferSize   = 50000
	PersistBufferSize = 50000
)

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

const (
	MarginModeCross    = 0
	MarginModeIsolated = 1
)

// ============================================================================
// 2. 监控
// ============================================================================

var (
	metricTradesTotal = prometheus.NewCounter(prometheus.CounterOpts{Name: "clearing_trades_total"})
	metricDedupHits   = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "clearing_dedup_hits"}, []string{"type"})
	metricDBBatches   = prometheus.NewCounter(prometheus.CounterOpts{Name: "clearing_db_batches"})
)

func init() {
	prometheus.MustRegister(metricTradesTotal, metricDedupHits, metricDBBatches)
}

// ============================================================================
// 3. 数据模型
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
	UserID         uint64
	Symbol         string
	Size           decimal.Decimal
	EntryValue     decimal.Decimal
	MarginMode     int             // 0: Cross, 1: Isolated
	IsolatedMargin decimal.Decimal // 逐仓保证金
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

	// 新增：指定开仓模式 (仅在开仓时有效，平仓时忽略)
	MakerMarginMode int `json:"maker_margin_mode"`
	TakerMarginMode int `json:"taker_margin_mode"`
	// 新增：逐仓初始保证金 (仅开仓有效)
	MakerInitialMargin decimal.Decimal `json:"maker_initial_margin"`
	TakerInitialMargin decimal.Decimal `json:"taker_initial_margin"`
}

type PartitionMsg struct {
	Trade  TradeEvent
	Offset int64
}

// ============================================================================
// 4. 引擎 (Engine)
// ============================================================================

type Engine struct {
	partitionID  int32
	accounts     map[AccountKey]*Account
	positions    map[string]*Position
	processedIDs map[uint64]int64

	inputChan  chan *PartitionMsg
	accChan    chan *Account
	posChan    chan *Position
	ledgerChan chan *LedgerItem
	offsetChan chan int64
	quitChan   chan struct{}

	dbPool *pgxpool.Pool
	logger *zap.Logger
	mu     sync.RWMutex
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

func (e *Engine) Start() {
	e.hydrate()
	e.wg.Add(1)
	go e.runProcessor()
	e.wg.Add(1)
	go e.runPersister()
	go e.runGC()
}

func (e *Engine) Stop() {
	close(e.quitChan)
	e.wg.Wait()
}

func (e *Engine) hydrate() {
	ctx := context.Background()
	// 1. Accounts
	rows, _ := e.dbPool.Query(ctx, "SELECT user_id, currency, balance FROM accounts")
	defer rows.Close()
	for rows.Next() {
		acc := &Account{}
		rows.Scan(&acc.UserID, &acc.Currency, &acc.Balance)
		e.accounts[AccountKey{acc.UserID, acc.Currency}] = acc
	}
	// 2. Positions (注意新字段)
	rows2, _ := e.dbPool.Query(ctx, "SELECT user_id, symbol, size, entry_value, margin_mode, isolated_margin FROM positions")
	defer rows2.Close()
	for rows2.Next() {
		pos := &Position{}
		rows2.Scan(&pos.UserID, &pos.Symbol, &pos.Size, &pos.EntryValue, &pos.MarginMode, &pos.IsolatedMargin)
		e.positions[fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)] = pos
	}
}

func (e *Engine) runGC() {
	ticker := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-e.quitChan:
			return
		case <-ticker.C:
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

func (e *Engine) checkIdempotency(matchID uint64) bool {
	if _, exists := e.processedIDs[matchID]; exists {
		metricDedupHits.WithLabelValues("mem").Inc()
		return true
	}
	var dummy int
	err := e.dbPool.QueryRow(context.Background(), "SELECT 1 FROM ledger_entries WHERE tx_id=$1 LIMIT 1", fmt.Sprintf("M-%d", matchID)).Scan(&dummy)
	if err == nil {
		metricDedupHits.WithLabelValues("db").Inc()
		e.processedIDs[matchID] = time.Now().Unix()
		return true
	}
	return false
}

func (e *Engine) runProcessor() {
	defer e.wg.Done()
	for {
		select {
		case <-e.quitChan:
			return
		case msg := <-e.inputChan:
			e.processTradeLogic(msg)
		}
	}
}

// 核心逻辑升级
func (e *Engine) processTradeLogic(msg *PartitionMsg) {
	trade := msg.Trade
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.checkIdempotency(trade.MatchID) {
		e.offsetChan <- msg.Offset
		return
	}
	e.processedIDs[trade.MatchID] = time.Now().Unix()

	ts := time.Now().UnixNano()
	tradeVal := trade.Price.Mul(trade.Amount)
	txKey := fmt.Sprintf("M-%d", trade.MatchID)
	settleCurrency := "USDT"

	// === 处理单边 (含逐仓逻辑) ===
	processSide := func(uid uint64, isMaker bool, amountDelta decimal.Decimal, mode int, initMargin decimal.Decimal) (decimal.Decimal, decimal.Decimal, *Position) {
		// 1. 费率
		rate := trade.TakerFeeRate
		if isMaker {
			rate = trade.MakerFeeRate
		}
		fee := tradeVal.Mul(rate)

		// 2. 获取持仓
		posKey := fmt.Sprintf("%d-%s", uid, trade.Symbol)
		pos, ok := e.positions[posKey]
		if !ok {
			// 新开仓：应用传入的 MarginMode 和 InitialMargin
			pos = &Position{
				UserID:         uid,
				Symbol:         trade.Symbol,
				Size:           decimal.Zero,
				MarginMode:     mode,
				IsolatedMargin: decimal.Zero,
			}
			e.positions[posKey] = pos
		}

		// 逐仓逻辑：如果是开仓，需要把初始保证金加进去
		// 这里简化：假设撮合引擎已经在 Balance 里冻结了这笔钱，并在 TradeEvent 里传过来了
		// 我们需要从 Balance 扣除 initMargin，加到 IsolatedMargin
		// 注意：initMargin 只在 Open 时为正，Close 时为 0
		if mode == MarginModeIsolated && !initMargin.IsZero() {
			pos.IsolatedMargin = pos.IsolatedMargin.Add(initMargin)
		}

		pnl := decimal.Zero
		// Flip / Reduce / Open
		isSameDir := pos.Size.IsZero() || (pos.Size.Sign() == amountDelta.Sign())

		if isSameDir {
			// 加仓
			cost := trade.Price.Mul(amountDelta.Abs())
			if amountDelta.IsNegative() {
				cost = cost.Neg()
			}
			pos.Size = pos.Size.Add(amountDelta)
			pos.EntryValue = pos.EntryValue.Add(cost)
		} else {
			// 减仓/翻转
			if amountDelta.Abs().GreaterThan(pos.Size.Abs()) {
				// Flip
				avg := pos.EntryValue.Div(pos.Size).Abs()
				diff := trade.Price.Sub(avg)
				if pos.Size.IsPositive() {
					pnl = diff.Mul(pos.Size)
				} else {
					pnl = diff.Mul(pos.Size.Abs()).Neg()
				}

				// 新仓位
				rem := amountDelta.Add(pos.Size)
				pos.Size = rem
				newCost := trade.Price.Mul(rem.Abs())
				if rem.IsNegative() {
					newCost = newCost.Neg()
				}
				pos.EntryValue = newCost

				// 翻转时的逐仓处理：
				// 旧仓位的 PnL 结算掉，新仓位的保证金重置 (这里逻辑较复杂，简化处理：PnL 归 PnL，保证金不动)
			} else {
				// Reduce
				avg := pos.EntryValue.Div(pos.Size).Abs()
				if pos.Size.IsPositive() {
					pnl = trade.Price.Sub(avg).Mul(amountDelta.Abs())
				} else {
					pnl = avg.Sub(trade.Price).Mul(amountDelta.Abs())
				}
				ratio := amountDelta.Div(pos.Size)
				released := pos.EntryValue.Mul(ratio)
				pos.Size = pos.Size.Add(amountDelta)
				pos.EntryValue = pos.EntryValue.Add(released)
			}
		}

		// Push
		select {
		case e.posChan <- pos:
		default:
		}
		return pnl, fee, pos
	}

	// 执行计算
	mPnL, mFee, mPos := processSide(trade.MakerID, true, trade.Amount.Neg(), trade.MakerMarginMode, trade.MakerInitialMargin)
	tPnL, tFee, tPos := processSide(trade.TakerID, false, trade.Amount, trade.TakerMarginMode, trade.TakerInitialMargin)

	// === 资金结算 (含逐仓穿仓保护) ===
	updateBal := func(uid uint64, pnl, fee decimal.Decimal, pos *Position, initMargin decimal.Decimal) {
		accKey := AccountKey{uid, settleCurrency}
		acc, ok := e.accounts[accKey]
		if !ok {
			acc = &Account{UserID: uid, Currency: settleCurrency}
			e.accounts[accKey] = acc
		}

		// 1. 先处理开仓保证金扣除 (从余额转入逐仓账户)
		if pos.MarginMode == MarginModeIsolated && !initMargin.IsZero() {
			acc.Balance = acc.Balance.Sub(initMargin)
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: settleCurrency, Amount: initMargin.Neg(), Type: "MARGIN_TRANSFER", RelatedID: trade.MatchID, CreatedAt: ts}
		}

		// 2. 处理 PnL 和 Fee
		netChange := pnl.Sub(fee)

		if pos.MarginMode == MarginModeIsolated {
			// >>> 逐仓模式 <<<
			// 盈亏和手续费优先作用于逐仓保证金
			pos.IsolatedMargin = pos.IsolatedMargin.Add(netChange)

			// 穿仓检查 (Isolated Bankruptcy)
			if pos.IsolatedMargin.IsNegative() {
				loss := pos.IsolatedMargin.Abs()
				pos.IsolatedMargin = decimal.Zero // 归零

				// 记录保险赔付
				e.ledgerChan <- &LedgerItem{TxID: txKey + "-INS", UserID: uid, Currency: settleCurrency, Amount: loss, Type: "INSURANCE_COVER_ISO", RelatedID: trade.MatchID, CreatedAt: ts}

				// 系统账户扣款 (不扣用户余额！)
				sysKey := AccountKey{0, settleCurrency}
				sys, _ := e.accounts[sysKey] // 假设已初始化
				sys.Balance = sys.Balance.Sub(loss)
				e.accChan <- sys
			}

			// 推送 Position 更新 (因为改了 IsolatedMargin)
			e.posChan <- pos

		} else {
			// >>> 全仓模式 <<<
			// 直接作用于余额
			acc.Balance = acc.Balance.Add(netChange)

			// 全仓穿仓检查
			if acc.Balance.IsNegative() {
				loss := acc.Balance.Abs()
				acc.Balance = decimal.Zero
				e.ledgerChan <- &LedgerItem{TxID: txKey + "-INS", UserID: uid, Currency: settleCurrency, Amount: loss, Type: "INSURANCE_COVER_CROSS", RelatedID: trade.MatchID, CreatedAt: ts}

				sysKey := AccountKey{0, settleCurrency}
				sys, _ := e.accounts[sysKey]
				sys.Balance = sys.Balance.Sub(loss)
				e.accChan <- sys
			}
		}

		// Push Account
		select {
		case e.accChan <- acc:
		default:
		}

		// Log Fee & PnL
		if !fee.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: settleCurrency, Amount: fee.Neg(), Type: "FEE", RelatedID: trade.MatchID, CreatedAt: ts}
		}
		if !pnl.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: settleCurrency, Amount: pnl, Type: "PNL", RelatedID: trade.MatchID, CreatedAt: ts}
		}
	}

	updateBal(trade.MakerID, mPnL, mFee, mPos, trade.MakerInitialMargin)
	updateBal(trade.TakerID, tPnL, tFee, tPos, trade.TakerInitialMargin)

	// Revenue
	totalFee := mFee.Add(tFee)
	if !totalFee.IsZero() {
		sysKey := AccountKey{0, settleCurrency}
		sys, ok := e.accounts[sysKey]
		if !ok {
			sys = &Account{UserID: 0, Currency: settleCurrency}
			e.accounts[sysKey] = sys
		}
		sys.Balance = sys.Balance.Add(totalFee)
		e.accChan <- sys
		e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: 0, Currency: settleCurrency, Amount: totalFee, Type: "REVENUE", RelatedID: trade.MatchID, CreatedAt: ts}
	}

	e.offsetChan <- msg.Offset
	metricTradesTotal.Inc()
}

// ============================================================================
// 5. 持久化器
// ============================================================================

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

		for _, acc := range accDedup {
			batch.Queue(`INSERT INTO accounts (user_id, currency, balance, updated_at) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, currency) DO UPDATE SET balance = $3, updated_at = $4`,
				acc.UserID, acc.Currency, acc.Balance, ts)
		}
		for _, pos := range posDedup {
			if pos.Size.IsZero() && pos.IsolatedMargin.IsZero() {
				// 仓位和保证金都归零才删除
				batch.Queue("DELETE FROM positions WHERE user_id = $1 AND symbol = $2", pos.UserID, pos.Symbol)
			} else {
				// 注意：必须更新 isolated_margin
				batch.Queue(`INSERT INTO positions (user_id, symbol, size, entry_value, margin_mode, isolated_margin, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7)
					ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, margin_mode = $5, isolated_margin = $6, updated_at = $7`,
					pos.UserID, pos.Symbol, pos.Size, pos.EntryValue, pos.MarginMode, pos.IsolatedMargin, ts)
			}
		}
		for _, l := range ledgers {
			batch.Queue(`INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id, created_at) 
				VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (tx_id, type, user_id) DO NOTHING`,
				l.TxID, l.UserID, l.Currency, l.Amount, l.Type, l.RelatedID, l.CreatedAt)
		}
		if maxOffset >= 0 {
			sysKey := fmt.Sprintf("%s-%d", KafkaTopic, e.partitionID)
			batch.Queue(`INSERT INTO system_state (topic_partition, last_offset) VALUES ($1, $2)
				ON CONFLICT (topic_partition) DO UPDATE SET last_offset = $2`, sysKey, maxOffset)
		}

		for {
			br := e.dbPool.SendBatch(context.Background(), batch)
			_, err := br.Exec()
			br.Close()
			if err == nil {
				metricDBBatches.Inc()
				break
			}
			e.logger.Error("Flush Retry", zap.Error(err))
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
			// Drain...
			flush()
			return
		}
	}
}

// ============================================================================
// 6. Partition Manager
// ============================================================================

type PartitionManager struct {
	engines map[int32]*Engine
	pool    *pgxpool.Pool
	logger  *zap.Logger
	mu      sync.Mutex
}

func NewPartitionManager(pool *pgxpool.Pool, logger *zap.Logger) *PartitionManager {
	return &PartitionManager{engines: make(map[int32]*Engine), pool: pool, logger: logger}
}

func (pm *PartitionManager) Dispatch(msg *kafka.Message) {
	partID := msg.TopicPartition.Partition
	// 无锁读取 (Fast Path)
	// 仅当 map 结构变更(Rebalance)时才需要锁，这里假设 Start 后 map 稳定
	// 严格来说需要 RLock，但在此单线程 Loop 中是安全的
	// 为严谨起见，我们在 Rebalance 事件中处理 Map 变更

	// 在单线程 select loop 中，不需要锁
	engine, ok := pm.engines[partID]
	if !ok {
		// Lazy Init (仅当没有在 AssignedPartitions 中初始化时)
		engine = NewEngine(partID, pm.pool, pm.logger)
		engine.Start()
		pm.engines[partID] = engine
	}

	var trade TradeEvent
	json.Unmarshal(msg.Value, &trade)

	select {
	case engine.inputChan <- &PartitionMsg{Trade: trade, Offset: int64(msg.TopicPartition.Offset)}:
	default:
		pm.logger.Warn("Engine full", zap.Int32("p", partID))
	}
}

// ============================================================================
// 7. Main
// ============================================================================

func main() {
	logger, _ := zap.NewProduction()
	config, _ := pgxpool.ParseConfig(DatabaseURL)
	config.MaxConns = 100
	pool, _ := pgxpool.NewWithConfig(context.Background(), config)

	pm := NewPartitionManager(pool, logger)

	c, _ := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  KafkaBrokers,
		"group.id":           KafkaGroupID,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})
	c.SubscribeTopics([]string{KafkaTopic}, nil)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":8080", nil)
	}()

	logger.Info("System Started v9.0 (Isolated Margin)")

	run := true
	for run {
		select {
		case sig := <-func() chan os.Signal { s := make(chan os.Signal, 1); signal.Notify(s, syscall.SIGINT); return s }():
			logger.Info("Signal", zap.String("s", sig.String()))
			run = false
		case ev := <-c.Events():
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				c.Assign(e.Partitions)
				for _, p := range e.Partitions {
					key := fmt.Sprintf("%s-%d", *p.Topic, p.Partition)
					var off int64 = -1
					pool.QueryRow(context.Background(), "SELECT last_offset FROM system_state WHERE topic_partition=$1", key).Scan(&off)
					if off >= 0 {
						p.Offset = kafka.Offset(off + 1)
						c.Seek(p, 0)
					}
					// 预启动 Engine
					if _, ok := pm.engines[p.Partition]; !ok {
						eng := NewEngine(p.Partition, pool, logger)
						eng.Start()
						pm.engines[p.Partition] = eng
					}
				}
			case *kafka.Message:
				pm.Dispatch(e)
			}
		}
	}

	for _, eng := range pm.engines {
		eng.Stop()
	}
	pool.Close()
	c.Close()
}
