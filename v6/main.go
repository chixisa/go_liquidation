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
// 1. 基础配置与工具
// ============================================================================

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

var (
	KafkaTopic   = getEnv("KAFKA_TOPIC", "engine.trades")
	KafkaGroupID = getEnv("KAFKA_GROUP", "clearing_engine_v6_prod")
	KafkaBrokers = getEnv("KAFKA_BROKERS", "localhost:9092")
	DatabaseURL  = getEnv("DATABASE_URL", "postgres://root:123456@localhost:5432/clearing_db?sslmode=disable")

	// 生产环境调优参数
	BatchSize     = 5000
	BatchInterval = 200 * time.Millisecond
)

// ============================================================================
// 2. 监控指标
// ============================================================================

var (
	tradesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_trades_total", Help: "Total trades processed"})
	tradesSkipped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_trades_skipped_total", Help: "Total duplicate trades skipped"})
	dbBatches = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_db_batches_total", Help: "Total DB batches committed"})
	memLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "clearing_mem_latency_seconds", Help: "Latency of in-memory processing",
		Buckets: prometheus.ExponentialBuckets(0.000001, 10, 5)})
)

func init() {
	prometheus.MustRegister(tradesTotal, tradesSkipped, dbBatches, memLatency)
}

// ============================================================================
// 3. 数据模型 (去掉了 Dirty 字段)
// ============================================================================

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

// ============================================================================
// 4. 内存引擎 (In-Memory Engine)
// ============================================================================

type Engine struct {
	accounts  map[uint64]*Account
	positions map[string]*Position

	// 内存去重缓存 (Idempotency Cache)
	// 生产环境建议使用 Hashicorp LRU，这里用简单的 Map + 定期清理模拟
	processedIDs map[uint64]int64 // MatchID -> Timestamp

	// 待持久化通道 (只传递指针)
	accChan    chan *Account
	posChan    chan *Position
	ledgerChan chan *LedgerItem

	dbPool *pgxpool.Pool
	logger *zap.Logger
	mu     sync.RWMutex // 仅用于 Read API，ProcessTrade 主流程不持锁
}

func NewEngine(pool *pgxpool.Pool, logger *zap.Logger) *Engine {
	return &Engine{
		accounts:     make(map[uint64]*Account),
		positions:    make(map[string]*Position),
		processedIDs: make(map[uint64]int64),
		// 缓冲区要足够大，防止 Persister 抖动阻塞主线程
		accChan:    make(chan *Account, 100000),
		posChan:    make(chan *Position, 100000),
		ledgerChan: make(chan *LedgerItem, 100000),
		dbPool:     pool,
		logger:     logger,
	}
}

// 启动清理过期 ID 的协程 (简单的 GC)
func (e *Engine) StartGC() {
	ticker := time.NewTicker(10 * time.Minute)
	for range ticker.C {
		e.mu.Lock() // 这里加锁没关系，10分钟一次，且很快
		now := time.Now().Unix()
		count := 0
		for id, ts := range e.processedIDs {
			// 清理 1 小时前的数据 (假设 Kafka 也就是重发最近的消息)
			if now-ts > 3600 {
				delete(e.processedIDs, id)
				count++
			}
		}
		e.mu.Unlock()
		if count > 0 {
			e.logger.Info("GC Idempotency Cache", zap.Int("deleted", count))
		}
	}
}

func (e *Engine) Hydrate(ctx context.Context) error {
	e.logger.Info("Hydrating from DB...")
	// 1. Accounts
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
	// 2. Positions
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
	e.logger.Info("Hydration Done", zap.Int("acc", len(e.accounts)), zap.Int("pos", len(e.positions)))
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

// ProcessTrade 纯内存处理，无锁设计
// 前提：Kafka Key=UserID 保证了单线程处理该用户的逻辑
func (e *Engine) ProcessTrade(trade TradeEvent) {
	timer := prometheus.NewTimer(memLatency)
	defer timer.ObserveDuration()

	// 1. 内存幂等检查 (极速)
	if _, exists := e.processedIDs[trade.MatchID]; exists {
		tradesSkipped.Inc()
		return
	}
	e.processedIDs[trade.MatchID] = time.Now().Unix()

	// 为了 API 读取的一致性，这里加读写锁的写锁
	// 如果追求极致 TPS，可以去掉这个锁，让 API 读到瞬时不一致也没关系
	e.mu.Lock()
	defer e.mu.Unlock()

	tradeVal := trade.Price.Mul(trade.Amount)
	ts := time.Now().UnixNano()
	txKey := fmt.Sprintf("M-%d", trade.MatchID)

	// --- 内部逻辑 (闭包) ---
	processSide := func(uid uint64, isMaker bool, amountDelta decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
		rate := trade.TakerFeeRate
		if isMaker {
			rate = trade.MakerFeeRate
		}
		fee := tradeVal.Mul(rate)

		pos := e.getPosition(uid, trade.Symbol)
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
				avgPrice := pos.EntryValue.Div(pos.Size).Abs()
				diff := trade.Price.Sub(avgPrice)
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
				ratio := amountDelta.Div(pos.Size)
				released := pos.EntryValue.Mul(ratio)
				avgPrice := pos.EntryValue.Div(pos.Size).Abs()
				if pos.Size.IsPositive() {
					pnl = trade.Price.Sub(avgPrice).Mul(amountDelta.Abs())
				} else {
					pnl = avgPrice.Sub(trade.Price).Mul(amountDelta.Abs())
				}
				pos.Size = pos.Size.Add(amountDelta)
				pos.EntryValue = pos.EntryValue.Add(released)
			}
		}

		// >>> 核心修改：直接推送指针，不改 Dirty 标记 <<<
		select {
		case e.posChan <- pos:
		default:
			// 队列满，理论上应该阻塞或者扩容，这里为了不崩，打印个日志
			e.logger.Warn("Position Channel Full!")
		}
		return pnl, fee
	}

	mPnL, mFee := processSide(trade.MakerID, true, trade.Amount.Neg())
	tPnL, tFee := processSide(trade.TakerID, false, trade.Amount)

	// --- 资金逻辑 ---
	updateBal := func(uid uint64, pnl, fee decimal.Decimal) {
		acc := e.getAccount(uid)
		change := pnl.Sub(fee)
		acc.Balance = acc.Balance.Add(change)

		select {
		case e.accChan <- acc:
		default:
			e.logger.Warn("Account Channel Full!")
		}

		// 流水 (Append Only)
		if !fee.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: "USDT", Amount: fee.Neg(), Type: "FEE", RelatedID: trade.MatchID, CreatedAt: ts}
		}
		if !pnl.IsZero() {
			e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: "USDT", Amount: pnl, Type: "PNL", RelatedID: trade.MatchID, CreatedAt: ts}
		}

		// Bankruptcy
		if acc.Balance.IsNegative() {
			loss := acc.Balance.Abs()
			acc.Balance = decimal.Zero
			e.ledgerChan <- &LedgerItem{TxID: txKey + "-INS", UserID: uid, Currency: "USDT", Amount: loss, Type: "INSURANCE_COVER", RelatedID: trade.MatchID, CreatedAt: ts}

			sys := e.getAccount(0)
			sys.Balance = sys.Balance.Sub(loss)
			e.accChan <- sys
		}
	}

	updateBal(trade.MakerID, mPnL, mFee)
	updateBal(trade.TakerID, tPnL, tFee)

	// Revenue
	totalFee := mFee.Add(tFee)
	if !totalFee.IsZero() {
		sys := e.getAccount(0)
		sys.Balance = sys.Balance.Add(totalFee)
		e.accChan <- sys
		e.ledgerChan <- &LedgerItem{TxID: txKey, UserID: 0, Currency: "USDT", Amount: totalFee, Type: "REVENUE", RelatedID: trade.MatchID, CreatedAt: ts}
	}

	tradesTotal.Inc()
}

// ============================================================================
// 5. 生产级持久化 (Robust Persister)
// ============================================================================

func (e *Engine) StartPersister(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(BatchInterval)
	defer ticker.Stop()

	// 消费者端去重缓存 (Consumer-side Dedup)
	accDedup := make(map[uint64]*Account)
	posDedup := make(map[string]*Position)
	var ledgers []*LedgerItem

	// 写入数据库的闭包 (带重试)
	flushDB := func() {
		if len(accDedup) == 0 && len(posDedup) == 0 && len(ledgers) == 0 {
			return
		}

		batch := &pgx.Batch{}

		// 1. Accounts (Upsert) - 遍历 Map，天然去重
		for _, acc := range accDedup {
			batch.Queue(`
				INSERT INTO accounts (user_id, currency, balance, updated_at) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, currency) DO UPDATE SET balance = $3, updated_at = $4
			`, acc.UserID, acc.Currency, acc.Balance, time.Now().UnixNano())
		}

		// 2. Positions (Upsert/Delete)
		for _, pos := range posDedup {
			if pos.Size.IsZero() {
				batch.Queue("DELETE FROM positions WHERE user_id = $1 AND symbol = $2", pos.UserID, pos.Symbol)
			} else {
				batch.Queue(`
					INSERT INTO positions (user_id, symbol, size, entry_value, updated_at) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = $5
				`, pos.UserID, pos.Symbol, pos.Size, pos.EntryValue, time.Now().UnixNano())
			}
		}

		// 3. Ledgers (Batch Insert)
		for _, l := range ledgers {
			batch.Queue(`INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id, created_at) 
				VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				l.TxID, l.UserID, l.Currency, l.Amount, l.Type, l.RelatedID, l.CreatedAt)
		}

		// >>> 生产级重试逻辑 (Retry Loop) <<<
		for {
			br := e.dbPool.SendBatch(context.Background(), batch) // 使用 Background context 防止关机时被取消
			_, err := br.Exec()
			br.Close()

			if err == nil {
				dbBatches.Inc()
				break // 成功，跳出重试
			}

			e.logger.Error("DB Flush Failed! Retrying...", zap.Error(err))
			time.Sleep(1 * time.Second) // 简单的固定退避，生产环境可用 Exponential Backoff
		}

		// 清空缓存
		clear(accDedup)
		clear(posDedup)
		ledgers = ledgers[:0]
	}

	for {
		select {
		case acc := <-e.accChan:
			accDedup[acc.UserID] = acc // Map 覆盖，实现去重
		case pos := <-e.posChan:
			key := fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)
			posDedup[key] = pos
		case l := <-e.ledgerChan:
			ledgers = append(ledgers, l)
			if len(ledgers) >= BatchSize {
				flushDB()
			}
		case <-ticker.C:
			flushDB()
		case <-ctx.Done(): // 收到停机信号
			e.logger.Info("Persister draining channels...")
			// 停机时，排空 Channel 里的剩余数据
			// 注意：这里假设 Engine 已经停止写入，否则永远排不空
		DrainLoop:
			for {
				select {
				case acc := <-e.accChan:
					accDedup[acc.UserID] = acc
				case pos := <-e.posChan:
					posDedup[fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)] = pos
				case l := <-e.ledgerChan:
					ledgers = append(ledgers, l)
				default:
					break DrainLoop // 通道空了
				}
			}
			flushDB() // 最后一次刷盘
			e.logger.Info("Persister finished.")
			return
		}
	}
}

// ============================================================================
// 6. 程序入口
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

	// Engine Init
	engine := NewEngine(pool, logger)
	if err := engine.Hydrate(context.Background()); err != nil {
		logger.Fatal("Hydration Failed", zap.Error(err))
	}
	go engine.StartGC() // 启动内存清理

	// Start Persister
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go engine.StartPersister(ctx, &wg)

	// Kafka Consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaBrokers,
		"group.id":          KafkaGroupID,
		"auto.offset.reset": "earliest",
		// 禁用自动提交，改为手动或者依赖幂等性 (这里为了吞吐量使用自动提交+幂等去重)
		"enable.auto.commit": true,
	})
	if err != nil {
		logger.Fatal("Kafka Init Failed", zap.Error(err))
	}
	c.SubscribeTopics([]string{KafkaTopic}, nil)

	// Consumer Loop
	logger.Info("Engine Ready v6.0 (No-Lock Persistence)")

	go func() {
		for {
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			var trade TradeEvent
			if err := json.Unmarshal(ev.Value, &trade); err != nil {
				logger.Error("Invalid JSON", zap.ByteString("val", ev.Value))
				continue
			}

			engine.ProcessTrade(trade)
		}
	}()

	// API
	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/balance", func(w http.ResponseWriter, r *http.Request) {
		uid, _ := strconv.ParseUint(r.URL.Query().Get("user_id"), 10, 64)

		engine.mu.RLock()
		acc := engine.getAccount(uid)
		bal := acc.Balance // 此时读到的是内存副本值，安全
		engine.mu.RUnlock()

		fmt.Fprintf(w, `{"user_id": %d, "balance": "%s"}`, uid, bal.String())
	})
	go http.ListenAndServe(":8080", nil)

	// Graceful Shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("Stopping...")
	// 1. 先取消 Context，通知 Persister 进入 Drain 模式
	cancel()
	// 2. 等待 Persister 处理完所有剩余数据
	wg.Wait()
	// 3. 关闭资源
	c.Close()
	pool.Close()
	logger.Info("Stopped gracefully.")
}
