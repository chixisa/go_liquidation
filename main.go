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
// 1. 配置区域
// ============================================================================

const (
	KafkaTopicTrades  = "engine.trades"
	KafkaGroupID      = "clearing_core_v5"
	
	// 性能调优参数
	PersistInterval   = 200 * time.Millisecond // 刷盘间隔
	PersistBatchSize  = 5000                   // 批量写入阈值 (Postgres COPY 性能极佳，可以设大)
	ChannelBufferSize = 100000                 // 内存缓冲队列长度
)

// ============================================================================
// 2. 内存状态结构 (Hot Data)
// ============================================================================

// Account 内存账户对象 (无锁，因为单线程独占)
type Account struct {
	UserID   uint64
	Currency string
	Balance  decimal.Decimal
	Dirty    bool // 脏标记：true 表示内存变了，但还没写库
}

// Position 内存持仓对象
type Position struct {
	UserID     uint64
	Symbol     string
	Size       decimal.Decimal
	EntryValue decimal.Decimal
	Dirty      bool
}

// LedgerItem 待写入的流水
type LedgerItem struct {
	TxID      string
	UserID    uint64
	Currency  string
	Amount    decimal.Decimal
	Type      string
	RelatedID uint64
	CreatedAt int64
}

// TradeEvent 来自撮合引擎的消息
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
// 3. 结算引擎核心 (The Core)
// ============================================================================

type Engine struct {
	// 状态数据 (Map Sharding 可以在这里做，本例简化为单 Map)
	// 在多 Partition 模式下，应该运行多个 Engine 实例，每个负责一部分 UserID
	accounts  map[uint64]*Account
	positions map[string]*Position

	// 待持久化队列 (RingBuffer 概念)
	persistChan chan interface{} // 存放 *Account, *Position, *LedgerItem

	dbPool *pgxpool.Pool
	logger *zap.Logger
	mu     sync.RWMutex // 仅用于外部 API 读取时的快照读，写入逻辑是单线程的
}

func NewEngine(pool *pgxpool.Pool, logger *zap.Logger) *Engine {
	return &Engine{
		accounts:    make(map[uint64]*Account),
		positions:   make(map[string]*Position),
		persistChan: make(chan interface{}, ChannelBufferSize),
		dbPool:      pool,
		logger:      logger,
	}
}

// Hydrate 启动时预热：将 DB 数据全量加载到内存
func (e *Engine) Hydrate(ctx context.Context) error {
	start := time.Now()
	e.logger.Info("Starting Hydration...")

	// 1. 加载账户
	rows, err := e.dbPool.Query(ctx, "SELECT user_id, currency, balance FROM accounts")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		acc := &Account{Currency: "USDT"} // 默认USDT
		if err := rows.Scan(&acc.UserID, &acc.Currency, &acc.Balance); err != nil {
			return err
		}
		e.accounts[acc.UserID] = acc
	}

	// 2. 加载持仓
	pRows, err := e.dbPool.Query(ctx, "SELECT user_id, symbol, size, entry_value FROM positions")
	if err != nil {
		return err
	}
	defer pRows.Close()
	for pRows.Next() {
		pos := &Position{}
		if err := pRows.Scan(&pos.UserID, &pos.Symbol, &pos.Size, &pos.EntryValue); err != nil {
			return err
		}
		key := fmt.Sprintf("%d-%s", pos.UserID, pos.Symbol)
		e.positions[key] = pos
	}

	e.logger.Info("Hydration Complete", 
		zap.Int("accounts", len(e.accounts)), 
		zap.Int("positions", len(e.positions)), 
		zap.Duration("duration", time.Since(start)))
	return nil
}

// getAccount 内存极速读取
func (e *Engine) getAccount(uid uint64) *Account {
	if acc, ok := e.accounts[uid]; ok {
		return acc
	}
	// 懒加载：如果内存没有，新建一个空账户
	acc := &Account{UserID: uid, Currency: "USDT", Balance: decimal.Zero}
	e.accounts[uid] = acc
	return acc
}

// getPosition 内存极速读取
func (e *Engine) getPosition(uid uint64, symbol string) *Position {
	key := fmt.Sprintf("%d-%s", uid, symbol)
	if pos, ok := e.positions[key]; ok {
		return pos
	}
	pos := &Position{UserID: uid, Symbol: symbol, Size: decimal.Zero, EntryValue: decimal.Zero}
	e.positions[key] = pos
	return pos
}

// ProcessTrade 核心业务逻辑 - 纯内存操作，无 IO
func (e *Engine) ProcessTrade(trade TradeEvent) {
	// 加读写锁是防止 API 读取时产生并发问题
	// 如果是纯处理线程，这里可以去掉锁，进一步提升性能
	e.mu.Lock()
	defer e.mu.Unlock()

	tradeVal := trade.Price.Mul(trade.Amount)
	ts := time.Now().UnixNano()

	// --- 定义处理单边的闭包 (和之前逻辑一致，但操作内存对象) ---
	processSide := func(uid uint64, isMaker bool, amountDelta decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
		// 1. 费率
		rate := trade.TakerFeeRate
		if isMaker { rate = trade.MakerFeeRate }
		fee := tradeVal.Mul(rate)

		// 2. 持仓逻辑
		pos := e.getPosition(uid, trade.Symbol)
		pnl := decimal.Zero

		// 简化版翻转逻辑 (和之前完全一致，略去注释)
		isSameDir := pos.Size.IsZero() || (pos.Size.Sign() == amountDelta.Sign())
		
		if isSameDir {
			// 加仓
			cost := trade.Price.Mul(amountDelta.Abs())
			if amountDelta.IsNegative() { cost = cost.Neg() }
			pos.Size = pos.Size.Add(amountDelta)
			pos.EntryValue = pos.EntryValue.Add(cost)
		} else {
			// 减仓/翻转
			if amountDelta.Abs().GreaterThan(pos.Size.Abs()) {
				// Flip
				avgPrice := pos.EntryValue.Div(pos.Size).Abs()
				diff := trade.Price.Sub(avgPrice)
				if pos.Size.IsPositive() {
					pnl = diff.Mul(pos.Size)
				} else {
					pnl = diff.Mul(pos.Size.Abs()).Neg()
				}
				
				remainder := amountDelta.Add(pos.Size)
				pos.Size = remainder
				newCost := trade.Price.Mul(remainder.Abs())
				if remainder.IsNegative() { newCost = newCost.Neg() }
				pos.EntryValue = newCost
			} else {
				// Reduce
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

		// 标记脏并发送到持久化队列
		if !pos.Dirty {
			pos.Dirty = true
			e.persistChan <- pos // 指针传递
		}
		
		return pnl, fee
	}

	// 执行 Maker/Taker 逻辑
	mPnL, mFee := processSide(trade.MakerID, true, trade.Amount.Neg())
	tPnL, tFee := processSide(trade.TakerID, false, trade.Amount)

	// --- 资金结算 ---
	updateBal := func(uid uint64, pnl, fee decimal.Decimal) {
		acc := e.getAccount(uid)
		change := pnl.Sub(fee)
		acc.Balance = acc.Balance.Add(change)

		if !acc.Dirty {
			acc.Dirty = true
			e.persistChan <- acc
		}

		// 生成流水
		txKey := fmt.Sprintf("M-%d", trade.MatchID)
		if !fee.IsZero() {
			e.persistChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: "USDT", Amount: fee.Neg(), Type: "FEE", RelatedID: trade.MatchID, CreatedAt: ts}
		}
		if !pnl.IsZero() {
			e.persistChan <- &LedgerItem{TxID: txKey, UserID: uid, Currency: "USDT", Amount: pnl, Type: "PNL", RelatedID: trade.MatchID, CreatedAt: ts}
		}

		// 破产逻辑 (Bankruptcy)
		if acc.Balance.IsNegative() {
			loss := acc.Balance.Abs()
			acc.Balance = decimal.Zero // 归零
			e.persistChan <- &LedgerItem{TxID: txKey+"-INS", UserID: uid, Currency: "USDT", Amount: loss, Type: "INSURANCE_COVER", RelatedID: trade.MatchID, CreatedAt: ts}
			
			// 扣减保险基金 (System ID 0)
			sys := e.getAccount(0)
			sys.Balance = sys.Balance.Sub(loss)
			if !sys.Dirty { sys.Dirty = true; e.persistChan <- sys }
		}
	}

	updateBal(trade.MakerID, mPnL, mFee)
	updateBal(trade.TakerID, tPnL, tFee)

	// 平台收入
	totalFee := mFee.Add(tFee)
	if !totalFee.IsZero() {
		sys := e.getAccount(0)
		sys.Balance = sys.Balance.Add(totalFee)
		if !sys.Dirty { sys.Dirty = true; e.persistChan <- sys }
		txKey := fmt.Sprintf("M-%d", trade.MatchID)
		e.persistChan <- &LedgerItem{TxID: txKey, UserID: 0, Currency: "USDT", Amount: totalFee, Type: "REVENUE", RelatedID: trade.MatchID, CreatedAt: ts}
	}
}

// ============================================================================
// 4. 异步持久化器 (The Persister)
// ============================================================================

// Persister 负责将内存变更批量刷入数据库
func (e *Engine) StartPersister(ctx context.Context) {
	ticker := time.NewTicker(PersistInterval)
	defer ticker.Stop()

	var (
		accBatch    []*Account
		posBatch    []*Position
		ledgerBatch []*LedgerItem
	)

	// 内部函数：执行刷盘
	flush := func() {
		if len(accBatch) == 0 && len(posBatch) == 0 && len(ledgerBatch) == 0 {
			return
		}

		batch := &pgx.Batch{}

		// 1. 流水 (Append Only) -> 使用 COPY 协议 (超快)
		// 为了简化代码，这里用 Batch Insert，如果追求极致性能可用 pgx.CopyFrom
		for _, l := range ledgerBatch {
			batch.Queue("INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
				l.TxID, l.UserID, l.Currency, l.Amount, l.Type, l.RelatedID, l.CreatedAt)
		}

		// 2. 账户 (Upsert)
		for _, acc := range accBatch {
			batch.Queue(`
				INSERT INTO accounts (user_id, currency, balance, updated_at) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, currency) DO UPDATE SET balance = $3, updated_at = $4
			`, acc.UserID, acc.Currency, acc.Balance, time.Now().UnixNano())
			
			// 回调清除脏标记 (需要加锁)
			e.mu.Lock()
			acc.Dirty = false
			e.mu.Unlock()
		}

		// 3. 持仓 (Upsert)
		for _, pos := range posBatch {
			batch.Queue(`
				INSERT INTO positions (user_id, symbol, size, entry_value, updated_at) VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = $5
			`, pos.UserID, pos.Symbol, pos.Size, pos.EntryValue, time.Now().UnixNano())
			
			e.mu.Lock()
			pos.Dirty = false
			e.mu.Unlock()
		}

		// 执行 Batch
		br := e.dbPool.SendBatch(ctx, batch)
		if _, err := br.Exec(); err != nil {
			e.logger.Error("Persist Failed", zap.Error(err))
			// 生产环境：必须报警并重试，否则内存和DB不一致
		} else {
			// e.logger.Debug("Persist Success", zap.Int("ledgers", len(ledgerBatch)))
		}
		br.Close()

		// 清空 buffer
		accBatch = accBatch[:0]
		posBatch = posBatch[:0]
		ledgerBatch = ledgerBatch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case item := <-e.persistChan:
			switch v := item.(type) {
			case *Account:
				accBatch = append(accBatch, v)
			case *Position:
				posBatch = append(posBatch, v)
			case *LedgerItem:
				ledgerBatch = append(ledgerBatch, v)
			}
			// 阈值触发
			if len(ledgerBatch) >= PersistBatchSize {
				flush()
			}
		}
	}
}

// ============================================================================
// 5. 程序入口
// ============================================================================

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 1. 数据库连接
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://root:123456@localhost:5432/clearing_db?sslmode=disable"
	}
	config, _ := pgxpool.ParseConfig(dbURL)
	config.MaxConns = 50 // 允许更多并发连接用于读 API
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		logger.Fatal("DB Init Error", zap.Error(err))
	}

	// 2. 初始化内存引擎
	engine := NewEngine(pool, logger)
	
	// 3. 预热：从数据库恢复状态 (Hydration)
	if err := engine.Hydrate(context.Background()); err != nil {
		logger.Fatal("Hydration Failed", zap.Error(err))
	}

	// 4. 启动持久化线程 (Writer)
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.StartPersister(ctx)
	}()

	// 5. 启动 Kafka 消费者 (Reader)
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" { brokers = "localhost:9092" }
	
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           KafkaGroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil { panic(err) }
	c.SubscribeTopics([]string{KafkaTopicTrades}, nil)

	// 6. 消费循环 (主线程)
	logger.Info("Clearing Engine v5.0 Started (In-Memory Mode)")
	
	go func() {
		for {
			ev, err := c.ReadMessage(100 * time.Millisecond)
			if err != nil { continue }

			var trade TradeEvent
			if err := json.Unmarshal(ev.Value, &trade); err != nil {
				continue // 坏消息忽略
			}

			// >>> 核心调用 <<<
			engine.ProcessTrade(trade)
			
			// 异步 Commit (追求吞吐量)
			// 极端情况下如果这里 crash，重启后会重放一部分消息
			// 因为我们有 idempotency_keys 表(虽然本例略去了插入过程)，或者依靠业务逻辑的覆盖写特性
			// 这种"覆盖写"逻辑本身大部分是幂等的 (除了 Balance 累加)
			// 真正的生产环境需要更严谨的 Offset 管理
			// _, _ = c.CommitMessage(ev) 
		}
	}()

	// 7. Read-Only API (直接读内存，极快)
	http.HandleFunc("/balance", func(w http.ResponseWriter, r *http.Request) {
		var uid uint64
		fmt.Sscanf(r.URL.Query().Get("user_id"), "%d", &uid)
		
		engine.mu.RLock()
		defer engine.mu.RUnlock()
		
		if acc, ok := engine.accounts[uid]; ok {
			fmt.Fprintf(w, `{"user_id": %d, "balance": "%s"}`, uid, acc.Balance)
		} else {
			fmt.Fprintf(w, `{"user_id": %d, "balance": "0"}`, uid)
		}
	})
	
	// Metrics
	http.Handle("/metrics", promhttp.Handler())
	
	go http.ListenAndServe(":8080", nil)

	// 8. 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("Shutting down...")
	cancel()  // 停止 Persister
	wg.Wait() // 等待最后一次刷盘完成
	pool.Close()
	logger.Info("Shutdown complete.")
}