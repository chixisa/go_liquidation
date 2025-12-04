package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// ============================================================================
// 1. 配置与常量
// ============================================================================

const (
	KafkaTopicTrades    = "engine.trades"
	KafkaTopicTransfers = "chain.transfers"
	// 使用新的 GroupID 以便重新消费旧消息进行测试
	KafkaGroupID = "clearing_service_prod_v3_no_lock"
	// Worker数量建议设置为 Kafka Partition 的数量 (例如 4 或 8)
	WorkerCount = 4
)

// ============================================================================
// 2. 监控指标 (Prometheus)
// ============================================================================

var (
	tradesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_trades_processed_total",
		Help: "Total number of trades processed successfully",
	})
	insuranceFundTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_insurance_fund_triggered_total",
		Help: "Total number of bankruptcies handled",
	})
)

func init() {
	prometheus.MustRegister(tradesProcessed)
	prometheus.MustRegister(insuranceFundTriggered)
}

// ============================================================================
// 3. 数据模型
// ============================================================================

type TradeEvent struct {
	MatchID      uint64          `json:"match_id"`
	MakerID      uint64          `json:"maker_id"`
	TakerID      uint64          `json:"taker_id"`
	Symbol       string          `json:"symbol"`
	Price        decimal.Decimal `json:"price"`
	Amount       decimal.Decimal `json:"amount"` // 正数代表Taker买入
	Timestamp    int64           `json:"timestamp"`
	MakerFeeRate decimal.Decimal `json:"maker_fee_rate"`
	TakerFeeRate decimal.Decimal `json:"taker_fee_rate"`
}

type TransferEvent struct {
	TxID     string          `json:"tx_id"`
	UserID   uint64          `json:"user_id"`
	Currency string          `json:"currency"`
	Amount   decimal.Decimal `json:"amount"`
	Type     string          `json:"type"` // DEPOSIT, WITHDRAWAL
}

// ============================================================================
// 4. 核心业务逻辑
// ============================================================================

type ClearingService struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewClearingService(db *sql.DB, logger *zap.Logger) *ClearingService {
	return &ClearingService{db: db, logger: logger}
}

// HandleTransfer 处理充值和提现
func (s *ClearingService) HandleTransfer(ctx context.Context, ev TransferEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 幂等性检查
	_, err = tx.ExecContext(ctx, "INSERT INTO idempotency_keys (key_id) VALUES ($1)", ev.TxID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			s.logger.Warn("Duplicate transfer skipped", zap.String("id", ev.TxID))
			return nil
		}
		return err
	}

	// 更新余额
	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts (user_id, currency, balance) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, currency) DO UPDATE SET balance = accounts.balance + $3, updated_at = NOW()
	`, ev.UserID, ev.Currency, ev.Amount)
	if err != nil {
		return err
	}

	// 写入流水
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id)
		VALUES ($1, $2, $3, $4, $5, 0)
	`, ev.TxID, ev.UserID, ev.Currency, ev.Amount, ev.Type)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// SettlePerpsTrade 核心清算逻辑 (无锁版 + 翻转逻辑 + 破产保护)
func (s *ClearingService) SettlePerpsTrade(ctx context.Context, trade TradeEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. 幂等性检查
	txKey := fmt.Sprintf("MATCH-%d", trade.MatchID)
	_, err = tx.ExecContext(ctx, "INSERT INTO idempotency_keys (key_id) VALUES ($1)", txKey)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil // 重复消息，直接忽略
		}
		return err
	}

	tradeValue := trade.Price.Mul(trade.Amount) // 名义价值

	// ====================================================================
	// 定义单边处理函数 (包含翻转 Flip 逻辑)
	// ====================================================================
	processSide := func(uid uint64, isMaker bool, sideAmount decimal.Decimal) (decimal.Decimal, decimal.Decimal, error) {
		// A. 计算手续费
		rate := trade.TakerFeeRate
		if isMaker {
			rate = trade.MakerFeeRate
		}
		fee := tradeValue.Mul(rate) // 费率基于名义价值

		// B. 查询持仓 (无锁 SELECT)
		// 前提：Kafka Key = UserID 保证了串行性
		var currentSize, currentEntryValue decimal.Decimal
		err := tx.QueryRowContext(ctx, `
			SELECT size, entry_value FROM positions 
			WHERE user_id = $1 AND symbol = $2
		`, uid, trade.Symbol).Scan(&currentSize, &currentEntryValue)

		if err == sql.ErrNoRows {
			currentSize = decimal.Zero
			currentEntryValue = decimal.Zero
		} else if err != nil {
			return decimal.Zero, decimal.Zero, err
		}

		realizedPnL := decimal.Zero
		newSize := currentSize.Add(sideAmount)
		newEntryValue := decimal.Zero

		// 判断方向是否一致 (同向=加仓，反向=减仓/翻转)
		isSameDirection := currentSize.IsZero() || (currentSize.Sign() == sideAmount.Sign())

		if isSameDirection {
			// === 情况1：加仓 (Open/Increase) ===
			// 成本直接累加。注意 EntryValue 符号要和 Size 保持一致
			entryDelta := trade.Price.Mul(sideAmount.Abs())
			if sideAmount.IsNegative() {
				entryDelta = entryDelta.Neg()
			}
			newEntryValue = currentEntryValue.Add(entryDelta)

		} else {
			// === 情况2：减仓 或 翻转 (Reduce or Flip) ===

			// 判断是否翻转：交易量绝对值 > 当前持仓绝对值
			if sideAmount.Abs().GreaterThan(currentSize.Abs()) {
				// >>> 翻转逻辑 (Flip) <<<
				
				// 1. 先把当前的仓位完全平掉，结算 PnL
				// PnL = (ExitPrice - AvgEntryPrice) * Size * Direction
				// 简化公式: PnL = (Price * Size) - EntryValue (注意符号处理)
				
				avgPrice := currentEntryValue.Div(currentSize).Abs()
				priceDiff := trade.Price.Sub(avgPrice)
				
				if currentSize.IsPositive() {
					// 做多平仓: (Price - Entry)
					realizedPnL = priceDiff.Mul(currentSize)
				} else {
					// 做空平仓: (Entry - Price)
					realizedPnL = priceDiff.Mul(currentSize.Abs()).Neg()
				}

				// 2. 用剩余的量反向开仓
				remainder := sideAmount.Add(currentSize) // 翻转后的剩余数量
				newEntryValue = trade.Price.Mul(remainder.Abs())
				if remainder.IsNegative() {
					newEntryValue = newEntryValue.Neg()
				}
				// newSize 已经是 remainder 了

			} else {
				// >>> 仅减仓逻辑 (Reduce Only) <<<
				
				// 计算平仓比例 (Ratio为负数)
				ratio := sideAmount.Div(currentSize)
				releasedCost := currentEntryValue.Mul(ratio)
				
				// 计算 PnL
				avgPrice := currentEntryValue.Div(currentSize).Abs()
				if currentSize.IsPositive() {
					// 做多卖出
					realizedPnL = trade.Price.Sub(avgPrice).Mul(sideAmount.Abs())
				} else {
					// 做空买入
					realizedPnL = avgPrice.Sub(trade.Price).Mul(sideAmount.Abs())
				}
				
				newEntryValue = currentEntryValue.Add(releasedCost)
			}
		}

		// C. 更新持仓数据库
		if newSize.IsZero() {
			_, err = tx.ExecContext(ctx, "DELETE FROM positions WHERE user_id = $1 AND symbol = $2", uid, trade.Symbol)
		} else {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO positions (user_id, symbol, size, entry_value) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = NOW()
			`, uid, trade.Symbol, newSize, newEntryValue)
		}
		if err != nil {
			return decimal.Zero, decimal.Zero, err
		}

		return realizedPnL, fee, nil
	}

	// ====================================================================
	// 执行双方处理
	// ====================================================================

	// Maker (卖方逻辑，Amount 取反)
	makerPnL, makerFee, err := processSide(trade.MakerID, true, trade.Amount.Neg())
	if err != nil { return err }

	// Taker (买方逻辑，Amount 为正)
	takerPnL, takerFee, err := processSide(trade.TakerID, false, trade.Amount)
	if err != nil { return err }

	// ====================================================================
	// 资金结算 (含破产保护)
	// ====================================================================
	updateBalance := func(uid uint64, pnl, fee decimal.Decimal) error {
		change := pnl.Sub(fee)

		// 1. 更新余额并获取最新值
		var newBalance decimal.Decimal
		err := tx.QueryRowContext(ctx, `
			INSERT INTO accounts (user_id, currency, balance) VALUES ($1, 'USDT', $2)
			ON CONFLICT (user_id, currency) DO UPDATE SET balance = accounts.balance + $2, updated_at = NOW()
			RETURNING balance
		`, uid, change).Scan(&newBalance)
		if err != nil {
			return err
		}

		// 2. 记账 (Ledger)
		if !fee.IsZero() {
			tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, $2, 'USDT', $3, 'FEE', $4)", txKey, uid, fee.Neg(), trade.MatchID)
		}
		if !pnl.IsZero() {
			tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, $2, 'USDT', $3, 'REALIZED_PNL', $4)", txKey, uid, pnl, trade.MatchID)
		}

		// 3. >>> 破产保护逻辑 (Bankruptcy) <<<
		if newBalance.IsNegative() {
			s.logger.Warn("User Bankrupt detected", zap.Uint64("user", uid), zap.String("balance", newBalance.String()))
			insuranceFundTriggered.Inc() // 监控计数 +1

			loss := newBalance.Abs() // 亏空的金额

			// A. 将用户余额置为 0
			_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = 0 WHERE user_id = $1 AND currency = 'USDT'", uid)
			if err != nil { return err }

			// B. 记录保险基金赔付流水
			_, err = tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, $2, 'USDT', $3, 'INSURANCE_COVER', $4)", txKey+"-INS", uid, loss, trade.MatchID)
			if err != nil { return err }

			// C. 从系统账户 (ID 0) 扣除这笔钱
			_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1 WHERE user_id = 0 AND currency = 'USDT'", loss)
			if err != nil { return err }
		}

		return nil
	}

	if err := updateBalance(trade.MakerID, makerPnL, makerFee); err != nil { return err }
	if err := updateBalance(trade.TakerID, takerPnL, takerFee); err != nil { return err }

	// 记录平台手续费收入 (转入 ID 0)
	totalFee := makerFee.Add(takerFee)
	if !totalFee.IsZero() {
		tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, 0, 'USDT', $2, 'FEE_INCOME', $3)", txKey, totalFee, trade.MatchID)
		tx.ExecContext(ctx, "UPDATE accounts SET balance = balance + $1 WHERE user_id = 0 AND currency = 'USDT'", totalFee)
	}

	tradesProcessed.Inc() // 监控计数
	return tx.Commit()
}

// ============================================================================
// 5. 消费者架构
// ============================================================================

type Consumer struct {
	service  *ClearingService
	consumer *kafka.Consumer
	logger   *zap.Logger
}

func NewConsumer(svc *ClearingService, brokers string) *Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  brokers,
		"group.id":           KafkaGroupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		panic(err)
	}
	c.SubscribeTopics([]string{KafkaTopicTrades, KafkaTopicTransfers}, nil)
	return &Consumer{service: svc, consumer: c, logger: svc.logger}
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.consumer.Close()
			return
		default:
			ev, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				continue
			}

			switch *ev.TopicPartition.Topic {
			case KafkaTopicTrades:
				var trade TradeEvent
				if err := json.Unmarshal(ev.Value, &trade); err != nil {
					c.logger.Error("Invalid Trade Msg", zap.ByteString("val", ev.Value))
					c.consumer.CommitMessage(ev)
					continue
				}
				// 重试逻辑
				for i := 0; i < 3; i++ {
					if err := c.service.SettlePerpsTrade(ctx, trade); err == nil {
						c.consumer.CommitMessage(ev)
						break
					} else {
						c.logger.Warn("Retry settling", zap.Error(err))
						time.Sleep(time.Millisecond * 50)
					}
				}

			case KafkaTopicTransfers:
				var transfer TransferEvent
				if err := json.Unmarshal(ev.Value, &transfer); err != nil {
					c.logger.Error("Invalid Transfer Msg", zap.ByteString("val", ev.Value))
					c.consumer.CommitMessage(ev)
					continue
				}
				if err := c.service.HandleTransfer(ctx, transfer); err == nil {
					c.consumer.CommitMessage(ev)
				}
			}
		}
	}
}

// ============================================================================
// 6. 入口与 API
// ============================================================================

func StartAPIServer(db *sql.DB, logger *zap.Logger) {
	// Prometheus Metrics
	http.Handle("/metrics", promhttp.Handler())

	// 简单的查余额 API (方便调试)
	http.HandleFunc("/balance", func(w http.ResponseWriter, r *http.Request) {
		uid := r.URL.Query().Get("user_id")
		var bal decimal.Decimal
		err := db.QueryRow("SELECT balance FROM accounts WHERE user_id = $1 AND currency = 'USDT'", uid).Scan(&bal)
		if err != nil {
			bal = decimal.Zero
		}
		fmt.Fprintf(w, `{"user_id": %s, "balance": "%s"}`, uid, bal.String())
	})

	logger.Info("API Server listening on :8080")
	http.ListenAndServe(":8080", nil)
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	dbDSN := os.Getenv("DATABASE_URL")
	if dbDSN == "" {
		dbDSN = "postgres://root:123456@localhost:5432/clearing_db?sslmode=disable"
	}
	db, err := sql.Open("pgx", dbDSN)
	if err != nil {
		logger.Fatal("DB Init Error", zap.Error(err))
	}
	
	// 初始化系统账户 (ID 0) 用于收手续费和赔付保险金
	db.Exec("INSERT INTO accounts (user_id, currency, balance) VALUES (0, 'USDT', 0) ON CONFLICT DO NOTHING")

	// 连接池调优
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.Ping(); err != nil {
		logger.Fatal("DB Ping Error", zap.Error(err))
	}

	svc := NewClearingService(db, logger)
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// 启动 Worker Pool
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			l := logger.With(zap.Int("worker", id))
			c := NewConsumer(svc, brokers)
			c.logger = l
			c.Start(ctx)
		}(i)
	}

	// 启动 API 服务
	go StartAPIServer(db, logger)

	logger.Info("Service Started", zap.Int("workers", WorkerCount))

	// 优雅退出
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("Stopping...")
	cancel()
	wg.Wait()
	db.Close()
	logger.Info("Stopped.")
}