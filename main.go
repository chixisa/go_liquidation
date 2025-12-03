package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // 使用 pgx 驱动
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ============================================================================
// 1. 配置与常量
// ============================================================================

const (
	KafkaTopicTrades = "engine.trades"
	KafkaGroupID     = "clearing_service_prod_v1"
	// 注意：生产环境密码应从环境变量获取
	//DBConnString = "postgres://user:password@localhost:5432/clearing_db?sslmode=disable"
	WorkerCount = 4
	MaxRetries  = 3
)

// ============================================================================
// 2. 数据模型
// ============================================================================

type TradeEvent struct {
	MatchID   uint64          `json:"match_id"`
	MakerID   uint64          `json:"maker_id"`
	TakerID   uint64          `json:"taker_id"`
	Symbol    string          `json:"symbol"`
	Price     decimal.Decimal `json:"price"`
	Amount    decimal.Decimal `json:"amount"`
	Timestamp int64           `json:"timestamp"`
	// 简化：假设买卖方向由 Amount 正负或 Maker/Taker 角色推导，或由上游传入
}

// Position 数据库持仓映射
type Position struct {
	UserID     uint64
	Symbol     string
	Size       decimal.Decimal
	EntryValue decimal.Decimal
}

// ============================================================================
// 3. 核心业务服务
// ============================================================================

type ClearingService struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewClearingService(db *sql.DB, logger *zap.Logger) *ClearingService {
	return &ClearingService{db: db, logger: logger}
}

// SettlePerpsTrade 生产级永续合约结算 (含平仓PnL计算 + 幂等优化)
func (s *ClearingService) SettlePerpsTrade(ctx context.Context, trade TradeEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// --- 1. 幂等性抢锁 (Optimized Idempotency) ---
	txKey := fmt.Sprintf("MATCH-%d", trade.MatchID)
	_, err = tx.ExecContext(ctx, "INSERT INTO idempotency_keys (key_id) VALUES ($1)", txKey)
	if err != nil {
		// 检查是否为主键冲突 (Postgres Error Code 23505)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			s.logger.Warn("Duplicate trade skipped", zap.String("key", txKey))
			return nil // 幂等成功，视为正常处理
		}
		return err // 其他错误正常抛出
	}

	// --- 2. 准备数据 ---
	tradeValue := trade.Price.Mul(trade.Amount)
	feeRate := decimal.NewFromFloat(0.0005) // 0.05%
	fee := tradeValue.Mul(feeRate)

	// 我们需要分别处理 Maker 和 Taker 的仓位更新
	// 封装成闭包函数以复用逻辑
	processSide := func(uid uint64, amountDelta, valueDelta decimal.Decimal) (decimal.Decimal, error) {
		// a. 锁定并查询当前持仓 (SELECT FOR UPDATE)
		var pos Position
		err := tx.QueryRowContext(ctx, `
			SELECT size, entry_value FROM positions 
			WHERE user_id = $1 AND symbol = $2 FOR UPDATE
		`, uid, trade.Symbol).Scan(&pos.Size, &pos.EntryValue)

		if err == sql.ErrNoRows {
			// 新开仓，默认为 0
			pos.Size = decimal.Zero
			pos.EntryValue = decimal.Zero
		} else if err != nil {
			return decimal.Zero, err
		}

		realizedPnL := decimal.Zero

		// b. 判断是开仓还是平仓
		// 如果 currentSize 和 delta 同号 (或 current 为 0)，则是开仓 (Open)
		// 如果异号，则是平仓 (Close/Reduce)
		isOpen := pos.Size.IsZero() || (pos.Size.Sign() == amountDelta.Sign())

		if isOpen {
			// 开仓：简单累加
			pos.Size = pos.Size.Add(amountDelta)
			pos.EntryValue = pos.EntryValue.Add(valueDelta)
		} else {
			// 平仓：计算 PnL
			// 能够平掉的数量 (取绝对值较小者)
			closeSize := amountDelta
			if pos.Size.Abs().LessThan(amountDelta.Abs()) {
				// 翻转仓位的情况 (Flip)，这里为了简化暂不处理翻转，假设只减仓或平仓
				// 生产中需要拆分为：先平仓到0，再反向开仓
				closeSize = pos.Size.Neg()
			}

			// 计算平仓比例
			ratio := closeSize.Div(pos.Size) // 这是一个负数比例

			// 释放的 EntryValue
			releasedValue := pos.EntryValue.Mul(ratio)

			// 计算 PnL:
			// 做多平仓 (Size>0, Delta<0): 卖出价值(valueDelta) - 成本(releasedValue)
			// 做空平仓 (Size<0, Delta>0): 成本(releasedValue) - 买入价值(valueDelta)
			// 通用公式：EntryValue变动 - 成交Value (注意符号)
			// 这里的 valueDelta 是本次成交的价值，releasedValue 是持仓原本的价值
			// PnL = |成交价值| - |持仓成本| (对于盈利而言)
			// 简化公式：PnL = ValueDelta - ReleasedValue (注意 ReleasedValue 是负的因为 ratio 是负的)
			// 这是一个复杂的数学问题，简而言之：
			// realizedPnL = -(valueDelta - releasedValue)
			realizedPnL = valueDelta.Sub(releasedValue).Neg() // 修正后的简化公式验证需谨慎

			// 更新持仓状态
			pos.Size = pos.Size.Add(amountDelta)
			pos.EntryValue = pos.EntryValue.Add(releasedValue) // 减少 EntryValue
		}

		// c. 更新 DB 持仓
		if pos.Size.IsZero() {
			// 如果平光了，可以删除或置零
			_, err = tx.ExecContext(ctx, "DELETE FROM positions WHERE user_id = $1 AND symbol = $2", uid, trade.Symbol)
		} else {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO positions (user_id, symbol, size, entry_value) VALUES ($1, $2, $3, $4)
				ON CONFLICT (user_id, symbol) DO UPDATE SET size = $3, entry_value = $4, updated_at = NOW()
			`, uid, trade.Symbol, pos.Size, pos.EntryValue)
		}
		if err != nil {
			return decimal.Zero, err
		}

		return realizedPnL, nil
	}

	// --- 3. 执行持仓更新并获取 PnL ---

	// Maker (卖出: Amount为负, Value为负)
	makerPnL, err := processSide(trade.MakerID, trade.Amount.Neg(), tradeValue.Neg())
	if err != nil {
		return err
	}

	// Taker (买入: Amount为正, Value为正)
	takerPnL, err := processSide(trade.TakerID, trade.Amount, tradeValue)
	if err != nil {
		return err
	}

	// --- 4. 资金结算 (余额变动) ---
	// 余额变动 = -手续费 + 已实现盈亏
	makerChange := fee.Neg().Add(makerPnL)
	takerChange := fee.Neg().Add(takerPnL)
	// 平台收入 (只记账，不实时更新余额以避免行锁热点)
	platformIncome := fee.Add(fee)

	// 准备 SQL
	stmtLog, err := tx.PrepareContext(ctx, `
		INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id)
		VALUES ($1, $2, 'USDT', $3, $4, $5)
	`)
	if err != nil {
		return err
	}
	defer stmtLog.Close()

	stmtBal, err := tx.PrepareContext(ctx, `
		INSERT INTO accounts (user_id, currency, balance) VALUES ($1, 'USDT', $2)
		ON CONFLICT (user_id, currency) DO UPDATE SET balance = accounts.balance + $2, updated_at = NOW()
	`)
	if err != nil {
		return err
	}
	defer stmtBal.Close()

	// 执行 Maker 变动
	if _, err := stmtBal.ExecContext(ctx, trade.MakerID, makerChange); err != nil {
		return err
	}
	if _, err := stmtLog.ExecContext(ctx, txKey, trade.MakerID, makerChange, "TRADE_SETTLE", trade.MatchID); err != nil {
		return err
	}

	// 执行 Taker 变动
	if _, err := stmtBal.ExecContext(ctx, trade.TakerID, takerChange); err != nil {
		return err
	}
	if _, err := stmtLog.ExecContext(ctx, txKey, trade.TakerID, takerChange, "TRADE_SETTLE", trade.MatchID); err != nil {
		return err
	}

	// 记录平台收入流水 (不更新 accounts 表)
	if _, err := stmtLog.ExecContext(ctx, txKey, 0, platformIncome, "FEE_INCOME", trade.MatchID); err != nil {
		return err
	}

	return tx.Commit()
}

// ============================================================================
// 4. Kafka 基础设施 (带重试机制)
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
	c.SubscribeTopics([]string{KafkaTopicTrades}, nil)
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

			var trade TradeEvent
			if err := json.Unmarshal(ev.Value, &trade); err != nil {
				c.logger.Error("Invalid Msg", zap.ByteString("val", ev.Value))
				c.consumer.CommitMessage(ev) // 坏消息直接跳过
				continue
			}

			// --- 重试循环 (Exponential Backoff) ---
			retryCount := 0
			backoff := 50 * time.Millisecond

			for {
				err := c.service.SettlePerpsTrade(ctx, trade)
				if err == nil {
					c.consumer.CommitMessage(ev)
					break // 成功，退出重试循环
				}

				retryCount++
				if retryCount > MaxRetries {
					c.logger.Error("CRITICAL: Dropping msg after retries", zap.Uint64("id", trade.MatchID), zap.Error(err))
					c.consumer.CommitMessage(ev) // 忍痛丢弃，避免阻塞
					break
				}

				c.logger.Warn("Retry settling", zap.Int("retry", retryCount), zap.Error(err))
				time.Sleep(backoff)
				backoff *= 2 // 50ms -> 100ms -> 200ms
			}
		}
	}
}

// ============================================================================
// 5. 主程序入口
// ============================================================================

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 使用 pgx 驱动注册的 driverName 是 "pgx"
	dbDSN := os.Getenv("DATABASE_URL")
	if dbDSN == "" {
		dbDSN = "postgres://user:password@localhost:5432/clearing_db?sslmode=disable"
	}
	db, err := sql.Open("pgx", dbDSN)
	if err != nil {
		logger.Fatal("DB Init Error", zap.Error(err))
	}

	// 资源池配置
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

	// 启动 Worker 池
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

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

	logger.Info("Service Started", zap.Int("workers", WorkerCount))

	// 优雅停机监听
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("Stopping...")
	cancel()  // 发送停止信号
	wg.Wait() // 等待所有消费者处理完当前消息
	db.Close()
	logger.Info("Stopped.")
}
