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
// 1. Configuration & Constants
// ============================================================================

const (
	KafkaTopicTrades    = "engine.trades"
	KafkaTopicTransfers = "chain.transfers" // NEW: Listen for deposits/withdrawals
	KafkaGroupID        = "clearing_service_prod_v2"
	WorkerCount         = 4
)

// ============================================================================
// 2. Metrics (Observability) - NEW
// ============================================================================

var (
	tradesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_trades_processed_total",
		Help: "Total number of trades processed",
	})
	insuranceFundTriggered = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clearing_insurance_fund_triggered_total",
		Help: "Total number of times insurance fund was used (Bankruptcy)",
	})
)

func init() {
	prometheus.MustRegister(tradesProcessed)
	prometheus.MustRegister(insuranceFundTriggered)
}

// ============================================================================
// 3. Data Models
// ============================================================================

type TradeEvent struct {
	MatchID      uint64          `json:"match_id"`
	MakerID      uint64          `json:"maker_id"`
	TakerID      uint64          `json:"taker_id"`
	Symbol       string          `json:"symbol"`
	Price        decimal.Decimal `json:"price"`
	Amount       decimal.Decimal `json:"amount"`
	Timestamp    int64           `json:"timestamp"`
	MakerFeeRate decimal.Decimal `json:"maker_fee_rate"` // NEW: Dynamic Fee
	TakerFeeRate decimal.Decimal `json:"taker_fee_rate"` // NEW: Dynamic Fee
}

// NEW: Transfer Event for Deposits/Withdrawals
type TransferEvent struct {
	TxID     string          `json:"tx_id"`
	UserID   uint64          `json:"user_id"`
	Currency string          `json:"currency"`
	Amount   decimal.Decimal `json:"amount"` // Positive for Deposit, Negative for Withdraw
	Type     string          `json:"type"`   // "DEPOSIT", "WITHDRAWAL"
}

// ============================================================================
// 4. Core Logic
// ============================================================================

type ClearingService struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewClearingService(db *sql.DB, logger *zap.Logger) *ClearingService {
	return &ClearingService{db: db, logger: logger}
}

// HandleTransfer handles deposits and withdrawals (NEW)
func (s *ClearingService) HandleTransfer(ctx context.Context, ev TransferEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Idempotency Check
	_, err = tx.ExecContext(ctx, "INSERT INTO idempotency_keys (key_id) VALUES ($1)", ev.TxID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			s.logger.Warn("Duplicate transfer skipped", zap.String("id", ev.TxID))
			return nil
		}
		return err
	}

	// Update Balance
	_, err = tx.ExecContext(ctx, `
		INSERT INTO accounts (user_id, currency, balance) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, currency) DO UPDATE SET balance = accounts.balance + $3, updated_at = NOW()
	`, ev.UserID, ev.Currency, ev.Amount)
	if err != nil {
		return err
	}

	// Insert Ledger
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id)
		VALUES ($1, $2, $3, $4, $5, 0)
	`, ev.TxID, ev.UserID, ev.Currency, ev.Amount, ev.Type)

	return tx.Commit()
}

// SettlePerpsTrade - Evolved with Flip Logic & Bankruptcy Logic
func (s *ClearingService) SettlePerpsTrade(ctx context.Context, trade TradeEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	txKey := fmt.Sprintf("MATCH-%d", trade.MatchID)
	_, err = tx.ExecContext(ctx, "INSERT INTO idempotency_keys (key_id) VALUES ($1)", txKey)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return nil
		}
		return err
	}

	tradeValue := trade.Price.Mul(trade.Amount) // Always positive here

	// --- Helper: Process Side with FLIP LOGIC ---
	processSide := func(uid uint64, isMaker bool, sideAmount decimal.Decimal) (decimal.Decimal, decimal.Decimal, error) {
		// 1. Fee Calculation
		rate := trade.TakerFeeRate
		if isMaker {
			rate = trade.MakerFeeRate
		}
		fee := tradeValue.Mul(rate)

		// 2. Lock & Query Position
		var currentSize, currentEntryValue decimal.Decimal
		err := tx.QueryRowContext(ctx, `
			SELECT size, entry_value FROM positions 
			WHERE user_id = $1 AND symbol = $2 FOR UPDATE
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

		isSameDirection := currentSize.IsZero() || (currentSize.Sign() == sideAmount.Sign())

		if isSameDirection {
			// Case A: Open / Increase
			entryDelta := trade.Price.Mul(sideAmount.Abs())
			if sideAmount.IsNegative() {
				entryDelta = entryDelta.Neg()
			}
			newEntryValue = currentEntryValue.Add(entryDelta)
		} else {
			// Case B: Reduce or Flip
			// Check Flip: |Transaction| > |CurrentPosition|
			if sideAmount.Abs().GreaterThan(currentSize.Abs()) {
				// >>> FLIP LOGIC <<<

				// 1. Close existing position completely
				// PnL = (Price - AvgEntry) * Size * Direction
				// Simplified: (Price - (EntryVal/Size)) * Size
				// Be careful with signs here.
				avgPrice := currentEntryValue.Div(currentSize).Abs()
				priceDiff := trade.Price.Sub(avgPrice)

				if currentSize.IsPositive() {
					// Long Close: (Price - Entry)
					realizedPnL = priceDiff.Mul(currentSize)
				} else {
					// Short Close: (Entry - Price)
					realizedPnL = priceDiff.Mul(currentSize.Abs()).Neg()
				}

				// 2. Open new position with remainder
				remainder := sideAmount.Add(currentSize) // The flipped amount
				newEntryValue = trade.Price.Mul(remainder.Abs())
				if remainder.IsNegative() {
					newEntryValue = newEntryValue.Neg()
				}
			} else {
				// >>> REDUCE ONLY LOGIC <<<
				ratio := sideAmount.Div(currentSize) // Negative ratio
				releasedCost := currentEntryValue.Mul(ratio)

				// PnL Calculation for partial close
				avgPrice := currentEntryValue.Div(currentSize).Abs()
				if currentSize.IsPositive() {
					// Long Close
					realizedPnL = trade.Price.Sub(avgPrice).Mul(sideAmount.Abs())
				} else {
					// Short Close
					realizedPnL = avgPrice.Sub(trade.Price).Mul(sideAmount.Abs())
				}
				newEntryValue = currentEntryValue.Add(releasedCost)
			}
		}

		// 3. Update Position DB
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

	// Execute Logic
	makerPnL, makerFee, err := processSide(trade.MakerID, true, trade.Amount.Neg())
	if err != nil {
		return err
	}
	takerPnL, takerFee, err := processSide(trade.TakerID, false, trade.Amount)
	if err != nil {
		return err
	}

	// --- Helper: Update Balance with BANKRUPTCY LOGIC ---
	updateBalance := func(uid uint64, pnl, fee decimal.Decimal) error {
		change := pnl.Sub(fee)

		// 1. Update Balance
		var newBalance decimal.Decimal
		err := tx.QueryRowContext(ctx, `
			INSERT INTO accounts (user_id, currency, balance) VALUES ($1, 'USDT', $2)
			ON CONFLICT (user_id, currency) DO UPDATE SET balance = accounts.balance + $2, updated_at = NOW()
			RETURNING balance
		`, uid, change).Scan(&newBalance)
		if err != nil {
			return err
		}

		// 2. Insert Ledger (Detailed)
		if !fee.IsZero() {
			tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, $2, 'USDT', $3, 'FEE', $4)", txKey, uid, fee.Neg(), trade.MatchID)
		}
		if !pnl.IsZero() {
			tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, $2, 'USDT', $3, 'REALIZED_PNL', $4)", txKey, uid, pnl, trade.MatchID)
		}

		// 3. >>> BANKRUPTCY / INSURANCE FUND LOGIC <<<
		// If balance < 0, the user is bankrupt. The system must cover the loss.
		if newBalance.IsNegative() {
			s.logger.Warn("User Bankrupt", zap.Uint64("user", uid), zap.String("bal", newBalance.String()))
			insuranceFundTriggered.Inc()

			loss := newBalance.Abs()
			// A. Reset user to 0
			_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = 0 WHERE user_id = $1 AND currency = 'USDT'", uid)
			if err != nil {
				return err
			}

			// B. Record Insurance Cover for User
			_, err = tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, $2, 'USDT', $3, 'INSURANCE_COVER', $4)", txKey+"-INS", uid, loss, trade.MatchID)
			if err != nil {
				return err
			}

			// C. Deduct from System Insurance Fund (User ID 0)
			_, err = tx.ExecContext(ctx, "UPDATE accounts SET balance = balance - $1 WHERE user_id = 0 AND currency = 'USDT'", loss)
			// Note: In production, if System ID 0 goes negative, the exchange is insolvent.
		}

		return nil
	}

	if err := updateBalance(trade.MakerID, makerPnL, makerFee); err != nil {
		return err
	}
	if err := updateBalance(trade.TakerID, takerPnL, takerFee); err != nil {
		return err
	}

	// Record Platform Fee Income
	totalFee := makerFee.Add(takerFee)
	if !totalFee.IsZero() {
		tx.ExecContext(ctx, "INSERT INTO ledger_entries (tx_id, user_id, currency, amount, type, related_id) VALUES ($1, 0, 'USDT', $2, 'FEE_INCOME', $3)", txKey, totalFee, trade.MatchID)
		// Also actually add to system account 0
		tx.ExecContext(ctx, "INSERT INTO accounts (user_id, currency, balance) VALUES (0, 'USDT', $1) ON CONFLICT (user_id, currency) DO UPDATE SET balance = accounts.balance + $1", totalFee)
	}

	tradesProcessed.Inc()
	return tx.Commit()
}

// ============================================================================
// 5. Consumer Infrastructure
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
	// Subscribe to BOTH Trades and Transfers
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

			// Route based on Topic
			switch *ev.TopicPartition.Topic {
			case KafkaTopicTrades:
				var trade TradeEvent
				if err := json.Unmarshal(ev.Value, &trade); err != nil {
					c.logger.Error("Invalid Trade", zap.ByteString("val", ev.Value))
					c.consumer.CommitMessage(ev)
					continue
				}
				// Simple Retry Loop
				for i := 0; i < 3; i++ {
					if err := c.service.SettlePerpsTrade(ctx, trade); err == nil {
						c.consumer.CommitMessage(ev)
						break
					} else {
						time.Sleep(time.Millisecond * 50)
					}
				}

			case KafkaTopicTransfers:
				var transfer TransferEvent
				if err := json.Unmarshal(ev.Value, &transfer); err != nil {
					c.logger.Error("Invalid Transfer", zap.ByteString("val", ev.Value))
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
// 6. Read API & Main
// ============================================================================

func StartAPIServer(db *sql.DB, logger *zap.Logger) {
	http.Handle("/metrics", promhttp.Handler())

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
	// Initialize System Account (ID 0) for Fees/Insurance
	db.Exec("INSERT INTO accounts (user_id, currency, balance) VALUES (0, 'USDT', 0) ON CONFLICT DO NOTHING")

	svc := NewClearingService(db, logger)
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	// 1. Start Workers
	for i := 0; i < WorkerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := NewConsumer(svc, brokers)
			c.Start(ctx)
		}(i)
	}

	// 2. Start API Server (in background)
	go StartAPIServer(db, logger)

	logger.Info("Service Started", zap.Int("workers", WorkerCount))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("Stopping...")
	cancel()
	wg.Wait()
	db.Close()
	logger.Info("Stopped.")
}
