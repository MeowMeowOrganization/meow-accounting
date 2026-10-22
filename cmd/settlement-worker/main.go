package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/MeowMeowOrganization/meow-accounting/internal/kafkaadapter"
	"github.com/MeowMeowOrganization/meow-accounting/internal/settlement"
	"github.com/MeowMeowOrganization/meow-accounting/internal/settlementworker"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if err := run(ctx, logger); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("settlement worker stopped (offset not advanced on failure)", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	dsn, topic, group := os.Getenv("DATABASE_URL"), os.Getenv("KAFKA_SETTLEMENT_TOPIC"), os.Getenv("KAFKA_GROUP_ID")
	brokersCSV := os.Getenv("KAFKA_BROKERS")
	if dsn == "" || topic == "" || group == "" || brokersCSV == "" {
		return fmt.Errorf("required: DATABASE_URL, KAFKA_BROKERS, KAFKA_SETTLEMENT_TOPIC, KAFKA_GROUP_ID")
	}
	brokers := strings.Split(brokersCSV, ",")
	for j := range brokers {
		brokers[j] = strings.TrimSpace(brokers[j])
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open accounting DB: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	startupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(startupCtx); err != nil {
		return fmt.Errorf("connect to accounting DB: %w", err)
	}
	var ready bool
	if err := db.QueryRowContext(startupCtx,
		`SELECT to_regclass('accounting.trade_executions') IS NOT NULL`).Scan(&ready); err != nil || !ready {
		return fmt.Errorf("required accounting migration 000005 not applied: %v", err)
	}
	repo, err := settlement.NewRepository(db)
	if err != nil {
		return err
	}
	decoder, err := settlementworker.NewProtoDecoder()
	if err != nil {
		return err
	}
	reader, err := kafkaadapter.NewReader(brokers, topic, group)
	if err != nil {
		return err
	}
	defer reader.Close()
	worker, err := settlementworker.New(reader, decoder, repo, logger, settlementworker.Config{})
	if err != nil {
		return err
	}
	logger.Info("settlement worker started", "topic", topic, "group", group)
	return worker.Run(ctx)
}
