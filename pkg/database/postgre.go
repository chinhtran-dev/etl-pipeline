package database

import (
	"context"
	"fmt"

	"etl-pipeline/config"

	loggerCustom "etl-pipeline/pkg/logger"

	"github.com/jackc/pgx/v4/pgxpool"
	"go.uber.org/fx"
)

func NewDatabase(lc fx.Lifecycle, config *config.Config, log *loggerCustom.StandardLogger) (*pgxpool.Pool, error) {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		config.DB.User,
		config.DB.Password,
		config.DB.Host,
		config.DB.Port,
		config.DB.DBName,
		config.DB.SSLMode,
	)

	pool, err := pgxpool.Connect(context.Background(), dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to TimescaleDB: %w", err)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			pool.Close()
			return nil
		},
	})

	return pool, nil
}

var Module = fx.Options(
	fx.Provide(NewDatabase),
)
