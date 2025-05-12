package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

type Repository interface {
	InsertRawDeviceData(ctx context.Context, tenantID, deviceID string, timestamp time.Time, data map[string]interface{}) error
}

type repository struct {
	db *pgxpool.Pool
}

func (r *repository) InsertRawDeviceData(ctx context.Context, tenantID string, deviceID string, timestamp time.Time, data map[string]interface{}) error {
	_, err := r.db.Exec(ctx, InsertRawDeviceData, tenantID, deviceID, timestamp, data)
	return err
}

func NewRepository(db *pgxpool.Pool) Repository {
	return &repository{db: db}
}
