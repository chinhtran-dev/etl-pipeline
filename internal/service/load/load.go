package load

import (
	"context"
	"etl-pipeline/internal/repository"
	"etl-pipeline/pkg/logger"
	"time"

	"go.uber.org/fx"
)

type load struct {
	repo   repository.Repository
	logger logger.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

type LoadParams struct {
	fx.In
	Repo   repository.Repository
	Logger logger.Logger
}

// Load implements Loader.
func (l *load) Load(tenantID string, deviceID string, timestamp time.Time, data map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(l.ctx, 30*time.Second)
	defer cancel()

	err := l.repo.InsertRawDeviceData(ctx, tenantID, deviceID, timestamp, data)
	if err != nil {
		return err
	}

	return nil
}

type Loader interface {
	Load(tenantID string, deviceID string, timestamp time.Time, data map[string]interface{}) error
}

func NewLoad(params LoadParams) Loader {
	ctx, cancel := context.WithCancel(context.Background())
	return &load{
		repo:   params.Repo,
		logger: params.Logger,
		ctx:    ctx,
		cancel: cancel,
	}
}
