package processor

import (
	"context"
	"errors"
	"etl-pipeline/internal/repository"
	"etl-pipeline/pkg/logger"
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	topicPrefix    = "hono.telemetry."
	dbTimeout      = 30 * time.Second
	maxMessageSize = 1 << 20 // 1MB
)

var (
	ErrInvalidTopic    = errors.New("invalid topic format")
	ErrMessageTooLarge = errors.New("message exceeds maximum size")
)

type Processor interface {
	Process(data []byte, topic string, key []byte)
}

type processor struct {
	repo   repository.Repository
	logger *logger.StandardLogger
	ctx    context.Context
	cancel context.CancelFunc
}

type ProcessorParams struct {
	fx.In
	Repo   repository.Repository
	Logger *logger.StandardLogger
}

func NewProcessor(params ProcessorParams) Processor {
	ctx, cancel := context.WithCancel(context.Background())
	return &processor{
		repo:   params.Repo,
		logger: params.Logger,
		ctx:    ctx,
		cancel: cancel,
	}
}

func extractTenantID(topic string) (string, error) {
	if len(topic) <= len(topicPrefix) || topic[:len(topicPrefix)] != topicPrefix {
		return "", ErrInvalidTopic
	}
	return topic[len(topicPrefix):], nil
}

func (p *processor) Process(data []byte, topic string, key []byte) {
	tenantID, err := extractTenantID(topic)
	if err != nil {
		p.logger.Error("Failed to extract")
	}

	deviceID := string(key)

	// Transform the incoming data
	timestamp, transformedData, err := transform(data)
	if err != nil {
		p.logger.Error("Failed to transform data",
			zap.Error(err),
			zap.String("tenantID", tenantID),
			zap.String("deviceID", deviceID))
		return
	}

	p.logger.Info("Processing message",
		zap.Any("transformedData", transformedData),
		zap.String("tenantID", tenantID),
		zap.String("deviceID", deviceID))

	// Create a new context for database operation
	ctx, cancel := context.WithTimeout(p.ctx, 30*time.Second)
	defer cancel()

	err = p.repo.InsertRawDeviceData(ctx, tenantID, deviceID, timestamp, transformedData)
	if err != nil {
		p.logger.Error("Failed to insert message",
			zap.Error(err),
			zap.String("tenantID", tenantID),
			zap.String("deviceID", deviceID))
		return
	}

	p.logger.Info("Message inserted",
		zap.String("tenantID", tenantID),
		zap.String("deviceID", deviceID))
}
