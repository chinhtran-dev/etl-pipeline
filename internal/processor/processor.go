package processor

import (
	"etl-pipeline/internal/service/extract"
	"etl-pipeline/internal/service/load"
	"etl-pipeline/internal/service/transform"
	"etl-pipeline/pkg/logger"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Processor interface {
	Process(data []byte, topic string, key []byte) error
}

type processor struct {
	Logger    logger.Logger
	Extract   extract.Extract
	Transform transform.Transform
	Load      load.Loader
}

type ProcessorParams struct {
	fx.In
	Logger    logger.Logger
	Extract   extract.Extract
	Transform transform.Transform
	Load      load.Loader
}

func NewProcessor(params ProcessorParams) Processor {
	return &processor{
		Logger:    params.Logger,
		Extract:   params.Extract,
		Transform: params.Transform,
		Load:      params.Load,
	}
}

func (p *processor) Process(data []byte, topic string, key []byte) error {
	tenantID, err := p.Extract.ExtractTenantId(topic)
	if err != nil {
		p.Logger.Error("Failed to extract")
	}

	deviceID := p.Extract.ExtractDeviceId(key)

	timestamp, transformedData, err := p.Transform.Transform(data)
	if err != nil {
		p.Logger.Error("Failed to transform data",
			zap.Error(err),
			zap.String("tenantID", tenantID),
			zap.String("deviceID", deviceID))
		return err
	}

	p.Logger.Info("Processing message",
		zap.Any("transformedData", transformedData),
		zap.String("tenantID", tenantID),
		zap.String("deviceID", deviceID))

	err = p.Load.Load(tenantID, deviceID, timestamp, transformedData)
	if err != nil {
		p.Logger.Error("Error load data",
			zap.Any("error", err))
		return err
	}

	p.Logger.Info("Message inserted",
		zap.String("tenantID", tenantID),
		zap.String("deviceID", deviceID))

	return nil
}
