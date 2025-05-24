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
	Process(data []byte) error
}

type processor struct {
	Logger    logger.Logger
	Extract   extract.HonoExtractor
	Transform transform.HonoTransformer
	Load      load.Loader
}

type ProcessorParams struct {
	fx.In
	Logger    logger.Logger
	Extract   extract.HonoExtractor
	Transform transform.HonoTransformer
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

func (p *processor) Process(data []byte) error {
	identity, value, timestamp, err := p.Extract.Extracter(data)
	if err != nil {
		p.Logger.Error("Failed to extract", zap.Error(err))
		return err
	}

	transformedData, err := p.Transform.HonoTransform(value)
	if err != nil {
		p.Logger.Error("Failed to transform data",
			zap.Error(err),
			zap.String("tenantID", identity.TenantId),
			zap.String("deviceID", identity.DeviceId))
		return err
	}

	p.Logger.Info("Processing message",
		zap.Any("transformedData", transformedData),
		zap.String("tenantID", identity.TenantId),
		zap.String("deviceID", identity.DeviceId))

	err = p.Load.Load(identity.TenantId, identity.DeviceId, timestamp, transformedData)
	if err != nil {
		p.Logger.Error("Error load data",
			zap.Any("error", err))
		return err
	}

	p.Logger.Info("Message inserted",
		zap.String("tenantID", identity.TenantId),
		zap.String("deviceID", identity.DeviceId))

	return nil
}
