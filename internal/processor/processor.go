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
	Process(data []byte, topic string, key []byte)
}

type processor struct {
	logger    logger.Logger
	extract   extract.Extract
	transform transform.Transform
	load      load.Loader
}

type ProcessorParams struct {
	fx.In
	Logger    logger.Logger
	extract   extract.Extract
	transform transform.Transform
	load      load.Loader
}

func NewProcessor(params ProcessorParams) Processor {
	return &processor{
		logger:    params.Logger,
		extract:   params.extract,
		transform: params.transform,
		load:      params.load,
	}
}

func (p *processor) Process(data []byte, topic string, key []byte) {
	tenantID, err := p.extract.ExtractTenantId(topic)
	if err != nil {
		p.logger.Error("Failed to extract")
	}

	deviceID := p.extract.ExtractDeviceId(key)

	timestamp, transformedData, err := p.transform.Transform(data)
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

	err = p.load.Load(tenantID, deviceID, timestamp, transformedData)
	if err != nil {
		p.logger.Error("Error load data",
			zap.Any("error", err))
		return
	}

	p.logger.Info("Message inserted",
		zap.String("tenantID", tenantID),
		zap.String("deviceID", deviceID))
}
