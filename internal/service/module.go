package service

import (
	"etl-pipeline/internal/service/extract"
	"etl-pipeline/internal/service/load"
	"etl-pipeline/internal/service/transform"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(extract.NewExtract),
	fx.Provide(transform.NewTransform),
	fx.Provide(load.NewLoad),
)
