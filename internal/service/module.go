package service

import (
	"etl-pipeline/internal/service/extract"
	"etl-pipeline/internal/service/load"
	"etl-pipeline/internal/service/transform"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(extract.NewHonoExtractor),
	fx.Provide(transform.NewHonoTransformer),
	fx.Provide(load.NewLoad),
)
