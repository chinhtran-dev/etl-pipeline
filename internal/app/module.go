package app

import (
	"etl-pipeline/internal/processor"
	"etl-pipeline/internal/repository"
	"etl-pipeline/internal/service"

	"go.uber.org/fx"
)

var Module = fx.Options(
	repository.Module,
	service.Module,
	processor.Module,
)
