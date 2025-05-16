package main

import (
	"etl-pipeline/config"
	kakfa "etl-pipeline/external/kafka"
	"etl-pipeline/internal/app"
	"etl-pipeline/pkg/database"
	"etl-pipeline/pkg/logger"

	"go.uber.org/fx"
)

func main() {
	fx.New(
		config.Module,
		database.Module,
		app.Module,
		logger.Module,
		kakfa.Module,
	).Run()
}
