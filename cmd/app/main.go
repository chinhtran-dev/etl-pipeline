package main

import (
	"etl-pipeline/config"
	"etl-pipeline/external"
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
		external.Module,

		fx.Invoke(external.RunConsumer),
	).Run()
}
