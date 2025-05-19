package kafka

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewPool),
	fx.Provide(NewKafkaReader),
	fx.Provide(NewKafkaWriter),
	fx.Invoke(RunReader),
	fx.Invoke(RunWriter),
)
