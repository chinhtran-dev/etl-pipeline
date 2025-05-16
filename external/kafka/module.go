package kakfa

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewPool),
	fx.Provide(NewKafkaConsumer),

	fx.Invoke(RunConsumer),
)
