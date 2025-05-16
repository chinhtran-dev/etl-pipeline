package kakfa

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewKafkaConsumer),
	fx.Provide(NewPool),

	fx.Invoke(RunConsumer),
)
