package logger

import (
	"context"
	"errors"
	"math"

	"etl-pipeline/config"
	"etl-pipeline/pkg/constant"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Module = fx.Options(
	fx.Provide(NewLogger),
)

type Logger interface {
	WithField(key string, value interface{}) Logger
	WithFields(fields map[string]interface{}) Logger
	WithError(err error) Logger
	WithErrorStr(errStr string) Logger
	WithContext(ctx context.Context) Logger
	WithInput(input interface{}) Logger
	WithOutput(output interface{}) Logger
	WithResponseTime(responseTime float64) Logger
	WithKeyword(keyword string) Logger
	WithURL(url string) Logger
	WithStatusCode(code int) Logger

	Debug(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Fatal(msg string, fields ...zap.Field)
}

type standardLogger struct {
	zapLogger *zap.Logger
}

func NewLogger(cfg *config.Config) Logger {
	var zapCfg zap.Config

	if cfg.Environment.Env == constant.DevelopmentEnv {
		zapCfg = zap.NewDevelopmentConfig()
		zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		zapCfg = zap.NewProductionConfig()
		zapCfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	zapCfg.OutputPaths = []string{"stdout"}
	logger, _ := zapCfg.Build(zap.AddCallerSkip(1))

	return &standardLogger{zapLogger: logger}
}

func (l *standardLogger) WithField(key string, value interface{}) Logger {
	return &standardLogger{zapLogger: l.zapLogger.With(zap.Any(key, value))}
}

func (l *standardLogger) WithFields(fields map[string]interface{}) Logger {
	zapFields := make([]zap.Field, 0, len(fields))
	for k, v := range fields {
		zapFields = append(zapFields, zap.Any(k, v))
	}
	return &standardLogger{zapLogger: l.zapLogger.With(zapFields...)}
}

func (l *standardLogger) WithError(err error) Logger {
	return &standardLogger{zapLogger: l.zapLogger.With(zap.Error(err))}
}

func (l *standardLogger) WithErrorStr(errStr string) Logger {
	return &standardLogger{zapLogger: l.zapLogger.With(zap.Error(errors.New(errStr)))}
}

func (l *standardLogger) WithContext(ctx context.Context) Logger {
	return l.WithField("context", ctx)
}

func (l *standardLogger) WithInput(input interface{}) Logger {
	return l.WithField("input", input)
}

func (l *standardLogger) WithOutput(output interface{}) Logger {
	return l.WithField("output", output)
}

func (l *standardLogger) WithResponseTime(responseTime float64) Logger {
	return &standardLogger{zapLogger: l.zapLogger.With(zap.Int("response_time_ms", int(math.Round(responseTime))))}
}

func (l *standardLogger) WithKeyword(keyword string) Logger {
	return l.WithField("keyword", keyword)
}

func (l *standardLogger) WithURL(url string) Logger {
	return l.WithField("url", url)
}

func (l *standardLogger) WithStatusCode(code int) Logger {
	return &standardLogger{zapLogger: l.zapLogger.With(zap.Int("status_code", code))}
}

func (l *standardLogger) Debug(msg string, fields ...zap.Field) {
	l.zapLogger.Debug(msg, fields...)
}

func (l *standardLogger) Info(msg string, fields ...zap.Field) {
	l.zapLogger.Info(msg, fields...)
}

func (l *standardLogger) Warn(msg string, fields ...zap.Field) {
	l.zapLogger.Warn(msg, fields...)
}

func (l *standardLogger) Error(msg string, fields ...zap.Field) {
	l.zapLogger.Error(msg, fields...)
}

func (l *standardLogger) Fatal(msg string, fields ...zap.Field) {
	l.zapLogger.Fatal(msg, fields...)
}
