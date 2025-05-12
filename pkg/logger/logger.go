package logger

import (
	"context"
	"errors"
	"math"
	"os"

	"etl-pipeline/config"
	"etl-pipeline/pkg/constant"

	"github.com/sirupsen/logrus"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewLogger),
)

type StandardLogger struct {
	*logrus.Logger
}

type Entry struct {
	*logrus.Entry
}

func NewLogger(config *config.Config) *StandardLogger {
	var prettyLog bool

	if config.Environment.Env == constant.DevelopmentEnv {
		prettyLog = true
	}
	logger := logrus.New()

	logger.SetFormatter(&logrus.JSONFormatter{
		PrettyPrint: prettyLog,
	})

	logger.SetOutput(os.Stdout)

	if config.Environment.Env == constant.ProductionEnv {
		logger.SetLevel(logrus.InfoLevel)
	}
	logger.SetReportCaller(true)

	return &StandardLogger{
		Logger: logger,
	}
}

func NewEntry(entry *logrus.Entry) *Entry {
	return &Entry{Entry: entry}
}

func (s *Entry) WithField(key string, value interface{}) *Entry {
	entry := s.Entry.WithField(key, value)
	return NewEntry(entry)
}

func (s *Entry) WithFields(fields logrus.Fields) *Entry {
	entry := s.Entry.WithFields(fields)
	return NewEntry(entry)
}

func (s *Entry) WithError(err error) *Entry {
	entry := s.Entry.WithError(err)
	return NewEntry(entry)
}

func (s *Entry) WithErrorStr(errStr string) *Entry {
	entry := s.Entry.WithError(errors.New(errStr))
	return NewEntry(entry)
}

func (s *Entry) WithContext(ctx context.Context) *Entry {
	entry := s.Entry.WithContext(ctx)
	return NewEntry(entry)
}

func (s *Entry) WithInput(input interface{}) *Entry {
	entry := s.Entry.WithField("input", input)
	return NewEntry(entry)
}

func (s *Entry) WithOutput(output interface{}) *Entry {
	entry := s.Entry.WithField("output", output)
	return NewEntry(entry)
}

func (s *Entry) WithResponseTime(responsetime float64) *Entry {
	resTime := math.Round(responsetime)
	fieldTime := "response_time (ms):"
	entry := s.Entry.WithField(fieldTime, resTime)
	return NewEntry(entry)
}

func (s *Entry) Withkeyword(keyword string) *Entry {
	entry := s.Entry.WithField("keyword", keyword)
	return NewEntry(entry)
}

func (s *Entry) WithURL(url string) *Entry {
	entry := s.Entry.WithField("url", url)
	return NewEntry(entry)
}

func (s *Entry) WithStatusCode(code int) *Entry {
	entry := s.Entry.WithField("status_code", code)
	return NewEntry(entry)
}

func (s *StandardLogger) WithFields(fields logrus.Fields) *Entry {
	entry := s.Logger.WithFields(fields)
	return NewEntry(entry)
}

func (s *StandardLogger) WithError(err error) *Entry {
	entry := s.Logger.WithError(err)
	return NewEntry(entry)
}

func (s *StandardLogger) WithErrorStr(errStr string) *Entry {
	entry := s.Logger.WithError(errors.New(errStr))
	return NewEntry(entry)
}

func (s *StandardLogger) WithField(key string, value interface{}) *Entry {
	entry := s.Logger.WithField(key, value)
	return NewEntry(entry)
}

func (s *StandardLogger) WithInput(input interface{}) *Entry {
	entry := s.Logger.WithField("input", input)
	return NewEntry(entry)
}

func (s *StandardLogger) WithResponseTime(responseTime float64) *Entry {
	fieldTime := "ResponseTime"
	entry := s.Logger.WithField(fieldTime, math.Round(responseTime))
	return NewEntry(entry)
}

func (s *StandardLogger) WithOutput(output interface{}) *Entry {
	entry := s.Logger.WithField("output", output)
	return NewEntry(entry)
}
