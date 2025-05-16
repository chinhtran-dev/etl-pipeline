package extract

import (
	"errors"
	"etl-pipeline/config"
)

var (
	ErrInvalidTopic = errors.New("invalid topic format")
)

type extract struct {
	config *config.Config
}

func (e *extract) ExtractDeviceId(key []byte) string {
	deviceId := string(key)
	return deviceId
}

// ExtractTenantId implements Extract.
func (e *extract) ExtractTenantId(topic string) (string, error) {
	if len(topic) <= len(e.config.Environment.TopicPrefix) || topic[:len(e.config.Environment.TopicPrefix)] != e.config.Environment.TopicPrefix {
		return "", ErrInvalidTopic
	}
	return topic[len(e.config.Environment.TopicPrefix):], nil
}

type Extract interface {
	ExtractTenantId(topic string) (string, error)
	ExtractDeviceId(key []byte) string
}

func NewExtract(config *config.Config) Extract {
	return &extract{config: config}
}
