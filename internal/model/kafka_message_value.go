package model

import "time"

type KafkaMessageValue struct {
	Topic     string                 `json:"topic"`
	Headers   map[string]interface{} `json:"headers"`
	Path      string                 `json:"path"`
	Value     interface{}            `json:"value"`
	Timestamp time.Time              `json:"timestamp"`
}
