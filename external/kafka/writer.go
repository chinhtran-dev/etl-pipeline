package kafka

import (
	"context"
	"encoding/json"
	"etl-pipeline/config"
	"etl-pipeline/pkg/logger"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Writer interface {
	WriteMessages(ctx context.Context, topic string, messages ...kafka.Message) error
	WriteToDLQ(ctx context.Context, msg kafka.Message, err error) error
	Close() error
}

type writer struct {
	writer *kafka.Writer
	dlq    *kafka.Writer
	logger logger.Logger
}

type WriterParams struct {
	fx.In
	Config *config.Config
	Logger logger.Logger
}

func NewKafkaWriter(p WriterParams) Writer {
	dialer, err := createSecureDialer(p.Config)
	if err != nil {
		p.Logger.Fatal("failed to create secure dialer", zap.Error(err))
	}

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      p.Config.Kafka.Brokers,
		Topic:        p.Config.Kafka.Topics[0],
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
		Dialer:       dialer,
		Async:        true,
	})

	dlq := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      p.Config.Kafka.DLQBrokers,
		Topic:        p.Config.Kafka.DLQTopic,
		BatchSize:    100,
		BatchTimeout: 100 * time.Millisecond,
		Dialer:       dialer,
		Async:        true,
	})

	return &writer{
		writer: w,
		dlq:    dlq,
		logger: p.Logger,
	}
}

// WriteMessages writes messages to the Kafka writer
func (w *writer) WriteMessages(ctx context.Context, topic string, messages ...kafka.Message) error {
	for i := range messages {
		messages[i].Topic = topic
	}
	return w.writer.WriteMessages(ctx, messages...)
}

// WriteToDLQ writes a message to the DLQ
func (w *writer) WriteToDLQ(ctx context.Context, msg kafka.Message, err error) error {
	// Create error details
	errorDetails := map[string]interface{}{
		"error":     err.Error(),
		"timestamp": time.Now().UTC(),
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
	}

	// Convert error details to JSON
	errorJSON, err := json.Marshal(errorDetails)
	if err != nil {
		w.logger.Error("Failed to marshal error details", zap.Error(err))
		errorJSON = []byte(err.Error())
	}

	// Add error information to message headers
	headers := append(msg.Headers, kafka.Header{
		Key:   "error_details",
		Value: errorJSON,
	})

	dlqMsg := kafka.Message{
		Topic:   w.dlq.Topic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
		Time:    time.Now(),
	}

	w.logger.Info("Writing message to DLQ",
		zap.String("topic", msg.Topic),
		zap.String("dlq_topic", w.dlq.Topic),
		zap.Error(err))

	return w.dlq.WriteMessages(ctx, dlqMsg)
}

// Close closes the Kafka writer
func (w *writer) Close() error {
	if err := w.writer.Close(); err != nil {
		return err
	}
	return w.dlq.Close()
}

// RunWriter runs the Kafka writer
func RunWriter(lc fx.Lifecycle, w Writer) {
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			return w.Close()
		},
	})
}
