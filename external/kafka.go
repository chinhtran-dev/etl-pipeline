package external

import (
	"context"
	"crypto/tls"
	"etl-pipeline/config"
	"etl-pipeline/internal/processor"
	"etl-pipeline/internal/service"
	"etl-pipeline/pkg/logger"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/fx"
)

type Consumer struct {
	reader    *kafka.Reader
	processor processor.Processor
	logger    *logger.StandardLogger
	pool      service.Pool
}

func createSecureDialer(config *config.Config) (*kafka.Dialer, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, config.Kafka.User, config.Kafka.Password)
	if err != nil {
		return nil, err
	}
	return &kafka.Dialer{
		Timeout:       30 * time.Second,
		DualStack:     true,
		TLS:           &tls.Config{InsecureSkipVerify: true},
		SASLMechanism: mechanism,
	}, nil
}

func NewKafkaReader(config *config.Config, processor processor.Processor, logger *logger.StandardLogger, pool service.Pool) (*Consumer, error) {
	dialer, err := createSecureDialer(config)
	if err != nil {
		return nil, err
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", config.Kafka.Brokers[0])
	if err != nil {
		return nil, err
	}

	_ = conn.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         config.Kafka.Brokers,
		GroupID:         config.Kafka.GroupID,
		GroupTopics:     config.Kafka.Topics,
		MinBytes:        10e3, // 10KB
		MaxBytes:        10e6,
		StartOffset:     kafka.LastOffset,
		CommitInterval:  time.Second,
		ReadLagInterval: -1,
		Dialer:          dialer,
		ReadBackoffMin:  100 * time.Millisecond,
		ReadBackoffMax:  1 * time.Second,
		MaxAttempts:     3,
	})

	logger.Info()
	return &Consumer{
		reader:    reader,
		processor: processor,
		logger:    logger,
		pool:      pool,
	}, nil
}

func RunConsumer(lc fx.Lifecycle, c *Consumer) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.pool.Start()
			go func() {
				for {
					readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
					msg, err := c.reader.ReadMessage(readCtx)
					cancel()

					if err != nil {
						if strings.Contains(err.Error(), "EOF") ||
							strings.Contains(err.Error(), "context deadline exceeded") {
							time.Sleep(time.Second)
							continue
						}
						c.logger.Printf("Error reading message: %v", err)
						continue
					}

					c.pool.Submit(func(ctx context.Context) {
						c.logger.Printf("Processing message from topic=%s partition=%d offset=%d key=%s",
							msg.Topic, msg.Partition, msg.Offset, string(msg.Key))
						c.processor.Process(msg.Value, msg.Topic, msg.Key)
					})
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.pool.Stop()
			return c.reader.Close()
		},
	})
}
