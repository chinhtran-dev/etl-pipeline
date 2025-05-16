package kakfa

import (
	"context"
	"crypto/tls"
	"etl-pipeline/config"
	"etl-pipeline/internal/processor"
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
	logger    logger.Logger
	pool      Pool
}

type ConsumerParams struct {
	fx.In
	config    *config.Config
	processor processor.Processor
	logger    logger.Logger
	pool      Pool
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

func NewKafkaConsumer(p *ConsumerParams) (*Consumer, error) {
	dialer, err := createSecureDialer(p.config)
	if err != nil {
		return nil, err
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", p.config.Kafka.Brokers[0])
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         p.config.Kafka.Brokers,
		GroupID:         p.config.Kafka.GroupID,
		GroupTopics:     p.config.Kafka.Topics,
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

	return &Consumer{
		reader:    reader,
		processor: p.processor,
		logger:    p.logger,
		pool:      p.pool,
	}, nil
}

func (c *Consumer) processMessage(msg kafka.Message) {
	c.logger.Info("Processing message from topic=" + msg.Topic)
	c.processor.Process(msg.Value, msg.Topic, msg.Key)
}

func (c *Consumer) readMessages(ctx context.Context) {
	for {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := c.reader.ReadMessage(readCtx)
		cancel()

		if err != nil {
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "context deadline exceeded") {
				time.Sleep(time.Second)
				continue
			}
			c.logger.Error("Error reading message: " + err.Error())
			continue
		}

		// Sử dụng pool để xử lý message
		c.pool.Submit(func(ctx context.Context) {
			c.processMessage(msg)
		})
	}
}

func RunConsumer(lc fx.Lifecycle, c *Consumer) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.pool.Start()
			go c.readMessages(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.pool.Stop()
			return c.reader.Close()
		},
	})
}
