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
	"go.uber.org/zap"
)

// ConsumerInterface defines the contract for Kafka consumer implementations
type Consumer interface {
	processMessage(ctx context.Context, msg kafka.Message) error
	readMessages(ctx context.Context)
	close() error
	start()
	stop()
}

type consumer struct {
	Reader    *kafka.Reader
	Processor processor.Processor
	Logger    logger.Logger
	Pool      Pool
}

type ConsumerParams struct {
	fx.In
	Config    *config.Config
	Processor processor.Processor
	Logger    logger.Logger
	Pool      Pool
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

func NewKafkaConsumer(p ConsumerParams) Consumer {
	dialer, err := createSecureDialer(p.Config)
	if err != nil {
		p.Logger.Fatal("failed to create secure dialer: " + err.Error())
		return nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     p.Config.Kafka.Brokers,
		GroupID:     p.Config.Kafka.GroupID,
		GroupTopics: p.Config.Kafka.Topics,
		MinBytes:    10e3,
		MaxBytes:    10e6,
		StartOffset: kafka.LastOffset,
		// Commit manually
		CommitInterval:  0,
		Dialer:          dialer,
		ReadLagInterval: -1,
		ReadBackoffMin:  100 * time.Millisecond,
		ReadBackoffMax:  1 * time.Second,
		MaxAttempts:     3,
	})

	return &consumer{
		Reader:    reader,
		Processor: p.Processor,
		Logger:    p.Logger,
		Pool:      p.Pool,
	}
}

func (c *consumer) start() {
	c.Pool.Start()
}

func (c *consumer) stop() {
	c.Pool.Stop()
}

func (c *consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	c.Logger.Info("Processing message from topic=" + msg.Topic)

	err := c.Processor.Process(msg.Value, msg.Topic, msg.Key)
	if err != nil {
		return err
	}

	return nil
}

func (c *consumer) readMessages(ctx context.Context) {
	for {
		readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		msg, err := c.Reader.ReadMessage(readCtx)
		cancel()

		if err != nil {
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "context deadline exceeded") {
				time.Sleep(time.Second)
				continue
			}
			c.Logger.Error("Error reading message",
				zap.Error(err))
			continue
		}

		// Submit task to worker pool
		c.Pool.Submit(func(taskCtx context.Context) {
			defer func() {
				if r := recover(); r != nil {
					c.Logger.Error("panic during processing",
						zap.Any("panic", r))
				}
			}()

			if err := c.processMessage(taskCtx, msg); err != nil {
				c.Logger.Error("Error processing message",
					zap.String("topic", msg.Topic),
					zap.Error(err))
			}

			if err := c.Reader.CommitMessages(taskCtx, msg); err != nil {
				c.Logger.Error("Error committing message",
					zap.String("topic", msg.Topic),
					zap.Error(err))
			}
		})
	}
}

func (c *consumer) close() error {
	return c.Reader.Close()
}

func RunConsumer(lc fx.Lifecycle, c Consumer) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.start()
			go c.readMessages(ctx)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			c.stop()
			return c.close()
		},
	})
}
