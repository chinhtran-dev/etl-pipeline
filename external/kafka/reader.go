package kafka

import (
	"context"
	"etl-pipeline/config"
	"etl-pipeline/internal/processor"
	"etl-pipeline/pkg/logger"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Reader interface {
	Start(ctx context.Context)
	Stop()
}

type kafkaReader struct {
	reader          *kafka.Reader
	processor       processor.Processor
	logger          logger.Logger
	pool            Pool
	writer          Writer
	commitBatchSize int
	commitInterval  time.Duration
	commitMutex     sync.Mutex
	pendingMessages []kafka.Message
	commitTicker    *time.Ticker
}

type ReaderParams struct {
	fx.In
	Config    *config.Config
	Processor processor.Processor
	Logger    logger.Logger
	Pool      Pool
	Writer    Writer
}

// NewKafkaReader creates a new Kafka reader
func NewKafkaReader(p ReaderParams) Reader {
	dialer, err := createSecureDialer(p.Config)
	if err != nil {
		p.Logger.Fatal("failed to create secure dialer", zap.Error(err))
	}

	conn, err := dialer.DialLeader(context.Background(), "tcp", p.Config.Kafka.Brokers[0], p.Config.Kafka.Topics[0], 0)
	if err != nil {
		p.Logger.Fatal("failed to dial leader", zap.Error(err))
	}

	conn.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         p.Config.Kafka.Brokers,
		GroupID:         p.Config.Kafka.GroupID,
		GroupTopics:     p.Config.Kafka.Topics,
		MinBytes:        10e3,
		MaxBytes:        10e6,
		StartOffset:     kafka.LastOffset,
		CommitInterval:  0,
		Dialer:          dialer,
		ReadLagInterval: time.Second,
		ReadBackoffMin:  100 * time.Millisecond,
		ReadBackoffMax:  time.Second,
		MaxAttempts:     p.Config.Kafka.MaxAttempts,
	})

	return &kafkaReader{
		reader:          reader,
		processor:       p.Processor,
		logger:          p.Logger,
		pool:            p.Pool,
		writer:          p.Writer,
		commitBatchSize: p.Config.Kafka.CommitBatchSize,
		commitInterval:  time.Duration(p.Config.Kafka.CommitInterval) * time.Second,
		pendingMessages: make([]kafka.Message, 0, p.Config.Kafka.CommitBatchSize),
	}
}

// Start starts the Kafka reader
func (r *kafkaReader) Start(ctx context.Context) {
	r.pool.Start()
	r.startCommitTicker(ctx)
	go r.readLoop(ctx)
}

// Stop stops the Kafka reader
func (r *kafkaReader) Stop() {
	if r.commitTicker != nil {
		r.commitTicker.Stop()
	}
	r.commitPendingMessages(context.Background())
	r.pool.Stop()
	_ = r.reader.Close()
}

// startCommitTicker starts the commit ticker
func (r *kafkaReader) startCommitTicker(ctx context.Context) {
	r.commitTicker = time.NewTicker(r.commitInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-r.commitTicker.C:
				r.commitPendingMessages(ctx)
			}
		}
	}()
}

// readLoop reads messages from the Kafka reader
func (r *kafkaReader) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Kafka reader stopped by context")
			return
		default:
			r.readAndHandleMessage(ctx)
		}
	}
}

// readAndHandleMessage reads and handles a message from the Kafka reader
func (r *kafkaReader) readAndHandleMessage(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	msg, err := r.reader.ReadMessage(readCtx)
	if err != nil {
		if isTemporaryError(err) {
			time.Sleep(time.Second)
			return
		}
		r.logger.Error("Error reading message", zap.Error(err))
		return
	}

	r.pool.Submit(func(taskCtx context.Context) {
		r.handleMessage(taskCtx, msg)
	})
}

// handleMessage handles a message from the Kafka reader
func (r *kafkaReader) handleMessage(ctx context.Context, msg kafka.Message) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("panic during processing", zap.Any("panic", rec))
		}
	}()

	r.logger.Info("Processing message",
		zap.String("topic", msg.Topic),
		zap.ByteString("key", msg.Key),
		zap.Int("partition", msg.Partition),
	)

	err := r.retryProcess(msg, r.reader.Config().MaxAttempts)
	if err != nil {
		r.logger.Error("Error processing message",
			zap.String("topic", msg.Topic),
			zap.Error(err),
		)

		if dlqErr := r.writer.WriteToDLQ(ctx, msg, err); dlqErr != nil {
			r.logger.Error("Failed to write to DLQ",
				zap.String("topic", msg.Topic),
				zap.Error(dlqErr),
			)
		}
	}

	r.queueMessageForCommit(ctx, msg)
}

// queueMessageForCommit queues a message for commit
func (r *kafkaReader) queueMessageForCommit(ctx context.Context, msg kafka.Message) {
	r.commitMutex.Lock()
	r.pendingMessages = append(r.pendingMessages, msg)

	if len(r.pendingMessages) >= r.commitBatchSize {
		batch := make([]kafka.Message, len(r.pendingMessages))
		copy(batch, r.pendingMessages)
		r.pendingMessages = r.pendingMessages[:0]
		r.commitMutex.Unlock()

		r.commitMessages(ctx, batch)
	} else {
		r.commitMutex.Unlock()
	}
}

// commitPendingMessages commits pending messages
func (r *kafkaReader) commitPendingMessages(ctx context.Context) {
	r.commitMutex.Lock()
	defer r.commitMutex.Unlock()

	if len(r.pendingMessages) == 0 {
		return
	}

	batch := make([]kafka.Message, len(r.pendingMessages))
	copy(batch, r.pendingMessages)
	r.pendingMessages = r.pendingMessages[:0]

	go r.commitMessages(ctx, batch)
}

// commitMessages commits a batch of messages
func (r *kafkaReader) commitMessages(ctx context.Context, msgs []kafka.Message) {
	if err := r.reader.CommitMessages(ctx, msgs...); err != nil {
		r.logger.Error("Error committing messages batch",
			zap.Int("batch_size", len(msgs)),
			zap.Error(err),
		)
		return
	}

	r.logger.Info("Committed message batch",
		zap.Int("batch_size", len(msgs)))
}

// retryProcess retries processing a message
func (r *kafkaReader) retryProcess(msg kafka.Message, maxAttempts int) error {
	var err error
	for i := 0; i < maxAttempts; i++ {
		err = r.processor.Process(msg)
		if err == nil {
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return err
}

// isTemporaryError checks if an error is temporary
func isTemporaryError(err error) bool {
	errStr := err.Error()
	return strings.Contains(errStr, "EOF") || strings.Contains(errStr, "context deadline") || strings.Contains(errStr, "connection reset")
}

// RunReader runs the Kafka reader
func RunReader(lc fx.Lifecycle, r Reader) {
	ctx, cancel := context.WithCancel(context.Background())

	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			r.Start(ctx)
			return nil
		},
		OnStop: func(_ context.Context) error {
			cancel()
			r.Stop()
			return nil
		},
	})
}
