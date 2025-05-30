package config

import (
	"log"
	"os"
	"strings"

	"github.com/kelseyhightower/envconfig"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

type Config struct {
	DB          DBConfig
	Kafka       KafkaConfig
	Environment EnvironmentConfig
}

type DBConfig struct {
	Host     string `envconfig:"DB_HOST" default:"localhost"`
	Port     string `envconfig:"DB_PORT" default:"5432"`
	User     string `envconfig:"DB_USER" default:"postgres"`
	Password string `envconfig:"DB_PASSWORD" default:"postgres"`
	DBName   string `envconfig:"DB_NAME" default:"postgres"`
	SSLMode  string `envconfig:"SSL_MODE" default:"disable"`
}

type KafkaConfig struct {
	Brokers         []string `envconfig:"KAFKA_BROKERS" required:"true"`
	GroupID         string   `envconfig:"KAFKA_GROUP_ID" required:"true"`
	Topics          []string `envconfig:"KAFKA_TOPICS" required:"true"`
	User            string   `envconfig:"KAFKA_USER" required:"true"`
	Password        string   `envconfig:"KAFKA_PASSWORD" required:"true"`
	MaxAttempts     int      `envconfig:"KAFKA_MAX_ATTEMPTS" default:"3"`
	DLQTopic        string   `envconfig:"KAFKA_DLQ_TOPIC" required:"true"`
	DLQBrokers      []string `envconfig:"KAFKA_DLQ_BROKERS" required:"true"`
	CommitBatchSize int      `envconfig:"KAFKA_COMMIT_BATCH_SIZE" default:"100"`
	CommitInterval  int      `envconfig:"KAFKA_COMMIT_INTERVAL" default:"5"`
}

type EnvironmentConfig struct {
	Env         string `envconfig:"ENVIRONMENT" default:"development"`
	NumWorkers  int    `envconfig:"NUM_WORKERS" default:"10"`
	TopicPrefix string `envconfig:"TOPIC_PREFIX" required:"true"`
}

func NewConfig() (*Config, error) {
	LoadConfig()

	var cfg Config

	if err := envconfig.Process("", &cfg.DB); err != nil {
		log.Fatalf("Failed to process DB config: %v", err)
	}
	if err := envconfig.Process("", &cfg.Kafka); err != nil {
		log.Fatalf("Failed to process Kafka config: %v", err)
	}
	if err := envconfig.Process("", &cfg.Environment); err != nil {
		log.Fatalf("Failed to process Environment config: %v", err)
	}

	return &cfg, nil
}

func LoadConfig() {
	viper.SetConfigFile(".env")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	for _, key := range viper.AllKeys() {
		val := viper.GetString(key)
		if val != "" {
			_ = os.Setenv(strings.ToUpper(key), val)
		}
	}
}

var Module = fx.Options(
	fx.Provide(NewConfig),
)
