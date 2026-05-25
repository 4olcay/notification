package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig
	DB       DBConfig
	Kafka    KafkaConfig
	Provider ProviderConfig
	Worker   WorkerConfig
}

type ServerConfig struct {
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type DBConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Database string `mapstructure:"database"`
	SSLMode  string `mapstructure:"sslmode"`
}

func (c DBConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Host, c.Port, c.User, c.Password, c.Database, c.SSLMode)
}

func (c DBConfig) URL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}

type KafkaConfig struct {
	Brokers         []string `mapstructure:"brokers"`
	TopicHigh       string   `mapstructure:"topic_high"`
	TopicNormal     string   `mapstructure:"topic_normal"`
	TopicLow        string   `mapstructure:"topic_low"`
	TopicDeadLetter string   `mapstructure:"topic_dead_letter"`
	GroupID         string   `mapstructure:"group_id"`
}

type ProviderConfig struct {
	WebhookURL              string        `mapstructure:"webhook_url"`
	Timeout                 time.Duration `mapstructure:"timeout"`
	CircuitBreakerThreshold int           `mapstructure:"circuit_breaker_threshold"`
	CircuitBreakerReset     time.Duration `mapstructure:"circuit_breaker_reset"`
}

type WorkerConfig struct {
	Concurrency     int     `mapstructure:"concurrency"`
	RateLimitPerSec float64 `mapstructure:"rate_limit_per_sec"`
	MaxRetries      int     `mapstructure:"max_retries"`
}

func Load() (*Config, error) {
	v := viper.New()

	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", 5*time.Second)
	v.SetDefault("server.write_timeout", 10*time.Second)

	v.SetDefault("db.host", "localhost")
	v.SetDefault("db.port", 5432)
	v.SetDefault("db.user", "postgres")
	v.SetDefault("db.password", "postgres")
	v.SetDefault("db.database", "notifications_db")
	v.SetDefault("db.sslmode", "disable")

	v.SetDefault("kafka.brokers", []string{"localhost:19092"})
	v.SetDefault("kafka.topic_high", "notifications.high")
	v.SetDefault("kafka.topic_normal", "notifications.normal")
	v.SetDefault("kafka.topic_low", "notifications.low")
	v.SetDefault("kafka.topic_dead_letter", "notifications.dead_letter")
	v.SetDefault("kafka.group_id", "notification-workers")

	v.SetDefault("provider.webhook_url", "https://webhook.site/your-uuid")
	v.SetDefault("provider.timeout", 10*time.Second)
	v.SetDefault("provider.circuit_breaker_threshold", 5)
	v.SetDefault("provider.circuit_breaker_reset", 30*time.Second)

	v.SetDefault("worker.concurrency", 10)
	v.SetDefault("worker.rate_limit_per_sec", 100.0)
	v.SetDefault("worker.max_retries", 5)

	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	var cfg Config
	decodeHook := viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	))
	if err := v.Unmarshal(&cfg, decodeHook); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}
