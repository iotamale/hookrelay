package config

import (
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                     string
	WorkerCount              int
	QueueSize                int
	MaxRetries               int
	BaseBackoff              time.Duration
	MaxBackoff               time.Duration
	BreakerGCInterval        time.Duration
	BreakerGCTTL             time.Duration
	FailuresThreshold        uint16
	OpenStateTimeoutDuration time.Duration
}

func Load() Config {
	if err := godotenv.Load(); err != nil {
		slog.Info("no .env file found, relying on system environment variables")
	}

	return Config{
		Port:                     getEnv("PORT", "8080"),
		WorkerCount:              getEnvAsInt("WORKER_COUNT", 8),
		QueueSize:                getEnvAsInt("QUEUE_SIZE", 1024),
		MaxRetries:               getEnvAsInt("MAX_RETRIES", 13),
		BaseBackoff:              getEnvAsDuration("BASE_BACKOFF", 1*time.Second),
		MaxBackoff:               getEnvAsDuration("MAX_BACKOFF", 1*time.Hour),
		BreakerGCInterval:        getEnvAsDuration("BREAKER_GC_INTERVAL", 1*time.Hour),
		BreakerGCTTL:             getEnvAsDuration("BREAKER_GC_TTL", 24*time.Hour),
		FailuresThreshold:        uint16(getEnvAsInt("FAILURES_THRESHOLD", 5)),
		OpenStateTimeoutDuration: getEnvAsDuration("OPEN_STATE_TIMEOUT", 60*time.Second),
	}
}

// getEnvAsInt gets string from env or returns fallback in case of an error.
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// getEnvAsInt safely parses string to int. In case of error it returns fallback.
func getEnvAsInt(key string, fallback int) int {
	strValue := getEnv(key, "")
	if value, err := strconv.Atoi(strValue); err == nil {
		return value
	}
	return fallback
}

// getEnvAsDuration safely parses string (e.g. "2h", "500ms") to time.Duration.
func getEnvAsDuration(key string, fallback time.Duration) time.Duration {
	strValue := getEnv(key, "")
	if value, err := time.ParseDuration(strValue); err == nil {
		return value
	}
	return fallback
}
