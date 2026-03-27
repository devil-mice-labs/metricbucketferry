package config

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
)

// Config holds the application configuration
type Config struct {
	ProjectID          string
	GCSBucket          string
	GCSPrefix          string
	MaxConcurrency     int
	MonitoringPageSize int // API page size for ListMetricDescriptors and ListTimeSeries
	GCSChunkSizeMB     int // Cloud Storage writer chunk size in MB
}

// Load loads configuration from environment variables
func Load() (*Config, error) {
	cfg := &Config{
		ProjectID:          getEnv("GOOGLE_CLOUD_PROJECT", ""),
		GCSBucket:          getEnv("GCS_BUCKET", ""),
		GCSPrefix:          getEnv("GCS_PREFIX", "metrics"),
		MaxConcurrency:     getEnvInt("MAX_CONCURRENCY", defaultConcurrency()),
		MonitoringPageSize: getEnvInt("MONITORING_PAGE_SIZE", 100000), // API maximum for best performance
		GCSChunkSizeMB:     getEnvInt("GCS_CHUNK_SIZE_MB", 32),        // Optimal for sustained streaming
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("GOOGLE_CLOUD_PROJECT is required")
	}
	if c.GCSBucket == "" {
		return fmt.Errorf("GCS_BUCKET is required")
	}
	return nil
}

// getEnv returns the environment variable value or a default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt returns the environment variable value as an integer or a default
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

// getEnvBool returns the environment variable value as a boolean or a default
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

// defaultConcurrency calculates the default concurrency level based on CPU cores
// For I/O-bound workloads (API calls, network), 4x CPU cores is a good starting point
// No artificial cap - API rate limiting handles overload with 429 responses
func defaultConcurrency() int {
	cores := runtime.NumCPU()

	// Heuristic: 4x CPU cores for I/O-bound work
	concurrency := cores * 4

	// Sanity minimum
	if concurrency < 1 {
		concurrency = 1
	}

	return concurrency
}
