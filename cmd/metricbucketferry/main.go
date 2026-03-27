package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/devil-mice-labs/metricbucketferry/internal/config"
	"github.com/devil-mice-labs/metricbucketferry/internal/exporter"
)

func main() {
	// Initialize structured logging with JSON output
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Error("Failed to load config", "error", err)
		os.Exit(1)
	}

	logger.Info("Starting Cloud Monitoring to Cloud Storage export",
		"project_id", cfg.ProjectID,
		"gcs_bucket", cfg.GCSBucket,
		"gcs_prefix", cfg.GCSPrefix)

	// Create context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Warn("Received shutdown signal", "signal", sig.String())
		cancel()
	}()

	// Capture run start timestamp - all files will use this same timestamp
	runStartTime := time.Now().UTC()

	// Initialize exporter
	exp, err := exporter.New(ctx, cfg, logger, runStartTime)
	if err != nil {
		logger.Error("Failed to create exporter", "error", err)
		os.Exit(1)
	}
	defer exp.Close()

	// Run metric descriptor export
	descriptors, err := exp.ExportMetricDescriptors(ctx)
	if err != nil {
		if ctx.Err() == context.Canceled {
			logger.Warn("Export canceled by user")
			os.Exit(130) // Standard exit code for SIGINT
		}
		logger.Error("Metric descriptor export failed", "error", err)
		os.Exit(1)
	}

	// Filter for GA descriptors and export time series
	gaDescriptors := exp.FilterGADescriptors(descriptors)
	if err := exp.ExportTimeSeries(ctx, gaDescriptors); err != nil {
		if ctx.Err() == context.Canceled {
			logger.Warn("Export canceled by user")
			os.Exit(130) // Standard exit code for SIGINT
		}
		logger.Error("Time series export failed", "error", err)
		os.Exit(1)
	}

	logger.Info("Full export completed successfully")
}
