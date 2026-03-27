package exporter

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"github.com/devil-mice-labs/metricbucketferry/internal/config"
	"github.com/devil-mice-labs/metricbucketferry/internal/monitoring"
	"github.com/devil-mice-labs/metricbucketferry/internal/storage"
	"github.com/devil-mice-labs/metricbucketferry/pkg/metricspb"
	"google.golang.org/genproto/googleapis/api"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Exporter orchestrates the metric descriptor export process
type Exporter struct {
	monitoringClient *monitoring.Client
	storageWriter    *storage.Writer
	config           *config.Config
	logger           *slog.Logger
	runStartTime     time.Time // Timestamp when the export run started - used for all file names
}

// New creates a new Exporter
func New(ctx context.Context, cfg *config.Config, logger *slog.Logger, runStartTime time.Time) (*Exporter, error) {
	// Initialize Cloud Monitoring client
	monitoringClient, err := monitoring.NewClient(ctx, cfg.ProjectID, cfg.MonitoringPageSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create monitoring client: %w", err)
	}

	// Initialize Cloud Storage writer
	storageWriter, err := storage.NewWriter(ctx, cfg.GCSBucket, cfg.GCSPrefix, cfg.GCSChunkSizeMB)
	if err != nil {
		monitoringClient.Close()
		return nil, fmt.Errorf("failed to create storage writer: %w", err)
	}

	return &Exporter{
		monitoringClient: monitoringClient,
		storageWriter:    storageWriter,
		config:           cfg,
		logger:           logger,
		runStartTime:     runStartTime,
	}, nil
}

// ExportMetricDescriptors exports all metric descriptors to Cloud Storage
// Returns the descriptors for further processing (e.g., filtering for GA metrics)
func (e *Exporter) ExportMetricDescriptors(ctx context.Context) ([]*metricpb.MetricDescriptor, error) {
	startTime := time.Now()

	// Fetch metric descriptors
	descriptors, err := e.monitoringClient.ListMetricDescriptors(ctx)
	if err != nil {
		return nil, fmt.Errorf("list metric descriptors: %w", err)
	}

	e.logger.Info("Starting export",
		"total_descriptors", len(descriptors),
		"max_concurrency", e.config.MaxConcurrency)

	// Export all descriptors with retry
	if err := e.processAllWithRetry(ctx, descriptors); err != nil {
		return nil, err
	}

	duration := time.Since(startTime)
	e.logger.Info("Export completed",
		"duration_seconds", duration.Seconds(),
		"total_descriptors", len(descriptors))

	return descriptors, nil
}

// processAllWithRetry processes all descriptors with exponential backoff retry
func (e *Exporter) processAllWithRetry(ctx context.Context, descriptors []*metricpb.MetricDescriptor) error {
	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			e.logger.Warn("Retrying export",
				"attempt", attempt+1,
				"backoff_seconds", backoff.Seconds())

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := e.processAll(ctx, descriptors); err != nil {
			lastErr = err
			e.logger.Warn("Export attempt failed",
				"attempt", attempt+1,
				"error", err)
			continue
		}

		return nil
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

// processAll processes all metric descriptors in a single operation
func (e *Exporter) processAll(ctx context.Context, descriptors []*metricpb.MetricDescriptor) error {
	// Transform to export format using existing method
	export := e.transformToExportFormat(descriptors)

	// Serialize to protobuf
	data, err := proto.Marshal(export)
	if err != nil {
		return fmt.Errorf("serialize descriptors: %w", err)
	}

	// Generate filename WITHOUT batch ID
	timestamp := e.runStartTime.Format(time.RFC3339)
	filename := fmt.Sprintf("metric-descriptors/%s.pb", timestamp)

	// Upload to Cloud Storage
	if err := e.storageWriter.WriteMetrics(ctx, data, filename); err != nil {
		return fmt.Errorf("upload descriptors: %w", err)
	}

	e.logger.Info("Descriptors uploaded",
		"size_bytes", len(data),
		"descriptors", len(descriptors),
		"filename", filename)

	return nil
}

// transformToExportFormat converts metric descriptors to the export format
func (e *Exporter) transformToExportFormat(descriptors []*metricpb.MetricDescriptor) *metricspb.MetricDescriptorExport {
	now := timestamppb.Now()

	export := &metricspb.MetricDescriptorExport{
		Metadata: &metricspb.ExportMetadata{
			ProjectId:       e.config.ProjectID,
			ExportTime:      now,
			ExporterVersion: "1.0.0",
			TotalCount:      int32(len(descriptors)),
		},
		Descriptors: make([]*metricspb.MetricDescriptorSnapshot, len(descriptors)),
	}

	for i, desc := range descriptors {
		export.Descriptors[i] = &metricspb.MetricDescriptorSnapshot{
			Descriptor_: desc,
			CapturedAt:  now,
		}
	}

	return export
}

// Close closes all underlying clients
func (e *Exporter) Close() error {
	var err1, err2 error

	if e.monitoringClient != nil {
		err1 = e.monitoringClient.Close()
	}

	if e.storageWriter != nil {
		err2 = e.storageWriter.Close()
	}

	if err1 != nil {
		return err1
	}
	return err2
}

// FilterGADescriptors filters metric descriptors by GA launch stage
func (e *Exporter) FilterGADescriptors(descriptors []*metricpb.MetricDescriptor) []*metricpb.MetricDescriptor {
	var gaDescriptors []*metricpb.MetricDescriptor
	for _, desc := range descriptors {
		if desc.LaunchStage == api.LaunchStage_GA {
			gaDescriptors = append(gaDescriptors, desc)
		}
	}
	e.logger.Info("Filtered GA descriptors",
		"total_descriptors", len(descriptors),
		"ga_descriptors", len(gaDescriptors))
	return gaDescriptors
}

// getYesterdayRange returns start and end times for yesterday (full 24h period UTC)
func (e *Exporter) getYesterdayRange() (time.Time, time.Time) {
	now := time.Now().UTC()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	yesterday := today.Add(-24 * time.Hour)
	return yesterday, today
}

// ExportTimeSeries exports time series for GA metrics
func (e *Exporter) ExportTimeSeries(ctx context.Context, gaDescriptors []*metricpb.MetricDescriptor) error {
	startTime, endTime := e.getYesterdayRange()

	e.logger.Info("Starting time series export",
		"num_metrics", len(gaDescriptors),
		"start_time", startTime.Format(time.RFC3339),
		"end_time", endTime.Format(time.RFC3339))

	// Export time series concurrently with semaphore
	return e.exportTimeSeriesConcurrent(ctx, gaDescriptors, startTime, endTime)
}

// exportTimeSeriesConcurrent exports time series for metrics concurrently.
//
// Concurrency model: Spawns up to MaxConcurrency goroutines that call
// monitoringClient.ListTimeSeries() concurrently. This is safe because
// the Google Cloud monitoring client is thread-safe and designed for
// concurrent use without additional synchronization.
func (e *Exporter) exportTimeSeriesConcurrent(ctx context.Context,
	descriptors []*metricpb.MetricDescriptor,
	startTime, endTime time.Time) error {

	sem := make(chan struct{}, e.config.MaxConcurrency)
	errCh := make(chan error, len(descriptors))
	var wg sync.WaitGroup

	for i, desc := range descriptors {
		wg.Add(1)
		go func(d *metricpb.MetricDescriptor, idx int) {
			defer wg.Done()

			sem <- struct{}{}        // Acquire semaphore
			defer func() { <-sem }() // Release semaphore

			if err := e.processTimeSeriesWithRetry(ctx, d, startTime, endTime, idx); err != nil {
				e.logger.Error("Time series export failed",
					"metric_type", d.Type,
					"index", idx,
					"error", err)
				errCh <- err
			}
		}(desc, i)
	}

	wg.Wait()
	close(errCh)

	// Collect errors
	var firstErr error
	errorCount := 0
	for err := range errCh {
		if err != nil {
			errorCount++
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	if firstErr != nil {
		return fmt.Errorf("failed to export %d time series: %w", errorCount, firstErr)
	}

	e.logger.Info("Time series export completed",
		"total_metrics", len(descriptors),
		"errors", errorCount)

	return nil
}

// processTimeSeriesWithRetry exports time series for a single metric with retry
func (e *Exporter) processTimeSeriesWithRetry(ctx context.Context,
	descriptor *metricpb.MetricDescriptor,
	startTime, endTime time.Time,
	index int) error {

	maxRetries := 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			e.logger.Warn("Retrying time series export",
				"metric_type", descriptor.Type,
				"attempt", attempt+1,
				"backoff_seconds", backoff.Seconds())

			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if err := e.processTimeSeries(ctx, descriptor, startTime, endTime, index); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("max retries exceeded for metric %s: %w", descriptor.Type, lastErr)
}

// processTimeSeries exports time series for a single metric
func (e *Exporter) processTimeSeries(ctx context.Context,
	descriptor *metricpb.MetricDescriptor,
	startTime, endTime time.Time,
	index int) error {

	// Fetch time series from API
	timeSeries, err := e.monitoringClient.ListTimeSeries(ctx, descriptor.Type, startTime, endTime)
	if err != nil {
		return fmt.Errorf("list time series for %s: %w", descriptor.Type, err)
	}

	// Skip if no data
	if len(timeSeries) == 0 {
		e.logger.Debug("No time series data",
			"metric_type", descriptor.Type)
		return nil
	}

	// Transform to export format
	export := e.transformTimeSeriesExport(descriptor.Type, timeSeries, startTime, endTime)

	// Serialize
	data, err := proto.Marshal(export)
	if err != nil {
		return fmt.Errorf("serialize time series for %s: %w", descriptor.Type, err)
	}

	// Generate filename
	timestamp := e.runStartTime.Format(time.RFC3339)
	safeMetricType := strings.ReplaceAll(descriptor.Type, "/", "_")
	filename := fmt.Sprintf("time-series/%s/%s.pb", timestamp, safeMetricType)

	// Upload to Cloud Storage
	if err := e.storageWriter.WriteMetrics(ctx, data, filename); err != nil {
		return fmt.Errorf("upload time series for %s: %w", descriptor.Type, err)
	}

	e.logger.Info("Time series uploaded",
		"metric_type", descriptor.Type,
		"num_series", len(timeSeries),
		"size_bytes", len(data),
		"filename", filename)

	return nil
}

// transformTimeSeriesExport creates a TimeSeriesExport message
func (e *Exporter) transformTimeSeriesExport(
	metricType string,
	timeSeries []*monitoringpb.TimeSeries,
	startTime, endTime time.Time) *metricspb.TimeSeriesExport {

	return &metricspb.TimeSeriesExport{
		Metadata: &metricspb.ExportMetadata{
			ProjectId:       e.config.ProjectID,
			ExportTime:      timestamppb.Now(),
			ExporterVersion: "1.0.0",
			TotalCount:      int32(len(timeSeries)),
		},
		MetricType: metricType,
		StartTime:  timestamppb.New(startTime),
		EndTime:    timestamppb.New(endTime),
		TimeSeries: timeSeries,
	}
}
