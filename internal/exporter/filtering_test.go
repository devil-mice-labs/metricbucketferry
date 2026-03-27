package exporter

import (
	"testing"
	"time"

	"github.com/devil-mice-labs/metricbucketferry/internal/config"
	"google.golang.org/genproto/googleapis/api"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"log/slog"
	"os"
)

func TestFilterGADescriptors(t *testing.T) {
	cfg := &config.Config{
		ProjectID:      "test-project",
		GCSBucket:      "test-bucket",
		GCSPrefix:      "test-prefix",
		MaxConcurrency: 5,
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	e := &Exporter{
		config: cfg,
		logger: logger,
	}

	// Create test descriptors with mixed launch stages
	descriptors := []*metricpb.MetricDescriptor{
		{Type: "ga-metric-1", LaunchStage: api.LaunchStage_GA},
		{Type: "beta-metric-1", LaunchStage: api.LaunchStage_BETA},
		{Type: "ga-metric-2", LaunchStage: api.LaunchStage_GA},
		{Type: "alpha-metric-1", LaunchStage: api.LaunchStage_ALPHA},
		{Type: "ga-metric-3", LaunchStage: api.LaunchStage_GA},
		{Type: "deprecated-metric-1", LaunchStage: api.LaunchStage_DEPRECATED},
		{Type: "unspecified-metric-1", LaunchStage: api.LaunchStage_LAUNCH_STAGE_UNSPECIFIED},
	}

	gaDescriptors := e.FilterGADescriptors(descriptors)

	// Should only return 3 GA descriptors
	expectedCount := 3
	if len(gaDescriptors) != expectedCount {
		t.Errorf("Expected %d GA descriptors, got %d", expectedCount, len(gaDescriptors))
	}

	// Verify all returned descriptors are GA
	for i, desc := range gaDescriptors {
		if desc.LaunchStage != api.LaunchStage_GA {
			t.Errorf("Descriptor %d has wrong launch stage: expected GA, got %v", i, desc.LaunchStage)
		}
	}

	// Verify the correct descriptors were selected
	expectedTypes := map[string]bool{
		"ga-metric-1": false,
		"ga-metric-2": false,
		"ga-metric-3": false,
	}

	for _, desc := range gaDescriptors {
		if _, exists := expectedTypes[desc.Type]; !exists {
			t.Errorf("Unexpected descriptor type: %s", desc.Type)
		}
		expectedTypes[desc.Type] = true
	}

	for metricType, found := range expectedTypes {
		if !found {
			t.Errorf("Expected GA descriptor not found: %s", metricType)
		}
	}
}

func TestFilterGADescriptors_AllGA(t *testing.T) {
	cfg := &config.Config{
		ProjectID: "test-project",
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	e := &Exporter{
		config: cfg,
		logger: logger,
	}

	// All descriptors are GA
	descriptors := []*metricpb.MetricDescriptor{
		{Type: "ga-metric-1", LaunchStage: api.LaunchStage_GA},
		{Type: "ga-metric-2", LaunchStage: api.LaunchStage_GA},
		{Type: "ga-metric-3", LaunchStage: api.LaunchStage_GA},
	}

	gaDescriptors := e.FilterGADescriptors(descriptors)

	if len(gaDescriptors) != len(descriptors) {
		t.Errorf("Expected all %d descriptors, got %d", len(descriptors), len(gaDescriptors))
	}
}

func TestFilterGADescriptors_NoneGA(t *testing.T) {
	cfg := &config.Config{
		ProjectID: "test-project",
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	e := &Exporter{
		config: cfg,
		logger: logger,
	}

	// No GA descriptors
	descriptors := []*metricpb.MetricDescriptor{
		{Type: "beta-metric-1", LaunchStage: api.LaunchStage_BETA},
		{Type: "alpha-metric-1", LaunchStage: api.LaunchStage_ALPHA},
	}

	gaDescriptors := e.FilterGADescriptors(descriptors)

	if len(gaDescriptors) != 0 {
		t.Errorf("Expected 0 GA descriptors, got %d", len(gaDescriptors))
	}
}

func TestGetYesterdayRange(t *testing.T) {
	cfg := &config.Config{
		ProjectID: "test-project",
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	e := &Exporter{
		config: cfg,
		logger: logger,
	}

	start, end := e.getYesterdayRange()

	// Verify both are UTC
	if start.Location() != time.UTC {
		t.Errorf("Start time not in UTC: %v", start.Location())
	}
	if end.Location() != time.UTC {
		t.Errorf("End time not in UTC: %v", end.Location())
	}

	// Verify start is at midnight
	if start.Hour() != 0 || start.Minute() != 0 || start.Second() != 0 || start.Nanosecond() != 0 {
		t.Errorf("Start time not at midnight: %v", start)
	}

	// Verify end is at midnight
	if end.Hour() != 0 || end.Minute() != 0 || end.Second() != 0 || end.Nanosecond() != 0 {
		t.Errorf("End time not at midnight: %v", end)
	}

	// Verify exactly 24 hours difference
	duration := end.Sub(start)
	expectedDuration := 24 * time.Hour
	if duration != expectedDuration {
		t.Errorf("Expected 24h difference, got %v", duration)
	}

	// Verify end is today at midnight
	now := time.Now().UTC()
	expectedEnd := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	if !end.Equal(expectedEnd) {
		t.Errorf("End time mismatch: expected %v, got %v", expectedEnd, end)
	}

	// Verify start is yesterday at midnight
	expectedStart := expectedEnd.Add(-24 * time.Hour)
	if !start.Equal(expectedStart) {
		t.Errorf("Start time mismatch: expected %v, got %v", expectedStart, start)
	}
}
