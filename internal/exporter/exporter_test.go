package exporter

import (
	"log/slog"
	"os"
	"testing"

	"github.com/devil-mice-labs/metricbucketferry/internal/config"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
)

func TestTransformToExportFormat(t *testing.T) {
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

	// Create test metric descriptors
	descriptors := []*metricpb.MetricDescriptor{
		{
			Type:        "test.googleapis.com/metric1",
			DisplayName: "Test Metric 1",
		},
		{
			Type:        "test.googleapis.com/metric2",
			DisplayName: "Test Metric 2",
		},
	}

	// Transform
	export := e.transformToExportFormat(descriptors)

	// Verify metadata
	if export.Metadata == nil {
		t.Fatal("Metadata is nil")
	}
	if export.Metadata.ProjectId != cfg.ProjectID {
		t.Errorf("Expected ProjectId %s, got %s", cfg.ProjectID, export.Metadata.ProjectId)
	}
	if export.Metadata.ExporterVersion != "1.0.0" {
		t.Errorf("Expected ExporterVersion 1.0.0, got %s", export.Metadata.ExporterVersion)
	}
	if export.Metadata.TotalCount != 2 {
		t.Errorf("Expected TotalCount 2, got %d", export.Metadata.TotalCount)
	}
	if export.Metadata.ExportTime == nil {
		t.Error("ExportTime is nil")
	}

	// Verify descriptors
	if len(export.Descriptors) != 2 {
		t.Errorf("Expected 2 descriptors, got %d", len(export.Descriptors))
	}

	for i, snapshot := range export.Descriptors {
		if snapshot.Descriptor_ == nil {
			t.Errorf("Descriptor %d is nil", i)
		}
		if snapshot.CapturedAt == nil {
			t.Errorf("CapturedAt %d is nil", i)
		}
		if snapshot.Descriptor_.Type != descriptors[i].Type {
			t.Errorf("Expected type %s, got %s", descriptors[i].Type, snapshot.Descriptor_.Type)
		}
	}
}
