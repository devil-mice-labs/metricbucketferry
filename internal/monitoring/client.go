package monitoring

import (
	"context"
	"fmt"
	"log"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Client wraps the Cloud Monitoring API client.
//
// Thread-safety: The underlying monitoring.MetricClient from Google Cloud Go
// library is thread-safe and designed for concurrent use. Multiple goroutines
// can safely call methods on the same Client instance without additional
// synchronization. The client uses gRPC connection pooling internally to
// handle concurrent requests efficiently.
type Client struct {
	metricClient *monitoring.MetricClient
	projectID    string
	pageSize     int32 // Page size for API requests
}

// NewClient creates a new Cloud Monitoring client
func NewClient(ctx context.Context, projectID string, pageSize int) (*Client, error) {
	metricClient, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric client: %w", err)
	}

	return &Client{
		metricClient: metricClient,
		projectID:    projectID,
		pageSize:     int32(pageSize),
	}, nil
}

// ListMetricDescriptors fetches all metric descriptors for the project
func (c *Client) ListMetricDescriptors(ctx context.Context) ([]*metricpb.MetricDescriptor, error) {
	req := &monitoringpb.ListMetricDescriptorsRequest{
		Name:     fmt.Sprintf("projects/%s", c.projectID),
		PageSize: c.pageSize,
	}

	it := c.metricClient.ListMetricDescriptors(ctx, req)
	var descriptors []*metricpb.MetricDescriptor

	log.Printf("Fetching metric descriptors for project: %s", c.projectID)

	for {
		md, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate metric descriptors: %w", err)
		}

		descriptors = append(descriptors, md)
	}

	log.Printf("Fetched %d metric descriptors", len(descriptors))
	return descriptors, nil
}

// ListTimeSeries fetches time series for a specific metric type
func (c *Client) ListTimeSeries(ctx context.Context, metricType string, startTime, endTime time.Time) ([]*monitoringpb.TimeSeries, error) {
	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(startTime),
		EndTime:   timestamppb.New(endTime),
	}

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:     fmt.Sprintf("projects/%s", c.projectID),
		Filter:   fmt.Sprintf(`metric.type = "%s"`, metricType),
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
		PageSize: c.pageSize,
	}

	it := c.metricClient.ListTimeSeries(ctx, req)
	var timeSeries []*monitoringpb.TimeSeries

	for {
		ts, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate time series: %w", err)
		}

		timeSeries = append(timeSeries, ts)
	}

	return timeSeries, nil
}

// Close closes the Cloud Monitoring client
func (c *Client) Close() error {
	return c.metricClient.Close()
}
