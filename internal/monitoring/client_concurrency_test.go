package monitoring

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestClientConcurrentListTimeSeries verifies that multiple goroutines can
// safely call ListTimeSeries on the same Client instance concurrently.
//
// This test validates that:
// - No race conditions occur during concurrent API calls
// - All goroutines complete successfully
// - Each goroutine receives valid data
//
// Run with: go test -race ./extractor/internal/monitoring/
func TestClientConcurrentListTimeSeries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	projectID := "dml-sandbox-pigeon"
	pageSize := 1000

	// Create a single client instance to be shared across goroutines
	client, err := NewClient(ctx, projectID, pageSize)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test different concurrency levels
	tests := []struct {
		name             string
		numGoroutines    int
		metricType       string
		expectedMinCount int // Minimum number of time series expected (0 if metric might be empty)
	}{
		{
			name:             "5 concurrent requests",
			numGoroutines:    5,
			metricType:       "compute.googleapis.com/instance/cpu/utilization",
			expectedMinCount: 0, // May be empty
		},
		{
			name:             "10 concurrent requests",
			numGoroutines:    10,
			metricType:       "compute.googleapis.com/instance/cpu/utilization",
			expectedMinCount: 0,
		},
		{
			name:             "20 concurrent requests",
			numGoroutines:    20,
			metricType:       "compute.googleapis.com/instance/cpu/utilization",
			expectedMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			var successCount atomic.Int32
			var errorCount atomic.Int32
			errChan := make(chan error, tt.numGoroutines)

			// Time range: last 24 hours
			endTime := time.Now().UTC()
			startTime := endTime.Add(-24 * time.Hour)

			// Record test duration
			testStart := time.Now()

			// Spawn concurrent goroutines
			for i := 0; i < tt.numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					// Call the client method
					timeSeries, err := client.ListTimeSeries(ctx, tt.metricType, startTime, endTime)
					if err != nil {
						t.Logf("Goroutine %d failed: %v", id, err)
						errorCount.Add(1)
						errChan <- err
						return
					}

					// Validate results - timeSeries can be nil or empty slice, both are valid
					// (metrics may have no data in the time range)
					// What matters is that the call succeeded without error

					// Log results for debugging
					t.Logf("Goroutine %d completed successfully, received %d time series", id, len(timeSeries))
					successCount.Add(1)
				}(i)
			}

			// Wait for all goroutines to complete
			wg.Wait()
			close(errChan)

			testDuration := time.Since(testStart)
			t.Logf("Test completed in %v", testDuration)

			// Verify all goroutines succeeded
			if errorCount.Load() > 0 {
				t.Errorf("Expected 0 errors, got %d errors", errorCount.Load())
				// Log first few errors
				count := 0
				for err := range errChan {
					if count < 3 {
						t.Logf("Error: %v", err)
						count++
					}
				}
			}

			if successCount.Load() != int32(tt.numGoroutines) {
				t.Errorf("Expected %d successful goroutines, got %d", tt.numGoroutines, successCount.Load())
			}
		})
	}
}

// TestClientConcurrentListMetricDescriptors verifies that multiple goroutines
// can safely call ListMetricDescriptors on the same Client instance concurrently.
func TestClientConcurrentListMetricDescriptors(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	projectID := "dml-sandbox-pigeon"
	pageSize := 1000

	// Create a single client instance to be shared across goroutines
	client, err := NewClient(ctx, projectID, pageSize)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Test different concurrency levels
	tests := []struct {
		name          string
		numGoroutines int
	}{
		{
			name:          "5 concurrent requests",
			numGoroutines: 5,
		},
		{
			name:          "10 concurrent requests",
			numGoroutines: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wg sync.WaitGroup
			var successCount atomic.Int32
			var errorCount atomic.Int32
			errChan := make(chan error, tt.numGoroutines)

			// Track descriptor counts for consistency check
			descriptorCounts := make([]int, tt.numGoroutines)
			var countMutex sync.Mutex

			testStart := time.Now()

			// Spawn concurrent goroutines
			for i := 0; i < tt.numGoroutines; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()

					// Call the client method
					descriptors, err := client.ListMetricDescriptors(ctx)
					if err != nil {
						t.Logf("Goroutine %d failed: %v", id, err)
						errorCount.Add(1)
						errChan <- err
						return
					}

					// Validate results
					if descriptors == nil {
						t.Errorf("Goroutine %d received nil descriptors", id)
						errorCount.Add(1)
						return
					}

					// Record count for consistency check
					countMutex.Lock()
					descriptorCounts[id] = len(descriptors)
					countMutex.Unlock()

					t.Logf("Goroutine %d completed successfully, received %d descriptors", id, len(descriptors))
					successCount.Add(1)
				}(i)
			}

			// Wait for all goroutines to complete
			wg.Wait()
			close(errChan)

			testDuration := time.Since(testStart)
			t.Logf("Test completed in %v", testDuration)

			// Verify all goroutines succeeded
			if errorCount.Load() > 0 {
				t.Errorf("Expected 0 errors, got %d errors", errorCount.Load())
				count := 0
				for err := range errChan {
					if count < 3 {
						t.Logf("Error: %v", err)
						count++
					}
				}
			}

			if successCount.Load() != int32(tt.numGoroutines) {
				t.Errorf("Expected %d successful goroutines, got %d", tt.numGoroutines, successCount.Load())
			}

			// Verify consistency: all goroutines should receive the same count
			// (or very close, since descriptors can change during the test)
			if successCount.Load() > 0 {
				firstCount := descriptorCounts[0]
				for i, count := range descriptorCounts[1:] {
					// Allow small variance due to API changes during test
					variance := abs(count - firstCount)
					if variance > 10 {
						t.Logf("Warning: Goroutine %d received %d descriptors, expected around %d (variance: %d)",
							i+1, count, firstCount, variance)
					}
				}
			}
		})
	}
}

// TestClientConcurrentMixedCalls verifies that different API methods can be
// called concurrently on the same Client instance.
func TestClientConcurrentMixedCalls(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()
	projectID := "dml-sandbox-pigeon"
	pageSize := 1000

	// Create a single client instance
	client, err := NewClient(ctx, projectID, pageSize)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	var errorCount atomic.Int32
	numGoroutines := 10

	testStart := time.Now()

	// Spawn goroutines that alternate between different API calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Alternate between ListTimeSeries and ListMetricDescriptors
			if id%2 == 0 {
				// ListTimeSeries
				endTime := time.Now().UTC()
				startTime := endTime.Add(-24 * time.Hour)
				_, err := client.ListTimeSeries(ctx, "compute.googleapis.com/instance/cpu/utilization", startTime, endTime)
				if err != nil {
					t.Logf("Goroutine %d (ListTimeSeries) failed: %v", id, err)
					errorCount.Add(1)
					return
				}
				t.Logf("Goroutine %d (ListTimeSeries) completed successfully", id)
			} else {
				// ListMetricDescriptors
				_, err := client.ListMetricDescriptors(ctx)
				if err != nil {
					t.Logf("Goroutine %d (ListMetricDescriptors) failed: %v", id, err)
					errorCount.Add(1)
					return
				}
				t.Logf("Goroutine %d (ListMetricDescriptors) completed successfully", id)
			}
		}(i)
	}

	wg.Wait()

	testDuration := time.Since(testStart)
	t.Logf("Mixed concurrent test completed in %v", testDuration)

	if errorCount.Load() > 0 {
		t.Errorf("Expected 0 errors, got %d errors", errorCount.Load())
	}
}

// abs returns the absolute value of an integer
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
