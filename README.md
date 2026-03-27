# metricbucketferry

Cloud Monitoring to Cloud Storage export tool

An experiment in high-performance monitoring data extraction from Cloud Monitoring using gRPC and Protobuf.

**Repository**: `github.com/devil-mice-labs/metricbucketferry`

## Overview

This tool exports Google Cloud Monitoring metric descriptors and time series data to Cloud Storage in Protobuf format. The data is exported directly in the Protobuf format received from the Cloud Monitoring API without modification, wrapped with export metadata.

**Note**: At this stage, the program is hardcoded to export monitoring data for the previous day only.

## Features

* **gRPC Communication** - Uses gRPC to read data from Cloud Monitoring API
* **Native Protobuf Export** - Exports data in the same Protobuf format received from Cloud Monitoring (with metadata wrapper)
* **Concurrent Processing** - Parallel time series processing with auto-detected concurrency (4×CPU cores for I/O-bound workload)
* **Structured Logging** - JSON-formatted logs with `log/slog` for easy parsing and monitoring
* **Error Handling** - Automatic retry with exponential backoff for transient failures
* **Graceful Shutdown** - Handles SIGINT/SIGTERM signals for clean termination
* **Progress Tracking** - Detailed logging of export progress and upload metrics

## Architecture

The application is organized into modular packages:

- `cmd/metricbucketferry/` - Application entry point with signal handling
- `internal/config/` - Configuration management from environment variables
- `internal/monitoring/` - Cloud Monitoring API client wrapper
- `internal/storage/` - Cloud Storage upload functionality
- `internal/exporter/` - Export orchestration with concurrent time series processing
- `pkg/metricspb/` - Generated Protobuf definitions
- `proto/metrics/` - Protobuf schema definitions

## Quick Start

### Prerequisites

- Go 1.26+
- Google Cloud credentials (Application Default Credentials)
- Cloud Storage bucket for exports
- Access to Cloud Monitoring data in a GCP project

### Build

```bash
make build
```

### Run

```bash
# Set required environment variables
export GOOGLE_CLOUD_PROJECT="your-project-id"  # Project to export monitoring data from
export GCS_BUCKET="your-bucket-name"           # Bucket to save exports

# Optional configuration
export GCS_PREFIX="metrics"                     # Object prefix (default: metrics)
export MAX_CONCURRENCY="16"                     # Concurrent workers (default: 4×CPU cores)

# Run the export
./metricbucketferry
```

### Test

```bash
make test
```

## Configuration

Configuration is managed through environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `GOOGLE_CLOUD_PROJECT` | **Yes** | *(none)* | GCP project ID where Cloud Monitoring metric descriptors and time series will be ingested from |
| `GCS_BUCKET` | **Yes** | *(none)* | Cloud Storage bucket where the export will be saved |
| `GCS_PREFIX` | No | `metrics` | Object prefix in the bucket |
| `MAX_CONCURRENCY` | No | Auto-detected (4×CPU cores) | Number of concurrent workers for time series export |
| `MONITORING_PAGE_SIZE` | No | `100000` | API page size for pagination (API maximum) |
| `GCS_CHUNK_SIZE_MB` | No | `32` | Cloud Storage upload chunk size in MB |

The application uses Application Default Credentials (ADC) for authentication.

## Output Format

Exported files are saved to Cloud Storage as:

```
gs://{GCS_BUCKET}/{GCS_PREFIX}/metric-descriptors/{timestamp}.pb
gs://{GCS_BUCKET}/{GCS_PREFIX}/time-series/{timestamp}/{metric_type}.pb
```

### Data Format

The exported data uses Protobuf format:
- **Source format**: Data is exported in the same Protobuf format received from the Cloud Monitoring API
- **No transformation**: Metric descriptors and time series are not modified by this application
- **Metadata wrapper**: Export metadata (project ID, timestamp, version) is added as a wrapper around the original data
- **Binary format**: Files use the `.pb` extension and contain binary-encoded Protocol Buffers

Each file contains:
- Protobuf-serialized metric descriptors or time series data (as received from API)
- Export metadata (project ID, timestamp, exporter version)

## Performance

- **Concurrency**: Auto-detected (4×CPU cores) for time series processing
- **Metric Descriptors**: ~8,300 descriptors exported to single file in ~4 seconds
- **Time Series**: Concurrent processing with exponential backoff for API rate limiting
- **Cloud Storage Optimization**: 32MB chunk size follows [GCS performance best practices](https://docs.cloud.google.com/storage/docs/request-rate) for sustained throughput

## gRPC Direct Connectivity

The application uses gRPC to communicate with Cloud Storage, which can leverage **direct connectivity** when running on Google Cloud. Direct connectivity bypasses Google Front Ends (GFEs), resulting in lower latency and connection overhead for better performance.

### Automatic Enablement

The Go Cloud Storage client library automatically enables direct connectivity when all of the following requirements are met:

- Application runs on **Compute Engine VMs**
- VM is **co-located with the bucket** (e.g., if bucket is in `us-central1`, VM can be in `us-central1-a`)
- For multi-region or dual-region buckets, VM must be in a region that makes up the multi-region
- **Firewall rules** allow:
  - IPv4 traffic to `34.126.0.0/18`
  - IPv6 traffic to `2001:4860:8040::/42`
  - Traffic to `storage.googleapis.com:443` and `directpath-pa.googleapis.com:443`
- Using **service account authentication**

No configuration or code changes are required—the client detects and enables it automatically. The application logs the direct connectivity status at startup.

For more information, see [gRPC direct connectivity documentation](https://docs.cloud.google.com/storage/docs/direct-connectivity).

## Development

### Generate Protobuf Code

```bash
make protoc
```

### Clean Build Artifacts

```bash
make clean
```

### Run Without Building

```bash
make run
```

## License

Copyright © 2026 Devil Mice Labs Ltd. All rights reserved.
