package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"cloud.google.com/go/storage"
)

// Writer handles uploading data to Cloud Storage
type Writer struct {
	client    *storage.Client
	bucket    string
	prefix    string
	chunkSize int // Chunk size in bytes for streaming uploads
}

// NewWriter creates a new Cloud Storage writer
func NewWriter(ctx context.Context, bucket, prefix string, chunkSizeMB int) (*Writer, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}

	// Check if gRPC direct connectivity is supported for this bucket
	// Direct connectivity bypasses Google Front Ends for lower latency when running on GCE
	if err := storage.CheckDirectConnectivitySupported(ctx, bucket); err != nil {
		slog.Info("Direct connectivity status detected",
			"bucket", bucket,
			"status", "disabled",
			"reason", err.Error())
	} else {
		slog.Info("Direct connectivity status detected",
			"bucket", bucket,
			"status", "enabled")
	}

	return &Writer{
		client:    client,
		bucket:    bucket,
		prefix:    prefix,
		chunkSize: chunkSizeMB * 1024 * 1024, // Convert MB to bytes
	}, nil
}

// WriteMetrics uploads metric data to Cloud Storage
func (w *Writer) WriteMetrics(ctx context.Context, data []byte, filename string) error {
	objectName := filename
	if w.prefix != "" {
		objectName = w.prefix + "/" + filename
	}

	obj := w.client.Bucket(w.bucket).Object(objectName)
	writer := obj.NewWriter(ctx)

	// Set chunk size for streaming uploads
	writer.ChunkSize = w.chunkSize

	// Set content type and metadata
	writer.ContentType = "application/x-protobuf"
	writer.Metadata = map[string]string{
		"source": "metricbucketferry",
		"format": "protobuf",
	}

	// Write data
	if _, err := writer.Write(data); err != nil {
		writer.Close()
		return fmt.Errorf("write failed: %w", err)
	}

	// Close and finalize upload
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close failed: %w", err)
	}

	return nil
}

// WriteStream uploads data from a reader to Cloud Storage
func (w *Writer) WriteStream(ctx context.Context, reader io.Reader, filename string) error {
	objectName := filename
	if w.prefix != "" {
		objectName = w.prefix + "/" + filename
	}

	obj := w.client.Bucket(w.bucket).Object(objectName)
	writer := obj.NewWriter(ctx)

	// Set chunk size for streaming uploads
	writer.ChunkSize = w.chunkSize

	// Set content type and metadata
	writer.ContentType = "application/x-protobuf"
	writer.Metadata = map[string]string{
		"source": "metricbucketferry",
		"format": "protobuf",
	}

	// Copy data from reader
	if _, err := io.Copy(writer, reader); err != nil {
		writer.Close()
		return fmt.Errorf("copy failed: %w", err)
	}

	// Close and finalize upload
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close failed: %w", err)
	}

	return nil
}

// Close closes the Cloud Storage client
func (w *Writer) Close() error {
	return w.client.Close()
}
