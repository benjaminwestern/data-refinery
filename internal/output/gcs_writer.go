// internal/output/gcs_writer.go
package output

import (
	"context"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
)

// GCSWriter provides streaming write capabilities for Google Cloud Storage objects
type GCSWriter struct {
	client     *storage.Client
	bucket     string
	objectName string
	writer     *storage.Writer
	ctx        context.Context
}

// NewGCSWriter creates a new GCS writer for streaming uploads
func NewGCSWriter(ctx context.Context, gcsPath string) (*GCSWriter, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bucket, objectName, err := parseGCSPath(gcsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GCS path: %w", err)
	}

	return &GCSWriter{
		client:     client,
		bucket:     bucket,
		objectName: objectName,
		ctx:        ctx,
	}, nil
}

// Open initializes the streaming writer to GCS
func (w *GCSWriter) Open() error {
	bucketHandle := w.client.Bucket(w.bucket)
	objectHandle := bucketHandle.Object(w.objectName)

	w.writer = objectHandle.NewWriter(w.ctx)

	// Set appropriate metadata for JSON/NDJSON files
	if strings.HasSuffix(w.objectName, ".ndjson") || strings.HasSuffix(w.objectName, ".jsonl") {
		w.writer.ContentType = "application/x-ndjson"
	} else if strings.HasSuffix(w.objectName, ".json") {
		w.writer.ContentType = "application/json"
	}

	return nil
}

// Write writes data to the GCS object
func (w *GCSWriter) Write(data []byte) (int, error) {
	if w.writer == nil {
		return 0, fmt.Errorf("writer not opened")
	}
	return w.writer.Write(data)
}

// WriteString writes a string to the GCS object
func (w *GCSWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// WriteLine writes a line with newline to the GCS object (useful for NDJSON)
func (w *GCSWriter) WriteLine(line string) (int, error) {
	return w.WriteString(line + "\n")
}

// Close closes the writer and finalizes the upload
func (w *GCSWriter) Close() error {
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			return fmt.Errorf("failed to close GCS writer: %w", err)
		}
		w.writer = nil
	}

	if w.client != nil {
		if err := w.client.Close(); err != nil {
			return fmt.Errorf("failed to close GCS client: %w", err)
		}
		w.client = nil
	}

	return nil
}

// GetObjectInfo returns information about the uploaded object
func (w *GCSWriter) GetObjectInfo() (*storage.ObjectAttrs, error) {
	if w.client == nil {
		return nil, fmt.Errorf("client is closed")
	}

	bucketHandle := w.client.Bucket(w.bucket)
	objectHandle := bucketHandle.Object(w.objectName)

	return objectHandle.Attrs(w.ctx)
}

// StreamCopy copies data from a reader to GCS with streaming
func (w *GCSWriter) StreamCopy(reader io.Reader) (int64, error) {
	if w.writer == nil {
		return 0, fmt.Errorf("writer not opened")
	}

	return io.Copy(w.writer, reader)
}

// parseGCSPath extracts bucket and object name from gs:// path
func parseGCSPath(gcsPath string) (bucket, objectName string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid GCS path, must start with gs://")
	}

	trimmedPath := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(trimmedPath, "/", 2)

	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid GCS path, must include bucket and object name")
	}

	bucket = parts[0]
	objectName = parts[1]

	if bucket == "" || objectName == "" {
		return "", "", fmt.Errorf("bucket and object name cannot be empty")
	}

	return bucket, objectName, nil
}

// GCSStreamProcessor handles streaming operations for GCS files
type GCSStreamProcessor struct {
	ctx context.Context
}

// NewGCSStreamProcessor creates a new GCS stream processor
func NewGCSStreamProcessor(ctx context.Context) *GCSStreamProcessor {
	return &GCSStreamProcessor{ctx: ctx}
}

// ProcessFile processes a GCS file with streaming read-modify-write
func (p *GCSStreamProcessor) ProcessFile(
	sourcePath string,
	targetPath string,
	processor func(line []byte) ([]byte, bool), // returns modified line and whether to keep it
) error {
	client, err := storage.NewClient(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}
	defer client.Close()

	// Open source for reading
	sourceBucket, sourceObject, err := parseGCSPath(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to parse source path: %w", err)
	}

	sourceReader, err := client.Bucket(sourceBucket).Object(sourceObject).NewReader(p.ctx)
	if err != nil {
		return fmt.Errorf("failed to open source object: %w", err)
	}
	defer sourceReader.Close()

	// Open target for writing
	writer, err := NewGCSWriter(p.ctx, targetPath)
	if err != nil {
		return fmt.Errorf("failed to create target writer: %w", err)
	}
	defer writer.Close()

	if err := writer.Open(); err != nil {
		return fmt.Errorf("failed to open target writer: %w", err)
	}

	// Process the file line by line
	return processStreamLineByLine(sourceReader, writer, processor)
}

// processStreamLineByLine processes a stream line by line applying the processor function
func processStreamLineByLine(
	reader io.Reader,
	writer *GCSWriter,
	processor func(line []byte) ([]byte, bool),
) error {
	const bufferSize = 4 * 1024 * 1024 // 4MB buffer
	buffer := make([]byte, bufferSize)
	var lineBuffer []byte

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			for i := 0; i < n; i++ {
				if buffer[i] == '\n' {
					// Process complete line
					if processedLine, keep := processor(lineBuffer); keep {
						if _, writeErr := writer.WriteLine(string(processedLine)); writeErr != nil {
							return fmt.Errorf("failed to write line: %w", writeErr)
						}
					}
					lineBuffer = lineBuffer[:0] // Reset buffer
				} else {
					lineBuffer = append(lineBuffer, buffer[i])
				}
			}
		}

		if err == io.EOF {
			// Process final line if it doesn't end with newline
			if len(lineBuffer) > 0 {
				if processedLine, keep := processor(lineBuffer); keep {
					if _, writeErr := writer.WriteString(string(processedLine)); writeErr != nil {
						return fmt.Errorf("failed to write final line: %w", writeErr)
					}
				}
			}
			break
		}

		if err != nil {
			return fmt.Errorf("failed to read from source: %w", err)
		}
	}

	return nil
}
