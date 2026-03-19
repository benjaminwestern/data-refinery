package source

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// InputSource represents a processable input file or object.
type InputSource interface {
	Path() string
	Open(ctx context.Context) (io.ReadCloser, error)
	Dir() string
	Size() int64
}

// DiscoverAll resolves every supplied path and returns a de-duplicated list of
// processable sources.
func DiscoverAll(ctx context.Context, paths []string) ([]InputSource, error) {
	var uniqueSources []InputSource
	discoveredPaths := make(map[string]bool)

	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		sources, err := Discover(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("error in path '%s': %w", p, err)
		}

		for _, s := range sources {
			canonicalPath := s.Path()
			if !discoveredPaths[canonicalPath] {
				discoveredPaths[canonicalPath] = true
				uniqueSources = append(uniqueSources, s)
			}
		}
	}

	if len(uniqueSources) == 0 {
		return nil, fmt.Errorf("no processable files found in any of the provided paths")
	}
	return uniqueSources, nil
}

// Discover resolves a single local path or GCS URI into one or more sources.
func Discover(ctx context.Context, path string) ([]InputSource, error) {
	if strings.HasPrefix(path, "gs://") {
		return discoverGCSObjects(ctx, path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	if !info.IsDir() {
		if !isProcessableLocalFile(path) {
			return nil, fmt.Errorf("local path is not a supported JSON/NDJSON file: %s", path)
		}
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("could not get absolute path for %s: %w", path, err)
		}
		return []InputSource{LocalFileSource{filePath: absPath, size: info.Size()}}, nil
	}
	return discoverLocalFiles(ctx, path)
}

// LocalFileSource implements InputSource for the local filesystem.
type LocalFileSource struct {
	filePath string
	size     int64
}

// Path returns the absolute file path.
func (lfs LocalFileSource) Path() string { return lfs.filePath }

// Open returns a streaming file reader.
func (lfs LocalFileSource) Open(_ context.Context) (io.ReadCloser, error) {
	return os.Open(lfs.filePath)
}

// Dir returns the containing directory for the file.
func (lfs LocalFileSource) Dir() string { return filepath.Dir(lfs.filePath) }

// Size returns the file size in bytes.
func (lfs LocalFileSource) Size() int64 { return lfs.size }

// GCSObjectSource implements InputSource for a Google Cloud Storage object.
type GCSObjectSource struct {
	bucketName string
	objectName string
	size       int64
	generation int64
}

// Path returns the object's full `gs://` URI.
func (gcs GCSObjectSource) Path() string {
	return fmt.Sprintf("gs://%s/%s", gcs.bucketName, gcs.objectName)
}

// Open returns a new streaming reader for the object.
func (gcs GCSObjectSource) Open(ctx context.Context) (io.ReadCloser, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	objectHandle := client.Bucket(gcs.bucketName).Object(gcs.objectName)
	if gcs.generation > 0 {
		objectHandle = objectHandle.If(storage.Conditions{GenerationMatch: gcs.generation})
	}

	reader, err := objectHandle.NewReader(ctx)
	if err != nil {
		client.Close()
		return nil, err
	}

	return &gcsReadCloser{reader: reader, client: client}, nil
}

// Dir returns the containing prefix for the object within its bucket.
func (gcs GCSObjectSource) Dir() string {
	if idx := strings.LastIndex(gcs.objectName, "/"); idx >= 0 {
		return fmt.Sprintf("gs://%s/%s", gcs.bucketName, gcs.objectName[:idx])
	}
	return fmt.Sprintf("gs://%s", gcs.bucketName)
}

// Size returns the object size in bytes.
func (gcs GCSObjectSource) Size() int64 {
	return gcs.size
}

// Generation returns the source object's discovered generation.
func (gcs GCSObjectSource) Generation() int64 {
	return gcs.generation
}

type gcsReadCloser struct {
	reader *storage.Reader
	client *storage.Client
}

func (g *gcsReadCloser) Read(p []byte) (int, error) {
	return g.reader.Read(p)
}

func (g *gcsReadCloser) Close() error {
	readerErr := g.reader.Close()
	clientErr := g.client.Close()
	if readerErr != nil {
		return readerErr
	}
	return clientErr
}

func discoverGCSObjects(ctx context.Context, path string) ([]InputSource, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w. Ensure you are authenticated", err)
	}
	defer client.Close()

	trimmedPath := strings.TrimPrefix(path, "gs://")
	parts := strings.SplitN(trimmedPath, "/", 2)
	bucketName := parts[0]
	var prefix string
	if len(parts) > 1 {
		prefix = parts[1]
	}
	if bucketName == "" {
		return nil, fmt.Errorf("invalid GCS path: bucket name cannot be empty in '%s'", path)
	}

	bucket := client.Bucket(bucketName)

	if _, err := bucket.Attrs(ctx); err != nil {
		return nil, fmt.Errorf("GCS bucket '%s' not found or access denied: %w", bucketName, err)
	}

	if prefix != "" {
		if attrs, err := bucket.Object(prefix).Attrs(ctx); err == nil {
			if !isProcessableGCSObject(attrs.Name, attrs.ContentType) {
				return nil, fmt.Errorf("GCS object '%s' is not a supported JSON/NDJSON file", path)
			}
			return []InputSource{GCSObjectSource{
				bucketName: bucketName,
				objectName: attrs.Name,
				size:       attrs.Size,
				generation: attrs.Generation,
			}}, nil
		}
	}

	query := &storage.Query{Prefix: prefix}
	it := bucket.Objects(ctx, query)
	var sources []InputSource

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to iterate GCS objects in bucket '%s': %w", bucketName, err)
		}
		if ctx.Err() != nil {
			return nil, context.Canceled
		}
		if strings.HasSuffix(attrs.Name, "/") {
			continue
		}
		if isProcessableGCSObject(attrs.Name, attrs.ContentType) {
			sources = append(sources, GCSObjectSource{
				bucketName: bucketName,
				objectName: attrs.Name,
				size:       attrs.Size,
				generation: attrs.Generation,
			})
		}
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no processable JSON files found in 'gs://%s' with prefix '%s'", bucketName, prefix)
	}
	return sources, nil
}

func isProcessableLocalFile(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".json") ||
		strings.HasSuffix(lower, ".ndjson") ||
		strings.HasSuffix(lower, ".jsonl")
}

func isProcessableGCSObject(name, contentType string) bool {
	allowedMimeTypes := map[string]bool{
		"application/json":           true,
		"application/x-ndjson":       true,
		"application/json-seq":       true,
		"application/jsonlines":      true,
		"application/jsonlines+json": true,
		"application/x-jsonlines":    true,
	}

	return allowedMimeTypes[contentType] || isProcessableLocalFile(name)
}

func discoverLocalFiles(ctx context.Context, dirPath string) ([]InputSource, error) {
	var sources []InputSource
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if ctx.Err() != nil {
			return context.Canceled
		}
		if err != nil {
			return err
		}
		if !info.IsDir() && (strings.HasSuffix(strings.ToLower(path), ".json") || strings.HasSuffix(strings.ToLower(path), ".ndjson") || strings.HasSuffix(strings.ToLower(path), ".jsonl")) {
			absPath, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf("could not get absolute path for %s: %w", path, err)
			}
			sources = append(sources, LocalFileSource{filePath: absPath, size: info.Size()})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk local directory %q: %w", dirPath, err)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no .json, .ndjson, or .jsonl files found in %s", dirPath)
	}
	return sources, nil
}
