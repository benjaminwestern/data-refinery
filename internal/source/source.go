package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/benjaminwestern/data-refinery/internal/safety"
	"google.golang.org/api/iterator"
)

// InputSource represents a processable input file or object.
type InputSource interface {
	Path() string
	Open(ctx context.Context) (io.ReadCloser, error)
	Dir() string
	Size() int64
}

// DiscoveryOptions controls which file types are considered processable for a
// discovery run.
type DiscoveryOptions struct {
	AllowedExtensions   []string
	AllowedContentTypes map[string]bool
	Description         string
}

// DefaultJSONDiscoveryOptions returns the discovery settings used by the
// existing analysis and rewrite workflows.
func DefaultJSONDiscoveryOptions() DiscoveryOptions {
	return DiscoveryOptions{
		AllowedExtensions: []string{".json", ".ndjson", ".jsonl"},
		AllowedContentTypes: map[string]bool{
			"application/json":           true,
			"application/x-ndjson":       true,
			"application/json-seq":       true,
			"application/jsonlines":      true,
			"application/jsonlines+json": true,
			"application/x-jsonlines":    true,
		},
		Description: "JSON, NDJSON, or JSONL",
	}
}

// DiscoverAll resolves every supplied path and returns a de-duplicated list of
// processable sources.
func DiscoverAll(ctx context.Context, paths []string) ([]InputSource, error) {
	return DiscoverAllWithOptions(ctx, paths, DefaultJSONDiscoveryOptions())
}

// DiscoverAllWithOptions resolves every supplied path and returns a
// de-duplicated list of sources that match the provided discovery rules.
func DiscoverAllWithOptions(ctx context.Context, paths []string, options DiscoveryOptions) ([]InputSource, error) {
	var uniqueSources []InputSource
	discoveredPaths := make(map[string]bool)

	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		sources, err := DiscoverWithOptions(ctx, p, options)
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
	return DiscoverWithOptions(ctx, path, DefaultJSONDiscoveryOptions())
}

// DiscoverWithOptions resolves a single local path or GCS URI into one or
// more sources using the supplied discovery rules.
func DiscoverWithOptions(ctx context.Context, path string, options DiscoveryOptions) ([]InputSource, error) {
	options = normalizeDiscoveryOptions(options)

	if strings.HasPrefix(path, "gs://") {
		return discoverGCSObjects(ctx, path, options)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	if !info.IsDir() {
		if !matchesLocalFile(path, options) {
			return nil, fmt.Errorf("local path is not a supported %s file: %s", options.Description, path)
		}
		absPath, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("could not get absolute path for %s: %w", path, err)
		}
		return []InputSource{LocalFileSource{
			filePath: absPath,
			size:     info.Size(),
			modTime:  info.ModTime(),
		}}, nil
	}
	return discoverLocalFiles(ctx, path, options)
}

// LocalFileSource implements InputSource for the local filesystem.
type LocalFileSource struct {
	filePath string
	size     int64
	modTime  time.Time
}

// Path returns the absolute file path.
func (lfs LocalFileSource) Path() string { return lfs.filePath }

// Open returns a streaming file reader.
func (lfs LocalFileSource) Open(_ context.Context) (io.ReadCloser, error) {
	file, err := os.Open(lfs.filePath)
	if err != nil {
		return nil, fmt.Errorf("open local file source %q: %w", lfs.filePath, err)
	}

	return file, nil
}

// Dir returns the containing directory for the file.
func (lfs LocalFileSource) Dir() string { return filepath.Dir(lfs.filePath) }

// Size returns the file size in bytes.
func (lfs LocalFileSource) Size() int64 { return lfs.size }

// ModTime returns the discovered last-modified time when available.
func (lfs LocalFileSource) ModTime() time.Time { return lfs.modTime }

// GCSObjectSource implements InputSource for a Google Cloud Storage object.
type GCSObjectSource struct {
	bucketName string
	objectName string
	size       int64
	generation int64
	modTime    time.Time
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
		safety.Close(client, "GCS client")
		return nil, fmt.Errorf("open GCS object reader for %s: %w", gcs.Path(), err)
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

// ModTime returns the discovered last-modified time when available.
func (gcs GCSObjectSource) ModTime() time.Time { return gcs.modTime }

type gcsReadCloser struct {
	reader *storage.Reader
	client *storage.Client
}

func (g *gcsReadCloser) Read(p []byte) (int, error) {
	n, err := g.reader.Read(p)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return n, io.EOF
		}

		return n, fmt.Errorf("read GCS object stream: %w", err)
	}

	return n, nil
}

func (g *gcsReadCloser) Close() error {
	readerErr := g.reader.Close()
	clientErr := g.client.Close()
	if readerErr != nil {
		return fmt.Errorf("close GCS object reader: %w", readerErr)
	}
	if clientErr != nil {
		return fmt.Errorf("close GCS client: %w", clientErr)
	}

	return nil
}

func discoverGCSObjects(ctx context.Context, path string, options DiscoveryOptions) ([]InputSource, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w. Ensure you are authenticated", err)
	}
	defer safety.Close(client, "GCS client")

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
			if !matchesGCSObject(attrs.Name, attrs.ContentType, options) {
				return nil, fmt.Errorf("GCS object '%s' is not a supported %s file", path, options.Description)
			}
			return []InputSource{GCSObjectSource{
				bucketName: bucketName,
				objectName: attrs.Name,
				size:       attrs.Size,
				generation: attrs.Generation,
				modTime:    attrs.Updated,
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
		if matchesGCSObject(attrs.Name, attrs.ContentType, options) {
			sources = append(sources, GCSObjectSource{
				bucketName: bucketName,
				objectName: attrs.Name,
				size:       attrs.Size,
				generation: attrs.Generation,
				modTime:    attrs.Updated,
			})
		}
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no supported %s files found in 'gs://%s' with prefix '%s'", options.Description, bucketName, prefix)
	}
	return sources, nil
}

func discoverLocalFiles(ctx context.Context, dirPath string, options DiscoveryOptions) ([]InputSource, error) {
	var sources []InputSource
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if ctx.Err() != nil {
			return context.Canceled
		}
		if err != nil {
			return err
		}
		if !info.IsDir() && matchesLocalFile(path, options) {
			absPath, err := filepath.Abs(path)
			if err != nil {
				return fmt.Errorf("could not get absolute path for %s: %w", path, err)
			}
			sources = append(sources, LocalFileSource{
				filePath: absPath,
				size:     info.Size(),
				modTime:  info.ModTime(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to walk local directory %q: %w", dirPath, err)
	}
	if len(sources) == 0 {
		return nil, fmt.Errorf("no supported %s files found in %s", options.Description, dirPath)
	}
	return sources, nil
}

func normalizeDiscoveryOptions(options DiscoveryOptions) DiscoveryOptions {
	if len(options.AllowedExtensions) == 0 && len(options.AllowedContentTypes) == 0 {
		return DefaultJSONDiscoveryOptions()
	}
	if strings.TrimSpace(options.Description) == "" {
		options.Description = "processable"
	}
	return options
}

func matchesLocalFile(path string, options DiscoveryOptions) bool {
	options = normalizeDiscoveryOptions(options)

	extension := strings.ToLower(filepath.Ext(path))
	for _, allowed := range options.AllowedExtensions {
		if strings.ToLower(strings.TrimSpace(allowed)) == extension {
			return true
		}
	}

	return false
}

func matchesGCSObject(name, contentType string, options DiscoveryOptions) bool {
	options = normalizeDiscoveryOptions(options)

	if options.AllowedContentTypes[strings.ToLower(strings.TrimSpace(contentType))] {
		return true
	}

	return matchesLocalFile(name, options)
}
