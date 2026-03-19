package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverSupportsSingleLocalFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "records.ndjson")
	if err := os.WriteFile(filePath, []byte("{\"id\":1}\n"), 0o644); err != nil {
		t.Fatalf("failed to write fixture: %v", err)
	}

	sources, err := Discover(context.Background(), filePath)
	if err != nil {
		t.Fatalf("Discover returned error: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("expected exactly one source, got %d", len(sources))
	}
	if sources[0].Path() == filePath {
		return
	}

	absPath, err := filepath.Abs(filePath)
	if err != nil {
		t.Fatalf("failed to resolve abs path: %v", err)
	}
	if sources[0].Path() != absPath {
		t.Fatalf("expected source path %q, got %q", absPath, sources[0].Path())
	}
}
