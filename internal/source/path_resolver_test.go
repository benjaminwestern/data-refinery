package source

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildContainedGCSLocalPathRejectsTraversal(t *testing.T) {
	root := t.TempDir()

	_, err := BuildContainedGCSLocalPath(root, "bucket", "nested/../escape.json")
	if err == nil {
		t.Fatal("expected traversal object name to be rejected")
	}
}

func TestBuildContainedGCSLocalPathStaysUnderRoot(t *testing.T) {
	root := t.TempDir()

	path, err := BuildContainedGCSLocalPath(root, "bucket", "folder/data.json")
	if err != nil {
		t.Fatalf("BuildContainedGCSLocalPath returned error: %v", err)
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("failed to compute relative path: %v", err)
	}
	if strings.HasPrefix(rel, "..") {
		t.Fatalf("expected path %q to stay under root %q", path, root)
	}
	if !strings.HasSuffix(path, filepath.Join("gcs", "bucket", "folder", "data.json")) {
		t.Fatalf("unexpected contained path: %q", path)
	}
}

func TestBuildContainedLocalPathStaysUnderRoot(t *testing.T) {
	root := t.TempDir()
	sourceFile := filepath.Join(root, "fixtures", "records.ndjson")

	path, err := BuildContainedLocalPath(root, sourceFile)
	if err != nil {
		t.Fatalf("BuildContainedLocalPath returned error: %v", err)
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("failed to compute relative path: %v", err)
	}
	if strings.HasPrefix(rel, "..") {
		t.Fatalf("expected path %q to stay under root %q", path, root)
	}
	if !strings.Contains(path, string(filepath.Separator)+"local"+string(filepath.Separator)) {
		t.Fatalf("expected contained local path, got %q", path)
	}
}
