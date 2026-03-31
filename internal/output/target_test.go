package output

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNewTargetLocalCommit(t *testing.T) {
	tmpDir := t.TempDir()
	targetPath := filepath.Join(tmpDir, "nested", "result.json")

	target, err := NewTarget(context.Background(), targetPath, WithTempDir(tmpDir), WithBufferSize(64))
	if err != nil {
		t.Fatalf("NewTarget failed: %v", err)
	}

	if _, err := target.Write([]byte("alpha")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := target.WriteLine([]byte("beta")); err != nil {
		t.Fatalf("WriteLine failed: %v", err)
	}
	if err := target.Commit(context.Background()); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	content, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if got, want := string(content), "alphabeta\n"; got != want {
		t.Fatalf("unexpected file content: got %q want %q", got, want)
	}

	leftovers, err := filepath.Glob(filepath.Join(tmpDir, "*.tmp"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(leftovers) != 0 {
		t.Fatalf("expected staged temp files to be removed, found %v", leftovers)
	}
}

func TestNewTargetAbortRemovesTempFile(t *testing.T) {
	tmpDir := t.TempDir()
	targetPath := filepath.Join(tmpDir, "aborted.json")

	target, err := NewTarget(context.Background(), targetPath, WithTempDir(tmpDir))
	if err != nil {
		t.Fatalf("NewTarget failed: %v", err)
	}

	if _, err := target.Write([]byte("discard me")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := target.Abort(context.Background()); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("expected no committed file at %s, got err=%v", targetPath, err)
	}
}

func TestNewTargetRemoteUsesInjectedCommitter(t *testing.T) {
	tmpDir := t.TempDir()
	targetPath := "gs://bucket/export/result.ndjson"

	originalCommitter := remoteCommitter
	defer func() {
		remoteCommitter = originalCommitter
	}()

	var (
		called      bool
		gotTempPath string
		gotTarget   string
		payload     string
	)

	remoteCommitter = func(ctx context.Context, tempPath, targetPath string) error {
		called = true
		gotTempPath = tempPath
		gotTarget = targetPath

		data, err := os.ReadFile(tempPath)
		if err != nil {
			return err
		}
		payload = string(data)
		return nil
	}

	target, err := NewTarget(context.Background(), targetPath, WithTempDir(tmpDir))
	if err != nil {
		t.Fatalf("NewTarget failed: %v", err)
	}

	if _, err := target.Write([]byte(`{"id":"123"}`)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := target.WriteLine([]byte(`{"id":"456"}`)); err != nil {
		t.Fatalf("WriteLine failed: %v", err)
	}
	if err := target.Commit(context.Background()); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !called {
		t.Fatal("expected remote committer to be invoked")
	}
	if gotTarget != targetPath {
		t.Fatalf("unexpected target path: got %q want %q", gotTarget, targetPath)
	}
	if gotTempPath == "" {
		t.Fatal("expected temp path to be passed to remote committer")
	}
	if got, want := payload, "{\"id\":\"123\"}{\"id\":\"456\"}\n"; got != want {
		t.Fatalf("unexpected staged payload: got %q want %q", got, want)
	}
	if _, err := os.Stat(gotTempPath); !os.IsNotExist(err) {
		t.Fatalf("expected staged file %s to be removed, got err=%v", gotTempPath, err)
	}
}
