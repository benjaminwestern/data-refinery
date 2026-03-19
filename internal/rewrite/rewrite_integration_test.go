package rewrite

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunPreviewDoesNotMutateSourceFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "orders.ndjson")
	original := strings.Join([]string{
		`{"id":"1","status":"active"}`,
		`{"id":"2","status":"deleted"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filePath, []byte(original), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:          []string{filePath},
		Workers:        1,
		LogPath:        filepath.Join(dir, "logs"),
		Mode:           ModePreview,
		TopLevelKey:    "status",
		TopLevelValues: []string{"deleted"},
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if summary.FilesModified != 1 || summary.LinesDeleted != 1 {
		t.Fatalf("unexpected preview summary: %+v", summary)
	}

	current, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read source file: %v", err)
	}
	if string(current) != original {
		t.Fatalf("preview mode should not mutate the source file")
	}
}

func TestRunApplyRewritesFileAndCreatesBackup(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "orders.ndjson")
	sourceData := strings.Join([]string{
		`{"id":"1","status":"active","active":false,"items":[{"type":"active"},{"type":"obsolete"}]}`,
		`{"id":"2","status":"deleted","active":false,"items":[{"type":"active"}]}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filePath, []byte(sourceData), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	backupRoot := filepath.Join(dir, "backups")
	summary, err := Run(context.Background(), &Config{
		Paths:             []string{filePath},
		Workers:           1,
		LogPath:           filepath.Join(dir, "logs"),
		Mode:              ModeApply,
		BackupDir:         backupRoot,
		TopLevelKey:       "status",
		TopLevelValues:    []string{"deleted"},
		ArrayKey:          "items",
		ArrayDeleteKey:    "type",
		ArrayDeleteValues: []string{"obsolete"},
		UpdateKey:         "active",
		UpdateOldValue:    "false",
		UpdateNewValue:    "true",
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if summary.FilesModified != 1 || summary.LinesDeleted != 1 || summary.LinesModified != 1 || summary.LinesUpdated != 1 {
		t.Fatalf("unexpected apply summary: %+v", summary)
	}

	rewritten, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read rewritten file: %v", err)
	}
	rewrittenText := string(rewritten)
	if strings.Contains(rewrittenText, `"status":"deleted"`) {
		t.Fatalf("deleted row still present: %s", rewrittenText)
	}
	if strings.Contains(rewrittenText, `"type":"obsolete"`) {
		t.Fatalf("obsolete item still present: %s", rewrittenText)
	}
	if !strings.Contains(rewrittenText, `"active":true`) {
		t.Fatalf("expected active flag to be updated: %s", rewrittenText)
	}

	backupPath, err := buildBackupPath(summary.BackupPath, filePath)
	if err != nil {
		t.Fatalf("failed to build backup path: %v", err)
	}
	backupData, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("failed to read backup file: %v", err)
	}
	if string(backupData) != sourceData {
		t.Fatalf("backup content mismatch")
	}
}
