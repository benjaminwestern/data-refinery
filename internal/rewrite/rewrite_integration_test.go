package rewrite

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benjaminwestern/data-refinery/internal/input"
)

type rewriteFileSource struct {
	path string
}

func (s rewriteFileSource) Path() string { return s.path }

func (s rewriteFileSource) Open(context.Context) (io.ReadCloser, error) {
	return os.Open(s.path)
}

func rewriteJSONFamilyFixture() string {
	return strings.Join([]string{
		`{"id":"1","status":"active","active":false,"items":[{"type":"active"},{"type":"obsolete"}]}`,
		`{"id":"2","status":"deleted","active":false,"items":[{"type":"active"}]}`,
	}, "\n") + "\n"
}

func TestRunPreviewDoesNotMutateSourceFile(t *testing.T) {
	tests := []struct {
		name string
		ext  string
	}{
		{name: "json", ext: ".json"},
		{name: "ndjson", ext: ".ndjson"},
		{name: "jsonl", ext: ".jsonl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, "orders"+tt.ext)
			original := rewriteJSONFamilyFixture()
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
		})
	}
}

func TestRunApplyRewritesFileAndCreatesBackup(t *testing.T) {
	tests := []struct {
		name string
		ext  string
	}{
		{name: "json", ext: ".json"},
		{name: "ndjson", ext: ".ndjson"},
		{name: "jsonl", ext: ".jsonl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			filePath := filepath.Join(dir, "orders"+tt.ext)
			sourceData := rewriteJSONFamilyFixture()
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
		})
	}
}

func rewriteXMLFixture() string {
	return strings.Join([]string{
		"<customers>",
		"  <customer status=\"active\">",
		"    <id>1</id>",
		"    <active>false</active>",
		"    <items>",
		"      <item>",
		"        <type>active</type>",
		"      </item>",
		"      <item>",
		"        <type>obsolete</type>",
		"      </item>",
		"    </items>",
		"  </customer>",
		"  <customer status=\"deleted\">",
		"    <id>2</id>",
		"    <active>false</active>",
		"    <items>",
		"      <item>",
		"        <type>active</type>",
		"      </item>",
		"    </items>",
		"  </customer>",
		"</customers>",
		"",
	}, "\n")
}

func TestRunPreviewDoesNotMutateXMLSourceFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "customers.xml")
	original := rewriteXMLFixture()
	if err := os.WriteFile(filePath, []byte(original), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:             []string{filePath},
		Workers:           1,
		LogPath:           filepath.Join(dir, "logs"),
		XMLRecordPath:     "customers.customer",
		Mode:              ModePreview,
		TopLevelKey:       "@status",
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
		t.Fatalf("unexpected preview summary: %+v", summary)
	}

	current, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read source file: %v", err)
	}
	if string(current) != original {
		t.Fatalf("preview mode should not mutate the XML source file")
	}
}

func TestRunApplyRewritesXMLFileAndCreatesBackup(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "customers.xml")
	sourceData := rewriteXMLFixture()
	if err := os.WriteFile(filePath, []byte(sourceData), 0o644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	backupRoot := filepath.Join(dir, "backups")
	summary, err := Run(context.Background(), &Config{
		Paths:             []string{filePath},
		Workers:           1,
		LogPath:           filepath.Join(dir, "logs"),
		XMLRecordPath:     "customers.customer",
		Mode:              ModeApply,
		BackupDir:         backupRoot,
		TopLevelKey:       "@status",
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

	records, err := input.ReadAll(context.Background(), rewriteFileSource{path: filePath}, input.WithXMLRecordPath("customers.customer"))
	if err != nil {
		t.Fatalf("failed to read rewritten XML: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 rewritten XML record, got %d", len(records))
	}

	record := records[0]
	if got := record.Data["@status"]; got != "active" {
		t.Fatalf("expected surviving XML record to stay active, got %v", got)
	}
	if got := record.Data["active"]; got != "true" {
		t.Fatalf("expected active field to be updated, got %v", got)
	}

	items, ok := record.Data["items"].(map[string]any)
	if !ok {
		t.Fatalf("expected items container map, got %T", record.Data["items"])
	}
	itemValue, ok := items["item"]
	if !ok {
		t.Fatalf("expected surviving items[item] entry")
	}
	itemMap, ok := itemValue.(map[string]any)
	if !ok {
		t.Fatalf("expected a single surviving item map, got %T", itemValue)
	}
	if got := itemMap["type"]; got != "active" {
		t.Fatalf("expected surviving item type to be active, got %v", got)
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
