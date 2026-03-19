package rewrite

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestProcessLineDeletesWholeRow(t *testing.T) {
	cfg := compileConfig(&Config{
		Paths:          []string{"/tmp/data.ndjson"},
		Workers:        1,
		Mode:           ModePreview,
		TopLevelKey:    "status",
		TopLevelValues: []string{"deleted"},
	})

	result, err := processLine([]byte(`{"id":"1","status":"deleted"}`), cfg)
	if err != nil {
		t.Fatalf("processLine returned error: %v", err)
	}
	if result.Keep {
		t.Fatal("expected row to be deleted")
	}
	if !result.Deleted {
		t.Fatal("expected delete result to be marked")
	}
}

func TestProcessLineDeletesArrayMatches(t *testing.T) {
	cfg := compileConfig(&Config{
		Paths:             []string{"/tmp/data.ndjson"},
		Workers:           1,
		Mode:              ModePreview,
		ArrayKey:          "items",
		ArrayDeleteKey:    "type",
		ArrayDeleteValues: []string{"obsolete"},
	})

	result, err := processLine([]byte(`{"items":[{"id":"a","type":"active"},{"id":"b","type":"obsolete"}]}`), cfg)
	if err != nil {
		t.Fatalf("processLine returned error: %v", err)
	}
	if !result.Keep || !result.Modified {
		t.Fatalf("expected modified row to be kept, got %+v", result)
	}

	var obj map[string]any
	if err := json.Unmarshal(result.Output, &obj); err != nil {
		t.Fatalf("failed to decode result: %v", err)
	}

	items := obj["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 remaining item, got %d", len(items))
	}
}

func TestProcessLineRecursivelyUpdatesValues(t *testing.T) {
	cfg := compileConfig(&Config{
		Paths:          []string{"/tmp/data.ndjson"},
		Workers:        1,
		Mode:           ModePreview,
		UpdateKey:      "active",
		UpdateOldValue: "false",
		UpdateNewValue: "true",
	})

	result, err := processLine([]byte(`{"active":false,"nested":{"active":false},"items":[{"active":false}]}`), cfg)
	if err != nil {
		t.Fatalf("processLine returned error: %v", err)
	}
	if !result.Updated {
		t.Fatalf("expected updated row, got %+v", result)
	}

	var obj map[string]any
	if err := json.Unmarshal(result.Output, &obj); err != nil {
		t.Fatalf("failed to decode result: %v", err)
	}

	if obj["active"] != true {
		t.Fatalf("expected top-level active=true, got %#v", obj["active"])
	}

	nested := obj["nested"].(map[string]any)
	if nested["active"] != true {
		t.Fatalf("expected nested active=true, got %#v", nested["active"])
	}

	items := obj["items"].([]any)
	firstItem := items[0].(map[string]any)
	if firstItem["active"] != true {
		t.Fatalf("expected array item active=true, got %#v", firstItem["active"])
	}
}

func TestParseValuesInputCSV(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "values.csv")
	if err := os.WriteFile(csvPath, []byte("value\nfirst\nsecond\n"), 0o644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	values, err := ParseValuesInput(csvPath)
	if err != nil {
		t.Fatalf("ParseValuesInput returned error: %v", err)
	}
	if len(values) != 2 || values[0] != "first" || values[1] != "second" {
		t.Fatalf("unexpected values: %#v", values)
	}
}

func TestBuildBackupPathRejectsGCSPathTraversal(t *testing.T) {
	root := t.TempDir()

	_, err := buildBackupPath(root, "gs://bucket/folder/../escape.ndjson")
	if err == nil {
		t.Fatal("expected traversal GCS object name to be rejected")
	}
}
