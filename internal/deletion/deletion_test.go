package deletion

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/search"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

func TestNewDeletionEngine(t *testing.T) {
	rules := []config.DeletionRule{
		{
			SearchTarget: "test_target",
			Action:       "delete_row",
		},
	}

	searchEngine, _ := search.NewSearchEngine([]config.SearchTarget{
		{
			Name:         "test_target",
			Type:         "direct",
			Path:         "id",
			TargetValues: []string{"123"},
		},
	})

	engine := NewDeletionEngine(rules, searchEngine)

	if engine == nil {
		t.Fatal("Expected deletion engine to be created")
	}
	if len(engine.rules) != 1 {
		t.Errorf("Expected 1 rule, got %d", len(engine.rules))
	}
	if engine.searchEngine == nil {
		t.Error("Expected search engine to be set")
	}
	if engine.outputPaths == nil {
		t.Error("Expected output paths to be initialized")
	}
	if engine.stats.MatchesByTarget == nil {
		t.Error("Expected matches by target to be initialized")
	}
}

func TestDeletionEngine_ProcessRow_DeleteRow(t *testing.T) {
	searchEngine, _ := search.NewSearchEngine([]config.SearchTarget{
		{
			Name:         "test_target",
			Type:         "direct",
			Path:         "id",
			TargetValues: []string{"123"},
		},
	})

	rules := []config.DeletionRule{
		{
			SearchTarget: "test_target",
			Action:       "delete_row",
		},
	}

	engine := NewDeletionEngine(rules, searchEngine)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	err := engine.processRow(data, location)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if engine.stats.ProcessedRows != 1 {
		t.Errorf("Expected 1 processed row, got %d", engine.stats.ProcessedRows)
	}
	if engine.stats.DeletedRows != 1 {
		t.Errorf("Expected 1 deleted row, got %d", engine.stats.DeletedRows)
	}
	if engine.stats.MatchesByTarget["test_target"] != 1 {
		t.Errorf("Expected 1 match for test_target, got %d", engine.stats.MatchesByTarget["test_target"])
	}
}

func TestDeletionEngine_ProcessRow_DeleteMatches(t *testing.T) {
	searchEngine, _ := search.NewSearchEngine([]config.SearchTarget{
		{
			Name:         "test_target",
			Type:         "direct",
			Path:         "remove_field",
			TargetValues: []string{"delete_me"},
		},
	})

	rules := []config.DeletionRule{
		{
			SearchTarget: "test_target",
			Action:       "delete_matches",
		},
	}

	engine := NewDeletionEngine(rules, searchEngine)

	data := report.JSONData{
		"id":           "123",
		"name":         "test",
		"remove_field": "delete_me",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	err := engine.processRow(data, location)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if engine.stats.ProcessedRows != 1 {
		t.Errorf("Expected 1 processed row, got %d", engine.stats.ProcessedRows)
	}
	if engine.stats.ModifiedRows != 1 {
		t.Errorf("Expected 1 modified row, got %d", engine.stats.ModifiedRows)
	}

	// Check that the field was deleted
	if _, exists := data["remove_field"]; exists {
		t.Error("Expected remove_field to be deleted")
	}
	if data["id"] != "123" {
		t.Error("Expected id field to remain unchanged")
	}
}

func TestDeletionEngine_ProcessRow_MarkForDeletion(t *testing.T) {
	searchEngine, _ := search.NewSearchEngine([]config.SearchTarget{
		{
			Name:         "test_target",
			Type:         "direct",
			Path:         "id",
			TargetValues: []string{"123"},
		},
	})

	rules := []config.DeletionRule{
		{
			SearchTarget: "test_target",
			Action:       "mark_for_deletion",
		},
	}

	engine := NewDeletionEngine(rules, searchEngine)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	err := engine.processRow(data, location)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if engine.stats.ProcessedRows != 1 {
		t.Errorf("Expected 1 processed row, got %d", engine.stats.ProcessedRows)
	}
	if engine.stats.ModifiedRows != 1 {
		t.Errorf("Expected 1 modified row, got %d", engine.stats.ModifiedRows)
	}

	// Check that deletion markers were added
	if marked, exists := data["_marked_for_deletion"]; !exists || marked != true {
		t.Error("Expected _marked_for_deletion to be true")
	}
	if reason, exists := data["_deletion_reason"]; !exists || reason != "test_target" {
		t.Error("Expected _deletion_reason to be set")
	}
	if matches, exists := data["_deletion_matches"]; !exists || matches != 1 {
		t.Error("Expected _deletion_matches to be 1")
	}
}

func TestDeletionEngine_ProcessRow_NoMatches(t *testing.T) {
	searchEngine, _ := search.NewSearchEngine([]config.SearchTarget{
		{
			Name:         "test_target",
			Type:         "direct",
			Path:         "id",
			TargetValues: []string{"999"}, // No match
		},
	})

	rules := []config.DeletionRule{
		{
			SearchTarget: "test_target",
			Action:       "delete_row",
		},
	}

	engine := NewDeletionEngine(rules, searchEngine)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	err := engine.processRow(data, location)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if engine.stats.ProcessedRows != 1 {
		t.Errorf("Expected 1 processed row, got %d", engine.stats.ProcessedRows)
	}
	if engine.stats.DeletedRows != 0 {
		t.Errorf("Expected 0 deleted rows, got %d", engine.stats.DeletedRows)
	}
	if engine.stats.ModifiedRows != 0 {
		t.Errorf("Expected 0 modified rows, got %d", engine.stats.ModifiedRows)
	}
}

func TestDeletionEngine_DeleteFromPath(t *testing.T) {
	engine := NewDeletionEngine(nil, nil)

	// Test simple key deletion
	t.Run("simple key deletion", func(t *testing.T) {
		data := report.JSONData{
			"id":   "123",
			"name": "test",
		}
		result := engine.deleteFromPath(data, "name")
		if !result {
			t.Error("Expected true, got false")
		}
		if _, exists := data["name"]; exists {
			t.Error("Expected name to be deleted")
		}
	})

	// Test nested path deletion
	t.Run("nested path deletion", func(t *testing.T) {
		data := report.JSONData{
			"user": map[string]interface{}{
				"profile": map[string]interface{}{
					"name": "test",
				},
			},
		}
		result := engine.deleteFromPath(data, "user.profile.name")
		if !result {
			t.Error("Expected true, got false")
		}
		user := data["user"].(map[string]interface{})
		profile := user["profile"].(map[string]interface{})
		if _, exists := profile["name"]; exists {
			t.Error("Expected name to be deleted from nested path")
		}
	})

	// Test non-existent key
	t.Run("non-existent key", func(t *testing.T) {
		data := report.JSONData{
			"id": "123",
		}
		result := engine.deleteFromPath(data, "missing")
		if result {
			t.Error("Expected false, got true")
		}
	})

	// Test empty path
	t.Run("empty path", func(t *testing.T) {
		data := report.JSONData{"id": "123"}
		result := engine.deleteFromPath(data, "")
		if result {
			t.Error("Expected false, got true")
		}
	})
}

func TestDeletionEngine_CopyData(t *testing.T) {
	engine := NewDeletionEngine(nil, nil)

	original := report.JSONData{
		"id":   "123",
		"name": "test",
		"nested": map[string]interface{}{
			"value": 42,
		},
		"array": []interface{}{1, 2, 3},
	}

	copied := engine.copyData(original)

	// Check that values are the same
	if copied["id"] != original["id"] {
		t.Error("Expected id to be copied")
	}
	if copied["name"] != original["name"] {
		t.Error("Expected name to be copied")
	}

	// Check that nested structures are deep copied
	originalNested := original["nested"].(map[string]interface{})
	copiedNested := copied["nested"].(map[string]interface{})

	if copiedNested["value"] != originalNested["value"] {
		t.Error("Expected nested value to be copied")
	}

	// Modify the copy and ensure original is unchanged
	copiedNested["value"] = 999
	if originalNested["value"] != 42 {
		t.Error("Expected original to be unchanged after modifying copy")
	}

	// Check array copying
	originalArray := original["array"].([]interface{})
	copiedArray := copied["array"].([]interface{})

	if len(copiedArray) != len(originalArray) {
		t.Error("Expected array to be copied")
	}

	// Modify copy array and ensure original is unchanged
	copiedArray[0] = 999
	if originalArray[0] != 1 {
		t.Error("Expected original array to be unchanged after modifying copy")
	}
}

func TestDeletionEngine_CopyValue(t *testing.T) {
	engine := NewDeletionEngine(nil, nil)

	tests := []struct {
		name  string
		value interface{}
	}{
		{"string", "test"},
		{"number", 42},
		{"boolean", true},
		{"nil", nil},
		{"map", map[string]interface{}{"key": "value"}},
		{"array", []interface{}{1, 2, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copied := engine.copyValue(tt.value)

			switch v := tt.value.(type) {
			case map[string]interface{}:
				copiedMap := copied.(map[string]interface{})
				if copiedMap["key"] != v["key"] {
					t.Error("Expected map value to be copied")
				}
				// Modify copy and ensure original is unchanged
				copiedMap["key"] = "modified"
				if v["key"] != "value" {
					t.Error("Expected original map to be unchanged")
				}
			case []interface{}:
				copiedArray := copied.([]interface{})
				if len(copiedArray) != len(v) {
					t.Error("Expected array length to match")
				}
				// Modify copy and ensure original is unchanged
				copiedArray[0] = 999
				if v[0] != 1 {
					t.Error("Expected original array to be unchanged")
				}
			default:
				if copied != tt.value {
					t.Error("Expected value to be copied")
				}
			}
		})
	}
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected int
	}{
		{"simple path", "key", 1},
		{"nested path", "user.profile.name", 3},
		{"empty path", "", 0},
		{"path with empty parts", "user..name", 2},
		{"single dot", ".", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parts := parsePath(tt.path)
			if len(parts) != tt.expected {
				t.Errorf("Expected %d parts, got %d", tt.expected, len(parts))
			}

			if tt.expected > 0 && tt.path != "" {
				// Check that parts are correctly parsed
				expectedKeys := strings.Split(tt.path, ".")
				for i, part := range parts {
					if i < len(expectedKeys) && expectedKeys[i] != "" {
						if part.key != expectedKeys[i] {
							t.Errorf("Expected key %s, got %s", expectedKeys[i], part.key)
						}
						if part.index != -1 {
							t.Errorf("Expected index -1, got %d", part.index)
						}
					}
				}
			}
		})
	}
}

func TestNewDeletionReportGenerator(t *testing.T) {
	stats := Stats{
		TotalRows:     100,
		ProcessedRows: 90,
		DeletedRows:   10,
	}

	generator := NewDeletionReportGenerator(stats)

	if generator == nil {
		t.Fatal("Expected generator to be created")
	}
	if generator.stats.TotalRows != 100 {
		t.Errorf("Expected total rows 100, got %d", generator.stats.TotalRows)
	}
	if generator.results == nil {
		t.Error("Expected results to be initialized")
	}
	if len(generator.results) != 0 {
		t.Error("Expected results to be empty initially")
	}
}

func TestDeletionReportGenerator_AddResult(t *testing.T) {
	stats := Stats{}
	generator := NewDeletionReportGenerator(stats)

	result := Result{
		Action:   "delete_row",
		RuleName: "test_rule",
	}

	generator.AddResult(result)

	if len(generator.results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(generator.results))
	}
	if generator.results[0].Action != "delete_row" {
		t.Errorf("Expected action 'delete_row', got '%s'", generator.results[0].Action)
	}
}

func TestDeletionReportGenerator_GenerateReport(t *testing.T) {
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)

	stats := Stats{
		TotalRows:     100,
		ProcessedRows: 90,
		DeletedRows:   10,
		StartTime:     startTime,
		EndTime:       endTime,
	}

	generator := NewDeletionReportGenerator(stats)

	result := Result{
		Action:   "delete_row",
		RuleName: "test_rule",
	}
	generator.AddResult(result)

	report := generator.GenerateReport()

	if report.Stats.TotalRows != 100 {
		t.Errorf("Expected total rows 100, got %d", report.Stats.TotalRows)
	}
	if len(report.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(report.Results))
	}
	if report.Duration != time.Minute {
		t.Errorf("Expected duration 1 minute, got %v", report.Duration)
	}
	if report.GeneratedAt.IsZero() {
		t.Error("Expected generated at to be set")
	}
}

func TestDeletionEngine_GetStats(t *testing.T) {
	engine := NewDeletionEngine(nil, nil)

	// Modify stats directly for testing
	engine.stats.TotalRows = 100
	engine.stats.ProcessedRows = 90
	engine.stats.DeletedRows = 10

	stats := engine.GetStats()

	if stats.TotalRows != 100 {
		t.Errorf("Expected total rows 100, got %d", stats.TotalRows)
	}
	if stats.ProcessedRows != 90 {
		t.Errorf("Expected processed rows 90, got %d", stats.ProcessedRows)
	}
	if stats.DeletedRows != 10 {
		t.Errorf("Expected deleted rows 10, got %d", stats.DeletedRows)
	}
}

func TestNewBatchDeletionProcessor(t *testing.T) {
	engine := NewDeletionEngine(nil, nil)
	workers := 4

	processor := NewBatchDeletionProcessor(engine, workers)

	if processor == nil {
		t.Fatal("Expected processor to be created")
	}
	if processor.engine != engine {
		t.Error("Expected engine to be set")
	}
	if processor.workers != workers {
		t.Errorf("Expected workers %d, got %d", workers, processor.workers)
	}
}

func TestBatchDeletionProcessor_ProcessSources_EmptyList(t *testing.T) {
	engine := NewDeletionEngine(nil, nil)
	processor := NewBatchDeletionProcessor(engine, 2)

	ctx := context.Background()
	err := processor.ProcessSources(ctx, []source.InputSource{})
	if err != nil {
		t.Errorf("Expected no error for empty sources, got %v", err)
	}
}
