package search

import (
	"testing"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
)

func TestNewSearchEngine(t *testing.T) {
	targets := []config.SearchTarget{
		{
			Name:          "test_direct",
			Type:          "direct",
			Path:          "id",
			TargetValues:  []string{"123", "456"},
			CaseSensitive: true,
		},
		{
			Name:          "test_array",
			Type:          "nested_array",
			Path:          "items[*].id",
			TargetValues:  []string{"789"},
			CaseSensitive: false,
		},
	}

	engine, err := NewSearchEngine(targets)
	if err != nil {
		t.Fatalf("Expected no error creating search engine, got %v", err)
	}

	if engine == nil {
		t.Fatal("Expected search engine to be created")
	}
	if len(engine.targets) != 2 {
		t.Errorf("Expected 2 targets, got %d", len(engine.targets))
	}
	if len(engine.matchers) != 2 {
		t.Errorf("Expected 2 matchers, got %d", len(engine.matchers))
	}
	if len(engine.results) != 2 {
		t.Errorf("Expected 2 result containers, got %d", len(engine.results))
	}
}

func TestNewSearchEngine_InvalidMatcher(t *testing.T) {
	targets := []config.SearchTarget{
		{
			Name:          "test_invalid",
			Type:          "invalid_type",
			Path:          "id",
			TargetValues:  []string{"123"},
			CaseSensitive: true,
		},
	}

	_, err := NewSearchEngine(targets)
	if err == nil {
		t.Fatal("Expected error for invalid matcher type")
	}
}

func TestCreateMatcher(t *testing.T) {
	tests := []struct {
		name        string
		matcherType string
		expectError bool
	}{
		{"direct matcher", "direct", false},
		{"nested_array matcher", "nested_array", false},
		{"nested_object matcher", "nested_object", false},
		{"jsonpath matcher", "jsonpath", false},
		{"invalid matcher", "invalid", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			matcher, err := createMatcher(tt.matcherType)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error for invalid matcher type")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
				if matcher == nil {
					t.Error("Expected matcher to be created")
				}
			}
		})
	}
}

func TestDirectMatcher_Match(t *testing.T) {
	matcher := &directMatcher{}

	target := config.SearchTarget{
		Name:          "test_id",
		Type:          "direct",
		Path:          "id",
		TargetValues:  []string{"123", "456"},
		CaseSensitive: true,
	}

	// Test data with matching ID
	data := report.JSONData{
		"id":   "123",
		"name": "test",
	}

	matches := matcher.Match(data, target)
	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}
	if matches[0].MatchedValue != "123" {
		t.Errorf("Expected matched value '123', got '%s'", matches[0].MatchedValue)
	}
	if matches[0].Path != "id" {
		t.Errorf("Expected path 'id', got '%s'", matches[0].Path)
	}
	if !matches[0].Found {
		t.Error("Expected match to be found")
	}

	// Test data with non-matching ID
	data2 := report.JSONData{
		"id":   "999",
		"name": "test",
	}

	matches2 := matcher.Match(data2, target)
	if len(matches2) != 0 {
		t.Errorf("Expected 0 matches, got %d", len(matches2))
	}

	// Test data with missing field
	data3 := report.JSONData{
		"name": "test",
	}

	matches3 := matcher.Match(data3, target)
	if len(matches3) != 0 {
		t.Errorf("Expected 0 matches for missing field, got %d", len(matches3))
	}
}

func TestDirectMatcher_CaseInsensitive(t *testing.T) {
	matcher := &directMatcher{}

	target := config.SearchTarget{
		Name:          "test_name",
		Type:          "direct",
		Path:          "name",
		TargetValues:  []string{"TEST"},
		CaseSensitive: false,
	}

	data := report.JSONData{
		"name": "test",
	}

	matches := matcher.Match(data, target)
	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
		return // Avoid panic
	}
	if matches[0].MatchedValue != "test" {
		t.Errorf("Expected matched value 'test', got '%s'", matches[0].MatchedValue)
	}
	if matches[0].Path != "name" {
		t.Errorf("Expected path 'name', got '%s'", matches[0].Path)
	}
}

func TestNestedArrayMatcher_Match(t *testing.T) {
	matcher := &nestedArrayMatcher{}

	target := config.SearchTarget{
		Name:          "test_items",
		Type:          "nested_array",
		Path:          "items[*].id",
		TargetValues:  []string{"123", "456"},
		CaseSensitive: true,
	}

	data := report.JSONData{
		"items": []any{
			map[string]any{
				"id":   "123",
				"name": "item1",
			},
			map[string]any{
				"id":   "789",
				"name": "item2",
			},
			map[string]any{
				"id":   "456",
				"name": "item3",
			},
		},
	}

	matches := matcher.Match(data, target)
	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}

	// Check paths are correctly formatted
	expectedPaths := []string{"items[0].id", "items[2].id"}
	for i, match := range matches {
		if match.Path != expectedPaths[i] {
			t.Errorf("Expected path '%s', got '%s'", expectedPaths[i], match.Path)
		}
	}
}

func TestNestedArrayMatcher_NoArray(t *testing.T) {
	matcher := &nestedArrayMatcher{}

	target := config.SearchTarget{
		Name:          "test_items",
		Type:          "nested_array",
		Path:          "items[*].id",
		TargetValues:  []string{"123"},
		CaseSensitive: true,
	}

	// Data without array field
	data := report.JSONData{
		"other": "value",
	}

	matches := matcher.Match(data, target)
	if len(matches) != 0 {
		t.Errorf("Expected 0 matches for missing array, got %d", len(matches))
	}

	// Data with non-array field
	data2 := report.JSONData{
		"items": "not an array",
	}

	matches2 := matcher.Match(data2, target)
	if len(matches2) != 0 {
		t.Errorf("Expected 0 matches for non-array, got %d", len(matches2))
	}
}

func TestNestedObjectMatcher_Match(t *testing.T) {
	matcher := &nestedObjectMatcher{}

	target := config.SearchTarget{
		Name:          "test_nested",
		Type:          "nested_object",
		Path:          "user.profile.id",
		TargetValues:  []string{"123"},
		CaseSensitive: true,
	}

	data := report.JSONData{
		"user": map[string]any{
			"profile": map[string]any{
				"id":   "123",
				"name": "John",
			},
		},
	}

	matches := matcher.Match(data, target)
	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
		// Debug: Let's print the data structure
		t.Logf("Data structure: %+v", data)
		return // Avoid panic
	}
	if matches[0].MatchedValue != "123" {
		t.Errorf("Expected matched value '123', got '%s'", matches[0].MatchedValue)
	}
	if matches[0].Path != "user.profile.id" {
		t.Errorf("Expected path 'user.profile.id', got '%s'", matches[0].Path)
	}
}

func TestNestedObjectMatcher_MissingPath(t *testing.T) {
	matcher := &nestedObjectMatcher{}

	target := config.SearchTarget{
		Name:          "test_nested",
		Type:          "nested_object",
		Path:          "user.profile.id",
		TargetValues:  []string{"123"},
		CaseSensitive: true,
	}

	// Data with partial path
	data := report.JSONData{
		"user": map[string]any{
			"name": "John",
		},
	}

	matches := matcher.Match(data, target)
	if len(matches) != 0 {
		t.Errorf("Expected 0 matches for missing path, got %d", len(matches))
	}
}

func TestJSONPathMatcher_SimpleMatch(t *testing.T) {
	matcher := &jsonPathMatcher{}

	target := config.SearchTarget{
		Name:          "test_path",
		Type:          "jsonpath",
		Path:          "user.id",
		TargetValues:  []string{"123"},
		CaseSensitive: true,
	}

	data := report.JSONData{
		"user": map[string]any{
			"id":   "123",
			"name": "John",
		},
	}

	matches := matcher.Match(data, target)
	if len(matches) != 1 {
		t.Errorf("Expected 1 match, got %d", len(matches))
	}
	if matches[0].MatchedValue != "123" {
		t.Errorf("Expected matched value '123', got '%s'", matches[0].MatchedValue)
	}
}

func TestJSONPathMatcher_WildcardMatch(t *testing.T) {
	matcher := &jsonPathMatcher{}

	target := config.SearchTarget{
		Name:          "test_wildcard",
		Type:          "jsonpath",
		Path:          "*.id",
		TargetValues:  []string{"123"},
		CaseSensitive: true,
	}

	data := report.JSONData{
		"user": map[string]any{
			"id":   "123",
			"name": "John",
		},
		"admin": map[string]any{
			"id":   "456",
			"name": "Admin",
		},
	}

	matches := matcher.Match(data, target)
	if len(matches) != 1 {
		t.Errorf("Expected 1 match for wildcard, got %d", len(matches))
	}
}

func TestSearchEngine_Search(t *testing.T) {
	targets := []config.SearchTarget{
		{
			Name:          "test_id",
			Type:          "direct",
			Path:          "id",
			TargetValues:  []string{"123"},
			CaseSensitive: true,
		},
	}

	engine, err := NewSearchEngine(targets)
	if err != nil {
		t.Fatalf("Failed to create search engine: %v", err)
	}

	data := report.JSONData{
		"id":   "123",
		"name": "test",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	engine.Search(data, location)

	results := engine.GetResults()
	if results.Summary.TotalMatches != 1 {
		t.Errorf("Expected 1 total match, got %d", results.Summary.TotalMatches)
	}
	if results.Summary.MatchesByTarget["test_id"] != 1 {
		t.Errorf("Expected 1 match for test_id, got %d", results.Summary.MatchesByTarget["test_id"])
	}

	targetResults := engine.GetResultsForTarget("test_id")
	if len(targetResults) != 1 {
		t.Errorf("Expected 1 result for test_id, got %d", len(targetResults))
	}
	if targetResults[0].Location.FilePath != "/test/file.json" {
		t.Errorf("Expected file path '/test/file.json', got '%s'", targetResults[0].Location.FilePath)
	}
	if targetResults[0].Target != "test_id" {
		t.Errorf("Expected target 'test_id', got '%s'", targetResults[0].Target)
	}
}

func TestSearchEngine_GetResultsForTarget(t *testing.T) {
	targets := []config.SearchTarget{
		{
			Name:          "test_id",
			Type:          "direct",
			Path:          "id",
			TargetValues:  []string{"123"},
			CaseSensitive: true,
		},
	}

	engine, err := NewSearchEngine(targets)
	if err != nil {
		t.Fatalf("Failed to create search engine: %v", err)
	}

	// Test with unknown target
	results := engine.GetResultsForTarget("unknown")
	if results != nil {
		t.Error("Expected nil results for unknown target")
	}

	// Test with known target (empty results)
	results = engine.GetResultsForTarget("test_id")
	if results == nil {
		t.Error("Expected empty slice for known target")
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestSearchEngine_ClearResults(t *testing.T) {
	targets := []config.SearchTarget{
		{
			Name:          "test_id",
			Type:          "direct",
			Path:          "id",
			TargetValues:  []string{"123"},
			CaseSensitive: true,
		},
	}

	engine, err := NewSearchEngine(targets)
	if err != nil {
		t.Fatalf("Failed to create search engine: %v", err)
	}

	// Add some results
	data := report.JSONData{
		"id": "123",
	}
	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}
	engine.Search(data, location)

	// Verify results exist
	results := engine.GetResults()
	if results.Summary.TotalMatches != 1 {
		t.Errorf("Expected 1 match before clear, got %d", results.Summary.TotalMatches)
	}

	// Clear results
	engine.ClearResults()

	// Verify results are cleared
	results = engine.GetResults()
	if results.Summary.TotalMatches != 0 {
		t.Errorf("Expected 0 matches after clear, got %d", results.Summary.TotalMatches)
	}
}

func TestUtilityMatcher_MatchRegex(t *testing.T) {
	matcher := &UtilityMatcher{}

	// Test valid regex
	match, err := matcher.MatchRegex("test123", `\d+`, true)
	if err != nil {
		t.Errorf("Expected no error for valid regex, got %v", err)
	}
	if !match {
		t.Error("Expected regex to match")
	}

	// Test case insensitive
	match, err = matcher.MatchRegex("TEST", "test", false)
	if err != nil {
		t.Errorf("Expected no error for case insensitive regex, got %v", err)
	}
	if !match {
		t.Error("Expected case insensitive regex to match")
	}

	// Test invalid regex
	_, err = matcher.MatchRegex("test", "[", true)
	if err == nil {
		t.Error("Expected error for invalid regex")
	}
}

func TestUtilityMatcher_MatchContains(t *testing.T) {
	matcher := &UtilityMatcher{}

	// Test case sensitive
	if !matcher.MatchContains("hello world", "world", true) {
		t.Error("Expected substring match")
	}
	if matcher.MatchContains("hello world", "WORLD", true) {
		t.Error("Expected no match for case sensitive")
	}

	// Test case insensitive
	if !matcher.MatchContains("hello world", "WORLD", false) {
		t.Error("Expected case insensitive substring match")
	}
}

func TestUtilityMatcher_MatchType(t *testing.T) {
	matcher := &UtilityMatcher{}

	tests := []struct {
		name     string
		value    any
		expected string
		want     bool
	}{
		{"string", "hello", "string", true},
		{"string wrong", "hello", "int", false},
		{"int", 42, "int", true},
		{"int as integer", 42, "integer", true},
		{"float", 3.14, "float", true},
		{"float as number", 3.14, "number", true},
		{"bool", true, "bool", true},
		{"bool as boolean", true, "boolean", true},
		{"slice", []int{1, 2, 3}, "slice", true},
		{"slice as array", []int{1, 2, 3}, "array", true},
		{"map", map[string]int{"a": 1}, "map", true},
		{"map as object", map[string]int{"a": 1}, "object", true},
		{"nil", nil, "nil", true},
		{"nil wrong", nil, "string", false},
		{"unknown type", "test", "unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matcher.MatchType(tt.value, tt.expected)
			if result != tt.want {
				t.Errorf("Expected %v for MatchType(%v, %s), got %v", tt.want, tt.value, tt.expected, result)
			}
		})
	}
}

func TestMatchValue_CaseSensitive(t *testing.T) {
	dm := &directMatcher{}

	if !dm.matchValue("Test", "Test", true) {
		t.Error("Expected exact match to succeed")
	}
	if dm.matchValue("Test", "test", true) {
		t.Error("Expected case sensitive match to fail")
	}
	if !dm.matchValue("Test", "test", false) {
		t.Error("Expected case insensitive match to succeed")
	}
}

func TestJSONPathMatcher_EvaluateSimplePath(t *testing.T) {
	matcher := &jsonPathMatcher{}

	data := report.JSONData{
		"user": map[string]any{
			"id":   "123",
			"name": "John",
		},
	}

	results := matcher.evaluateSimplePath(data, "user.id")
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
	if results[0].Path != "user.id" {
		t.Errorf("Expected path 'user.id', got '%s'", results[0].Path)
	}
	if results[0].Value != "123" {
		t.Errorf("Expected value '123', got '%v'", results[0].Value)
	}

	// Test non-existent path
	results2 := matcher.evaluateSimplePath(data, "user.nonexistent")
	if len(results2) != 0 {
		t.Errorf("Expected 0 results for non-existent path, got %d", len(results2))
	}
}
