package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
)

func TestNewSchemaAnalyzer(t *testing.T) {
	cfg := config.SchemaDiscoveryConfig{
		Enabled:       true,
		SamplePercent: 0.1,
		MaxDepth:      5,
		MaxSamples:    1000,
		OutputFormats: []string{"json"},
	}

	analyzer := NewSchemaAnalyzer(cfg)

	if analyzer == nil {
		t.Fatal("Expected analyzer to be created")
	}
	if analyzer.globalSchema == nil {
		t.Error("Expected global schema to be initialized")
	}
	if analyzer.folderSchemas == nil {
		t.Error("Expected folder schemas to be initialized")
	}
	if analyzer.config.SamplePercent != 0.1 {
		t.Errorf("Expected sample percent 0.1, got %f", analyzer.config.SamplePercent)
	}
}

func TestNewSchema(t *testing.T) {
	schema := newSchema()

	if schema == nil {
		t.Fatal("Expected schema to be created")
	}
	if schema.Fields == nil {
		t.Error("Expected fields to be initialized")
	}
	if schema.TotalSamples != 0 {
		t.Errorf("Expected total samples to be 0, got %d", schema.TotalSamples)
	}
}

func TestNewFieldSchema(t *testing.T) {
	path := "test.field"
	field := newFieldSchema(path)

	if field == nil {
		t.Fatal("Expected field schema to be created")
	}
	if field.Path != path {
		t.Errorf("Expected path '%s', got '%s'", path, field.Path)
	}
	if field.Examples == nil {
		t.Error("Expected examples to be initialized")
	}
	if field.UniqueValues == nil {
		t.Error("Expected unique values to be initialized")
	}
	if field.SubFields == nil {
		t.Error("Expected sub fields to be initialized")
	}
}

func TestGetValueType(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{})

	tests := []struct {
		name     string
		value    any
		expected string
	}{
		{"nil", nil, "null"},
		{"string", "test", "string"},
		{"integer", 42, "integer"},
		{"float", 3.14, "number"},
		{"boolean", true, "boolean"},
		{"array", []any{1, 2, 3}, "array"},
		{"object", map[string]any{"key": "value"}, "object"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.getValueType(tt.value)
			if result != tt.expected {
				t.Errorf("Expected type '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestBuildPath(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{})

	tests := []struct {
		name     string
		prefix   string
		key      string
		expected string
	}{
		{"empty prefix", "", "key", "key"},
		{"with prefix", "root", "key", "root.key"},
		{"nested path", "root.nested", "key", "root.nested.key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzer.buildPath(tt.prefix, tt.key)
			if result != tt.expected {
				t.Errorf("Expected path '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestShouldSample(t *testing.T) {
	// Test max samples limit
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		SamplePercent: 1.0, // 100%
		MaxSamples:    5,
	})

	// Should sample first 5 times
	for i := 0; i < 5; i++ {
		analyzer.sampledRows = int64(i)
		if !analyzer.shouldSample() {
			t.Errorf("Should sample at iteration %d", i)
		}
	}

	// Should not sample after max samples
	analyzer.sampledRows = 5
	if analyzer.shouldSample() {
		t.Error("Should not sample after max samples reached")
	}

	// Test sample percent (this is probabilistic, so we test with 0%)
	analyzer2 := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		SamplePercent: 0.0,
		MaxSamples:    1000,
	})

	if analyzer2.shouldSample() {
		t.Error("Should not sample with 0% sample rate")
	}
}

func TestAnalyzeRow(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		Enabled:       true,
		SamplePercent: 1.0, // 100% to ensure sampling
		MaxDepth:      5,
		MaxSamples:    1000,
		GroupByFolder: true,
	})

	testData := report.JSONData{
		"id":     "123",
		"name":   "Test",
		"age":    30,
		"active": true,
		"metadata": map[string]any{
			"created": "2023-01-01",
			"version": 1,
		},
	}

	filePath := "/data/users/user1.json"
	analyzer.AnalyzeRow(testData, filePath)

	if analyzer.totalRows != 1 {
		t.Errorf("Expected total rows 1, got %d", analyzer.totalRows)
	}
	if analyzer.sampledRows != 1 {
		t.Errorf("Expected sampled rows 1, got %d", analyzer.sampledRows)
	}

	// Check global schema
	if analyzer.globalSchema.TotalSamples != 1 {
		t.Errorf("Expected global schema total samples 1, got %d", analyzer.globalSchema.TotalSamples)
	}

	// Check that fields were analyzed
	if len(analyzer.globalSchema.Fields) == 0 {
		t.Error("Expected fields to be analyzed in global schema")
	}

	// Check folder schema
	folder := filepath.Dir(filePath)
	if _, exists := analyzer.folderSchemas[folder]; !exists {
		t.Error("Expected folder schema to be created")
	}
}

func TestUpdateFieldSchema(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		MaxDepth: 5,
	})

	field := newFieldSchema("test.field")

	// Test string value
	analyzer.updateFieldSchema(field, "test string", 0)
	if field.Type != "string" {
		t.Errorf("Expected type 'string', got '%s'", field.Type)
	}
	if field.Occurrences != 1 {
		t.Errorf("Expected occurrences 1, got %d", field.Occurrences)
	}
	if field.MinLength != 11 || field.MaxLength != 11 {
		t.Errorf("Expected min/max length 11, got %d/%d", field.MinLength, field.MaxLength)
	}
	if len(field.Examples) != 1 {
		t.Errorf("Expected 1 example, got %d", len(field.Examples))
	}

	// Test null value
	analyzer.updateFieldSchema(field, nil, 0)
	if !field.IsNullable {
		t.Error("Expected field to be nullable")
	}
	if field.Type != "mixed" {
		t.Errorf("Expected type 'mixed', got '%s'", field.Type)
	}

	// Test array value
	arrayField := newFieldSchema("test.array")
	arrayValue := []any{"item1", "item2"}
	analyzer.updateFieldSchema(arrayField, arrayValue, 0)
	if arrayField.Type != "array" {
		t.Errorf("Expected type 'array', got '%s'", arrayField.Type)
	}
	if arrayField.ArrayElementType != "string" {
		t.Errorf("Expected array element type 'string', got '%s'", arrayField.ArrayElementType)
	}
}

func TestUpdateFieldSchemaWithObject(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		MaxDepth: 5,
	})

	field := newFieldSchema("test.object")
	objectValue := map[string]any{
		"nested1": "value1",
		"nested2": 42,
	}

	analyzer.updateFieldSchema(field, objectValue, 0)

	if field.Type != "object" {
		t.Errorf("Expected type 'object', got '%s'", field.Type)
	}
	if len(field.SubFields) != 2 {
		t.Errorf("Expected 2 sub fields, got %d", len(field.SubFields))
	}

	// Check sub fields
	nested1, exists := field.SubFields["nested1"]
	if !exists {
		t.Error("Expected nested1 field to exist")
	} else if nested1.Type != "string" {
		t.Errorf("Expected nested1 type 'string', got '%s'", nested1.Type)
	}

	nested2, exists := field.SubFields["nested2"]
	if !exists {
		t.Error("Expected nested2 field to exist")
	} else if nested2.Type != "integer" {
		t.Errorf("Expected nested2 type 'integer', got '%s'", nested2.Type)
	}
}

func TestGenerateReport(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		Enabled:       true,
		SamplePercent: 1.0,
		MaxDepth:      5,
		MaxSamples:    1000,
		GroupByFolder: true,
	})

	testData := report.JSONData{
		"id":   "123",
		"name": "Test",
	}

	filePath := "/data/users/user1.json"
	analyzer.AnalyzeRow(testData, filePath)

	report := analyzer.GenerateReport()

	if report == nil {
		t.Fatal("Expected report to be generated")
	}
	if report.TotalRows != 1 {
		t.Errorf("Expected total rows 1, got %d", report.TotalRows)
	}
	if report.SampledRows != 1 {
		t.Errorf("Expected sampled rows 1, got %d", report.SampledRows)
	}
	if report.SampleRate != 1.0 {
		t.Errorf("Expected sample rate 1.0, got %f", report.SampleRate)
	}
	if report.GlobalSchema == nil {
		t.Error("Expected global schema in report")
	}

	// Check that percentages are calculated
	for _, field := range report.GlobalSchema.Fields {
		if field.Percentage == 0 {
			t.Errorf("Expected non-zero percentage for field %s", field.Path)
		}
	}
}

func TestSaveJSONReport(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		Enabled:       true,
		SamplePercent: 1.0,
		MaxDepth:      5,
		MaxSamples:    1000,
	})

	testData := report.JSONData{
		"id":   "123",
		"name": "Test",
	}

	analyzer.AnalyzeRow(testData, "/data/test.json")

	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "schema.json")

	reportData := analyzer.GenerateReport()
	err := analyzer.saveJSONReport(reportData, filename)
	if err != nil {
		t.Fatalf("Expected no error saving JSON report, got %v", err)
	}

	// Check file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Error("Expected JSON file to be created")
	}

	// Check file content
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read JSON file: %v", err)
	}

	var savedReport SchemaReport
	if err := json.Unmarshal(data, &savedReport); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	if savedReport.TotalRows != 1 {
		t.Errorf("Expected total rows 1 in saved report, got %d", savedReport.TotalRows)
	}
}

func TestSaveCSVReport(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		Enabled:       true,
		SamplePercent: 1.0,
		MaxDepth:      5,
		MaxSamples:    1000,
	})

	testData := report.JSONData{
		"id":   "123",
		"name": "Test",
	}

	analyzer.AnalyzeRow(testData, "/data/test.json")

	tmpDir := t.TempDir()
	filename := filepath.Join(tmpDir, "schema.csv")

	reportData := analyzer.GenerateReport()
	err := analyzer.saveCSVReport(reportData, filename)
	if err != nil {
		t.Fatalf("Expected no error saving CSV report, got %v", err)
	}

	// Check file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Error("Expected CSV file to be created")
	}

	// Check file content
	data, err := os.ReadFile(filename)
	if err != nil {
		t.Fatalf("Failed to read CSV file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "Path,Type,Occurrences") {
		t.Error("Expected CSV header to be present")
	}
	if !strings.Contains(content, "global:id") {
		t.Error("Expected field data to be present")
	}
}

func TestSaveReport(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		Enabled:       true,
		SamplePercent: 1.0,
		MaxDepth:      5,
		MaxSamples:    1000,
	})

	testData := report.JSONData{
		"id":   "123",
		"name": "Test",
	}

	analyzer.AnalyzeRow(testData, "/data/test.json")

	tmpDir := t.TempDir()
	baseFilename := filepath.Join(tmpDir, "schema")

	err := analyzer.SaveReport(baseFilename, []string{"json", "csv"})
	if err != nil {
		t.Fatalf("Expected no error saving report, got %v", err)
	}

	// Check JSON file exists
	jsonFile := baseFilename + ".json"
	if _, err := os.Stat(jsonFile); os.IsNotExist(err) {
		t.Error("Expected JSON file to be created")
	}

	// Check CSV file exists
	csvFile := baseFilename + ".csv"
	if _, err := os.Stat(csvFile); os.IsNotExist(err) {
		t.Error("Expected CSV file to be created")
	}
}

func TestEnsureFolderSchema(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{})

	folder := "/data/users"

	// First call should create schema
	schema1 := analyzer.ensureFolderSchema(folder)
	if schema1 == nil {
		t.Error("Expected schema to be created")
	}

	// Second call should return existing schema
	schema2 := analyzer.ensureFolderSchema(folder)
	if schema1 != schema2 {
		t.Error("Expected same schema instance to be returned")
	}

	// Check that schema was stored
	if len(analyzer.folderSchemas) != 1 {
		t.Errorf("Expected 1 folder schema, got %d", len(analyzer.folderSchemas))
	}
}

func TestCalculatePercentages(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{})

	schema := newSchema()
	schema.TotalSamples = 100

	field1 := newFieldSchema("field1")
	field1.Occurrences = 80
	schema.Fields["field1"] = field1

	field2 := newFieldSchema("field2")
	field2.Occurrences = 50
	schema.Fields["field2"] = field2

	analyzer.calculatePercentages(schema)

	if field1.Percentage != 80.0 {
		t.Errorf("Expected field1 percentage 80.0, got %f", field1.Percentage)
	}
	if field2.Percentage != 50.0 {
		t.Errorf("Expected field2 percentage 50.0, got %f", field2.Percentage)
	}
}

func TestAnalyzeFieldsWithDepthLimit(t *testing.T) {
	analyzer := NewSchemaAnalyzer(config.SchemaDiscoveryConfig{
		MaxDepth: 1,
	})

	schema := newSchema()
	data := report.JSONData{
		"level1": map[string]any{
			"level2": map[string]any{
				"level3": "should not be analyzed",
			},
		},
	}

	analyzer.analyzeFields(data, schema, "", 0)

	// Should have level1 field
	if _, exists := schema.Fields["level1"]; !exists {
		t.Error("Expected level1 field to exist")
	}

	// Should have level2 as subfield
	level1 := schema.Fields["level1"]
	if _, exists := level1.SubFields["level2"]; !exists {
		t.Error("Expected level2 subfield to exist")
	}

	// Should not have level3 due to depth limit
	level2 := level1.SubFields["level2"]
	if _, exists := level2.SubFields["level3"]; exists {
		t.Error("Expected level3 subfield to not exist due to depth limit")
	}
}
