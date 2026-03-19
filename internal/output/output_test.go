package output

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

func TestNewOutputManager(t *testing.T) {
	basePath := "/tmp/test_output"
	om := NewOutputManager(basePath)

	if om == nil {
		t.Fatal("Expected output manager to be created")
	}
	if om.basePath != basePath {
		t.Errorf("Expected base path %s, got %s", basePath, om.basePath)
	}
	if om.outputFiles == nil {
		t.Error("Expected output files to be initialized")
	}
	if om.outputWriters == nil {
		t.Error("Expected output writers to be initialized")
	}
	if om.timestamp == "" {
		t.Error("Expected timestamp to be set")
	}
}

func TestOutputManager_CreateOutputFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Test creating a new file
	filePath, err := om.CreateOutputFile("test", "json")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}

	expectedPath := filepath.Join(tmpDir, "test_"+om.timestamp+".json")
	if filePath != expectedPath {
		t.Errorf("Expected file path %s, got %s", expectedPath, filePath)
	}

	// Check that file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Error("Expected file to exist")
	}

	// Test creating the same file again (should return existing path)
	filePath2, err := om.CreateOutputFile("test", "json")
	if err != nil {
		t.Fatalf("Failed to create output file again: %v", err)
	}

	if filePath != filePath2 {
		t.Errorf("Expected same file path, got %s and %s", filePath, filePath2)
	}
}

func TestOutputManager_WriteJSONLine(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create a test file
	filePath, err := om.CreateOutputFile("test", "json")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}

	// Write test data
	testData := map[string]any{
		"id":   "123",
		"name": "test",
	}

	err = om.WriteJSONLine(filePath, testData)
	if err != nil {
		t.Fatalf("Failed to write JSON line: %v", err)
	}

	// Close and verify content
	err = om.Close()
	if err != nil {
		t.Fatalf("Failed to close output manager: %v", err)
	}

	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	expected := `{"id":"123","name":"test"}` + "\n"
	if string(content) != expected {
		t.Errorf("Expected content %s, got %s", expected, string(content))
	}
}

func TestOutputManager_WriteSearchResults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create mock search results
	searchResults := &search.SearchResults{
		Summary: search.SearchSummary{
			TotalMatches: 2,
			MatchesByTarget: map[string]int{
				"test_target": 1,
			},
			ProcessedRows: 100,
		},
		Results: map[string][]search.MatchResult{
			"test_target": {
				{
					Path:     "id",
					Value:    "123",
					Target:   "test_target",
					Location: report.LocationInfo{FilePath: "/test/file.json", LineNumber: 1},
				},
			},
		},
	}

	om.searchResults = searchResults

	// Test different output formats
	formats := []config.OutputFormat{
		{Format: "json", Filename: "search_results.json"},
		{Format: "csv", Filename: "search_results.csv"},
		{Format: "txt", Filename: "search_results.txt"},
	}

	for _, format := range formats {
		t.Run(format.Format, func(t *testing.T) {
			err := om.WriteSearchResults(format)
			if err != nil {
				t.Fatalf("Failed to write search results in %s format: %v", format.Format, err)
			}

			// Check that file was created
			expectedPath := filepath.Join(tmpDir, format.Filename)
			if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
				t.Errorf("Expected file %s to exist", expectedPath)
			}
		})
	}
}

func TestOutputManager_WriteSchemaResults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create mock schema analyzer
	schemaConfig := config.SchemaDiscoveryConfig{
		Enabled:       true,
		OutputFormats: []string{"json"},
	}
	schemaAnalyzer := schema.NewSchemaAnalyzer(schemaConfig)

	// Add some test data
	testData := report.JSONData{
		"id":   "123",
		"name": "test",
		"age":  25,
	}

	schemaAnalyzer.AnalyzeRow(testData, "/test/file.json")

	om.schemaAnalyzer = schemaAnalyzer

	// Test different output formats
	formats := []config.OutputFormat{
		{Format: "json", Filename: "schema_results.json"},
		{Format: "csv", Filename: "schema_results.csv"},
		{Format: "yaml", Filename: "schema_results.yaml"},
	}

	for _, format := range formats {
		t.Run(format.Format, func(t *testing.T) {
			err := om.WriteSchemaResults(format)
			if err != nil {
				t.Fatalf("Failed to write schema results in %s format: %v", format.Format, err)
			}

			// Check that file was created
			expectedPath := filepath.Join(tmpDir, format.Filename)
			if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
				t.Errorf("Expected file %s to exist", expectedPath)
			}
		})
	}
}

func TestOutputManager_WriteDeletionResults(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create mock deletion results
	deletionResults := &deletion.DeletionStats{
		TotalRows:     100,
		ProcessedRows: 90,
		DeletedRows:   10,
		ModifiedRows:  5,
		StartTime:     time.Now(),
		EndTime:       time.Now().Add(time.Minute),
	}

	om.deletionResults = deletionResults

	// Test different output formats
	formats := []config.OutputFormat{
		{Format: "json", Filename: "deletion_results.json"},
		{Format: "csv", Filename: "deletion_results.csv"},
		{Format: "txt", Filename: "deletion_results.txt"},
	}

	for _, format := range formats {
		t.Run(format.Format, func(t *testing.T) {
			err := om.WriteDeletionResults(format)
			if err != nil {
				t.Fatalf("Failed to write deletion results in %s format: %v", format.Format, err)
			}

			// Check that file was created
			expectedPath := filepath.Join(tmpDir, format.Filename)
			if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
				t.Errorf("Expected file %s to exist", expectedPath)
			}
		})
	}
}

func TestOutputManager_ProcessSources(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a test JSON file
	testFile := filepath.Join(tmpDir, "test.json")
	testData := `{"id":"123","name":"test"}` + "\n" + `{"id":"456","name":"another"}`
	err = os.WriteFile(testFile, []byte(testData), 0o644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	om := NewOutputManager(tmpDir)

	// Create mock source
	sources := []source.InputSource{
		source.LocalFileSource{},
	}

	ctx := context.Background()
	err = om.ProcessSources(ctx, sources)
	if err != nil {
		t.Fatalf("Failed to process sources: %v", err)
	}
}

func TestOutputManager_SetResults(t *testing.T) {
	om := NewOutputManager("/tmp/test")

	// Test setting search results
	searchResults := &search.SearchResults{
		Results: make(map[string][]search.MatchResult),
		Summary: search.SearchSummary{
			TotalMatches: 5,
		},
	}
	om.SetSearchResults(searchResults)

	if om.searchResults != searchResults {
		t.Error("Expected search results to be set")
	}

	// Test setting schema analyzer
	schemaConfig := config.SchemaDiscoveryConfig{
		Enabled:       true,
		OutputFormats: []string{"json"},
	}
	schemaAnalyzer := schema.NewSchemaAnalyzer(schemaConfig)
	om.SetSchemaAnalyzer(schemaAnalyzer)

	if om.schemaAnalyzer != schemaAnalyzer {
		t.Error("Expected schema analyzer to be set")
	}

	// Test setting deletion results
	deletionResults := &deletion.DeletionStats{
		TotalRows: 100,
	}
	om.SetDeletionResults(deletionResults)

	if om.deletionResults != deletionResults {
		t.Error("Expected deletion results to be set")
	}
}

func TestOutputManager_Close(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create multiple files
	_, err = om.CreateOutputFile("test1", "json")
	if err != nil {
		t.Fatalf("Failed to create first file: %v", err)
	}

	_, err = om.CreateOutputFile("test2", "json")
	if err != nil {
		t.Fatalf("Failed to create second file: %v", err)
	}

	// Close should not error
	err = om.Close()
	if err != nil {
		t.Fatalf("Failed to close output manager: %v", err)
	}

	// Second close should not error
	err = om.Close()
	if err != nil {
		t.Fatalf("Failed to close output manager second time: %v", err)
	}
}

func TestOutputManager_GetOutputPath(t *testing.T) {
	basePath := "/tmp/test_output"
	om := NewOutputManager(basePath)

	outputPath := om.GetOutputPath("test", "json")
	expectedPath := filepath.Join(basePath, "test_"+om.timestamp+".json")
	if outputPath != expectedPath {
		t.Errorf("Expected output path %s, got %s", expectedPath, outputPath)
	}
}

func TestOutputManager_GetTimestamp(t *testing.T) {
	om := NewOutputManager("/tmp/test")

	timestamp := om.GetTimestamp()
	if timestamp == "" {
		t.Error("Expected timestamp to be set")
	}

	// Verify timestamp format
	_, err := time.Parse("2006-01-02_15-04-05", timestamp)
	if err != nil {
		t.Errorf("Expected valid timestamp format, got %s: %v", timestamp, err)
	}
}

func TestOutputManager_FlushAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create and write to a file
	filePath, err := om.CreateOutputFile("test", "json")
	if err != nil {
		t.Fatalf("Failed to create output file: %v", err)
	}

	testData := map[string]any{
		"id": "123",
	}

	err = om.WriteJSONLine(filePath, testData)
	if err != nil {
		t.Fatalf("Failed to write JSON line: %v", err)
	}

	// Flush should not error
	err = om.FlushAll()
	if err != nil {
		t.Fatalf("Failed to flush all: %v", err)
	}
}

func TestOutputManager_CreateDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Create a subdirectory
	subDir := filepath.Join(tmpDir, "subdir")
	err = om.CreateDirectory(subDir)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Check that directory exists
	if _, err := os.Stat(subDir); os.IsNotExist(err) {
		t.Error("Expected directory to exist")
	}

	// Creating the same directory again should not error
	err = om.CreateDirectory(subDir)
	if err != nil {
		t.Fatalf("Failed to create directory again: %v", err)
	}
}

func TestOutputManager_WriteMetadata(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	metadata := map[string]any{
		"version":     "1.0.0",
		"generated":   time.Now().Format(time.RFC3339),
		"total_files": 5,
	}

	err = om.WriteMetadata(metadata)
	if err != nil {
		t.Fatalf("Failed to write metadata: %v", err)
	}

	// Check that metadata file was created
	metadataPath := filepath.Join(tmpDir, "metadata_"+om.timestamp+".json")
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Error("Expected metadata file to exist")
	}

	// Read and verify content
	content, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatalf("Failed to read metadata file: %v", err)
	}

	var readMetadata map[string]any
	err = json.Unmarshal(content, &readMetadata)
	if err != nil {
		t.Fatalf("Failed to unmarshal metadata: %v", err)
	}

	if readMetadata["version"] != "1.0.0" {
		t.Error("Expected version to be preserved")
	}
}

func TestOutputManager_GenerateReport(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	om := NewOutputManager(tmpDir)

	// Set up mock data
	searchResults := &search.SearchResults{
		Results: map[string][]search.MatchResult{
			"test_target": {
				{
					Path:     "id",
					Value:    "123",
					Target:   "test_target",
					Location: report.LocationInfo{FilePath: "/test/file.json", LineNumber: 1},
				},
			},
		},
		Summary: search.SearchSummary{
			TotalMatches: 10,
		},
	}

	om.SetSearchResults(searchResults)

	report := om.GenerateReport()
	if report == nil {
		t.Fatal("Expected report to be generated")
	}

	if report.SearchResults != searchResults {
		t.Error("Expected search results to be included in report")
	}

	if report.GeneratedAt.IsZero() {
		t.Error("Expected generated timestamp to be set")
	}
}

func TestWriteAdvancedArtifacts(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "advanced_output")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &config.Config{
		LogPath: tmpDir,
		Advanced: &config.AdvancedConfig{
			SchemaDiscovery: config.SchemaDiscoveryConfig{
				Enabled:       true,
				OutputFormats: []string{"json", "csv"},
			},
		},
	}

	rep := &report.AnalysisReport{
		SearchResults: &search.SearchResults{
			Results: map[string][]search.MatchResult{
				"target_email": {
					{
						Found:        true,
						Path:         "email",
						Value:        "john@example.com",
						MatchedValue: "john@example.com",
						Target:       "target_email",
						Location: report.LocationInfo{
							FilePath:   "/tmp/source.json",
							LineNumber: 1,
						},
					},
				},
			},
			Summary: search.SearchSummary{TotalMatches: 1, ProcessedRows: 1},
		},
		SchemaReport: &schema.SchemaReport{
			GlobalSchema: &schema.Schema{
				Fields: map[string]*schema.FieldSchema{
					"email": {
						Path:        "email",
						Type:        "string",
						Occurrences: 1,
						Percentage:  100,
					},
				},
			},
			TotalRows:   1,
			SampledRows: 1,
			SampleRate:  1,
		},
		DeletionStats: &deletion.DeletionStats{
			TotalRows:       1,
			ProcessedRows:   1,
			ModifiedRows:    1,
			MatchesByTarget: map[string]int64{"target_email": 1},
			StartTime:       time.Now().Add(-1 * time.Second),
			EndTime:         time.Now(),
		},
	}

	if err := WriteAdvancedArtifacts(tmpDir, cfg, rep); err != nil {
		t.Fatalf("Failed to write advanced artifacts: %v", err)
	}

	expectedPatterns := []string{
		"search_results_*.json",
		"search_target_target_email_*.json",
		"schema_report_*.json",
		"schema_report_*.csv",
		"deletion_stats_*.json",
		"deletion_summary_*.txt",
	}

	for _, pattern := range expectedPatterns {
		matches, err := filepath.Glob(filepath.Join(tmpDir, pattern))
		if err != nil {
			t.Fatalf("Failed to glob pattern %s: %v", pattern, err)
		}
		if len(matches) == 0 {
			t.Fatalf("Expected at least one file matching %s", pattern)
		}
	}
}
