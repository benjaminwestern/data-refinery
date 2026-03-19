package analyser

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/benjaminwestern/dupe-analyser/internal/config"
	"github.com/benjaminwestern/dupe-analyser/internal/deletion"
	"github.com/benjaminwestern/dupe-analyser/internal/search"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
)

// mockInputSource implements source.InputSource for testing
type mockInputSource struct {
	path    string
	content []byte
	size    int64
	dir     string
}

func (m *mockInputSource) Path() string { return m.path }
func (m *mockInputSource) Size() int64  { return m.size }
func (m *mockInputSource) Dir() string  { return m.dir }
func (m *mockInputSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return &mockReader{content: m.content}, nil
}

type mockReader struct {
	content []byte
	pos     int
}

func (m *mockReader) Read(p []byte) (int, error) {
	if m.pos >= len(m.content) {
		return 0, io.EOF
	}
	n := copy(p, m.content[m.pos:])
	m.pos += n
	return n, nil
}

func (m *mockReader) Close() error {
	return nil
}

// Helper function to create test JSON data
func createTestJSONData(id string, name string, age int) string {
	data := map[string]any{
		"id":   id,
		"name": name,
		"age":  age,
	}
	jsonData, _ := json.Marshal(data)
	return string(jsonData)
}

// Helper function to create mock source with JSON lines
func createMockSource(path string, jsonLines []string) *mockInputSource {
	content := strings.Join(jsonLines, "\n")
	return &mockInputSource{
		path:    path,
		content: []byte(content),
		size:    int64(len(content)),
		dir:     filepath.Dir(path),
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		config      *config.Config
		expectError bool
	}{
		{
			name: "basic configuration",
			config: &config.Config{
				Key:      "id",
				Workers:  4,
				CheckKey: true,
				CheckRow: true,
				LogPath:  "test_logs",
			},
			expectError: false,
		},
		{
			name: "validation only",
			config: &config.Config{
				Key:          "user_id",
				Workers:      2,
				CheckKey:     true,
				CheckRow:     false,
				ValidateOnly: true,
				LogPath:      "test_logs",
			},
			expectError: false,
		},
		{
			name: "row checking only",
			config: &config.Config{
				Key:      "test_id",
				Workers:  8,
				CheckKey: false,
				CheckRow: true,
				LogPath:  "test_logs",
			},
			expectError: false,
		},
		{
			name: "invalid config - no key",
			config: &config.Config{
				Workers:  4,
				CheckKey: true,
				CheckRow: true,
				LogPath:  "test_logs",
			},
			expectError: true,
		},
		{
			name: "invalid config - no workers",
			config: &config.Config{
				Key:      "id",
				Workers:  0,
				CheckKey: true,
				CheckRow: true,
				LogPath:  "test_logs",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analyser, err := New(tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if analyser.config.Key != tt.config.Key {
				t.Errorf("Expected uniqueKey %q, got %q", tt.config.Key, analyser.config.Key)
			}
			if analyser.config.Workers != tt.config.Workers {
				t.Errorf("Expected numWorkers %d, got %d", tt.config.Workers, analyser.config.Workers)
			}
			if analyser.config.CheckKey != tt.config.CheckKey {
				t.Errorf("Expected checkKey %v, got %v", tt.config.CheckKey, analyser.config.CheckKey)
			}
			if analyser.config.CheckRow != tt.config.CheckRow {
				t.Errorf("Expected checkRow %v, got %v", tt.config.CheckRow, analyser.config.CheckRow)
			}

			// Verify initialization
			if analyser.processedPaths == nil {
				t.Error("processedPaths should be initialized")
			}
			if analyser.ProcessedFiles == nil {
				t.Error("ProcessedFiles should be initialized")
			}
			if analyser.TotalRows == nil {
				t.Error("TotalRows should be initialized")
			}
			if analyser.CurrentFolder == nil {
				t.Error("CurrentFolder should be initialized")
			}
		})
	}
}

func TestNewAdvanced(t *testing.T) {
	tests := []struct {
		name        string
		config      *config.Config
		expectError bool
	}{
		{
			name: "basic config without advanced features",
			config: &config.Config{
				Key:      "id",
				Workers:  4,
				CheckKey: true,
				CheckRow: true,
				LogPath:  "test_logs",
			},
			expectError: false,
		},
		{
			name: "config with advanced features",
			config: &config.Config{
				Key:      "id",
				Workers:  4,
				CheckKey: true,
				CheckRow: true,
				LogPath:  "test_logs",
				Advanced: &config.AdvancedConfig{
					SearchTargets: []config.SearchTarget{
						{
							Name:         "email_search",
							Type:         "direct",
							Path:         "email",
							TargetValues: []string{"@"},
						},
					},
					SchemaDiscovery: config.SchemaDiscoveryConfig{
						Enabled:       true,
						OutputFormats: []string{"json"},
					},
					HashingStrategy: config.HashingStrategy{
						Mode: "full_row",
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			analyser, err := New(tt.config)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if analyser.config.Key != tt.config.Key {
				t.Errorf("Expected uniqueKey %q, got %q", tt.config.Key, analyser.config.Key)
			}
			if analyser.config.Workers != tt.config.Workers {
				t.Errorf("Expected numWorkers %d, got %d", tt.config.Workers, analyser.config.Workers)
			}

			if tt.config.Advanced != nil {
				if len(tt.config.Advanced.SearchTargets) > 0 && analyser.searchEngine == nil {
					t.Error("Expected searchEngine to be initialized")
				}
				if tt.config.Advanced.SchemaDiscovery.Enabled && analyser.schemaAnalyzer == nil {
					t.Error("Expected schemaAnalyzer to be initialized")
				}
				if analyser.selectiveHasher == nil {
					t.Error("Expected selectiveHasher to be initialized")
				}
			}
		})
	}
}

func TestGetUnprocessedSources(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	sources := []source.InputSource{
		createMockSource("/path/to/file1.json", []string{}),
		createMockSource("/path/to/file2.json", []string{}),
		createMockSource("/path/to/file3.json", []string{}),
	}

	// Initially, all sources should be unprocessed
	unprocessed := analyser.GetUnprocessedSources(sources)
	if len(unprocessed) != 3 {
		t.Errorf("Expected 3 unprocessed sources, got %d", len(unprocessed))
	}

	// Mark some sources as processed
	analyser.processedPathsMutex.Lock()
	analyser.processedPaths["/path/to/file1.json"] = true
	analyser.processedPaths["/path/to/file3.json"] = true
	analyser.processedPathsMutex.Unlock()

	unprocessed = analyser.GetUnprocessedSources(sources)
	if len(unprocessed) != 1 {
		t.Errorf("Expected 1 unprocessed source, got %d", len(unprocessed))
	}
	if unprocessed[0].Path() != "/path/to/file2.json" {
		t.Errorf("Expected unprocessed source to be file2.json, got %s", unprocessed[0].Path())
	}
}

func TestAnalyserRun(t *testing.T) {
	tests := []struct {
		name          string
		sources       []source.InputSource
		checkKey      bool
		checkRow      bool
		validateOnly  bool
		expectedDupes int
		expectedRows  int64
		expectedFiles int32
	}{
		{
			name: "duplicate key detection",
			sources: []source.InputSource{
				createMockSource("/test/file1.json", []string{
					createTestJSONData("1", "John", 25),
					createTestJSONData("2", "Jane", 30),
					createTestJSONData("1", "John Doe", 26), // Duplicate ID
				}),
				createMockSource("/test/file2.json", []string{
					createTestJSONData("3", "Bob", 35),
					createTestJSONData("2", "Jane Smith", 31), // Duplicate ID
				}),
			},
			checkKey:      true,
			checkRow:      false,
			validateOnly:  false,
			expectedDupes: 2, // ID "1" and "2" are duplicated
			expectedRows:  5,
			expectedFiles: 2,
		},
		{
			name: "duplicate row detection",
			sources: []source.InputSource{
				createMockSource("/test/file1.json", []string{
					createTestJSONData("1", "John", 25),
					createTestJSONData("2", "Jane", 30),
					createTestJSONData("1", "John", 25), // Duplicate row
				}),
			},
			checkKey:      false,
			checkRow:      true,
			validateOnly:  false,
			expectedDupes: 1, // One duplicate row
			expectedRows:  3,
			expectedFiles: 1,
		},
		{
			name: "validation only mode",
			sources: []source.InputSource{
				createMockSource("/test/file1.json", []string{
					createTestJSONData("1", "John", 25),
					createTestJSONData("2", "Jane", 30),
				}),
			},
			checkKey:      true,
			checkRow:      false,
			validateOnly:  true,
			expectedDupes: 0, // No duplicates stored in validation mode
			expectedRows:  2,
			expectedFiles: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{
				Key:          "id",
				Workers:      2,
				CheckKey:     tt.checkKey,
				CheckRow:     tt.checkRow,
				ValidateOnly: tt.validateOnly,
				LogPath:      "test_logs",
			}

			analyser, err := New(cfg)
			if err != nil {
				t.Fatalf("Failed to create analyser: %v", err)
			}

			ctx := context.Background()
			report := analyser.Run(ctx, tt.sources)

			if report == nil {
				t.Fatal("Expected report to be non-nil")
			}

			// Check row count
			if report.Summary.TotalRowsProcessed != tt.expectedRows {
				t.Errorf("Expected %d rows processed, got %d", tt.expectedRows, report.Summary.TotalRowsProcessed)
			}

			// Check files count
			if report.Summary.FilesProcessed != tt.expectedFiles {
				t.Errorf("Expected %d files processed, got %d", tt.expectedFiles, report.Summary.FilesProcessed)
			}

			// Check duplicates
			if tt.checkKey && !tt.validateOnly {
				if len(report.DuplicateIDs) != tt.expectedDupes {
					t.Errorf("Expected %d duplicate IDs, got %d", tt.expectedDupes, len(report.DuplicateIDs))
				}
			}

			if tt.checkRow && !tt.validateOnly {
				if len(report.DuplicateRows) != tt.expectedDupes {
					t.Errorf("Expected %d duplicate rows, got %d", tt.expectedDupes, len(report.DuplicateRows))
				}
			}
		})
	}
}

func TestAnalyserConcurrency(t *testing.T) {
	// Create multiple sources to test concurrent processing
	sources := make([]source.InputSource, 10)
	for i := 0; i < 10; i++ {
		jsonLines := make([]string, 100)
		for j := 0; j < 100; j++ {
			jsonLines[j] = createTestJSONData(fmt.Sprintf("id_%d_%d", i, j), fmt.Sprintf("name_%d_%d", i, j), j%50)
		}
		sources[i] = createMockSource(fmt.Sprintf("/test/file%d.json", i), jsonLines)
	}

	cfg := &config.Config{
		Key:      "id",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	ctx := context.Background()

	start := time.Now()
	report := analyser.Run(ctx, sources)
	duration := time.Since(start)

	if report == nil {
		t.Fatal("Expected report to be non-nil")
	}

	expectedRows := int64(1000) // 10 files * 100 rows each
	if report.Summary.TotalRowsProcessed != expectedRows {
		t.Errorf("Expected %d rows processed, got %d", expectedRows, report.Summary.TotalRowsProcessed)
	}

	if report.Summary.FilesProcessed != 10 {
		t.Errorf("Expected 10 files processed, got %d", report.Summary.FilesProcessed)
	}

	// Should process relatively quickly with multiple workers
	if duration > 5*time.Second {
		t.Errorf("Processing took too long: %v", duration)
	}

	t.Logf("Processed %d rows in %v", report.Summary.TotalRowsProcessed, duration)
}

func TestAnalyserCancellation(t *testing.T) {
	// Create a large source that would take time to process
	jsonLines := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		jsonLines[i] = createTestJSONData(fmt.Sprintf("id_%d", i), fmt.Sprintf("name_%d", i), i%100)
	}

	sources := []source.InputSource{
		createMockSource("/test/large_file.json", jsonLines),
	}

	cfg := &config.Config{
		Key:      "id",
		Workers:  1,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a short time
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	report := analyser.Run(ctx, sources)
	duration := time.Since(start)

	if report == nil {
		t.Fatal("Expected report to be non-nil")
	}

	// Should have been cancelled quickly
	if duration > 1*time.Second {
		t.Errorf("Processing should have been cancelled quickly, took: %v", duration)
	}

	// Should be marked as partial report
	if !report.Summary.IsPartialReport {
		t.Error("Expected report to be marked as partial due to cancellation")
	}

	t.Logf("Processing cancelled after %v, processed %d rows", duration, report.Summary.TotalRowsProcessed)
}

func TestAnalyserAdvancedFeatures(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  2,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
		Advanced: &config.AdvancedConfig{
			SearchTargets: []config.SearchTarget{
				{
					Name:         "email_search",
					Type:         "direct",
					Path:         "email",
					TargetValues: []string{"@"},
				},
			},
			SchemaDiscovery: config.SchemaDiscoveryConfig{
				Enabled:       true,
				OutputFormats: []string{"json"},
			},
			HashingStrategy: config.HashingStrategy{
				Mode:        "selective",
				IncludeKeys: []string{"name", "email"},
			},
		},
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create advanced analyser: %v", err)
	}

	sources := []source.InputSource{
		createMockSource("/test/file1.json", []string{
			`{"id": "1", "name": "John", "email": "john@example.com", "age": 25}`,
			`{"id": "2", "name": "Jane", "email": "jane@example.com", "age": 30}`,
			`{"id": "3", "name": "Bob", "phone": "555-1234", "age": 35}`,
		}),
	}

	ctx := context.Background()
	report := analyser.Run(ctx, sources)

	if report == nil {
		t.Fatal("Expected report to be non-nil")
	}

	// Check that advanced features were used
	if report.SearchResults == nil {
		t.Error("Expected search results to be populated")
	}
	if report.SchemaReport == nil {
		t.Error("Expected schema report to be populated")
	}

	// Test getter methods
	searchResults := analyser.GetSearchResults()
	if searchResults == nil {
		t.Error("Expected search results from getter method")
	}

	schemaReport := analyser.GetSchemaReport()
	if schemaReport == nil {
		t.Error("Expected schema report from getter method")
	}

	deletionStats := analyser.GetDeletionStats()
	if deletionStats != nil {
		t.Error("Expected deletion stats to be nil (no deletion rules configured)")
	}
}

func TestAnalyserDeletionDoesNotDuplicateSearchMatches(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  1,
		CheckKey: true,
		CheckRow: false,
		LogPath:  "test_logs",
		Advanced: &config.AdvancedConfig{
			SearchTargets: []config.SearchTarget{
				{
					Name:         "target_email",
					Type:         "direct",
					Path:         "email",
					TargetValues: []string{"john@example.com"},
				},
			},
			DeletionRules: []config.DeletionRule{
				{
					SearchTarget: "target_email",
					Action:       "mark_for_deletion",
				},
			},
		},
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	sources := []source.InputSource{
		createMockSource("/test/file.json", []string{
			`{"id": "1", "email": "john@example.com"}`,
		}),
	}

	rep := analyser.Run(context.Background(), sources)
	if rep == nil {
		t.Fatal("Expected report to be non-nil")
	}

	var searchResults *search.SearchResults
	switch typed := rep.SearchResults.(type) {
	case *search.SearchResults:
		searchResults = typed
	case search.SearchResults:
		searchResults = &typed
	}
	if searchResults == nil {
		t.Fatal("Expected typed search results in report")
	}

	if searchResults.Summary.TotalMatches != 1 {
		t.Fatalf("Expected exactly 1 accumulated search match, got %d", searchResults.Summary.TotalMatches)
	}

	var deletionStats *deletion.DeletionStats
	switch typed := rep.DeletionStats.(type) {
	case *deletion.DeletionStats:
		deletionStats = typed
	case deletion.DeletionStats:
		deletionStats = &typed
	}
	if deletionStats == nil {
		t.Fatal("Expected typed deletion stats in report")
	}

	if deletionStats.ModifiedRows != 1 {
		t.Fatalf("Expected exactly 1 modified row, got %d", deletionStats.ModifiedRows)
	}

	if deletionStats.MatchesByTarget["target_email"] != 1 {
		t.Fatalf("Expected exactly 1 deletion match for target_email, got %d", deletionStats.MatchesByTarget["target_email"])
	}
}

func TestAnalyserClose(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  2,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	// Should not error when closing analyser without advanced features
	if err := analyser.Close(); err != nil {
		t.Errorf("Unexpected error closing analyser: %v", err)
	}

	// Test with advanced features
	cfg2 := &config.Config{
		Key:      "id",
		Workers:  2,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
		Advanced: &config.AdvancedConfig{
			SearchTargets: []config.SearchTarget{
				{
					Name:         "test_search",
					Type:         "direct",
					Path:         "name",
					TargetValues: []string{"test"},
				},
			},
			DeletionRules: []config.DeletionRule{
				{
					SearchTarget: "test_search",
					Action:       "delete_row",
				},
			},
			HashingStrategy: config.HashingStrategy{
				Mode: "full_row",
			},
		},
	}

	advancedAnalyser, err := New(cfg2)
	if err != nil {
		t.Fatalf("Failed to create advanced analyser: %v", err)
	}

	if err := advancedAnalyser.Close(); err != nil {
		t.Errorf("Unexpected error closing advanced analyser: %v", err)
	}
}

func TestAnalyserAtomicOperations(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	// Test atomic operations
	if analyser.ProcessedFiles.Load() != 0 {
		t.Error("Expected ProcessedFiles to be 0 initially")
	}

	if analyser.TotalRows.Load() != 0 {
		t.Error("Expected TotalRows to be 0 initially")
	}

	// Simulate some processing
	analyser.ProcessedFiles.Add(1)
	analyser.TotalRows.Add(100)

	if analyser.ProcessedFiles.Load() != 1 {
		t.Error("Expected ProcessedFiles to be 1 after increment")
	}

	if analyser.TotalRows.Load() != 100 {
		t.Error("Expected TotalRows to be 100 after increment")
	}

	// Test CurrentFolder
	analyser.CurrentFolder.Store("test_folder")
	if analyser.CurrentFolder.Load().(string) != "test_folder" {
		t.Error("Expected CurrentFolder to be 'test_folder'")
	}
}

func TestAnalyserFolderStatistics(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  2,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	sources := []source.InputSource{
		createMockSource("/folder1/file1.json", []string{
			createTestJSONData("1", "John", 25),
			createTestJSONData("2", "Jane", 30),
		}),
		createMockSource("/folder1/file2.json", []string{
			createTestJSONData("3", "Bob", 35),
		}),
		createMockSource("/folder2/file3.json", []string{
			createTestJSONData("4", "Alice", 28),
			createTestJSONData("5", "Charlie", 32),
		}),
	}

	ctx := context.Background()
	report := analyser.Run(ctx, sources)

	if report == nil {
		t.Fatal("Expected report to be non-nil")
	}

	// Check folder details
	if len(report.Summary.FolderDetails) != 2 {
		t.Errorf("Expected 2 folders, got %d", len(report.Summary.FolderDetails))
	}

	folder1Detail, exists := report.Summary.FolderDetails["/folder1"]
	if !exists {
		t.Error("Expected folder1 to be in folder details")
	} else {
		if folder1Detail.FilesProcessed != 2 {
			t.Errorf("Expected 2 files processed in folder1, got %d", folder1Detail.FilesProcessed)
		}
		if folder1Detail.RowsProcessed != 3 {
			t.Errorf("Expected 3 rows processed in folder1, got %d", folder1Detail.RowsProcessed)
		}
	}

	folder2Detail, exists := report.Summary.FolderDetails["/folder2"]
	if !exists {
		t.Error("Expected folder2 to be in folder details")
	} else {
		if folder2Detail.FilesProcessed != 1 {
			t.Errorf("Expected 1 file processed in folder2, got %d", folder2Detail.FilesProcessed)
		}
		if folder2Detail.RowsProcessed != 2 {
			t.Errorf("Expected 2 rows processed in folder2, got %d", folder2Detail.RowsProcessed)
		}
	}
}

func TestAnalyserErrorHandling(t *testing.T) {
	cfg := &config.Config{
		Key:      "id",
		Workers:  2,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	analyser, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create analyser: %v", err)
	}

	// Test with invalid JSON
	sources := []source.InputSource{
		createMockSource("/test/invalid.json", []string{
			`{"id": "1", "name": "John"}`, // Valid JSON
			`{"id": "2", "name": "Jane"`,  // Invalid JSON (missing closing brace)
			`{"id": "3", "name": "Bob"}`,  // Valid JSON
		}),
	}

	ctx := context.Background()
	report := analyser.Run(ctx, sources)

	if report == nil {
		t.Fatal("Expected report to be non-nil")
	}

	// Should process valid lines and skip invalid ones
	if report.Summary.TotalRowsProcessed != 2 {
		t.Errorf("Expected 2 rows processed (skipping invalid JSON), got %d", report.Summary.TotalRowsProcessed)
	}

	if report.Summary.FilesProcessed != 1 {
		t.Errorf("Expected 1 file processed, got %d", report.Summary.FilesProcessed)
	}
}

func BenchmarkAnalyserRun(b *testing.B) {
	// Create benchmark data
	jsonLines := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		jsonLines[i] = createTestJSONData(fmt.Sprintf("id_%d", i), fmt.Sprintf("name_%d", i), i%100)
	}

	sources := []source.InputSource{
		createMockSource("/test/benchmark.json", jsonLines),
	}

	cfg := &config.Config{
		Key:      "id",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		analyser, err := New(cfg)
		if err != nil {
			b.Fatalf("Failed to create analyser: %v", err)
		}

		ctx := context.Background()
		report := analyser.Run(ctx, sources)
		if report == nil {
			b.Fatal("Expected report to be non-nil")
		}
	}
}

func BenchmarkAnalyserConcurrency(b *testing.B) {
	// Create multiple sources for concurrency testing
	sources := make([]source.InputSource, 10)
	for i := 0; i < 10; i++ {
		jsonLines := make([]string, 100)
		for j := 0; j < 100; j++ {
			jsonLines[j] = createTestJSONData(fmt.Sprintf("id_%d_%d", i, j), fmt.Sprintf("name_%d_%d", i, j), j%50)
		}
		sources[i] = createMockSource(fmt.Sprintf("/test/file%d.json", i), jsonLines)
	}

	cfg := &config.Config{
		Key:      "id",
		Workers:  8,
		CheckKey: true,
		CheckRow: true,
		LogPath:  "test_logs",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		analyser, err := New(cfg)
		if err != nil {
			b.Fatalf("Failed to create analyser: %v", err)
		}

		ctx := context.Background()
		report := analyser.Run(ctx, sources)
		if report == nil {
			b.Fatal("Expected report to be non-nil")
		}
	}
}
