package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocationInfo(t *testing.T) {
	tests := []struct {
		name       string
		location   LocationInfo
		expectJSON string
	}{
		{
			name: "basic location info",
			location: LocationInfo{
				FilePath:   "/path/to/file.json",
				LineNumber: 42,
			},
			expectJSON: `{"filePath":"/path/to/file.json","lineNumber":42}`,
		},
		{
			name: "empty location info",
			location: LocationInfo{
				FilePath:   "",
				LineNumber: 0,
			},
			expectJSON: `{"filePath":"","lineNumber":0}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON marshaling
			jsonData, err := json.Marshal(tt.location)
			if err != nil {
				t.Errorf("Failed to marshal LocationInfo: %v", err)
			}

			if string(jsonData) != tt.expectJSON {
				t.Errorf("Expected JSON %s, got %s", tt.expectJSON, string(jsonData))
			}

			// Test JSON unmarshaling
			var unmarshaledLocation LocationInfo
			if err := json.Unmarshal(jsonData, &unmarshaledLocation); err != nil {
				t.Errorf("Failed to unmarshal LocationInfo: %v", err)
			}

			if unmarshaledLocation != tt.location {
				t.Errorf("Expected %+v, got %+v", tt.location, unmarshaledLocation)
			}
		})
	}
}

func TestJSONData(t *testing.T) {
	tests := []struct {
		name     string
		jsonData JSONData
		expected map[string]interface{}
	}{
		{
			name: "basic JSON data",
			jsonData: JSONData{
				"id":   "123",
				"name": "John Doe",
				"age":  30,
			},
			expected: map[string]interface{}{
				"id":   "123",
				"name": "John Doe",
				"age":  30,
			},
		},
		{
			name: "nested JSON data",
			jsonData: JSONData{
				"user": map[string]interface{}{
					"id":   "456",
					"name": "Jane Smith",
				},
				"metadata": map[string]interface{}{
					"created": "2023-01-01",
					"tags":    []string{"important", "user"},
				},
			},
			expected: map[string]interface{}{
				"user": map[string]interface{}{
					"id":   "456",
					"name": "Jane Smith",
				},
				"metadata": map[string]interface{}{
					"created": "2023-01-01",
					"tags":    []string{"important", "user"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON marshaling
			jsonBytes, err := json.Marshal(tt.jsonData)
			if err != nil {
				t.Errorf("Failed to marshal JSONData: %v", err)
			}

			// Test JSON unmarshaling
			var unmarshaledData JSONData
			if err := json.Unmarshal(jsonBytes, &unmarshaledData); err != nil {
				t.Errorf("Failed to unmarshal JSONData: %v", err)
			}

			// Compare keys
			if len(unmarshaledData) != len(tt.expected) {
				t.Errorf("Expected %d keys, got %d", len(tt.expected), len(unmarshaledData))
			}

			for key := range tt.expected {
				if _, exists := unmarshaledData[key]; !exists {
					t.Errorf("Expected key %s to exist in unmarshaled data", key)
				}
			}
		})
	}
}

func TestFolderDetail(t *testing.T) {
	tests := []struct {
		name   string
		detail FolderDetail
	}{
		{
			name: "basic folder detail",
			detail: FolderDetail{
				ProcessedSizeBytes: 1024 * 1024,
				TotalSizeBytes:     2 * 1024 * 1024,
				FilesProcessed:     5,
				TotalFiles:         10,
				KeysFound:          100,
				RowsProcessed:      500,
			},
		},
		{
			name: "empty folder detail",
			detail: FolderDetail{
				ProcessedSizeBytes: 0,
				TotalSizeBytes:     0,
				FilesProcessed:     0,
				TotalFiles:         0,
				KeysFound:          0,
				RowsProcessed:      0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test JSON marshaling
			jsonData, err := json.Marshal(tt.detail)
			if err != nil {
				t.Errorf("Failed to marshal FolderDetail: %v", err)
			}

			// Test JSON unmarshaling
			var unmarshaledDetail FolderDetail
			if err := json.Unmarshal(jsonData, &unmarshaledDetail); err != nil {
				t.Errorf("Failed to unmarshal FolderDetail: %v", err)
			}

			if unmarshaledDetail != tt.detail {
				t.Errorf("Expected %+v, got %+v", tt.detail, unmarshaledDetail)
			}
		})
	}
}

func TestHumanSize(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{
			name:     "bytes",
			bytes:    512,
			expected: "512 B",
		},
		{
			name:     "kilobytes",
			bytes:    1024,
			expected: "1.0 KiB",
		},
		{
			name:     "megabytes",
			bytes:    1024 * 1024,
			expected: "1.0 MiB",
		},
		{
			name:     "gigabytes",
			bytes:    1024 * 1024 * 1024,
			expected: "1.0 GiB",
		},
		{
			name:     "terabytes",
			bytes:    1024 * 1024 * 1024 * 1024,
			expected: "1.0 TiB",
		},
		{
			name:     "fractional megabytes",
			bytes:    1536 * 1024, // 1.5 MB
			expected: "1.5 MiB",
		},
		{
			name:     "zero bytes",
			bytes:    0,
			expected: "0 B",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HumanSize(tt.bytes)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestAnalysisReport_String(t *testing.T) {
	// Create a sample report
	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:        false,
			IsPartialReport:           false,
			FilesProcessed:            5,
			TotalFiles:                5,
			ProcessedDataSizeBytes:    1024 * 1024,
			TotalDataSizeOverallBytes: 1024 * 1024,
			ProcessedDataSizeHuman:    "1.0 MiB",
			TotalDataSizeOverallHuman: "1.0 MiB",
			TotalElapsedTime:          "2.5s",
			TotalRowsProcessed:        1000,
			UniqueKey:                 "id",
			TotalKeyOccurrences:       1000,
			UniqueKeysDuplicated:      5,
			DuplicateRowInstances:     10,
			AverageRowsPerFile:        200.0,
			AverageFilesPerFolder:     2.5,
			DuplicateIDsPerFolder:     map[string]int{"/folder1": 3, "/folder2": 2},
			DuplicateRowsPerFolder:    map[string]int{"/folder1": 6, "/folder2": 4},
			FolderDetails: map[string]FolderDetail{
				"/folder1": {
					ProcessedSizeBytes: 512 * 1024,
					TotalSizeBytes:     512 * 1024,
					FilesProcessed:     3,
					TotalFiles:         3,
					KeysFound:          600,
					RowsProcessed:      600,
				},
				"/folder2": {
					ProcessedSizeBytes: 512 * 1024,
					TotalSizeBytes:     512 * 1024,
					FilesProcessed:     2,
					TotalFiles:         2,
					KeysFound:          400,
					RowsProcessed:      400,
				},
			},
		},
		DuplicateIDs: map[string][]LocationInfo{
			"123": {
				{FilePath: "/folder1/file1.json", LineNumber: 1},
				{FilePath: "/folder1/file2.json", LineNumber: 5},
			},
			"456": {
				{FilePath: "/folder2/file1.json", LineNumber: 10},
				{FilePath: "/folder2/file2.json", LineNumber: 15},
			},
		},
		DuplicateRows: map[string][]LocationInfo{
			"hash1": {
				{FilePath: "/folder1/file1.json", LineNumber: 20},
				{FilePath: "/folder1/file2.json", LineNumber: 25},
			},
		},
	}

	tests := []struct {
		name                string
		isFullReport        bool
		checkKey            bool
		checkRow            bool
		showFolderBreakdown bool
		shouldContain       []string
		shouldNotContain    []string
	}{
		{
			name:                "summary report",
			isFullReport:        false,
			checkKey:            true,
			checkRow:            true,
			showFolderBreakdown: false,
			shouldContain: []string{
				"Analysis Summary",
				"2.5s",
				"1000",
				"id",
				"5",
				"10",
			},
			shouldNotContain: []string{
				"Full Duplicate ID Details",
				"Per-Folder Breakdown",
			},
		},
		{
			name:                "full report with folder breakdown",
			isFullReport:        true,
			checkKey:            true,
			checkRow:            true,
			showFolderBreakdown: true,
			shouldContain: []string{
				"Analysis Summary",
				"Per-Folder Breakdown",
				"Full Duplicate ID Details",
				"Full Duplicate Row Details",
				"/folder1",
				"/folder2",
			},
			shouldNotContain: []string{},
		},
		{
			name:                "key checking only",
			isFullReport:        false,
			checkKey:            true,
			checkRow:            false,
			showFolderBreakdown: false,
			shouldContain: []string{
				"Analysis Summary",
				"id",
				"5",
			},
			shouldNotContain: []string{
				"Duplicate Row Instances",
			},
		},
		{
			name:                "row checking only",
			isFullReport:        false,
			checkKey:            false,
			checkRow:            true,
			showFolderBreakdown: false,
			shouldContain: []string{
				"Analysis Summary",
				"10",
			},
			shouldNotContain: []string{
				"Occurrences of",
				"Unique",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := report.String(tt.isFullReport, tt.checkKey, tt.checkRow, tt.showFolderBreakdown)

			for _, shouldContain := range tt.shouldContain {
				if !strings.Contains(result, shouldContain) {
					t.Errorf("Expected result to contain %q, but it didn't", shouldContain)
				}
			}

			for _, shouldNotContain := range tt.shouldNotContain {
				if strings.Contains(result, shouldNotContain) {
					t.Errorf("Expected result to not contain %q, but it did", shouldNotContain)
				}
			}

			// Basic sanity check
			if len(result) == 0 {
				t.Error("Expected non-empty result")
			}
		})
	}
}

func TestAnalysisReport_ValidationString(t *testing.T) {
	// Create a validation report
	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:  true,
			IsPartialReport:     false,
			FilesProcessed:      3,
			TotalFiles:          3,
			TotalRowsProcessed:  500,
			UniqueKey:           "user_id",
			TotalKeyOccurrences: 450,
			TotalElapsedTime:    "1.2s",
			FolderDetails: map[string]FolderDetail{
				"/data": {
					FilesProcessed: 3,
					TotalFiles:     3,
					KeysFound:      450,
					RowsProcessed:  500,
				},
			},
		},
	}

	tests := []struct {
		name                string
		showFolderBreakdown bool
		shouldContain       []string
	}{
		{
			name:                "validation summary",
			showFolderBreakdown: false,
			shouldContain: []string{
				"Key Validation Summary",
				"user_id",
				"3",
				"500",
				"450",
				"1.2s",
			},
		},
		{
			name:                "validation with folder breakdown",
			showFolderBreakdown: true,
			shouldContain: []string{
				"Key Validation Summary",
				"Per-Folder Breakdown",
				"/data",
				"3",
				"500",
				"450",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := report.String(false, true, false, tt.showFolderBreakdown)

			for _, shouldContain := range tt.shouldContain {
				if !strings.Contains(result, shouldContain) {
					t.Errorf("Expected result to contain %q, but it didn't", shouldContain)
				}
			}

			// Should not contain analysis-specific content
			if strings.Contains(result, "Analysis Summary") {
				t.Error("Validation report should not contain 'Analysis Summary'")
			}
		})
	}
}

func TestAnalysisReport_ToJSON(t *testing.T) {
	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:     false,
			IsPartialReport:        false,
			FilesProcessed:         2,
			TotalFiles:             2,
			TotalRowsProcessed:     100,
			UniqueKey:              "id",
			TotalKeyOccurrences:    100,
			UniqueKeysDuplicated:   1,
			DuplicateRowInstances:  2,
			AverageRowsPerFile:     50.0,
			AverageFilesPerFolder:  2.0,
			DuplicateIDsPerFolder:  map[string]int{"/test": 1},
			DuplicateRowsPerFolder: map[string]int{"/test": 2},
			FolderDetails: map[string]FolderDetail{
				"/test": {
					ProcessedSizeBytes: 1024,
					TotalSizeBytes:     1024,
					FilesProcessed:     2,
					TotalFiles:         2,
					KeysFound:          100,
					RowsProcessed:      100,
				},
			},
		},
		DuplicateIDs: map[string][]LocationInfo{
			"123": {
				{FilePath: "/test/file1.json", LineNumber: 1},
				{FilePath: "/test/file2.json", LineNumber: 5},
			},
		},
		DuplicateRows: map[string][]LocationInfo{
			"hash1": {
				{FilePath: "/test/file1.json", LineNumber: 10},
				{FilePath: "/test/file2.json", LineNumber: 15},
			},
		},
	}

	jsonStr, err := report.ToJSON()
	if err != nil {
		t.Errorf("Failed to convert report to JSON: %v", err)
	}

	// Test that it's valid JSON
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		t.Errorf("Generated JSON is not valid: %v", err)
	}

	// Check that key fields are present
	if _, exists := parsed["summary"]; !exists {
		t.Error("JSON should contain 'summary' field")
	}
	if _, exists := parsed["duplicateIds"]; !exists {
		t.Error("JSON should contain 'duplicateIds' field")
	}
	if _, exists := parsed["duplicateRows"]; !exists {
		t.Error("JSON should contain 'duplicateRows' field")
	}

	// Test that the JSON is formatted (indented)
	if !strings.Contains(jsonStr, "\n") {
		t.Error("JSON should be formatted with indentation")
	}
}

func TestAnalysisReport_Save(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()
	baseFilename := filepath.Join(tempDir, "test_report")

	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:     false,
			IsPartialReport:        false,
			FilesProcessed:         1,
			TotalFiles:             1,
			TotalRowsProcessed:     10,
			UniqueKey:              "id",
			TotalKeyOccurrences:    10,
			UniqueKeysDuplicated:   0,
			DuplicateRowInstances:  0,
			AverageRowsPerFile:     10.0,
			AverageFilesPerFolder:  1.0,
			DuplicateIDsPerFolder:  map[string]int{},
			DuplicateRowsPerFolder: map[string]int{},
			FolderDetails: map[string]FolderDetail{
				"/test": {
					ProcessedSizeBytes: 100,
					TotalSizeBytes:     100,
					FilesProcessed:     1,
					TotalFiles:         1,
					KeysFound:          10,
					RowsProcessed:      10,
				},
			},
		},
		DuplicateIDs:  map[string][]LocationInfo{},
		DuplicateRows: map[string][]LocationInfo{},
	}

	tests := []struct {
		name                string
		enableTxt           bool
		enableJson          bool
		checkKey            bool
		checkRow            bool
		showFolderBreakdown bool
		expectedFiles       []string
	}{
		{
			name:                "save both txt and json",
			enableTxt:           true,
			enableJson:          true,
			checkKey:            true,
			checkRow:            true,
			showFolderBreakdown: true,
			expectedFiles: []string{
				"test_report_summary.txt",
				"test_report_details.txt",
				"test_report.json",
			},
		},
		{
			name:       "save only txt",
			enableTxt:  true,
			enableJson: false,
			checkKey:   true,
			checkRow:   false,
			expectedFiles: []string{
				"test_report_summary.txt",
				"test_report_details.txt",
			},
		},
		{
			name:       "save only json",
			enableTxt:  false,
			enableJson: true,
			checkKey:   false,
			checkRow:   true,
			expectedFiles: []string{
				"test_report.json",
			},
		},
		{
			name:          "save nothing",
			enableTxt:     false,
			enableJson:    false,
			expectedFiles: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing files
			for _, file := range []string{
				"test_report_summary.txt",
				"test_report_details.txt",
				"test_report.json",
			} {
				os.Remove(filepath.Join(tempDir, file))
			}

			// Save the report
			report.Save(baseFilename, tt.enableTxt, tt.enableJson, tt.checkKey, tt.checkRow, tt.showFolderBreakdown)

			// Check that expected files exist
			for _, expectedFile := range tt.expectedFiles {
				fullPath := filepath.Join(tempDir, expectedFile)
				if _, err := os.Stat(fullPath); os.IsNotExist(err) {
					t.Errorf("Expected file %s to exist, but it doesn't", expectedFile)
				} else if err != nil {
					t.Errorf("Error checking file %s: %v", expectedFile, err)
				} else {
					// Verify file has content
					content, err := os.ReadFile(fullPath)
					if err != nil {
						t.Errorf("Error reading file %s: %v", expectedFile, err)
					} else if len(content) == 0 {
						t.Errorf("File %s is empty", expectedFile)
					}
				}
			}

			// Check that unexpected files don't exist
			allPossibleFiles := []string{
				"test_report_summary.txt",
				"test_report_details.txt",
				"test_report.json",
			}
			for _, file := range allPossibleFiles {
				found := false
				for _, expected := range tt.expectedFiles {
					if file == expected {
						found = true
						break
					}
				}
				if !found {
					fullPath := filepath.Join(tempDir, file)
					if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
						t.Errorf("Unexpected file %s exists", file)
					}
				}
			}
		})
	}
}

func TestSaveAndLog(t *testing.T) {
	// Create a temporary directory for test files
	tempDir := t.TempDir()

	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:     false,
			IsPartialReport:        false,
			FilesProcessed:         1,
			TotalFiles:             1,
			TotalRowsProcessed:     5,
			UniqueKey:              "id",
			TotalKeyOccurrences:    5,
			UniqueKeysDuplicated:   0,
			DuplicateRowInstances:  0,
			AverageRowsPerFile:     5.0,
			AverageFilesPerFolder:  1.0,
			DuplicateIDsPerFolder:  map[string]int{},
			DuplicateRowsPerFolder: map[string]int{},
			FolderDetails: map[string]FolderDetail{
				"/test": {
					ProcessedSizeBytes: 50,
					TotalSizeBytes:     50,
					FilesProcessed:     1,
					TotalFiles:         1,
					KeysFound:          5,
					RowsProcessed:      5,
				},
			},
		},
		DuplicateIDs:  map[string][]LocationInfo{},
		DuplicateRows: map[string][]LocationInfo{},
	}

	// Call SaveAndLog
	baseName := SaveAndLog(report, tempDir, true, true, true, true, true)

	// Check that the base name is reasonable
	if !strings.HasPrefix(baseName, filepath.Join(tempDir, "report-")) {
		t.Errorf("Expected base name to start with %s, got %s", filepath.Join(tempDir, "report-"), baseName)
	}

	// Check that files were created
	expectedFiles := []string{
		baseName + "_summary.txt",
		baseName + "_details.txt",
		baseName + ".json",
	}

	for _, expectedFile := range expectedFiles {
		if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
			t.Errorf("Expected file %s to exist, but it doesn't", expectedFile)
		}
	}

	// Check that the timestamp format is reasonable
	baseFileName := filepath.Base(baseName)
	if !strings.HasPrefix(baseFileName, "report-") {
		t.Errorf("Expected base file name to start with 'report-', got %s", baseFileName)
	}

	// The timestamp should be in format YYYY-MM-DD_HH-MM-SS
	timestampPart := strings.TrimPrefix(baseFileName, "report-")
	if len(timestampPart) != len("2006-01-02_15-04-05") {
		t.Errorf("Expected timestamp format YYYY-MM-DD_HH-MM-SS, got %s", timestampPart)
	}
}

func TestAnalysisReport_WithAdvancedFeatures(t *testing.T) {
	// Test report with advanced features
	searchResults := map[string]interface{}{
		"results": map[string]interface{}{
			"email_search": []interface{}{
				map[string]interface{}{
					"location": LocationInfo{FilePath: "/test/file.json", LineNumber: 1},
					"match":    "test@example.com",
				},
			},
		},
	}

	var searchResultsInterface interface{} = searchResults

	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:     false,
			IsPartialReport:        false,
			FilesProcessed:         1,
			TotalFiles:             1,
			TotalRowsProcessed:     1,
			UniqueKey:              "id",
			TotalKeyOccurrences:    1,
			UniqueKeysDuplicated:   0,
			DuplicateRowInstances:  0,
			AverageRowsPerFile:     1.0,
			AverageFilesPerFolder:  1.0,
			DuplicateIDsPerFolder:  map[string]int{},
			DuplicateRowsPerFolder: map[string]int{},
			FolderDetails:          map[string]FolderDetail{},
		},
		DuplicateIDs:  map[string][]LocationInfo{},
		DuplicateRows: map[string][]LocationInfo{},
		SearchResults: &searchResultsInterface,
	}

	// Test JSON serialization includes advanced features
	jsonStr, err := report.ToJSON()
	if err != nil {
		t.Errorf("Failed to convert report with advanced features to JSON: %v", err)
	}

	if !strings.Contains(jsonStr, "searchResults") {
		t.Error("JSON should contain searchResults field")
	}

	// Test that string representation works with advanced features
	result := report.String(false, true, true, false)
	if len(result) == 0 {
		t.Error("Expected non-empty string representation")
	}
}

func BenchmarkAnalysisReport_String(b *testing.B) {
	// Create a large report for benchmarking
	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:     false,
			IsPartialReport:        false,
			FilesProcessed:         100,
			TotalFiles:             100,
			TotalRowsProcessed:     10000,
			UniqueKey:              "id",
			TotalKeyOccurrences:    10000,
			UniqueKeysDuplicated:   50,
			DuplicateRowInstances:  100,
			AverageRowsPerFile:     100.0,
			AverageFilesPerFolder:  10.0,
			DuplicateIDsPerFolder:  make(map[string]int),
			DuplicateRowsPerFolder: make(map[string]int),
			FolderDetails:          make(map[string]FolderDetail),
		},
		DuplicateIDs:  make(map[string][]LocationInfo),
		DuplicateRows: make(map[string][]LocationInfo),
	}

	// Add some data
	for i := 0; i < 10; i++ {
		folderName := filepath.Join("/test", "folder", string(rune('A'+i)))
		report.Summary.FolderDetails[folderName] = FolderDetail{
			ProcessedSizeBytes: 1024 * 1024,
			TotalSizeBytes:     1024 * 1024,
			FilesProcessed:     10,
			TotalFiles:         10,
			KeysFound:          1000,
			RowsProcessed:      1000,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = report.String(false, true, true, true)
	}
}

func BenchmarkAnalysisReport_ToJSON(b *testing.B) {
	// Create a report for benchmarking
	report := &AnalysisReport{
		Summary: SummaryReport{
			IsValidationReport:     false,
			IsPartialReport:        false,
			FilesProcessed:         50,
			TotalFiles:             50,
			TotalRowsProcessed:     5000,
			UniqueKey:              "id",
			TotalKeyOccurrences:    5000,
			UniqueKeysDuplicated:   25,
			DuplicateRowInstances:  50,
			AverageRowsPerFile:     100.0,
			AverageFilesPerFolder:  5.0,
			DuplicateIDsPerFolder:  make(map[string]int),
			DuplicateRowsPerFolder: make(map[string]int),
			FolderDetails:          make(map[string]FolderDetail),
		},
		DuplicateIDs:  make(map[string][]LocationInfo),
		DuplicateRows: make(map[string][]LocationInfo),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = report.ToJSON()
	}
}
