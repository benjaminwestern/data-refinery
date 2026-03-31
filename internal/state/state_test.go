// internal/state/state_test.go
package state

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
)

func TestStateManager_Basic(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Test initialization
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	state, err := sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	if state.SessionID != "test_session" {
		t.Errorf("Expected session ID 'test_session', got '%s'", state.SessionID)
	}

	if state.UniqueKey != "test_key" {
		t.Errorf("Expected unique key 'test_key', got '%s'", state.UniqueKey)
	}

	if state.Status != StatusPending {
		t.Errorf("Expected status 'pending', got '%s'", state.Status)
	}
}

func TestStateManager_SaveLoad(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	originalState, err := sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Update state
	err = sm.UpdateState(func(s *AnalysisState) error {
		s.Status = StatusRunning
		s.TotalRows = 100
		s.ProcessedFiles = 5
		s.IDLocations["test_id"] = []report.LocationInfo{
			{FilePath: "/test/file1.json", LineNumber: 10},
			{FilePath: "/test/file2.json", LineNumber: 20},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}

	// Save state
	if err := sm.SaveState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	// Create new state manager and load
	sm2, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create second state manager: %v", err)
	}
	defer sm2.Close()

	loadedState, err := sm2.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to load state: %v", err)
	}

	// Verify loaded state
	if loadedState.Status != StatusRunning {
		t.Errorf("Expected status 'running', got '%s'", loadedState.Status)
	}

	if loadedState.TotalRows != 100 {
		t.Errorf("Expected total rows 100, got %d", loadedState.TotalRows)
	}

	if loadedState.ProcessedFiles != 5 {
		t.Errorf("Expected processed files 5, got %d", loadedState.ProcessedFiles)
	}

	if len(loadedState.IDLocations["test_id"]) != 2 {
		t.Errorf("Expected 2 locations for test_id, got %d", len(loadedState.IDLocations["test_id"]))
	}

	// Check that state was loaded from file, not created new
	if loadedState.CreatedAt.Equal(originalState.CreatedAt) {
		t.Log("State was successfully loaded from file")
	}
}

func TestStateManager_ErrorHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Add error
	testErr := fmt.Errorf("test error")
	sm.AddError(testErr, "test_type", "test_source", "error")

	// Get current state
	currentState := sm.GetCurrentState()
	if len(currentState.ErrorLog) != 1 {
		t.Errorf("Expected 1 error in log, got %d", len(currentState.ErrorLog))
	}

	if currentState.LastError == nil {
		t.Error("Expected last error to be set")
	}

	if currentState.LastError.Message != "test error" {
		t.Errorf("Expected error message 'test error', got '%s'", currentState.LastError.Message)
	}

	if currentState.LastError.Type != "test_type" {
		t.Errorf("Expected error type 'test_type', got '%s'", currentState.LastError.Type)
	}
}

func TestStateManager_StatusUpdates(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Test status updates
	statuses := []Status{
		StatusRunning,
		StatusPaused,
		StatusRunning,
		StatusCompleted,
	}

	for _, status := range statuses {
		err := sm.UpdateStatus(status)
		if err != nil {
			t.Fatalf("Failed to update status to %s: %v", status, err)
		}

		currentState := sm.GetCurrentState()
		if currentState.Status != status {
			t.Errorf("Expected status %s, got %s", status, currentState.Status)
		}
	}
}

func TestStateManager_ProcessingStats(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Update processing stats
	stats := ProcessingStats{
		StartTime:             time.Now().Add(-1 * time.Hour),
		EndTime:               time.Now(),
		Duration:              time.Hour,
		TotalBytesProcessed:   1024 * 1024,
		ProcessingRate:        1024.0,
		AverageProcessingTime: time.Second,
		PeakMemoryUsage:       2048 * 1024,
		TotalErrorsRecovered:  5,
		WorkerUtilization:     0.85,
	}

	err = sm.UpdateProcessingStats(stats)
	if err != nil {
		t.Fatalf("Failed to update processing stats: %v", err)
	}

	currentState := sm.GetCurrentState()
	if currentState.ProcessingStats.TotalBytesProcessed != stats.TotalBytesProcessed {
		t.Errorf("Expected total bytes processed %d, got %d",
			stats.TotalBytesProcessed, currentState.ProcessingStats.TotalBytesProcessed)
	}

	if currentState.ProcessingStats.ProcessingRate != stats.ProcessingRate {
		t.Errorf("Expected processing rate %f, got %f",
			stats.ProcessingRate, currentState.ProcessingStats.ProcessingRate)
	}
}

func TestStateManager_AdvancedFeatures(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Update state with advanced features
	err = sm.UpdateState(func(s *AnalysisState) error {
		s.SearchResults = &search.Results{
			Results: map[string][]search.MatchResult{
				"test_target": {
					{
						Found:        true,
						Path:         "test.path",
						Value:        "test_value",
						MatchedValue: "test_value",
						Target:       "test_target",
					},
				},
			},
			Summary: search.Summary{
				TotalMatches:    1,
				MatchesByTarget: map[string]int{"test_target": 1},
				ProcessedRows:   100,
			},
		}

		s.SchemaReport = &schema.Report{
			TotalRows:   100,
			SampledRows: 10,
			SampleRate:  0.1,
		}

		s.DeletionStats = &deletion.Stats{
			TotalRows:     100,
			ProcessedRows: 80,
			DeletedRows:   20,
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}

	// Save and load state
	if err := sm.SaveState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	sm2, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create second state manager: %v", err)
	}
	defer sm2.Close()

	loadedState, err := sm2.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to load state: %v", err)
	}

	// Verify advanced features are preserved
	if loadedState.SearchResults == nil {
		t.Error("Expected search results to be preserved")
	} else if len(loadedState.SearchResults.Results) != 1 {
		t.Errorf("Expected 1 search result, got %d", len(loadedState.SearchResults.Results))
	}

	if loadedState.SchemaReport == nil {
		t.Error("Expected schema report to be preserved")
	} else if loadedState.SchemaReport.TotalRows != 100 {
		t.Errorf("Expected 100 total rows, got %d", loadedState.SchemaReport.TotalRows)
	}

	if loadedState.DeletionStats == nil {
		t.Error("Expected deletion stats to be preserved")
	} else if loadedState.DeletionStats.TotalRows != 100 {
		t.Errorf("Expected 100 total rows, got %d", loadedState.DeletionStats.TotalRows)
	}
}

func TestStateManager_AutoSave(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session", WithAutoSaveInterval(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Start auto save
	sm.StartAutoSave()

	// Update state
	err = sm.UpdateState(func(s *AnalysisState) error {
		s.TotalRows = 42
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}

	// Wait for auto save
	time.Sleep(200 * time.Millisecond)

	// Stop auto save
	sm.StopAutoSave()

	// Create new state manager and verify state was saved
	sm2, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create second state manager: %v", err)
	}
	defer sm2.Close()

	loadedState, err := sm2.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to load state: %v", err)
	}

	if loadedState.TotalRows != 42 {
		t.Errorf("Expected total rows 42 (auto-saved), got %d", loadedState.TotalRows)
	}
}

func TestStateManager_Recovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	state, err := sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Update state
	err = sm.UpdateState(func(s *AnalysisState) error {
		s.Status = StatusRunning
		s.TotalRows = 100
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}

	// Save state
	if err := sm.SaveState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	// Test recovery
	recoveredState, err := sm.RecoverFromState(state.UpdatedAt)
	if err != nil {
		t.Fatalf("Failed to recover from state: %v", err)
	}

	if recoveredState.Status != StatusRecovering {
		t.Errorf("Expected status 'recovering', got '%s'", recoveredState.Status)
	}

	if recoveredState.TotalRows != 100 {
		t.Errorf("Expected total rows 100, got %d", recoveredState.TotalRows)
	}

	// Check recovery entry in error log
	if len(recoveredState.ErrorLog) == 0 {
		t.Error("Expected recovery entry in error log")
	}

	lastError := recoveredState.ErrorLog[len(recoveredState.ErrorLog)-1]
	if lastError.Type != "recovery" {
		t.Errorf("Expected last error type 'recovery', got '%s'", lastError.Type)
	}
}

func TestStateManager_CleanupOldStates(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session", WithMaxStateHistory(2))
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Save multiple states
	for i := 0; i < 5; i++ {
		err = sm.UpdateState(func(s *AnalysisState) error {
			s.TotalRows = int64(i * 100)
			return nil
		})
		if err != nil {
			t.Fatalf("Failed to update state %d: %v", i, err)
		}

		if err := sm.SaveState(); err != nil {
			t.Fatalf("Failed to save state %d: %v", i, err)
		}

		// Small delay to ensure different timestamps
		time.Sleep(10 * time.Millisecond)
	}

	// Before cleanup
	timestamps, err := sm.ListStates()
	if err != nil {
		t.Fatalf("Failed to list states: %v", err)
	}

	if len(timestamps) < 5 {
		t.Errorf("Expected at least 5 states before cleanup, got %d", len(timestamps))
	}

	// Cleanup
	err = sm.CleanupOldStates()
	if err != nil {
		t.Fatalf("Failed to cleanup old states: %v", err)
	}

	// After cleanup
	timestamps, err = sm.ListStates()
	if err != nil {
		t.Fatalf("Failed to list states after cleanup: %v", err)
	}

	if len(timestamps) != 2 {
		t.Errorf("Expected 2 states after cleanup, got %d", len(timestamps))
	}
}

func TestStateManager_SourceMetadata(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "state_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer sm.Close()

	// Initialize state
	cfg := &config.Config{
		Key:      "test_key",
		Workers:  4,
		CheckKey: true,
		CheckRow: true,
	}

	_, err = sm.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to initialize state: %v", err)
	}

	// Add source metadata
	err = sm.UpdateState(func(s *AnalysisState) error {
		s.SourceMetadata = []SourceMetadata{
			{
				Path:        "/test/file1.json",
				Dir:         "/test",
				Size:        1024,
				Processed:   true,
				ProcessedAt: time.Now(),
				LineCount:   100,
				ErrorCount:  2,
			},
			{
				Path:       "/test/file2.json",
				Dir:        "/test",
				Size:       2048,
				Processed:  false,
				LineCount:  200,
				ErrorCount: 0,
			},
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to update state: %v", err)
	}

	// Save and load state
	if err := sm.SaveState(); err != nil {
		t.Fatalf("Failed to save state: %v", err)
	}

	sm2, err := NewStateManager(tempDir, "test_session")
	if err != nil {
		t.Fatalf("Failed to create second state manager: %v", err)
	}
	defer sm2.Close()

	loadedState, err := sm2.InitializeState(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Failed to load state: %v", err)
	}

	// Verify source metadata
	if len(loadedState.SourceMetadata) != 2 {
		t.Errorf("Expected 2 source metadata entries, got %d", len(loadedState.SourceMetadata))
		return // Don't continue if we don't have the expected number of entries
	}

	if loadedState.SourceMetadata[0].Path != "/test/file1.json" {
		t.Errorf("Expected path '/test/file1.json', got '%s'", loadedState.SourceMetadata[0].Path)
	}

	if !loadedState.SourceMetadata[0].Processed {
		t.Error("Expected first file to be processed")
	}

	if loadedState.SourceMetadata[1].Processed {
		t.Error("Expected second file to not be processed")
	}
}
