// internal/deletion/interactive_purge_test.go
package deletion

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/benjaminwestern/dupe-analyser/internal/backup"
	"github.com/benjaminwestern/dupe-analyser/internal/errors"
)

func TestInteractivePurgeEngine_BasicFunctionality(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "interactive_purge_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test file
	testFile := filepath.Join(tempDir, "test.json")
	testData := []string{
		`{"id": "1", "name": "John", "age": 30}`,
		`{"id": "2", "name": "Jane", "age": 25}`,
		`{"id": "3", "name": "Bob", "age": 35}`,
		`{"id": "4", "name": "Alice", "age": 28}`,
	}

	err = os.WriteFile(testFile, []byte(fmt.Sprintf("%s\n", testData[0])+
		fmt.Sprintf("%s\n", testData[1])+
		fmt.Sprintf("%s\n", testData[2])+
		fmt.Sprintf("%s\n", testData[3])), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create backup directory
	backupDir := filepath.Join(tempDir, "backup")
	err = os.MkdirAll(backupDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create backup directory: %v", err)
	}

	// Create backup manager and error handler
	backupManager := backup.NewPurgedRowManager()
	errorHandler := errors.NewErrorHandler(100)

	// Create interactive purge engine
	config := &InteractivePurgeConfig{
		BackupDir:         backupDir,
		TempDir:           filepath.Join(tempDir, "temp"),
		MaxRetries:        3,
		EnableRollback:    true,
		ValidateIntegrity: true,
		ChunkSize:         1000,
	}

	engine, err := NewInteractivePurgeEngine(
		context.Background(),
		backupManager,
		errorHandler,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create interactive purge engine: %v", err)
	}
	defer engine.Cleanup()

	// Define records to delete (lines 2 and 4)
	recordsToDelete := map[string]map[int]bool{
		testFile: {
			2: true, // Jane
			4: true, // Alice
		},
	}

	// Process the purge
	result, err := engine.ProcessInteractivePurge(recordsToDelete, config)
	if err != nil {
		t.Fatalf("ProcessInteractivePurge failed: %v", err)
	}

	// Verify results
	if result.ProcessedFiles != 1 {
		t.Errorf("Expected 1 processed file, got %d", result.ProcessedFiles)
	}
	if result.TotalDeleted != 2 {
		t.Errorf("Expected 2 deleted records, got %d", result.TotalDeleted)
	}
	if result.TotalProcessed != 4 {
		t.Errorf("Expected 4 processed records, got %d", result.TotalProcessed)
	}
	if result.Status != "completed" {
		t.Errorf("Expected status 'completed', got '%s'", result.Status)
	}

	// Verify file contents
	modifiedContent, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read modified file: %v", err)
	}

	expectedContent := fmt.Sprintf("%s\n%s\n", testData[0], testData[2])
	if string(modifiedContent) != expectedContent {
		t.Errorf("Expected file content:\n%s\nGot:\n%s", expectedContent, string(modifiedContent))
	}

	// Verify backup was created
	backupStorage := backupManager.GetStorage(backupDir)
	storageInfo := backupStorage.GetStorageInfo()
	if len(storageInfo) == 0 {
		t.Error("Expected backup storage info to be created")
	}

	// Verify transaction tracking
	transactions := engine.GetAllTransactions()
	if len(transactions) != 1 {
		t.Errorf("Expected 1 transaction, got %d", len(transactions))
	}

	transaction := transactions[0]
	if transaction.Status != TransactionCompleted {
		t.Errorf("Expected transaction status 'completed', got '%s'", transaction.Status)
	}
	if transaction.DeletedRecords != 2 {
		t.Errorf("Expected 2 deleted records in transaction, got %d", transaction.DeletedRecords)
	}
}

func TestInteractivePurgeEngine_ErrorHandling(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "interactive_purge_error_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create backup directory
	backupDir := filepath.Join(tempDir, "backup")
	err = os.MkdirAll(backupDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create backup directory: %v", err)
	}

	// Create backup manager and error handler
	backupManager := backup.NewPurgedRowManager()
	errorHandler := errors.NewErrorHandler(100)

	// Create interactive purge engine
	config := &InteractivePurgeConfig{
		BackupDir:         backupDir,
		TempDir:           filepath.Join(tempDir, "temp"),
		MaxRetries:        3,
		EnableRollback:    true,
		ValidateIntegrity: true,
		ChunkSize:         1000,
	}

	engine, err := NewInteractivePurgeEngine(
		context.Background(),
		backupManager,
		errorHandler,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create interactive purge engine: %v", err)
	}
	defer engine.Cleanup()

	// Define records to delete from non-existent file
	nonExistentFile := filepath.Join(tempDir, "non_existent.json")
	recordsToDelete := map[string]map[int]bool{
		nonExistentFile: {
			1: true,
		},
	}

	// Process the purge
	result, err := engine.ProcessInteractivePurge(recordsToDelete, config)
	if err != nil {
		t.Fatalf("ProcessInteractivePurge failed: %v", err)
	}

	// Verify error handling
	if result.FailedFiles != 1 {
		t.Errorf("Expected 1 failed file, got %d", result.FailedFiles)
	}
	if result.Status != "failed" {
		t.Errorf("Expected status 'failed', got '%s'", result.Status)
	}
	// The error handler collects errors internally, so we check through the error handler
	if errorHandler.GetCollector().GetErrorCount() == 0 {
		t.Error("Expected errors to be recorded in error handler")
	}
}

func TestInteractivePurgeEngine_RollbackFunctionality(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "interactive_purge_rollback_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test file with invalid JSON to trigger rollback
	testFile := filepath.Join(tempDir, "test.json")
	testData := []string{
		`{"id": "1", "name": "John", "age": 30}`,
		`{"id": "2", "name": "Jane", "age": 25}`,
		`invalid_json_line`,
		`{"id": "4", "name": "Alice", "age": 28}`,
	}

	err = os.WriteFile(testFile, []byte(fmt.Sprintf("%s\n", testData[0])+
		fmt.Sprintf("%s\n", testData[1])+
		fmt.Sprintf("%s\n", testData[2])+
		fmt.Sprintf("%s\n", testData[3])), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create backup directory
	backupDir := filepath.Join(tempDir, "backup")
	err = os.MkdirAll(backupDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create backup directory: %v", err)
	}

	// Create backup manager and error handler
	backupManager := backup.NewPurgedRowManager()
	errorHandler := errors.NewErrorHandler(100)

	// Create interactive purge engine with validation enabled
	config := &InteractivePurgeConfig{
		BackupDir:         backupDir,
		TempDir:           filepath.Join(tempDir, "temp"),
		MaxRetries:        3,
		EnableRollback:    true,
		ValidateIntegrity: true, // This will trigger rollback on invalid JSON
		ChunkSize:         1000,
	}

	engine, err := NewInteractivePurgeEngine(
		context.Background(),
		backupManager,
		errorHandler,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create interactive purge engine: %v", err)
	}
	defer engine.Cleanup()

	// Define records to delete
	recordsToDelete := map[string]map[int]bool{
		testFile: {
			2: true, // Jane
		},
	}

	// Process the purge (might succeed since we handle malformed JSON gracefully)
	result, err := engine.ProcessInteractivePurge(recordsToDelete, config)

	// The engine should either fail validation or succeed with error handling
	if err != nil {
		// If it fails, verify rollback occurred
		if result.FailedFiles != 1 {
			t.Errorf("Expected 1 failed file, got %d", result.FailedFiles)
		}
	} else {
		// If it succeeds, it handled the error gracefully
		// Check that the original file is still intact or properly processed
		t.Logf("Engine handled malformed JSON gracefully with status: %s", result.Status)
	}

	// Verify original file is intact
	originalContent, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to read original file after rollback: %v", err)
	}

	expectedContent := fmt.Sprintf("%s\n%s\n%s\n%s\n", testData[0], testData[1], testData[2], testData[3])
	if string(originalContent) != expectedContent {
		t.Errorf("Original file was modified despite rollback. Expected:\n%s\nGot:\n%s", expectedContent, string(originalContent))
	}
}

func TestInteractivePurgeEngine_TransactionManagement(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "interactive_purge_transaction_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create multiple test files
	testFiles := []string{
		filepath.Join(tempDir, "test1.json"),
		filepath.Join(tempDir, "test2.json"),
	}

	for i, testFile := range testFiles {
		testData := []string{
			fmt.Sprintf(`{"id": "%d1", "name": "John%d", "age": 30}`, i, i),
			fmt.Sprintf(`{"id": "%d2", "name": "Jane%d", "age": 25}`, i, i),
		}

		err = os.WriteFile(testFile, []byte(fmt.Sprintf("%s\n%s\n", testData[0], testData[1])), 0644)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	// Create backup directory
	backupDir := filepath.Join(tempDir, "backup")
	err = os.MkdirAll(backupDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create backup directory: %v", err)
	}

	// Create backup manager and error handler
	backupManager := backup.NewPurgedRowManager()
	errorHandler := errors.NewErrorHandler(100)

	// Create interactive purge engine
	config := &InteractivePurgeConfig{
		BackupDir:         backupDir,
		TempDir:           filepath.Join(tempDir, "temp"),
		MaxRetries:        3,
		EnableRollback:    true,
		ValidateIntegrity: true,
		ChunkSize:         1000,
	}

	engine, err := NewInteractivePurgeEngine(
		context.Background(),
		backupManager,
		errorHandler,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create interactive purge engine: %v", err)
	}
	defer engine.Cleanup()

	// Define records to delete from both files
	recordsToDelete := map[string]map[int]bool{
		testFiles[0]: {
			1: true, // First line of first file
		},
		testFiles[1]: {
			2: true, // Second line of second file
		},
	}

	// Process the purge
	result, err := engine.ProcessInteractivePurge(recordsToDelete, config)
	if err != nil {
		t.Fatalf("ProcessInteractivePurge failed: %v", err)
	}

	// Verify results
	if result.ProcessedFiles != 2 {
		t.Errorf("Expected 2 processed files, got %d", result.ProcessedFiles)
	}
	if result.TotalDeleted != 2 {
		t.Errorf("Expected 2 deleted records, got %d", result.TotalDeleted)
	}

	// Verify transaction tracking
	transactions := engine.GetAllTransactions()
	if len(transactions) != 2 {
		t.Errorf("Expected 2 transactions, got %d", len(transactions))
	}

	// Verify all transactions completed successfully
	for _, transaction := range transactions {
		if transaction.Status != TransactionCompleted {
			t.Errorf("Expected transaction status 'completed', got '%s'", transaction.Status)
		}
	}

	// Test transaction log saving and loading
	transactionLogFile := filepath.Join(tempDir, "transaction_log.json")
	err = engine.SaveTransactionLog(transactionLogFile)
	if err != nil {
		t.Fatalf("Failed to save transaction log: %v", err)
	}

	// Create new engine and load transaction log
	newEngine, err := NewInteractivePurgeEngine(
		context.Background(),
		backupManager,
		errorHandler,
		config,
	)
	if err != nil {
		t.Fatalf("Failed to create new interactive purge engine: %v", err)
	}
	defer newEngine.Cleanup()

	err = newEngine.LoadTransactionLog(transactionLogFile)
	if err != nil {
		t.Fatalf("Failed to load transaction log: %v", err)
	}

	// Verify loaded transactions
	loadedTransactions := newEngine.GetAllTransactions()
	if len(loadedTransactions) != 2 {
		t.Errorf("Expected 2 loaded transactions, got %d", len(loadedTransactions))
	}
}

func TestInteractivePurgeEngine_ConfigurationOptions(t *testing.T) {
	// Create temp directory for test
	tempDir, err := os.MkdirTemp("", "interactive_purge_config_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create backup directory
	backupDir := filepath.Join(tempDir, "backup")
	err = os.MkdirAll(backupDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create backup directory: %v", err)
	}

	// Create backup manager and error handler
	backupManager := backup.NewPurgedRowManager()
	errorHandler := errors.NewErrorHandler(100)

	// Test with different configuration options
	configs := []*InteractivePurgeConfig{
		{
			BackupDir:         backupDir,
			TempDir:           filepath.Join(tempDir, "temp1"),
			MaxRetries:        1,
			EnableRollback:    false,
			ValidateIntegrity: false,
			ChunkSize:         500,
		},
		{
			BackupDir:         backupDir,
			TempDir:           filepath.Join(tempDir, "temp2"),
			MaxRetries:        5,
			EnableRollback:    true,
			ValidateIntegrity: true,
			ChunkSize:         2000,
		},
	}

	for i, config := range configs {
		t.Run(fmt.Sprintf("Config_%d", i), func(t *testing.T) {
			engine, err := NewInteractivePurgeEngine(
				context.Background(),
				backupManager,
				errorHandler,
				config,
			)
			if err != nil {
				t.Fatalf("Failed to create interactive purge engine: %v", err)
			}
			defer engine.Cleanup()

			// Verify engine was created successfully
			if engine == nil {
				t.Error("Expected engine to be created")
			}

			// Verify temp directory was created
			if _, err := os.Stat(config.TempDir); os.IsNotExist(err) {
				t.Error("Expected temp directory to be created")
			}
		})
	}
}
