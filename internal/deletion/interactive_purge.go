// Package deletion provides interactive duplicate purge workflows.
package deletion

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/backup"
	"github.com/benjaminwestern/data-refinery/internal/errors"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/safety"
)

// InteractivePurgeEngine handles interactive purging operations with comprehensive error handling.
type InteractivePurgeEngine struct {
	ctx            context.Context
	backupManager  *backup.PurgedRowManager
	errorHandler   *errors.ErrorHandler
	tempDir        string
	transactionLog map[string]*PurgeTransaction
	mutex          sync.RWMutex
}

// PurgeTransaction represents a single purge transaction with rollback capability.
type PurgeTransaction struct {
	ID               string                 `json:"id"`
	SourcePath       string                 `json:"source_path"`
	BackupPath       string                 `json:"backup_path"`
	TempPath         string                 `json:"temp_path"`
	RecordsToDelete  map[int]bool           `json:"records_to_delete"`
	Status           PurgeTransactionStatus `json:"status"`
	StartTime        time.Time              `json:"start_time"`
	EndTime          time.Time              `json:"end_time"`
	OriginalSize     int64                  `json:"original_size"`
	DeletedRecords   int64                  `json:"deleted_records"`
	ProcessedRecords int64                  `json:"processed_records"`
	ErrorCount       int64                  `json:"error_count"`
	StorageKey       string                 `json:"storage_key"`
}

// PurgeTransactionStatus represents the status of a purge transaction.
type PurgeTransactionStatus string

// PurgeTransactionStatus values describe the lifecycle of an interactive purge.
const (
	TransactionPending    PurgeTransactionStatus = "pending"
	TransactionRunning    PurgeTransactionStatus = "running"
	TransactionCompleted  PurgeTransactionStatus = "completed"
	TransactionFailed     PurgeTransactionStatus = "failed"
	TransactionRolledBack PurgeTransactionStatus = "rolled_back"
)

// InteractivePurgeConfig contains configuration for interactive purging.
type InteractivePurgeConfig struct {
	BackupDir         string `json:"backup_dir"`
	TempDir           string `json:"temp_dir"`
	MaxRetries        int    `json:"max_retries"`
	EnableRollback    bool   `json:"enable_rollback"`
	ValidateIntegrity bool   `json:"validate_integrity"`
	ChunkSize         int    `json:"chunk_size"`
}

// InteractivePurgeResult contains the result of interactive purge operation.
type InteractivePurgeResult struct {
	TransactionID  string              `json:"transaction_id"`
	TotalFiles     int                 `json:"total_files"`
	ProcessedFiles int                 `json:"processed_files"`
	FailedFiles    int                 `json:"failed_files"`
	TotalDeleted   int64               `json:"total_deleted"`
	TotalProcessed int64               `json:"total_processed"`
	TotalErrors    int64               `json:"total_errors"`
	Duration       time.Duration       `json:"duration"`
	Transactions   []*PurgeTransaction `json:"transactions"`
	Errors         []errors.ErrorEntry `json:"errors"`
	Status         string              `json:"status"`
}

// NewInteractivePurgeEngine creates a new interactive purge engine.
func NewInteractivePurgeEngine(ctx context.Context, backupManager *backup.PurgedRowManager, errorHandler *errors.ErrorHandler, config *InteractivePurgeConfig) (*InteractivePurgeEngine, error) {
	// Create temp directory
	tempDir := config.TempDir
	if tempDir == "" {
		var err error
		tempDir, err = os.MkdirTemp("", "data-refinery-purge-*")
		if err != nil {
			return nil, fmt.Errorf("failed to create temp directory: %w", err)
		}
	}

	// Ensure temp directory exists
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	return &InteractivePurgeEngine{
		ctx:            ctx,
		backupManager:  backupManager,
		errorHandler:   errorHandler,
		tempDir:        tempDir,
		transactionLog: make(map[string]*PurgeTransaction),
	}, nil
}

// ProcessInteractivePurge processes interactive purging for multiple files.
func (ipe *InteractivePurgeEngine) ProcessInteractivePurge(
	recordsToDelete map[string]map[int]bool,
	config *InteractivePurgeConfig,
) (*InteractivePurgeResult, error) {
	startTime := time.Now()
	transactionID := fmt.Sprintf("interactive_%d", startTime.Unix())

	result := &InteractivePurgeResult{
		TransactionID:  transactionID,
		TotalFiles:     len(recordsToDelete),
		ProcessedFiles: 0,
		FailedFiles:    0,
		TotalDeleted:   0,
		TotalProcessed: 0,
		TotalErrors:    0,
		Transactions:   make([]*PurgeTransaction, 0),
		Errors:         make([]errors.ErrorEntry, 0),
		Status:         "running",
	}

	// Sort files for consistent processing order
	filePaths := make([]string, 0, len(recordsToDelete))
	for filePath := range recordsToDelete {
		filePaths = append(filePaths, filePath)
	}
	sort.Strings(filePaths)

	// Process each file
	for _, filePath := range filePaths {
		if err := ipe.ctx.Err(); err != nil {
			result.Status = "cancelled"
			return result, fmt.Errorf("interactive purge cancelled: %w", err)
		}

		lineNumbers := recordsToDelete[filePath]
		transaction, err := ipe.processFile(filePath, lineNumbers, config)
		if err != nil {
			result.FailedFiles++
			result.TotalErrors++

			// Record error using error handler
			if ipe.errorHandler != nil {
				ipe.errorHandler.GetCollector().AddError(
					errors.ErrorTypeFileAccess,
					errors.SeverityError,
					fmt.Sprintf("Failed to process file %s", filePath),
					err.Error(),
					&report.LocationInfo{FilePath: filePath},
					map[string]any{"file_path": filePath},
				)
			}
			continue
		}

		result.ProcessedFiles++
		result.TotalDeleted += transaction.DeletedRecords
		result.TotalProcessed += transaction.ProcessedRecords
		result.TotalErrors += transaction.ErrorCount
		result.Transactions = append(result.Transactions, transaction)
	}

	result.Duration = time.Since(startTime)

	if result.FailedFiles == 0 {
		result.Status = "completed"
	} else if result.ProcessedFiles == 0 {
		result.Status = "failed"
	} else {
		result.Status = "partial"
	}

	return result, nil
}

// processFile processes a single file for interactive purging.
func (ipe *InteractivePurgeEngine) processFile(
	filePath string,
	lineNumbers map[int]bool,
	config *InteractivePurgeConfig,
) (*PurgeTransaction, error) {
	transactionID := fmt.Sprintf("%s_%d", filepath.Base(filePath), time.Now().Unix())

	transaction := &PurgeTransaction{
		ID:              transactionID,
		SourcePath:      filePath,
		RecordsToDelete: lineNumbers,
		Status:          TransactionPending,
		StartTime:       time.Now(),
	}

	// Lock transaction log
	ipe.mutex.Lock()
	ipe.transactionLog[transactionID] = transaction
	ipe.mutex.Unlock()

	// Generate backup path
	backupPath := filepath.Join(config.BackupDir, fmt.Sprintf("purged_%s", filepath.Base(filePath)))
	transaction.BackupPath = backupPath

	// Generate temp path
	tempPath := filepath.Join(ipe.tempDir, fmt.Sprintf("temp_%s", filepath.Base(filePath)))
	transaction.TempPath = tempPath

	// Initialize backup storage
	backupStorage := ipe.backupManager.GetStorage(config.BackupDir)
	storageKey, err := backupStorage.InitializeStorage(filePath, "interactive_purge", "Interactive duplicate purge")
	if err != nil {
		transaction.Status = TransactionFailed
		return transaction, fmt.Errorf("failed to initialize backup storage: %w", err)
	}
	transaction.StorageKey = storageKey

	// Process with transaction safety
	transaction.Status = TransactionRunning

	if err := ipe.processFileWithTransaction(transaction, backupStorage, config); err != nil {
		transaction.Status = TransactionFailed
		transaction.EndTime = time.Now()

		// Attempt rollback if enabled
		if config.EnableRollback {
			if rollbackErr := ipe.rollbackTransaction(transaction); rollbackErr != nil {
				return transaction, fmt.Errorf("failed to process file and rollback failed: %w (original error: %v)", rollbackErr, err)
			}
			transaction.Status = TransactionRolledBack
		}

		return transaction, err
	}

	// Finalize backup storage
	if err := backupStorage.FinalizeStorage(storageKey, transaction.ProcessedRecords); err != nil {
		transaction.Status = TransactionFailed
		return transaction, fmt.Errorf("failed to finalize backup storage: %w", err)
	}

	transaction.Status = TransactionCompleted
	transaction.EndTime = time.Now()

	return transaction, nil
}

// processFileWithTransaction processes a file with full transaction safety.
func (ipe *InteractivePurgeEngine) processFileWithTransaction(
	transaction *PurgeTransaction,
	backupStorage *backup.PurgedRowStorage,
	config *InteractivePurgeConfig,
) error {
	// Open source file
	sourceFile, err := os.Open(transaction.SourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer safety.Close(sourceFile, transaction.SourcePath)

	// Get file info
	fileInfo, err := sourceFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}
	transaction.OriginalSize = fileInfo.Size()

	// Create temp file
	tempFile, err := os.Create(transaction.TempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer safety.Close(tempFile, transaction.TempPath)

	// Process file line by line
	scanner := bufio.NewScanner(sourceFile)
	writer := bufio.NewWriter(tempFile)
	defer safety.Flush(writer, transaction.TempPath)

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes()
		transaction.ProcessedRecords++

		// Check if this line should be deleted
		if transaction.RecordsToDelete[lineNumber] {
			// Store in backup
			location := report.LocationInfo{
				FilePath:   transaction.SourcePath,
				LineNumber: lineNumber,
			}

			if err := backupStorage.StorePurgedRow(
				transaction.StorageKey,
				json.RawMessage(line),
				location,
				"Interactive selection",
				map[string]interface{}{
					"line_number":           lineNumber,
					"selected_for_deletion": true,
				},
			); err != nil {
				transaction.ErrorCount++

				// Record error but continue processing
				if ipe.errorHandler != nil {
					ipe.errorHandler.GetCollector().AddError(
						errors.ErrorTypeFileAccess,
						errors.SeverityWarning,
						fmt.Sprintf("Failed to backup line %d", lineNumber),
						err.Error(),
						&location,
						map[string]any{"transaction_id": transaction.ID},
					)
				}
			}
			transaction.DeletedRecords++
			continue // Skip writing to temp file (delete the line)
		}

		// Write line to temp file
		if _, err := writer.Write(line); err != nil {
			return fmt.Errorf("failed to write line to temp file: %w", err)
		}
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline to temp file: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan source file: %w", err)
	}

	// Flush writer
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush temp file: %w", err)
	}

	// Close temp file before rename
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// Validate integrity if enabled
	if config.ValidateIntegrity {
		if err := ipe.validateFileIntegrity(transaction); err != nil {
			return fmt.Errorf("file integrity validation failed: %w", err)
		}
	}

	// Atomically replace original file with temp file
	if err := os.Rename(transaction.TempPath, transaction.SourcePath); err != nil {
		return fmt.Errorf("failed to replace original file: %w", err)
	}

	return nil
}

// validateFileIntegrity validates the integrity of the processed file.
func (ipe *InteractivePurgeEngine) validateFileIntegrity(transaction *PurgeTransaction) error {
	// Open the temp file and validate JSON structure
	tempFile, err := os.Open(transaction.TempPath)
	if err != nil {
		return fmt.Errorf("failed to open temp file for validation: %w", err)
	}
	defer safety.Close(tempFile, transaction.TempPath)

	scanner := bufio.NewScanner(tempFile)
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes()

		// Try to parse as JSON to validate structure
		var jsonData map[string]interface{}
		if err := json.Unmarshal(line, &jsonData); err != nil {
			return fmt.Errorf("invalid JSON at line %d: %w", lineNumber, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan temp file during validation: %w", err)
	}

	return nil
}

// rollbackTransaction rolls back a failed transaction.
func (ipe *InteractivePurgeEngine) rollbackTransaction(transaction *PurgeTransaction) error {
	// Remove temp file if it exists
	if transaction.TempPath != "" {
		if err := os.Remove(transaction.TempPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove temp file: %w", err)
		}
	}

	// The original file should still be intact since we use atomic rename
	// No additional rollback needed for the source file

	return nil
}

// GetTransactionStatus returns the status of a specific transaction.
func (ipe *InteractivePurgeEngine) GetTransactionStatus(transactionID string) (*PurgeTransaction, bool) {
	ipe.mutex.RLock()
	defer ipe.mutex.RUnlock()

	transaction, exists := ipe.transactionLog[transactionID]
	return transaction, exists
}

// GetAllTransactions returns all transactions.
func (ipe *InteractivePurgeEngine) GetAllTransactions() []*PurgeTransaction {
	ipe.mutex.RLock()
	defer ipe.mutex.RUnlock()

	transactions := make([]*PurgeTransaction, 0, len(ipe.transactionLog))
	for _, transaction := range ipe.transactionLog {
		transactions = append(transactions, transaction)
	}

	return transactions
}

// SaveTransactionLog saves the transaction log to disk.
func (ipe *InteractivePurgeEngine) SaveTransactionLog(path string) error {
	ipe.mutex.RLock()
	defer ipe.mutex.RUnlock()

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create transaction log file: %w", err)
	}
	defer safety.Close(file, path)

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(ipe.transactionLog); err != nil {
		return fmt.Errorf("failed to encode transaction log: %w", err)
	}

	return nil
}

// LoadTransactionLog loads the transaction log from disk.
func (ipe *InteractivePurgeEngine) LoadTransactionLog(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No existing log is fine
		}
		return fmt.Errorf("failed to open transaction log file: %w", err)
	}
	defer safety.Close(file, path)

	decoder := json.NewDecoder(file)

	ipe.mutex.Lock()
	defer ipe.mutex.Unlock()

	if err := decoder.Decode(&ipe.transactionLog); err != nil {
		return fmt.Errorf("failed to decode transaction log: %w", err)
	}

	return nil
}

// Cleanup removes temporary files and cleans up resources.
func (ipe *InteractivePurgeEngine) Cleanup() error {
	// Remove temp directory
	if err := os.RemoveAll(ipe.tempDir); err != nil {
		return fmt.Errorf("failed to remove temp directory: %w", err)
	}

	// Close backup manager
	if err := ipe.backupManager.CloseAll(); err != nil {
		return fmt.Errorf("failed to close backup manager: %w", err)
	}

	return nil
}
