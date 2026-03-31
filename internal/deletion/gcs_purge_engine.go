// Package deletion provides local and GCS-backed duplicate purge workflows.
package deletion

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/benjaminwestern/data-refinery/internal/backup"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/safety"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

// GCSPurgeEngine handles purging operations for GCS files with streaming.
type GCSPurgeEngine struct {
	ctx           context.Context
	backupManager *backup.PurgedRowManager
	pathResolver  *source.PathResolver
}

// NewGCSPurgeEngine creates a new GCS purge engine.
func NewGCSPurgeEngine(ctx context.Context, backupManager *backup.PurgedRowManager) *GCSPurgeEngine {
	return &GCSPurgeEngine{
		ctx:           ctx,
		backupManager: backupManager,
		pathResolver:  source.NewPathResolver(true, ""),
	}
}

// IDBasedPurgeConfig contains configuration for ID-based purging.
type IDBasedPurgeConfig struct {
	TargetIDs     []string `json:"target_ids"`
	KeyPath       string   `json:"key_path"`
	CaseSensitive bool     `json:"case_sensitive"`
	BackupPath    string   `json:"backup_path"`
}

// NestedArrayPurgeConfig contains configuration for nested array purging.
type NestedArrayPurgeConfig struct {
	ArrayPath     string   `json:"array_path"`
	ItemKeyPath   string   `json:"item_key_path"`
	TargetValues  []string `json:"target_values"`
	CaseSensitive bool     `json:"case_sensitive"`
	BackupPath    string   `json:"backup_path"`
}

// PurgeResult contains the result of a purge operation.
type PurgeResult struct {
	SourcePath        string `json:"source_path"`
	TargetPath        string `json:"target_path"`
	BackupPath        string `json:"backup_path"`
	OriginalRowCount  int64  `json:"original_row_count"`
	ProcessedRowCount int64  `json:"processed_row_count"`
	DeletedRowCount   int64  `json:"deleted_row_count"`
	ModifiedRowCount  int64  `json:"modified_row_count"`
	ErrorCount        int64  `json:"error_count"`
	OperationType     string `json:"operation_type"`
}

// processFile processes a file line by line with the given processor function.
func (gpe *GCSPurgeEngine) processFile(sourcePath, targetPath string, processor func([]byte) ([]byte, bool)) error {
	// For now, just create a simple file processor that reads all lines
	// In a full implementation, this would handle GCS streaming

	// Read source file
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer safety.Close(sourceFile, sourcePath)

	// Create target file
	targetFile, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	defer safety.Close(targetFile, targetPath)

	scanner := bufio.NewScanner(sourceFile)
	writer := bufio.NewWriter(targetFile)
	defer safety.Flush(writer, targetPath)

	for scanner.Scan() {
		line := scanner.Bytes()
		if processedLine, keep := processor(line); keep && processedLine != nil {
			if _, err := writer.Write(processedLine); err != nil {
				return fmt.Errorf("failed to write line: %w", err)
			}
			if _, err := writer.WriteString("\n"); err != nil {
				return fmt.Errorf("failed to write newline: %w", err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to scan file: %w", err)
	}

	return nil
}

// ProcessIDBasedPurge processes ID-based purging for a GCS file.
func (gpe *GCSPurgeEngine) ProcessIDBasedPurge(
	sourcePath string,
	config *IDBasedPurgeConfig,
) (*PurgeResult, error) {
	// Generate target path
	targetPath, err := gpe.pathResolver.GenerateProcessedPath(sourcePath, "_id_purged")
	if err != nil {
		return nil, fmt.Errorf("failed to generate target path: %w", err)
	}

	// Initialize backup storage
	backupStorage := gpe.backupManager.GetStorage(config.BackupPath)
	storageKey, err := backupStorage.InitializeStorage(sourcePath, "id_purge", "ID-based purge")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize backup storage: %w", err)
	}

	result := &PurgeResult{
		SourcePath:    sourcePath,
		TargetPath:    targetPath,
		BackupPath:    config.BackupPath,
		OperationType: "id_purge",
	}

	// Create target ID set for faster lookup
	targetIDSet := make(map[string]bool)
	for _, id := range config.TargetIDs {
		key := id
		if !config.CaseSensitive {
			key = strings.ToLower(id)
		}
		targetIDSet[key] = true
	}

	// Process file with streaming
	lineNumber := int64(0)
	err = gpe.processFile(sourcePath, targetPath, func(line []byte) ([]byte, bool) {
		lineNumber++
		result.OriginalRowCount++

		// Parse JSON line
		var data map[string]interface{}
		if err := json.Unmarshal(line, &data); err != nil {
			result.ErrorCount++
			return line, true // Keep malformed lines
		}

		// Extract ID value
		idValue, exists := gpe.extractValueByPath(data, config.KeyPath)
		if !exists {
			result.ProcessedRowCount++
			return line, true // Keep rows without target key
		}

		// Check if ID should be deleted
		idString := fmt.Sprintf("%v", idValue)
		if !config.CaseSensitive {
			idString = strings.ToLower(idString)
		}

		if targetIDSet[idString] {
			// Store in backup before deletion
			location := report.LocationInfo{
				FilePath:   sourcePath,
				LineNumber: int(lineNumber),
			}

			if backupErr := backupStorage.StorePurgedRow(
				storageKey,
				json.RawMessage(line),
				location,
				"ID match",
				map[string]interface{}{
					"matched_id": idValue,
					"key_path":   config.KeyPath,
				},
			); backupErr != nil {
				result.ErrorCount++
			}

			result.DeletedRowCount++
			return nil, false // Delete this row
		}

		result.ProcessedRowCount++
		return line, true // Keep this row
	})
	if err != nil {
		return nil, fmt.Errorf("failed to process file: %w", err)
	}

	// Finalize backup storage
	if finalizeErr := backupStorage.FinalizeStorage(storageKey, result.OriginalRowCount); finalizeErr != nil {
		return nil, fmt.Errorf("failed to finalize backup storage: %w", finalizeErr)
	}

	return result, nil
}

// ProcessNestedArrayPurge processes nested array purging for a GCS file.
func (gpe *GCSPurgeEngine) ProcessNestedArrayPurge(
	sourcePath string,
	config *NestedArrayPurgeConfig,
) (*PurgeResult, error) {
	// Generate target path
	targetPath, err := gpe.pathResolver.GenerateProcessedPath(sourcePath, "_array_purged")
	if err != nil {
		return nil, fmt.Errorf("failed to generate target path: %w", err)
	}

	// Initialize backup storage
	backupStorage := gpe.backupManager.GetStorage(config.BackupPath)
	storageKey, err := backupStorage.InitializeStorage(sourcePath, "array_purge", "Nested array purge")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize backup storage: %w", err)
	}

	result := &PurgeResult{
		SourcePath:    sourcePath,
		TargetPath:    targetPath,
		BackupPath:    config.BackupPath,
		OperationType: "array_purge",
	}

	// Create target value set for faster lookup
	targetValueSet := make(map[string]bool)
	for _, value := range config.TargetValues {
		key := value
		if !config.CaseSensitive {
			key = strings.ToLower(value)
		}
		targetValueSet[key] = true
	}

	// Process file with streaming
	lineNumber := int64(0)
	err = gpe.processFile(sourcePath, targetPath, func(line []byte) ([]byte, bool) {
		lineNumber++
		result.OriginalRowCount++

		// Parse JSON line
		var data map[string]interface{}
		if err := json.Unmarshal(line, &data); err != nil {
			result.ErrorCount++
			return line, true // Keep malformed lines
		}

		// Extract array
		arrayValue, exists := gpe.extractValueByPath(data, config.ArrayPath)
		if !exists {
			result.ProcessedRowCount++
			return line, true // Keep rows without target array
		}

		array, ok := arrayValue.([]interface{})
		if !ok {
			result.ProcessedRowCount++
			return line, true // Keep rows where target is not an array
		}

		// Process array items
		var filteredArray []interface{}
		var deletedItems []interface{}
		hasChanges := false

		for _, item := range array {
			itemMap, ok := item.(map[string]interface{})
			if !ok {
				filteredArray = append(filteredArray, item)
				continue
			}

			// Extract item key value
			itemKeyValue, exists := gpe.extractValueByPath(itemMap, config.ItemKeyPath)
			if !exists {
				filteredArray = append(filteredArray, item)
				continue
			}

			// Check if item should be deleted
			itemKeyString := fmt.Sprintf("%v", itemKeyValue)
			if !config.CaseSensitive {
				itemKeyString = strings.ToLower(itemKeyString)
			}

			if targetValueSet[itemKeyString] {
				deletedItems = append(deletedItems, item)
				hasChanges = true
			} else {
				filteredArray = append(filteredArray, item)
			}
		}

		// If no changes, return original line
		if !hasChanges {
			result.ProcessedRowCount++
			return line, true
		}

		// Store deleted items in backup
		if len(deletedItems) > 0 {
			location := report.LocationInfo{
				FilePath:   sourcePath,
				LineNumber: int(lineNumber),
			}

			if backupErr := backupStorage.StorePurgedRow(
				storageKey,
				json.RawMessage(line),
				location,
				"Array items deleted",
				map[string]interface{}{
					"deleted_items": deletedItems,
					"array_path":    config.ArrayPath,
					"item_key_path": config.ItemKeyPath,
				},
			); backupErr != nil {
				result.ErrorCount++
			}
		}

		// Update data with filtered array
		if err := gpe.setValueByPath(data, config.ArrayPath, filteredArray); err != nil {
			result.ErrorCount++
			return line, true // Keep original on error
		}

		// Marshal modified data
		modifiedLine, err := json.Marshal(data)
		if err != nil {
			result.ErrorCount++
			return line, true // Keep original on error
		}

		result.ModifiedRowCount++
		return modifiedLine, true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to process file: %w", err)
	}

	// Finalize backup storage
	if finalizeErr := backupStorage.FinalizeStorage(storageKey, result.OriginalRowCount); finalizeErr != nil {
		return nil, fmt.Errorf("failed to finalize backup storage: %w", finalizeErr)
	}

	return result, nil
}

// extractValueByPath extracts a value from a map using a dot-separated path.
func (gpe *GCSPurgeEngine) extractValueByPath(data map[string]interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}

	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		if i == len(parts)-1 {
			// Final part - return the value
			value, exists := current[part]
			return value, exists
		}

		// Intermediate part - navigate deeper
		value, exists := current[part]
		if !exists {
			return nil, false
		}

		nextMap, ok := value.(map[string]interface{})
		if !ok {
			return nil, false
		}

		current = nextMap
	}

	return nil, false
}

// setValueByPath sets a value in a map using a dot-separated path.
func (gpe *GCSPurgeEngine) setValueByPath(data map[string]interface{}, path string, value interface{}) error {
	if path == "" {
		return fmt.Errorf("empty path")
	}

	parts := strings.Split(path, ".")
	current := data

	for i, part := range parts {
		if i == len(parts)-1 {
			// Final part - set the value
			current[part] = value
			return nil
		}

		// Intermediate part - navigate deeper
		existing, exists := current[part]
		if !exists {
			// Create intermediate object
			newMap := make(map[string]interface{})
			current[part] = newMap
			current = newMap
		} else {
			nextMap, ok := existing.(map[string]interface{})
			if !ok {
				return fmt.Errorf("path %s is not navigable", strings.Join(parts[:i+1], "."))
			}
			current = nextMap
		}
	}

	return nil
}

// PurgeProcessor handles batch purge operations.
type PurgeProcessor struct {
	engine *GCSPurgeEngine
}

// NewPurgeProcessor creates a new purge processor.
func NewPurgeProcessor(engine *GCSPurgeEngine) *PurgeProcessor {
	return &PurgeProcessor{engine: engine}
}

// ProcessBatch processes multiple purge operations in batch.
func (pp *PurgeProcessor) ProcessBatch(operations []PurgeOperation) ([]*PurgeResult, error) {
	var results []*PurgeResult
	var errors []error

	for _, op := range operations {
		var result *PurgeResult
		var err error

		switch op.Type {
		case "id_purge":
			if config, ok := op.Config.(*IDBasedPurgeConfig); ok {
				result, err = pp.engine.ProcessIDBasedPurge(op.SourcePath, config)
			} else {
				err = fmt.Errorf("invalid config type for id_purge")
			}
		case "array_purge":
			if config, ok := op.Config.(*NestedArrayPurgeConfig); ok {
				result, err = pp.engine.ProcessNestedArrayPurge(op.SourcePath, config)
			} else {
				err = fmt.Errorf("invalid config type for array_purge")
			}
		default:
			err = fmt.Errorf("unsupported operation type: %s", op.Type)
		}

		if err != nil {
			errors = append(errors, fmt.Errorf("failed to process %s: %w", op.SourcePath, err))
			continue
		}

		results = append(results, result)
	}

	if len(errors) > 0 {
		return results, fmt.Errorf("batch processing errors: %v", errors)
	}

	return results, nil
}

// PurgeOperation represents a single purge operation.
type PurgeOperation struct {
	Type       string      `json:"type"`
	SourcePath string      `json:"source_path"`
	Config     interface{} `json:"config"`
}

// Close closes the GCS purge engine.
func (gpe *GCSPurgeEngine) Close() error {
	if err := gpe.backupManager.CloseAll(); err != nil {
		return fmt.Errorf("close backup manager: %w", err)
	}

	return nil
}
