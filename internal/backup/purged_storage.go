// internal/backup/purged_storage.go
package backup

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/benjaminwestern/dupe-analyser/internal/report"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
)

// PurgedRowStorage manages storage of purged rows with original structure preservation
type PurgedRowStorage struct {
	basePath          string
	timestamp         string
	pathResolver      *source.PathResolver
	storageFiles      map[string]*os.File
	storageWriters    map[string]io.Writer
	mutex             sync.Mutex
	operationMetadata map[string]*OperationMetadata
}

// FileFormat represents the detected file format
type FileFormat string

const (
	FormatJSON    FileFormat = "json"
	FormatNDJSON  FileFormat = "ndjson"
	FormatJSONL   FileFormat = "jsonl"
	FormatUnknown FileFormat = "unknown"
)

// detectFormatFromPath detects format from file path extension
func detectFormatFromPath(path string) FileFormat {
	lowerPath := strings.ToLower(path)

	if strings.HasSuffix(lowerPath, ".json") {
		return FormatJSON
	} else if strings.HasSuffix(lowerPath, ".ndjson") {
		return FormatNDJSON
	} else if strings.HasSuffix(lowerPath, ".jsonl") {
		return FormatJSONL
	}

	return FormatUnknown
}

// OperationMetadata contains metadata about a purge operation
type OperationMetadata struct {
	OperationType string      `json:"operation_type"`
	SourcePath    string      `json:"source_path"`
	TargetPath    string      `json:"target_path"`
	StartTime     time.Time   `json:"start_time"`
	EndTime       time.Time   `json:"end_time"`
	TotalRows     int64       `json:"total_rows"`
	PurgedRows    int64       `json:"purged_rows"`
	Format        FileFormat  `json:"format"`
	PurgeReason   string      `json:"purge_reason"`
	PurgeDetails  interface{} `json:"purge_details"`
}

// PurgedRowEntry represents a purged row with metadata
type PurgedRowEntry struct {
	OriginalRow  json.RawMessage     `json:"original_row"`
	Location     report.LocationInfo `json:"location"`
	PurgeReason  string              `json:"purge_reason"`
	PurgeDetails interface{}         `json:"purge_details"`
	Timestamp    time.Time           `json:"timestamp"`
}

// NewPurgedRowStorage creates a new purged row storage instance
func NewPurgedRowStorage(basePath string) *PurgedRowStorage {
	return &PurgedRowStorage{
		basePath:          basePath,
		timestamp:         time.Now().Format("2006-01-02_15-04-05"),
		pathResolver:      source.NewPathResolver(true, basePath),
		storageFiles:      make(map[string]*os.File),
		storageWriters:    make(map[string]io.Writer),
		operationMetadata: make(map[string]*OperationMetadata),
	}
}

// InitializeStorage initializes storage for a specific source file
func (prs *PurgedRowStorage) InitializeStorage(
	sourcePath string,
	operationType string,
	purgeReason string,
) (string, error) {
	prs.mutex.Lock()
	defer prs.mutex.Unlock()

	// Create storage key
	storageKey := prs.generateStorageKey(sourcePath, operationType)

	// Generate storage path preserving structure
	storagePath, err := prs.generateStoragePath(sourcePath, operationType)
	if err != nil {
		return "", fmt.Errorf("failed to generate storage path: %w", err)
	}

	// Create directory structure
	if err := os.MkdirAll(filepath.Dir(storagePath), 0755); err != nil {
		return "", fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Create storage file
	file, err := os.Create(storagePath)
	if err != nil {
		return "", fmt.Errorf("failed to create storage file: %w", err)
	}

	// Set up writer based on format
	var writer io.Writer
	format := detectFormatFromPath(sourcePath)

	switch format {
	case FormatJSON:
		jsonWriter := &jsonArrayWriter{file: file, isFirst: true}
		writer = jsonWriter
	case FormatNDJSON, FormatJSONL:
		writer = file
	default:
		writer = file
	}

	prs.storageFiles[storageKey] = file
	prs.storageWriters[storageKey] = writer

	// Initialize operation metadata
	prs.operationMetadata[storageKey] = &OperationMetadata{
		OperationType: operationType,
		SourcePath:    sourcePath,
		TargetPath:    storagePath,
		StartTime:     time.Now(),
		Format:        format,
		PurgeReason:   purgeReason,
	}

	return storageKey, nil
}

// StorePurgedRow stores a purged row with metadata
func (prs *PurgedRowStorage) StorePurgedRow(
	storageKey string,
	originalRow json.RawMessage,
	location report.LocationInfo,
	purgeReason string,
	purgeDetails interface{},
) error {
	prs.mutex.Lock()
	defer prs.mutex.Unlock()

	writer, exists := prs.storageWriters[storageKey]
	if !exists {
		return fmt.Errorf("storage key %s not found", storageKey)
	}

	entry := PurgedRowEntry{
		OriginalRow:  originalRow,
		Location:     location,
		PurgeReason:  purgeReason,
		PurgeDetails: purgeDetails,
		Timestamp:    time.Now(),
	}

	// Update metadata
	if metadata, exists := prs.operationMetadata[storageKey]; exists {
		metadata.PurgedRows++
	}

	// Write entry based on format
	return prs.writeEntry(writer, entry)
}

// StorePurgedRows stores multiple purged rows
func (prs *PurgedRowStorage) StorePurgedRows(
	storageKey string,
	rows []json.RawMessage,
	locations []report.LocationInfo,
	purgeReason string,
	purgeDetails interface{},
) error {
	if len(rows) != len(locations) {
		return fmt.Errorf("rows and locations count mismatch")
	}

	for i, row := range rows {
		if err := prs.StorePurgedRow(storageKey, row, locations[i], purgeReason, purgeDetails); err != nil {
			return fmt.Errorf("failed to store row %d: %w", i, err)
		}
	}

	return nil
}

// FinalizeStorage finalizes storage for a source file
func (prs *PurgedRowStorage) FinalizeStorage(storageKey string, totalRows int64) error {
	prs.mutex.Lock()
	defer prs.mutex.Unlock()

	// Update metadata
	if metadata, exists := prs.operationMetadata[storageKey]; exists {
		metadata.EndTime = time.Now()
		metadata.TotalRows = totalRows
	}

	// Finalize writer
	if writer, exists := prs.storageWriters[storageKey]; exists {
		if jsonWriter, ok := writer.(*jsonArrayWriter); ok {
			if err := jsonWriter.finalize(); err != nil {
				return fmt.Errorf("failed to finalize JSON writer: %w", err)
			}
		}
	}

	// Close file
	if file, exists := prs.storageFiles[storageKey]; exists {
		if err := file.Close(); err != nil {
			return fmt.Errorf("failed to close storage file: %w", err)
		}
		delete(prs.storageFiles, storageKey)
		delete(prs.storageWriters, storageKey)
	}

	// Write metadata file
	if err := prs.writeMetadata(storageKey); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// generateStorageKey generates a unique storage key
func (prs *PurgedRowStorage) generateStorageKey(sourcePath, operationType string) string {
	baseName := prs.pathResolver.ExtractBaseName(sourcePath)
	return fmt.Sprintf("%s_%s_%s", baseName, operationType, prs.timestamp)
}

// generateStoragePath generates storage path preserving structure
func (prs *PurgedRowStorage) generateStoragePath(sourcePath, operationType string) (string, error) {
	// Create subdirectory for operation type
	operationDir := filepath.Join(prs.basePath, "purged_data", operationType, prs.timestamp)

	var relativePath string
	if strings.HasPrefix(sourcePath, "gs://") {
		// For GCS paths, create local structure
		pathInfo, err := prs.pathResolver.ParseGCSPath(sourcePath)
		if err != nil {
			return "", fmt.Errorf("failed to parse GCS path: %w", err)
		}
		relativePath = filepath.Join("gcs", pathInfo.Bucket, pathInfo.ObjectName)
	} else {
		// For local paths, preserve directory structure
		relPath, err := prs.pathResolver.GetRelativePath(sourcePath, prs.basePath)
		if err != nil {
			// Use absolute path structure if relative fails
			absPath, absErr := filepath.Abs(sourcePath)
			if absErr != nil {
				return "", fmt.Errorf("failed to get path structure: %w", err)
			}
			relativePath = strings.TrimPrefix(absPath, "/")
		} else {
			relativePath = filepath.Join("local", relPath)
		}
	}

	// Add purged suffix to filename
	dir := filepath.Dir(relativePath)
	fileName := filepath.Base(relativePath)
	ext := filepath.Ext(fileName)
	baseName := strings.TrimSuffix(fileName, ext)

	purgedFileName := fmt.Sprintf("%s_purged%s", baseName, ext)
	storagePath := filepath.Join(operationDir, dir, purgedFileName)

	return storagePath, nil
}

// writeEntry writes a purged row entry to storage
func (prs *PurgedRowStorage) writeEntry(writer io.Writer, entry PurgedRowEntry) error {
	entryData, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal entry: %w", err)
	}

	if jsonWriter, ok := writer.(*jsonArrayWriter); ok {
		_, err := jsonWriter.writeEntry(entryData)
		return err
	}

	// For NDJSON format
	if _, err := writer.Write(entryData); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	if _, err := writer.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

// writeMetadata writes operation metadata to a separate file
func (prs *PurgedRowStorage) writeMetadata(storageKey string) error {
	metadata, exists := prs.operationMetadata[storageKey]
	if !exists {
		return fmt.Errorf("metadata for key %s not found", storageKey)
	}

	metadataPath := metadata.TargetPath + ".metadata.json"
	metadataFile, err := os.Create(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer metadataFile.Close()

	encoder := json.NewEncoder(metadataFile)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to encode metadata: %w", err)
	}

	return nil
}

// GetStorageInfo returns information about stored data
func (prs *PurgedRowStorage) GetStorageInfo() map[string]*OperationMetadata {
	prs.mutex.Lock()
	defer prs.mutex.Unlock()

	info := make(map[string]*OperationMetadata)
	for key, metadata := range prs.operationMetadata {
		// Create a copy to avoid data races
		info[key] = &OperationMetadata{
			OperationType: metadata.OperationType,
			SourcePath:    metadata.SourcePath,
			TargetPath:    metadata.TargetPath,
			StartTime:     metadata.StartTime,
			EndTime:       metadata.EndTime,
			TotalRows:     metadata.TotalRows,
			PurgedRows:    metadata.PurgedRows,
			Format:        metadata.Format,
			PurgeReason:   metadata.PurgeReason,
			PurgeDetails:  metadata.PurgeDetails,
		}
	}

	return info
}

// RestorePurgedRows restores purged rows from storage
func (prs *PurgedRowStorage) RestorePurgedRows(storageKey string) ([]PurgedRowEntry, error) {
	metadata, exists := prs.operationMetadata[storageKey]
	if !exists {
		return nil, fmt.Errorf("storage key %s not found", storageKey)
	}

	file, err := os.Open(metadata.TargetPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open storage file: %w", err)
	}
	defer file.Close()

	var entries []PurgedRowEntry

	switch metadata.Format {
	case FormatJSON:
		// Read JSON array
		decoder := json.NewDecoder(file)
		if err := decoder.Decode(&entries); err != nil {
			return nil, fmt.Errorf("failed to decode JSON array: %w", err)
		}
	case FormatNDJSON, FormatJSONL:
		// Read NDJSON
		decoder := json.NewDecoder(file)
		for decoder.More() {
			var entry PurgedRowEntry
			if err := decoder.Decode(&entry); err != nil {
				return nil, fmt.Errorf("failed to decode NDJSON entry: %w", err)
			}
			entries = append(entries, entry)
		}
	default:
		return nil, fmt.Errorf("unsupported format: %s", metadata.Format)
	}

	return entries, nil
}

// Close closes all open storage files
func (prs *PurgedRowStorage) Close() error {
	prs.mutex.Lock()
	defer prs.mutex.Unlock()

	var errors []error

	for key, file := range prs.storageFiles {
		if err := file.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close storage file %s: %w", key, err))
		}
	}

	prs.storageFiles = make(map[string]*os.File)
	prs.storageWriters = make(map[string]io.Writer)

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}

	return nil
}

// jsonArrayWriter handles JSON array format writing
type jsonArrayWriter struct {
	file    *os.File
	isFirst bool
}

func (jaw *jsonArrayWriter) Write(data []byte) (int, error) {
	return jaw.writeEntry(data)
}

func (jaw *jsonArrayWriter) writeEntry(data []byte) (int, error) {
	totalWritten := 0

	if jaw.isFirst {
		n, err := jaw.file.Write([]byte("["))
		if err != nil {
			return n, err
		}
		totalWritten += n
		jaw.isFirst = false
	} else {
		n, err := jaw.file.Write([]byte(","))
		if err != nil {
			return totalWritten, err
		}
		totalWritten += n
	}

	n, err := jaw.file.Write(data)
	if err != nil {
		return totalWritten, err
	}
	totalWritten += n

	return totalWritten, nil
}

func (jaw *jsonArrayWriter) finalize() error {
	if _, err := jaw.file.Write([]byte("]")); err != nil {
		return err
	}
	return nil
}

// PurgedRowManager manages multiple purged row storages
type PurgedRowManager struct {
	storages map[string]*PurgedRowStorage
	mutex    sync.Mutex
}

// NewPurgedRowManager creates a new purged row manager
func NewPurgedRowManager() *PurgedRowManager {
	return &PurgedRowManager{
		storages: make(map[string]*PurgedRowStorage),
	}
}

// GetStorage gets or creates a storage for a base path
func (prm *PurgedRowManager) GetStorage(basePath string) *PurgedRowStorage {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	if storage, exists := prm.storages[basePath]; exists {
		return storage
	}

	storage := NewPurgedRowStorage(basePath)
	prm.storages[basePath] = storage
	return storage
}

// CloseAll closes all storages
func (prm *PurgedRowManager) CloseAll() error {
	prm.mutex.Lock()
	defer prm.mutex.Unlock()

	var errors []error

	for basePath, storage := range prm.storages {
		if err := storage.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close storage %s: %w", basePath, err))
		}
	}

	prm.storages = make(map[string]*PurgedRowStorage)

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}

	return nil
}
