// internal/processing/row_processor.go
package processing

import (
	"fmt"
	"hash"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/hasher"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
)

// RowProcessor defines the interface for processing individual rows of data
type RowProcessor interface {
	ProcessRow(data report.JSONData, location report.LocationInfo, rowHasher hash.Hash64) error
	Close() error
}

// RowProcessorConfig holds configuration for row processing
type RowProcessorConfig struct {
	UniqueKey       string
	CheckKey        bool
	CheckRow        bool
	ValidateOnly    bool
	SearchEngine    *search.SearchEngine
	SchemaAnalyzer  *schema.SchemaAnalyzer
	DeletionEngine  *deletion.DeletionEngine
	SelectiveHasher *hasher.SelectiveHasher
}

// DefaultRowProcessor implements the RowProcessor interface
type DefaultRowProcessor struct {
	config *RowProcessorConfig

	// Data storage for duplicate detection
	idLocations             map[string][]report.LocationInfo
	idMutex                 sync.Mutex
	rowHashes               map[string][]report.LocationInfo
	rowMutex                sync.Mutex
	keysFoundPerFolder      map[string]int64
	keysFoundMutex          sync.Mutex
	rowsProcessedPerFolder  map[string]int64
	rowsProcessedMutex      sync.Mutex
	filesProcessedPerFolder map[string]map[string]bool
	filesProcessedMutex     sync.Mutex
}

// NewRowProcessor creates a new row processor with the given configuration
func NewRowProcessor(config *RowProcessorConfig) *DefaultRowProcessor {
	return &DefaultRowProcessor{
		config:                  config,
		idLocations:             make(map[string][]report.LocationInfo),
		rowHashes:               make(map[string][]report.LocationInfo),
		keysFoundPerFolder:      make(map[string]int64),
		rowsProcessedPerFolder:  make(map[string]int64),
		filesProcessedPerFolder: make(map[string]map[string]bool),
	}
}

// ProcessRow processes a single row of data through all configured processors
func (rp *DefaultRowProcessor) ProcessRow(data report.JSONData, location report.LocationInfo, rowHasher hash.Hash64) error {
	// Schema discovery
	if rp.config.SchemaAnalyzer != nil {
		rp.config.SchemaAnalyzer.AnalyzeRow(data, location.FilePath)
	}

	// Search processing
	if rp.config.SearchEngine != nil {
		rp.config.SearchEngine.Search(data, location)
	}

	// Deletion processing
	if rp.config.DeletionEngine != nil {
		if err := rp.config.DeletionEngine.ProcessRowData(data, location); err != nil {
			log.Printf("Error processing row with deletion engine: %v\n", err)
			return err
		}
	}

	// Key checking
	if rp.config.CheckKey {
		if err := rp.processKeyCheck(data, location); err != nil {
			return err
		}
	}

	// Row hashing for duplicate detection
	if rp.config.CheckRow && !rp.config.ValidateOnly {
		if err := rp.processRowHash(data, location, rowHasher); err != nil {
			return err
		}
	}

	// Update row processing statistics
	dir := folderFromPath(location.FilePath)
	rp.rowsProcessedMutex.Lock()
	rp.rowsProcessedPerFolder[dir]++
	rp.rowsProcessedMutex.Unlock()

	// Track files processed per folder
	rp.filesProcessedMutex.Lock()
	if rp.filesProcessedPerFolder[dir] == nil {
		rp.filesProcessedPerFolder[dir] = make(map[string]bool)
	}
	rp.filesProcessedPerFolder[dir][location.FilePath] = true
	rp.filesProcessedMutex.Unlock()

	return nil
}

func folderFromPath(filePath string) string {
	if strings.HasPrefix(filePath, "gs://") {
		if idx := strings.LastIndex(filePath, "/"); idx > len("gs://") {
			return filePath[:idx]
		}
		return filePath
	}
	return filepath.Dir(filePath)
}

// processKeyCheck handles key-based duplicate detection
func (rp *DefaultRowProcessor) processKeyCheck(data report.JSONData, location report.LocationInfo) error {
	if _, ok := data[rp.config.UniqueKey]; ok {
		dir := folderFromPath(location.FilePath)
		rp.keysFoundMutex.Lock()
		rp.keysFoundPerFolder[dir]++
		rp.keysFoundMutex.Unlock()

		if rp.config.ValidateOnly {
			return nil
		}

		idStr := fmt.Sprintf("%v", data[rp.config.UniqueKey])
		rp.idMutex.Lock()
		rp.idLocations[idStr] = append(rp.idLocations[idStr], location)
		rp.idMutex.Unlock()
	}
	return nil
}

// processRowHash handles row-based duplicate detection
func (rp *DefaultRowProcessor) processRowHash(data report.JSONData, location report.LocationInfo, rowHasher hash.Hash64) error {
	var hashStr string

	if rp.config.SelectiveHasher != nil {
		hashStr = rp.config.SelectiveHasher.HashRow(data)
	} else {
		// Default full row hashing
		rowHasher.Reset()
		if err := rp.hashMapToHasher(data, rowHasher); err != nil {
			return fmt.Errorf("row hash error: %w", err)
		}
		hashStr = fmt.Sprintf("%x", rowHasher.Sum64())
	}

	rp.rowMutex.Lock()
	rp.rowHashes[hashStr] = append(rp.rowHashes[hashStr], location)
	rp.rowMutex.Unlock()

	return nil
}

// hashMapToHasher converts a map to a consistent hash
func (rp *DefaultRowProcessor) hashMapToHasher(data map[string]interface{}, hasher hash.Hash64) error {
	hasher.Reset()

	// Create a consistent hash by processing keys in sorted order
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	// Sort keys for consistent hashing
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	for _, key := range keys {
		hasher.Write([]byte(key))
		hasher.Write([]byte(fmt.Sprintf("%v", data[key])))
	}

	return nil
}

// GetIDLocations returns the current ID locations for duplicate detection
func (rp *DefaultRowProcessor) GetIDLocations() map[string][]report.LocationInfo {
	rp.idMutex.Lock()
	defer rp.idMutex.Unlock()

	result := make(map[string][]report.LocationInfo)
	for k, v := range rp.idLocations {
		result[k] = append([]report.LocationInfo{}, v...)
	}
	return result
}

// GetRowHashes returns the current row hashes for duplicate detection
func (rp *DefaultRowProcessor) GetRowHashes() map[string][]report.LocationInfo {
	rp.rowMutex.Lock()
	defer rp.rowMutex.Unlock()

	result := make(map[string][]report.LocationInfo)
	for k, v := range rp.rowHashes {
		result[k] = append([]report.LocationInfo{}, v...)
	}
	return result
}

// GetKeysFoundPerFolder returns the keys found statistics per folder
func (rp *DefaultRowProcessor) GetKeysFoundPerFolder() map[string]int64 {
	rp.keysFoundMutex.Lock()
	defer rp.keysFoundMutex.Unlock()

	result := make(map[string]int64)
	for k, v := range rp.keysFoundPerFolder {
		result[k] = v
	}
	return result
}

// GetRowsProcessedPerFolder returns the rows processed statistics per folder
func (rp *DefaultRowProcessor) GetRowsProcessedPerFolder() map[string]int64 {
	rp.rowsProcessedMutex.Lock()
	defer rp.rowsProcessedMutex.Unlock()

	result := make(map[string]int64)
	for k, v := range rp.rowsProcessedPerFolder {
		result[k] = v
	}
	return result
}

// GetFilesProcessedPerFolder returns the files processed statistics per folder
func (rp *DefaultRowProcessor) GetFilesProcessedPerFolder() map[string]int {
	rp.filesProcessedMutex.Lock()
	defer rp.filesProcessedMutex.Unlock()

	result := make(map[string]int)
	for folder, files := range rp.filesProcessedPerFolder {
		result[folder] = len(files)
	}
	return result
}

// Close cleans up resources used by the row processor
func (rp *DefaultRowProcessor) Close() error {
	// Clear all maps to free memory
	rp.idMutex.Lock()
	rp.idLocations = make(map[string][]report.LocationInfo)
	rp.idMutex.Unlock()

	rp.rowMutex.Lock()
	rp.rowHashes = make(map[string][]report.LocationInfo)
	rp.rowMutex.Unlock()

	rp.keysFoundMutex.Lock()
	rp.keysFoundPerFolder = make(map[string]int64)
	rp.keysFoundMutex.Unlock()

	rp.rowsProcessedMutex.Lock()
	rp.rowsProcessedPerFolder = make(map[string]int64)
	rp.rowsProcessedMutex.Unlock()

	return nil
}
