// Package backup provides storage helpers for preserved deduplication artefacts.
package backup

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/report"
)

// DedupStorage handles storage of deduplicated rows.
type DedupStorage struct {
	*PurgedRowStorage
	dedupConfig *DedupConfig
}

// DedupConfig contains configuration for deduplication storage.
type DedupConfig struct {
	Key          string `json:"key"`
	Strategy     string `json:"strategy"` // "first", "last", "interactive"
	BackupPath   string `json:"backup_path"`
	PreserveBest bool   `json:"preserve_best"` // Keep the "best" duplicate based on criteria
	BestCriteria string `json:"best_criteria"` // "most_complete", "newest", "oldest"
}

// NewDedupStorage creates a new deduplication storage instance.
func NewDedupStorage(basePath string, config *DedupConfig) *DedupStorage {
	return &DedupStorage{
		PurgedRowStorage: NewPurgedRowStorage(basePath),
		dedupConfig:      config,
	}
}

// ProcessDuplicates processes a set of duplicate rows and determines which to keep.
func (ds *DedupStorage) ProcessDuplicates(
	duplicateKey string,
	duplicateRows []DuplicateRow,
	sourcePath string,
) (*DedupResult, error) {
	if len(duplicateRows) < 2 {
		return &DedupResult{
			Key:             duplicateKey,
			TotalDuplicates: len(duplicateRows),
			KeptRows:        len(duplicateRows),
			PurgedRows:      0,
		}, nil
	}

	// Initialize storage
	storageKey, err := ds.InitializeStorage(sourcePath, "deduplication",
		fmt.Sprintf("Deduplication for key: %s", duplicateKey))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// Determine which rows to keep based on strategy
	keepRows, purgeRows, err := ds.selectRowsToKeep(duplicateRows)
	if err != nil {
		return nil, fmt.Errorf("failed to select rows: %w", err)
	}

	// Store purged rows
	for _, row := range purgeRows {
		if err := ds.StorePurgedRow(
			storageKey,
			row.Data,
			row.Location,
			"duplicate",
			map[string]interface{}{
				"duplicate_key": duplicateKey,
				"strategy":      ds.dedupConfig.Strategy,
				"kept_location": keepRows[0].Location,
			},
		); err != nil {
			return nil, fmt.Errorf("failed to store purged row: %w", err)
		}
	}

	// Finalize storage
	if err := ds.FinalizeStorage(storageKey, int64(len(duplicateRows))); err != nil {
		return nil, fmt.Errorf("failed to finalize storage: %w", err)
	}

	return &DedupResult{
		Key:             duplicateKey,
		TotalDuplicates: len(duplicateRows),
		KeptRows:        len(keepRows),
		PurgedRows:      len(purgeRows),
		KeptLocations:   ds.extractLocations(keepRows),
		PurgedLocations: ds.extractLocations(purgeRows),
		Strategy:        ds.dedupConfig.Strategy,
	}, nil
}

// selectRowsToKeep determines which rows to keep based on the configured strategy.
func (ds *DedupStorage) selectRowsToKeep(duplicateRows []DuplicateRow) ([]DuplicateRow, []DuplicateRow, error) {
	switch ds.dedupConfig.Strategy {
	case "first":
		return ds.selectFirst(duplicateRows)
	case "last":
		return ds.selectLast(duplicateRows)
	case "interactive":
		return ds.selectInteractive(duplicateRows)
	case "best":
		return ds.selectBest(duplicateRows)
	default:
		return nil, nil, fmt.Errorf("unknown strategy: %s", ds.dedupConfig.Strategy)
	}
}

// selectFirst keeps the first occurrence.
func (ds *DedupStorage) selectFirst(duplicateRows []DuplicateRow) ([]DuplicateRow, []DuplicateRow, error) {
	if len(duplicateRows) == 0 {
		return nil, nil, fmt.Errorf("no rows to process")
	}

	// Sort by file path and line number to ensure consistent "first"
	sorted := make([]DuplicateRow, len(duplicateRows))
	copy(sorted, duplicateRows)

	// Simple sort by file path then line number
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if ds.isRowEarlier(sorted[i], sorted[j]) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted[:1], sorted[1:], nil
}

// selectLast keeps the last occurrence.
func (ds *DedupStorage) selectLast(duplicateRows []DuplicateRow) ([]DuplicateRow, []DuplicateRow, error) {
	if len(duplicateRows) == 0 {
		return nil, nil, fmt.Errorf("no rows to process")
	}

	// Sort by file path and line number to ensure consistent "last"
	sorted := make([]DuplicateRow, len(duplicateRows))
	copy(sorted, duplicateRows)

	// Simple sort by file path then line number
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if ds.isRowEarlier(sorted[i], sorted[j]) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	return sorted[len(sorted)-1:], sorted[:len(sorted)-1], nil
}

// selectInteractive would present choices to user (placeholder for now).
func (ds *DedupStorage) selectInteractive(duplicateRows []DuplicateRow) ([]DuplicateRow, []DuplicateRow, error) {
	// For now, fallback to first strategy
	// In a full implementation, this would present a TUI for user selection
	return ds.selectFirst(duplicateRows)
}

// selectBest keeps the "best" row based on configured criteria.
func (ds *DedupStorage) selectBest(duplicateRows []DuplicateRow) ([]DuplicateRow, []DuplicateRow, error) {
	if len(duplicateRows) == 0 {
		return nil, nil, fmt.Errorf("no rows to process")
	}

	var bestRow DuplicateRow
	var bestIndex int

	switch ds.dedupConfig.BestCriteria {
	case "most_complete":
		bestRow, bestIndex = ds.findMostComplete(duplicateRows)
	case "newest":
		bestRow, bestIndex = ds.findNewest(duplicateRows)
	case "oldest":
		bestRow, bestIndex = ds.findOldest(duplicateRows)
	default:
		return ds.selectFirst(duplicateRows)
	}

	// Create result slices
	keepRows := []DuplicateRow{bestRow}
	purgeRows := make([]DuplicateRow, 0, len(duplicateRows)-1)

	for i, row := range duplicateRows {
		if i != bestIndex {
			purgeRows = append(purgeRows, row)
		}
	}

	return keepRows, purgeRows, nil
}

// findMostComplete finds the row with the most non-null fields.
func (ds *DedupStorage) findMostComplete(duplicateRows []DuplicateRow) (DuplicateRow, int) {
	if len(duplicateRows) == 0 {
		return DuplicateRow{}, -1
	}

	bestRow := duplicateRows[0]
	bestIndex := 0
	bestScore := ds.calculateCompletenessScore(bestRow)

	for i, row := range duplicateRows[1:] {
		score := ds.calculateCompletenessScore(row)
		if score > bestScore {
			bestRow = row
			bestIndex = i + 1
			bestScore = score
		}
	}

	return bestRow, bestIndex
}

// findNewest finds the row with the most recent timestamp.
func (ds *DedupStorage) findNewest(duplicateRows []DuplicateRow) (DuplicateRow, int) {
	if len(duplicateRows) == 0 {
		return DuplicateRow{}, -1
	}

	bestRow := duplicateRows[0]
	bestIndex := 0
	bestTime := ds.extractTimestamp(bestRow)

	for i, row := range duplicateRows[1:] {
		timestamp := ds.extractTimestamp(row)
		if timestamp.After(bestTime) {
			bestRow = row
			bestIndex = i + 1
			bestTime = timestamp
		}
	}

	return bestRow, bestIndex
}

// findOldest finds the row with the oldest timestamp.
func (ds *DedupStorage) findOldest(duplicateRows []DuplicateRow) (DuplicateRow, int) {
	if len(duplicateRows) == 0 {
		return DuplicateRow{}, -1
	}

	bestRow := duplicateRows[0]
	bestIndex := 0
	bestTime := ds.extractTimestamp(bestRow)

	for i, row := range duplicateRows[1:] {
		timestamp := ds.extractTimestamp(row)
		if timestamp.Before(bestTime) {
			bestRow = row
			bestIndex = i + 1
			bestTime = timestamp
		}
	}

	return bestRow, bestIndex
}

// calculateCompletenessScore calculates a score based on how complete a row is.
func (ds *DedupStorage) calculateCompletenessScore(row DuplicateRow) int {
	var data map[string]interface{}
	if err := json.Unmarshal(row.Data, &data); err != nil {
		return 0
	}

	score := 0
	for _, value := range data {
		if value != nil {
			switch v := value.(type) {
			case string:
				if v != "" {
					score++
				}
			case []interface{}:
				if len(v) > 0 {
					score++
				}
			case map[string]interface{}:
				if len(v) > 0 {
					score++
				}
			default:
				score++
			}
		}
	}

	return score
}

// extractTimestamp extracts a timestamp from a row.
func (ds *DedupStorage) extractTimestamp(row DuplicateRow) time.Time {
	var data map[string]interface{}
	if err := json.Unmarshal(row.Data, &data); err != nil {
		return time.Time{}
	}

	// Common timestamp field names
	timestampFields := []string{"timestamp", "created_at", "updated_at", "date", "time"}

	for _, field := range timestampFields {
		if value, exists := data[field]; exists {
			if timeStr, ok := value.(string); ok {
				// Try common timestamp formats
				formats := []string{
					time.RFC3339,
					"2006-01-02T15:04:05Z",
					"2006-01-02 15:04:05",
					"2006-01-02",
				}

				for _, format := range formats {
					if t, err := time.Parse(format, timeStr); err == nil {
						return t
					}
				}
			}
		}
	}

	return time.Time{}
}

// isRowEarlier determines if row1 comes before row2.
func (ds *DedupStorage) isRowEarlier(row1, row2 DuplicateRow) bool {
	if row1.Location.FilePath == row2.Location.FilePath {
		return row1.Location.LineNumber < row2.Location.LineNumber
	}
	return row1.Location.FilePath < row2.Location.FilePath
}

// extractLocations extracts location information from rows.
func (ds *DedupStorage) extractLocations(rows []DuplicateRow) []report.LocationInfo {
	locations := make([]report.LocationInfo, len(rows))
	for i, row := range rows {
		locations[i] = row.Location
	}
	return locations
}

// DuplicateRow represents a row that is a duplicate.
type DuplicateRow struct {
	Data     json.RawMessage        `json:"data"`
	Location report.LocationInfo    `json:"location"`
	Metadata map[string]interface{} `json:"metadata"`
}

// DedupResult contains the result of a deduplication operation.
type DedupResult struct {
	Key             string                `json:"key"`
	TotalDuplicates int                   `json:"total_duplicates"`
	KeptRows        int                   `json:"kept_rows"`
	PurgedRows      int                   `json:"purged_rows"`
	KeptLocations   []report.LocationInfo `json:"kept_locations"`
	PurgedLocations []report.LocationInfo `json:"purged_locations"`
	Strategy        string                `json:"strategy"`
	ProcessedAt     time.Time             `json:"processed_at"`
}

// DedupSummary contains summary statistics for deduplication.
type DedupSummary struct {
	TotalKeys      int                     `json:"total_keys"`
	DuplicateKeys  int                     `json:"duplicate_keys"`
	TotalRows      int64                   `json:"total_rows"`
	KeptRows       int64                   `json:"kept_rows"`
	PurgedRows     int64                   `json:"purged_rows"`
	Strategy       string                  `json:"strategy"`
	ProcessingTime time.Duration           `json:"processing_time"`
	ResultsByKey   map[string]*DedupResult `json:"results_by_key"`
}

// DedupProcessor handles batch deduplication operations.
type DedupProcessor struct {
	storage   *DedupStorage
	config    *DedupConfig
	summary   *DedupSummary
	startTime time.Time
}

// NewDedupProcessor creates a new deduplication processor.
func NewDedupProcessor(config *DedupConfig, basePath string) *DedupProcessor {
	return &DedupProcessor{
		storage: NewDedupStorage(basePath, config),
		config:  config,
		summary: &DedupSummary{
			Strategy:     config.Strategy,
			ResultsByKey: make(map[string]*DedupResult),
		},
		startTime: time.Now(),
	}
}

// ProcessDuplicateMap processes a map of duplicate keys to rows.
func (dp *DedupProcessor) ProcessDuplicateMap(
	duplicateMap map[string][]DuplicateRow,
	sourcePath string,
) (*DedupSummary, error) {
	dp.summary.TotalKeys = len(duplicateMap)

	for key, rows := range duplicateMap {
		if len(rows) > 1 {
			dp.summary.DuplicateKeys++
		}

		dp.summary.TotalRows += int64(len(rows))

		result, err := dp.storage.ProcessDuplicates(key, rows, sourcePath)
		if err != nil {
			return nil, fmt.Errorf("failed to process duplicates for key %s: %w", key, err)
		}

		result.ProcessedAt = time.Now()
		dp.summary.ResultsByKey[key] = result
		dp.summary.KeptRows += int64(result.KeptRows)
		dp.summary.PurgedRows += int64(result.PurgedRows)
	}

	dp.summary.ProcessingTime = time.Since(dp.startTime)
	return dp.summary, nil
}

// SaveSummary saves the deduplication summary to a file.
func (dp *DedupProcessor) SaveSummary(basePath string) error {
	data, err := json.MarshalIndent(dp.summary, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal summary: %w", err)
	}

	summaryPath := filepath.Join(basePath, "deduplication_summary.json")
	if err := os.WriteFile(summaryPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write summary: %w", err)
	}

	return nil
}

// Close closes the deduplication processor.
func (dp *DedupProcessor) Close() error {
	return dp.storage.Close()
}
