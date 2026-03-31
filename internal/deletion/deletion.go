package deletion

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/benjaminwestern/data-refinery/internal/backup"
	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/path"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/safety"
	"github.com/benjaminwestern/data-refinery/internal/search"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

// Engine handles deletion and purging operations.
type Engine struct {
	rules             []config.DeletionRule
	searchEngine      *search.Engine
	jsonPathProcessor *path.JSONPathProcessor
	outputPaths       map[string]*os.File
	gcsWriters        map[string]*GCSWriter // For GCS write-back
	outputMutex       sync.Mutex
	stats             Stats
	backupStorage     *backup.PurgedRowStorage
	backupEnabled     bool
	storageKeys       map[string]string // Maps source path to storage key
	storageKeysMutex  sync.Mutex
	ctx               context.Context // Context for GCS operations
}

// GCSWriter provides streaming write capabilities for Google Cloud Storage objects.
type GCSWriter struct {
	client     *storage.Client
	bucket     string
	objectName string
	writer     *storage.Writer
	ctx        context.Context
}

// NewGCSWriter creates a new GCS writer for streaming uploads.
func NewGCSWriter(ctx context.Context, gcsPath string) (*GCSWriter, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	bucket, objectName, err := parseGCSPath(gcsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse GCS path: %w", err)
	}

	return &GCSWriter{
		client:     client,
		bucket:     bucket,
		objectName: objectName,
		ctx:        ctx,
	}, nil
}

// Open initializes the streaming writer to GCS.
func (w *GCSWriter) Open() error {
	bucketHandle := w.client.Bucket(w.bucket)
	objectHandle := bucketHandle.Object(w.objectName)

	w.writer = objectHandle.NewWriter(w.ctx)

	// Set appropriate metadata for JSON/NDJSON files
	if strings.HasSuffix(w.objectName, ".ndjson") || strings.HasSuffix(w.objectName, ".jsonl") {
		w.writer.ContentType = "application/x-ndjson"
	} else if strings.HasSuffix(w.objectName, ".json") {
		w.writer.ContentType = "application/json"
	}

	return nil
}

// Write writes data to the GCS object.
func (w *GCSWriter) Write(data []byte) (int, error) {
	if w.writer == nil {
		return 0, fmt.Errorf("writer not opened")
	}
	n, err := w.writer.Write(data)
	if err != nil {
		return n, fmt.Errorf("write GCS object data: %w", err)
	}
	return n, nil
}

// WriteString writes a string to the GCS object.
func (w *GCSWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// WriteLine writes a line with newline to the GCS object (useful for NDJSON).
func (w *GCSWriter) WriteLine(line string) (int, error) {
	return w.WriteString(line + "\n")
}

// Close closes the writer and finalizes the upload.
func (w *GCSWriter) Close() error {
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			return fmt.Errorf("failed to close GCS writer: %w", err)
		}
		w.writer = nil
	}

	if w.client != nil {
		if err := w.client.Close(); err != nil {
			return fmt.Errorf("failed to close GCS client: %w", err)
		}
		w.client = nil
	}

	return nil
}

// parseGCSPath extracts bucket and object name from gs:// path.
func parseGCSPath(gcsPath string) (bucket, objectName string, err error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return "", "", fmt.Errorf("invalid GCS path, must start with gs://")
	}

	trimmedPath := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(trimmedPath, "/", 2)

	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid GCS path, must include bucket and object name")
	}

	bucket = parts[0]
	objectName = parts[1]

	if bucket == "" || objectName == "" {
		return "", "", fmt.Errorf("bucket and object name cannot be empty")
	}

	return bucket, objectName, nil
}

// Stats captures aggregate counts for a deletion run.
type Stats struct {
	TotalRows       int64            `json:"totalRows"`
	ProcessedRows   int64            `json:"processedRows"`
	DeletedRows     int64            `json:"deletedRows"`
	ModifiedRows    int64            `json:"modifiedRows"`
	OutputRows      int64            `json:"outputRows"`
	MatchesByTarget map[string]int64 `json:"matchesByTarget"`
	ErrorCount      int64            `json:"errorCount"`
	StartTime       time.Time        `json:"startTime"`
	EndTime         time.Time        `json:"endTime"`
}

// Result represents the result of a deletion operation.
type Result struct {
	Action      string               `json:"action"`
	OriginalRow any                  `json:"originalRow"`
	ModifiedRow any                  `json:"modifiedRow,omitempty"`
	Matches     []search.MatchResult `json:"matches"`
	Location    report.LocationInfo  `json:"location"`
	RuleName    string               `json:"ruleName"`
}

// NewDeletionEngine creates a new deletion engine.
func NewDeletionEngine(rules []config.DeletionRule, searchEngine *search.Engine) *Engine {
	return &Engine{
		rules:             rules,
		searchEngine:      searchEngine,
		jsonPathProcessor: path.NewJSONPathProcessor(),
		outputPaths:       make(map[string]*os.File),
		gcsWriters:        make(map[string]*GCSWriter),
		backupEnabled:     false,
		storageKeys:       make(map[string]string),
		ctx:               context.Background(),
		stats: Stats{
			MatchesByTarget: make(map[string]int64),
			StartTime:       time.Now(),
		},
	}
}

// NewDeletionEngineWithBackup creates a new deletion engine with backup enabled.
func NewDeletionEngineWithBackup(rules []config.DeletionRule, searchEngine *search.Engine, backupPath string) *Engine {
	backupStorage := backup.NewPurgedRowStorage(backupPath)

	return &Engine{
		rules:             rules,
		searchEngine:      searchEngine,
		jsonPathProcessor: path.NewJSONPathProcessor(),
		outputPaths:       make(map[string]*os.File),
		gcsWriters:        make(map[string]*GCSWriter),
		backupStorage:     backupStorage,
		backupEnabled:     true,
		storageKeys:       make(map[string]string),
		ctx:               context.Background(),
		stats: Stats{
			MatchesByTarget: make(map[string]int64),
			StartTime:       time.Now(),
		},
	}
}

// ProcessSource processes a source with deletion rules.
func (de *Engine) ProcessSource(ctx context.Context, src source.InputSource) error {
	reader, err := src.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open source %s: %w", src.Path(), err)
	}
	defer safety.Close(reader, src.Path())

	scanner := bufio.NewScanner(reader)
	const maxCapacity = 4 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineNumber := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("scan deletion source: %w", ctx.Err())
		default:
		}

		lineNumber++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		de.stats.TotalRows++

		var data report.JSONData
		if err := json.Unmarshal(line, &data); err != nil {
			de.stats.ErrorCount++
			continue
		}

		location := report.LocationInfo{
			FilePath:   src.Path(),
			LineNumber: lineNumber,
		}

		if err := de.processRow(data, location); err != nil {
			de.stats.ErrorCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	return nil
}

// ProcessRowData processes a single row with deletion rules
// This method is used for integrated processing with the main analyser.
func (de *Engine) ProcessRowData(data report.JSONData, location report.LocationInfo) error {
	de.stats.TotalRows++
	return de.processRowWithMatches(data, location, false)
}

// processRow processes a single row with deletion rules.
func (de *Engine) processRow(data report.JSONData, location report.LocationInfo) error {
	return de.processRowWithMatches(data, location, true)
}

func (de *Engine) processRowWithMatches(data report.JSONData, location report.LocationInfo, accumulateSearchResults bool) error {
	de.stats.ProcessedRows++

	if de.searchEngine == nil {
		return nil
	}

	var matchesByTarget map[string][]search.MatchResult
	if accumulateSearchResults {
		de.searchEngine.Search(data, location)
		results := de.searchEngine.GetResults()
		matchesByTarget = results.Results
	} else {
		matchesByTarget = de.searchEngine.MatchRow(data, location)
	}

	// Apply deletion rules
	for _, rule := range de.rules {
		matches := matchesByTarget[rule.SearchTarget]
		if len(matches) == 0 {
			continue
		}

		// Update statistics
		de.stats.MatchesByTarget[rule.SearchTarget] += int64(len(matches))

		// Apply the rule
		switch rule.Action {
		case "delete_row":
			if err := de.handleDeleteRow(data, matches, location, rule); err != nil {
				return err
			}
		case "delete_matches":
			if err := de.handleDeleteMatches(data, matches, location, rule); err != nil {
				return err
			}
		case "mark_for_deletion":
			if err := de.handleMarkForDeletion(data, matches, location, rule); err != nil {
				return err
			}
		case "delete_sub_key":
			if err := de.handleDeleteSubKey(data, matches, location, rule); err != nil {
				return err
			}
		}
	}

	return nil
}

// getOrCreateStorageKey gets or creates a storage key for a source path.
func (de *Engine) getOrCreateStorageKey(sourcePath string) (string, error) {
	de.storageKeysMutex.Lock()
	defer de.storageKeysMutex.Unlock()

	if key, exists := de.storageKeys[sourcePath]; exists {
		return key, nil
	}

	// Initialize storage for this source
	key, err := de.backupStorage.InitializeStorage(sourcePath, "deletion", "row_deletion")
	if err != nil {
		return "", fmt.Errorf("initialize backup storage: %w", err)
	}

	de.storageKeys[sourcePath] = key
	return key, nil
}

// backupRow backs up a row before deletion.
func (de *Engine) backupRow(data report.JSONData, location report.LocationInfo, action string) error {
	if !de.backupEnabled || de.backupStorage == nil {
		return nil
	}

	storageKey, err := de.getOrCreateStorageKey(location.FilePath)
	if err != nil {
		return fmt.Errorf("failed to get storage key: %w", err)
	}

	// Convert data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if err := de.backupStorage.StorePurgedRow(storageKey, jsonData, location, action, nil); err != nil {
		return fmt.Errorf("store purged row backup: %w", err)
	}

	return nil
}

// handleDeleteRow handles the delete_row action.
func (de *Engine) handleDeleteRow(data report.JSONData, matches []search.MatchResult, location report.LocationInfo, rule config.DeletionRule) error {
	// Backup the row before deletion if backup is enabled
	if err := de.backupRow(data, location, "delete_row"); err != nil {
		return fmt.Errorf("failed to backup row before deletion: %w", err)
	}

	// Skip writing this row entirely (delete it)
	de.stats.DeletedRows++

	// Optionally write to deletion log
	if rule.OutputPath != "" {
		result := Result{
			Action:      "delete_row",
			OriginalRow: data,
			Matches:     matches,
			Location:    location,
			RuleName:    rule.SearchTarget,
		}

		if err := de.writeToOutput(rule.OutputPath, result); err != nil {
			return err
		}
	}

	return nil
}

// handleDeleteMatches handles the delete_matches action.
func (de *Engine) handleDeleteMatches(data report.JSONData, matches []search.MatchResult, location report.LocationInfo, rule config.DeletionRule) error {
	originalData := de.copyData(data)
	modified := false

	for _, match := range matches {
		if de.deleteFromPath(data, match.Path) {
			modified = true
		}
	}

	if modified {
		de.stats.ModifiedRows++

		// Write modified row to output
		if rule.OutputPath != "" {
			result := Result{
				Action:      "delete_matches",
				OriginalRow: originalData,
				ModifiedRow: data,
				Matches:     matches,
				Location:    location,
				RuleName:    rule.SearchTarget,
			}

			if err := de.writeToOutput(rule.OutputPath, result); err != nil {
				return err
			}
		}
	}

	return nil
}

// handleMarkForDeletion handles the mark_for_deletion action.
func (de *Engine) handleMarkForDeletion(data report.JSONData, matches []search.MatchResult, location report.LocationInfo, rule config.DeletionRule) error {
	// Add metadata to mark for deletion
	data["_marked_for_deletion"] = true
	data["_deletion_reason"] = rule.SearchTarget
	data["_deletion_matches"] = len(matches)

	de.stats.ModifiedRows++

	// Write marked row to output
	if rule.OutputPath != "" {
		result := Result{
			Action:      "mark_for_deletion",
			OriginalRow: data,
			Matches:     matches,
			Location:    location,
			RuleName:    rule.SearchTarget,
		}

		if err := de.writeToOutput(rule.OutputPath, result); err != nil {
			return err
		}
	}

	return nil
}

// handleDeleteSubKey handles the delete_sub_key action.
func (de *Engine) handleDeleteSubKey(data report.JSONData, matches []search.MatchResult, location report.LocationInfo, rule config.DeletionRule) error {
	if rule.SubKeyPath == "" {
		return fmt.Errorf("sub_key_path is required for delete_sub_key action")
	}

	originalData := de.copyData(data)

	// Use the JSON path processor to delete sub-keys
	modified, err := de.jsonPathProcessor.DeleteSubKeyTargets(data, rule.SubKeyPath, rule.SubKeyValues)
	if err != nil {
		return fmt.Errorf("failed to delete sub-key targets: %w", err)
	}

	if modified {
		de.stats.ModifiedRows++

		// Write modified row to output
		if rule.OutputPath != "" {
			result := Result{
				Action:      "delete_sub_key",
				OriginalRow: originalData,
				ModifiedRow: data,
				Matches:     matches,
				Location:    location,
				RuleName:    rule.SearchTarget,
			}

			if err := de.writeToOutput(rule.OutputPath, result); err != nil {
				return err
			}
		}
	}

	return nil
}

// deleteFromPath removes data at the specified path.
func (de *Engine) deleteFromPath(data report.JSONData, path string) bool {
	if path == "" {
		return false
	}

	// Handle simple key deletion
	if !strings.Contains(path, ".") && !strings.Contains(path, "[") {
		if _, exists := data[path]; exists {
			delete(data, path)
			return true
		}
		return false
	}

	// Handle nested path deletion
	result := de.deleteFromNestedPath(data, path)
	return result
}

// deleteFromNestedPath removes data from nested paths.
func (de *Engine) deleteFromNestedPath(data any, path string) bool {
	if path == "" {
		return false
	}

	// Parse the path
	parts := parsePath(path)
	if len(parts) == 0 {
		return false
	}

	// Navigate to the parent and delete the final key
	current := data
	for _, part := range parts[:len(parts)-1] {
		switch v := current.(type) {
		case report.JSONData:
			if next, exists := v[part.key]; exists {
				current = next
			} else {
				return false
			}
		case map[string]any:
			if next, exists := v[part.key]; exists {
				current = next
			} else {
				return false
			}
		case []any:
			if part.index >= 0 && part.index < len(v) {
				current = v[part.index]
			} else {
				return false
			}
		default:
			return false
		}
	}

	// Delete the final element
	finalPart := parts[len(parts)-1]
	switch v := current.(type) {
	case report.JSONData:
		if _, exists := v[finalPart.key]; exists {
			delete(v, finalPart.key)
			return true
		}
	case map[string]any:
		if _, exists := v[finalPart.key]; exists {
			delete(v, finalPart.key)
			return true
		}
	case []any:
		if finalPart.index >= 0 && finalPart.index < len(v) {
			// Note: For arrays, we need to actually modify the parent
			// This is a limitation of Go - we can't modify the slice in place
			// without a reference to the parent
			// For now, we'll just return false for array indices
			return false
		}
	}

	return false
}

// pathPart represents a part of a path.
type pathPart struct {
	key   string
	index int
}

// parsePath parses a path string into parts.
func parsePath(path string) []pathPart {
	var parts []pathPart
	// This is a simplified path parser
	// In production, you'd want a more robust parser

	// For now, just handle simple dot notation
	keys := strings.Split(path, ".")
	for _, key := range keys {
		if key != "" {
			parts = append(parts, pathPart{key: key, index: -1})
		}
	}

	return parts
}

// writeToOutput writes data to the specified output file or GCS location.
func (de *Engine) writeToOutput(outputPath string, data any) error {
	de.outputMutex.Lock()
	defer de.outputMutex.Unlock()

	if strings.HasPrefix(outputPath, "gs://") {
		return de.writeToGCSOutputLocked(outputPath, data)
	}

	return de.writeToLocalOutputLocked(outputPath, data)
}

func (de *Engine) writeToGCSOutputLocked(outputPath string, data any) error {
	gcsWriter, err := de.gcsWriterForOutputLocked(outputPath)
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	if _, err := gcsWriter.WriteLine(string(jsonData)); err != nil {
		return fmt.Errorf("failed to write to GCS output: %w", err)
	}

	return nil
}

func (de *Engine) gcsWriterForOutputLocked(outputPath string) (*GCSWriter, error) {
	if gcsWriter, exists := de.gcsWriters[outputPath]; exists {
		return gcsWriter, nil
	}

	gcsWriter, err := NewGCSWriter(de.ctx, outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS writer for %s: %w", outputPath, err)
	}
	if err := gcsWriter.Open(); err != nil {
		return nil, fmt.Errorf("failed to open GCS writer for %s: %w", outputPath, err)
	}

	de.gcsWriters[outputPath] = gcsWriter
	return gcsWriter, nil
}

func (de *Engine) writeToLocalOutputLocked(outputPath string, data any) error {
	file, err := de.localOutputFileLocked(outputPath)
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	if _, err := file.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write to output file: %w", err)
	}
	if _, err := file.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline to output file: %w", err)
	}

	return nil
}

func (de *Engine) localOutputFileLocked(outputPath string) (*os.File, error) {
	if file, exists := de.outputPaths[outputPath]; exists {
		return file, nil
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o700); err != nil {
		return nil, fmt.Errorf("failed to create output directory for %s: %w", outputPath, err)
	}

	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file %s: %w", outputPath, err)
	}
	if err := file.Chmod(0o600); err != nil {
		safety.Close(file, outputPath)
		return nil, fmt.Errorf("failed to set output file permissions for %s: %w", outputPath, err)
	}

	de.outputPaths[outputPath] = file
	return file, nil
}

// copyData creates a deep copy of the data.
func (de *Engine) copyData(data report.JSONData) report.JSONData {
	result := make(report.JSONData)
	for k, v := range data {
		result[k] = de.copyValue(v)
	}
	return result
}

// copyValue creates a deep copy of a value.
func (de *Engine) copyValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		result := make(map[string]any)
		for k, val := range v {
			result[k] = de.copyValue(val)
		}
		return result
	case []any:
		result := make([]any, len(v))
		for i, val := range v {
			result[i] = de.copyValue(val)
		}
		return result
	default:
		return value
	}
}

// Close closes all output files and finalizes statistics.
func (de *Engine) Close() error {
	// Finalize backup storage if enabled
	if de.backupEnabled && de.backupStorage != nil {
		// Finalize all storage keys
		for sourcePath, storageKey := range de.storageKeys {
			if err := de.backupStorage.FinalizeStorage(storageKey, 0); err != nil {
				return fmt.Errorf("failed to finalize storage for %s: %w", sourcePath, err)
			}
		}

		// Close backup storage
		if err := de.backupStorage.Close(); err != nil {
			return fmt.Errorf("failed to close backup storage: %w", err)
		}
	}

	// Close output files
	de.outputMutex.Lock()
	defer de.outputMutex.Unlock()

	var errors []error

	// Close local files
	for path, file := range de.outputPaths {
		if err := file.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close file %s: %w", path, err))
		}
	}

	// Close GCS writers
	for path, writer := range de.gcsWriters {
		if err := writer.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close GCS writer %s: %w", path, err))
		}
	}

	de.stats.EndTime = time.Now()

	if len(errors) > 0 {
		return fmt.Errorf("errors closing files: %v", errors)
	}

	return nil
}

// GetStats returns the current deletion statistics.
func (de *Engine) GetStats() Stats {
	stats := de.stats
	if stats.EndTime.IsZero() {
		stats.EndTime = time.Now()
	}
	return stats
}

// BatchDeletionProcessor processes multiple sources with deletion rules.
type BatchDeletionProcessor struct {
	engine  *Engine
	workers int
}

// NewBatchDeletionProcessor creates a new batch deletion processor.
func NewBatchDeletionProcessor(engine *Engine, workers int) *BatchDeletionProcessor {
	return &BatchDeletionProcessor{
		engine:  engine,
		workers: workers,
	}
}

// ProcessSources processes multiple sources in parallel.
func (bdp *BatchDeletionProcessor) ProcessSources(ctx context.Context, sources []source.InputSource) error {
	if len(sources) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	sourceChan := make(chan source.InputSource, bdp.workers)
	errorChan := make(chan error, len(sources))

	// Start workers
	for i := 0; i < bdp.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for src := range sourceChan {
				if err := bdp.engine.ProcessSource(ctx, src); err != nil {
					errorChan <- err
				}
			}
		}()
	}

	// Send sources to workers
	go func() {
		defer close(sourceChan)
		for _, src := range sources {
			select {
			case sourceChan <- src:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()
	close(errorChan)

	// Collect errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("batch processing errors: %v", errors)
	}

	return nil
}

// ReportGenerator generates reports for deletion operations.
type ReportGenerator struct {
	stats   Stats
	results []Result
}

// NewDeletionReportGenerator creates a new deletion report generator.
func NewDeletionReportGenerator(stats Stats) *ReportGenerator {
	return &ReportGenerator{
		stats:   stats,
		results: make([]Result, 0),
	}
}

// AddResult adds a deletion result to the report.
func (drg *ReportGenerator) AddResult(result Result) {
	drg.results = append(drg.results, result)
}

// GenerateReport generates a comprehensive deletion report.
func (drg *ReportGenerator) GenerateReport() Report {
	duration := drg.stats.EndTime.Sub(drg.stats.StartTime)

	return Report{
		Stats:       drg.stats,
		Results:     drg.results,
		Duration:    duration,
		GeneratedAt: time.Now(),
	}
}

// Report represents a comprehensive deletion report.
type Report struct {
	Stats       Stats         `json:"stats"`
	Results     []Result      `json:"results"`
	Duration    time.Duration `json:"duration"`
	GeneratedAt time.Time     `json:"generatedAt"`
}

// SaveReport saves the deletion report to a file.
func (dr *Report) SaveReport(filename string) error {
	data, err := json.MarshalIndent(dr, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(filename), 0o700); err != nil {
		return fmt.Errorf("failed to create report directory: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o600); err != nil {
		return fmt.Errorf("write deletion report: %w", err)
	}

	return nil
}
