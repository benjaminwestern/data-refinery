// Package analyser coordinates analysis runs over discovered input sources.
package analyser

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/errors"
	"github.com/benjaminwestern/data-refinery/internal/hasher"
	"github.com/benjaminwestern/data-refinery/internal/input"
	"github.com/benjaminwestern/data-refinery/internal/memory"
	"github.com/benjaminwestern/data-refinery/internal/processing"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
	"github.com/benjaminwestern/data-refinery/internal/source"
	"github.com/benjaminwestern/data-refinery/internal/state"
)

// Analyser holds the state and configuration for an analysis run.
type Analyser struct {
	config              *config.Config
	processedPaths      map[string]bool
	processedPathsMutex sync.Mutex

	// Atomic counters
	ProcessedFiles *atomic.Int32
	TotalRows      *atomic.Int64
	CurrentFolder  *atomic.Value

	// Core components
	errorHandler   *errors.ErrorCollector
	stateManager   *state.Manager
	errorRecovery  *processing.ErrorRecoverySystem
	circuitBreaker *processing.CircuitBreaker

	// Advanced features (always available)
	searchEngine    *search.Engine
	deletionEngine  *deletion.Engine
	schemaAnalyzer  *schema.Analyzer
	selectiveHasher *hasher.SelectiveHasher

	// Memory management
	memoryManager *memory.Manager

	// Processing components
	rowProcessor    *processing.DefaultRowProcessor
	reportGenerator *processing.ReportGenerator
}

// New creates a new, configured Analyser instance with unified configuration.
func New(cfg *config.Config) (*Analyser, error) {
	// Use the internal validation function for now
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Set default error handling max errors if not provided
	maxErrors := 1000
	if cfg.ErrorHandling != nil && cfg.ErrorHandling.MaxErrorsPerFile > 0 {
		maxErrors = cfg.ErrorHandling.MaxErrorsPerFile
	}

	a := &Analyser{
		config:         cfg,
		processedPaths: make(map[string]bool),
		ProcessedFiles: new(atomic.Int32),
		TotalRows:      new(atomic.Int64),
		CurrentFolder:  new(atomic.Value),
		errorHandler:   errors.NewErrorCollector(maxErrors),
	}

	// Initialize memory manager
	maxMemoryMB := int64(2048) // 2GB default
	if cfg.MemoryManagement != nil && cfg.MemoryManagement.MaxMemoryUsage > 0 {
		maxMemoryMB = cfg.MemoryManagement.MaxMemoryUsage / (1024 * 1024) // Convert bytes to MB
	}
	a.memoryManager = memory.NewMemoryManager(maxMemoryMB)

	// Initialize advanced error recovery system
	a.errorRecovery = processing.NewErrorRecoverySystem(processing.ErrorRecoveryConfig{
		MaxConcurrentRecoveries:   10,
		RecoveryTimeout:           30 * time.Second,
		MaxRecoveryAttempts:       3,
		EnableCircuitBreaker:      true,
		EnableRetry:               true,
		EnableFallback:            true,
		EnableGracefulDegradation: true,
		ResourceLimits: &processing.ResourceLimits{
			MaxCPUUsage:    0.8,
			MaxMemoryUsage: 1024 * 1024 * 1024, // 1GB
			MaxGoroutines:  1000,
			MaxDuration:    5 * time.Minute,
		},
		HistorySize: 100,
	})

	// Initialize circuit breaker
	a.circuitBreaker = processing.NewCircuitBreaker("analyser", processing.DefaultCircuitBreakerConfig())

	// Initialize search engine if advanced config has targets
	if cfg.Advanced != nil && len(cfg.Advanced.SearchTargets) > 0 {
		searchEngine, err := search.NewSearchEngine(cfg.Advanced.SearchTargets)
		if err != nil {
			return nil, fmt.Errorf("failed to create search engine: %w", err)
		}
		a.searchEngine = searchEngine
	}

	// Initialize deletion engine if advanced config has rules
	if cfg.Advanced != nil && len(cfg.Advanced.DeletionRules) > 0 && a.searchEngine != nil {
		a.deletionEngine = deletion.NewDeletionEngine(cfg.Advanced.DeletionRules, a.searchEngine)
	}

	// Initialize schema analyzer if advanced config has schema discovery enabled
	if cfg.Advanced != nil && cfg.Advanced.SchemaDiscovery.Enabled {
		a.schemaAnalyzer = schema.NewSchemaAnalyzer(cfg.Advanced.SchemaDiscovery)
	}

	// Initialize selective hasher with advanced config or default
	var hashingStrategy config.HashingStrategy
	if cfg.Advanced != nil {
		hashingStrategy = cfg.Advanced.HashingStrategy
	} else {
		// Default hashing strategy
		hashingStrategy = config.HashingStrategy{
			Mode:      "full_row",
			Algorithm: "fnv",
			Normalize: false,
		}
	}
	a.selectiveHasher = hasher.NewSelectiveHasher(hashingStrategy)

	// Initialize processing components
	a.rowProcessor = processing.NewRowProcessor(&processing.RowProcessorConfig{
		UniqueKey:       cfg.Key,
		CheckKey:        cfg.CheckKey,
		CheckRow:        cfg.CheckRow,
		ValidateOnly:    cfg.ValidateOnly,
		SearchEngine:    a.searchEngine,
		SchemaAnalyzer:  a.schemaAnalyzer,
		DeletionEngine:  a.deletionEngine,
		SelectiveHasher: a.selectiveHasher,
	})

	// Initialize report generator
	a.reportGenerator = processing.NewReportGenerator(
		a.searchEngine,
		a.schemaAnalyzer,
		a.deletionEngine,
	)

	// Configure error handler features
	enableRecovery := true
	enableCircuitBreaker := true
	logErrors := true

	if cfg.ErrorHandling != nil {
		enableRecovery = cfg.ErrorHandling.ContinueOnError
		enableCircuitBreaker = cfg.ErrorHandling.CircuitBreakerThreshold > 0
		// Use logErrors as a proxy for error handling being enabled
	}

	a.errorHandler.EnableFeatures(enableRecovery, enableCircuitBreaker, logErrors)

	if cfg.ErrorHandling != nil && cfg.ErrorHandling.CircuitBreakerTimeout > 0 {
		a.errorHandler.SetCircuitBreakerTimeout(cfg.ErrorHandling.CircuitBreakerTimeout)
	}

	return a, nil
}

// validateConfig provides basic validation and mirrors the internal checks for now.
func validateConfig(cfg *config.Config) error {
	if cfg.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if cfg.Workers > 100 {
		return fmt.Errorf("workers cannot exceed 100")
	}
	if cfg.Key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if cfg.LogPath == "" {
		return fmt.Errorf("log path cannot be empty")
	}
	return nil
}

// handleRecoverableError processes errors through the advanced error recovery system.
func (a *Analyser) handleRecoverableError(ctx context.Context, err error, operation string) error {
	if a.errorRecovery == nil {
		return err
	}

	// Try to recover from the error
	recoveryResult, recoveryErr := a.errorRecovery.Recover(ctx, err)
	if recoveryErr == nil && recoveryResult.Success {
		log.Printf("Successfully recovered from error in %s: %s", operation, recoveryResult.RecoveredBy)
		return nil
	}

	// If recovery failed, log the error and return it
	log.Printf("Failed to recover from error in %s: %v", operation, err)
	return err
}

// StreamingProcessor manages streaming patterns for large datasets.
type StreamingProcessor struct {
	batchSize       int
	batchProcessor  func([]StreamingItem) error
	memoryManager   *memory.Manager
	currentBatch    []StreamingItem
	processedItems  int64
	memoryThreshold int64
	flushThreshold  int
}

// StreamingItem represents a single item in the streaming pipeline.
type StreamingItem struct {
	Data       report.JSONData
	Location   report.LocationInfo
	LineNumber int
}

// NewStreamingProcessor creates a new streaming processor.
func NewStreamingProcessor(batchSize int, memoryManager *memory.Manager) *StreamingProcessor {
	return &StreamingProcessor{
		batchSize:       batchSize,
		memoryManager:   memoryManager,
		currentBatch:    make([]StreamingItem, 0, batchSize),
		memoryThreshold: 500 * 1024 * 1024, // 500MB threshold
		flushThreshold:  batchSize / 2,     // Flush when half full if memory pressure
	}
}

// AddItem adds an item to the streaming processor.
func (sp *StreamingProcessor) AddItem(item StreamingItem) error {
	sp.currentBatch = append(sp.currentBatch, item)

	// Check if we need to flush due to batch size or memory pressure
	shouldFlush := len(sp.currentBatch) >= sp.batchSize

	if sp.memoryManager != nil {
		sp.memoryManager.CheckMemoryUsage()
		if sp.memoryManager.ShouldReduceMemory() && len(sp.currentBatch) >= sp.flushThreshold {
			shouldFlush = true
		}
	}

	if shouldFlush {
		return sp.Flush()
	}

	return nil
}

// Flush processes the current batch.
func (sp *StreamingProcessor) Flush() error {
	if len(sp.currentBatch) == 0 {
		return nil
	}

	if sp.batchProcessor != nil {
		if err := sp.batchProcessor(sp.currentBatch); err != nil {
			return err
		}
	}

	sp.processedItems += int64(len(sp.currentBatch))
	sp.currentBatch = sp.currentBatch[:0] // Reset batch but keep capacity

	// Force GC after processing large batches to free memory
	if sp.memoryManager != nil && sp.processedItems%10000 == 0 {
		sp.memoryManager.ForceGC()
	}

	return nil
}

// Close finalizes the streaming processor.
func (sp *StreamingProcessor) Close() error {
	return sp.Flush()
}

// executeWithCircuitBreaker executes an operation with circuit breaker protection.
func (a *Analyser) executeWithCircuitBreaker(ctx context.Context, operation string, fn func() error) error {
	if a.circuitBreaker == nil {
		return fn()
	}

	if err := a.circuitBreaker.Execute(ctx, fn); err != nil {
		return fmt.Errorf("%s failed: %w", operation, err)
	}

	return nil
}

// GetUnprocessedSources filters a list of all sources against the ones that have
// already been successfully processed by this analyser instance.
func (a *Analyser) GetUnprocessedSources(allSources []source.InputSource) []source.InputSource {
	a.processedPathsMutex.Lock()
	defer a.processedPathsMutex.Unlock()

	var unprocessed []source.InputSource
	for _, s := range allSources {
		if !a.processedPaths[s.Path()] {
			unprocessed = append(unprocessed, s)
		}
	}
	return unprocessed
}

// Run executes the analysis process on a given set of sources and returns a full report.
func (a *Analyser) Run(ctx context.Context, sources []source.InputSource) *report.AnalysisReport {
	// Initialize state management if configured
	if a.config.StateManagement != nil && a.config.StateManagement.Enabled {
		if err := a.initializeStateManagement(ctx); err != nil {
			log.Printf("Warning: Failed to initialize state management: %v", err)
		}
	}

	// Update state to running if state manager is available
	if a.stateManager != nil {
		if err := a.stateManager.UpdateStatus(state.StatusRunning); err != nil {
			log.Printf("Failed to update analysis state to %q: %v", state.StatusRunning, err)
		}
	}

	var workerWg sync.WaitGroup
	sourceChan := make(chan source.InputSource, a.config.Workers)

	for i := 0; i < a.config.Workers; i++ {
		workerWg.Add(1)
		go a.worker(ctx, sourceChan, &workerWg)
	}

	go func() {
		defer close(sourceChan)
	feedLoop:
		for _, s := range sources {
			select {
			case sourceChan <- s:
			case <-ctx.Done():
				break feedLoop
			}
		}
	}()

	workerWg.Wait()

	// Update state to completed if state manager is available
	if a.stateManager != nil {
		status := state.StatusCompleted
		if ctx.Err() != nil {
			status = state.StatusCancelled
		}
		if err := a.stateManager.UpdateStatus(status); err != nil {
			log.Printf("Failed to update analysis state to %q: %v", status, err)
		}
	}

	return a.generateReport(sources, ctx.Err() != nil, a.config.ValidateOnly)
}

func (a *Analyser) worker(ctx context.Context, sourceChan <-chan source.InputSource, wg *sync.WaitGroup) {
	defer wg.Done()
	for src := range sourceChan {
		select {
		case <-ctx.Done():
			return
		default:
			a.processSource(ctx, src)
		}
	}
}

func (a *Analyser) processSource(ctx context.Context, src source.InputSource) {
	a.CurrentFolder.Store(src.Dir())
	// Check memory pressure before processing
	if a.memoryManager != nil {
		a.memoryManager.CheckMemoryUsage()
		if a.memoryManager.ShouldReduceMemory() {
			log.Printf("Memory pressure detected, forcing GC")
			a.memoryManager.ForceGC()
		}
	}

	bufferSize := 4 * 1024 * 1024 // 4MB default
	maxCapacity := 4 * 1024 * 1024
	if a.config.Performance != nil && a.config.Performance.BufferSize > 0 {
		bufferSize = a.config.Performance.BufferSize
	}
	if a.config.Performance != nil && a.config.Performance.MaxBufferSize > 0 {
		maxCapacity = a.config.Performance.MaxBufferSize
	}
	if maxCapacity < bufferSize {
		maxCapacity = bufferSize
	}

	reader, err := newAnalysisReader(ctx, src, bufferSize, maxCapacity, a.config.XMLRecordPath)
	if err != nil {
		log.Printf("Error opening source %q: %v\n", src.Path(), err)
		return
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("Error closing source %q: %v\n", src.Path(), closeErr)
		}
	}()

	// Use the row processor for processing
	rowHasher := fnv.New64a()
	recordCount := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		record, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Reader error in source %q: %v\n", src.Path(), err)
			return
		}
		if record.DecodeErr != nil {
			log.Printf("Error decoding JSON on line %d in source %q: %v\n", record.LineNumber, src.Path(), record.DecodeErr)
			continue
		}

		a.TotalRows.Add(1)
		recordCount++

		location := report.LocationInfo{
			FilePath:   src.Path(),
			LineNumber: record.LineNumber,
		}

		// Use the row processor for all processing
		if a.rowProcessor != nil {
			err := a.executeWithCircuitBreaker(ctx, "row_processing", func() error {
				return a.rowProcessor.ProcessRow(record.Data, location, rowHasher)
			})
			if err != nil {
				if recoveredErr := a.handleRecoverableError(ctx, err, "row processing"); recoveredErr != nil {
					log.Printf("Error processing row: %v\n", err)
				}
			}
		}

		// Periodic memory pressure check
		if a.memoryManager != nil && recordCount%1000 == 0 {
			a.memoryManager.CheckMemoryUsage()
			if a.memoryManager.ShouldReduceMemory() {
				log.Printf("Memory pressure detected during processing, forcing GC")
				a.memoryManager.ForceGC()
			}
		}
	}

	a.processedPathsMutex.Lock()
	a.processedPaths[src.Path()] = true
	a.processedPathsMutex.Unlock()
	a.ProcessedFiles.Add(1)
}

func newAnalysisReader(ctx context.Context, src source.InputSource, bufferSize, maxCapacity int, xmlRecordPath string) (input.Reader, error) {
	format := input.DetectFormatFromPath(src.Path())
	opts := []input.ReaderOption{
		input.WithScanBuffer(bufferSize, maxCapacity),
	}
	switch format {
	case input.FormatJSON:
		opts = append(opts, input.WithJSONMode(input.JSONModeLineStream), input.WithDecodeErrorsAsRecords())
	case input.FormatNDJSON, input.FormatJSONL:
		opts = append(opts, input.WithDecodeErrorsAsRecords())
	case input.FormatXML:
		if strings.TrimSpace(xmlRecordPath) != "" {
			opts = append(opts, input.WithXMLRecordPath(xmlRecordPath))
		}
	case input.FormatCSV, input.FormatTSV, input.FormatXLSX:
	case input.FormatUnknown:
		return nil, fmt.Errorf("unsupported analysis input format for %s", src.Path())
	}

	reader, err := input.NewReaderFromSource(ctx, src, opts...)
	if err != nil {
		return nil, fmt.Errorf("create analysis reader for %s: %w", src.Path(), err)
	}

	return reader, nil
}

func (a *Analyser) generateReport(sources []source.InputSource, wasCancelled, isValidation bool) *report.AnalysisReport {
	if a.reportGenerator != nil {
		return a.reportGenerator.GenerateReport(
			sources,
			a.rowProcessor,
			a.TotalRows.Load(),
			int64(a.ProcessedFiles.Load()),
			a.config.Key,
			isValidation,
			wasCancelled,
		)
	}

	// Fallback to basic report for non-advanced mode
	return &report.AnalysisReport{
		DuplicateIDs:  make(map[string][]report.LocationInfo),
		DuplicateRows: make(map[string][]report.LocationInfo),
		Summary: report.SummaryReport{
			IsValidationReport: isValidation,
			IsPartialReport:    wasCancelled,
			FilesProcessed:     a.ProcessedFiles.Load(),
			TotalFiles:         len(sources),
			TotalRowsProcessed: a.TotalRows.Load(),
			UniqueKey:          a.config.Key,
		},
	}
}

// GetSearchResults returns the search results if the search engine is configured.
func (a *Analyser) GetSearchResults() *search.Results {
	if a.searchEngine == nil {
		return nil
	}
	results := a.searchEngine.GetResults()
	return &results
}

// GetSchemaReport returns the schema report if the schema analyzer is configured.
func (a *Analyser) GetSchemaReport() *schema.Report {
	if a.schemaAnalyzer == nil {
		return nil
	}
	return a.schemaAnalyzer.GenerateReport()
}

// GetDeletionStats returns the deletion statistics if the deletion engine is configured.
func (a *Analyser) GetDeletionStats() *deletion.Stats {
	if a.deletionEngine == nil {
		return nil
	}
	stats := a.deletionEngine.GetStats()
	return &stats
}

// Close closes all advanced components and saves the final state.
func (a *Analyser) Close() error {
	// Close processing components
	if a.rowProcessor != nil {
		if err := a.rowProcessor.Close(); err != nil {
			log.Printf("Failed to close row processor: %v", err)
		}
	}

	// Close state manager first to save final state
	if a.stateManager != nil {
		if err := a.stateManager.Close(); err != nil {
			log.Printf("Failed to close state manager: %v", err)
		}
	}

	// Close deletion engine
	if a.deletionEngine != nil {
		if err := a.deletionEngine.Close(); err != nil {
			return fmt.Errorf("close deletion engine: %w", err)
		}
	}

	return nil
}

// GetStateManager returns the state manager for external use.
func (a *Analyser) GetStateManager() *state.Manager {
	return a.stateManager
}

// initializeStateManagement initializes the state management system.
func (a *Analyser) initializeStateManagement(ctx context.Context) error {
	if a.stateManager != nil {
		return nil // Already initialized
	}

	stateDir := a.config.GetStateDir()
	sessionID := fmt.Sprintf("data_refinery_%d", time.Now().Unix())

	// Create state manager with configuration
	options := []state.ManagerOption{
		state.WithAutoSaveInterval(a.config.StateManagement.AutoSaveInterval),
		state.WithMaxStateHistory(a.config.StateManagement.MaxStateHistory),
		state.WithCompressionLevel(a.config.StateManagement.CompressionLevel),
	}

	sm, err := state.NewStateManager(stateDir, sessionID, options...)
	if err != nil {
		return fmt.Errorf("failed to create state manager: %w", err)
	}

	a.stateManager = sm

	// Initialize state
	_, err = sm.InitializeState(ctx, a.config)
	if err != nil {
		return fmt.Errorf("failed to initialize state: %w", err)
	}

	// Start auto-save
	sm.StartAutoSave()

	return nil
}
