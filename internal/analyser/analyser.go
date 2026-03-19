// internal/analyser/analyser.go
package analyser

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benjaminwestern/dupe-analyser/internal/config"
	"github.com/benjaminwestern/dupe-analyser/internal/deletion"
	"github.com/benjaminwestern/dupe-analyser/internal/errors"
	"github.com/benjaminwestern/dupe-analyser/internal/hasher"
	"github.com/benjaminwestern/dupe-analyser/internal/memory"
	"github.com/benjaminwestern/dupe-analyser/internal/processing"
	"github.com/benjaminwestern/dupe-analyser/internal/report"
	"github.com/benjaminwestern/dupe-analyser/internal/schema"
	"github.com/benjaminwestern/dupe-analyser/internal/search"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
	"github.com/benjaminwestern/dupe-analyser/internal/state"
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
	stateManager   *state.StateManager
	errorRecovery  *processing.ErrorRecoverySystem
	circuitBreaker *processing.CircuitBreaker

	// Advanced features (always available)
	searchEngine    *search.SearchEngine
	deletionEngine  *deletion.DeletionEngine
	schemaAnalyzer  *schema.SchemaAnalyzer
	selectiveHasher *hasher.SelectiveHasher

	// Memory management
	memoryManager *memory.MemoryManager

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

// validateConfig provides basic validation - matches the internal function for now
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

// handleRecoverableError processes errors through the advanced error recovery system
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

// StreamingProcessor manages streaming patterns for large datasets
type StreamingProcessor struct {
	batchSize       int
	batchProcessor  func([]StreamingItem) error
	memoryManager   *memory.MemoryManager
	currentBatch    []StreamingItem
	processedItems  int64
	memoryThreshold int64
	flushThreshold  int
}

// StreamingItem represents a single item in the streaming pipeline
type StreamingItem struct {
	Data       report.JSONData
	Location   report.LocationInfo
	LineNumber int
}

// NewStreamingProcessor creates a new streaming processor
func NewStreamingProcessor(batchSize int, memoryManager *memory.MemoryManager) *StreamingProcessor {
	return &StreamingProcessor{
		batchSize:       batchSize,
		memoryManager:   memoryManager,
		currentBatch:    make([]StreamingItem, 0, batchSize),
		memoryThreshold: 500 * 1024 * 1024, // 500MB threshold
		flushThreshold:  batchSize / 2,     // Flush when half full if memory pressure
	}
}

// AddItem adds an item to the streaming processor
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

// Flush processes the current batch
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

// Close finalizes the streaming processor
func (sp *StreamingProcessor) Close() error {
	return sp.Flush()
}

// executeWithCircuitBreaker executes an operation with circuit breaker protection
func (a *Analyser) executeWithCircuitBreaker(ctx context.Context, operation string, fn func() error) error {
	if a.circuitBreaker == nil {
		return fn()
	}

	return a.circuitBreaker.Execute(ctx, fn)
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
		a.stateManager.UpdateStatus(state.StatusRunning)
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
		a.stateManager.UpdateStatus(status)
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
	reader, err := src.Open(ctx)
	if err != nil {
		log.Printf("Error opening source %q: %v\n", src.Path(), err)
		return
	}
	defer reader.Close()

	// Check memory pressure before processing
	if a.memoryManager != nil {
		a.memoryManager.CheckMemoryUsage()
		if a.memoryManager.ShouldReduceMemory() {
			log.Printf("Memory pressure detected, forcing GC")
			a.memoryManager.ForceGC()
		}
	}

	// Use the row processor for processing
	rowHasher := fnv.New64a()
	scanner := bufio.NewScanner(reader)

	// Adjust buffer size based on memory configuration
	bufferSize := 4 * 1024 * 1024 // 4MB default
	if a.config.Performance != nil && a.config.Performance.BufferSize > 0 {
		bufferSize = a.config.Performance.BufferSize
	}

	const maxCapacity = 4 * 1024 * 1024
	buf := make([]byte, bufferSize)
	scanner.Buffer(buf, maxCapacity)

	lineNumber := 0
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lineNumber++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var data report.JSONData
		if err := json.Unmarshal(line, &data); err != nil {
			log.Printf("Error decoding JSON on line %d in source %q: %v\n", lineNumber, src.Path(), err)
			continue
		}

		a.TotalRows.Add(1)

		location := report.LocationInfo{
			FilePath:   src.Path(),
			LineNumber: lineNumber,
		}

		// Use the row processor for all processing
		if a.rowProcessor != nil {
			err := a.executeWithCircuitBreaker(ctx, "row_processing", func() error {
				return a.rowProcessor.ProcessRow(data, location, rowHasher)
			})
			if err != nil {
				if recoveredErr := a.handleRecoverableError(ctx, err, "row processing"); recoveredErr != nil {
					log.Printf("Error processing row: %v\n", err)
				}
			}
		}

		// Periodic memory pressure check
		if a.memoryManager != nil && lineNumber%1000 == 0 {
			a.memoryManager.CheckMemoryUsage()
			if a.memoryManager.ShouldReduceMemory() {
				log.Printf("Memory pressure detected during processing, forcing GC")
				a.memoryManager.ForceGC()
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error in source %q: %v\n", src.Path(), err)
		return
	}

	a.processedPathsMutex.Lock()
	a.processedPaths[src.Path()] = true
	a.processedPathsMutex.Unlock()
	a.ProcessedFiles.Add(1)
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

// GetSearchResults returns the search results if search engine is configured
func (a *Analyser) GetSearchResults() *search.SearchResults {
	if a.searchEngine == nil {
		return nil
	}
	results := a.searchEngine.GetResults()
	return &results
}

// GetSchemaReport returns the schema report if schema analyzer is configured
func (a *Analyser) GetSchemaReport() *schema.SchemaReport {
	if a.schemaAnalyzer == nil {
		return nil
	}
	return a.schemaAnalyzer.GenerateReport()
}

// GetDeletionStats returns the deletion statistics if deletion engine is configured
func (a *Analyser) GetDeletionStats() *deletion.DeletionStats {
	if a.deletionEngine == nil {
		return nil
	}
	stats := a.deletionEngine.GetStats()
	return &stats
}

// Close closes all advanced components and saves final state
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
		return a.deletionEngine.Close()
	}

	return nil
}

// GetStateManager returns the state manager for external use
func (a *Analyser) GetStateManager() *state.StateManager {
	return a.stateManager
}

// initializeStateManagement initializes the state management system
func (a *Analyser) initializeStateManagement(ctx context.Context) error {
	if a.stateManager != nil {
		return nil // Already initialized
	}

	stateDir := a.config.GetStateDir()
	sessionID := fmt.Sprintf("analyser_%d", time.Now().Unix())

	// Create state manager with configuration
	options := []state.StateManagerOption{
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

// RowProcessingTask represents a task for processing a single row
type RowProcessingTask struct {
	Data      report.JSONData
	Location  report.LocationInfo
	RowHasher hash.Hash64
}

// processSourceWithWorkerPool demonstrates how to use WorkerPool for row-level parallelism
// This method shows how to integrate the WorkerPool pattern into the analyser
func (a *Analyser) processSourceWithWorkerPool(ctx context.Context, src source.InputSource) {
	a.CurrentFolder.Store(src.Dir())
	reader, err := src.Open(ctx)
	if err != nil {
		log.Printf("Error opening source %q: %v\n", src.Path(), err)
		return
	}
	defer reader.Close()

	// Create a WorkerPool for row processing
	processor := processing.TaskProcessorFunc[RowProcessingTask](func(ctx context.Context, task RowProcessingTask) (interface{}, error) {
		if a.rowProcessor != nil {
			return nil, a.rowProcessor.ProcessRow(task.Data, task.Location, task.RowHasher)
		}
		return nil, nil
	})

	// Configure worker pool with reasonable defaults
	opts := processing.WorkerPoolOptions{
		Workers:    a.config.Workers,     // Use the config workers
		BufferSize: a.config.Workers * 4, // Buffer to handle burst processing
	}

	workerPool := processing.NewWorkerPool(processor, opts)
	workerPool.Start()
	defer workerPool.Stop()

	// Process rows and collect them in batches for efficient processing
	scanner := bufio.NewScanner(reader)
	const maxCapacity = 4 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	var tasks []RowProcessingTask
	lineNumber := 0
	batchSize := 100 // Process rows in batches

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		lineNumber++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var data report.JSONData
		if err := json.Unmarshal(line, &data); err != nil {
			log.Printf("Error decoding JSON on line %d in source %q: %v\n", lineNumber, src.Path(), err)
			continue
		}

		location := report.LocationInfo{
			FilePath:   src.Path(),
			LineNumber: lineNumber,
		}

		// Create a separate hasher for each task to avoid race conditions
		rowHasher := fnv.New64a()

		tasks = append(tasks, RowProcessingTask{
			Data:      data,
			Location:  location,
			RowHasher: rowHasher,
		})

		// Process in batches to avoid memory buildup
		if len(tasks) >= batchSize {
			results := workerPool.ProcessBatch(tasks)

			// Handle any errors in the batch
			for _, result := range results {
				if result.Error != nil {
					log.Printf("Error processing row: %v\n", result.Error)
				}
			}

			// Update row count
			a.TotalRows.Add(int64(len(tasks)))

			// Clear the batch
			tasks = tasks[:0]
		}
	}

	// Process any remaining tasks
	if len(tasks) > 0 {
		results := workerPool.ProcessBatch(tasks)

		// Handle any errors in the final batch
		for _, result := range results {
			if result.Error != nil {
				log.Printf("Error processing row: %v\n", result.Error)
			}
		}

		// Update row count
		a.TotalRows.Add(int64(len(tasks)))
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Scanner error in source %q: %v\n", src.Path(), err)
		return
	}

	// Update processed file tracking
	a.processedPathsMutex.Lock()
	a.processedPaths[src.Path()] = true
	a.processedPathsMutex.Unlock()
	a.ProcessedFiles.Add(1)
}
