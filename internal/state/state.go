// internal/state/state.go
package state

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
)

const (
	stateTimestampLayout  = "20060102_150405.000000000"
	legacyStateTimeLayout = "20060102_150405"
	stateFileExtension    = ".state"
)

// AnalysisState represents the complete state of an analysis that can be persisted
type AnalysisState struct {
	SessionID              string                           `json:"session_id"`
	CreatedAt              time.Time                        `json:"created_at"`
	UpdatedAt              time.Time                        `json:"updated_at"`
	Config                 *config.Config                   `json:"config"`
	UniqueKey              string                           `json:"unique_key"`
	NumWorkers             int                              `json:"num_workers"`
	CheckKey               bool                             `json:"check_key"`
	CheckRow               bool                             `json:"check_row"`
	ValidateOnly           bool                             `json:"validate_only"`
	IdLocations            map[string][]report.LocationInfo `json:"id_locations"`
	RowHashes              map[string][]report.LocationInfo `json:"row_hashes"`
	KeysFoundPerFolder     map[string]int64                 `json:"keys_found_per_folder"`
	RowsProcessedPerFolder map[string]int64                 `json:"rows_processed_per_folder"`
	ProcessedFiles         int32                            `json:"processed_files"`
	TotalRows              int64                            `json:"total_rows"`
	CurrentFolder          string                           `json:"current_folder"`
	ProcessedPaths         map[string]bool                  `json:"processed_paths"`

	// Advanced features state
	SearchResults *search.SearchResults   `json:"search_results,omitempty"`
	SchemaReport  *schema.SchemaReport    `json:"schema_report,omitempty"`
	DeletionStats *deletion.DeletionStats `json:"deletion_stats,omitempty"`

	// Processing metadata
	SourceMetadata  []SourceMetadata `json:"source_metadata"`
	ProcessingStats ProcessingStats  `json:"processing_stats"`

	// State status
	Status    StateStatus  `json:"status"`
	ErrorLog  []StateError `json:"error_log"`
	LastError *StateError  `json:"last_error,omitempty"`

	// Compression and versioning
	Version    string `json:"version"`
	Compressed bool   `json:"compressed"`
	Checksum   string `json:"checksum"`
}

// SourceMetadata contains metadata about processed sources
type SourceMetadata struct {
	Path        string    `json:"path"`
	Dir         string    `json:"dir"`
	Size        int64     `json:"size"`
	Processed   bool      `json:"processed"`
	ProcessedAt time.Time `json:"processed_at,omitempty"`
	LineCount   int       `json:"line_count"`
	ErrorCount  int       `json:"error_count"`
}

// ProcessingStats contains statistics about the processing run
type ProcessingStats struct {
	StartTime             time.Time     `json:"start_time"`
	EndTime               time.Time     `json:"end_time,omitempty"`
	Duration              time.Duration `json:"duration"`
	TotalBytesProcessed   int64         `json:"total_bytes_processed"`
	ProcessingRate        float64       `json:"processing_rate"` // bytes per second
	AverageProcessingTime time.Duration `json:"average_processing_time"`
	PeakMemoryUsage       int64         `json:"peak_memory_usage"`
	TotalErrorsRecovered  int           `json:"total_errors_recovered"`
	WorkerUtilization     float64       `json:"worker_utilization"`
}

// StateStatus represents the current status of the analysis
type StateStatus string

const (
	StatusPending    StateStatus = "pending"
	StatusRunning    StateStatus = "running"
	StatusPaused     StateStatus = "paused"
	StatusCompleted  StateStatus = "completed"
	StatusCancelled  StateStatus = "cancelled"
	StatusError      StateStatus = "error"
	StatusRecovering StateStatus = "recovering"
)

// StateError represents an error that occurred during analysis
type StateError struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Type      string    `json:"type"`
	Source    string    `json:"source"`
	Severity  string    `json:"severity"`
	Recovered bool      `json:"recovered"`
}

// StateManager manages the persistence and recovery of analysis state
type StateManager struct {
	stateDir         string
	sessionID        string
	compressionLevel int
	autoSaveInterval time.Duration
	maxStateHistory  int

	// Runtime state
	currentState   *AnalysisState
	stateMutex     sync.RWMutex
	autoSaveTicker *time.Ticker
	autoSaveStop   chan bool

	// Callbacks
	onStateChanged func(*AnalysisState)
	onError        func(error)
}

// NewStateManager creates a new state manager with the specified configuration
func NewStateManager(stateDir, sessionID string, options ...StateManagerOption) (*StateManager, error) {
	sm := &StateManager{
		stateDir:         stateDir,
		sessionID:        sessionID,
		compressionLevel: 6, // Default compression level
		autoSaveInterval: 30 * time.Second,
		maxStateHistory:  10,
		autoSaveStop:     make(chan bool, 1),
	}

	// Apply options
	for _, opt := range options {
		opt(sm)
	}

	// Ensure state directory exists
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create state directory: %w", err)
	}

	return sm, nil
}

// StateManagerOption allows customization of the state manager
type StateManagerOption func(*StateManager)

// WithCompressionLevel sets the compression level (0-9)
func WithCompressionLevel(level int) StateManagerOption {
	return func(sm *StateManager) {
		sm.compressionLevel = level
	}
}

// WithAutoSaveInterval sets the auto-save interval
func WithAutoSaveInterval(interval time.Duration) StateManagerOption {
	return func(sm *StateManager) {
		sm.autoSaveInterval = interval
	}
}

// WithMaxStateHistory sets the maximum number of state history files to keep
func WithMaxStateHistory(max int) StateManagerOption {
	return func(sm *StateManager) {
		sm.maxStateHistory = max
	}
}

// WithStateChangedCallback sets a callback for state changes
func WithStateChangedCallback(callback func(*AnalysisState)) StateManagerOption {
	return func(sm *StateManager) {
		sm.onStateChanged = callback
	}
}

// WithErrorCallback sets a callback for errors
func WithErrorCallback(callback func(error)) StateManagerOption {
	return func(sm *StateManager) {
		sm.onError = callback
	}
}

// InitializeState creates a new analysis state or loads an existing one
func (sm *StateManager) InitializeState(ctx context.Context, cfg *config.Config) (*AnalysisState, error) {
	sm.stateMutex.Lock()
	defer sm.stateMutex.Unlock()

	// Try to load existing state first
	if state, err := sm.loadLatestState(); err == nil {
		// Verify that the existing state is compatible with the current config
		if state.Config != nil && state.Config.Key == cfg.Key {
			sm.currentState = state
			return state, nil
		}
	}

	// Create new state if no existing state or incompatible
	state := &AnalysisState{
		SessionID:              sm.sessionID,
		CreatedAt:              time.Now(),
		UpdatedAt:              time.Now(),
		Config:                 cfg,
		UniqueKey:              cfg.Key,
		NumWorkers:             cfg.Workers,
		CheckKey:               cfg.CheckKey,
		CheckRow:               cfg.CheckRow,
		ValidateOnly:           cfg.ValidateOnly,
		IdLocations:            make(map[string][]report.LocationInfo),
		RowHashes:              make(map[string][]report.LocationInfo),
		KeysFoundPerFolder:     make(map[string]int64),
		RowsProcessedPerFolder: make(map[string]int64),
		ProcessedPaths:         make(map[string]bool),
		SourceMetadata:         make([]SourceMetadata, 0),
		ProcessingStats: ProcessingStats{
			StartTime: time.Now(),
		},
		Status:     StatusPending,
		ErrorLog:   make([]StateError, 0),
		Version:    "1.0.0",
		Compressed: true,
	}

	sm.currentState = state
	return state, nil
}

// UpdateState updates the current state with new data
func (sm *StateManager) UpdateState(updateFunc func(*AnalysisState) error) error {
	sm.stateMutex.Lock()
	defer sm.stateMutex.Unlock()

	if sm.currentState == nil {
		return fmt.Errorf("no current state to update")
	}

	// Apply the update
	if err := updateFunc(sm.currentState); err != nil {
		return fmt.Errorf("failed to update state: %w", err)
	}

	// Update timestamp
	sm.currentState.UpdatedAt = time.Now()

	// Trigger callback if set
	if sm.onStateChanged != nil {
		sm.onStateChanged(sm.currentState)
	}

	return nil
}

// SaveState persists the current state to disk
func (sm *StateManager) SaveState() error {
	sm.stateMutex.RLock()
	defer sm.stateMutex.RUnlock()

	if sm.currentState == nil {
		return nil
	}

	return sm.saveState(sm.currentState)
}

// LoadState loads a specific state by timestamp
func (sm *StateManager) LoadState(timestamp time.Time) (*AnalysisState, error) {
	sm.stateMutex.Lock()
	defer sm.stateMutex.Unlock()

	filename := sm.getStateFilename(timestamp)
	state, err := sm.loadStateFromFile(filename)
	if err != nil {
		return nil, err
	}

	sm.currentState = state
	return state, nil
}

// GetCurrentState returns the current analysis state (thread-safe)
func (sm *StateManager) GetCurrentState() *AnalysisState {
	sm.stateMutex.RLock()
	defer sm.stateMutex.RUnlock()

	if sm.currentState == nil {
		return nil
	}

	// Return a copy to prevent external modification
	stateCopy := *sm.currentState
	return &stateCopy
}

// StartAutoSave begins automatic state saving at the configured interval
func (sm *StateManager) StartAutoSave() {
	if sm.autoSaveTicker != nil {
		sm.StopAutoSave()
	}

	sm.autoSaveTicker = time.NewTicker(sm.autoSaveInterval)
	go func() {
		ticker := sm.autoSaveTicker
		for {
			select {
			case <-ticker.C:
				if err := sm.SaveState(); err != nil {
					if sm.onError != nil {
						sm.onError(fmt.Errorf("auto-save failed: %w", err))
					}
				}
			case <-sm.autoSaveStop:
				return
			}
		}
	}()
}

// StopAutoSave stops automatic state saving
func (sm *StateManager) StopAutoSave() {
	if sm.autoSaveTicker != nil {
		sm.autoSaveTicker.Stop()
		sm.autoSaveTicker = nil

		select {
		case sm.autoSaveStop <- true:
		default:
		}
	}
}

// ListStates returns a list of available state files
func (sm *StateManager) ListStates() ([]time.Time, error) {
	files, err := os.ReadDir(sm.stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read state directory: %w", err)
	}

	var timestamps []time.Time
	sessionPrefix := sm.sessionID + "_"
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".state" {
			// Only process files for this session
			if strings.HasPrefix(file.Name(), sessionPrefix) {
				// Extract timestamp from filename
				if timestamp, err := sm.parseStateFilename(file.Name()); err == nil {
					timestamps = append(timestamps, timestamp)
				}
			}
		}
	}

	return timestamps, nil
}

// CleanupOldStates removes old state files beyond the maximum history
func (sm *StateManager) CleanupOldStates() error {
	timestamps, err := sm.ListStates()
	if err != nil {
		return err
	}

	if len(timestamps) <= sm.maxStateHistory {
		return nil
	}

	// Sort timestamps and remove oldest ones
	// Sort in descending order (newest first)
	for i := 0; i < len(timestamps)-1; i++ {
		for j := i + 1; j < len(timestamps); j++ {
			if timestamps[i].Before(timestamps[j]) {
				timestamps[i], timestamps[j] = timestamps[j], timestamps[i]
			}
		}
	}

	// Remove old files
	for i := sm.maxStateHistory; i < len(timestamps); i++ {
		filename := sm.getStateFilename(timestamps[i])
		if err := os.Remove(filename); err != nil {
			return fmt.Errorf("failed to remove old state file %s: %w", filename, err)
		}
	}

	return nil
}

// RecoverFromState attempts to recover an analysis from a saved state
func (sm *StateManager) RecoverFromState(timestamp time.Time) (*AnalysisState, error) {
	state, err := sm.LoadState(timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to load state for recovery: %w", err)
	}

	// Update status to recovering
	state.Status = StatusRecovering
	state.UpdatedAt = time.Now()

	// Add recovery entry to error log
	state.ErrorLog = append(state.ErrorLog, StateError{
		Timestamp: time.Now(),
		Message:   "Recovery initiated",
		Type:      "recovery",
		Source:    "state_manager",
		Severity:  "info",
		Recovered: true,
	})

	return state, nil
}

// AddError adds an error to the state error log
func (sm *StateManager) AddError(err error, errorType, source, severity string) {
	sm.stateMutex.Lock()
	defer sm.stateMutex.Unlock()

	if sm.currentState == nil {
		return
	}

	stateError := StateError{
		Timestamp: time.Now(),
		Message:   err.Error(),
		Type:      errorType,
		Source:    source,
		Severity:  severity,
		Recovered: false,
	}

	sm.currentState.ErrorLog = append(sm.currentState.ErrorLog, stateError)
	sm.currentState.LastError = &stateError
	sm.currentState.UpdatedAt = time.Now()
}

// UpdateProcessingStats updates the processing statistics
func (sm *StateManager) UpdateProcessingStats(stats ProcessingStats) error {
	return sm.UpdateState(func(state *AnalysisState) error {
		state.ProcessingStats = stats
		return nil
	})
}

// UpdateStatus updates the analysis status
func (sm *StateManager) UpdateStatus(status StateStatus) error {
	return sm.UpdateState(func(state *AnalysisState) error {
		state.Status = status
		return nil
	})
}

// Private helper methods

func (sm *StateManager) saveState(state *AnalysisState) error {
	filename := sm.getStateFilename(state.UpdatedAt)

	// Create temporary file
	tempFile := filename + ".tmp"
	file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("failed to create temporary state file: %w", err)
	}
	defer file.Close()

	// Use compression if enabled
	var encoder *json.Encoder
	if state.Compressed {
		gzipWriter, err := gzip.NewWriterLevel(file, sm.compressionLevel)
		if err != nil {
			return fmt.Errorf("failed to create gzip writer: %w", err)
		}
		defer gzipWriter.Close()
		encoder = json.NewEncoder(gzipWriter)
	} else {
		encoder = json.NewEncoder(file)
	}

	// Encode state
	if err := encoder.Encode(state); err != nil {
		return fmt.Errorf("failed to encode state: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempFile, filename); err != nil {
		return fmt.Errorf("failed to rename temporary state file: %w", err)
	}

	return nil
}

func (sm *StateManager) loadStateFromFile(filename string) (*AnalysisState, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open state file: %w", err)
	}
	defer file.Close()

	var decoder *json.Decoder

	// Check if file is compressed by reading first few bytes
	buf := make([]byte, 2)
	if _, err := file.Read(buf); err != nil {
		return nil, fmt.Errorf("failed to read file header: %w", err)
	}

	// Reset file position
	if _, err := file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek file: %w", err)
	}

	// Check for gzip magic number
	if buf[0] == 0x1f && buf[1] == 0x8b {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		decoder = json.NewDecoder(gzipReader)
	} else {
		decoder = json.NewDecoder(file)
	}

	var state AnalysisState
	if err := decoder.Decode(&state); err != nil {
		return nil, fmt.Errorf("failed to decode state: %w", err)
	}

	return &state, nil
}

func (sm *StateManager) loadLatestState() (*AnalysisState, error) {
	timestamps, err := sm.ListStates()
	if err != nil {
		return nil, err
	}

	if len(timestamps) == 0 {
		return nil, fmt.Errorf("no state files found")
	}

	// Find the latest timestamp
	latest := timestamps[0]
	for _, ts := range timestamps[1:] {
		if ts.After(latest) {
			latest = ts
		}
	}

	filename := sm.getStateFilename(latest)
	return sm.loadStateFromFile(filename)
}

func (sm *StateManager) getStateFilename(timestamp time.Time) string {
	return filepath.Join(sm.stateDir, fmt.Sprintf("%s_%s%s",
		sm.sessionID, timestamp.Format(stateTimestampLayout), stateFileExtension))
}

func (sm *StateManager) parseStateFilename(filename string) (time.Time, error) {
	base := filepath.Base(filename)
	if !strings.HasSuffix(base, stateFileExtension) {
		return time.Time{}, fmt.Errorf("invalid state filename format")
	}

	base = strings.TrimSuffix(base, stateFileExtension)
	prefix := sm.sessionID + "_"
	if !strings.HasPrefix(base, prefix) {
		return time.Time{}, fmt.Errorf("state file does not belong to session")
	}

	timestampStr := strings.TrimPrefix(base, prefix)
	if timestampStr == "" {
		return time.Time{}, fmt.Errorf("invalid state filename format")
	}

	if ts, err := time.Parse(stateTimestampLayout, timestampStr); err == nil {
		return ts, nil
	}

	if ts, err := time.Parse(legacyStateTimeLayout, timestampStr); err == nil {
		return ts, nil
	}

	return time.Time{}, fmt.Errorf("invalid state filename format")
}

// Close properly shuts down the state manager
func (sm *StateManager) Close() error {
	sm.StopAutoSave()

	// Save final state
	if err := sm.SaveState(); err != nil {
		return fmt.Errorf("failed to save final state: %w", err)
	}

	// Cleanup old states
	if err := sm.CleanupOldStates(); err != nil {
		return fmt.Errorf("failed to cleanup old states: %w", err)
	}

	return nil
}
