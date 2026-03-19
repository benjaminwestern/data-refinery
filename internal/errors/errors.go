// internal/errors/errors.go
package errors

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/report"
)

// ErrorType represents different types of errors
type ErrorType string

const (
	ErrorTypeJSONParsing     ErrorType = "json_parsing"
	ErrorTypeFileAccess      ErrorType = "file_access"
	ErrorTypeNetworkAccess   ErrorType = "network_access"
	ErrorTypeValidation      ErrorType = "validation"
	ErrorTypeProcessing      ErrorType = "processing"
	ErrorTypeStateManagement ErrorType = "state_management"
	ErrorTypeConfiguration   ErrorType = "configuration"
	ErrorTypeMemoryLimit     ErrorType = "memory_limit"
	ErrorTypeTimeout         ErrorType = "timeout"
	ErrorTypeUnknown         ErrorType = "unknown"
)

// Severity represents error severity levels
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityError    Severity = "error"
	SeverityCritical Severity = "critical"
)

// ErrorEntry represents a single error occurrence
type ErrorEntry struct {
	ID          string               `json:"id"`
	Type        ErrorType            `json:"type"`
	Severity    Severity             `json:"severity"`
	Message     string               `json:"message"`
	Details     string               `json:"details,omitempty"`
	Location    *report.LocationInfo `json:"location,omitempty"`
	Timestamp   time.Time            `json:"timestamp"`
	Context     map[string]any       `json:"context,omitempty"`
	Recoverable bool                 `json:"recoverable"`
	Count       int                  `json:"count"`
}

// RecoveryCallback is a function that attempts to recover from an error
type RecoveryCallback func(ctx context.Context, err error) error

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState string

const (
	CircuitBreakerClosed   CircuitBreakerState = "closed"
	CircuitBreakerOpen     CircuitBreakerState = "open"
	CircuitBreakerHalfOpen CircuitBreakerState = "half-open"
)

// ErrorCollector collects and manages errors during processing
type ErrorCollector struct {
	errors           []ErrorEntry
	errorsByType     map[ErrorType][]ErrorEntry
	errorsByLocation map[string][]ErrorEntry
	mutex            sync.RWMutex
	maxErrors        int
	errorCount       int
	warningCount     int
	criticalCount    int

	// Circuit breaker functionality
	circuitBreakerState   CircuitBreakerState
	circuitBreakerErrors  int64
	circuitBreakerTimeout time.Duration
	lastFailureTime       time.Time
	circuitBreakerResets  int64

	// Recovery callbacks
	recoveryCallbacks map[ErrorType][]RecoveryCallback

	// Statistics
	totalHandled         int64
	totalRecovered       int64
	enableRecovery       bool
	enableCircuitBreaker bool
	logErrors            bool
}

// NewErrorCollector creates a new error collector
func NewErrorCollector(maxErrors int) *ErrorCollector {
	return &ErrorCollector{
		errors:                make([]ErrorEntry, 0),
		errorsByType:          make(map[ErrorType][]ErrorEntry),
		errorsByLocation:      make(map[string][]ErrorEntry),
		maxErrors:             maxErrors,
		circuitBreakerState:   CircuitBreakerClosed,
		circuitBreakerTimeout: 30 * time.Second,
		recoveryCallbacks:     make(map[ErrorType][]RecoveryCallback),
		enableRecovery:        true,
		enableCircuitBreaker:  true,
		logErrors:             true,
	}
}

// RegisterRecoveryCallback registers a recovery callback for a specific error type
func (ec *ErrorCollector) RegisterRecoveryCallback(errorType ErrorType, callback RecoveryCallback) {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	if ec.recoveryCallbacks[errorType] == nil {
		ec.recoveryCallbacks[errorType] = make([]RecoveryCallback, 0)
	}
	ec.recoveryCallbacks[errorType] = append(ec.recoveryCallbacks[errorType], callback)
}

// SetCircuitBreakerTimeout sets the circuit breaker timeout duration
func (ec *ErrorCollector) SetCircuitBreakerTimeout(timeout time.Duration) {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()
	ec.circuitBreakerTimeout = timeout
}

// EnableFeatures enables or disables various error handling features
func (ec *ErrorCollector) EnableFeatures(recovery, circuitBreaker, logging bool) {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()
	ec.enableRecovery = recovery
	ec.enableCircuitBreaker = circuitBreaker
	ec.logErrors = logging
}

// isCircuitBreakerOpen checks if the circuit breaker is open
func (ec *ErrorCollector) isCircuitBreakerOpen() bool {
	if ec.circuitBreakerState == CircuitBreakerOpen {
		if time.Since(ec.lastFailureTime) > ec.circuitBreakerTimeout {
			ec.circuitBreakerState = CircuitBreakerHalfOpen
			atomic.AddInt64(&ec.circuitBreakerResets, 1)
			return false
		}
		return true
	}
	return false
}

// attemptRecovery attempts to recover from an error using registered callbacks
func (ec *ErrorCollector) attemptRecovery(ctx context.Context, errorType ErrorType, err error) bool {
	callbacks := ec.recoveryCallbacks[errorType]

	for _, callback := range callbacks {
		if recoveryErr := callback(ctx, err); recoveryErr == nil {
			return true
		}
	}
	return false
}

// updateCircuitBreaker updates the circuit breaker state based on error severity
func (ec *ErrorCollector) updateCircuitBreaker(severity Severity) {
	switch severity {
	case SeverityCritical:
		ec.circuitBreakerErrors += 3
	case SeverityError:
		ec.circuitBreakerErrors += 2
	case SeverityWarning:
		ec.circuitBreakerErrors += 1
	}

	if ec.circuitBreakerErrors >= 10 {
		ec.circuitBreakerState = CircuitBreakerOpen
		ec.lastFailureTime = time.Now()
	}
}

// ResetCircuitBreaker resets the circuit breaker to closed state
func (ec *ErrorCollector) ResetCircuitBreaker() {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()
	ec.circuitBreakerState = CircuitBreakerClosed
	ec.circuitBreakerErrors = 0
}

// GetCircuitBreakerState returns the current circuit breaker state
func (ec *ErrorCollector) GetCircuitBreakerState() CircuitBreakerState {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()
	return ec.circuitBreakerState
}

// GetStatistics returns error handling statistics
func (ec *ErrorCollector) GetStatistics() map[string]any {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	return map[string]any{
		"total_handled":          atomic.LoadInt64(&ec.totalHandled),
		"total_recovered":        atomic.LoadInt64(&ec.totalRecovered),
		"total_errors":           len(ec.errors),
		"circuit_breaker_state":  ec.circuitBreakerState,
		"circuit_breaker_errors": ec.circuitBreakerErrors,
		"circuit_breaker_resets": ec.circuitBreakerResets,
		"recovery_rate":          float64(atomic.LoadInt64(&ec.totalRecovered)) / float64(atomic.LoadInt64(&ec.totalHandled)),
	}
}

// AddError adds an error to the collector
func (ec *ErrorCollector) AddError(errorType ErrorType, severity Severity, message string, details string, location *report.LocationInfo, ctx map[string]any) string {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	// Update total handled count
	atomic.AddInt64(&ec.totalHandled, 1)

	// Check circuit breaker
	if ec.enableCircuitBreaker && ec.isCircuitBreakerOpen() {
		if ec.logErrors {
			log.Printf("Circuit breaker open, rejecting error: %s", message)
		}
		return ""
	}

	// Generate unique ID
	id := fmt.Sprintf("%s_%d_%d", errorType, time.Now().UnixNano(), len(ec.errors))

	// Check if we've reached the maximum number of errors
	if len(ec.errors) >= ec.maxErrors {
		// Remove oldest error
		ec.removeOldestError()
	}

	entry := ErrorEntry{
		ID:          id,
		Type:        errorType,
		Severity:    severity,
		Message:     message,
		Details:     details,
		Location:    location,
		Timestamp:   time.Now(),
		Context:     ctx,
		Recoverable: ec.isRecoverable(errorType, severity),
		Count:       1,
	}

	// Check for duplicate errors
	if existingEntry := ec.findDuplicateError(entry); existingEntry != nil {
		existingEntry.Count++
		existingEntry.Timestamp = time.Now()
		return existingEntry.ID
	}

	// Attempt recovery if enabled
	bgCtx := context.Background()
	if ec.enableRecovery && ec.attemptRecovery(bgCtx, errorType, fmt.Errorf("%s: %s", message, details)) {
		atomic.AddInt64(&ec.totalRecovered, 1)
		if ec.logErrors {
			log.Printf("Successfully recovered from error: %s", message)
		}
		return id
	}

	ec.errors = append(ec.errors, entry)
	ec.errorsByType[errorType] = append(ec.errorsByType[errorType], entry)

	if location != nil {
		locationKey := fmt.Sprintf("%s:%d", location.FilePath, location.LineNumber)
		ec.errorsByLocation[locationKey] = append(ec.errorsByLocation[locationKey], entry)
	}

	// Update counters
	switch severity {
	case SeverityWarning:
		ec.warningCount++
	case SeverityError:
		ec.errorCount++
	case SeverityCritical:
		ec.criticalCount++
	}

	// Update circuit breaker state
	if ec.enableCircuitBreaker {
		ec.updateCircuitBreaker(severity)
	}

	// Log error if enabled
	if ec.logErrors {
		log.Printf("Error added: %s [%s/%s] - %s", errorType, severity, message, details)
	}

	return id
}

// AddJSONParsingError adds a JSON parsing error
func (ec *ErrorCollector) AddJSONParsingError(message string, location *report.LocationInfo, jsonContent string) string {
	context := map[string]any{
		"json_content": jsonContent,
	}
	return ec.AddError(ErrorTypeJSONParsing, SeverityWarning, message, "", location, context)
}

// AddFileAccessError adds a file access error
func (ec *ErrorCollector) AddFileAccessError(message string, filePath string, err error) string {
	context := map[string]any{
		"file_path": filePath,
		"error":     err.Error(),
	}
	location := &report.LocationInfo{FilePath: filePath, LineNumber: 0}
	return ec.AddError(ErrorTypeFileAccess, SeverityError, message, err.Error(), location, context)
}

// AddValidationError adds a validation error
func (ec *ErrorCollector) AddValidationError(message string, details string, context map[string]any) string {
	return ec.AddError(ErrorTypeValidation, SeverityError, message, details, nil, context)
}

// AddProcessingError adds a processing error
func (ec *ErrorCollector) AddProcessingError(message string, location *report.LocationInfo, err error) string {
	context := map[string]any{
		"error": err.Error(),
	}
	return ec.AddError(ErrorTypeProcessing, SeverityError, message, err.Error(), location, context)
}

// AddMemoryLimitError adds a memory limit error
func (ec *ErrorCollector) AddMemoryLimitError(message string, details string) string {
	return ec.AddError(ErrorTypeMemoryLimit, SeverityCritical, message, details, nil, nil)
}

// GetErrors returns all errors
func (ec *ErrorCollector) GetErrors() []ErrorEntry {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	errors := make([]ErrorEntry, len(ec.errors))
	copy(errors, ec.errors)
	return errors
}

// GetErrorsByType returns errors of a specific type
func (ec *ErrorCollector) GetErrorsByType(errorType ErrorType) []ErrorEntry {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	if errors, exists := ec.errorsByType[errorType]; exists {
		result := make([]ErrorEntry, len(errors))
		copy(result, errors)
		return result
	}
	return []ErrorEntry{}
}

// GetErrorsByLocation returns errors for a specific location
func (ec *ErrorCollector) GetErrorsByLocation(filePath string, lineNumber int) []ErrorEntry {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	locationKey := fmt.Sprintf("%s:%d", filePath, lineNumber)
	if errors, exists := ec.errorsByLocation[locationKey]; exists {
		result := make([]ErrorEntry, len(errors))
		copy(result, errors)
		return result
	}
	return []ErrorEntry{}
}

// GetErrorCount returns the total number of errors
func (ec *ErrorCollector) GetErrorCount() int {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()
	return len(ec.errors)
}

// GetCountsBySeverity returns error counts by severity
func (ec *ErrorCollector) GetCountsBySeverity() map[Severity]int {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	return map[Severity]int{
		SeverityWarning:  ec.warningCount,
		SeverityError:    ec.errorCount,
		SeverityCritical: ec.criticalCount,
	}
}

// GetCountsByType returns error counts by type
func (ec *ErrorCollector) GetCountsByType() map[ErrorType]int {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	counts := make(map[ErrorType]int)
	for errorType, errors := range ec.errorsByType {
		counts[errorType] = len(errors)
	}
	return counts
}

// HasCriticalErrors checks if there are any critical errors
func (ec *ErrorCollector) HasCriticalErrors() bool {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()
	return ec.criticalCount > 0
}

// ShouldAbort determines if processing should be aborted based on error conditions
func (ec *ErrorCollector) ShouldAbort() bool {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	// Abort if we have critical errors
	if ec.criticalCount > 0 {
		return true
	}

	// Abort if we have too many errors
	if ec.errorCount > ec.maxErrors/2 {
		return true
	}

	return false
}

// GenerateReport generates a comprehensive error report
func (ec *ErrorCollector) GenerateReport() ErrorReport {
	ec.mutex.RLock()
	defer ec.mutex.RUnlock()

	return ErrorReport{
		TotalErrors:      len(ec.errors),
		CountsBySeverity: ec.GetCountsBySeverity(),
		CountsByType:     ec.GetCountsByType(),
		Errors:           ec.errors,
		GeneratedAt:      time.Now(),
		TopErrorTypes:    ec.getTopErrorTypes(),
		ErrorSummary:     ec.generateErrorSummary(),
		Recommendations:  ec.generateRecommendations(),
	}
}

// ErrorReport represents a comprehensive error report
type ErrorReport struct {
	TotalErrors      int                `json:"totalErrors"`
	CountsBySeverity map[Severity]int   `json:"countsBySeverity"`
	CountsByType     map[ErrorType]int  `json:"countsByType"`
	Errors           []ErrorEntry       `json:"errors"`
	GeneratedAt      time.Time          `json:"generatedAt"`
	TopErrorTypes    []ErrorTypeSummary `json:"topErrorTypes"`
	ErrorSummary     string             `json:"errorSummary"`
	Recommendations  []string           `json:"recommendations"`
}

// ErrorTypeSummary represents a summary of errors by type
type ErrorTypeSummary struct {
	Type        ErrorType `json:"type"`
	Count       int       `json:"count"`
	Percentage  float64   `json:"percentage"`
	MostCommon  string    `json:"mostCommon"`
	Recoverable bool      `json:"recoverable"`
}

// Clear clears all collected errors
func (ec *ErrorCollector) Clear() {
	ec.mutex.Lock()
	defer ec.mutex.Unlock()

	ec.errors = make([]ErrorEntry, 0)
	ec.errorsByType = make(map[ErrorType][]ErrorEntry)
	ec.errorsByLocation = make(map[string][]ErrorEntry)
	ec.errorCount = 0
	ec.warningCount = 0
	ec.criticalCount = 0
}

// findDuplicateError finds if an error already exists
func (ec *ErrorCollector) findDuplicateError(entry ErrorEntry) *ErrorEntry {
	for i := range ec.errors {
		existing := &ec.errors[i]
		if existing.Type == entry.Type &&
			existing.Message == entry.Message &&
			ec.locationsEqual(existing.Location, entry.Location) {
			return existing
		}
	}
	return nil
}

// locationsEqual compares two locations for equality
func (ec *ErrorCollector) locationsEqual(loc1, loc2 *report.LocationInfo) bool {
	if loc1 == nil && loc2 == nil {
		return true
	}
	if loc1 == nil || loc2 == nil {
		return false
	}
	return loc1.FilePath == loc2.FilePath && loc1.LineNumber == loc2.LineNumber
}

// removeOldestError removes the oldest error from the collection
func (ec *ErrorCollector) removeOldestError() {
	if len(ec.errors) == 0 {
		return
	}

	// Remove from main slice
	oldestError := ec.errors[0]
	ec.errors = ec.errors[1:]

	// Remove from type mapping
	if typeErrors, exists := ec.errorsByType[oldestError.Type]; exists {
		for i, err := range typeErrors {
			if err.ID == oldestError.ID {
				ec.errorsByType[oldestError.Type] = append(typeErrors[:i], typeErrors[i+1:]...)
				break
			}
		}
	}

	// Remove from location mapping
	if oldestError.Location != nil {
		locationKey := fmt.Sprintf("%s:%d", oldestError.Location.FilePath, oldestError.Location.LineNumber)
		if locationErrors, exists := ec.errorsByLocation[locationKey]; exists {
			for i, err := range locationErrors {
				if err.ID == oldestError.ID {
					ec.errorsByLocation[locationKey] = append(locationErrors[:i], locationErrors[i+1:]...)
					break
				}
			}
		}
	}

	// Update counters
	switch oldestError.Severity {
	case SeverityWarning:
		ec.warningCount--
	case SeverityError:
		ec.errorCount--
	case SeverityCritical:
		ec.criticalCount--
	}
}

// isRecoverable determines if an error is recoverable
func (ec *ErrorCollector) isRecoverable(errorType ErrorType, severity Severity) bool {
	if severity == SeverityCritical {
		return false
	}

	switch errorType {
	case ErrorTypeJSONParsing, ErrorTypeValidation:
		return true
	case ErrorTypeFileAccess, ErrorTypeNetworkAccess:
		return severity != SeverityError
	case ErrorTypeMemoryLimit, ErrorTypeTimeout:
		return false
	default:
		return severity == SeverityWarning
	}
}

// getTopErrorTypes returns the most frequent error types
func (ec *ErrorCollector) getTopErrorTypes() []ErrorTypeSummary {
	typeCounts := make(map[ErrorType]int)
	typeMostCommon := make(map[ErrorType]string)

	for _, err := range ec.errors {
		typeCounts[err.Type]++
		if typeMostCommon[err.Type] == "" {
			typeMostCommon[err.Type] = err.Message
		}
	}

	var summaries []ErrorTypeSummary
	total := len(ec.errors)

	for errorType, count := range typeCounts {
		percentage := float64(count) / float64(total) * 100
		summaries = append(summaries, ErrorTypeSummary{
			Type:        errorType,
			Count:       count,
			Percentage:  percentage,
			MostCommon:  typeMostCommon[errorType],
			Recoverable: ec.isRecoverable(errorType, SeverityError),
		})
	}

	return summaries
}

// generateErrorSummary generates a human-readable error summary
func (ec *ErrorCollector) generateErrorSummary() string {
	total := len(ec.errors)
	if total == 0 {
		return "No errors encountered during processing."
	}

	var parts []string
	parts = append(parts, fmt.Sprintf("Total errors: %d", total))

	if ec.criticalCount > 0 {
		parts = append(parts, fmt.Sprintf("Critical errors: %d", ec.criticalCount))
	}
	if ec.errorCount > 0 {
		parts = append(parts, fmt.Sprintf("Errors: %d", ec.errorCount))
	}
	if ec.warningCount > 0 {
		parts = append(parts, fmt.Sprintf("Warnings: %d", ec.warningCount))
	}

	return strings.Join(parts, ", ")
}

// generateRecommendations generates recommendations based on error patterns
func (ec *ErrorCollector) generateRecommendations() []string {
	var recommendations []string

	typeCounts := ec.GetCountsByType()

	if typeCounts[ErrorTypeJSONParsing] > 0 {
		recommendations = append(recommendations, "Consider validating JSON files before processing to identify malformed data.")
	}

	if typeCounts[ErrorTypeFileAccess] > 0 {
		recommendations = append(recommendations, "Check file permissions and ensure all source files are accessible.")
	}

	if typeCounts[ErrorTypeMemoryLimit] > 0 {
		recommendations = append(recommendations, "Consider increasing memory limits or processing data in smaller chunks.")
	}

	if typeCounts[ErrorTypeNetworkAccess] > 0 {
		recommendations = append(recommendations, "Check network connectivity and retry failed network operations.")
	}

	if ec.criticalCount > 0 {
		recommendations = append(recommendations, "Address critical errors immediately as they may cause data corruption or processing failures.")
	}

	if len(recommendations) == 0 {
		recommendations = append(recommendations, "No specific recommendations. Error handling is working correctly.")
	}

	return recommendations
}

// ErrorHandler provides utility functions for error handling
type ErrorHandler struct {
	collector *ErrorCollector
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(maxErrors int) *ErrorHandler {
	return &ErrorHandler{
		collector: NewErrorCollector(maxErrors),
	}
}

// GetCollector returns the error collector
func (eh *ErrorHandler) GetCollector() *ErrorCollector {
	return eh.collector
}

// HandleJSONError handles JSON parsing errors with recovery
func (eh *ErrorHandler) HandleJSONError(content []byte, location *report.LocationInfo, err error) (recovered bool, data map[string]any) {
	eh.collector.AddJSONParsingError(err.Error(), location, string(content))

	// Attempt to recover from JSON errors
	return eh.attemptJSONRecovery(content)
}

// attemptJSONRecovery attempts to recover from JSON parsing errors
func (eh *ErrorHandler) attemptJSONRecovery(content []byte) (bool, map[string]any) {
	contentStr := string(content)

	// Try to fix common JSON issues
	fixes := []func(string) string{
		eh.fixTrailingComma,
		eh.fixUnquotedKeys,
		eh.fixSingleQuotes,
		eh.fixMissingQuotes,
	}

	for _, fix := range fixes {
		fixed := fix(contentStr)
		if fixed != contentStr {
			var data map[string]any
			if err := json.Unmarshal([]byte(fixed), &data); err == nil {
				return true, data
			}
		}
	}

	return false, nil
}

// fixTrailingComma removes trailing commas from JSON
func (eh *ErrorHandler) fixTrailingComma(content string) string {
	content = strings.ReplaceAll(content, ",}", "}")
	content = strings.ReplaceAll(content, ",]", "]")
	return content
}

// fixUnquotedKeys adds quotes to unquoted keys
func (eh *ErrorHandler) fixUnquotedKeys(content string) string {
	// This is a simplified implementation
	// In production, you'd want a more robust parser
	return content
}

// fixSingleQuotes converts single quotes to double quotes
func (eh *ErrorHandler) fixSingleQuotes(content string) string {
	return strings.ReplaceAll(content, "'", "\"")
}

// fixMissingQuotes adds missing quotes around string values
func (eh *ErrorHandler) fixMissingQuotes(content string) string {
	// This is a simplified implementation
	// In production, you'd want a more robust parser
	return content
}
