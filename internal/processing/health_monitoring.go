package processing

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

var (
	ErrHealthCheckFailed    = errors.New("health check failed")
	ErrHealthCheckTimeout   = errors.New("health check timed out")
	ErrHealthCheckDisabled  = errors.New("health check is disabled")
	ErrHealthServiceUnknown = errors.New("health service unknown")
)

// HealthStatus represents the health status of a component
type HealthStatus int

const (
	HealthStatusUnknown HealthStatus = iota
	HealthStatusHealthy
	HealthStatusDegraded
	HealthStatusUnhealthy
	HealthStatusCritical
	HealthStatusMaintenance
)

func (h HealthStatus) String() string {
	switch h {
	case HealthStatusHealthy:
		return "HEALTHY"
	case HealthStatusDegraded:
		return "DEGRADED"
	case HealthStatusUnhealthy:
		return "UNHEALTHY"
	case HealthStatusCritical:
		return "CRITICAL"
	case HealthStatusMaintenance:
		return "MAINTENANCE"
	default:
		return "UNKNOWN"
	}
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	ComponentName string
	Status        HealthStatus
	Message       string
	Details       map[string]interface{}
	Timestamp     time.Time
	Duration      time.Duration
	Error         error
}

// HealthChecker defines the interface for health checks
type HealthChecker interface {
	Check(ctx context.Context) HealthCheckResult
	Name() string
}

// HealthCheck represents a health check function
type HealthCheck struct {
	name        string
	checkFunc   func(context.Context) HealthCheckResult
	timeout     time.Duration
	interval    time.Duration
	enabled     bool
	critical    bool
	description string
}

// NewHealthCheck creates a new health check
func NewHealthCheck(name string, checkFunc func(context.Context) HealthCheckResult) *HealthCheck {
	return &HealthCheck{
		name:        name,
		checkFunc:   checkFunc,
		timeout:     5 * time.Second,
		interval:    30 * time.Second,
		enabled:     true,
		critical:    false,
		description: "",
	}
}

// WithTimeout sets the timeout for the health check
func (hc *HealthCheck) WithTimeout(timeout time.Duration) *HealthCheck {
	hc.timeout = timeout
	return hc
}

// WithInterval sets the interval for the health check
func (hc *HealthCheck) WithInterval(interval time.Duration) *HealthCheck {
	hc.interval = interval
	return hc
}

// WithCritical marks the health check as critical
func (hc *HealthCheck) WithCritical(critical bool) *HealthCheck {
	hc.critical = critical
	return hc
}

// WithDescription sets the description for the health check
func (hc *HealthCheck) WithDescription(description string) *HealthCheck {
	hc.description = description
	return hc
}

// Check executes the health check
func (hc *HealthCheck) Check(ctx context.Context) HealthCheckResult {
	if !hc.enabled {
		return HealthCheckResult{
			ComponentName: hc.name,
			Status:        HealthStatusUnknown,
			Message:       "Health check is disabled",
			Timestamp:     time.Now(),
			Duration:      0,
		}
	}

	// Create timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, hc.timeout)
	defer cancel()

	// Execute check with timeout
	start := time.Now()
	result := hc.checkFunc(timeoutCtx)
	duration := time.Since(start)

	// Set standard fields
	result.ComponentName = hc.name
	result.Timestamp = time.Now()
	result.Duration = duration

	// Check for timeout
	if timeoutCtx.Err() == context.DeadlineExceeded {
		result.Status = HealthStatusUnhealthy
		result.Message = "Health check timed out"
		result.Error = ErrHealthCheckTimeout
	}

	return result
}

// Name returns the name of the health check
func (hc *HealthCheck) Name() string {
	return hc.name
}

// SystemHealthMonitor monitors the overall health of the system
type SystemHealthMonitor struct {
	mu            sync.RWMutex
	healthChecks  map[string]*HealthCheck
	lastResults   map[string]HealthCheckResult
	overallStatus HealthStatus
	enabled       bool
	checkInterval time.Duration

	// Background monitoring
	stopCh chan struct{}
	stopWg sync.WaitGroup

	// Metrics
	totalChecks      uint64
	successfulChecks uint64
	failedChecks     uint64
	avgCheckDuration time.Duration

	// Alerting
	alertHandlers []AlertHandler

	// History
	historySize  int
	checkHistory []HealthCheckResult

	// System metrics
	systemMetrics    *SystemMetrics
	metricsCollector *MetricsCollector
}

// AlertHandler handles health alerts
type AlertHandler interface {
	HandleAlert(alert HealthAlert)
}

// HealthAlert represents a health alert
type HealthAlert struct {
	ComponentName string
	Status        HealthStatus
	Message       string
	Timestamp     time.Time
	Severity      AlertSeverity
	Details       map[string]interface{}
}

// AlertSeverity represents the severity of an alert
type AlertSeverity int

const (
	AlertSeverityInfo AlertSeverity = iota
	AlertSeverityWarning
	AlertSeverityError
	AlertSeverityCritical
)

func (a AlertSeverity) String() string {
	switch a {
	case AlertSeverityInfo:
		return "INFO"
	case AlertSeverityWarning:
		return "WARNING"
	case AlertSeverityError:
		return "ERROR"
	case AlertSeverityCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}

// SystemHealthConfig holds configuration for system health monitoring
type SystemHealthConfig struct {
	Enabled             bool
	CheckInterval       time.Duration
	HistorySize         int
	EnableSystemMetrics bool
	AlertHandlers       []AlertHandler
}

// DefaultSystemHealthConfig returns default configuration
func DefaultSystemHealthConfig() SystemHealthConfig {
	return SystemHealthConfig{
		Enabled:             true,
		CheckInterval:       30 * time.Second,
		HistorySize:         100,
		EnableSystemMetrics: true,
		AlertHandlers:       []AlertHandler{},
	}
}

// NewSystemHealthMonitor creates a new system health monitor
func NewSystemHealthMonitor(config SystemHealthConfig) *SystemHealthMonitor {
	shm := &SystemHealthMonitor{
		healthChecks:     make(map[string]*HealthCheck),
		lastResults:      make(map[string]HealthCheckResult),
		overallStatus:    HealthStatusHealthy,
		enabled:          config.Enabled,
		checkInterval:    config.CheckInterval,
		stopCh:           make(chan struct{}),
		alertHandlers:    config.AlertHandlers,
		historySize:      config.HistorySize,
		checkHistory:     make([]HealthCheckResult, 0, config.HistorySize),
		metricsCollector: NewMetricsCollector(),
	}

	// Add default health checks
	shm.addDefaultHealthChecks()

	// Start monitoring if enabled
	if config.Enabled {
		shm.startMonitoring()
	}

	return shm
}

// addDefaultHealthChecks adds default system health checks
func (shm *SystemHealthMonitor) addDefaultHealthChecks() {
	// Memory health check
	memoryCheck := NewHealthCheck("memory", func(ctx context.Context) HealthCheckResult {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		memoryUsage := float64(m.Alloc) / float64(m.Sys)

		var status HealthStatus
		var message string

		switch {
		case memoryUsage < 0.7:
			status = HealthStatusHealthy
			message = "Memory usage is normal"
		case memoryUsage < 0.85:
			status = HealthStatusDegraded
			message = "Memory usage is elevated"
		case memoryUsage < 0.95:
			status = HealthStatusUnhealthy
			message = "Memory usage is high"
		default:
			status = HealthStatusCritical
			message = "Memory usage is critical"
		}

		return HealthCheckResult{
			Status:  status,
			Message: message,
			Details: map[string]interface{}{
				"alloc":      m.Alloc,
				"sys":        m.Sys,
				"usage":      memoryUsage,
				"heap_alloc": m.HeapAlloc,
				"heap_sys":   m.HeapSys,
				"gc_cycles":  m.NumGC,
			},
		}
	}).WithCritical(true).WithDescription("Monitors system memory usage")

	// Goroutine health check
	goroutineCheck := NewHealthCheck("goroutines", func(ctx context.Context) HealthCheckResult {
		numGoroutines := runtime.NumGoroutine()

		var status HealthStatus
		var message string

		switch {
		case numGoroutines < 1000:
			status = HealthStatusHealthy
			message = "Goroutine count is normal"
		case numGoroutines < 5000:
			status = HealthStatusDegraded
			message = "Goroutine count is elevated"
		case numGoroutines < 10000:
			status = HealthStatusUnhealthy
			message = "Goroutine count is high"
		default:
			status = HealthStatusCritical
			message = "Goroutine count is critical"
		}

		return HealthCheckResult{
			Status:  status,
			Message: message,
			Details: map[string]interface{}{
				"count":      numGoroutines,
				"cpu":        runtime.NumCPU(),
				"gomaxprocs": runtime.GOMAXPROCS(0),
			},
		}
	}).WithCritical(true).WithDescription("Monitors goroutine count")

	// Disk space health check (placeholder)
	diskCheck := NewHealthCheck("disk", func(ctx context.Context) HealthCheckResult {
		// In a real implementation, this would check actual disk usage
		return HealthCheckResult{
			Status:  HealthStatusHealthy,
			Message: "Disk space is adequate",
			Details: map[string]interface{}{
				"usage": 0.5,
			},
		}
	}).WithDescription("Monitors disk space usage")

	shm.RegisterHealthCheck(memoryCheck)
	shm.RegisterHealthCheck(goroutineCheck)
	shm.RegisterHealthCheck(diskCheck)
}

// RegisterHealthCheck registers a health check
func (shm *SystemHealthMonitor) RegisterHealthCheck(healthCheck *HealthCheck) {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	shm.healthChecks[healthCheck.name] = healthCheck
}

// UnregisterHealthCheck unregisters a health check
func (shm *SystemHealthMonitor) UnregisterHealthCheck(name string) {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	delete(shm.healthChecks, name)
	delete(shm.lastResults, name)
}

// startMonitoring starts the background health monitoring
func (shm *SystemHealthMonitor) startMonitoring() {
	shm.stopWg.Add(1)
	go func() {
		defer shm.stopWg.Done()

		ticker := time.NewTicker(shm.checkInterval)
		defer ticker.Stop()

		// Run initial health check
		shm.runHealthChecks()

		for {
			select {
			case <-shm.stopCh:
				return
			case <-ticker.C:
				shm.runHealthChecks()
			}
		}
	}()
}

// runHealthChecks runs all registered health checks
func (shm *SystemHealthMonitor) runHealthChecks() {
	if !shm.enabled {
		return
	}

	shm.mu.RLock()
	checks := make(map[string]*HealthCheck)
	for name, check := range shm.healthChecks {
		checks[name] = check
	}
	shm.mu.RUnlock()

	// Run checks concurrently
	results := make(chan HealthCheckResult, len(checks))
	var wg sync.WaitGroup

	for _, check := range checks {
		wg.Add(1)
		go func(hc *HealthCheck) {
			defer wg.Done()
			result := hc.Check(context.Background())
			results <- result
		}(check)
	}

	wg.Wait()
	close(results)

	// Process results
	shm.mu.Lock()
	defer shm.mu.Unlock()

	overallStatus := HealthStatusHealthy
	for result := range results {
		shm.lastResults[result.ComponentName] = result

		// Add to history
		shm.checkHistory = append(shm.checkHistory, result)
		if len(shm.checkHistory) > shm.historySize {
			shm.checkHistory = shm.checkHistory[1:]
		}

		// Update metrics
		shm.totalChecks++
		if result.Error == nil && result.Status == HealthStatusHealthy {
			shm.successfulChecks++
		} else {
			shm.failedChecks++
		}

		// Update average duration
		if shm.totalChecks > 0 {
			shm.avgCheckDuration = time.Duration(
				(int64(shm.avgCheckDuration)*int64(shm.totalChecks-1) + int64(result.Duration)) / int64(shm.totalChecks),
			)
		}

		// Determine overall status
		if result.Status == HealthStatusCritical {
			overallStatus = HealthStatusCritical
		} else if result.Status == HealthStatusUnhealthy && overallStatus != HealthStatusCritical {
			overallStatus = HealthStatusUnhealthy
		} else if result.Status == HealthStatusDegraded && overallStatus == HealthStatusHealthy {
			overallStatus = HealthStatusDegraded
		}

		// Send alerts for status changes
		shm.checkAndSendAlerts(result)
	}

	shm.overallStatus = overallStatus
}

// checkAndSendAlerts checks and sends alerts for status changes
func (shm *SystemHealthMonitor) checkAndSendAlerts(result HealthCheckResult) {
	// Check if this is a status change
	if lastResult, exists := shm.lastResults[result.ComponentName]; exists {
		if lastResult.Status != result.Status {
			// Status changed, send alert
			alert := HealthAlert{
				ComponentName: result.ComponentName,
				Status:        result.Status,
				Message:       result.Message,
				Timestamp:     result.Timestamp,
				Severity:      shm.getSeverityForStatus(result.Status),
				Details:       result.Details,
			}

			shm.sendAlert(alert)
		}
	}
}

// getSeverityForStatus maps health status to alert severity
func (shm *SystemHealthMonitor) getSeverityForStatus(status HealthStatus) AlertSeverity {
	switch status {
	case HealthStatusHealthy:
		return AlertSeverityInfo
	case HealthStatusDegraded:
		return AlertSeverityWarning
	case HealthStatusUnhealthy:
		return AlertSeverityError
	case HealthStatusCritical:
		return AlertSeverityCritical
	default:
		return AlertSeverityInfo
	}
}

// sendAlert sends an alert to all registered handlers
func (shm *SystemHealthMonitor) sendAlert(alert HealthAlert) {
	for _, handler := range shm.alertHandlers {
		go handler.HandleAlert(alert)
	}
}

// GetOverallStatus returns the overall system health status
func (shm *SystemHealthMonitor) GetOverallStatus() HealthStatus {
	shm.mu.RLock()
	defer shm.mu.RUnlock()

	return shm.overallStatus
}

// GetHealthCheckResults returns the latest health check results
func (shm *SystemHealthMonitor) GetHealthCheckResults() map[string]HealthCheckResult {
	shm.mu.RLock()
	defer shm.mu.RUnlock()

	results := make(map[string]HealthCheckResult)
	for name, result := range shm.lastResults {
		results[name] = result
	}

	return results
}

// RunHealthCheck runs a specific health check
func (shm *SystemHealthMonitor) RunHealthCheck(name string) (HealthCheckResult, error) {
	shm.mu.RLock()
	check, exists := shm.healthChecks[name]
	shm.mu.RUnlock()

	if !exists {
		return HealthCheckResult{}, ErrHealthServiceUnknown
	}

	return check.Check(context.Background()), nil
}

// GetHealthCheckHistory returns the health check history
func (shm *SystemHealthMonitor) GetHealthCheckHistory() []HealthCheckResult {
	shm.mu.RLock()
	defer shm.mu.RUnlock()

	history := make([]HealthCheckResult, len(shm.checkHistory))
	copy(history, shm.checkHistory)

	return history
}

// HealthMetrics contains health monitoring metrics
type HealthMetrics struct {
	TotalChecks      uint64
	SuccessfulChecks uint64
	FailedChecks     uint64
	SuccessRate      float64
	AvgCheckDuration time.Duration
	OverallStatus    HealthStatus
	ComponentCount   int
	LastCheckTime    time.Time
}

// GetHealthMetrics returns health monitoring metrics
func (shm *SystemHealthMonitor) GetHealthMetrics() HealthMetrics {
	shm.mu.RLock()
	defer shm.mu.RUnlock()

	var successRate float64
	if shm.totalChecks > 0 {
		successRate = float64(shm.successfulChecks) / float64(shm.totalChecks)
	}

	var lastCheckTime time.Time
	if len(shm.checkHistory) > 0 {
		lastCheckTime = shm.checkHistory[len(shm.checkHistory)-1].Timestamp
	}

	return HealthMetrics{
		TotalChecks:      shm.totalChecks,
		SuccessfulChecks: shm.successfulChecks,
		FailedChecks:     shm.failedChecks,
		SuccessRate:      successRate,
		AvgCheckDuration: shm.avgCheckDuration,
		OverallStatus:    shm.overallStatus,
		ComponentCount:   len(shm.healthChecks),
		LastCheckTime:    lastCheckTime,
	}
}

// Enable enables health monitoring
func (shm *SystemHealthMonitor) Enable() {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	if !shm.enabled {
		shm.enabled = true
		shm.startMonitoring()
	}
}

// Disable disables health monitoring
func (shm *SystemHealthMonitor) Disable() {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	shm.enabled = false
}

// IsEnabled returns whether health monitoring is enabled
func (shm *SystemHealthMonitor) IsEnabled() bool {
	shm.mu.RLock()
	defer shm.mu.RUnlock()

	return shm.enabled
}

// AddAlertHandler adds an alert handler
func (shm *SystemHealthMonitor) AddAlertHandler(handler AlertHandler) {
	shm.mu.Lock()
	defer shm.mu.Unlock()

	shm.alertHandlers = append(shm.alertHandlers, handler)
}

// Shutdown shuts down the health monitor
func (shm *SystemHealthMonitor) Shutdown(ctx context.Context) error {
	close(shm.stopCh)

	// Wait for monitoring to stop
	done := make(chan struct{})
	go func() {
		shm.stopWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Built-in Alert Handlers

// LogAlertHandler logs alerts
type LogAlertHandler struct {
	logger func(message string)
}

// NewLogAlertHandler creates a new log alert handler
func NewLogAlertHandler(logger func(message string)) *LogAlertHandler {
	return &LogAlertHandler{logger: logger}
}

// HandleAlert handles an alert by logging it
func (h *LogAlertHandler) HandleAlert(alert HealthAlert) {
	message := fmt.Sprintf("Health Alert: %s - %s (%s) - %s",
		alert.ComponentName,
		alert.Status.String(),
		alert.Severity.String(),
		alert.Message)

	if h.logger != nil {
		h.logger(message)
	}
}

// EmailAlertHandler sends email alerts (placeholder)
type EmailAlertHandler struct {
	recipients []string
	sender     func(to []string, subject, body string) error
}

// NewEmailAlertHandler creates a new email alert handler
func NewEmailAlertHandler(recipients []string, sender func(to []string, subject, body string) error) *EmailAlertHandler {
	return &EmailAlertHandler{
		recipients: recipients,
		sender:     sender,
	}
}

// HandleAlert handles an alert by sending an email
func (h *EmailAlertHandler) HandleAlert(alert HealthAlert) {
	if h.sender == nil {
		return
	}

	subject := fmt.Sprintf("Health Alert: %s - %s", alert.ComponentName, alert.Status.String())
	body := fmt.Sprintf("Component: %s\nStatus: %s\nSeverity: %s\nMessage: %s\nTimestamp: %s",
		alert.ComponentName,
		alert.Status.String(),
		alert.Severity.String(),
		alert.Message,
		alert.Timestamp.Format(time.RFC3339))

	go h.sender(h.recipients, subject, body)
}

// WebhookAlertHandler sends webhook alerts (placeholder)
type WebhookAlertHandler struct {
	url    string
	sender func(url string, payload interface{}) error
}

// NewWebhookAlertHandler creates a new webhook alert handler
func NewWebhookAlertHandler(url string, sender func(url string, payload interface{}) error) *WebhookAlertHandler {
	return &WebhookAlertHandler{
		url:    url,
		sender: sender,
	}
}

// HandleAlert handles an alert by sending a webhook
func (h *WebhookAlertHandler) HandleAlert(alert HealthAlert) {
	if h.sender == nil {
		return
	}

	payload := map[string]interface{}{
		"component": alert.ComponentName,
		"status":    alert.Status.String(),
		"severity":  alert.Severity.String(),
		"message":   alert.Message,
		"timestamp": alert.Timestamp.Format(time.RFC3339),
		"details":   alert.Details,
	}

	go h.sender(h.url, payload)
}

// Diagnostic System

// DiagnosticCollector collects diagnostic information
type DiagnosticCollector struct {
	mu                 sync.RWMutex
	diagnostics        map[string]DiagnosticData
	collectors         []func() DiagnosticData
	collectionInterval time.Duration
	stopCh             chan struct{}
	stopWg             sync.WaitGroup
}

// DiagnosticData represents diagnostic information
type DiagnosticData struct {
	Name      string
	Category  string
	Data      map[string]interface{}
	Timestamp time.Time
}

// NewDiagnosticCollector creates a new diagnostic collector
func NewDiagnosticCollector(interval time.Duration) *DiagnosticCollector {
	dc := &DiagnosticCollector{
		diagnostics:        make(map[string]DiagnosticData),
		collectionInterval: interval,
		stopCh:             make(chan struct{}),
	}

	// Add default collectors
	dc.addDefaultCollectors()

	// Start collection
	dc.startCollection()

	return dc
}

// addDefaultCollectors adds default diagnostic collectors
func (dc *DiagnosticCollector) addDefaultCollectors() {
	// Runtime diagnostics
	dc.AddCollector(func() DiagnosticData {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		return DiagnosticData{
			Name:     "runtime",
			Category: "system",
			Data: map[string]interface{}{
				"goroutines":     runtime.NumGoroutine(),
				"memory_alloc":   m.Alloc,
				"memory_sys":     m.Sys,
				"gc_cycles":      m.NumGC,
				"gc_pause_total": m.PauseTotalNs,
				"heap_objects":   m.HeapObjects,
				"stack_inuse":    m.StackInuse,
				"next_gc":        m.NextGC,
			},
			Timestamp: time.Now(),
		}
	})

	// Process diagnostics
	dc.AddCollector(func() DiagnosticData {
		return DiagnosticData{
			Name:     "process",
			Category: "system",
			Data: map[string]interface{}{
				"pid":        0, // Would be os.Getpid() in real implementation
				"ppid":       0, // Would be os.Getppid() in real implementation
				"uid":        0, // Would be os.Getuid() in real implementation
				"gid":        0, // Would be os.Getgid() in real implementation
				"num_cpu":    runtime.NumCPU(),
				"gomaxprocs": runtime.GOMAXPROCS(0),
				"version":    runtime.Version(),
				"goos":       runtime.GOOS,
				"goarch":     runtime.GOARCH,
			},
			Timestamp: time.Now(),
		}
	})
}

// AddCollector adds a diagnostic collector
func (dc *DiagnosticCollector) AddCollector(collector func() DiagnosticData) {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	dc.collectors = append(dc.collectors, collector)
}

// startCollection starts the diagnostic collection loop
func (dc *DiagnosticCollector) startCollection() {
	dc.stopWg.Add(1)
	go func() {
		defer dc.stopWg.Done()

		ticker := time.NewTicker(dc.collectionInterval)
		defer ticker.Stop()

		// Run initial collection
		dc.collectDiagnostics()

		for {
			select {
			case <-dc.stopCh:
				return
			case <-ticker.C:
				dc.collectDiagnostics()
			}
		}
	}()
}

// collectDiagnostics collects diagnostic information
func (dc *DiagnosticCollector) collectDiagnostics() {
	dc.mu.RLock()
	collectors := make([]func() DiagnosticData, len(dc.collectors))
	copy(collectors, dc.collectors)
	dc.mu.RUnlock()

	for _, collector := range collectors {
		data := collector()

		dc.mu.Lock()
		dc.diagnostics[data.Name] = data
		dc.mu.Unlock()
	}
}

// GetDiagnostics returns all collected diagnostics
func (dc *DiagnosticCollector) GetDiagnostics() map[string]DiagnosticData {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	diagnostics := make(map[string]DiagnosticData)
	for name, data := range dc.diagnostics {
		diagnostics[name] = data
	}

	return diagnostics
}

// GetDiagnostic returns a specific diagnostic
func (dc *DiagnosticCollector) GetDiagnostic(name string) (DiagnosticData, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	data, exists := dc.diagnostics[name]
	return data, exists
}

// Shutdown shuts down the diagnostic collector
func (dc *DiagnosticCollector) Shutdown() {
	close(dc.stopCh)
	dc.stopWg.Wait()
}
