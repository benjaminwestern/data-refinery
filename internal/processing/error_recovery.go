package processing

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/safety"
)

var (
	errRecoveryFailed    = errors.New("recovery failed")
	errNoRecoveryHandler = errors.New("no recovery handler found")
	errRecoveryDisabled  = errors.New("recovery is disabled")
)

const unknownLabel = "UNKNOWN"

// RecoveryStrategy defines how to recover from specific errors.
type RecoveryStrategy interface {
	CanRecover(error) bool
	Recover(context.Context, error) error
	Name() string
}

// RecoveryAction defines the action to take during recovery.
type RecoveryAction int

const (
	actionRetry RecoveryAction = iota
	actionFallback
	actionCircuitBreaker
	actionGracefulDegradation
	actionSkip
	actionAbort
)

func (a RecoveryAction) String() string {
	switch a {
	case actionRetry:
		return "RETRY"
	case actionFallback:
		return "FALLBACK"
	case actionCircuitBreaker:
		return "CIRCUIT_BREAKER"
	case actionGracefulDegradation:
		return "GRACEFUL_DEGRADATION"
	case actionSkip:
		return "SKIP"
	case actionAbort:
		return "ABORT"
	default:
		return unknownLabel
	}
}

// RecoveryContext provides context for recovery operations.
type RecoveryContext struct {
	OriginalError    error
	Attempt          int
	MaxAttempts      int
	StartTime        time.Time
	LastRecoveryTime time.Time
	Metadata         map[string]interface{}
	ResourceLimits   *ResourceLimits
}

// ResourceLimits defines limits for resource usage during recovery.
type ResourceLimits struct {
	MaxCPUUsage    float64
	MaxMemoryUsage int64
	MaxGoroutines  int
	MaxDuration    time.Duration
}

// RecoveryResult contains the result of a recovery operation.
type RecoveryResult struct {
	Success     bool
	Action      RecoveryAction
	Duration    time.Duration
	Error       error
	RecoveredBy string
	NextAction  RecoveryAction
	Metadata    map[string]interface{}
}

// ErrorRecoverySystem manages error recovery for the system.
type ErrorRecoverySystem struct {
	strategies      []RecoveryStrategy
	fallbackHandler RecoveryStrategy
	circuitBreaker  *CircuitBreaker
	retryer         *Retryer

	// Configuration
	config ErrorRecoveryConfig

	// State management
	mu               sync.RWMutex
	enabled          bool
	activeRecoveries int32
	maxRecoveries    int32
	recoveryHistory  []RecoveryResult
	historySize      int

	// Metrics
	totalRecoveries      uint64
	successfulRecoveries uint64
	failedRecoveries     uint64
	strategyUsage        map[string]uint64
	averageRecoveryTime  time.Duration
	lastRecoveryTime     time.Time

	// Resource monitoring
	resourceMonitor *ResourceMonitor

	// Shutdown
	shutdownCh chan struct{}
	shutdownWg sync.WaitGroup
}

// ErrorRecoveryConfig holds configuration for error recovery.
type ErrorRecoveryConfig struct {
	MaxConcurrentRecoveries   int
	RecoveryTimeout           time.Duration
	MaxRecoveryAttempts       int
	EnableCircuitBreaker      bool
	EnableRetry               bool
	EnableFallback            bool
	EnableGracefulDegradation bool
	ResourceLimits            *ResourceLimits
	HistorySize               int
}

// DefaultErrorRecoveryConfig returns default configuration.
func DefaultErrorRecoveryConfig() ErrorRecoveryConfig {
	return ErrorRecoveryConfig{
		MaxConcurrentRecoveries:   10,
		RecoveryTimeout:           30 * time.Second,
		MaxRecoveryAttempts:       3,
		EnableCircuitBreaker:      true,
		EnableRetry:               true,
		EnableFallback:            true,
		EnableGracefulDegradation: true,
		ResourceLimits: &ResourceLimits{
			MaxCPUUsage:    0.8,
			MaxMemoryUsage: 1024 * 1024 * 1024, // 1GB
			MaxGoroutines:  1000,
			MaxDuration:    5 * time.Minute,
		},
		HistorySize: 100,
	}
}

// NewErrorRecoverySystem creates a new error recovery system.
func NewErrorRecoverySystem(config ErrorRecoveryConfig) *ErrorRecoverySystem {
	if config.MaxConcurrentRecoveries <= 0 {
		config.MaxConcurrentRecoveries = 10
	}
	if config.RecoveryTimeout <= 0 {
		config.RecoveryTimeout = 30 * time.Second
	}
	if config.MaxRecoveryAttempts <= 0 {
		config.MaxRecoveryAttempts = 3
	}
	if config.HistorySize <= 0 {
		config.HistorySize = 100
	}

	ers := &ErrorRecoverySystem{
		config:          config,
		enabled:         true,
		maxRecoveries:   safety.SaturatingInt32(config.MaxConcurrentRecoveries),
		historySize:     config.HistorySize,
		recoveryHistory: make([]RecoveryResult, 0, config.HistorySize),
		strategyUsage:   make(map[string]uint64),
		shutdownCh:      make(chan struct{}),
		resourceMonitor: NewResourceMonitor(),
	}

	// Initialize circuit breaker if enabled
	if config.EnableCircuitBreaker {
		cbConfig := DefaultCircuitBreakerConfig()
		ers.circuitBreaker = NewCircuitBreaker("error_recovery", cbConfig)
	}

	// Initialize retryer if enabled
	if config.EnableRetry {
		retryConfig := DefaultRetryConfig()
		ers.retryer = NewRetryer(retryConfig)
	}

	// Register default recovery strategies
	ers.registerDefaultStrategies()

	return ers
}

// registerDefaultStrategies registers default recovery strategies.
func (ers *ErrorRecoverySystem) registerDefaultStrategies() {
	if ers.config.EnableRetry {
		ers.RegisterStrategy(&retryRecoveryStrategy{retryer: ers.retryer})
	}
	if ers.config.EnableCircuitBreaker {
		ers.RegisterStrategy(&circuitBreakerRecoveryStrategy{circuitBreaker: ers.circuitBreaker})
	}
	if ers.config.EnableFallback {
		ers.RegisterStrategy(&fallbackRecoveryStrategy{})
	}
	if ers.config.EnableGracefulDegradation {
		ers.RegisterStrategy(&gracefulDegradationStrategy{})
	}
}

// RegisterStrategy registers a recovery strategy.
func (ers *ErrorRecoverySystem) RegisterStrategy(strategy RecoveryStrategy) {
	ers.mu.Lock()
	defer ers.mu.Unlock()

	ers.strategies = append(ers.strategies, strategy)
	ers.strategyUsage[strategy.Name()] = 0
}

// SetFallbackHandler sets a fallback handler for when no strategy can recover.
func (ers *ErrorRecoverySystem) SetFallbackHandler(handler RecoveryStrategy) {
	ers.mu.Lock()
	defer ers.mu.Unlock()

	ers.fallbackHandler = handler
}

// Recover attempts to recover from an error using registered strategies.
func (ers *ErrorRecoverySystem) Recover(ctx context.Context, err error) (*RecoveryResult, error) {
	if !ers.enabled {
		return nil, errRecoveryDisabled
	}

	// Check if we can start a new recovery
	if atomic.LoadInt32(&ers.activeRecoveries) >= ers.maxRecoveries {
		return &RecoveryResult{
			Success:     false,
			Action:      actionAbort,
			Error:       errors.New("too many concurrent recoveries"),
			RecoveredBy: "system",
		}, errRecoveryFailed
	}

	// Increment active recoveries
	atomic.AddInt32(&ers.activeRecoveries, 1)
	defer atomic.AddInt32(&ers.activeRecoveries, -1)

	// Create recovery context
	recoveryCtx := &RecoveryContext{
		OriginalError:    err,
		Attempt:          1,
		MaxAttempts:      ers.config.MaxRecoveryAttempts,
		StartTime:        time.Now(),
		LastRecoveryTime: time.Now(),
		Metadata:         make(map[string]interface{}),
		ResourceLimits:   ers.config.ResourceLimits,
	}

	// Create timeout context
	timeoutCtx := ctx
	if ers.config.RecoveryTimeout > 0 {
		var cancel context.CancelFunc
		timeoutCtx, cancel = context.WithTimeout(ctx, ers.config.RecoveryTimeout)
		defer cancel()
	}

	// Try recovery strategies
	result := ers.tryRecovery(timeoutCtx, recoveryCtx)

	// Record metrics
	ers.recordRecovery(result)

	return result, nil
}

// tryRecovery attempts recovery using available strategies.
func (ers *ErrorRecoverySystem) tryRecovery(ctx context.Context, recoveryCtx *RecoveryContext) *RecoveryResult {
	ers.mu.RLock()
	strategies := make([]RecoveryStrategy, len(ers.strategies))
	copy(strategies, ers.strategies)
	ers.mu.RUnlock()

	for _, strategy := range strategies {
		// Check if strategy can recover this error
		if !strategy.CanRecover(recoveryCtx.OriginalError) {
			continue
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return &RecoveryResult{
				Success:     false,
				Action:      actionAbort,
				Error:       ctx.Err(),
				RecoveredBy: "context",
			}
		default:
		}

		// Check resource limits
		if ers.config.ResourceLimits != nil && !ers.checkResourceLimits(recoveryCtx) {
			continue
		}

		// Attempt recovery
		startTime := time.Now()
		err := strategy.Recover(ctx, recoveryCtx.OriginalError)
		duration := time.Since(startTime)

		// Update strategy usage
		ers.mu.Lock()
		ers.strategyUsage[strategy.Name()]++
		ers.mu.Unlock()

		// Check if recovery succeeded
		if err == nil {
			return &RecoveryResult{
				Success:     true,
				Action:      ers.getActionForStrategy(strategy),
				Duration:    duration,
				RecoveredBy: strategy.Name(),
				NextAction:  actionRetry,
				Metadata:    recoveryCtx.Metadata,
			}
		}

		// Recovery failed, continue to next strategy
		recoveryCtx.Attempt++
		recoveryCtx.LastRecoveryTime = time.Now()
	}

	// Try fallback handler if available
	if ers.fallbackHandler != nil {
		startTime := time.Now()
		err := ers.fallbackHandler.Recover(ctx, recoveryCtx.OriginalError)
		duration := time.Since(startTime)

		if err == nil {
			return &RecoveryResult{
				Success:     true,
				Action:      actionFallback,
				Duration:    duration,
				RecoveredBy: ers.fallbackHandler.Name(),
				NextAction:  actionSkip,
				Metadata:    recoveryCtx.Metadata,
			}
		}
	}

	// No recovery possible
	return &RecoveryResult{
		Success:     false,
		Action:      actionAbort,
		Duration:    time.Since(recoveryCtx.StartTime),
		Error:       fmt.Errorf("%w: %v", errNoRecoveryHandler, recoveryCtx.OriginalError),
		RecoveredBy: "none",
	}
}

// checkResourceLimits checks if resource limits are exceeded.
func (ers *ErrorRecoverySystem) checkResourceLimits(ctx *RecoveryContext) bool {
	if ctx.ResourceLimits == nil {
		return true
	}

	limits := ctx.ResourceLimits

	// Check CPU usage
	if limits.MaxCPUUsage > 0 {
		if cpuUsage := ers.resourceMonitor.GetCPUUsage(); cpuUsage > limits.MaxCPUUsage {
			return false
		}
	}

	// Check memory usage
	if limits.MaxMemoryUsage > 0 {
		if memUsage := ers.resourceMonitor.GetMemoryUsage(); memUsage > limits.MaxMemoryUsage {
			return false
		}
	}

	// Check goroutine count
	if limits.MaxGoroutines > 0 {
		if goroutines := runtime.NumGoroutine(); goroutines > limits.MaxGoroutines {
			return false
		}
	}

	// Check duration
	if limits.MaxDuration > 0 {
		if elapsed := time.Since(ctx.StartTime); elapsed > limits.MaxDuration {
			return false
		}
	}

	return true
}

// getActionForStrategy returns the appropriate action for a strategy.
func (ers *ErrorRecoverySystem) getActionForStrategy(strategy RecoveryStrategy) RecoveryAction {
	switch strategy.(type) {
	case *retryRecoveryStrategy:
		return actionRetry
	case *circuitBreakerRecoveryStrategy:
		return actionCircuitBreaker
	case *fallbackRecoveryStrategy:
		return actionFallback
	case *gracefulDegradationStrategy:
		return actionGracefulDegradation
	default:
		return actionRetry
	}
}

// recordRecovery records recovery metrics and history.
func (ers *ErrorRecoverySystem) recordRecovery(result *RecoveryResult) {
	ers.mu.Lock()
	defer ers.mu.Unlock()

	// Update metrics
	ers.totalRecoveries++
	ers.lastRecoveryTime = time.Now()

	if result.Success {
		ers.successfulRecoveries++
	} else {
		ers.failedRecoveries++
	}

	// Update average recovery time
	totalOps := ers.totalRecoveries
	if totalOps > 0 {
		totalOpsInt := safety.SaturatingInt64(totalOps)
		priorOpsInt := safety.SaturatingInt64(totalOps - 1)
		ers.averageRecoveryTime = time.Duration(
			(int64(ers.averageRecoveryTime)*priorOpsInt + int64(result.Duration)) / totalOpsInt,
		)
	}

	// Add to history
	ers.recoveryHistory = append(ers.recoveryHistory, *result)
	if len(ers.recoveryHistory) > ers.historySize {
		ers.recoveryHistory = ers.recoveryHistory[1:]
	}
}

// Enable enables the error recovery system.
func (ers *ErrorRecoverySystem) Enable() {
	ers.mu.Lock()
	defer ers.mu.Unlock()

	ers.enabled = true
}

// Disable disables the error recovery system.
func (ers *ErrorRecoverySystem) Disable() {
	ers.mu.Lock()
	defer ers.mu.Unlock()

	ers.enabled = false
}

// IsEnabled returns whether the error recovery system is enabled.
func (ers *ErrorRecoverySystem) IsEnabled() bool {
	ers.mu.RLock()
	defer ers.mu.RUnlock()

	return ers.enabled
}

// ErrorRecoveryMetrics contains metrics about error recovery.
type ErrorRecoveryMetrics struct {
	TotalRecoveries      uint64
	SuccessfulRecoveries uint64
	FailedRecoveries     uint64
	SuccessRate          float64
	ActiveRecoveries     int32
	MaxRecoveries        int32
	AverageRecoveryTime  time.Duration
	LastRecoveryTime     time.Time
	StrategyUsage        map[string]uint64
	RecentHistory        []RecoveryResult
	ResourceUsage        ResourceUsage
}

// Metrics returns metrics about the error recovery system.
func (ers *ErrorRecoverySystem) Metrics() ErrorRecoveryMetrics {
	ers.mu.RLock()
	defer ers.mu.RUnlock()

	var successRate float64
	if ers.totalRecoveries > 0 {
		successRate = float64(ers.successfulRecoveries) / float64(ers.totalRecoveries)
	}

	// Get recent history (last 10 entries)
	historySize := 10
	if len(ers.recoveryHistory) < historySize {
		historySize = len(ers.recoveryHistory)
	}
	recentHistory := make([]RecoveryResult, historySize)
	if historySize > 0 {
		copy(recentHistory, ers.recoveryHistory[len(ers.recoveryHistory)-historySize:])
	}

	// Copy strategy usage
	strategyUsage := make(map[string]uint64)
	for strategy, count := range ers.strategyUsage {
		strategyUsage[strategy] = count
	}

	return ErrorRecoveryMetrics{
		TotalRecoveries:      ers.totalRecoveries,
		SuccessfulRecoveries: ers.successfulRecoveries,
		FailedRecoveries:     ers.failedRecoveries,
		SuccessRate:          successRate,
		ActiveRecoveries:     atomic.LoadInt32(&ers.activeRecoveries),
		MaxRecoveries:        ers.maxRecoveries,
		AverageRecoveryTime:  ers.averageRecoveryTime,
		LastRecoveryTime:     ers.lastRecoveryTime,
		StrategyUsage:        strategyUsage,
		RecentHistory:        recentHistory,
		ResourceUsage:        ers.resourceMonitor.GetResourceUsage(),
	}
}

// Reset resets the error recovery system metrics.
func (ers *ErrorRecoverySystem) Reset() {
	ers.mu.Lock()
	defer ers.mu.Unlock()

	ers.totalRecoveries = 0
	ers.successfulRecoveries = 0
	ers.failedRecoveries = 0
	ers.averageRecoveryTime = 0
	ers.lastRecoveryTime = time.Time{}
	ers.recoveryHistory = ers.recoveryHistory[:0]

	// Reset strategy usage
	for strategy := range ers.strategyUsage {
		ers.strategyUsage[strategy] = 0
	}
}

// Shutdown shuts down the error recovery system.
func (ers *ErrorRecoverySystem) Shutdown(ctx context.Context) error {
	close(ers.shutdownCh)

	// Wait for active recoveries to complete
	done := make(chan struct{})
	go func() {
		ers.shutdownWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("error recovery shutdown cancelled: %w", ctx.Err())
	}
}

// Concrete Recovery Strategy Implementations

// retryRecoveryStrategy implements retry-based recovery.
type retryRecoveryStrategy struct {
	retryer *Retryer
}

func (r *retryRecoveryStrategy) Name() string {
	return "retry"
}

func (r *retryRecoveryStrategy) CanRecover(err error) bool {
	if r.retryer == nil {
		return false
	}
	return r.retryer.GetConfig().RetryCondition(err)
}

func (r *retryRecoveryStrategy) Recover(ctx context.Context, err error) error {
	if r.retryer == nil {
		return errors.New("no retryer configured")
	}

	// Create a dummy function that returns the error
	// In a real implementation, this would retry the actual operation
	return r.retryer.Execute(ctx, func() error {
		return err
	})
}

// circuitBreakerRecoveryStrategy implements circuit breaker recovery.
type circuitBreakerRecoveryStrategy struct {
	circuitBreaker *CircuitBreaker
}

func (c *circuitBreakerRecoveryStrategy) Name() string {
	return "circuit_breaker"
}

func (c *circuitBreakerRecoveryStrategy) CanRecover(_ error) bool {
	if c.circuitBreaker == nil {
		return false
	}
	return c.circuitBreaker.IsReady()
}

func (c *circuitBreakerRecoveryStrategy) Recover(ctx context.Context, _ error) error {
	if c.circuitBreaker == nil {
		return errors.New("no circuit breaker configured")
	}

	// Use circuit breaker to execute a recovery function
	return c.circuitBreaker.Execute(ctx, func() error {
		// In a real implementation, this would execute the actual recovery logic
		// For now, we simulate a successful recovery
		return nil
	})
}

// fallbackRecoveryStrategy implements fallback recovery.
type fallbackRecoveryStrategy struct {
	fallbackFunc func(context.Context, error) error
}

func (f *fallbackRecoveryStrategy) Name() string {
	return "fallback"
}

func (f *fallbackRecoveryStrategy) CanRecover(_ error) bool {
	return true // Fallback can always be attempted
}

func (f *fallbackRecoveryStrategy) Recover(ctx context.Context, err error) error {
	if f.fallbackFunc != nil {
		return f.fallbackFunc(ctx, err)
	}

	// Default fallback: log the error and continue
	// In a real implementation, this would implement actual fallback logic
	return nil
}

// gracefulDegradationStrategy implements graceful degradation.
type gracefulDegradationStrategy struct {
	degradationFunc func(context.Context, error) error
}

func (g *gracefulDegradationStrategy) Name() string {
	return "graceful_degradation"
}

func (g *gracefulDegradationStrategy) CanRecover(_ error) bool {
	return true // Graceful degradation can always be attempted
}

func (g *gracefulDegradationStrategy) Recover(ctx context.Context, err error) error {
	if g.degradationFunc != nil {
		return g.degradationFunc(ctx, err)
	}

	// Default degradation: reduce functionality but continue
	// In a real implementation, this would implement actual degradation logic
	return nil
}

// ResourceMonitor monitors system resources.
type ResourceMonitor struct{}

// ResourceUsage contains current resource usage information.
type ResourceUsage struct {
	CPUUsage       float64
	MemoryUsage    int64
	GoroutineCount int
	Timestamp      time.Time
}

// NewResourceMonitor creates a new resource monitor.
func NewResourceMonitor() *ResourceMonitor {
	return &ResourceMonitor{}
}

// GetCPUUsage returns current CPU usage (placeholder implementation).
func (rm *ResourceMonitor) GetCPUUsage() float64 {
	// In a real implementation, this would measure actual CPU usage
	return 0.5
}

// GetMemoryUsage returns current memory usage.
func (rm *ResourceMonitor) GetMemoryUsage() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return safety.SaturatingInt64(m.Alloc)
}

// GetResourceUsage returns current resource usage.
func (rm *ResourceMonitor) GetResourceUsage() ResourceUsage {
	return ResourceUsage{
		CPUUsage:       rm.GetCPUUsage(),
		MemoryUsage:    rm.GetMemoryUsage(),
		GoroutineCount: runtime.NumGoroutine(),
		Timestamp:      time.Now(),
	}
}
