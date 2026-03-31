package processing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/safety"
)

var errDegradationDisabled = errors.New("graceful degradation is disabled")

// DegradationLevel represents the level of degradation.
type DegradationLevel int

const (
	degradationNone DegradationLevel = iota
	degradationMinimal
	degradationPartial
	degradationSignificant
	degradationSevere
	degradationEmergency
)

func (d DegradationLevel) String() string {
	switch d {
	case degradationNone:
		return "NONE"
	case degradationMinimal:
		return "MINIMAL"
	case degradationPartial:
		return "PARTIAL"
	case degradationSignificant:
		return "SIGNIFICANT"
	case degradationSevere:
		return "SEVERE"
	case degradationEmergency:
		return "EMERGENCY"
	default:
		return unknownLabel
	}
}

// DegradationMode defines how the system should behave in degraded mode.
type DegradationMode int

const (
	modeReducedFunctionality DegradationMode = iota
	modeEssentialOnly
	modeReadOnly
	modeFailfast
	modeOffline
)

func (m DegradationMode) String() string {
	switch m {
	case modeReducedFunctionality:
		return "REDUCED_FUNCTIONALITY"
	case modeEssentialOnly:
		return "ESSENTIAL_ONLY"
	case modeReadOnly:
		return "READ_ONLY"
	case modeFailfast:
		return "FAILFAST"
	case modeOffline:
		return "OFFLINE"
	default:
		return unknownLabel
	}
}

// DegradationTrigger defines what triggers degradation.
type DegradationTrigger int

const (
	triggerErrorRate DegradationTrigger = iota
	triggerLatency
	triggerResourceUsage
	triggerDependencyFailure
	triggerManual
	triggerCircuitBreaker
)

func (t DegradationTrigger) String() string {
	switch t {
	case triggerErrorRate:
		return "ERROR_RATE"
	case triggerLatency:
		return "LATENCY"
	case triggerResourceUsage:
		return "RESOURCE_USAGE"
	case triggerDependencyFailure:
		return "DEPENDENCY_FAILURE"
	case triggerManual:
		return "MANUAL"
	case triggerCircuitBreaker:
		return "CIRCUIT_BREAKER"
	default:
		return unknownLabel
	}
}

// DegradationRule defines when and how to degrade.
type DegradationRule struct {
	Name      string
	Trigger   DegradationTrigger
	Condition func(metrics *SystemMetrics) bool
	Action    DegradationAction
	Level     DegradationLevel
	Mode      DegradationMode
	Duration  time.Duration
	Priority  int
	Enabled   bool
	Metadata  map[string]interface{}
}

// DegradationAction defines what action to take.
type DegradationAction struct {
	Type          DegradationActionType
	Features      []string
	Limits        *PerformanceLimits
	Fallback      func(context.Context) error
	RecoveryCheck func(context.Context, *SystemMetrics) bool
	Description   string
}

// DegradationActionType defines the type of degradation action.
type DegradationActionType int

const (
	actionDisableFeatures DegradationActionType = iota
	actionReducePerformance
	actionReduceAccuracy
	actionSimplifyCaching
	actionLimitConnections
	actionSkipProcessing
	actionUseDefaults
	actionEnableReadOnly
)

func (a DegradationActionType) String() string {
	switch a {
	case actionDisableFeatures:
		return "DISABLE_FEATURES"
	case actionReducePerformance:
		return "REDUCE_PERFORMANCE"
	case actionReduceAccuracy:
		return "REDUCE_ACCURACY"
	case actionSimplifyCaching:
		return "SIMPLIFY_CACHING"
	case actionLimitConnections:
		return "LIMIT_CONNECTIONS"
	case actionSkipProcessing:
		return "SKIP_PROCESSING"
	case actionUseDefaults:
		return "USE_DEFAULTS"
	case actionEnableReadOnly:
		return "ENABLE_READ_ONLY"
	default:
		return unknownLabel
	}
}

// PerformanceLimits defines performance limits during degradation.
type PerformanceLimits struct {
	MaxConcurrency   int
	MaxThroughput    int
	MaxLatency       time.Duration
	MaxMemoryUsage   int64
	MaxCPUUsage      float64
	ReducedWorkers   int
	SimplifiedLogic  bool
	SkipValidation   bool
	UseApproximation bool
}

// SystemMetrics contains system health metrics.
type SystemMetrics struct {
	ErrorRate         float64
	AverageLatency    time.Duration
	P95Latency        time.Duration
	P99Latency        time.Duration
	CPUUsage          float64
	MemoryUsage       int64
	ActiveConnections int
	ThroughputRPS     int
	DependencyHealth  map[string]bool
	Timestamp         time.Time
}

// DegradationStatus represents the current degradation status.
type DegradationStatus struct {
	Active           bool
	Level            DegradationLevel
	Mode             DegradationMode
	Trigger          DegradationTrigger
	StartTime        time.Time
	EstimatedEnd     time.Time
	ActiveRules      []string
	DisabledFeatures []string
	Limits           *PerformanceLimits
	Reason           string
	RecoveryProgress float64
}

// GracefulDegradationManager manages graceful degradation.
type GracefulDegradationManager struct {
	// Configuration
	rules         []DegradationRule
	globalLimits  *PerformanceLimits
	enabled       bool
	autoRecovery  bool
	recoveryDelay time.Duration

	// State
	mu               sync.RWMutex
	currentLevel     DegradationLevel
	currentMode      DegradationMode
	currentTrigger   DegradationTrigger
	degradationStart time.Time
	activeRules      map[string]bool
	disabledFeatures map[string]bool

	// Metrics
	metricsCollector *MetricsCollector

	// Control
	stopCh chan struct{}
	stopWg sync.WaitGroup

	// Callbacks
	onDegradationStart func(status DegradationStatus)
	onDegradationEnd   func(status DegradationStatus)
	onLevelChange      func(from, to DegradationLevel)
	onModeChange       func(from, to DegradationMode)

	// Metrics
	degradationCount     uint64
	totalDegradationTime time.Duration
	avgDegradationTime   time.Duration
	recoveryCount        uint64
	avgRecoveryTime      time.Duration
	ruleActivationCount  map[string]uint64
}

// GracefulDegradationConfig holds configuration for graceful degradation.
type GracefulDegradationConfig struct {
	Enabled            bool
	AutoRecovery       bool
	RecoveryDelay      time.Duration
	MonitoringInterval time.Duration
	GlobalLimits       *PerformanceLimits
	Rules              []DegradationRule
	Callbacks          DegradationCallbacks
}

// DegradationCallbacks holds callback functions.
type DegradationCallbacks struct {
	OnDegradationStart func(status DegradationStatus)
	OnDegradationEnd   func(status DegradationStatus)
	OnLevelChange      func(from, to DegradationLevel)
	OnModeChange       func(from, to DegradationMode)
}

// DefaultGracefulDegradationConfig returns default configuration.
func DefaultGracefulDegradationConfig() GracefulDegradationConfig {
	return GracefulDegradationConfig{
		Enabled:            true,
		AutoRecovery:       true,
		RecoveryDelay:      30 * time.Second,
		MonitoringInterval: 5 * time.Second,
		GlobalLimits: &PerformanceLimits{
			MaxConcurrency:   100,
			MaxThroughput:    1000,
			MaxLatency:       5 * time.Second,
			MaxMemoryUsage:   1024 * 1024 * 1024, // 1GB
			MaxCPUUsage:      0.8,
			ReducedWorkers:   50,
			SimplifiedLogic:  false,
			SkipValidation:   false,
			UseApproximation: false,
		},
		Rules: []DegradationRule{
			{
				Name:    "high_error_rate",
				Trigger: triggerErrorRate,
				Condition: func(metrics *SystemMetrics) bool {
					return metrics.ErrorRate > 0.1 // 10% error rate
				},
				Action: DegradationAction{
					Type:        actionReducePerformance,
					Description: "Reduce performance due to high error rate",
				},
				Level:    degradationMinimal,
				Mode:     modeReducedFunctionality,
				Duration: 5 * time.Minute,
				Priority: 1,
				Enabled:  true,
			},
			{
				Name:    "high_latency",
				Trigger: triggerLatency,
				Condition: func(metrics *SystemMetrics) bool {
					return metrics.P95Latency > 10*time.Second
				},
				Action: DegradationAction{
					Type:        actionReduceAccuracy,
					Description: "Reduce accuracy due to high latency",
				},
				Level:    degradationPartial,
				Mode:     modeReducedFunctionality,
				Duration: 10 * time.Minute,
				Priority: 2,
				Enabled:  true,
			},
			{
				Name:    "resource_exhaustion",
				Trigger: triggerResourceUsage,
				Condition: func(metrics *SystemMetrics) bool {
					return metrics.CPUUsage > 0.9 || metrics.MemoryUsage > 966367641 // 0.9 GB
				},
				Action: DegradationAction{
					Type:        actionLimitConnections,
					Description: "Limit connections due to resource exhaustion",
				},
				Level:    degradationSignificant,
				Mode:     modeEssentialOnly,
				Duration: 15 * time.Minute,
				Priority: 3,
				Enabled:  true,
			},
		},
		Callbacks: DegradationCallbacks{
			OnDegradationStart: func(_ DegradationStatus) {},
			OnDegradationEnd:   func(_ DegradationStatus) {},
			OnLevelChange:      func(_, _ DegradationLevel) {},
			OnModeChange:       func(_, _ DegradationMode) {},
		},
	}
}

// NewGracefulDegradationManager creates a new graceful degradation manager.
func NewGracefulDegradationManager(config GracefulDegradationConfig) *GracefulDegradationManager {
	gdm := &GracefulDegradationManager{
		rules:               config.Rules,
		globalLimits:        config.GlobalLimits,
		enabled:             config.Enabled,
		autoRecovery:        config.AutoRecovery,
		recoveryDelay:       config.RecoveryDelay,
		currentLevel:        degradationNone,
		currentMode:         modeReducedFunctionality,
		activeRules:         make(map[string]bool),
		disabledFeatures:    make(map[string]bool),
		metricsCollector:    NewMetricsCollector(),
		stopCh:              make(chan struct{}),
		onDegradationStart:  config.Callbacks.OnDegradationStart,
		onDegradationEnd:    config.Callbacks.OnDegradationEnd,
		onLevelChange:       config.Callbacks.OnLevelChange,
		onModeChange:        config.Callbacks.OnModeChange,
		ruleActivationCount: make(map[string]uint64),
	}

	// Initialize rule activation counts
	for _, rule := range config.Rules {
		gdm.ruleActivationCount[rule.Name] = 0
	}

	// Start monitoring if enabled
	if config.Enabled {
		gdm.startMonitoring(config.MonitoringInterval)
	}

	return gdm
}

// startMonitoring starts the degradation monitoring loop.
func (gdm *GracefulDegradationManager) startMonitoring(interval time.Duration) {
	gdm.stopWg.Add(1)
	go func() {
		defer gdm.stopWg.Done()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-gdm.stopCh:
				return
			case <-ticker.C:
				gdm.checkDegradationConditions()
			}
		}
	}()
}

// checkDegradationConditions checks if degradation should be triggered.
func (gdm *GracefulDegradationManager) checkDegradationConditions() {
	if !gdm.enabled {
		return
	}

	// Get current metrics
	metrics := gdm.metricsCollector.GetCurrentMetrics()

	gdm.mu.Lock()
	defer gdm.mu.Unlock()

	// Check each rule
	triggeredRules := make([]DegradationRule, 0)

	for _, rule := range gdm.rules {
		if !rule.Enabled {
			continue
		}

		if rule.Condition(metrics) {
			triggeredRules = append(triggeredRules, rule)
		}
	}

	// Determine if degradation should be activated
	if len(triggeredRules) > 0 {
		gdm.activateDegradation(triggeredRules)
	} else if gdm.autoRecovery && gdm.currentLevel != degradationNone {
		gdm.checkRecovery(metrics)
	}
}

// activateDegradation activates degradation based on triggered rules.
func (gdm *GracefulDegradationManager) activateDegradation(rules []DegradationRule) {
	// Find the highest priority rule
	var highestPriorityRule *DegradationRule
	for i, rule := range rules {
		if highestPriorityRule == nil || rule.Priority > highestPriorityRule.Priority {
			highestPriorityRule = &rules[i]
		}
	}

	if highestPriorityRule == nil {
		return
	}

	// Check if we need to change degradation level
	previousLevel := gdm.currentLevel
	previousMode := gdm.currentMode

	if highestPriorityRule.Level == gdm.currentLevel {
		return
	}

	gdm.currentLevel = highestPriorityRule.Level
	gdm.currentMode = highestPriorityRule.Mode
	gdm.currentTrigger = highestPriorityRule.Trigger
	gdm.ruleActivationCount[highestPriorityRule.Name]++
	gdm.recordDegradationStart(previousLevel)
	gdm.notifyDegradationTransition(previousLevel, previousMode)
	gdm.activeRules[highestPriorityRule.Name] = true
	gdm.applyDegradationAction(highestPriorityRule.Action)
}

// applyDegradationAction applies a degradation action.
func (gdm *GracefulDegradationManager) applyDegradationAction(action DegradationAction) {
	switch action.Type {
	case actionDisableFeatures:
		for _, feature := range action.Features {
			gdm.disabledFeatures[feature] = true
		}
	case actionReducePerformance:
		if action.Limits != nil {
			gdm.globalLimits = action.Limits
		}
	case actionReduceAccuracy:
		if action.Limits != nil {
			gdm.globalLimits.UseApproximation = action.Limits.UseApproximation
		}
	case actionSimplifyCaching:
		if action.Limits != nil {
			gdm.globalLimits.SimplifiedLogic = action.Limits.SimplifiedLogic
		}
	case actionLimitConnections:
		if action.Limits != nil {
			gdm.globalLimits.MaxConcurrency = action.Limits.MaxConcurrency
		}
	case actionSkipProcessing:
		if action.Limits != nil {
			gdm.globalLimits.SkipValidation = action.Limits.SkipValidation
		}
	case actionUseDefaults:
		// Use default values for processing
	case actionEnableReadOnly:
		// Enable read-only mode
	}
}

// checkRecovery checks if the system can recover from degradation.
func (gdm *GracefulDegradationManager) checkRecovery(metrics *SystemMetrics) {
	// Check if recovery conditions are met
	canRecover := true

	for ruleName := range gdm.activeRules {
		rule := gdm.findRuleByName(ruleName)
		if rule == nil {
			continue
		}
		if gdm.recoveryBlocked(rule, metrics) {
			canRecover = false
			break
		}
	}

	if canRecover {
		// Wait for recovery delay
		if time.Since(gdm.degradationStart) >= gdm.recoveryDelay {
			gdm.recover()
		}
	}
}

func (gdm *GracefulDegradationManager) recordDegradationStart(previousLevel DegradationLevel) {
	if previousLevel != degradationNone {
		return
	}

	gdm.degradationStart = time.Now()
	gdm.degradationCount++

	if gdm.onDegradationStart != nil {
		gdm.onDegradationStart(gdm.getCurrentStatus())
	}
}

func (gdm *GracefulDegradationManager) notifyDegradationTransition(previousLevel DegradationLevel, previousMode DegradationMode) {
	if gdm.onLevelChange != nil && previousLevel != gdm.currentLevel {
		gdm.onLevelChange(previousLevel, gdm.currentLevel)
	}
	if gdm.onModeChange != nil && previousMode != gdm.currentMode {
		gdm.onModeChange(previousMode, gdm.currentMode)
	}
}

func (gdm *GracefulDegradationManager) recoveryBlocked(rule *DegradationRule, metrics *SystemMetrics) bool {
	if rule.Action.RecoveryCheck != nil {
		return !rule.Action.RecoveryCheck(context.Background(), metrics)
	}

	return rule.Condition(metrics)
}

// recover recovers from degradation.
func (gdm *GracefulDegradationManager) recover() {
	previousLevel := gdm.currentLevel
	previousMode := gdm.currentMode

	// Reset degradation state
	gdm.currentLevel = degradationNone
	gdm.currentMode = modeReducedFunctionality
	gdm.currentTrigger = triggerManual

	// Clear active rules and disabled features
	gdm.activeRules = make(map[string]bool)
	gdm.disabledFeatures = make(map[string]bool)

	// Reset limits to default
	config := DefaultGracefulDegradationConfig()
	gdm.globalLimits = config.GlobalLimits

	// Record recovery metrics
	gdm.recoveryCount++
	degradationDuration := time.Since(gdm.degradationStart)
	gdm.totalDegradationTime += degradationDuration

	// Update average degradation time
	if gdm.degradationCount > 0 {
		gdm.avgDegradationTime = gdm.totalDegradationTime / safety.SaturatingDuration(gdm.degradationCount)
	}

	// Update average recovery time
	if gdm.recoveryCount > 0 {
		gdm.avgRecoveryTime = gdm.totalDegradationTime / safety.SaturatingDuration(gdm.recoveryCount)
	}

	// Notify recovery
	if gdm.onDegradationEnd != nil {
		status := gdm.getCurrentStatus()
		gdm.onDegradationEnd(status)
	}

	// Notify level change
	if gdm.onLevelChange != nil {
		gdm.onLevelChange(previousLevel, gdm.currentLevel)
	}

	// Notify mode change
	if gdm.onModeChange != nil {
		gdm.onModeChange(previousMode, gdm.currentMode)
	}
}

// findRuleByName finds a rule by name.
func (gdm *GracefulDegradationManager) findRuleByName(name string) *DegradationRule {
	for i, rule := range gdm.rules {
		if rule.Name == name {
			return &gdm.rules[i]
		}
	}
	return nil
}

// getCurrentStatus returns the current degradation status.
func (gdm *GracefulDegradationManager) getCurrentStatus() DegradationStatus {
	activeRules := make([]string, 0, len(gdm.activeRules))
	for rule := range gdm.activeRules {
		activeRules = append(activeRules, rule)
	}

	disabledFeatures := make([]string, 0, len(gdm.disabledFeatures))
	for feature := range gdm.disabledFeatures {
		disabledFeatures = append(disabledFeatures, feature)
	}

	var estimatedEnd time.Time
	if gdm.currentLevel != degradationNone {
		estimatedEnd = gdm.degradationStart.Add(gdm.avgDegradationTime)
	}

	return DegradationStatus{
		Active:           gdm.currentLevel != degradationNone,
		Level:            gdm.currentLevel,
		Mode:             gdm.currentMode,
		Trigger:          gdm.currentTrigger,
		StartTime:        gdm.degradationStart,
		EstimatedEnd:     estimatedEnd,
		ActiveRules:      activeRules,
		DisabledFeatures: disabledFeatures,
		Limits:           gdm.globalLimits,
		RecoveryProgress: gdm.calculateRecoveryProgress(),
	}
}

// calculateRecoveryProgress calculates recovery progress.
func (gdm *GracefulDegradationManager) calculateRecoveryProgress() float64 {
	if gdm.currentLevel == degradationNone {
		return 1.0
	}

	if gdm.avgDegradationTime == 0 {
		return 0.0
	}

	elapsed := time.Since(gdm.degradationStart)
	progress := float64(elapsed) / float64(gdm.avgDegradationTime)

	if progress > 1.0 {
		progress = 1.0
	}

	return progress
}

// ManualDegrade manually triggers degradation.
func (gdm *GracefulDegradationManager) ManualDegrade(level DegradationLevel, mode DegradationMode, duration time.Duration) error {
	gdm.mu.Lock()
	defer gdm.mu.Unlock()

	if !gdm.enabled {
		return errDegradationDisabled
	}

	previousLevel := gdm.currentLevel
	previousMode := gdm.currentMode

	gdm.currentLevel = level
	gdm.currentMode = mode
	gdm.currentTrigger = triggerManual

	if previousLevel == degradationNone {
		gdm.degradationStart = time.Now()
		gdm.degradationCount++

		if gdm.onDegradationStart != nil {
			status := gdm.getCurrentStatus()
			gdm.onDegradationStart(status)
		}
	}

	if gdm.onLevelChange != nil && previousLevel != gdm.currentLevel {
		gdm.onLevelChange(previousLevel, gdm.currentLevel)
	}

	if gdm.onModeChange != nil && previousMode != gdm.currentMode {
		gdm.onModeChange(previousMode, gdm.currentMode)
	}

	// Schedule recovery if duration is specified
	if duration > 0 {
		time.AfterFunc(duration, func() {
			gdm.mu.Lock()
			defer gdm.mu.Unlock()
			gdm.recover()
		})
	}

	return nil
}

// ManualRecover manually triggers recovery.
func (gdm *GracefulDegradationManager) ManualRecover() error {
	gdm.mu.Lock()
	defer gdm.mu.Unlock()

	if !gdm.enabled {
		return errDegradationDisabled
	}

	if gdm.currentLevel == degradationNone {
		return errors.New("system is not in degraded mode")
	}

	gdm.recover()
	return nil
}

// IsFeatureEnabled checks if a feature is enabled.
func (gdm *GracefulDegradationManager) IsFeatureEnabled(feature string) bool {
	gdm.mu.RLock()
	defer gdm.mu.RUnlock()

	return !gdm.disabledFeatures[feature]
}

// GetCurrentLimits returns current performance limits.
func (gdm *GracefulDegradationManager) GetCurrentLimits() *PerformanceLimits {
	gdm.mu.RLock()
	defer gdm.mu.RUnlock()

	// Return a copy to prevent external modification
	if gdm.globalLimits == nil {
		return nil
	}

	limits := *gdm.globalLimits
	return &limits
}

// GetStatus returns the current degradation status.
func (gdm *GracefulDegradationManager) GetStatus() DegradationStatus {
	gdm.mu.RLock()
	defer gdm.mu.RUnlock()

	return gdm.getCurrentStatus()
}

// DegradationMetrics contains metrics about degradation.
type DegradationMetrics struct {
	DegradationCount     uint64
	RecoveryCount        uint64
	TotalDegradationTime time.Duration
	AvgDegradationTime   time.Duration
	AvgRecoveryTime      time.Duration
	CurrentLevel         DegradationLevel
	CurrentMode          DegradationMode
	RuleActivationCount  map[string]uint64
	IsActive             bool
	LastDegradationTime  time.Time
}

// GetMetrics returns degradation metrics.
func (gdm *GracefulDegradationManager) GetMetrics() DegradationMetrics {
	gdm.mu.RLock()
	defer gdm.mu.RUnlock()

	// Copy rule activation counts
	ruleCount := make(map[string]uint64)
	for rule, count := range gdm.ruleActivationCount {
		ruleCount[rule] = count
	}

	return DegradationMetrics{
		DegradationCount:     gdm.degradationCount,
		RecoveryCount:        gdm.recoveryCount,
		TotalDegradationTime: gdm.totalDegradationTime,
		AvgDegradationTime:   gdm.avgDegradationTime,
		AvgRecoveryTime:      gdm.avgRecoveryTime,
		CurrentLevel:         gdm.currentLevel,
		CurrentMode:          gdm.currentMode,
		RuleActivationCount:  ruleCount,
		IsActive:             gdm.currentLevel != degradationNone,
		LastDegradationTime:  gdm.degradationStart,
	}
}

// Enable enables graceful degradation.
func (gdm *GracefulDegradationManager) Enable() {
	gdm.mu.Lock()
	defer gdm.mu.Unlock()

	gdm.enabled = true
}

// Disable disables graceful degradation.
func (gdm *GracefulDegradationManager) Disable() {
	gdm.mu.Lock()
	defer gdm.mu.Unlock()

	gdm.enabled = false

	// If currently degraded, recover
	if gdm.currentLevel != degradationNone {
		gdm.recover()
	}
}

// Shutdown shuts down the degradation manager.
func (gdm *GracefulDegradationManager) Shutdown(ctx context.Context) error {
	close(gdm.stopCh)

	// Wait for monitoring to stop
	done := make(chan struct{})
	go func() {
		gdm.stopWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("graceful degradation shutdown cancelled: %w", ctx.Err())
	}
}

// MetricsCollector collects system metrics.
type MetricsCollector struct {
	mu sync.RWMutex
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// GetCurrentMetrics returns current system metrics.
func (mc *MetricsCollector) GetCurrentMetrics() *SystemMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	// In a real implementation, this would collect actual metrics
	// For now, we return mock data
	return &SystemMetrics{
		ErrorRate:         0.05,
		AverageLatency:    500 * time.Millisecond,
		P95Latency:        2 * time.Second,
		P99Latency:        5 * time.Second,
		CPUUsage:          0.6,
		MemoryUsage:       512 * 1024 * 1024, // 512MB
		ActiveConnections: 100,
		ThroughputRPS:     500,
		DependencyHealth: map[string]bool{
			"database": true,
			"cache":    true,
			"api":      true,
		},
		Timestamp: time.Now(),
	}
}
