package processing

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveScalingConfig defines the configuration for adaptive worker scaling
type AdaptiveScalingConfig struct {
	MinWorkers          int           // Minimum number of workers
	MaxWorkers          int           // Maximum number of workers
	TargetQueueDepth    int           // Target queue depth to maintain
	ScaleUpThreshold    float64       // Queue depth ratio to trigger scale up
	ScaleDownThreshold  float64       // Queue depth ratio to trigger scale down
	ScaleUpCooldown     time.Duration // Cooldown period after scaling up
	ScaleDownCooldown   time.Duration // Cooldown period after scaling down
	CPUThreshold        float64       // CPU utilization threshold
	MemoryThreshold     float64       // Memory utilization threshold
	EvaluationInterval  time.Duration // How often to evaluate scaling
	EnableCPUScaling    bool          // Whether to consider CPU utilization
	EnableMemoryScaling bool          // Whether to consider memory utilization
}

// DefaultAdaptiveScalingConfig returns a default configuration
func DefaultAdaptiveScalingConfig() *AdaptiveScalingConfig {
	return &AdaptiveScalingConfig{
		MinWorkers:          1,
		MaxWorkers:          runtime.NumCPU() * 2,
		TargetQueueDepth:    100,
		ScaleUpThreshold:    0.8, // Scale up if queue is 80% full
		ScaleDownThreshold:  0.3, // Scale down if queue is 30% full
		ScaleUpCooldown:     time.Second * 10,
		ScaleDownCooldown:   time.Second * 30,
		CPUThreshold:        0.7, // 70% CPU utilization
		MemoryThreshold:     0.8, // 80% memory utilization
		EvaluationInterval:  time.Second * 5,
		EnableCPUScaling:    true,
		EnableMemoryScaling: true,
	}
}

// AdaptiveScaler manages adaptive worker scaling
type AdaptiveScaler struct {
	config           *AdaptiveScalingConfig
	currentWorkers   atomic.Int32
	targetWorkers    atomic.Int32
	lastScaleUp      atomic.Int64
	lastScaleDown    atomic.Int64
	cpuUsage         atomic.Int64 // CPU usage percentage * 100
	memoryUsage      atomic.Int64 // Memory usage percentage * 100
	queueDepth       atomic.Int64
	processingRate   atomic.Int64 // Items processed per second
	ctx              context.Context
	cancel           context.CancelFunc
	mutex            sync.RWMutex
	scalingCallbacks []func(oldWorkers, newWorkers int)
	systemMonitor    *SystemMonitor
	scalingHistory   []ScalingEvent
	historyMutex     sync.RWMutex
	enabled          atomic.Bool
}

// ScalingEvent represents a scaling decision event
type ScalingEvent struct {
	Timestamp      time.Time
	OldWorkers     int
	NewWorkers     int
	Reason         string
	QueueDepth     int
	CPUUsage       float64
	MemoryUsage    float64
	ProcessingRate int64
}

// SystemMonitor monitors system resources
type SystemMonitor struct {
	cpuUsage    atomic.Int64
	memoryUsage atomic.Int64
	lastUpdate  atomic.Int64
	mutex       sync.RWMutex
}

// NewAdaptiveScaler creates a new adaptive scaler
func NewAdaptiveScaler(ctx context.Context, config *AdaptiveScalingConfig) *AdaptiveScaler {
	if config == nil {
		config = DefaultAdaptiveScalingConfig()
	}

	scalerCtx, cancel := context.WithCancel(ctx)

	scaler := &AdaptiveScaler{
		config:           config,
		ctx:              scalerCtx,
		cancel:           cancel,
		scalingCallbacks: make([]func(int, int), 0),
		systemMonitor:    NewSystemMonitor(),
		scalingHistory:   make([]ScalingEvent, 0, 100),
	}

	scaler.currentWorkers.Store(int32(config.MinWorkers))
	scaler.targetWorkers.Store(int32(config.MinWorkers))
	scaler.enabled.Store(true)

	// Start monitoring
	go scaler.monitorAndScale()

	return scaler
}

// NewSystemMonitor creates a new system monitor
func NewSystemMonitor() *SystemMonitor {
	monitor := &SystemMonitor{}

	// Start system monitoring
	go monitor.monitorSystem()

	return monitor
}

// monitorSystem continuously monitors system resources
func (sm *SystemMonitor) monitorSystem() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		sm.updateSystemMetrics()
	}
}

// updateSystemMetrics updates CPU and memory usage metrics
func (sm *SystemMonitor) updateSystemMetrics() {
	// Get memory statistics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate memory usage percentage (simplified)
	memoryUsage := float64(memStats.Alloc) / float64(memStats.Sys) * 100
	if memoryUsage > 100 {
		memoryUsage = 100
	}

	// Get CPU usage (simplified - in a real implementation, you'd use OS-specific APIs)
	cpuUsage := sm.estimateCPUUsage()

	sm.cpuUsage.Store(int64(cpuUsage * 100))
	sm.memoryUsage.Store(int64(memoryUsage * 100))
	sm.lastUpdate.Store(time.Now().UnixNano())
}

// estimateCPUUsage provides a simple CPU usage estimation
func (sm *SystemMonitor) estimateCPUUsage() float64 {
	// This is a simplified CPU estimation - in production, use proper OS APIs
	numGoroutines := runtime.NumGoroutine()
	numCPU := runtime.NumCPU()

	// Rough estimation based on goroutine count and CPU cores
	usage := float64(numGoroutines) / float64(numCPU*10) // Assuming 10 goroutines per CPU is ~100%
	if usage > 1.0 {
		usage = 1.0
	}

	return usage
}

// GetCPUUsage returns current CPU usage percentage
func (sm *SystemMonitor) GetCPUUsage() float64 {
	return float64(sm.cpuUsage.Load()) / 100.0
}

// GetMemoryUsage returns current memory usage percentage
func (sm *SystemMonitor) GetMemoryUsage() float64 {
	return float64(sm.memoryUsage.Load()) / 100.0
}

// AddScalingCallback adds a callback to be called when scaling occurs
func (as *AdaptiveScaler) AddScalingCallback(callback func(oldWorkers, newWorkers int)) {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.scalingCallbacks = append(as.scalingCallbacks, callback)
}

// UpdateQueueDepth updates the current queue depth
func (as *AdaptiveScaler) UpdateQueueDepth(depth int) {
	as.queueDepth.Store(int64(depth))
}

// UpdateProcessingRate updates the processing rate
func (as *AdaptiveScaler) UpdateProcessingRate(rate int64) {
	as.processingRate.Store(rate)
}

// GetCurrentWorkers returns the current number of workers
func (as *AdaptiveScaler) GetCurrentWorkers() int {
	return int(as.currentWorkers.Load())
}

// GetTargetWorkers returns the target number of workers
func (as *AdaptiveScaler) GetTargetWorkers() int {
	return int(as.targetWorkers.Load())
}

// Enable enables adaptive scaling
func (as *AdaptiveScaler) Enable() {
	as.enabled.Store(true)
}

// Disable disables adaptive scaling
func (as *AdaptiveScaler) Disable() {
	as.enabled.Store(false)
}

// IsEnabled returns whether adaptive scaling is enabled
func (as *AdaptiveScaler) IsEnabled() bool {
	return as.enabled.Load()
}

// monitorAndScale continuously monitors and scales workers
func (as *AdaptiveScaler) monitorAndScale() {
	ticker := time.NewTicker(as.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-as.ctx.Done():
			return
		case <-ticker.C:
			if as.enabled.Load() {
				as.evaluateScaling()
			}
		}
	}
}

// evaluateScaling evaluates whether scaling is needed
func (as *AdaptiveScaler) evaluateScaling() {
	currentWorkers := as.GetCurrentWorkers()
	queueDepth := int(as.queueDepth.Load())

	// Calculate queue depth ratio
	queueRatio := float64(queueDepth) / float64(as.config.TargetQueueDepth)

	// Get system metrics
	cpuUsage := as.systemMonitor.GetCPUUsage()
	memoryUsage := as.systemMonitor.GetMemoryUsage()

	// Update internal metrics
	as.cpuUsage.Store(int64(cpuUsage * 100))
	as.memoryUsage.Store(int64(memoryUsage * 100))

	// Determine if scaling is needed
	decision := as.makeScalingDecision(currentWorkers, queueRatio, cpuUsage, memoryUsage)

	if decision != nil {
		as.executeScaling(decision)
	}
}

// ScalingDecision represents a scaling decision
type ScalingDecision struct {
	NewWorkers int
	Reason     string
	Urgency    int // 1-5, with 5 being most urgent
}

// makeScalingDecision determines if scaling is needed
func (as *AdaptiveScaler) makeScalingDecision(currentWorkers int, queueRatio, cpuUsage, memoryUsage float64) *ScalingDecision {
	now := time.Now().UnixNano()

	// Check cooldown periods
	if as.isInCooldown(now) {
		return nil
	}

	// Scale up conditions
	if as.shouldScaleUp(currentWorkers, queueRatio, cpuUsage, memoryUsage) {
		newWorkers := as.calculateScaleUp(currentWorkers, queueRatio, cpuUsage, memoryUsage)
		if newWorkers > currentWorkers {
			return &ScalingDecision{
				NewWorkers: newWorkers,
				Reason:     as.getScaleUpReason(queueRatio, cpuUsage, memoryUsage),
				Urgency:    as.getScaleUpUrgency(queueRatio, cpuUsage, memoryUsage),
			}
		}
	}

	// Scale down conditions
	if as.shouldScaleDown(currentWorkers, queueRatio, cpuUsage, memoryUsage) {
		newWorkers := as.calculateScaleDown(currentWorkers, queueRatio, cpuUsage, memoryUsage)
		if newWorkers < currentWorkers {
			return &ScalingDecision{
				NewWorkers: newWorkers,
				Reason:     as.getScaleDownReason(queueRatio, cpuUsage, memoryUsage),
				Urgency:    as.getScaleDownUrgency(queueRatio, cpuUsage, memoryUsage),
			}
		}
	}

	return nil
}

// isInCooldown checks if we're in a cooldown period
func (as *AdaptiveScaler) isInCooldown(now int64) bool {
	lastScaleUp := as.lastScaleUp.Load()
	lastScaleDown := as.lastScaleDown.Load()

	if lastScaleUp > 0 && time.Duration(now-lastScaleUp) < as.config.ScaleUpCooldown {
		return true
	}

	if lastScaleDown > 0 && time.Duration(now-lastScaleDown) < as.config.ScaleDownCooldown {
		return true
	}

	return false
}

// shouldScaleUp determines if we should scale up
func (as *AdaptiveScaler) shouldScaleUp(currentWorkers int, queueRatio, cpuUsage, memoryUsage float64) bool {
	if currentWorkers >= as.config.MaxWorkers {
		return false
	}

	// Queue-based scaling
	if queueRatio >= as.config.ScaleUpThreshold {
		return true
	}

	// CPU-based scaling
	if as.config.EnableCPUScaling && cpuUsage >= as.config.CPUThreshold {
		return true
	}

	// Memory-based scaling (if memory usage is high, we might need more workers to process faster)
	if as.config.EnableMemoryScaling && memoryUsage >= as.config.MemoryThreshold {
		return true
	}

	return false
}

// shouldScaleDown determines if we should scale down
func (as *AdaptiveScaler) shouldScaleDown(currentWorkers int, queueRatio, cpuUsage, memoryUsage float64) bool {
	if currentWorkers <= as.config.MinWorkers {
		return false
	}

	// Only scale down if all conditions are met
	return queueRatio <= as.config.ScaleDownThreshold &&
		(!as.config.EnableCPUScaling || cpuUsage < as.config.CPUThreshold*0.5) &&
		(!as.config.EnableMemoryScaling || memoryUsage < as.config.MemoryThreshold*0.5)
}

// calculateScaleUp calculates the number of workers to scale up to
func (as *AdaptiveScaler) calculateScaleUp(currentWorkers int, queueRatio, cpuUsage, memoryUsage float64) int {
	// Calculate based on queue pressure
	queueFactor := int(queueRatio) + 1

	// Calculate based on CPU usage
	cpuFactor := 1
	if as.config.EnableCPUScaling && cpuUsage > as.config.CPUThreshold {
		cpuFactor = int(cpuUsage/as.config.CPUThreshold) + 1
	}

	// Use the maximum factor but cap it
	scaleFactor := queueFactor
	if cpuFactor > scaleFactor {
		scaleFactor = cpuFactor
	}

	newWorkers := currentWorkers + scaleFactor
	if newWorkers > as.config.MaxWorkers {
		newWorkers = as.config.MaxWorkers
	}

	return newWorkers
}

// calculateScaleDown calculates the number of workers to scale down to
func (as *AdaptiveScaler) calculateScaleDown(currentWorkers int, queueRatio, cpuUsage, memoryUsage float64) int {
	// Scale down gradually
	reduction := 1

	// If queue is very low, scale down more aggressively
	if queueRatio < as.config.ScaleDownThreshold*0.5 {
		reduction = currentWorkers / 4 // Reduce by 25%
		if reduction < 1 {
			reduction = 1
		}
	}

	newWorkers := currentWorkers - reduction
	if newWorkers < as.config.MinWorkers {
		newWorkers = as.config.MinWorkers
	}

	return newWorkers
}

// getScaleUpReason returns the reason for scaling up
func (as *AdaptiveScaler) getScaleUpReason(queueRatio, cpuUsage, memoryUsage float64) string {
	if queueRatio >= as.config.ScaleUpThreshold {
		return "High queue depth"
	}
	if cpuUsage >= as.config.CPUThreshold {
		return "High CPU usage"
	}
	if memoryUsage >= as.config.MemoryThreshold {
		return "High memory usage"
	}
	return "General system pressure"
}

// getScaleDownReason returns the reason for scaling down
func (as *AdaptiveScaler) getScaleDownReason(queueRatio, cpuUsage, memoryUsage float64) string {
	return "Low system utilization"
}

// getScaleUpUrgency returns the urgency level for scaling up
func (as *AdaptiveScaler) getScaleUpUrgency(queueRatio, cpuUsage, memoryUsage float64) int {
	if queueRatio >= 2.0 || cpuUsage >= 0.9 || memoryUsage >= 0.9 {
		return 5 // Critical
	}
	if queueRatio >= 1.5 || cpuUsage >= 0.8 || memoryUsage >= 0.8 {
		return 4 // High
	}
	if queueRatio >= 1.0 || cpuUsage >= 0.7 || memoryUsage >= 0.7 {
		return 3 // Medium
	}
	return 2 // Low
}

// getScaleDownUrgency returns the urgency level for scaling down
func (as *AdaptiveScaler) getScaleDownUrgency(queueRatio, cpuUsage, memoryUsage float64) int {
	if queueRatio <= 0.1 && cpuUsage <= 0.2 && memoryUsage <= 0.3 {
		return 3 // Medium urgency to scale down
	}
	return 1 // Low urgency
}

// executeScaling executes the scaling decision
func (as *AdaptiveScaler) executeScaling(decision *ScalingDecision) {
	oldWorkers := as.GetCurrentWorkers()
	newWorkers := decision.NewWorkers

	// Update worker count
	as.currentWorkers.Store(int32(newWorkers))
	as.targetWorkers.Store(int32(newWorkers))

	// Update cooldown timestamp
	now := time.Now().UnixNano()
	if newWorkers > oldWorkers {
		as.lastScaleUp.Store(now)
	} else {
		as.lastScaleDown.Store(now)
	}

	// Record scaling event
	as.recordScalingEvent(oldWorkers, newWorkers, decision.Reason)

	// Execute callbacks
	as.mutex.RLock()
	callbacks := make([]func(int, int), len(as.scalingCallbacks))
	copy(callbacks, as.scalingCallbacks)
	as.mutex.RUnlock()

	for _, callback := range callbacks {
		go callback(oldWorkers, newWorkers)
	}
}

// recordScalingEvent records a scaling event in history
func (as *AdaptiveScaler) recordScalingEvent(oldWorkers, newWorkers int, reason string) {
	as.historyMutex.Lock()
	defer as.historyMutex.Unlock()

	event := ScalingEvent{
		Timestamp:      time.Now(),
		OldWorkers:     oldWorkers,
		NewWorkers:     newWorkers,
		Reason:         reason,
		QueueDepth:     int(as.queueDepth.Load()),
		CPUUsage:       float64(as.cpuUsage.Load()) / 100.0,
		MemoryUsage:    float64(as.memoryUsage.Load()) / 100.0,
		ProcessingRate: as.processingRate.Load(),
	}

	as.scalingHistory = append(as.scalingHistory, event)

	// Keep only last 100 events
	if len(as.scalingHistory) > 100 {
		as.scalingHistory = as.scalingHistory[1:]
	}
}

// GetScalingHistory returns the scaling history
func (as *AdaptiveScaler) GetScalingHistory() []ScalingEvent {
	as.historyMutex.RLock()
	defer as.historyMutex.RUnlock()

	history := make([]ScalingEvent, len(as.scalingHistory))
	copy(history, as.scalingHistory)

	return history
}

// GetCurrentMetrics returns current system metrics
func (as *AdaptiveScaler) GetCurrentMetrics() map[string]interface{} {
	return map[string]interface{}{
		"currentWorkers": as.GetCurrentWorkers(),
		"targetWorkers":  as.GetTargetWorkers(),
		"queueDepth":     as.queueDepth.Load(),
		"cpuUsage":       float64(as.cpuUsage.Load()) / 100.0,
		"memoryUsage":    float64(as.memoryUsage.Load()) / 100.0,
		"processingRate": as.processingRate.Load(),
		"enabled":        as.IsEnabled(),
		"lastScaleUp":    as.lastScaleUp.Load(),
		"lastScaleDown":  as.lastScaleDown.Load(),
	}
}

// Stop stops the adaptive scaler
func (as *AdaptiveScaler) Stop() {
	as.cancel()
}

// SetConfig updates the scaling configuration
func (as *AdaptiveScaler) SetConfig(config *AdaptiveScalingConfig) {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.config = config

	// Ensure current workers are within new bounds
	current := as.GetCurrentWorkers()
	if current < config.MinWorkers {
		as.currentWorkers.Store(int32(config.MinWorkers))
		as.targetWorkers.Store(int32(config.MinWorkers))
	} else if current > config.MaxWorkers {
		as.currentWorkers.Store(int32(config.MaxWorkers))
		as.targetWorkers.Store(int32(config.MaxWorkers))
	}
}
