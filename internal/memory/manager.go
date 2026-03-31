// Package memory monitors process memory usage and pressure thresholds.
package memory

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/safety"
)

// Manager provides memory usage monitoring and management.
type Manager struct {
	maxMemoryMB       int64
	warningThreshold  float64
	criticalThreshold float64
	checkInterval     time.Duration

	// Monitoring state
	mu           sync.RWMutex
	currentUsage int64
	peakUsage    int64
	warnings     []Warning
	callbacks    []Callback

	// Control channels
	stopChan chan struct{}
	stopped  bool
}

// Warning represents a memory usage warning.
type Warning struct {
	Timestamp time.Time
	Level     WarningLevel
	Usage     int64
	Message   string
}

// WarningLevel represents the severity of a memory warning.
type WarningLevel int

// WarningLevel values describe the severity of memory pressure events.
const (
	WarningLevelInfo WarningLevel = iota
	WarningLevelWarning
	WarningLevelCritical
)

// Callback is called when memory thresholds are exceeded.
type Callback func(usage int64, level WarningLevel, message string)

// Stats represents current memory statistics.
type Stats struct {
	CurrentUsageMB int64
	PeakUsageMB    int64
	MaxMemoryMB    int64
	UsagePercent   float64
	GCCount        uint32
	LastGCTime     time.Time
	NumGoroutines  int
}

// NewMemoryManager creates a new memory manager.
func NewMemoryManager(maxMemoryMB int64) *Manager {
	return &Manager{
		maxMemoryMB:       maxMemoryMB,
		warningThreshold:  0.75, // 75% of max
		criticalThreshold: 0.90, // 90% of max
		checkInterval:     time.Second * 5,
		warnings:          make([]Warning, 0),
		callbacks:         make([]Callback, 0),
		stopChan:          make(chan struct{}),
	}
}

// Start begins memory monitoring.
func (mm *Manager) Start() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.stopped {
		return
	}

	go mm.monitorMemory()
}

// Stop halts memory monitoring.
func (mm *Manager) Stop() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if !mm.stopped {
		close(mm.stopChan)
		mm.stopped = true
	}
}

// AddCallback adds a memory threshold callback.
func (mm *Manager) AddCallback(callback Callback) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.callbacks = append(mm.callbacks, callback)
}

// GetStats returns current memory statistics.
func (mm *Manager) GetStats() Stats {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	currentMB := safety.SaturatingInt64(memStats.Alloc) / 1024 / 1024

	return Stats{
		CurrentUsageMB: currentMB,
		PeakUsageMB:    mm.peakUsage,
		MaxMemoryMB:    mm.maxMemoryMB,
		UsagePercent:   float64(currentMB) / float64(mm.maxMemoryMB) * 100,
		GCCount:        memStats.NumGC,
		LastGCTime:     time.Unix(0, safety.SaturatingInt64(memStats.LastGC)),
		NumGoroutines:  runtime.NumGoroutine(),
	}
}

// GetWarnings returns recent memory warnings.
func (mm *Manager) GetWarnings() []Warning {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Return a copy to prevent race conditions
	warnings := make([]Warning, len(mm.warnings))
	copy(warnings, mm.warnings)
	return warnings
}

// ForceGC triggers garbage collection.
func (mm *Manager) ForceGC() {
	runtime.GC()
	runtime.GC() // Run twice for better cleanup
}

// CheckMemoryUsage checks current memory usage and triggers warnings if needed.
func (mm *Manager) CheckMemoryUsage() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	currentMB := safety.SaturatingInt64(memStats.Alloc) / 1024 / 1024

	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.currentUsage = currentMB
	if currentMB > mm.peakUsage {
		mm.peakUsage = currentMB
	}

	// Check thresholds
	usagePercent := float64(currentMB) / float64(mm.maxMemoryMB)

	if usagePercent >= mm.criticalThreshold {
		mm.addWarning(WarningLevelCritical, currentMB,
			fmt.Sprintf("Critical memory usage: %d MB (%.1f%%)", currentMB, usagePercent*100))
	} else if usagePercent >= mm.warningThreshold {
		mm.addWarning(WarningLevelWarning, currentMB,
			fmt.Sprintf("High memory usage: %d MB (%.1f%%)", currentMB, usagePercent*100))
	}
}

// monitorMemory runs the memory monitoring loop.
func (mm *Manager) monitorMemory() {
	ticker := time.NewTicker(mm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mm.CheckMemoryUsage()
		case <-mm.stopChan:
			return
		}
	}
}

// addWarning adds a memory warning and triggers callbacks.
func (mm *Manager) addWarning(level WarningLevel, usage int64, message string) {
	warning := Warning{
		Timestamp: time.Now(),
		Level:     level,
		Usage:     usage,
		Message:   message,
	}

	mm.warnings = append(mm.warnings, warning)

	// Keep only the last 100 warnings
	if len(mm.warnings) > 100 {
		mm.warnings = mm.warnings[len(mm.warnings)-100:]
	}

	// Trigger callbacks
	for _, callback := range mm.callbacks {
		go callback(usage, level, message)
	}
}

// ShouldReduceMemory returns true if memory usage is too high.
func (mm *Manager) ShouldReduceMemory() bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	usagePercent := float64(mm.currentUsage) / float64(mm.maxMemoryMB)
	return usagePercent >= mm.warningThreshold
}

// GetMemoryPressure returns a value between 0 and 1 indicating memory pressure.
func (mm *Manager) GetMemoryPressure() float64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	pressure := float64(mm.currentUsage) / float64(mm.maxMemoryMB)
	if pressure > 1.0 {
		return 1.0
	}
	return pressure
}

// SetThresholds updates memory warning thresholds.
func (mm *Manager) SetThresholds(warning, critical float64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.warningThreshold = warning
	mm.criticalThreshold = critical
}

// SetMaxMemory updates the maximum memory limit.
func (mm *Manager) SetMaxMemory(maxMemoryMB int64) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.maxMemoryMB = maxMemoryMB
}

// String returns a string representation of the warning level.
func (level WarningLevel) String() string {
	switch level {
	case WarningLevelInfo:
		return "INFO"
	case WarningLevelWarning:
		return "WARNING"
	case WarningLevelCritical:
		return "CRITICAL"
	default:
		return "UNKNOWN"
	}
}
