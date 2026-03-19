package processing

import (
	"context"
	"testing"
	"time"
)

// TestAdaptiveScaler_BasicFunctionality tests basic adaptive scaling functionality
func TestAdaptiveScaler_BasicFunctionality(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 10
	config.EvaluationInterval = time.Millisecond * 100

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	// Test initial state
	if scaler.GetCurrentWorkers() != 2 {
		t.Errorf("Expected 2 initial workers, got %d", scaler.GetCurrentWorkers())
	}

	if !scaler.IsEnabled() {
		t.Error("Scaler should be enabled by default")
	}

	// Test enable/disable
	scaler.Disable()
	if scaler.IsEnabled() {
		t.Error("Scaler should be disabled after Disable()")
	}

	scaler.Enable()
	if !scaler.IsEnabled() {
		t.Error("Scaler should be enabled after Enable()")
	}
}

// TestAdaptiveScaler_QueueBasedScaling tests queue-based scaling
func TestAdaptiveScaler_QueueBasedScaling(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 8
	config.TargetQueueDepth = 10
	config.ScaleUpThreshold = 0.8   // Scale up if queue > 8
	config.ScaleDownThreshold = 0.3 // Scale down if queue < 3
	config.EvaluationInterval = time.Millisecond * 100
	config.ScaleUpCooldown = time.Millisecond * 200
	config.ScaleDownCooldown = time.Millisecond * 500

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	// Track scaling events
	var scalingEvents []struct {
		oldWorkers int
		newWorkers int
	}

	scaler.AddScalingCallback(func(oldWorkers, newWorkers int) {
		scalingEvents = append(scalingEvents, struct {
			oldWorkers int
			newWorkers int
		}{oldWorkers, newWorkers})
	})

	// Test scale up due to high queue depth
	scaler.UpdateQueueDepth(15) // Higher than threshold (10 * 0.8 = 8)
	time.Sleep(time.Millisecond * 150)

	if scaler.GetCurrentWorkers() <= 2 {
		t.Error("Expected scale up due to high queue depth")
	}

	// Test scale down due to low queue depth
	scaler.UpdateQueueDepth(2)         // Lower than threshold (10 * 0.3 = 3)
	time.Sleep(time.Millisecond * 600) // Wait for cooldown

	if scaler.GetCurrentWorkers() >= 8 {
		t.Error("Expected scale down due to low queue depth")
	}

	// Check that scaling events were recorded
	if len(scalingEvents) == 0 {
		t.Error("Expected scaling events to be recorded")
	}

	t.Logf("Scaling events: %+v", scalingEvents)
}

// TestAdaptiveScaler_CooldownPeriods tests cooldown periods
func TestAdaptiveScaler_CooldownPeriods(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 10
	config.TargetQueueDepth = 10
	config.ScaleUpThreshold = 0.5
	config.ScaleDownThreshold = 0.3
	config.EvaluationInterval = time.Millisecond * 50
	config.ScaleUpCooldown = time.Millisecond * 500
	config.ScaleDownCooldown = time.Millisecond * 1000

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	initialWorkers := scaler.GetCurrentWorkers()

	// Trigger scale up
	scaler.UpdateQueueDepth(8) // Above threshold
	time.Sleep(time.Millisecond * 100)

	workersAfterScaleUp := scaler.GetCurrentWorkers()
	if workersAfterScaleUp <= initialWorkers {
		t.Error("Expected scale up")
	}

	// Immediately try to scale down - should be blocked by cooldown
	scaler.UpdateQueueDepth(2) // Below threshold
	time.Sleep(time.Millisecond * 100)

	workersAfterCooldown := scaler.GetCurrentWorkers()
	if workersAfterCooldown < workersAfterScaleUp {
		t.Error("Scale down should be blocked by cooldown period")
	}

	// Wait for cooldown to expire
	time.Sleep(time.Millisecond * 1100)

	workersAfterCooldownExpiry := scaler.GetCurrentWorkers()
	if workersAfterCooldownExpiry >= workersAfterScaleUp {
		t.Error("Expected scale down after cooldown period")
	}
}

// TestAdaptiveScaler_Metrics tests metrics collection and reporting
func TestAdaptiveScaler_Metrics(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.EvaluationInterval = time.Millisecond * 50

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	// Update metrics
	scaler.UpdateQueueDepth(50)
	scaler.UpdateProcessingRate(100)

	// Allow some time for metrics to be updated
	time.Sleep(time.Millisecond * 100)

	// Get metrics
	metrics := scaler.GetCurrentMetrics()

	// Check that metrics are populated
	if currentWorkers, ok := metrics["currentWorkers"]; !ok || currentWorkers.(int) <= 0 {
		t.Error("Expected valid currentWorkers metric")
	}

	if queueDepth, ok := metrics["queueDepth"]; !ok || queueDepth.(int64) != 50 {
		t.Errorf("Expected queueDepth of 50, got %v", queueDepth)
	}

	if processingRate, ok := metrics["processingRate"]; !ok || processingRate.(int64) != 100 {
		t.Errorf("Expected processingRate of 100, got %v", processingRate)
	}

	if enabled, ok := metrics["enabled"]; !ok || !enabled.(bool) {
		t.Error("Expected enabled metric to be true")
	}

	// Check CPU and memory metrics are present
	if _, ok := metrics["cpuUsage"]; !ok {
		t.Error("Expected cpuUsage metric")
	}

	if _, ok := metrics["memoryUsage"]; !ok {
		t.Error("Expected memoryUsage metric")
	}
}

// TestAdaptiveScaler_ScalingHistory tests scaling history tracking
func TestAdaptiveScaler_ScalingHistory(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 10
	config.TargetQueueDepth = 10
	config.ScaleUpThreshold = 0.5
	config.EvaluationInterval = time.Millisecond * 50
	config.ScaleUpCooldown = time.Millisecond * 100

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	// Initial history should be empty
	history := scaler.GetScalingHistory()
	if len(history) != 0 {
		t.Error("Expected empty scaling history initially")
	}

	// Trigger scaling
	scaler.UpdateQueueDepth(8) // Above threshold
	time.Sleep(time.Millisecond * 100)

	// Check history
	history = scaler.GetScalingHistory()
	if len(history) == 0 {
		t.Error("Expected scaling history to be recorded")
	}

	// Check history entry
	event := history[0]
	if event.OldWorkers >= event.NewWorkers {
		t.Error("Expected scale up event")
	}

	if event.Reason == "" {
		t.Error("Expected scaling reason to be recorded")
	}

	if event.Timestamp.IsZero() {
		t.Error("Expected timestamp to be recorded")
	}

	t.Logf("Scaling event: %+v", event)
}

// TestAdaptiveScaler_ConfigUpdate tests dynamic configuration updates
func TestAdaptiveScaler_ConfigUpdate(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 8

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	// Test initial configuration
	if scaler.GetCurrentWorkers() != 2 {
		t.Errorf("Expected 2 initial workers, got %d", scaler.GetCurrentWorkers())
	}

	// Update configuration
	newConfig := DefaultAdaptiveScalingConfig()
	newConfig.MinWorkers = 4
	newConfig.MaxWorkers = 12

	scaler.SetConfig(newConfig)

	// Check that current workers adjusted to new minimum
	if scaler.GetCurrentWorkers() != 4 {
		t.Errorf("Expected 4 workers after config update, got %d", scaler.GetCurrentWorkers())
	}

	// Test that new maximum is respected
	scaler.UpdateQueueDepth(1000) // Very high queue depth
	time.Sleep(time.Millisecond * 100)

	if scaler.GetCurrentWorkers() > 12 {
		t.Errorf("Expected workers to not exceed new maximum of 12, got %d", scaler.GetCurrentWorkers())
	}
}

// TestAdaptiveScaler_SystemResourceScaling tests CPU and memory-based scaling
func TestAdaptiveScaler_SystemResourceScaling(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 10
	config.TargetQueueDepth = 100
	config.ScaleUpThreshold = 2.0 // Very high threshold so queue won't trigger scaling
	config.CPUThreshold = 0.5     // Low CPU threshold for testing
	config.MemoryThreshold = 0.5  // Low memory threshold for testing
	config.EvaluationInterval = time.Millisecond * 100
	config.ScaleUpCooldown = time.Millisecond * 200
	config.EnableCPUScaling = true
	config.EnableMemoryScaling = true

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	initialWorkers := scaler.GetCurrentWorkers()

	// Set low queue depth to ensure scaling is resource-based
	scaler.UpdateQueueDepth(10)

	// Allow some time for system monitoring to update
	time.Sleep(time.Millisecond * 200)

	// System resource-based scaling may trigger due to system load
	// This is more of a system test that depends on actual system resources
	workersAfterResourceCheck := scaler.GetCurrentWorkers()

	// The exact behavior depends on system load, so we just check that
	// the scaler is functioning and workers are within bounds
	if workersAfterResourceCheck < config.MinWorkers || workersAfterResourceCheck > config.MaxWorkers {
		t.Errorf("Workers %d should be within bounds [%d, %d]",
			workersAfterResourceCheck, config.MinWorkers, config.MaxWorkers)
	}

	t.Logf("Workers after resource check: %d (initial: %d)", workersAfterResourceCheck, initialWorkers)
}

// TestSystemMonitor tests system monitoring functionality
func TestSystemMonitor(t *testing.T) {
	monitor := NewSystemMonitor()

	// Allow some time for monitoring to start
	time.Sleep(time.Millisecond * 100)

	// Check that metrics are being collected
	cpuUsage := monitor.GetCPUUsage()
	if cpuUsage < 0 || cpuUsage > 1 {
		t.Errorf("CPU usage should be between 0 and 1, got %f", cpuUsage)
	}

	memoryUsage := monitor.GetMemoryUsage()
	if memoryUsage < 0 || memoryUsage > 1 {
		t.Errorf("Memory usage should be between 0 and 1, got %f", memoryUsage)
	}

	t.Logf("CPU usage: %.2f%%, Memory usage: %.2f%%", cpuUsage*100, memoryUsage*100)
}

// TestAdaptiveScaler_Integration tests integration with other components
func TestAdaptiveScaler_Integration(t *testing.T) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.MinWorkers = 2
	config.MaxWorkers = 8
	config.TargetQueueDepth = 20
	config.ScaleUpThreshold = 0.6
	config.ScaleDownThreshold = 0.3
	config.EvaluationInterval = time.Millisecond * 100
	config.ScaleUpCooldown = time.Millisecond * 200
	config.ScaleDownCooldown = time.Millisecond * 400

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	// Simulate varying load patterns
	loadPattern := []int{5, 15, 25, 20, 10, 5, 2, 1}

	for i, load := range loadPattern {
		scaler.UpdateQueueDepth(load)
		scaler.UpdateProcessingRate(int64(load * 10))

		time.Sleep(time.Millisecond * 150)

		workers := scaler.GetCurrentWorkers()
		t.Logf("Load %d: queue=%d, workers=%d", i, load, workers)

		// Workers should be within bounds
		if workers < config.MinWorkers || workers > config.MaxWorkers {
			t.Errorf("Workers %d out of bounds [%d, %d] for load %d",
				workers, config.MinWorkers, config.MaxWorkers, load)
		}
	}

	// Check final state
	finalWorkers := scaler.GetCurrentWorkers()
	if finalWorkers > config.MaxWorkers {
		t.Errorf("Expected workers to be within max bounds, got %d", finalWorkers)
	}

	// After low load, workers should eventually scale down (but may take time due to cooldown)
	if finalWorkers < config.MinWorkers {
		t.Errorf("Expected workers to be above minimum bounds, got %d", finalWorkers)
	}

	// Check scaling history
	history := scaler.GetScalingHistory()
	if len(history) == 0 {
		t.Error("Expected scaling events to be recorded during load simulation")
	}

	t.Logf("Final workers: %d, Scaling events: %d", finalWorkers, len(history))
}

// BenchmarkAdaptiveScaler benchmarks adaptive scaler performance
func BenchmarkAdaptiveScaler(b *testing.B) {
	ctx := context.Background()
	config := DefaultAdaptiveScalingConfig()
	config.EvaluationInterval = time.Millisecond * 10

	scaler := NewAdaptiveScaler(ctx, config)
	defer scaler.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		scaler.UpdateQueueDepth(i % 100)
		scaler.UpdateProcessingRate(int64(i % 1000))

		if i%100 == 0 {
			_ = scaler.GetCurrentMetrics()
		}
	}

	b.StopTimer()

	// Report final metrics
	metrics := scaler.GetCurrentMetrics()
	history := scaler.GetScalingHistory()

	b.Logf("Final workers: %v, Scaling events: %d", metrics["currentWorkers"], len(history))
}
