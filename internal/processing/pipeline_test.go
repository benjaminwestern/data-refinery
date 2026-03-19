package processing

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPipelineBasicFunctionality tests basic pipeline operations
func TestPipelineBasicFunctionality(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 10)

	// Add a simple doubling stage
	pipeline.AddStage("doubler", func(ctx context.Context, input int) (int, error) {
		return input * 2, nil
	}, 2, 5)

	// Add a simple adding stage
	pipeline.AddStage("adder", func(ctx context.Context, input int) (int, error) {
		return input + 1, nil
	}, 2, 5)

	// Start pipeline
	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Test basic processing
	if err := pipeline.Submit(5); err != nil {
		t.Fatalf("Failed to submit item: %v", err)
	}

	select {
	case result := <-pipeline.Results():
		expected := (5 * 2) + 1 // 11
		if result != expected {
			t.Errorf("Expected %d, got %d", expected, result)
		}
	case <-time.After(time.Second):
		t.Fatal("Pipeline processing timed out")
	}
}

// TestPipelineMultipleItems tests processing multiple items
func TestPipelineMultipleItems(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 20)

	// Add processing stages
	pipeline.AddStage("multiply", func(ctx context.Context, input int) (int, error) {
		return input * 3, nil
	}, 4, 10)

	pipeline.AddStage("subtract", func(ctx context.Context, input int) (int, error) {
		return input - 2, nil
	}, 4, 10)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit multiple items
	inputs := []int{1, 2, 3, 4, 5}
	for _, input := range inputs {
		if err := pipeline.Submit(input); err != nil {
			t.Fatalf("Failed to submit item %d: %v", input, err)
		}
	}

	// Collect results
	results := make([]int, 0, len(inputs))
	for i := 0; i < len(inputs); i++ {
		select {
		case result := <-pipeline.Results():
			results = append(results, result)
		case <-time.After(time.Second * 2):
			t.Fatal("Pipeline processing timed out")
		}
	}

	// Verify all items were processed
	if len(results) != len(inputs) {
		t.Errorf("Expected %d results, got %d", len(inputs), len(results))
	}

	// Check that results are correct (order may vary due to concurrency)
	expectedSum := 0
	actualSum := 0

	for _, input := range inputs {
		expectedSum += (input * 3) - 2
	}

	for _, result := range results {
		actualSum += result
	}

	if actualSum != expectedSum {
		t.Errorf("Expected sum %d, got %d", expectedSum, actualSum)
	}
}

// TestPipelineErrorHandling tests error handling in pipeline stages
func TestPipelineErrorHandling(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 10)

	// Add a stage that errors on negative numbers
	pipeline.AddStage("validator", func(ctx context.Context, input int) (int, error) {
		if input < 0 {
			return 0, fmt.Errorf("negative number: %d", input)
		}
		return input, nil
	}, 2, 5)

	// Add a normal processing stage
	pipeline.AddStage("doubler", func(ctx context.Context, input int) (int, error) {
		return input * 2, nil
	}, 2, 5)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit valid and invalid items
	inputs := []int{1, -1, 2, -2, 3}
	for _, input := range inputs {
		if err := pipeline.Submit(input); err != nil {
			t.Fatalf("Failed to submit item %d: %v", input, err)
		}
	}

	// Collect results and errors
	var results []int
	var errors []error

	for i := 0; i < len(inputs); i++ {
		select {
		case result := <-pipeline.Results():
			results = append(results, result)
		case err := <-pipeline.Errors():
			errors = append(errors, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Pipeline processing timed out")
		}
	}

	// Should have 3 results (positive numbers) and 2 errors (negative numbers)
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}

	if len(errors) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(errors))
	}

	// Check results are doubled
	expectedResults := []int{2, 4, 6} // 1*2, 2*2, 3*2
	actualSum := 0
	expectedSum := 0

	for _, result := range results {
		actualSum += result
	}

	for _, expected := range expectedResults {
		expectedSum += expected
	}

	if actualSum != expectedSum {
		t.Errorf("Expected sum %d, got %d", expectedSum, actualSum)
	}
}

// TestPipelineBackpressure tests backpressure functionality
func TestPipelineBackpressure(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 5) // Small buffer to trigger backpressure

	// Add a slow processing stage to create backpressure
	pipeline.AddStage("slow", func(ctx context.Context, input int) (int, error) {
		time.Sleep(time.Millisecond * 50) // Simulate slow processing
		return input, nil
	}, 1, 2) // Single worker, small buffer

	// Add a fast stage
	pipeline.AddStage("fast", func(ctx context.Context, input int) (int, error) {
		return input * 2, nil
	}, 4, 10)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit many items quickly to trigger backpressure
	go func() {
		for i := 0; i < 20; i++ {
			if err := pipeline.Submit(i); err != nil {
				t.Logf("Submit failed (expected with backpressure): %v", err)
			}
		}
	}()

	// Give time for backpressure to activate
	time.Sleep(time.Millisecond * 100)

	// Check if backpressure is active
	isActive, threshold := pipeline.GetBackpressureStatus()
	if !isActive {
		t.Log("Backpressure not active - this is normal if processing is fast enough")
	} else {
		t.Logf("Backpressure active with threshold: %d", threshold)
	}

	// Allow processing to complete
	time.Sleep(time.Second * 2)

	// Collect some results
	resultCount := 0
	for {
		select {
		case <-pipeline.Results():
			resultCount++
		case <-time.After(time.Millisecond * 100):
			// No more results available
			goto done
		}
	}

done:
	if resultCount == 0 {
		t.Fatal("No results received")
	}

	t.Logf("Processed %d items with backpressure", resultCount)
}

// TestPipelineMetrics tests metrics collection
func TestPipelineMetrics(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 10)

	// Add processing stages
	pipeline.AddStage("stage1", func(ctx context.Context, input int) (int, error) {
		time.Sleep(time.Millisecond * 10) // Simulate processing time
		return input * 2, nil
	}, 2, 5)

	pipeline.AddStage("stage2", func(ctx context.Context, input int) (int, error) {
		time.Sleep(time.Millisecond * 5)
		return input + 1, nil
	}, 2, 5)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit items
	itemCount := 10
	for i := 0; i < itemCount; i++ {
		if err := pipeline.Submit(i); err != nil {
			t.Fatalf("Failed to submit item %d: %v", i, err)
		}
	}

	// Process results
	for i := 0; i < itemCount; i++ {
		select {
		case <-pipeline.Results():
		case <-time.After(time.Second * 2):
			t.Fatal("Pipeline processing timed out")
		}
	}

	// Allow metrics to be collected
	time.Sleep(time.Second)

	// Check system metrics
	metrics := pipeline.GetMetrics()
	if metrics.TotalThroughput.Load() < 0 {
		t.Error("Invalid throughput metric")
	}

	if metrics.ActiveWorkers.Load() != 4 { // 2 + 2 workers
		t.Errorf("Expected 4 active workers, got %d", metrics.ActiveWorkers.Load())
	}

	// Check stage-specific metrics
	stage1Metrics := pipeline.GetStageMetrics("stage1")
	if stage1Metrics == nil {
		t.Fatal("Stage1 metrics not found")
	}

	if stage1Metrics.ProcessedItems.Load() != int64(itemCount) {
		t.Errorf("Expected %d processed items in stage1, got %d",
			itemCount, stage1Metrics.ProcessedItems.Load())
	}

	stage2Metrics := pipeline.GetStageMetrics("stage2")
	if stage2Metrics == nil {
		t.Fatal("Stage2 metrics not found")
	}

	if stage2Metrics.ProcessedItems.Load() != int64(itemCount) {
		t.Errorf("Expected %d processed items in stage2, got %d",
			itemCount, stage2Metrics.ProcessedItems.Load())
	}

	t.Logf("System throughput: %d items/sec", metrics.TotalThroughput.Load())
	t.Logf("Stage1 processed: %d items", stage1Metrics.ProcessedItems.Load())
	t.Logf("Stage2 processed: %d items", stage2Metrics.ProcessedItems.Load())
}

// TestPipelineConcurrentSubmission tests concurrent submission to pipeline
func TestPipelineConcurrentSubmission(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 50)

	// Add processing stage
	pipeline.AddStage("processor", func(ctx context.Context, input int) (int, error) {
		return input * input, nil
	}, 8, 20)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit items concurrently
	itemCount := 100
	var wg sync.WaitGroup

	for i := 0; i < itemCount; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			if err := pipeline.Submit(val); err != nil {
				t.Errorf("Failed to submit item %d: %v", val, err)
			}
		}(i)
	}

	wg.Wait()

	// Collect results
	results := make([]int, 0, itemCount)
	for i := 0; i < itemCount; i++ {
		select {
		case result := <-pipeline.Results():
			results = append(results, result)
		case <-time.After(time.Second * 3):
			t.Fatalf("Pipeline processing timed out, got %d of %d results", len(results), itemCount)
		}
	}

	if len(results) != itemCount {
		t.Errorf("Expected %d results, got %d", itemCount, len(results))
	}

	// Verify results are correct squares
	resultMap := make(map[int]bool)
	for _, result := range results {
		resultMap[result] = true
	}

	for i := 0; i < itemCount; i++ {
		expected := i * i
		if !resultMap[expected] {
			t.Errorf("Missing expected result: %d", expected)
		}
	}
}

// TestPipelineStartStop tests pipeline start/stop functionality
func TestPipelineStartStop(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 10)

	// Add stage
	pipeline.AddStage("test", func(ctx context.Context, input int) (int, error) {
		return input, nil
	}, 2, 5)

	// Test initial state
	if pipeline.IsRunning() {
		t.Error("Pipeline should not be running initially")
	}

	// Start pipeline
	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}

	if !pipeline.IsRunning() {
		t.Error("Pipeline should be running after start")
	}

	// Test double start
	if err := pipeline.Start(); err == nil {
		t.Error("Expected error when starting already running pipeline")
	}

	// Stop pipeline
	if err := pipeline.Stop(); err != nil {
		t.Fatalf("Failed to stop pipeline: %v", err)
	}

	if pipeline.IsRunning() {
		t.Error("Pipeline should not be running after stop")
	}

	// Test double stop
	if err := pipeline.Stop(); err == nil {
		t.Error("Expected error when stopping already stopped pipeline")
	}
}

// TestPipelineContextCancellation tests context cancellation
func TestPipelineContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipeline := NewPipeline[int](ctx, 10)

	// Add a stage that takes some time
	pipeline.AddStage("slow", func(ctx context.Context, input int) (int, error) {
		select {
		case <-time.After(time.Second):
			return input, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}, 2, 5)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit an item
	if err := pipeline.Submit(1); err != nil {
		t.Fatalf("Failed to submit item: %v", err)
	}

	// Cancel context after a short time
	go func() {
		time.Sleep(time.Millisecond * 100)
		cancel()
	}()

	// Should receive context cancellation
	select {
	case <-pipeline.Results():
		t.Error("Should not receive result after context cancellation")
	case err := <-pipeline.Errors():
		if err != context.Canceled && err.Error() != "context canceled" {
			t.Logf("Received error: %v (type: %T)", err, err)
			// Context cancellation may come through as wrapped error
			if !strings.Contains(err.Error(), "context canceled") {
				t.Errorf("Expected context cancellation error, got %v", err)
			}
		}
	case <-time.After(time.Second * 2):
		t.Error("Should receive cancellation error within 2 seconds")
	}
}

// BenchmarkPipelineProcessing benchmarks pipeline processing performance
func BenchmarkPipelineProcessing(b *testing.B) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 100)

	// Add multiple stages
	pipeline.AddStage("stage1", func(ctx context.Context, input int) (int, error) {
		return input * 2, nil
	}, 4, 25)

	pipeline.AddStage("stage2", func(ctx context.Context, input int) (int, error) {
		return input + 1, nil
	}, 4, 25)

	pipeline.AddStage("stage3", func(ctx context.Context, input int) (int, error) {
		return input * 3, nil
	}, 4, 25)

	if err := pipeline.Start(); err != nil {
		b.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	b.ResetTimer()

	start := time.Now()

	// Submit items
	for i := 0; i < b.N; i++ {
		if err := pipeline.Submit(i); err != nil {
			b.Fatalf("Failed to submit item %d: %v", i, err)
		}
	}

	// Process results
	for i := 0; i < b.N; i++ {
		select {
		case <-pipeline.Results():
		case <-time.After(time.Second * 10):
			b.Fatalf("Pipeline processing timed out at item %d", i)
		}
	}

	duration := time.Since(start)
	b.Logf("Processed %d items in %v (%.0f items/sec)", b.N, duration, float64(b.N)/duration.Seconds())
}

// TestPipelineBackpressureConfiguration tests backpressure configuration
func TestPipelineBackpressureConfiguration(t *testing.T) {
	ctx := context.Background()
	pipeline := NewPipeline[int](ctx, 10)

	// Set custom backpressure threshold
	pipeline.SetBackpressureThreshold(5)

	pipeline.AddStage("test", func(ctx context.Context, input int) (int, error) {
		time.Sleep(time.Millisecond * 100) // Slow processing
		return input, nil
	}, 1, 2)

	if err := pipeline.Start(); err != nil {
		t.Fatalf("Failed to start pipeline: %v", err)
	}
	defer pipeline.Stop()

	// Submit items to trigger backpressure
	for i := 0; i < 10; i++ {
		go func(val int) {
			pipeline.Submit(val)
		}(i)
	}

	// Allow some processing time
	time.Sleep(time.Millisecond * 200)

	// Check backpressure status
	isActive, threshold := pipeline.GetBackpressureStatus()
	t.Logf("Backpressure active: %v, threshold: %d", isActive, threshold)

	// Clean up
	time.Sleep(time.Second)
}
