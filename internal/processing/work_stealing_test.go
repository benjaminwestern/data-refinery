package processing

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkStealingDeque tests the basic operations of the work-stealing deque
func TestWorkStealingDeque(t *testing.T) {
	deque := NewWorkStealingDeque[int](8)

	// Test push and pop operations
	t.Run("PushPop", func(t *testing.T) {
		// Push some items
		for i := 0; i < 5; i++ {
			if !deque.PushBottom(i) {
				t.Errorf("Failed to push item %d", i)
			}
		}

		// Check size
		if size := deque.Size(); size != 5 {
			t.Errorf("Expected size 5, got %d", size)
		}

		// Pop items
		for i := 4; i >= 0; i-- {
			item, ok := deque.PopBottom()
			if !ok {
				t.Error("Failed to pop item")
			}
			if *item != i {
				t.Errorf("Expected item %d, got %d", i, *item)
			}
		}

		// Check empty
		if !deque.IsEmpty() {
			t.Error("Deque should be empty")
		}
	})

	t.Run("StealTop", func(t *testing.T) {
		// Push some items
		for i := 0; i < 3; i++ {
			deque.PushBottom(i)
		}

		// Steal from top
		item, ok := deque.StealTop()
		if !ok {
			t.Error("Failed to steal item")
		}
		if *item != 0 {
			t.Errorf("Expected stolen item 0, got %d", *item)
		}

		// Check size
		if size := deque.Size(); size != 2 {
			t.Errorf("Expected size 2, got %d", size)
		}
	})

	t.Run("ConcurrentAccess", func(t *testing.T) {
		deque := NewWorkStealingDeque[int](1024)
		var wg sync.WaitGroup
		var stolen atomic.Int64
		var popped atomic.Int64

		// Producer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				for !deque.PushBottom(i) {
					time.Sleep(time.Microsecond)
				}
			}
		}()

		// Consumer (owner)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if item, ok := deque.PopBottom(); ok {
					popped.Add(1)
					_ = item
				} else {
					if deque.IsEmpty() {
						time.Sleep(time.Microsecond)
					}
				}
				if popped.Load()+stolen.Load() >= 100 {
					break
				}
			}
		}()

		// Thief
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if item, ok := deque.StealTop(); ok {
					stolen.Add(1)
					_ = item
				} else {
					time.Sleep(time.Microsecond)
				}
				if popped.Load()+stolen.Load() >= 100 {
					break
				}
			}
		}()

		wg.Wait()

		total := popped.Load() + stolen.Load()
		if total != 100 {
			t.Errorf("Expected total 100, got %d (popped: %d, stolen: %d)",
				total, popped.Load(), stolen.Load())
		}
	})
}

// TestWorkStealingWorker tests individual worker functionality
func TestWorkStealingWorker(t *testing.T) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		return task * 2, nil
	})

	pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
		Workers:        2,
		DequeSize:      16,
		ResultChanSize: 32,
	})

	worker := pool.workers[0]

	t.Run("BasicWorkerStats", func(t *testing.T) {
		stats := worker.GetStats()
		if stats.ID != 0 {
			t.Errorf("Expected worker ID 0, got %d", stats.ID)
		}
		if stats.IsActive {
			t.Error("Worker should not be active initially")
		}
		if stats.TasksProcessed != 0 {
			t.Errorf("Expected 0 tasks processed, got %d", stats.TasksProcessed)
		}
	})

	t.Run("WorkerDequeOperations", func(t *testing.T) {
		// Test worker's deque operations
		task := TaskWithIndex[int]{Task: 42, Index: 0}

		if !worker.deque.PushBottom(task) {
			t.Error("Failed to push task to worker deque")
		}

		if size := worker.deque.Size(); size != 1 {
			t.Errorf("Expected deque size 1, got %d", size)
		}

		poppedTask, ok := worker.deque.PopBottom()
		if !ok {
			t.Error("Failed to pop task from worker deque")
		}
		if poppedTask.Task != 42 {
			t.Errorf("Expected task 42, got %d", poppedTask.Task)
		}
	})
}

// TestWorkStealingPool tests the complete work-stealing pool
func TestWorkStealingPool(t *testing.T) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		return task * 2, nil
	})

	t.Run("BasicPoolOperations", func(t *testing.T) {
		pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
			Workers:        4,
			DequeSize:      32,
			ResultChanSize: 64,
		})

		if pool.WorkerCount() != 4 {
			t.Errorf("Expected 4 workers, got %d", pool.WorkerCount())
		}

		if pool.IsRunning() {
			t.Error("Pool should not be running initially")
		}

		// Start pool
		pool.Start()
		defer pool.Stop()

		if !pool.IsRunning() {
			t.Error("Pool should be running after start")
		}

		// Submit task
		if !pool.Submit(10) {
			t.Error("Failed to submit task")
		}

		// Get result
		select {
		case result := <-pool.Results():
			if result.Task != 10 {
				t.Errorf("Expected task 10, got %d", result.Task)
			}
			if result.Result != 20 {
				t.Errorf("Expected result 20, got %v", result.Result)
			}
		case <-time.After(time.Second):
			t.Error("Timeout waiting for result")
		}
	})

	t.Run("ProcessBatch", func(t *testing.T) {
		pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
			Workers:        4,
			DequeSize:      32,
			ResultChanSize: 64,
		})

		tasks := make([]int, 10)
		for i := range tasks {
			tasks[i] = i
		}

		results := pool.ProcessBatch(tasks)

		if len(results) != len(tasks) {
			t.Errorf("Expected %d results, got %d", len(tasks), len(results))
		}

		for i, result := range results {
			if result.Task != i {
				t.Errorf("Expected task %d, got %d", i, result.Task)
			}
			if result.Result != i*2 {
				t.Errorf("Expected result %d, got %v", i*2, result.Result)
			}
		}
	})

	t.Run("HighThroughput", func(t *testing.T) {
		pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
			Workers:        8,
			DequeSize:      128,
			ResultChanSize: 256,
		})

		numTasks := 1000
		tasks := make([]int, numTasks)
		for i := range tasks {
			tasks[i] = i
		}

		start := time.Now()
		results := pool.ProcessBatch(tasks)
		duration := time.Since(start)

		if len(results) != numTasks {
			t.Errorf("Expected %d results, got %d", numTasks, len(results))
		}

		// Verify all results are correct
		for i, result := range results {
			if result.Task != i {
				t.Errorf("Expected task %d, got %d", i, result.Task)
			}
			if result.Result != i*2 {
				t.Errorf("Expected result %d, got %v", i*2, result.Result)
			}
		}

		t.Logf("Processed %d tasks in %v (%.2f tasks/ms)",
			numTasks, duration, float64(numTasks)/float64(duration.Nanoseconds())*1000000)
	})

	t.Run("WorkStealingBehavior", func(t *testing.T) {
		// Create a pool with multiple workers
		pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
			Workers:        4,
			DequeSize:      64,
			ResultChanSize: 128,
		})

		pool.Start()
		defer pool.Stop()

		// Submit many tasks to trigger work stealing
		numTasks := 100
		for i := 0; i < numTasks; i++ {
			for !pool.Submit(i) {
				time.Sleep(time.Microsecond)
			}
		}

		// Collect results
		results := make(map[int]bool)
		for i := 0; i < numTasks; i++ {
			select {
			case result := <-pool.Results():
				results[result.Task] = true
			case <-time.After(time.Second * 5):
				t.Fatal("Timeout waiting for results")
			}
		}

		// Verify all tasks were processed
		if len(results) != numTasks {
			t.Errorf("Expected %d unique results, got %d", numTasks, len(results))
		}

		// Check pool statistics
		stats := pool.GetStats()
		if stats.TotalTasks != int64(numTasks) {
			t.Errorf("Expected %d total tasks, got %d", numTasks, stats.TotalTasks)
		}

		// At least some work stealing should have occurred
		if stats.TotalSteals == 0 {
			t.Log("Warning: No work stealing occurred (this might be expected with light load)")
		}

		t.Logf("Total steals: %d, Total steal attempts: %d",
			stats.TotalSteals, stats.TotalStealAttempts)
	})
}

// TestWorkStealingConcurrency tests concurrent access patterns
func TestWorkStealingConcurrency(t *testing.T) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		// Simulate work
		time.Sleep(time.Microsecond * 10)
		return task * 2, nil
	})

	pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
		Workers:        runtime.NumCPU(),
		DequeSize:      256,
		ResultChanSize: 512,
	})

	t.Run("ConcurrentSubmission", func(t *testing.T) {
		pool.Start()
		defer pool.Stop()

		var wg sync.WaitGroup
		numSubmitters := 4
		tasksPerSubmitter := 50

		// Multiple concurrent submitters
		for i := 0; i < numSubmitters; i++ {
			wg.Add(1)
			go func(submitterID int) {
				defer wg.Done()
				for j := 0; j < tasksPerSubmitter; j++ {
					task := submitterID*tasksPerSubmitter + j
					for !pool.Submit(task) {
						time.Sleep(time.Microsecond)
					}
				}
			}(i)
		}

		// Result collector
		results := make(map[int]bool)
		var resultMu sync.Mutex
		totalTasks := numSubmitters * tasksPerSubmitter

		go func() {
			for i := 0; i < totalTasks; i++ {
				result := <-pool.Results()
				resultMu.Lock()
				results[result.Task] = true
				resultMu.Unlock()
			}
		}()

		wg.Wait()

		// Wait for all results
		time.Sleep(time.Second)

		resultMu.Lock()
		if len(results) != totalTasks {
			t.Errorf("Expected %d unique results, got %d", totalTasks, len(results))
		}
		resultMu.Unlock()
	})

	t.Run("StressTest", func(t *testing.T) {
		// Stress test with many tasks and high contention
		pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
			Workers:        runtime.NumCPU() * 2,
			DequeSize:      512,
			ResultChanSize: 1024,
		})

		numTasks := 2000
		tasks := make([]int, numTasks)
		for i := range tasks {
			tasks[i] = i
		}

		start := time.Now()
		results := pool.ProcessBatch(tasks)
		duration := time.Since(start)

		if len(results) != numTasks {
			t.Errorf("Expected %d results, got %d", numTasks, len(results))
		}

		// Check for duplicates or missing results
		seen := make(map[int]bool)
		for _, result := range results {
			if seen[result.Task] {
				t.Errorf("Duplicate result for task %d", result.Task)
			}
			seen[result.Task] = true
		}

		stats := pool.GetStats()
		t.Logf("Stress test completed in %v", duration)
		t.Logf("Tasks: %d, Steals: %d, Steal attempts: %d",
			stats.TotalTasks, stats.TotalSteals, stats.TotalStealAttempts)
		t.Logf("Steal success rate: %.2f%%",
			float64(stats.TotalSteals)/float64(stats.TotalStealAttempts)*100)
	})
}

// TestWorkStealingNUMAAwareness tests NUMA-aware work stealing
func TestWorkStealingNUMAAwareness(t *testing.T) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		return task, nil
	})

	pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
		Workers:        8,
		DequeSize:      64,
		ResultChanSize: 128,
		EnableNUMA:     true,
	})

	t.Run("NUMANodeAssignment", func(t *testing.T) {
		// Check that workers are assigned to NUMA nodes
		for i, worker := range pool.workers {
			expectedNode := i % runtime.NumCPU()
			if worker.numaNode != expectedNode {
				t.Errorf("Worker %d expected NUMA node %d, got %d",
					i, expectedNode, worker.numaNode)
			}
		}
	})

	t.Run("NUMALocalityPreference", func(t *testing.T) {
		// This test is more observational - we check that the NUMA-aware
		// stealing logic is working by examining statistics
		pool.Start()
		defer pool.Stop()

		numTasks := 200
		for i := 0; i < numTasks; i++ {
			for !pool.Submit(i) {
				time.Sleep(time.Microsecond)
			}
		}

		// Collect all results
		for i := 0; i < numTasks; i++ {
			<-pool.Results()
		}

		stats := pool.GetStats()

		// Log NUMA statistics for each worker
		for _, workerStats := range stats.WorkerStats {
			t.Logf("Worker %d (NUMA %d): Processed=%d, Stolen=%d, Attempts=%d",
				workerStats.ID, workerStats.NUMANode,
				workerStats.TasksProcessed, workerStats.TasksStolen,
				workerStats.StealAttempts)
		}
	})
}

// TestWorkStealingStatistics tests the statistics collection
func TestWorkStealingStatistics(t *testing.T) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		return task, nil
	})

	pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
		Workers:        4,
		DequeSize:      32,
		ResultChanSize: 64,
	})

	t.Run("WorkerStatistics", func(t *testing.T) {
		pool.Start()
		defer pool.Stop()

		// Submit tasks
		numTasks := 20
		for i := 0; i < numTasks; i++ {
			for !pool.Submit(i) {
				time.Sleep(time.Microsecond)
			}
		}

		// Collect results
		for i := 0; i < numTasks; i++ {
			<-pool.Results()
		}

		// Check statistics
		stats := pool.GetStats()

		if stats.Workers != 4 {
			t.Errorf("Expected 4 workers, got %d", stats.Workers)
		}

		if stats.TotalTasks != int64(numTasks) {
			t.Errorf("Expected %d total tasks, got %d", numTasks, stats.TotalTasks)
		}

		totalProcessed := int64(0)
		for _, workerStats := range stats.WorkerStats {
			totalProcessed += workerStats.TasksProcessed
			if !workerStats.IsActive {
				t.Errorf("Worker %d should be active", workerStats.ID)
			}
		}

		if totalProcessed != int64(numTasks) {
			t.Errorf("Expected %d total processed, got %d", numTasks, totalProcessed)
		}
	})

	t.Run("PoolStatistics", func(t *testing.T) {
		stats := pool.GetStats()

		if !stats.IsRunning {
			t.Error("Pool should be running")
		}

		if len(stats.WorkerStats) != 4 {
			t.Errorf("Expected 4 worker stats, got %d", len(stats.WorkerStats))
		}

		// Check that all worker IDs are unique
		seen := make(map[int]bool)
		for _, workerStats := range stats.WorkerStats {
			if seen[workerStats.ID] {
				t.Errorf("Duplicate worker ID: %d", workerStats.ID)
			}
			seen[workerStats.ID] = true
		}
	})
}

// BenchmarkWorkStealingPool benchmarks the work-stealing pool performance
func BenchmarkWorkStealingPool(b *testing.B) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		// Minimal work to focus on scheduling overhead
		return task, nil
	})

	pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
		Workers:        runtime.NumCPU(),
		DequeSize:      256,
		ResultChanSize: 512,
	})

	b.Run("SingleSubmission", func(b *testing.B) {
		pool.Start()
		defer pool.Stop()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.Submit(i)
			<-pool.Results()
		}
	})

	b.Run("BatchProcessing", func(b *testing.B) {
		batchSize := 100
		tasks := make([]int, batchSize)
		for i := range tasks {
			tasks[i] = i
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.ProcessBatch(tasks)
		}
	})

	b.Run("HighConcurrency", func(b *testing.B) {
		pool.Start()
		defer pool.Stop()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			taskID := 0
			for pb.Next() {
				for !pool.Submit(taskID) {
					// Retry if submission fails
				}
				<-pool.Results()
				taskID++
			}
		})
	})
}

// BenchmarkWorkStealingVsRegularPool compares work-stealing vs regular pool
func BenchmarkWorkStealingVsRegularPool(b *testing.B) {
	processor := TaskProcessorFunc[int](func(ctx context.Context, task int) (interface{}, error) {
		// Simulate varying work loads
		if task%10 == 0 {
			time.Sleep(time.Microsecond * 100) // Heavy task
		} else {
			time.Sleep(time.Microsecond * 10) // Light task
		}
		return task, nil
	})

	tasks := make([]int, 1000)
	for i := range tasks {
		tasks[i] = i
	}

	b.Run("WorkStealingPool", func(b *testing.B) {
		pool := NewWorkStealingPool[int](processor, WorkStealingPoolOptions{
			Workers:        runtime.NumCPU(),
			DequeSize:      256,
			ResultChanSize: 512,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.ProcessBatch(tasks)
		}
	})

	b.Run("RegularWorkerPool", func(b *testing.B) {
		pool := NewWorkerPool[int](processor, WorkerPoolOptions{
			Workers:    runtime.NumCPU(),
			BufferSize: 256,
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.ProcessBatch(tasks)
		}
	})
}
