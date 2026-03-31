package processing

import (
	"context"
	"sync"
	"testing"
	"time"
)

type testTask struct {
	id    int
	sleep time.Duration
}

type testProcessor struct {
	processedTasks []testTask
	mu             sync.Mutex
}

func (p *testProcessor) Process(ctx context.Context, task testTask) (interface{}, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if task.sleep > 0 {
		time.Sleep(task.sleep)
	}

	p.processedTasks = append(p.processedTasks, task)
	return task.id * 2, nil
}

func (p *testProcessor) getProcessedTasks() []testTask {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]testTask(nil), p.processedTasks...)
}

func TestNewWorkerPool(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{
		Workers:    2,
		BufferSize: 4,
	}

	pool := NewWorkerPool(processor, opts)

	if pool == nil {
		t.Fatal("Expected non-nil pool")
	}

	if pool.WorkerCount() != 2 {
		t.Errorf("Expected 2 workers, got %d", pool.WorkerCount())
	}

	if pool.IsRunning() {
		t.Error("Expected pool to not be running initially")
	}
}

func TestWorkerPool_StartStop(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{Workers: 2}
	pool := NewWorkerPool(processor, opts)

	// Start the pool
	pool.Start()
	if !pool.IsRunning() {
		t.Error("Expected pool to be running after Start()")
	}

	// Stop the pool
	pool.Stop()
	if pool.IsRunning() {
		t.Error("Expected pool to not be running after Stop()")
	}

	// Multiple stops should be safe
	pool.Stop()
}

func TestWorkerPool_ProcessBatch(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{Workers: 2}
	pool := NewWorkerPool(processor, opts)

	tasks := []testTask{
		{id: 1},
		{id: 2},
		{id: 3},
		{id: 4},
	}

	results := pool.ProcessBatch(tasks)

	if len(results) != len(tasks) {
		t.Errorf("Expected %d results, got %d", len(tasks), len(results))
	}

	// Check that all tasks were processed
	for i, result := range results {
		if result.Error != nil {
			t.Errorf("Task %d had error: %v", i, result.Error)
		}

		expected := tasks[i].id * 2
		if result.Result != expected {
			t.Errorf("Task %d: expected result %d, got %v", i, expected, result.Result)
		}
	}

	// Check that processor received all tasks
	processedTasks := processor.getProcessedTasks()
	if len(processedTasks) != len(tasks) {
		t.Errorf("Expected %d processed tasks, got %d", len(tasks), len(processedTasks))
	}
}

func TestWorkerPool_Submit(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{Workers: 2}
	pool := NewWorkerPool(processor, opts)

	pool.Start()
	defer pool.Stop()

	task := testTask{id: 42}

	if !pool.Submit(task) {
		t.Error("Expected task submission to succeed")
	}

	// Wait for result
	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Errorf("Unexpected error: %v", result.Error)
		}
		if result.Result != 84 {
			t.Errorf("Expected result 84, got %v", result.Result)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for result")
	}
}

func TestWorkerPool_SubmitBlocking(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{Workers: 2}
	pool := NewWorkerPool(processor, opts)

	pool.Start()
	defer pool.Stop()

	task := testTask{id: 99}

	if err := pool.SubmitBlocking(task); err != nil {
		t.Errorf("Expected blocking submission to succeed, got: %v", err)
	}

	// Wait for result
	select {
	case result := <-pool.Results():
		if result.Error != nil {
			t.Errorf("Unexpected error: %v", result.Error)
		}
		if result.Result != 198 {
			t.Errorf("Expected result 198, got %v", result.Result)
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for result")
	}
}

func TestWorkerPool_ConcurrentProcessing(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{Workers: 4}
	pool := NewWorkerPool(processor, opts)

	const numTasks = 100
	tasks := make([]testTask, numTasks)
	for i := 0; i < numTasks; i++ {
		tasks[i] = testTask{id: i}
	}

	start := time.Now()
	results := pool.ProcessBatch(tasks)
	elapsed := time.Since(start)

	if len(results) != numTasks {
		t.Errorf("Expected %d results, got %d", numTasks, len(results))
	}

	// Verify all tasks were processed correctly
	for i, result := range results {
		if result.Error != nil {
			t.Errorf("Task %d had error: %v", i, result.Error)
		}

		expected := tasks[i].id * 2
		if result.Result != expected {
			t.Errorf("Task %d: expected result %d, got %v", i, expected, result.Result)
		}
	}

	t.Logf("Processed %d tasks in %v", numTasks, elapsed)
}

func TestWorkerPool_TaskProcessorFunc(t *testing.T) {
	processorFunc := TaskProcessorFunc[testTask](func(ctx context.Context, task testTask) (interface{}, error) {
		return task.id * 3, nil
	})

	opts := WorkerPoolOptions{Workers: 1}
	pool := NewWorkerPool(processorFunc, opts)

	tasks := []testTask{{id: 5}}
	results := pool.ProcessBatch(tasks)

	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}

	if results[0].Result != 15 {
		t.Errorf("Expected result 15, got %v", results[0].Result)
	}
}

func TestWorkerPool_DefaultOptions(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{} // Use defaults
	pool := NewWorkerPool(processor, opts)

	// Should use runtime.NumCPU() for workers
	expectedWorkers := pool.WorkerCount()
	if expectedWorkers <= 0 {
		t.Error("Expected positive number of workers")
	}

	t.Logf("Default workers: %d", expectedWorkers)
}

func TestWorkerPool_StoppedSubmission(t *testing.T) {
	processor := &testProcessor{}
	opts := WorkerPoolOptions{Workers: 1}
	pool := NewWorkerPool(processor, opts)

	// Pool is not started, submissions should fail
	task := testTask{id: 1}
	if pool.Submit(task) {
		t.Error("Expected submission to fail when pool is stopped")
	}

	if err := pool.SubmitBlocking(task); err != errPoolClosed {
		t.Errorf("Expected errPoolClosed, got %v", err)
	}
}
