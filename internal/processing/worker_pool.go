package processing

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/safety"
)

var (
	errPoolClosed = errors.New("worker pool is closed")
	errTimeout    = errors.New("operation timed out")
)

// ConcurrencyManager manages structured concurrency for the worker pool.
type ConcurrencyManager struct {
	mu         sync.RWMutex
	goroutines map[string]context.CancelFunc
	shutdownCh chan struct{}
	shutdownWg sync.WaitGroup
	isShutdown bool
}

// NewConcurrencyManager creates a new concurrency manager.
func NewConcurrencyManager() *ConcurrencyManager {
	return &ConcurrencyManager{
		goroutines: make(map[string]context.CancelFunc),
		shutdownCh: make(chan struct{}),
	}
}

// SpawnGoroutine spawns a managed goroutine with structured lifecycle.
func (cm *ConcurrencyManager) SpawnGoroutine(ctx context.Context, id string, fn func(context.Context)) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.isShutdown {
		return errors.New("concurrency manager is shutdown")
	}

	// Create a context that can be cancelled
	childCtx, cancel := safety.ManagedContext(ctx)
	cm.goroutines[id] = cancel

	cm.shutdownWg.Add(1)
	go func() {
		defer cm.shutdownWg.Done()
		defer func() {
			cm.mu.Lock()
			delete(cm.goroutines, id)
			cm.mu.Unlock()
		}()

		fn(childCtx)
	}()

	return nil
}

// CancelGoroutine cancels a specific goroutine.
func (cm *ConcurrencyManager) CancelGoroutine(id string) {
	cm.mu.RLock()
	cancel, exists := cm.goroutines[id]
	cm.mu.RUnlock()

	if exists {
		cancel()
	}
}

// Shutdown gracefully shuts down all managed goroutines.
func (cm *ConcurrencyManager) Shutdown(timeout time.Duration) error {
	cm.mu.Lock()
	if cm.isShutdown {
		cm.mu.Unlock()
		return nil
	}
	cm.isShutdown = true

	// Cancel all goroutines
	for _, cancel := range cm.goroutines {
		cancel()
	}
	cm.mu.Unlock()

	// Wait for all goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		cm.shutdownWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errTimeout
	}
}

// WorkerPool manages a pool of workers for concurrent processing.
type WorkerPool[T any] struct {
	workers            int
	taskChan           chan TaskWithIndex[T]
	resultChan         chan WorkResult[T]
	wg                 sync.WaitGroup
	processor          TaskProcessor[T]
	ctx                context.Context
	cancel             context.CancelFunc
	mu                 sync.RWMutex
	running            bool
	concurrencyManager *ConcurrencyManager
}

// TaskWithIndex wraps a task with its index for ordering.
type TaskWithIndex[T any] struct {
	Task  T
	Index int
}

// WorkResult represents the result of processing a task.
type WorkResult[T any] struct {
	Task   T
	Error  error
	Result interface{}
	Index  int // Track original task index for ordering
}

// TaskProcessor defines the interface for processing tasks.
type TaskProcessor[T any] interface {
	Process(ctx context.Context, task T) (interface{}, error)
}

// TaskProcessorFunc is a function adapter for TaskProcessor.
type TaskProcessorFunc[T any] func(ctx context.Context, task T) (interface{}, error)

// Process satisfies TaskProcessor by forwarding to the wrapped function.
func (f TaskProcessorFunc[T]) Process(ctx context.Context, task T) (interface{}, error) {
	return f(ctx, task)
}

// WorkerPoolOptions configures the worker pool.
type WorkerPoolOptions struct {
	Workers    int
	BufferSize int
	// AdaptiveBuffering enables dynamic buffer size adjustment based on workload
	AdaptiveBuffering bool
	// MaxBufferSize sets the maximum buffer size when adaptive buffering is enabled
	MaxBufferSize int
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool[T any](processor TaskProcessor[T], opts WorkerPoolOptions) *WorkerPool[T] {
	if opts.Workers <= 0 {
		opts.Workers = runtime.NumCPU()
	}
	if opts.BufferSize <= 0 {
		// Use a more intelligent default buffer size
		opts.BufferSize = opts.Workers * 8 // Better balance between memory and throughput
	}

	// Set reasonable maximum buffer size for adaptive buffering
	if opts.AdaptiveBuffering && opts.MaxBufferSize <= 0 {
		opts.MaxBufferSize = opts.Workers * 32 // Allow growth but limit memory usage
	}

	ctx, cancel := safety.ManagedContext(context.Background())

	return &WorkerPool[T]{
		workers:            opts.Workers,
		taskChan:           make(chan TaskWithIndex[T], opts.BufferSize),
		resultChan:         make(chan WorkResult[T], opts.BufferSize),
		processor:          processor,
		ctx:                ctx,
		cancel:             cancel,
		concurrencyManager: NewConcurrencyManager(),
	}
}

// Start starts the worker pool.
func (wp *WorkerPool[T]) Start() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if wp.running {
		return
	}

	wp.running = true

	// Start workers
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}

	// Start result collector
	go wp.collectResults()
}

// Stop stops the worker pool and waits for all workers to finish.
func (wp *WorkerPool[T]) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	if !wp.running {
		return
	}

	wp.running = false
	close(wp.taskChan)
	wp.wg.Wait()
	close(wp.resultChan)
	wp.cancel()

	// Use structured concurrency for graceful shutdown
	if wp.concurrencyManager != nil {
		if err := wp.concurrencyManager.Shutdown(5 * time.Second); err != nil {
			log.Printf("worker pool shutdown warning: %v", err)
		}
	}
}

// Submit submits a task to the worker pool.
func (wp *WorkerPool[T]) Submit(task T) bool {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.running {
		return false
	}

	taskWithIndex := TaskWithIndex[T]{Task: task, Index: -1}
	select {
	case wp.taskChan <- taskWithIndex:
		return true
	case <-wp.ctx.Done():
		return false
	default:
		return false
	}
}

// SubmitBlocking submits a task and blocks until accepted or context is cancelled.
func (wp *WorkerPool[T]) SubmitBlocking(task T) error {
	return wp.submitBlockingWithIndex(TaskWithIndex[T]{Task: task, Index: -1})
}

// submitBlockingWithIndex submits a task with index and blocks until accepted or context is cancelled.
func (wp *WorkerPool[T]) submitBlockingWithIndex(taskWithIndex TaskWithIndex[T]) error {
	wp.mu.RLock()
	defer wp.mu.RUnlock()

	if !wp.running {
		return errPoolClosed
	}

	select {
	case wp.taskChan <- taskWithIndex:
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("submit worker pool task: %w", wp.ctx.Err())
	}
}

// Results returns the results channel.
func (wp *WorkerPool[T]) Results() <-chan WorkResult[T] {
	return wp.resultChan
}

// worker is the worker goroutine that processes tasks.
func (wp *WorkerPool[T]) worker() {
	defer wp.wg.Done()

	for {
		select {
		case taskWithIndex, ok := <-wp.taskChan:
			if !ok {
				return
			}

			result, err := wp.processor.Process(wp.ctx, taskWithIndex.Task)

			select {
			case wp.resultChan <- WorkResult[T]{
				Task:   taskWithIndex.Task,
				Error:  err,
				Result: result,
				Index:  taskWithIndex.Index,
			}:
			case <-wp.ctx.Done():
				return
			}

		case <-wp.ctx.Done():
			return
		}
	}
}

// collectResults collects results and handles any cleanup.
func (wp *WorkerPool[T]) collectResults() {
	// This goroutine ensures the results channel stays open
	// until all workers are done
	wp.wg.Wait()
}

// IsRunning returns whether the pool is currently running.
func (wp *WorkerPool[T]) IsRunning() bool {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.running
}

// WorkerCount returns the number of workers in the pool.
func (wp *WorkerPool[T]) WorkerCount() int {
	return wp.workers
}

// WithContext returns a new WorkerPool with the given context.
func (wp *WorkerPool[T]) WithContext(ctx context.Context) *WorkerPool[T] {
	newCtx, cancel := safety.ManagedContext(ctx)

	// Create a new worker pool with the same configuration but new context
	newPool := &WorkerPool[T]{
		workers:    wp.workers,
		taskChan:   wp.taskChan,
		resultChan: wp.resultChan,
		processor:  wp.processor,
		ctx:        newCtx,
		cancel:     cancel,
		running:    wp.running,
	}

	return newPool
}

// SetMaxWorkers dynamically adjusts the number of workers (for future enhancement).
func (wp *WorkerPool[T]) SetMaxWorkers(maxWorkers int) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	// For now, just store the intended worker count
	// In a full implementation, this would spawn or terminate workers
	if maxWorkers > 0 {
		wp.workers = maxWorkers
	}
}

// ProcessBatch processes a batch of tasks and returns all results in the same order.
func (wp *WorkerPool[T]) ProcessBatch(tasks []T) []WorkResult[T] {
	shouldStop := false
	if !wp.IsRunning() {
		wp.Start()
		shouldStop = true
	}

	results := make([]WorkResult[T], len(tasks))

	// Submit all tasks with their indices in a separate goroutine
	// to avoid blocking when the channel is full
	go func() {
		for i, task := range tasks {
			taskWithIndex := TaskWithIndex[T]{Task: task, Index: i}
			select {
			case wp.taskChan <- taskWithIndex:
				// Successfully submitted
			case <-wp.ctx.Done():
				// Context cancelled, stop submitting
				return
			}
		}
	}()

	// Collect results for all tasks
	for i := 0; i < len(tasks); i++ {
		select {
		case result := <-wp.Results():
			if result.Index >= 0 && result.Index < len(results) {
				results[result.Index] = result
			}
		case <-wp.ctx.Done():
			// Context cancelled, fill remaining with errors
			for j := i; j < len(tasks); j++ {
				if results[j].Error == nil && results[j].Result == nil {
					results[j] = WorkResult[T]{
						Task:  tasks[j],
						Error: wp.ctx.Err(),
						Index: j,
					}
				}
			}
			return results
		}
	}

	if shouldStop {
		wp.Stop()
	}

	return results
}
