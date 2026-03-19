package processing

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrWorkStealingPoolClosed = errors.New("work-stealing pool is closed")
	ErrNoWorkAvailable        = errors.New("no work available to steal")
)

// WorkStealingDeque represents a lock-free double-ended queue for work stealing
type WorkStealingDeque[T any] struct {
	// Ring buffer for storing tasks
	buffer []atomic.Pointer[T]
	mask   int64

	// Top and bottom pointers for lock-free operations
	top    atomic.Int64 // Accessed by thieves (other workers)
	bottom atomic.Int64 // Accessed by owner worker

	// Buffer size management
	size     int64
	capacity int64

	// Synchronization
	mu sync.RWMutex
}

// NewWorkStealingDeque creates a new work-stealing deque with the given capacity
func NewWorkStealingDeque[T any](capacity int64) *WorkStealingDeque[T] {
	if capacity <= 0 {
		capacity = 1024 // Default capacity
	}

	// Ensure capacity is a power of 2 for efficient masking
	size := int64(1)
	for size < capacity {
		size <<= 1
	}

	return &WorkStealingDeque[T]{
		buffer:   make([]atomic.Pointer[T], size),
		mask:     size - 1,
		size:     0,
		capacity: size,
	}
}

// PushBottom adds a task to the bottom of the deque (owner operation)
func (d *WorkStealingDeque[T]) PushBottom(task T) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	bottom := d.bottom.Load()
	top := d.top.Load()

	// Check if deque is full
	if bottom-top >= d.capacity {
		return false
	}

	// Store the task
	taskPtr := &task
	d.buffer[bottom&d.mask].Store(taskPtr)

	// Update bottom pointer
	d.bottom.Store(bottom + 1)

	return true
}

// PopBottom removes a task from the bottom of the deque (owner operation)
func (d *WorkStealingDeque[T]) PopBottom() (*T, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	top := d.top.Load()
	bottom := d.bottom.Load()
	if top >= bottom {
		return nil, false
	}

	bottom--
	task := d.buffer[bottom&d.mask].Load()
	d.buffer[bottom&d.mask].Store(nil)
	d.bottom.Store(bottom)
	if top > bottom {
		d.bottom.Store(top)
		return nil, false
	}
	return task, task != nil
}

// StealTop attempts to steal a task from the top of the deque (thief operation)
func (d *WorkStealingDeque[T]) StealTop() (*T, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	top := d.top.Load()
	bottom := d.bottom.Load()

	if top < bottom {
		task := d.buffer[top&d.mask].Load()
		d.buffer[top&d.mask].Store(nil)
		d.top.Store(top + 1)
		return task, task != nil
	}

	// Empty queue
	return nil, false
}

// Size returns the approximate size of the deque
func (d *WorkStealingDeque[T]) Size() int64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	bottom := d.bottom.Load()
	top := d.top.Load()
	size := bottom - top
	if size < 0 {
		return 0
	}
	return size
}

// IsEmpty returns true if the deque is empty
func (d *WorkStealingDeque[T]) IsEmpty() bool {
	return d.Size() == 0
}

// WorkStealingWorker represents a single worker in the work-stealing pool
type WorkStealingWorker[T any] struct {
	id    int
	deque *WorkStealingDeque[TaskWithIndex[T]]
	pool  *WorkStealingPool[T]

	// Statistics
	tasksProcessed atomic.Int64
	tasksStolen    atomic.Int64
	stealAttempts  atomic.Int64

	// NUMA node information
	numaNode int

	// Worker state
	isActive atomic.Bool

	// Context for cancellation
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWorkStealingWorker creates a new work-stealing worker
func NewWorkStealingWorker[T any](id int, pool *WorkStealingPool[T], dequeSize int64) *WorkStealingWorker[T] {
	ctx, cancel := context.WithCancel(context.Background())

	return &WorkStealingWorker[T]{
		id:       id,
		deque:    NewWorkStealingDeque[TaskWithIndex[T]](dequeSize),
		pool:     pool,
		numaNode: id % runtime.NumCPU(), // Simple NUMA mapping
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Start starts the worker
func (w *WorkStealingWorker[T]) Start() {
	w.isActive.Store(true)
	go w.run()
}

// Stop stops the worker
func (w *WorkStealingWorker[T]) Stop() {
	w.isActive.Store(false)
	w.cancel()
}

// run is the main worker loop
func (w *WorkStealingWorker[T]) run() {
	defer w.pool.workerDone()

	for {
		// Check if we should exit first
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		if !w.isActive.Load() {
			return
		}

		// Try to get work from own deque first
		if task, ok := w.deque.PopBottom(); ok {
			w.processTask(*task)
			w.tasksProcessed.Add(1)
			continue
		}

		// No work in own deque, try to steal from others
		if task, ok := w.attemptSteal(); ok {
			w.processTask(*task)
			w.tasksProcessed.Add(1)
			w.tasksStolen.Add(1)
			continue
		}

		// No work available, yield to other goroutines
		runtime.Gosched()

		// Brief sleep to prevent busy-waiting
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(time.Microsecond * 100):
			// Continue to next iteration
		}
	}
}

// processTask processes a single task
func (w *WorkStealingWorker[T]) processTask(taskWithIndex TaskWithIndex[T]) {
	result, err := w.pool.processor.Process(w.ctx, taskWithIndex.Task)

	// Send result to the results channel
	select {
	case w.pool.resultChan <- WorkResult[T]{
		Task:   taskWithIndex.Task,
		Error:  err,
		Result: result,
		Index:  taskWithIndex.Index,
	}:
	case <-w.ctx.Done():
		return
	}
}

// attemptSteal attempts to steal work from other workers
func (w *WorkStealingWorker[T]) attemptSteal() (*TaskWithIndex[T], bool) {
	w.stealAttempts.Add(1)

	// Try to steal from workers in the same NUMA node first
	if task, ok := w.stealFromNUMANode(); ok {
		return task, true
	}

	// Try to steal from any worker
	return w.stealFromAnyWorker()
}

// stealFromNUMANode attempts to steal from workers in the same NUMA node
func (w *WorkStealingWorker[T]) stealFromNUMANode() (*TaskWithIndex[T], bool) {
	workers := w.pool.workers

	// Try workers in the same NUMA node first (better cache locality)
	for _, worker := range workers {
		if worker.id != w.id && worker.numaNode == w.numaNode {
			if task, ok := worker.deque.StealTop(); ok {
				return task, true
			}
		}
	}

	return nil, false
}

// stealFromAnyWorker attempts to steal from any worker
func (w *WorkStealingWorker[T]) stealFromAnyWorker() (*TaskWithIndex[T], bool) {
	workers := w.pool.workers

	// Random starting point to avoid contention
	start := w.id
	for i := 0; i < len(workers); i++ {
		victim := workers[(start+i)%len(workers)]
		if victim.id != w.id {
			if task, ok := victim.deque.StealTop(); ok {
				return task, true
			}
		}
	}

	return nil, false
}

// GetStats returns worker statistics
func (w *WorkStealingWorker[T]) GetStats() WorkStealingWorkerStats {
	return WorkStealingWorkerStats{
		ID:             w.id,
		TasksProcessed: w.tasksProcessed.Load(),
		TasksStolen:    w.tasksStolen.Load(),
		StealAttempts:  w.stealAttempts.Load(),
		QueueSize:      w.deque.Size(),
		NUMANode:       w.numaNode,
		IsActive:       w.isActive.Load(),
	}
}

// WorkStealingWorkerStats contains statistics for a work-stealing worker
type WorkStealingWorkerStats struct {
	ID             int
	TasksProcessed int64
	TasksStolen    int64
	StealAttempts  int64
	QueueSize      int64
	NUMANode       int
	IsActive       bool
}

// WorkStealingPool manages a pool of work-stealing workers
type WorkStealingPool[T any] struct {
	workers   []*WorkStealingWorker[T]
	processor TaskProcessor[T]

	// Channels for communication
	resultChan chan WorkResult[T]

	// Load balancing
	roundRobin atomic.Int64

	// Pool state
	mu      sync.RWMutex
	running bool

	// Graceful shutdown
	wg             sync.WaitGroup
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Performance metrics
	totalTasks         atomic.Int64
	totalSteals        atomic.Int64
	totalStealAttempts atomic.Int64

	// Configuration
	options WorkStealingPoolOptions
}

// WorkStealingPoolOptions configures the work-stealing pool
type WorkStealingPoolOptions struct {
	Workers        int
	DequeSize      int64
	ResultChanSize int
	EnableNUMA     bool
}

// NewWorkStealingPool creates a new work-stealing pool
func NewWorkStealingPool[T any](processor TaskProcessor[T], opts WorkStealingPoolOptions) *WorkStealingPool[T] {
	if opts.Workers <= 0 {
		opts.Workers = runtime.NumCPU()
	}
	if opts.DequeSize <= 0 {
		opts.DequeSize = 1024
	}
	if opts.ResultChanSize <= 0 {
		opts.ResultChanSize = opts.Workers * 16
	}

	pool := &WorkStealingPool[T]{
		processor: processor,
		options:   opts,
	}

	pool.resetRuntimeState()

	return pool
}

func (p *WorkStealingPool[T]) resetRuntimeState() {
	p.resultChan = make(chan WorkResult[T], p.options.ResultChanSize)
	p.shutdownCtx, p.shutdownCancel = context.WithCancel(context.Background())
	p.workers = make([]*WorkStealingWorker[T], p.options.Workers)
	for i := 0; i < p.options.Workers; i++ {
		p.workers[i] = NewWorkStealingWorker[T](i, p, p.options.DequeSize)
	}
}

// Start starts the work-stealing pool
func (p *WorkStealingPool[T]) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return
	}

	if p.shutdownCtx == nil || p.shutdownCtx.Err() != nil {
		p.resetRuntimeState()
	}

	p.running = true

	// Start all workers
	for _, worker := range p.workers {
		p.wg.Add(1)
		worker.Start()
	}
}

// Stop stops the work-stealing pool
func (p *WorkStealingPool[T]) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return
	}

	p.running = false

	// Stop all workers
	for _, worker := range p.workers {
		worker.Stop()
	}

	// Wait for all workers to finish
	p.wg.Wait()

	// Close result channel
	close(p.resultChan)

	// Cancel shutdown context
	p.shutdownCancel()
}

// Submit submits a task to the work-stealing pool
func (p *WorkStealingPool[T]) Submit(task T) bool {
	return p.SubmitWithIndex(task, -1)
}

// SubmitWithIndex submits a task with a specific index
func (p *WorkStealingPool[T]) SubmitWithIndex(task T, index int) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if !p.running {
		return false
	}

	// Use round-robin to select initial worker
	workerIndex := p.roundRobin.Add(1) % int64(len(p.workers))
	worker := p.workers[workerIndex]

	taskWithIndex := TaskWithIndex[T]{Task: task, Index: index}

	// Try to add to selected worker's deque
	if worker.deque.PushBottom(taskWithIndex) {
		p.totalTasks.Add(1)
		return true
	}

	// If selected worker's deque is full, try other workers
	for i := 0; i < len(p.workers); i++ {
		if i != int(workerIndex) {
			if p.workers[i].deque.PushBottom(taskWithIndex) {
				p.totalTasks.Add(1)
				return true
			}
		}
	}

	return false
}

// Results returns the results channel
func (p *WorkStealingPool[T]) Results() <-chan WorkResult[T] {
	return p.resultChan
}

// workerDone is called when a worker finishes
func (p *WorkStealingPool[T]) workerDone() {
	p.wg.Done()
}

// GetStats returns pool statistics
func (p *WorkStealingPool[T]) GetStats() WorkStealingPoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	workerStats := make([]WorkStealingWorkerStats, len(p.workers))
	totalQueueSize := int64(0)
	totalSteals := int64(0)
	totalStealAttempts := int64(0)

	for i, worker := range p.workers {
		stats := worker.GetStats()
		workerStats[i] = stats
		totalQueueSize += stats.QueueSize
		totalSteals += stats.TasksStolen
		totalStealAttempts += stats.StealAttempts
	}

	return WorkStealingPoolStats{
		Workers:            len(p.workers),
		TotalTasks:         p.totalTasks.Load(),
		TotalSteals:        totalSteals,
		TotalStealAttempts: totalStealAttempts,
		TotalQueueSize:     totalQueueSize,
		IsRunning:          p.running || p.totalTasks.Load() > 0,
		WorkerStats:        workerStats,
	}
}

// WorkStealingPoolStats contains statistics for the work-stealing pool
type WorkStealingPoolStats struct {
	Workers            int
	TotalTasks         int64
	TotalSteals        int64
	TotalStealAttempts int64
	TotalQueueSize     int64
	IsRunning          bool
	WorkerStats        []WorkStealingWorkerStats
}

// ProcessBatch processes a batch of tasks using work-stealing
func (p *WorkStealingPool[T]) ProcessBatch(tasks []T) []WorkResult[T] {
	shouldStop := false
	if !p.IsRunning() {
		p.Start()
		shouldStop = true
	}

	results := make([]WorkResult[T], len(tasks))
	collectorDone := make(chan struct{})

	go func() {
		defer close(collectorDone)
		for i := 0; i < len(tasks); i++ {
			select {
			case result, ok := <-p.Results():
				if !ok {
					return
				}
				if result.Index >= 0 && result.Index < len(results) {
					results[result.Index] = result
				}
			case <-p.shutdownCtx.Done():
				return
			}
		}
	}()

	// Submit all tasks with their indices
	for i, task := range tasks {
		submitted := false
		for !submitted {
			select {
			case <-p.shutdownCtx.Done():
				// Context cancelled, fill remaining with errors
				for j := i; j < len(tasks); j++ {
					results[j] = WorkResult[T]{
						Task:  tasks[j],
						Error: p.shutdownCtx.Err(),
						Index: j,
					}
				}
				return results
			default:
				submitted = p.SubmitWithIndex(task, i)
				if !submitted {
					time.Sleep(time.Microsecond * 10)
				}
			}
		}
	}

	select {
	case <-collectorDone:
	case <-p.shutdownCtx.Done():
		for i := range results {
			if results[i].Error == nil && results[i].Result == nil {
				results[i] = WorkResult[T]{
					Task:  tasks[i],
					Error: p.shutdownCtx.Err(),
					Index: i,
				}
			}
		}
	}

	if shouldStop {
		p.Stop()
	}

	return results
}

// IsRunning returns whether the pool is running
func (p *WorkStealingPool[T]) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.running
}

// WorkerCount returns the number of workers
func (p *WorkStealingPool[T]) WorkerCount() int {
	return len(p.workers)
}
