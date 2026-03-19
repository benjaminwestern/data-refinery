package processing

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineStage represents a single stage in the processing pipeline
type PipelineStage[T any] struct {
	Name         string
	Processor    func(ctx context.Context, input T) (T, error)
	Workers      int
	BufferSize   int
	input        chan T
	output       chan T
	errorChan    chan error
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	metrics      *PipelineMetrics
	stageMutex   sync.RWMutex
	isRunning    atomic.Bool
	backpressure *BackpressureController
}

// PipelineMetrics tracks performance metrics for pipeline stages
type PipelineMetrics struct {
	ProcessedItems   atomic.Int64
	ProcessingTime   atomic.Int64 // nanoseconds
	ErrorCount       atomic.Int64
	QueueDepth       atomic.Int64
	ThroughputPerSec atomic.Int64
	lastUpdate       atomic.Int64
	mutex            sync.RWMutex
}

// BackpressureController manages flow control between pipeline stages
type BackpressureController struct {
	maxQueueDepth    int
	currentDepth     atomic.Int64
	throttleDelay    time.Duration
	enabled          atomic.Bool
	adaptiveScaling  bool
	lastThrottleTime atomic.Int64
}

// Pipeline represents a multi-stage processing pipeline
type Pipeline[T any] struct {
	stages       []*PipelineStage[T]
	input        chan T
	output       chan T
	errorChan    chan error
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	metrics      *PipelineSystemMetrics
	mutex        sync.RWMutex
	isRunning    atomic.Bool
	backpressure *SystemBackpressureController
}

// PipelineSystemMetrics tracks overall pipeline performance
type PipelineSystemMetrics struct {
	TotalThroughput   atomic.Int64
	EndToEndLatency   atomic.Int64
	SystemUtilization atomic.Int64
	ActiveWorkers     atomic.Int64
	QueuedItems       atomic.Int64
	ErrorRate         atomic.Int64
	lastSnapshot      atomic.Int64
}

// SystemBackpressureController manages system-wide backpressure
type SystemBackpressureController struct {
	enabled           atomic.Bool
	globalThreshold   int
	adaptiveThreshold atomic.Int64
	congestionSignal  chan struct{}
	releaseSignal     chan struct{}
	mutex             sync.RWMutex
}

// NewPipeline creates a new processing pipeline
func NewPipeline[T any](ctx context.Context, bufferSize int) *Pipeline[T] {
	pipelineCtx, cancel := context.WithCancel(ctx)

	return &Pipeline[T]{
		stages:    make([]*PipelineStage[T], 0),
		input:     make(chan T, bufferSize),
		output:    make(chan T, bufferSize),
		errorChan: make(chan error, bufferSize),
		ctx:       pipelineCtx,
		cancel:    cancel,
		metrics:   &PipelineSystemMetrics{},
		backpressure: &SystemBackpressureController{
			globalThreshold:  bufferSize * 2,
			congestionSignal: make(chan struct{}, 1),
			releaseSignal:    make(chan struct{}, 1),
		},
	}
}

// AddStage adds a new processing stage to the pipeline
func (p *Pipeline[T]) AddStage(name string, processor func(ctx context.Context, input T) (T, error), workers int, bufferSize int) *Pipeline[T] {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isRunning.Load() {
		panic("cannot add stages to running pipeline")
	}

	stageCtx, cancel := context.WithCancel(p.ctx)

	stage := &PipelineStage[T]{
		Name:       name,
		Processor:  processor,
		Workers:    workers,
		BufferSize: bufferSize,
		input:      make(chan T, bufferSize),
		output:     make(chan T, bufferSize),
		errorChan:  make(chan error, bufferSize),
		ctx:        stageCtx,
		cancel:     cancel,
		metrics:    &PipelineMetrics{},
		backpressure: &BackpressureController{
			maxQueueDepth:   bufferSize * 2,
			throttleDelay:   time.Microsecond * 100,
			adaptiveScaling: true,
		},
	}

	p.stages = append(p.stages, stage)
	return p
}

// Start begins processing the pipeline
func (p *Pipeline[T]) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.isRunning.Load() {
		return fmt.Errorf("pipeline is already running")
	}

	if len(p.stages) == 0 {
		return fmt.Errorf("pipeline has no stages")
	}

	// Start backpressure monitoring
	p.wg.Add(3)
	go p.monitorBackpressure()

	// Start metrics collection
	go p.collectMetrics()

	// Start each stage
	for i, stage := range p.stages {
		if err := p.startStage(stage, i); err != nil {
			return fmt.Errorf("failed to start stage %s: %w", stage.Name, err)
		}
	}

	// Start pipeline coordinator
	go p.coordinate()

	p.isRunning.Store(true)
	p.metrics.lastSnapshot.Store(time.Now().UnixNano())

	return nil
}

// startStage starts a specific pipeline stage
func (p *Pipeline[T]) startStage(stage *PipelineStage[T], index int) error {
	stage.isRunning.Store(true)
	stage.metrics.lastUpdate.Store(time.Now().UnixNano())

	// Determine input channel
	var inputChan chan T
	if index == 0 {
		inputChan = p.input
	} else {
		inputChan = p.stages[index-1].output
	}

	// Start workers for this stage
	stage.wg.Add(stage.Workers + 2)
	for i := 0; i < stage.Workers; i++ {
		go p.runStageWorker(stage, inputChan, i)
	}

	// Start stage error handler
	go p.handleStageErrors(stage)

	// Start stage metrics collector
	go p.collectStageMetrics(stage)

	return nil
}

// runStageWorker runs a single worker for a pipeline stage
func (p *Pipeline[T]) runStageWorker(stage *PipelineStage[T], inputChan chan T, workerID int) {
	defer stage.wg.Done()

	for {
		select {
		case <-stage.ctx.Done():
			return
		case item, ok := <-inputChan:
			if !ok {
				return
			}

			// Apply backpressure if needed
			if stage.backpressure.enabled.Load() {
				p.applyBackpressure(stage)
			}

			// Process the item
			start := time.Now()
			result, err := stage.Processor(stage.ctx, item)
			processingTime := time.Since(start)

			// Update metrics
			stage.metrics.ProcessedItems.Add(1)
			stage.metrics.ProcessingTime.Add(processingTime.Nanoseconds())

			if err != nil {
				stage.metrics.ErrorCount.Add(1)
				select {
				case stage.errorChan <- fmt.Errorf("stage %s worker %d: %w", stage.Name, workerID, err):
				default:
					// Error channel full, drop error
				}
				continue
			}

			// Send to output
			select {
			case stage.output <- result:
			case <-stage.ctx.Done():
				return
			}
		}
	}
}

// applyBackpressure applies backpressure control to slow down processing
func (p *Pipeline[T]) applyBackpressure(stage *PipelineStage[T]) {
	currentDepth := stage.backpressure.currentDepth.Load()
	if currentDepth > int64(stage.backpressure.maxQueueDepth) {
		// Calculate adaptive delay
		delay := stage.backpressure.throttleDelay
		if stage.backpressure.adaptiveScaling {
			overload := float64(currentDepth) / float64(stage.backpressure.maxQueueDepth)
			delay = time.Duration(float64(delay) * overload)
		}

		stage.backpressure.lastThrottleTime.Store(time.Now().UnixNano())
		time.Sleep(delay)
	}
}

// coordinate coordinates the overall pipeline flow
func (p *Pipeline[T]) coordinate() {
	defer p.wg.Done()

	lastStage := p.stages[len(p.stages)-1]

	// Monitor pipeline health
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case item := <-lastStage.output:
			select {
			case p.output <- item:
			case <-p.ctx.Done():
				return
			}
		case <-ticker.C:
			p.updateSystemMetrics()
		}
	}
}

// updateSystemMetrics updates overall pipeline metrics
func (p *Pipeline[T]) updateSystemMetrics() {
	now := time.Now().UnixNano()
	lastSnapshot := p.metrics.lastSnapshot.Load()

	if lastSnapshot > 0 {
		duration := time.Duration(now - lastSnapshot)

		// Calculate throughput
		var totalProcessed int64
		var totalErrors int64
		var totalQueueDepth int64
		var activeWorkers int64

		for _, stage := range p.stages {
			totalProcessed += stage.metrics.ProcessedItems.Load()
			totalErrors += stage.metrics.ErrorCount.Load()
			totalQueueDepth += int64(len(stage.input)) + int64(len(stage.output))
			activeWorkers += int64(stage.Workers)
		}

		throughput := totalProcessed * int64(time.Second) / int64(duration)
		errorRate := totalErrors * 1000 / (totalProcessed + 1) // per mille

		p.metrics.TotalThroughput.Store(throughput)
		p.metrics.QueuedItems.Store(totalQueueDepth)
		p.metrics.ActiveWorkers.Store(activeWorkers)
		p.metrics.ErrorRate.Store(errorRate)
	}

	p.metrics.lastSnapshot.Store(now)
}

// monitorBackpressure monitors system-wide backpressure
func (p *Pipeline[T]) monitorBackpressure() {
	defer p.wg.Done()

	ticker := time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkBackpressure()
		}
	}
}

// checkBackpressure checks if system-wide backpressure should be applied
func (p *Pipeline[T]) checkBackpressure() {
	totalQueueDepth := int64(len(p.input)) + int64(len(p.output))

	for _, stage := range p.stages {
		stageDepth := int64(len(stage.input)) + int64(len(stage.output))
		totalQueueDepth += stageDepth
		stage.backpressure.currentDepth.Store(stageDepth)
	}

	// Enable backpressure if queues are getting too deep
	if totalQueueDepth > int64(p.backpressure.globalThreshold) {
		p.backpressure.enabled.Store(true)
		p.backpressure.adaptiveThreshold.Store(totalQueueDepth)

		// Signal congestion
		select {
		case p.backpressure.congestionSignal <- struct{}{}:
		default:
		}

		// Enable backpressure on all stages
		for _, stage := range p.stages {
			stage.backpressure.enabled.Store(true)
		}
	} else if totalQueueDepth < int64(p.backpressure.globalThreshold/2) {
		p.backpressure.enabled.Store(false)

		// Signal release
		select {
		case p.backpressure.releaseSignal <- struct{}{}:
		default:
		}

		// Disable backpressure on all stages
		for _, stage := range p.stages {
			stage.backpressure.enabled.Store(false)
		}
	}
}

// collectMetrics collects performance metrics
func (p *Pipeline[T]) collectMetrics() {
	defer p.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.updateSystemMetrics()
		}
	}
}

// collectStageMetrics collects metrics for a specific stage
func (p *Pipeline[T]) collectStageMetrics(stage *PipelineStage[T]) {
	defer stage.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stage.ctx.Done():
			return
		case <-ticker.C:
			p.updateStageMetrics(stage)
		}
	}
}

// updateStageMetrics updates metrics for a specific stage
func (p *Pipeline[T]) updateStageMetrics(stage *PipelineStage[T]) {
	now := time.Now().UnixNano()
	lastUpdate := stage.metrics.lastUpdate.Load()

	if lastUpdate > 0 {
		duration := time.Duration(now - lastUpdate)
		processed := stage.metrics.ProcessedItems.Load()

		if duration > 0 {
			throughput := processed * int64(time.Second) / int64(duration)
			stage.metrics.ThroughputPerSec.Store(throughput)
		}

		stage.metrics.QueueDepth.Store(int64(len(stage.input)) + int64(len(stage.output)))
	}

	stage.metrics.lastUpdate.Store(now)
}

// handleStageErrors handles errors from a specific stage
func (p *Pipeline[T]) handleStageErrors(stage *PipelineStage[T]) {
	defer stage.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			if err := p.ctx.Err(); err != nil {
				select {
				case p.errorChan <- err:
				default:
				}
			}
			return
		case <-stage.ctx.Done():
			if err := stage.ctx.Err(); err != nil {
				select {
				case p.errorChan <- err:
				case <-p.ctx.Done():
				default:
				}
			}
			return
		case err, ok := <-stage.errorChan:
			if !ok {
				return
			}

			// Forward error to pipeline error channel
			select {
			case p.errorChan <- err:
			case <-p.ctx.Done():
				// Pipeline context done while trying to send error
				return
			default:
				// Pipeline error channel full, drop error
			}
		}
	}
}

// Submit submits an item to the pipeline for processing
func (p *Pipeline[T]) Submit(item T) error {
	if !p.isRunning.Load() {
		return fmt.Errorf("pipeline is not running")
	}

	select {
	case p.input <- item:
		return nil
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

// Results returns the output channel for processed items
func (p *Pipeline[T]) Results() <-chan T {
	return p.output
}

// Errors returns the error channel
func (p *Pipeline[T]) Errors() <-chan error {
	return p.errorChan
}

// GetMetrics returns current pipeline metrics
func (p *Pipeline[T]) GetMetrics() *PipelineSystemMetrics {
	p.updateSystemMetrics()
	return p.metrics
}

// GetStageMetrics returns metrics for a specific stage
func (p *Pipeline[T]) GetStageMetrics(stageName string) *PipelineMetrics {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	for _, stage := range p.stages {
		if stage.Name == stageName {
			p.updateStageMetrics(stage)
			return stage.metrics
		}
	}
	return nil
}

// Stop stops the pipeline and all stages
func (p *Pipeline[T]) Stop() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.isRunning.Load() {
		return fmt.Errorf("pipeline is not running")
	}

	p.isRunning.Store(false)

	// Cancel all contexts
	p.cancel()
	for _, stage := range p.stages {
		stage.cancel()
	}

	// Stop all stages
	for _, stage := range p.stages {
		stage.wg.Wait()
		stage.isRunning.Store(false)
	}

	// Wait for pipeline-level goroutines to finish
	p.wg.Wait()

	return nil
}

// IsRunning returns whether the pipeline is currently running
func (p *Pipeline[T]) IsRunning() bool {
	return p.isRunning.Load()
}

// GetBackpressureStatus returns current backpressure status
func (p *Pipeline[T]) GetBackpressureStatus() (bool, int64) {
	return p.backpressure.enabled.Load(), p.backpressure.adaptiveThreshold.Load()
}

// SetBackpressureThreshold sets the global backpressure threshold
func (p *Pipeline[T]) SetBackpressureThreshold(threshold int) {
	p.backpressure.mutex.Lock()
	defer p.backpressure.mutex.Unlock()

	p.backpressure.globalThreshold = threshold
}
