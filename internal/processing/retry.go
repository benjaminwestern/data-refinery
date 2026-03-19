package processing

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

var (
	ErrRetryExhausted    = errors.New("retry attempts exhausted")
	ErrRetryContextDone  = errors.New("retry context cancelled")
	ErrRetryNotRetryable = errors.New("error is not retryable")
	ErrRetryTimeout      = errors.New("retry timeout exceeded")
)

// RetryableError represents an error that can be retried
type RetryableError interface {
	error
	IsRetryable() bool
	RetryAfter() time.Duration
}

// retryableErrorImpl is a concrete implementation of RetryableError
type retryableErrorImpl struct {
	err        error
	retryable  bool
	retryAfter time.Duration
}

func (r *retryableErrorImpl) Error() string {
	return r.err.Error()
}

func (r *retryableErrorImpl) IsRetryable() bool {
	return r.retryable
}

func (r *retryableErrorImpl) RetryAfter() time.Duration {
	return r.retryAfter
}

func (r *retryableErrorImpl) Unwrap() error {
	return r.err
}

// NewRetryableError creates a new retryable error
func NewRetryableError(err error, retryable bool, retryAfter time.Duration) RetryableError {
	return &retryableErrorImpl{
		err:        err,
		retryable:  retryable,
		retryAfter: retryAfter,
	}
}

// BackoffStrategy defines the strategy for calculating retry delays
type BackoffStrategy interface {
	NextDelay(attempt int, lastError error) time.Duration
	Reset()
}

// ExponentialBackoff implements exponential backoff with jitter
type ExponentialBackoff struct {
	BaseDelay  time.Duration
	MaxDelay   time.Duration
	Multiplier float64
	Jitter     bool
	JitterType JitterType
	randomizer *rand.Rand
	mu         sync.Mutex
}

// JitterType defines the type of jitter to apply
type JitterType int

const (
	JitterNone JitterType = iota
	JitterFull
	JitterEqual
	JitterDecorrelated
)

// NewExponentialBackoff creates a new exponential backoff strategy
func NewExponentialBackoff(baseDelay, maxDelay time.Duration, multiplier float64, jitter bool) *ExponentialBackoff {
	if multiplier <= 1.0 {
		multiplier = 2.0
	}
	if maxDelay <= 0 {
		maxDelay = 60 * time.Second
	}
	if baseDelay <= 0 {
		baseDelay = 100 * time.Millisecond
	}

	return &ExponentialBackoff{
		BaseDelay:  baseDelay,
		MaxDelay:   maxDelay,
		Multiplier: multiplier,
		Jitter:     jitter,
		JitterType: JitterFull,
		randomizer: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NextDelay calculates the next delay based on the attempt number
func (eb *ExponentialBackoff) NextDelay(attempt int, lastError error) time.Duration {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if attempt < 0 {
		attempt = 0
	}

	// Check if the error has a specific retry after duration
	if retryableErr, ok := lastError.(RetryableError); ok {
		if retryAfter := retryableErr.RetryAfter(); retryAfter > 0 {
			return retryAfter
		}
	}

	// Calculate base delay with exponential backoff
	delay := float64(eb.BaseDelay) * math.Pow(eb.Multiplier, float64(attempt))

	// Cap at maximum delay
	if delay > float64(eb.MaxDelay) {
		delay = float64(eb.MaxDelay)
	}

	// Apply jitter if enabled
	if eb.Jitter {
		delay = eb.applyJitter(delay)
	}

	return time.Duration(delay)
}

// applyJitter applies jitter to the delay
func (eb *ExponentialBackoff) applyJitter(delay float64) float64 {
	switch eb.JitterType {
	case JitterFull:
		// Full jitter: random value between 0 and delay
		return eb.randomizer.Float64() * delay
	case JitterEqual:
		// Equal jitter: delay/2 + random(0, delay/2)
		return delay/2 + eb.randomizer.Float64()*(delay/2)
	case JitterDecorrelated:
		// Decorrelated jitter: min(maxDelay, randomBetween(baseDelay, delay * 3))
		minDelay := float64(eb.BaseDelay)
		maxJitter := math.Min(float64(eb.MaxDelay), delay*3)
		return minDelay + eb.randomizer.Float64()*(maxJitter-minDelay)
	default:
		return delay
	}
}

// Reset resets the backoff strategy
func (eb *ExponentialBackoff) Reset() {
	// Nothing to reset for exponential backoff
}

// LinearBackoff implements linear backoff
type LinearBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Increment time.Duration
}

// NewLinearBackoff creates a new linear backoff strategy
func NewLinearBackoff(baseDelay, maxDelay, increment time.Duration) *LinearBackoff {
	return &LinearBackoff{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Increment: increment,
	}
}

// NextDelay calculates the next delay for linear backoff
func (lb *LinearBackoff) NextDelay(attempt int, lastError error) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	// Check if the error has a specific retry after duration
	if retryableErr, ok := lastError.(RetryableError); ok {
		if retryAfter := retryableErr.RetryAfter(); retryAfter > 0 {
			return retryAfter
		}
	}

	delay := lb.BaseDelay + time.Duration(attempt)*lb.Increment
	if delay > lb.MaxDelay {
		delay = lb.MaxDelay
	}

	return delay
}

// Reset resets the linear backoff strategy
func (lb *LinearBackoff) Reset() {
	// Nothing to reset for linear backoff
}

// FixedBackoff implements fixed delay backoff
type FixedBackoff struct {
	Delay time.Duration
}

// NewFixedBackoff creates a new fixed backoff strategy
func NewFixedBackoff(delay time.Duration) *FixedBackoff {
	return &FixedBackoff{Delay: delay}
}

// NextDelay returns the fixed delay
func (fb *FixedBackoff) NextDelay(attempt int, lastError error) time.Duration {
	// Check if the error has a specific retry after duration
	if retryableErr, ok := lastError.(RetryableError); ok {
		if retryAfter := retryableErr.RetryAfter(); retryAfter > 0 {
			return retryAfter
		}
	}

	return fb.Delay
}

// Reset resets the fixed backoff strategy
func (fb *FixedBackoff) Reset() {
	// Nothing to reset for fixed backoff
}

// RetryCondition defines when to retry an operation
type RetryCondition func(error) bool

// DefaultRetryCondition is the default retry condition
func DefaultRetryCondition(err error) bool {
	if err == nil {
		return false
	}

	// Check if error is explicitly retryable
	if retryableErr, ok := err.(RetryableError); ok {
		return retryableErr.IsRetryable()
	}

	// Default conditions for common errors
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return true
	case errors.Is(err, context.Canceled):
		return false
	default:
		return true
	}
}

// RetryConfig holds configuration for retry operations
type RetryConfig struct {
	MaxAttempts     int
	BackoffStrategy BackoffStrategy
	RetryCondition  RetryCondition
	Timeout         time.Duration
	OnRetry         func(attempt int, err error, nextDelay time.Duration)
	OnFailure       func(err error, attempts int)
	OnSuccess       func(attempts int)
}

// DefaultRetryConfig returns a default retry configuration
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts:     3,
		BackoffStrategy: NewExponentialBackoff(100*time.Millisecond, 30*time.Second, 2.0, true),
		RetryCondition:  DefaultRetryCondition,
		Timeout:         0, // No timeout
		OnRetry:         func(attempt int, err error, nextDelay time.Duration) {},
		OnFailure:       func(err error, attempts int) {},
		OnSuccess:       func(attempts int) {},
	}
}

// Retryer manages retry operations
type Retryer struct {
	config RetryConfig
	mu     sync.RWMutex

	// Metrics
	totalAttempts   uint64
	totalSuccesses  uint64
	totalFailures   uint64
	totalRetries    uint64
	averageAttempts float64
	lastAttemptTime time.Time
	successRate     float64
}

// NewRetryer creates a new retryer with the given configuration
func NewRetryer(config RetryConfig) *Retryer {
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.BackoffStrategy == nil {
		config.BackoffStrategy = NewExponentialBackoff(100*time.Millisecond, 30*time.Second, 2.0, true)
	}
	if config.RetryCondition == nil {
		config.RetryCondition = DefaultRetryCondition
	}
	if config.OnRetry == nil {
		config.OnRetry = func(attempt int, err error, nextDelay time.Duration) {}
	}
	if config.OnFailure == nil {
		config.OnFailure = func(err error, attempts int) {}
	}
	if config.OnSuccess == nil {
		config.OnSuccess = func(attempts int) {}
	}

	return &Retryer{
		config: config,
	}
}

// Execute executes the given function with retry logic
func (r *Retryer) Execute(ctx context.Context, fn func() error) error {
	r.mu.Lock()
	r.totalAttempts++
	r.lastAttemptTime = time.Now()
	r.mu.Unlock()

	var lastError error
	var timeoutCtx context.Context
	var cancel context.CancelFunc

	// Create timeout context if specified
	if r.config.Timeout > 0 {
		timeoutCtx, cancel = context.WithTimeout(ctx, r.config.Timeout)
		defer cancel()
	} else {
		timeoutCtx = ctx
	}

	// Reset backoff strategy
	r.config.BackoffStrategy.Reset()

	for attempt := 0; attempt < r.config.MaxAttempts; attempt++ {
		// Check if context is done
		select {
		case <-timeoutCtx.Done():
			r.recordFailure(attempt + 1)
			r.config.OnFailure(timeoutCtx.Err(), attempt+1)
			return ErrRetryContextDone
		default:
		}

		// Execute the function
		err := fn()

		// Success case
		if err == nil {
			r.recordSuccess(attempt + 1)
			r.config.OnSuccess(attempt + 1)
			return nil
		}

		lastError = err

		// Check if error is retryable
		if !r.config.RetryCondition(err) {
			r.recordFailure(attempt + 1)
			r.config.OnFailure(err, attempt+1)
			return fmt.Errorf("%w: %v", ErrRetryNotRetryable, err)
		}

		// Don't wait after the last attempt
		if attempt == r.config.MaxAttempts-1 {
			break
		}

		// Calculate next delay
		delay := r.config.BackoffStrategy.NextDelay(attempt, err)

		// Call retry callback
		r.config.OnRetry(attempt+1, err, delay)

		// Wait for the delay or context cancellation
		select {
		case <-timeoutCtx.Done():
			r.recordFailure(attempt + 1)
			r.config.OnFailure(timeoutCtx.Err(), attempt+1)
			return ErrRetryContextDone
		case <-time.After(delay):
			r.recordRetry()
		}
	}

	// All attempts exhausted
	r.recordFailure(r.config.MaxAttempts)
	r.config.OnFailure(lastError, r.config.MaxAttempts)
	return fmt.Errorf("%w: %v", ErrRetryExhausted, lastError)
}

// ExecuteWithResult executes a function that returns a result and error
func (r *Retryer) ExecuteWithResult(ctx context.Context, fn func() (interface{}, error)) (interface{}, error) {
	var result interface{}

	err := r.Execute(ctx, func() error {
		var err error
		result, err = fn()
		return err
	})

	return result, err
}

// recordSuccess records a successful operation
func (r *Retryer) recordSuccess(attempts int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalSuccesses++
	r.updateAverageAttempts(attempts)
	r.updateSuccessRate()
}

// recordFailure records a failed operation
func (r *Retryer) recordFailure(attempts int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalFailures++
	r.updateAverageAttempts(attempts)
	r.updateSuccessRate()
}

// recordRetry records a retry attempt
func (r *Retryer) recordRetry() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalRetries++
}

// updateAverageAttempts updates the average attempts
func (r *Retryer) updateAverageAttempts(attempts int) {
	totalOps := r.totalSuccesses + r.totalFailures
	if totalOps > 0 {
		r.averageAttempts = (r.averageAttempts*float64(totalOps-1) + float64(attempts)) / float64(totalOps)
	}
}

// updateSuccessRate updates the success rate
func (r *Retryer) updateSuccessRate() {
	totalOps := r.totalSuccesses + r.totalFailures
	if totalOps > 0 {
		r.successRate = float64(r.totalSuccesses) / float64(totalOps)
	}
}

// RetryMetrics contains metrics about retry operations
type RetryMetrics struct {
	TotalAttempts   uint64
	TotalSuccesses  uint64
	TotalFailures   uint64
	TotalRetries    uint64
	AverageAttempts float64
	SuccessRate     float64
	LastAttemptTime time.Time
	MaxAttempts     int
	BackoffStrategy string
}

// Metrics returns metrics about the retryer
func (r *Retryer) Metrics() RetryMetrics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var backoffType string
	switch r.config.BackoffStrategy.(type) {
	case *ExponentialBackoff:
		backoffType = "exponential"
	case *LinearBackoff:
		backoffType = "linear"
	case *FixedBackoff:
		backoffType = "fixed"
	default:
		backoffType = "unknown"
	}

	return RetryMetrics{
		TotalAttempts:   r.totalAttempts,
		TotalSuccesses:  r.totalSuccesses,
		TotalFailures:   r.totalFailures,
		TotalRetries:    r.totalRetries,
		AverageAttempts: r.averageAttempts,
		SuccessRate:     r.successRate,
		LastAttemptTime: r.lastAttemptTime,
		MaxAttempts:     r.config.MaxAttempts,
		BackoffStrategy: backoffType,
	}
}

// Reset resets the retryer's metrics
func (r *Retryer) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.totalAttempts = 0
	r.totalSuccesses = 0
	r.totalFailures = 0
	r.totalRetries = 0
	r.averageAttempts = 0
	r.successRate = 0
	r.lastAttemptTime = time.Time{}
}

// UpdateConfig updates the retry configuration
func (r *Retryer) UpdateConfig(config RetryConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.config = config
}

// GetConfig returns the current retry configuration
func (r *Retryer) GetConfig() RetryConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.config
}

// RetryManager manages multiple retryers
type RetryManager struct {
	retryers map[string]*Retryer
	mu       sync.RWMutex
}

// NewRetryManager creates a new retry manager
func NewRetryManager() *RetryManager {
	return &RetryManager{
		retryers: make(map[string]*Retryer),
	}
}

// GetOrCreate gets an existing retryer or creates a new one
func (rm *RetryManager) GetOrCreate(name string, config RetryConfig) *Retryer {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if retryer, exists := rm.retryers[name]; exists {
		return retryer
	}

	retryer := NewRetryer(config)
	rm.retryers[name] = retryer
	return retryer
}

// Get gets an existing retryer
func (rm *RetryManager) Get(name string) (*Retryer, bool) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	retryer, exists := rm.retryers[name]
	return retryer, exists
}

// Remove removes a retryer
func (rm *RetryManager) Remove(name string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	delete(rm.retryers, name)
}

// List returns all retryer names
func (rm *RetryManager) List() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	names := make([]string, 0, len(rm.retryers))
	for name := range rm.retryers {
		names = append(names, name)
	}
	return names
}

// GetMetrics returns metrics for all retryers
func (rm *RetryManager) GetMetrics() map[string]RetryMetrics {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	metrics := make(map[string]RetryMetrics)
	for name, retryer := range rm.retryers {
		metrics[name] = retryer.Metrics()
	}
	return metrics
}

// Reset resets all retryers
func (rm *RetryManager) Reset() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, retryer := range rm.retryers {
		retryer.Reset()
	}
}

// Convenience functions for common retry scenarios

// WithExponentialBackoff creates a retryer with exponential backoff
func WithExponentialBackoff(maxAttempts int, baseDelay, maxDelay time.Duration) *Retryer {
	config := DefaultRetryConfig()
	config.MaxAttempts = maxAttempts
	config.BackoffStrategy = NewExponentialBackoff(baseDelay, maxDelay, 2.0, true)
	return NewRetryer(config)
}

// WithLinearBackoff creates a retryer with linear backoff
func WithLinearBackoff(maxAttempts int, baseDelay, increment, maxDelay time.Duration) *Retryer {
	config := DefaultRetryConfig()
	config.MaxAttempts = maxAttempts
	config.BackoffStrategy = NewLinearBackoff(baseDelay, maxDelay, increment)
	return NewRetryer(config)
}

// WithFixedBackoff creates a retryer with fixed backoff
func WithFixedBackoff(maxAttempts int, delay time.Duration) *Retryer {
	config := DefaultRetryConfig()
	config.MaxAttempts = maxAttempts
	config.BackoffStrategy = NewFixedBackoff(delay)
	return NewRetryer(config)
}

// Retry is a convenience function for simple retry operations
func Retry(ctx context.Context, maxAttempts int, fn func() error) error {
	retryer := WithExponentialBackoff(maxAttempts, 100*time.Millisecond, 30*time.Second)
	return retryer.Execute(ctx, fn)
}

// RetryWithBackoff is a convenience function for retry with custom backoff
func RetryWithBackoff(ctx context.Context, backoff BackoffStrategy, maxAttempts int, fn func() error) error {
	config := DefaultRetryConfig()
	config.MaxAttempts = maxAttempts
	config.BackoffStrategy = backoff
	retryer := NewRetryer(config)
	return retryer.Execute(ctx, fn)
}
