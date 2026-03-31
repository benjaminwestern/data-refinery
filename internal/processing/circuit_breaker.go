package processing

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/safety"
)

var (
	errCircuitBreakerOpen    = errors.New("circuit breaker is open")
	errTooManyRequests       = errors.New("too many requests")
	errHalfOpenLimitExceeded = errors.New("half-open state limit exceeded")
)

// CircuitBreakerState represents the state of the circuit breaker.
type CircuitBreakerState int

const (
	stateClosed CircuitBreakerState = iota
	stateOpen
	stateHalfOpen
)

func (s CircuitBreakerState) String() string {
	switch s {
	case stateClosed:
		return "CLOSED"
	case stateOpen:
		return "OPEN"
	case stateHalfOpen:
		return "HALF_OPEN"
	default:
		return unknownLabel
	}
}

// CircuitBreakerConfig holds configuration for the circuit breaker.
type CircuitBreakerConfig struct {
	MaxRequests           uint32        // Maximum requests allowed when half-open
	Interval              time.Duration // Statistical window for closed state
	Timeout               time.Duration // Timeout for open state
	ReadyToTrip           func(counts Counts) bool
	OnStateChange         func(name string, from, to CircuitBreakerState)
	IsSuccessful          func(err error) bool
	FallbackFunc          func(ctx context.Context, err error) error
	MaxConcurrent         int           // Maximum concurrent requests
	SlowCallDuration      time.Duration // Duration after which a call is considered slow
	SlowCallRateThreshold float64       // Threshold for slow call rate
}

// DefaultCircuitBreakerConfig returns a default configuration.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:           10,
		Interval:              60 * time.Second,
		Timeout:               30 * time.Second,
		ReadyToTrip:           DefaultReadyToTrip,
		OnStateChange:         func(_ string, _, _ CircuitBreakerState) {},
		IsSuccessful:          func(err error) bool { return err == nil },
		FallbackFunc:          func(_ context.Context, err error) error { return err },
		MaxConcurrent:         100,
		SlowCallDuration:      10 * time.Second,
		SlowCallRateThreshold: 0.5,
	}
}

// DefaultReadyToTrip is the default function to determine if the circuit should trip.
func DefaultReadyToTrip(counts Counts) bool {
	return counts.Requests >= 5 && counts.FailureRatio() >= 0.6
}

// Counts holds the statistics for the circuit breaker.
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
	SlowCalls            uint32
}

// SuccessRatio returns the success ratio.
func (c Counts) SuccessRatio() float64 {
	if c.Requests == 0 {
		return 0.0
	}
	return float64(c.TotalSuccesses) / float64(c.Requests)
}

// FailureRatio returns the failure ratio.
func (c Counts) FailureRatio() float64 {
	if c.Requests == 0 {
		return 0.0
	}
	return float64(c.TotalFailures) / float64(c.Requests)
}

// SlowCallRatio returns the slow call ratio.
func (c Counts) SlowCallRatio() float64 {
	if c.Requests == 0 {
		return 0.0
	}
	return float64(c.SlowCalls) / float64(c.Requests)
}

// Reset resets the counts.
func (c *Counts) Reset() {
	c.Requests = 0
	c.TotalSuccesses = 0
	c.TotalFailures = 0
	c.ConsecutiveSuccesses = 0
	c.ConsecutiveFailures = 0
	c.SlowCalls = 0
}

// CircuitBreaker represents a circuit breaker.
type CircuitBreaker struct {
	name       string
	config     CircuitBreakerConfig
	state      CircuitBreakerState
	generation uint64
	counts     Counts
	expiry     time.Time
	mu         sync.RWMutex

	// Concurrency control
	activeRequests int32
	maxConcurrent  int32

	// Metrics
	stateTransitions map[CircuitBreakerState]uint64
	lastStateChange  time.Time
	totalRequests    uint64
	totalSuccesses   uint64
	totalFailures    uint64
	totalTimeouts    uint64
	totalSlowCalls   uint64
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(name string, config CircuitBreakerConfig) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:             name,
		config:           config,
		state:            stateClosed,
		generation:       0,
		expiry:           time.Now().Add(config.Interval),
		maxConcurrent:    safety.SaturatingInt32(config.MaxConcurrent),
		stateTransitions: make(map[CircuitBreakerState]uint64),
		lastStateChange:  time.Now(),
	}

	// Initialize state transition counters
	cb.stateTransitions[stateClosed] = 0
	cb.stateTransitions[stateOpen] = 0
	cb.stateTransitions[stateHalfOpen] = 0

	return cb
}

// Execute executes the given function if the circuit breaker allows it.
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	// Check if we can execute
	generation, err := cb.beforeRequest()
	if err != nil {
		// Try fallback if available
		if cb.config.FallbackFunc != nil {
			return cb.config.FallbackFunc(ctx, err)
		}
		return err
	}

	// Track execution time for slow call detection
	startTime := time.Now()

	// Execute the function with timeout
	var fnErr error
	done := make(chan struct{})

	go func() {
		defer close(done)
		fnErr = fn()
	}()

	select {
	case <-done:
		// Function completed
		duration := time.Since(startTime)
		cb.afterRequest(generation, cb.config.IsSuccessful(fnErr), duration)
		return fnErr
	case <-ctx.Done():
		// Context cancelled
		cb.afterRequest(generation, false, time.Since(startTime))
		return fmt.Errorf("circuit breaker execution cancelled: %w", ctx.Err())
	}
}

// beforeRequest checks if a request can be executed.
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	// Check concurrency limit
	if cb.activeRequests >= cb.maxConcurrent {
		return generation, errTooManyRequests
	}

	switch state {
	case stateClosed:
		cb.activeRequests++
		return generation, nil
	case stateOpen:
		return generation, errCircuitBreakerOpen
	case stateHalfOpen:
		if cb.counts.Requests >= cb.config.MaxRequests {
			return generation, errHalfOpenLimitExceeded
		}
		cb.activeRequests++
		return generation, nil
	default:
		return generation, errors.New("unknown circuit breaker state")
	}
}

// afterRequest updates the circuit breaker state after a request.
func (cb *CircuitBreaker) afterRequest(generation uint64, success bool, duration time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Decrease active requests
	cb.activeRequests--

	// Check if the generation is still current
	if generation != cb.generation {
		return
	}

	// Determine if this was a slow call
	isSlow := duration > cb.config.SlowCallDuration

	// Update counts
	cb.counts.Requests++
	cb.totalRequests++

	if success {
		cb.counts.TotalSuccesses++
		cb.counts.ConsecutiveSuccesses++
		cb.counts.ConsecutiveFailures = 0
		cb.totalSuccesses++
	} else {
		cb.counts.TotalFailures++
		cb.counts.ConsecutiveFailures++
		cb.counts.ConsecutiveSuccesses = 0
		cb.totalFailures++
	}

	if isSlow {
		cb.counts.SlowCalls++
		cb.totalSlowCalls++
	}

	// Update state based on current state and results
	switch cb.state {
	case stateClosed:
		if cb.config.ReadyToTrip(cb.counts) || cb.shouldTripOnSlowCalls() {
			cb.setState(stateOpen, time.Now())
		}
	case stateOpen:
		// Transitions out of open state are handled when the timeout expires.
	case stateHalfOpen:
		if success {
			cb.setState(stateClosed, time.Now())
		} else {
			cb.setState(stateOpen, time.Now())
		}
	}
}

// shouldTripOnSlowCalls checks if the circuit should trip based on slow call rate.
func (cb *CircuitBreaker) shouldTripOnSlowCalls() bool {
	if cb.config.SlowCallRateThreshold <= 0 {
		return false
	}

	if cb.counts.Requests < 5 {
		return false
	}

	return cb.counts.SlowCallRatio() >= cb.config.SlowCallRateThreshold
}

// currentState returns the current state and generation.
func (cb *CircuitBreaker) currentState(now time.Time) (CircuitBreakerState, uint64) {
	switch cb.state {
	case stateClosed:
		if cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case stateOpen:
		if cb.expiry.Before(now) {
			cb.setState(stateHalfOpen, now)
		}
	case stateHalfOpen:
		// Half-open state progresses based on request outcomes.
	}
	return cb.state, cb.generation
}

// setState changes the state of the circuit breaker.
func (cb *CircuitBreaker) setState(state CircuitBreakerState, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state
	cb.stateTransitions[state]++
	cb.lastStateChange = now
	cb.toNewGeneration(now)

	// Call state change callback
	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(cb.name, prev, state)
	}
}

// toNewGeneration creates a new generation.
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts.Reset()

	var zero time.Time
	switch cb.state {
	case stateClosed:
		if cb.config.Interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.config.Interval)
		}
	case stateOpen:
		cb.expiry = now.Add(cb.config.Timeout)
	case stateHalfOpen:
		cb.expiry = zero
	}
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	state, _ := cb.currentState(time.Now())
	return state
}

// Counts returns the current counts.
func (cb *CircuitBreaker) Counts() Counts {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.counts
}

// Metrics returns comprehensive metrics about the circuit breaker.
func (cb *CircuitBreaker) Metrics() CircuitBreakerMetrics {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return CircuitBreakerMetrics{
		Name:                cb.name,
		State:               cb.state,
		Generation:          cb.generation,
		Counts:              cb.counts,
		StateTransitions:    cb.stateTransitions,
		LastStateChange:     cb.lastStateChange,
		TotalRequests:       cb.totalRequests,
		TotalSuccesses:      cb.totalSuccesses,
		TotalFailures:       cb.totalFailures,
		TotalTimeouts:       cb.totalTimeouts,
		TotalSlowCalls:      cb.totalSlowCalls,
		ActiveRequests:      cb.activeRequests,
		MaxConcurrent:       cb.maxConcurrent,
		UptimePercentage:    cb.calculateUptimePercentage(),
		AverageResponseTime: cb.calculateAverageResponseTime(),
	}
}

// CircuitBreakerMetrics contains detailed metrics about the circuit breaker.
type CircuitBreakerMetrics struct {
	Name                string
	State               CircuitBreakerState
	Generation          uint64
	Counts              Counts
	StateTransitions    map[CircuitBreakerState]uint64
	LastStateChange     time.Time
	TotalRequests       uint64
	TotalSuccesses      uint64
	TotalFailures       uint64
	TotalTimeouts       uint64
	TotalSlowCalls      uint64
	ActiveRequests      int32
	MaxConcurrent       int32
	UptimePercentage    float64
	AverageResponseTime time.Duration
}

// calculateUptimePercentage calculates the uptime percentage.
func (cb *CircuitBreaker) calculateUptimePercentage() float64 {
	if cb.totalRequests == 0 {
		return 100.0
	}
	return float64(cb.totalSuccesses) / float64(cb.totalRequests) * 100.0
}

// calculateAverageResponseTime calculates the average response time.
func (cb *CircuitBreaker) calculateAverageResponseTime() time.Duration {
	// This is a simplified calculation
	// In a real implementation, you'd track actual response times
	if cb.totalSlowCalls > 0 {
		return cb.config.SlowCallDuration / 2
	}
	return time.Millisecond * 100
}

// Reset resets the circuit breaker to its initial state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(stateClosed, time.Now())
	cb.totalRequests = 0
	cb.totalSuccesses = 0
	cb.totalFailures = 0
	cb.totalTimeouts = 0
	cb.totalSlowCalls = 0
	cb.activeRequests = 0

	// Reset state transition counters
	for state := range cb.stateTransitions {
		cb.stateTransitions[state] = 0
	}
}

// IsReady returns true if the circuit breaker is ready to handle requests.
func (cb *CircuitBreaker) IsReady() bool {
	state := cb.State()
	return state == stateClosed || state == stateHalfOpen
}

// CircuitBreakerManager manages multiple circuit breakers.
type CircuitBreakerManager struct {
	breakers map[string]*CircuitBreaker
	mu       sync.RWMutex
}

// NewCircuitBreakerManager creates a new circuit breaker manager.
func NewCircuitBreakerManager() *CircuitBreakerManager {
	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
	}
}

// GetOrCreate gets an existing circuit breaker or creates a new one.
func (cbm *CircuitBreakerManager) GetOrCreate(name string, config CircuitBreakerConfig) *CircuitBreaker {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	if cb, exists := cbm.breakers[name]; exists {
		return cb
	}

	cb := NewCircuitBreaker(name, config)
	cbm.breakers[name] = cb
	return cb
}

// Get gets an existing circuit breaker.
func (cbm *CircuitBreakerManager) Get(name string) (*CircuitBreaker, bool) {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	cb, exists := cbm.breakers[name]
	return cb, exists
}

// Remove removes a circuit breaker.
func (cbm *CircuitBreakerManager) Remove(name string) {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	delete(cbm.breakers, name)
}

// List returns all circuit breaker names.
func (cbm *CircuitBreakerManager) List() []string {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	names := make([]string, 0, len(cbm.breakers))
	for name := range cbm.breakers {
		names = append(names, name)
	}
	return names
}

// GetMetrics returns metrics for all circuit breakers.
func (cbm *CircuitBreakerManager) GetMetrics() map[string]CircuitBreakerMetrics {
	cbm.mu.RLock()
	defer cbm.mu.RUnlock()

	metrics := make(map[string]CircuitBreakerMetrics)
	for name, cb := range cbm.breakers {
		metrics[name] = cb.Metrics()
	}
	return metrics
}

// Reset resets all circuit breakers.
func (cbm *CircuitBreakerManager) Reset() {
	cbm.mu.Lock()
	defer cbm.mu.Unlock()

	for _, cb := range cbm.breakers {
		cb.Reset()
	}
}
