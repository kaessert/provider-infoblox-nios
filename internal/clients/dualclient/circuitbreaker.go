package dualclient

import (
	"sync"
	"time"
)

// DefaultCircuitBreakerThreshold is the number of consecutive candidate
// read failures that trips the circuit open (default 5).
const DefaultCircuitBreakerThreshold = 5

// DefaultCircuitBreakerBackoff is how long the circuit stays open — all
// reads pinned to the primary — after tripping (default 60s).
const DefaultCircuitBreakerBackoff = 60 * time.Second

// CircuitBreaker tracks consecutive candidate read failures for one
// read-endpoint connection and decides when to stop trying the candidate
// (open), when to allow a single trial read through again (half-open),
// and when to resume normal candidate routing (closed).
//
// A CircuitBreaker instance is meant to be created once per controller
// connection and reused across reconciles — a breaker recreated on every
// Connect() call never accumulates enough failures to trip, defeating its
// purpose. It is safe for concurrent use.
type CircuitBreaker struct {
	mu sync.Mutex

	threshold int
	backoff   time.Duration

	consecutiveFailures int
	openUntil           time.Time

	// now is overridable in tests; defaults to time.Now.
	now func() time.Time
}

// NewCircuitBreaker constructs a CircuitBreaker. A threshold <= 0 uses
// DefaultCircuitBreakerThreshold; a backoff <= 0 uses
// DefaultCircuitBreakerBackoff.
func NewCircuitBreaker(threshold int, backoff time.Duration) *CircuitBreaker {
	if threshold <= 0 {
		threshold = DefaultCircuitBreakerThreshold
	}
	if backoff <= 0 {
		backoff = DefaultCircuitBreakerBackoff
	}
	return &CircuitBreaker{threshold: threshold, backoff: backoff, now: time.Now}
}

// RecordSuccess closes the circuit and resets the consecutive-failure
// count. Call this after a candidate read succeeds — including the trial
// read allowed through while half-open.
func (b *CircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFailures = 0
	b.openUntil = time.Time{}
}

// RecordFailure increments the consecutive-failure count and, once it
// reaches the threshold, opens the circuit for the configured backoff
// window. Returns true if this call is what tripped the circuit open.
func (b *CircuitBreaker) RecordFailure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFailures++
	if b.consecutiveFailures >= b.threshold {
		b.openUntil = b.now().Add(b.backoff)
		return true
	}
	return false
}

// IsOpen reports whether candidate reads are currently suppressed. Once
// the backoff window elapses, IsOpen half-opens the circuit: it returns
// false (allowing a trial candidate read) but leaves the
// consecutive-failure count untouched until that trial calls
// RecordSuccess (closing the circuit) or RecordFailure (which — since the
// count is already at/above threshold — re-opens it for a fresh backoff
// window).
func (b *CircuitBreaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openUntil.IsZero() {
		return false
	}
	if b.now().After(b.openUntil) {
		// Half-open: clear openUntil so this call (and this call only)
		// reports closed, letting one trial read through. If that read
		// fails, RecordFailure sees consecutiveFailures already at
		// threshold and re-opens immediately with a fresh window.
		b.openUntil = time.Time{}
		return false
	}
	return true
}

// FailureCount returns the current consecutive-failure count, for
// building descriptive ReadRouting condition messages
// ("CandidateDegraded: 7 consecutive failures").
func (b *CircuitBreaker) FailureCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutiveFailures
}
