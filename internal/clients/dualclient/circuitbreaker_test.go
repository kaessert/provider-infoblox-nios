package dualclient

import (
	"testing"
	"time"
)

// fakeClock is an injectable, manually-advanced clock for deterministic
// circuit-breaker timing tests (no real sleeps).
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newBreakerWithClock(threshold int, backoff time.Duration) (*CircuitBreaker, *fakeClock) {
	b := NewCircuitBreaker(threshold, backoff)
	clock := &fakeClock{t: time.Unix(0, 0)}
	b.now = clock.now
	return b, clock
}

func TestCircuitBreakerDefaults(t *testing.T) {
	b := NewCircuitBreaker(0, 0)
	if b.threshold != DefaultCircuitBreakerThreshold {
		t.Fatalf("threshold = %d, want default %d", b.threshold, DefaultCircuitBreakerThreshold)
	}
	if b.backoff != DefaultCircuitBreakerBackoff {
		t.Fatalf("backoff = %v, want default %v", b.backoff, DefaultCircuitBreakerBackoff)
	}
}

func TestCircuitBreakerClosedUntilThreshold(t *testing.T) {
	b, _ := newBreakerWithClock(3, time.Minute)

	for i := 0; i < 2; i++ {
		if opened := b.RecordFailure(); opened {
			t.Fatalf("RecordFailure #%d unexpectedly opened the circuit before threshold", i+1)
		}
		if b.IsOpen() {
			t.Fatalf("circuit reported open before reaching threshold (failure #%d)", i+1)
		}
	}
}

func TestCircuitBreakerOpensAtThreshold(t *testing.T) {
	b, _ := newBreakerWithClock(5, time.Minute)

	var opened bool
	for i := 0; i < 5; i++ {
		opened = b.RecordFailure()
	}
	if !opened {
		t.Fatal("RecordFailure at the threshold count must report opened == true")
	}
	if !b.IsOpen() {
		t.Fatal("IsOpen() must be true immediately after tripping")
	}
	if got := b.FailureCount(); got != 5 {
		t.Fatalf("FailureCount() = %d, want 5", got)
	}
}

func TestCircuitBreakerHalfOpensAfterBackoff(t *testing.T) {
	b, clock := newBreakerWithClock(1, 10*time.Second)
	b.RecordFailure() // opens immediately at threshold 1

	if !b.IsOpen() {
		t.Fatal("circuit must be open immediately after tripping")
	}

	clock.advance(11 * time.Second)
	if b.IsOpen() {
		t.Fatal("circuit must half-open (report closed) once the backoff window elapses")
	}
}

func TestCircuitBreakerClosesOnSuccessAfterHalfOpen(t *testing.T) {
	b, clock := newBreakerWithClock(1, 10*time.Second)
	b.RecordFailure()
	clock.advance(11 * time.Second)

	if b.IsOpen() {
		t.Fatal("expected half-open (closed) trial window")
	}
	b.RecordSuccess()

	if b.IsOpen() {
		t.Fatal("circuit must remain closed after a successful trial read")
	}
	if got := b.FailureCount(); got != 0 {
		t.Fatalf("FailureCount() after RecordSuccess = %d, want 0", got)
	}
}

func TestCircuitBreakerReopensOnFailureAfterHalfOpen(t *testing.T) {
	b, clock := newBreakerWithClock(1, 10*time.Second)
	b.RecordFailure()
	clock.advance(11 * time.Second)

	if b.IsOpen() {
		t.Fatal("expected half-open (closed) trial window")
	}
	b.RecordFailure() // trial read fails again

	if !b.IsOpen() {
		t.Fatal("a failed trial read during half-open must re-open the circuit")
	}
}
