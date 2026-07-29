// Package dualclient unit tests. Since Client only routes between two
// pre-built ibclient.IBObjectManager instances (it never itself calls SDK
// methods), these tests use minimal fake IBConnector-backed
// ObjectManagers distinguishable by pointer identity — routing
// correctness is verified by which of the two comes back from
// Writer/Reader, not by exercising real WAPI calls.
package dualclient

import (
	"testing"
	"time"

	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
)

// fakeConnector is a minimal no-op IBConnector — enough to construct a
// distinguishable ibclient.IBObjectManager via ibclient.NewObjectManager
// without any network I/O.
type fakeConnector struct{}

func (fakeConnector) CreateObject(ibclient.IBObject) (string, error) { return "", nil }
func (fakeConnector) GetObject(ibclient.IBObject, string, *ibclient.QueryParams, interface{}) error {
	return nil
}
func (fakeConnector) DeleteObject(string) (string, error)                    { return "", nil }
func (fakeConnector) UpdateObject(ibclient.IBObject, string) (string, error) { return "", nil }

func newFakeObjMgr() ibclient.IBObjectManager {
	return ibclient.NewObjectManager(fakeConnector{}, "", "")
}

func TestNewIsPassthrough(t *testing.T) {
	primary := newFakeObjMgr()
	c := New(primary)

	if c.HasCandidate() {
		t.Fatal("passthrough client must report HasCandidate() == false")
	}
	if c.Writer() != primary {
		t.Fatal("Writer() must return the primary")
	}
	if c.Reader(true) != primary {
		t.Fatal("Reader(true) on a passthrough client must fall back to primary")
	}
	if c.Reader(false) != primary {
		t.Fatal("Reader(false) must return primary")
	}
}

func TestWithCandidateRoutesWritesToPrimary(t *testing.T) {
	primary := newFakeObjMgr()
	candidate := newFakeObjMgr()
	c := WithCandidate(primary, candidate, nil)

	if !c.HasCandidate() {
		t.Fatal("HasCandidate() must be true once a candidate is configured")
	}
	if c.Writer() != primary {
		t.Fatal("Writer() must always return primary, even with a candidate configured")
	}
}

func TestReaderRoutesToCandidateWhenGateClear(t *testing.T) {
	primary := newFakeObjMgr()
	candidate := newFakeObjMgr()
	c := WithCandidate(primary, candidate, nil)

	if got := c.Reader(true); got != candidate {
		t.Fatalf("Reader(true) = %v, want candidate", got)
	}
}

func TestReaderFallsBackToPrimaryWhenGateNotClear(t *testing.T) {
	primary := newFakeObjMgr()
	candidate := newFakeObjMgr()
	c := WithCandidate(primary, candidate, nil)

	if got := c.Reader(false); got != primary {
		t.Fatalf("Reader(false) = %v, want primary", got)
	}
}

func TestReaderFallsBackToPrimaryWhenCircuitOpen(t *testing.T) {
	primary := newFakeObjMgr()
	candidate := newFakeObjMgr()
	breaker := NewCircuitBreaker(1, time.Minute)
	breaker.RecordFailure() // trips open at threshold 1

	c := WithCandidate(primary, candidate, breaker)

	if got := c.Reader(true); got != primary {
		t.Fatalf("Reader(true) with an open circuit = %v, want primary", got)
	}
}

func TestBreakerAccessor(t *testing.T) {
	breaker := NewCircuitBreaker(5, time.Minute)
	c := WithCandidate(newFakeObjMgr(), newFakeObjMgr(), breaker)
	if c.Breaker() != breaker {
		t.Fatal("Breaker() must return the configured breaker instance")
	}
	if New(newFakeObjMgr()).Breaker() != nil {
		t.Fatal("passthrough client must have a nil Breaker()")
	}
}
