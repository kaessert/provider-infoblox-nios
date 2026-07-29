// Package dualclient wraps two infoblox-go-client ObjectManager instances —
// a primary (always used for writes) and an optional read-only candidate
// (typically a Grid Master Candidate) used to offload Observe traffic. It
// implements the routing half of the read/write endpoint split: Create,
// Update, and Delete always go to the primary; Observe goes to the
// candidate only when the caller (the SOA-serial convergence gate in the
// sibling convergence package, or a resource-type override such as IPAM)
// decides the candidate is safe to read from.
//
// When no candidate is configured, Client is a pure passthrough to the
// primary — every method behaves exactly like a direct
// ibclient.IBObjectManager, so resources that never opt into a read
// endpoint see no behavior change at all.
package dualclient

import (
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
)

// Client wraps a primary WAPI ObjectManager and an optional read-only
// candidate ObjectManager.
type Client struct {
	primary   ibclient.IBObjectManager
	candidate ibclient.IBObjectManager
	breaker   *CircuitBreaker
}

// New returns a passthrough Client backed only by the primary endpoint —
// identical to today's single-endpoint provider behavior. Use this when
// the ProviderConfig has no readEndpoint configured.
func New(primary ibclient.IBObjectManager) *Client {
	return &Client{primary: primary}
}

// WithCandidate returns a Client that also has a read-only candidate
// endpoint. breaker tracks consecutive candidate read failures across
// calls to Reader and may be nil (in which case the circuit never opens).
func WithCandidate(primary, candidate ibclient.IBObjectManager, breaker *CircuitBreaker) *Client {
	return &Client{primary: primary, candidate: candidate, breaker: breaker}
}

// HasCandidate reports whether a read-only candidate endpoint is
// configured. Resources that have no serial-based (or other) convergence
// signal — IPAM objects — should still call Reader(false) even when this
// is true, since HasCandidate only reports configuration, not routing
// eligibility.
func (c *Client) HasCandidate() bool {
	return c.candidate != nil
}

// Breaker returns the circuit breaker tracking candidate read health, or
// nil if none was configured (passthrough clients, or candidates created
// without one).
func (c *Client) Breaker() *CircuitBreaker {
	return c.breaker
}

// Writer always returns the primary ObjectManager. Create, Update, and
// Delete must never touch the candidate — the ADR's write routing table
// pins all mutations to the primary unconditionally.
func (c *Client) Writer() ibclient.IBObjectManager {
	return c.primary
}

// Reader returns the ObjectManager Observe should use. useCandidate is
// the routing decision computed by the caller (the convergence gate's
// RouteDecision.UseCandidate, or false for IPAM/primary-only resources).
// Reader falls back to the primary whenever:
//   - useCandidate is false (gate says not caught up, or resource is
//     pinned to primary), or
//   - no candidate endpoint is configured at all (passthrough), or
//   - the circuit breaker is currently open (sustained candidate
//     failures).
func (c *Client) Reader(useCandidate bool) ibclient.IBObjectManager {
	if !useCandidate || c.candidate == nil {
		return c.primary
	}
	if c.breaker != nil && c.breaker.IsOpen() {
		return c.primary
	}
	return c.candidate
}
