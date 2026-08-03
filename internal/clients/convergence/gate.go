package convergence

import (
	"context"
	"fmt"
	"time"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/dualclient"
)

// Convergence modes, mirrored from the ProviderConfig
// readEndpoint.convergence.mode enum.
const (
	ModeSOASerial   = "soaSerial"
	ModePrimaryOnly = "primaryOnly"
)

// ReadRouting condition reasons. Status is True only for
// ReasonCandidateReady; every other reason routes to primary and sets
// Status False.
const (
	ReasonCandidateReady        = "CandidateReady"
	ReasonWaitingForReplication = "WaitingForReplication"
	ReasonConvergenceTimeout    = "ConvergenceTimeout"
	ReasonCandidateDegraded     = "CandidateDegraded"
	ReasonPrimaryOnly           = "PrimaryOnly"
)

// ReadRoutingConditionType is the status condition type every resource
// using a read endpoint reports.
const ReadRoutingConditionType xpv2.ConditionType = "ReadRouting"

// DefaultTimeout and DefaultPollInterval mirror the ConvergenceConfig CRD
// defaults (kubebuilder:default) so callers that build a Gate without a
// ProviderConfig on hand (e.g. tests) get the same behavior.
const (
	DefaultTimeout      = 60 * time.Second
	DefaultPollInterval = 2 * time.Second
)

// RouteDecision is the convergence gate's answer to "should this Observe
// read from the candidate?" plus the human-readable detail needed to set
// the ReadRouting condition and, for fallback decisions, emit a
// Kubernetes Warning event — fallbacks must never silently degrade.
type RouteDecision struct {
	// UseCandidate is the routing decision: true routes Observe to the
	// candidate, false routes it to the primary.
	UseCandidate bool
	// Reason is one of the Reason* constants above.
	Reason string
	// Message is a human-readable detail for the ReadRouting condition
	// and, when Warning is true, the Kubernetes event message.
	Message string
	// Warning is true when callers should emit a Kubernetes Warning event
	// alongside the condition — set for guard-triggered fallbacks,
	// candidate errors, circuit-breaker trips, and convergence timeouts.
	// It is false for routine, expected states (steady-state candidate
	// routing, still-waiting-for-replication shortly after a write,
	// configured-primary-only resources) that are not failures.
	Warning bool
}

// ReadRoutingCondition converts a RouteDecision into the ReadRouting
// status condition every managed resource using a read endpoint must
// report.
func ReadRoutingCondition(d RouteDecision) xpv2.Condition {
	status := corev1.ConditionFalse
	if d.UseCandidate {
		status = corev1.ConditionTrue
	}
	return xpv2.Condition{
		Type:               ReadRoutingConditionType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             xpv2.ConditionReason(d.Reason),
		Message:            d.Message,
	}
}

// EffectiveMode applies the mandatory IPAM override: IPAM objects have no
// SOA-serial (or any other) convergence signal, so they always use
// primaryOnly regardless of the configured mode. overridden is true only
// when the configured mode was soaSerial and had to be overridden —
// callers use it to decide whether to log an info-level message noting
// that soaSerial was configured for an IPAM resource and overridden.
func EffectiveMode(configuredMode string, isIPAM bool) (mode string, overridden bool) {
	if !isIPAM {
		return configuredMode, false
	}
	return ModePrimaryOnly, configuredMode == ModeSOASerial
}

// Gate implements the SOA-serial convergence check. Primary reads the
// just-written serial right after a DNS mutation; Candidate reads
// member_soa_serials to check whether the candidate has caught up.
// Candidate is nil when no readEndpoint is configured, or Reader always
// returns PrimaryOnly decisions without ever attempting a candidate call —
// see Evaluate.
type Gate struct {
	Primary   *Client
	Candidate *Client
	Breaker   *dualclient.CircuitBreaker

	// Hostname is the candidate grid member's identifier as it appears in
	// member_soa_serials[].grid_primary. In the common single-candidate
	// deployment this is the candidate readEndpoint's own host.
	Hostname string

	// clock is overridable in tests.
	clock func() time.Time
}

// NewGate constructs a Gate. primary and candidate are raw WAPI clients
// pointed at the primary and candidate endpoints respectively; candidate
// may be nil when no readEndpoint is configured (Evaluate then always
// returns a PrimaryOnly decision without making any HTTP call).
func NewGate(primary, candidate *Client, breaker *dualclient.CircuitBreaker, candidateHostname string) *Gate {
	return &Gate{Primary: primary, Candidate: candidate, Breaker: breaker, Hostname: candidateHostname, clock: time.Now}
}

// RecordWrite reads the zone's current SOA serial from the primary right
// after a DNS write and stores it as the pending-convergence watermark on
// obj. If the zone has no grid_primary assigned or doesn't exist
// (found=false), there is no serial to gate on — this is not an error,
// RecordWrite simply does not set an annotation, so subsequent Observes
// route to primary/candidate purely per EffectiveMode/configuration with
// no pending gate.
func (g *Gate) RecordWrite(ctx context.Context, obj metav1.Object, fqdn, view string) error {
	if g.Primary == nil {
		return nil
	}
	serial, found, err := g.Primary.ReadZoneSerial(ctx, fqdn, view)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return SetPendingSerial(obj, fqdn, view, serial, g.now())
}

// Evaluate decides whether obj's next Observe should read from the
// candidate. mode should already have EffectiveMode's IPAM override
// applied by the caller. It delegates to a small chain of helpers, each
// covering one guard scenario, so no single function carries the whole
// decision tree's branching.
func (g *Gate) Evaluate(ctx context.Context, obj metav1.Object, fqdn, view string, mode string, timeout time.Duration) RouteDecision {
	if d, done := g.evaluateStructural(mode); done {
		return d
	}

	pending, d, done := g.evaluatePending(obj)
	if done {
		return d
	}

	if d, done := g.evaluateTimeout(obj, pending, timeout); done {
		return d
	}

	return g.evaluateCandidateSerial(ctx, obj, fqdn, view, pending)
}

// evaluateStructural handles the decisions that never require a candidate
// HTTP call: a resource pinned to primaryOnly, no read endpoint
// configured at all, and a currently-open circuit breaker.
func (g *Gate) evaluateStructural(mode string) (RouteDecision, bool) {
	if mode == ModePrimaryOnly {
		return RouteDecision{UseCandidate: false, Reason: ReasonPrimaryOnly, Message: "convergence mode is primaryOnly for this resource"}, true
	}
	if g.Candidate == nil {
		return RouteDecision{UseCandidate: false, Reason: ReasonPrimaryOnly, Message: "no read endpoint configured"}, true
	}
	if g.Breaker != nil && g.Breaker.IsOpen() {
		return RouteDecision{
			UseCandidate: false,
			Reason:       ReasonCandidateDegraded,
			Message:      "candidate read circuit breaker is open after repeated failures; routing to primary",
			Warning:      true,
		}, true
	}
	return RouteDecision{}, false
}

// evaluatePending reads obj's pending-zone-serial annotation. done is true
// when the caller should return the accompanying RouteDecision directly:
// either the annotation is corrupt (fail safe to primary) or absent
// (steady state — safe to read from the candidate).
func (g *Gate) evaluatePending(obj metav1.Object) (pending *PendingZoneSerial, decision RouteDecision, done bool) {
	pending, ok, err := GetPendingSerial(obj)
	if err != nil {
		// A corrupt annotation must never be trusted as "caught up" — fail
		// safe to primary rather than serving a possibly-stale candidate
		// read.
		return nil, RouteDecision{
			UseCandidate: false,
			Reason:       ReasonWaitingForReplication,
			Message:      "pending-zone-serial annotation is unreadable; routing to primary until the next write resets it: " + err.Error(),
			Warning:      true,
		}, true
	}
	if !ok {
		// Steady state: no write is pending convergence, so it is safe to
		// read from the candidate.
		return nil, RouteDecision{UseCandidate: true, Reason: ReasonCandidateReady, Message: "no pending write; reading from candidate"}, true
	}
	return pending, RouteDecision{}, false
}

// evaluateTimeout gives up waiting for a pending write to converge once
// convergence.timeout has elapsed since it was recorded, clearing the
// watermark so the resource resumes normal candidate routing on its next
// write rather than staying pinned to primary forever.
func (g *Gate) evaluateTimeout(obj metav1.Object, pending *PendingZoneSerial, timeout time.Duration) (RouteDecision, bool) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if elapsed := g.now().Sub(pending.Since); elapsed > timeout {
		ClearPendingSerial(obj)
		return RouteDecision{
			UseCandidate: false,
			Reason:       ReasonConvergenceTimeout,
			Message:      fmt.Sprintf("candidate did not reach serial %d for zone %s within %s; falling back to primary", pending.Serial, pending.Zone, timeout),
			Warning:      true,
		}, true
	}
	return RouteDecision{}, false
}

// evaluateCandidateSerial reads the candidate's member_soa_serials and
// compares the entry for g.Hostname against pending.Serial.
func (g *Gate) evaluateCandidateSerial(ctx context.Context, obj metav1.Object, fqdn, view string, pending *PendingZoneSerial) RouteDecision {
	members, err := g.Candidate.ReadMemberSerials(ctx, fqdn, view)
	if err != nil {
		if g.Breaker != nil {
			g.Breaker.RecordFailure()
		}
		return RouteDecision{
			UseCandidate: false,
			Reason:       ReasonCandidateDegraded,
			Message:      "candidate WAPI is unreachable or returned an error; routing to primary: " + err.Error(),
			Warning:      true,
		}
	}
	if len(members) == 0 {
		// Zone not found, or found but has no grid_primary assigned —
		// either way there is no serial signal to compare against.
		return RouteDecision{
			UseCandidate: false,
			Reason:       ReasonPrimaryOnly,
			Message:      "zone " + ZoneKey(fqdn, view) + " is not served by grid DNS (not found, or no grid_primary assigned); routing to primary",
			Warning:      true,
		}
	}

	for _, m := range members {
		if m.GridPrimary == g.Hostname {
			return g.decideFromMemberSerial(obj, pending, m)
		}
	}

	// The candidate's own hostname is absent from member_soa_serials —
	// treat this the same as "not caught up".
	if g.Breaker != nil {
		g.Breaker.RecordSuccess()
	}
	return RouteDecision{
		UseCandidate: false,
		Reason:       ReasonWaitingForReplication,
		Message:      "candidate hostname " + g.Hostname + " not present in member_soa_serials yet; routing to primary",
	}
}

// decideFromMemberSerial makes the final caught-up/not-caught-up call once
// the candidate's own member_soa_serials entry has been located.
func (g *Gate) decideFromMemberSerial(obj metav1.Object, pending *PendingZoneSerial, m MemberSerial) RouteDecision {
	if m.Serial >= pending.Serial {
		ClearPendingSerial(obj)
		if g.Breaker != nil {
			g.Breaker.RecordSuccess()
		}
		return RouteDecision{UseCandidate: true, Reason: ReasonCandidateReady, Message: "candidate has caught up to the pending write"}
	}
	if g.Breaker != nil {
		g.Breaker.RecordSuccess()
	}
	return RouteDecision{
		UseCandidate: false,
		Reason:       ReasonWaitingForReplication,
		Message:      fmt.Sprintf("candidate serial %d has not yet caught up to pending write serial %d for zone %s", m.Serial, pending.Serial, pending.Zone),
	}
}

func (g *Gate) now() time.Time {
	if g.clock != nil {
		return g.clock()
	}
	return time.Now()
}
