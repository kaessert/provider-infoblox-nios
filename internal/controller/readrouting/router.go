// Package readrouting implements the generic decision layer behind this
// provider's read/write endpoint split (see the ProviderConfig
// readEndpoint field): deciding whether an Observe call may read from a
// read-only candidate endpoint instead of the always-authoritative
// primary, setting the ReadRouting status condition, emitting a
// Kubernetes Warning event on every fallback transition away from the
// candidate, and wrapping the convergence gate's write-recording so a
// resource controller's Update() persists the resulting watermark
// conflict-safely.
//
// Nothing here is specific to any one resource kind — a Router is built
// from a *convergence.Gate plus a candidate connector and operates purely
// on the zone identity (fqdn, view) its caller supplies, and the primary
// connector its caller already owns. Router does not keep its own copy of
// the primary: every resource controller already tracks one (e.g. for
// CRUD and identity resolution), so BeginObserve takes it as a parameter
// instead of duplicating that state here. Deriving the zone identity from
// a resource's own spec fields (e.g. a DNS record's parent zone from its
// own name) is likewise the caller's job, since only the caller knows its
// own resource shape.
//
// A Router is nil-safe end to end: when a resource's ProviderConfig has
// no readEndpoint configured, Gate is nil and every method behaves as a
// byte-for-byte no-op — reads always resolve to the caller's primary, no
// extra WAPI call is made, and no condition or event is set. This matches
// every controller's behavior before the read/write split existed.
package readrouting

import (
	"context"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/convergence"
)

// errRecordWrite is wrapped with resource-agnostic context. The caller's
// own error wrapping (e.g. "cannot update ARecord") supplies the
// resource-specific detail, exactly as it did before this package
// existed.
const errRecordWrite = "cannot record convergence watermark for read-endpoint routing after a write"

// Object is the subset of a managed resource's methods Router needs:
// annotation access, for the pending-zone-serial watermark, and status
// conditions, for the ReadRouting condition. Every generated managed
// resource type in this provider satisfies it; resource.Conditioned is
// the same interface crossplane-runtime itself uses for condition
// access.
type Object interface {
	k8sclient.Object
	resource.Conditioned
}

// WithRecorder returns a copy of r with Recorder set to recorder. A
// config.Conn's Router is built once in the credential bridge, which has
// no event.Recorder to give it — each controller package's Setup builds
// its own recorder (event.NewAPIRecorder, shared with
// managed.WithRecorder) and its Connect injects it into a copy of the
// shared Router value here, immediately before storing it on the
// external client.
func WithRecorder(r Router, recorder event.Recorder) Router {
	r.Recorder = recorder
	return r
}

// Router is everything a resource controller's ExternalClient needs to
// route Observe reads between the caller's primary and a read-endpoint
// candidate, gated on SOA-serial convergence for DNS resources (see
// ADR-IN-0005). It replaces five loose per-scope fields (candidate
// connector, gate, convergence mode, convergence timeout, event
// recorder) with the one value a credential bridge hands out.
//
// The zero value is a valid, permanently-primary-only Router: Gate is
// nil, so BeginObserve always returns the primary it was given and
// RecordWrite is a no-op. This is exactly the "no readEndpoint
// configured" case.
type Router struct {
	// Candidate is the read-endpoint WAPI connector. Only ever returned
	// by BeginObserve when Gate.Evaluate reports UseCandidate — never
	// touched otherwise, so a nil Candidate is safe whenever Gate is
	// also nil.
	Candidate ibclient.IBConnector
	// Gate implements the SOA-serial convergence check. nil when no
	// readEndpoint is configured on the owning ProviderConfig — every
	// method on Router treats a nil Gate as "always use Primary".
	Gate *convergence.Gate
	// Mode is the ProviderConfig's resolved
	// readEndpoint.convergence.mode ("soaSerial" or "primaryOnly"),
	// before any per-call zone-serial-signal override — see
	// BeginObserve's hasZoneSerial parameter.
	Mode string
	// Timeout is the resolved readEndpoint.convergence.timeout.
	Timeout time.Duration
	// Recorder emits Kubernetes Warning events for read-routing fallback
	// transitions (every fallback away from the candidate must be
	// visible via `kubectl describe`, not only in the ReadRouting
	// condition). nil in a resource package's white-box unit tests that
	// build a Router without going through Connect().
	Recorder event.Recorder
}

// BeginObserve decides whether this Observe call may read from the
// candidate, sets the ReadRouting status condition on cr, emits a
// Kubernetes Warning event on any fallback transition, and reports
// whether evaluating the gate mutated cr's pending-zone-serial
// annotation.
//
// primary is the caller's own authoritative WAPI connector — the one it
// already uses for CRUD and, in this provider, identity resolution.
// BeginObserve returns it unchanged whenever no candidate is configured,
// the gate declines the candidate, or Gate is nil.
//
// fqdn/view identify the DNS zone the convergence gate keys its per-zone
// watermark on — the caller derives these from its own resource's spec
// fields (e.g. convergence.ZoneFQDNFromRecordName for a record whose
// zone is implicit in its own name); Router has no resource-specific
// knowledge of how to compute them. hasZoneSerial states whether this
// call's resource has a zone SOA-serial convergence signal at all (see
// convergence.EffectiveMode) — IPAM is one instance of a signal-less
// resource, not the definition of one: DTC pools/LBDNs/servers, DNS
// views and extensible-attribute definitions are equally signal-less
// and must also pass hasZoneSerial=false, which forces primaryOnly and
// never issues a candidate call. DNS record and zone resources with a
// real SOA serial pass hasZoneSerial=true.
//
// annotationChanged MUST be folded into the caller's
// ExternalObservation.ResourceLateInitialized: crossplane-runtime only
// persists metadata mutations made during Observe when that flag is set
// (see the reconciler's late-initialize Update call), so an annotation
// clear reported as ResourceLateInitialized: false is silently discarded
// and every subsequent Observe re-derives it from the stale annotation.
func (r Router) BeginObserve(ctx context.Context, cr Object, primary ibclient.IBConnector, fqdn, view string, hasZoneSerial bool) (readFrom ibclient.IBConnector, annotationChanged bool) {
	if r.Gate == nil {
		return primary, false
	}

	effectiveMode, _ := convergence.EffectiveMode(r.Mode, !hasZoneSerial)

	before := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]
	decision := r.Gate.Evaluate(ctx, cr, fqdn, view, effectiveMode, r.Timeout)
	after := cr.GetAnnotations()[convergence.PendingZoneSerialAnnotation]

	cr.SetConditions(convergence.ReadRoutingCondition(decision))
	r.emitWarning(cr, decision)

	readFrom = primary
	if decision.UseCandidate {
		readFrom = r.Candidate
	}
	return readFrom, before != after
}

// RecordWrite is Gate.RecordAndPersistWrite's nil-safe wrapper, called
// right after a successful primary write. It persists the resulting
// pending-zone-serial annotation immediately via a conflict-safe merge
// patch rather than relying on crossplane-runtime to flush it — required
// specifically for calls made from an ExternalClient's Update() method,
// whose metadata mutations crossplane-runtime does not otherwise
// guarantee to persist (see convergence.Gate.RecordAndPersistWrite's
// doc). A no-op when no readEndpoint is configured (r.Gate == nil).
func (r Router) RecordWrite(ctx context.Context, kube k8sclient.Client, cr k8sclient.Object, fqdn, view string) error {
	if r.Gate == nil {
		return nil
	}
	if err := r.Gate.RecordAndPersistWrite(ctx, kube, cr, fqdn, view); err != nil {
		return errors.Wrap(err, errRecordWrite)
	}
	return nil
}

// emitWarning records a Kubernetes Warning event for a fallback
// RouteDecision. It is a no-op when either the decision carries no
// warning or the caller was built without a Recorder (e.g. a resource
// package's white-box unit tests that construct a Router directly
// instead of going through Connect()).
func (r Router) emitWarning(cr k8sclient.Object, decision convergence.RouteDecision) {
	if r.Recorder == nil || !decision.Warning {
		return
	}
	r.Recorder.Event(cr, event.Event{
		Type:    event.TypeWarning,
		Reason:  event.Reason(decision.Reason),
		Message: decision.Message,
	})
}
