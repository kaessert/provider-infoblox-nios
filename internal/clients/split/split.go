/*
Copyright 2021 Upbound Inc.
*/

// Package split implements an opt-in Infoblox gridmaster read/write split for
// the no-fork (Terraform Plugin SDKv2, in-process) runtime.
//
// # What it does
//
// The customer's native tooling talks to two Infoblox endpoints: a gridmaster
// candidate for reads (their read_url) and the primary gridmaster for writes
// (their write_url). This package mirrors that split for Crossplane:
//
//   - Observe (read)                 -> gridmaster candidate  (read endpoint)
//   - Create / Update / Delete (write) -> primary gridmaster  (write endpoint)
//
// It works by decorating the generated per-resource
// tjcontroller.NewTerraformPluginSDKAsyncConnector (the write connecter) with
// a second async connecter built against the read endpoint, then routing each
// managed.ExternalClient call to the appropriate underlying client.
//
// # Scope: DNS only
//
// Only DNS records (API group "dns.infoblox-nios.crossplane.io": the A, AAAA,
// CNAME, MX, PTR, SRV and TXT record kinds) are offloaded to the read
// candidate. IPAM resources (networks, ranges, allocations, ...) always Observe
// against the primary. IPAM state is far more sensitive to replication lag —
// next-available-IP allocation in particular must read the authoritative
// primary — so it is deliberately excluded from the split. See isDNS.
//
// # Why this is only possible under no-fork
//
// In the old CLI runtime upjet's WorkspaceStore keyed an on-disk Terraform
// workspace by MR UID (/tmp/<UID>). Two endpoint setups would have collided on
// that path, so a split was impossible. Under no-fork there is no on-disk
// workspace: the SDKv2 external client uses the provider meta from
// terraform.Setup.Meta, which the setup builder Configures per endpoint. Two
// in-process clients (read-configured and write-configured metas) can
// therefore coexist for the same MR.
//
// # Backward compatibility
//
// The split is opt-in. If Configure is never called (or is called with a nil
// read setup) WrapConnector returns the write connecter unchanged and behavior
// is byte-for-byte identical to the single-endpoint provider. Even when the
// split is wired, if the credentials secret has no read_server the read setup
// builder produces a setup identical to the write setup (see
// clients.TerraformReadSetupBuilder), so reads and writes hit the same
// endpoint — again a no-op.
//
// # Read-after-write hazard and the convergence gate (IMPORTANT)
//
// A read routed to the candidate immediately after a write to the primary may
// observe stale data if grid replication has not yet propagated the change.
// That would make Observe report spurious drift or a missing resource,
// triggering reconcile churn (repeated Update/Create against the primary).
//
// This package guards against that in two complementary ways, neither of which
// is a timer — there is no replication-lag knob to tune:
//
//  1. Shared OperationTrackerStore. The read and write connecters share a
//     single upjet OperationTrackerStore, so they resolve to the *same*
//     AsyncTracker per MR. The async runtime marks LastOperation running while
//     an asynchronous Create/Update/Delete is in flight; because the tracker
//     is shared, the read client's Observe sees IsRunning()==true and never
//     probes the candidate mid-flight — it serves the primary instead.
//
//  2. Candidate-observe convergence gate. After a write the decorator marks the
//     MR "post-write" and keeps routing Observe to the *primary* while probing
//     the candidate on each reconcile. Only once the candidate itself reports
//     the resource Exists and UpToDate (candidateCaughtUp) does the marker
//     clear and steady-state reads return to the candidate. The window ends
//     when replication has demonstrably converged — observed, not timed.
//
// # Safe degradation
//
// If the candidate never catches up (e.g. a broken or lagging replica) the
// post-write marker never clears and reads for that MR stay on the primary
// indefinitely. This is intentional: the split degrades to no-offload rather
// than ever serving stale data. There is deliberately no timeout and no
// max-attempts.
//
// # Convergence seam
//
// The "has the candidate caught up?" decision is factored into candidateCaughtUp
// so the convergence signal can be swapped without touching the routing state
// machine. Today it uses the candidate's own Observe result; a future
// implementation could instead compare the NIOS SOA serial watermark
// (zone_auth.soa_serial_number) of the primary against the candidate behind
// this same seam.
//
// The split is BUILT, COMPILES and is WIRED, but is UNVALIDATED against a real
// NIOS grid.
package split

import (
	"context"
	"sync"

	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	xpresource "github.com/crossplane/crossplane-runtime/pkg/resource"
	ujconfig "github.com/crossplane/upjet/pkg/config"
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
	ujresource "github.com/crossplane/upjet/pkg/resource"
	"github.com/crossplane/upjet/pkg/terraform"
	tfsdk "github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dnsv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/dns/v1alpha1"
)

// dnsGroup is the API group whose resources are eligible for the read/write
// split. It mirrors apis/dns/v1alpha1.CRDGroup and is referenced by isDNS to
// scope the candidate offload to DNS records only.
const dnsGroup = dnsv1alpha1.CRDGroup

// ManagementPolicies mirrors the provider's --enable-management-policies flag
// onto the read connecter so the read-side Observe merges init parameters the
// same way the write connecter does (avoiding spurious drift). main sets this
// before Configure; it defaults to the provider default (management policies
// enabled).
var ManagementPolicies = true

var (
	mu         sync.RWMutex
	readSetup  terraform.SetupFn
	readOTS    *tjcontroller.OperationTrackerStore
	configured bool
)

// Configure registers the read-side dependencies for the split. It is intended
// to be called exactly once from the provider main. Package-level state is
// acceptable here because there is a single reconciler process and the
// dependencies (read setup builder, shared operation-tracker store) are process
// singletons.
//
// The OperationTrackerStore passed here SHOULD be the same store used by the
// write connecters (o.OperationTrackerStore). Sharing is deliberate and
// required for the async LastOperation coordination described in the package
// doc — see the "separate vs shared OTS" note. If readSetup is nil the split
// is disabled and WrapConnector becomes a pass-through.
func Configure(rs terraform.SetupFn, rots *tjcontroller.OperationTrackerStore) {
	mu.Lock()
	defer mu.Unlock()
	readSetup = rs
	readOTS = rots
	configured = rs != nil && rots != nil
}

// Enabled reports whether Configure registered a usable read setup.
func Enabled() bool {
	mu.RLock()
	defer mu.RUnlock()
	return configured
}

// WrapConnector decorates writeConn (the generated async write connecter) with
// read/write routing. If the split has not been Configured it returns writeConn
// unchanged so the provider behaves exactly as the single-endpoint build.
//
// The read connecter is a second async connecter built against the read setup
// and the shared operation-tracker store, for the same config.Resource.
func WrapConnector(kube client.Client, cfg *ujconfig.Resource, writeConn managed.ExternalConnecter, logger logging.Logger, opts ...tjcontroller.TerraformPluginSDKAsyncOption) managed.ExternalConnecter {
	mu.RLock()
	defer mu.RUnlock()
	if !configured {
		return writeConn
	}
	readOpts := append([]tjcontroller.TerraformPluginSDKAsyncOption{
		tjcontroller.WithTerraformPluginSDKAsyncLogger(logger),
		tjcontroller.WithTerraformPluginSDKAsyncManagementPolicies(ManagementPolicies),
	}, opts...)
	readConn := tjcontroller.NewTerraformPluginSDKAsyncConnector(kube, readOTS, readSetup, cfg, readOpts...)
	return &connector{
		write:  writeConn,
		read:   readConn,
		ots:    readOTS,
		logger: logger,
	}
}

// connector is the read/write-splitting managed.ExternalConnecter.
type connector struct {
	write  managed.ExternalConnecter
	read   managed.ExternalConnecter
	ots    *tjcontroller.OperationTrackerStore
	logger logging.Logger
}

// Connect connects both underlying connecters and returns a routing external
// client. If the read connect fails we fall back to the write client for reads
// too, so a candidate outage degrades to single-endpoint behavior rather than
// failing the whole reconcile.
func (c *connector) Connect(ctx context.Context, mg xpresource.Managed) (managed.ExternalClient, error) {
	w, err := c.write.Connect(ctx, mg)
	if err != nil {
		return nil, err
	}
	r, err := c.read.Connect(ctx, mg)
	sameClient := false
	if err != nil {
		c.logger.Debug("read endpoint connect failed; serving reads from the write endpoint for this reconcile", "error", err)
		r = w
		sameClient = true
	}
	return &external{
		write:      w,
		read:       r,
		ots:        c.ots,
		sameClient: sameClient,
		logger:     c.logger,
	}, nil
}

// external routes managed.ExternalClient calls to the read or write client.
type external struct {
	write      managed.ExternalClient
	read       managed.ExternalClient
	ots        *tjcontroller.OperationTrackerStore
	sameClient bool
	logger     logging.Logger
}

// postWrite tracks, per MR UID, whether the MR is in the post-write convergence
// window: a write has been applied to the primary and the candidate has not yet
// been observed to have caught up. While a UID is present here Observe is served
// from the primary (and probes the candidate); the entry is removed once the
// candidate converges (candidateCaughtUp) or the MR is deleted.
//
// Access is serialized by the manager's per-object reconciliation
// (MaxConcurrentReconciles is 1 and controller-runtime serializes reconciles
// for a given object key), so a single guarding mutex is sufficient.
var (
	wsMu      sync.Mutex
	postWrite = map[types.UID]struct{}{}
)

func markPostWrite(uid types.UID) {
	wsMu.Lock()
	defer wsMu.Unlock()
	postWrite[uid] = struct{}{}
}

func clearPostWrite(uid types.UID) {
	wsMu.Lock()
	defer wsMu.Unlock()
	delete(postWrite, uid)
}

func inPostWrite(uid types.UID) bool {
	wsMu.Lock()
	defer wsMu.Unlock()
	_, ok := postWrite[uid]
	return ok
}

// isDNS reports whether mg is a DNS record (API group
// "dns.infoblox-nios.crossplane.io"). Only DNS resources are offloaded to the
// read candidate; everything else (IPAM) always Observes against the primary.
func isDNS(mg xpresource.Managed) bool {
	return mg.GetObjectKind().GroupVersionKind().Group == dnsGroup
}

// candidateCaughtUp is the swappable convergence check for the read/write split:
// it decides whether the read candidate has replicated the last primary write
// and may therefore serve steady-state reads again.
//
// It is deliberately factored out of the routing state machine so the
// convergence signal can be replaced without touching the state machine. Today
// it trusts the candidate's own Observe result — the resource exists and is
// up-to-date on the candidate. A future implementation could instead compare
// the NIOS SOA serial watermark (zone_auth.soa_serial_number) of the primary
// against the candidate here, and the state machine would be unchanged.
func candidateCaughtUp(obs managed.ExternalObservation, err error) bool {
	return err == nil && obs.ResourceExists && obs.ResourceUpToDate
}

func (e *external) Observe(ctx context.Context, mg xpresource.Managed) (managed.ExternalObservation, error) {
	// read == write fallback: routing is irrelevant, serve from the write client.
	if e.sameClient {
		return e.write.Observe(ctx, mg)
	}
	tr, ok := mg.(ujresource.Terraformed)
	if !ok {
		return e.write.Observe(ctx, mg)
	}
	// GUARD #1: an async write is in flight on the shared tracker. Never probe
	// the candidate mid-flight; serve the primary.
	if e.ots.Tracker(tr).LastOperation.IsRunning() {
		return e.write.Observe(ctx, mg)
	}
	// SCOPE: only DNS records are offloaded to the candidate; IPAM always reads
	// the authoritative primary.
	if !isDNS(mg) {
		return e.write.Observe(ctx, mg)
	}
	// Post-write convergence gate: while the candidate has not been observed to
	// have caught up, serve reads from the primary but probe the candidate. Once
	// the candidate converges, clear the marker and serve from it. If it never
	// converges the marker never clears and reads stay on the primary
	// indefinitely (degrade to no-offload, never serve stale).
	if inPostWrite(mg.GetUID()) {
		obs, err := e.read.Observe(ctx, mg)
		if candidateCaughtUp(obs, err) {
			clearPostWrite(mg.GetUID())
			return obs, nil
		}
		return e.write.Observe(ctx, mg)
	}
	// Steady state: read from the candidate.
	return e.read.Observe(ctx, mg)
}

func (e *external) Create(ctx context.Context, mg xpresource.Managed) (managed.ExternalCreation, error) {
	if err := e.primeWrite(ctx, mg); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot prime the write client before create")
	}
	markPostWrite(mg.GetUID())
	return e.write.Create(ctx, mg)
}

func (e *external) Update(ctx context.Context, mg xpresource.Managed) (managed.ExternalUpdate, error) {
	if err := e.primeWrite(ctx, mg); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot prime the write client before update")
	}
	markPostWrite(mg.GetUID())
	return e.write.Update(ctx, mg)
}

// primeWrite ensures the write (primary) client has computed its Terraform
// instance diff before a mutating Create/Update is applied.
//
// The upjet Terraform Plugin SDK external client computes the instance diff in
// Observe and consumes it in Create/Update (schema.Resource.Apply is called with
// that diff). Because the split routes the reconciler's Observe to the *read*
// (candidate) client, the *write* (primary) client never observed and its diff
// is nil; upjet's Create/Update (unlike Delete) do not guard against a nil diff
// and would panic (Create) or apply an empty diff (Update). Priming the write
// client with its own Observe against the primary populates that diff.
//
// This is also semantically correct for a read/write split: the create/update
// decision is (re)confirmed against the authoritative primary gridmaster rather
// than a possibly-stale replication candidate. When the read connect fell back
// to the write client (sameClient), the reconciler's Observe already ran on the
// write client, so no priming is necessary.
//
// The read client's Observe additionally mutates the *shared* Terraform state on
// the operation tracker. For a not-yet-created resource RefreshWithoutUpgrade
// returns a nil state, so the tracker's tfState becomes nil; the write client's
// priming Observe would then dereference a nil state and panic. Seed a non-nil
// empty state first (RefreshWithoutUpgrade treats an empty ID as "does not
// exist"), so the write client observes not-exists and computes the correct
// creation diff.
func (e *external) primeWrite(ctx context.Context, mg xpresource.Managed) error {
	if e.sameClient {
		return nil
	}
	if tr, ok := mg.(ujresource.Terraformed); ok {
		t := e.ots.Tracker(tr)
		if t.GetTfState() == nil {
			t.SetTfState(&tfsdk.InstanceState{})
		}
	}
	_, err := e.write.Observe(ctx, mg)
	return err
}

func (e *external) Delete(ctx context.Context, mg xpresource.Managed) (managed.ExternalDelete, error) {
	// Clear any post-write marker; the resource is going away.
	clearPostWrite(mg.GetUID())
	return e.write.Delete(ctx, mg)
}

func (e *external) Disconnect(ctx context.Context) error {
	werr := e.write.Disconnect(ctx)
	if e.sameClient {
		return werr
	}
	rerr := e.read.Disconnect(ctx)
	if werr != nil {
		return werr
	}
	return rerr
}
