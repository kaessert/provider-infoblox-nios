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
// # Read-after-write hazard (IMPORTANT)
//
// A read routed to the candidate immediately after a write to the primary may
// observe stale data if grid replication has not yet propagated the change.
// That would make Observe report spurious drift or a missing resource,
// triggering reconcile churn (repeated Update/Create against the primary).
//
// This package guards against that in two complementary ways:
//
//  1. Shared OperationTrackerStore. The read and write connecters share a
//     single upjet OperationTrackerStore, so they resolve to the *same*
//     AsyncTracker per MR. The async runtime marks LastOperation running while
//     an asynchronous Create/Update/Delete is in flight; because the tracker
//     is shared, the read client's Observe sees IsRunning()==true and returns
//     early (ResourceExists && ResourceUpToDate) instead of querying the
//     candidate mid-flight. See the OTS decision in the package tests/notes.
//
//  2. Post-write grace window. Once the asynchronous write completes and the
//     operation is no longer running, this decorator keeps routing Observe to
//     the *write* (primary) client for GraceWindow, giving the grid time to
//     replicate to the candidate before reads move back to it. The window is
//     anchored to the observed completion of the async operation (detected via
//     the shared tracker), not to the moment the async call returned, so a
//     slow write does not consume the window prematurely.
//
// The split is BUILT, COMPILES and is WIRED, but is UNVALIDATED against a real
// NIOS grid. GraceWindow's default is a conservative starting point and must
// be tuned to the customer's measured replication lag during e2e validation.
package split

import (
	"context"
	"sync"
	"time"

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
)

// GraceWindow is how long after an observed write completion reads continue to
// be served from the write (primary) endpoint, to absorb grid replication lag
// before reads move back to the candidate. It is a package-level knob so it
// can be tuned without changing controller code; the default is conservative
// and MUST be validated against the customer's real replication behavior.
var GraceWindow = 30 * time.Second

// now returns the current wall-clock time. It is a package-level indirection so
// tests can drive the grace-window state machine deterministically; production
// code leaves it as time.Now and behavior is unchanged.
var now = time.Now

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

// writeState tracks, per MR UID, the read-after-write grace state. Access is
// serialized by the manager's per-object reconciliation (MaxConcurrentReconciles
// is 1 and controller-runtime serializes reconciles for a given object key), so
// a single guarding mutex is sufficient.
type writeState struct {
	wasRunning bool
	graceUntil time.Time
}

var (
	wsMu    sync.Mutex
	wsByUID = map[types.UID]*writeState{}
)

func stateFor(uid types.UID) *writeState {
	wsMu.Lock()
	defer wsMu.Unlock()
	s, ok := wsByUID[uid]
	if !ok {
		s = &writeState{}
		wsByUID[uid] = s
	}
	return s
}

// routeReadToPrimary decides whether an Observe should be served from the write
// (primary) endpoint instead of the read (candidate) endpoint, and advances the
// grace state machine. It returns true when either an async write is in flight
// or the post-write grace window is still open.
//
// The window is anchored to the *observed completion* of the async operation:
// while the shared tracker reports IsRunning we route to primary and remember
// that a write was running; on the first Observe that finds it no longer
// running we open a fresh GraceWindow from now. This is robust to the async
// runtime clearing LastOperation (which would otherwise make an end-time-based
// anchor panic/false-negative).
func (e *external) routeReadToPrimary(mg xpresource.Managed) bool {
	if e.sameClient {
		// read == write; routing is irrelevant.
		return false
	}
	tr, ok := mg.(ujresource.Terraformed)
	if !ok {
		return false
	}
	running := e.ots.Tracker(tr).LastOperation.IsRunning()
	s := stateFor(mg.GetUID())

	wsMu.Lock()
	defer wsMu.Unlock()
	if running {
		s.wasRunning = true
		return true
	}
	if s.wasRunning {
		// The async write just completed; start the replication grace window.
		s.wasRunning = false
		s.graceUntil = now().Add(GraceWindow)
		return true
	}
	return now().Before(s.graceUntil)
}

func (e *external) Observe(ctx context.Context, mg xpresource.Managed) (managed.ExternalObservation, error) {
	if e.routeReadToPrimary(mg) {
		return e.write.Observe(ctx, mg)
	}
	return e.read.Observe(ctx, mg)
}

func (e *external) Create(ctx context.Context, mg xpresource.Managed) (managed.ExternalCreation, error) {
	if err := e.primeWrite(ctx, mg); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, "cannot prime the write client before create")
	}
	return e.write.Create(ctx, mg)
}

func (e *external) Update(ctx context.Context, mg xpresource.Managed) (managed.ExternalUpdate, error) {
	if err := e.primeWrite(ctx, mg); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, "cannot prime the write client before update")
	}
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
	// Clear any grace state; the resource is going away.
	wsMu.Lock()
	delete(wsByUID, mg.GetUID())
	wsMu.Unlock()
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
