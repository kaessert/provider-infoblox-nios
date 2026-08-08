package recorda

import (
	"context"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recorda/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const clusterControllerName = "cluster-recorda.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=recorda.infobloxnios.crossplane.io,resources=arecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recorda.infobloxnios.crossplane.io,resources=arecords/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.ARecord].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.ARecord) (managed.TypedExternalClient[*clusterv1alpha1.ARecord], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return nil, errors.New(errGetPC + ": no ProviderConfigReference set")
	}

	pc := &apisv1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: ref.Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	conn, err := config.GetLegacy(ctx, c.kube, pc)
	if err != nil {
		return nil, err
	}
	mgrConn := identity.NewManagerAndConnector(conn.Connector)

	return &clusterExternal{
		kube:   c.kube,
		objMgr: mgrConn.Manager,
		conn:   mgrConn.Connector,
		// prober is left nil (defaults to identity.DefaultProber in
		// ensureIdentityPrerequisite) so every controller in the process
		// shares one TTL-bounded verdict cache per Grid endpoint.
		endpoint: conn.Endpoint,
	}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.ARecord].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — it needs visibility into search match counts
	// that objMgr's typed methods hide. See resolveARecordIdentity.
	conn ibclient.IBConnector
	// prober checks that the Grid's "Crossplane Internal ID" extensible
	// attribute definition exists before Create stamps identity onto a
	// new object — without the definition WAPI rejects the write with an
	// opaque error, so the probe surfaces an actionable one instead. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host. See
	// ensureIdentityPrerequisite's empty-string fallback.
	endpoint string
}

// Observe resolves the ARecord through the shared UID-in-EA identity
// ladder — steady-state resolve, adopt when the EA is missing, or a
// rotation search by uid when the stored _ref 404s — and compares the
// result against the desired spec. See observeARecord for the ladder
// itself.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.ARecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeARecord(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	if err != nil {
		// A *identity.PrerequisiteError carries the missing-extensible-
		// attribute-definition remediation text verbatim in its own
		// Error() text — return it unwrapped, matching Create's behavior,
		// instead of burying it under the generic "cannot observe ARecord"
		// prefix below.
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveARecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cr.Status.AtProvider = clusterv1alpha1.ARecordObservation{
		Name:     res.obs.Name,
		IPv4Addr: res.obs.IPv4Addr,
		Comment:  res.obs.Comment,
		TTL:      res.obs.TTL,
		UseTTL:   res.obs.UseTTL,
		ExtAttrs: res.obs.ExtAttrs,
		View:     res.obs.View,
		// Cidr/NetworkView are create-time-only allocation hints the WAPI
		// never echoes back in a GET response — mirrored directly from
		// ForProvider (informational only) rather than from the observed
		// RecordA.
		Cidr:        p.Cidr,
		NetworkView: p.NetworkView,
		Ref:         res.obs.Ref,
		Zone:        res.obs.Zone,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = res.obs.ID

	// A rotated or previously-unknown reference must be persisted
	// through a path crossplane-runtime actually writes back to the API
	// server. res.lateInit is already forced true alongside
	// res.refreshedRef by observeARecord for exactly this reason.
	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	// An adopted object (ref resolved, no identity stamp yet) must never
	// be reported up to date — see observeResult.adopted — so the next
	// reconcile is guaranteed to call Update, which always re-asserts
	// the identity stamp (see updateARecord).
	upToDate := isUpToDate(p.Name, p.IPv4Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, res.rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new ARecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request (see createARecord), and records the server-assigned _ref as
// the external name.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.ARecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	// Local, network-free validation first — a bad request must never
	// cost a probe round-trip.
	if err := validateARecordCreateInputs(p.IPv4Addr, p.Cidr, uid); err != nil {
		return managed.ExternalCreation{}, err
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createARecord(e.objMgr, p.Name, p.View, p.IPv4Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, p.Cidr, p.NetworkView, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateARecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ARecord fields. View (immutable) is never
// sent — see updateARecord. Every call re-asserts the identity stamp
// (updateARecord) since a WAPI PUT carrying extattrs replaces the whole
// map rather than merging it — live-verified against a real Grid.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.ARecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateARecord(e.objMgr, externalID, p.Name, p.IPv4Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateARecord)
	}

	// UpdateARecord always returns the object's current _ref. Live
	// verification against a real NIOS Grid Manager confirmed that
	// renaming an A record changes its _ref and the old _ref immediately
	// 404s — so the external-name annotation must be refreshed here even
	// though name is not in the immutable-fields table.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the ARecord, resolving through the shared identity
// ladder first — see deleteARecordIdentity for the full ownership-
// verification rules a stale or rotated _ref must satisfy before a
// delete is issued.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.ARecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteARecordIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.ARecord] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.ARecord]    = &clusterExternal{}
)

// setupClusterARecord wires the cluster-scoped ARecord reconciler with the
// controller-runtime manager. Called from SetupGated (gate callback) and
// Setup (immediate path) in controller.go.
func setupClusterARecord(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.ARecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster ARecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.ARecord](driftdetection.WrapConnector[*clusterv1alpha1.ARecord](&clusterConnector{
			kube:  mgr.GetClient(),
			usage: resource.NewLegacyProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
		})),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		//nolint:staticcheck // event.NewAPIRecorder still requires the deprecated record.EventRecorder type; no replacement exists yet in this crossplane-runtime version.
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))),
	}
	if o.Features.Enabled(feature.EnableBetaManagementPolicies) {
		opts = append(opts, managed.WithManagementPolicies())
	}
	if o.ChangeLogOptions != nil && o.Features.Enabled(feature.EnableAlphaChangeLogs) {
		opts = append(opts, managed.WithChangeLogger(o.ChangeLogOptions.ChangeLogger))
	}
	if o.MetricOptions != nil {
		opts = append(opts, managed.WithMetricRecorder(o.MetricOptions.MRMetrics))
	}

	r := managed.NewReconciler(mgr,
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("ARecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.ARecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
