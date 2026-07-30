package dtclbdn

import (
	"context"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/feature"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtclbdn/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
)

const clusterControllerName = "cluster-dtclbdn.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=dtclbdn.infobloxnios.crossplane.io,resources=dtclbdns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dtclbdn.infobloxnios.crossplane.io,resources=dtclbdns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dtcpool.infobloxnios.crossplane.io,resources=dtcpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=zoneauth.infobloxnios.crossplane.io,resources=zoneauths,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.DTCLBDN].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI client
// bundle.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.DTCLBDN) (managed.TypedExternalClient[*clusterv1alpha1.DTCLBDN], error) {
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

	creds, err := extractCredentials(ctx, c.kube, pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, "")
	if err != nil {
		return nil, err
	}

	// sslVerify governs TLS verification for all endpoints (primary and
	// read); it is a ProviderConfig policy field, not a per-credential
	// Secret key. Defaults to true (secure) when unset — the kubebuilder
	// default handles the YAML path, but Go code must handle the
	// nil-pointer case too (e.g. objects created before this field
	// existed).
	sslVerify := true
	if pc.Spec.SSLVerify != nil {
		sslVerify = *pc.Spec.SSLVerify
	}

	clients, err := newClients(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{clients: clients}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.DTCLBDN].
type clusterExternal struct {
	clients *dtcLbdnClients
}

// Observe fetches the DTCLBDN from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.DTCLBDN) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling getDtcLbdnByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := getDtcLbdnByRef(e.clients.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDTCLBDN)
	}

	o := observeFromDtcLbdn(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.DTCLBDNObservation{
		Name:        o.Name,
		LBMethod:    o.LBMethod,
		Patterns:    o.Patterns,
		AuthZones:   o.AuthZones,
		Types:       o.Types,
		Priority:    o.Priority,
		Persistence: o.Persistence,
		Topology:    o.Topology,
		TTL:         o.TTL,
		UseTTL:      o.UseTTL,
		Comment:     o.Comment,
		Disable:     o.Disable,
		ExtAttrs:    o.ExtAttrs,
		Ref:         o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID
	cr.Status.AtProvider.Pools = poolsToCluster(o.Pools)
	cr.Status.AtProvider.Health = healthToCluster(o.Health)

	p := &cr.Spec.ForProvider
	pools := poolsFromCluster(p.Pools)
	lateInit := lateInitialize(&p.Priority, &p.Persistence, &p.Topology, &p.TTL, &p.UseTTL, &p.Comment, &p.Disable, &p.ExtAttrs, rec)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.LBMethod, p.Patterns, pools, p.AuthZones, p.Types, p.Priority, p.Persistence, p.Topology, p.TTL, p.UseTTL, p.Comment, p.Disable, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new DTCLBDN and records the server-assigned _ref
// as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.DTCLBDN) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createDtcLbdn(e.clients.conn, p.Name, p.LBMethod, p.Patterns, poolsFromCluster(p.Pools), p.AuthZones, p.Types, p.Priority, p.Persistence, p.Topology, p.TTL, p.UseTTL, p.Comment, p.Disable, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDTCLBDN)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update replaces the mutable DTCLBDN fields. There are no known
// immutable fields for DTCLBDN, so every field is echoed (this API uses
// PUT full-replace semantics).
func (e *clusterExternal) Update(_ context.Context, cr *clusterv1alpha1.DTCLBDN) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateDtcLbdn(e.clients.conn, externalID, p.Name, p.LBMethod, p.Patterns, poolsFromCluster(p.Pools), p.AuthZones, p.Types, p.Priority, p.Persistence, p.Topology, p.TTL, p.UseTTL, p.Comment, p.Disable, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDTCLBDN)
	}

	// UpdateDtcLbdn always returns the object's current _ref. Renaming a
	// DTC LBDN (a mutable field) may change its _ref (live-verified,
	// ADR-IN-0004) — refresh the annotation whenever it differs.
	if rec.Ref != "" && rec.Ref != externalID {
		meta.SetExternalName(cr, rec.Ref)
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the DTCLBDN. A 404 is treated as already-deleted
// (idempotent, hard-delete).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.DTCLBDN) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteDtcLbdn(e.clients.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteDTCLBDN)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// ── cluster-scoped CRD <-> shared-type conversion ───────────────────────

func poolsFromCluster(pools []clusterv1alpha1.DTCLBDNPoolLink) []poolLink {
	if len(pools) == 0 {
		return nil
	}
	out := make([]poolLink, 0, len(pools))
	for _, p := range pools {
		out = append(out, poolLink{Pool: p.Pool, Ratio: p.Ratio})
	}
	return out
}

func poolsToCluster(pools []poolLink) []clusterv1alpha1.DTCLBDNPoolLink {
	if len(pools) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.DTCLBDNPoolLink, 0, len(pools))
	for _, p := range pools {
		out = append(out, clusterv1alpha1.DTCLBDNPoolLink{Pool: p.Pool, Ratio: p.Ratio})
	}
	return out
}

func healthToCluster(h *dtcHealth) *clusterv1alpha1.DTCLBDNHealth {
	if h == nil {
		return nil
	}
	return &clusterv1alpha1.DTCLBDNHealth{
		Availability: h.Availability,
		Description:  h.Description,
		EnabledState: h.EnabledState,
	}
}

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.DTCLBDN] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.DTCLBDN]    = &clusterExternal{}
)

// setupClusterDTCLBDN wires the cluster-scoped DTCLBDN reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupClusterDTCLBDN(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.DTCLBDNList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster DTCLBDN state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.DTCLBDN](&clusterConnector{
			kube:  mgr.GetClient(),
			usage: resource.NewLegacyProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
		}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("DTCLBDN")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.DTCLBDN{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
