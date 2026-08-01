package dtcpool

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtcpool/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-dtcpool.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=dtcpool.infobloxnios.crossplane.io,resources=dtcpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dtcpool.infobloxnios.crossplane.io,resources=dtcpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dtcserver.infobloxnios.crossplane.io,resources=dtcservers,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.DTCPool].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI client
// bundle.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.DTCPool) (managed.TypedExternalClient[*clusterv1alpha1.DTCPool], error) {
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

	return &clusterExternal{kube: c.kube, clients: clients}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.DTCPool].
type clusterExternal struct {
	kube    k8sclient.Client
	clients *dtcPoolClients
}

// Observe fetches the DTCPool from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.DTCPool) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling getDtcPoolByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := getDtcPoolByRef(e.clients.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDTCPool)
	}

	o := observeFromDtcPool(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.DTCPoolObservation{
		Name:                o.Name,
		LBPreferredMethod:   o.LBPreferredMethod,
		LBAlternateMethod:   o.LBAlternateMethod,
		Availability:        o.Availability,
		Quorum:              o.Quorum,
		LBPreferredTopology: o.LBPreferredTopology,
		LBAlternateTopology: o.LBAlternateTopology,
		Comment:             o.Comment,
		Disable:             o.Disable,
		TTL:                 o.TTL,
		UseTTL:              o.UseTTL,
		ExtAttrs:            o.ExtAttrs,
		Ref:                 o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID
	cr.Status.AtProvider.Servers = serverLinksToCluster(o.Servers)
	cr.Status.AtProvider.Monitors = poolMonitorsToCluster(o.Monitors)
	cr.Status.AtProvider.LBDynamicRatioPreferred = dynRatioToCluster(o.LBDynamicRatioPreferred)
	cr.Status.AtProvider.LBDynamicRatioAlternate = dynRatioToCluster(o.LBDynamicRatioAlternate)
	cr.Status.AtProvider.ConsolidatedMonitors = consolidatedMonitorsToCluster(o.ConsolidatedMonitors)
	cr.Status.AtProvider.Health = healthToCluster(o.Health)

	p := &cr.Spec.ForProvider
	servers := serversFromCluster(p.Servers)
	monitors := monitorsFromCluster(p.Monitors)
	lbdrp := dynRatioFromCluster(p.LBDynamicRatioPreferred)
	lbdra := dynRatioFromCluster(p.LBDynamicRatioAlternate)
	lateInit := lateInitialize(&p.Comment, &p.Disable, &p.Availability, &p.Quorum, &p.TTL, &p.UseTTL, &p.ExtAttrs, &p.LBAlternateMethod, &p.LBPreferredTopology, &p.LBAlternateTopology, &lbdrp, &lbdra, rec)
	if lateInit {
		p.LBDynamicRatioPreferred = dynRatioToCluster(lbdrp)
		p.LBDynamicRatioAlternate = dynRatioToCluster(lbdra)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.LBPreferredMethod, p.LBAlternateMethod, p.Comment, p.Availability, p.LBPreferredTopology, p.LBAlternateTopology, p.Quorum, p.TTL, p.Disable, p.UseTTL, servers, monitors, lbdrp, lbdra, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new DTCPool and records the server-assigned _ref
// as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.DTCPool) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createDtcPool(e.clients.conn, p.Name, p.LBPreferredMethod, p.LBAlternateMethod, p.Comment, serversFromCluster(p.Servers), p.Availability, p.Quorum, p.LBPreferredTopology, dynRatioFromCluster(p.LBDynamicRatioPreferred), p.LBAlternateTopology, dynRatioFromCluster(p.LBDynamicRatioAlternate), monitorsFromCluster(p.Monitors), p.Disable, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDTCPool)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update replaces the mutable DTCPool fields. There are no known
// immutable fields for DTCPool, so every field is echoed (this API uses
// PUT full-replace semantics).
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.DTCPool) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateDtcPool(e.clients.conn, externalID, p.Name, p.LBPreferredMethod, p.LBAlternateMethod, p.Comment, serversFromCluster(p.Servers), p.Availability, p.Quorum, p.LBPreferredTopology, dynRatioFromCluster(p.LBDynamicRatioPreferred), p.LBAlternateTopology, dynRatioFromCluster(p.LBDynamicRatioAlternate), monitorsFromCluster(p.Monitors), p.Disable, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDTCPool)
	}

	// UpdateDtcPool always returns the object's current _ref. Renaming a
	// DTC Pool (a mutable field) may change its _ref (per the live-verified
	// _ref-instability fact) — refresh the annotation whenever it differs.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the DTCPool. A 404 is treated as already-deleted
// (idempotent, hard-delete).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.DTCPool) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteDtcPool(e.clients.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteDTCPool)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// ── cluster-scoped CRD <-> shared-type conversion ───────────────────────

func serversFromCluster(servers []clusterv1alpha1.DTCPoolServerLink) []serverLink {
	if len(servers) == 0 {
		return nil
	}
	out := make([]serverLink, 0, len(servers))
	for _, s := range servers {
		out = append(out, serverLink{Server: s.Server, Ratio: s.Ratio})
	}
	return out
}

func serverLinksToCluster(servers []serverLink) []clusterv1alpha1.DTCPoolServerLink {
	if len(servers) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.DTCPoolServerLink, 0, len(servers))
	for _, s := range servers {
		out = append(out, clusterv1alpha1.DTCPoolServerLink{Server: s.Server, Ratio: s.Ratio})
	}
	return out
}

func monitorsFromCluster(monitors []clusterv1alpha1.DTCPoolMonitor) []poolMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]poolMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, poolMonitor{Monitor: m.Monitor})
	}
	return out
}

func poolMonitorsToCluster(monitors []poolMonitor) []clusterv1alpha1.DTCPoolMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.DTCPoolMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, clusterv1alpha1.DTCPoolMonitor{Monitor: m.Monitor})
	}
	return out
}

func dynRatioFromCluster(d *clusterv1alpha1.DTCPoolDynamicRatio) *dynRatio {
	if d == nil {
		return nil
	}
	return &dynRatio{
		Method:              d.Method,
		Monitor:             d.Monitor,
		MonitorMetric:       d.MonitorMetric,
		MonitorWeighing:     d.MonitorWeighing,
		InvertMonitorMetric: d.InvertMonitorMetric,
	}
}

func dynRatioToCluster(d *dynRatio) *clusterv1alpha1.DTCPoolDynamicRatio {
	if d == nil {
		return nil
	}
	return &clusterv1alpha1.DTCPoolDynamicRatio{
		Method:              d.Method,
		Monitor:             d.Monitor,
		MonitorMetric:       d.MonitorMetric,
		MonitorWeighing:     d.MonitorWeighing,
		InvertMonitorMetric: d.InvertMonitorMetric,
	}
}

func consolidatedMonitorsToCluster(list []consolidatedMonitorHealth) []clusterv1alpha1.DTCPoolConsolidatedMonitorHealth {
	if len(list) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.DTCPoolConsolidatedMonitorHealth, 0, len(list))
	for _, c := range list {
		out = append(out, clusterv1alpha1.DTCPoolConsolidatedMonitorHealth{
			Members:                 c.Members,
			Monitor:                 c.Monitor,
			Availability:            c.Availability,
			FullHealthCommunication: c.FullHealthCommunication,
		})
	}
	return out
}

func healthToCluster(h *poolHealth) *clusterv1alpha1.DTCPoolHealth {
	if h == nil {
		return nil
	}
	return &clusterv1alpha1.DTCPoolHealth{
		Availability: h.Availability,
		Description:  h.Description,
		EnabledState: h.EnabledState,
	}
}

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.DTCPool] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.DTCPool]    = &clusterExternal{}
)

// setupClusterDTCPool wires the cluster-scoped DTCPool reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupClusterDTCPool(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.DTCPoolList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster DTCPool state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.DTCPool](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("DTCPool")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.DTCPool{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
