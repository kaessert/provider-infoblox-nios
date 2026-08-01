package dtcserver

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtcserver/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-dtcserver.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=dtcserver.infobloxnios.crossplane.io,resources=dtcservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dtcserver.infobloxnios.crossplane.io,resources=dtcservers/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.DTCServer].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI client
// bundle.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.DTCServer) (managed.TypedExternalClient[*clusterv1alpha1.DTCServer], error) {
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

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.DTCServer].
type clusterExternal struct {
	kube    k8sclient.Client
	clients *dtcServerClients
}

// Observe fetches the DTCServer from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.DTCServer) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetDtcServerByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.clients.objMgr.GetDtcServerByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDTCServer)
	}

	o := observeFromDtcServer(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.DTCServerObservation{
		Name:                 o.Name,
		Host:                 o.Host,
		Comment:              o.Comment,
		Disable:              o.Disable,
		AutoCreateHostRecord: o.AutoCreateHostRecord,
		SniHostname:          o.SniHostname,
		UseSniHostname:       o.UseSniHostname,
		ExtAttrs:             o.ExtAttrs,
		Ref:                  o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID
	cr.Status.AtProvider.Monitors = monitorPairsToCluster(o.Monitors)
	cr.Status.AtProvider.Health = healthToCluster(o.Health)

	p := &cr.Spec.ForProvider
	monitors := monitorsFromCluster(p.Monitors)
	lateInit := lateInitialize(&p.Comment, &p.Disable, &p.AutoCreateHostRecord, &p.UseSniHostname, &p.SniHostname, &monitors, &p.ExtAttrs, rec)
	if lateInit {
		p.Monitors = monitorPairsToCluster(monitors)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.Host, p.Comment, p.Disable, p.AutoCreateHostRecord, p.UseSniHostname, p.SniHostname, monitors, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new DTCServer and records the server-assigned _ref
// as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.DTCServer) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createDtcServer(e.clients.conn, p.Name, p.Host, p.Comment, p.AutoCreateHostRecord, p.Disable, monitorsFromCluster(p.Monitors), p.SniHostname, p.UseSniHostname, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDTCServer)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update replaces the mutable DTCServer fields. There are no known
// immutable fields for DTCServer, so every field is echoed.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.DTCServer) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateDtcServer(e.clients.conn, externalID, p.Name, p.Host, p.Comment, p.AutoCreateHostRecord, p.Disable, monitorsFromCluster(p.Monitors), p.SniHostname, p.UseSniHostname, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDTCServer)
	}

	// UpdateDtcServer always returns the object's current _ref. Renaming
	// a DTC Server (a mutable field) may change its _ref, mirroring the
	// ARecord precedent — refresh the annotation whenever it differs.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the DTCServer. A 404 on the stored _ref is not treated
// as already-deleted by itself — see deleteDtcServerResolving404 —
// because the _ref is a derived handle that rotates whenever an identity
// field changes, and a stale handle 404s exactly like a genuinely
// deleted object.
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.DTCServer) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteDtcServerResolving404(e.clients.objMgr, externalID, p.Name, p.Host); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// ── cluster-scoped CRD <-> shared-type monitor/health conversion ────────

func monitorsFromCluster(monitors []clusterv1alpha1.DTCServerMonitor) []monitorPair {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]monitorPair, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, monitorPair{Monitor: m.Monitor, Host: m.Host})
	}
	return out
}

func monitorPairsToCluster(monitors []monitorPair) []clusterv1alpha1.DTCServerMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.DTCServerMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, clusterv1alpha1.DTCServerMonitor{Monitor: m.Monitor, Host: m.Host})
	}
	return out
}

func healthToCluster(h *dtcHealth) *clusterv1alpha1.DTCServerHealth {
	if h == nil {
		return nil
	}
	return &clusterv1alpha1.DTCServerHealth{
		Availability: h.Availability,
		Description:  h.Description,
		EnabledState: h.EnabledState,
	}
}

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.DTCServer] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.DTCServer]    = &clusterExternal{}
)

// setupClusterDTCServer wires the cluster-scoped DTCServer reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupClusterDTCServer(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.DTCServerList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster DTCServer state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.DTCServer](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("DTCServer")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.DTCServer{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
