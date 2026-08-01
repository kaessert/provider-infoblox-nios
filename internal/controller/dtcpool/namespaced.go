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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtcpool/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

const namespacedControllerName = "namespaced-dtcpool.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=dtcpool.infobloxnios.m.crossplane.io,resources=dtcpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dtcpool.infobloxnios.m.crossplane.io,resources=dtcpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dtcserver.infobloxnios.m.crossplane.io,resources=dtcservers,verbs=get;list;watch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.DTCPool].
// The Kind field on providerConfigRef selects which config type to fetch:
//   - "ProviderConfig" → namespace-scoped config (same namespace as the CR)
//   - "ClusterProviderConfig" → cluster-scoped config
//
// Per the dual-scope resource-spec convention, there is no IsNotFound
// fallback between the two — the Kind field explicitly declares intent
// and an unsupported Kind is a hard error.
type namespacedConnector struct {
	kube  k8sclient.Client
	usage *resource.ProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// ProviderConfig or ClusterProviderConfig by Kind, and returns an
// authenticated WAPI client bundle.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.DTCPool) (managed.TypedExternalClient[*namespacedv1alpha1.DTCPool], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return nil, errors.New(errGetPC + ": no ProviderConfigReference set")
	}

	var creds *nioCredentials
	// sslVerify governs TLS verification for all endpoints (primary and
	// read); it is a ProviderConfig/ClusterProviderConfig policy field,
	// not a per-credential Secret key. Defaults to true (secure) when
	// unset — the kubebuilder default handles the YAML path, but Go code
	// must handle the nil-pointer case too (e.g. objects created before
	// this field existed).
	sslVerify := true
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return nil, errors.Wrap(err, errGetPC)
		}
		var err error
		creds, err = extractCredentials(ctx, c.kube, pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, pc.GetNamespace())
		if err != nil {
			return nil, err
		}
		if pc.Spec.SSLVerify != nil {
			sslVerify = *pc.Spec.SSLVerify
		}

	case "ClusterProviderConfig":
		cpc := &apisv1alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name}, cpc); err != nil {
			return nil, errors.Wrap(err, errGetClusterPC)
		}
		var err error
		creds, err = extractCredentials(ctx, c.kube, cpc.Spec.Credentials.Source, cpc.Spec.Credentials.SecretRef, "")
		if err != nil {
			return nil, err
		}
		if cpc.Spec.SSLVerify != nil {
			sslVerify = *cpc.Spec.SSLVerify
		}

	default:
		return nil, errors.Errorf("%s: %s", errUnsupportedKind, ref.Kind)
	}

	clients, err := newClients(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{kube: c.kube, clients: clients}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.DTCPool].
type namespacedExternal struct {
	kube    k8sclient.Client
	clients *dtcPoolClients
}

// Observe fetches the DTCPool from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.DTCPool) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := getDtcPoolByRef(e.clients.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := dtcPoolExistsByNaturalKey(e.clients.conn, cr.Spec.ForProvider.Name)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveDTCPool)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDTCPool)
	}

	o := observeFromDtcPool(externalID, rec)
	cr.Status.AtProvider = namespacedv1alpha1.DTCPoolObservation{
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
	cr.Status.AtProvider.Servers = serverLinksToNamespaced(o.Servers)
	cr.Status.AtProvider.Monitors = poolMonitorsToNamespaced(o.Monitors)
	cr.Status.AtProvider.LBDynamicRatioPreferred = dynRatioToNamespaced(o.LBDynamicRatioPreferred)
	cr.Status.AtProvider.LBDynamicRatioAlternate = dynRatioToNamespaced(o.LBDynamicRatioAlternate)
	cr.Status.AtProvider.ConsolidatedMonitors = consolidatedMonitorsToNamespaced(o.ConsolidatedMonitors)
	cr.Status.AtProvider.Health = healthToNamespaced(o.Health)

	p := &cr.Spec.ForProvider
	servers := serversFromNamespaced(p.Servers)
	monitors := monitorsFromNamespaced(p.Monitors)
	lbdrp := dynRatioFromNamespaced(p.LBDynamicRatioPreferred)
	lbdra := dynRatioFromNamespaced(p.LBDynamicRatioAlternate)
	lateInit := lateInitialize(&p.Comment, &p.Disable, &p.Availability, &p.Quorum, &p.TTL, &p.UseTTL, &p.ExtAttrs, &p.LBAlternateMethod, &p.LBPreferredTopology, &p.LBAlternateTopology, &lbdrp, &lbdra, rec)
	if lateInit {
		p.LBDynamicRatioPreferred = dynRatioToNamespaced(lbdrp)
		p.LBDynamicRatioAlternate = dynRatioToNamespaced(lbdra)
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
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.DTCPool) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createDtcPool(e.clients.conn, p.Name, p.LBPreferredMethod, p.LBAlternateMethod, p.Comment, serversFromNamespaced(p.Servers), p.Availability, p.Quorum, p.LBPreferredTopology, dynRatioFromNamespaced(p.LBDynamicRatioPreferred), p.LBAlternateTopology, dynRatioFromNamespaced(p.LBDynamicRatioAlternate), monitorsFromNamespaced(p.Monitors), p.Disable, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDTCPool)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update replaces the mutable DTCPool fields. There are no known
// immutable fields for DTCPool, so every field is echoed (this API uses
// PUT full-replace semantics).
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.DTCPool) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateDtcPool(e.clients.conn, externalID, p.Name, p.LBPreferredMethod, p.LBAlternateMethod, p.Comment, serversFromNamespaced(p.Servers), p.Availability, p.Quorum, p.LBPreferredTopology, dynRatioFromNamespaced(p.LBDynamicRatioPreferred), p.LBAlternateTopology, dynRatioFromNamespaced(p.LBDynamicRatioAlternate), monitorsFromNamespaced(p.Monitors), p.Disable, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDTCPool)
	}

	// See clusterExternal.Update — UpdateDtcPool always returns the
	// object's current _ref, and renaming may change it.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the DTCPool. A 404 on the stored _ref is not treated as
// already-deleted by itself — see deleteDtcPoolResolving404 — because the
// _ref is a derived handle that rotates whenever an identity field
// changes, and a stale handle 404s exactly like a genuinely deleted
// object.
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.DTCPool) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteDtcPoolResolving404(e.clients.objMgr, e.clients.conn, externalID, p.Name); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// ── namespaced CRD <-> shared-type conversion ───────────────────────────

func serversFromNamespaced(servers []namespacedv1alpha1.DTCPoolServerLink) []serverLink {
	if len(servers) == 0 {
		return nil
	}
	out := make([]serverLink, 0, len(servers))
	for _, s := range servers {
		out = append(out, serverLink{Server: s.Server, Ratio: s.Ratio})
	}
	return out
}

func serverLinksToNamespaced(servers []serverLink) []namespacedv1alpha1.DTCPoolServerLink {
	if len(servers) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.DTCPoolServerLink, 0, len(servers))
	for _, s := range servers {
		out = append(out, namespacedv1alpha1.DTCPoolServerLink{Server: s.Server, Ratio: s.Ratio})
	}
	return out
}

func monitorsFromNamespaced(monitors []namespacedv1alpha1.DTCPoolMonitor) []poolMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]poolMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, poolMonitor{Monitor: m.Monitor})
	}
	return out
}

func poolMonitorsToNamespaced(monitors []poolMonitor) []namespacedv1alpha1.DTCPoolMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.DTCPoolMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, namespacedv1alpha1.DTCPoolMonitor{Monitor: m.Monitor})
	}
	return out
}

func dynRatioFromNamespaced(d *namespacedv1alpha1.DTCPoolDynamicRatio) *dynRatio {
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

func dynRatioToNamespaced(d *dynRatio) *namespacedv1alpha1.DTCPoolDynamicRatio {
	if d == nil {
		return nil
	}
	return &namespacedv1alpha1.DTCPoolDynamicRatio{
		Method:              d.Method,
		Monitor:             d.Monitor,
		MonitorMetric:       d.MonitorMetric,
		MonitorWeighing:     d.MonitorWeighing,
		InvertMonitorMetric: d.InvertMonitorMetric,
	}
}

func consolidatedMonitorsToNamespaced(list []consolidatedMonitorHealth) []namespacedv1alpha1.DTCPoolConsolidatedMonitorHealth {
	if len(list) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.DTCPoolConsolidatedMonitorHealth, 0, len(list))
	for _, c := range list {
		out = append(out, namespacedv1alpha1.DTCPoolConsolidatedMonitorHealth{
			Members:                 c.Members,
			Monitor:                 c.Monitor,
			Availability:            c.Availability,
			FullHealthCommunication: c.FullHealthCommunication,
		})
	}
	return out
}

func healthToNamespaced(h *poolHealth) *namespacedv1alpha1.DTCPoolHealth {
	if h == nil {
		return nil
	}
	return &namespacedv1alpha1.DTCPoolHealth{
		Availability: h.Availability,
		Description:  h.Description,
		EnabledState: h.EnabledState,
	}
}

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.DTCPool] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.DTCPool]    = &namespacedExternal{}
)

// setupNamespacedDTCPool wires the namespaced DTCPool reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedDTCPool(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.DTCPoolList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced DTCPool state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.DTCPool](&namespacedConnector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("DTCPool")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.DTCPool{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
