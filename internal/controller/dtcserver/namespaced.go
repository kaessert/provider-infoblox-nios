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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtcserver/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

const namespacedControllerName = "namespaced-dtcserver.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=dtcserver.infobloxnios.m.crossplane.io,resources=dtcservers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dtcserver.infobloxnios.m.crossplane.io,resources=dtcservers/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.DTCServer].
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
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.DTCServer) (managed.TypedExternalClient[*namespacedv1alpha1.DTCServer], error) {
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

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.DTCServer].
type namespacedExternal struct {
	kube    k8sclient.Client
	clients *dtcServerClients
}

// Observe fetches the DTCServer from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.DTCServer) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.clients.objMgr.GetDtcServerByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := dtcServerExistsByNaturalKey(e.clients.objMgr, cr.Spec.ForProvider.Name, cr.Spec.ForProvider.Host)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveDTCServer)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDTCServer)
	}

	o := observeFromDtcServer(externalID, rec)
	cr.Status.AtProvider = namespacedv1alpha1.DTCServerObservation{
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
	cr.Status.AtProvider.Monitors = monitorPairsToNamespaced(o.Monitors)
	cr.Status.AtProvider.Health = healthToNamespaced(o.Health)

	p := &cr.Spec.ForProvider
	monitors := monitorsFromNamespaced(p.Monitors)
	lateInit := lateInitialize(&p.Comment, &p.Disable, &p.AutoCreateHostRecord, &p.UseSniHostname, &p.SniHostname, &monitors, &p.ExtAttrs, rec)
	if lateInit {
		p.Monitors = monitorPairsToNamespaced(monitors)
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
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.DTCServer) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createDtcServer(e.clients.conn, p.Name, p.Host, p.Comment, p.AutoCreateHostRecord, p.Disable, monitorsFromNamespaced(p.Monitors), p.SniHostname, p.UseSniHostname, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDTCServer)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update replaces the mutable DTCServer fields. There are no known
// immutable fields for DTCServer, so every field is echoed.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.DTCServer) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateDtcServer(e.clients.conn, externalID, p.Name, p.Host, p.Comment, p.AutoCreateHostRecord, p.Disable, monitorsFromNamespaced(p.Monitors), p.SniHostname, p.UseSniHostname, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDTCServer)
	}

	// See clusterExternal.Update — UpdateDtcServer always returns the
	// object's current _ref, and renaming may change it.
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
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.DTCServer) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteDtcServerResolving404(e.clients.objMgr, externalID, p.Name, p.Host); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// ── namespaced CRD <-> shared-type monitor/health conversion ────────────

func monitorsFromNamespaced(monitors []namespacedv1alpha1.DTCServerMonitor) []monitorPair {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]monitorPair, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, monitorPair{Monitor: m.Monitor, Host: m.Host})
	}
	return out
}

func monitorPairsToNamespaced(monitors []monitorPair) []namespacedv1alpha1.DTCServerMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.DTCServerMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, namespacedv1alpha1.DTCServerMonitor{Monitor: m.Monitor, Host: m.Host})
	}
	return out
}

func healthToNamespaced(h *dtcHealth) *namespacedv1alpha1.DTCServerHealth {
	if h == nil {
		return nil
	}
	return &namespacedv1alpha1.DTCServerHealth{
		Availability: h.Availability,
		Description:  h.Description,
		EnabledState: h.EnabledState,
	}
}

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.DTCServer] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.DTCServer]    = &namespacedExternal{}
)

// setupNamespacedDTCServer wires the namespaced DTCServer reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedDTCServer(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.DTCServerList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced DTCServer state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.DTCServer](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("DTCServer")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.DTCServer{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
