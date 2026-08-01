package zonedelegated

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
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zonedelegated/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

const namespacedControllerName = "namespaced-zonedelegated.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=zonedelegated.infobloxnios.m.crossplane.io,resources=zonedelegateds,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zonedelegated.infobloxnios.m.crossplane.io,resources=zonedelegateds/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.ZoneDelegated].
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
// authenticated WAPI ObjectManager.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.ZoneDelegated) (managed.TypedExternalClient[*namespacedv1alpha1.ZoneDelegated], error) {
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

	objMgr, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{kube: c.kube, objMgr: objMgr}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.ZoneDelegated].
type namespacedExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
}

// namespacedDelegateToSDK converts the CRD's ZoneDelegatedNameServer list
// into the SDK's []ibclient.NameServer shape.
func namespacedDelegateToSDK(in []namespacedv1alpha1.ZoneDelegatedNameServer) []ibclient.NameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]ibclient.NameServer, len(in))
	for i, ns := range in {
		out[i] = ibclient.NameServer{Name: strOrEmpty(ns.Name), Address: strOrEmpty(ns.Address)}
	}
	return out
}

// namespacedDelegateFromSDK converts the SDK's []ibclient.NameServer
// shape back into the CRD's ZoneDelegatedNameServer list.
func namespacedDelegateFromSDK(in []ibclient.NameServer) []namespacedv1alpha1.ZoneDelegatedNameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.ZoneDelegatedNameServer, len(in))
	for i, ns := range in {
		name := ns.Name
		addr := ns.Address
		out[i] = namespacedv1alpha1.ZoneDelegatedNameServer{Name: &name, Address: &addr}
	}
	return out
}

// Observe fetches the ZoneDelegated from the WAPI by its _ref external
// name and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.ZoneDelegated) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.objMgr.GetZoneDelegatedByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := zoneDelegatedExistsByNaturalKey(e.objMgr, cr.Spec.ForProvider.Fqdn, cr.Spec.ForProvider.View)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveZoneDelegated)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveZoneDelegated)
	}

	o := observeFromZoneDelegated(externalID, rec)
	cr.Status.AtProvider = namespacedv1alpha1.ZoneDelegatedObservation{
		Fqdn:            o.Fqdn,
		DelegateTo:      namespacedDelegateFromSDK(rec.DelegateTo.NameServers),
		View:            o.View,
		ZoneFormat:      o.ZoneFormat,
		Comment:         o.Comment,
		Disable:         o.Disable,
		Locked:          o.Locked,
		NsGroup:         o.NsGroup,
		DelegatedTTL:    o.DelegatedTTL,
		UseDelegatedTTL: o.UseDelegatedTTL,
		ExtAttrs:        o.ExtAttrs,
		Ref:             o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.Comment, &p.NsGroup, &p.Disable, &p.Locked, &p.UseDelegatedTTL, &p.DelegatedTTL, &p.ExtAttrs, &p.View, &p.ZoneFormat, rec)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(namespacedDelegateToSDK(p.DelegateTo), p.Comment, p.NsGroup, p.Disable, p.Locked, p.UseDelegatedTTL, p.DelegatedTTL, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new ZoneDelegated and records the server-assigned
// _ref as the external name.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.ZoneDelegated) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createZoneDelegated(e.objMgr, p.Fqdn, p.View, p.ZoneFormat, p.Comment, p.NsGroup, p.Disable, p.Locked, p.UseDelegatedTTL, p.DelegatedTTL, namespacedDelegateToSDK(p.DelegateTo), p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateZoneDelegated)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ZoneDelegated fields. fqdn, view, and
// zoneFormat (immutable) are never sent — see updateZoneDelegated.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.ZoneDelegated) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateZoneDelegated(e.objMgr, externalID, p.Comment, p.NsGroup, p.Disable, p.Locked, p.UseDelegatedTTL, p.DelegatedTTL, namespacedDelegateToSDK(p.DelegateTo), p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateZoneDelegated)
	}

	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the ZoneDelegated. A 404 against the stored _ref is not
// treated as already-deleted by itself — see
// deleteZoneDelegatedResolving404 — because the _ref is a derived handle
// that rotates when fqdn/view changes.
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.ZoneDelegated) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteZoneDelegatedResolving404(e.objMgr, externalID, cr.Spec.ForProvider.Fqdn, cr.Spec.ForProvider.View); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.ZoneDelegated] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.ZoneDelegated]    = &namespacedExternal{}
)

// setupNamespacedZoneDelegated wires the namespaced ZoneDelegated
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupNamespacedZoneDelegated(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.ZoneDelegatedList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced ZoneDelegated state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.ZoneDelegated](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("ZoneDelegated")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.ZoneDelegated{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
