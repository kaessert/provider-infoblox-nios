package ipv4sharednetwork

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

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/ipv4sharednetwork/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

const namespacedControllerName = "namespaced-ipv4sharednetwork.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=ipv4sharednetwork.infobloxnios.m.crossplane.io,resources=ipv4sharednetworks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ipv4sharednetwork.infobloxnios.m.crossplane.io,resources=ipv4sharednetworks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=network.infobloxnios.m.crossplane.io,resources=networks,verbs=get;list;watch

// namespacedConnector implements
// managed.TypedExternalConnector[*namespacedv1alpha1.IPv4SharedNetwork].
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
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.TypedExternalClient[*namespacedv1alpha1.IPv4SharedNetwork], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return nil, errors.New(errGetPC + ": no ProviderConfigReference set")
	}

	var creds *nioCredentials
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

	default:
		return nil, errors.Errorf("%s: %s", errUnsupportedKind, ref.Kind)
	}

	objMgr, err := newObjectManager(creds)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{objMgr: objMgr}, nil
}

// namespacedExternal implements
// managed.TypedExternalClient[*namespacedv1alpha1.IPv4SharedNetwork].
type namespacedExternal struct {
	objMgr ibclient.IBObjectManager
}

// Observe fetches the IPv4SharedNetwork from the WAPI by its _ref external
// name and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	sn, err := e.objMgr.GetIpv4SharedNetworkByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveIPv4SharedNet)
	}

	o := observeFromIPv4SharedNetwork(externalID, sn)
	cr.Status.AtProvider = namespacedv1alpha1.IPv4SharedNetworkObservation{
		Name:                  o.Name,
		Networks:              o.Networks,
		NetworkView:           o.NetworkView,
		Comment:               o.Comment,
		ExtAttrs:              o.ExtAttrs,
		Disable:               o.Disable,
		UseOptions:            o.UseOptions,
		Options:               optionsToNamespaced(o.Options),
		Ref:                   o.Ref,
		Authority:             o.Authority,
		DdnsTTL:               o.DdnsTTL,
		EnableDdns:            o.EnableDdns,
		DhcpUtilization:       o.DhcpUtilization,
		DhcpUtilizationStatus: o.DhcpUtilizationStatus,
		DynamicHosts:          o.DynamicHosts,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.NetworkView, &p.Comment, &p.Disable, &p.UseOptions, &p.ExtAttrs, o)
	if len(p.Options) == 0 && len(o.Options) > 0 {
		p.Options = optionsToNamespaced(o.Options)
		lateInit = true
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.Networks, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromNamespaced(p.Options), sn),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new IPv4SharedNetwork and records the
// server-assigned _ref as the external name.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	sn, err := createIPv4SharedNetwork(e.objMgr, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromNamespaced(p.Options))
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateIPv4SharedNet)
	}

	meta.SetExternalName(cr, sn.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable IPv4SharedNetwork fields (name, networks,
// comment, extattrs, disable, useOptions, options). networkView is
// immutable and is never sent as the top-level network_view key — see
// updateIPv4SharedNetwork.
func (e *namespacedExternal) Update(_ context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	sn, err := updateIPv4SharedNetwork(e.objMgr, externalID, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromNamespaced(p.Options))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateIPv4SharedNet)
	}

	// See clusterExternal.Update — name is mutable and the returned _ref
	// must be re-annotated whenever it changes.
	if sn.Ref != "" && sn.Ref != externalID {
		meta.SetExternalName(cr, sn.Ref)
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the IPv4SharedNetwork. A 404 is treated as
// already-deleted (idempotent). This is a hard delete — a subsequent GET
// on the same ref 404s.
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteIPv4SharedNetwork(e.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteIPv4SharedNet)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.IPv4SharedNetwork] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.IPv4SharedNetwork]    = &namespacedExternal{}
)

// setupNamespacedIPv4SharedNetwork wires the namespaced IPv4SharedNetwork
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupNamespacedIPv4SharedNetwork(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.IPv4SharedNetworkList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced IPv4SharedNetwork state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.IPv4SharedNetwork](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("IPv4SharedNetwork")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.IPv4SharedNetwork{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
