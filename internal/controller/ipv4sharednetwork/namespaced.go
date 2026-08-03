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
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/ipv4sharednetwork/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
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

	mc, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{
		kube:     c.kube,
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		endpoint: creds.Host,
	}, nil
}

// namespacedExternal implements
// managed.TypedExternalClient[*namespacedv1alpha1.IPv4SharedNetwork].
type namespacedExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — see resolveIPv4SharedNetworkIdentity.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host.
	endpoint string
}

// Observe resolves the IPv4SharedNetwork through the shared UID-in-EA
// identity ladder and compares the result against the desired spec. See
// observeIPv4SharedNetwork for the ladder itself.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeIPv4SharedNetwork(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.NetworkView, &p.Comment, &p.Disable, &p.UseOptions, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveIPv4SharedNet)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	o := res.obs
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

	lateInit := res.lateInit
	if len(p.Options) == 0 && len(o.Options) > 0 && boolOrFalse(p.UseOptions) {
		p.Options = optionsToNamespaced(o.Options)
		lateInit = true
	}

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	upToDate := isUpToDate(p.Name, p.Networks, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromNamespaced(p.Options), res.sn) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new IPv4SharedNetwork, stamping the managed
// resource's own uid into the object's identity extensible attribute in
// the same request, and records the server-assigned _ref as the external
// name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if uid == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	p := cr.Spec.ForProvider
	sn, err := createIPv4SharedNetwork(e.objMgr, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromNamespaced(p.Options), uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateIPv4SharedNet)
	}

	meta.SetExternalName(cr, sn.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable IPv4SharedNetwork fields. networkView is
// immutable and is never sent as the top-level network_view key — see
// updateIPv4SharedNetwork. Every call re-asserts the identity stamp.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	sn, err := updateIPv4SharedNetwork(e.objMgr, externalID, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromNamespaced(p.Options), string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateIPv4SharedNet)
	}

	// See clusterExternal.Update — UpdateIpv4SharedNetwork always returns
	// the object's current _ref, and renaming changes the _ref.
	if sn.Ref != "" && sn.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, sn.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the IPv4SharedNetwork, resolving through the shared
// identity ladder first.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.IPv4SharedNetwork) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteIPv4SharedNetworkIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
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
		if err := mgr.Add(statemetrics.NewResilientRecorder(
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
