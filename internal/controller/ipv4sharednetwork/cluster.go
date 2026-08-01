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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/ipv4sharednetwork/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-ipv4sharednetwork.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=ipv4sharednetwork.infobloxnios.crossplane.io,resources=ipv4sharednetworks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ipv4sharednetwork.infobloxnios.crossplane.io,resources=ipv4sharednetworks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=network.infobloxnios.crossplane.io,resources=networks,verbs=get;list;watch

// clusterConnector implements
// managed.TypedExternalConnector[*clusterv1alpha1.IPv4SharedNetwork].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.TypedExternalClient[*clusterv1alpha1.IPv4SharedNetwork], error) {
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

	objMgr, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{kube: c.kube, objMgr: objMgr}, nil
}

// clusterExternal implements
// managed.TypedExternalClient[*clusterv1alpha1.IPv4SharedNetwork].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
}

// Observe fetches the IPv4SharedNetwork from the WAPI by its _ref external
// name and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling
	// GetIpv4SharedNetworkByRef with the CR's Kubernetes name (not a real
	// WAPI _ref) would error against the API on every reconcile until
	// Create() overwrites the annotation with the real _ref.
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
	cr.Status.AtProvider = clusterv1alpha1.IPv4SharedNetworkObservation{
		Name:                  o.Name,
		Networks:              o.Networks,
		NetworkView:           o.NetworkView,
		Comment:               o.Comment,
		ExtAttrs:              o.ExtAttrs,
		Disable:               o.Disable,
		UseOptions:            o.UseOptions,
		Options:               optionsToCluster(o.Options),
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
	// Only back-fill options when useOptions is on (post-backfill value
	// above). When it is off, the observed options are WAPI's own
	// default set, not values implied by the user's config.
	if len(p.Options) == 0 && len(o.Options) > 0 && boolOrFalse(p.UseOptions) {
		p.Options = optionsToCluster(o.Options)
		lateInit = true
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.Networks, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromCluster(p.Options), sn),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new IPv4SharedNetwork and records the
// server-assigned _ref as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	sn, err := createIPv4SharedNetwork(e.objMgr, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromCluster(p.Options))
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
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	sn, err := updateIPv4SharedNetwork(e.objMgr, externalID, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromCluster(p.Options))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateIPv4SharedNet)
	}

	// UpdateIpv4SharedNetwork always returns the object's current _ref.
	// Name is mutable for this resource (per the external-name strategy
	// table), and the WAPI _ref for a shared network embeds its name — so
	// the external-name annotation must be refreshed here whenever the
	// server returns a different _ref (renaming plays the same role it
	// does for ARecord).
	if sn.Ref != "" && sn.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, sn.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the IPv4SharedNetwork. A 404 is treated as
// already-deleted (idempotent). This is a hard delete — a subsequent GET
// on the same ref 404s.
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteIPv4SharedNetworkResolving404(e.objMgr, externalID, p.NetworkView, p.Name); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.IPv4SharedNetwork] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.IPv4SharedNetwork]    = &clusterExternal{}
)

// setupClusterIPv4SharedNetwork wires the cluster-scoped IPv4SharedNetwork
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupClusterIPv4SharedNetwork(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.IPv4SharedNetworkList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster IPv4SharedNetwork state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.IPv4SharedNetwork](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("IPv4SharedNetwork")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.IPv4SharedNetwork{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
