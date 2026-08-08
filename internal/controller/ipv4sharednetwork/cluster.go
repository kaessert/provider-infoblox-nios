package ipv4sharednetwork

import (
	"context"
	"strings"

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/ipv4sharednetwork/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
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

	conn, err := config.GetLegacy(ctx, c.kube, pc)
	if err != nil {
		return nil, err
	}
	mc := identity.NewManagerAndConnector(conn.Connector)

	return &clusterExternal{
		kube:     c.kube,
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		endpoint: conn.Endpoint,
	}, nil
}

// clusterExternal implements
// managed.TypedExternalClient[*clusterv1alpha1.IPv4SharedNetwork].
type clusterExternal struct {
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
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalObservation, error) {
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

	lateInit := res.lateInit
	// Only back-fill options when useOptions is on (post-backfill value
	// above). When it is off, the observed options are WAPI's own
	// default set, not values implied by the user's config.
	if len(p.Options) == 0 && len(o.Options) > 0 && boolOrFalse(p.UseOptions) {
		p.Options = optionsToCluster(o.Options)
		lateInit = true
	}

	// A rotated or previously-unknown reference must be persisted
	// through a path crossplane-runtime actually writes back to the API
	// server. res.lateInit is already forced true alongside
	// res.refreshedRef by observeIPv4SharedNetwork for exactly this
	// reason.
	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	// An adopted object (ref resolved, no identity stamp yet) must never
	// be reported up to date — see observeResult.adopted — so the next
	// reconcile is guaranteed to call Update, which always re-asserts
	// the identity stamp (see updateIPv4SharedNetwork).
	upToDate := isUpToDate(p.Name, p.Networks, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromCluster(p.Options), res.sn) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new IPv4SharedNetwork, stamping the managed
// resource's own uid into the object's identity extensible attribute in
// the same request (see createIPv4SharedNetwork), and records the
// server-assigned _ref as the external name.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	p := cr.Spec.ForProvider
	sn, err := createIPv4SharedNetwork(e.objMgr, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromCluster(p.Options), uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateIPv4SharedNet)
	}

	meta.SetExternalName(cr, sn.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable IPv4SharedNetwork fields (name, networks,
// comment, extattrs, disable, useOptions, options). networkView is
// immutable and is never sent as the top-level network_view key — see
// updateIPv4SharedNetwork. Every call re-asserts the identity stamp since
// a WAPI PUT carrying extattrs replaces the whole map rather than merging
// it — live-verified against a real Grid.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	sn, err := updateIPv4SharedNetwork(e.objMgr, externalID, p.Name, p.Networks, p.NetworkView, p.Comment, p.ExtAttrs, p.Disable, p.UseOptions, optionsFromCluster(p.Options), string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateIPv4SharedNet)
	}

	// UpdateIpv4SharedNetwork always returns the object's current _ref.
	// Name is mutable for this resource, and the WAPI _ref for a shared
	// network embeds its name — so the external-name annotation must be
	// refreshed here whenever the server returns a different _ref
	// (renaming plays the same role it does for ARecord).
	if sn.Ref != "" && sn.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, sn.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the IPv4SharedNetwork, resolving through the shared
// identity ladder first — see deleteIPv4SharedNetworkIdentity for the
// full ownership-verification rules a stale or rotated _ref must satisfy
// before a delete is issued.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.IPv4SharedNetwork) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteIPv4SharedNetworkIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
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
		if err := mgr.Add(statemetrics.NewResilientRecorder(
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
		managed.WithTypedExternalConnector[*clusterv1alpha1.IPv4SharedNetwork](driftdetection.WrapConnector[*clusterv1alpha1.IPv4SharedNetwork](&clusterConnector{
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
