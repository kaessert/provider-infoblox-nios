package networkcontainer

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkcontainer/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const clusterControllerName = "cluster-networkcontainer.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=networkcontainer.infobloxnios.crossplane.io,resources=networkcontainers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networkcontainer.infobloxnios.crossplane.io,resources=networkcontainers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.NetworkContainer].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI
// ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.TypedExternalClient[*clusterv1alpha1.NetworkContainer], error) {
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
	objMgr := identity.NewManagerAndConnector(conn.Connector)

	return &clusterExternal{
		kube:     c.kube,
		objMgr:   objMgr.Manager,
		conn:     objMgr.Connector,
		endpoint: conn.Endpoint,
	}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.NetworkContainer].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — see resolveNetworkContainerIdentity.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host.
	endpoint string
}

// Observe resolves the NetworkContainer through the shared UID-in-EA
// identity ladder (family-aware — see resolveNetworkContainerIdentity)
// and compares the result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeNetworkContainer(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Network, &p.Comment, p.ParentCidr, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveNetworkContainer)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	o := res.obs
	cr.Status.AtProvider = clusterv1alpha1.NetworkContainerObservation{
		NetworkView: o.NetworkView,
		Network:     o.Network,
		Comment:     o.Comment,
		ExtAttrs:    o.ExtAttrs,
		Ref:         o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	// A rotated or previously-unknown reference must be persisted through
	// a path crossplane-runtime actually writes back to the API server.
	// res.lateInit is already forced true alongside res.refreshedRef by
	// observeNetworkContainer for exactly this reason.
	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	// An adopted object (ref resolved, no identity stamp yet) must never
	// be reported up to date — see observeResult.adopted — so the next
	// reconcile is guaranteed to call Update, which always re-asserts the
	// identity stamp (see updateNetworkContainer).
	upToDate := isUpToDate(p.Comment, p.ExtAttrs, res.nc) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new NetworkContainer, stamping the managed
// resource's own uid into the object's identity extensible attribute in
// the same request, and records the server-assigned _ref as the external
// name. Routes across three creation paths — see
// createOrAllocateNetworkContainer.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	p := cr.Spec.ForProvider
	nc, err := createOrAllocateNetworkContainer(e.objMgr, p.NetworkView, p.Network, p.ParentCidr, p.Comment, p.AllocatePrefixLen, p.FilterParams, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateNetworkContainer)
	}

	meta.SetExternalName(cr, nc.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable NetworkContainer fields. networkView and
// network (immutable identity fields) are never sent — see
// updateNetworkContainer. Every call re-asserts the identity stamp.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	nc, err := updateNetworkContainer(e.objMgr, externalID, p.Comment, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateNetworkContainer)
	}

	// UpdateNetworkContainer always returns the object's current _ref.
	// The identity fields (networkView, network) are immutable, so the
	// _ref does not change across an update, but refreshing the
	// annotation here is a defensive no-op if that ever changes.
	if nc.Ref != "" && nc.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, nc.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the NetworkContainer, resolving through the shared
// identity ladder first — see deleteNetworkContainerIdentity for the
// full ownership-verification rules a stale or rotated _ref must satisfy
// before a delete is issued.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalDelete, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)
	if err := deleteNetworkContainerIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID()), p.Network, p.ParentCidr); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.NetworkContainer] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.NetworkContainer]    = &clusterExternal{}
)

// setupClusterNetworkContainer wires the cluster-scoped NetworkContainer
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupClusterNetworkContainer(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.NetworkContainerList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster NetworkContainer state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.NetworkContainer](driftdetection.WrapConnector[*clusterv1alpha1.NetworkContainer](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("NetworkContainer")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.NetworkContainer{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
