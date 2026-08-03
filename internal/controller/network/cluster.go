package network

import (
	"context"
	"strings"

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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/network/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
)

const clusterControllerName = "cluster-network.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=network.infobloxnios.crossplane.io,resources=networks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=network.infobloxnios.crossplane.io,resources=networks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.Network].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.Network) (managed.TypedExternalClient[*clusterv1alpha1.Network], error) {
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

	mc, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		endpoint: creds.Host,
	}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.Network].
type clusterExternal struct {
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — see resolveNetworkIdentity.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host.
	endpoint string
}

// Observe resolves the Network through the shared UID-in-EA identity
// ladder (family-aware — see resolveNetworkIdentity) and compares the
// result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.Network) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeNetwork(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Network, &p.Comment, p.ParentCidr, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveNetwork)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	o := res.obs
	members := make([]clusterv1alpha1.NetworkMember, 0, len(o.Members))
	for _, m := range o.Members {
		members = append(members, clusterv1alpha1.NetworkMember{
			DhcpMemberName:       m.DhcpMemberName,
			DhcpMemberIPv4Addr:   m.DhcpMemberIPv4Addr,
			DhcpMemberIPv6Addr:   m.DhcpMemberIPv6Addr,
			MsDhcpServerIPv4Addr: m.MsDhcpServerIPv4Addr,
		})
	}
	cr.Status.AtProvider = clusterv1alpha1.NetworkObservation{
		NetworkView: o.NetworkView,
		Network:     o.Network,
		Comment:     o.Comment,
		ExtAttrs:    o.ExtAttrs,
		Ref:         o.Ref,
		Members:     members,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	// A rotated or previously-unknown reference must be persisted through
	// a path crossplane-runtime actually writes back to the API server.
	// res.lateInit is already forced true alongside res.refreshedRef by
	// observeNetwork for exactly this reason.
	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	// An adopted object (ref resolved, no identity stamp yet) must never
	// be reported up to date — see observeResult.adopted — so the next
	// reconcile is guaranteed to call Update, which always re-asserts the
	// identity stamp (see updateNetwork).
	upToDate := isUpToDate(p.Comment, p.ExtAttrs, res.nw) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new Network, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
// Routes across three creation paths — see createOrAllocateNetwork. The
// WAPI object type (network vs ipv6network) is selected at runtime from
// the CIDR format.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.Network) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	p := cr.Spec.ForProvider
	nw, err := createOrAllocateNetwork(e.objMgr, p.NetworkView, p.Network, p.ParentCidr, p.Comment, p.Object, p.AllocatePrefixLen, p.FilterParams, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateNetwork)
	}

	meta.SetExternalName(cr, nw.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable Network fields (comment, extattrs).
// networkView and network (cidr) are immutable and are never sent — see
// updateNetwork. Every call re-asserts the identity stamp.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.Network) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	if err := updateNetwork(e.objMgr, externalID, p.Comment, p.ExtAttrs, string(cr.GetUID())); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateNetwork)
	}

	// _ref is stable across updates (unlike ARecord, renaming plays no
	// role here — networkView/cidr are immutable) so the external-name
	// annotation never needs to be refreshed after a PUT.
	return managed.ExternalUpdate{}, nil
}

// Delete removes the Network, resolving through the shared identity
// ladder first — see deleteNetworkIdentity for the full
// ownership-verification rules a stale or rotated _ref must satisfy
// before a delete is issued. This is a hard delete — a subsequent GET on
// the same ref 404s.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.Network) (managed.ExternalDelete, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)
	if err := deleteNetworkIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID()), p.Network, p.ParentCidr); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.Network] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.Network]    = &clusterExternal{}
)

// setupClusterNetwork wires the cluster-scoped Network reconciler with the
// controller-runtime manager. Called from SetupGated (gate callback) and
// Setup (immediate path) in controller.go.
func setupClusterNetwork(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.NetworkList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster Network state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.Network](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("Network")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.Network{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
