package rangepkg

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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/range/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-range.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=range.infobloxnios.m.crossplane.io,resources=ranges,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=range.infobloxnios.m.crossplane.io,resources=ranges/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networkview.infobloxnios.m.crossplane.io,resources=networkviews,verbs=get;list;watch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.Range].
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
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.Range) (managed.TypedExternalClient[*namespacedv1alpha1.Range], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return nil, errors.New(errGetPC + ": no ProviderConfigReference set")
	}

	var conn *config.Conn
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return nil, errors.Wrap(err, errGetPC)
		}
		var err error
		conn, err = config.Get(ctx, c.kube, pc)
		if err != nil {
			return nil, err
		}

	case "ClusterProviderConfig":
		cpc := &apisv1alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name}, cpc); err != nil {
			return nil, errors.Wrap(err, errGetClusterPC)
		}
		var err error
		conn, err = config.GetCluster(ctx, c.kube, cpc)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("%s: %s", errUnsupportedKind, ref.Kind)
	}

	mc := identity.NewManagerAndConnector(conn.Connector)

	return &namespacedExternal{
		kube:     c.kube,
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		endpoint: conn.Endpoint,
	}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.Range].
type namespacedExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — see resolveRangeIdentity.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host.
	endpoint string
}

// Observe resolves the Range through the shared UID-in-EA identity ladder
// and compares the result against the desired spec. See observeRange for
// the ladder itself.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.Range) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeRange(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.NetworkView, &p.Network, &p.Comment, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveRange)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	o := res.obs
	cr.Status.AtProvider = namespacedv1alpha1.RangeObservation{
		StartAddr:   o.StartAddr,
		EndAddr:     o.EndAddr,
		NetworkView: o.NetworkView,
		Network:     o.Network,
		Comment:     o.Comment,
		ExtAttrs:    o.ExtAttrs,
		Ref:         o.Ref,
		// Template is a create-time-only allocation hint the WAPI never
		// echoes back in a GET response — mirrored directly from
		// ForProvider (informational only) rather than from the observed
		// Range.
		Template: p.Template,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(p.StartAddr, p.EndAddr, p.NetworkView, p.Network, p.Comment, p.ExtAttrs, res.rng) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new Range, stamping the managed resource's own uid
// into the object's identity extensible attribute in the same request,
// and records the server-assigned _ref as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.Range) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	p := cr.Spec.ForProvider
	rng, err := createRange(e.objMgr, p.StartAddr, p.EndAddr, p.NetworkView, p.Network, p.Template, p.Comment, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateRange)
	}

	meta.SetExternalName(cr, rng.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable Range fields. template (immutable, create-only)
// is never sent — see updateRange. Every call re-asserts the identity
// stamp.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.Range) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rng, err := updateRange(e.objMgr, externalID, p.StartAddr, p.EndAddr, p.NetworkView, p.Network, p.Comment, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateRange)
	}

	// See clusterExternal.Update — UpdateNetworkRange always returns the
	// object's current _ref, and changing identity-bearing fields can
	// change it.
	if rng.Ref != "" && rng.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rng.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the Range, resolving through the shared identity ladder
// first.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.Range) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteRangeIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.Range] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.Range]    = &namespacedExternal{}
)

// setupNamespacedRange wires the namespaced Range reconciler with the
// controller-runtime manager. Called from SetupGated (gate callback) and
// Setup (immediate path) in controller.go.
func setupNamespacedRange(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.RangeList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced Range state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.Range](driftdetection.WrapConnector[*namespacedv1alpha1.Range](&namespacedConnector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("Range")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.Range{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
