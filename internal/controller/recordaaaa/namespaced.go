package recordaaaa

import (
	"context"

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

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordaaaa/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-recordaaaa.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordaaaa.infobloxnios.m.crossplane.io,resources=aaaarecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordaaaa.infobloxnios.m.crossplane.io,resources=aaaarecords/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.AAAARecord].
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
// authenticated WAPI object manager.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.AAAARecord) (managed.TypedExternalClient[*namespacedv1alpha1.AAAARecord], error) {
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

	mgrConn := identity.NewManagerAndConnector(conn.Connector)

	return &namespacedExternal{
		kube:     c.kube,
		objMgr:   mgrConn.Manager,
		conn:     mgrConn.Connector,
		endpoint: conn.Endpoint,
	}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.AAAARecord].
type namespacedExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — it needs visibility into search match counts
	// that objMgr's typed methods hide. See resolveAAAARecordIdentity.
	conn ibclient.IBConnector
	// prober checks that the Grid's "Crossplane Internal ID" extensible
	// attribute definition exists before Create stamps identity onto a
	// new object — without the definition WAPI rejects the write with an
	// opaque error, so the probe surfaces an actionable one instead. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host. See
	// ensureIdentityPrerequisite's empty-string fallback.
	endpoint string
}

// Observe resolves the AAAARecord through the shared UID-in-EA identity
// ladder — steady-state resolve, adopt when the EA is missing, or a
// rotation search by uid when the stored _ref 404s — and compares the
// spec. See observeAAAARecord for the ladder itself.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.AAAARecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeAAAARecord(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveAAAARecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cr.Status.AtProvider = namespacedv1alpha1.AAAARecordObservation{
		Name:        res.obs.Name,
		IPv6Addr:    res.obs.IPv6Addr,
		Comment:     res.obs.Comment,
		TTL:         res.obs.TTL,
		UseTTL:      res.obs.UseTTL,
		ExtAttrs:    res.obs.ExtAttrs,
		View:        res.obs.View,
		Cidr:        p.Cidr,
		NetworkView: p.NetworkView,
		Ref:         res.obs.Ref,
		Zone:        res.obs.Zone,
	}
	cr.Status.AtProvider.ID = res.obs.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(p.Name, p.IPv6Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, res.rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new AAAARecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request (see createAAAARecord), and records the server-assigned _ref
// as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.AAAARecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if err := validateAAAARecordCreateInputs(p.IPv6Addr, p.Cidr, uid); err != nil {
		return managed.ExternalCreation{}, err
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createAAAARecord(e.objMgr, p.Name, p.View, p.IPv6Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, p.Cidr, p.NetworkView, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateAAAARecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable AAAARecord fields. View (immutable) is
// never sent — see updateAAAARecord. Every call re-asserts the identity
// stamp (updateAAAARecord).
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.AAAARecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateAAAARecord(e.objMgr, externalID, p.Name, p.IPv6Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateAAAARecord)
	}

	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the AAAARecord, resolving through the shared identity
// ladder first — see deleteAAAARecordIdentity for the full
// ownership-verification rules a stale or rotated _ref must satisfy
// before a delete is issued.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.AAAARecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteAAAARecordIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.AAAARecord] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.AAAARecord]    = &namespacedExternal{}
)

// setupNamespacedAAAARecord wires the namespaced AAAARecord reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupNamespacedAAAARecord(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.AAAARecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced AAAARecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.AAAARecord](driftdetection.WrapConnector[*namespacedv1alpha1.AAAARecord](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("AAAARecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.AAAARecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
