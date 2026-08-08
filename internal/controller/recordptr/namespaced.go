package recordptr

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

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordptr/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/convergence"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/readrouting"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-recordptr.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordptr.infobloxnios.m.crossplane.io,resources=ptrrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordptr.infobloxnios.m.crossplane.io,resources=ptrrecords/status,verbs=get;update;patch
// PTRRecord's ptrdname field names another DNS record by FQDN (best-effort
// reference — WAPI does not require the target to exist). Grant read
// access to the referenced record type for future reference resolution.
// +kubebuilder:rbac:groups=recorda.infobloxnios.m.crossplane.io,resources=arecords,verbs=get;list;watch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.PTRRecord].
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
	// recorder emits Kubernetes Warning events for read-routing fallback
	// transitions (see readrouting.Router's emitWarning). nil in this
	// package's white-box tests that build namespacedConnector directly.
	recorder event.Recorder
}

// Connect tracks ProviderConfig usage, resolves the referenced
// ProviderConfig or ClusterProviderConfig by Kind, and returns an
// authenticated WAPI ObjectManager.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.PTRRecord) (managed.TypedExternalClient[*namespacedv1alpha1.PTRRecord], error) {
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

	objMgr := identity.NewManagerAndConnector(conn.Connector)

	return &namespacedExternal{
		kube:     c.kube,
		objMgr:   objMgr.Manager,
		conn:     objMgr.Connector,
		endpoint: conn.Endpoint,
		router:   readrouting.WithRecorder(conn.Router, c.recorder),
	}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.PTRRecord].
type namespacedExternal struct {
	kube     k8sclient.Client
	objMgr   ibclient.IBObjectManager
	conn     ibclient.IBConnector
	prober   *identity.Prober
	endpoint string

	// router routes Observe reads between the primary and an (optional)
	// candidate read endpoint, gated by SOA-serial convergence, and
	// wraps the convergence gate's write-recording for Create/Update.
	// Its zero value (Gate == nil) is "always read from the primary".
	router readrouting.Router
}

// Observe resolves the PTRRecord through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.PTRRecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	fqdn := convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name))
	readFrom, annotationChanged := e.router.BeginObserve(ctx, cr, e.conn, fqdn, strOrEmpty(p.View), true)

	res, err := observePTRRecord(ctx, readFrom, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Name, &p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObservePTRRecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cr.Status.AtProvider = namespacedv1alpha1.PTRRecordObservation{
		Ptrdname: res.obs.Ptrdname,
		Name:     res.obs.Name,
		IPv4Addr: res.obs.IPv4Addr,
		IPv6Addr: res.obs.IPv6Addr,
		View:     res.obs.View,
		Comment:  res.obs.Comment,
		TTL:      res.obs.TTL,
		UseTTL:   res.obs.UseTTL,
		ExtAttrs: res.obs.ExtAttrs,
		// Cidr/NetworkView are create-time-only allocation hints the WAPI
		// never echoes back in a GET response — mirrored directly from
		// ForProvider (informational only) rather than from the observed
		// RecordPTR.
		Cidr:        p.Cidr,
		NetworkView: p.NetworkView,
		Ref:         res.obs.Ref,
		Zone:        res.obs.Zone,
	}
	cr.Status.AtProvider.ID = res.obs.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(p.Ptrdname, p.Name, p.IPv4Addr, p.IPv6Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, res.rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: upToDate,
		// annotationChanged folds the read-routing gate's own annotation
		// mutation into the same persistence path already used for
		// res.lateInit.
		ResourceLateInitialized: res.lateInit || annotationChanged,
	}, nil
}

// Create provisions a new PTRRecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.PTRRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if err := validatePTRRecordCreateInputs(p.IPv4Addr, p.IPv6Addr, p.Cidr, uid); err != nil {
		return managed.ExternalCreation{}, err
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createPTRRecord(e.objMgr, p.Ptrdname, p.Name, p.IPv4Addr, p.IPv6Addr, p.View, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, p.Cidr, p.NetworkView, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreatePTRRecord)
	}

	meta.SetExternalName(cr, rec.Ref)

	fqdn := convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name))
	if err := e.router.RecordWrite(ctx, e.kube, cr, fqdn, strOrEmpty(p.View)); err != nil {
		return managed.ExternalCreation{}, err
	}

	return managed.ExternalCreation{}, nil
}

// Update patches the mutable PTRRecord fields. View (immutable) is never
// sent — see updatePTRRecord. Every call re-asserts the identity stamp.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.PTRRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updatePTRRecord(e.objMgr, externalID, p.Ptrdname, p.Name, p.IPv4Addr, p.IPv6Addr, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdatePTRRecord)
	}

	// See clusterExternal.Update — UpdatePTRRecord always returns the
	// object's current _ref, and ptrdname/name are both _ref-mutating.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}

	if err := e.router.RecordWrite(ctx, e.kube, cr, convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name)), strOrEmpty(p.View)); err != nil {
		return managed.ExternalUpdate{}, err
	}

	return managed.ExternalUpdate{}, nil
}

// Delete removes the PTRRecord, resolving through the shared identity
// ladder first.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.PTRRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deletePTRRecordIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.PTRRecord] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.PTRRecord]    = &namespacedExternal{}
)

// setupNamespacedPTRRecord wires the namespaced PTRRecord reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedPTRRecord(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.PTRRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced PTRRecord state recorder")
		}
	}

	//nolint:staticcheck // event.NewAPIRecorder still requires the deprecated record.EventRecorder type; no replacement exists yet in this crossplane-runtime version.
	recorder := event.NewAPIRecorder(mgr.GetEventRecorderFor(name))

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.PTRRecord](driftdetection.WrapConnector[*namespacedv1alpha1.PTRRecord](&namespacedConnector{
			kube:     mgr.GetClient(),
			usage:    resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
			recorder: recorder,
		})),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(recorder),
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("PTRRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.PTRRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
