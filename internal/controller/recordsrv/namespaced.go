package recordsrv

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

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordsrv/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-recordsrv.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordsrv.infobloxnios.m.crossplane.io,resources=srvrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordsrv.infobloxnios.m.crossplane.io,resources=srvrecords/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.SRVRecord].
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
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.TypedExternalClient[*namespacedv1alpha1.SRVRecord], error) {
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

	return &namespacedExternal{kube: c.kube, objMgr: objMgr.Manager, conn: objMgr.Connector, endpoint: conn.Endpoint}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.SRVRecord].
type namespacedExternal struct {
	kube     k8sclient.Client
	objMgr   ibclient.IBObjectManager
	conn     ibclient.IBConnector
	prober   *identity.Prober
	endpoint string
}

// Observe resolves the SRVRecord through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeSRVRecord(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveSRVRecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cr.Status.AtProvider = namespacedv1alpha1.SRVRecordObservation{
		Name:     res.obs.Name,
		Target:   res.obs.Target,
		Priority: res.obs.Priority,
		Weight:   res.obs.Weight,
		Port:     res.obs.Port,
		Comment:  res.obs.Comment,
		TTL:      res.obs.TTL,
		UseTTL:   res.obs.UseTTL,
		ExtAttrs: res.obs.ExtAttrs,
		View:     res.obs.View,
		Ref:      res.obs.Ref,
		Zone:     res.obs.Zone,
	}
	cr.Status.AtProvider.ID = res.obs.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(p.Name, p.Target, p.Comment, p.Priority, p.Weight, p.Port, p.TTL, p.UseTTL, p.ExtAttrs, res.rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new SRVRecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createSRVRecord(e.objMgr, strOrEmpty(p.View), p.Name, p.Target, p.Comment, p.Priority, p.Weight, p.Port, p.TTL, p.UseTTL, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateSRVRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable SRVRecord fields. View (immutable) is never
// sent — see updateSRVRecord. Because name/target/priority/weight/port
// are all _ref-mutating, the external-name annotation is refreshed
// whenever the WAPI response returns a different _ref than the one used
// to issue the request (UNSTABLE _ref). Every call re-asserts the
// identity stamp.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateSRVRecord(e.objMgr, externalID, p.Name, p.Target, p.Comment, p.Priority, p.Weight, p.Port, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateSRVRecord)
	}

	// See clusterExternal.Update — UpdateSRVRecord always returns the
	// object's current _ref, and any _ref-mutating field changing mints a
	// new one.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the SRVRecord, resolving through the shared identity
// ladder first.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteSRVRecordIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.SRVRecord] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.SRVRecord]    = &namespacedExternal{}
)

// setupNamespacedSRVRecord wires the namespaced SRVRecord reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedSRVRecord(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.SRVRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced SRVRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.SRVRecord](driftdetection.WrapConnector[*namespacedv1alpha1.SRVRecord](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("SRVRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.SRVRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
