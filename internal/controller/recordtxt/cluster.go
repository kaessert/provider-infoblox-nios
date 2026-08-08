package recordtxt

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordtxt/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/convergence"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/readrouting"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const clusterControllerName = "cluster-recordtxt.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordtxt.infobloxnios.crossplane.io,resources=txtrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordtxt.infobloxnios.crossplane.io,resources=txtrecords/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.TXTRecord].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
	// recorder emits Kubernetes Warning events for read-routing fallback
	// transitions (see readrouting.Router's emitWarning). nil in this
	// package's white-box tests that build clusterConnector directly.
	recorder event.Recorder
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.TXTRecord) (managed.TypedExternalClient[*clusterv1alpha1.TXTRecord], error) {
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
		router:   readrouting.WithRecorder(conn.Router, c.recorder),
	}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.TXTRecord].
type clusterExternal struct {
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

// Observe resolves the TXTRecord through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	fqdn := convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name))
	readFrom, annotationChanged := e.router.BeginObserve(ctx, cr, e.conn, fqdn, strOrEmpty(p.View), true)

	res, err := observeTXTRecord(ctx, readFrom, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveTXTRecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cr.Status.AtProvider = clusterv1alpha1.TXTRecordObservation{
		Name:     res.obs.Name,
		Text:     res.obs.Text,
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

	upToDate := isUpToDate(p.Name, p.Text, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, res.rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:   true,
		ResourceUpToDate: upToDate,
		// annotationChanged folds the read-routing gate's own annotation
		// mutation into the same persistence path already used for
		// res.lateInit.
		ResourceLateInitialized: res.lateInit || annotationChanged,
	}, nil
}

// Create provisions a new TXTRecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createTXTRecord(e.objMgr, p.View, p.Name, p.Text, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateTXTRecord)
	}

	meta.SetExternalName(cr, rec.Ref)

	fqdn := convergence.ZoneFQDNFromRecordName(strOrEmpty(p.Name))
	if err := e.router.RecordWrite(ctx, e.kube, cr, fqdn, strOrEmpty(p.View)); err != nil {
		return managed.ExternalCreation{}, err
	}

	return managed.ExternalCreation{}, nil
}

// Update patches the mutable TXTRecord fields (PUT partial/merge
// semantics — the WAPI merges the sent fields onto the existing object).
// View (immutable) is never sent — see updateTXTRecord. name/text
// changes cause the WAPI to assign a new _ref, so it must be
// re-captured from the response. Every call re-asserts the identity
// stamp.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateTXTRecord(e.objMgr, externalID, p.Name, p.Text, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateTXTRecord)
	}

	// _ref instability: when name or text changes, the WAPI returns a
	// NEW _ref for the object. The external-name annotation must be
	// refreshed here or the next reconcile's identity resolution will
	// need to fall through to the search path.
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

// Delete removes the TXTRecord, resolving through the shared identity
// ladder first.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteTXTRecordIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.TXTRecord] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.TXTRecord]    = &clusterExternal{}
)

// setupClusterTXTRecord wires the cluster-scoped TXTRecord reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupClusterTXTRecord(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.TXTRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster TXTRecord state recorder")
		}
	}

	//nolint:staticcheck // event.NewAPIRecorder still requires the deprecated record.EventRecorder type; no replacement exists yet in this crossplane-runtime version.
	recorder := event.NewAPIRecorder(mgr.GetEventRecorderFor(name))

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.TXTRecord](driftdetection.WrapConnector[*clusterv1alpha1.TXTRecord](&clusterConnector{
			kube:     mgr.GetClient(),
			usage:    resource.NewLegacyProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("TXTRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.TXTRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
