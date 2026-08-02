package recordmx

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
	"github.com/crossplane/crossplane-runtime/v2/pkg/statemetrics"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordmx/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-recordmx.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordmx.infobloxnios.crossplane.io,resources=mxrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordmx.infobloxnios.crossplane.io,resources=mxrecords/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.MXRecord].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.MXRecord) (managed.TypedExternalClient[*clusterv1alpha1.MXRecord], error) {
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

	return &clusterExternal{kube: c.kube, objMgr: objMgr.Manager, conn: objMgr.Connector, endpoint: creds.Host}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.MXRecord].
type clusterExternal struct {
	kube     k8sclient.Client
	objMgr   ibclient.IBObjectManager
	conn     ibclient.IBConnector
	prober   *identity.Prober
	endpoint string
}

// Observe resolves the MXRecord through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.MXRecord) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeMXRecord(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveMXRecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	cr.Status.AtProvider = clusterv1alpha1.MXRecordObservation{
		Name:          res.obs.Name,
		MailExchanger: res.obs.MailExchanger,
		Preference:    res.obs.Preference,
		Comment:       res.obs.Comment,
		TTL:           res.obs.TTL,
		UseTTL:        res.obs.UseTTL,
		ExtAttrs:      res.obs.ExtAttrs,
		View:          res.obs.View,
		Ref:           res.obs.Ref,
		Zone:          res.obs.Zone,
	}
	cr.Status.AtProvider.ID = res.obs.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	upToDate := isUpToDate(p.Name, p.MailExchanger, p.Preference, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, res.rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new MXRecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.MXRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createMXRecord(e.objMgr, p.View, p.Name, p.MailExchanger, p.Preference, p.TTL, p.UseTTL, p.Comment, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateMXRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable MXRecord fields. View is echoed back
// unchanged (see updateMXRecord's doc comment) — it is never treated as
// an update target. Every call re-asserts the identity stamp.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.MXRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateMXRecord(e.objMgr, externalID, p.View, p.Name, p.MailExchanger, p.Preference, p.TTL, p.UseTTL, p.Comment, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateMXRecord)
	}

	// UpdateMXRecord always returns the object's current _ref. Renaming
	// (or changing mailExchanger/preference) mutates the _ref, so the
	// external-name annotation must be refreshed here.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the MXRecord, resolving through the shared identity
// ladder first.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.MXRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteMXRecordIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.MXRecord] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.MXRecord]    = &clusterExternal{}
)

// setupClusterMXRecord wires the cluster-scoped MXRecord reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupClusterMXRecord(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.MXRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster MXRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.MXRecord](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("MXRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.MXRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
