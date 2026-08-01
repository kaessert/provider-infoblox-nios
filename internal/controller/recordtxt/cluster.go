package recordtxt

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordtxt/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
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

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.TXTRecord].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
}

// Observe fetches the TXTRecord from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetTXTRecordByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.objMgr.GetTXTRecordByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := txtRecordExistsByNaturalKey(e.objMgr, cr.Spec.ForProvider.View, cr.Spec.ForProvider.Name, cr.Spec.ForProvider.Text)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveTXTRecord)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveTXTRecord)
	}

	o := observeFromRecordTXT(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.TXTRecordObservation{
		Name:     o.Name,
		Text:     o.Text,
		Comment:  o.Comment,
		TTL:      o.TTL,
		UseTTL:   o.UseTTL,
		ExtAttrs: o.ExtAttrs,
		View:     o.View,
		Ref:      o.Ref,
		Zone:     o.Zone,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs, rec)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.Text, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new TXTRecord and records the server-assigned _ref
// as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createTXTRecord(e.objMgr, p.View, p.Name, p.Text, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateTXTRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable TXTRecord fields (PUT partial/merge
// semantics — the WAPI merges the sent fields onto the existing object).
// View (immutable) is never sent — see updateTXTRecord. name/text
// changes cause the WAPI to assign a new _ref, so it must be
// re-captured from the response.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateTXTRecord(e.objMgr, externalID, p.Name, p.Text, p.Comment, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateTXTRecord)
	}

	// _ref instability: when name or text changes, the WAPI returns a
	// NEW _ref for the object. The external-name annotation must be
	// refreshed here or the next reconcile's GetTXTRecordByRef call will
	// 404 against the now-stale old ref.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the TXTRecord. A 404 on the stored _ref is not treated
// as already-deleted by itself — see deleteTXTRecordResolving404 —
// because the _ref is a derived handle that rotates whenever an identity
// field changes, and a stale handle 404s exactly like a genuinely
// deleted object.
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.TXTRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteTXTRecordResolving404(e.objMgr, externalID, p.View, p.Name, p.Text); err != nil {
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
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.TXTRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster TXTRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.TXTRecord](&clusterConnector{
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
