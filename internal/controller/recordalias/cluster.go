package recordalias

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordalias/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

const clusterControllerName = "cluster-recordalias.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordalias.infobloxnios.crossplane.io,resources=aliasrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordalias.infobloxnios.crossplane.io,resources=aliasrecords/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.AliasRecord].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.AliasRecord) (managed.TypedExternalClient[*clusterv1alpha1.AliasRecord], error) {
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

	objMgr, conn, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{kube: c.kube, objMgr: objMgr, conn: conn}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.AliasRecord].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	conn   ibclient.IBConnector
}

// Observe fetches the AliasRecord from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.AliasRecord) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetAliasRecordByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.objMgr.GetAliasRecordByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := aliasRecordExistsByNaturalKey(e.conn, cr.Spec.ForProvider.View, cr.Spec.ForProvider.Name, cr.Spec.ForProvider.TargetName, cr.Spec.ForProvider.TargetType)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveAliasRec)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveAliasRec)
	}

	o := observeFromRecordAlias(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.AliasRecordObservation{
		Name:       o.Name,
		TargetName: o.TargetName,
		TargetType: o.TargetType,
		View:       o.View,
		Comment:    o.Comment,
		Disable:    o.Disable,
		TTL:        o.TTL,
		UseTTL:     o.UseTTL,
		ExtAttrs:   o.ExtAttrs,
		Ref:        o.Ref,
		Zone:       o.Zone,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.Comment, &p.Disable, &p.TTL, &p.UseTTL, &p.ExtAttrs, rec)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.TargetName, p.TargetType, p.Comment, p.Disable, p.TTL, p.UseTTL, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new AliasRecord and records the server-assigned
// _ref as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.AliasRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createAliasRecord(e.objMgr, p.Name, p.View, p.TargetName, p.TargetType, p.Comment, p.Disable, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateAliasRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable AliasRecord fields. View (soft-immutable) is
// never sent — see updateAliasRecord.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.AliasRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	newRef, err := updateAliasRecord(e.conn, externalID, p.Name, p.TargetName, p.TargetType, p.Comment, p.Disable, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateAliasRecord)
	}

	// The update response always carries the object's current _ref, and
	// name is _ref-mutating — renaming an alias changes its _ref and the
	// old _ref immediately 404s — so the external-name annotation must be
	// refreshed here whenever it differs.
	if newRef != "" && newRef != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, newRef); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the AliasRecord. A 404 on the stored _ref is not
// treated as already-deleted by itself — see
// deleteAliasRecordResolving404 — because the _ref is a derived handle
// that rotates whenever an identity field changes, and a stale handle
// 404s exactly like a genuinely deleted object.
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.AliasRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteAliasRecordResolving404(e.objMgr, e.conn, externalID, p.View, p.Name, p.TargetName, p.TargetType); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.AliasRecord] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.AliasRecord]    = &clusterExternal{}
)

// setupClusterAliasRecord wires the cluster-scoped AliasRecord reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupClusterAliasRecord(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.AliasRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster AliasRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.AliasRecord](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("AliasRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.AliasRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
