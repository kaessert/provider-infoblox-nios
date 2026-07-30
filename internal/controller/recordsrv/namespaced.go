package recordsrv

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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordsrv/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
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

	var creds *nioCredentials
	// sslVerify governs TLS verification for all endpoints (primary and
	// read); it is a ProviderConfig/ClusterProviderConfig policy field,
	// not a per-credential Secret key. Defaults to true (secure) when
	// unset — the kubebuilder default handles the YAML path, but Go code
	// must handle the nil-pointer case too (e.g. objects created before
	// this field existed).
	sslVerify := true
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return nil, errors.Wrap(err, errGetPC)
		}
		var err error
		creds, err = extractCredentials(ctx, c.kube, pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, pc.GetNamespace())
		if err != nil {
			return nil, err
		}
		if pc.Spec.SSLVerify != nil {
			sslVerify = *pc.Spec.SSLVerify
		}

	case "ClusterProviderConfig":
		cpc := &apisv1alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name}, cpc); err != nil {
			return nil, errors.Wrap(err, errGetClusterPC)
		}
		var err error
		creds, err = extractCredentials(ctx, c.kube, cpc.Spec.Credentials.Source, cpc.Spec.Credentials.SecretRef, "")
		if err != nil {
			return nil, err
		}
		if cpc.Spec.SSLVerify != nil {
			sslVerify = *cpc.Spec.SSLVerify
		}

	default:
		return nil, errors.Errorf("%s: %s", errUnsupportedKind, ref.Kind)
	}

	objMgr, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{objMgr: objMgr}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.SRVRecord].
type namespacedExternal struct {
	objMgr ibclient.IBObjectManager
}

// Observe fetches the SRVRecord from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.objMgr.GetSRVRecordByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveSRVRecord)
	}

	o := observeFromRecordSRV(externalID, rec)
	cr.Status.AtProvider = namespacedv1alpha1.SRVRecordObservation{
		Name:     o.Name,
		Target:   o.Target,
		Priority: o.Priority,
		Weight:   o.Weight,
		Port:     o.Port,
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
		ResourceUpToDate:        isUpToDate(p.Name, p.Target, p.Comment, p.Priority, p.Weight, p.Port, p.TTL, p.UseTTL, p.ExtAttrs, rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new SRVRecord and records the server-assigned _ref
// as the external name.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createSRVRecord(e.objMgr, strOrEmpty(p.View), p.Name, p.Target, p.Comment, p.Priority, p.Weight, p.Port, p.TTL, p.UseTTL, p.ExtAttrs)
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
// to issue the request (UNSTABLE _ref).
func (e *namespacedExternal) Update(_ context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateSRVRecord(e.objMgr, externalID, p.Name, p.Target, p.Comment, p.Priority, p.Weight, p.Port, p.TTL, p.UseTTL, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateSRVRecord)
	}

	// See clusterExternal.Update — UpdateSRVRecord always returns the
	// object's current _ref, and any _ref-mutating field changing mints a
	// new one.
	if rec.Ref != "" && rec.Ref != externalID {
		meta.SetExternalName(cr, rec.Ref)
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the SRVRecord. A 404 is treated as already-deleted
// (idempotent).
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.SRVRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteSRVRecord(e.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteSRVRecord)
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
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
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
		managed.WithTypedExternalConnector[*namespacedv1alpha1.SRVRecord](&namespacedConnector{
			kube:  mgr.GetClient(),
			usage: resource.NewProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
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
