package recordns

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

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordns/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-recordns.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordns.infobloxnios.m.crossplane.io,resources=nsrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordns.infobloxnios.m.crossplane.io,resources=nsrecords/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.NSRecord].
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
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.NSRecord) (managed.TypedExternalClient[*namespacedv1alpha1.NSRecord], error) {
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

	mgrConn, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{kube: c.kube, objMgr: mgrConn.Manager, conn: mgrConn.Connector}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.NSRecord].
type namespacedExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector resolveNSRecordByNaturalKey
	// searches against directly — it needs visibility into the match
	// count that objMgr's typed getters hide. See that helper's doc.
	conn ibclient.IBConnector
}

// namespacedAddressesToShared converts the namespaced-scoped
// NSRecordAddress list into the shared nsRecordAddress representation
// used by the controller.go helpers.
func namespacedAddressesToShared(addrs []namespacedv1alpha1.NSRecordAddress) []nsRecordAddress {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]nsRecordAddress, len(addrs))
	for i, a := range addrs {
		out[i] = nsRecordAddress{Address: a.Address, AutoCreatePtr: a.AutoCreatePtr}
	}
	return out
}

// namespacedAddressesFromShared is the inverse of namespacedAddressesToShared.
func namespacedAddressesFromShared(addrs []nsRecordAddress) []namespacedv1alpha1.NSRecordAddress {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.NSRecordAddress, len(addrs))
	for i, a := range addrs {
		out[i] = namespacedv1alpha1.NSRecordAddress{Address: a.Address, AutoCreatePtr: a.AutoCreatePtr}
	}
	return out
}

// namespacedCloudInfoFromShared converts the shared nsCloudInfo mirror
// into the namespaced-scoped NSRecordCloudInfo type.
func namespacedCloudInfoFromShared(ci *nsCloudInfo) *namespacedv1alpha1.NSRecordCloudInfo {
	if ci == nil {
		return nil
	}
	out := &namespacedv1alpha1.NSRecordCloudInfo{
		DelegatedScope: ci.DelegatedScope,
		DelegatedRoot:  ci.DelegatedRoot,
		OwnedByAdaptor: ci.OwnedByAdaptor,
		Usage:          ci.Usage,
		Tenant:         ci.Tenant,
		MgmtPlatform:   ci.MgmtPlatform,
		AuthorityType:  ci.AuthorityType,
	}
	if ci.DelegatedMember != nil {
		out.DelegatedMember = &namespacedv1alpha1.NSRecordCloudInfoDelegatedMember{
			Ipv4Addr: ci.DelegatedMember.Ipv4Addr,
			Ipv6Addr: ci.DelegatedMember.Ipv6Addr,
			Name:     ci.DelegatedMember.Name,
		}
	}
	return out
}

// Observe resolves the NSRecord through its natural key (name, view,
// nameserver — see the package doc for why this tuple is sound identity
// evidence) and compares the result against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.NSRecord) (managed.ExternalObservation, error) {
	ref := observeRefFor(cr.GetName(), meta.GetExternalName(cr))

	result, err := resolveNSRecordForObserve(e.objMgr, e.conn, ref, cr.Spec.ForProvider.Name, cr.Spec.ForProvider.View, cr.Spec.ForProvider.Nameserver)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveNSRecord)
	}
	if result == nil {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	rec := result.rec

	// A ref recovered through the natural-key search (rather than
	// resolved directly) must be persisted through ResourceLateInitialized
	// below — that is the one path crossplane-runtime's managed
	// reconciler actually writes an Observe-time external-name change
	// back to the API server.
	if result.refreshed {
		meta.SetExternalName(cr, rec.Ref)
	}

	o := observeFromRecordNS(rec.Ref, rec)
	cr.Status.AtProvider = namespacedv1alpha1.NSRecordObservation{
		Name:             o.Name,
		Nameserver:       o.Nameserver,
		View:             o.View,
		Addresses:        namespacedAddressesFromShared(o.Addresses),
		MsDelegationName: o.MsDelegationName,
		Ref:              o.Ref,
		Zone:             o.Zone,
		Creator:          o.Creator,
		DNSName:          o.DNSName,
		LastQueried:      o.LastQueried,
		CloudInfo:        namespacedCloudInfoFromShared(o.CloudInfo),
		Policy:           o.Policy,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.MsDelegationName, rec) || result.refreshed

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Nameserver, p.MsDelegationName, namespacedAddressesToShared(p.Addresses), rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new NSRecord and records the server-assigned _ref
// as the external name.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.NSRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createNSRecord(e.objMgr, p.Name, p.Nameserver, p.View, p.MsDelegationName, namespacedAddressesToShared(p.Addresses))
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateNSRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable NSRecord fields (nameserver, addresses,
// msDelegationName). name and view (immutable) are never sent — see
// updateNSRecord.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.NSRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateNSRecord(e.objMgr, externalID, p.Nameserver, p.MsDelegationName, namespacedAddressesToShared(p.Addresses))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateNSRecord)
	}

	// See clusterExternal.Update — UpdateNSRecord returns the object's
	// current _ref, and NS-record _refs are unstable (live-verified).
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the NSRecord. A 404 on the stored _ref is not treated
// as already-deleted by itself — see deleteNSRecordResolving404 — because
// the _ref is a derived handle that rotates whenever an identity field
// changes, and a stale handle 404s exactly like a genuinely deleted
// object.
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.NSRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteNSRecordResolving404(e.objMgr, e.conn, externalID, p.Name, p.View, p.Nameserver); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.NSRecord] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.NSRecord]    = &namespacedExternal{}
)

// setupNamespacedNSRecord wires the namespaced NSRecord reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedNSRecord(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.NSRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced NSRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.NSRecord](driftdetection.WrapConnector[*namespacedv1alpha1.NSRecord](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("NSRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.NSRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
