package recordns

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
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordns/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
)

const clusterControllerName = "cluster-recordns.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=recordns.infobloxnios.crossplane.io,resources=nsrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=recordns.infobloxnios.crossplane.io,resources=nsrecords/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.NSRecord].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI
// ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.NSRecord) (managed.TypedExternalClient[*clusterv1alpha1.NSRecord], error) {
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

	// SSLVerify governs TLS verification for all endpoints (primary and
	// read); it is a ProviderConfig policy field, not a per-credential
	// Secret key. Defaults to true (secure) when unset — the kubebuilder
	// default handles the YAML path, but Go code must handle the
	// nil-pointer case too (e.g. objects created before this field
	// existed).
	sslVerify := true
	if pc.Spec.SSLVerify != nil {
		sslVerify = *pc.Spec.SSLVerify
	}

	mgrConn, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{kube: c.kube, objMgr: mgrConn.Manager, conn: mgrConn.Connector}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.NSRecord].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector nsRecordExistsByNaturalKey
	// searches against directly — it needs visibility into the match
	// count that objMgr's typed getters hide. See that helper's doc.
	conn ibclient.IBConnector
}

// clusterAddressesToShared converts the cluster-scoped NSRecordAddress
// list into the shared nsRecordAddress representation used by the
// controller.go helpers.
func clusterAddressesToShared(addrs []clusterv1alpha1.NSRecordAddress) []nsRecordAddress {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]nsRecordAddress, len(addrs))
	for i, a := range addrs {
		out[i] = nsRecordAddress{Address: a.Address, AutoCreatePtr: a.AutoCreatePtr}
	}
	return out
}

// clusterAddressesFromShared is the inverse of clusterAddressesToShared.
func clusterAddressesFromShared(addrs []nsRecordAddress) []clusterv1alpha1.NSRecordAddress {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.NSRecordAddress, len(addrs))
	for i, a := range addrs {
		out[i] = clusterv1alpha1.NSRecordAddress{Address: a.Address, AutoCreatePtr: a.AutoCreatePtr}
	}
	return out
}

// clusterCloudInfoFromShared converts the shared nsCloudInfo mirror into
// the cluster-scoped NSRecordCloudInfo type.
func clusterCloudInfoFromShared(ci *nsCloudInfo) *clusterv1alpha1.NSRecordCloudInfo {
	if ci == nil {
		return nil
	}
	out := &clusterv1alpha1.NSRecordCloudInfo{
		DelegatedScope: ci.DelegatedScope,
		DelegatedRoot:  ci.DelegatedRoot,
		OwnedByAdaptor: ci.OwnedByAdaptor,
		Usage:          ci.Usage,
		Tenant:         ci.Tenant,
		MgmtPlatform:   ci.MgmtPlatform,
		AuthorityType:  ci.AuthorityType,
	}
	if ci.DelegatedMember != nil {
		out.DelegatedMember = &clusterv1alpha1.NSRecordCloudInfoDelegatedMember{
			Ipv4Addr: ci.DelegatedMember.Ipv4Addr,
			Ipv6Addr: ci.DelegatedMember.Ipv6Addr,
			Name:     ci.DelegatedMember.Name,
		}
	}
	return out
}

// Observe fetches the NSRecord from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.NSRecord) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetNSRecordByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.objMgr.GetNSRecordByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := nsRecordExistsByNaturalKey(e.conn, cr.Spec.ForProvider.Name, cr.Spec.ForProvider.View, cr.Spec.ForProvider.Nameserver)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveNSRecord)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveNSRecord)
	}

	o := observeFromRecordNS(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.NSRecordObservation{
		Name:             o.Name,
		Nameserver:       o.Nameserver,
		View:             o.View,
		Addresses:        clusterAddressesFromShared(o.Addresses),
		MsDelegationName: o.MsDelegationName,
		Ref:              o.Ref,
		Zone:             o.Zone,
		Creator:          o.Creator,
		DNSName:          o.DNSName,
		LastQueried:      o.LastQueried,
		CloudInfo:        clusterCloudInfoFromShared(o.CloudInfo),
		Policy:           o.Policy,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.MsDelegationName, rec)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Nameserver, p.MsDelegationName, clusterAddressesToShared(p.Addresses), rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new NSRecord and records the server-assigned _ref
// as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.NSRecord) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createNSRecord(e.objMgr, p.Name, p.Nameserver, p.View, p.MsDelegationName, clusterAddressesToShared(p.Addresses))
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateNSRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable NSRecord fields (nameserver, addresses,
// msDelegationName). name and view (immutable) are never sent — see
// updateNSRecord.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.NSRecord) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateNSRecord(e.objMgr, externalID, p.Nameserver, p.MsDelegationName, clusterAddressesToShared(p.Addresses))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateNSRecord)
	}

	// UpdateNSRecord returns the object's current _ref. Live verification
	// against a real Grid found the _ref for NS records is unstable — a
	// nameserver/addresses change can cause NIOS to mutate the _ref — so
	// the external-name annotation must be refreshed here whenever the
	// returned ref differs from what we sent.
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
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.NSRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteNSRecordResolving404(e.objMgr, e.conn, externalID, p.Name, p.View, p.Nameserver); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.NSRecord] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.NSRecord]    = &clusterExternal{}
)

// setupClusterNSRecord wires the cluster-scoped NSRecord reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupClusterNSRecord(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.NSRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster NSRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.NSRecord](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("NSRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.NSRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
