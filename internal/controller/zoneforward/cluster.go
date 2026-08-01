package zoneforward

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

	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneforward/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

const clusterControllerName = "cluster-zoneforward.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=zoneforward.infobloxnios.crossplane.io,resources=zoneforwards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zoneforward.infobloxnios.crossplane.io,resources=zoneforwards/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.ZoneForward].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI
// ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.ZoneForward) (managed.TypedExternalClient[*clusterv1alpha1.ZoneForward], error) {
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

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.ZoneForward].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
}

// clusterNameServersToSDK converts the CRD's NameServer list into the
// SDK's []ibclient.NameServer shape.
func clusterNameServersToSDK(in []clusterv1alpha1.NameServer) []ibclient.NameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]ibclient.NameServer, len(in))
	for i, ns := range in {
		out[i] = ibclient.NameServer{Name: strOrEmpty(ns.Name), Address: strOrEmpty(ns.Address)}
	}
	return out
}

// clusterNameServersFromSDK converts the SDK's []ibclient.NameServer
// shape back into the CRD's NameServer list.
func clusterNameServersFromSDK(in []ibclient.NameServer) []clusterv1alpha1.NameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.NameServer, len(in))
	for i, ns := range in {
		name := ns.Name
		addr := ns.Address
		out[i] = clusterv1alpha1.NameServer{Name: &name, Address: &addr}
	}
	return out
}

// clusterForwardingServersToSDK converts the CRD's ForwardingServer list
// into the SDK's []*ibclient.Forwardingmemberserver shape.
func clusterForwardingServersToSDK(in []clusterv1alpha1.ForwardingServer) []*ibclient.Forwardingmemberserver {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Forwardingmemberserver, len(in))
	for i, fs := range in {
		out[i] = &ibclient.Forwardingmemberserver{
			Name:                  strOrEmpty(fs.Name),
			ForwardersOnly:        boolOrFalse(fs.ForwardersOnly),
			ForwardTo:             ibclient.NullableNameServers{NameServers: clusterNameServersToSDK(fs.ForwardTo)},
			UseOverrideForwarders: boolOrFalse(fs.UseOverrideForwarders),
		}
	}
	return out
}

// clusterForwardingServersFromSDK converts the SDK's
// []*ibclient.Forwardingmemberserver shape back into the CRD's
// ForwardingServer list.
func clusterForwardingServersFromSDK(in []*ibclient.Forwardingmemberserver) []clusterv1alpha1.ForwardingServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.ForwardingServer, 0, len(in))
	for _, fs := range in {
		if fs == nil {
			continue
		}
		name := fs.Name
		forwardersOnly := fs.ForwardersOnly
		useOverride := fs.UseOverrideForwarders
		out = append(out, clusterv1alpha1.ForwardingServer{
			Name:                  &name,
			ForwardersOnly:        &forwardersOnly,
			ForwardTo:             clusterNameServersFromSDK(fs.ForwardTo.NameServers),
			UseOverrideForwarders: &useOverride,
		})
	}
	return out
}

// Observe fetches the ZoneForward from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetZoneForwardByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := e.objMgr.GetZoneForwardByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := zoneForwardExistsByNaturalKey(e.objMgr, cr.Spec.ForProvider.Fqdn, cr.Spec.ForProvider.View)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveZoneForward)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveZoneForward)
	}

	o := observeFromZoneForward(externalID, rec)
	cr.Status.AtProvider = clusterv1alpha1.ZoneForwardObservation{
		Fqdn:              o.Fqdn,
		ForwardTo:         clusterNameServersFromSDK(rec.ForwardTo.NameServers),
		View:              o.View,
		ZoneFormat:        o.ZoneFormat,
		Comment:           o.Comment,
		Disable:           o.Disable,
		ForwardersOnly:    o.ForwardersOnly,
		NsGroup:           o.NsGroup,
		ExternalNsGroup:   o.ExternalNsGroup,
		ForwardingServers: clusterForwardingServersFromSDK(nullableForwardingServersSlice(rec.ForwardingServers)),
		Extattrs:          o.ExtAttrs,
		Ref:               o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.Comment, &p.NsGroup, &p.ExternalNsGroup, &p.Disable, &p.ForwardersOnly, &p.Extattrs, &p.View, &p.ZoneFormat, rec)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists: true,
		ResourceUpToDate: isUpToDate(
			clusterNameServersToSDK(p.ForwardTo),
			clusterForwardingServersToSDK(p.ForwardingServers),
			p.Comment, p.NsGroup, p.ExternalNsGroup,
			p.Disable, p.ForwardersOnly,
			p.Extattrs,
			rec,
		),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new ZoneForward and records the server-assigned
// _ref as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	rec, err := createZoneForward(e.objMgr, p.Fqdn, p.View, p.ZoneFormat, p.Comment, p.NsGroup, p.ExternalNsGroup, p.Disable, p.ForwardersOnly, clusterNameServersToSDK(p.ForwardTo), clusterForwardingServersToSDK(p.ForwardingServers), p.Extattrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateZoneForward)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ZoneForward fields. fqdn, view, and
// zoneFormat (immutable) are never sent — see updateZoneForward.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateZoneForward(e.objMgr, externalID, p.Comment, p.NsGroup, p.ExternalNsGroup, p.Disable, p.ForwardersOnly, clusterNameServersToSDK(p.ForwardTo), clusterForwardingServersToSDK(p.ForwardingServers), p.Extattrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateZoneForward)
	}

	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the ZoneForward. A 404 against the stored _ref is not
// treated as already-deleted by itself — see
// deleteZoneForwardResolving404 — because the _ref is a derived handle
// that rotates when fqdn/view changes.
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	p := cr.Spec.ForProvider
	if err := deleteZoneForwardResolving404(e.objMgr, externalID, p.Fqdn, p.View); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.ZoneForward] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.ZoneForward]    = &clusterExternal{}
)

// setupClusterZoneForward wires the cluster-scoped ZoneForward reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupClusterZoneForward(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.ZoneForwardList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster ZoneForward state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.ZoneForward](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("ZoneForward")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.ZoneForward{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
