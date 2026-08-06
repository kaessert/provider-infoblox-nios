package zoneforward

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

	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneforward/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
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

	mgrConn, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{
		kube:     c.kube,
		objMgr:   mgrConn.Manager,
		conn:     mgrConn.Connector,
		endpoint: creds.Host,
	}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.ZoneForward].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key.
	endpoint string
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

// Observe resolves the ZoneForward through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeZoneForward(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Comment, &p.NsGroup, &p.ExternalNsGroup, &p.Disable, &p.ForwardersOnly, &p.ExtAttrs, &p.View, &p.ZoneFormat)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveZoneForward)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec := res.rec
	o := res.obs
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
		ExtAttrs:          o.ExtAttrs,
		Ref:               o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(
		clusterNameServersToSDK(p.ForwardTo),
		clusterForwardingServersToSDK(p.ForwardingServers),
		p.Comment, p.NsGroup, p.ExternalNsGroup,
		p.Disable, p.ForwardersOnly,
		p.ExtAttrs,
		rec,
	) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new ZoneForward, stamping the managed resource's
// own uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createZoneForward(e.objMgr, p.Fqdn, p.View, p.ZoneFormat, p.Comment, p.NsGroup, p.ExternalNsGroup, p.Disable, p.ForwardersOnly, clusterNameServersToSDK(p.ForwardTo), clusterForwardingServersToSDK(p.ForwardingServers), p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateZoneForward)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ZoneForward fields. fqdn, view, and
// zoneFormat (immutable) are never sent — see updateZoneForward. Every
// call re-asserts the identity stamp since a WAPI PUT carrying extattrs
// replaces the whole map rather than merging it.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateZoneForward(e.objMgr, externalID, p.Comment, p.NsGroup, p.ExternalNsGroup, p.Disable, p.ForwardersOnly, clusterNameServersToSDK(p.ForwardTo), clusterForwardingServersToSDK(p.ForwardingServers), p.ExtAttrs, string(cr.GetUID()))
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

// Delete removes the ZoneForward, resolving through the shared identity
// ladder first — see deleteZoneForwardIdentity for the full ownership-
// verification rules a stale or rotated _ref must satisfy before a
// delete is issued.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.ZoneForward) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteZoneForwardIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
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
		if err := mgr.Add(statemetrics.NewResilientRecorder(
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
