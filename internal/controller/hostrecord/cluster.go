package hostrecord

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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/hostrecord/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const clusterControllerName = "cluster-hostrecord.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=hostrecord.infobloxnios.crossplane.io,resources=hostrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hostrecord.infobloxnios.crossplane.io,resources=hostrecords/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.HostRecord].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI client.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.HostRecord) (managed.TypedExternalClient[*clusterv1alpha1.HostRecord], error) {
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

	hc := newHostRecordClient(conn.Connector)
	hc.endpoint = conn.Endpoint

	return &clusterExternal{kube: c.kube, client: hc}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.HostRecord].
type clusterExternal struct {
	kube   k8sclient.Client
	client *hostRecordClient
}

// clusterCompareFields snapshots the mutable ForProvider fields of a
// cluster-scoped HostRecord into the scope-agnostic comparison struct.
func clusterCompareFields(p *clusterv1alpha1.HostRecordParameters) hostRecordCompareFields {
	return hostRecordCompareFields{
		Name:            p.Name,
		Ipv4Addrs:       clusterIpv4AddrsToValues(p.Ipv4Addrs),
		Ipv6Addrs:       clusterIpv6AddrsToValues(p.Ipv6Addrs),
		View:            p.View,
		Aliases:         p.Aliases,
		ConfigureForDNS: p.ConfigureForDNS,
		Comment:         p.Comment,
		Disable:         p.Disable,
		TTL:             p.TTL,
		UseTTL:          p.UseTTL,
		ExtAttrs:        p.ExtAttrs,
	}
}

func clusterIpv4AddrsToValues(addrs []clusterv1alpha1.HostRecordIpv4Addr) []ipv4AddrValue {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]ipv4AddrValue, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, ipv4AddrValue{
			Ipv4Addr:         a.Ipv4Addr,
			MAC:              a.MAC,
			ConfigureForDHCP: a.ConfigureForDHCP,
			NextServer:       a.NextServer,
		})
	}
	return out
}

func clusterIpv4AddrsFromValues(vals []ipv4AddrValue) []clusterv1alpha1.HostRecordIpv4Addr {
	if len(vals) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.HostRecordIpv4Addr, 0, len(vals))
	for _, v := range vals {
		out = append(out, clusterv1alpha1.HostRecordIpv4Addr{
			Ipv4Addr:         v.Ipv4Addr,
			MAC:              v.MAC,
			ConfigureForDHCP: v.ConfigureForDHCP,
			NextServer:       v.NextServer,
		})
	}
	return out
}

func clusterIpv6AddrsToValues(addrs []clusterv1alpha1.HostRecordIpv6Addr) []ipv6AddrValue {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]ipv6AddrValue, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, ipv6AddrValue{
			Ipv6Addr:         a.Ipv6Addr,
			Duid:             a.Duid,
			ConfigureForDHCP: a.ConfigureForDHCP,
		})
	}
	return out
}

func clusterIpv6AddrsFromValues(vals []ipv6AddrValue) []clusterv1alpha1.HostRecordIpv6Addr {
	if len(vals) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.HostRecordIpv6Addr, 0, len(vals))
	for _, v := range vals {
		out = append(out, clusterv1alpha1.HostRecordIpv6Addr{
			Ipv6Addr:         v.Ipv6Addr,
			Duid:             v.Duid,
			ConfigureForDHCP: v.ConfigureForDHCP,
		})
	}
	return out
}

// Observe resolves the HostRecord through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalObservation, error) {
	res, err := observeHostRecordIdentity(ctx, e.client.conn, e.client.prober, e.client.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()))
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveHostRecord)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	rec := res.rec

	o := observeFromHostRecord(rec.Ref, rec)
	p := &cr.Spec.ForProvider
	cr.Status.AtProvider = clusterv1alpha1.HostRecordObservation{
		Name:            o.Name,
		Ipv4Addrs:       clusterIpv4AddrsFromValues(o.Ipv4Addrs),
		Ipv6Addrs:       clusterIpv6AddrsFromValues(o.Ipv6Addrs),
		NetworkView:     o.NetworkView,
		View:            o.View,
		Aliases:         o.Aliases,
		ConfigureForDNS: o.ConfigureForDNS,
		Comment:         o.Comment,
		Disable:         o.Disable,
		TTL:             o.TTL,
		UseTTL:          o.UseTTL,
		ExtAttrs:        o.ExtAttrs,
		Ref:             o.Ref,
		Zone:            o.Zone,
		DNSName:         o.DNSName,
		DNSAliases:      o.DNSAliases,
		// ipv4Cidr/ipv6Cidr/filterParams/ipAddressType are create-time-only
		// allocation parameters WAPI never returns from GetHostRecordByRef
		// — echoed from spec for observability rather than left unpopulated.
		Ipv4Cidr:      p.Ipv4Cidr,
		Ipv6Cidr:      p.Ipv6Cidr,
		FilterParams:  p.FilterParams,
		IpAddressType: p.IpAddressType,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	localIpv4 := clusterIpv4AddrsToValues(p.Ipv4Addrs)
	localIpv6 := clusterIpv6AddrsToValues(p.Ipv6Addrs)
	lateInit := lateInitialize(&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs, &p.View, &p.ConfigureForDNS, &p.Disable, &p.Aliases, &localIpv4, &localIpv6, rec)
	if lateInit {
		p.Ipv4Addrs = clusterIpv4AddrsFromValues(localIpv4)
		p.Ipv6Addrs = clusterIpv6AddrsFromValues(localIpv6)
	}
	if res.refreshedRef != "" {
		lateInit = true
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(clusterCompareFields(p), rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new HostRecord, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
// Dispatches to one of three provisioning strategies — see
// provisionHostRecord.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalCreation, error) {
	p := &cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	cf := clusterCompareFields(p)
	if err := validateHostRecordAllocation(cf, p.Ipv4Cidr, p.Ipv6Cidr, p.FilterParams, p.IpAddressType); err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateHostRecord)
	}
	if err := ensureIdentityPrerequisite(ctx, e.client.prober, e.client.conn, e.client.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := provisionHostRecord(e.client.objMgr, cf, p.NetworkView, p.Ipv4Cidr, p.Ipv6Cidr, p.FilterParams, p.IpAddressType, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateHostRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable HostRecord fields. networkView (immutable) is
// never sent — see updateHostRecord. Every call re-asserts the identity
// stamp.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalUpdate, error) {
	p := &cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.client.prober, e.client.conn, e.client.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateHostRecord(e.client.objMgr, externalID, clusterCompareFields(p), string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateHostRecord)
	}

	// UpdateHostRecord always returns the object's current _ref. Live
	// verification against a real NIOS Grid Manager confirmed that
	// renaming a host record (or changing its DNS view) changes its
	// _ref and the old _ref immediately 404s — so the external-name
	// annotation must be refreshed here.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the HostRecord, resolving through the shared identity
// ladder first.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteHostRecordIdentity(ctx, e.client.conn, e.client.objMgr, e.client.prober, e.client.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.HostRecord] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.HostRecord]    = &clusterExternal{}
)

// setupClusterHostRecord wires the cluster-scoped HostRecord reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupClusterHostRecord(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.HostRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster HostRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.HostRecord](driftdetection.WrapConnector[*clusterv1alpha1.HostRecord](&clusterConnector{
			kube:  mgr.GetClient(),
			usage: resource.NewLegacyProviderConfigUsageTracker(mgr.GetClient(), &apisv1alpha1.ProviderConfigUsage{}),
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("HostRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.HostRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
