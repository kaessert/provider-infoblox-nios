package hostrecord

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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/hostrecord/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
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

	creds, err := extractCredentials(ctx, c.kube, pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, "")
	if err != nil {
		return nil, err
	}

	hc, err := newHostRecordClient(creds)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{client: hc}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.HostRecord].
type clusterExternal struct {
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

// Observe fetches the HostRecord from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling getHostRecordByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := getHostRecordByRef(e.client.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveHostRecord)
	}

	o := observeFromHostRecord(externalID, rec)
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

	localIpv4 := clusterIpv4AddrsToValues(p.Ipv4Addrs)
	localIpv6 := clusterIpv6AddrsToValues(p.Ipv6Addrs)
	lateInit := lateInitialize(&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs, &p.View, &p.ConfigureForDNS, &p.Disable, &p.Aliases, &localIpv4, &localIpv6, rec)
	if lateInit {
		p.Ipv4Addrs = clusterIpv4AddrsFromValues(localIpv4)
		p.Ipv6Addrs = clusterIpv6AddrsFromValues(localIpv6)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(clusterCompareFields(p), rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new HostRecord and records the server-assigned _ref
// as the external name. Dispatches to one of three provisioning
// strategies — see provisionHostRecord.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalCreation, error) {
	p := &cr.Spec.ForProvider
	rec, err := provisionHostRecord(e.client.objMgr, clusterCompareFields(p), p.NetworkView, p.Ipv4Cidr, p.Ipv6Cidr, p.FilterParams, p.IpAddressType)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateHostRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable HostRecord fields. networkView (immutable) is
// never sent — see updateHostRecord.
func (e *clusterExternal) Update(_ context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalUpdate, error) {
	p := &cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateHostRecord(e.client.objMgr, externalID, clusterCompareFields(p))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateHostRecord)
	}

	// UpdateHostRecord always returns the object's current _ref. Live
	// verification against a real NIOS Grid Manager confirmed that
	// renaming a host record (or changing its DNS view) changes its
	// _ref and the old _ref immediately 404s — so the external-name
	// annotation must be refreshed here.
	if rec.Ref != "" && rec.Ref != externalID {
		meta.SetExternalName(cr, rec.Ref)
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the HostRecord. A 404 is treated as already-deleted
// (idempotent).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.HostRecord) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteHostRecord(e.client.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteHostRecord)
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
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
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
		managed.WithTypedExternalConnector[*clusterv1alpha1.HostRecord](&clusterConnector{
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
