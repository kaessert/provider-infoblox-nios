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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/hostrecord/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const namespacedControllerName = "namespaced-hostrecord.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=hostrecord.infobloxnios.m.crossplane.io,resources=hostrecords,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=hostrecord.infobloxnios.m.crossplane.io,resources=hostrecords/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.HostRecord].
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
// authenticated WAPI client.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.HostRecord) (managed.TypedExternalClient[*namespacedv1alpha1.HostRecord], error) {
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

	hc, err := newHostRecordClient(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{kube: c.kube, client: hc}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.HostRecord].
type namespacedExternal struct {
	kube   k8sclient.Client
	client *hostRecordClient
}

// namespacedCompareFields snapshots the mutable ForProvider fields of a
// namespaced HostRecord into the scope-agnostic comparison struct.
func namespacedCompareFields(p *namespacedv1alpha1.HostRecordParameters) hostRecordCompareFields {
	return hostRecordCompareFields{
		Name:            p.Name,
		Ipv4Addrs:       namespacedIpv4AddrsToValues(p.Ipv4Addrs),
		Ipv6Addrs:       namespacedIpv6AddrsToValues(p.Ipv6Addrs),
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

func namespacedIpv4AddrsToValues(addrs []namespacedv1alpha1.HostRecordIpv4Addr) []ipv4AddrValue {
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

func namespacedIpv4AddrsFromValues(vals []ipv4AddrValue) []namespacedv1alpha1.HostRecordIpv4Addr {
	if len(vals) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.HostRecordIpv4Addr, 0, len(vals))
	for _, v := range vals {
		out = append(out, namespacedv1alpha1.HostRecordIpv4Addr{
			Ipv4Addr:         v.Ipv4Addr,
			MAC:              v.MAC,
			ConfigureForDHCP: v.ConfigureForDHCP,
			NextServer:       v.NextServer,
		})
	}
	return out
}

func namespacedIpv6AddrsToValues(addrs []namespacedv1alpha1.HostRecordIpv6Addr) []ipv6AddrValue {
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

func namespacedIpv6AddrsFromValues(vals []ipv6AddrValue) []namespacedv1alpha1.HostRecordIpv6Addr {
	if len(vals) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.HostRecordIpv6Addr, 0, len(vals))
	for _, v := range vals {
		out = append(out, namespacedv1alpha1.HostRecordIpv6Addr{
			Ipv6Addr:         v.Ipv6Addr,
			Duid:             v.Duid,
			ConfigureForDHCP: v.ConfigureForDHCP,
		})
	}
	return out
}

// Observe fetches the HostRecord from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.HostRecord) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
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
	cr.Status.AtProvider = namespacedv1alpha1.HostRecordObservation{
		Name:            o.Name,
		Ipv4Addrs:       namespacedIpv4AddrsFromValues(o.Ipv4Addrs),
		Ipv6Addrs:       namespacedIpv6AddrsFromValues(o.Ipv6Addrs),
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

	localIpv4 := namespacedIpv4AddrsToValues(p.Ipv4Addrs)
	localIpv6 := namespacedIpv6AddrsToValues(p.Ipv6Addrs)
	lateInit := lateInitialize(&p.Comment, &p.TTL, &p.UseTTL, &p.ExtAttrs, &p.View, &p.ConfigureForDNS, &p.Disable, &p.Aliases, &localIpv4, &localIpv6, rec)
	if lateInit {
		p.Ipv4Addrs = namespacedIpv4AddrsFromValues(localIpv4)
		p.Ipv6Addrs = namespacedIpv6AddrsFromValues(localIpv6)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(namespacedCompareFields(p), rec),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new HostRecord and records the server-assigned _ref
// as the external name. Dispatches to one of three provisioning
// strategies — see provisionHostRecord.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.HostRecord) (managed.ExternalCreation, error) {
	p := &cr.Spec.ForProvider
	rec, err := provisionHostRecord(e.client.objMgr, namespacedCompareFields(p), p.NetworkView, p.Ipv4Cidr, p.Ipv6Cidr, p.FilterParams, p.IpAddressType)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateHostRecord)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable HostRecord fields. networkView (immutable) is
// never sent — see updateHostRecord.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.HostRecord) (managed.ExternalUpdate, error) {
	p := &cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateHostRecord(e.client.objMgr, externalID, namespacedCompareFields(p))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateHostRecord)
	}

	// See clusterExternal.Update — UpdateHostRecord always returns the
	// object's current _ref, and renaming (or changing view) changes the
	// _ref.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the HostRecord. A 404 is treated as already-deleted
// (idempotent).
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.HostRecord) (managed.ExternalDelete, error) {
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
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.HostRecord] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.HostRecord]    = &namespacedExternal{}
)

// setupNamespacedHostRecord wires the namespaced HostRecord reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupNamespacedHostRecord(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.HostRecordList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced HostRecord state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.HostRecord](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("HostRecord")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.HostRecord{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
