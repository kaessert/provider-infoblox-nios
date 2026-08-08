package fixedaddress

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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/fixedaddress/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-fixedaddress.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=fixedaddress.infobloxnios.m.crossplane.io,resources=fixedaddresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fixedaddress.infobloxnios.m.crossplane.io,resources=fixedaddresses/status,verbs=get;update;patch

// Cross-resource reference RBAC (spec.forProvider.networkView resolves
// against NetworkView, which is always cluster-scoped regardless of this
// controller's own scope).
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.FixedAddress].
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
// authenticated WAPI object manager.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.FixedAddress) (managed.TypedExternalClient[*namespacedv1alpha1.FixedAddress], error) {
	if err := c.usage.Track(ctx, cr); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	ref := cr.GetProviderConfigReference()
	if ref == nil {
		return nil, errors.New(errGetPC + ": no ProviderConfigReference set")
	}

	var conn *config.Conn
	switch ref.Kind {
	case "ProviderConfig":
		pc := &apisv1alpha1.ProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name, Namespace: cr.GetNamespace()}, pc); err != nil {
			return nil, errors.Wrap(err, errGetPC)
		}
		var err error
		conn, err = config.Get(ctx, c.kube, pc)
		if err != nil {
			return nil, err
		}

	case "ClusterProviderConfig":
		cpc := &apisv1alpha1.ClusterProviderConfig{}
		if err := c.kube.Get(ctx, k8sclient.ObjectKey{Name: ref.Name}, cpc); err != nil {
			return nil, errors.Wrap(err, errGetClusterPC)
		}
		var err error
		conn, err = config.GetCluster(ctx, c.kube, cpc)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("%s: %s", errUnsupportedKind, ref.Kind)
	}

	objMgr := identity.NewManagerAndConnector(conn.Connector)

	return &namespacedExternal{
		kube:     c.kube,
		objMgr:   objMgr.Manager,
		conn:     objMgr.Connector,
		endpoint: conn.Endpoint,
	}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.FixedAddress].
type namespacedExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — see resolveFixedAddressIdentity.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host.
	endpoint string
}

// namespacedToFields converts a namespacedv1alpha1.FixedAddressParameters
// value into the scope-agnostic fixedAddressFields shape.
func namespacedToFields(p namespacedv1alpha1.FixedAddressParameters) fixedAddressFields {
	return fixedAddressFields{
		IPv4Addr:                    p.IPv4Addr,
		IPv6Addr:                    p.IPv6Addr,
		MAC:                         p.MAC,
		NetworkView:                 p.NetworkView,
		Network:                     p.Network,
		Name:                        p.Name,
		MatchClient:                 p.MatchClient,
		Comment:                     p.Comment,
		ExtAttrs:                    p.ExtAttrs,
		Disable:                     p.Disable,
		AgentCircuitID:              p.AgentCircuitID,
		AgentRemoteID:               p.AgentRemoteID,
		ClientIdentifierPrependZero: p.ClientIdentifierPrependZero,
		DHCPClientIdentifier:        p.DHCPClientIdentifier,
		Options:                     namespacedOptionsToShared(p.Options),
		UseOptions:                  p.UseOptions,
	}
}

// namespacedApplyFields copies the (possibly late-initialized) shared
// fields back onto a namespacedv1alpha1.FixedAddressParameters value.
func namespacedApplyFields(p *namespacedv1alpha1.FixedAddressParameters, f fixedAddressFields) {
	p.IPv4Addr = f.IPv4Addr
	p.IPv6Addr = f.IPv6Addr
	p.MAC = f.MAC
	p.NetworkView = f.NetworkView
	p.Network = f.Network
	p.Name = f.Name
	p.MatchClient = f.MatchClient
	p.Comment = f.Comment
	p.ExtAttrs = f.ExtAttrs
	p.Disable = f.Disable
	p.AgentCircuitID = f.AgentCircuitID
	p.AgentRemoteID = f.AgentRemoteID
	p.ClientIdentifierPrependZero = f.ClientIdentifierPrependZero
	p.DHCPClientIdentifier = f.DHCPClientIdentifier
	p.Options = namespacedOptionsFromShared(f.Options)
	p.UseOptions = f.UseOptions
}

func namespacedOptionsToShared(opts []*namespacedv1alpha1.FixedAddressDhcpOption) []dhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]dhcpOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		out = append(out, dhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

func namespacedOptionsFromShared(opts []dhcpOption) []*namespacedv1alpha1.FixedAddressDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.FixedAddressDhcpOption, 0, len(opts))
	for _, o := range opts {
		out = append(out, &namespacedv1alpha1.FixedAddressDhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

func namespacedCloudInfoFromShared(ci *sharedCloudInfo) *namespacedv1alpha1.FixedAddressCloudInfo {
	if ci == nil {
		return nil
	}
	out := &namespacedv1alpha1.FixedAddressCloudInfo{
		DelegatedScope: ci.DelegatedScope,
		DelegatedRoot:  ci.DelegatedRoot,
		OwnedByAdaptor: ci.OwnedByAdaptor,
		Usage:          ci.Usage,
		Tenant:         ci.Tenant,
		MgmtPlatform:   ci.MgmtPlatform,
		AuthorityType:  ci.AuthorityType,
	}
	if ci.DelegatedMember != nil {
		out.DelegatedMember = &namespacedv1alpha1.FixedAddressCloudInfoDelegatedMember{
			Ipv4Addr: ci.DelegatedMember.IPv4Addr,
			Ipv6Addr: ci.DelegatedMember.IPv6Addr,
			Name:     ci.DelegatedMember.Name,
		}
	}
	return out
}

// Observe resolves the FixedAddress through the shared UID-in-EA
// identity ladder and compares the result against the desired spec.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.FixedAddress) (managed.ExternalObservation, error) {
	f := namespacedToFields(cr.Spec.ForProvider)

	res, err := observeFixedAddress(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()), f.isIPv6())
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveFixedAddress)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	fa := res.fa

	o := res.obs
	cr.Status.AtProvider = namespacedv1alpha1.FixedAddressObservation{
		IPv4Addr:                    o.IPv4Addr,
		IPv6Addr:                    o.IPv6Addr,
		MAC:                         o.MAC,
		NetworkView:                 o.NetworkView,
		Network:                     o.Network,
		Name:                        o.Name,
		MatchClient:                 o.MatchClient,
		Comment:                     o.Comment,
		ExtAttrs:                    o.ExtAttrs,
		Disable:                     o.Disable,
		AgentCircuitID:              o.AgentCircuitID,
		AgentRemoteID:               o.AgentRemoteID,
		ClientIdentifierPrependZero: o.ClientIdentifierPrependZero,
		DHCPClientIdentifier:        o.DHCPClientIdentifier,
		Options:                     namespacedOptionsFromShared(o.Options),
		UseOptions:                  o.UseOptions,
		Ref:                         o.Ref,
		DUID:                        o.DUID,
		CloudInfo:                   namespacedCloudInfoFromShared(o.CloudInfo),
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	f = namespacedToFields(*p)
	lateInit := lateInitialize(&f, fa)
	namespacedApplyFields(p, f)

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
		lateInit = true
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(f, fa) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new FixedAddress via AllocateIP (NON-STANDARD — no
// CreateFixedAddress method exists), stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.FixedAddress) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	f := namespacedToFields(cr.Spec.ForProvider)
	fa, err := createFixedAddress(e.objMgr, f, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateFixedAddress)
	}

	meta.SetExternalName(cr, fa.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable FixedAddress fields. Because ipv4addr is
// _ref-mutating (UNSTABLE external name, live-verified), the external-name
// annotation is refreshed whenever the WAPI response returns a different
// _ref than the one used to issue the request. Every call re-asserts the
// identity stamp.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.FixedAddress) (managed.ExternalUpdate, error) {
	f := namespacedToFields(cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	fa, err := updateFixedAddress(e.objMgr, externalID, f, cr.Status.AtProvider.MatchClient, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateFixedAddress)
	}

	if fa.Ref != "" && fa.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, fa.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the FixedAddress, resolving through the shared identity
// ladder first.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.FixedAddress) (managed.ExternalDelete, error) {
	f := namespacedToFields(cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)
	if err := deleteFixedAddressIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID()), f.isIPv6()); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.FixedAddress] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.FixedAddress]    = &namespacedExternal{}
)

// setupNamespacedFixedAddress wires the namespaced FixedAddress reconciler
// with the controller-runtime manager. Called from SetupGated (gate
// callback) and Setup (immediate path) in controller.go.
func setupNamespacedFixedAddress(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.FixedAddressList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced FixedAddress state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.FixedAddress](driftdetection.WrapConnector[*namespacedv1alpha1.FixedAddress](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("FixedAddress")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.FixedAddress{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
