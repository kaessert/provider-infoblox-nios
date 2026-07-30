package fixedaddress

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/fixedaddress/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
)

const clusterControllerName = "cluster-fixedaddress.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=fixedaddress.infobloxnios.crossplane.io,resources=fixedaddresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fixedaddress.infobloxnios.crossplane.io,resources=fixedaddresses/status,verbs=get;update;patch

// Cross-resource reference RBAC (spec.forProvider.networkView resolves
// against NetworkView, which is always cluster-scoped regardless of this
// controller's own scope).
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.FixedAddress].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI object
// manager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.FixedAddress) (managed.TypedExternalClient[*clusterv1alpha1.FixedAddress], error) {
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

	return &clusterExternal{objMgr: objMgr}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.FixedAddress].
type clusterExternal struct {
	objMgr ibclient.IBObjectManager
}

// clusterToFields converts a clusterv1alpha1.FixedAddressParameters value
// into the scope-agnostic fixedAddressFields shape.
func clusterToFields(p clusterv1alpha1.FixedAddressParameters) fixedAddressFields {
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
		Options:                     clusterOptionsToShared(p.Options),
		UseOptions:                  p.UseOptions,
	}
}

// clusterApplyFields copies the (possibly late-initialized) shared fields
// back onto a clusterv1alpha1.FixedAddressParameters value.
func clusterApplyFields(p *clusterv1alpha1.FixedAddressParameters, f fixedAddressFields) {
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
	p.Options = clusterOptionsFromShared(f.Options)
	p.UseOptions = f.UseOptions
}

func clusterOptionsToShared(opts []*clusterv1alpha1.FixedAddressDhcpOption) []dhcpOption {
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

func clusterOptionsFromShared(opts []dhcpOption) []*clusterv1alpha1.FixedAddressDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.FixedAddressDhcpOption, 0, len(opts))
	for _, o := range opts {
		out = append(out, &clusterv1alpha1.FixedAddressDhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

func clusterCloudInfoFromShared(ci *sharedCloudInfo) *clusterv1alpha1.FixedAddressCloudInfo {
	if ci == nil {
		return nil
	}
	out := &clusterv1alpha1.FixedAddressCloudInfo{
		DelegatedScope: ci.DelegatedScope,
		DelegatedRoot:  ci.DelegatedRoot,
		OwnedByAdaptor: ci.OwnedByAdaptor,
		Usage:          ci.Usage,
		Tenant:         ci.Tenant,
		MgmtPlatform:   ci.MgmtPlatform,
		AuthorityType:  ci.AuthorityType,
	}
	if ci.DelegatedMember != nil {
		out.DelegatedMember = &clusterv1alpha1.FixedAddressCloudInfoDelegatedMember{
			Ipv4Addr: ci.DelegatedMember.IPv4Addr,
			Ipv6Addr: ci.DelegatedMember.IPv6Addr,
			Name:     ci.DelegatedMember.Name,
		}
	}
	return out
}

// Observe fetches the FixedAddress from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.FixedAddress) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetFixedAddressByRef
	// with the CR's Kubernetes name (not a real WAPI _ref) would error
	// against the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	fa, err := e.objMgr.GetFixedAddressByRef(externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveFixedAddress)
	}

	o := observeFromFixedAddress(externalID, fa)
	cr.Status.AtProvider = clusterv1alpha1.FixedAddressObservation{
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
		Options:                     clusterOptionsFromShared(o.Options),
		UseOptions:                  o.UseOptions,
		Ref:                         o.Ref,
		DUID:                        o.DUID,
		CloudInfo:                   clusterCloudInfoFromShared(o.CloudInfo),
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	f := clusterToFields(*p)
	lateInit := lateInitialize(&f, fa)
	clusterApplyFields(p, f)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(f, fa),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new FixedAddress via AllocateIP (NON-STANDARD — no
// CreateFixedAddress method exists) and records the server-assigned _ref
// as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.FixedAddress) (managed.ExternalCreation, error) {
	f := clusterToFields(cr.Spec.ForProvider)
	fa, err := createFixedAddress(e.objMgr, f)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateFixedAddress)
	}

	meta.SetExternalName(cr, fa.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable FixedAddress fields. Because ipv4addr is
// _ref-mutating (UNSTABLE external name, ADR-IN-0004), the external-name
// annotation is refreshed whenever the WAPI response returns a different
// _ref than the one used to issue the request.
func (e *clusterExternal) Update(_ context.Context, cr *clusterv1alpha1.FixedAddress) (managed.ExternalUpdate, error) {
	f := clusterToFields(cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)

	fa, err := updateFixedAddress(e.objMgr, externalID, f, cr.Status.AtProvider.MatchClient)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateFixedAddress)
	}

	if fa.Ref != "" && fa.Ref != externalID {
		meta.SetExternalName(cr, fa.Ref)
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the FixedAddress. A 404 is treated as already-deleted
// (idempotent).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.FixedAddress) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteFixedAddress(e.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteFixedAddress)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.FixedAddress] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.FixedAddress]    = &clusterExternal{}
)

// setupClusterFixedAddress wires the cluster-scoped FixedAddress
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupClusterFixedAddress(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.FixedAddressList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster FixedAddress state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.FixedAddress](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("FixedAddress")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.FixedAddress{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
