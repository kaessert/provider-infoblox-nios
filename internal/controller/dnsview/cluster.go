package dnsview

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dnsview/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-dnsview.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=dnsview.infobloxnios.crossplane.io,resources=dnsviews,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dnsview.infobloxnios.crossplane.io,resources=dnsviews/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.DNSView].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI connector.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.DNSView) (managed.TypedExternalClient[*clusterv1alpha1.DNSView], error) {
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

	conn, err := newConnector(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{kube: c.kube, conn: conn}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.DNSView].
type clusterExternal struct {
	kube k8sclient.Client
	conn ibclient.IBConnector
}

// Observe fetches the DNSView from the WAPI by its _ref external name and
// compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.DNSView) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetObject with the
	// CR's Kubernetes name (not a real WAPI _ref) would error against the
	// API on every reconcile until Create() overwrites the annotation
	// with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	v, err := getViewByRef(e.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDNSView)
	}

	observed := fieldsFromView(v)
	cloudInfo := cloudInfoValueFromSDK(v.CloudInfo)
	cr.Status.AtProvider = clusterObservationFromFields(observed, externalID, strPtrOrNil(v.Ref), &v.IsDefault, cloudInfo)
	// Explicit assignment (rather than relying solely on the ID field
	// folded into the struct literal above) keeps the server-assigned
	// identifier's provenance obvious at the call site — it always
	// mirrors the external name used to fetch this view.
	cr.Status.AtProvider.ID = externalID

	desired := fieldsFromClusterParams(&cr.Spec.ForProvider)
	lateInit, changed := lateInitializeFields(desired, observed)
	if changed {
		applyFieldsToClusterParams(lateInit, &cr.Spec.ForProvider)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(lateInit, observed),
		ResourceLateInitialized: changed,
	}, nil
}

// Create provisions a new DNSView and records the server-assigned _ref as
// the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.DNSView) (managed.ExternalCreation, error) {
	f := fieldsFromClusterParams(&cr.Spec.ForProvider)
	ref, err := createView(e.conn, f)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDNSView)
	}

	meta.SetExternalName(cr, ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable DNSView fields (is_default is read-only and
// never sent — see buildView). WAPI's view PUT is a partial merge.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.DNSView) (managed.ExternalUpdate, error) {
	f := fieldsFromClusterParams(&cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)

	ref, err := updateView(e.conn, externalID, f)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDNSView)
	}

	// DNSView is in the _ref-unstable resource group: renaming the view
	// changes its _ref, and the old _ref immediately 404s afterward — so
	// the external-name annotation must be refreshed here whenever the
	// PUT response's _ref differs from the one we sent the request to.
	if ref != "" && ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the DNSView. Well-known views (default/External/Internal)
// are never actually deleted from the Grid — see isWellKnownDNSViewName —
// so the Kubernetes object is still allowed to go away (the finalizer
// clears) without taking Grid-wide DNS resolution down with it. A 404 on a
// custom view is treated as already-deleted (idempotent).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.DNSView) (managed.ExternalDelete, error) {
	name := cr.Status.AtProvider.Name
	if name == nil {
		name = cr.Spec.ForProvider.Name
	}
	if isWellKnownDNSViewName(name) {
		return managed.ExternalDelete{}, nil
	}

	externalID := meta.GetExternalName(cr)
	if err := deleteView(e.conn, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteDNSView)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.DNSView] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.DNSView]    = &clusterExternal{}
)

// setupClusterDNSView wires the cluster-scoped DNSView reconciler with the
// controller-runtime manager. Called from SetupGated (gate callback) and
// Setup (immediate path) in controller.go.
func setupClusterDNSView(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.DNSViewList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster DNSView state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.DNSView](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("DNSView")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.DNSView{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// ── Cluster-scoped field bag conversions ─────────────────────────────────

// fieldsFromClusterParams extracts the scope-neutral field bag from a Cluster-scoped DNSViewParameters.
func fieldsFromClusterParams(p *clusterv1alpha1.DNSViewParameters) dnsViewFields {
	return dnsViewFields{
		Name:                                p.Name,
		Comment:                             p.Comment,
		NetworkView:                         p.NetworkView,
		Disable:                             p.Disable,
		BlacklistAction:                     p.BlacklistAction,
		BlacklistLogQuery:                   p.BlacklistLogQuery,
		BlacklistRedirectAddresses:          p.BlacklistRedirectAddresses,
		BlacklistRedirectTTL:                p.BlacklistRedirectTTL,
		BlacklistRulesets:                   p.BlacklistRulesets,
		UseBlacklist:                        p.UseBlacklist,
		EnableBlacklist:                     p.EnableBlacklist,
		RootNameServerType:                  p.RootNameServerType,
		UseRootNameServer:                   p.UseRootNameServer,
		DdnsForceCreationTimestampUpdate:    p.DdnsForceCreationTimestampUpdate,
		UseDdnsForceCreationTimestampUpdate: p.UseDdnsForceCreationTimestampUpdate,
		DdnsPrincipalGroup:                  p.DdnsPrincipalGroup,
		DdnsPrincipalTracking:               p.DdnsPrincipalTracking,
		UseDdnsPrincipalSecurity:            p.UseDdnsPrincipalSecurity,
		DdnsRestrictPatterns:                p.DdnsRestrictPatterns,
		DdnsRestrictPatternsList:            p.DdnsRestrictPatternsList,
		UseDdnsPatternsRestriction:          p.UseDdnsPatternsRestriction,
		DdnsRestrictProtected:               p.DdnsRestrictProtected,
		UseDdnsRestrictProtected:            p.UseDdnsRestrictProtected,
		DdnsRestrictSecure:                  p.DdnsRestrictSecure,
		DdnsRestrictStatic:                  p.DdnsRestrictStatic,
		UseDdnsRestrictStatic:               p.UseDdnsRestrictStatic,
		Dns64Enabled:                        p.Dns64Enabled,
		Dns64Groups:                         p.Dns64Groups,
		UseDns64:                            p.UseDns64,
		DnssecEnabled:                       p.DnssecEnabled,
		DnssecExpiredSignaturesEnabled:      p.DnssecExpiredSignaturesEnabled,
		DnssecNegativeTrustAnchors:          p.DnssecNegativeTrustAnchors,
		DnssecValidationEnabled:             p.DnssecValidationEnabled,
		UseDnssec:                           p.UseDnssec,
		EnableFixedRrsetOrderFqdns:          p.EnableFixedRrsetOrderFqdns,
		UseFixedRrsetOrderFqdns:             p.UseFixedRrsetOrderFqdns,
		EnableMatchRecursiveOnly:            p.EnableMatchRecursiveOnly,
		FilterAaaa:                          p.FilterAaaa,
		UseFilterAaaa:                       p.UseFilterAaaa,
		ForwardOnly:                         p.ForwardOnly,
		Forwarders:                          p.Forwarders,
		UseForwarders:                       p.UseForwarders,
		LameTTL:                             p.LameTTL,
		UseLameTTL:                          p.UseLameTTL,
		MaxCacheTTL:                         p.MaxCacheTTL,
		UseMaxCacheTTL:                      p.UseMaxCacheTTL,
		MaxNcacheTTL:                        p.MaxNcacheTTL,
		UseMaxNcacheTTL:                     p.UseMaxNcacheTTL,
		NotifyDelay:                         p.NotifyDelay,
		NxdomainLogQuery:                    p.NxdomainLogQuery,
		NxdomainRedirect:                    p.NxdomainRedirect,
		NxdomainRedirectAddresses:           p.NxdomainRedirectAddresses,
		NxdomainRedirectAddressesV6:         p.NxdomainRedirectAddressesV6,
		NxdomainRedirectTTL:                 p.NxdomainRedirectTTL,
		NxdomainRulesets:                    p.NxdomainRulesets,
		UseNxdomainRedirect:                 p.UseNxdomainRedirect,
		Recursion:                           p.Recursion,
		UseRecursion:                        p.UseRecursion,
		UseResponseRateLimiting:             p.UseResponseRateLimiting,
		RpzDropIPRuleEnabled:                p.RpzDropIPRuleEnabled,
		RpzDropIPRuleMinPrefixLengthIPv4:    p.RpzDropIPRuleMinPrefixLengthIPv4,
		RpzDropIPRuleMinPrefixLengthIPv6:    p.RpzDropIPRuleMinPrefixLengthIPv6,
		UseRpzDropIPRule:                    p.UseRpzDropIPRule,
		RpzQnameWaitRecurse:                 p.RpzQnameWaitRecurse,
		UseRpzQnameWaitRecurse:              p.UseRpzQnameWaitRecurse,
		UseScavengingSettings:               p.UseScavengingSettings,
		UseSortlist:                         p.UseSortlist,
		ExtAttrs:                            p.ExtAttrs,
		CustomRootNameServers:               nameServerValuesFromCluster(p.CustomRootNameServers),
		DnssecTrustedKeys:                   dnssecTrustedKeyValuesFromPtrCluster(p.DnssecTrustedKeys),
		FixedRrsetOrderFqdns:                fixedRrsetOrderFqdnValuesFromPtrCluster(p.FixedRrsetOrderFqdns),
		FilterAaaaList:                      addressAcValuesFromPtrCluster(p.FilterAaaaList),
		MatchClients:                        addressAcValuesFromPtrCluster(p.MatchClients),
		MatchDestinations:                   addressAcValuesFromPtrCluster(p.MatchDestinations),
		Sortlist:                            sortlistEntryValuesFromPtrCluster(p.Sortlist),
		ResponseRateLimiting:                responseRateLimitingValueFromCluster(p.ResponseRateLimiting),
		ScavengingSettings:                  scavengingSettingsValueFromCluster(p.ScavengingSettings),
	}
}

// applyFieldsToClusterParams writes the field bag back into a Cluster-scoped DNSViewParameters (used after late-init back-fill).
func applyFieldsToClusterParams(f dnsViewFields, p *clusterv1alpha1.DNSViewParameters) {
	p.Name = f.Name
	p.Comment = f.Comment
	p.NetworkView = f.NetworkView
	p.Disable = f.Disable
	p.BlacklistAction = f.BlacklistAction
	p.BlacklistLogQuery = f.BlacklistLogQuery
	p.BlacklistRedirectAddresses = f.BlacklistRedirectAddresses
	p.BlacklistRedirectTTL = f.BlacklistRedirectTTL
	p.BlacklistRulesets = f.BlacklistRulesets
	p.UseBlacklist = f.UseBlacklist
	p.EnableBlacklist = f.EnableBlacklist
	p.RootNameServerType = f.RootNameServerType
	p.UseRootNameServer = f.UseRootNameServer
	p.DdnsForceCreationTimestampUpdate = f.DdnsForceCreationTimestampUpdate
	p.UseDdnsForceCreationTimestampUpdate = f.UseDdnsForceCreationTimestampUpdate
	p.DdnsPrincipalGroup = f.DdnsPrincipalGroup
	p.DdnsPrincipalTracking = f.DdnsPrincipalTracking
	p.UseDdnsPrincipalSecurity = f.UseDdnsPrincipalSecurity
	p.DdnsRestrictPatterns = f.DdnsRestrictPatterns
	p.DdnsRestrictPatternsList = f.DdnsRestrictPatternsList
	p.UseDdnsPatternsRestriction = f.UseDdnsPatternsRestriction
	p.DdnsRestrictProtected = f.DdnsRestrictProtected
	p.UseDdnsRestrictProtected = f.UseDdnsRestrictProtected
	p.DdnsRestrictSecure = f.DdnsRestrictSecure
	p.DdnsRestrictStatic = f.DdnsRestrictStatic
	p.UseDdnsRestrictStatic = f.UseDdnsRestrictStatic
	p.Dns64Enabled = f.Dns64Enabled
	p.Dns64Groups = f.Dns64Groups
	p.UseDns64 = f.UseDns64
	p.DnssecEnabled = f.DnssecEnabled
	p.DnssecExpiredSignaturesEnabled = f.DnssecExpiredSignaturesEnabled
	p.DnssecNegativeTrustAnchors = f.DnssecNegativeTrustAnchors
	p.DnssecValidationEnabled = f.DnssecValidationEnabled
	p.UseDnssec = f.UseDnssec
	p.EnableFixedRrsetOrderFqdns = f.EnableFixedRrsetOrderFqdns
	p.UseFixedRrsetOrderFqdns = f.UseFixedRrsetOrderFqdns
	p.EnableMatchRecursiveOnly = f.EnableMatchRecursiveOnly
	p.FilterAaaa = f.FilterAaaa
	p.UseFilterAaaa = f.UseFilterAaaa
	p.ForwardOnly = f.ForwardOnly
	p.Forwarders = f.Forwarders
	p.UseForwarders = f.UseForwarders
	p.LameTTL = f.LameTTL
	p.UseLameTTL = f.UseLameTTL
	p.MaxCacheTTL = f.MaxCacheTTL
	p.UseMaxCacheTTL = f.UseMaxCacheTTL
	p.MaxNcacheTTL = f.MaxNcacheTTL
	p.UseMaxNcacheTTL = f.UseMaxNcacheTTL
	p.NotifyDelay = f.NotifyDelay
	p.NxdomainLogQuery = f.NxdomainLogQuery
	p.NxdomainRedirect = f.NxdomainRedirect
	p.NxdomainRedirectAddresses = f.NxdomainRedirectAddresses
	p.NxdomainRedirectAddressesV6 = f.NxdomainRedirectAddressesV6
	p.NxdomainRedirectTTL = f.NxdomainRedirectTTL
	p.NxdomainRulesets = f.NxdomainRulesets
	p.UseNxdomainRedirect = f.UseNxdomainRedirect
	p.Recursion = f.Recursion
	p.UseRecursion = f.UseRecursion
	p.UseResponseRateLimiting = f.UseResponseRateLimiting
	p.RpzDropIPRuleEnabled = f.RpzDropIPRuleEnabled
	p.RpzDropIPRuleMinPrefixLengthIPv4 = f.RpzDropIPRuleMinPrefixLengthIPv4
	p.RpzDropIPRuleMinPrefixLengthIPv6 = f.RpzDropIPRuleMinPrefixLengthIPv6
	p.UseRpzDropIPRule = f.UseRpzDropIPRule
	p.RpzQnameWaitRecurse = f.RpzQnameWaitRecurse
	p.UseRpzQnameWaitRecurse = f.UseRpzQnameWaitRecurse
	p.UseScavengingSettings = f.UseScavengingSettings
	p.UseSortlist = f.UseSortlist
	p.ExtAttrs = f.ExtAttrs
	p.CustomRootNameServers = nameServerValuesToCluster(f.CustomRootNameServers)
	p.DnssecTrustedKeys = dnssecTrustedKeyValuesToPtrCluster(f.DnssecTrustedKeys)
	p.FixedRrsetOrderFqdns = fixedRrsetOrderFqdnValuesToPtrCluster(f.FixedRrsetOrderFqdns)
	p.FilterAaaaList = addressAcValuesToPtrCluster(f.FilterAaaaList)
	p.MatchClients = addressAcValuesToPtrCluster(f.MatchClients)
	p.MatchDestinations = addressAcValuesToPtrCluster(f.MatchDestinations)
	p.Sortlist = sortlistEntryValuesToPtrCluster(f.Sortlist)
	p.ResponseRateLimiting = responseRateLimitingValueToCluster(f.ResponseRateLimiting)
	p.ScavengingSettings = scavengingSettingsValueToCluster(f.ScavengingSettings)
}

// clusterObservationFromFields builds a Cluster-scoped DNSViewObservation from the field bag plus the response-only fields (id, ref, isDefault, cloudInfo).
func clusterObservationFromFields(f dnsViewFields, id string, ref *string, isDefault *bool, cloudInfo *cloudInfoValue) clusterv1alpha1.DNSViewObservation {
	o := clusterv1alpha1.DNSViewObservation{
		ID:                                  id,
		Name:                                f.Name,
		Comment:                             f.Comment,
		NetworkView:                         f.NetworkView,
		Disable:                             f.Disable,
		BlacklistAction:                     f.BlacklistAction,
		BlacklistLogQuery:                   f.BlacklistLogQuery,
		BlacklistRedirectAddresses:          f.BlacklistRedirectAddresses,
		BlacklistRedirectTTL:                f.BlacklistRedirectTTL,
		BlacklistRulesets:                   f.BlacklistRulesets,
		UseBlacklist:                        f.UseBlacklist,
		EnableBlacklist:                     f.EnableBlacklist,
		RootNameServerType:                  f.RootNameServerType,
		UseRootNameServer:                   f.UseRootNameServer,
		DdnsForceCreationTimestampUpdate:    f.DdnsForceCreationTimestampUpdate,
		UseDdnsForceCreationTimestampUpdate: f.UseDdnsForceCreationTimestampUpdate,
		DdnsPrincipalGroup:                  f.DdnsPrincipalGroup,
		DdnsPrincipalTracking:               f.DdnsPrincipalTracking,
		UseDdnsPrincipalSecurity:            f.UseDdnsPrincipalSecurity,
		DdnsRestrictPatterns:                f.DdnsRestrictPatterns,
		DdnsRestrictPatternsList:            f.DdnsRestrictPatternsList,
		UseDdnsPatternsRestriction:          f.UseDdnsPatternsRestriction,
		DdnsRestrictProtected:               f.DdnsRestrictProtected,
		UseDdnsRestrictProtected:            f.UseDdnsRestrictProtected,
		DdnsRestrictSecure:                  f.DdnsRestrictSecure,
		DdnsRestrictStatic:                  f.DdnsRestrictStatic,
		UseDdnsRestrictStatic:               f.UseDdnsRestrictStatic,
		Dns64Enabled:                        f.Dns64Enabled,
		Dns64Groups:                         f.Dns64Groups,
		UseDns64:                            f.UseDns64,
		DnssecEnabled:                       f.DnssecEnabled,
		DnssecExpiredSignaturesEnabled:      f.DnssecExpiredSignaturesEnabled,
		DnssecNegativeTrustAnchors:          f.DnssecNegativeTrustAnchors,
		DnssecValidationEnabled:             f.DnssecValidationEnabled,
		UseDnssec:                           f.UseDnssec,
		EnableFixedRrsetOrderFqdns:          f.EnableFixedRrsetOrderFqdns,
		UseFixedRrsetOrderFqdns:             f.UseFixedRrsetOrderFqdns,
		EnableMatchRecursiveOnly:            f.EnableMatchRecursiveOnly,
		FilterAaaa:                          f.FilterAaaa,
		UseFilterAaaa:                       f.UseFilterAaaa,
		ForwardOnly:                         f.ForwardOnly,
		Forwarders:                          f.Forwarders,
		UseForwarders:                       f.UseForwarders,
		LameTTL:                             f.LameTTL,
		UseLameTTL:                          f.UseLameTTL,
		MaxCacheTTL:                         f.MaxCacheTTL,
		UseMaxCacheTTL:                      f.UseMaxCacheTTL,
		MaxNcacheTTL:                        f.MaxNcacheTTL,
		UseMaxNcacheTTL:                     f.UseMaxNcacheTTL,
		NotifyDelay:                         f.NotifyDelay,
		NxdomainLogQuery:                    f.NxdomainLogQuery,
		NxdomainRedirect:                    f.NxdomainRedirect,
		NxdomainRedirectAddresses:           f.NxdomainRedirectAddresses,
		NxdomainRedirectAddressesV6:         f.NxdomainRedirectAddressesV6,
		NxdomainRedirectTTL:                 f.NxdomainRedirectTTL,
		NxdomainRulesets:                    f.NxdomainRulesets,
		UseNxdomainRedirect:                 f.UseNxdomainRedirect,
		Recursion:                           f.Recursion,
		UseRecursion:                        f.UseRecursion,
		UseResponseRateLimiting:             f.UseResponseRateLimiting,
		RpzDropIPRuleEnabled:                f.RpzDropIPRuleEnabled,
		RpzDropIPRuleMinPrefixLengthIPv4:    f.RpzDropIPRuleMinPrefixLengthIPv4,
		RpzDropIPRuleMinPrefixLengthIPv6:    f.RpzDropIPRuleMinPrefixLengthIPv6,
		UseRpzDropIPRule:                    f.UseRpzDropIPRule,
		RpzQnameWaitRecurse:                 f.RpzQnameWaitRecurse,
		UseRpzQnameWaitRecurse:              f.UseRpzQnameWaitRecurse,
		UseScavengingSettings:               f.UseScavengingSettings,
		UseSortlist:                         f.UseSortlist,
		ExtAttrs:                            f.ExtAttrs,
		CustomRootNameServers:               nameServerValuesToCluster(f.CustomRootNameServers),
		DnssecTrustedKeys:                   dnssecTrustedKeyValuesToPtrCluster(f.DnssecTrustedKeys),
		FixedRrsetOrderFqdns:                fixedRrsetOrderFqdnValuesToPtrCluster(f.FixedRrsetOrderFqdns),
		FilterAaaaList:                      addressAcValuesToPtrCluster(f.FilterAaaaList),
		MatchClients:                        addressAcValuesToPtrCluster(f.MatchClients),
		MatchDestinations:                   addressAcValuesToPtrCluster(f.MatchDestinations),
		Sortlist:                            sortlistEntryValuesToPtrCluster(f.Sortlist),
		ResponseRateLimiting:                responseRateLimitingValueToCluster(f.ResponseRateLimiting),
		ScavengingSettings:                  scavengingSettingsValueToCluster(f.ScavengingSettings),
		Ref:                                 ref,
		IsDefault:                           isDefault,
		CloudInfo:                           cloudInfoValueToCluster(cloudInfo),
	}
	return o
}

// ── Cluster-scoped nested value bag conversions ──────────────────────────

func nameServerValueFromCluster(in clusterv1alpha1.DNSViewNameServer) nameServerValue {
	return nameServerValue{
		Address:                      in.Address,
		Name:                         in.Name,
		SharedWithMsParentDelegation: in.SharedWithMsParentDelegation,
		Stealth:                      in.Stealth,
		TsigKey:                      in.TsigKey,
		TsigKeyAlg:                   in.TsigKeyAlg,
		TsigKeyName:                  in.TsigKeyName,
		UseTsigKeyName:               in.UseTsigKeyName,
	}
}

func nameServerValueToCluster(in nameServerValue) clusterv1alpha1.DNSViewNameServer {
	return clusterv1alpha1.DNSViewNameServer{
		Address:                      in.Address,
		Name:                         in.Name,
		SharedWithMsParentDelegation: in.SharedWithMsParentDelegation,
		Stealth:                      in.Stealth,
		TsigKey:                      in.TsigKey,
		TsigKeyAlg:                   in.TsigKeyAlg,
		TsigKeyName:                  in.TsigKeyName,
		UseTsigKeyName:               in.UseTsigKeyName,
	}
}

func nameServerValuesFromCluster(in []clusterv1alpha1.DNSViewNameServer) []nameServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]nameServerValue, 0, len(in))
	for _, item := range in {
		out = append(out, nameServerValueFromCluster(item))
	}
	return out
}

func nameServerValuesToCluster(in []nameServerValue) []clusterv1alpha1.DNSViewNameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.DNSViewNameServer, 0, len(in))
	for _, item := range in {
		out = append(out, nameServerValueToCluster(item))
	}
	return out
}

func dnssecTrustedKeyValueFromCluster(in clusterv1alpha1.DNSViewDnssecTrustedKey) dnssecTrustedKeyValue {
	return dnssecTrustedKeyValue{
		Fqdn:               in.Fqdn,
		Algorithm:          in.Algorithm,
		Key:                in.Key,
		SecureEntryPoint:   in.SecureEntryPoint,
		DnssecMustBeSecure: in.DnssecMustBeSecure,
	}
}

func dnssecTrustedKeyValueToCluster(in dnssecTrustedKeyValue) clusterv1alpha1.DNSViewDnssecTrustedKey {
	return clusterv1alpha1.DNSViewDnssecTrustedKey{
		Fqdn:               in.Fqdn,
		Algorithm:          in.Algorithm,
		Key:                in.Key,
		SecureEntryPoint:   in.SecureEntryPoint,
		DnssecMustBeSecure: in.DnssecMustBeSecure,
	}
}

func dnssecTrustedKeyValuesFromPtrCluster(in []*clusterv1alpha1.DNSViewDnssecTrustedKey) []dnssecTrustedKeyValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]dnssecTrustedKeyValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, dnssecTrustedKeyValueFromCluster(*item))
	}
	return out
}

func dnssecTrustedKeyValuesToPtrCluster(in []dnssecTrustedKeyValue) []*clusterv1alpha1.DNSViewDnssecTrustedKey {
	if len(in) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.DNSViewDnssecTrustedKey, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := dnssecTrustedKeyValueToCluster(item)
		out = append(out, &crdItem)
	}
	return out
}

func addressAcValueFromCluster(in clusterv1alpha1.DNSViewAddressAc) addressAcValue {
	return addressAcValue{
		Address:        in.Address,
		Permission:     in.Permission,
		TsigKey:        in.TsigKey,
		TsigKeyAlg:     in.TsigKeyAlg,
		TsigKeyName:    in.TsigKeyName,
		UseTsigKeyName: in.UseTsigKeyName,
	}
}

func addressAcValueToCluster(in addressAcValue) clusterv1alpha1.DNSViewAddressAc {
	return clusterv1alpha1.DNSViewAddressAc{
		Address:        in.Address,
		Permission:     in.Permission,
		TsigKey:        in.TsigKey,
		TsigKeyAlg:     in.TsigKeyAlg,
		TsigKeyName:    in.TsigKeyName,
		UseTsigKeyName: in.UseTsigKeyName,
	}
}

func addressAcValuesFromPtrCluster(in []*clusterv1alpha1.DNSViewAddressAc) []addressAcValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]addressAcValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, addressAcValueFromCluster(*item))
	}
	return out
}

func addressAcValuesToPtrCluster(in []addressAcValue) []*clusterv1alpha1.DNSViewAddressAc {
	if len(in) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.DNSViewAddressAc, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := addressAcValueToCluster(item)
		out = append(out, &crdItem)
	}
	return out
}

func fixedRrsetOrderFqdnValueFromCluster(in clusterv1alpha1.DNSViewFixedRrsetOrderFqdn) fixedRrsetOrderFqdnValue {
	return fixedRrsetOrderFqdnValue{
		Fqdn:       in.Fqdn,
		RecordType: in.RecordType,
	}
}

func fixedRrsetOrderFqdnValueToCluster(in fixedRrsetOrderFqdnValue) clusterv1alpha1.DNSViewFixedRrsetOrderFqdn {
	return clusterv1alpha1.DNSViewFixedRrsetOrderFqdn{
		Fqdn:       in.Fqdn,
		RecordType: in.RecordType,
	}
}

func fixedRrsetOrderFqdnValuesFromPtrCluster(in []*clusterv1alpha1.DNSViewFixedRrsetOrderFqdn) []fixedRrsetOrderFqdnValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]fixedRrsetOrderFqdnValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, fixedRrsetOrderFqdnValueFromCluster(*item))
	}
	return out
}

func fixedRrsetOrderFqdnValuesToPtrCluster(in []fixedRrsetOrderFqdnValue) []*clusterv1alpha1.DNSViewFixedRrsetOrderFqdn {
	if len(in) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.DNSViewFixedRrsetOrderFqdn, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := fixedRrsetOrderFqdnValueToCluster(item)
		out = append(out, &crdItem)
	}
	return out
}

func expressionOpValueFromCluster(in clusterv1alpha1.DNSViewExpressionOp) expressionOpValue {
	return expressionOpValue{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func expressionOpValueToCluster(in expressionOpValue) clusterv1alpha1.DNSViewExpressionOp {
	return clusterv1alpha1.DNSViewExpressionOp{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func expressionOpValuesFromPtrCluster(in []*clusterv1alpha1.DNSViewExpressionOp) []expressionOpValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]expressionOpValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, expressionOpValueFromCluster(*item))
	}
	return out
}

func expressionOpValuesToPtrCluster(in []expressionOpValue) []*clusterv1alpha1.DNSViewExpressionOp {
	if len(in) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.DNSViewExpressionOp, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := expressionOpValueToCluster(item)
		out = append(out, &crdItem)
	}
	return out
}

func eaExpressionOpValueFromCluster(in clusterv1alpha1.DNSViewEaExpressionOp) eaExpressionOpValue {
	return eaExpressionOpValue{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func eaExpressionOpValueToCluster(in eaExpressionOpValue) clusterv1alpha1.DNSViewEaExpressionOp {
	return clusterv1alpha1.DNSViewEaExpressionOp{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func eaExpressionOpValuesFromPtrCluster(in []*clusterv1alpha1.DNSViewEaExpressionOp) []eaExpressionOpValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]eaExpressionOpValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, eaExpressionOpValueFromCluster(*item))
	}
	return out
}

func eaExpressionOpValuesToPtrCluster(in []eaExpressionOpValue) []*clusterv1alpha1.DNSViewEaExpressionOp {
	if len(in) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.DNSViewEaExpressionOp, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := eaExpressionOpValueToCluster(item)
		out = append(out, &crdItem)
	}
	return out
}

func sortlistEntryValueFromCluster(in clusterv1alpha1.DNSViewSortlistEntry) sortlistEntryValue {
	return sortlistEntryValue{Address: in.Address, MatchList: in.MatchList}
}

func sortlistEntryValueToCluster(in sortlistEntryValue) clusterv1alpha1.DNSViewSortlistEntry {
	return clusterv1alpha1.DNSViewSortlistEntry{Address: in.Address, MatchList: in.MatchList}
}

func sortlistEntryValuesFromPtrCluster(in []*clusterv1alpha1.DNSViewSortlistEntry) []sortlistEntryValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]sortlistEntryValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, sortlistEntryValueFromCluster(*item))
	}
	return out
}

func sortlistEntryValuesToPtrCluster(in []sortlistEntryValue) []*clusterv1alpha1.DNSViewSortlistEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.DNSViewSortlistEntry, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := sortlistEntryValueToCluster(item)
		out = append(out, &crdItem)
	}
	return out
}

func responseRateLimitingValueFromCluster(in *clusterv1alpha1.DNSViewResponseRateLimiting) *responseRateLimitingValue {
	if in == nil {
		return nil
	}
	return &responseRateLimitingValue{
		EnableRrl:          in.EnableRrl,
		LogOnly:            in.LogOnly,
		ResponsesPerSecond: in.ResponsesPerSecond,
		Window:             in.Window,
		Slip:               in.Slip,
	}
}

func responseRateLimitingValueToCluster(in *responseRateLimitingValue) *clusterv1alpha1.DNSViewResponseRateLimiting {
	if in == nil {
		return nil
	}
	return &clusterv1alpha1.DNSViewResponseRateLimiting{
		EnableRrl:          in.EnableRrl,
		LogOnly:            in.LogOnly,
		ResponsesPerSecond: in.ResponsesPerSecond,
		Window:             in.Window,
		Slip:               in.Slip,
	}
}

func scavengingScheduleValueFromCluster(in *clusterv1alpha1.DNSViewScavengingSchedule) *scavengingScheduleValue {
	if in == nil {
		return nil
	}
	return &scavengingScheduleValue{
		Weekdays:        in.Weekdays,
		TimeZone:        in.TimeZone,
		RecurringTime:   in.RecurringTime,
		Frequency:       in.Frequency,
		Every:           in.Every,
		MinutesPastHour: in.MinutesPastHour,
		HourOfDay:       in.HourOfDay,
		Year:            in.Year,
		Month:           in.Month,
		DayOfMonth:      in.DayOfMonth,
		Repeat:          in.Repeat,
		Disable:         in.Disable,
	}
}

func scavengingScheduleValueToCluster(in *scavengingScheduleValue) *clusterv1alpha1.DNSViewScavengingSchedule {
	if in == nil {
		return nil
	}
	return &clusterv1alpha1.DNSViewScavengingSchedule{
		Weekdays:        in.Weekdays,
		TimeZone:        in.TimeZone,
		RecurringTime:   in.RecurringTime,
		Frequency:       in.Frequency,
		Every:           in.Every,
		MinutesPastHour: in.MinutesPastHour,
		HourOfDay:       in.HourOfDay,
		Year:            in.Year,
		Month:           in.Month,
		DayOfMonth:      in.DayOfMonth,
		Repeat:          in.Repeat,
		Disable:         in.Disable,
	}
}

func scavengingSettingsValueFromCluster(in *clusterv1alpha1.DNSViewScavengingSettings) *scavengingSettingsValue {
	if in == nil {
		return nil
	}
	return &scavengingSettingsValue{
		EnableScavenging:          in.EnableScavenging,
		EnableRecurrentScavenging: in.EnableRecurrentScavenging,
		EnableAutoReclamation:     in.EnableAutoReclamation,
		EnableRrLastQueried:       in.EnableRrLastQueried,
		EnableZoneLastQueried:     in.EnableZoneLastQueried,
		ReclaimAssociatedRecords:  in.ReclaimAssociatedRecords,
		ScavengingSchedule:        scavengingScheduleValueFromCluster(in.ScavengingSchedule),
		ExpressionList:            expressionOpValuesFromPtrCluster(in.ExpressionList),
		EaExpressionList:          eaExpressionOpValuesFromPtrCluster(in.EaExpressionList),
	}
}

func scavengingSettingsValueToCluster(in *scavengingSettingsValue) *clusterv1alpha1.DNSViewScavengingSettings {
	if in == nil {
		return nil
	}
	return &clusterv1alpha1.DNSViewScavengingSettings{
		EnableScavenging:          in.EnableScavenging,
		EnableRecurrentScavenging: in.EnableRecurrentScavenging,
		EnableAutoReclamation:     in.EnableAutoReclamation,
		EnableRrLastQueried:       in.EnableRrLastQueried,
		EnableZoneLastQueried:     in.EnableZoneLastQueried,
		ReclaimAssociatedRecords:  in.ReclaimAssociatedRecords,
		ScavengingSchedule:        scavengingScheduleValueToCluster(in.ScavengingSchedule),
		ExpressionList:            expressionOpValuesToPtrCluster(in.ExpressionList),
		EaExpressionList:          eaExpressionOpValuesToPtrCluster(in.EaExpressionList),
	}
}

func cloudInfoValueToCluster(in *cloudInfoValue) *clusterv1alpha1.DNSViewCloudInfo {
	if in == nil {
		return nil
	}
	var dm *clusterv1alpha1.DNSViewCloudInfoDelegatedMember
	if in.DelegatedMember != nil {
		dm = &clusterv1alpha1.DNSViewCloudInfoDelegatedMember{
			Ipv4Addr: in.DelegatedMember.Ipv4Addr,
			Ipv6Addr: in.DelegatedMember.Ipv6Addr,
			Name:     in.DelegatedMember.Name,
		}
	}
	return &clusterv1alpha1.DNSViewCloudInfo{
		DelegatedMember: dm,
		DelegatedScope:  in.DelegatedScope,
		DelegatedRoot:   in.DelegatedRoot,
		OwnedByAdaptor:  in.OwnedByAdaptor,
		Usage:           in.Usage,
		Tenant:          in.Tenant,
		MgmtPlatform:    in.MgmtPlatform,
		AuthorityType:   in.AuthorityType,
	}
}
