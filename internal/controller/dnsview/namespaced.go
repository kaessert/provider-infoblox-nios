package dnsview

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

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dnsview/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-dnsview.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=dnsview.infobloxnios.m.crossplane.io,resources=dnsviews,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dnsview.infobloxnios.m.crossplane.io,resources=dnsviews/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.DNSView].
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
// authenticated WAPI connector.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.DNSView) (managed.TypedExternalClient[*namespacedv1alpha1.DNSView], error) {
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

	conn, err := newConnector(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &namespacedExternal{kube: c.kube, conn: conn, endpoint: creds.Host}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.DNSView].
type namespacedExternal struct {
	kube k8sclient.Client
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key.
	endpoint string
}

// Observe resolves the DNSView through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.DNSView) (managed.ExternalObservation, error) {
	ref := observeRefFor(cr.GetName(), meta.GetExternalName(cr))

	v, outcome, err := resolveViewIdentity(ctx, e.conn, ref, string(cr.GetUID()))
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); prereqErr != nil {
				return managed.ExternalObservation{}, prereqErr
			}
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDNSView)
	}
	if outcome == identity.OutcomeNotFound {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	observed := fieldsFromView(v)
	cloudInfo := cloudInfoValueFromSDK(v.CloudInfo)
	cr.Status.AtProvider = namespacedObservationFromFields(observed, v.Ref, strPtrOrNil(v.Ref), &v.IsDefault, cloudInfo)
	// Explicit assignment (rather than relying solely on the ID field
	// folded into the struct literal above) keeps the server-assigned
	// identifier's provenance obvious at the call site — it always
	// mirrors the external name used to fetch this view.
	cr.Status.AtProvider.ID = v.Ref

	desired := fieldsFromNamespacedParams(&cr.Spec.ForProvider)
	// Strip the reserved identity extattr before comparing/late-initializing
	// — the CRD schema never includes it, and the full-mirror AtProvider
	// copy above intentionally keeps the unstripped map (convention 0032).
	observedForCompare := observed
	observedForCompare.ExtAttrs = extAttrsFromEA(identity.Strip(v.Ea))
	lateInit, changed := lateInitializeFields(desired, observedForCompare)
	if changed {
		applyFieldsToNamespacedParams(lateInit, &cr.Spec.ForProvider)
	}

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		meta.SetExternalName(cr, v.Ref)
		changed = true
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(lateInit, observedForCompare) && outcome != identity.OutcomeAdopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: changed,
	}, nil
}

// Create provisions a new DNSView, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.DNSView) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	f := fieldsFromNamespacedParams(&cr.Spec.ForProvider)
	ref, err := createView(e.conn, f, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDNSView)
	}

	meta.SetExternalName(cr, ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable DNSView fields (is_default is read-only and
// never sent — see buildView). WAPI's view PUT is a partial merge, but
// every call re-asserts the identity stamp since buildView always
// populates Ea.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.DNSView) (managed.ExternalUpdate, error) {
	f := fieldsFromNamespacedParams(&cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	ref, err := updateView(e.conn, externalID, f, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDNSView)
	}

	// See clusterExternal.Update — DNSView is in the _ref-unstable
	// resource group, and renaming the view changes its _ref.
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
// clears) without taking Grid-wide DNS resolution down with it. For a
// custom view, deleteViewIdentity resolves through the shared identity
// ladder first — see its doc for the full ownership-verification rules a
// stale or rotated _ref must satisfy before a delete is issued.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.DNSView) (managed.ExternalDelete, error) {
	name := cr.Status.AtProvider.Name
	if name == nil {
		name = cr.Spec.ForProvider.Name
	}
	if isWellKnownDNSViewName(name) {
		return managed.ExternalDelete{}, nil
	}

	externalID := meta.GetExternalName(cr)
	if err := deleteViewIdentity(ctx, e.conn, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.DNSView] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.DNSView]    = &namespacedExternal{}
)

// setupNamespacedDNSView wires the namespaced DNSView reconciler with the
// controller-runtime manager. Called from SetupGated (gate callback) and
// Setup (immediate path) in controller.go.
func setupNamespacedDNSView(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.DNSViewList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced DNSView state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.DNSView](driftdetection.WrapConnector[*namespacedv1alpha1.DNSView](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("DNSView")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.DNSView{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// ── Namespaced field bag conversions ─────────────────────────────────────

// fieldsFromNamespacedParams extracts the scope-neutral field bag from a Namespaced-scoped DNSViewParameters.
func fieldsFromNamespacedParams(p *namespacedv1alpha1.DNSViewParameters) dnsViewFields {
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
		CustomRootNameServers:               nameServerValuesFromNamespaced(p.CustomRootNameServers),
		DnssecTrustedKeys:                   dnssecTrustedKeyValuesFromPtrNamespaced(p.DnssecTrustedKeys),
		FixedRrsetOrderFqdns:                fixedRrsetOrderFqdnValuesFromPtrNamespaced(p.FixedRrsetOrderFqdns),
		FilterAaaaList:                      addressAcValuesFromPtrNamespaced(p.FilterAaaaList),
		MatchClients:                        addressAcValuesFromPtrNamespaced(p.MatchClients),
		MatchDestinations:                   addressAcValuesFromPtrNamespaced(p.MatchDestinations),
		Sortlist:                            sortlistEntryValuesFromPtrNamespaced(p.Sortlist),
		ResponseRateLimiting:                responseRateLimitingValueFromNamespaced(p.ResponseRateLimiting),
		ScavengingSettings:                  scavengingSettingsValueFromNamespaced(p.ScavengingSettings),
	}
}

// applyFieldsToNamespacedParams writes the field bag back into a Namespaced-scoped DNSViewParameters (used after late-init back-fill).
func applyFieldsToNamespacedParams(f dnsViewFields, p *namespacedv1alpha1.DNSViewParameters) {
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
	p.CustomRootNameServers = nameServerValuesToNamespaced(f.CustomRootNameServers)
	p.DnssecTrustedKeys = dnssecTrustedKeyValuesToPtrNamespaced(f.DnssecTrustedKeys)
	p.FixedRrsetOrderFqdns = fixedRrsetOrderFqdnValuesToPtrNamespaced(f.FixedRrsetOrderFqdns)
	p.FilterAaaaList = addressAcValuesToPtrNamespaced(f.FilterAaaaList)
	p.MatchClients = addressAcValuesToPtrNamespaced(f.MatchClients)
	p.MatchDestinations = addressAcValuesToPtrNamespaced(f.MatchDestinations)
	p.Sortlist = sortlistEntryValuesToPtrNamespaced(f.Sortlist)
	p.ResponseRateLimiting = responseRateLimitingValueToNamespaced(f.ResponseRateLimiting)
	p.ScavengingSettings = scavengingSettingsValueToNamespaced(f.ScavengingSettings)
}

// namespacedObservationFromFields builds a Namespaced-scoped DNSViewObservation from the field bag plus the response-only fields (id, ref, isDefault, cloudInfo).
func namespacedObservationFromFields(f dnsViewFields, id string, ref *string, isDefault *bool, cloudInfo *cloudInfoValue) namespacedv1alpha1.DNSViewObservation {
	o := namespacedv1alpha1.DNSViewObservation{
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
		CustomRootNameServers:               nameServerValuesToNamespaced(f.CustomRootNameServers),
		DnssecTrustedKeys:                   dnssecTrustedKeyValuesToPtrNamespaced(f.DnssecTrustedKeys),
		FixedRrsetOrderFqdns:                fixedRrsetOrderFqdnValuesToPtrNamespaced(f.FixedRrsetOrderFqdns),
		FilterAaaaList:                      addressAcValuesToPtrNamespaced(f.FilterAaaaList),
		MatchClients:                        addressAcValuesToPtrNamespaced(f.MatchClients),
		MatchDestinations:                   addressAcValuesToPtrNamespaced(f.MatchDestinations),
		Sortlist:                            sortlistEntryValuesToPtrNamespaced(f.Sortlist),
		ResponseRateLimiting:                responseRateLimitingValueToNamespaced(f.ResponseRateLimiting),
		ScavengingSettings:                  scavengingSettingsValueToNamespaced(f.ScavengingSettings),
		Ref:                                 ref,
		IsDefault:                           isDefault,
		CloudInfo:                           cloudInfoValueToNamespaced(cloudInfo),
	}
	return o
}

// ── Namespaced nested value bag conversions ──────────────────────────────

func nameServerValueFromNamespaced(in namespacedv1alpha1.DNSViewNameServer) nameServerValue {
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

func nameServerValueToNamespaced(in nameServerValue) namespacedv1alpha1.DNSViewNameServer {
	return namespacedv1alpha1.DNSViewNameServer{
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

func nameServerValuesFromNamespaced(in []namespacedv1alpha1.DNSViewNameServer) []nameServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]nameServerValue, 0, len(in))
	for _, item := range in {
		out = append(out, nameServerValueFromNamespaced(item))
	}
	return out
}

func nameServerValuesToNamespaced(in []nameServerValue) []namespacedv1alpha1.DNSViewNameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.DNSViewNameServer, 0, len(in))
	for _, item := range in {
		out = append(out, nameServerValueToNamespaced(item))
	}
	return out
}

func dnssecTrustedKeyValueFromNamespaced(in namespacedv1alpha1.DNSViewDnssecTrustedKey) dnssecTrustedKeyValue {
	return dnssecTrustedKeyValue{
		Fqdn:               in.Fqdn,
		Algorithm:          in.Algorithm,
		Key:                in.Key,
		SecureEntryPoint:   in.SecureEntryPoint,
		DnssecMustBeSecure: in.DnssecMustBeSecure,
	}
}

func dnssecTrustedKeyValueToNamespaced(in dnssecTrustedKeyValue) namespacedv1alpha1.DNSViewDnssecTrustedKey {
	return namespacedv1alpha1.DNSViewDnssecTrustedKey{
		Fqdn:               in.Fqdn,
		Algorithm:          in.Algorithm,
		Key:                in.Key,
		SecureEntryPoint:   in.SecureEntryPoint,
		DnssecMustBeSecure: in.DnssecMustBeSecure,
	}
}

func dnssecTrustedKeyValuesFromPtrNamespaced(in []*namespacedv1alpha1.DNSViewDnssecTrustedKey) []dnssecTrustedKeyValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]dnssecTrustedKeyValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, dnssecTrustedKeyValueFromNamespaced(*item))
	}
	return out
}

func dnssecTrustedKeyValuesToPtrNamespaced(in []dnssecTrustedKeyValue) []*namespacedv1alpha1.DNSViewDnssecTrustedKey {
	if len(in) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.DNSViewDnssecTrustedKey, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := dnssecTrustedKeyValueToNamespaced(item)
		out = append(out, &crdItem)
	}
	return out
}

func addressAcValueFromNamespaced(in namespacedv1alpha1.DNSViewAddressAc) addressAcValue {
	return addressAcValue{
		Address:        in.Address,
		Permission:     in.Permission,
		TsigKey:        in.TsigKey,
		TsigKeyAlg:     in.TsigKeyAlg,
		TsigKeyName:    in.TsigKeyName,
		UseTsigKeyName: in.UseTsigKeyName,
	}
}

func addressAcValueToNamespaced(in addressAcValue) namespacedv1alpha1.DNSViewAddressAc {
	return namespacedv1alpha1.DNSViewAddressAc{
		Address:        in.Address,
		Permission:     in.Permission,
		TsigKey:        in.TsigKey,
		TsigKeyAlg:     in.TsigKeyAlg,
		TsigKeyName:    in.TsigKeyName,
		UseTsigKeyName: in.UseTsigKeyName,
	}
}

func addressAcValuesFromPtrNamespaced(in []*namespacedv1alpha1.DNSViewAddressAc) []addressAcValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]addressAcValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, addressAcValueFromNamespaced(*item))
	}
	return out
}

func addressAcValuesToPtrNamespaced(in []addressAcValue) []*namespacedv1alpha1.DNSViewAddressAc {
	if len(in) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.DNSViewAddressAc, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := addressAcValueToNamespaced(item)
		out = append(out, &crdItem)
	}
	return out
}

func fixedRrsetOrderFqdnValueFromNamespaced(in namespacedv1alpha1.DNSViewFixedRrsetOrderFqdn) fixedRrsetOrderFqdnValue {
	return fixedRrsetOrderFqdnValue{
		Fqdn:       in.Fqdn,
		RecordType: in.RecordType,
	}
}

func fixedRrsetOrderFqdnValueToNamespaced(in fixedRrsetOrderFqdnValue) namespacedv1alpha1.DNSViewFixedRrsetOrderFqdn {
	return namespacedv1alpha1.DNSViewFixedRrsetOrderFqdn{
		Fqdn:       in.Fqdn,
		RecordType: in.RecordType,
	}
}

func fixedRrsetOrderFqdnValuesFromPtrNamespaced(in []*namespacedv1alpha1.DNSViewFixedRrsetOrderFqdn) []fixedRrsetOrderFqdnValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]fixedRrsetOrderFqdnValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, fixedRrsetOrderFqdnValueFromNamespaced(*item))
	}
	return out
}

func fixedRrsetOrderFqdnValuesToPtrNamespaced(in []fixedRrsetOrderFqdnValue) []*namespacedv1alpha1.DNSViewFixedRrsetOrderFqdn {
	if len(in) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.DNSViewFixedRrsetOrderFqdn, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := fixedRrsetOrderFqdnValueToNamespaced(item)
		out = append(out, &crdItem)
	}
	return out
}

func expressionOpValueFromNamespaced(in namespacedv1alpha1.DNSViewExpressionOp) expressionOpValue {
	return expressionOpValue{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func expressionOpValueToNamespaced(in expressionOpValue) namespacedv1alpha1.DNSViewExpressionOp {
	return namespacedv1alpha1.DNSViewExpressionOp{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func expressionOpValuesFromPtrNamespaced(in []*namespacedv1alpha1.DNSViewExpressionOp) []expressionOpValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]expressionOpValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, expressionOpValueFromNamespaced(*item))
	}
	return out
}

func expressionOpValuesToPtrNamespaced(in []expressionOpValue) []*namespacedv1alpha1.DNSViewExpressionOp {
	if len(in) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.DNSViewExpressionOp, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := expressionOpValueToNamespaced(item)
		out = append(out, &crdItem)
	}
	return out
}

func eaExpressionOpValueFromNamespaced(in namespacedv1alpha1.DNSViewEaExpressionOp) eaExpressionOpValue {
	return eaExpressionOpValue{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func eaExpressionOpValueToNamespaced(in eaExpressionOpValue) namespacedv1alpha1.DNSViewEaExpressionOp {
	return namespacedv1alpha1.DNSViewEaExpressionOp{
		Op:      in.Op,
		Op1:     in.Op1,
		Op1Type: in.Op1Type,
		Op2:     in.Op2,
		Op2Type: in.Op2Type,
	}
}

func eaExpressionOpValuesFromPtrNamespaced(in []*namespacedv1alpha1.DNSViewEaExpressionOp) []eaExpressionOpValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]eaExpressionOpValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, eaExpressionOpValueFromNamespaced(*item))
	}
	return out
}

func eaExpressionOpValuesToPtrNamespaced(in []eaExpressionOpValue) []*namespacedv1alpha1.DNSViewEaExpressionOp {
	if len(in) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.DNSViewEaExpressionOp, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := eaExpressionOpValueToNamespaced(item)
		out = append(out, &crdItem)
	}
	return out
}

func sortlistEntryValueFromNamespaced(in namespacedv1alpha1.DNSViewSortlistEntry) sortlistEntryValue {
	return sortlistEntryValue{Address: in.Address, MatchList: in.MatchList}
}

func sortlistEntryValueToNamespaced(in sortlistEntryValue) namespacedv1alpha1.DNSViewSortlistEntry {
	return namespacedv1alpha1.DNSViewSortlistEntry{Address: in.Address, MatchList: in.MatchList}
}

func sortlistEntryValuesFromPtrNamespaced(in []*namespacedv1alpha1.DNSViewSortlistEntry) []sortlistEntryValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]sortlistEntryValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, sortlistEntryValueFromNamespaced(*item))
	}
	return out
}

func sortlistEntryValuesToPtrNamespaced(in []sortlistEntryValue) []*namespacedv1alpha1.DNSViewSortlistEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.DNSViewSortlistEntry, 0, len(in))
	for _, item := range in {
		item := item
		crdItem := sortlistEntryValueToNamespaced(item)
		out = append(out, &crdItem)
	}
	return out
}

func responseRateLimitingValueFromNamespaced(in *namespacedv1alpha1.DNSViewResponseRateLimiting) *responseRateLimitingValue {
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

func responseRateLimitingValueToNamespaced(in *responseRateLimitingValue) *namespacedv1alpha1.DNSViewResponseRateLimiting {
	if in == nil {
		return nil
	}
	return &namespacedv1alpha1.DNSViewResponseRateLimiting{
		EnableRrl:          in.EnableRrl,
		LogOnly:            in.LogOnly,
		ResponsesPerSecond: in.ResponsesPerSecond,
		Window:             in.Window,
		Slip:               in.Slip,
	}
}

func scavengingScheduleValueFromNamespaced(in *namespacedv1alpha1.DNSViewScavengingSchedule) *scavengingScheduleValue {
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

func scavengingScheduleValueToNamespaced(in *scavengingScheduleValue) *namespacedv1alpha1.DNSViewScavengingSchedule {
	if in == nil {
		return nil
	}
	return &namespacedv1alpha1.DNSViewScavengingSchedule{
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

func scavengingSettingsValueFromNamespaced(in *namespacedv1alpha1.DNSViewScavengingSettings) *scavengingSettingsValue {
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
		ScavengingSchedule:        scavengingScheduleValueFromNamespaced(in.ScavengingSchedule),
		ExpressionList:            expressionOpValuesFromPtrNamespaced(in.ExpressionList),
		EaExpressionList:          eaExpressionOpValuesFromPtrNamespaced(in.EaExpressionList),
	}
}

func scavengingSettingsValueToNamespaced(in *scavengingSettingsValue) *namespacedv1alpha1.DNSViewScavengingSettings {
	if in == nil {
		return nil
	}
	return &namespacedv1alpha1.DNSViewScavengingSettings{
		EnableScavenging:          in.EnableScavenging,
		EnableRecurrentScavenging: in.EnableRecurrentScavenging,
		EnableAutoReclamation:     in.EnableAutoReclamation,
		EnableRrLastQueried:       in.EnableRrLastQueried,
		EnableZoneLastQueried:     in.EnableZoneLastQueried,
		ReclaimAssociatedRecords:  in.ReclaimAssociatedRecords,
		ScavengingSchedule:        scavengingScheduleValueToNamespaced(in.ScavengingSchedule),
		ExpressionList:            expressionOpValuesToPtrNamespaced(in.ExpressionList),
		EaExpressionList:          eaExpressionOpValuesToPtrNamespaced(in.EaExpressionList),
	}
}

func cloudInfoValueToNamespaced(in *cloudInfoValue) *namespacedv1alpha1.DNSViewCloudInfo {
	if in == nil {
		return nil
	}
	var dm *namespacedv1alpha1.DNSViewCloudInfoDelegatedMember
	if in.DelegatedMember != nil {
		dm = &namespacedv1alpha1.DNSViewCloudInfoDelegatedMember{
			Ipv4Addr: in.DelegatedMember.Ipv4Addr,
			Ipv6Addr: in.DelegatedMember.Ipv6Addr,
			Name:     in.DelegatedMember.Name,
		}
	}
	return &namespacedv1alpha1.DNSViewCloudInfo{
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
