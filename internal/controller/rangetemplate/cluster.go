package rangetemplate

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/rangetemplate/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-rangetemplate.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=rangetemplate.infobloxnios.crossplane.io,resources=rangetemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rangetemplate.infobloxnios.crossplane.io,resources=rangetemplates/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.RangeTemplate].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.RangeTemplate) (managed.TypedExternalClient[*clusterv1alpha1.RangeTemplate], error) {
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

	mc, err := newObjectManager(creds, sslVerify)
	if err != nil {
		return nil, err
	}

	return &clusterExternal{
		kube:     c.kube,
		objMgr:   mc.Manager,
		conn:     mc.Connector,
		endpoint: creds.Host,
	}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.RangeTemplate].
type clusterExternal struct {
	kube   k8sclient.Client
	objMgr ibclient.IBObjectManager
	// conn is the lower-level WAPI connector the identity ladder resolves
	// against directly — see resolveRangeTemplateIdentity.
	conn ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber — see ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// resolved by Connect from the ProviderConfig's Grid host.
	endpoint string
}

// ── cluster type <-> scope-neutral currency conversion ─────────────────────

func clusterOptionsToCommon(opts []*clusterv1alpha1.RangeTemplateOption) []templateOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]templateOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		out = append(out, templateOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

func commonToClusterOptions(opts []templateOption) []*clusterv1alpha1.RangeTemplateOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.RangeTemplateOption, 0, len(opts))
	for _, o := range opts {
		opt := o
		out = append(out, &clusterv1alpha1.RangeTemplateOption{Name: opt.Name, Num: opt.Num, VendorClass: opt.VendorClass, Value: opt.Value, UseOption: opt.UseOption})
	}
	return out
}

func clusterMemberToCommon(m *clusterv1alpha1.RangeTemplateMember) *templateMember {
	if m == nil {
		return nil
	}
	return &templateMember{Ipv4Addr: m.Ipv4Addr, Ipv6Addr: m.Ipv6Addr, Name: m.Name}
}

func commonToClusterMember(m *templateMember) *clusterv1alpha1.RangeTemplateMember {
	if m == nil {
		return nil
	}
	return &clusterv1alpha1.RangeTemplateMember{Ipv4Addr: m.Ipv4Addr, Ipv6Addr: m.Ipv6Addr, Name: m.Name}
}

// Observe resolves the RangeTemplate through the shared UID-in-EA
// identity ladder and compares the result against the desired spec.
func (e *clusterExternal) Observe(ctx context.Context, cr *clusterv1alpha1.RangeTemplate) (managed.ExternalObservation, error) {
	res, err := observeRangeTemplate(ctx, e.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()))
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveRangeTemplate)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	rec := res.rec

	o := res.obs
	cr.Status.AtProvider = clusterv1alpha1.RangeTemplateObservation{
		Name:                  o.Name,
		NumberOfAddresses:     o.NumberOfAddresses,
		Offset:                o.Offset,
		Comment:               o.Comment,
		ExtAttrs:              o.ExtAttrs,
		Options:               commonToClusterOptions(o.Options),
		UseOptions:            o.UseOptions,
		ServerAssociationType: o.ServerAssociationType,
		FailoverAssociation:   o.FailoverAssociation,
		Member:                commonToClusterMember(o.Member),
		CloudApiCompatible:    o.CloudApiCompatible,
		Ref:                   o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	options := clusterOptionsToCommon(p.Options)
	member := clusterMemberToCommon(p.Member)
	lateInit := lateInitialize(&p.Comment, &p.ExtAttrs, &options, &p.UseOptions, &p.ServerAssociationType, &p.FailoverAssociation, &member, &p.CloudApiCompatible, rec)
	if lateInit {
		p.Options = commonToClusterOptions(options)
		p.Member = commonToClusterMember(member)
	}
	if res.lateInit {
		lateInit = true
	}

	// A rotated or previously-unknown reference must be persisted through
	// a path crossplane-runtime actually writes back to the API server.
	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	// An adopted object (ref resolved, no identity stamp yet) must never
	// be reported up to date — see observeResult.adopted — so the next
	// reconcile is guaranteed to call Update, which always re-asserts the
	// identity stamp (see updateRangeTemplate).
	upToDate := isUpToDate(p.Name, p.NumberOfAddresses, p.Offset, p.Comment, p.ExtAttrs, clusterOptionsToCommon(p.Options), p.UseOptions, p.ServerAssociationType, p.FailoverAssociation, clusterMemberToCommon(p.Member), rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new RangeTemplate, stamping the managed resource's
// own uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *clusterExternal) Create(ctx context.Context, cr *clusterv1alpha1.RangeTemplate) (managed.ExternalCreation, error) {
	uid := string(cr.GetUID())
	if uid == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	p := cr.Spec.ForProvider
	rec, err := createRangeTemplate(e.objMgr, p.Name, p.NumberOfAddresses, p.Offset, p.Comment, p.ExtAttrs, clusterOptionsToCommon(p.Options), p.UseOptions, p.ServerAssociationType, p.FailoverAssociation, clusterMemberToCommon(p.Member), p.MsServer, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateRangeTemplate)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable RangeTemplate fields. No immutable fields are
// known for RangeTemplate — every ForProvider field is sent. Every call
// re-asserts the identity stamp.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.RangeTemplate) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	rec, err := updateRangeTemplate(e.objMgr, externalID, p.Name, p.NumberOfAddresses, p.Offset, p.Comment, p.ExtAttrs, clusterOptionsToCommon(p.Options), p.UseOptions, p.ServerAssociationType, p.FailoverAssociation, clusterMemberToCommon(p.Member), p.MsServer, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateRangeTemplate)
	}

	// UpdateRangeTemplate always returns the object's current _ref, and
	// renaming a range template (like ARecord) can change its _ref — see
	// object_manager_range_template.go's NewRangeTemplate/UpdateRangeTemplate,
	// which re-fetches by the new ref after the PUT.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the RangeTemplate, resolving through the shared
// identity ladder first.
func (e *clusterExternal) Delete(ctx context.Context, cr *clusterv1alpha1.RangeTemplate) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteRangeTemplateIdentity(ctx, e.conn, e.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.RangeTemplate] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.RangeTemplate]    = &clusterExternal{}
)

// setupClusterRangeTemplate wires the cluster-scoped RangeTemplate
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupClusterRangeTemplate(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.RangeTemplateList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster RangeTemplate state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.RangeTemplate](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("RangeTemplate")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.RangeTemplate{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
