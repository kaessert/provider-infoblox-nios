package extensibleattributedef

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/extensibleattributedef/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
)

const clusterControllerName = "cluster-extensibleattributedef.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=extensibleattributedef.infobloxnios.crossplane.io,resources=extensibleattributedefs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensibleattributedef.infobloxnios.crossplane.io,resources=extensibleattributedefs/status,verbs=get;update;patch

// clusterConnector implements
// managed.TypedExternalConnector[*clusterv1alpha1.ExtensibleAttributeDef].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// (legacy) ClusterProviderConfig-equivalent — the cluster-scoped
// ProviderConfig — and returns an authenticated WAPI Connector.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.ExtensibleAttributeDef) (managed.TypedExternalClient[*clusterv1alpha1.ExtensibleAttributeDef], error) {
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

// clusterExternal implements
// managed.TypedExternalClient[*clusterv1alpha1.ExtensibleAttributeDef].
type clusterExternal struct {
	kube k8sclient.Client
	conn *ibclient.Connector
}

// listValuesToStrings converts the cluster-scoped CRD's []EADefListValue
// into the shared []string representation.
func listValuesToStrings(vals []clusterv1alpha1.EADefListValue) []string {
	if len(vals) == 0 {
		return nil
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = v.Value
	}
	return out
}

// stringsToListValues converts the shared []string representation back
// into the cluster-scoped CRD's []EADefListValue.
func stringsToListValues(vals []string) []clusterv1alpha1.EADefListValue {
	if len(vals) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.EADefListValue, len(vals))
	for i, v := range vals {
		out[i] = clusterv1alpha1.EADefListValue{Value: v}
	}
	return out
}

// Observe fetches the ExtensibleAttributeDef from the WAPI by its _ref
// external name and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.ExtensibleAttributeDef) (managed.ExternalObservation, error) {
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

	def, err := getEADefinitionByRef(e.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveEADefinition)
	}

	o := observeFromEADefinition(externalID, def)
	cr.Status.AtProvider = clusterv1alpha1.ExtensibleAttributeDefObservation{
		Name:               o.Name,
		Type:               o.Type,
		Comment:            o.Comment,
		DefaultValue:       o.DefaultValue,
		Min:                o.Min,
		Max:                o.Max,
		Flags:              o.Flags,
		ListValues:         stringsToListValues(o.ListValues),
		AllowedObjectTypes: o.AllowedObjectTypes,
		Ref:                o.Ref,
		Namespace:          o.Namespace,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	listValues := listValuesToStrings(p.ListValues)
	lateInit := lateInitialize(&p.Comment, &p.DefaultValue, &p.Flags, &listValues, &p.AllowedObjectTypes, def)
	p.ListValues = stringsToListValues(listValues)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.Comment, p.DefaultValue, p.Flags, listValuesToStrings(p.ListValues), p.AllowedObjectTypes, def),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new ExtensibleAttributeDef and records the
// server-assigned _ref as the external name.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.ExtensibleAttributeDef) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	def, err := createEADefinition(e.conn, p.Name, p.Type, p.Comment, p.DefaultValue, p.Min, p.Max, p.Flags,
		toSDKListValues(listValuesToStrings(p.ListValues)), p.AllowedObjectTypes, descendantsActionToSDK(p.DescendantsAction))
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateEADefinition)
	}

	meta.SetExternalName(cr, def.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ExtensibleAttributeDef fields. type/min/max
// (immutable) are never sent — see updateEADefinition.
func (e *clusterExternal) Update(ctx context.Context, cr *clusterv1alpha1.ExtensibleAttributeDef) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	newRef, err := updateEADefinition(e.conn, externalID, p.Name, p.Comment, p.DefaultValue, p.Flags,
		toSDKListValues(listValuesToStrings(p.ListValues)), p.AllowedObjectTypes, descendantsActionToSDK(p.DescendantsAction))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateEADefinition)
	}

	// Live verification (see ADR-IN-0004) confirmed renaming an
	// ExtensibleAttributeDef changes its _ref, so the external-name
	// annotation must be refreshed here.
	if newRef != "" && newRef != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, newRef); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the ExtensibleAttributeDef. A 404 against the stored
// _ref is not treated as already-deleted by itself — see
// deleteEADefinitionResolving404 — because the _ref is a derived handle
// (name-based) that rotates when name changes.
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.ExtensibleAttributeDef) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteEADefinitionResolving404(e.conn, externalID, cr.Spec.ForProvider.Name); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.ExtensibleAttributeDef] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.ExtensibleAttributeDef]    = &clusterExternal{}
)

// setupClusterExtensibleAttributeDef wires the cluster-scoped
// ExtensibleAttributeDef reconciler with the controller-runtime manager.
// Called from SetupGated (gate callback) and Setup (immediate path) in
// controller.go.
func setupClusterExtensibleAttributeDef(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.ExtensibleAttributeDefList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster ExtensibleAttributeDef state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.ExtensibleAttributeDef](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("ExtensibleAttributeDef")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.ExtensibleAttributeDef{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
