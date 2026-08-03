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
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/extensibleattributedef/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
)

const namespacedControllerName = "namespaced-extensibleattributedef.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=extensibleattributedef.infobloxnios.m.crossplane.io,resources=extensibleattributedefs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=extensibleattributedef.infobloxnios.m.crossplane.io,resources=extensibleattributedefs/status,verbs=get;update;patch

// namespacedConnector implements
// managed.TypedExternalConnector[*namespacedv1alpha1.ExtensibleAttributeDef].
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
// authenticated WAPI Connector.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.ExtensibleAttributeDef) (managed.TypedExternalClient[*namespacedv1alpha1.ExtensibleAttributeDef], error) {
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

	return &namespacedExternal{kube: c.kube, conn: conn}, nil
}

// namespacedExternal implements
// managed.TypedExternalClient[*namespacedv1alpha1.ExtensibleAttributeDef].
type namespacedExternal struct {
	kube k8sclient.Client
	conn *ibclient.Connector
}

// namespacedListValuesToStrings converts the namespaced-scoped CRD's
// []EADefListValue into the shared []string representation.
func namespacedListValuesToStrings(vals []namespacedv1alpha1.EADefListValue) []string {
	if len(vals) == 0 {
		return nil
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = v.Value
	}
	return out
}

// namespacedStringsToListValues converts the shared []string
// representation back into the namespaced-scoped CRD's
// []EADefListValue.
func namespacedStringsToListValues(vals []string) []namespacedv1alpha1.EADefListValue {
	if len(vals) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.EADefListValue, len(vals))
	for i, v := range vals {
		out[i] = namespacedv1alpha1.EADefListValue{Value: v}
	}
	return out
}

// Observe fetches the ExtensibleAttributeDef from the WAPI by its _ref
// external name and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.ExtensibleAttributeDef) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	def, err := getEADefinitionByRef(e.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			// The stored external-name is a derived handle: it rotates
			// whenever an identity-composing field changes, so a 404 here
			// is not proof the object is gone (see the staleref package
			// doc). Resolve the natural key before concluding that.
			found, searchErr := eaDefinitionExistsByNaturalKey(e.conn, cr.Spec.ForProvider.Name)
			if searchErr != nil {
				return managed.ExternalObservation{}, errors.Wrap(searchErr, errObserveEADefinition)
			}
			if found {
				return managed.ExternalObservation{}, staleref.ObserveRefusalError()
			}
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveEADefinition)
	}

	o := observeFromEADefinition(externalID, def)
	cr.Status.AtProvider = namespacedv1alpha1.ExtensibleAttributeDefObservation{
		Name:               o.Name,
		Type:               o.Type,
		Comment:            o.Comment,
		DefaultValue:       o.DefaultValue,
		Min:                o.Min,
		Max:                o.Max,
		Flags:              o.Flags,
		ListValues:         namespacedStringsToListValues(o.ListValues),
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
	listValues := namespacedListValuesToStrings(p.ListValues)
	lateInit := lateInitialize(&p.Comment, &p.DefaultValue, &p.Flags, &listValues, &p.AllowedObjectTypes, def)
	p.ListValues = namespacedStringsToListValues(listValues)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Name, p.Comment, p.DefaultValue, p.Flags, namespacedListValuesToStrings(p.ListValues), p.AllowedObjectTypes, def),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new ExtensibleAttributeDef and records the
// server-assigned _ref as the external name.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.ExtensibleAttributeDef) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	if isReservedIdentityDefinitionName(p.Name) {
		return managed.ExternalCreation{}, errors.New(errReservedName)
	}
	// See refuseIfResolvedRefIsReserved: the annotation may already point
	// at the reserved definition's _ref even though the spec name above
	// checked out — resolve it before creating anything under its alias.
	if err := refuseIfResolvedRefIsReserved(e.conn, meta.GetExternalName(cr), cr.GetName()); err != nil {
		return managed.ExternalCreation{}, err
	}

	def, err := createEADefinition(e.conn, p.Name, p.Type, p.Comment, p.DefaultValue, p.Min, p.Max, p.Flags,
		toSDKListValues(namespacedListValuesToStrings(p.ListValues)), p.AllowedObjectTypes, descendantsActionToSDK(p.DescendantsAction))
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateEADefinition)
	}

	meta.SetExternalName(cr, def.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ExtensibleAttributeDef fields. type/min/max
// (immutable) are never sent — see updateEADefinition.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.ExtensibleAttributeDef) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	if isReservedIdentityDefinitionName(p.Name) {
		return managed.ExternalUpdate{}, errors.New(errReservedName)
	}

	externalID := meta.GetExternalName(cr)

	// The spec-name check above only sees spec.forProvider.name. Resolve
	// what externalID actually addresses before issuing the PUT — see
	// refuseIfResolvedRefIsReserved.
	if err := refuseIfResolvedRefIsReserved(e.conn, externalID, cr.GetName()); err != nil {
		return managed.ExternalUpdate{}, err
	}

	newRef, err := updateEADefinition(e.conn, externalID, p.Name, p.Comment, p.DefaultValue, p.Flags,
		toSDKListValues(namespacedListValuesToStrings(p.ListValues)), p.AllowedObjectTypes, descendantsActionToSDK(p.DescendantsAction))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateEADefinition)
	}

	// See clusterExternal.Update — renaming an ExtensibleAttributeDef
	// changes its _ref (live-verified against a real Grid).
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
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.ExtensibleAttributeDef) (managed.ExternalDelete, error) {
	if isReservedIdentityDefinitionName(cr.Spec.ForProvider.Name) {
		return managed.ExternalDelete{}, errors.New(errReservedName)
	}

	externalID := meta.GetExternalName(cr)

	// The spec-name check above only sees spec.forProvider.name. Resolve
	// what externalID actually addresses before issuing the DELETE — see
	// refuseIfResolvedRefIsReserved.
	if err := refuseIfResolvedRefIsReserved(e.conn, externalID, cr.GetName()); err != nil {
		return managed.ExternalDelete{}, err
	}

	if err := deleteEADefinitionResolving404(e.conn, externalID, cr.Spec.ForProvider.Name); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.ExtensibleAttributeDef] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.ExtensibleAttributeDef]    = &namespacedExternal{}
)

// setupNamespacedExtensibleAttributeDef wires the namespaced
// ExtensibleAttributeDef reconciler with the controller-runtime manager.
// Called from SetupGated (gate callback) and Setup (immediate path) in
// controller.go.
func setupNamespacedExtensibleAttributeDef(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.ExtensibleAttributeDefList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced ExtensibleAttributeDef state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.ExtensibleAttributeDef](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("ExtensibleAttributeDef")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.ExtensibleAttributeDef{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
