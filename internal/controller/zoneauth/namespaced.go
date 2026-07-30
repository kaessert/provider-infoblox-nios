package zoneauth

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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneauth/v1alpha1"
)

const namespacedControllerName = "namespaced-zoneauth.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=zoneauth.infobloxnios.m.crossplane.io,resources=zoneauths,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zoneauth.infobloxnios.m.crossplane.io,resources=zoneauths/status,verbs=get;update;patch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.ZoneAuth].
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
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.ZoneAuth) (managed.TypedExternalClient[*namespacedv1alpha1.ZoneAuth], error) {
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

	return &namespacedExternal{conn: conn}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.ZoneAuth].
type namespacedExternal struct {
	conn ibclient.IBConnector
}

// namespacedFieldsFromSpec converts a namespaced ZoneAuthParameters into
// the scope-neutral field bag.
func namespacedFieldsFromSpec(p *namespacedv1alpha1.ZoneAuthParameters) zoneAuthFields {
	return zoneAuthFields{
		FQDN:                strOrEmpty(p.FQDN),
		View:                p.View,
		ZoneFormat:          strOrEmpty(p.ZoneFormat),
		Comment:             p.Comment,
		Disable:             p.Disable,
		SoaDefaultTTL:       p.SoaDefaultTTL,
		SoaExpire:           p.SoaExpire,
		SoaNegativeTTL:      p.SoaNegativeTTL,
		SoaRefresh:          p.SoaRefresh,
		SoaRetry:            p.SoaRetry,
		NsGroup:             p.NsGroup,
		ExtAttrs:            p.ExtAttrs,
		GridPrimary:         namespacedMemberServerValues(p.GridPrimary),
		GridSecondaries:     namespacedMemberServerValues(p.GridSecondaries),
		ExternalPrimaries:   namespacedExternalServerValues(p.ExternalPrimaries),
		ExternalSecondaries: namespacedExternalServerValues(p.ExternalSecondaries),
	}
}

func namespacedMemberServerValues(in []namespacedv1alpha1.MemberServer) []memberServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]memberServerValue, 0, len(in))
	for _, m := range in {
		out = append(out, memberServerValue{
			Name:                     strOrEmpty(m.Name),
			Stealth:                  boolOrFalse(m.Stealth),
			GridReplicate:            boolOrFalse(m.GridReplicate),
			Lead:                     boolOrFalse(m.Lead),
			PreferredPrimaries:       namespacedExternalServerValues(m.PreferredPrimaries),
			EnablePreferredPrimaries: boolOrFalse(m.EnablePreferredPrimaries),
		})
	}
	return out
}

func namespacedExternalServerValues(in []namespacedv1alpha1.ExternalServer) []externalServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]externalServerValue, 0, len(in))
	for _, s := range in {
		out = append(out, externalServerValue{
			Address:                      strOrEmpty(s.Address),
			Name:                         strOrEmpty(s.Name),
			Stealth:                      boolOrFalse(s.Stealth),
			SharedWithMsParentDelegation: boolOrFalse(s.SharedWithMsParentDelegation),
			TsigKey:                      strOrEmpty(s.TsigKey),
			TsigKeyAlg:                   strOrEmpty(s.TsigKeyAlg),
			TsigKeyName:                  strOrEmpty(s.TsigKeyName),
			UseTsigKeyName:               boolOrFalse(s.UseTsigKeyName),
		})
	}
	return out
}

func namespacedMemberServersFromValues(in []memberServerValue) []namespacedv1alpha1.MemberServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.MemberServer, 0, len(in))
	for _, v := range in {
		out = append(out, namespacedv1alpha1.MemberServer{
			Name:                     strPtrOrNil(v.Name),
			Stealth:                  &v.Stealth,
			GridReplicate:            &v.GridReplicate,
			Lead:                     &v.Lead,
			PreferredPrimaries:       namespacedExternalServersFromValues(v.PreferredPrimaries),
			EnablePreferredPrimaries: &v.EnablePreferredPrimaries,
		})
	}
	return out
}

func namespacedExternalServersFromValues(in []externalServerValue) []namespacedv1alpha1.ExternalServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.ExternalServer, 0, len(in))
	for _, v := range in {
		out = append(out, namespacedv1alpha1.ExternalServer{
			Address:                      strPtrOrNil(v.Address),
			Name:                         strPtrOrNil(v.Name),
			Stealth:                      &v.Stealth,
			SharedWithMsParentDelegation: &v.SharedWithMsParentDelegation,
			TsigKey:                      strPtrOrNil(v.TsigKey),
			TsigKeyAlg:                   strPtrOrNil(v.TsigKeyAlg),
			TsigKeyName:                  strPtrOrNil(v.TsigKeyName),
			UseTsigKeyName:               &v.UseTsigKeyName,
		})
	}
	return out
}

// Observe fetches the ZoneAuth from the WAPI by its _ref external name
// and compares it against the desired spec.
func (e *namespacedExternal) Observe(_ context.Context, cr *namespacedv1alpha1.ZoneAuth) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy) — see
	// clusterExternal.Observe for the full rationale.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec, err := getZoneAuthByRef(e.conn, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveZoneAuth)
	}

	observed := fieldsFromZoneAuth(rec)
	cr.Status.AtProvider.ID = externalID
	cr.Status.AtProvider.FQDN = strPtrOrNil(observed.FQDN)
	cr.Status.AtProvider.View = observed.View
	cr.Status.AtProvider.ZoneFormat = strPtrOrNil(observed.ZoneFormat)
	cr.Status.AtProvider.Comment = observed.Comment
	cr.Status.AtProvider.Disable = observed.Disable
	cr.Status.AtProvider.SoaDefaultTTL = observed.SoaDefaultTTL
	cr.Status.AtProvider.SoaExpire = observed.SoaExpire
	cr.Status.AtProvider.SoaNegativeTTL = observed.SoaNegativeTTL
	cr.Status.AtProvider.SoaRefresh = observed.SoaRefresh
	cr.Status.AtProvider.SoaRetry = observed.SoaRetry
	cr.Status.AtProvider.NsGroup = observed.NsGroup
	cr.Status.AtProvider.ExtAttrs = observed.ExtAttrs
	cr.Status.AtProvider.GridPrimary = namespacedMemberServersFromValues(observed.GridPrimary)
	cr.Status.AtProvider.GridSecondaries = namespacedMemberServersFromValues(observed.GridSecondaries)
	cr.Status.AtProvider.ExternalPrimaries = namespacedExternalServersFromValues(observed.ExternalPrimaries)
	cr.Status.AtProvider.ExternalSecondaries = namespacedExternalServersFromValues(observed.ExternalSecondaries)
	cr.Status.AtProvider.Ref = strPtrOrNil(rec.Ref)

	p := &cr.Spec.ForProvider
	desired := namespacedFieldsFromSpec(p)
	updated, lateInit := lateInitializeFields(desired, observed)
	if lateInit {
		p.Comment = updated.Comment
		p.Disable = updated.Disable
		p.SoaDefaultTTL = updated.SoaDefaultTTL
		p.SoaExpire = updated.SoaExpire
		p.SoaNegativeTTL = updated.SoaNegativeTTL
		p.SoaRefresh = updated.SoaRefresh
		p.SoaRetry = updated.SoaRetry
		p.NsGroup = updated.NsGroup
		p.ExtAttrs = updated.ExtAttrs
		p.GridPrimary = namespacedMemberServersFromValues(updated.GridPrimary)
		p.GridSecondaries = namespacedMemberServersFromValues(updated.GridSecondaries)
		p.ExternalPrimaries = namespacedExternalServersFromValues(updated.ExternalPrimaries)
		p.ExternalSecondaries = namespacedExternalServersFromValues(updated.ExternalSecondaries)
		desired = updated
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(desired, observed),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new ZoneAuth and records the server-assigned _ref
// as the external name.
func (e *namespacedExternal) Create(_ context.Context, cr *namespacedv1alpha1.ZoneAuth) (managed.ExternalCreation, error) {
	f := namespacedFieldsFromSpec(&cr.Spec.ForProvider)
	ref, err := createZoneAuth(e.conn, f)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateZoneAuth)
	}

	meta.SetExternalName(cr, ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ZoneAuth fields. fqdn/view/zoneFormat
// (immutable) are never sent — see buildZoneAuthForUpdate.
func (e *namespacedExternal) Update(_ context.Context, cr *namespacedv1alpha1.ZoneAuth) (managed.ExternalUpdate, error) {
	f := namespacedFieldsFromSpec(&cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)

	if _, err := updateZoneAuth(e.conn, externalID, f); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateZoneAuth)
	}

	// _ref stability: see clusterExternal.Update — no external-name
	// refresh needed here.
	return managed.ExternalUpdate{}, nil
}

// Delete removes the ZoneAuth. A 404 is treated as already-deleted
// (idempotent).
func (e *namespacedExternal) Delete(_ context.Context, cr *namespacedv1alpha1.ZoneAuth) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteZoneAuth(e.conn, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteZoneAuth)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.ZoneAuth] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.ZoneAuth]    = &namespacedExternal{}
)

// setupNamespacedZoneAuth wires the namespaced ZoneAuth reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedZoneAuth(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.ZoneAuthList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced ZoneAuth state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.ZoneAuth](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("ZoneAuth")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.ZoneAuth{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
