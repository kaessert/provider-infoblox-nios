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
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneauth/v1alpha1"
)

const clusterControllerName = "cluster-zoneauth.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=zoneauth.infobloxnios.crossplane.io,resources=zoneauths,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=zoneauth.infobloxnios.crossplane.io,resources=zoneauths/status,verbs=get;update;patch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.ZoneAuth].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI
// Connector.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.ZoneAuth) (managed.TypedExternalClient[*clusterv1alpha1.ZoneAuth], error) {
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

	return &clusterExternal{conn: conn}, nil
}

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.ZoneAuth].
type clusterExternal struct {
	conn ibclient.IBConnector
}

// clusterFieldsFromSpec converts a cluster-scoped ZoneAuthParameters into
// the scope-neutral field bag.
func clusterFieldsFromSpec(p *clusterv1alpha1.ZoneAuthParameters) zoneAuthFields {
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
		UseGridZoneTimer:    p.UseGridZoneTimer,
		NsGroup:             p.NsGroup,
		ExtAttrs:            p.ExtAttrs,
		GridPrimary:         clusterMemberServerValues(p.GridPrimary),
		GridSecondaries:     clusterMemberServerValues(p.GridSecondaries),
		ExternalPrimaries:   clusterExternalServerValues(p.ExternalPrimaries),
		ExternalSecondaries: clusterExternalServerValues(p.ExternalSecondaries),
		UseExternalPrimary:  p.UseExternalPrimary,
	}
}

func clusterMemberServerValues(in []clusterv1alpha1.MemberServer) []memberServerValue {
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
			PreferredPrimaries:       clusterExternalServerValues(m.PreferredPrimaries),
			EnablePreferredPrimaries: boolOrFalse(m.EnablePreferredPrimaries),
		})
	}
	return out
}

func clusterExternalServerValues(in []clusterv1alpha1.ExternalServer) []externalServerValue {
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

func clusterMemberServersFromValues(in []memberServerValue) []clusterv1alpha1.MemberServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.MemberServer, 0, len(in))
	for _, v := range in {
		out = append(out, clusterv1alpha1.MemberServer{
			Name:                     strPtrOrNil(v.Name),
			Stealth:                  &v.Stealth,
			GridReplicate:            &v.GridReplicate,
			Lead:                     &v.Lead,
			PreferredPrimaries:       clusterExternalServersFromValues(v.PreferredPrimaries),
			EnablePreferredPrimaries: &v.EnablePreferredPrimaries,
		})
	}
	return out
}

func clusterExternalServersFromValues(in []externalServerValue) []clusterv1alpha1.ExternalServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]clusterv1alpha1.ExternalServer, 0, len(in))
	for _, v := range in {
		out = append(out, clusterv1alpha1.ExternalServer{
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
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.ZoneAuth) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling GetObject with the
	// CR's Kubernetes name (not a real WAPI _ref) would error against
	// the API on every reconcile until Create() overwrites the
	// annotation with the real _ref.
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
	cr.Status.AtProvider.UseGridZoneTimer = observed.UseGridZoneTimer
	cr.Status.AtProvider.NsGroup = observed.NsGroup
	cr.Status.AtProvider.ExtAttrs = observed.ExtAttrs
	cr.Status.AtProvider.GridPrimary = clusterMemberServersFromValues(observed.GridPrimary)
	cr.Status.AtProvider.GridSecondaries = clusterMemberServersFromValues(observed.GridSecondaries)
	cr.Status.AtProvider.ExternalPrimaries = clusterExternalServersFromValues(observed.ExternalPrimaries)
	cr.Status.AtProvider.ExternalSecondaries = clusterExternalServersFromValues(observed.ExternalSecondaries)
	cr.Status.AtProvider.UseExternalPrimary = observed.UseExternalPrimary
	cr.Status.AtProvider.Ref = strPtrOrNil(rec.Ref)

	p := &cr.Spec.ForProvider
	desired := clusterFieldsFromSpec(p)
	updated, lateInit := lateInitializeFields(desired, observed)
	if lateInit {
		p.Comment = updated.Comment
		p.Disable = updated.Disable
		p.SoaDefaultTTL = updated.SoaDefaultTTL
		p.SoaExpire = updated.SoaExpire
		p.SoaNegativeTTL = updated.SoaNegativeTTL
		p.SoaRefresh = updated.SoaRefresh
		p.SoaRetry = updated.SoaRetry
		p.UseGridZoneTimer = updated.UseGridZoneTimer
		p.NsGroup = updated.NsGroup
		p.ExtAttrs = updated.ExtAttrs
		p.GridPrimary = clusterMemberServersFromValues(updated.GridPrimary)
		p.GridSecondaries = clusterMemberServersFromValues(updated.GridSecondaries)
		p.ExternalPrimaries = clusterExternalServersFromValues(updated.ExternalPrimaries)
		p.ExternalSecondaries = clusterExternalServersFromValues(updated.ExternalSecondaries)
		p.UseExternalPrimary = updated.UseExternalPrimary
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
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.ZoneAuth) (managed.ExternalCreation, error) {
	f := clusterFieldsFromSpec(&cr.Spec.ForProvider)
	ref, err := createZoneAuth(e.conn, f)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateZoneAuth)
	}

	meta.SetExternalName(cr, ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable ZoneAuth fields. fqdn/view/zoneFormat
// (immutable) are never sent — see buildZoneAuthForUpdate.
func (e *clusterExternal) Update(_ context.Context, cr *clusterv1alpha1.ZoneAuth) (managed.ExternalUpdate, error) {
	f := clusterFieldsFromSpec(&cr.Spec.ForProvider)
	externalID := meta.GetExternalName(cr)

	if _, err := updateZoneAuth(e.conn, externalID, f); err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateZoneAuth)
	}

	// _ref stability: fqdn/view/zone_format are all immutable, so the
	// _ref never changes after create — no external-name refresh needed
	// here (unlike ARecord, where renaming changes the _ref).
	return managed.ExternalUpdate{}, nil
}

// Delete removes the ZoneAuth. A 404 is treated as already-deleted
// (idempotent).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.ZoneAuth) (managed.ExternalDelete, error) {
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
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.ZoneAuth] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.ZoneAuth]    = &clusterExternal{}
)

// setupClusterZoneAuth wires the cluster-scoped ZoneAuth reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupClusterZoneAuth(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.ZoneAuthList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster ZoneAuth state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.ZoneAuth](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("ZoneAuth")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.ZoneAuth{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
