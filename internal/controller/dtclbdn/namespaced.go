package dtclbdn

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
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtclbdn/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/config"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/externalname"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/statemetrics"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/driftdetection"
)

const namespacedControllerName = "namespaced-dtclbdn.infobloxnios.m.crossplane.io"

// ── Namespaced controller ────────────────────────────────────────────────

// +kubebuilder:rbac:groups=dtclbdn.infobloxnios.m.crossplane.io,resources=dtclbdns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=dtclbdn.infobloxnios.m.crossplane.io,resources=dtclbdns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=dtcpool.infobloxnios.crossplane.io,resources=dtcpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=zoneauth.infobloxnios.crossplane.io,resources=zoneauths,verbs=get;list;watch

// namespacedConnector implements managed.TypedExternalConnector[*namespacedv1alpha1.DTCLBDN].
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
// authenticated WAPI client bundle.
func (c *namespacedConnector) Connect(ctx context.Context, cr *namespacedv1alpha1.DTCLBDN) (managed.TypedExternalClient[*namespacedv1alpha1.DTCLBDN], error) {
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

	clients := newClients(conn.Connector)

	return &namespacedExternal{kube: c.kube, clients: clients, endpoint: conn.Endpoint}, nil
}

// namespacedExternal implements managed.TypedExternalClient[*namespacedv1alpha1.DTCLBDN].
type namespacedExternal struct {
	kube    k8sclient.Client
	clients *dtcLbdnClients
	// prober checks the identity extensible-attribute-definition
	// prerequisite before Create stamps identity onto a new object. nil
	// defaults to identity.DefaultProber.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key.
	endpoint string
}

// Observe resolves the DTCLBDN through the shared UID-in-EA identity
// ladder and compares the result against the desired spec.
func (e *namespacedExternal) Observe(ctx context.Context, cr *namespacedv1alpha1.DTCLBDN) (managed.ExternalObservation, error) {
	p := &cr.Spec.ForProvider

	res, err := observeDtcLbdn(ctx, e.clients.conn, e.prober, e.endpoint, cr.GetName(), meta.GetExternalName(cr), string(cr.GetUID()),
		&p.Priority, &p.Persistence, &p.Topology, &p.TTL, &p.UseTTL, &p.Comment, &p.Disable, &p.ExtAttrs)
	if err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return managed.ExternalObservation{}, err
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveDTCLBDN)
	}
	if !res.exists {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	rec := res.rec
	o := res.obs
	cr.Status.AtProvider = namespacedv1alpha1.DTCLBDNObservation{
		Name:        o.Name,
		LBMethod:    o.LBMethod,
		Patterns:    o.Patterns,
		AuthZones:   o.AuthZones,
		Types:       o.Types,
		Priority:    o.Priority,
		Persistence: o.Persistence,
		Topology:    o.Topology,
		TTL:         o.TTL,
		UseTTL:      o.UseTTL,
		Comment:     o.Comment,
		Disable:     o.Disable,
		ExtAttrs:    o.ExtAttrs,
		Ref:         o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID
	cr.Status.AtProvider.Pools = poolsToNamespaced(o.Pools)
	cr.Status.AtProvider.Health = healthToNamespaced(o.Health)

	pools := poolsFromNamespaced(p.Pools)

	if res.refreshedRef != "" {
		meta.SetExternalName(cr, res.refreshedRef)
	}

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv2.Available())

	upToDate := isUpToDate(p.Name, p.LBMethod, p.Patterns, pools, p.AuthZones, p.Types, p.Priority, p.Persistence, p.Topology, p.TTL, p.UseTTL, p.Comment, p.Disable, p.ExtAttrs, rec) && !res.adopted

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        upToDate,
		ResourceLateInitialized: res.lateInit,
	}, nil
}

// Create provisions a new DTCLBDN, stamping the managed resource's own
// uid into the object's identity extensible attribute in the same
// request, and records the server-assigned _ref as the external name.
func (e *namespacedExternal) Create(ctx context.Context, cr *namespacedv1alpha1.DTCLBDN) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	uid := string(cr.GetUID())

	if strings.TrimSpace(uid) == "" {
		return managed.ExternalCreation{}, errors.New(errEmptyUID)
	}
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.clients.conn, e.endpoint); err != nil {
		return managed.ExternalCreation{}, err
	}

	rec, err := createDtcLbdn(e.clients.conn, p.Name, p.LBMethod, p.Patterns, poolsFromNamespaced(p.Pools), p.AuthZones, p.Types, p.Priority, p.Persistence, p.Topology, p.TTL, p.UseTTL, p.Comment, p.Disable, p.ExtAttrs, uid)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateDTCLBDN)
	}

	meta.SetExternalName(cr, rec.Ref)
	return managed.ExternalCreation{}, nil
}

// Update replaces the mutable DTCLBDN fields. There are no known
// immutable fields for DTCLBDN, so every field is echoed (this API uses
// PUT full-replace semantics). Every call re-asserts the identity stamp
// since a WAPI PUT carrying extattrs replaces the whole map rather than
// merging it.
func (e *namespacedExternal) Update(ctx context.Context, cr *namespacedv1alpha1.DTCLBDN) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	// Every mutating PUT re-asserts the identity
	// stamp, so Update depends on the definition existing exactly like
	// Create — unlike the search paths (Observe/Delete), which only
	// need it reactively when a search actually fails.
	if err := ensureIdentityPrerequisite(ctx, e.prober, e.clients.conn, e.endpoint); err != nil {
		return managed.ExternalUpdate{}, err
	}

	rec, err := updateDtcLbdn(e.clients.conn, externalID, p.Name, p.LBMethod, p.Patterns, poolsFromNamespaced(p.Pools), p.AuthZones, p.Types, p.Priority, p.Persistence, p.Topology, p.TTL, p.UseTTL, p.Comment, p.Disable, p.ExtAttrs, string(cr.GetUID()))
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateDTCLBDN)
	}

	// See clusterExternal.Update — UpdateDtcLbdn always returns the
	// object's current _ref, and renaming may change it.
	if rec.Ref != "" && rec.Ref != externalID {
		if err := externalname.Refresh(ctx, e.kube, cr, rec.Ref); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errPersistExternalName)
		}
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the DTCLBDN, resolving through the shared identity
// ladder first — see deleteDtcLbdnIdentity for the full ownership-
// verification rules a stale or rotated _ref must satisfy before a
// delete is issued.
func (e *namespacedExternal) Delete(ctx context.Context, cr *namespacedv1alpha1.DTCLBDN) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteDtcLbdnIdentity(ctx, e.clients.conn, e.clients.objMgr, e.prober, e.endpoint, externalID, string(cr.GetUID())); err != nil {
		return managed.ExternalDelete{}, err
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *namespacedExternal) Disconnect(_ context.Context) error { return nil }

// ── namespaced CRD <-> shared-type conversion ───────────────────────────

func poolsFromNamespaced(pools []namespacedv1alpha1.DTCLBDNPoolLink) []poolLink {
	if len(pools) == 0 {
		return nil
	}
	out := make([]poolLink, 0, len(pools))
	for _, p := range pools {
		out = append(out, poolLink{Pool: p.Pool, Ratio: p.Ratio})
	}
	return out
}

func poolsToNamespaced(pools []poolLink) []namespacedv1alpha1.DTCLBDNPoolLink {
	if len(pools) == 0 {
		return nil
	}
	out := make([]namespacedv1alpha1.DTCLBDNPoolLink, 0, len(pools))
	for _, p := range pools {
		out = append(out, namespacedv1alpha1.DTCLBDNPoolLink{Pool: p.Pool, Ratio: p.Ratio})
	}
	return out
}

func healthToNamespaced(h *dtcHealth) *namespacedv1alpha1.DTCLBDNHealth {
	if h == nil {
		return nil
	}
	return &namespacedv1alpha1.DTCLBDNHealth{
		Availability: h.Availability,
		Description:  h.Description,
		EnabledState: h.EnabledState,
	}
}

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*namespacedv1alpha1.DTCLBDN] = &namespacedConnector{}
	_ managed.TypedExternalClient[*namespacedv1alpha1.DTCLBDN]    = &namespacedExternal{}
)

// setupNamespacedDTCLBDN wires the namespaced DTCLBDN reconciler with
// the controller-runtime manager. Called from SetupGated (gate callback)
// and Setup (immediate path) in controller.go.
func setupNamespacedDTCLBDN(mgr ctrl.Manager, o controller.Options) error {
	name := namespacedControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewResilientRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&namespacedv1alpha1.DTCLBDNList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register namespaced DTCLBDN state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*namespacedv1alpha1.DTCLBDN](driftdetection.WrapConnector[*namespacedv1alpha1.DTCLBDN](&namespacedConnector{
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
		resource.ManagedKind(namespacedv1alpha1.SchemeGroupVersion.WithKind("DTCLBDN")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&namespacedv1alpha1.DTCLBDN{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
