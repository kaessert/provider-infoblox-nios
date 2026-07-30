package networkcontainer

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkcontainer/v1alpha1"
	apisv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
)

const clusterControllerName = "cluster-networkcontainer.infobloxnios.crossplane.io"

// ── Cluster-scoped controller ─────────────────────────────────────────────

// +kubebuilder:rbac:groups=networkcontainer.infobloxnios.crossplane.io,resources=networkcontainers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networkcontainer.infobloxnios.crossplane.io,resources=networkcontainers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=networkview.infobloxnios.crossplane.io,resources=networkviews,verbs=get;list;watch

// clusterConnector implements managed.TypedExternalConnector[*clusterv1alpha1.NetworkContainer].
// Cluster-scoped MRs always reference the legacy cluster-scoped
// ProviderConfig directly by name (no Kind field on the reference).
type clusterConnector struct {
	kube  k8sclient.Client
	usage *resource.LegacyProviderConfigUsageTracker
}

// Connect tracks ProviderConfig usage, resolves the referenced
// cluster-scoped ProviderConfig, and returns an authenticated WAPI
// ObjectManager.
func (c *clusterConnector) Connect(ctx context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.TypedExternalClient[*clusterv1alpha1.NetworkContainer], error) {
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

// clusterExternal implements managed.TypedExternalClient[*clusterv1alpha1.NetworkContainer].
type clusterExternal struct {
	objMgr ibclient.IBObjectManager
}

// Observe fetches the NetworkContainer from the WAPI by its _ref external
// name and compares it against the desired spec.
func (e *clusterExternal) Observe(_ context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalObservation, error) {
	externalID := meta.GetExternalName(cr)

	// Pre-create guard (server-assigned external-name strategy): the
	// default NameAsExternalName initializer sets external-name =
	// metadata.name before Create() has run. Calling
	// GetNetworkContainerByRef with the CR's Kubernetes name (not a real
	// WAPI _ref) would error against the API on every reconcile until
	// Create() overwrites the annotation with the real _ref.
	if externalID == cr.GetName() {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}

	nc, err := getNetworkContainerByRef(e.objMgr, externalID)
	if err != nil {
		if isNotFound(err) {
			return managed.ExternalObservation{ResourceExists: false}, nil
		}
		return managed.ExternalObservation{}, errors.Wrap(err, errObserveNetworkContainer)
	}

	o := observeFromNetworkContainer(externalID, nc)
	cr.Status.AtProvider = clusterv1alpha1.NetworkContainerObservation{
		NetworkView: o.NetworkView,
		Network:     o.Network,
		Comment:     o.Comment,
		ExtAttrs:    o.ExtAttrs,
		Ref:         o.Ref,
	}
	// Explicit assignment (rather than folding ID into the struct literal
	// above) keeps the server-assigned identifier's provenance obvious at
	// the call site — it always mirrors the external name used to fetch
	// this record, not a field returned inside the WAPI response body.
	cr.Status.AtProvider.ID = o.ID

	p := &cr.Spec.ForProvider
	lateInit := lateInitialize(&p.Network, &p.Comment, &p.ExtAttrs, nc)

	// Set Available condition — required in crossplane-runtime v2, not
	// set automatically.
	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceUpToDate:        isUpToDate(p.Comment, p.ExtAttrs, nc),
		ResourceLateInitialized: lateInit,
	}, nil
}

// Create provisions a new NetworkContainer and records the server-assigned
// _ref as the external name. Routes across three creation paths — see
// createOrAllocateNetworkContainer.
func (e *clusterExternal) Create(_ context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalCreation, error) {
	p := cr.Spec.ForProvider
	nc, err := createOrAllocateNetworkContainer(e.objMgr, p.NetworkView, p.Network, p.ParentCidr, p.Comment, p.AllocatePrefixLen, p.FilterParams, p.ExtAttrs)
	if err != nil {
		return managed.ExternalCreation{}, errors.Wrap(err, errCreateNetworkContainer)
	}

	meta.SetExternalName(cr, nc.Ref)
	return managed.ExternalCreation{}, nil
}

// Update patches the mutable NetworkContainer fields. networkView and
// network (immutable identity fields) are never sent — see
// updateNetworkContainer.
func (e *clusterExternal) Update(_ context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalUpdate, error) {
	p := cr.Spec.ForProvider
	externalID := meta.GetExternalName(cr)

	nc, err := updateNetworkContainer(e.objMgr, externalID, p.Comment, p.ExtAttrs)
	if err != nil {
		return managed.ExternalUpdate{}, errors.Wrap(err, errUpdateNetworkContainer)
	}

	// UpdateNetworkContainer always returns the object's current _ref.
	// The identity fields (networkView, network) are immutable, so the
	// _ref does not change across an update, but refreshing the
	// annotation here is a defensive no-op if that ever changes.
	if nc.Ref != "" && nc.Ref != externalID {
		meta.SetExternalName(cr, nc.Ref)
	}
	return managed.ExternalUpdate{}, nil
}

// Delete removes the NetworkContainer. A 404 is treated as
// already-deleted (idempotent).
func (e *clusterExternal) Delete(_ context.Context, cr *clusterv1alpha1.NetworkContainer) (managed.ExternalDelete, error) {
	externalID := meta.GetExternalName(cr)
	if err := deleteNetworkContainer(e.objMgr, externalID); err != nil {
		if isNotFound(err) {
			return managed.ExternalDelete{}, nil
		}
		return managed.ExternalDelete{}, errors.Wrap(err, errDeleteNetworkContainer)
	}
	return managed.ExternalDelete{}, nil
}

// Disconnect is a no-op — the WAPI Connector holds no persistent
// connection that needs explicit teardown per reconcile.
func (e *clusterExternal) Disconnect(_ context.Context) error { return nil }

// Ensure interface compliance at compile time.
var (
	_ managed.TypedExternalConnector[*clusterv1alpha1.NetworkContainer] = &clusterConnector{}
	_ managed.TypedExternalClient[*clusterv1alpha1.NetworkContainer]    = &clusterExternal{}
)

// setupClusterNetworkContainer wires the cluster-scoped NetworkContainer
// reconciler with the controller-runtime manager. Called from SetupGated
// (gate callback) and Setup (immediate path) in controller.go.
func setupClusterNetworkContainer(mgr ctrl.Manager, o controller.Options) error {
	name := clusterControllerName

	if o.MetricOptions != nil {
		if err := mgr.Add(statemetrics.NewMRStateRecorder(
			mgr.GetClient(),
			o.Logger,
			o.MetricOptions.MRStateMetrics,
			&clusterv1alpha1.NetworkContainerList{},
			o.MetricOptions.PollStateMetricInterval,
		)); err != nil {
			return errors.Wrap(err, "cannot register cluster NetworkContainer state recorder")
		}
	}

	opts := []managed.ReconcilerOption{
		managed.WithTypedExternalConnector[*clusterv1alpha1.NetworkContainer](&clusterConnector{
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
		resource.ManagedKind(clusterv1alpha1.SchemeGroupVersion.WithKind("NetworkContainer")),
		opts...,
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		WithEventFilter(resource.DesiredStateChanged()).
		For(&clusterv1alpha1.NetworkContainer{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
