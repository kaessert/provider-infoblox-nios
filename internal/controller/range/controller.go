// Package rangepkg implements the Crossplane controller for the Infoblox
// NIOS Range managed resource (a DHCP address range within a Network).
// "range" is a reserved Go keyword, so the package (like the standalone
// apis/common/rangepkg reference copy) is named rangepkg even though the
// directory is "range" — the directory name is what wires this package
// into the provider's auto-generated controller registration.
//
// Like the ARecord controller, this wraps the official infoblox-go-client
// Go SDK directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateNetworkRange/GetNetworkRangeByRef/UpdateNetworkRange/
// DeleteNetworkRange) instead of a generic HTTP request/response
// envelope, so there is no internal REST client to compose.
//
// Range is wired to the UID-in-EA object-identity ladder (see the
// internal/clients/identity package doc): the WAPI _ref every create
// call returns is a derived handle — a rendering of the object's own
// start/end address pair, not a stable backend-assigned ID — so this
// controller stamps the managed resource's own metadata.uid onto the
// Grid object as an extensible attribute and resolves every
// Observe/Delete through the shared identity.Resolve ladder instead of
// trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package rangepkg

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/range/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/range/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage              = "cannot track ProviderConfig usage"
	errPersistExternalName       = "cannot persist refreshed external name"
	errGetPC                     = "cannot get ProviderConfig"
	errGetClusterPC              = "cannot get ClusterProviderConfig"
	errUnsupportedKind           = "unsupported provider config kind"
	errGetSecret                 = "cannot get credentials secret"
	errNoSecretRef               = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds          = "unsupported credentials source: only Secret is supported"
	errMissingCredKey            = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager          = "cannot create Infoblox NIOS WAPI object manager"
	errObserveRange              = "cannot observe Range"
	errCreateRange               = "cannot create Range"
	errUpdateRange               = "cannot update Range"
	errDeleteRange               = "cannot delete Range"
	errEmptyUID                  = "cannot stamp Range identity: managed resource's metadata.uid is empty"
	errDeleteUnverifiedOwnership = "refusing to delete: the resolved object's identity extensible attribute is absent or belongs to a different owner, so ownership cannot be verified before an irreversible delete. " +
		"Reconcile the external-name annotation, verify the Grid object manually, or remove the finalizer to abandon it without deleting."
	errPrerequisiteCheck = "cannot verify the identity extensible attribute definition prerequisite"
)

// unresolvedProbeEndpoint is the identity-prerequisite-probe cache key
// used when an ExternalClient is built without a resolved Grid endpoint.
// See the doc on this constant in the recorda package for the full
// rationale — production code always goes through Connect().
const unresolvedProbeEndpoint = "unresolved-grid-endpoint"

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
const wapiVersion = "2.9.7"

// ── Credential bridge ───────────────────────────────────────────────────────

// nioCredentials holds the WAPI connection parameters extracted from the
// ProviderConfig's credentials Secret (host/username/password keys). TLS
// verification is governed by the ProviderConfig's own sslVerify spec
// field, not by anything in this Secret.
type nioCredentials struct {
	Host     string
	Username string
	Password string
}

// extractCredentials reads the Secret referenced by a ProviderConfig's
// credentials block and parses the host/username/password keys. source and
// secretRef are the shared crossplane-runtime CommonCredentialSelectors
// fields, which are structurally identical across every ProviderConfig
// kind this provider defines (cluster ProviderConfig, namespaced
// ProviderConfig, namespaced ClusterProviderConfig) — so this single
// helper serves all three connectors.
func extractCredentials(ctx context.Context, kube k8sclient.Client, source xpv2.CredentialsSource, secretRef *xpv2.SecretKeySelector, fallbackNamespace string) (*nioCredentials, error) {
	if source != xpv2.CredentialsSourceSecret {
		return nil, errors.New(errUnsupportedCreds)
	}
	if secretRef == nil {
		return nil, errors.New(errNoSecretRef)
	}

	ns := secretRef.Namespace
	if ns == "" {
		ns = fallbackNamespace
	}

	secret := &corev1.Secret{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: ns, Name: secretRef.Name}, secret); err != nil {
		return nil, errors.Wrap(err, errGetSecret)
	}

	host := string(secret.Data["host"])
	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	if host == "" || username == "" || password == "" {
		return nil, errors.New(errMissingCredKey)
	}

	return &nioCredentials{Host: host, Username: username, Password: password}, nil
}

// newObjectManager constructs an authenticated identity.ManagerAndConnector
// from the given credentials — the SDK's high-level ObjectManager for the
// ordinary CRUD calls, and the lower-level Connector the identity ladder
// needs directly (it operates below ObjectManager's typed methods so it
// can see search match counts). The Connector performs HTTP Basic Auth on
// every request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newObjectManager(creds *nioCredentials, sslVerify bool) (identity.ManagerAndConnector, error) {
	return newObjectManagerWithScheme(creds, sslVerify, "https", "443")
}

// newObjectManagerWithScheme is the scheme/port-parameterized variant of
// newObjectManager used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newObjectManagerWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (identity.ManagerAndConnector, error) {
	hostConfig := ibclient.HostConfig{
		Scheme:  scheme,
		Host:    creds.Host,
		Version: wapiVersion,
		Port:    port,
	}
	authConfig := ibclient.AuthConfig{
		Username: creds.Username,
		Password: creds.Password,
	}
	// sslVerify comes from the ProviderConfig's own spec field (not
	// the credentials Secret) — see the Connect methods in
	// cluster.go/namespaced.go. Set to false only when the Grid
	// Manager uses a self-signed certificate whose SAN does not match
	// the reachable host address.
	sslVerifyStr := "true"
	if !sslVerify {
		sslVerifyStr = "false"
	}
	transportConfig := ibclient.NewTransportConfig(sslVerifyStr, 60, 10)

	conn, err := ibclient.NewConnector(
		hostConfig,
		authConfig,
		transportConfig,
		&ibclient.WapiRequestBuilder{},
		&ibclient.WapiHttpRequestor{},
	)
	if err != nil {
		return identity.ManagerAndConnector{}, errors.Wrap(err, errNewObjectManager)
	}

	return identity.NewManagerAndConnector(conn), nil
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// buildEA converts the CRD's simplified string-valued extensible-attributes
// map into the SDK's EA type (map[string]interface{}). The SDK wraps each
// value as {"value": ...} on the wire (see ibclient.EA.MarshalJSON);
// callers pass raw values here.
func buildEA(extAttrs map[string]string) ibclient.EA {
	if len(extAttrs) == 0 {
		return nil
	}
	ea := make(ibclient.EA, len(extAttrs))
	for k, v := range extAttrs {
		ea[k] = v
	}
	return ea
}

// extAttrsFromEA converts the SDK's EA map (arbitrary interface{} values,
// already unwrapped from the {"value": ...} envelope by EA.UnmarshalJSON)
// into the CRD's simplified string-valued map.
func extAttrsFromEA(ea ibclient.EA) map[string]string {
	if len(ea) == 0 {
		return nil
	}
	out := make(map[string]string, len(ea))
	for k, v := range ea {
		out[k] = stringifyEAValue(v)
	}
	return out
}

// stringifyEAValue renders an extensible-attribute value (which may be a
// string, an ibclient.Bool, an int, or a []string after EA.UnmarshalJSON
// decodes it) as its CRD string representation.
func stringifyEAValue(v interface{}) string {
	switch val := v.(type) {
	case nil:
		return ""
	case string:
		return val
	case ibclient.Bool:
		if val {
			return "True"
		}
		return "False"
	case []string:
		return strings.Join(val, ",")
	default:
		return fmt.Sprintf("%v", val)
	}
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// ── Error classification ─────────────────────────────────────────────────

// errStatusRe extracts the HTTP status code from the SDK's generic
// formatted error ("WAPI request error: <status>('<text>')\n..."). The Go
// SDK wraps HTTP errors this way for everything except object-not-found,
// which it surfaces as a typed *ibclient.NotFoundError instead — error
// classification must unwrap both forms.
var errStatusRe = regexp.MustCompile(`WAPI request error: (\d{3})`)

// isNotFound reports whether err indicates the WAPI object does not exist
// (HTTP 404).
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	var nfErr *ibclient.NotFoundError
	if errors.As(err, &nfErr) {
		return true
	}
	m := errStatusRe.FindStringSubmatch(err.Error())
	if len(m) != 2 {
		return false
	}
	code, convErr := strconv.Atoi(m[1])
	return convErr == nil && code == http.StatusNotFound
}

// ── Field comparison / late-init ────────────────────────────────────────
//
// These helpers take pointers to the individual ForProvider fields rather
// than a whole RangeParameters value. The cluster and namespaced
// RangeParameters types are structurally identical (same field names and
// primitive types) but are distinct named Go types, so parameterizing on
// the field pointers instead lets both scopes share this logic
// unconditionally.

// isUpToDate compares the desired Range fields against the observed Range.
// template is immutable (a create-only parameter accepted by
// CreateNetworkRange but absent from UpdateNetworkRange, and not returned
// by GetNetworkRangeByRef) and is intentionally excluded from this
// comparison and from AtProvider. The Grid's extattrs map is compared with
// the provider's own identity stamp stripped out (identity.Strip): the CRD
// schema never includes that reserved key, so leaving it in would produce
// a permanent phantom diff.
func isUpToDate(startAddr, endAddr, networkView, network, comment *string, extAttrs map[string]string, rng *ibclient.Range) bool {
	if strOrEmpty(startAddr) != strOrEmpty(rng.StartAddr) {
		return false
	}
	if strOrEmpty(endAddr) != strOrEmpty(rng.EndAddr) {
		return false
	}
	if strOrEmpty(networkView) != strOrEmpty(rng.NetworkView) {
		return false
	}
	if strOrEmpty(network) != strOrEmpty(rng.Network) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rng.Comment) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rng.Ea)))
}

// extAttrsEqual reports whether two extensible-attribute maps are
// equivalent, treating a nil map and an empty map as equal (avoids a
// false phantom diff when the API omits an empty extattrs object).
func extAttrsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// lateInitialize back-fills server-defaulted optional fields (networkView,
// network, comment, extAttrs) from the observed Range into spec so
// isUpToDate does not see phantom drift on the next reconcile.
// CreateNetworkRange itself defaults network_view to "default" server-side
// when the create request omits it, so a Range created through this
// controller already carries the resolved value in the create response —
// this back-fill additionally covers Ranges discovered via Observe that
// were created some other way. Required fields (startAddr, endAddr) and
// the immutable template field are never late-initialized. extAttrs is
// back-filled with the provider's own identity stamp stripped out
// (identity.Strip) — the CRD schema never includes that reserved key, so
// late-initializing it into spec.forProvider would fail CEL validation
// and produce a permanent diff. Returns true if any field was changed.
func lateInitialize(networkView, network, comment **string, extAttrs *map[string]string, rng *ibclient.Range) bool {
	changed := false

	if *networkView == nil && rng.NetworkView != nil && *rng.NetworkView != "" {
		nv := *rng.NetworkView
		*networkView = &nv
		changed = true
	}
	if *network == nil && rng.Network != nil && *rng.Network != "" {
		n := *rng.Network
		*network = &n
		changed = true
	}
	if *comment == nil && rng.Comment != nil && *rng.Comment != "" {
		c := *rng.Comment
		*comment = &c
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromRng := extAttrsFromEA(identity.Strip(rng.Ea)); len(fromRng) > 0 {
			*extAttrs = fromRng
			changed = true
		}
	}

	return changed
}

// observedRange holds the primitive field values extracted from a WAPI
// Range response. The cluster and namespaced RangeObservation types are
// structurally similar but are distinct named types, so each scope
// copies this intermediate struct's fields into its own Observation type
// at the call site.
type observedRange struct {
	ID          string
	StartAddr   *string
	EndAddr     *string
	NetworkView *string
	Network     *string
	Comment     *string
	ExtAttrs    map[string]string
	Ref         *string
}

// observeFromRange extracts the fields mirrored by RangeObservation (the
// full-mirror AtProvider convention) from a WAPI Range response. ExtAttrs
// intentionally mirrors the Grid's complete extattrs map, including the
// provider's own identity stamp — unlike isUpToDate and lateInitialize,
// AtProvider is a read-only status mirror, not compared against
// spec.forProvider, so surfacing the stamp there is informative rather
// than a source of phantom drift. template is deliberately excluded — it
// is a create-only parameter never echoed back by GetNetworkRange.
func observeFromRange(externalID string, rng *ibclient.Range) observedRange {
	o := observedRange{
		ID:       externalID,
		ExtAttrs: extAttrsFromEA(rng.Ea),
	}
	if rng.StartAddr != nil && *rng.StartAddr != "" {
		v := *rng.StartAddr
		o.StartAddr = &v
	}
	if rng.EndAddr != nil && *rng.EndAddr != "" {
		v := *rng.EndAddr
		o.EndAddr = &v
	}
	if rng.NetworkView != nil && *rng.NetworkView != "" {
		v := *rng.NetworkView
		o.NetworkView = &v
	}
	if rng.Network != nil && *rng.Network != "" {
		v := *rng.Network
		o.Network = &v
	}
	if rng.Comment != nil && *rng.Comment != "" {
		v := *rng.Comment
		o.Comment = &v
	}
	if rng.Ref != "" {
		v := rng.Ref
		o.Ref = &v
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createRange issues the WAPI create call, stamping the owning managed
// resource's uid into the object's extensible attributes in the same
// request that creates it (identity.Stamp) — there is no follow-up call,
// so there is no window in which the object exists without its identity
// stamp. name is always passed as "" (Range has no user-facing name field
// on this CRD — its identity is the start/end address pair, not a name);
// disable/member/failOverAssociation/options/useOptions/
// serverAssociation/msServer are not exposed by the CRD and are always
// passed at their zero value.
func createRange(objMgr ibclient.IBObjectManager, startAddr, endAddr, networkView, network, template, comment *string, extAttrs map[string]string, uid string) (*ibclient.Range, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateNetworkRange(
		strOrEmpty(comment),
		"",
		strOrEmpty(network),
		strOrEmpty(networkView),
		strOrEmpty(startAddr),
		strOrEmpty(endAddr),
		false,
		ea,
		nil,
		"",
		nil,
		false,
		"",
		strOrEmpty(template),
		"",
	)
}

// updateRange issues the WAPI update call. template is intentionally
// never passed — UpdateNetworkRange has no template parameter (immutable,
// create-only field). Every call re-asserts the identity stamp
// (identity.Stamp) in the extattrs it sends. Live verification against a
// real NIOS Grid Manager confirmed that a PUT carrying an extattrs object
// *replaces* the whole map — it is not a per-key merge — so omitting the
// stamp here would wipe it off the object on the very first field update
// after create.
func updateRange(objMgr ibclient.IBObjectManager, ref string, startAddr, endAddr, networkView, network, comment *string, extAttrs map[string]string, uid string) (*ibclient.Range, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.UpdateNetworkRange(
		ref,
		strOrEmpty(comment),
		"",
		strOrEmpty(network),
		strOrEmpty(startAddr),
		strOrEmpty(endAddr),
		false,
		ea,
		nil,
		"",
		nil,
		false,
		"",
		strOrEmpty(networkView),
		"",
	)
}

// deleteRange issues the WAPI delete call.
func deleteRange(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNetworkRange(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────

// ensureIdentityPrerequisite probes the Grid for the identity extensible
// attribute definition before any call that stamps identity onto a new
// object (identity.Stamp). A *identity.PrerequisiteError is returned
// verbatim — its Error() text is the operator-facing remediation, naming
// the exact WAPI call an administrator should run — so the caller's
// Synced=False condition carries it directly. Any other error (a
// transient failure probing or creating the definition) is wrapped like
// any other WAPI error and is retriable.
func ensureIdentityPrerequisite(ctx context.Context, prober *identity.Prober, conn ibclient.IBConnector, endpoint string) error {
	if prober == nil {
		prober = identity.DefaultProber
	}
	if endpoint == "" {
		endpoint = unresolvedProbeEndpoint
	}

	if err := prober.Ensure(ctx, conn, endpoint); err != nil {
		var prereq *identity.PrerequisiteError
		if errors.As(err, &prereq) {
			return err
		}
		return errors.Wrap(err, errPrerequisiteCheck)
	}
	return nil
}

// ── Identity resolution (shared by both scopes) ─────────────────────────

// observeRefFor derives the reference the identity ladder should attempt
// first for a managed resource's stored external-name. When the
// annotation still holds the framework's NameAsExternalName default (the
// CR's own Kubernetes name) no real WAPI _ref has ever been assigned, so
// this reports "" rather than handing the ladder a value that can never
// resolve.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveRangeIdentity resolves the Range identified by ref/uid through
// the shared UID-in-EA ladder.
func resolveRangeIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.Range, identity.Outcome, error) {
	return identity.Resolve[*ibclient.Range](ctx, conn, ibclient.NewEmptyRange, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting a
// Range through the identity ladder during Observe — common to both
// scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	rng          *ibclient.Range
	obs          observedRange
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeRange runs the identity ladder for Observe and late-initializes
// the given ForProvider field pointers from the resolved object.
func observeRange(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, networkView, network, comment **string, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rng, outcome, err := resolveRangeIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return observeResult{}, prereqErr
			}
		}
		return observeResult{}, err
	}
	if outcome == identity.OutcomeNotFound {
		return observeResult{exists: false}, nil
	}

	res := observeResult{
		exists:  true,
		rng:     rng,
		obs:     observeFromRange(rng.Ref, rng),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(networkView, network, comment, extAttrs, rng)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rng.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteRangeIdentity issues the WAPI delete for the Range this managed
// resource owns, resolving through the identity ladder first so a stale
// _ref is never mistaken for a deleted object.
func deleteRangeIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveRangeIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteRange)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteRange(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteRange)
	default:
		return errors.New("identity: unresolved Range outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced Range
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterRange(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster Range controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("Range"))

	o.Gate.Register(func() {
		if err := setupNamespacedRange(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced Range controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("Range"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced Range controllers
// immediately without SafeStart gating (RBAC fallback path, for
// environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterRange(mgr, o); err != nil {
		return err
	}
	return setupNamespacedRange(mgr, o)
}
