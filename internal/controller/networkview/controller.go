// Package networkview implements the Crossplane controller for the
// Infoblox NIOS NetworkView managed resource. Like the ARecord controller,
// this provider wraps the official infoblox-go-client Go SDK directly —
// the SDK's ObjectManager exposes typed CRUD methods
// (CreateNetworkView/GetNetworkViewByRef/UpdateNetworkView/
// DeleteNetworkView) instead of a generic HTTP request/response envelope,
// so there is no internal REST client to compose.
//
// NetworkView follows a well-known-default pattern: the Grid always has
// exactly one NetworkView with is_default=true, and it cannot be deleted
// or un-defaulted. Standard CRUD applies for every other instance;
// is_default is cataloged as an immutable, response-only field.
//
// NetworkView is wired to the UID-in-EA object-identity ladder (see the
// internal/clients/identity package doc): the WAPI _ref every create call
// returns is a derived handle — a rendering of the object's own name, not
// a stable backend-assigned ID — so this controller stamps the managed
// resource's own metadata.uid onto the Grid object as an extensible
// attribute and resolves every Observe/Delete through the shared
// identity.Resolve ladder instead of trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package networkview

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

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkview/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkview/v1alpha1"
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
	errNewClient                 = "cannot create Infoblox NIOS WAPI client"
	errObserveNetworkView        = "cannot observe NetworkView"
	errCreateNetworkView         = "cannot create NetworkView"
	errUpdateNetworkView         = "cannot update NetworkView"
	errDeleteNetworkView         = "cannot delete NetworkView"
	errEmptyUID                  = "cannot stamp NetworkView identity: managed resource's metadata.uid is empty"
	errDeleteUnverifiedOwnership = "refusing to delete: the resolved object's identity extensible attribute is absent or belongs to a different owner, so ownership cannot be verified before an irreversible delete. " +
		"Reconcile the external-name annotation, verify the Grid object manually, or remove the finalizer to abandon it without deleting."
	errPrerequisiteCheck = "cannot verify the identity extensible attribute definition prerequisite"
)

// unresolvedProbeEndpoint is the identity-prerequisite-probe cache key
// used when an ExternalClient is built without a resolved Grid endpoint.
// Production code always goes through Connect(), which resolves the
// endpoint from the ProviderConfig's credentials Secret (validated
// non-empty by extractCredentials) before constructing the client — this
// fallback is only ever reached by this package's own white-box unit
// tests that build clusterExternal/namespacedExternal directly, bypassing
// Connect().
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
func extractCredentials(ctx context.Context, kube k8sclient.Client, source xpv1.CredentialsSource, secretRef *xpv1.SecretKeySelector, fallbackNamespace string) (*nioCredentials, error) {
	if source != xpv1.CredentialsSourceSecret {
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

// newClient constructs an authenticated identity.ManagerAndConnector from
// the given credentials — the SDK's high-level ObjectManager for the
// ordinary CRUD calls, and the lower-level Connector the identity ladder
// needs directly (it operates below ObjectManager's typed methods so it
// can see search match counts). The Connector performs HTTP Basic Auth on
// every request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newClient(creds *nioCredentials, sslVerify bool) (identity.ManagerAndConnector, error) {
	return newClientWithScheme(creds, sslVerify, "https", "443")
}

// newClientWithScheme is the scheme/port-parameterized variant of
// newClient used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newClientWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (identity.ManagerAndConnector, error) {
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
		return identity.ManagerAndConnector{}, errors.Wrap(err, errNewClient)
	}

	return identity.NewManagerAndConnector(conn), nil
}

// newEmptyNetworkView builds the query/candidate object the identity
// ladder issues both the ref-fetch and the identity-EA search through.
// ibclient.NewEmptyNetworkView's own default return-field set
// (extattrs, name, comment) omits is_default, which would otherwise
// leave AtProvider.IsDefault permanently false regardless of the
// server's actual value — a full-mirror correctness issue since exactly
// one NetworkView per Grid always has is_default=true. This wrapper
// extends the field set the same way the pre-identity getNetworkViewByRef
// helper did, so every identity.Resolve call site (ref-fetch and
// identity-EA search alike) requests it consistently.
func newEmptyNetworkView() *ibclient.NetworkView {
	nv := ibclient.NewEmptyNetworkView()
	nv.SetReturnFields(append(nv.ReturnFields(), "is_default"))
	return nv
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
// than a whole NetworkViewParameters value. The cluster and namespaced
// NetworkViewParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so parameterizing
// on the field pointers instead lets both scopes share this logic
// unconditionally.

// isUpToDate compares the desired NetworkView fields against the observed
// NetworkView. is_default is immutable (well-known-default: exactly one
// NetworkView per Grid always has is_default=true, and it can never be
// changed via UpdateNetworkView) and is intentionally excluded from this
// comparison. The Grid's extattrs map is compared with the provider's own
// identity stamp stripped out (identity.Strip): the CRD schema never
// includes that reserved key, so leaving it in would produce a permanent
// phantom diff.
func isUpToDate(name, comment *string, extAttrs map[string]string, nv *ibclient.NetworkView) bool {
	if strOrEmpty(name) != strOrEmpty(nv.Name) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(nv.Comment) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(nv.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// extAttrs) from the observed NetworkView into spec so isUpToDate does not
// see phantom drift on the next reconcile. Name is never late-initialized
// — it is a required ForProvider field, always user-supplied. extAttrs is
// back-filled with the provider's own identity stamp stripped out
// (identity.Strip) — the CRD schema never includes that reserved key, so
// late-initializing it into spec.forProvider would fail CEL validation
// and produce a permanent diff. Returns true if any field was changed.
func lateInitialize(comment **string, extAttrs *map[string]string, nv *ibclient.NetworkView) bool {
	changed := false

	if *comment == nil && nv.Comment != nil && *nv.Comment != "" {
		c := *nv.Comment
		*comment = &c
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromNV := extAttrsFromEA(identity.Strip(nv.Ea)); len(fromNV) > 0 {
			*extAttrs = fromNV
			changed = true
		}
	}

	return changed
}

// observedNetworkView holds the primitive field values extracted from a
// WAPI NetworkView response. The cluster and namespaced
// NetworkViewObservation types are structurally similar but are distinct
// named types, so each scope copies this intermediate struct's fields
// into its own Observation type at the call site.
type observedNetworkView struct {
	ID        string
	Name      *string
	Comment   *string
	ExtAttrs  map[string]string
	Ref       *string
	IsDefault *bool
}

// observeFromNetworkView extracts the fields mirrored by
// NetworkViewObservation (the full-mirror AtProvider convention) from a
// WAPI NetworkView response fetched via the identity ladder (which
// requests is_default in addition to the SDK's default field set — see
// newEmptyNetworkView). ExtAttrs intentionally mirrors the Grid's complete
// extattrs map, including the provider's own identity stamp — unlike
// isUpToDate and lateInitialize, AtProvider is a read-only status mirror,
// not compared against spec.forProvider, so surfacing the stamp there is
// informative rather than a source of phantom drift.
func observeFromNetworkView(externalID string, nv *ibclient.NetworkView) observedNetworkView {
	o := observedNetworkView{
		ID:       externalID,
		Name:     nv.Name,
		ExtAttrs: extAttrsFromEA(nv.Ea),
	}
	if nv.Comment != nil && *nv.Comment != "" {
		c := *nv.Comment
		o.Comment = &c
	}
	if nv.Ref != "" {
		r := nv.Ref
		o.Ref = &r
	}
	isDefault := nv.IsDefault
	o.IsDefault = &isDefault
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createNetworkView issues the WAPI create call, stamping the owning
// managed resource's uid into the object's extensible attributes in the
// same request that creates it (identity.Stamp) — there is no follow-up
// call, so there is no window in which the object exists without its
// identity stamp.
func createNetworkView(objMgr ibclient.IBObjectManager, name, comment *string, extAttrs map[string]string, uid string) (*ibclient.NetworkView, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateNetworkView(strOrEmpty(name), strOrEmpty(comment), ea)
}

// updateNetworkView issues the WAPI update call. is_default is never
// passed — UpdateNetworkView has no is_default parameter (immutable
// field); its internal GET-modify-PUT cycle only requests
// extattrs/name/comment, so is_default stays at its Go zero value (false)
// and is omitted from the outbound PUT body by the `omitempty` JSON tag.
//
// Every call re-asserts the identity stamp (identity.Stamp) in the
// extattrs it sends. Live verification against a real NIOS Grid Manager
// confirmed that a PUT carrying an extattrs object *replaces* the whole
// map — it is not a per-key merge — so omitting the stamp here would wipe
// it off the object on the very first field update after create.
func updateNetworkView(objMgr ibclient.IBObjectManager, ref string, name, comment *string, extAttrs map[string]string, uid string) (*ibclient.NetworkView, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.UpdateNetworkView(ref, strOrEmpty(name), strOrEmpty(comment), ea)
}

// deleteNetworkView issues the WAPI delete call. Deleting the Grid's
// default NetworkView is rejected by the server (it cannot be deleted) —
// that surfaces as a normal terminal error from this call, not a special
// case this controller needs to detect proactively.
func deleteNetworkView(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNetworkView(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────
//
// The "Crossplane Internal ID" extensible-attribute definition is an
// install prerequisite for every resource that stamps or resolves
// identity through it. Wired into Create() (before the identity stamp),
// and into observeNetworkView/deleteNetworkViewIdentity's identity-EA
// search fallback (see the doc on those functions for why the guard is
// reactive). Not wired into Connect(), which must stay network-lazy — see
// newClient's doc.

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
// resolve — Resolve treats "" as "search by identity attribute only",
// which is exactly the create-crash-window recovery path.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveNetworkViewIdentity resolves the NetworkView identified by
// ref/uid through the shared UID-in-EA ladder: the stored reference is
// trusted only after its stamped identity attribute is confirmed to
// match uid; a stale or absent reference falls back to a search by that
// stamp, which is also what locates the object when ref is empty.
func resolveNetworkViewIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.NetworkView, identity.Outcome, error) {
	return identity.Resolve[*ibclient.NetworkView](ctx, conn, newEmptyNetworkView, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting a
// NetworkView through the identity ladder during Observe — common to
// both scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	nv           *ibclient.NetworkView
	obs          observedNetworkView
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeNetworkView runs the identity ladder for Observe and
// late-initializes the given ForProvider field pointers from the
// resolved object.
func observeNetworkView(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, comment **string, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	nv, outcome, err := resolveNetworkViewIdentity(ctx, conn, ref, uid)
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
		nv:      nv,
		obs:     observeFromNetworkView(nv.Ref, nv),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(comment, extAttrs, nv)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = nv.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteNetworkViewIdentity issues the WAPI delete for the NetworkView
// this managed resource owns, resolving through the identity ladder
// first so a stale _ref is never mistaken for a deleted object.
func deleteNetworkViewIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveNetworkViewIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteNetworkView)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteNetworkView(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteNetworkView)
	default:
		return errors.New("identity: unresolved NetworkView outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced NetworkView
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterNetworkView(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster NetworkView controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("NetworkView"))

	o.Gate.Register(func() {
		if err := setupNamespacedNetworkView(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced NetworkView controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("NetworkView"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced NetworkView
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterNetworkView(mgr, o); err != nil {
		return err
	}
	return setupNamespacedNetworkView(mgr, o)
}
