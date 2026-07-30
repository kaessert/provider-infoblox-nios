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
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage       = "cannot track ProviderConfig usage"
	errGetPC              = "cannot get ProviderConfig"
	errGetClusterPC       = "cannot get ClusterProviderConfig"
	errUnsupportedKind    = "unsupported provider config kind"
	errGetSecret          = "cannot get credentials secret"
	errNoSecretRef        = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds   = "unsupported credentials source: only Secret is supported"
	errMissingCredKey     = "credentials secret is missing one of the required host/username/password keys"
	errNewClient          = "cannot create Infoblox NIOS WAPI client"
	errObserveNetworkView = "cannot observe NetworkView"
	errCreateNetworkView  = "cannot create NetworkView"
	errUpdateNetworkView  = "cannot update NetworkView"
	errDeleteNetworkView  = "cannot delete NetworkView"
)

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

// newClient constructs an authenticated ibclient.IBObjectManager and the
// underlying Connector from the given credentials. The Connector performs
// HTTP Basic Auth on every request and only validates configuration
// locally — no network round-trip happens until the first
// Observe/Create/Update/Delete call. The raw Connector is returned
// alongside the ObjectManager so Observe can issue a custom GET that
// requests the is_default field (see getNetworkViewByRef) — the SDK's own
// GetNetworkViewByRef wrapper does not request it.
func newClient(creds *nioCredentials, sslVerify bool) (ibclient.IBObjectManager, *ibclient.Connector, error) {
	return newClientWithScheme(creds, sslVerify, "https", "443")
}

// newClientWithScheme is the scheme/port-parameterized variant of
// newClient used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newClientWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (ibclient.IBObjectManager, *ibclient.Connector, error) {
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
		return nil, nil, errors.Wrap(err, errNewClient)
	}

	return ibclient.NewObjectManager(conn, "", ""), conn, nil
}

// getNetworkViewByRef fetches a NetworkView by its WAPI _ref, extending
// the SDK's default field set (extattrs, name, comment) to also request
// is_default. objMgr.GetNetworkViewByRef does not request is_default,
// which would otherwise leave AtProvider.IsDefault permanently false
// regardless of the server's actual value — a full-mirror correctness
// issue since exactly one NetworkView per Grid always has is_default=true.
func getNetworkViewByRef(conn *ibclient.Connector, ref string) (*ibclient.NetworkView, error) {
	nv := ibclient.NewEmptyNetworkView()
	nv.SetReturnFields(append(nv.ReturnFields(), "is_default"))
	if err := conn.GetObject(nv, ref, ibclient.NewQueryParams(false, nil), nv); err != nil {
		return nil, err
	}
	return nv, nil
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
// comparison.
func isUpToDate(name, comment *string, extAttrs map[string]string, nv *ibclient.NetworkView) bool {
	if strOrEmpty(name) != strOrEmpty(nv.Name) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(nv.Comment) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(nv.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// extAttrs) from the observed NetworkView into spec so isUpToDate does not
// see phantom drift on the next reconcile. Name is never late-initialized
// — it is a required ForProvider field, always user-supplied. Returns
// true if any field was changed.
func lateInitialize(comment **string, extAttrs *map[string]string, nv *ibclient.NetworkView) bool {
	changed := false

	if *comment == nil && nv.Comment != nil && *nv.Comment != "" {
		c := *nv.Comment
		*comment = &c
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromNV := extAttrsFromEA(nv.Ea); len(fromNV) > 0 {
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
// WAPI NetworkView response fetched via getNetworkViewByRef (which
// explicitly requests is_default in addition to the SDK's default field
// set).
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

// createNetworkView issues the WAPI create call.
func createNetworkView(objMgr ibclient.IBObjectManager, name, comment *string, extAttrs map[string]string) (*ibclient.NetworkView, error) {
	return objMgr.CreateNetworkView(strOrEmpty(name), strOrEmpty(comment), buildEA(extAttrs))
}

// updateNetworkView issues the WAPI update call. is_default is never
// passed — UpdateNetworkView has no is_default parameter (immutable
// field); its internal GET-modify-PUT cycle only requests
// extattrs/name/comment, so is_default stays at its Go zero value (false)
// and is omitted from the outbound PUT body by the `omitempty` JSON tag.
func updateNetworkView(objMgr ibclient.IBObjectManager, ref string, name, comment *string, extAttrs map[string]string) (*ibclient.NetworkView, error) {
	return objMgr.UpdateNetworkView(ref, strOrEmpty(name), strOrEmpty(comment), buildEA(extAttrs))
}

// deleteNetworkView issues the WAPI delete call. Deleting the Grid's
// default NetworkView is rejected by the server (it cannot be deleted) —
// that surfaces as a normal terminal error from this call, not a special
// case this controller needs to detect proactively.
func deleteNetworkView(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNetworkView(ref)
	return err
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
