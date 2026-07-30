// Package networkcontainer implements the Crossplane controller for the
// Infoblox NIOS NetworkContainer managed resource. Like the ARecord and
// ZoneDelegated controllers, this provider wraps the official
// infoblox-go-client Go SDK directly — the SDK's ObjectManager exposes
// typed CRUD methods (CreateNetworkContainer/GetNetworkContainerByRef/
// UpdateNetworkContainer/DeleteNetworkContainer) instead of a generic HTTP
// request/response envelope, so there is no internal REST client to
// compose.
//
// WAPI represents IPv4 and IPv6 network containers as two distinct object
// types (`networkcontainer` and `ipv6networkcontainer`); this provider
// exposes both through a single NetworkContainer MR and selects the wire
// object type at runtime from the CIDR family of spec.forProvider.network
// (see isIPv6CIDR). Once created, the resource's `_ref` already encodes
// the object type, so Observe/Update/Delete (all ref-scoped calls) never
// need to re-derive it.
//
// networkView and network (the CIDR) are immutable identity fields —
// both are absent from UpdateNetworkContainer's parameter list, and WAPI
// rejects attempts to move a container between network views or resize
// it in place.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
//
// This provider has no shared internal/clients package — each resource
// controller package defines its own credential bridge (mirrors the
// ARecord/recorda controller package).
package networkcontainer

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/networkcontainer/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/networkcontainer/v1alpha1"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage            = "cannot track ProviderConfig usage"
	errGetPC                   = "cannot get ProviderConfig"
	errGetClusterPC            = "cannot get ClusterProviderConfig"
	errUnsupportedKind         = "unsupported provider config kind"
	errGetSecret               = "cannot get credentials secret"
	errNoSecretRef             = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds        = "unsupported credentials source: only Secret is supported"
	errMissingCredKey          = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager        = "cannot create Infoblox NIOS WAPI object manager"
	errObserveNetworkContainer = "cannot observe NetworkContainer"
	errCreateNetworkContainer  = "cannot create NetworkContainer"
	errUpdateNetworkContainer  = "cannot update NetworkContainer"
	errDeleteNetworkContainer  = "cannot delete NetworkContainer"

	errParentCidrAndFilterParams = "parentCidr and filterParams are mutually exclusive"
	errAllocatePrefixLenRequired = "allocatePrefixLen is required when parentCidr or filterParams is set"
	errMissingAllocationInput    = "one of network, parentCidr, or filterParams is required"
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

// newObjectManager constructs an authenticated ibclient.IBObjectManager
// from the given credentials. The Connector performs HTTP Basic Auth on
// every request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newObjectManager(creds *nioCredentials, sslVerify bool) (ibclient.IBObjectManager, error) {
	return newObjectManagerWithScheme(creds, sslVerify, "https", "443")
}

// newObjectManagerWithScheme is the scheme/port-parameterized variant of
// newObjectManager used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newObjectManagerWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (ibclient.IBObjectManager, error) {
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
		return nil, errors.Wrap(err, errNewObjectManager)
	}

	return ibclient.NewObjectManager(conn, "", ""), nil
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// isIPv6CIDR reports whether cidr describes an IPv6 network. WAPI models
// IPv4 and IPv6 network containers as distinct object types
// (networkcontainer vs ipv6networkcontainer); CreateNetworkContainer
// requires the caller to select the correct one up front. Malformed CIDRs
// are not rejected here — WAPI is the authoritative validator of CIDR
// syntax; the colon heuristic is only a fallback to still pick a
// plausible object type so the create request round-trips to the server
// (which returns the real validation error) instead of panicking locally.
func isIPv6CIDR(cidr string) bool {
	if ip, _, err := net.ParseCIDR(cidr); err == nil {
		return ip.To4() == nil
	}
	return strings.Contains(cidr, ":")
}

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
// These helpers take pointers to the individual mutable ForProvider fields
// rather than a whole NetworkContainerParameters value. The cluster and
// namespaced NetworkContainerParameters types are structurally identical
// (same field names and primitive types) but are distinct named Go types,
// so parameterizing on the field pointers instead lets both scopes share
// this logic unconditionally.
//
// networkView and network (cidr) are immutable identity fields — both are
// absent from UpdateNetworkContainer's parameter list (see
// updateNetworkContainer) — and are intentionally excluded from
// isUpToDate. They are also never late-initialized: both are required
// ForProvider fields, always user-supplied.

// isUpToDate compares the desired mutable NetworkContainer fields
// (comment, extAttrs) against the observed NetworkContainer. parentCidr,
// allocatePrefixLen, and filterParams are also excluded — they are
// create-time-only inputs to the allocation call, never echoed back by
// the WAPI response, so there is nothing to compare them against.
func isUpToDate(comment *string, extAttrs map[string]string, nc *ibclient.NetworkContainer) bool {
	if strOrEmpty(comment) != nc.Comment {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(nc.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (network,
// comment, extAttrs) from the observed NetworkContainer into spec so
// isUpToDate does not see phantom drift on the next reconcile. network is
// normally user-supplied (static CIDR path), but is left nil when the
// resource was created via the parentCidr or filterParams allocation
// paths — this back-fills it with the server-allocated CIDR so it
// becomes the stable identity for future Observe/Update cycles.
// networkView (required, immutable) is always user-supplied and never
// late-initialized. Returns true if any field was changed.
func lateInitialize(network, comment **string, extAttrs *map[string]string, nc *ibclient.NetworkContainer) bool {
	changed := false

	if *network == nil && nc.Cidr != "" {
		c := nc.Cidr
		*network = &c
		changed = true
	}
	if *comment == nil && nc.Comment != "" {
		c := nc.Comment
		*comment = &c
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromNC := extAttrsFromEA(nc.Ea); len(fromNC) > 0 {
			*extAttrs = fromNC
			changed = true
		}
	}

	return changed
}

// observedNetworkContainer holds the primitive field values extracted
// from a WAPI NetworkContainer response. The cluster and namespaced
// NetworkContainerObservation types are structurally similar but are
// distinct named types, so each scope copies this intermediate struct's
// fields into its own Observation type at the call site.
type observedNetworkContainer struct {
	ID          string
	NetworkView *string
	Network     *string
	Comment     *string
	ExtAttrs    map[string]string
	Ref         *string
}

// observeFromNetworkContainer extracts the fields mirrored by
// NetworkContainerObservation (the full-mirror AtProvider convention)
// from a WAPI NetworkContainer response fetched via
// GetNetworkContainerByRef (which requests extattrs, network,
// network_view, and comment by default — see ibclient.NewNetworkContainer).
func observeFromNetworkContainer(externalID string, nc *ibclient.NetworkContainer) observedNetworkContainer {
	o := observedNetworkContainer{
		ID:       externalID,
		ExtAttrs: extAttrsFromEA(nc.Ea),
	}
	if nc.NetviewName != "" {
		v := nc.NetviewName
		o.NetworkView = &v
	}
	if nc.Cidr != "" {
		c := nc.Cidr
		o.Network = &c
	}
	if nc.Comment != "" {
		c := nc.Comment
		o.Comment = &c
	}
	if nc.Ref != "" {
		r := nc.Ref
		o.Ref = &r
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createNetworkContainer issues the WAPI create call for the static-CIDR
// path, selecting the networkcontainer/ipv6networkcontainer object type
// from the CIDR family of network.
func createNetworkContainer(objMgr ibclient.IBObjectManager, networkView, network, comment *string, extAttrs map[string]string) (*ibclient.NetworkContainer, error) {
	cidr := strOrEmpty(network)
	return objMgr.CreateNetworkContainer(strOrEmpty(networkView), cidr, isIPv6CIDR(cidr), strOrEmpty(comment), buildEA(extAttrs))
}

// createOrAllocateNetworkContainer routes NetworkContainer creation across
// the three supported paths, selected by which ForProvider fields are
// set:
//   - network set                       → createNetworkContainer (static CIDR, existing path, unchanged)
//   - parentCidr + allocatePrefixLen    → AllocateNetworkContainer (subnet container carved from a parent CIDR)
//   - filterParams + allocatePrefixLen  → AllocateNetworkContainerByEA (subnet container carved from an EA-matched parent container)
//
// parentCidr and filterParams are mutually exclusive; either requires
// allocatePrefixLen. Unlike Network's AllocateNetworkByEA, there is no
// WAPI object-type filter parameter for the container variant.
//
// AllocateNetworkContainerByEA has no parent CIDR to infer the address
// family from, so isIPv6 is always passed as false for that path —
// EA-based allocation of an IPv6 container would require an explicit
// ipVersion field on the CRD, which this catalog does not define.
func createOrAllocateNetworkContainer(objMgr ibclient.IBObjectManager, networkView, network, parentCidr, comment *string, allocatePrefixLen *uint, filterParams, extAttrs map[string]string) (*ibclient.NetworkContainer, error) {
	if strOrEmpty(parentCidr) != "" && len(filterParams) > 0 {
		return nil, errors.New(errParentCidrAndFilterParams)
	}

	switch {
	case strOrEmpty(network) != "":
		return createNetworkContainer(objMgr, networkView, network, comment, extAttrs)

	case strOrEmpty(parentCidr) != "":
		if allocatePrefixLen == nil {
			return nil, errors.New(errAllocatePrefixLenRequired)
		}
		cidr := strOrEmpty(parentCidr)
		return objMgr.AllocateNetworkContainer(strOrEmpty(networkView), cidr, isIPv6CIDR(cidr), *allocatePrefixLen, strOrEmpty(comment), buildEA(extAttrs))

	case len(filterParams) > 0:
		if allocatePrefixLen == nil {
			return nil, errors.New(errAllocatePrefixLenRequired)
		}
		return objMgr.AllocateNetworkContainerByEA(strOrEmpty(networkView), false, strOrEmpty(comment), buildEA(extAttrs), filterParams, *allocatePrefixLen)

	default:
		// Unreachable in practice — the CRD's XValidation rule requires one
		// of network, parentCidr, or filterParams to be set.
		return nil, errors.New(errMissingAllocationInput)
	}
}

// updateNetworkContainer issues the WAPI update call. networkView and
// network (immutable identity fields) are never passed —
// UpdateNetworkContainer has no parameters for them; its internal
// GET-modify-PUT cycle only requests extattrs/comment, and explicitly
// clears NetviewName before sending so it never leaks into the PUT body.
func updateNetworkContainer(objMgr ibclient.IBObjectManager, ref string, comment *string, extAttrs map[string]string) (*ibclient.NetworkContainer, error) {
	return objMgr.UpdateNetworkContainer(ref, buildEA(extAttrs), strOrEmpty(comment))
}

// getNetworkContainerByRef issues the WAPI get-by-ref call.
func getNetworkContainerByRef(objMgr ibclient.IBObjectManager, ref string) (*ibclient.NetworkContainer, error) {
	return objMgr.GetNetworkContainerByRef(ref)
}

// deleteNetworkContainer issues the WAPI delete call.
func deleteNetworkContainer(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNetworkContainer(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced
// NetworkContainer controllers with the SafeStart gate. Each controller
// starts only after its respective CRD has been installed in the
// cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterNetworkContainer(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster NetworkContainer controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("NetworkContainer"))

	o.Gate.Register(func() {
		if err := setupNamespacedNetworkContainer(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced NetworkContainer controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("NetworkContainer"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced NetworkContainer
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterNetworkContainer(mgr, o); err != nil {
		return err
	}
	return setupNamespacedNetworkContainer(mgr, o)
}
