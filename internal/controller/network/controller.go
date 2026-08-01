// Package network implements the Crossplane controller for the Infoblox
// NIOS Network managed resource. Like recorda, this provider wraps the
// official infoblox-go-client Go SDK directly — the SDK's ObjectManager
// exposes typed CRUD methods (CreateNetwork/GetNetworkByRef/UpdateNetwork/
// DeleteNetwork) instead of a generic HTTP request/response envelope, so
// there is no internal REST client to compose.
//
// The underlying WAPI object type is runtime-selected: "network" for IPv4
// CIDRs, "ipv6network" for IPv6 CIDRs. The SDK's CreateNetwork takes an
// explicit isIPv6 bool; GetNetworkByRef/UpdateNetwork infer it from the
// "ipv6network/" ref prefix, so only Create needs the CIDR-format check
// performed by this package.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package network

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/network/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/network/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage     = "cannot track ProviderConfig usage"
	errGetPC            = "cannot get ProviderConfig"
	errGetClusterPC     = "cannot get ClusterProviderConfig"
	errUnsupportedKind  = "unsupported provider config kind"
	errGetSecret        = "cannot get credentials secret"
	errNoSecretRef      = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds = "unsupported credentials source: only Secret is supported"
	errMissingCredKey   = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager = "cannot create Infoblox NIOS WAPI object manager"
	errObserveNetwork   = "cannot observe Network"
	errCreateNetwork    = "cannot create Network"
	errUpdateNetwork    = "cannot update Network"
	errDeleteNetwork    = "cannot delete Network"
	errMissingCIDR      = "network CIDR is required to determine the WAPI object type (network vs ipv6network)"

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

// isIPv6CIDR reports whether cidr parses as an IPv6 network. Used only by
// Create — GetNetworkByRef and UpdateNetwork infer the WAPI object type
// from the "ipv6network/" _ref prefix instead. A CIDR that fails to parse
// falls back to the IPv4 object type; CEL/CRD validation on the required
// ForProvider.Network field is expected to reject malformed input before
// it ever reaches this helper.
func isIPv6CIDR(cidr string) bool {
	ip, _, err := net.ParseCIDR(cidr)
	if err != nil || ip == nil {
		return false
	}
	return ip.To4() == nil
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
// Network's immutable fields (networkView, network/cidr) are excluded from
// isUpToDate and are never part of updateNetwork's request — the
// UpdateNetwork SDK method has no parameter for either. Only comment and
// extAttrs are mutable.

// isUpToDate compares the desired mutable Network fields against the
// observed ibclient.Network. networkView and network (cidr) are
// intentionally excluded — both are immutable (absent from the
// UpdateNetwork SDK method signature). parentCidr, allocatePrefixLen,
// filterParams, and object are also excluded — they are create-time-only
// inputs to the allocation call, never echoed back by the WAPI response,
// so there is nothing to compare them against.
func isUpToDate(comment *string, extAttrs map[string]string, nw *ibclient.Network) bool {
	if strOrEmpty(comment) != nw.Comment {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(nw.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (network,
// comment, extAttrs) from the observed Network into spec so isUpToDate
// does not see phantom drift on the next reconcile. network is normally
// user-supplied (static CIDR path), but is left nil when the resource was
// created via the parentCidr or filterParams allocation paths — this
// back-fills it with the server-allocated CIDR so it becomes the stable
// identity for future Observe/Update cycles. networkView (required,
// immutable) is always user-supplied and never late-initialized. Returns
// true if any field was changed.
func lateInitialize(network, comment **string, extAttrs *map[string]string, nw *ibclient.Network) bool {
	changed := false

	if *network == nil && nw.Cidr != "" {
		c := nw.Cidr
		*network = &c
		changed = true
	}
	if *comment == nil && nw.Comment != "" {
		c := nw.Comment
		*comment = &c
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromNw := extAttrsFromEA(nw.Ea); len(fromNw) > 0 {
			*extAttrs = fromNw
			changed = true
		}
	}

	return changed
}

// observedNetworkMember is the scope-agnostic intermediate representation
// of a WAPI NetworkMember (dhcpmember or msdhcpserver union). Each scope's
// controller copies these fields into its own generated NetworkMember type
// at the call site — see observedNetwork below for why an intermediate
// type is used instead of a direct struct conversion.
type observedNetworkMember struct {
	DhcpMemberName       *string
	DhcpMemberIPv4Addr   *string
	DhcpMemberIPv6Addr   *string
	MsDhcpServerIPv4Addr *string
}

// convertMembers extracts the scope-agnostic field values from the SDK's
// NetworkMember union type.
func convertMembers(members []ibclient.NetworkMember) []observedNetworkMember {
	if len(members) == 0 {
		return nil
	}
	out := make([]observedNetworkMember, 0, len(members))
	for _, m := range members {
		om := observedNetworkMember{}
		switch {
		case m.DhcpMember != nil:
			if m.DhcpMember.Name != "" {
				n := m.DhcpMember.Name
				om.DhcpMemberName = &n
			}
			if m.DhcpMember.Ipv4Addr != "" {
				v := m.DhcpMember.Ipv4Addr
				om.DhcpMemberIPv4Addr = &v
			}
			if m.DhcpMember.Ipv6Addr != "" {
				v := m.DhcpMember.Ipv6Addr
				om.DhcpMemberIPv6Addr = &v
			}
		case m.MsDhcpServer != nil:
			if m.MsDhcpServer.Ipv4Addr != "" {
				v := m.MsDhcpServer.Ipv4Addr
				om.MsDhcpServerIPv4Addr = &v
			}
		}
		out = append(out, om)
	}
	return out
}

// observedNetwork holds the primitive field values extracted from a WAPI
// Network response. The cluster and namespaced NetworkObservation types
// are structurally similar but are distinct named types, so they are not
// directly convertible — each scope copies this intermediate struct's
// fields into its own Observation type at the call site.
type observedNetwork struct {
	ID          string
	NetworkView *string
	Network     *string
	Comment     *string
	ExtAttrs    map[string]string
	Ref         *string
	Members     []observedNetworkMember
}

// observeFromNetwork extracts the fields mirrored by NetworkObservation
// (the full-mirror AtProvider convention) from a WAPI Network response.
func observeFromNetwork(externalID string, nw *ibclient.Network) observedNetwork {
	o := observedNetwork{
		ID:       externalID,
		ExtAttrs: extAttrsFromEA(nw.Ea),
		Members:  convertMembers(nw.Members),
	}
	if nw.NetviewName != "" {
		v := nw.NetviewName
		o.NetworkView = &v
	}
	if nw.Cidr != "" {
		v := nw.Cidr
		o.Network = &v
	}
	if nw.Comment != "" {
		c := nw.Comment
		o.Comment = &c
	}
	if nw.Ref != "" {
		r := nw.Ref
		o.Ref = &r
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createNetwork issues the WAPI create call for the static-CIDR path. The
// WAPI object type ("network" vs "ipv6network") is selected at runtime
// from the CIDR's format.
func createNetwork(objMgr ibclient.IBObjectManager, networkView, cidr, comment *string, extAttrs map[string]string) (*ibclient.Network, error) {
	c := strOrEmpty(cidr)
	if c == "" {
		return nil, errors.New(errMissingCIDR)
	}
	return objMgr.CreateNetwork(strOrEmpty(networkView), c, isIPv6CIDR(c), strOrEmpty(comment), buildEA(extAttrs))
}

// createOrAllocateNetwork routes Network creation across the three
// supported paths, selected by which ForProvider fields are set:
//   - network set                       → createNetwork (static CIDR, existing path, unchanged)
//   - parentCidr + allocatePrefixLen    → AllocateNetwork (subnet carved from a parent CIDR)
//   - filterParams + allocatePrefixLen  → AllocateNetworkByEA (subnet carved from an EA-matched container)
//
// parentCidr and filterParams are mutually exclusive; either requires
// allocatePrefixLen. object only applies to the filterParams path (WAPI
// object type filter for the EA search, e.g. "networkcontainer") and is
// ignored otherwise.
//
// AllocateNetworkByEA has no parent CIDR to infer the address family
// from, so isIPv6 is always passed as false for that path — EA-based
// allocation of an IPv6 subnet would require an explicit ipVersion field
// on the CRD, which this catalog does not define.
func createOrAllocateNetwork(objMgr ibclient.IBObjectManager, networkView, network, parentCidr, comment, object *string, allocatePrefixLen *uint, filterParams, extAttrs map[string]string) (*ibclient.Network, error) {
	if strOrEmpty(parentCidr) != "" && len(filterParams) > 0 {
		return nil, errors.New(errParentCidrAndFilterParams)
	}

	switch {
	case strOrEmpty(network) != "":
		return createNetwork(objMgr, networkView, network, comment, extAttrs)

	case strOrEmpty(parentCidr) != "":
		if allocatePrefixLen == nil {
			return nil, errors.New(errAllocatePrefixLenRequired)
		}
		cidr := strOrEmpty(parentCidr)
		return objMgr.AllocateNetwork(strOrEmpty(networkView), cidr, isIPv6CIDR(cidr), *allocatePrefixLen, strOrEmpty(comment), buildEA(extAttrs))

	case len(filterParams) > 0:
		if allocatePrefixLen == nil {
			return nil, errors.New(errAllocatePrefixLenRequired)
		}
		return objMgr.AllocateNetworkByEA(strOrEmpty(networkView), false, strOrEmpty(comment), buildEA(extAttrs), filterParams, *allocatePrefixLen, strOrEmpty(object))

	default:
		// Unreachable in practice — the CRD's XValidation rule requires one
		// of network, parentCidr, or filterParams to be set.
		return nil, errors.New(errMissingAllocationInput)
	}
}

// updateNetwork issues the WAPI update call. Only comment and extattrs are
// sent — UpdateNetwork has no networkView/cidr parameters (both
// immutable).
func updateNetwork(objMgr ibclient.IBObjectManager, ref string, comment *string, extAttrs map[string]string) (*ibclient.Network, error) {
	return objMgr.UpdateNetwork(ref, buildEA(extAttrs), strOrEmpty(comment))
}

// deleteNetwork issues the WAPI delete call (hard delete — a subsequent
// GET on the same ref 404s).
func deleteNetwork(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNetwork(ref)
	return err
}

// networkExistsByNaturalKey reports whether a live Network still exists
// under the CR's own (networkView, cidr) identity — the same fields WAPI
// uses to compute the _ref. Used by Delete() when the stored _ref 404s: a
// hit here means the _ref is merely stale, not that the object is gone.
// GetNetwork errors out (rather than returning a *NotFoundError) when
// either networkView or cidr is empty, so the search is skipped
// (found=false) in that case rather than treated as an error. ea is
// always passed as nil — it is an optional additional filter; omitting
// it still filters correctly on networkView+cidr alone.
func networkExistsByNaturalKey(objMgr ibclient.IBObjectManager, networkView, cidr *string) (bool, error) {
	nv := strOrEmpty(networkView)
	c := strOrEmpty(cidr)
	if nv == "" || c == "" {
		return false, nil
	}
	_, err := objMgr.GetNetwork(nv, c, isIPv6CIDR(c), nil)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// deleteNetworkResolving404 issues the WAPI delete and, on a 404 against
// the stored _ref, resolves the object's natural key before concluding it
// is gone. A 404 on a derived handle is evidence the handle rotated, not
// evidence the object was removed: if the natural-key search still finds
// a live network, deleting is refused because ownership of that network
// cannot be verified from the search alone (see the staleref package doc
// for the full rationale).
func deleteNetworkResolving404(objMgr ibclient.IBObjectManager, ref string, networkView, cidr *string) error {
	delErr := deleteNetwork(objMgr, ref)
	if delErr == nil {
		return nil
	}
	if !isNotFound(delErr) {
		return errors.Wrap(delErr, errDeleteNetwork)
	}
	found, searchErr := networkExistsByNaturalKey(objMgr, networkView, cidr)
	if searchErr != nil {
		return errors.Wrap(searchErr, errDeleteNetwork)
	}
	if found {
		return staleref.RefusalError()
	}
	return nil
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced Network
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterNetwork(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster Network controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("Network"))

	o.Gate.Register(func() {
		if err := setupNamespacedNetwork(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced Network controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("Network"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced Network controllers
// immediately without SafeStart gating (RBAC fallback path, for
// environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterNetwork(mgr, o); err != nil {
		return err
	}
	return setupNamespacedNetwork(mgr, o)
}
