// Package network implements the Crossplane controller for the Infoblox
// NIOS Network managed resource. Like recorda, this provider wraps the
// official infoblox-go-client Go SDK directly — the SDK's ObjectManager
// exposes typed CRUD methods (CreateNetwork/GetNetworkByRef/UpdateNetwork/
// DeleteNetwork) instead of a generic HTTP request/response envelope, so
// there is no internal REST client to compose.
//
// WAPI models IPv4 and IPv6 networks as two distinct object types
// ("network" and "ipv6network") — a DUAL-OBJECT-TYPE resource. This
// provider exposes both through a single Network MR and selects the wire
// object type at runtime from the CIDR family of whichever spec field
// carries CIDR information (see networkFamily). The family is unknown
// only on the filterParams-only allocation path (no CIDR anywhere in
// spec) — that case is never silently assumed to be v4; see
// resolveNetworkIdentityUnknownFamily.
//
// networkView and network (the CIDR) are immutable identity fields — both
// are absent from UpdateNetwork's parameter list, and WAPI rejects
// attempts to move a network between network views or resize it in
// place.
//
// Network is wired to the UID-in-EA object-identity ladder (see the
// internal/clients/identity package doc): the WAPI _ref every create
// call returns is a derived handle, not a stable backend-assigned ID —
// so this controller stamps the managed resource's own metadata.uid onto
// the Grid object as an extensible attribute and resolves every
// Observe/Delete through the shared identity.Resolve ladder instead of
// trusting the stored _ref alone.
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
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
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

	errEmptyUID                  = "cannot stamp Network identity: managed resource's metadata.uid is empty"
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

// isIPv6CIDR reports whether cidr parses as an IPv6 network. WAPI models
// IPv4 and IPv6 networks as distinct object types ("network" vs
// "ipv6network"); CreateNetwork/AllocateNetwork require the caller to
// select the correct one up front, and the identity ladder's search step
// must target the same one (see networkFamily / newEmptyNetwork). A CIDR
// that fails to parse falls back to the IPv4 object type; CEL/CRD
// validation on the required ForProvider.Network field is expected to
// reject malformed input before it ever reaches this helper.
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
// so there is nothing to compare them against. The Grid's extattrs map is
// compared with the provider's own identity stamp stripped out
// (identity.Strip): the CRD schema never includes that reserved key, so
// leaving it in would produce a permanent phantom diff.
func isUpToDate(comment *string, extAttrs map[string]string, nw *ibclient.Network) bool {
	if strOrEmpty(comment) != nw.Comment {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(nw.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (network,
// comment, extAttrs) from the observed Network into spec so isUpToDate
// does not see phantom drift on the next reconcile. network is normally
// user-supplied (static CIDR path), but is left nil when the resource was
// created via the parentCidr or filterParams allocation paths — this
// back-fills it with the server-allocated CIDR so it becomes the stable
// identity for future Observe/Update cycles. networkView (required,
// immutable) is always user-supplied and never late-initialized. extAttrs
// is back-filled with the provider's own identity stamp stripped out
// (identity.Strip) — the CRD schema never includes that reserved key, so
// late-initializing it into spec.forProvider would fail CEL validation
// and produce a permanent diff. Returns true if any field was changed.
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
		if fromNw := extAttrsFromEA(identity.Strip(nw.Ea)); len(fromNw) > 0 {
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
// ExtAttrs intentionally mirrors the Grid's complete extattrs map,
// including the provider's own identity stamp — unlike isUpToDate and
// lateInitialize, AtProvider is a read-only status mirror, not compared
// against spec.forProvider, so surfacing the stamp there is informative
// rather than a source of phantom drift.
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
// from the CIDR's format. Stamps the owning managed resource's uid into
// the object's extensible attributes in the same request that creates it
// (identity.Stamp).
func createNetwork(objMgr ibclient.IBObjectManager, networkView, cidr, comment *string, extAttrs map[string]string, uid string) (*ibclient.Network, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	c := strOrEmpty(cidr)
	if c == "" {
		return nil, errors.New(errMissingCIDR)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateNetwork(strOrEmpty(networkView), c, isIPv6CIDR(c), strOrEmpty(comment), ea)
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
// on the CRD, which this catalog does not define. This is a CREATE-time
// decision only; it does NOT extend to the identity SEARCH step during
// Observe/Delete for objects created this way — see
// resolveNetworkIdentityUnknownFamily, which searches both object types
// rather than assuming this create-time convention still holds.
//
// Every leaf SDK call stamps the owning managed resource's uid into the
// object's extensible attributes in the same request that creates it
// (identity.Stamp).
func createOrAllocateNetwork(objMgr ibclient.IBObjectManager, networkView, network, parentCidr, comment, object *string, allocatePrefixLen *uint, filterParams, extAttrs map[string]string, uid string) (*ibclient.Network, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	if strOrEmpty(parentCidr) != "" && len(filterParams) > 0 {
		return nil, errors.New(errParentCidrAndFilterParams)
	}

	switch {
	case strOrEmpty(network) != "":
		return createNetwork(objMgr, networkView, network, comment, extAttrs, uid)

	case strOrEmpty(parentCidr) != "":
		if allocatePrefixLen == nil {
			return nil, errors.New(errAllocatePrefixLenRequired)
		}
		cidr := strOrEmpty(parentCidr)
		ea := identity.Stamp(buildEA(extAttrs), uid)
		return objMgr.AllocateNetwork(strOrEmpty(networkView), cidr, isIPv6CIDR(cidr), *allocatePrefixLen, strOrEmpty(comment), ea)

	case len(filterParams) > 0:
		if allocatePrefixLen == nil {
			return nil, errors.New(errAllocatePrefixLenRequired)
		}
		ea := identity.Stamp(buildEA(extAttrs), uid)
		return objMgr.AllocateNetworkByEA(strOrEmpty(networkView), false, strOrEmpty(comment), ea, filterParams, *allocatePrefixLen, strOrEmpty(object))

	default:
		// Unreachable in practice — the CRD's XValidation rule requires one
		// of network, parentCidr, or filterParams to be set.
		return nil, errors.New(errMissingAllocationInput)
	}
}

// updateNetwork issues the WAPI update call. Only comment and extattrs are
// sent — UpdateNetwork has no networkView/cidr parameters (both
// immutable). Every call re-asserts the identity stamp (identity.Stamp)
// in the extattrs it sends. Live verification against a real NIOS Grid
// Manager confirmed that a PUT carrying an extattrs object *replaces* the
// whole map — it is not a per-key merge — so omitting the stamp here
// would wipe it off the object on the very first field update after
// create. The updated object is not returned: networkView/cidr are
// immutable, so unlike other resources the response's _ref can never
// differ from the ref this call was issued with — there is nothing for a
// caller to inspect.
func updateNetwork(objMgr ibclient.IBObjectManager, ref string, comment *string, extAttrs map[string]string, uid string) error {
	if strings.TrimSpace(uid) == "" {
		return errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	_, err := objMgr.UpdateNetwork(ref, ea, strOrEmpty(comment))
	return err
}

// deleteNetwork issues the WAPI delete call (hard delete — a subsequent
// GET on the same ref 404s).
func deleteNetwork(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNetwork(ref)
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
// first for a managed resource's stored external-name. See recorda's doc
// for the full rationale.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// newEmptyNetwork builds the query/candidate object the identity ladder
// issues both the ref-fetch and the identity-EA search through,
// selecting the correct WAPI object type ("network" vs "ipv6network")
// for the given family. There is no NewEmptyNetwork constructor in the
// SDK — NewNetwork with empty netview/cidr/comment/nil ea is used purely
// to obtain a correctly-typed candidate object (it still sets objectType
// and ReturnFields as if constructing a real one). A bare
// &ibclient.Network{} would leave the unexported objectType field at its
// zero value (""), which is the dual-object-type hazard this wrapper
// exists to close — see the newEmpty correctness test.
func newEmptyNetwork(isIPv6 bool) func() *ibclient.Network {
	return func() *ibclient.Network {
		return ibclient.NewNetwork("", "", isIPv6, "", nil)
	}
}

// networkFamily derives the address family an identity search should
// target from whichever spec field carries CIDR information: network
// (static-CIDR path) or parentCidr (allocate-from-parent path) — both are
// always present in spec for their respective creation paths, so the
// family is derivable from either. The filterParams-only allocation path
// has no CIDR anywhere in spec: ok=false signals that case, and the
// caller must NOT guess — see resolveNetworkIdentityUnknownFamily.
func networkFamily(network, parentCidr *string) (isIPv6, ok bool) {
	if c := strOrEmpty(network); c != "" {
		return isIPv6CIDR(c), true
	}
	if c := strOrEmpty(parentCidr); c != "" {
		return isIPv6CIDR(c), true
	}
	return false, false
}

// resolveNetworkIdentity resolves the Network identified by ref/uid
// through the shared UID-in-EA ladder, selecting the correct WAPI object
// type for the identity-EA search from whichever spec field carries CIDR
// information (networkFamily). Family only matters for the search step:
// a fetch by ref addresses the object directly (BuildUrl ignores objType
// whenever ref is non-empty), so a resolving ref never touches the
// family logic at all — see the ref!="" branch below, which tries the
// ref with an arbitrary family and only consults networkFamily's
// "unknown" fallback if that ref-fetch itself falls through to a
// (necessarily family-scoped) search and finds nothing.
func resolveNetworkIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string, network, parentCidr *string) (*ibclient.Network, identity.Outcome, error) {
	if isIPv6, ok := networkFamily(network, parentCidr); ok {
		return identity.Resolve[*ibclient.Network](ctx, conn, newEmptyNetwork(isIPv6), ref, uid)
	}
	if ref != "" {
		obj, outcome, err := identity.Resolve[*ibclient.Network](ctx, conn, newEmptyNetwork(false), ref, uid)
		if err != nil || outcome != identity.OutcomeNotFound {
			return obj, outcome, err
		}
		// ref 404'd and family is unknown — fall through to the dual
		// search below rather than trusting the (arbitrarily v4-scoped)
		// NotFound this ref-fetch attempt produced.
	}
	return resolveNetworkIdentityUnknownFamily(ctx, conn, uid)
}

// resolveNetworkIdentityUnknownFamily handles the filterParams-only
// allocation case: no CIDR anywhere in spec to derive a family from.
// Both object types are searched and the results are unioned — a match
// under both types is refused as ambiguous rather than resolved by
// picking one, and a match under neither means the object genuinely does
// not exist. This function does NOT default to v4: doing so would
// silently miss an IPv6 object stamped with this uid, which is exactly
// the duplicate-creation hazard the identity ladder exists to close.
func resolveNetworkIdentityUnknownFamily(ctx context.Context, conn ibclient.IBConnector, uid string) (*ibclient.Network, identity.Outcome, error) {
	v4, outV4, errV4 := identity.Resolve[*ibclient.Network](ctx, conn, newEmptyNetwork(false), "", uid)
	if errV4 != nil {
		return nil, identity.OutcomeNotFound, errV4
	}
	v6, outV6, errV6 := identity.Resolve[*ibclient.Network](ctx, conn, newEmptyNetwork(true), "", uid)
	if errV6 != nil {
		return nil, identity.OutcomeNotFound, errV6
	}

	foundV4 := outV4 == identity.OutcomeFoundByUID
	foundV6 := outV6 == identity.OutcomeFoundByUID
	switch {
	case foundV4 && foundV6:
		return nil, identity.OutcomeNotFound, &identity.AmbiguousMatchError{ObjectType: "network/ipv6network", UID: uid, Count: 2}
	case foundV4:
		return v4, identity.OutcomeFoundByUID, nil
	case foundV6:
		return v6, identity.OutcomeFoundByUID, nil
	default:
		return nil, identity.OutcomeNotFound, nil
	}
}

// observeResult bundles the shared parts of resolving and inspecting a
// Network through the identity ladder during Observe — common to both
// scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	nw           *ibclient.Network
	obs          observedNetwork
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeNetwork runs the identity ladder for Observe and
// late-initializes the given ForProvider field pointers from the
// resolved object.
func observeNetwork(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, network, comment **string, parentCidr *string, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	nw, outcome, err := resolveNetworkIdentity(ctx, conn, ref, uid, *network, parentCidr)
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
		nw:      nw,
		obs:     observeFromNetwork(nw.Ref, nw),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(network, comment, extAttrs, nw)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = nw.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteNetworkIdentity issues the WAPI delete for the Network this
// managed resource owns, resolving through the identity ladder first so
// a stale _ref is never mistaken for a deleted object.
func deleteNetworkIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string, network, parentCidr *string) error {
	obj, outcome, err := resolveNetworkIdentity(ctx, conn, ref, uid, network, parentCidr)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteNetwork)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteNetwork(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteNetwork)
	default:
		return errors.New("identity: unresolved Network outcome")
	}
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
