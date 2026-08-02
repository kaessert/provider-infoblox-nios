// Package fixedaddress implements the Crossplane controller for the
// Infoblox NIOS FixedAddress managed resource. Like the other controllers
// in this provider, it wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods instead of
// a generic HTTP request/response envelope, so there is no internal REST
// client to compose.
//
// FixedAddress is a NON-STANDARD CRUD resource: creation goes through
// AllocateIP(...), not a Create<Resource>-named method, and the WAPI
// object type is runtime-selected between "fixedaddress" (IPv4) and
// "ipv6fixedaddress" (IPv6) based on which of ipv4addr/ipv6addr is set.
//
// Unlike Network/NetworkContainer, the address family here is ALWAYS
// derivable directly from spec: the CRD's CEL rule guarantees exactly one
// of ipv4addr/ipv6addr is set for the lifetime of the object, so there is
// no "unknown family" identity-search case to handle — see
// fixedAddressFields.isIPv6.
//
// FixedAddress is wired to the UID-in-EA object-identity ladder (see the
// internal/clients/identity package doc): the WAPI _ref every create
// call returns is a derived handle, not a stable backend-assigned ID —
// so this controller stamps the managed resource's own metadata.uid onto
// the Grid object as an extensible attribute and resolves every
// Observe/Delete through the shared identity.Resolve ladder instead of
// trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package fixedaddress

import (
	"context"
	"fmt"
	"math"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/fixedaddress/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/fixedaddress/v1alpha1"
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
	errObserveFixedAddress       = "cannot observe FixedAddress"
	errCreateFixedAddress        = "cannot create FixedAddress"
	errUpdateFixedAddress        = "cannot update FixedAddress"
	errDeleteFixedAddress        = "cannot delete FixedAddress"
	errEmptyUID                  = "cannot stamp FixedAddress identity: managed resource's metadata.uid is empty"
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

// matchClientDefault is the value WAPI applies to an IPv4 fixed address
// when no match_client is supplied at create time. UpdateFixedAddress
// rejects an empty match_client for IPv4 objects (the SDK validates it
// against the WAPI enum before issuing the PUT), so Update() must always
// send a concrete value — this is the fallback used when neither the
// desired spec nor the last-observed state has one yet (e.g. the first
// Update immediately following Create, before late-init has back-filled
// spec.forProvider.matchClient from a prior Observe).
const matchClientDefault = "MAC_ADDRESS"

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

// ── Scope-agnostic field model ──────────────────────────────────────────
//
// fixedAddressFields and dhcpOption decouple the shared isUpToDate/
// lateInitialize/create/update/observe logic below from the cluster and
// namespaced FixedAddressParameters/FixedAddressDhcpOption types, which
// are structurally identical (same field names and primitive types) but
// are distinct named Go types. Each scope (cluster.go, namespaced.go)
// converts to/from this shared shape at the call site.

// dhcpOption mirrors one entry of FixedAddressParameters.Options,
// independent of scope.
type dhcpOption struct {
	Name        *string
	Num         *int64
	VendorClass *string
	Value       *string
	UseOption   *bool
}

// fixedAddressFields mirrors every FixedAddressParameters field,
// independent of scope.
type fixedAddressFields struct {
	IPv4Addr                    *string
	IPv6Addr                    *string
	MAC                         *string
	NetworkView                 *string
	Network                     *string
	Name                        *string
	MatchClient                 *string
	Comment                     *string
	ExtAttrs                    map[string]string
	Disable                     *bool
	AgentCircuitID              *string
	AgentRemoteID               *string
	ClientIdentifierPrependZero *bool
	DHCPClientIdentifier        *string
	Options                     []dhcpOption
	UseOptions                  *bool
}

// isIPv6 reports whether the desired fixed address is an IPv6 record
// (ipv6addr set) as opposed to an IPv4 record (ipv4addr set). The CEL rule
// on FixedAddressParameters guarantees exactly one of the two is set.
func (f fixedAddressFields) isIPv6() bool {
	return f.IPv6Addr != nil
}

// sharedCloudInfoDelegatedMember mirrors FixedAddressCloudInfoDelegatedMember,
// independent of scope.
type sharedCloudInfoDelegatedMember struct {
	IPv4Addr *string
	IPv6Addr *string
	Name     *string
}

// sharedCloudInfo mirrors FixedAddressCloudInfo, independent of scope.
type sharedCloudInfo struct {
	DelegatedMember *sharedCloudInfoDelegatedMember
	DelegatedScope  *string
	DelegatedRoot   *string
	OwnedByAdaptor  *bool
	Usage           *string
	Tenant          *string
	MgmtPlatform    *string
	AuthorityType   *string
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

func boolOrFalse(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func int64OrZero(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

// uint32FromInt64 converts an optional *int64 DHCP option code (validated
// 0-65535 by CRD/CEL rules) into the uint32 the SDK's Dhcpoption.Num field
// expects. Values outside the valid uint32 range (or negative) are
// clamped to 0 rather than silently wrapping.
func uint32FromInt64(v *int64) uint32 {
	if v == nil || *v < 0 || *v > math.MaxUint32 {
		return 0
	}
	return uint32(*v)
}

// toSDKOptions converts the CRD's DHCP option list into the SDK's
// []*ibclient.Dhcpoption representation.
func toSDKOptions(opts []dhcpOption) []*ibclient.Dhcpoption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*ibclient.Dhcpoption, 0, len(opts))
	for _, o := range opts {
		out = append(out, &ibclient.Dhcpoption{
			Name:        strOrEmpty(o.Name),
			Num:         uint32FromInt64(o.Num),
			VendorClass: strOrEmpty(o.VendorClass),
			Value:       strOrEmpty(o.Value),
			UseOption:   boolOrFalse(o.UseOption),
		})
	}
	return out
}

// dhcpOptionsFromSDK converts the SDK's []*ibclient.Dhcpoption response
// into the shared dhcpOption representation.
func dhcpOptionsFromSDK(opts []*ibclient.Dhcpoption) []dhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]dhcpOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		name := o.Name
		num := int64(o.Num)
		vendorClass := o.VendorClass
		value := o.Value
		useOption := o.UseOption
		out = append(out, dhcpOption{
			Name:        &name,
			Num:         &num,
			VendorClass: &vendorClass,
			Value:       &value,
			UseOption:   &useOption,
		})
	}
	return out
}

// dhcpOptionsEqual reports whether two DHCP option lists are equivalent
// (order-sensitive — WAPI preserves list order).
func dhcpOptionsEqual(a, b []dhcpOption) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Name) != strOrEmpty(b[i].Name) {
			return false
		}
		if int64OrZero(a[i].Num) != int64OrZero(b[i].Num) {
			return false
		}
		if strOrEmpty(a[i].VendorClass) != strOrEmpty(b[i].VendorClass) {
			return false
		}
		if strOrEmpty(a[i].Value) != strOrEmpty(b[i].Value) {
			return false
		}
		if boolOrFalse(a[i].UseOption) != boolOrFalse(b[i].UseOption) {
			return false
		}
	}
	return true
}

// observedCloudInfo extracts the shared cloud-info shape from a WAPI
// GridCloudapiInfo response, returning nil when the server did not
// include cloud API delegation info (common — cloud_info is only
// populated for cloud-managed objects, and is not even requested by the
// SDK's default IPv6 return-fields set).
func observedCloudInfo(ci *ibclient.GridCloudapiInfo) *sharedCloudInfo {
	if ci == nil {
		return nil
	}
	out := &sharedCloudInfo{}
	owned := ci.OwnedByAdaptor
	out.OwnedByAdaptor = &owned
	if ci.DelegatedScope != "" {
		v := ci.DelegatedScope
		out.DelegatedScope = &v
	}
	if ci.DelegatedRoot != "" {
		v := ci.DelegatedRoot
		out.DelegatedRoot = &v
	}
	if ci.Usage != "" {
		v := ci.Usage
		out.Usage = &v
	}
	if ci.Tenant != "" {
		v := ci.Tenant
		out.Tenant = &v
	}
	if ci.MgmtPlatform != "" {
		v := ci.MgmtPlatform
		out.MgmtPlatform = &v
	}
	if ci.AuthorityType != "" {
		v := ci.AuthorityType
		out.AuthorityType = &v
	}
	if ci.DelegatedMember != nil {
		dm := &sharedCloudInfoDelegatedMember{}
		if ci.DelegatedMember.Ipv4Addr != "" {
			v := ci.DelegatedMember.Ipv4Addr
			dm.IPv4Addr = &v
		}
		if ci.DelegatedMember.Ipv6Addr != "" {
			v := ci.DelegatedMember.Ipv6Addr
			dm.IPv6Addr = &v
		}
		if ci.DelegatedMember.Name != "" {
			v := ci.DelegatedMember.Name
			dm.Name = &v
		}
		out.DelegatedMember = dm
	}
	return out
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

// isUpToDate compares the desired FixedAddress fields against the observed
// FixedAddress. No fields are known to be immutable for this resource
// (network_view/network appear in both AllocateIP and UpdateFixedAddress;
// the IPv4/IPv6 address family is fixed at creation but that constraint is
// enforced by the CEL rule on the CRD, not by excluding a field here) —
// every field is compared. Split into two helpers (address/identity vs.
// DHCP-relay fields) to keep cyclomatic complexity manageable.
func isUpToDate(f fixedAddressFields, fa *ibclient.FixedAddress) bool {
	return isUpToDateAddress(f, fa) && isUpToDateDHCP(f, fa)
}

// isUpToDateAddress compares the address-identity fields.
//
// The mac/DUID comparison is family-aware: ibclient.NewEmptyFixedAddress
// requests a different returnFields list per WAPI object type, and for
// "ipv6fixedaddress" that list carries "duid", never "mac" — every GET for
// an IPv6 fixed address comes back with fa.Mac == nil regardless of what
// was sent. spec.forProvider.mac doubles as the DUID input for IPv6 (WAPI
// requires it non-empty on create), so it must be compared against
// fa.Duid — the field WAPI actually populates for that family — not
// fa.Mac, which would never match and would trigger an Update on every
// reconcile.
func isUpToDateAddress(f fixedAddressFields, fa *ibclient.FixedAddress) bool {
	isIPv6 := f.isIPv6()
	if isIPv6 {
		if strOrEmpty(f.IPv6Addr) != fa.IPv6Address {
			return false
		}
		if strOrEmpty(f.MAC) != fa.Duid {
			return false
		}
	} else {
		if strOrEmpty(f.IPv4Addr) != fa.IPv4Address {
			return false
		}
		if strOrEmpty(f.MAC) != strOrEmpty(fa.Mac) {
			return false
		}
	}
	if strOrEmpty(f.NetworkView) != fa.NetviewName {
		return false
	}
	if strOrEmpty(f.Network) != fa.Cidr {
		return false
	}
	if strOrEmpty(f.Name) != strOrEmpty(fa.Name) {
		return false
	}
	if strOrEmpty(f.MatchClient) != strOrEmpty(fa.MatchClient) {
		return false
	}
	if strOrEmpty(f.Comment) != fa.Comment {
		return false
	}
	return extAttrsEqual(f.ExtAttrs, extAttrsFromEA(identity.Strip(fa.Ea)))
}

// isUpToDateDHCP compares the DHCP-relay and options fields.
func isUpToDateDHCP(f fixedAddressFields, fa *ibclient.FixedAddress) bool {
	if boolOrFalse(f.Disable) != boolOrFalse(fa.Disable) {
		return false
	}
	if strOrEmpty(f.AgentCircuitID) != strOrEmpty(fa.AgentCircuitId) {
		return false
	}
	if strOrEmpty(f.AgentRemoteID) != strOrEmpty(fa.AgentRemoteId) {
		return false
	}
	if boolOrFalse(f.ClientIdentifierPrependZero) != boolOrFalse(fa.ClientIdentifierPrependZero) {
		return false
	}
	if strOrEmpty(f.DHCPClientIdentifier) != strOrEmpty(fa.DhcpClientIdentifier) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(f.UseOptions) != boolOrFalse(fa.UseOptions) {
		return false
	}
	// Only compare options when the flag is on. When it is off, WAPI
	// ignores the submitted DHCP options and returns its own default set
	// on every GET — comparing them against the spec value never
	// converges.
	if boolOrFalse(f.UseOptions) {
		return dhcpOptionsEqual(f.Options, dhcpOptionsFromSDK(fa.Options))
	}
	return true
}

// lateInitialize back-fills server-defaulted optional fields from the
// observed FixedAddress into f so isUpToDate does not see phantom drift on
// the next reconcile. Mutates f in place and returns true if any field was
// changed. ipv4addr/ipv6addr are only back-filled when the corresponding
// field is set to an empty string (rather than nil) in spec — that is the
// dynamic-allocation convention (spec.forProvider.network supplies a CIDR
// and the literal address is resolved by AllocateIP at create time), so
// the resolved literal address is captured here to avoid re-triggering
// dynamic allocation on every subsequent Update. Split into three helpers
// (address, identity, DHCP-relay fields) to keep cyclomatic complexity
// manageable.
func lateInitialize(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	addrChanged := lateInitializeAddress(f, fa)
	idChanged := lateInitializeIdentity(f, fa)
	dhcpChanged := lateInitializeDHCP(f, fa)
	return addrChanged || idChanged || dhcpChanged
}

// lateInitializeAddress back-fills the dynamic-allocation literal address
// (see lateInitialize doc comment).
func lateInitializeAddress(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	changed := false
	isIPv6 := f.isIPv6()

	if !isIPv6 && f.IPv4Addr != nil && *f.IPv4Addr == "" && fa.IPv4Address != "" {
		v := fa.IPv4Address
		f.IPv4Addr = &v
		changed = true
	}
	if isIPv6 && f.IPv6Addr != nil && *f.IPv6Addr == "" && fa.IPv6Address != "" {
		v := fa.IPv6Address
		f.IPv6Addr = &v
		changed = true
	}
	return changed
}

// lateInitializeIdentity back-fills MAC/network/name/match-client/comment/
// extattrs fields. Split into two halves to keep cyclomatic complexity
// manageable.
func lateInitializeIdentity(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	a := lateInitializeIdentityLocation(f, fa)
	b := lateInitializeIdentityMeta(f, fa)
	return a || b
}

// lateInitializeIdentityLocation back-fills MAC/network-view/network/name.
func lateInitializeIdentityLocation(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	changed := false

	if f.MAC == nil && fa.Mac != nil && *fa.Mac != "" {
		v := *fa.Mac
		f.MAC = &v
		changed = true
	}
	if f.NetworkView == nil && fa.NetviewName != "" {
		v := fa.NetviewName
		f.NetworkView = &v
		changed = true
	}
	if f.Network == nil && fa.Cidr != "" {
		v := fa.Cidr
		f.Network = &v
		changed = true
	}
	if f.Name == nil && fa.Name != nil && *fa.Name != "" {
		v := *fa.Name
		f.Name = &v
		changed = true
	}
	return changed
}

// lateInitializeIdentityMeta back-fills match-client/comment/extattrs.
func lateInitializeIdentityMeta(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	changed := false

	if f.MatchClient == nil && fa.MatchClient != nil && *fa.MatchClient != "" {
		v := *fa.MatchClient
		f.MatchClient = &v
		changed = true
	}
	if f.Comment == nil && fa.Comment != "" {
		v := fa.Comment
		f.Comment = &v
		changed = true
	}
	if len(f.ExtAttrs) == 0 {
		if fromFA := extAttrsFromEA(identity.Strip(fa.Ea)); len(fromFA) > 0 {
			f.ExtAttrs = fromFA
			changed = true
		}
	}
	return changed
}

// lateInitializeDHCP back-fills DHCP-relay and options fields. Split into
// two halves to keep cyclomatic complexity manageable.
func lateInitializeDHCP(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	a := lateInitializeDHCPAgent(f, fa)
	b := lateInitializeDHCPOptions(f, fa)
	return a || b
}

// lateInitializeDHCPAgent back-fills disable/agent-circuit-id/agent-remote-id.
func lateInitializeDHCPAgent(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	changed := false

	if f.Disable == nil && fa.Disable != nil {
		v := *fa.Disable
		f.Disable = &v
		changed = true
	}
	if f.AgentCircuitID == nil && fa.AgentCircuitId != nil && *fa.AgentCircuitId != "" {
		v := *fa.AgentCircuitId
		f.AgentCircuitID = &v
		changed = true
	}
	if f.AgentRemoteID == nil && fa.AgentRemoteId != nil && *fa.AgentRemoteId != "" {
		v := *fa.AgentRemoteId
		f.AgentRemoteID = &v
		changed = true
	}
	return changed
}

// lateInitializeDHCPOptions back-fills client-identifier/options/use-options.
func lateInitializeDHCPOptions(f *fixedAddressFields, fa *ibclient.FixedAddress) bool {
	changed := false

	if f.ClientIdentifierPrependZero == nil && fa.ClientIdentifierPrependZero != nil {
		v := *fa.ClientIdentifierPrependZero
		f.ClientIdentifierPrependZero = &v
		changed = true
	}
	if f.DHCPClientIdentifier == nil && fa.DhcpClientIdentifier != nil && *fa.DhcpClientIdentifier != "" {
		v := *fa.DhcpClientIdentifier
		f.DHCPClientIdentifier = &v
		changed = true
	}
	if f.UseOptions == nil && fa.UseOptions != nil {
		v := *fa.UseOptions
		f.UseOptions = &v
		changed = true
	}
	// Only back-fill options when useOptions is on (post-backfill value
	// above). When it is off, the observed options are WAPI's own
	// default set, not values implied by the user's config.
	if len(f.Options) == 0 && boolOrFalse(f.UseOptions) {
		if fromFA := dhcpOptionsFromSDK(fa.Options); len(fromFA) > 0 {
			f.Options = fromFA
			changed = true
		}
	}

	return changed
}

// observedFixedAddress holds the primitive field values extracted from a
// WAPI FixedAddress response. The cluster and namespaced
// FixedAddressObservation types are structurally similar but are distinct
// named types with distinct nested-struct field types (e.g.
// *FixedAddressCloudInfo), so they are not directly convertible — each
// scope copies this intermediate struct's fields into its own Observation
// type at the call site.
type observedFixedAddress struct {
	ID                          string
	IPv4Addr                    *string
	IPv6Addr                    *string
	MAC                         *string
	NetworkView                 *string
	Network                     *string
	Name                        *string
	MatchClient                 *string
	Comment                     *string
	ExtAttrs                    map[string]string
	Disable                     *bool
	AgentCircuitID              *string
	AgentRemoteID               *string
	ClientIdentifierPrependZero *bool
	DHCPClientIdentifier        *string
	Options                     []dhcpOption
	UseOptions                  *bool
	Ref                         *string
	DUID                        *string
	CloudInfo                   *sharedCloudInfo
}

// observeFromFixedAddress extracts the fields mirrored by
// FixedAddressObservation (the full-mirror AtProvider convention) from a
// WAPI FixedAddress response fetched via GetFixedAddressByRef.
func observeFromFixedAddress(externalID string, fa *ibclient.FixedAddress) observedFixedAddress {
	o := observedFixedAddress{
		ID:                          externalID,
		MAC:                         fa.Mac,
		Name:                        fa.Name,
		MatchClient:                 fa.MatchClient,
		ExtAttrs:                    extAttrsFromEA(fa.Ea),
		Disable:                     fa.Disable,
		AgentCircuitID:              fa.AgentCircuitId,
		AgentRemoteID:               fa.AgentRemoteId,
		ClientIdentifierPrependZero: fa.ClientIdentifierPrependZero,
		DHCPClientIdentifier:        fa.DhcpClientIdentifier,
		Options:                     dhcpOptionsFromSDK(fa.Options),
		UseOptions:                  fa.UseOptions,
		CloudInfo:                   observedCloudInfo(fa.CloudInfo),
	}
	if fa.IPv4Address != "" {
		v := fa.IPv4Address
		o.IPv4Addr = &v
	}
	if fa.IPv6Address != "" {
		v := fa.IPv6Address
		o.IPv6Addr = &v
	}
	if fa.NetviewName != "" {
		v := fa.NetviewName
		o.NetworkView = &v
	}
	if fa.Cidr != "" {
		v := fa.Cidr
		o.Network = &v
	}
	if fa.Comment != "" {
		v := fa.Comment
		o.Comment = &v
	}
	if fa.Ref != "" {
		v := fa.Ref
		o.Ref = &v
	}
	if fa.Duid != "" {
		v := fa.Duid
		o.DUID = &v
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createFixedAddress issues the WAPI create call. NON-STANDARD: the WAPI
// create wrapper is AllocateIP, not a Create<Resource>-named method — see
// the package doc comment. Stamps the owning managed resource's uid into
// the object's extensible attributes in the same request that creates it
// (identity.Stamp) — there is no follow-up call, so there is no window in
// which the object exists without its identity stamp.
func createFixedAddress(objMgr ibclient.IBObjectManager, f fixedAddressFields, uid string) (*ibclient.FixedAddress, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	isIPv6 := f.isIPv6()
	ipAddr := strOrEmpty(f.IPv4Addr)
	if isIPv6 {
		ipAddr = strOrEmpty(f.IPv6Addr)
	}
	ea := identity.Stamp(buildEA(f.ExtAttrs), uid)
	return objMgr.AllocateIP(
		strOrEmpty(f.NetworkView),
		strOrEmpty(f.Network),
		ipAddr,
		isIPv6,
		strOrEmpty(f.MAC),
		strOrEmpty(f.Name),
		strOrEmpty(f.Comment),
		ea,
		strOrEmpty(f.MatchClient),
		strOrEmpty(f.AgentCircuitID),
		strOrEmpty(f.AgentRemoteID),
		f.ClientIdentifierPrependZero,
		strOrEmpty(f.DHCPClientIdentifier),
		boolOrFalse(f.Disable),
		toSDKOptions(f.Options),
		boolOrFalse(f.UseOptions),
	)
}

// matchClientForUpdate resolves the match_client value to send on Update.
// UpdateFixedAddress rejects an empty match_client for IPv4 objects (the
// SDK validates it against the WAPI enum before issuing the PUT); IPv6
// objects have no such validation. Prefers the desired spec value, falls
// back to the last-observed server value (captured in
// cr.Status.AtProvider by the Observe call earlier in this reconcile), and
// finally falls back to matchClientDefault.
func matchClientForUpdate(desired, observed *string, isIPv6 bool) string {
	if isIPv6 {
		return strOrEmpty(desired)
	}
	if desired != nil && *desired != "" {
		return *desired
	}
	if observed != nil && *observed != "" {
		return *observed
	}
	return matchClientDefault
}

// updateFixedAddress issues the WAPI update call. Every call re-asserts
// the identity stamp (identity.Stamp) in the extattrs it sends. Live
// verification against a real NIOS Grid Manager confirmed that a PUT
// carrying an extattrs object *replaces* the whole map — it is not a
// per-key merge — so omitting the stamp here would wipe it off the
// object on the very first field update after create.
func updateFixedAddress(objMgr ibclient.IBObjectManager, ref string, f fixedAddressFields, observedMatchClient *string, uid string) (*ibclient.FixedAddress, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	isIPv6 := f.isIPv6()
	ipAddr := strOrEmpty(f.IPv4Addr)
	if isIPv6 {
		ipAddr = strOrEmpty(f.IPv6Addr)
	}
	matchClient := matchClientForUpdate(f.MatchClient, observedMatchClient, isIPv6)
	ea := identity.Stamp(buildEA(f.ExtAttrs), uid)
	return objMgr.UpdateFixedAddress(
		ref,
		strOrEmpty(f.NetworkView),
		strOrEmpty(f.Name),
		strOrEmpty(f.Network),
		ipAddr,
		matchClient,
		strOrEmpty(f.MAC),
		strOrEmpty(f.Comment),
		ea,
		strOrEmpty(f.AgentCircuitID),
		strOrEmpty(f.AgentRemoteID),
		f.ClientIdentifierPrependZero,
		strOrEmpty(f.DHCPClientIdentifier),
		boolOrFalse(f.Disable),
		toSDKOptions(f.Options),
		boolOrFalse(f.UseOptions),
	)
}

// deleteFixedAddress issues the WAPI delete call.
func deleteFixedAddress(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteFixedAddress(ref)
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

// newEmptyFixedAddress builds the query/candidate object the identity
// ladder issues both the ref-fetch and the identity-EA search through,
// selecting the correct WAPI object type ("fixedaddress" vs
// "ipv6fixedaddress") for the given family. Unlike Network/
// NetworkContainer, FixedAddress's family is always known from spec (see
// the package doc comment), so there is no dual-search fallback to write.
func newEmptyFixedAddress(isIPv6 bool) func() *ibclient.FixedAddress {
	return func() *ibclient.FixedAddress {
		return ibclient.NewEmptyFixedAddress(isIPv6)
	}
}

// resolveFixedAddressIdentity resolves the FixedAddress identified by
// ref/uid through the shared UID-in-EA ladder, using the given address
// family to select the correct WAPI object type for the search step (see
// newEmptyFixedAddress).
func resolveFixedAddressIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string, isIPv6 bool) (*ibclient.FixedAddress, identity.Outcome, error) {
	return identity.Resolve[*ibclient.FixedAddress](ctx, conn, newEmptyFixedAddress(isIPv6), ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting a
// FixedAddress through the identity ladder during Observe — common to
// both scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	fa           *ibclient.FixedAddress
	obs          observedFixedAddress
	refreshedRef string
	adopted      bool
}

// observeFixedAddress runs the identity ladder for Observe. Unlike the
// simpler resources, FixedAddress's late-init step needs scope-specific
// type conversion (fixedAddressFields <-> the generated CRD type), so the
// caller (cluster.go / namespaced.go) performs lateInitialize itself
// using the returned observeResult.fa — this function only resolves
// identity and builds the observation snapshot.
func observeFixedAddress(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, isIPv6 bool) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	fa, outcome, err := resolveFixedAddressIdentity(ctx, conn, ref, uid, isIPv6)
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
		fa:      fa,
		obs:     observeFromFixedAddress(fa.Ref, fa),
		adopted: outcome == identity.OutcomeAdopted,
	}

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = fa.Ref
	}

	return res, nil
}

// deleteFixedAddressIdentity issues the WAPI delete for the FixedAddress
// this managed resource owns, resolving through the identity ladder first
// so a stale _ref is never mistaken for a deleted object.
func deleteFixedAddressIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string, isIPv6 bool) error {
	obj, outcome, err := resolveFixedAddressIdentity(ctx, conn, ref, uid, isIPv6)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteFixedAddress)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteFixedAddress(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteFixedAddress)
	default:
		return errors.New("identity: unresolved FixedAddress outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced FixedAddress
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterFixedAddress(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster FixedAddress controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("FixedAddress"))

	o.Gate.Register(func() {
		if err := setupNamespacedFixedAddress(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced FixedAddress controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("FixedAddress"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced FixedAddress
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterFixedAddress(mgr, o); err != nil {
		return err
	}
	return setupNamespacedFixedAddress(mgr, o)
}
