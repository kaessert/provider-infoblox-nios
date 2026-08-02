// Package hostrecord implements the Crossplane controller for the
// Infoblox NIOS HostRecord managed resource (WAPI object type
// record:host). Like the ARecord controller, this provider wraps the
// official infoblox-go-client Go SDK directly — the SDK's ObjectManager
// exposes typed CRUD methods (CreateHostRecord/GetHostRecordByRef/
// UpdateHostRecord/DeleteHostRecord) instead of a generic HTTP
// request/response envelope, so there is no internal REST client to
// compose.
//
// HostRecord is wired to the UID-in-EA object-identity ladder (see
// recorda's package doc for the full rationale): the WAPI _ref this
// resource's create call returns is a derived handle, not a stable
// backend-assigned ID, so this controller stamps the managed resource's
// own metadata.uid onto the Grid object as an extensible attribute and
// resolves every Observe/Delete through the shared identity.Resolve
// ladder instead of trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package hostrecord

import (
	"context"
	"fmt"
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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/hostrecord/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/hostrecord/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage        = "cannot track ProviderConfig usage"
	errPersistExternalName = "cannot persist refreshed external name"
	errGetPC               = "cannot get ProviderConfig"
	errGetClusterPC        = "cannot get ClusterProviderConfig"
	errUnsupportedKind     = "unsupported provider config kind"
	errGetSecret           = "cannot get credentials secret"
	errNoSecretRef         = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds    = "unsupported credentials source: only Secret is supported"
	errMissingCredKey      = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager    = "cannot create Infoblox NIOS WAPI object manager"
	errObserveHostRecord   = "cannot observe HostRecord"
	errCreateHostRecord    = "cannot create HostRecord"
	errUpdateHostRecord    = "cannot update HostRecord"
	errDeleteHostRecord    = "cannot delete HostRecord"

	errIpv4CidrWithStaticAddr   = "ipv4Cidr cannot be combined with a static ipv4Addrs address"
	errIpv6CidrWithStaticAddr   = "ipv6Cidr cannot be combined with a static ipv6Addrs address"
	errFilterParamsWithCidr     = "filterParams cannot be combined with ipv4Cidr or ipv6Cidr"
	errFilterParamsRequiresType = "ipAddressType is required when filterParams is set"
	errUnexpectedAllocationType = "unexpected response type from AllocateNextAvailableIp"

	errEmptyUID                  = "cannot stamp HostRecord identity: managed resource's metadata.uid is empty"
	errDeleteUnverifiedOwnership = "refusing to delete: the resolved object's identity extensible attribute is absent or belongs to a different owner, so ownership cannot be verified before an irreversible delete. " +
		"Reconcile the external-name annotation, verify the Grid object manually, or remove the finalizer to abandon it without deleting."
	errPrerequisiteCheck = "cannot verify the identity extensible attribute definition prerequisite"
)

// unresolvedProbeEndpoint is the identity-prerequisite-probe cache key
// used when an ExternalClient is built without a resolved Grid endpoint.
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

// hostRecordClient bundles the SDK's high-level ObjectManager (used for the
// CreateHostRecord/UpdateHostRecord/DeleteHostRecord convenience wrappers)
// together with the lower-level Connector it wraps. The identity ladder
// resolves against the Connector directly — see
// newEmptyHostRecordForIdentity for why — because it operates below
// ObjectManager's typed methods so it can see search match counts.
type hostRecordClient struct {
	objMgr ibclient.IBObjectManager
	conn   ibclient.IBConnector
	// prober checks the identity extensible-attribute-definition
	// prerequisite (ADR-IN-0006 §4) before Create stamps identity onto a
	// new object. nil defaults to identity.DefaultProber — see
	// ensureIdentityPrerequisite.
	prober *identity.Prober
	// endpoint is this client's identity-prerequisite-probe cache key,
	// set by Connect from the ProviderConfig's Grid host after
	// construction. See ensureIdentityPrerequisite's empty-string
	// fallback.
	endpoint string
}

// newHostRecordClient constructs an authenticated hostRecordClient from the
// given credentials. The Connector performs HTTP Basic Auth on every
// request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newHostRecordClient(creds *nioCredentials, sslVerify bool) (*hostRecordClient, error) {
	return newHostRecordClientWithScheme(creds, sslVerify, "https", "443")
}

// newHostRecordClientWithScheme is the scheme/port-parameterized variant of
// newHostRecordClient used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newHostRecordClientWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (*hostRecordClient, error) {
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

	return &hostRecordClient{
		objMgr: ibclient.NewObjectManager(conn, "", ""),
		conn:   conn,
	}, nil
}

// newEmptyHostRecordForIdentity builds a HostRecord template extending
// the ObjectManager.GetHostRecordByRef wrapper's fixed default field set
// (extattrs, ipv4addrs, ipv6addrs, name, view, zone, comment,
// network_view, aliases, use_ttl, ttl, configure_for_dns — see
// ibclient.NewEmptyHostRecord) with three response-only fields that
// wrapper never requests: disable, dns_name, dns_aliases. WAPI's
// _return_fields query parameter REPLACES rather than appends to the
// object's default field set, so without this extension Disable,
// DNSName, and DNSAliases would silently stay at their Go zero value in
// every observation regardless of the record's real server-side state —
// comparing spec.Disable against an always-nil observed Disable would
// leave the resource stuck at ResourceUpToDate:false forever the first
// time a user disables a record. The upstream SDK itself uses this exact
// SetReturnFields-append pattern internally (see
// ObjectManager.SearchHostRecordByAltId), so this stays entirely within
// the SDK's supported API — it just skips the ObjectManager convenience
// wrapper's hard-coded field list for the read path.
//
// This also serves as the newEmpty constructor the identity ladder
// (identity.Resolve) uses for both its by-ref GET and its EA-filtered
// search — both need the same extended field set so a resolved object's
// Observation is never missing these three fields regardless of which
// ladder rung found it.
func newEmptyHostRecordForIdentity() *ibclient.HostRecord {
	rec := ibclient.NewEmptyHostRecord()
	rec.SetReturnFields(append(rec.ReturnFields(), "disable", "dns_name", "dns_aliases"))
	return rec
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// ipv4AddrValue is the package-local canonical form of the CRD's per-scope
// HostRecordIpv4Addr type (cluster and namespaced generate structurally
// identical but distinctly named copies — see recorda's isUpToDate doc
// comment for the full rationale on why this provider parameterizes
// shared logic on plain field values instead of the generated types
// directly).
type ipv4AddrValue struct {
	Ipv4Addr         string
	MAC              *string
	ConfigureForDHCP *bool
	NextServer       *string
}

// ipv6AddrValue is the ipv6 counterpart of ipv4AddrValue.
type ipv6AddrValue struct {
	Ipv6Addr         string
	Duid             *string
	ConfigureForDHCP *bool
}

// ipv4AddrValuesFromSDK converts a WAPI response's Ipv4Addrs slice into the
// package-local canonical form.
func ipv4AddrValuesFromSDK(addrs []ibclient.HostRecordIpv4Addr) []ipv4AddrValue {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]ipv4AddrValue, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, ipv4AddrValue{
			Ipv4Addr:         strOrEmpty(a.Ipv4Addr),
			MAC:              a.Mac,
			ConfigureForDHCP: a.EnableDhcp,
			NextServer:       a.Nextserver,
		})
	}
	return out
}

// ipv6AddrValuesFromSDK converts a WAPI response's Ipv6Addrs slice into the
// package-local canonical form.
func ipv6AddrValuesFromSDK(addrs []ibclient.HostRecordIpv6Addr) []ipv6AddrValue {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]ipv6AddrValue, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, ipv6AddrValue{
			Ipv6Addr:         strOrEmpty(a.Ipv6Addr),
			Duid:             a.Duid,
			ConfigureForDHCP: a.EnableDhcp,
		})
	}
	return out
}

// firstIpv4AddrAndMAC returns the address/MAC pair of the first Ipv4Addrs
// entry, or two empty strings when the list is empty. See
// ipv4AddrsEqual's doc comment for why only the first entry is ever
// forwarded to WAPI.
func firstIpv4AddrAndMAC(addrs []ipv4AddrValue) (addr, mac string) {
	if len(addrs) == 0 {
		return "", ""
	}
	return addrs[0].Ipv4Addr, strOrEmpty(addrs[0].MAC)
}

// firstIpv6AddrAndDuid is the ipv6 counterpart of firstIpv4AddrAndMAC.
func firstIpv6AddrAndDuid(addrs []ipv6AddrValue) (addr, duid string) {
	if len(addrs) == 0 {
		return "", ""
	}
	return addrs[0].Ipv6Addr, strOrEmpty(addrs[0].Duid)
}

// anyIpv4AddrSet reports whether any entry in addrs carries a non-empty
// static address. Used by validateHostRecordAllocation to reject
// ipv4Cidr when combined with a static address — even though only the
// first entry is ever forwarded to WAPI (see ipv4AddrsEqual), a static
// address anywhere in the list signals user intent that conflicts with
// dynamic allocation.
func anyIpv4AddrSet(addrs []ipv4AddrValue) bool {
	for _, a := range addrs {
		if a.Ipv4Addr != "" {
			return true
		}
	}
	return false
}

// anyIpv6AddrSet is the ipv6 counterpart of anyIpv4AddrSet.
func anyIpv6AddrSet(addrs []ipv6AddrValue) bool {
	for _, a := range addrs {
		if a.Ipv6Addr != "" {
			return true
		}
	}
	return false
}

func firstConfigureForDHCPv4(addrs []ipv4AddrValue) bool {
	if len(addrs) == 0 {
		return false
	}
	return boolOrFalse(addrs[0].ConfigureForDHCP)
}

func firstConfigureForDHCPv6(addrs []ipv6AddrValue) bool {
	if len(addrs) == 0 {
		return false
	}
	return boolOrFalse(addrs[0].ConfigureForDHCP)
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

func uint32OrZero(v *uint32) uint32 {
	if v == nil {
		return 0
	}
	return *v
}

func boolOrFalse(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
// These helpers take a read-only snapshot of the ForProvider fields
// (hostRecordCompareFields) rather than a whole HostRecordParameters
// value. The cluster and namespaced HostRecordParameters types are
// structurally identical (same field names and primitive types) but are
// distinct named Go types, so a direct struct conversion between them is
// not available — each scope builds this snapshot at the call site.

// hostRecordCompareFields is a read-only, scope-agnostic snapshot of the
// mutable HostRecordParameters fields used by isUpToDate and the SDK call
// wrappers. networkView is intentionally excluded — it is immutable (see
// createHostRecord) — and is passed separately only where Create needs
// it.
type hostRecordCompareFields struct {
	Name            *string
	Ipv4Addrs       []ipv4AddrValue
	Ipv6Addrs       []ipv6AddrValue
	View            *string
	Aliases         []string
	ConfigureForDNS *bool
	Comment         *string
	Disable         *bool
	TTL             *uint32
	UseTTL          *bool
	ExtAttrs        map[string]string
}

// ipv4AddrsEqual compares only the first entry of each list.
//
// The CreateHostRecord/UpdateHostRecord SDK methods this controller uses
// accept a single scalar ipv4Addr/macAddr pair, not a list — despite
// HostRecordParameters exposing Ipv4Addrs as a CRD list matching the WAPI
// object model. Only the first entry is ever sent to WAPI (see
// createHostRecord/updateHostRecord); comparing beyond it would leave the
// resource permanently ResourceUpToDate:false whenever a caller supplies
// more than one address, since the SDK wrapper this provider uses can
// never converge additional entries — an infinite reconcile loop. This
// mirrors the ARecord controller's documented, tested SDK limitations
// (e.g. RemoveAssociatedPtr). NextServer is similarly excluded: the
// exposed SDK parameters have no next-server hook.
func ipv4AddrsEqual(spec []ipv4AddrValue, observed []ibclient.HostRecordIpv4Addr) bool {
	specAddr, specMAC := firstIpv4AddrAndMAC(spec)
	obsValues := ipv4AddrValuesFromSDK(observed)
	obsAddr, obsMAC := firstIpv4AddrAndMAC(obsValues)
	if specAddr != obsAddr || specMAC != obsMAC {
		return false
	}
	return firstConfigureForDHCPv4(spec) == firstConfigureForDHCPv4(obsValues)
}

// ipv6AddrsEqual is the ipv6 counterpart of ipv4AddrsEqual — see its doc
// comment for the SDK limitation this works around.
func ipv6AddrsEqual(spec []ipv6AddrValue, observed []ibclient.HostRecordIpv6Addr) bool {
	specAddr, specDuid := firstIpv6AddrAndDuid(spec)
	obsValues := ipv6AddrValuesFromSDK(observed)
	obsAddr, obsDuid := firstIpv6AddrAndDuid(obsValues)
	if specAddr != obsAddr || specDuid != obsDuid {
		return false
	}
	return firstConfigureForDHCPv6(spec) == firstConfigureForDHCPv6(obsValues)
}

// isUpToDate compares the desired HostRecord fields against the observed
// HostRecord. networkView is immutable — live testing against a Grid
// Manager confirmed WAPI rejects updates to it — and is intentionally
// excluded from this comparison.
func isUpToDate(p hostRecordCompareFields, rec *ibclient.HostRecord) bool {
	if strOrEmpty(p.Name) != strOrEmpty(rec.Name) {
		return false
	}
	if !ipv4AddrsEqual(p.Ipv4Addrs, rec.Ipv4Addrs) {
		return false
	}
	if !ipv6AddrsEqual(p.Ipv6Addrs, rec.Ipv6Addrs) {
		return false
	}
	if strOrEmpty(p.View) != strOrEmpty(rec.View) {
		return false
	}
	if !stringSlicesEqual(p.Aliases, rec.Aliases) {
		return false
	}
	if boolOrFalse(p.ConfigureForDNS) != boolOrFalse(rec.EnableDns) {
		return false
	}
	if strOrEmpty(p.Comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if boolOrFalse(p.Disable) != boolOrFalse(rec.Disable) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(p.UseTTL) != boolOrFalse(rec.UseTtl) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted ttl and returns the zone default on every
	// GET — comparing it against the spec value never converges.
	if boolOrFalse(p.UseTTL) {
		if uint32OrZero(p.TTL) != uint32OrZero(rec.Ttl) {
			return false
		}
	}
	return extAttrsEqual(p.ExtAttrs, extAttrsFromEA(identity.Strip(rec.Ea)))
}

// lateInitString back-fills dst from src when dst is unset and src
// carries a non-empty server-assigned value. Returns true if dst changed.
func lateInitString(dst **string, src *string) bool {
	if *dst != nil || src == nil || *src == "" {
		return false
	}
	v := *src
	*dst = &v
	return true
}

// lateInitUint32 is the uint32 counterpart of lateInitString.
func lateInitUint32(dst **uint32, src *uint32) bool {
	if *dst != nil || src == nil {
		return false
	}
	v := *src
	*dst = &v
	return true
}

// lateInitBool is the bool counterpart of lateInitString (no non-empty
// check — false is a meaningful, distinct server value from unset).
func lateInitBool(dst **bool, src *bool) bool {
	if *dst != nil || src == nil {
		return false
	}
	v := *src
	*dst = &v
	return true
}

// lateInitExtAttrs back-fills dst from the observed EA map when dst is
// empty. Returns true if dst changed.
func lateInitExtAttrs(dst *map[string]string, ea ibclient.EA) bool {
	if len(*dst) != 0 {
		return false
	}
	fromRec := extAttrsFromEA(ea)
	if len(fromRec) == 0 {
		return false
	}
	*dst = fromRec
	return true
}

// lateInitAliases back-fills dst from the observed Aliases list when dst
// is empty. Returns true if dst changed.
func lateInitAliases(dst *[]string, observed []string) bool {
	if len(*dst) != 0 || len(observed) == 0 {
		return false
	}
	*dst = append([]string(nil), observed...)
	return true
}

// lateInitIpv4Addrs back-fills dst from the observed Ipv4Addrs list when
// dst is empty. Returns true if dst changed. Only reached when Create
// used dynamic allocation (ipv4Cidr or filterParams) — the CRD's
// Required marker on Ipv4Addrs only requires the field be present, and
// an empty list satisfies it, so a user provisioning via CIDR/EA-filter
// allocation legitimately submits ipv4Addrs: [].
func lateInitIpv4Addrs(dst *[]ipv4AddrValue, observed []ibclient.HostRecordIpv4Addr) bool {
	if len(*dst) != 0 {
		return false
	}
	fromRec := ipv4AddrValuesFromSDK(observed)
	if len(fromRec) == 0 {
		return false
	}
	*dst = fromRec
	return true
}

// lateInitIpv6Addrs back-fills dst from the observed Ipv6Addrs list when
// dst is empty. Returns true if dst changed.
func lateInitIpv6Addrs(dst *[]ipv6AddrValue, observed []ibclient.HostRecordIpv6Addr) bool {
	if len(*dst) != 0 {
		return false
	}
	fromRec := ipv6AddrValuesFromSDK(observed)
	if len(fromRec) == 0 {
		return false
	}
	*dst = fromRec
	return true
}

// lateInitialize back-fills server-defaulted optional fields — comment,
// ttl, useTtl, extAttrs, view, configureForDns, disable, aliases, and
// ipv4Addrs/ipv6Addrs — from the observed HostRecord into spec so
// isUpToDate does not see phantom drift on the next reconcile. The
// required Name field and the immutable networkView field are never
// late-initialized. Ipv4Addrs/Ipv6Addrs are late-initialized only when
// still empty in spec — the case where Create used dynamic allocation
// (ipv4Cidr, ipv6Cidr, or filterParams) instead of a static address, so
// the WAPI-assigned address is captured back into spec for a stable
// comparison on every later reconcile. Returns true if any field was
// changed.
func lateInitialize(
	comment **string,
	ttl **uint32,
	useTTL **bool,
	extAttrs *map[string]string,
	view **string,
	configureForDNS **bool,
	disable **bool,
	aliases *[]string,
	ipv4Addrs *[]ipv4AddrValue,
	ipv6Addrs *[]ipv6AddrValue,
	rec *ibclient.HostRecord,
) bool {
	changed := lateInitString(comment, rec.Comment)
	changed = lateInitBool(useTTL, rec.UseTtl) || changed
	// Only back-fill ttl when useTtl is on (post-backfill value above).
	// When it is off, the observed ttl is WAPI's zone default, not a
	// value implied by the user's config — writing it into spec would
	// silently claim a TTL that is not in effect.
	if boolOrFalse(*useTTL) {
		changed = lateInitUint32(ttl, rec.Ttl) || changed
	}
	changed = lateInitExtAttrs(extAttrs, identity.Strip(rec.Ea)) || changed
	changed = lateInitString(view, rec.View) || changed
	changed = lateInitBool(configureForDNS, rec.EnableDns) || changed
	changed = lateInitBool(disable, rec.Disable) || changed
	changed = lateInitAliases(aliases, rec.Aliases) || changed
	changed = lateInitIpv4Addrs(ipv4Addrs, rec.Ipv4Addrs) || changed
	changed = lateInitIpv6Addrs(ipv6Addrs, rec.Ipv6Addrs) || changed
	return changed
}

// observedHostRecord holds the primitive field values extracted from a
// WAPI HostRecord response. The cluster and namespaced
// HostRecordObservation types are structurally similar but are distinct
// named types, so they are not directly convertible — each scope copies
// this intermediate struct's fields into its own Observation type at the
// call site.
type observedHostRecord struct {
	ID              string
	Name            *string
	Ipv4Addrs       []ipv4AddrValue
	Ipv6Addrs       []ipv6AddrValue
	NetworkView     *string
	View            *string
	Aliases         []string
	ConfigureForDNS *bool
	Comment         *string
	Disable         *bool
	TTL             *uint32
	UseTTL          *bool
	ExtAttrs        map[string]string
	Ref             *string
	Zone            *string
	DNSName         *string
	DNSAliases      []string
}

// observeFromHostRecord extracts the fields mirrored by
// HostRecordObservation (the full-mirror AtProvider convention) from a
// WAPI HostRecord response resolved via the identity ladder.
func observeFromHostRecord(externalID string, rec *ibclient.HostRecord) observedHostRecord {
	o := observedHostRecord{
		ID:        externalID,
		Name:      rec.Name,
		Ipv4Addrs: ipv4AddrValuesFromSDK(rec.Ipv4Addrs),
		Ipv6Addrs: ipv6AddrValuesFromSDK(rec.Ipv6Addrs),
		Aliases:   rec.Aliases,
		ExtAttrs:  extAttrsFromEA(rec.Ea),
	}
	if rec.NetworkView != "" {
		v := rec.NetworkView
		o.NetworkView = &v
	}
	if rec.View != nil && *rec.View != "" {
		v := *rec.View
		o.View = &v
	}
	if rec.EnableDns != nil {
		v := *rec.EnableDns
		o.ConfigureForDNS = &v
	}
	if rec.Comment != nil && *rec.Comment != "" {
		v := *rec.Comment
		o.Comment = &v
	}
	if rec.Disable != nil {
		v := *rec.Disable
		o.Disable = &v
	}
	if rec.Ttl != nil {
		v := *rec.Ttl
		o.TTL = &v
	}
	if rec.UseTtl != nil {
		v := *rec.UseTtl
		o.UseTTL = &v
	}
	if rec.Ref != "" {
		v := rec.Ref
		o.Ref = &v
	}
	if rec.Zone != "" {
		v := rec.Zone
		o.Zone = &v
	}
	if rec.DnsName != "" {
		v := rec.DnsName
		o.DNSName = &v
	}
	if len(rec.DnsAliases) > 0 {
		o.DNSAliases = rec.DnsAliases
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// validateHostRecordAllocation enforces the mutual-exclusivity rules
// between HostRecord's IP-provisioning strategies: static
// ipv4Addrs/ipv6Addrs entries, CIDR-based next-available-IP allocation
// (ipv4Cidr/ipv6Cidr, routed through CreateHostRecord), and EA-filter-based
// next-available-IP allocation (filterParams + ipAddressType, routed
// through AllocateNextAvailableIp). Called by provisionHostRecord before
// dispatching to either SDK call.
func validateHostRecordAllocation(p hostRecordCompareFields, ipv4Cidr, ipv6Cidr *string, filterParams map[string]string, ipAddressType *string) error {
	v4Cidr := strOrEmpty(ipv4Cidr)
	v6Cidr := strOrEmpty(ipv6Cidr)

	if v4Cidr != "" && anyIpv4AddrSet(p.Ipv4Addrs) {
		return errors.New(errIpv4CidrWithStaticAddr)
	}
	if v6Cidr != "" && anyIpv6AddrSet(p.Ipv6Addrs) {
		return errors.New(errIpv6CidrWithStaticAddr)
	}
	if len(filterParams) > 0 {
		if v4Cidr != "" || v6Cidr != "" {
			return errors.New(errFilterParamsWithCidr)
		}
		if strOrEmpty(ipAddressType) == "" {
			return errors.New(errFilterParamsRequiresType)
		}
	}
	return nil
}

// createHostRecord issues the WAPI create call. Only the first entry of
// Ipv4Addrs/Ipv6Addrs is forwarded — see ipv4AddrsEqual's doc comment for
// the SDK limitation this works around. When ipv4Cidr/ipv6Cidr is
// non-empty and the corresponding static address is empty, the SDK's
// CreateHostRecord substitutes a func:nextavailableip expression for that
// address internally — this is the CIDR-based dynamic allocation path
// (mutually exclusive with a static address of the same family; see
// validateHostRecordAllocation). networkView is a create-time-only
// parameter — it is immutable, so it is passed here but never again on
// Update.
func createHostRecord(objMgr ibclient.IBObjectManager, p hostRecordCompareFields, networkView, ipv4Cidr, ipv6Cidr *string, uid string) (*ibclient.HostRecord, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	ipv4Addr, mac := firstIpv4AddrAndMAC(p.Ipv4Addrs)
	ipv6Addr, duid := firstIpv6AddrAndDuid(p.Ipv6Addrs)
	enableDhcp := firstConfigureForDHCPv4(p.Ipv4Addrs) || firstConfigureForDHCPv6(p.Ipv6Addrs)

	return objMgr.CreateHostRecord(
		boolOrFalse(p.ConfigureForDNS),
		enableDhcp,
		strOrEmpty(p.Name),
		strOrEmpty(networkView),
		strOrEmpty(p.View),
		strOrEmpty(ipv4Cidr),
		strOrEmpty(ipv6Cidr),
		ipv4Addr,
		ipv6Addr,
		mac,
		duid,
		boolOrFalse(p.UseTTL),
		uint32OrZero(p.TTL),
		strOrEmpty(p.Comment),
		identity.Stamp(buildEA(p.ExtAttrs), uid),
		p.Aliases,
		boolOrFalse(p.Disable),
	)
}

// allocateNextAvailableHostRecord issues the WAPI EA-filter-based
// next-available-IP allocation call — used when filterParams and
// ipAddressType are set instead of a static address or a CIDR.
// filterParams becomes the WAPI search filter (the SDK's objectParams
// argument) used to find a candidate Network object to allocate an
// address from; networkView is merged in as well when set, mirroring the
// infoblox-go-client's own reference caller convention, since
// "network_view" is a plain network-object search field rather than an
// extensible attribute. The SDK returns the allocated object as
// interface{} — for objectType "record:host" this is always a
// *ibclient.HostRecord (see ObjectManager.AllocateNextAvailableIp), but
// the type assertion is still checked defensively.
func allocateNextAvailableHostRecord(objMgr ibclient.IBObjectManager, p hostRecordCompareFields, networkView *string, filterParams map[string]string, ipAddressType string, uid string) (*ibclient.HostRecord, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	_, mac := firstIpv4AddrAndMAC(p.Ipv4Addrs)
	_, duid := firstIpv6AddrAndDuid(p.Ipv6Addrs)
	enableDhcp := firstConfigureForDHCPv4(p.Ipv4Addrs) || firstConfigureForDHCPv6(p.Ipv6Addrs)

	objectParams := make(map[string]string, len(filterParams)+1)
	for k, v := range filterParams {
		objectParams[k] = v
	}
	if nv := strOrEmpty(networkView); nv != "" {
		objectParams["network_view"] = nv
	}

	res, err := objMgr.AllocateNextAvailableIp(
		strOrEmpty(p.Name),
		"record:host",
		objectParams,
		nil, // params (_parameters) — no additional WAPI search filters beyond objectParams
		false,
		identity.Stamp(buildEA(p.ExtAttrs), uid),
		strOrEmpty(p.Comment),
		boolOrFalse(p.Disable),
		nil, // n — allocate a single address
		ipAddressType,
		boolOrFalse(p.ConfigureForDNS),
		enableDhcp,
		mac,
		duid,
		strOrEmpty(networkView),
		strOrEmpty(p.View),
		boolOrFalse(p.UseTTL),
		uint32OrZero(p.TTL),
		p.Aliases,
	)
	if err != nil {
		return nil, err
	}
	rec, ok := res.(*ibclient.HostRecord)
	if !ok {
		return nil, errors.Errorf("%s: got %T", errUnexpectedAllocationType, res)
	}
	return rec, nil
}

// provisionHostRecord validates the desired allocation strategy and
// dispatches HostRecord creation accordingly: EA-filter-based allocation
// (filterParams + ipAddressType) takes priority when set, otherwise
// createHostRecord handles both the static-address and CIDR-based
// allocation cases (the SDK itself decides between them based on whether
// ipv4Cidr/ipv6Cidr is non-empty).
func provisionHostRecord(objMgr ibclient.IBObjectManager, p hostRecordCompareFields, networkView, ipv4Cidr, ipv6Cidr *string, filterParams map[string]string, ipAddressType *string, uid string) (*ibclient.HostRecord, error) {
	if err := validateHostRecordAllocation(p, ipv4Cidr, ipv6Cidr, filterParams, ipAddressType); err != nil {
		return nil, err
	}
	if len(filterParams) > 0 {
		return allocateNextAvailableHostRecord(objMgr, p, networkView, filterParams, strOrEmpty(ipAddressType), uid)
	}
	return createHostRecord(objMgr, p, networkView, ipv4Cidr, ipv6Cidr, uid)
}

// updateHostRecord issues the WAPI update call. networkView is never
// passed (immutable field) — UpdateHostRecord accepts a netView parameter
// but the SDK itself never forwards it to the underlying object (it
// always hard-codes an empty network_view on update), so this wrapper
// mirrors that by passing "" explicitly. Every call re-asserts the
// identity stamp (identity.Stamp) — a WAPI PUT carrying extattrs replaces
// the whole map, not a per-key merge.
func updateHostRecord(objMgr ibclient.IBObjectManager, ref string, p hostRecordCompareFields, uid string) (*ibclient.HostRecord, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	ipv4Addr, mac := firstIpv4AddrAndMAC(p.Ipv4Addrs)
	ipv6Addr, duid := firstIpv6AddrAndDuid(p.Ipv6Addrs)
	enableDhcp := firstConfigureForDHCPv4(p.Ipv4Addrs) || firstConfigureForDHCPv6(p.Ipv6Addrs)

	return objMgr.UpdateHostRecord(
		ref,
		boolOrFalse(p.ConfigureForDNS),
		enableDhcp,
		strOrEmpty(p.Name),
		"", // netView — networkView is immutable, see doc comment above
		strOrEmpty(p.View),
		"", // ipv4cidr — see createHostRecord
		"", // ipv6cidr — see createHostRecord
		ipv4Addr,
		ipv6Addr,
		mac,
		duid,
		boolOrFalse(p.UseTTL),
		uint32OrZero(p.TTL),
		strOrEmpty(p.Comment),
		identity.Stamp(buildEA(p.ExtAttrs), uid),
		p.Aliases,
		boolOrFalse(p.Disable),
	)
}

// deleteHostRecord issues the WAPI delete call.
func deleteHostRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteHostRecord(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────

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

// resolveHostRecordIdentity resolves the HostRecord identified by
// ref/uid through the shared UID-in-EA ladder — see recorda's
// resolveARecordIdentity for the full rationale. newEmptyHostRecordForIdentity
// (not the bare ibclient.NewEmptyHostRecord) supplies the extended
// return-fields template so a resolved object's disable/dns_name/
// dns_aliases fields are populated regardless of which ladder rung found
// it.
func resolveHostRecordIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.HostRecord, identity.Outcome, error) {
	return identity.Resolve[*ibclient.HostRecord](ctx, conn, newEmptyHostRecordForIdentity, ref, uid)
}

// hostRecordObserveResult bundles the shared parts of resolving a
// HostRecord through the identity ladder during Observe — common to both
// scopes, which differ only in their concrete CRD types and how they
// translate hostRecordCompareFields to/from their generated Ipv4Addrs/
// Ipv6Addrs types.
type hostRecordObserveResult struct {
	exists       bool
	rec          *ibclient.HostRecord
	adopted      bool
	refreshedRef string
}

func observeHostRecordIdentity(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string) (hostRecordObserveResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveHostRecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return hostRecordObserveResult{}, prereqErr
			}
		}
		return hostRecordObserveResult{}, err
	}
	if outcome == identity.OutcomeNotFound {
		return hostRecordObserveResult{exists: false}, nil
	}

	res := hostRecordObserveResult{
		exists:  true,
		rec:     rec,
		adopted: outcome == identity.OutcomeAdopted,
	}
	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
	}
	return res, nil
}

// deleteHostRecordIdentity issues the WAPI delete for the HostRecord this
// managed resource owns, resolving through the identity ladder first so
// a stale _ref is never mistaken for a deleted object. See recorda's
// deleteARecordIdentity for the full rationale (identical pattern).
func deleteHostRecordIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveHostRecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteHostRecord)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteHostRecord(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteHostRecord)
	default:
		return errors.New("identity: unresolved HostRecord outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced HostRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterHostRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster HostRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("HostRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedHostRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced HostRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("HostRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced HostRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterHostRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedHostRecord(mgr, o)
}
