// Package recordns implements the Crossplane controller for the Infoblox
// NIOS NSRecord managed resource (WAPI object type `record:ns`). Like the
// other DNS-record controllers in this provider, it wraps the official
// infoblox-go-client Go SDK directly — the SDK's ObjectManager exposes
// typed CRUD methods (CreateNSRecord/GetNSRecordByRef/UpdateNSRecord/
// DeleteNSRecord) instead of a generic HTTP request/response envelope, so
// there is no internal REST client to compose.
//
// NSRecord is deliberately NOT wired to the UID-in-EA object-identity
// ladder the other eight DNS record types use (see recorda's package doc
// for that ladder's rationale). The ladder stamps the managed resource's
// metadata.uid onto the Grid object as an extensible attribute and later
// resolves it by searching on that attribute — record:ns has no
// extensible-attribute support at all: the WAPI object type's field list
// does not include extattrs, and its SDK constructor
// (ibclient.NewEmptyRecordNS) requests no extattrs in its default return
// fields (verified against the vendored SDK; every EA-capable record
// type's constructor does). This was also confirmed live: a GET
// record:ns?_schema request against a real Grid (WAPI 2.12) returns
// eleven fields — addresses, cloud_info, creator, dns_name, last_queried,
// ms_delegation_name, name, nameserver, policy, view, zone — and extattrs
// is not among them. At the Go level this shows up as *ibclient.RecordNS
// having no Ea field at all, unlike every other EA-capable record type in
// this provider; identity.Resolve hard-errors on that condition by design
// (it refuses to silently skip the identity check for a type that looks
// like it should have one) rather than treating the absence as "nothing
// to verify". There is nowhere to stamp identity onto, so there is
// nothing for identity.Resolve to search on.
//
// This package still reuses identity.ManagerAndConnector purely as
// connector-bundling infrastructure (newObjectManager below) — that is
// unrelated to the identity ladder itself. In its place, NSRecord
// resolves identity from its own natural key: name, view, and nameserver
// are now all immutable for the object's lifetime (CEL self==oldSelf on
// all three). name and view were always server-enforced immutable
// (live-verified ADR-IN-0004: `supports=rws`, no `u` — WAPI itself
// rejects a PUT that changes either); nameserver is provider-imposed
// immutable — WAPI genuinely allows updating it in place, but it was the
// one mutable component of the tuple, so freezing it at the CRD/CEL layer
// was what closed this resource's ownership-verification gap. Those same
// three fields are exactly the tuple WAPI computes the object's `_ref`
// from (live-verified ADR-IN-0004). Because none of the three can be
// mutated into matching a different object's tuple, a Grid object that
// matches this resource's (name, view, nameserver) IS this resource's
// object, not merely an object that currently happens to look like it —
// there is no mutation path by which some other object could drift into
// the same shape. Exactly one match is therefore resolved as the same
// object and adopted (see resolveNSRecordByNaturalKey for the boundary
// this still enforces: more than one match refuses rather than assumes
// WAPI's own addressing guarantee holds in every Grid state).
//
// Delete/Observe keep the two-step resolution most resources in this
// provider use on a stale handle (see the staleref package doc for the
// general case, still followed by resources that lack a sound way to
// verify a natural-key match): a 404 on the stored _ref triggers the
// natural-key search above before concluding the object is gone. Unlike
// those resources, NSRecord's search result is conclusive by
// construction once it resolves to exactly one match, so this package
// does not import staleref — there is no "provisional, ownership
// unverifiable" state left once every field composing the tuple is
// immutable. resolveNSRecordForObserve and deleteNSRecordResolving404
// both reach the same natural-key search and answer the same way: zero
// matches means the object is genuinely gone, exactly one match is
// adopted, and more than one match refuses (AmbiguousNaturalKeyMatchError)
// rather than take the first result. The search also has no dependency on
// a prior ref at all, so it doubles as create-crash-window recovery: a
// Create() that reached the Grid but crashed before the external-name
// annotation was persisted is found the same way a rotated ref is (see
// observeRefFor).
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and identity resolution lives
// here.
package recordns

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordns/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordns/v1alpha1"
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
	errObserveNSRecord     = "cannot observe NSRecord"
	errCreateNSRecord      = "cannot create NSRecord"
	errUpdateNSRecord      = "cannot update NSRecord"
	errDeleteNSRecord      = "cannot delete NSRecord"
)

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
const wapiVersion = "2.9.7"

// ── Credential bridge ───────────────────────────────────────────────────────

// nioCredentials holds the WAPI connection parameters extracted from the
// ProviderConfig's credentials Secret (host/username/password keys). TLS
// verification is governed by the ProviderConfig's own sslVerify spec
// field, not by anything in this Secret — see newObjectManager.
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

// newObjectManager constructs an authenticated
// identity.ManagerAndConnector from the given credentials — the SDK's
// high-level ObjectManager for the ordinary CRUD calls, and the
// lower-level Connector that resolveNSRecordByNaturalKey needs directly
// so it can issue a server-side filtered search and see the match count
// (ObjectManager's typed getters hide that, and its GetAllRecordNS
// helper additionally flattens a not-found result into a plain string
// error that the type-based classifier in isNotFound cannot recognize —
// see resolveNSRecordByNaturalKey's doc for the full rationale). The
// Connector performs HTTP Basic Auth on every request and only validates
// configuration locally — no network round-trip happens until the first
// Observe/Create/Update/Delete call.
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
	// sslVerify is resolved by the caller from the ProviderConfig's
	// sslVerify spec field (default: true when nil). Set to false only
	// when the Grid Manager uses a self-signed certificate whose SAN
	// does not match the reachable host address.
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

// nsRecordAddress is a scope-agnostic mirror of the generated
// NSRecordAddress type (identical field set in both the cluster and
// namespaced API packages, but distinct named Go types) used to shuttle
// glue-address values through the shared helpers below without importing
// either scope's API package here.
type nsRecordAddress struct {
	Address       *string
	AutoCreatePtr *bool
}

// buildAddresses converts the CRD's glue-address list into the SDK's
// []*ibclient.ZoneNameServer wire type.
func buildAddresses(addrs []nsRecordAddress) []*ibclient.ZoneNameServer {
	if len(addrs) == 0 {
		return nil
	}
	out := make([]*ibclient.ZoneNameServer, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, &ibclient.ZoneNameServer{
			Address:       strOrEmpty(a.Address),
			AutoCreatePtr: boolOrFalse(a.AutoCreatePtr),
		})
	}
	return out
}

// addressesFromZoneNameServers converts the SDK's glue-address list back
// into the CRD's simplified representation.
func addressesFromZoneNameServers(zns []*ibclient.ZoneNameServer) []nsRecordAddress {
	if len(zns) == 0 {
		return nil
	}
	out := make([]nsRecordAddress, 0, len(zns))
	for _, z := range zns {
		if z == nil {
			continue
		}
		addr := z.Address
		autoCreate := z.AutoCreatePtr
		out = append(out, nsRecordAddress{Address: &addr, AutoCreatePtr: &autoCreate})
	}
	return out
}

// addressesEqual reports whether two glue-address lists are equivalent,
// comparing element-by-element in order. WAPI returns addresses in a
// stable order matching the request, so an order-sensitive comparison
// does not produce phantom diffs in practice.
func addressesEqual(a, b []nsRecordAddress) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Address) != strOrEmpty(b[i].Address) {
			return false
		}
		if boolOrFalse(a[i].AutoCreatePtr) != boolOrFalse(b[i].AutoCreatePtr) {
			return false
		}
	}
	return true
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
// than a whole NSRecordParameters value. The cluster and namespaced
// NSRecordParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct
// struct conversion between them is not always available once other
// resources in this provider grow reference fields; parameterizing on the
// field pointers instead lets both scopes share this logic unconditionally.

// isUpToDate compares the desired mutable NSRecord fields (nameserver,
// addresses, msDelegationName) against the observed RecordNS. name and
// view are hard-immutable (live-verified against a real Grid: `supports=rws`, no
// `u` — WAPI rejects a PUT that changes either field even though the Go
// SDK's UpdateNSRecord signature still accepts them) and zone is
// derived/response-only — all three are intentionally excluded from this
// comparison to avoid an infinite reconcile loop.
func isUpToDate(nameserver, msDelegationName *string, addresses []nsRecordAddress, rec *ibclient.RecordNS) bool {
	if strOrEmpty(nameserver) != strOrEmpty(rec.Nameserver) {
		return false
	}
	// msDelegationName is the MS delegation point name — it is only ever
	// persisted on Microsoft/AD-integrated zones. On an ordinary
	// Grid-managed zone, NIOS accepts a PUT that sets it with HTTP 200 but
	// never stores the value, so the field comes back empty on every
	// subsequent GET no matter what the spec asks for. Diffing a spec
	// value against a field the API structurally cannot echo back
	// produces a permanent phantom drift and an infinite Update loop that
	// still reports ReconcileSuccess. This comparison is therefore
	// observation-only-when-empty: it is skipped entirely when the
	// observed value is empty (the API gives no evidence either way), but
	// applied normally whenever the API has actually persisted a value
	// (Microsoft/AD-integrated zones), so real drift there is still
	// caught.
	if observed := strOrEmpty(rec.MsDelegationName); observed != "" {
		if strOrEmpty(msDelegationName) != observed {
			return false
		}
	}
	return addressesEqual(addresses, addressesFromZoneNameServers(rec.Addresses))
}

// lateInitialize back-fills the server-defaulted optional msDelegationName
// field from the observed RecordNS into spec so isUpToDate does not see
// phantom drift on the next reconcile. Required fields (name, nameserver,
// view, addresses) are never late-initialized — they are always
// user-supplied (required on the CRD). Returns true if the field was
// changed.
func lateInitialize(msDelegationName **string, rec *ibclient.RecordNS) bool {
	if *msDelegationName == nil && rec.MsDelegationName != nil && *rec.MsDelegationName != "" {
		v := *rec.MsDelegationName
		*msDelegationName = &v
		return true
	}
	return false
}

// nsCloudInfo and nsCloudInfoDelegatedMember are scope-agnostic mirrors of
// the generated NSRecordCloudInfo / NSRecordCloudInfoDelegatedMember types,
// used the same way nsRecordAddress is above.
type nsCloudInfo struct {
	DelegatedMember *nsCloudInfoDelegatedMember
	DelegatedScope  *string
	DelegatedRoot   *string
	OwnedByAdaptor  *bool
	Usage           *string
	Tenant          *string
	MgmtPlatform    *string
	AuthorityType   *string
}

type nsCloudInfoDelegatedMember struct {
	Ipv4Addr *string
	Ipv6Addr *string
	Name     *string
}

// observedNSRecord holds the primitive field values extracted from a WAPI
// RecordNS response. The cluster and namespaced NSRecordObservation types
// are structurally similar but are distinct named types with distinct
// nested-struct field types (e.g. *NSRecordCloudInfo), so they are not
// directly convertible — each scope copies this intermediate struct's
// fields into its own Observation type at the call site.
type observedNSRecord struct {
	ID               string
	Name             *string
	Nameserver       *string
	View             *string
	Addresses        []nsRecordAddress
	MsDelegationName *string
	Ref              *string
	Zone             *string
	Creator          *string
	DNSName          *string
	LastQueried      *int64
	CloudInfo        *nsCloudInfo
	Policy           *string
}

// observeFromRecordNS extracts every field mirrored by
// NSRecordObservation (the full-mirror AtProvider convention) from a WAPI
// RecordNS response. NewEmptyRecordNS (see the SDK's
// object_manager_ns_record.go) requests every field this provider
// surfaces, so all of the following are populated whenever the API sets
// them.
func observeFromRecordNS(externalID string, rec *ibclient.RecordNS) observedNSRecord {
	o := observedNSRecord{
		ID:         externalID,
		Nameserver: rec.Nameserver,
		Addresses:  addressesFromZoneNameServers(rec.Addresses),
	}
	if rec.Name != "" {
		v := rec.Name
		o.Name = &v
	}
	if rec.View != "" {
		v := rec.View
		o.View = &v
	}
	if rec.MsDelegationName != nil && *rec.MsDelegationName != "" {
		v := *rec.MsDelegationName
		o.MsDelegationName = &v
	}
	if rec.Ref != "" {
		v := rec.Ref
		o.Ref = &v
	}
	if rec.Zone != "" {
		v := rec.Zone
		o.Zone = &v
	}
	if rec.Creator != "" {
		v := rec.Creator
		o.Creator = &v
	}
	if rec.DnsName != "" {
		v := rec.DnsName
		o.DNSName = &v
	}
	if rec.LastQueried != nil {
		v := rec.LastQueried.Unix()
		o.LastQueried = &v
	}
	if rec.Policy != "" {
		v := rec.Policy
		o.Policy = &v
	}
	o.CloudInfo = observeCloudInfo(rec.CloudInfo)
	return o
}

// observeCloudInfoDelegatedMember converts the SDK's Dhcpmember (the
// concrete type behind GridCloudapiInfo.DelegatedMember) into the shared
// nsCloudInfoDelegatedMember mirror. Split out of observeCloudInfo to
// keep both functions under the cyclomatic-complexity limit.
func observeCloudInfoDelegatedMember(dm *ibclient.Dhcpmember) *nsCloudInfoDelegatedMember {
	if dm == nil {
		return nil
	}
	out := &nsCloudInfoDelegatedMember{}
	if dm.Ipv4Addr != "" {
		v := dm.Ipv4Addr
		out.Ipv4Addr = &v
	}
	if dm.Ipv6Addr != "" {
		v := dm.Ipv6Addr
		out.Ipv6Addr = &v
	}
	if dm.Name != "" {
		v := dm.Name
		out.Name = &v
	}
	return out
}

// observeCloudInfo converts a WAPI GridCloudapiInfo response into the
// shared nsCloudInfo mirror. Split out of observeFromRecordNS to keep
// both functions under the cyclomatic-complexity limit.
func observeCloudInfo(ci *ibclient.GridCloudapiInfo) *nsCloudInfo {
	if ci == nil {
		return nil
	}
	out := &nsCloudInfo{
		DelegatedMember: observeCloudInfoDelegatedMember(ci.DelegatedMember),
		OwnedByAdaptor:  &ci.OwnedByAdaptor,
	}
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
	return out
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createNSRecord issues the WAPI create call. name, nameserver, and view
// are all required by WAPI on create; addresses is also required
// (live-verified — the SDK's CreateNSRecord rejects an empty
// addresses list).
func createNSRecord(objMgr ibclient.IBObjectManager, name, nameserver, view, msDelegationName *string, addresses []nsRecordAddress) (*ibclient.RecordNS, error) {
	return objMgr.CreateNSRecord(
		strOrEmpty(name),
		strOrEmpty(nameserver),
		strOrEmpty(view),
		buildAddresses(addresses),
		strOrEmpty(msDelegationName),
	)
}

// updateNSRecord issues the WAPI update call carrying only the mutable
// fields (nameserver, addresses, msDelegationName). name and view are
// intentionally passed as empty strings — the SDK's NewRecordNS helper
// (called internally by UpdateNSRecord) tags both fields `omitempty`, so
// an empty value drops them from the outgoing JSON entirely rather than
// echoing the current (unchanged) value. Live verification against a
// real Grid confirmed WAPI rejects the PUT outright if either field is present, so
// they must never appear in the request body — not even unchanged.
// Update semantics for record:ns are partial/merge (fields omitted from
// the body keep their existing server-side value), so this is safe.
func updateNSRecord(objMgr ibclient.IBObjectManager, ref string, nameserver, msDelegationName *string, addresses []nsRecordAddress) (*ibclient.RecordNS, error) {
	return objMgr.UpdateNSRecord(
		ref,
		"", // name — immutable, must not be sent (see doc comment above)
		strOrEmpty(nameserver),
		"", // dnsView — immutable, must not be sent (see doc comment above)
		buildAddresses(addresses),
		strOrEmpty(msDelegationName),
	)
}

// deleteNSRecord issues the WAPI delete call.
func deleteNSRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteNSRecord(ref)
	return err
}

// ── Natural-key identity resolution ─────────────────────────────────────
//
// See the package doc for why a natural-key match is sound evidence for
// NSRecord specifically (unlike the general case the staleref package
// doc describes): name, view, and nameserver are all immutable, and
// together they are exactly the tuple WAPI computes the object's _ref
// from, so a match on all three cannot be a different object that merely
// looks the same right now.

// AmbiguousNaturalKeyMatchError is returned when a natural-key search for
// an NSRecord's (name, view, nameserver) tuple matches more than one Grid
// object. WAPI's own addressing scheme should make this impossible — two
// objects computing the same _ref cannot both exist — but
// resolveNSRecordByNaturalKey verifies the match count rather than
// assuming that guarantee holds for every Grid state, and refuses on
// anything but exactly one match. This mirrors the posture
// identity.AmbiguousMatchError enforces for the UID-in-EA ladder used by
// every other rotating-handle resource in this provider: taking [0] from
// more than one match is exactly the defect that posture exists to
// prevent. Match with errors.As, e.g.:
//
//	var ambiguous *recordns.AmbiguousNaturalKeyMatchError
//	if errors.As(err, &ambiguous) { ... }
type AmbiguousNaturalKeyMatchError struct {
	Name       string
	View       string
	Nameserver string
	// Count is the number of objects the search matched (always > 1).
	Count int
}

func (e *AmbiguousNaturalKeyMatchError) Error() string {
	return fmt.Sprintf(
		"refusing to resolve NSRecord: %d Grid objects match name=%q view=%q nameserver=%q — "+
			"this tuple is expected to be unique (it is exactly what WAPI computes the object's _ref "+
			"from), so this indicates a Grid state this provider cannot safely resolve automatically; "+
			"an operator should reconcile the duplicate objects on the Grid directly",
		e.Count, e.Name, e.View, e.Nameserver,
	)
}

// resolveNSRecordByNaturalKey searches the Grid for a live NSRecord
// matching the CR's own (name, view, nameserver) tuple and returns it
// when exactly one object matches. Used by both resolveNSRecordForObserve
// (stale-ref recovery and the create-crash window, where there has never
// been a ref to try) and deleteNSRecordResolving404 (stale-ref recovery
// before concluding an object is gone).
//
// This issues the search itself through the raw connector with all three
// fields as server-side filters and answers from the match count — it
// does NOT go through the SDK's GetAllRecordNS. That high-level helper
// wraps a failed search with a plain "%s"-formatted error ("failed
// getting NS Record: <cause>"), which discards the underlying
// *ibclient.NotFoundError's type. A genuinely empty result set is exactly
// how the real WAPI answers a search that matches nothing, so on the live
// Grid GetAllRecordNS turns "the object is confirmed gone" into an
// unrecognized error that isNotFound cannot classify — callers would then
// treat the search as failed rather than as a clean absence, and the
// managed resource could never be deleted. Going through the connector
// directly keeps the *ibclient.NotFoundError's type intact so isNotFound
// classifies it correctly. All three fields are required non-empty; when
// any is missing there is no way to search for the object, so the search
// is skipped (nil, nil — treated as "not found") rather than treated as
// an error.
func resolveNSRecordByNaturalKey(conn ibclient.IBConnector, name, view, nameserver *string) (*ibclient.RecordNS, error) {
	if strOrEmpty(name) == "" || strOrEmpty(view) == "" || strOrEmpty(nameserver) == "" {
		return nil, nil
	}
	sf := map[string]string{
		"name":       strOrEmpty(name),
		"view":       strOrEmpty(view),
		"nameserver": strOrEmpty(nameserver),
	}
	var res []ibclient.RecordNS
	err := conn.GetObject(ibclient.NewEmptyRecordNS(), "", ibclient.NewQueryParams(false, sf), &res)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	switch len(res) {
	case 0:
		return nil, nil
	case 1:
		return &res[0], nil
	default:
		return nil, &AmbiguousNaturalKeyMatchError{
			Name:       strOrEmpty(name),
			View:       strOrEmpty(view),
			Nameserver: strOrEmpty(nameserver),
			Count:      len(res),
		}
	}
}

// observeRefFor derives the reference Observe() should try first. When
// the external-name annotation still holds the framework's
// NameAsExternalName default (the CR's own Kubernetes name), no real WAPI
// _ref has ever been assigned, so this reports "" rather than handing
// resolveNSRecordForObserve a value that can only ever 404 (or, worse,
// hit the no-_ref search endpoint with an unrelated path segment). An
// empty ref skips the GET entirely and goes straight to the natural-key
// search — which is also exactly what recovers the create-crash window: a
// Create() that reached the Grid but crashed before the runtime persisted
// the external-name annotation leaves the CR in this same state.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// nsRecordObserveResult bundles a resolved NSRecord for Observe with
// whether its reference was recovered through the natural-key search
// rather than resolved directly from the stored ref. A recovered
// reference must be persisted through a path crossplane-runtime actually
// writes back to the API server — ResourceLateInitialized, since Observe
// itself has no other conflict-safe way to persist an annotation change
// (see the externalname package doc for why Update alone cannot be
// trusted for this).
type nsRecordObserveResult struct {
	rec       *ibclient.RecordNS
	refreshed bool
}

// resolveNSRecordForObserve resolves the NSRecord Observe() reports on:
// it tries ref first (skipped entirely when ref is empty — see
// observeRefFor), then falls back to the natural-key search on a 404. A
// nil result with a nil error means the object is genuinely absent.
func resolveNSRecordForObserve(objMgr ibclient.IBObjectManager, conn ibclient.IBConnector, ref string, name, view, nameserver *string) (*nsRecordObserveResult, error) {
	if ref != "" {
		rec, err := objMgr.GetNSRecordByRef(ref)
		if err == nil {
			return &nsRecordObserveResult{rec: rec}, nil
		}
		if !isNotFound(err) {
			return nil, err
		}
		// The stored ref 404'd — fall through to the natural-key search.
	}

	rec, err := resolveNSRecordByNaturalKey(conn, name, view, nameserver)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	return &nsRecordObserveResult{rec: rec, refreshed: true}, nil
}

// deleteNSRecordResolving404 issues the WAPI delete and, on a 404 against
// the stored _ref, resolves the object's natural key before concluding
// it is gone. A 404 on a derived handle is evidence the handle rotated,
// not evidence the object was removed. Once resolved, exactly one match
// is provably the same object (see the package doc), so it is deleted
// under its current ref rather than refused — the ownership-unverifiable
// refusal earlier revisions of this function applied to any natural-key
// hit no longer applies once name, view, and nameserver are all
// immutable; an ambiguous (more than one) match still refuses, since the
// tuple's expected uniqueness is exactly what would be violated for that
// to happen.
func deleteNSRecordResolving404(objMgr ibclient.IBObjectManager, conn ibclient.IBConnector, ref string, name, view, nameserver *string) error {
	delErr := deleteNSRecord(objMgr, ref)
	if delErr == nil {
		return nil
	}
	if !isNotFound(delErr) {
		return errors.Wrap(delErr, errDeleteNSRecord)
	}

	rec, searchErr := resolveNSRecordByNaturalKey(conn, name, view, nameserver)
	if searchErr != nil {
		// A resolution failure (including AmbiguousNaturalKeyMatchError)
		// must never be reported as a successful delete — fail closed.
		return errors.Wrap(searchErr, errDeleteNSRecord)
	}
	if rec == nil {
		// Genuinely absent: the stale ref recovered nothing.
		return nil
	}

	if delErr := deleteNSRecord(objMgr, rec.Ref); delErr != nil {
		if isNotFound(delErr) {
			// Raced with another deleter between the search and this
			// call — the end state (gone) is what Delete() promises.
			return nil
		}
		return errors.Wrap(delErr, errDeleteNSRecord)
	}
	return nil
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced NSRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterNSRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster NSRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("NSRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedNSRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced NSRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("NSRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced NSRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterNSRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedNSRecord(mgr, o)
}
