// Package recordns implements the Crossplane controller for the Infoblox
// NIOS NSRecord managed resource (WAPI object type `record:ns`). Like the
// other DNS-record controllers in this provider, it wraps the official
// infoblox-go-client Go SDK directly — the SDK's ObjectManager exposes
// typed CRUD methods (CreateNSRecord/GetNSRecordByRef/UpdateNSRecord/
// DeleteNSRecord) instead of a generic HTTP request/response envelope, so
// there is no internal REST client to compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordns

import (
	"context"
	"net/http"
	"regexp"
	"strconv"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordns/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordns/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
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
// sslVerify comes from the ProviderConfig's own spec field (not the
// credentials Secret) — see the Connect methods in cluster.go/namespaced.go.
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
		return nil, errors.Wrap(err, errNewObjectManager)
	}

	return ibclient.NewObjectManager(conn, "", ""), nil
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
// view are hard-immutable (live-verified ADR-IN-0004: `supports=rws`, no
// `u` — WAPI rejects a PUT that changes either field even though the Go
// SDK's UpdateNSRecord signature still accepts them) and zone is
// derived/response-only — all three are intentionally excluded from this
// comparison to avoid an infinite reconcile loop.
func isUpToDate(nameserver, msDelegationName *string, addresses []nsRecordAddress, rec *ibclient.RecordNS) bool {
	if strOrEmpty(nameserver) != strOrEmpty(rec.Nameserver) {
		return false
	}
	if strOrEmpty(msDelegationName) != strOrEmpty(rec.MsDelegationName) {
		return false
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
// (live-verified ADR-IN-0004 — the SDK's CreateNSRecord rejects an empty
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
// echoing the current (unchanged) value. Live verification (ADR-IN-0004)
// confirmed WAPI rejects the PUT outright if either field is present, so
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

// nsRecordExistsByNaturalKey reports whether a live NSRecord still exists
// under the CR's own (name, view) identity — the same tuple WAPI uses to
// compute the _ref (both fields are hard-immutable, see isUpToDate's doc
// comment). Used by Delete() when the stored _ref 404s: a hit here means
// the _ref is merely stale, not that the object is gone. NSRecord has no
// single-object natural-key getter, so this searches via
// GetAllRecordNS (a list call) with a server-side query filter instead.
// Both fields are required non-empty; when either is missing there is no
// way to re-discover the object, so the search is skipped (found=false)
// rather than treated as an error.
func nsRecordExistsByNaturalKey(objMgr ibclient.IBObjectManager, name, view *string) (bool, error) {
	if strOrEmpty(name) == "" || strOrEmpty(view) == "" {
		return false, nil
	}
	sf := map[string]string{
		"name": strOrEmpty(name),
		"view": strOrEmpty(view),
	}
	res, err := objMgr.GetAllRecordNS(ibclient.NewQueryParams(false, sf))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return len(res) > 0, nil
}

// deleteNSRecordResolving404 issues the WAPI delete and, on a 404 against
// the stored _ref, resolves the object's natural key before concluding
// it is gone. A 404 on a derived handle is evidence the handle rotated,
// not evidence the object was removed: if the natural-key search still
// finds a live NS record, deleting is refused because ownership of that
// record cannot be verified from the search alone (see the staleref
// package doc for the full rationale).
func deleteNSRecordResolving404(objMgr ibclient.IBObjectManager, ref string, name, view *string) error {
	delErr := deleteNSRecord(objMgr, ref)
	if delErr == nil {
		return nil
	}
	if !isNotFound(delErr) {
		return errors.Wrap(delErr, errDeleteNSRecord)
	}
	found, searchErr := nsRecordExistsByNaturalKey(objMgr, name, view)
	if searchErr != nil {
		return errors.Wrap(searchErr, errDeleteNSRecord)
	}
	if found {
		return staleref.RefusalError()
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
