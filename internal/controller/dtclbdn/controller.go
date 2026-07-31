// Package dtclbdn implements the Crossplane controller for the Infoblox
// NIOS DTCLBDN managed resource (WAPI object type `dtc:lbdn`).
//
// The SDK's ObjectManager.CreateDtcLbdn/UpdateDtcLbdn convenience wrappers
// accept `pools []*DtcPoolLink` entries whose `Pool` field is expected to
// hold a pool *name* and `authZones []AuthZonesLink` entries shaped as
// {Fqdn, DnsView} pairs — both wrappers resolve the real WAPI `_ref` via
// an extra name/fqdn+view lookup HTTP round trip, and (live-verified,
// ADR-IN-0004) silently drop any pool entry whose `Pool` value already
// looks like a `_ref` (matches `^dtc:pool:`) instead of resolving it. This
// provider's DTCLBDN CRD stores pool and auth-zone references directly
// (the user supplies an existing DTCPool/ZoneAuth object's `_ref`,
// resolved ahead of time by the generated cross-resource reference
// resolver — see zz_generated.resolvers.go), so this package builds the
// DtcLbdn SDK struct itself and issues Create/Update through the
// lower-level ibclient.IBConnector returned alongside the ObjectManager —
// bypassing the name/fqdn-based lookups entirely, mirroring the precedent
// already established by the dtcpool and dtcserver packages for the same
// reason. Delete has no such mismatch, so it uses the ObjectManager's
// DeleteDtcLbdn convenience method directly.
//
// The WAPI `auto_consolidated_monitors` attribute does not exist on this
// deployment's Grid Manager (live-verified, ADR-IN-0004) — this package
// never sets it, and this CRD does not expose it as a field at all. The
// SDK's ObjectManager.GetDtcLbdnByRef convenience method (via
// NewEmptyDtcLbdn) unconditionally adds `auto_consolidated_monitors` to
// the WAPI `_return_fields` list on every GET, and this deployment's WAPI
// schema rejects the unrecognized field with a 400 — failing the GET
// itself, not just omitting that one field from the response. This
// package therefore never calls GetDtcLbdnByRef; getDtcLbdnByRef below
// issues the same GET through the low-level IBConnector with a trimmed
// return-fields list (see dtcLbdnReturnFields) that excludes only
// `auto_consolidated_monitors`.
//
// The DTCLBDN `_ref` is unstable: renaming the LBDN (a mutable field)
// changes its `_ref` (live-verified, ADR-IN-0004). Create and Update both
// read the `_ref` back from the WAPI response and refresh the
// crossplane.io/external-name annotation whenever it differs from the
// externalID used to issue the request.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package dtclbdn

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtclbdn/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtclbdn/v1alpha1"
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
	errObserveDTCLBDN   = "cannot observe DTCLBDN"
	errCreateDTCLBDN    = "cannot create DTCLBDN"
	errUpdateDTCLBDN    = "cannot update DTCLBDN"
	errDeleteDTCLBDN    = "cannot delete DTCLBDN"
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

// dtcLbdnClients bundles the two SDK handles this package needs: the
// high-level ObjectManager (used for the ref-only Delete call) and the
// lower-level IBConnector (used for Create/Update/Read, so this package
// can build the DtcLbdn struct itself and skip the ObjectManager's
// name/fqdn-based pool/auth-zone/topology lookups — see the package doc
// comment).
type dtcLbdnClients struct {
	objMgr ibclient.IBObjectManager
	conn   ibclient.IBConnector
}

// newClients constructs an authenticated ibclient.IBObjectManager plus the
// underlying IBConnector from the given credentials. The Connector performs
// HTTP Basic Auth on every request and only validates configuration
// locally — no network round-trip happens until the first
// Observe/Create/Update/Delete call.
func newClients(creds *nioCredentials, sslVerify bool) (*dtcLbdnClients, error) {
	return newClientsWithScheme(creds, sslVerify, "https", "443")
}

// newClientsWithScheme is the scheme/port-parameterized variant of
// newClients used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newClientsWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (*dtcLbdnClients, error) {
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

	return &dtcLbdnClients{
		objMgr: ibclient.NewObjectManager(conn, "", ""),
		conn:   conn,
	}, nil
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// poolLink is a scope-agnostic mirror of the CRD's DTCLBDNPoolLink struct
// (minus the reference/selector fields, which are resolved by the
// generated ResolveReferences method before Observe/Create/Update ever
// run — this package only ever sees the resolved Pool ref string).
type poolLink struct {
	Pool  *string
	Ratio *uint32
}

// buildPools converts the CRD's simplified pool-link slice into the SDK's
// []*ibclient.DtcPoolLink wire type.
func buildPools(pools []poolLink) []*ibclient.DtcPoolLink {
	if len(pools) == 0 {
		return nil
	}
	out := make([]*ibclient.DtcPoolLink, 0, len(pools))
	for _, p := range pools {
		out = append(out, &ibclient.DtcPoolLink{Pool: strOrEmpty(p.Pool), Ratio: uint32OrZero(p.Ratio)})
	}
	return out
}

// poolsFromSDK converts the SDK's []*ibclient.DtcPoolLink response into
// the CRD's simplified pool-link slice.
func poolsFromSDK(pools []*ibclient.DtcPoolLink) []poolLink {
	if len(pools) == 0 {
		return nil
	}
	out := make([]poolLink, 0, len(pools))
	for _, p := range pools {
		if p == nil {
			continue
		}
		var pool *string
		if p.Pool != "" {
			v := p.Pool
			pool = &v
		}
		var ratio *uint32
		if p.Ratio != 0 {
			v := p.Ratio
			ratio = &v
		}
		out = append(out, poolLink{Pool: pool, Ratio: ratio})
	}
	return out
}

// poolsEqual reports whether two pool-link slices are equivalent, treating
// a nil slice and an empty slice as equal.
func poolsEqual(a, b []poolLink) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Pool) != strOrEmpty(b[i].Pool) {
			return false
		}
		if uint32OrZero(a[i].Ratio) != uint32OrZero(b[i].Ratio) {
			return false
		}
	}
	return true
}

// buildAuthZones converts the CRD's simplified slice of bare ZoneAuth
// `_ref` strings into the SDK's []*ibclient.ZoneAuth wire type. The SDK's
// DtcLbdn.MarshalJSON unwraps each entry back down to its bare Ref string
// on the wire (WAPI requires `auth_zones` as an array of bare `_ref`
// strings, not objects — live-verified, ADR-IN-0004).
func buildAuthZones(refs []string) []*ibclient.ZoneAuth {
	if len(refs) == 0 {
		return nil
	}
	out := make([]*ibclient.ZoneAuth, 0, len(refs))
	for _, r := range refs {
		if r == "" {
			continue
		}
		out = append(out, &ibclient.ZoneAuth{Ref: r})
	}
	return out
}

// authZonesFromSDK converts the SDK's []*ibclient.ZoneAuth response
// (populated from the bare `_ref` strings by DtcLbdn.UnmarshalJSON) into
// the CRD's simplified slice of bare ref strings.
func authZonesFromSDK(zones []*ibclient.ZoneAuth) []string {
	if len(zones) == 0 {
		return nil
	}
	out := make([]string, 0, len(zones))
	for _, z := range zones {
		if z == nil || z.Ref == "" {
			continue
		}
		out = append(out, z.Ref)
	}
	return out
}

// dtcHealth is a scope-agnostic mirror of the CRD's DTCLBDNHealth struct,
// populated from the SDK's DtcHealth response type.
type dtcHealth struct {
	Availability *string
	Description  *string
	EnabledState *string
}

// healthFromSDK converts the SDK's *ibclient.DtcHealth response-only field
// into the scope-agnostic dtcHealth struct. Returns nil if the API omitted
// health (e.g. a Grid Manager that has not yet run a health check).
func healthFromSDK(h *ibclient.DtcHealth) *dtcHealth {
	if h == nil {
		return nil
	}
	out := &dtcHealth{}
	if h.Availability != "" {
		v := h.Availability
		out.Availability = &v
	}
	if h.Description != "" {
		v := h.Description
		out.Description = &v
	}
	if h.EnabledState != "" {
		v := h.EnabledState
		out.EnabledState = &v
	}
	return out
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

// stringSlicesEqual reports whether two string slices contain the same
// elements in the same order, treating a nil slice and an empty slice as
// equal.
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

func uint32OrZero(u *uint32) uint32 {
	if u == nil {
		return 0
	}
	return *u
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
// than a whole DTCLBDNParameters value. The cluster and namespaced
// DTCLBDNParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so parameterizing
// on the field pointers lets both scopes share this logic unconditionally.

// isUpToDate compares the desired DTCLBDN fields against the observed
// DtcLbdn. There are no known immutable fields for DTCLBDN — every Create
// parameter is also accepted by Update — so all mutable fields
// participate in this comparison. Split into two helpers (scalar fields,
// collection fields) to keep cyclomatic complexity manageable.
func isUpToDate(name, lbMethod *string, patterns []string, pools []poolLink, authZones []string, types []string, priority, persistence *uint32, topology *string, ttl *uint32, useTTL *bool, comment *string, disable *bool, extAttrs map[string]string, rec *ibclient.DtcLbdn) bool {
	if !scalarFieldsUpToDate(name, lbMethod, priority, persistence, topology, ttl, useTTL, comment, disable, rec) {
		return false
	}
	return collectionFieldsUpToDate(patterns, pools, authZones, types, extAttrs, rec)
}

// scalarFieldsUpToDate compares the primitive-typed DTCLBDN fields.
func scalarFieldsUpToDate(name, lbMethod *string, priority, persistence *uint32, topology *string, ttl *uint32, useTTL *bool, comment *string, disable *bool, rec *ibclient.DtcLbdn) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(lbMethod) != rec.LbMethod {
		return false
	}
	if uint32OrZero(priority) != uint32OrZero(rec.Priority) {
		return false
	}
	if uint32OrZero(persistence) != uint32OrZero(rec.Persistence) {
		return false
	}
	if strOrEmpty(topology) != strOrEmpty(rec.Topology) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useTTL) != boolOrFalse(rec.UseTtl) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted ttl and returns the zone default on every
	// GET — comparing it against the spec value never converges.
	if boolOrFalse(useTTL) {
		if uint32OrZero(ttl) != uint32OrZero(rec.Ttl) {
			return false
		}
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	return boolOrFalse(disable) == boolOrFalse(rec.Disable)
}

// collectionFieldsUpToDate compares the slice/composite-typed DTCLBDN
// fields.
func collectionFieldsUpToDate(patterns []string, pools []poolLink, authZones []string, types []string, extAttrs map[string]string, rec *ibclient.DtcLbdn) bool {
	if !stringSlicesEqual(patterns, rec.Patterns) {
		return false
	}
	if !poolsEqual(pools, poolsFromSDK(rec.Pools)) {
		return false
	}
	if !stringSlicesEqual(authZones, authZonesFromSDK(rec.AuthZones)) {
		return false
	}
	if !stringSlicesEqual(types, rec.Types) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (priority,
// persistence, topology, ttl, useTtl, comment, disable, extattrs) from the
// observed DtcLbdn into spec so isUpToDate does not see phantom drift on
// the next reconcile. Required fields (name, lbMethod, patterns) and
// user-managed collections (pools, authZones, types) are never
// late-initialized. Returns true if any field was changed.
func lateInitialize(priority, persistence **uint32, topology **string, ttl **uint32, useTTL **bool, comment **string, disable **bool, extAttrs *map[string]string, rec *ibclient.DtcLbdn) bool {
	changed := lateInitUint32(priority, rec.Priority)
	changed = lateInitUint32(persistence, rec.Persistence) || changed
	changed = lateInitString(topology, rec.Topology) || changed
	changed = lateInitBool(useTTL, rec.UseTtl) || changed
	// Only back-fill ttl when useTtl is on (post-backfill value above).
	// When it is off, the observed ttl is WAPI's zone default, not a
	// value implied by the user's config — writing it into spec would
	// silently claim a TTL that is not in effect.
	if boolOrFalse(*useTTL) {
		changed = lateInitUint32(ttl, rec.Ttl) || changed
	}
	changed = lateInitString(comment, rec.Comment) || changed
	changed = lateInitBool(disable, rec.Disable) || changed
	changed = lateInitExtAttrs(extAttrs, rec.Ea) || changed
	return changed
}

// lateInitString back-fills a single optional *string spec field from the
// observed value, leaving user-supplied values untouched.
func lateInitString(field **string, observed *string) bool {
	if *field != nil || observed == nil || *observed == "" {
		return false
	}
	v := *observed
	*field = &v
	return true
}

// lateInitBool back-fills a single optional *bool spec field from the
// observed value, leaving user-supplied values untouched.
func lateInitBool(field **bool, observed *bool) bool {
	if *field != nil || observed == nil {
		return false
	}
	v := *observed
	*field = &v
	return true
}

// lateInitUint32 back-fills a single optional *uint32 spec field from the
// observed value, leaving user-supplied values untouched.
func lateInitUint32(field **uint32, observed *uint32) bool {
	if *field != nil || observed == nil {
		return false
	}
	v := *observed
	*field = &v
	return true
}

// lateInitExtAttrs back-fills the extensible-attributes map from the
// observed value when spec left it empty.
func lateInitExtAttrs(field *map[string]string, observed ibclient.EA) bool {
	if len(*field) != 0 {
		return false
	}
	fromRec := extAttrsFromEA(observed)
	if len(fromRec) == 0 {
		return false
	}
	*field = fromRec
	return true
}

// observedDTCLBDN holds the primitive field values extracted from a WAPI
// DtcLbdn response. The cluster and namespaced DTCLBDNObservation types
// are structurally similar but are distinct named types, so each scope
// copies this intermediate struct's fields into its own Observation type
// at the call site.
type observedDTCLBDN struct {
	ID          string
	Name        *string
	LBMethod    *string
	Patterns    []string
	Pools       []poolLink
	AuthZones   []string
	Types       []string
	Priority    *uint32
	Persistence *uint32
	Topology    *string
	TTL         *uint32
	UseTTL      *bool
	Comment     *string
	Disable     *bool
	ExtAttrs    map[string]string
	Ref         *string
	Health      *dtcHealth
}

// observeFromDtcLbdn extracts the fields mirrored by DTCLBDNObservation
// (the full-mirror AtProvider convention) from a WAPI DtcLbdn response.
func observeFromDtcLbdn(externalID string, rec *ibclient.DtcLbdn) observedDTCLBDN {
	o := observedDTCLBDN{
		ID:        externalID,
		Name:      rec.Name,
		Patterns:  rec.Patterns,
		Pools:     poolsFromSDK(rec.Pools),
		AuthZones: authZonesFromSDK(rec.AuthZones),
		Types:     rec.Types,
		ExtAttrs:  extAttrsFromEA(rec.Ea),
		Health:    healthFromSDK(rec.Health),
	}
	if rec.LbMethod != "" {
		v := rec.LbMethod
		o.LBMethod = &v
	}
	if rec.Priority != nil {
		v := *rec.Priority
		o.Priority = &v
	}
	if rec.Persistence != nil {
		v := *rec.Persistence
		o.Persistence = &v
	}
	if rec.Topology != nil && *rec.Topology != "" {
		v := *rec.Topology
		o.Topology = &v
	}
	if rec.Ttl != nil {
		v := *rec.Ttl
		o.TTL = &v
	}
	if rec.UseTtl != nil {
		v := *rec.UseTtl
		o.UseTTL = &v
	}
	if rec.Comment != nil && *rec.Comment != "" {
		v := *rec.Comment
		o.Comment = &v
	}
	if rec.Disable != nil {
		v := *rec.Disable
		o.Disable = &v
	}
	if rec.Ref != "" {
		v := rec.Ref
		o.Ref = &v
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// buildDtcLbdn constructs the ibclient.DtcLbdn struct sent on Create and
// Update. AutoConsolidatedMonitors is deliberately never set — see the
// package doc comment.
func buildDtcLbdn(name, lbMethod *string, patterns []string, pools []poolLink, authZones []string, types []string, priority, persistence *uint32, topology *string, ttl *uint32, useTTL *bool, comment *string, disable *bool, extAttrs map[string]string) *ibclient.DtcLbdn {
	return &ibclient.DtcLbdn{
		Name:        name,
		LbMethod:    strOrEmpty(lbMethod),
		Patterns:    patterns,
		Pools:       buildPools(pools),
		AuthZones:   buildAuthZones(authZones),
		Types:       types,
		Priority:    priority,
		Persistence: persistence,
		Topology:    topology,
		Ttl:         ttl,
		UseTtl:      useTTL,
		Comment:     comment,
		Disable:     disable,
		Ea:          buildEA(extAttrs),
	}
}

// createDtcLbdn issues the WAPI create call via the low-level IBConnector
// — see the package doc comment for why the ObjectManager wrapper cannot
// be used as-is for this resource's pools/authZones/topology fields.
func createDtcLbdn(conn ibclient.IBConnector, name, lbMethod *string, patterns []string, pools []poolLink, authZones []string, types []string, priority, persistence *uint32, topology *string, ttl *uint32, useTTL *bool, comment *string, disable *bool, extAttrs map[string]string) (*ibclient.DtcLbdn, error) {
	lbdn := buildDtcLbdn(name, lbMethod, patterns, pools, authZones, types, priority, persistence, topology, ttl, useTTL, comment, disable, extAttrs)
	ref, err := conn.CreateObject(lbdn)
	if err != nil {
		return nil, err
	}
	lbdn.Ref = ref
	return lbdn, nil
}

// updateDtcLbdn issues the WAPI update call. All fields are mutable —
// there are no known immutable fields for DTCLBDN — so every field is
// echoed on every update (this API uses PUT full-replace semantics).
func updateDtcLbdn(conn ibclient.IBConnector, ref string, name, lbMethod *string, patterns []string, pools []poolLink, authZones []string, types []string, priority, persistence *uint32, topology *string, ttl *uint32, useTTL *bool, comment *string, disable *bool, extAttrs map[string]string) (*ibclient.DtcLbdn, error) {
	lbdn := buildDtcLbdn(name, lbMethod, patterns, pools, authZones, types, priority, persistence, topology, ttl, useTTL, comment, disable, extAttrs)
	lbdn.Ref = ref
	refRes, err := conn.UpdateObject(lbdn, ref)
	if err != nil {
		return nil, err
	}
	lbdn.Ref = refRes
	return lbdn, nil
}

// deleteDtcLbdn issues the WAPI delete call.
func deleteDtcLbdn(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteDtcLbdn(ref)
	return err
}

// dtcLbdnReturnFields lists every DtcLbdn field this package reads on
// Observe, deliberately omitting `auto_consolidated_monitors`. That field
// does not exist on this deployment's Grid Manager WAPI schema
// (live-verified, ADR-IN-0004): the SDK's own read helper
// (ibclient.ObjectManager.GetDtcLbdnByRef, via NewEmptyDtcLbdn)
// unconditionally requests it, and the GET is rejected outright with a
// 400 "Unknown argument/field" error — not a per-field omission, the
// whole response never comes back. getDtcLbdnByRef below issues the same
// GET through the low-level IBConnector with this trimmed field list
// instead, mirroring the Create/Update bypass explained in the package
// doc comment.
var dtcLbdnReturnFields = []string{
	"extattrs", "disable", "auth_zones", "lb_method", "patterns", "persistence", "pools", "priority", "topology", "types", "health", "ttl", "use_ttl",
}

// getDtcLbdnByRef issues the WAPI GET call for the DTCLBDN identified by
// ref via the low-level IBConnector — see dtcLbdnReturnFields and the
// package doc comment for why the ObjectManager's GetDtcLbdnByRef
// convenience method cannot be used as-is for this deployment.
func getDtcLbdnByRef(conn ibclient.IBConnector, ref string) (*ibclient.DtcLbdn, error) {
	lbdn := &ibclient.DtcLbdn{}
	lbdn.SetReturnFields(append(lbdn.ReturnFields(), dtcLbdnReturnFields...))
	err := conn.GetObject(lbdn, ref, ibclient.NewQueryParams(false, nil), &lbdn)
	return lbdn, err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced DTCLBDN
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterDTCLBDN(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster DTCLBDN controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("DTCLBDN"))

	o.Gate.Register(func() {
		if err := setupNamespacedDTCLBDN(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced DTCLBDN controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("DTCLBDN"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced DTCLBDN
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterDTCLBDN(mgr, o); err != nil {
		return err
	}
	return setupNamespacedDTCLBDN(mgr, o)
}
