// Package recorda implements the Crossplane controller for the Infoblox
// NIOS ARecord managed resource. Unlike the typical REST-envelope
// providers in this factory, this provider wraps the official
// infoblox-go-client Go SDK directly — the SDK's ObjectManager exposes
// typed CRUD methods (CreateARecord/GetARecordByRef/UpdateARecord/
// DeleteARecord) instead of a generic HTTP request/response envelope, so
// there is no internal REST client to compose.
//
// ARecord is the pilot resource for the UID-in-EA object-identity ladder
// (the object identity decision this provider's design history records):
// the WAPI _ref every create call returns is a derived handle — a
// rendering of the object's own identity fields, not a stable
// backend-assigned ID — so this controller stamps the managed resource's
// own metadata.uid onto the Grid object as an extensible attribute and
// resolves every Observe/Delete through the shared identity.Resolve
// ladder instead of trusting the stored _ref alone. See
// resolveARecordIdentity and the internal/clients/identity package doc
// for the full rationale.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recorda

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recorda/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recorda/v1alpha1"
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
	errObserveARecord            = "cannot observe ARecord"
	errCreateARecord             = "cannot create ARecord"
	errUpdateARecord             = "cannot update ARecord"
	errDeleteARecord             = "cannot delete ARecord"
	errCidrIPv4Mutex             = "cidr and ipv4Addr are mutually exclusive"
	errEmptyUID                  = "cannot stamp ARecord identity: managed resource's metadata.uid is empty"
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

// newObjectManager constructs an authenticated
// identity.ManagerAndConnector from the given credentials — the SDK's
// high-level ObjectManager for the ordinary CRUD calls, and the
// lower-level Connector the identity ladder needs directly (it operates
// below ObjectManager's typed methods so it can see search match
// counts). The Connector performs HTTP Basic Auth on every request and
// only validates configuration locally — no network round-trip happens
// until the first Observe/Create/Update/Delete call. sslVerify comes
// from the ProviderConfig's own spec field (not the credentials Secret)
// — see the Connect methods in cluster.go/namespaced.go.
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

// ttlOrZero converts an optional *uint32 TTL into the uint32 the SDK
// expects, treating nil as 0.
func ttlOrZero(ttl *uint32) uint32 {
	if ttl == nil {
		return 0
	}
	return *ttl
}

func boolOrFalse(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func boolPtrOrFalse(b *bool) bool {
	return boolOrFalse(b)
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
// than a whole ARecordParameters value. The cluster and namespaced
// ARecordParameters types are structurally identical (same field names and
// primitive types) but are distinct named Go types, so a direct struct
// conversion between them is not always available once other resources in
// this provider grow reference fields; parameterizing on the field
// pointers instead lets both scopes share this logic unconditionally.

// isUpToDate compares the desired ARecord fields against the observed
// RecordA. View is immutable (WAPI ties the object's _ref to view+name;
// the UpdateARecord SDK method has no view parameter) and
// RemoveAssociatedPtr is write-only (delete-time option, never present in
// a GET response) — both are intentionally excluded from this comparison.
// The Grid's extattrs map is compared with the provider's own identity
// stamp stripped out (identity.Strip): the CRD schema never includes that
// reserved key, so leaving it in would produce a permanent phantom diff.
func isUpToDate(name, ipv4Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordA) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(ipv4Addr) != strOrEmpty(rec.Ipv4Addr) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useTTL) != boolPtrOrFalse(rec.UseTtl) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted ttl and returns the zone default on every
	// GET — comparing it against the spec value never converges.
	if boolOrFalse(useTTL) {
		if ttlOrZero(ttl) != ttlOrZero(rec.Ttl) {
			return false
		}
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rec.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// ttl, useTtl, extAttrs) from the observed RecordA into spec so isUpToDate
// does not see phantom drift on the next reconcile. Required fields
// (name, ipv4Addr) and the immutable view field are never
// late-initialized — view is always user-supplied (required on the CRD)
// and name/ipv4Addr are always user-supplied too. extAttrs is
// back-filled with the provider's own identity stamp stripped out
// (identity.Strip) — the CRD schema never includes that reserved key, so
// late-initializing it into spec.forProvider would fail CEL validation
// and produce a permanent diff. Returns true if any field was changed.
func lateInitialize(comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordA) bool {
	changed := false

	if *comment == nil && rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		*comment = &c
		changed = true
	}
	if *useTTL == nil && rec.UseTtl != nil {
		u := *rec.UseTtl
		*useTTL = &u
		changed = true
	}
	// Only back-fill ttl when useTtl is on (post-backfill value above).
	// When it is off, the observed ttl is WAPI's zone default, not a
	// value implied by the user's config — writing it into spec would
	// silently claim a TTL that is not in effect.
	if *ttl == nil && rec.Ttl != nil && boolOrFalse(*useTTL) {
		t := *rec.Ttl
		*ttl = &t
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromRec := extAttrsFromEA(identity.Strip(rec.Ea)); len(fromRec) > 0 {
			*extAttrs = fromRec
			changed = true
		}
	}

	return changed
}

// observedARecord holds the primitive field values extracted from a WAPI
// RecordA response. The cluster and namespaced ARecordObservation types
// are structurally similar but are distinct named types with distinct
// nested-struct field types (e.g. *ARecordCloudInfo), so they are not
// directly convertible — each scope copies this intermediate struct's
// fields into its own Observation type at the call site.
type observedARecord struct {
	ID       string
	Name     *string
	IPv4Addr *string
	Comment  *string
	TTL      *uint32
	UseTTL   *bool
	ExtAttrs map[string]string
	View     *string
	Ref      *string
	Zone     *string
}

// observeFromRecordA extracts the fields mirrored by ARecordObservation
// (the full-mirror AtProvider convention) from a WAPI RecordA response.
// ExtAttrs intentionally mirrors the Grid's complete extattrs map,
// including the provider's own identity stamp — unlike isUpToDate and
// lateInitialize, AtProvider is a read-only status mirror, not compared
// against spec.forProvider, so surfacing the stamp there is informative
// rather than a source of phantom drift. The SDK's GetARecordByRef method
// requests only a fixed subset of fields by default (extattrs, ipv4addr,
// name, view, zone, comment, ttl, use_ttl); response-only fields outside
// that set (creator, discovered_data, cloud_info, etc.) are not requested
// by this provider and are left at their zero value in AtProvider.
func observeFromRecordA(externalID string, rec *ibclient.RecordA) observedARecord {
	o := observedARecord{
		ID:       externalID,
		Name:     rec.Name,
		IPv4Addr: rec.Ipv4Addr,
		ExtAttrs: extAttrsFromEA(rec.Ea),
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Ttl != nil {
		t := *rec.Ttl
		o.TTL = &t
	}
	if rec.UseTtl != nil {
		u := *rec.UseTtl
		o.UseTTL = &u
	}
	if rec.View != "" {
		v := rec.View
		o.View = &v
	}
	if rec.Ref != "" {
		r := rec.Ref
		o.Ref = &r
	}
	if rec.Zone != "" {
		z := rec.Zone
		o.Zone = &z
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// validateARecordCreateInputs performs the local, network-free sanity
// checks a create call requires: uid must be set (identity.Stamp cannot
// stamp an empty value), and cidr/ipv4Addr are mutually exclusive. Both
// Create() (before it probes the identity prerequisite — a purely local
// validation failure must never cost a network round-trip) and
// createARecord (for callers that invoke it directly) run this check
// first.
func validateARecordCreateInputs(ipv4Addr, cidr *string, uid string) error {
	if strings.TrimSpace(uid) == "" {
		return errors.New(errEmptyUID)
	}
	if strOrEmpty(cidr) != "" && strOrEmpty(ipv4Addr) != "" {
		return errors.New(errCidrIPv4Mutex)
	}
	return nil
}

// createARecord issues the WAPI create call, stamping the owning managed
// resource's uid into the object's extensible attributes in the same
// request that creates it (identity.Stamp) — there is no follow-up call,
// so there is no window in which the object exists without its identity
// stamp. When cidr is set, the WAPI dynamically allocates the next
// available IPv4 address from the given network view
// (func:nextavailableip) instead of using a caller-supplied static
// address — cidr and ipv4Addr are mutually exclusive, enforced below
// before the SDK call is issued. CreateARecord (unlike
// CreateAAAARecord/CreatePTRRecord) does not default an empty network
// view to "default" itself, so this wrapper applies that default
// explicitly for consistent behavior across all three next-available-IP
// record types.
func createARecord(objMgr ibclient.IBObjectManager, name, view, ipv4Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, cidr, networkView *string, uid string) (*ibclient.RecordA, error) {
	if err := validateARecordCreateInputs(ipv4Addr, cidr, uid); err != nil {
		return nil, err
	}

	cidrVal := strOrEmpty(cidr)
	netView := strOrEmpty(networkView)
	if cidrVal != "" && netView == "" {
		netView = "default"
	}

	ea := identity.Stamp(buildEA(extAttrs), uid)

	return objMgr.CreateARecord(
		netView,
		strOrEmpty(view),
		strOrEmpty(name),
		cidrVal,
		strOrEmpty(ipv4Addr),
		ttlOrZero(ttl),
		boolOrFalse(useTTL),
		strOrEmpty(comment),
		ea,
	)
}

// updateARecord issues the WAPI update call. view is intentionally never
// passed — UpdateARecord has no view parameter (immutable field). cidr and
// netView are always empty, mirroring createARecord.
//
// Every call re-asserts the identity stamp (identity.Stamp) in the
// extattrs it sends. Live verification against a real NIOS Grid Manager
// confirmed that a PUT carrying an extattrs object *replaces* the whole
// map — it is not a per-key merge — so omitting the stamp here would wipe
// it off the object on the very first field update after create.
func updateARecord(objMgr ibclient.IBObjectManager, ref string, name, ipv4Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, uid string) (*ibclient.RecordA, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}

	ea := identity.Stamp(buildEA(extAttrs), uid)

	return objMgr.UpdateARecord(
		ref,
		strOrEmpty(name),
		strOrEmpty(ipv4Addr),
		"", // cidr — not exposed by this provider
		"", // netView — not exposed by this provider
		ttlOrZero(ttl),
		boolOrFalse(useTTL),
		strOrEmpty(comment),
		ea,
	)
}

// deleteARecord issues the WAPI delete call. removeAssociatedPtr is
// accepted as a ForProvider field for schema completeness, but the SDK's
// DeleteARecord wrapper takes only the object reference — it exposes no
// query-parameter or request-body hook for the WAPI remove_associated_ptr
// delete option, so this provider cannot honor a user-set value. NIOS's
// default PTR-cleanup behavior applies on every delete regardless.
func deleteARecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteARecord(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────
//
// The "Crossplane Internal ID" extensible-attribute definition is an
// install prerequisite for every resource that stamps or resolves
// identity through it. Wired into Create() (before the identity
// stamp), and into observeARecord/deleteARecordIdentity's identity-EA
// search fallback (see the doc on those functions for why the guard is
// reactive — only exercised on the search's own failure — instead of an
// unconditional probe on every call). Not wired into Connect(), which
// must stay network-lazy — see newObjectManager's doc.

// ensureIdentityPrerequisite probes the Grid for the identity extensible
// attribute definition before any call that stamps identity onto a new
// object (identity.Stamp). A *identity.PrerequisiteError is returned
// verbatim — its Error() text is the operator-facing remediation, naming
// the exact WAPI call an administrator should run — so the caller's
// Synced=False condition carries it directly. Any other error (a
// transient failure probing or creating the definition) is wrapped like
// any other WAPI error and is retriable.
//
// prober defaults to identity.DefaultProber when nil — the production
// path always shares one Prober across every controller so the TTL
// round-trip budget applies provider-wide (see identity.DefaultProber's
// doc); tests that need an isolated verdict cache, instead of sharing
// state with every other test in this binary, inject their own
// identity.NewProber() instance. endpoint similarly falls back to
// unresolvedProbeEndpoint only when the caller was built without a
// resolved Grid host — see that constant's doc.
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
//
// WAPI's only object handle for this type (_ref) is a rendering of the
// object's own mutable identity fields (name, view), so it rotates
// whenever they change — a bare 404 on the stored handle is therefore not
// proof the object is gone. This replaces the resource's original
// natural-key delete refusal with the UID-in-EA ladder implemented once
// in internal/clients/identity and reused by every rotating-identifier
// NIOS type. ARecord is the pilot wiring.
//
// The identity-EA search (searchByUID inside identity.Resolve, hit
// whenever the stored reference is empty or has 404'd — both in
// observeARecord and in deleteARecordIdentity, since both resolve through
// the same ladder) queries WAPI by "*Crossplane Internal ID=<uid>". Live
// verification against a real Grid confirmed this fails exactly like the
// Create-time stamp does when the definition is absent — WAPI returns
// HTTP 400 ("AdmConProtoError: Unknown extensible attribute: Crossplane
// Internal ID"), not an empty result set — so an unguarded search would
// surface that opaque 400 instead of the operator remediation text.
//
// Unlike Create (which always stamps identity and so always needs the
// prerequisite before it runs), the search is only reached on the
// fallback path — the steady-state case (a stored reference that still
// resolves) never touches it. Gating every observeARecord/
// deleteARecordIdentity call on the probe would tax that steady-state
// path with an extra WAPI round-trip it does not need — the Prober's TTL
// cache amortizes this to at most one extra call per endpoint per cache
// window, but "at most one per window" is still more than "zero" for the
// common case. Both functions instead probe reactively: only when
// resolveARecordIdentity itself returns an error, distinguishing "the
// search failed because the prerequisite is missing" (surface
// *identity.PrerequisiteError verbatim) from any other resolution failure
// (HandleReuseError, AmbiguousMatchError, a transient WAPI error —
// propagate unchanged). A successful resolution, including the ordinary
// "reference resolves directly" and "search finds nothing" cases, never
// calls the probe at all.

// observeRefFor derives the reference the identity ladder should attempt
// first for a managed resource's stored external-name. When the
// annotation still holds the framework's NameAsExternalName default (the
// CR's own Kubernetes name) no real WAPI _ref has ever been assigned, so
// this reports "" rather than handing the ladder a value that can never
// resolve — Resolve treats "" as "search by identity attribute only",
// which is exactly the create-crash-window recovery path (see
// resolveARecordIdentity).
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveARecordIdentity resolves the ARecord identified by ref/uid
// through the shared UID-in-EA ladder: the stored reference is trusted
// only after its stamped identity attribute is confirmed to match uid; a
// stale or absent reference falls back to a search by that stamp, which
// is also what locates the object when ref is empty. One generic
// implementation (identity.Resolve) replaces the bespoke natural-key
// fallback this resource previously used, closing the ownership-
// verification gap that made natural-key adoption unsafe.
func resolveARecordIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.RecordA, identity.Outcome, error) {
	return identity.Resolve[*ibclient.RecordA](ctx, conn, ibclient.NewEmptyRecordA, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting an
// ARecord through the identity ladder during Observe — common to both
// scopes, which differ only in their concrete CRD types.
type observeResult struct {
	// exists reports whether the object was found by the ladder (any
	// outcome other than NotFound).
	exists bool
	// rec is the resolved WAPI object. Only valid when exists is true.
	rec *ibclient.RecordA
	// obs is the field snapshot extracted from rec for AtProvider. Only
	// valid when exists is true.
	obs observedARecord
	// lateInit reports whether the caller must set
	// ExternalObservation.ResourceLateInitialized — either because
	// lateInitialize back-filled an optional field, or because the ref
	// rotated/was recovered and the refreshed value must be persisted
	// through a path crossplane-runtime actually writes back to the API
	// server (see the externalname package doc for why Update alone
	// cannot be trusted for this).
	lateInit bool
	// refreshedRef is non-empty when the resolved object's _ref differs
	// from the caller's original external-name and must be written back
	// onto the managed resource (identity.OutcomeRotated or
	// OutcomeFoundByUID).
	refreshedRef string
	// adopted reports whether the object resolved by reference but
	// carried no identity stamp (identity.OutcomeAdopted). The caller
	// must not report the resource up to date in this case even when
	// every other field already matches — that would never trigger the
	// Update call that stamps the identity attribute (see
	// updateARecord), leaving the object permanently unmanageable by the
	// ladder should its _ref ever rotate.
	adopted bool
}

// observeARecord runs the identity ladder for Observe and late-initializes
// the given ForProvider field pointers from the resolved object. comment,
// ttl, useTTL and extAttrs are pointers to the caller's
// spec.forProvider fields, mutated in place exactly like lateInitialize.
//
// prober/endpoint are the caller's identity-prerequisite-probe cache
// handle (see ensureIdentityPrerequisite) — used only reactively, and
// only when the failure is identifiably from the identity-EA search step
// (identity.IsSearchFailure). See the "Identity resolution" doc above
// this function for why the guard does not run unconditionally, and why
// it must not fire on every resolution error indiscriminately: doing so
// would call the probe against servers/scenarios that were never set up
// to answer it (an unrelated ref-GET failure, or one of the identity
// ladder's own typed refusals), and since the identity-prerequisite
// verdict is cached by endpoint for several minutes of wall-clock time, a
// single mismatched probe call there would poison every later Observe
// sharing that cache key for the rest of that window.
func observeARecord(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveARecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			// The identity-EA search itself failed — this is the one
			// failure mode the missing-EA-definition 400 described above
			// produces. Probing here, only for this specific cause,
			// distinguishes it from a ref-GET failure or a typed
			// resolution refusal, neither of which the probe has
			// anything useful to say about.
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
		rec:     rec,
		obs:     observeFromRecordA(rec.Ref, rec),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(comment, ttl, useTTL, extAttrs, rec)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteARecordIdentity issues the WAPI delete for the ARecord this
// managed resource owns, resolving through the identity ladder first so a
// stale _ref is never mistaken for a deleted object. A bare 404 on the
// stored reference is not evidence the object is gone — it is evidence
// the reference is stale — so this always resolves ownership before
// concluding either way:
//
//   - the ladder finds nothing at all (by ref or by identity search):
//     the object is genuinely absent; report success.
//   - the ladder resolves an object whose identity attribute matches uid
//     (by ref, or by recovering a rotated/lost ref through search):
//     ownership is verified; delete it.
//   - the ladder resolves an object but cannot verify ownership (the
//     identity attribute is absent or held by a different uid): refuse.
//     Deleting is irreversible, so this is stricter than Observe's
//     adopt-and-re-stamp handling of the same "attribute absent" case —
//     observing continues managing an object leniently, but destroying
//     it requires proof.
//
// prober/endpoint are the caller's identity-prerequisite-probe cache
// handle, used the same reactive way observeARecord uses them: only when
// resolveARecordIdentity's own failure is identifiably from the
// identity-EA search step (identity.IsSearchFailure), so a delete
// whose reference still resolves — or whose failure is unrelated to the
// search — never pays for the extra round-trip or risks poisoning the
// shared probe cache with an unrelated verdict. A
// *identity.PrerequisiteError found this way is returned verbatim,
// unwrapped, matching Create's behavior — everything else stays wrapped
// with delete-time context.
func deleteARecordIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveARecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		// HandleReuseError, AmbiguousMatchError, or any other resolution
		// failure (including a transient WAPI error on the lookup
		// itself): refuse rather than guess. No mutating call is made.
		// Wrapped for delete-time context; errors.As still traverses the
		// wrap to match the typed refusal errors.
		return errors.Wrap(err, errDeleteARecord)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteARecord(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			// Deleted by someone else between resolution and this call —
			// still a successful delete from this reconcile's point of
			// view.
			return nil
		}
		return errors.Wrap(delErr, errDeleteARecord)
	default:
		return errors.New("identity: unresolved ARecord outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced ARecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterARecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster ARecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("ARecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedARecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced ARecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("ARecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced ARecord controllers
// immediately without SafeStart gating (RBAC fallback path, for
// environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterARecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedARecord(mgr, o)
}
