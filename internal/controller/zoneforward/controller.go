// Package zoneforward implements the Crossplane controller for the
// Infoblox NIOS ZoneForward managed resource (WAPI `zone_forward` object
// type). Like the ARecord controller, this provider wraps the official
// infoblox-go-client Go SDK directly — the SDK's ObjectManager exposes
// typed CRUD methods (CreateZoneForward/GetZoneForwardByRef/
// UpdateZoneForward/DeleteZoneForward) instead of a generic HTTP
// request/response envelope, so there is no internal REST client to
// compose.
//
// ZoneForward is wired to the UID-in-EA object-identity ladder (see the
// recorda/ARecord controller, the pilot resource, and the
// internal/clients/identity package doc for the full rationale): the
// WAPI _ref every create call returns is a derived handle, not a stable
// backend-assigned ID, so this controller stamps the managed resource's
// own metadata.uid onto the Grid object as an extensible attribute and
// resolves every Observe/Delete through the shared identity.Resolve
// ladder instead of trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
//
// This provider has no shared internal/clients package — each resource
// controller package defines its own credential bridge (mirrors the
// ARecord/recorda controller package).
package zoneforward

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneforward/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneforward/v1alpha1"
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
	errObserveZoneForward        = "cannot observe ZoneForward"
	errCreateZoneForward         = "cannot create ZoneForward"
	errUpdateZoneForward         = "cannot update ZoneForward"
	errDeleteZoneForward         = "cannot delete ZoneForward"
	errEmptyUID                  = "cannot stamp ZoneForward identity: managed resource's metadata.uid is empty"
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

// newObjectManager constructs an authenticated
// identity.ManagerAndConnector from the given credentials — the SDK's
// high-level ObjectManager for the ordinary CRUD calls, and the
// lower-level Connector the identity ladder needs directly (it operates
// below ObjectManager's typed methods so it can see search match
// counts). The Connector performs HTTP Basic Auth on every request and
// only validates configuration locally — no network round-trip happens
// until the first Observe/Create/Update/Delete call.
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

// nameServersEqual compares two ordered lists of SDK NameServer values by
// their Name and Address fields — the only two fields the CRD's
// NameServer type exposes (SharedWithMsParentDelegation, Stealth,
// TsigKey* are response-only WAPI internals not surfaced by this
// provider). Order matters: forwardTo is a WAPI list, and the SDK/WAPI
// preserve list order on round-trip.
func nameServersEqual(a, b []ibclient.NameServer) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Address != b[i].Address {
			return false
		}
	}
	return true
}

// forwardingServersEqual compares two ordered lists of SDK
// Forwardingmemberserver values by the fields the CRD's ForwardingServer
// type exposes. Order matters, mirroring nameServersEqual.
func forwardingServersEqual(a, b []*ibclient.Forwardingmemberserver) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		av, bv := a[i], b[i]
		if av == nil || bv == nil {
			if av != bv {
				return false
			}
			continue
		}
		if av.Name != bv.Name {
			return false
		}
		if av.ForwardersOnly != bv.ForwardersOnly {
			return false
		}
		if av.UseOverrideForwarders != bv.UseOverrideForwarders {
			return false
		}
		if !nameServersEqual(av.ForwardTo.NameServers, bv.ForwardTo.NameServers) {
			return false
		}
	}
	return true
}

// nullableForwardingServersSlice unwraps a possibly-nil
// *ibclient.NullableForwardingServers into its underlying slice, treating
// a nil wrapper the same as an absent/empty override list.
func nullableForwardingServersSlice(n *ibclient.NullableForwardingServers) []*ibclient.Forwardingmemberserver {
	if n == nil {
		return nil
	}
	return n.Servers
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
// (plus the pre-converted SDK-shaped forwardTo/forwardingServers slices)
// rather than a whole ZoneForwardParameters value. The cluster and
// namespaced ZoneForwardParameters types are structurally identical (same
// field names and primitive types) but are distinct named Go types, and
// ForwardTo/ForwardingServers additionally nest distinct-per-scope struct
// types (NameServer, ForwardingServer) — so callers convert them to the
// SDK's shape at the call site (see cluster.go/namespaced.go) before
// invoking these shared helpers.
//
// fqdn, view, and zoneFormat are immutable (confirmed absent from the
// UpdateZoneForward SDK method signature) — they are intentionally
// excluded from isUpToDate.

// isUpToDate compares the desired mutable ZoneForward fields against the
// observed ZoneForward.
func isUpToDate(forwardTo []ibclient.NameServer, forwardingServers []*ibclient.Forwardingmemberserver, comment, nsGroup, externalNsGroup *string, disable, forwardersOnly *bool, extAttrs map[string]string, rec *ibclient.ZoneForward) bool {
	if !nameServersEqual(forwardTo, rec.ForwardTo.NameServers) {
		return false
	}
	if !forwardingServersEqual(forwardingServers, nullableForwardingServersSlice(rec.ForwardingServers)) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(rec.Disable) {
		return false
	}
	if boolOrFalse(forwardersOnly) != boolOrFalse(rec.ForwardersOnly) {
		return false
	}
	if strOrEmpty(nsGroup) != strOrEmpty(rec.NsGroup) {
		return false
	}
	if strOrEmpty(externalNsGroup) != strOrEmpty(rec.ExternalNsGroup) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rec.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// nsGroup, externalNsGroup, disable, forwardersOnly, extAttrs, view,
// zoneFormat) from the observed ZoneForward into spec so isUpToDate does
// not see phantom drift on the next reconcile. view and zoneFormat are
// immutable once set (excluded from isUpToDate above), but they are still
// server-defaulted when omitted at create time (WAPI defaults view to
// "default" and zoneFormat to "FORWARD") — recording the server-assigned
// value keeps spec truthful to what was actually applied. The required
// fields (fqdn, forwardTo) are never late-initialized — both are always
// user-supplied. forwardingServers is a list with a sensible empty
// default (no per-member overrides) and is intentionally not
// late-initialized — mirrors forwardTo/delegateTo precedent for list
// fields in this provider.
func lateInitialize(comment, nsGroup, externalNsGroup **string, disable, forwardersOnly **bool, extAttrs *map[string]string, view, zoneFormat **string, rec *ibclient.ZoneForward) bool {
	// Split across small helpers (rather than one long if-chain) to keep
	// cyclomatic complexity low per function — each helper covers a
	// handful of related fields.
	changed := lateInitializeStrings(comment, nsGroup, externalNsGroup, rec)
	if lateInitializeFlags(disable, forwardersOnly, rec) {
		changed = true
	}
	if lateInitializeExtAttrs(extAttrs, rec) {
		changed = true
	}
	if lateInitializeImmutableDefaults(view, zoneFormat, rec) {
		changed = true
	}
	return changed
}

// lateInitializeStrings back-fills comment, nsGroup, and externalNsGroup.
func lateInitializeStrings(comment, nsGroup, externalNsGroup **string, rec *ibclient.ZoneForward) bool {
	changed := false
	if *comment == nil && rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		*comment = &c
		changed = true
	}
	if *nsGroup == nil && rec.NsGroup != nil && *rec.NsGroup != "" {
		n := *rec.NsGroup
		*nsGroup = &n
		changed = true
	}
	if *externalNsGroup == nil && rec.ExternalNsGroup != nil && *rec.ExternalNsGroup != "" {
		e := *rec.ExternalNsGroup
		*externalNsGroup = &e
		changed = true
	}
	return changed
}

// lateInitializeFlags back-fills disable and forwardersOnly.
func lateInitializeFlags(disable, forwardersOnly **bool, rec *ibclient.ZoneForward) bool {
	changed := false
	if *disable == nil && rec.Disable != nil {
		d := *rec.Disable
		*disable = &d
		changed = true
	}
	if *forwardersOnly == nil && rec.ForwardersOnly != nil {
		f := *rec.ForwardersOnly
		*forwardersOnly = &f
		changed = true
	}
	return changed
}

// lateInitializeExtAttrs back-fills extAttrs.
func lateInitializeExtAttrs(extAttrs *map[string]string, rec *ibclient.ZoneForward) bool {
	if len(*extAttrs) == 0 {
		if fromRec := extAttrsFromEA(identity.Strip(rec.Ea)); len(fromRec) > 0 {
			*extAttrs = fromRec
			return true
		}
	}
	return false
}

// lateInitializeImmutableDefaults back-fills the immutable-once-set view
// and zoneFormat fields with their server-assigned default (see the
// lateInitialize doc comment for why immutable fields are still
// late-initialized).
func lateInitializeImmutableDefaults(view, zoneFormat **string, rec *ibclient.ZoneForward) bool {
	changed := false
	if *view == nil && rec.View != nil && *rec.View != "" {
		v := *rec.View
		*view = &v
		changed = true
	}
	if *zoneFormat == nil && rec.ZoneFormat != "" {
		z := rec.ZoneFormat
		*zoneFormat = &z
		changed = true
	}
	return changed
}

// ── Observation ──────────────────────────────────────────────────────────

// observedZoneForward holds the scalar field values extracted from a WAPI
// ZoneForward response (full-mirror AtProvider, minus ForwardTo and
// ForwardingServers). The cluster and namespaced ZoneForwardObservation
// types are structurally similar but nest distinct-per-scope NameServer
// and ForwardingServer types, so each scope converts rec.ForwardTo and
// rec.ForwardingServers to its own CRD types at the call site and copies
// these scalar fields into its own Observation type.
type observedZoneForward struct {
	ID              string
	Fqdn            *string
	View            *string
	ZoneFormat      *string
	Comment         *string
	Disable         *bool
	ForwardersOnly  *bool
	NsGroup         *string
	ExternalNsGroup *string
	ExtAttrs        map[string]string
	Ref             *string
}

// observeFromZoneForward extracts the scalar fields mirrored by
// ZoneForwardObservation from a WAPI ZoneForward response.
// NewEmptyZoneForward requests zone_format, ns_group, external_ns_group,
// comment, disable, extattrs, forwarders_only, and forwarding_servers in
// addition to the object's default return fields (fqdn, view, forward_to)
// — every field this provider surfaces is always present on a GET
// response.
func observeFromZoneForward(externalID string, rec *ibclient.ZoneForward) observedZoneForward {
	o := observedZoneForward{
		ID:       externalID,
		ExtAttrs: extAttrsFromEA(rec.Ea),
	}
	if rec.Fqdn != "" {
		f := rec.Fqdn
		o.Fqdn = &f
	}
	if rec.View != nil && *rec.View != "" {
		v := *rec.View
		o.View = &v
	}
	if rec.ZoneFormat != "" {
		z := rec.ZoneFormat
		o.ZoneFormat = &z
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Disable != nil {
		d := *rec.Disable
		o.Disable = &d
	}
	if rec.ForwardersOnly != nil {
		fo := *rec.ForwardersOnly
		o.ForwardersOnly = &fo
	}
	if rec.NsGroup != nil && *rec.NsGroup != "" {
		n := *rec.NsGroup
		o.NsGroup = &n
	}
	if rec.ExternalNsGroup != nil && *rec.ExternalNsGroup != "" {
		e := *rec.ExternalNsGroup
		o.ExternalNsGroup = &e
	}
	if rec.Ref != "" {
		r := rec.Ref
		o.Ref = &r
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createZoneForward issues the WAPI create call, stamping the owning
// managed resource's uid into the object's extensible attributes in the
// same request that creates it (identity.Stamp) — there is no follow-up
// call, so there is no window in which the object exists without its
// identity stamp. forwardTo and forwardingServers are already converted
// to the SDK's shape by the caller (see cluster.go/namespaced.go).
func createZoneForward(objMgr ibclient.IBObjectManager, fqdn, view, zoneFormat, comment, nsGroup, externalNsGroup *string, disable, forwardersOnly *bool, forwardTo []ibclient.NameServer, forwardingServers []*ibclient.Forwardingmemberserver, extAttrs map[string]string, uid string) (*ibclient.ZoneForward, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateZoneForward(
		strOrEmpty(comment),
		boolOrFalse(disable),
		ea,
		ibclient.NullableNameServers{NameServers: forwardTo},
		boolOrFalse(forwardersOnly),
		&ibclient.NullableForwardingServers{Servers: forwardingServers},
		strOrEmpty(fqdn),
		strOrEmpty(nsGroup),
		strOrEmpty(view),
		strOrEmpty(zoneFormat),
		strOrEmpty(externalNsGroup),
	)
}

// updateZoneForward issues the WAPI update call. fqdn, view, and
// zoneFormat are intentionally never passed — UpdateZoneForward has no
// parameters for them (immutable fields). Every call re-asserts the
// identity stamp (identity.Stamp) in the extattrs it sends — a WAPI PUT
// carrying extattrs replaces the whole map rather than merging it, so
// omitting the stamp here would wipe it off the object on the first
// field update after create.
func updateZoneForward(objMgr ibclient.IBObjectManager, ref string, comment, nsGroup, externalNsGroup *string, disable, forwardersOnly *bool, forwardTo []ibclient.NameServer, forwardingServers []*ibclient.Forwardingmemberserver, extAttrs map[string]string, uid string) (*ibclient.ZoneForward, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.UpdateZoneForward(
		ref,
		strOrEmpty(comment),
		boolOrFalse(disable),
		ea,
		ibclient.NullableNameServers{NameServers: forwardTo},
		boolOrFalse(forwardersOnly),
		&ibclient.NullableForwardingServers{Servers: forwardingServers},
		strOrEmpty(nsGroup),
		strOrEmpty(externalNsGroup),
	)
}

// deleteZoneForward issues the WAPI delete call.
func deleteZoneForward(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteZoneForward(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────
//
// See recorda's ensureIdentityPrerequisite doc for the full rationale:
// the identity extensible-attribute definition is an install
// prerequisite, probed unconditionally before Create (which always
// stamps identity) and reactively — only on the identity-EA search
// step's own failure — for Observe/Delete.

// ensureIdentityPrerequisite probes the Grid for the identity extensible
// attribute definition before any call that stamps identity onto a new
// object. A *identity.PrerequisiteError is returned verbatim. prober
// defaults to identity.DefaultProber when nil; endpoint falls back to
// unresolvedProbeEndpoint when the caller was built without a resolved
// Grid host.
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

// resolveZoneForwardIdentity resolves the ZoneForward identified by
// ref/uid through the shared UID-in-EA ladder.
func resolveZoneForwardIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.ZoneForward, identity.Outcome, error) {
	return identity.Resolve[*ibclient.ZoneForward](ctx, conn, ibclient.NewEmptyZoneForward, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting a
// ZoneForward through the identity ladder during Observe — common to
// both scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	rec          *ibclient.ZoneForward
	obs          observedZoneForward
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeZoneForward runs the identity ladder for Observe and
// late-initializes the given ForProvider field pointers from the
// resolved object.
func observeZoneForward(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, comment, nsGroup, externalNsGroup **string, disable, forwardersOnly **bool, extAttrs *map[string]string, view, zoneFormat **string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveZoneForwardIdentity(ctx, conn, ref, uid)
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
		rec:     rec,
		obs:     observeFromZoneForward(rec.Ref, rec),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(comment, nsGroup, externalNsGroup, disable, forwardersOnly, extAttrs, view, zoneFormat, rec)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteZoneForwardIdentity issues the WAPI delete for the ZoneForward
// this managed resource owns, resolving through the identity ladder
// first so a stale _ref is never mistaken for a deleted object. See
// recorda's deleteARecordIdentity doc for the full ownership-
// verification rules.
func deleteZoneForwardIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveZoneForwardIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteZoneForward)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteZoneForward(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteZoneForward)
	default:
		return errors.New("identity: unresolved ZoneForward outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced ZoneForward
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterZoneForward(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster ZoneForward controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("ZoneForward"))

	o.Gate.Register(func() {
		if err := setupNamespacedZoneForward(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced ZoneForward controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("ZoneForward"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced ZoneForward
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterZoneForward(mgr, o); err != nil {
		return err
	}
	return setupNamespacedZoneForward(mgr, o)
}
