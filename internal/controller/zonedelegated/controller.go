// Package zonedelegated implements the Crossplane controller for the
// Infoblox NIOS ZoneDelegated managed resource (WAPI `zone_delegated`
// object type). Like the ARecord controller, this provider wraps the
// official infoblox-go-client Go SDK directly — the SDK's ObjectManager
// exposes typed CRUD methods (CreateZoneDelegated/GetZoneDelegatedByRef/
// UpdateZoneDelegated/DeleteZoneDelegated) instead of a generic HTTP
// request/response envelope, so there is no internal REST client to
// compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
//
// This provider has no shared internal/clients package — each resource
// controller package defines its own credential bridge (mirrors the
// ARecord/recorda controller package).
package zonedelegated

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zonedelegated/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zonedelegated/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage         = "cannot track ProviderConfig usage"
	errPersistExternalName  = "cannot persist refreshed external name"
	errGetPC                = "cannot get ProviderConfig"
	errGetClusterPC         = "cannot get ClusterProviderConfig"
	errUnsupportedKind      = "unsupported provider config kind"
	errGetSecret            = "cannot get credentials secret"
	errNoSecretRef          = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds     = "unsupported credentials source: only Secret is supported"
	errMissingCredKey       = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager     = "cannot create Infoblox NIOS WAPI object manager"
	errObserveZoneDelegated = "cannot observe ZoneDelegated"
	errCreateZoneDelegated  = "cannot create ZoneDelegated"
	errUpdateZoneDelegated  = "cannot update ZoneDelegated"
	errDeleteZoneDelegated  = "cannot delete ZoneDelegated"
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

func uint32OrZero(v *uint32) uint32 {
	if v == nil {
		return 0
	}
	return *v
}

// nameServersEqual compares two ordered lists of SDK NameServer values by
// their Name and Address fields — the only two fields the CRD's
// ZoneDelegatedNameServer type exposes (SharedWithMsParentDelegation,
// Stealth, TsigKey* are response-only WAPI internals not surfaced by this
// provider). Order matters: delegateTo is a WAPI list, and the SDK/WAPI
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
// (plus the pre-converted SDK-shaped delegateTo slice) rather than a whole
// ZoneDelegatedParameters value. The cluster and namespaced
// ZoneDelegatedParameters types are structurally identical (same field
// names and primitive types) but are distinct named Go types, and
// DelegateTo additionally nests a distinct-per-scope struct type
// (ZoneDelegatedNameServer) — so callers convert DelegateTo to the SDK's
// []ibclient.NameServer shape at the call site (see cluster.go/
// namespaced.go) before invoking these shared helpers.
//
// fqdn, view, and zoneFormat are immutable (confirmed absent from the
// UpdateZoneDelegated SDK method signature, and WAPI additionally rejects
// changing view at the data level) — they are intentionally excluded from
// isUpToDate.

// isUpToDate compares the desired mutable ZoneDelegated fields against the
// observed ZoneDelegated.
func isUpToDate(delegateTo []ibclient.NameServer, comment, nsGroup *string, disable, locked, useDelegatedTTL *bool, delegatedTTL *uint32, extAttrs map[string]string, rec *ibclient.ZoneDelegated) bool {
	if !nameServersEqual(delegateTo, rec.DelegateTo.NameServers) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(rec.Disable) {
		return false
	}
	if boolOrFalse(locked) != boolOrFalse(rec.Locked) {
		return false
	}
	if strOrEmpty(nsGroup) != strOrEmpty(rec.NsGroup) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useDelegatedTTL) != boolOrFalse(rec.UseDelegatedTtl) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted delegated ttl and returns its own default on
	// every GET — comparing it against the spec value never converges.
	if boolOrFalse(useDelegatedTTL) {
		if uint32OrZero(delegatedTTL) != uint32OrZero(rec.DelegatedTtl) {
			return false
		}
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// disable, locked, nsGroup, delegatedTtl, useDelegatedTtl, extAttrs, view,
// zoneFormat) from the observed ZoneDelegated into spec so isUpToDate does
// not see phantom drift on the next reconcile. view and zoneFormat are
// immutable once set (excluded from isUpToDate above), but they are still
// server-defaulted when omitted at create time (CreateZoneDelegated
// defaults view to "default" and zoneFormat to "FORWARD") — recording the
// server-assigned value keeps spec truthful to what was actually applied.
// The required fields (fqdn, delegateTo) are never late-initialized —
// both are always user-supplied.
func lateInitialize(comment, nsGroup **string, disable, locked, useDelegatedTTL **bool, delegatedTTL **uint32, extAttrs *map[string]string, view, zoneFormat **string, rec *ibclient.ZoneDelegated) bool {
	// Split across small helpers (rather than one long if-chain) to keep
	// cyclomatic complexity low per function — each helper covers a
	// handful of related fields.
	changed := lateInitializeStrings(comment, nsGroup, rec)
	if lateInitializeFlags(disable, locked, useDelegatedTTL, rec) {
		changed = true
	}
	if lateInitializeTTLAndExtAttrs(delegatedTTL, useDelegatedTTL, extAttrs, rec) {
		changed = true
	}
	if lateInitializeImmutableDefaults(view, zoneFormat, rec) {
		changed = true
	}
	return changed
}

// lateInitializeStrings back-fills comment and nsGroup.
func lateInitializeStrings(comment, nsGroup **string, rec *ibclient.ZoneDelegated) bool {
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
	return changed
}

// lateInitializeFlags back-fills disable, locked, and useDelegatedTtl.
func lateInitializeFlags(disable, locked, useDelegatedTTL **bool, rec *ibclient.ZoneDelegated) bool {
	changed := false
	if *disable == nil && rec.Disable != nil {
		d := *rec.Disable
		*disable = &d
		changed = true
	}
	if *locked == nil && rec.Locked != nil {
		l := *rec.Locked
		*locked = &l
		changed = true
	}
	if *useDelegatedTTL == nil && rec.UseDelegatedTtl != nil {
		u := *rec.UseDelegatedTtl
		*useDelegatedTTL = &u
		changed = true
	}
	return changed
}

// lateInitializeTTLAndExtAttrs back-fills delegatedTtl and extAttrs.
// delegatedTtl is only back-filled when useDelegatedTTL is on (the
// caller runs lateInitializeFlags first, so *useDelegatedTTL already
// reflects any backfill) — when it is off, the observed delegatedTtl is
// WAPI's own default, not a value implied by the user's config.
func lateInitializeTTLAndExtAttrs(delegatedTTL **uint32, useDelegatedTTL **bool, extAttrs *map[string]string, rec *ibclient.ZoneDelegated) bool {
	changed := false
	if *delegatedTTL == nil && rec.DelegatedTtl != nil && boolOrFalse(*useDelegatedTTL) {
		t := *rec.DelegatedTtl
		*delegatedTTL = &t
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromRec := extAttrsFromEA(rec.Ea); len(fromRec) > 0 {
			*extAttrs = fromRec
			changed = true
		}
	}
	return changed
}

// lateInitializeImmutableDefaults back-fills the immutable-once-set view
// and zoneFormat fields with their server-assigned default (see the
// lateInitialize doc comment for why immutable fields are still
// late-initialized).
func lateInitializeImmutableDefaults(view, zoneFormat **string, rec *ibclient.ZoneDelegated) bool {
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

// observedZoneDelegated holds the scalar field values extracted from a
// WAPI ZoneDelegated response (full-mirror AtProvider, minus DelegateTo).
// The cluster and namespaced ZoneDelegatedObservation types are
// structurally similar but nest a distinct-per-scope
// ZoneDelegatedNameServer type for DelegateTo, so each scope converts
// rec.DelegateTo.NameServers to its own CRD type at the call site and
// copies these scalar fields into its own Observation type.
type observedZoneDelegated struct {
	ID              string
	Fqdn            *string
	View            *string
	ZoneFormat      *string
	Comment         *string
	Disable         *bool
	Locked          *bool
	NsGroup         *string
	DelegatedTTL    *uint32
	UseDelegatedTTL *bool
	ExtAttrs        map[string]string
	Ref             *string
}

// observeFromZoneDelegated extracts the scalar fields mirrored by
// ZoneDelegatedObservation from a WAPI ZoneDelegated response.
// NewZoneDelegated requests delegate_to, fqdn, view, comment, disable,
// locked, ns_group, delegated_ttl, use_delegated_ttl, zone_format, and
// extattrs — every field this provider surfaces is always present on a
// GET response.
func observeFromZoneDelegated(externalID string, rec *ibclient.ZoneDelegated) observedZoneDelegated {
	o := observedZoneDelegated{
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
	if rec.Locked != nil {
		l := *rec.Locked
		o.Locked = &l
	}
	if rec.NsGroup != nil && *rec.NsGroup != "" {
		n := *rec.NsGroup
		o.NsGroup = &n
	}
	if rec.DelegatedTtl != nil {
		t := *rec.DelegatedTtl
		o.DelegatedTTL = &t
	}
	if rec.UseDelegatedTtl != nil {
		u := *rec.UseDelegatedTtl
		o.UseDelegatedTTL = &u
	}
	if rec.Ref != "" {
		r := rec.Ref
		o.Ref = &r
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createZoneDelegated issues the WAPI create call. delegateTo is already
// converted to the SDK's []ibclient.NameServer shape by the caller (see
// cluster.go/namespaced.go).
func createZoneDelegated(objMgr ibclient.IBObjectManager, fqdn, view, zoneFormat, comment, nsGroup *string, disable, locked, useDelegatedTTL *bool, delegatedTTL *uint32, delegateTo []ibclient.NameServer, extAttrs map[string]string) (*ibclient.ZoneDelegated, error) {
	return objMgr.CreateZoneDelegated(
		strOrEmpty(fqdn),
		ibclient.NullableNameServers{NameServers: delegateTo},
		strOrEmpty(comment),
		boolOrFalse(disable),
		boolOrFalse(locked),
		strOrEmpty(nsGroup),
		uint32OrZero(delegatedTTL),
		boolOrFalse(useDelegatedTTL),
		buildEA(extAttrs),
		strOrEmpty(view),
		strOrEmpty(zoneFormat),
	)
}

// updateZoneDelegated issues the WAPI update call. fqdn, view, and
// zoneFormat are intentionally never passed — UpdateZoneDelegated has no
// parameters for them (immutable fields).
func updateZoneDelegated(objMgr ibclient.IBObjectManager, ref string, comment, nsGroup *string, disable, locked, useDelegatedTTL *bool, delegatedTTL *uint32, delegateTo []ibclient.NameServer, extAttrs map[string]string) (*ibclient.ZoneDelegated, error) {
	return objMgr.UpdateZoneDelegated(
		ref,
		ibclient.NullableNameServers{NameServers: delegateTo},
		strOrEmpty(comment),
		boolOrFalse(disable),
		boolOrFalse(locked),
		strOrEmpty(nsGroup),
		uint32OrZero(delegatedTTL),
		boolOrFalse(useDelegatedTTL),
		buildEA(extAttrs),
	)
}

// deleteZoneDelegated issues the WAPI delete call.
func deleteZoneDelegated(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteZoneDelegated(ref)
	return err
}

// zoneDelegatedExistsByNaturalKey reports whether a live ZoneDelegated
// still exists under the CR's own fqdn — the same field WAPI uses to
// compute the _ref. Used by Delete() when the stored _ref 404s: a hit
// here means the _ref is merely stale, not that the object is gone.
// GetZoneDelegated does not surface a not-found condition as an error —
// per its own implementation it returns (nil, nil) both when fqdn is
// empty and when the search finds no match — so absence is detected by
// checking the returned pointer rather than by classifying an error. When
// fqdn is empty there is no way to re-discover the object, so the search
// is skipped (found=false) rather than treated as an error.
func zoneDelegatedExistsByNaturalKey(objMgr ibclient.IBObjectManager, fqdn *string) (bool, error) {
	if strOrEmpty(fqdn) == "" {
		return false, nil
	}
	rec, err := objMgr.GetZoneDelegated(strOrEmpty(fqdn))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return rec != nil, nil
}

// deleteZoneDelegatedResolving404 issues the WAPI delete and, on a 404
// against the stored _ref, resolves the object's natural key before
// concluding it is gone. A 404 on a derived handle is evidence the handle
// rotated, not evidence the object was removed: if the natural-key search
// still finds a live zone, deleting is refused because ownership of that
// zone cannot be verified from the search alone (see the staleref package
// doc for the full rationale).
func deleteZoneDelegatedResolving404(objMgr ibclient.IBObjectManager, ref string, fqdn *string) error {
	delErr := deleteZoneDelegated(objMgr, ref)
	if delErr == nil {
		return nil
	}
	if !isNotFound(delErr) {
		return errors.Wrap(delErr, errDeleteZoneDelegated)
	}
	found, searchErr := zoneDelegatedExistsByNaturalKey(objMgr, fqdn)
	if searchErr != nil {
		return errors.Wrap(searchErr, errDeleteZoneDelegated)
	}
	if found {
		return staleref.RefusalError()
	}
	return nil
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced
// ZoneDelegated controllers with the SafeStart gate. Each controller
// starts only after its respective CRD has been installed in the
// cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterZoneDelegated(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster ZoneDelegated controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("ZoneDelegated"))

	o.Gate.Register(func() {
		if err := setupNamespacedZoneDelegated(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced ZoneDelegated controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("ZoneDelegated"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced ZoneDelegated
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterZoneDelegated(mgr, o); err != nil {
		return err
	}
	return setupNamespacedZoneDelegated(mgr, o)
}
