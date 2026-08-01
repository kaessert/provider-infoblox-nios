// Package recordptr implements the Crossplane controller for the Infoblox
// NIOS PTRRecord managed resource. Like recorda, this provider wraps the
// official infoblox-go-client Go SDK directly — the SDK's ObjectManager
// exposes typed CRUD methods (CreatePTRRecord/GetPTRRecordByRef/
// UpdatePTRRecord/DeletePTRRecord) instead of a generic HTTP
// request/response envelope, so there is no internal REST client to
// compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordptr

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordptr/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordptr/v1alpha1"
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
	errObservePTRRecord    = "cannot observe PTRRecord"
	errCreatePTRRecord     = "cannot create PTRRecord"
	errUpdatePTRRecord     = "cannot update PTRRecord"
	errDeletePTRRecord     = "cannot delete PTRRecord"
	errCidrIPMutex         = "cidr and ipv4Addr/ipv6Addr are mutually exclusive"
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

// ttlOrZero converts an optional *uint32 TTL into the uint32 the SDK
// expects, treating an unset field as 0 (matching the SDK/WAPI zero
// value, "not cached").
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
// than a whole PTRRecordParameters value. The cluster and namespaced
// PTRRecordParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct
// struct conversion between them is not always available once other
// resources in this provider grow reference fields; parameterizing on
// the field pointers instead lets both scopes share this logic
// unconditionally.

// isUpToDate compares the desired PTRRecord fields against the observed
// RecordPTR. View is hard immutable (WAPI rejects PUT with "Field is not
// allowed for update: view") and Zone is response-only (derived,
// never a Create/Update parameter) — both are intentionally excluded
// from this comparison.
func isUpToDate(ptrdname, name, ipv4Addr, ipv6Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordPTR) bool {
	if strOrEmpty(ptrdname) != strOrEmpty(rec.PtrdName) {
		return false
	}
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(ipv4Addr) != strOrEmpty(rec.Ipv4Addr) {
		return false
	}
	if strOrEmpty(ipv6Addr) != strOrEmpty(rec.Ipv6Addr) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
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
		if ttlOrZero(ttl) != ttlOrZero(rec.Ttl) {
			return false
		}
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (name,
// comment, ttl, useTtl, extAttrs) from the observed RecordPTR into spec
// so isUpToDate does not see phantom drift on the next reconcile.
// ptrdname and view are never late-initialized — ptrdname is a required
// ForProvider field and view is both required and immutable, always
// user-supplied. Returns true if any field was changed.
func lateInitialize(name, comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordPTR) bool {
	changed := false

	if *name == nil && rec.Name != nil && *rec.Name != "" {
		n := *rec.Name
		*name = &n
		changed = true
	}
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
		if fromRec := extAttrsFromEA(rec.Ea); len(fromRec) > 0 {
			*extAttrs = fromRec
			changed = true
		}
	}

	return changed
}

// observedRecordPTR holds the primitive field values extracted from a
// WAPI RecordPTR response that are mirrored by PTRRecordObservation. The
// cluster and namespaced PTRRecordObservation types are structurally
// similar but distinct named types, so they are not directly
// convertible — each scope copies this intermediate struct's fields into
// its own Observation type at the call site.
type observedRecordPTR struct {
	ID       string
	Ptrdname *string
	Name     *string
	IPv4Addr *string
	IPv6Addr *string
	View     *string
	Comment  *string
	TTL      *uint32
	UseTTL   *bool
	ExtAttrs map[string]string
	Ref      *string
	Zone     *string
}

// observeFromRecordPTR extracts the fields mirrored by
// PTRRecordObservation (the full-mirror AtProvider convention) from a
// WAPI RecordPTR response. GetPTRRecordByRef requests only the field
// subset in NewEmptyRecordPTR's default returnFields (extattrs, ipv4addr,
// ipv6addr, name, ptrdname, view, zone, comment, use_ttl, ttl);
// response-only fields outside that set (creator, discovered_data,
// cloud_info, etc.) are not requested by this provider and are left at
// their zero value in AtProvider.
func observeFromRecordPTR(externalID string, rec *ibclient.RecordPTR) observedRecordPTR {
	o := observedRecordPTR{
		ID:       externalID,
		Ptrdname: rec.PtrdName,
		Name:     rec.Name,
		IPv4Addr: rec.Ipv4Addr,
		IPv6Addr: rec.Ipv6Addr,
		ExtAttrs: extAttrsFromEA(rec.Ea),
	}
	if rec.View != "" {
		v := rec.View
		o.View = &v
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

// createPTRRecord issues the WAPI create call. When cidr is set, the
// WAPI dynamically allocates the next available IPv4/IPv6 address from
// the given network view (func:nextavailableip) instead of using a
// caller-supplied static address — cidr and ipv4Addr/ipv6Addr are
// mutually exclusive, enforced below before the SDK call is issued.
// CreatePTRRecord already defaults an empty network view to "default"
// internally; this wrapper applies the same default explicitly for
// consistency with createARecord (whose SDK counterpart does not
// self-default).
func createPTRRecord(objMgr ibclient.IBObjectManager, ptrdname, name, ipv4Addr, ipv6Addr, view, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, cidr, networkView *string) (*ibclient.RecordPTR, error) {
	cidrVal := strOrEmpty(cidr)
	if cidrVal != "" && (strOrEmpty(ipv4Addr) != "" || strOrEmpty(ipv6Addr) != "") {
		return nil, errors.New(errCidrIPMutex)
	}

	ipAddr := strOrEmpty(ipv4Addr)
	if ipAddr == "" {
		ipAddr = strOrEmpty(ipv6Addr)
	}

	netView := strOrEmpty(networkView)
	if cidrVal != "" && netView == "" {
		netView = "default"
	}

	return objMgr.CreatePTRRecord(
		netView,
		strOrEmpty(view),
		strOrEmpty(ptrdname),
		strOrEmpty(name),
		cidrVal,
		ipAddr,
		boolOrFalse(useTTL),
		ttlOrZero(ttl),
		strOrEmpty(comment),
		buildEA(extAttrs),
	)
}

// updatePTRRecord issues the WAPI update call. view is intentionally
// never passed — UpdatePTRRecord has no view parameter (immutable
// field). cidr and netView are always empty, mirroring createPTRRecord.
func updatePTRRecord(objMgr ibclient.IBObjectManager, ref string, ptrdname, name, ipv4Addr, ipv6Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*ibclient.RecordPTR, error) {
	ipAddr := strOrEmpty(ipv4Addr)
	if ipAddr == "" {
		ipAddr = strOrEmpty(ipv6Addr)
	}
	return objMgr.UpdatePTRRecord(
		ref,
		"", // netview — not exposed by this provider
		strOrEmpty(ptrdname),
		strOrEmpty(name),
		"", // cidr — not exposed by this provider
		ipAddr,
		boolOrFalse(useTTL),
		ttlOrZero(ttl),
		strOrEmpty(comment),
		buildEA(extAttrs),
	)
}

// deletePTRRecord issues the WAPI delete call.
func deletePTRRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeletePTRRecord(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced PTRRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterPTRRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster PTRRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("PTRRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedPTRRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced PTRRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("PTRRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced PTRRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterPTRRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedPTRRecord(mgr, o)
}
