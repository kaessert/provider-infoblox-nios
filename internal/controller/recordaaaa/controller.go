// Package recordaaaa implements the Crossplane controller for the
// Infoblox NIOS AAAARecord managed resource. Like recorda (its IPv4
// counterpart), this provider wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateAAAARecord/GetAAAARecordByRef/UpdateAAAARecord/DeleteAAAARecord)
// instead of a generic HTTP request/response envelope, so there is no
// internal REST client to compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordaaaa

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordaaaa/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordaaaa/v1alpha1"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage      = "cannot track ProviderConfig usage"
	errGetPC             = "cannot get ProviderConfig"
	errGetClusterPC      = "cannot get ClusterProviderConfig"
	errUnsupportedKind   = "unsupported provider config kind"
	errGetSecret         = "cannot get credentials secret"
	errNoSecretRef       = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds  = "unsupported credentials source: only Secret is supported"
	errMissingCredKey    = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager  = "cannot create Infoblox NIOS WAPI object manager"
	errObserveAAAARecord = "cannot observe AAAARecord"
	errCreateAAAARecord  = "cannot create AAAARecord"
	errUpdateAAAARecord  = "cannot update AAAARecord"
	errDeleteAAAARecord  = "cannot delete AAAARecord"
	errCidrIPv6Mutex     = "cidr and ipv6Addr are mutually exclusive"
)

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
const wapiVersion = "2.9.7"

// ── Credential bridge ───────────────────────────────────────────────────────

// nioCredentials holds the WAPI connection parameters extracted from the
// ProviderConfig's credentials Secret (host/username/password keys, plus
// the optional ssl_verify key).
type nioCredentials struct {
	Host      string
	Username  string
	Password  string
	SslVerify bool
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

	// sslVerify is secure by default (true). Setting the optional
	// "ssl_verify" Secret key to "false" disables TLS certificate
	// verification — used when the Grid Manager presents a self-signed
	// certificate whose SAN does not match the reachable host address.
	sslVerify := true
	if v := string(secret.Data["ssl_verify"]); v == "false" {
		sslVerify = false
	}

	return &nioCredentials{Host: host, Username: username, Password: password, SslVerify: sslVerify}, nil
}

// newObjectManager constructs an authenticated ibclient.IBObjectManager
// from the given credentials. The Connector performs HTTP Basic Auth on
// every request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newObjectManager(creds *nioCredentials) (ibclient.IBObjectManager, error) {
	return newObjectManagerWithScheme(creds, "https", "443")
}

// newObjectManagerWithScheme is the scheme/port-parameterized variant of
// newObjectManager used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newObjectManagerWithScheme(creds *nioCredentials, scheme, port string) (ibclient.IBObjectManager, error) {
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
	// SslVerify is configurable via the credentials Secret's optional
	// "ssl_verify" key (default: "true"). Set to "false" when the Grid
	// Manager uses a self-signed certificate whose SAN does not match
	// the reachable host address.
	sslVerifyStr := "true"
	if !creds.SslVerify {
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

// ttlOrZero converts an optional *int64 TTL into the uint32 the SDK
// expects. TTL is a DNS time-to-live in seconds; values outside the valid
// uint32 range (or negative) are clamped to 0 rather than silently
// wrapping — CEL/CRD validation on the ForProvider field is expected to
// reject out-of-range values before they ever reach this helper.
func ttlOrZero(ttl *int64) uint32 {
	if ttl == nil || *ttl < 0 || *ttl > math.MaxUint32 {
		return 0
	}
	return uint32(*ttl)
}

// uint32PtrOrZero converts an optional *uint32 TTL (as returned by the
// SDK) into a plain uint32 for comparison against ttlOrZero.
func uint32PtrOrZero(ttl *uint32) uint32 {
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
// than a whole AAAARecordParameters value. The cluster and namespaced
// AAAARecordParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct struct
// conversion between them is not always available once other resources in
// this provider grow reference fields; parameterizing on the field
// pointers instead lets both scopes share this logic unconditionally.

// isUpToDate compares the desired AAAARecord fields against the observed
// RecordAAAA. View is immutable (WAPI ties the object's _ref to
// view+zone+name; the UpdateAAAARecord SDK method has no view parameter)
// and RemoveAssociatedPtr is write-only (delete-time option, never
// present in a GET response) — both are intentionally excluded from this
// comparison.
func isUpToDate(name, ipv6Addr, comment *string, ttl *int64, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordAAAA) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(ipv6Addr) != strOrEmpty(rec.Ipv6Addr) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if ttlOrZero(ttl) != uint32PtrOrZero(rec.Ttl) {
		return false
	}
	if boolOrFalse(useTTL) != boolOrFalse(rec.UseTtl) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// ttl, useTtl, extAttrs) from the observed RecordAAAA into spec so
// isUpToDate does not see phantom drift on the next reconcile. Required
// fields (name, ipv6Addr) and the immutable view field are never
// late-initialized — view is always user-supplied (required on the CRD)
// and name/ipv6Addr are always user-supplied too. Returns true if any
// field was changed.
func lateInitialize(comment **string, ttl **int64, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordAAAA) bool {
	changed := false

	if *comment == nil && rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		*comment = &c
		changed = true
	}
	if *ttl == nil && rec.Ttl != nil {
		t := int64(*rec.Ttl)
		*ttl = &t
		changed = true
	}
	if *useTTL == nil && rec.UseTtl != nil {
		u := *rec.UseTtl
		*useTTL = &u
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

// observedAAAARecord holds the primitive field values extracted from a
// WAPI RecordAAAA response. The cluster and namespaced
// AAAARecordObservation types are structurally similar but are distinct
// named types with distinct nested-struct field types (e.g.
// *AAAARecordCloudInfo), so they are not directly convertible — each
// scope copies this intermediate struct's fields into its own
// Observation type at the call site.
type observedAAAARecord struct {
	ID       string
	Name     *string
	IPv6Addr *string
	Comment  *string
	TTL      *int64
	UseTTL   *bool
	ExtAttrs map[string]string
	View     *string
	Ref      *string
	Zone     *string
}

// observeFromRecordAAAA extracts the fields mirrored by
// AAAARecordObservation (the full-mirror AtProvider convention) from a
// WAPI RecordAAAA response. The SDK's GetAAAARecordByRef method requests
// only a fixed subset of fields by default (extattrs, ipv6addr, name,
// view, zone, comment, ttl, use_ttl); response-only fields outside that
// set (creator, discovered_data, cloud_info, etc.) are not requested by
// this provider and are left at their zero value in AtProvider.
func observeFromRecordAAAA(externalID string, rec *ibclient.RecordAAAA) observedAAAARecord {
	o := observedAAAARecord{
		ID:       externalID,
		Name:     rec.Name,
		IPv6Addr: rec.Ipv6Addr,
		ExtAttrs: extAttrsFromEA(rec.Ea),
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Ttl != nil {
		t := int64(*rec.Ttl)
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

// createAAAARecord issues the WAPI create call. When cidr is set, the
// WAPI dynamically allocates the next available IPv6 address from the
// given network view (func:nextavailableip) instead of using a
// caller-supplied static address — cidr and ipv6Addr are mutually
// exclusive, enforced below before the SDK call is issued. CreateAAAARecord
// already defaults an empty network view to "default" internally; this
// wrapper applies the same default explicitly for consistency with
// createARecord (whose SDK counterpart does not self-default).
func createAAAARecord(objMgr ibclient.IBObjectManager, name, view, ipv6Addr, comment *string, ttl *int64, useTTL *bool, extAttrs map[string]string, cidr, networkView *string) (*ibclient.RecordAAAA, error) {
	cidrVal := strOrEmpty(cidr)
	if cidrVal != "" && strOrEmpty(ipv6Addr) != "" {
		return nil, errors.New(errCidrIPv6Mutex)
	}

	netView := strOrEmpty(networkView)
	if cidrVal != "" && netView == "" {
		netView = "default"
	}

	return objMgr.CreateAAAARecord(
		netView,
		strOrEmpty(view),
		strOrEmpty(name),
		cidrVal,
		strOrEmpty(ipv6Addr),
		boolOrFalse(useTTL),
		ttlOrZero(ttl),
		strOrEmpty(comment),
		buildEA(extAttrs),
	)
}

// updateAAAARecord issues the WAPI update call. view is intentionally
// never passed — UpdateAAAARecord has no view parameter (immutable
// field). cidr and netView are always empty, mirroring createAAAARecord.
func updateAAAARecord(objMgr ibclient.IBObjectManager, ref string, name, ipv6Addr, comment *string, ttl *int64, useTTL *bool, extAttrs map[string]string) (*ibclient.RecordAAAA, error) {
	return objMgr.UpdateAAAARecord(
		ref,
		"", // netView — not exposed by this provider
		strOrEmpty(name),
		strOrEmpty(ipv6Addr),
		"", // cidr — not exposed by this provider
		boolOrFalse(useTTL),
		ttlOrZero(ttl),
		strOrEmpty(comment),
		buildEA(extAttrs),
	)
}

// deleteAAAARecord issues the WAPI delete call. removeAssociatedPtr is
// accepted as a ForProvider field for schema completeness, but the SDK's
// DeleteAAAARecord wrapper takes only the object reference — it exposes
// no query-parameter or request-body hook for the WAPI
// remove_associated_ptr delete option, so this provider cannot honor a
// user-set value. NIOS's default PTR-cleanup behavior applies on every
// delete regardless.
func deleteAAAARecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteAAAARecord(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced AAAARecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterAAAARecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster AAAARecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("AAAARecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedAAAARecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced AAAARecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("AAAARecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced AAAARecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterAAAARecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedAAAARecord(mgr, o)
}
