// Package recordsrv implements the Crossplane controller for the Infoblox
// NIOS SRVRecord managed resource. Like recorda, this provider wraps the
// official infoblox-go-client Go SDK directly — the SDK's ObjectManager
// exposes typed CRUD methods (CreateSRVRecord/GetSRVRecordByRef/
// UpdateSRVRecord/DeleteSRVRecord) instead of a generic HTTP
// request/response envelope, so there is no internal REST client to
// compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordsrv

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordsrv/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordsrv/v1alpha1"
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
	errObserveSRVRecord = "cannot observe SRVRecord"
	errCreateSRVRecord  = "cannot create SRVRecord"
	errUpdateSRVRecord  = "cannot update SRVRecord"
	errDeleteSRVRecord  = "cannot delete SRVRecord"
)

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
const wapiVersion = "2.9.7"

// ── Credential bridge ───────────────────────────────────────────────────────

// nioCredentials holds the WAPI connection parameters extracted from the
// ProviderConfig's credentials Secret (host/username/password keys).
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
	// SslVerify defaults to secure (true). The ProviderConfig secret
	// format documented for this provider (host/username/password only)
	// has no field to disable verification; a self-signed Grid Manager
	// certificate must be trusted at the OS/pod level.
	transportConfig := ibclient.NewTransportConfig("true", 60, 10)

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
	return uint32OrZero(ttl)
}

// uint32OrZero converts an optional *int64 field (priority, weight, port,
// ttl — all validated 0-65535 or 0-MaxUint32 by CRD/CEL rules) into the
// uint32 the SDK's CreateSRVRecord/UpdateSRVRecord methods expect. Values
// outside the valid uint32 range (or negative) are clamped to 0 rather
// than silently wrapping.
func uint32OrZero(v *int64) uint32 {
	if v == nil || *v < 0 || *v > math.MaxUint32 {
		return 0
	}
	return uint32(*v)
}

// uint32PtrToInt64Ptr converts an observed *uint32 SDK field into the
// *int64 CRD representation, returning nil when the SDK did not populate
// the field.
func uint32PtrToInt64Ptr(v *uint32) *int64 {
	if v == nil {
		return nil
	}
	i := int64(*v)
	return &i
}

// uint32PtrOrZero converts an optional *uint32 (as returned by the SDK)
// into a plain uint32 for comparison against uint32OrZero.
func uint32PtrOrZero(v *uint32) uint32 {
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
// than a whole SRVRecordParameters value. The cluster and namespaced
// SRVRecordParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct
// struct conversion between them is not always available once other
// resources in this provider grow reference fields; parameterizing on
// the field pointers instead lets both scopes share this logic
// unconditionally.

// isUpToDate compares the desired SRVRecord fields against the observed
// RecordSRV. View is immutable (WAPI ties the object's _ref to
// view+name+zone; the UpdateSRVRecord SDK method has no view parameter)
// and is intentionally excluded from this comparison.
func isUpToDate(name, target, comment *string, priority, weight, port, ttl *int64, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordSRV) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(target) != strOrEmpty(rec.Target) {
		return false
	}
	if uint32OrZero(priority) != uint32PtrOrZero(rec.Priority) {
		return false
	}
	if uint32OrZero(weight) != uint32PtrOrZero(rec.Weight) {
		return false
	}
	if uint32OrZero(port) != uint32PtrOrZero(rec.Port) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if ttlOrZero(ttl) != uint32PtrOrZero(rec.Ttl) {
		return false
	}
	if boolOrFalse(useTTL) != boolPtrOrFalse(rec.UseTtl) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// ttl, useTtl, extAttrs) from the observed RecordSRV into spec so
// isUpToDate does not see phantom drift on the next reconcile. Required
// fields (name, target, priority, weight, port) and the immutable view
// field are never late-initialized — view is always user-supplied
// (required on the CRD) and name/target/priority/weight/port are always
// user-supplied too. Returns true if any field was changed.
func lateInitialize(comment **string, ttl **int64, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordSRV) bool {
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

// observedSRVRecord holds the primitive field values extracted from a
// WAPI RecordSRV response. The cluster and namespaced SRVRecordObservation
// types are structurally similar but are distinct named types with
// distinct nested-struct field types (e.g. *SRVRecordCloudInfo), so they
// are not directly convertible — each scope copies this intermediate
// struct's fields into its own Observation type at the call site.
type observedSRVRecord struct {
	ID       string
	Name     *string
	Target   *string
	Priority *int64
	Weight   *int64
	Port     *int64
	Comment  *string
	TTL      *int64
	UseTTL   *bool
	ExtAttrs map[string]string
	View     *string
	Ref      *string
	Zone     *string
}

// observeFromRecordSRV extracts the fields mirrored by
// SRVRecordObservation (the full-mirror AtProvider convention) from a
// WAPI RecordSRV response. The SDK's GetSRVRecordByRef method requests
// only a fixed subset of fields by default (name, view, priority,
// weight, port, target, ttl, use_ttl, comment, extattrs, zone);
// response-only fields outside that set (creator, dns_name, dns_target,
// cloud_info, aws_rte53_record_info, ms_ad_user_data, etc.) are not
// requested by this provider and are left at their zero value in
// AtProvider.
func observeFromRecordSRV(externalID string, rec *ibclient.RecordSRV) observedSRVRecord {
	o := observedSRVRecord{
		ID:       externalID,
		Name:     rec.Name,
		Target:   rec.Target,
		Priority: uint32PtrToInt64Ptr(rec.Priority),
		Weight:   uint32PtrToInt64Ptr(rec.Weight),
		Port:     uint32PtrToInt64Ptr(rec.Port),
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

// createSRVRecord issues the WAPI create call.
func createSRVRecord(objMgr ibclient.IBObjectManager, view string, name, target, comment *string, priority, weight, port, ttl *int64, useTTL *bool, extAttrs map[string]string) (*ibclient.RecordSRV, error) {
	return objMgr.CreateSRVRecord(
		view,
		strOrEmpty(name),
		uint32OrZero(priority),
		uint32OrZero(weight),
		uint32OrZero(port),
		strOrEmpty(target),
		ttlOrZero(ttl),
		boolOrFalse(useTTL),
		strOrEmpty(comment),
		buildEA(extAttrs),
	)
}

// updateSRVRecord issues the WAPI update call. view is intentionally
// never passed — UpdateSRVRecord has no view parameter (immutable
// field). Because priority/weight/port/name/target are all included in
// every PUT, and any of them changing causes NIOS to mint a new _ref, the
// caller MUST re-read Ref from the response and refresh the external-name
// annotation if it changed.
func updateSRVRecord(objMgr ibclient.IBObjectManager, ref string, name, target, comment *string, priority, weight, port, ttl *int64, useTTL *bool, extAttrs map[string]string) (*ibclient.RecordSRV, error) {
	return objMgr.UpdateSRVRecord(
		ref,
		strOrEmpty(name),
		uint32OrZero(priority),
		uint32OrZero(weight),
		uint32OrZero(port),
		strOrEmpty(target),
		ttlOrZero(ttl),
		boolOrFalse(useTTL),
		strOrEmpty(comment),
		buildEA(extAttrs),
	)
}

// deleteSRVRecord issues the WAPI delete call.
func deleteSRVRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteSRVRecord(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced SRVRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterSRVRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster SRVRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("SRVRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedSRVRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced SRVRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("SRVRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced SRVRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterSRVRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedSRVRecord(mgr, o)
}
