// Package recordalias implements the Crossplane controller for the
// Infoblox NIOS AliasRecord managed resource. Like the ARecord
// controller, this provider wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateAliasRecord/GetAliasRecordByRef/UpdateAliasRecord/
// DeleteAliasRecord) instead of a generic HTTP request/response envelope,
// so there is no internal REST client to compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordalias

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordalias/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordalias/v1alpha1"
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
	errObserveAliasRec   = "cannot observe AliasRecord"
	errCreateAliasRecord = "cannot create AliasRecord"
	errUpdateAliasRecord = "cannot update AliasRecord"
	errDeleteAliasRecord = "cannot delete AliasRecord"
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
// (and the underlying IBConnector it wraps) from the given credentials.
// The Connector performs HTTP Basic Auth on every request and only
// validates configuration locally — no network round-trip happens until
// the first Observe/Create/Update/Delete call.
//
// The raw IBConnector is also returned (alongside the ObjectManager) so
// Update can issue a partial PUT that omits the immutable `view` field —
// see updateAliasRecord's doc comment for why the generated
// UpdateAliasRecord wrapper cannot be used for that.
func newObjectManager(creds *nioCredentials, sslVerify bool) (ibclient.IBObjectManager, ibclient.IBConnector, error) {
	return newObjectManagerWithScheme(creds, sslVerify, "https", "443")
}

// newObjectManagerWithScheme is the scheme/port-parameterized variant of
// newObjectManager used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newObjectManagerWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (ibclient.IBObjectManager, ibclient.IBConnector, error) {
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
		return nil, nil, errors.Wrap(err, errNewObjectManager)
	}

	return ibclient.NewObjectManager(conn, "", ""), conn, nil
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

// uint32OrZero converts an optional *uint32 TTL into a plain uint32 for
// comparison/marshaling. Unlike ARecord (whose ForProvider.TTL is an
// *int64 requiring range clamping), AliasRecord's ForProvider.TTL is
// already *uint32 — matching the SDK's Ttl field type directly.
func uint32OrZero(ttl *uint32) uint32 {
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
// than a whole AliasRecordParameters value. The cluster and namespaced
// AliasRecordParameters types are structurally identical (same field
// names and primitive types) but are distinct named Go types, so a direct
// struct conversion between them is not always available once other
// resources in this provider grow reference fields; parameterizing on the
// field pointers instead lets both scopes share this logic
// unconditionally.

// isUpToDate compares the desired AliasRecord fields against the observed
// RecordAlias. View is soft-immutable (the WAPI schema advertises it as
// updatable, but a live update attempt is rejected at the data level —
// see the immutable-fields table) and is intentionally excluded from this
// comparison so drift there never triggers an infinite reconcile loop.
func isUpToDate(name, targetName, targetType, comment *string, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordAlias) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(targetName) != strOrEmpty(rec.TargetName) {
		return false
	}
	if strOrEmpty(targetType) != rec.TargetType {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(rec.Disable) {
		return false
	}
	if uint32OrZero(ttl) != uint32OrZero(rec.Ttl) {
		return false
	}
	if boolOrFalse(useTTL) != boolOrFalse(rec.UseTtl) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// disable, ttl, useTtl, extAttrs) from the observed RecordAlias into spec
// so isUpToDate does not see phantom drift on the next reconcile.
// Required fields (name, targetName, targetType) and the immutable view
// field are never late-initialized — they are always user-supplied
// (required on the CRD). Returns true if any field was changed.
func lateInitialize(comment **string, disable **bool, ttl **uint32, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordAlias) bool {
	changed := false

	if *comment == nil && rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		*comment = &c
		changed = true
	}
	if *disable == nil && rec.Disable != nil {
		d := *rec.Disable
		*disable = &d
		changed = true
	}
	if *ttl == nil && rec.Ttl != nil {
		t := *rec.Ttl
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

// observedAliasRecord holds the primitive field values extracted from a
// WAPI RecordAlias response. The cluster and namespaced
// AliasRecordObservation types are structurally similar but are distinct
// named types, so they are not directly convertible — each scope copies
// this intermediate struct's fields into its own Observation type at the
// call site.
type observedAliasRecord struct {
	ID         string
	Name       *string
	TargetName *string
	TargetType *string
	View       *string
	Comment    *string
	Disable    *bool
	TTL        *uint32
	UseTTL     *bool
	ExtAttrs   map[string]string
	Ref        *string
	Zone       *string
}

// observeFromRecordAlias extracts the fields mirrored by
// AliasRecordObservation (the full-mirror AtProvider convention) from a
// WAPI RecordAlias response. NewEmptyAliasRecord requests extattrs,
// cloud_info, comment, disable, dns_name, dns_target_name, creator, ttl,
// use_ttl, view, and zone in addition to the default name/target_name/
// target_type/view fields — response-only fields outside that set
// (creator, cloud_info, aws_rte53_record_info, etc.) are not mirrored
// into AtProvider by this provider.
func observeFromRecordAlias(externalID string, rec *ibclient.RecordAlias) observedAliasRecord {
	o := observedAliasRecord{
		ID:         externalID,
		Name:       rec.Name,
		TargetName: rec.TargetName,
		ExtAttrs:   extAttrsFromEA(rec.Ea),
	}
	if rec.TargetType != "" {
		tt := rec.TargetType
		o.TargetType = &tt
	}
	if rec.View != nil && *rec.View != "" {
		v := *rec.View
		o.View = &v
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Disable != nil {
		d := *rec.Disable
		o.Disable = &d
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

// createAliasRecord issues the WAPI create call.
func createAliasRecord(objMgr ibclient.IBObjectManager, name, view, targetName, targetType, comment *string, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*ibclient.RecordAlias, error) {
	return objMgr.CreateAliasRecord(
		strOrEmpty(name),
		strOrEmpty(view),
		strOrEmpty(targetName),
		strOrEmpty(targetType),
		strOrEmpty(comment),
		boolOrFalse(disable),
		buildEA(extAttrs),
		uint32OrZero(ttl),
		boolOrFalse(useTTL),
	)
}

// updateAliasRecord issues a partial WAPI PUT against the record's _ref,
// bypassing the generated ObjectManager.UpdateAliasRecord wrapper.
//
// Unlike ARecord's UpdateARecord (which has no view parameter at all,
// naturally excluding it from the wire body), UpdateAliasRecord always
// takes a dnsView argument and unconditionally marshals it as "view" on
// every PUT. The immutable-fields contract requires immutable fields to
// never appear in the update request body, even when the API would
// silently ignore them, so the generated wrapper cannot be used here.
// Building the RecordAlias payload by hand (leaving View nil, which the
// `omitempty` JSON tag then drops) and issuing the PUT directly through
// the IBConnector this ObjectManager wraps is the only way to honor that
// contract with this SDK.
func updateAliasRecord(conn ibclient.IBConnector, ref string, name, targetName, targetType, comment *string, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string) (string, error) {
	n := strOrEmpty(name)
	tn := strOrEmpty(targetName)
	c := strOrEmpty(comment)
	d := boolOrFalse(disable)
	t := uint32OrZero(ttl)
	u := boolOrFalse(useTTL)

	rec := &ibclient.RecordAlias{
		Name:       &n,
		TargetName: &tn,
		TargetType: strOrEmpty(targetType),
		Comment:    &c,
		Disable:    &d,
		Ttl:        &t,
		UseTtl:     &u,
		Ea:         buildEA(extAttrs),
	}

	return conn.UpdateObject(rec, ref)
}

// deleteAliasRecord issues the WAPI delete call.
func deleteAliasRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteAliasRecord(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced AliasRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterAliasRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster AliasRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("AliasRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedAliasRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced AliasRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("AliasRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced AliasRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterAliasRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedAliasRecord(mgr, o)
}
