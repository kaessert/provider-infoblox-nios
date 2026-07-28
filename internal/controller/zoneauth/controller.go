// Package zoneauth implements the Crossplane controller for the Infoblox
// NIOS ZoneAuth managed resource (authoritative DNS zones, WAPI object
// type zone_auth).
//
// Unlike the ARecord controller, this package talks to the WAPI through
// the raw ibclient.IBConnector (CreateObject/GetObject/UpdateObject/
// DeleteObject) instead of the SDK's ObjectManager convenience wrappers.
// The ObjectManager's CreateZoneAuth helper only accepts fqdn+extattrs,
// and there is no UpdateZoneAuth wrapper at all — the zone_auth object
// supports far more fields (view, zone_format, SOA settings, grid/external
// primaries and secondaries, ...) than the wrapper methods expose, so
// this controller builds ibclient.ZoneAuth values directly and issues
// WAPI calls through the Connector.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared WAPI plumbing, field comparison, and late-init logic lives here.
package zoneauth

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/zoneauth/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/zoneauth/v1alpha1"
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
	errNewConnector     = "cannot create Infoblox NIOS WAPI connector"
	errObserveZoneAuth  = "cannot observe ZoneAuth"
	errCreateZoneAuth   = "cannot create ZoneAuth"
	errUpdateZoneAuth   = "cannot update ZoneAuth"
	errDeleteZoneAuth   = "cannot delete ZoneAuth"
	errEmptyRef         = "empty reference to an object is not allowed"
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

// newConnector constructs an authenticated ibclient.IBConnector from the
// given credentials. The Connector performs HTTP Basic Auth on every
// request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newConnector(creds *nioCredentials) (ibclient.IBConnector, error) {
	return newConnectorWithScheme(creds, "https", "443")
}

// newConnectorWithScheme is the scheme/port-parameterized variant of
// newConnector used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newConnectorWithScheme(creds *nioCredentials, scheme, port string) (ibclient.IBConnector, error) {
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
		return nil, errors.Wrap(err, errNewConnector)
	}

	return conn, nil
}

// ── primitive translation helpers (shared by both scopes) ──────────────────

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

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func strPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func boolOrFalse(b *bool) bool {
	if b == nil {
		return false
	}
	return *b
}

func uint32OrZero(u *uint32) uint32 {
	if u == nil {
		return 0
	}
	return *u
}

// ── error classification ─────────────────────────────────────────────────

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

// ── MemberServer / ExternalServer value bags ────────────────────────────
//
// The cluster and namespaced MemberServer/ExternalServer CRD types are
// structurally identical (same field names and primitive pointer types)
// but are distinct named Go types generated per-scope, so they are not
// directly convertible. These flat, non-pointer value structs are the
// common currency both scopes convert to/from before calling into the
// shared SDK-facing helpers below — nil-vs-zero-value ambiguity is
// resolved once, at the point each scope extracts values from its own CRD
// type (see clusterMemberServerValues/namespacedMemberServerValues etc. in
// cluster.go/namespaced.go).

type memberServerValue struct {
	Name                     string
	Stealth                  bool
	GridReplicate            bool
	Lead                     bool
	PreferredPrimaries       []externalServerValue
	EnablePreferredPrimaries bool
}

type externalServerValue struct {
	Address                      string
	Name                         string
	Stealth                      bool
	SharedWithMsParentDelegation bool
	TsigKey                      string
	TsigKeyAlg                   string
	TsigKeyName                  string
	UseTsigKeyName               bool
}

func memberServerValuesEqual(a, b []memberServerValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name ||
			a[i].Stealth != b[i].Stealth ||
			a[i].GridReplicate != b[i].GridReplicate ||
			a[i].Lead != b[i].Lead ||
			a[i].EnablePreferredPrimaries != b[i].EnablePreferredPrimaries ||
			!externalServerValuesEqual(a[i].PreferredPrimaries, b[i].PreferredPrimaries) {
			return false
		}
	}
	return true
}

func externalServerValuesEqual(a, b []externalServerValue) bool {
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

// memberServerValuesFromSDK converts the SDK's []*ibclient.Memberserver
// (as returned in a GET response) into the flat value-bag representation.
func memberServerValuesFromSDK(in []*ibclient.Memberserver) []memberServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]memberServerValue, 0, len(in))
	for _, m := range in {
		if m == nil {
			continue
		}
		out = append(out, memberServerValue{
			Name:                     m.Name,
			Stealth:                  m.Stealth,
			GridReplicate:            m.GridReplicate,
			Lead:                     m.Lead,
			PreferredPrimaries:       externalServerValuesFromSDK(m.PreferredPrimaries),
			EnablePreferredPrimaries: m.EnablePreferredPrimaries,
		})
	}
	return out
}

// memberServerValuesToSDK converts the flat value-bag representation into
// the SDK's []*ibclient.Memberserver, for use in Create/Update request
// bodies.
func memberServerValuesToSDK(in []memberServerValue) []*ibclient.Memberserver {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Memberserver, 0, len(in))
	for _, v := range in {
		out = append(out, &ibclient.Memberserver{
			Name:                     v.Name,
			Stealth:                  v.Stealth,
			GridReplicate:            v.GridReplicate,
			Lead:                     v.Lead,
			PreferredPrimaries:       externalServerValuesToSDK(v.PreferredPrimaries),
			EnablePreferredPrimaries: v.EnablePreferredPrimaries,
		})
	}
	return out
}

// externalServerValuesFromSDK converts the SDK's []ibclient.NameServer
// into the flat value-bag representation.
func externalServerValuesFromSDK(in []ibclient.NameServer) []externalServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]externalServerValue, 0, len(in))
	for _, n := range in {
		out = append(out, externalServerValue{
			Address:                      n.Address,
			Name:                         n.Name,
			Stealth:                      n.Stealth,
			SharedWithMsParentDelegation: n.SharedWithMsParentDelegation,
			TsigKey:                      n.TsigKey,
			TsigKeyAlg:                   n.TsigKeyAlg,
			TsigKeyName:                  n.TsigKeyName,
			UseTsigKeyName:               n.UseTsigKeyName,
		})
	}
	return out
}

// externalServerValuesToSDK converts the flat value-bag representation
// into the SDK's []ibclient.NameServer, for use in Create/Update request
// bodies.
func externalServerValuesToSDK(in []externalServerValue) []ibclient.NameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]ibclient.NameServer, 0, len(in))
	for _, v := range in {
		out = append(out, ibclient.NameServer{
			Address:                      v.Address,
			Name:                         v.Name,
			Stealth:                      v.Stealth,
			SharedWithMsParentDelegation: v.SharedWithMsParentDelegation,
			TsigKey:                      v.TsigKey,
			TsigKeyAlg:                   v.TsigKeyAlg,
			TsigKeyName:                  v.TsigKeyName,
			UseTsigKeyName:               v.UseTsigKeyName,
		})
	}
	return out
}

// ── ZoneAuth field bag (shared spec/observation currency) ───────────────
//
// zoneAuthFields holds every ZoneAuthParameters/ZoneAuthObservation field
// in its flat, scope-neutral form. Both cluster.go and namespaced.go
// convert their scope-specific ForProvider struct into this bag (and back
// out into their scope-specific Observation struct), so all comparison,
// late-init, request-building, and response-parsing logic lives here
// exactly once.
type zoneAuthFields struct {
	FQDN                string
	View                *string // nil = unset (let WAPI apply the Grid's default view)
	ZoneFormat          string
	Comment             *string
	Disable             *bool
	SoaDefaultTTL       *uint32
	SoaExpire           *uint32
	SoaNegativeTTL      *uint32
	SoaRefresh          *uint32
	SoaRetry            *uint32
	NsGroup             *string
	ExtAttrs            map[string]string
	GridPrimary         []memberServerValue
	GridSecondaries     []memberServerValue
	ExternalPrimaries   []externalServerValue
	ExternalSecondaries []externalServerValue
}

// fieldsFromZoneAuth extracts the full-mirror field bag from a WAPI
// ZoneAuth response.
func fieldsFromZoneAuth(rec *ibclient.ZoneAuth) zoneAuthFields {
	return zoneAuthFields{
		FQDN:                rec.Fqdn,
		View:                rec.View,
		ZoneFormat:          rec.ZoneFormat,
		Comment:             rec.Comment,
		Disable:             rec.Disable,
		SoaDefaultTTL:       rec.SoaDefaultTtl,
		SoaExpire:           rec.SoaExpire,
		SoaNegativeTTL:      rec.SoaNegativeTtl,
		SoaRefresh:          rec.SoaRefresh,
		SoaRetry:            rec.SoaRetry,
		NsGroup:             rec.NsGroup,
		ExtAttrs:            extAttrsFromEA(rec.Ea),
		GridPrimary:         memberServerValuesFromSDK(rec.GridPrimary),
		GridSecondaries:     memberServerValuesFromSDK(rec.GridSecondaries),
		ExternalPrimaries:   externalServerValuesFromSDK(rec.ExternalPrimaries),
		ExternalSecondaries: externalServerValuesFromSDK(rec.ExternalSecondaries),
	}
}

// isUpToDate compares the desired ZoneAuth fields against the observed
// ones. FQDN, View, and ZoneFormat are immutable (WAPI rejects a PUT that
// changes any of them — see the package doc comment and the blueprint's
// immutable-fields table) and are intentionally excluded here: including
// them would trigger a permanent Update loop the moment spec drifted from
// the value WAPI silently pinned at creation.
func isUpToDate(desired, observed zoneAuthFields) bool {
	if strOrEmpty(desired.Comment) != strOrEmpty(observed.Comment) {
		return false
	}
	if boolOrFalse(desired.Disable) != boolOrFalse(observed.Disable) {
		return false
	}
	if uint32OrZero(desired.SoaDefaultTTL) != uint32OrZero(observed.SoaDefaultTTL) {
		return false
	}
	if uint32OrZero(desired.SoaExpire) != uint32OrZero(observed.SoaExpire) {
		return false
	}
	if uint32OrZero(desired.SoaNegativeTTL) != uint32OrZero(observed.SoaNegativeTTL) {
		return false
	}
	if uint32OrZero(desired.SoaRefresh) != uint32OrZero(observed.SoaRefresh) {
		return false
	}
	if uint32OrZero(desired.SoaRetry) != uint32OrZero(observed.SoaRetry) {
		return false
	}
	if strOrEmpty(desired.NsGroup) != strOrEmpty(observed.NsGroup) {
		return false
	}
	if !extAttrsEqual(desired.ExtAttrs, observed.ExtAttrs) {
		return false
	}
	if !memberServerValuesEqual(desired.GridPrimary, observed.GridPrimary) {
		return false
	}
	if !memberServerValuesEqual(desired.GridSecondaries, observed.GridSecondaries) {
		return false
	}
	if !externalServerValuesEqual(desired.ExternalPrimaries, observed.ExternalPrimaries) {
		return false
	}
	if !externalServerValuesEqual(desired.ExternalSecondaries, observed.ExternalSecondaries) {
		return false
	}
	return true
}

// lateInitializeFields back-fills server-defaulted mutable fields from the
// observed ZoneAuth into the desired field bag (the caller writes the
// result back into the scope-specific ForProvider struct), so isUpToDate
// does not see phantom drift on the next reconcile. FQDN (required),
// View, and ZoneFormat (both immutable) are intentionally never
// late-initialized — filling in View from the server's default-view
// assignment would turn a single Observe call into a spec mutation that
// then has to satisfy the "immutable after creation" CEL rule on every
// subsequent update, which is unnecessary risk for a field the immutable-
// fields table already documents as excluded from update. Returns the
// updated bag and whether anything changed.
func lateInitializeFields(desired zoneAuthFields, observed zoneAuthFields) (zoneAuthFields, bool) {
	desired, scalarsChanged := lateInitializeScalars(desired, observed)
	desired, collectionsChanged := lateInitializeCollections(desired, observed)
	return desired, scalarsChanged || collectionsChanged
}

// lateInitializeScalars back-fills the scalar (non-slice) mutable fields.
// Each field uses lateInitPtr (or lateInitStringPtr, for the two fields
// where an empty string is treated the same as "no server value") so the
// per-field bookkeeping is a single expression rather than a repeated
// if-block — keeping this function's branching low.
func lateInitializeScalars(desired, observed zoneAuthFields) (zoneAuthFields, bool) {
	changed := false
	changed = lateInitStringPtr(&desired.Comment, observed.Comment) || changed
	changed = lateInitPtr(&desired.Disable, observed.Disable) || changed
	changed = lateInitPtr(&desired.SoaDefaultTTL, observed.SoaDefaultTTL) || changed
	changed = lateInitPtr(&desired.SoaExpire, observed.SoaExpire) || changed
	changed = lateInitPtr(&desired.SoaNegativeTTL, observed.SoaNegativeTTL) || changed
	changed = lateInitPtr(&desired.SoaRefresh, observed.SoaRefresh) || changed
	changed = lateInitPtr(&desired.SoaRetry, observed.SoaRetry) || changed
	changed = lateInitStringPtr(&desired.NsGroup, observed.NsGroup) || changed
	if len(desired.ExtAttrs) == 0 && len(observed.ExtAttrs) > 0 {
		desired.ExtAttrs = observed.ExtAttrs
		changed = true
	}

	return desired, changed
}

// lateInitPtr back-fills *desired from observed when desired is unset.
// Used for pointer fields (bool, uint32, ...) where the server's zero
// value is itself a meaningful, back-fillable answer.
func lateInitPtr[T any](desired **T, observed *T) bool {
	if *desired == nil && observed != nil {
		*desired = observed
		return true
	}
	return false
}

// lateInitStringPtr is the string-field variant of lateInitPtr: an empty
// observed string is treated the same as "no server value" (nothing to
// back-fill), matching this provider's convention of omitting empty
// strings from AtProvider mirrors.
func lateInitStringPtr(desired **string, observed *string) bool {
	if *desired == nil && observed != nil && *observed != "" {
		*desired = observed
		return true
	}
	return false
}

// lateInitializeCollections back-fills the slice-valued mutable fields
// (grid/external primaries and secondaries).
func lateInitializeCollections(desired, observed zoneAuthFields) (zoneAuthFields, bool) {
	changed := false

	if len(desired.GridPrimary) == 0 && len(observed.GridPrimary) > 0 {
		desired.GridPrimary = observed.GridPrimary
		changed = true
	}
	if len(desired.GridSecondaries) == 0 && len(observed.GridSecondaries) > 0 {
		desired.GridSecondaries = observed.GridSecondaries
		changed = true
	}
	if len(desired.ExternalPrimaries) == 0 && len(observed.ExternalPrimaries) > 0 {
		desired.ExternalPrimaries = observed.ExternalPrimaries
		changed = true
	}
	if len(desired.ExternalSecondaries) == 0 && len(observed.ExternalSecondaries) > 0 {
		desired.ExternalSecondaries = observed.ExternalSecondaries
		changed = true
	}

	return desired, changed
}

// ── WAPI request builders ────────────────────────────────────────────────

// newZoneAuthForGet builds a ZoneAuth query object requesting every field
// mirrored by ZoneAuthObservation (full-mirror AtProvider convention),
// beyond the SDK's built-in default of {extattrs, fqdn, view}.
func newZoneAuthForGet() *ibclient.ZoneAuth {
	z := ibclient.NewZoneAuth(ibclient.ZoneAuth{})
	z.SetReturnFields(append(z.ReturnFields(),
		"zone_format",
		"comment",
		"disable",
		"soa_default_ttl",
		"soa_expire",
		"soa_negative_ttl",
		"soa_refresh",
		"soa_retry",
		"ns_group",
		"grid_primary",
		"grid_secondaries",
		"external_primaries",
		"external_secondaries",
	))
	return z
}

// buildZoneAuthForCreate builds the WAPI create request body. Unlike
// Update, Create includes the identity fields (fqdn, view, zone_format) —
// they are immutable only in the sense that they cannot be changed by a
// later PUT, not that they are absent from the initial POST.
func buildZoneAuthForCreate(f zoneAuthFields) *ibclient.ZoneAuth {
	z := &ibclient.ZoneAuth{
		Fqdn:                f.FQDN,
		View:                f.View,
		ZoneFormat:          f.ZoneFormat,
		Comment:             f.Comment,
		Disable:             f.Disable,
		SoaDefaultTtl:       f.SoaDefaultTTL,
		SoaExpire:           f.SoaExpire,
		SoaNegativeTtl:      f.SoaNegativeTTL,
		SoaRefresh:          f.SoaRefresh,
		SoaRetry:            f.SoaRetry,
		NsGroup:             f.NsGroup,
		Ea:                  buildEA(f.ExtAttrs),
		GridPrimary:         memberServerValuesToSDK(f.GridPrimary),
		GridSecondaries:     memberServerValuesToSDK(f.GridSecondaries),
		ExternalPrimaries:   externalServerValuesToSDK(f.ExternalPrimaries),
		ExternalSecondaries: externalServerValuesToSDK(f.ExternalSecondaries),
	}
	return z
}

// buildZoneAuthForUpdate builds the WAPI PUT request body. WAPI's zone_auth
// PUT is a partial merge (only included fields change), and fqdn/view/
// zone_format all have `supports=rws` (no `u`) or are rejected at the data
// level (view — "Cannot move zones between views") — so this builder
// intentionally leaves them at their Go zero value, which the SDK's
// `omitempty` tags then exclude from the marshaled JSON body entirely.
func buildZoneAuthForUpdate(f zoneAuthFields) *ibclient.ZoneAuth {
	z := &ibclient.ZoneAuth{
		Comment:             f.Comment,
		Disable:             f.Disable,
		SoaDefaultTtl:       f.SoaDefaultTTL,
		SoaExpire:           f.SoaExpire,
		SoaNegativeTtl:      f.SoaNegativeTTL,
		SoaRefresh:          f.SoaRefresh,
		SoaRetry:            f.SoaRetry,
		NsGroup:             f.NsGroup,
		Ea:                  buildEA(f.ExtAttrs),
		GridPrimary:         memberServerValuesToSDK(f.GridPrimary),
		GridSecondaries:     memberServerValuesToSDK(f.GridSecondaries),
		ExternalPrimaries:   externalServerValuesToSDK(f.ExternalPrimaries),
		ExternalSecondaries: externalServerValuesToSDK(f.ExternalSecondaries),
	}
	return z
}

// ── WAPI call wrappers (shared by both scopes) ──────────────────────────

// getZoneAuthByRef issues a direct WAPI GET for the zone_auth object
// identified by ref, requesting every field mirrored by
// ZoneAuthObservation.
func getZoneAuthByRef(conn ibclient.IBConnector, ref string) (*ibclient.ZoneAuth, error) {
	if ref == "" {
		return nil, errors.New(errEmptyRef)
	}
	z := newZoneAuthForGet()
	if err := conn.GetObject(z, ref, ibclient.NewQueryParams(false, nil), z); err != nil {
		return nil, err
	}
	return z, nil
}

// createZoneAuth issues a direct WAPI POST for a new zone_auth object and
// returns the server-assigned _ref.
func createZoneAuth(conn ibclient.IBConnector, f zoneAuthFields) (string, error) {
	return conn.CreateObject(buildZoneAuthForCreate(f))
}

// updateZoneAuth issues a direct WAPI PUT against ref with only the
// mutable zone_auth fields (see buildZoneAuthForUpdate). Returns the
// object's current _ref — per the blueprint's _ref-stability note, this
// always equals ref, since every field the _ref is derived from
// (fqdn/view/zone_format) is immutable.
func updateZoneAuth(conn ibclient.IBConnector, ref string, f zoneAuthFields) (string, error) {
	return conn.UpdateObject(buildZoneAuthForUpdate(f), ref)
}

// deleteZoneAuth issues a direct WAPI DELETE for the zone_auth object
// identified by ref.
func deleteZoneAuth(conn ibclient.IBConnector, ref string) error {
	_, err := conn.DeleteObject(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced ZoneAuth
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterZoneAuth(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster ZoneAuth controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("ZoneAuth"))

	o.Gate.Register(func() {
		if err := setupNamespacedZoneAuth(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced ZoneAuth controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("ZoneAuth"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced ZoneAuth
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterZoneAuth(mgr, o); err != nil {
		return err
	}
	return setupNamespacedZoneAuth(mgr, o)
}
