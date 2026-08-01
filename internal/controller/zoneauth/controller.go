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
//
// ZoneAuth is wired to the UID-in-EA object-identity ladder (see the
// recorda/ARecord controller, the pilot resource, and the
// internal/clients/identity package doc for the full rationale). There
// is no ibclient.NewEmptyZoneAuth in the SDK, so this package's own
// newZoneAuthForGet (already used for the direct-GET path) doubles as
// the newEmpty constructor identity.Resolve needs.
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
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage              = "cannot track ProviderConfig usage"
	errGetPC                     = "cannot get ProviderConfig"
	errGetClusterPC              = "cannot get ClusterProviderConfig"
	errUnsupportedKind           = "unsupported provider config kind"
	errGetSecret                 = "cannot get credentials secret"
	errNoSecretRef               = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds          = "unsupported credentials source: only Secret is supported"
	errMissingCredKey            = "credentials secret is missing one of the required host/username/password keys"
	errNewConnector              = "cannot create Infoblox NIOS WAPI connector"
	errObserveZoneAuth           = "cannot observe ZoneAuth"
	errCreateZoneAuth            = "cannot create ZoneAuth"
	errUpdateZoneAuth            = "cannot update ZoneAuth"
	errDeleteZoneAuth            = "cannot delete ZoneAuth"
	errEmptyRef                  = "empty reference to an object is not allowed"
	errEmptyUID                  = "cannot stamp ZoneAuth identity: managed resource's metadata.uid is empty"
	errDeleteUnverifiedOwnership = "refusing to delete: the resolved object's identity extensible attribute is absent or belongs to a different owner, so ownership cannot be verified before an irreversible delete. " +
		"Reconcile the external-name annotation, verify the Grid object manually, or remove the finalizer to abandon it without deleting."
	errPrerequisiteCheck = "cannot verify the identity extensible attribute definition prerequisite"
)

// unresolvedProbeEndpoint is the identity-prerequisite-probe cache key
// used when an ExternalClient is built without a resolved Grid endpoint.
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

// newConnector constructs an authenticated ibclient.IBConnector from the
// given credentials. The Connector performs HTTP Basic Auth on every
// request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newConnector(creds *nioCredentials, sslVerify bool) (ibclient.IBConnector, error) {
	return newConnectorWithScheme(creds, sslVerify, "https", "443")
}

// newConnectorWithScheme is the scheme/port-parameterized variant of
// newConnector used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newConnectorWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (ibclient.IBConnector, error) {
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

// gatedUint32Equal compares desired and observed *uint32 values only when
// useFlag is on. When useFlag is off, the API echoes back a grid/parent-
// inherited default rather than something the user's spec can drive, so
// the two sides are unrelated quantities and always report equal — the
// flag's own (unconditional) comparator is what actually detects drift
// on the flag itself.
func gatedUint32Equal(useFlag *bool, desired, observed *uint32) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return uint32OrZero(desired) == uint32OrZero(observed)
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
	for i, av := range a {
		bv := b[i] //nolint:gosec // length-checked above, index is always in range
		if av.Name != bv.Name ||
			av.Stealth != bv.Stealth ||
			av.GridReplicate != bv.GridReplicate ||
			av.Lead != bv.Lead {
			return false
		}
		// EnablePreferredPrimaries represents whether the
		// PreferredPrimaries field values of this member are used —
		// semantically a use flag even though it is not named use_*.
		// Compare the flag first and unconditionally, so a true ->
		// false transition is still detected as drift.
		if av.EnablePreferredPrimaries != bv.EnablePreferredPrimaries {
			return false
		}
		// Only compare PreferredPrimaries when the flag is on — same
		// pattern as ExternalServer.UseTsigKeyName gating TsigKeyName
		// in externalServerValueEqual.
		if av.EnablePreferredPrimaries && !externalServerValuesEqual(av.PreferredPrimaries, bv.PreferredPrimaries) {
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
		if !externalServerValueEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// externalServerValueEqual compares two externalServerValue items. The
// SDK's NameServer type documents use_tsig_key_name as the use flag for
// tsig_key_name: when it is off, tsig_key_name is not something the user's
// spec can drive (the appliance does not apply it to this external
// server), so the two sides are unrelated quantities and comparing them
// unconditionally can never converge.
func externalServerValueEqual(a, b externalServerValue) bool {
	if a.Address != b.Address ||
		a.Name != b.Name ||
		a.Stealth != b.Stealth ||
		a.SharedWithMsParentDelegation != b.SharedWithMsParentDelegation ||
		a.TsigKey != b.TsigKey ||
		a.TsigKeyAlg != b.TsigKeyAlg {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if a.UseTsigKeyName != b.UseTsigKeyName {
		return false
	}
	// Only compare tsig_key_name when the flag is on.
	if a.UseTsigKeyName {
		if a.TsigKeyName != b.TsigKeyName {
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
//
// Use-flag audit: the SDK's zone_auth object documents 18 "Use flag for:"
// fields. use_grid_zone_timer (gating the five SOA timer fields below) is
// the only one whose paired value field(s) are modelled in this CRD —
// every other flag's paired value(s) (allow_active_dir, allow_query,
// allow_transfer, allow_update, allow_update_forwarding,
// copy_xfer_to_notify, ddns_force_creation_timestamp_update,
// ddns_restrict_patterns/ddns_restrict_patterns_list,
// ddns_restrict_secure/ddns_principal_tracking/ddns_principal_group,
// ddns_restrict_protected, ddns_restrict_static, dnssec_key_params,
// import_from, notify_delay, record_name_policy,
// scavenging_settings/last_queried_acl, soa_email) are absent from
// ZoneAuthParameters entirely, so there is nothing for those flags to
// gate here. use_tsig_key_name (ExternalServer) and
// enable_preferred_primaries (MemberServer, semantically a use flag
// though not use_*-named) are separately modelled and gated in
// externalServerValueEqual/memberServerValuesEqual above.
type zoneAuthFields struct {
	FQDN           string
	View           *string // nil = unset (let WAPI apply the Grid's default view)
	ZoneFormat     string
	Comment        *string
	Disable        *bool
	SoaDefaultTTL  *uint32
	SoaExpire      *uint32
	SoaNegativeTTL *uint32
	SoaRefresh     *uint32
	SoaRetry       *uint32
	// UseGridZoneTimer is the SDK-documented use flag for SoaDefaultTTL,
	// SoaExpire, SoaNegativeTTL, SoaRefresh, and SoaRetry. When false, the
	// zone inherits the Grid's SOA timer settings and the appliance
	// echoes back the Grid's values instead of what was submitted — see
	// isUpToDate and lateInitializeScalars for how the five SOA fields
	// are gated on it. The wire builders (buildZoneAuthForCreate/
	// buildZoneAuthForUpdate) never submit this field's literal value
	// as-is — see effectiveUseGridZoneTimer, which forces it on whenever
	// any of the five gated fields is set, so a user setting one of them
	// without also setting this flag is never silently ineffective.
	UseGridZoneTimer    *bool
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
		UseGridZoneTimer:    rec.UseGridZoneTimer,
		NsGroup:             rec.NsGroup,
		ExtAttrs:            extAttrsFromEA(rec.Ea),
		GridPrimary:         memberServerValuesFromSDK(rec.GridPrimary),
		GridSecondaries:     memberServerValuesFromSDK(rec.GridSecondaries),
		ExternalPrimaries:   externalServerValuesFromSDK(rec.ExternalPrimaries),
		ExternalSecondaries: externalServerValuesFromSDK(rec.ExternalSecondaries),
	}
}

// anySOATimerFieldSet reports whether any of the five use_grid_zone_timer
// -gated SOA fields is set on f. WAPI only honors soa_default_ttl,
// soa_expire, soa_negative_ttl, soa_refresh, and soa_retry when
// use_grid_zone_timer is on — a zone with the flag off inherits the
// Grid's timer values and ignores whatever was submitted for these five
// fields. Setting any one of them therefore only has an effect once the
// flag is (or is forced) on.
func anySOATimerFieldSet(f zoneAuthFields) bool {
	return f.SoaDefaultTTL != nil ||
		f.SoaExpire != nil ||
		f.SoaNegativeTTL != nil ||
		f.SoaRefresh != nil ||
		f.SoaRetry != nil
}

// effectiveUseGridZoneTimer resolves the use_grid_zone_timer value that
// will actually be submitted to WAPI for f: forced on whenever any of the
// five gated SOA fields is set (regardless of what f.UseGridZoneTimer
// itself says — explicit false or unset), otherwise f.UseGridZoneTimer
// unchanged. buildZoneAuthForCreate/buildZoneAuthForUpdate use this to
// build the wire payload; isUpToDate and lateInitializeScalars use it too
// so their gating always matches what was (or will be) sent on the wire —
// comparing against the raw, unforced field would otherwise detect
// permanent drift (or silently ignore real drift) the moment a user set a
// soa_* field without also setting use_grid_zone_timer: true.
func effectiveUseGridZoneTimer(f zoneAuthFields) *bool {
	if anySOATimerFieldSet(f) {
		t := true
		return &t
	}
	return f.UseGridZoneTimer
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
	// use_grid_zone_timer is the SDK-documented use flag for all five
	// SOA timer fields — compare it first and unconditionally, so a
	// true -> false (or false -> true) transition on the flag itself is
	// always detected as drift regardless of what the SOA fields say.
	// desiredUseGridZoneTimer is the *effective* value (see
	// effectiveUseGridZoneTimer): once any soa_* field is set, the wire
	// builders force the flag on, so comparing against the raw,
	// unforced field would either loop forever (if desired explicitly
	// says false) or mask real drift (if desired never sets the flag).
	desiredUseGridZoneTimer := effectiveUseGridZoneTimer(desired)
	if boolOrFalse(desiredUseGridZoneTimer) != boolOrFalse(observed.UseGridZoneTimer) {
		return false
	}
	// When the flag is off, the zone inherits the Grid's timer settings
	// and the appliance echoes back the Grid's values rather than what
	// was submitted — the two sides are unrelated quantities, so only
	// compare the SOA fields when the flag is (or will become) on.
	if !gatedUint32Equal(desiredUseGridZoneTimer, desired.SoaDefaultTTL, observed.SoaDefaultTTL) {
		return false
	}
	if !gatedUint32Equal(desiredUseGridZoneTimer, desired.SoaExpire, observed.SoaExpire) {
		return false
	}
	if !gatedUint32Equal(desiredUseGridZoneTimer, desired.SoaNegativeTTL, observed.SoaNegativeTTL) {
		return false
	}
	if !gatedUint32Equal(desiredUseGridZoneTimer, desired.SoaRefresh, observed.SoaRefresh) {
		return false
	}
	if !gatedUint32Equal(desiredUseGridZoneTimer, desired.SoaRetry, observed.SoaRetry) {
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
	// UseGridZoneTimer itself always back-fills unconditionally (it is
	// the gate, not a gated value). The five SOA fields it gates only
	// back-fill when the flag is (or will become) on — see
	// effectiveUseFlag: back-filling a Grid-inherited SOA value into
	// spec while the flag is off would silently claim a setting the
	// zone does not actually have in effect.
	changed = lateInitPtr(&desired.UseGridZoneTimer, observed.UseGridZoneTimer) || changed
	// effectiveDesiredUseGridZoneTimer is captured once, before the
	// gated back-fills below run, using the same wire-forcing semantics
	// as effectiveUseGridZoneTimer/the wire builders: if the caller has
	// already set any of the five gated SOA fields, the flag is treated
	// as on regardless of its own literal value (nil or explicit
	// false), matching what buildZoneAuthForCreate/buildZoneAuthForUpdate
	// will actually submit. gatedLateInitPtr still falls back to the
	// observed flag when neither the flag nor any SOA field is set on
	// desired (see effectiveUseFlag).
	effectiveDesiredUseGridZoneTimer := effectiveUseGridZoneTimer(desired)
	changed = gatedLateInitPtr(effectiveDesiredUseGridZoneTimer, observed.UseGridZoneTimer, &desired.SoaDefaultTTL, observed.SoaDefaultTTL) || changed
	changed = gatedLateInitPtr(effectiveDesiredUseGridZoneTimer, observed.UseGridZoneTimer, &desired.SoaExpire, observed.SoaExpire) || changed
	changed = gatedLateInitPtr(effectiveDesiredUseGridZoneTimer, observed.UseGridZoneTimer, &desired.SoaNegativeTTL, observed.SoaNegativeTTL) || changed
	changed = gatedLateInitPtr(effectiveDesiredUseGridZoneTimer, observed.UseGridZoneTimer, &desired.SoaRefresh, observed.SoaRefresh) || changed
	changed = gatedLateInitPtr(effectiveDesiredUseGridZoneTimer, observed.UseGridZoneTimer, &desired.SoaRetry, observed.SoaRetry) || changed
	changed = lateInitStringPtr(&desired.NsGroup, observed.NsGroup) || changed
	if len(desired.ExtAttrs) == 0 && len(observed.ExtAttrs) > 0 {
		desired.ExtAttrs = observed.ExtAttrs
		changed = true
	}

	return desired, changed
}

// effectiveUseFlag resolves what UseGridZoneTimer's value will be once
// lateInitializeFields has finished: the user's own spec value if they
// set one, otherwise the value that will be back-filled from observed.
// Both the flag's own late-init op and every SOA field it gates read
// through this helper so the gate does not depend on which op happens to
// run first.
func effectiveUseFlag(desiredFlag, observedFlag *bool) bool {
	if desiredFlag != nil {
		return *desiredFlag
	}
	return boolOrFalse(observedFlag)
}

// gatedLateInitPtr back-fills *desired from observed only when the
// gating use flag is (or will become) true. When the flag is off, the
// observed value is the Grid's inherited default rather than something
// the user's spec implies — writing it into spec would silently claim a
// setting that is not actually in effect.
func gatedLateInitPtr[T any](useFlagDesired, useFlagObserved *bool, desired **T, observed *T) bool {
	if !effectiveUseFlag(useFlagDesired, useFlagObserved) {
		return false
	}
	return lateInitPtr(desired, observed)
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
		"use_grid_zone_timer",
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
//
// UseGridZoneTimer is sent via effectiveUseGridZoneTimer rather than
// f.UseGridZoneTimer directly: if any of the five soa_* fields is set, the
// flag is forced on regardless of what the caller wrote (or left unset)
// for it. WAPI silently ignores soa_default_ttl/soa_expire/
// soa_negative_ttl/soa_refresh/soa_retry while use_grid_zone_timer is off
// — a zone would otherwise inherit the Grid's timer values with no error
// and no drift signal, even though the user explicitly configured them.
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
		UseGridZoneTimer:    effectiveUseGridZoneTimer(f),
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
//
// UseGridZoneTimer uses effectiveUseGridZoneTimer for the same reason as
// buildZoneAuthForCreate — see its doc comment.
func buildZoneAuthForUpdate(f zoneAuthFields) *ibclient.ZoneAuth {
	z := &ibclient.ZoneAuth{
		Comment:             f.Comment,
		Disable:             f.Disable,
		SoaDefaultTtl:       f.SoaDefaultTTL,
		SoaExpire:           f.SoaExpire,
		SoaNegativeTtl:      f.SoaNegativeTTL,
		SoaRefresh:          f.SoaRefresh,
		SoaRetry:            f.SoaRetry,
		UseGridZoneTimer:    effectiveUseGridZoneTimer(f),
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
// returns the server-assigned _ref. Stamps the owning managed resource's
// uid into the object's extensible attributes in the same request that
// creates it (identity.Stamp).
func createZoneAuth(conn ibclient.IBConnector, f zoneAuthFields, uid string) (string, error) {
	if uid == "" {
		return "", errors.New(errEmptyUID)
	}
	z := buildZoneAuthForCreate(f)
	z.Ea = identity.Stamp(z.Ea, uid)
	return conn.CreateObject(z)
}

// updateZoneAuth issues a direct WAPI PUT against ref with only the
// mutable zone_auth fields (see buildZoneAuthForUpdate). Returns the
// object's current _ref — per the blueprint's _ref-stability note, this
// always equals ref, since every field the _ref is derived from
// (fqdn/view/zone_format) is immutable. Every call re-asserts the
// identity stamp since a WAPI PUT carrying extattrs replaces the whole
// map rather than merging it.
func updateZoneAuth(conn ibclient.IBConnector, ref string, f zoneAuthFields, uid string) (string, error) {
	if uid == "" {
		return "", errors.New(errEmptyUID)
	}
	z := buildZoneAuthForUpdate(f)
	z.Ea = identity.Stamp(z.Ea, uid)
	return conn.UpdateObject(z, ref)
}

// deleteZoneAuth issues a direct WAPI DELETE for the zone_auth object
// identified by ref.
func deleteZoneAuth(conn ibclient.IBConnector, ref string) error {
	_, err := conn.DeleteObject(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────

// ensureIdentityPrerequisite probes the Grid for the identity extensible
// attribute definition before any call that stamps identity onto a new
// object. See recorda's ensureIdentityPrerequisite for the full
// rationale.
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
// first for a managed resource's stored external-name.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveZoneAuthIdentity resolves the ZoneAuth identified by ref/uid
// through the shared UID-in-EA ladder. There is no
// ibclient.NewEmptyZoneAuth in the SDK, so newZoneAuthForGet (already
// used by the direct-GET path) doubles as the newEmpty constructor.
func resolveZoneAuthIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.ZoneAuth, identity.Outcome, error) {
	return identity.Resolve[*ibclient.ZoneAuth](ctx, conn, newZoneAuthForGet, ref, uid)
}

// deleteZoneAuthIdentity issues the WAPI delete for the ZoneAuth this
// managed resource owns, resolving through the identity ladder first so
// a stale _ref is never mistaken for a deleted object. See recorda's
// deleteARecordIdentity doc for the full ownership-verification rules.
func deleteZoneAuthIdentity(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveZoneAuthIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteZoneAuth)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteZoneAuth(conn, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteZoneAuth)
	default:
		return errors.New("identity: unresolved ZoneAuth outcome")
	}
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
