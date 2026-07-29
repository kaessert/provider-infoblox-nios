// Package dtcpool implements the Crossplane controller for the Infoblox
// NIOS DTCPool managed resource (WAPI object type `dtc:pool`).
//
// The SDK's ObjectManager.CreateDtcPool/UpdateDtcPool convenience wrappers
// accept `monitors []Monitor` entries shaped as {Name, Type} pairs and a
// `servers []*DtcServerLink` slice whose `Server` field is expected to
// hold a server *name* — both wrappers resolve the real WAPI `_ref` via an
// extra name+type/name lookup HTTP round trip. This provider's DTCPool CRD
// stores those references directly (the user supplies an existing
// DTCServer/monitor object's `_ref`, resolved ahead of time by the
// generated cross-resource reference resolver — see
// zz_generated.resolvers.go), so this package builds the DtcPool SDK
// struct itself and issues Create/Update through the lower-level
// ibclient.IBConnector returned alongside the ObjectManager — bypassing
// the name-based lookups entirely, mirroring the precedent already
// established by the dtcserver package for the same reason. Delete has
// no such mismatch, so it uses the ObjectManager's DeleteDtcPool
// convenience method directly.
//
// The WAPI `auto_consolidated_monitors` attribute does not exist on this
// deployment's Grid Manager (live-verified) — this package never sets it,
// which also means the SDK's DtcPool.MarshalJSON never emits a
// `consolidated_monitors` field in outgoing requests (its wrapper logic
// is keyed off that pointer being non-nil). ConsolidatedMonitors is
// therefore treated as a read-only, response-only field: mirrored into
// AtProvider but never sent on Create/Update.
//
// Read (by ref) has its own, separate mismatch with this attribute: the
// SDK's ObjectManager.GetDtcPoolByRef convenience method (via
// NewEmptyDtcPool) unconditionally adds `auto_consolidated_monitors` to
// the WAPI `_return_fields` list on every GET, and this deployment's WAPI
// schema rejects the unrecognized field with a 400 — failing the GET
// itself, not just omitting that one field from the response. This
// package therefore never calls GetDtcPoolByRef; getDtcPoolByRef below
// issues the same GET through the low-level IBConnector with a trimmed
// return-fields list (see dtcPoolReturnFields) that excludes only
// `auto_consolidated_monitors`.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package dtcpool

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtcpool/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtcpool/v1alpha1"
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
	errObserveDTCPool   = "cannot observe DTCPool"
	errCreateDTCPool    = "cannot create DTCPool"
	errUpdateDTCPool    = "cannot update DTCPool"
	errDeleteDTCPool    = "cannot delete DTCPool"
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

// dtcPoolClients bundles the two SDK handles this package needs: the
// high-level ObjectManager (used for the ref-only Get/Delete calls) and
// the lower-level IBConnector (used for Create/Update, so this package can
// build the DtcPool struct itself and skip the ObjectManager's
// name-based server/monitor lookups — see the package doc comment).
type dtcPoolClients struct {
	objMgr ibclient.IBObjectManager
	conn   ibclient.IBConnector
}

// newClients constructs an authenticated ibclient.IBObjectManager plus the
// underlying IBConnector from the given credentials. The Connector performs
// HTTP Basic Auth on every request and only validates configuration
// locally — no network round-trip happens until the first
// Observe/Create/Update/Delete call.
func newClients(creds *nioCredentials) (*dtcPoolClients, error) {
	return newClientsWithScheme(creds, "https", "443")
}

// newClientsWithScheme is the scheme/port-parameterized variant of
// newClients used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newClientsWithScheme(creds *nioCredentials, scheme, port string) (*dtcPoolClients, error) {
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

	return &dtcPoolClients{
		objMgr: ibclient.NewObjectManager(conn, "", ""),
		conn:   conn,
	}, nil
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// serverLink is a scope-agnostic mirror of the CRD's DTCPoolServerLink
// struct (minus the reference/selector fields, which are resolved by the
// generated ResolveReferences method before Observe/Create/Update ever
// run — this package only ever sees the resolved Server ref string).
type serverLink struct {
	Server *string
	Ratio  *uint32
}

// buildServers converts the CRD's simplified server-link slice into the
// SDK's []*ibclient.DtcServerLink wire type.
func buildServers(servers []serverLink) []*ibclient.DtcServerLink {
	if len(servers) == 0 {
		return nil
	}
	out := make([]*ibclient.DtcServerLink, 0, len(servers))
	for _, s := range servers {
		out = append(out, &ibclient.DtcServerLink{Server: strOrEmpty(s.Server), Ratio: uint32OrZero(s.Ratio)})
	}
	return out
}

// serverLinksFromSDK converts the SDK's []*ibclient.DtcServerLink response
// into the CRD's simplified server-link slice.
func serverLinksFromSDK(servers []*ibclient.DtcServerLink) []serverLink {
	if len(servers) == 0 {
		return nil
	}
	out := make([]serverLink, 0, len(servers))
	for _, s := range servers {
		if s == nil {
			continue
		}
		var server *string
		if s.Server != "" {
			v := s.Server
			server = &v
		}
		var ratio *uint32
		if s.Ratio != 0 {
			v := s.Ratio
			ratio = &v
		}
		out = append(out, serverLink{Server: server, Ratio: ratio})
	}
	return out
}

// serversEqual reports whether two server-link slices are equivalent,
// treating a nil slice and an empty slice as equal.
func serversEqual(a, b []serverLink) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Server) != strOrEmpty(b[i].Server) {
			return false
		}
		if uint32OrZero(a[i].Ratio) != uint32OrZero(b[i].Ratio) {
			return false
		}
	}
	return true
}

// poolMonitor is a scope-agnostic mirror of the CRD's DTCPoolMonitor
// struct — a bare monitor `_ref`.
type poolMonitor struct {
	Monitor *string
}

// buildMonitors converts the CRD's simplified monitor-ref slice into the
// SDK's []*ibclient.DtcMonitorHttp wire type (used generically to carry
// any monitor sub-type's `_ref`).
func buildMonitors(monitors []poolMonitor) []*ibclient.DtcMonitorHttp {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]*ibclient.DtcMonitorHttp, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, &ibclient.DtcMonitorHttp{Ref: strOrEmpty(m.Monitor)})
	}
	return out
}

// monitorsFromSDK converts the SDK's []*ibclient.DtcMonitorHttp response
// into the CRD's simplified monitor-ref slice.
func monitorsFromSDK(monitors []*ibclient.DtcMonitorHttp) []poolMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]poolMonitor, 0, len(monitors))
	for _, m := range monitors {
		if m == nil {
			continue
		}
		var ref *string
		if m.Ref != "" {
			v := m.Ref
			ref = &v
		}
		out = append(out, poolMonitor{Monitor: ref})
	}
	return out
}

// monitorsEqual reports whether two monitor-ref slices are equivalent,
// treating a nil slice and an empty slice as equal.
func monitorsEqual(a, b []poolMonitor) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Monitor) != strOrEmpty(b[i].Monitor) {
			return false
		}
	}
	return true
}

// dynRatio is a scope-agnostic mirror of the CRD's DTCPoolDynamicRatio
// struct, reused for both lbDynamicRatioPreferred and
// lbDynamicRatioAlternate.
type dynRatio struct {
	Method              *string
	Monitor             *string
	MonitorMetric       *string
	MonitorWeighing     *string
	InvertMonitorMetric *bool
}

// buildDynRatio converts the CRD's dynamic-ratio struct into the SDK's
// *ibclient.SettingDynamicratio wire type. Returns nil unchanged (the SDK
// field itself is a pointer, omitted entirely when unset).
func buildDynRatio(d *dynRatio) *ibclient.SettingDynamicratio {
	if d == nil {
		return nil
	}
	return &ibclient.SettingDynamicratio{
		Method:              strOrEmpty(d.Method),
		Monitor:             strOrEmpty(d.Monitor),
		MonitorMetric:       strOrEmpty(d.MonitorMetric),
		MonitorWeighing:     strOrEmpty(d.MonitorWeighing),
		InvertMonitorMetric: boolOrFalse(d.InvertMonitorMetric),
	}
}

// dynRatioFromSDK converts the SDK's *ibclient.SettingDynamicratio
// response into the CRD's dynamic-ratio struct. Returns nil if the API
// omitted the setting.
func dynRatioFromSDK(d *ibclient.SettingDynamicratio) *dynRatio {
	if d == nil {
		return nil
	}
	out := &dynRatio{}
	if d.Method != "" {
		v := d.Method
		out.Method = &v
	}
	if d.Monitor != "" {
		v := d.Monitor
		out.Monitor = &v
	}
	if d.MonitorMetric != "" {
		v := d.MonitorMetric
		out.MonitorMetric = &v
	}
	if d.MonitorWeighing != "" {
		v := d.MonitorWeighing
		out.MonitorWeighing = &v
	}
	if d.InvertMonitorMetric {
		v := true
		out.InvertMonitorMetric = &v
	}
	return out
}

// dynRatioEqual reports whether two dynamic-ratio settings are
// equivalent, treating nil and an all-zero-value struct as equal (avoids
// a false phantom diff when the API omits the setting entirely).
func dynRatioEqual(a, b *dynRatio) bool {
	if a == nil && b == nil {
		return true
	}
	empty := &dynRatio{}
	if a == nil {
		a = empty
	}
	if b == nil {
		b = empty
	}
	return strOrEmpty(a.Method) == strOrEmpty(b.Method) &&
		strOrEmpty(a.Monitor) == strOrEmpty(b.Monitor) &&
		strOrEmpty(a.MonitorMetric) == strOrEmpty(b.MonitorMetric) &&
		strOrEmpty(a.MonitorWeighing) == strOrEmpty(b.MonitorWeighing) &&
		boolOrFalse(a.InvertMonitorMetric) == boolOrFalse(b.InvertMonitorMetric)
}

// consolidatedMonitorHealth is a scope-agnostic mirror of the CRD's
// DTCPoolConsolidatedMonitorHealth struct — response-only, never sent on
// Create/Update (see package doc comment).
type consolidatedMonitorHealth struct {
	Members                 []string
	Monitor                 *string
	Availability            *string
	FullHealthCommunication *bool
}

// consolidatedMonitorsFromSDK converts the SDK's response-only
// ConsolidatedMonitors field into the scope-agnostic mirror type.
func consolidatedMonitorsFromSDK(list []*ibclient.DtcPoolConsolidatedMonitorHealth) []consolidatedMonitorHealth {
	if len(list) == 0 {
		return nil
	}
	out := make([]consolidatedMonitorHealth, 0, len(list))
	for _, c := range list {
		if c == nil {
			continue
		}
		item := consolidatedMonitorHealth{Members: c.Members}
		if c.Monitor != "" {
			v := c.Monitor
			item.Monitor = &v
		}
		if c.Availability != "" {
			v := c.Availability
			item.Availability = &v
		}
		if c.FullHealthCommunication {
			v := true
			item.FullHealthCommunication = &v
		}
		out = append(out, item)
	}
	return out
}

// poolHealth is a scope-agnostic mirror of the CRD's DTCPoolHealth
// struct, populated from the SDK's DtcHealth response type.
type poolHealth struct {
	Availability *string
	Description  *string
	EnabledState *string
}

// healthFromSDK converts the SDK's *ibclient.DtcHealth response-only field
// into the scope-agnostic poolHealth struct. Returns nil if the API
// omitted health (e.g. a Grid Manager that has not yet run a health
// check).
func healthFromSDK(h *ibclient.DtcHealth) *poolHealth {
	if h == nil {
		return nil
	}
	out := &poolHealth{}
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
// than a whole DTCPoolParameters value. The cluster and namespaced
// DTCPoolParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so parameterizing
// on the field pointers lets both scopes share this logic unconditionally.

// isUpToDate compares the desired DTCPool fields against the observed
// DtcPool. There are no known immutable fields for DTCPool — every Create
// parameter is also accepted by Update — so all mutable fields
// participate in this comparison. Split into two helpers (scalar fields,
// collection/composite fields) to keep cyclomatic complexity manageable.
func isUpToDate(name, lbPreferredMethod, lbAlternateMethod, comment, availability, lbPreferredTopology, lbAlternateTopology *string, quorum, ttl *uint32, disable, useTTL *bool, servers []serverLink, monitors []poolMonitor, lbDynamicRatioPreferred, lbDynamicRatioAlternate *dynRatio, extAttrs map[string]string, rec *ibclient.DtcPool) bool {
	if !scalarFieldsUpToDate(name, lbPreferredMethod, lbAlternateMethod, comment, availability, lbPreferredTopology, lbAlternateTopology, quorum, ttl, disable, useTTL, rec) {
		return false
	}
	return collectionFieldsUpToDate(servers, monitors, lbDynamicRatioPreferred, lbDynamicRatioAlternate, extAttrs, rec)
}

// scalarFieldsUpToDate compares the primitive-typed DTCPool fields.
func scalarFieldsUpToDate(name, lbPreferredMethod, lbAlternateMethod, comment, availability, lbPreferredTopology, lbAlternateTopology *string, quorum, ttl *uint32, disable, useTTL *bool, rec *ibclient.DtcPool) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(lbPreferredMethod) != rec.LbPreferredMethod {
		return false
	}
	if strOrEmpty(lbAlternateMethod) != rec.LbAlternateMethod {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if strOrEmpty(availability) != rec.Availability {
		return false
	}
	if strOrEmpty(lbPreferredTopology) != strOrEmpty(rec.LbPreferredTopology) {
		return false
	}
	if strOrEmpty(lbAlternateTopology) != strOrEmpty(rec.LbAlternateTopology) {
		return false
	}
	if uint32OrZero(quorum) != uint32OrZero(rec.Quorum) {
		return false
	}
	if uint32OrZero(ttl) != uint32OrZero(rec.Ttl) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(rec.Disable) {
		return false
	}
	return boolOrFalse(useTTL) == boolOrFalse(rec.UseTtl)
}

// collectionFieldsUpToDate compares the slice/composite-typed DTCPool
// fields.
func collectionFieldsUpToDate(servers []serverLink, monitors []poolMonitor, lbDynamicRatioPreferred, lbDynamicRatioAlternate *dynRatio, extAttrs map[string]string, rec *ibclient.DtcPool) bool {
	if !serversEqual(servers, serverLinksFromSDK(rec.Servers)) {
		return false
	}
	if !monitorsEqual(monitors, monitorsFromSDK(rec.Monitors)) {
		return false
	}
	if !dynRatioEqual(lbDynamicRatioPreferred, dynRatioFromSDK(rec.LbDynamicRatioPreferred)) {
		return false
	}
	if !dynRatioEqual(lbDynamicRatioAlternate, dynRatioFromSDK(rec.LbDynamicRatioAlternate)) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// disable, availability, quorum, ttl, useTtl, extattrs, lbAlternateMethod,
// lbPreferredTopology, lbAlternateTopology, lbDynamicRatioPreferred,
// lbDynamicRatioAlternate) from the observed DtcPool into spec so
// isUpToDate does not see phantom drift on the next reconcile. Required
// fields (name, lbPreferredMethod) and user-managed collections (servers,
// monitors) are never late-initialized. Returns true if any field was
// changed.
func lateInitialize(comment **string, disable **bool, availability **string, quorum **uint32, ttl **uint32, useTTL **bool, extAttrs *map[string]string, lbAlternateMethod **string, lbPreferredTopology **string, lbAlternateTopology **string, lbDynamicRatioPreferred **dynRatio, lbDynamicRatioAlternate **dynRatio, rec *ibclient.DtcPool) bool {
	changed := lateInitString(comment, rec.Comment)
	changed = lateInitBool(disable, rec.Disable) || changed
	changed = lateInitStringFromPlain(availability, rec.Availability) || changed
	changed = lateInitUint32(quorum, rec.Quorum) || changed
	changed = lateInitUint32(ttl, rec.Ttl) || changed
	changed = lateInitBool(useTTL, rec.UseTtl) || changed
	changed = lateInitExtAttrs(extAttrs, rec.Ea) || changed
	changed = lateInitStringFromPlain(lbAlternateMethod, rec.LbAlternateMethod) || changed
	changed = lateInitString(lbPreferredTopology, rec.LbPreferredTopology) || changed
	changed = lateInitString(lbAlternateTopology, rec.LbAlternateTopology) || changed
	changed = lateInitDynRatio(lbDynamicRatioPreferred, rec.LbDynamicRatioPreferred) || changed
	changed = lateInitDynRatio(lbDynamicRatioAlternate, rec.LbDynamicRatioAlternate) || changed
	return changed
}

// lateInitString back-fills a single optional *string spec field from an
// observed *string, leaving user-supplied values untouched.
func lateInitString(field **string, observed *string) bool {
	if *field != nil || observed == nil || *observed == "" {
		return false
	}
	v := *observed
	*field = &v
	return true
}

// lateInitStringFromPlain back-fills a single optional *string spec field
// from an observed plain (non-pointer) string, leaving user-supplied
// values untouched.
func lateInitStringFromPlain(field **string, observed string) bool {
	if *field != nil || observed == "" {
		return false
	}
	v := observed
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

// lateInitDynRatio back-fills the dynamic-ratio setting from the observed
// value when spec left it unset.
func lateInitDynRatio(field **dynRatio, observed *ibclient.SettingDynamicratio) bool {
	if *field != nil || observed == nil {
		return false
	}
	fromRec := dynRatioFromSDK(observed)
	if fromRec == nil {
		return false
	}
	*field = fromRec
	return true
}

// observedDTCPool holds the primitive field values extracted from a WAPI
// DtcPool response. The cluster and namespaced DTCPoolObservation types
// are structurally similar but are distinct named types, so each scope
// copies this intermediate struct's fields into its own Observation type
// at the call site.
type observedDTCPool struct {
	ID                      string
	Name                    *string
	LBPreferredMethod       *string
	LBAlternateMethod       *string
	Servers                 []serverLink
	Availability            *string
	Quorum                  *uint32
	LBPreferredTopology     *string
	LBDynamicRatioPreferred *dynRatio
	LBAlternateTopology     *string
	LBDynamicRatioAlternate *dynRatio
	Monitors                []poolMonitor
	Comment                 *string
	Disable                 *bool
	TTL                     *uint32
	UseTTL                  *bool
	ExtAttrs                map[string]string
	Ref                     *string
	ConsolidatedMonitors    []consolidatedMonitorHealth
	Health                  *poolHealth
}

// observeFromDtcPool extracts the fields mirrored by DTCPoolObservation
// (the full-mirror AtProvider convention) from a WAPI DtcPool response.
func observeFromDtcPool(externalID string, rec *ibclient.DtcPool) observedDTCPool {
	o := observedDTCPool{
		ID:                      externalID,
		Name:                    rec.Name,
		Servers:                 serverLinksFromSDK(rec.Servers),
		Monitors:                monitorsFromSDK(rec.Monitors),
		ExtAttrs:                extAttrsFromEA(rec.Ea),
		LBDynamicRatioPreferred: dynRatioFromSDK(rec.LbDynamicRatioPreferred),
		LBDynamicRatioAlternate: dynRatioFromSDK(rec.LbDynamicRatioAlternate),
		ConsolidatedMonitors:    consolidatedMonitorsFromSDK(rec.ConsolidatedMonitors),
		Health:                  healthFromSDK(rec.Health),
	}
	if rec.LbPreferredMethod != "" {
		v := rec.LbPreferredMethod
		o.LBPreferredMethod = &v
	}
	if rec.LbAlternateMethod != "" {
		v := rec.LbAlternateMethod
		o.LBAlternateMethod = &v
	}
	if rec.Availability != "" {
		v := rec.Availability
		o.Availability = &v
	}
	if rec.Quorum != nil {
		v := *rec.Quorum
		o.Quorum = &v
	}
	if rec.LbPreferredTopology != nil && *rec.LbPreferredTopology != "" {
		v := *rec.LbPreferredTopology
		o.LBPreferredTopology = &v
	}
	if rec.LbAlternateTopology != nil && *rec.LbAlternateTopology != "" {
		v := *rec.LbAlternateTopology
		o.LBAlternateTopology = &v
	}
	if rec.Comment != nil && *rec.Comment != "" {
		v := *rec.Comment
		o.Comment = &v
	}
	if rec.Disable != nil {
		v := *rec.Disable
		o.Disable = &v
	}
	if rec.Ttl != nil {
		v := *rec.Ttl
		o.TTL = &v
	}
	if rec.UseTtl != nil {
		v := *rec.UseTtl
		o.UseTTL = &v
	}
	if rec.Ref != "" {
		v := rec.Ref
		o.Ref = &v
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// buildDtcPool constructs the ibclient.DtcPool struct sent on Create and
// Update. AutoConsolidatedMonitors and ConsolidatedMonitors are
// deliberately never set — see the package doc comment.
func buildDtcPool(name, lbPreferredMethod, lbAlternateMethod, comment *string, servers []serverLink, availability *string, quorum *uint32, lbPreferredTopology *string, lbDynamicRatioPreferred *dynRatio, lbAlternateTopology *string, lbDynamicRatioAlternate *dynRatio, monitors []poolMonitor, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string) *ibclient.DtcPool {
	return &ibclient.DtcPool{
		Name:                    name,
		LbPreferredMethod:       strOrEmpty(lbPreferredMethod),
		LbAlternateMethod:       strOrEmpty(lbAlternateMethod),
		Comment:                 comment,
		Servers:                 buildServers(servers),
		Availability:            strOrEmpty(availability),
		Quorum:                  quorum,
		LbPreferredTopology:     lbPreferredTopology,
		LbDynamicRatioPreferred: buildDynRatio(lbDynamicRatioPreferred),
		LbAlternateTopology:     lbAlternateTopology,
		LbDynamicRatioAlternate: buildDynRatio(lbDynamicRatioAlternate),
		Monitors:                buildMonitors(monitors),
		Disable:                 disable,
		Ttl:                     ttl,
		UseTtl:                  useTTL,
		Ea:                      buildEA(extAttrs),
	}
}

// createDtcPool issues the WAPI create call via the low-level IBConnector
// — see the package doc comment for why the ObjectManager wrapper cannot
// be used as-is for this resource's servers/monitors fields.
func createDtcPool(conn ibclient.IBConnector, name, lbPreferredMethod, lbAlternateMethod, comment *string, servers []serverLink, availability *string, quorum *uint32, lbPreferredTopology *string, lbDynamicRatioPreferred *dynRatio, lbAlternateTopology *string, lbDynamicRatioAlternate *dynRatio, monitors []poolMonitor, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*ibclient.DtcPool, error) {
	pool := buildDtcPool(name, lbPreferredMethod, lbAlternateMethod, comment, servers, availability, quorum, lbPreferredTopology, lbDynamicRatioPreferred, lbAlternateTopology, lbDynamicRatioAlternate, monitors, disable, ttl, useTTL, extAttrs)
	ref, err := conn.CreateObject(pool)
	if err != nil {
		return nil, err
	}
	pool.Ref = ref
	return pool, nil
}

// updateDtcPool issues the WAPI update call. All fields are mutable —
// there are no known immutable fields for DTCPool — so every field is
// echoed on every update (this API uses PUT full-replace semantics).
func updateDtcPool(conn ibclient.IBConnector, ref string, name, lbPreferredMethod, lbAlternateMethod, comment *string, servers []serverLink, availability *string, quorum *uint32, lbPreferredTopology *string, lbDynamicRatioPreferred *dynRatio, lbAlternateTopology *string, lbDynamicRatioAlternate *dynRatio, monitors []poolMonitor, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string) (*ibclient.DtcPool, error) {
	pool := buildDtcPool(name, lbPreferredMethod, lbAlternateMethod, comment, servers, availability, quorum, lbPreferredTopology, lbDynamicRatioPreferred, lbAlternateTopology, lbDynamicRatioAlternate, monitors, disable, ttl, useTTL, extAttrs)
	pool.Ref = ref
	refRes, err := conn.UpdateObject(pool, ref)
	if err != nil {
		return nil, err
	}
	pool.Ref = refRes
	return pool, nil
}

// dtcPoolReturnFields lists every DtcPool field this package reads on
// Observe, deliberately omitting `auto_consolidated_monitors`. That
// field does not exist on this deployment's Grid Manager WAPI schema
// (live-verified): the SDK's own read helper
// (ibclient.ObjectManager.GetDtcPoolByRef, via NewEmptyDtcPool)
// unconditionally requests it, and the GET is rejected outright with a
// 400 "Unknown argument/field" error — not a per-field omission, the
// whole response never comes back. getDtcPoolByRef below issues the
// same GET through the low-level IBConnector with this trimmed field
// list instead, mirroring the Create/Update bypass explained in the
// package doc comment.
var dtcPoolReturnFields = []string{
	"lb_preferred_method", "servers", "lb_dynamic_ratio_preferred", "monitors", "consolidated_monitors", "disable",
	"extattrs", "health", "lb_alternate_method", "lb_alternate_topology", "lb_dynamic_ratio_alternate", "lb_preferred_topology", "quorum", "ttl", "use_ttl", "availability",
}

// getDtcPoolByRef issues the WAPI GET call for the DTCPool identified by
// ref via the low-level IBConnector — see dtcPoolReturnFields and the
// package doc comment for why the ObjectManager's GetDtcPoolByRef
// convenience method cannot be used as-is for this deployment.
func getDtcPoolByRef(conn ibclient.IBConnector, ref string) (*ibclient.DtcPool, error) {
	pool := &ibclient.DtcPool{}
	pool.SetReturnFields(append(pool.ReturnFields(), dtcPoolReturnFields...))
	err := conn.GetObject(pool, ref, ibclient.NewQueryParams(false, nil), &pool)
	return pool, err
}

// deleteDtcPool issues the WAPI delete call.
func deleteDtcPool(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteDtcPool(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced DTCPool
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterDTCPool(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster DTCPool controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("DTCPool"))

	o.Gate.Register(func() {
		if err := setupNamespacedDTCPool(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced DTCPool controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("DTCPool"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced DTCPool
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterDTCPool(mgr, o); err != nil {
		return err
	}
	return setupNamespacedDTCPool(mgr, o)
}
