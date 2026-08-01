// Package dtcserver implements the Crossplane controller for the Infoblox
// NIOS DTCServer managed resource (WAPI object type `dtc:server`). Like
// the recorda package, this provider wraps the official infoblox-go-client
// Go SDK directly instead of composing a generic REST envelope client.
//
// The SDK's ObjectManager.CreateDtcServer/UpdateDtcServer convenience
// wrappers accept `monitors []map[string]interface{}` entries shaped as
// {"monitor": ibclient.Monitor{Name, Type}, "host": string} — they resolve
// the monitor's WAPI `_ref` internally via an extra lookup-by-name-and-type
// HTTP round trip. This provider's DTCServerMonitor CRD field stores the
// monitor's `_ref` directly (the user supplies an existing NIOS monitor
// object's reference, not a name+type pair), so this package builds the
// DtcServer SDK struct itself (via ibclient.NewDtcServer) and issues
// Create/Update through the lower-level ibclient.IBConnector returned
// alongside the ObjectManager — bypassing the name+type lookup entirely
// and avoiding an unnecessary API call per monitor.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package dtcserver

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dtcserver/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dtcserver/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/controller/staleref"
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
	errObserveDTCServer    = "cannot observe DTCServer"
	errCreateDTCServer     = "cannot create DTCServer"
	errUpdateDTCServer     = "cannot update DTCServer"
	errDeleteDTCServer     = "cannot delete DTCServer"
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

// dtcServerClients bundles the two SDK handles this package needs: the
// high-level ObjectManager (used for the ref-only Get/Delete calls) and
// the lower-level IBConnector (used for Create/Update, so this package can
// build the DtcServer struct itself and skip the ObjectManager's
// monitor name+type resolution — see the package doc comment).
type dtcServerClients struct {
	objMgr ibclient.IBObjectManager
	conn   ibclient.IBConnector
}

// newClients constructs an authenticated ibclient.IBObjectManager plus the
// underlying IBConnector from the given credentials. The Connector performs
// HTTP Basic Auth on every request and only validates configuration
// locally — no network round-trip happens until the first
// Observe/Create/Update/Delete call.
func newClients(creds *nioCredentials, sslVerify bool) (*dtcServerClients, error) {
	return newClientsWithScheme(creds, sslVerify, "https", "443")
}

// newClientsWithScheme is the scheme/port-parameterized variant of
// newClients used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newClientsWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (*dtcServerClients, error) {
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

	return &dtcServerClients{
		objMgr: ibclient.NewObjectManager(conn, "", ""),
		conn:   conn,
	}, nil
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// monitorPair is a scope-agnostic mirror of the CRD's DTCServerMonitor
// struct. The cluster and namespaced DTCServerMonitor types are
// structurally identical but are distinct named Go types, so shared logic
// operates on this intermediate type instead — each scope converts to/from
// its own DTCServerMonitor slice at the call site.
type monitorPair struct {
	Monitor *string
	Host    *string
}

// buildMonitors converts the CRD's simplified monitor-pair slice into the
// SDK's []*ibclient.DtcServerMonitor wire type.
func buildMonitors(monitors []monitorPair) []*ibclient.DtcServerMonitor {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]*ibclient.DtcServerMonitor, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, &ibclient.DtcServerMonitor{
			Monitor: strOrEmpty(m.Monitor),
			Host:    strOrEmpty(m.Host),
		})
	}
	return out
}

// monitorPairsFromSDK converts the SDK's []*ibclient.DtcServerMonitor
// response into the CRD's simplified monitor-pair slice.
func monitorPairsFromSDK(monitors []*ibclient.DtcServerMonitor) []monitorPair {
	if len(monitors) == 0 {
		return nil
	}
	out := make([]monitorPair, 0, len(monitors))
	for _, m := range monitors {
		if m == nil {
			continue
		}
		var monitor, host *string
		if m.Monitor != "" {
			v := m.Monitor
			monitor = &v
		}
		if m.Host != "" {
			v := m.Host
			host = &v
		}
		out = append(out, monitorPair{Monitor: monitor, Host: host})
	}
	return out
}

// monitorsEqual reports whether two monitor-pair slices are equivalent,
// treating a nil slice and an empty slice as equal.
func monitorsEqual(a, b []monitorPair) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Monitor) != strOrEmpty(b[i].Monitor) {
			return false
		}
		if strOrEmpty(a[i].Host) != strOrEmpty(b[i].Host) {
			return false
		}
	}
	return true
}

// dtcHealth is a scope-agnostic mirror of the CRD's DTCServerHealth
// struct, populated from the SDK's DtcHealth response type.
type dtcHealth struct {
	Availability *string
	Description  *string
	EnabledState *string
}

// healthFromSDK converts the SDK's *ibclient.DtcHealth response-only field
// into the scope-agnostic dtcHealth struct. Returns nil if the API omitted
// health (e.g. a Grid Manager that has not yet run a health check).
func healthFromSDK(h *ibclient.DtcHealth) *dtcHealth {
	if h == nil {
		return nil
	}
	out := &dtcHealth{}
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
// than a whole DTCServerParameters value. The cluster and namespaced
// DTCServerParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct struct
// conversion between them is not always available once other resources in
// this provider grow reference fields; parameterizing on the field
// pointers instead lets both scopes share this logic unconditionally.

// isUpToDate compares the desired DTCServer fields against the observed
// DtcServer. There are no known immutable fields for DTCServer — every
// Create parameter is also accepted by Update — so all mutable fields
// participate in this comparison.
func isUpToDate(name, host, comment *string, disable, autoCreateHostRecord, useSniHostname *bool, sniHostname *string, monitors []monitorPair, extAttrs map[string]string, rec *ibclient.DtcServer) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(host) != strOrEmpty(rec.Host) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(rec.Disable) {
		return false
	}
	if boolOrFalse(autoCreateHostRecord) != boolOrFalse(rec.AutoCreateHostRecord) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useSniHostname) != boolOrFalse(rec.UseSniHostname) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted SNI hostname and returns its own default on
	// every GET — comparing it against the spec value never converges.
	if boolOrFalse(useSniHostname) {
		if strOrEmpty(sniHostname) != strOrEmpty(rec.SniHostname) {
			return false
		}
	}
	if !monitorsEqual(monitors, monitorPairsFromSDK(rec.Monitors)) {
		return false
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// disable, autoCreateHostRecord, sniHostname, useSniHostname, monitors,
// extAttrs) from the observed DtcServer into spec so isUpToDate does not
// see phantom drift on the next reconcile. Required fields (name, host)
// are never late-initialized — they are always user-supplied. Returns
// true if any field was changed.
func lateInitialize(comment **string, disable, autoCreateHostRecord, useSniHostname **bool, sniHostname **string, monitors *[]monitorPair, extAttrs *map[string]string, rec *ibclient.DtcServer) bool {
	changed := lateInitString(comment, rec.Comment)
	changed = lateInitBool(disable, rec.Disable) || changed
	changed = lateInitBool(autoCreateHostRecord, rec.AutoCreateHostRecord) || changed
	changed = lateInitBool(useSniHostname, rec.UseSniHostname) || changed
	// Only back-fill sniHostname when useSniHostname is on (post-backfill
	// value above). When it is off, the observed value is WAPI's own
	// default, not one implied by the user's config.
	if boolOrFalse(*useSniHostname) {
		changed = lateInitString(sniHostname, rec.SniHostname) || changed
	}
	changed = lateInitMonitors(monitors, rec.Monitors) || changed
	changed = lateInitExtAttrs(extAttrs, rec.Ea) || changed

	return changed
}

// lateInitString back-fills a single optional *string spec field from the
// observed value, leaving user-supplied values untouched.
func lateInitString(field **string, observed *string) bool {
	if *field != nil || observed == nil || *observed == "" {
		return false
	}
	v := *observed
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

// lateInitMonitors back-fills the monitors slice from the observed value
// when spec left it empty.
func lateInitMonitors(field *[]monitorPair, observed []*ibclient.DtcServerMonitor) bool {
	if len(*field) != 0 {
		return false
	}
	fromRec := monitorPairsFromSDK(observed)
	if len(fromRec) == 0 {
		return false
	}
	*field = fromRec
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

// observedDTCServer holds the primitive field values extracted from a WAPI
// DtcServer response. The cluster and namespaced DTCServerObservation
// types are structurally similar but are distinct named types, so each
// scope copies this intermediate struct's fields into its own Observation
// type at the call site.
type observedDTCServer struct {
	ID                   string
	Name                 *string
	Host                 *string
	Comment              *string
	Disable              *bool
	AutoCreateHostRecord *bool
	SniHostname          *string
	UseSniHostname       *bool
	Monitors             []monitorPair
	ExtAttrs             map[string]string
	Ref                  *string
	Health               *dtcHealth
}

// observeFromDtcServer extracts the fields mirrored by
// DTCServerObservation (the full-mirror AtProvider convention) from a WAPI
// DtcServer response.
func observeFromDtcServer(externalID string, rec *ibclient.DtcServer) observedDTCServer {
	o := observedDTCServer{
		ID:       externalID,
		Name:     rec.Name,
		Host:     rec.Host,
		ExtAttrs: extAttrsFromEA(rec.Ea),
		Monitors: monitorPairsFromSDK(rec.Monitors),
		Health:   healthFromSDK(rec.Health),
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Disable != nil {
		d := *rec.Disable
		o.Disable = &d
	}
	if rec.AutoCreateHostRecord != nil {
		a := *rec.AutoCreateHostRecord
		o.AutoCreateHostRecord = &a
	}
	if rec.SniHostname != nil && *rec.SniHostname != "" {
		s := *rec.SniHostname
		o.SniHostname = &s
	}
	if rec.UseSniHostname != nil {
		u := *rec.UseSniHostname
		o.UseSniHostname = &u
	}
	if rec.Ref != "" {
		r := rec.Ref
		o.Ref = &r
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createDtcServer issues the WAPI create call. It builds the DtcServer
// struct itself (via ibclient.NewDtcServer) and calls the low-level
// IBConnector directly instead of ObjectManager.CreateDtcServer — see the
// package doc comment for why the ObjectManager wrapper cannot be used
// as-is for this resource's monitors field.
func createDtcServer(conn ibclient.IBConnector, name, host, comment *string, autoCreateHostRecord, disable *bool, monitors []monitorPair, sniHostname *string, useSniHostname *bool, extAttrs map[string]string) (*ibclient.DtcServer, error) {
	dtcServer := ibclient.NewDtcServer(
		strOrEmpty(comment),
		strOrEmpty(name),
		strOrEmpty(host),
		boolOrFalse(autoCreateHostRecord),
		boolOrFalse(disable),
		buildEA(extAttrs),
		buildMonitors(monitors),
		strOrEmpty(sniHostname),
		boolOrFalse(useSniHostname),
	)
	ref, err := conn.CreateObject(dtcServer)
	if err != nil {
		return nil, err
	}
	dtcServer.Ref = ref
	return dtcServer, nil
}

// updateDtcServer issues the WAPI update call. All fields are mutable —
// there are no known immutable fields for DTCServer — so every field is
// echoed on every update.
func updateDtcServer(conn ibclient.IBConnector, ref string, name, host, comment *string, autoCreateHostRecord, disable *bool, monitors []monitorPair, sniHostname *string, useSniHostname *bool, extAttrs map[string]string) (*ibclient.DtcServer, error) {
	dtcServer := ibclient.NewDtcServer(
		strOrEmpty(comment),
		strOrEmpty(name),
		strOrEmpty(host),
		boolOrFalse(autoCreateHostRecord),
		boolOrFalse(disable),
		buildEA(extAttrs),
		buildMonitors(monitors),
		strOrEmpty(sniHostname),
		boolOrFalse(useSniHostname),
	)
	dtcServer.Ref = ref
	refRes, err := conn.UpdateObject(dtcServer, ref)
	if err != nil {
		return nil, err
	}
	dtcServer.Ref = refRes
	return dtcServer, nil
}

// deleteDtcServer issues the WAPI delete call.
func deleteDtcServer(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteDtcServer(ref)
	return err
}

// dtcServerExistsByNaturalKey reports whether a live DTCServer still
// exists under the CR's own (name, host) identity — the same tuple WAPI
// uses to compute the _ref. Used by Delete() when the stored _ref 404s: a
// hit here means the _ref is merely stale, not that the object is gone.
// Unlike dtclbdn/dtcpool, this resource has no auto_consolidated_monitors
// schema mismatch on this deployment (confirmed: Observe() calls
// objMgr.GetDtcServerByRef directly), so this uses the ObjectManager's
// plain GetDtcServer(name, host) convenience method rather than a
// low-level IBConnector search. GetDtcServer requires both fields
// non-empty; when either is missing there is no way to re-discover the
// object, so the search is skipped (found=false) rather than treated as
// an error.
func dtcServerExistsByNaturalKey(objMgr ibclient.IBObjectManager, name, host *string) (bool, error) {
	if strOrEmpty(name) == "" || strOrEmpty(host) == "" {
		return false, nil
	}
	_, err := objMgr.GetDtcServer(strOrEmpty(name), strOrEmpty(host))
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// deleteDtcServerResolving404 issues the WAPI delete and, on a 404
// against the stored _ref, resolves the object's natural key before
// concluding it is gone. A 404 on a derived handle is evidence the handle
// rotated, not evidence the object was removed: if the natural-key search
// still finds a live object, deleting is refused because ownership of
// that object cannot be verified from the search alone (see the staleref
// package doc for the full rationale).
func deleteDtcServerResolving404(objMgr ibclient.IBObjectManager, ref string, name, host *string) error {
	delErr := deleteDtcServer(objMgr, ref)
	if delErr == nil {
		return nil
	}
	if !isNotFound(delErr) {
		return errors.Wrap(delErr, errDeleteDTCServer)
	}
	found, searchErr := dtcServerExistsByNaturalKey(objMgr, name, host)
	if searchErr != nil {
		return errors.Wrap(searchErr, errDeleteDTCServer)
	}
	if found {
		return staleref.RefusalError()
	}
	return nil
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced DTCServer
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterDTCServer(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster DTCServer controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("DTCServer"))

	o.Gate.Register(func() {
		if err := setupNamespacedDTCServer(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced DTCServer controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("DTCServer"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced DTCServer
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterDTCServer(mgr, o); err != nil {
		return err
	}
	return setupNamespacedDTCServer(mgr, o)
}
