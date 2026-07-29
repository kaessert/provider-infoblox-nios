// Package dnsview implements the Crossplane controller for the Infoblox
// NIOS DNSView managed resource (WAPI object type view).
//
// Unlike most resources in this provider, DNSView talks to the WAPI
// through the raw ibclient.IBConnector (CreateObject/GetObject/
// UpdateObject/DeleteObject) instead of the SDK's ObjectManager
// convenience wrappers. The SDK exposes only a single read-only helper for
// this object (GetDNSView), with no Create/Update/Delete wrapper at all —
// so this controller builds ibclient.View values directly and issues WAPI
// calls through the Connector, the same pattern used by this provider's
// ZoneAuth and ExtensibleAttributeDef controllers.
//
// The NIOS appliance always provisions three well-known views ("default",
// "External", "Internal") that can be renamed and reconfigured but must
// never be deleted. This controller protects those three names from
// accidental deletion — see isWellKnownDNSViewName and Delete in
// cluster.go/namespaced.go.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared WAPI plumbing, field comparison, and late-init logic lives here.
package dnsview

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/dnsview/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/dnsview/v1alpha1"
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
	errObserveDNSView   = "cannot observe DNSView"
	errCreateDNSView    = "cannot create DNSView"
	errUpdateDNSView    = "cannot update DNSView"
	errDeleteDNSView    = "cannot delete DNSView"
	errEmptyRef         = "empty reference to an object is not allowed"
)

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
const wapiVersion = "2.9.7"

// wellKnownDNSViewNames holds the three views the NIOS Grid Manager always
// provisions. They can be renamed and reconfigured, but the appliance
// rejects deleting the last remaining view and this controller additionally
// refuses to attempt WAPI deletion for any of them by name — protecting
// against an accidental MR deletion silently taking down Grid-wide DNS
// resolution. Delete() treats these as a no-op success (see cluster.go /
// namespaced.go) so the Kubernetes object can still be removed.
var wellKnownDNSViewNames = map[string]bool{
	"default":  true,
	"External": true,
	"Internal": true,
}

// isWellKnownDNSViewName reports whether name identifies one of the three
// views the NIOS appliance always provisions.
func isWellKnownDNSViewName(name *string) bool {
	if name == nil {
		return false
	}
	return wellKnownDNSViewNames[*name]
}

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

// strPtrOrNil converts a plain (possibly empty) SDK string into the CRD's
// pointer representation, treating an empty string as "not set".
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

// boolPtrOrNil converts a plain SDK bool into the CRD's pointer
// representation, treating false as "not set" — the WAPI SDK's nested
// (non-View) structs use plain bool fields with no way to distinguish an
// explicit false from an absent field, so this is the same trade-off the
// rest of the provider makes for plain-string SDK fields (see strPtrOrNil).
func boolPtrOrNil(b bool) *bool {
	if !b {
		return nil
	}
	return &b
}

func int64OrZero(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

// int64PtrToUint32Ptr converts a CRD *int64 into the SDK's *uint32
// representation, preserving nil (unset). Out-of-range values (negative,
// or above uint32 max) clamp to 0 rather than silently wrapping — CEL/CRD
// validation on the ForProvider field is expected to reject those before
// they ever reach this helper (mirrors this provider's ttlOrZero pattern).
func int64PtrToUint32Ptr(i *int64) *uint32 {
	if i == nil {
		return nil
	}
	v := int64ToUint32Clamped(*i)
	return &v
}

// uint32PtrToInt64Ptr converts an SDK *uint32 into the CRD's *int64
// representation, preserving nil (unset).
func uint32PtrToInt64Ptr(u *uint32) *int64 {
	if u == nil {
		return nil
	}
	v := int64(*u)
	return &v
}

// uint32ValToInt64Ptr converts a plain (non-pointer) SDK uint32 into the
// CRD's *int64 representation, treating zero as "not set" — nested WAPI
// structs use plain numeric fields with the same not-set/zero ambiguity
// described in boolPtrOrNil.
func uint32ValToInt64Ptr(u uint32) *int64 {
	if u == 0 {
		return nil
	}
	v := int64(u)
	return &v
}

// int64PtrToUint32Val converts a CRD *int64 into the plain (non-pointer)
// SDK uint32 a nested WAPI struct field expects. See int64PtrToUint32Ptr
// for the out-of-range clamping rationale.
func int64PtrToUint32Val(i *int64) uint32 {
	if i == nil {
		return 0
	}
	return int64ToUint32Clamped(*i)
}

// int64ToUint32Clamped converts an int64 to uint32, clamping negative or
// overflowing values to 0 instead of wrapping.
func int64ToUint32Clamped(i int64) uint32 {
	if i < 0 || i > math.MaxUint32 {
		return 0
	}
	return uint32(i)
}

// unixTimePtrToInt64Ptr converts the SDK's epoch-seconds UnixTime into the
// CRD's plain *int64 representation.
func unixTimePtrToInt64Ptr(u *ibclient.UnixTime) *int64 {
	if u == nil {
		return nil
	}
	v := u.Unix()
	return &v
}

// int64PtrToUnixTimePtr converts the CRD's plain *int64 (epoch seconds)
// into the SDK's UnixTime representation.
func int64PtrToUnixTimePtr(i *int64) *ibclient.UnixTime {
	if i == nil {
		return nil
	}
	return &ibclient.UnixTime{Time: time.Unix(*i, 0)}
}

func stringSliceEqual(a, b []string) bool {
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

// nestedSliceEqual compares two slices of any nested value-bag type.
// Length is checked first so a nil slice and an empty slice both compare
// equal without falling into reflect.DeepEqual's nil-vs-empty distinction.
func nestedSliceEqual[T any](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

// lateInitPtr back-fills *desired from observed when desired is unset.
// Used for pointer fields (scalar or nested-struct) where the server's
// zero value is itself a meaningful, back-fillable answer.
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

func lateInitStringSlice(desired *[]string, observed []string) bool {
	if len(*desired) == 0 && len(observed) > 0 {
		*desired = observed
		return true
	}
	return false
}

func lateInitNestedSlice[T any](desired *[]T, observed []T) bool {
	if len(*desired) == 0 && len(observed) > 0 {
		*desired = observed
		return true
	}
	return false
}

func lateInitMap(desired *map[string]string, observed map[string]string) bool {
	if len(*desired) == 0 && len(observed) > 0 {
		*desired = observed
		return true
	}
	return false
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

// ── nested value bags (shared scope-neutral currency) ───────────────────

// nameServerValue is the scope-neutral value bag for the SDK's NameServer / CRD's DNSViewNameServer nested type.
type nameServerValue struct {
	Address                      *string
	Name                         *string
	SharedWithMsParentDelegation *bool
	Stealth                      *bool
	TsigKey                      *string
	TsigKeyAlg                   *string
	TsigKeyName                  *string
	UseTsigKeyName               *bool
}

func nameServerValueFromSDKItem(in ibclient.NameServer) nameServerValue {
	return nameServerValue{
		Address:                      strPtrOrNil(in.Address),
		Name:                         strPtrOrNil(in.Name),
		SharedWithMsParentDelegation: boolPtrOrNil(in.SharedWithMsParentDelegation),
		Stealth:                      boolPtrOrNil(in.Stealth),
		TsigKey:                      strPtrOrNil(in.TsigKey),
		TsigKeyAlg:                   strPtrOrNil(in.TsigKeyAlg),
		TsigKeyName:                  strPtrOrNil(in.TsigKeyName),
		UseTsigKeyName:               boolPtrOrNil(in.UseTsigKeyName),
	}
}

func nameServerValueToSDKItem(in nameServerValue) ibclient.NameServer {
	return ibclient.NameServer{
		Address:                      strOrEmpty(in.Address),
		Name:                         strOrEmpty(in.Name),
		SharedWithMsParentDelegation: boolOrFalse(in.SharedWithMsParentDelegation),
		Stealth:                      boolOrFalse(in.Stealth),
		TsigKey:                      strOrEmpty(in.TsigKey),
		TsigKeyAlg:                   strOrEmpty(in.TsigKeyAlg),
		TsigKeyName:                  strOrEmpty(in.TsigKeyName),
		UseTsigKeyName:               boolOrFalse(in.UseTsigKeyName),
	}
}

func nameServerValuesFromSDK(in []ibclient.NameServer) []nameServerValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]nameServerValue, 0, len(in))
	for _, item := range in {
		out = append(out, nameServerValueFromSDKItem(item))
	}
	return out
}

func nameServerValuesToSDK(in []nameServerValue) []ibclient.NameServer {
	if len(in) == 0 {
		return nil
	}
	out := make([]ibclient.NameServer, 0, len(in))
	for _, item := range in {
		out = append(out, nameServerValueToSDKItem(item))
	}
	return out
}

// dnssecTrustedKeyValue is the scope-neutral value bag for the SDK's Dnssectrustedkey / CRD's DNSViewDnssecTrustedKey nested type.
type dnssecTrustedKeyValue struct {
	Fqdn               *string
	Algorithm          *string
	Key                *string
	SecureEntryPoint   *bool
	DnssecMustBeSecure *bool
}

func dnssecTrustedKeyValueFromSDKItem(in ibclient.Dnssectrustedkey) dnssecTrustedKeyValue {
	return dnssecTrustedKeyValue{
		Fqdn:               strPtrOrNil(in.Fqdn),
		Algorithm:          strPtrOrNil(in.Algorithm),
		Key:                strPtrOrNil(in.Key),
		SecureEntryPoint:   boolPtrOrNil(in.SecureEntryPoint),
		DnssecMustBeSecure: boolPtrOrNil(in.DnssecMustBeSecure),
	}
}

func dnssecTrustedKeyValueToSDKItem(in dnssecTrustedKeyValue) ibclient.Dnssectrustedkey {
	return ibclient.Dnssectrustedkey{
		Fqdn:               strOrEmpty(in.Fqdn),
		Algorithm:          strOrEmpty(in.Algorithm),
		Key:                strOrEmpty(in.Key),
		SecureEntryPoint:   boolOrFalse(in.SecureEntryPoint),
		DnssecMustBeSecure: boolOrFalse(in.DnssecMustBeSecure),
	}
}

func dnssecTrustedKeyValuesFromSDKPtr(in []*ibclient.Dnssectrustedkey) []dnssecTrustedKeyValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]dnssecTrustedKeyValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, dnssecTrustedKeyValueFromSDKItem(*item))
	}
	return out
}

func dnssecTrustedKeyValuesToSDKPtr(in []dnssecTrustedKeyValue) []*ibclient.Dnssectrustedkey {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Dnssectrustedkey, 0, len(in))
	for _, item := range in {
		item := item
		sdkItem := dnssecTrustedKeyValueToSDKItem(item)
		out = append(out, &sdkItem)
	}
	return out
}

// addressAcValue is the scope-neutral value bag for the SDK's Addressac / CRD's DNSViewAddressAc nested type.
type addressAcValue struct {
	Address        *string
	Permission     *string
	TsigKey        *string
	TsigKeyAlg     *string
	TsigKeyName    *string
	UseTsigKeyName *bool
}

func addressAcValueFromSDKItem(in ibclient.Addressac) addressAcValue {
	return addressAcValue{
		Address:        strPtrOrNil(in.Address),
		Permission:     strPtrOrNil(in.Permission),
		TsigKey:        strPtrOrNil(in.TsigKey),
		TsigKeyAlg:     strPtrOrNil(in.TsigKeyAlg),
		TsigKeyName:    strPtrOrNil(in.TsigKeyName),
		UseTsigKeyName: boolPtrOrNil(in.UseTsigKeyName),
	}
}

func addressAcValueToSDKItem(in addressAcValue) ibclient.Addressac {
	return ibclient.Addressac{
		Address:        strOrEmpty(in.Address),
		Permission:     strOrEmpty(in.Permission),
		TsigKey:        strOrEmpty(in.TsigKey),
		TsigKeyAlg:     strOrEmpty(in.TsigKeyAlg),
		TsigKeyName:    strOrEmpty(in.TsigKeyName),
		UseTsigKeyName: boolOrFalse(in.UseTsigKeyName),
	}
}

func addressAcValuesFromSDKPtr(in []*ibclient.Addressac) []addressAcValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]addressAcValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, addressAcValueFromSDKItem(*item))
	}
	return out
}

func addressAcValuesToSDKPtr(in []addressAcValue) []*ibclient.Addressac {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Addressac, 0, len(in))
	for _, item := range in {
		item := item
		sdkItem := addressAcValueToSDKItem(item)
		out = append(out, &sdkItem)
	}
	return out
}

// fixedRrsetOrderFqdnValue is the scope-neutral value bag for the SDK's GridDnsFixedrrsetorderfqdn / CRD's DNSViewFixedRrsetOrderFqdn nested type.
type fixedRrsetOrderFqdnValue struct {
	Fqdn       *string
	RecordType *string
}

func fixedRrsetOrderFqdnValueFromSDKItem(in ibclient.GridDnsFixedrrsetorderfqdn) fixedRrsetOrderFqdnValue {
	return fixedRrsetOrderFqdnValue{
		Fqdn:       strPtrOrNil(in.Fqdn),
		RecordType: strPtrOrNil(in.RecordType),
	}
}

func fixedRrsetOrderFqdnValueToSDKItem(in fixedRrsetOrderFqdnValue) ibclient.GridDnsFixedrrsetorderfqdn {
	return ibclient.GridDnsFixedrrsetorderfqdn{
		Fqdn:       strOrEmpty(in.Fqdn),
		RecordType: strOrEmpty(in.RecordType),
	}
}

func fixedRrsetOrderFqdnValuesFromSDKPtr(in []*ibclient.GridDnsFixedrrsetorderfqdn) []fixedRrsetOrderFqdnValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]fixedRrsetOrderFqdnValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, fixedRrsetOrderFqdnValueFromSDKItem(*item))
	}
	return out
}

func fixedRrsetOrderFqdnValuesToSDKPtr(in []fixedRrsetOrderFqdnValue) []*ibclient.GridDnsFixedrrsetorderfqdn {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.GridDnsFixedrrsetorderfqdn, 0, len(in))
	for _, item := range in {
		item := item
		sdkItem := fixedRrsetOrderFqdnValueToSDKItem(item)
		out = append(out, &sdkItem)
	}
	return out
}

// expressionOpValue is the scope-neutral value bag for the SDK's Expressionop / CRD's DNSViewExpressionOp nested type.
type expressionOpValue struct {
	Op      *string
	Op1     *string
	Op1Type *string
	Op2     *string
	Op2Type *string
}

func expressionOpValueFromSDKItem(in ibclient.Expressionop) expressionOpValue {
	return expressionOpValue{
		Op:      strPtrOrNil(in.Op),
		Op1:     strPtrOrNil(in.Op1),
		Op1Type: strPtrOrNil(in.Op1Type),
		Op2:     strPtrOrNil(in.Op2),
		Op2Type: strPtrOrNil(in.Op2Type),
	}
}

func expressionOpValueToSDKItem(in expressionOpValue) ibclient.Expressionop {
	return ibclient.Expressionop{
		Op:      strOrEmpty(in.Op),
		Op1:     strOrEmpty(in.Op1),
		Op1Type: strOrEmpty(in.Op1Type),
		Op2:     strOrEmpty(in.Op2),
		Op2Type: strOrEmpty(in.Op2Type),
	}
}

func expressionOpValuesFromSDKPtr(in []*ibclient.Expressionop) []expressionOpValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]expressionOpValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, expressionOpValueFromSDKItem(*item))
	}
	return out
}

func expressionOpValuesToSDKPtr(in []expressionOpValue) []*ibclient.Expressionop {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Expressionop, 0, len(in))
	for _, item := range in {
		item := item
		sdkItem := expressionOpValueToSDKItem(item)
		out = append(out, &sdkItem)
	}
	return out
}

// eaExpressionOpValue is the scope-neutral value bag for the SDK's Eaexpressionop / CRD's DNSViewEaExpressionOp nested type.
type eaExpressionOpValue struct {
	Op      *string
	Op1     *string
	Op1Type *string
	Op2     *string
	Op2Type *string
}

func eaExpressionOpValueFromSDKItem(in ibclient.Eaexpressionop) eaExpressionOpValue {
	return eaExpressionOpValue{
		Op:      strPtrOrNil(in.Op),
		Op1:     strPtrOrNil(in.Op1),
		Op1Type: strPtrOrNil(in.Op1Type),
		Op2:     strPtrOrNil(in.Op2),
		Op2Type: strPtrOrNil(in.Op2Type),
	}
}

func eaExpressionOpValueToSDKItem(in eaExpressionOpValue) ibclient.Eaexpressionop {
	return ibclient.Eaexpressionop{
		Op:      strOrEmpty(in.Op),
		Op1:     strOrEmpty(in.Op1),
		Op1Type: strOrEmpty(in.Op1Type),
		Op2:     strOrEmpty(in.Op2),
		Op2Type: strOrEmpty(in.Op2Type),
	}
}

func eaExpressionOpValuesFromSDKPtr(in []*ibclient.Eaexpressionop) []eaExpressionOpValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]eaExpressionOpValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, eaExpressionOpValueFromSDKItem(*item))
	}
	return out
}

func eaExpressionOpValuesToSDKPtr(in []eaExpressionOpValue) []*ibclient.Eaexpressionop {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Eaexpressionop, 0, len(in))
	for _, item := range in {
		item := item
		sdkItem := eaExpressionOpValueToSDKItem(item)
		out = append(out, &sdkItem)
	}
	return out
}

// responseRateLimitingValue is the scope-neutral value bag for the SDK's GridResponseratelimiting / CRD's DNSViewResponseRateLimiting nested type.
type responseRateLimitingValue struct {
	EnableRrl          *bool
	LogOnly            *bool
	ResponsesPerSecond *int64
	Window             *int64
	Slip               *int64
}

func responseRateLimitingValueFromSDK(in *ibclient.GridResponseratelimiting) *responseRateLimitingValue {
	if in == nil {
		return nil
	}
	return &responseRateLimitingValue{
		EnableRrl:          boolPtrOrNil(in.EnableRrl),
		LogOnly:            boolPtrOrNil(in.LogOnly),
		ResponsesPerSecond: uint32ValToInt64Ptr(in.ResponsesPerSecond),
		Window:             uint32ValToInt64Ptr(in.Window),
		Slip:               uint32ValToInt64Ptr(in.Slip),
	}
}

func responseRateLimitingValueToSDK(in *responseRateLimitingValue) *ibclient.GridResponseratelimiting {
	if in == nil {
		return nil
	}
	return &ibclient.GridResponseratelimiting{
		EnableRrl:          boolOrFalse(in.EnableRrl),
		LogOnly:            boolOrFalse(in.LogOnly),
		ResponsesPerSecond: int64PtrToUint32Val(in.ResponsesPerSecond),
		Window:             int64PtrToUint32Val(in.Window),
		Slip:               int64PtrToUint32Val(in.Slip),
	}
}

// sortlistEntryValue is the scope-neutral value bag for the SDK's Sortlist / CRD's DNSViewSortlistEntry nested type.
type sortlistEntryValue struct {
	Address   *string
	MatchList []string
}

func sortlistEntryValueFromSDKItem(in ibclient.Sortlist) sortlistEntryValue {
	return sortlistEntryValue{Address: strPtrOrNil(in.Address), MatchList: in.MatchList}
}

func sortlistEntryValueToSDKItem(in sortlistEntryValue) ibclient.Sortlist {
	return ibclient.Sortlist{Address: strOrEmpty(in.Address), MatchList: in.MatchList}
}

func sortlistEntryValuesFromSDKPtr(in []*ibclient.Sortlist) []sortlistEntryValue {
	if len(in) == 0 {
		return nil
	}
	out := make([]sortlistEntryValue, 0, len(in))
	for _, item := range in {
		if item == nil {
			continue
		}
		out = append(out, sortlistEntryValueFromSDKItem(*item))
	}
	return out
}

func sortlistEntryValuesToSDKPtr(in []sortlistEntryValue) []*ibclient.Sortlist {
	if len(in) == 0 {
		return nil
	}
	out := make([]*ibclient.Sortlist, 0, len(in))
	for _, item := range in {
		item := item
		sdkItem := sortlistEntryValueToSDKItem(item)
		out = append(out, &sdkItem)
	}
	return out
}

// scavengingScheduleValue is the scope-neutral value bag for the SDK's SettingSchedule / CRD's DNSViewScavengingSchedule nested type.
type scavengingScheduleValue struct {
	Weekdays        []string
	TimeZone        *string
	RecurringTime   *int64
	Frequency       *string
	Every           *int64
	MinutesPastHour *int64
	HourOfDay       *int64
	Year            *int64
	Month           *int64
	DayOfMonth      *int64
	Repeat          *string
	Disable         *bool
}

func scavengingScheduleValueFromSDK(in *ibclient.SettingSchedule) *scavengingScheduleValue {
	if in == nil {
		return nil
	}
	return &scavengingScheduleValue{
		Weekdays:        in.Weekdays,
		TimeZone:        strPtrOrNil(in.TimeZone),
		RecurringTime:   unixTimePtrToInt64Ptr(in.RecurringTime),
		Frequency:       strPtrOrNil(in.Frequency),
		Every:           uint32ValToInt64Ptr(in.Every),
		MinutesPastHour: uint32ValToInt64Ptr(in.MinutesPastHour),
		HourOfDay:       uint32ValToInt64Ptr(in.HourOfDay),
		Year:            uint32ValToInt64Ptr(in.Year),
		Month:           uint32ValToInt64Ptr(in.Month),
		DayOfMonth:      uint32ValToInt64Ptr(in.DayOfMonth),
		Repeat:          strPtrOrNil(in.Repeat),
		Disable:         boolPtrOrNil(in.Disable),
	}
}

func scavengingScheduleValueToSDK(in *scavengingScheduleValue) *ibclient.SettingSchedule {
	if in == nil {
		return nil
	}
	return &ibclient.SettingSchedule{
		Weekdays:        in.Weekdays,
		TimeZone:        strOrEmpty(in.TimeZone),
		RecurringTime:   int64PtrToUnixTimePtr(in.RecurringTime),
		Frequency:       strOrEmpty(in.Frequency),
		Every:           int64PtrToUint32Val(in.Every),
		MinutesPastHour: int64PtrToUint32Val(in.MinutesPastHour),
		HourOfDay:       int64PtrToUint32Val(in.HourOfDay),
		Year:            int64PtrToUint32Val(in.Year),
		Month:           int64PtrToUint32Val(in.Month),
		DayOfMonth:      int64PtrToUint32Val(in.DayOfMonth),
		Repeat:          strOrEmpty(in.Repeat),
		Disable:         boolOrFalse(in.Disable),
	}
}

// scavengingSettingsValue is the scope-neutral value bag for the SDK's SettingScavenging / CRD's DNSViewScavengingSettings nested type.
type scavengingSettingsValue struct {
	EnableScavenging          *bool
	EnableRecurrentScavenging *bool
	EnableAutoReclamation     *bool
	EnableRrLastQueried       *bool
	EnableZoneLastQueried     *bool
	ReclaimAssociatedRecords  *bool
	ScavengingSchedule        *scavengingScheduleValue
	ExpressionList            []expressionOpValue
	EaExpressionList          []eaExpressionOpValue
}

func scavengingSettingsValueFromSDK(in *ibclient.SettingScavenging) *scavengingSettingsValue {
	if in == nil {
		return nil
	}
	return &scavengingSettingsValue{
		EnableScavenging:          boolPtrOrNil(in.EnableScavenging),
		EnableRecurrentScavenging: boolPtrOrNil(in.EnableRecurrentScavenging),
		EnableAutoReclamation:     boolPtrOrNil(in.EnableAutoReclamation),
		EnableRrLastQueried:       boolPtrOrNil(in.EnableRrLastQueried),
		EnableZoneLastQueried:     boolPtrOrNil(in.EnableZoneLastQueried),
		ReclaimAssociatedRecords:  boolPtrOrNil(in.ReclaimAssociatedRecords),
		ScavengingSchedule:        scavengingScheduleValueFromSDK(in.ScavengingSchedule),
		ExpressionList:            expressionOpValuesFromSDKPtr(in.ExpressionList),
		EaExpressionList:          eaExpressionOpValuesFromSDKPtr(in.EaExpressionList),
	}
}

func scavengingSettingsValueToSDK(in *scavengingSettingsValue) *ibclient.SettingScavenging {
	if in == nil {
		return nil
	}
	return &ibclient.SettingScavenging{
		EnableScavenging:          boolOrFalse(in.EnableScavenging),
		EnableRecurrentScavenging: boolOrFalse(in.EnableRecurrentScavenging),
		EnableAutoReclamation:     boolOrFalse(in.EnableAutoReclamation),
		EnableRrLastQueried:       boolOrFalse(in.EnableRrLastQueried),
		EnableZoneLastQueried:     boolOrFalse(in.EnableZoneLastQueried),
		ReclaimAssociatedRecords:  boolOrFalse(in.ReclaimAssociatedRecords),
		ScavengingSchedule:        scavengingScheduleValueToSDK(in.ScavengingSchedule),
		ExpressionList:            expressionOpValuesToSDKPtr(in.ExpressionList),
		EaExpressionList:          eaExpressionOpValuesToSDKPtr(in.EaExpressionList),
	}
}

// cloudInfoDelegatedMemberValue is the scope-neutral value bag for the SDK's Dhcpmember / CRD's DNSViewCloudInfoDelegatedMember nested type (cloud-managed views only, response-only).
type cloudInfoDelegatedMemberValue struct {
	Ipv4Addr *string
	Ipv6Addr *string
	Name     *string
}

// cloudInfoValue is the scope-neutral value bag for the SDK's GridCloudapiInfo / CRD's DNSViewCloudInfo nested type (response-only, no ForProvider equivalent).
type cloudInfoValue struct {
	DelegatedMember *cloudInfoDelegatedMemberValue
	DelegatedScope  *string
	DelegatedRoot   *string
	OwnedByAdaptor  *bool
	Usage           *string
	Tenant          *string
	MgmtPlatform    *string
	AuthorityType   *string
}

// cloudInfoValueFromSDK converts a WAPI GridCloudapiInfo response into the
// scope-neutral value bag. There is no reverse (ToSDK) direction — CloudInfo
// is response-only (cloud-managed views only) and never sent in a request.
func cloudInfoValueFromSDK(in *ibclient.GridCloudapiInfo) *cloudInfoValue {
	if in == nil {
		return nil
	}
	var dm *cloudInfoDelegatedMemberValue
	if in.DelegatedMember != nil {
		dm = &cloudInfoDelegatedMemberValue{
			Ipv4Addr: strPtrOrNil(in.DelegatedMember.Ipv4Addr),
			Ipv6Addr: strPtrOrNil(in.DelegatedMember.Ipv6Addr),
			Name:     strPtrOrNil(in.DelegatedMember.Name),
		}
	}
	return &cloudInfoValue{
		DelegatedMember: dm,
		DelegatedScope:  strPtrOrNil(in.DelegatedScope),
		DelegatedRoot:   strPtrOrNil(in.DelegatedRoot),
		OwnedByAdaptor:  boolPtrOrNil(in.OwnedByAdaptor),
		Usage:           strPtrOrNil(in.Usage),
		Tenant:          strPtrOrNil(in.Tenant),
		MgmtPlatform:    strPtrOrNil(in.MgmtPlatform),
		AuthorityType:   strPtrOrNil(in.AuthorityType),
	}
}

// ── DNSView field bag (shared spec/observation currency) ────────────────
//
// dnsViewFields holds every mutable DNSViewParameters/DNSViewObservation
// field in its flat, scope-neutral form (is_default, the server-assigned
// _ref, and cloud_info are response-only and handled separately — see
// clusterObservationFromFields/namespacedObservationFromFields in
// cluster.go/namespaced.go). Both cluster.go and namespaced.go convert
// their scope-specific ForProvider struct into this bag (and back out
// into their scope-specific Observation struct), so all comparison,
// late-init, request-building, and response-parsing logic lives here
// exactly once.
type dnsViewFields struct {
	Name                                *string
	Comment                             *string
	NetworkView                         *string
	Disable                             *bool
	BlacklistAction                     *string
	BlacklistLogQuery                   *bool
	BlacklistRedirectAddresses          []string
	BlacklistRedirectTTL                *int64
	BlacklistRulesets                   []string
	UseBlacklist                        *bool
	EnableBlacklist                     *bool
	RootNameServerType                  *string
	UseRootNameServer                   *bool
	DdnsForceCreationTimestampUpdate    *bool
	UseDdnsForceCreationTimestampUpdate *bool
	DdnsPrincipalGroup                  *string
	DdnsPrincipalTracking               *bool
	UseDdnsPrincipalSecurity            *bool
	DdnsRestrictPatterns                *bool
	DdnsRestrictPatternsList            []string
	UseDdnsPatternsRestriction          *bool
	DdnsRestrictProtected               *bool
	UseDdnsRestrictProtected            *bool
	DdnsRestrictSecure                  *bool
	DdnsRestrictStatic                  *bool
	UseDdnsRestrictStatic               *bool
	Dns64Enabled                        *bool
	Dns64Groups                         []string
	UseDns64                            *bool
	DnssecEnabled                       *bool
	DnssecExpiredSignaturesEnabled      *bool
	DnssecNegativeTrustAnchors          []string
	DnssecValidationEnabled             *bool
	UseDnssec                           *bool
	EnableFixedRrsetOrderFqdns          *bool
	UseFixedRrsetOrderFqdns             *bool
	EnableMatchRecursiveOnly            *bool
	FilterAaaa                          *string
	UseFilterAaaa                       *bool
	ForwardOnly                         *bool
	Forwarders                          []string
	UseForwarders                       *bool
	LameTTL                             *int64
	UseLameTTL                          *bool
	MaxCacheTTL                         *int64
	UseMaxCacheTTL                      *bool
	MaxNcacheTTL                        *int64
	UseMaxNcacheTTL                     *bool
	MaxUDPSize                          *int64
	UseMaxUDPSize                       *bool
	NotifyDelay                         *int64
	NxdomainLogQuery                    *bool
	NxdomainRedirect                    *bool
	NxdomainRedirectAddresses           []string
	NxdomainRedirectAddressesV6         []string
	NxdomainRedirectTTL                 *int64
	NxdomainRulesets                    []string
	UseNxdomainRedirect                 *bool
	Recursion                           *bool
	UseRecursion                        *bool
	UseResponseRateLimiting             *bool
	RpzDropIPRuleEnabled                *bool
	RpzDropIPRuleMinPrefixLengthIPv4    *int64
	RpzDropIPRuleMinPrefixLengthIPv6    *int64
	UseRpzDropIPRule                    *bool
	RpzQnameWaitRecurse                 *bool
	UseRpzQnameWaitRecurse              *bool
	UseScavengingSettings               *bool
	UseSortlist                         *bool
	ExtAttrs                            map[string]string

	CustomRootNameServers []nameServerValue
	DnssecTrustedKeys     []dnssecTrustedKeyValue
	FixedRrsetOrderFqdns  []fixedRrsetOrderFqdnValue
	FilterAaaaList        []addressAcValue
	MatchClients          []addressAcValue
	MatchDestinations     []addressAcValue
	ResponseRateLimiting  *responseRateLimitingValue
	ScavengingSettings    *scavengingSettingsValue
	Sortlist              []sortlistEntryValue
}

// ── WAPI request/response translation ────────────────────────────────────

func buildView(f dnsViewFields) *ibclient.View {
	v := &ibclient.View{
		Name:                                f.Name,
		Comment:                             f.Comment,
		NetworkView:                         f.NetworkView,
		Disable:                             f.Disable,
		BlacklistAction:                     strOrEmpty(f.BlacklistAction),
		BlacklistLogQuery:                   f.BlacklistLogQuery,
		BlacklistRedirectAddresses:          f.BlacklistRedirectAddresses,
		BlacklistRedirectTtl:                int64PtrToUint32Ptr(f.BlacklistRedirectTTL),
		BlacklistRulesets:                   f.BlacklistRulesets,
		UseBlacklist:                        f.UseBlacklist,
		EnableBlacklist:                     f.EnableBlacklist,
		RootNameServerType:                  strOrEmpty(f.RootNameServerType),
		UseRootNameServer:                   f.UseRootNameServer,
		DdnsForceCreationTimestampUpdate:    f.DdnsForceCreationTimestampUpdate,
		UseDdnsForceCreationTimestampUpdate: f.UseDdnsForceCreationTimestampUpdate,
		DdnsPrincipalGroup:                  f.DdnsPrincipalGroup,
		DdnsPrincipalTracking:               f.DdnsPrincipalTracking,
		UseDdnsPrincipalSecurity:            f.UseDdnsPrincipalSecurity,
		DdnsRestrictPatterns:                f.DdnsRestrictPatterns,
		DdnsRestrictPatternsList:            f.DdnsRestrictPatternsList,
		UseDdnsPatternsRestriction:          f.UseDdnsPatternsRestriction,
		DdnsRestrictProtected:               f.DdnsRestrictProtected,
		UseDdnsRestrictProtected:            f.UseDdnsRestrictProtected,
		DdnsRestrictSecure:                  f.DdnsRestrictSecure,
		DdnsRestrictStatic:                  f.DdnsRestrictStatic,
		UseDdnsRestrictStatic:               f.UseDdnsRestrictStatic,
		Dns64Enabled:                        f.Dns64Enabled,
		Dns64Groups:                         f.Dns64Groups,
		UseDns64:                            f.UseDns64,
		DnssecEnabled:                       f.DnssecEnabled,
		DnssecExpiredSignaturesEnabled:      f.DnssecExpiredSignaturesEnabled,
		DnssecNegativeTrustAnchors:          f.DnssecNegativeTrustAnchors,
		DnssecValidationEnabled:             f.DnssecValidationEnabled,
		UseDnssec:                           f.UseDnssec,
		EnableFixedRrsetOrderFqdns:          f.EnableFixedRrsetOrderFqdns,
		UseFixedRrsetOrderFqdns:             f.UseFixedRrsetOrderFqdns,
		EnableMatchRecursiveOnly:            f.EnableMatchRecursiveOnly,
		FilterAaaa:                          strOrEmpty(f.FilterAaaa),
		UseFilterAaaa:                       f.UseFilterAaaa,
		ForwardOnly:                         f.ForwardOnly,
		Forwarders:                          f.Forwarders,
		UseForwarders:                       f.UseForwarders,
		LameTtl:                             int64PtrToUint32Ptr(f.LameTTL),
		UseLameTtl:                          f.UseLameTTL,
		MaxCacheTtl:                         int64PtrToUint32Ptr(f.MaxCacheTTL),
		UseMaxCacheTtl:                      f.UseMaxCacheTTL,
		MaxNcacheTtl:                        int64PtrToUint32Ptr(f.MaxNcacheTTL),
		UseMaxNcacheTtl:                     f.UseMaxNcacheTTL,
		MaxUdpSize:                          int64PtrToUint32Ptr(f.MaxUDPSize),
		UseMaxUdpSize:                       f.UseMaxUDPSize,
		NotifyDelay:                         int64PtrToUint32Ptr(f.NotifyDelay),
		NxdomainLogQuery:                    f.NxdomainLogQuery,
		NxdomainRedirect:                    f.NxdomainRedirect,
		NxdomainRedirectAddresses:           f.NxdomainRedirectAddresses,
		NxdomainRedirectAddressesV6:         f.NxdomainRedirectAddressesV6,
		NxdomainRedirectTtl:                 int64PtrToUint32Ptr(f.NxdomainRedirectTTL),
		NxdomainRulesets:                    f.NxdomainRulesets,
		UseNxdomainRedirect:                 f.UseNxdomainRedirect,
		Recursion:                           f.Recursion,
		UseRecursion:                        f.UseRecursion,
		UseResponseRateLimiting:             f.UseResponseRateLimiting,
		RpzDropIpRuleEnabled:                f.RpzDropIPRuleEnabled,
		RpzDropIpRuleMinPrefixLengthIpv4:    int64PtrToUint32Ptr(f.RpzDropIPRuleMinPrefixLengthIPv4),
		RpzDropIpRuleMinPrefixLengthIpv6:    int64PtrToUint32Ptr(f.RpzDropIPRuleMinPrefixLengthIPv6),
		UseRpzDropIpRule:                    f.UseRpzDropIPRule,
		RpzQnameWaitRecurse:                 f.RpzQnameWaitRecurse,
		UseRpzQnameWaitRecurse:              f.UseRpzQnameWaitRecurse,
		UseScavengingSettings:               f.UseScavengingSettings,
		UseSortlist:                         f.UseSortlist,
		Ea:                                  buildEA(f.ExtAttrs),
		CustomRootNameServers:               nameServerValuesToSDK(f.CustomRootNameServers),
		DnssecTrustedKeys:                   dnssecTrustedKeyValuesToSDKPtr(f.DnssecTrustedKeys),
		FixedRrsetOrderFqdns:                fixedRrsetOrderFqdnValuesToSDKPtr(f.FixedRrsetOrderFqdns),
		FilterAaaaList:                      addressAcValuesToSDKPtr(f.FilterAaaaList),
		MatchClients:                        addressAcValuesToSDKPtr(f.MatchClients),
		MatchDestinations:                   addressAcValuesToSDKPtr(f.MatchDestinations),
		Sortlist:                            sortlistEntryValuesToSDKPtr(f.Sortlist),
		ResponseRateLimiting:                responseRateLimitingValueToSDK(f.ResponseRateLimiting),
		ScavengingSettings:                  scavengingSettingsValueToSDK(f.ScavengingSettings),
	}
	return v
}
func fieldsFromView(v *ibclient.View) dnsViewFields {
	f := dnsViewFields{
		Name:                                v.Name,
		Comment:                             v.Comment,
		NetworkView:                         v.NetworkView,
		Disable:                             v.Disable,
		BlacklistAction:                     strPtrOrNil(v.BlacklistAction),
		BlacklistLogQuery:                   v.BlacklistLogQuery,
		BlacklistRedirectAddresses:          v.BlacklistRedirectAddresses,
		BlacklistRedirectTTL:                uint32PtrToInt64Ptr(v.BlacklistRedirectTtl),
		BlacklistRulesets:                   v.BlacklistRulesets,
		UseBlacklist:                        v.UseBlacklist,
		EnableBlacklist:                     v.EnableBlacklist,
		RootNameServerType:                  strPtrOrNil(v.RootNameServerType),
		UseRootNameServer:                   v.UseRootNameServer,
		DdnsForceCreationTimestampUpdate:    v.DdnsForceCreationTimestampUpdate,
		UseDdnsForceCreationTimestampUpdate: v.UseDdnsForceCreationTimestampUpdate,
		DdnsPrincipalGroup:                  v.DdnsPrincipalGroup,
		DdnsPrincipalTracking:               v.DdnsPrincipalTracking,
		UseDdnsPrincipalSecurity:            v.UseDdnsPrincipalSecurity,
		DdnsRestrictPatterns:                v.DdnsRestrictPatterns,
		DdnsRestrictPatternsList:            v.DdnsRestrictPatternsList,
		UseDdnsPatternsRestriction:          v.UseDdnsPatternsRestriction,
		DdnsRestrictProtected:               v.DdnsRestrictProtected,
		UseDdnsRestrictProtected:            v.UseDdnsRestrictProtected,
		DdnsRestrictSecure:                  v.DdnsRestrictSecure,
		DdnsRestrictStatic:                  v.DdnsRestrictStatic,
		UseDdnsRestrictStatic:               v.UseDdnsRestrictStatic,
		Dns64Enabled:                        v.Dns64Enabled,
		Dns64Groups:                         v.Dns64Groups,
		UseDns64:                            v.UseDns64,
		DnssecEnabled:                       v.DnssecEnabled,
		DnssecExpiredSignaturesEnabled:      v.DnssecExpiredSignaturesEnabled,
		DnssecNegativeTrustAnchors:          v.DnssecNegativeTrustAnchors,
		DnssecValidationEnabled:             v.DnssecValidationEnabled,
		UseDnssec:                           v.UseDnssec,
		EnableFixedRrsetOrderFqdns:          v.EnableFixedRrsetOrderFqdns,
		UseFixedRrsetOrderFqdns:             v.UseFixedRrsetOrderFqdns,
		EnableMatchRecursiveOnly:            v.EnableMatchRecursiveOnly,
		FilterAaaa:                          strPtrOrNil(v.FilterAaaa),
		UseFilterAaaa:                       v.UseFilterAaaa,
		ForwardOnly:                         v.ForwardOnly,
		Forwarders:                          v.Forwarders,
		UseForwarders:                       v.UseForwarders,
		LameTTL:                             uint32PtrToInt64Ptr(v.LameTtl),
		UseLameTTL:                          v.UseLameTtl,
		MaxCacheTTL:                         uint32PtrToInt64Ptr(v.MaxCacheTtl),
		UseMaxCacheTTL:                      v.UseMaxCacheTtl,
		MaxNcacheTTL:                        uint32PtrToInt64Ptr(v.MaxNcacheTtl),
		UseMaxNcacheTTL:                     v.UseMaxNcacheTtl,
		MaxUDPSize:                          uint32PtrToInt64Ptr(v.MaxUdpSize),
		UseMaxUDPSize:                       v.UseMaxUdpSize,
		NotifyDelay:                         uint32PtrToInt64Ptr(v.NotifyDelay),
		NxdomainLogQuery:                    v.NxdomainLogQuery,
		NxdomainRedirect:                    v.NxdomainRedirect,
		NxdomainRedirectAddresses:           v.NxdomainRedirectAddresses,
		NxdomainRedirectAddressesV6:         v.NxdomainRedirectAddressesV6,
		NxdomainRedirectTTL:                 uint32PtrToInt64Ptr(v.NxdomainRedirectTtl),
		NxdomainRulesets:                    v.NxdomainRulesets,
		UseNxdomainRedirect:                 v.UseNxdomainRedirect,
		Recursion:                           v.Recursion,
		UseRecursion:                        v.UseRecursion,
		UseResponseRateLimiting:             v.UseResponseRateLimiting,
		RpzDropIPRuleEnabled:                v.RpzDropIpRuleEnabled,
		RpzDropIPRuleMinPrefixLengthIPv4:    uint32PtrToInt64Ptr(v.RpzDropIpRuleMinPrefixLengthIpv4),
		RpzDropIPRuleMinPrefixLengthIPv6:    uint32PtrToInt64Ptr(v.RpzDropIpRuleMinPrefixLengthIpv6),
		UseRpzDropIPRule:                    v.UseRpzDropIpRule,
		RpzQnameWaitRecurse:                 v.RpzQnameWaitRecurse,
		UseRpzQnameWaitRecurse:              v.UseRpzQnameWaitRecurse,
		UseScavengingSettings:               v.UseScavengingSettings,
		UseSortlist:                         v.UseSortlist,
		ExtAttrs:                            extAttrsFromEA(v.Ea),
		CustomRootNameServers:               nameServerValuesFromSDK(v.CustomRootNameServers),
		DnssecTrustedKeys:                   dnssecTrustedKeyValuesFromSDKPtr(v.DnssecTrustedKeys),
		FixedRrsetOrderFqdns:                fixedRrsetOrderFqdnValuesFromSDKPtr(v.FixedRrsetOrderFqdns),
		FilterAaaaList:                      addressAcValuesFromSDKPtr(v.FilterAaaaList),
		MatchClients:                        addressAcValuesFromSDKPtr(v.MatchClients),
		MatchDestinations:                   addressAcValuesFromSDKPtr(v.MatchDestinations),
		Sortlist:                            sortlistEntryValuesFromSDKPtr(v.Sortlist),
		ResponseRateLimiting:                responseRateLimitingValueFromSDK(v.ResponseRateLimiting),
		ScavengingSettings:                  scavengingSettingsValueFromSDK(v.ScavengingSettings),
	}
	return f
}

// ── field comparison / late-init ─────────────────────────────────────────

// dnsViewFieldComparators holds one equality check per mutable
// DNSViewParameters field (is_default is immutable/response-only and has
// no ForProvider representation, so it is never compared here). Expressed
// as a data-driven table rather than one large if-chain — a single
// function comparing all ~80 fields inline exceeds this repo's
// cyclomatic-complexity budget; a table plus a single loop keeps
// isUpToDate itself trivial to read while still comparing every field.
var dnsViewFieldComparators = []func(desired, observed dnsViewFields) bool{
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.Name) == strOrEmpty(observed.Name)
	},
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.Comment) == strOrEmpty(observed.Comment)
	},
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.NetworkView) == strOrEmpty(observed.NetworkView)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.Disable) == boolOrFalse(observed.Disable)
	},
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.BlacklistAction) == strOrEmpty(observed.BlacklistAction)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.BlacklistLogQuery) == boolOrFalse(observed.BlacklistLogQuery)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.BlacklistRedirectAddresses, observed.BlacklistRedirectAddresses)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.BlacklistRedirectTTL) == int64OrZero(observed.BlacklistRedirectTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.BlacklistRulesets, observed.BlacklistRulesets)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseBlacklist) == boolOrFalse(observed.UseBlacklist)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.EnableBlacklist) == boolOrFalse(observed.EnableBlacklist)
	},
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.RootNameServerType) == strOrEmpty(observed.RootNameServerType)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRootNameServer) == boolOrFalse(observed.UseRootNameServer)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DdnsForceCreationTimestampUpdate) == boolOrFalse(observed.DdnsForceCreationTimestampUpdate)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsForceCreationTimestampUpdate) == boolOrFalse(observed.UseDdnsForceCreationTimestampUpdate)
	},
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.DdnsPrincipalGroup) == strOrEmpty(observed.DdnsPrincipalGroup)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DdnsPrincipalTracking) == boolOrFalse(observed.DdnsPrincipalTracking)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsPrincipalSecurity) == boolOrFalse(observed.UseDdnsPrincipalSecurity)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DdnsRestrictPatterns) == boolOrFalse(observed.DdnsRestrictPatterns)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.DdnsRestrictPatternsList, observed.DdnsRestrictPatternsList)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsPatternsRestriction) == boolOrFalse(observed.UseDdnsPatternsRestriction)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DdnsRestrictProtected) == boolOrFalse(observed.DdnsRestrictProtected)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsRestrictProtected) == boolOrFalse(observed.UseDdnsRestrictProtected)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DdnsRestrictSecure) == boolOrFalse(observed.DdnsRestrictSecure)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DdnsRestrictStatic) == boolOrFalse(observed.DdnsRestrictStatic)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsRestrictStatic) == boolOrFalse(observed.UseDdnsRestrictStatic)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.Dns64Enabled) == boolOrFalse(observed.Dns64Enabled)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.Dns64Groups, observed.Dns64Groups)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDns64) == boolOrFalse(observed.UseDns64)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DnssecEnabled) == boolOrFalse(observed.DnssecEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DnssecExpiredSignaturesEnabled) == boolOrFalse(observed.DnssecExpiredSignaturesEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.DnssecNegativeTrustAnchors, observed.DnssecNegativeTrustAnchors)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.DnssecValidationEnabled) == boolOrFalse(observed.DnssecValidationEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDnssec) == boolOrFalse(observed.UseDnssec)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.EnableFixedRrsetOrderFqdns) == boolOrFalse(observed.EnableFixedRrsetOrderFqdns)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseFixedRrsetOrderFqdns) == boolOrFalse(observed.UseFixedRrsetOrderFqdns)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.EnableMatchRecursiveOnly) == boolOrFalse(observed.EnableMatchRecursiveOnly)
	},
	func(desired, observed dnsViewFields) bool {
		return strOrEmpty(desired.FilterAaaa) == strOrEmpty(observed.FilterAaaa)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseFilterAaaa) == boolOrFalse(observed.UseFilterAaaa)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.ForwardOnly) == boolOrFalse(observed.ForwardOnly)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.Forwarders, observed.Forwarders)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseForwarders) == boolOrFalse(observed.UseForwarders)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.LameTTL) == int64OrZero(observed.LameTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseLameTTL) == boolOrFalse(observed.UseLameTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.MaxCacheTTL) == int64OrZero(observed.MaxCacheTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseMaxCacheTTL) == boolOrFalse(observed.UseMaxCacheTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.MaxNcacheTTL) == int64OrZero(observed.MaxNcacheTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseMaxNcacheTTL) == boolOrFalse(observed.UseMaxNcacheTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.MaxUDPSize) == int64OrZero(observed.MaxUDPSize)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseMaxUDPSize) == boolOrFalse(observed.UseMaxUDPSize)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.NotifyDelay) == int64OrZero(observed.NotifyDelay)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.NxdomainLogQuery) == boolOrFalse(observed.NxdomainLogQuery)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.NxdomainRedirect) == boolOrFalse(observed.NxdomainRedirect)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.NxdomainRedirectAddresses, observed.NxdomainRedirectAddresses)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.NxdomainRedirectAddressesV6, observed.NxdomainRedirectAddressesV6)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.NxdomainRedirectTTL) == int64OrZero(observed.NxdomainRedirectTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.NxdomainRulesets, observed.NxdomainRulesets)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseNxdomainRedirect) == boolOrFalse(observed.UseNxdomainRedirect)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.Recursion) == boolOrFalse(observed.Recursion)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRecursion) == boolOrFalse(observed.UseRecursion)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseResponseRateLimiting) == boolOrFalse(observed.UseResponseRateLimiting)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.RpzDropIPRuleEnabled) == boolOrFalse(observed.RpzDropIPRuleEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.RpzDropIPRuleMinPrefixLengthIPv4) == int64OrZero(observed.RpzDropIPRuleMinPrefixLengthIPv4)
	},
	func(desired, observed dnsViewFields) bool {
		return int64OrZero(desired.RpzDropIPRuleMinPrefixLengthIPv6) == int64OrZero(observed.RpzDropIPRuleMinPrefixLengthIPv6)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRpzDropIPRule) == boolOrFalse(observed.UseRpzDropIPRule)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.RpzQnameWaitRecurse) == boolOrFalse(observed.RpzQnameWaitRecurse)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRpzQnameWaitRecurse) == boolOrFalse(observed.UseRpzQnameWaitRecurse)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseScavengingSettings) == boolOrFalse(observed.UseScavengingSettings)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseSortlist) == boolOrFalse(observed.UseSortlist)
	},
	func(desired, observed dnsViewFields) bool { return extAttrsEqual(desired.ExtAttrs, observed.ExtAttrs) },
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.CustomRootNameServers, observed.CustomRootNameServers)
	},
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.DnssecTrustedKeys, observed.DnssecTrustedKeys)
	},
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.FixedRrsetOrderFqdns, observed.FixedRrsetOrderFqdns)
	},
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.FilterAaaaList, observed.FilterAaaaList)
	},
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.MatchClients, observed.MatchClients)
	},
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.MatchDestinations, observed.MatchDestinations)
	},
	func(desired, observed dnsViewFields) bool {
		return nestedSliceEqual(desired.Sortlist, observed.Sortlist)
	},
	func(desired, observed dnsViewFields) bool {
		return reflect.DeepEqual(desired.ResponseRateLimiting, observed.ResponseRateLimiting)
	},
	func(desired, observed dnsViewFields) bool {
		return reflect.DeepEqual(desired.ScavengingSettings, observed.ScavengingSettings)
	},
}

// isUpToDate compares the desired DNSView fields against the observed
// ones using dnsViewFieldComparators. is_default is excluded (see the
// comparator table's doc comment) — it is read-only and has no
// ForProvider representation, so including it would trigger a permanent
// Update loop the moment the WAPI-assigned value differed from a
// hypothetical desired value that can never actually be set.
func isUpToDate(desired, observed dnsViewFields) bool {
	for _, eq := range dnsViewFieldComparators {
		if !eq(desired, observed) {
			return false
		}
	}
	return true
}

// dnsViewLateInitOps holds one back-fill operation per mutable
// DNSViewParameters field (Name is required and never late-initialized;
// is_default has no ForProvider field at all). Expressed as a
// data-driven table for the same cyclomatic-complexity reason as
// dnsViewFieldComparators above.
var dnsViewLateInitOps = []func(desired *dnsViewFields, observed dnsViewFields) bool{
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringPtr(&desired.Comment, observed.Comment)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringPtr(&desired.NetworkView, observed.NetworkView)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.Disable, observed.Disable)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringPtr(&desired.BlacklistAction, observed.BlacklistAction)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.BlacklistLogQuery, observed.BlacklistLogQuery)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.BlacklistRedirectAddresses, observed.BlacklistRedirectAddresses)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.BlacklistRedirectTTL, observed.BlacklistRedirectTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.BlacklistRulesets, observed.BlacklistRulesets)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseBlacklist, observed.UseBlacklist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.EnableBlacklist, observed.EnableBlacklist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringPtr(&desired.RootNameServerType, observed.RootNameServerType)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRootNameServer, observed.UseRootNameServer)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DdnsForceCreationTimestampUpdate, observed.DdnsForceCreationTimestampUpdate)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsForceCreationTimestampUpdate, observed.UseDdnsForceCreationTimestampUpdate)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringPtr(&desired.DdnsPrincipalGroup, observed.DdnsPrincipalGroup)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DdnsPrincipalTracking, observed.DdnsPrincipalTracking)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsPrincipalSecurity, observed.UseDdnsPrincipalSecurity)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DdnsRestrictPatterns, observed.DdnsRestrictPatterns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.DdnsRestrictPatternsList, observed.DdnsRestrictPatternsList)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsPatternsRestriction, observed.UseDdnsPatternsRestriction)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DdnsRestrictProtected, observed.DdnsRestrictProtected)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsRestrictProtected, observed.UseDdnsRestrictProtected)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DdnsRestrictSecure, observed.DdnsRestrictSecure)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DdnsRestrictStatic, observed.DdnsRestrictStatic)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsRestrictStatic, observed.UseDdnsRestrictStatic)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.Dns64Enabled, observed.Dns64Enabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.Dns64Groups, observed.Dns64Groups)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDns64, observed.UseDns64)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DnssecEnabled, observed.DnssecEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DnssecExpiredSignaturesEnabled, observed.DnssecExpiredSignaturesEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.DnssecNegativeTrustAnchors, observed.DnssecNegativeTrustAnchors)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.DnssecValidationEnabled, observed.DnssecValidationEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDnssec, observed.UseDnssec)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.EnableFixedRrsetOrderFqdns, observed.EnableFixedRrsetOrderFqdns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseFixedRrsetOrderFqdns, observed.UseFixedRrsetOrderFqdns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.EnableMatchRecursiveOnly, observed.EnableMatchRecursiveOnly)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringPtr(&desired.FilterAaaa, observed.FilterAaaa)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseFilterAaaa, observed.UseFilterAaaa)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.ForwardOnly, observed.ForwardOnly)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.Forwarders, observed.Forwarders)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseForwarders, observed.UseForwarders)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.LameTTL, observed.LameTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseLameTTL, observed.UseLameTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.MaxCacheTTL, observed.MaxCacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseMaxCacheTTL, observed.UseMaxCacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.MaxNcacheTTL, observed.MaxNcacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseMaxNcacheTTL, observed.UseMaxNcacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.MaxUDPSize, observed.MaxUDPSize)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseMaxUDPSize, observed.UseMaxUDPSize)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.NotifyDelay, observed.NotifyDelay)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.NxdomainLogQuery, observed.NxdomainLogQuery)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.NxdomainRedirect, observed.NxdomainRedirect)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.NxdomainRedirectAddresses, observed.NxdomainRedirectAddresses)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.NxdomainRedirectAddressesV6, observed.NxdomainRedirectAddressesV6)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.NxdomainRedirectTTL, observed.NxdomainRedirectTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.NxdomainRulesets, observed.NxdomainRulesets)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseNxdomainRedirect, observed.UseNxdomainRedirect)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.Recursion, observed.Recursion)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRecursion, observed.UseRecursion)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseResponseRateLimiting, observed.UseResponseRateLimiting)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.RpzDropIPRuleEnabled, observed.RpzDropIPRuleEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.RpzDropIPRuleMinPrefixLengthIPv4, observed.RpzDropIPRuleMinPrefixLengthIPv4)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.RpzDropIPRuleMinPrefixLengthIPv6, observed.RpzDropIPRuleMinPrefixLengthIPv6)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRpzDropIPRule, observed.UseRpzDropIPRule)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.RpzQnameWaitRecurse, observed.RpzQnameWaitRecurse)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRpzQnameWaitRecurse, observed.UseRpzQnameWaitRecurse)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseScavengingSettings, observed.UseScavengingSettings)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseSortlist, observed.UseSortlist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitMap(&desired.ExtAttrs, observed.ExtAttrs)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.CustomRootNameServers, observed.CustomRootNameServers)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.DnssecTrustedKeys, observed.DnssecTrustedKeys)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.FixedRrsetOrderFqdns, observed.FixedRrsetOrderFqdns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.FilterAaaaList, observed.FilterAaaaList)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.MatchClients, observed.MatchClients)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.MatchDestinations, observed.MatchDestinations)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.Sortlist, observed.Sortlist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.ResponseRateLimiting, observed.ResponseRateLimiting)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.ScavengingSettings, observed.ScavengingSettings)
	},
}

// lateInitializeFields back-fills server-defaulted mutable fields from
// observed into desired (so isUpToDate does not see phantom drift on the
// next reconcile) by running every operation in dnsViewLateInitOps.
// Returns the updated bag and whether anything changed.
func lateInitializeFields(desired, observed dnsViewFields) (dnsViewFields, bool) {
	changed := false
	for _, op := range dnsViewLateInitOps {
		if op(&desired, observed) {
			changed = true
		}
	}
	return desired, changed
}

// ── WAPI call wrappers (shared by both scopes) ──────────────────────────

// dnsViewReturnFields lists every WAPI view field beyond
// ibclient.NewEmptyDNSView's default return-field set (extattrs, name,
// network_view, comment) — i.e. every field mirrored by DNSViewObservation
// (full-mirror AtProvider convention).
//
// edns_udp_size/use_edns_udp_size/last_queried_acl are deliberately
// excluded: the provider is pinned to WAPI 2.9.7, whose `view` object
// schema does not define these fields at all (confirmed live against the
// Grid Manager). Requesting them in the GET return-fields list fails
// every Observe() with a 400 (AdmConProtoError: Unknown argument/field).
// The controller no longer reads, writes, or compares these fields
// anywhere.
var dnsViewReturnFields = []string{
	"blacklist_action",
	"blacklist_log_query",
	"blacklist_redirect_addresses",
	"blacklist_redirect_ttl",
	"blacklist_rulesets",
	"cloud_info",
	"custom_root_name_servers",
	"ddns_force_creation_timestamp_update",
	"ddns_principal_group",
	"ddns_principal_tracking",
	"ddns_restrict_patterns",
	"ddns_restrict_patterns_list",
	"ddns_restrict_protected",
	"ddns_restrict_secure",
	"ddns_restrict_static",
	"disable",
	"dns64_enabled",
	"dns64_groups",
	"dnssec_enabled",
	"dnssec_expired_signatures_enabled",
	"dnssec_negative_trust_anchors",
	"dnssec_trusted_keys",
	"dnssec_validation_enabled",
	"enable_blacklist",
	"enable_fixed_rrset_order_fqdns",
	"enable_match_recursive_only",
	"filter_aaaa",
	"filter_aaaa_list",
	"fixed_rrset_order_fqdns",
	"forward_only",
	"forwarders",
	"is_default",
	"lame_ttl",
	"match_clients",
	"match_destinations",
	"max_cache_ttl",
	"max_ncache_ttl",
	"max_udp_size",
	"notify_delay",
	"nxdomain_log_query",
	"nxdomain_redirect",
	"nxdomain_redirect_addresses",
	"nxdomain_redirect_addresses_v6",
	"nxdomain_redirect_ttl",
	"nxdomain_rulesets",
	"recursion",
	"response_rate_limiting",
	"root_name_server_type",
	"rpz_drop_ip_rule_enabled",
	"rpz_drop_ip_rule_min_prefix_length_ipv4",
	"rpz_drop_ip_rule_min_prefix_length_ipv6",
	"rpz_qname_wait_recurse",
	"scavenging_settings",
	"sortlist",
	"use_blacklist",
	"use_ddns_force_creation_timestamp_update",
	"use_ddns_patterns_restriction",
	"use_ddns_principal_security",
	"use_ddns_restrict_protected",
	"use_ddns_restrict_static",
	"use_dns64",
	"use_dnssec",
	"use_filter_aaaa",
	"use_fixed_rrset_order_fqdns",
	"use_forwarders",
	"use_lame_ttl",
	"use_max_cache_ttl",
	"use_max_ncache_ttl",
	"use_max_udp_size",
	"use_nxdomain_redirect",
	"use_recursion",
	"use_response_rate_limiting",
	"use_root_name_server",
	"use_rpz_drop_ip_rule",
	"use_rpz_qname_wait_recurse",
	"use_scavenging_settings",
	"use_sortlist",
}

// newViewForGet builds a View query object requesting every field mirrored
// by DNSViewObservation, beyond the SDK's built-in default of {extattrs,
// name, network_view, comment} (ibclient.NewEmptyDNSView's default return
// fields — used here rather than the bare View{} zero value's even
// smaller default of {comment, is_default, name}, since GetDNSView already
// establishes extattrs/network_view as baseline fields for this object).
func newViewForGet() *ibclient.View {
	v := ibclient.NewEmptyDNSView()
	v.SetReturnFields(append(v.ReturnFields(), dnsViewReturnFields...))
	return v
}

// getViewByRef issues a direct WAPI GET for the view object identified by
// ref, requesting every field mirrored by DNSViewObservation.
func getViewByRef(conn ibclient.IBConnector, ref string) (*ibclient.View, error) {
	if ref == "" {
		return nil, errors.New(errEmptyRef)
	}
	v := newViewForGet()
	if err := conn.GetObject(v, ref, ibclient.NewQueryParams(false, nil), v); err != nil {
		return nil, err
	}
	return v, nil
}

// createView issues a direct WAPI POST for a new view object and returns
// the server-assigned _ref.
func createView(conn ibclient.IBConnector, f dnsViewFields) (string, error) {
	return conn.CreateObject(buildView(f))
}

// updateView issues a direct WAPI PUT against ref with the mutable view
// fields (is_default has no ForProvider representation and is therefore
// never present in buildView's output — see the immutable-fields note in
// the package doc comment). WAPI's view PUT is a partial merge (only
// included fields change). Returns the object's current _ref, which
// differs from ref whenever name changed (DNSView is in the _ref-unstable
// resource group).
func updateView(conn ibclient.IBConnector, ref string, f dnsViewFields) (string, error) {
	return conn.UpdateObject(buildView(f), ref)
}

// deleteView issues a direct WAPI DELETE for the view object identified by
// ref.
func deleteView(conn ibclient.IBConnector, ref string) error {
	_, err := conn.DeleteObject(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced DNSView
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterDNSView(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster DNSView controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("DNSView"))

	o.Gate.Register(func() {
		if err := setupNamespacedDNSView(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced DNSView controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("DNSView"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced DNSView controllers
// immediately without SafeStart gating (RBAC fallback path, for
// environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterDNSView(mgr, o); err != nil {
		return err
	}
	return setupNamespacedDNSView(mgr, o)
}
