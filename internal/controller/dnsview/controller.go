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

func uint32OrZero(u *uint32) uint32 {
	if u == nil {
		return 0
	}
	return *u
}

// uint32PtrOrNil converts a plain (non-pointer) SDK uint32 into the CRD's
// *uint32 representation, treating zero as "not set" — nested WAPI structs
// use plain numeric fields with the same not-set/zero ambiguity described
// in boolPtrOrNil.
func uint32PtrOrNil(u uint32) *uint32 {
	if u == 0 {
		return nil
	}
	v := u
	return &v
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

// nestedSliceEqual compares two slices of any nested value-bag type. The
// nil-and-empty check comes first (rather than being folded into the
// length check below) so a nil slice and an empty slice always compare
// equal without ever falling into reflect.DeepEqual's nil-vs-empty
// distinction — reflect.DeepEqual(([]T)(nil), []T{}) is false, which would
// otherwise report permanent drift whenever the API omits an empty list
// from its response.
func nestedSliceEqual[T any](a, b []T) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// gatedBoolEqual compares a bool field that only applies while its use
// flag is on. When the flag is off, the WAPI SDK documents the field as
// grid/parent-inherited (the response echoes the server's own default,
// not what was submitted), so the two sides are unrelated quantities and
// the comparison always reports true — the flag's own (unconditional)
// comparator is what actually detects drift on the flag itself.
func gatedBoolEqual(useFlag, desired, observed *bool) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return boolOrFalse(desired) == boolOrFalse(observed)
}

// gatedStringEqual is the string-field variant of gatedBoolEqual.
func gatedStringEqual(useFlag *bool, desired, observed *string) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return strOrEmpty(desired) == strOrEmpty(observed)
}

// gatedUint32Equal is the *uint32-field variant of gatedBoolEqual.
func gatedUint32Equal(useFlag *bool, desired, observed *uint32) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return uint32OrZero(desired) == uint32OrZero(observed)
}

// gatedStringSliceEqual is the []string-field variant of gatedBoolEqual.
func gatedStringSliceEqual(useFlag *bool, desired, observed []string) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return stringSliceEqual(desired, observed)
}

// gatedNestedSliceEqual is the nested-value-bag-slice variant of
// gatedBoolEqual. eq performs the actual per-item comparison (usually
// nestedSliceEqual[T], or a type-specific comparator for value bags that
// carry their own nested use-flag pair, e.g. nameServerValuesEqual).
func gatedNestedSliceEqual[T any](useFlag *bool, desired, observed []T, eq func(a, b []T) bool) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return eq(desired, observed)
}

// gatedPtrDeepEqual is the nested-struct-pointer variant of gatedBoolEqual
// (e.g. *responseRateLimitingValue, *scavengingSettingsValue).
func gatedPtrDeepEqual[T any](useFlag *bool, desired, observed *T) bool {
	if !boolOrFalse(useFlag) {
		return true
	}
	return reflect.DeepEqual(desired, observed)
}

// effectiveUseFlag resolves what a use flag's value will be once
// lateInitializeFields has finished: the user's own spec value if they set
// one, otherwise the value that will be back-filled from observed. Both the
// flag's own late-init op and every value op it gates read through this
// helper so the gate does not depend on which op happens to run first in
// the ops table (the table is unordered with respect to this dependency).
func effectiveUseFlag(desiredFlag, observedFlag *bool) bool {
	if desiredFlag != nil {
		return *desiredFlag
	}
	return boolOrFalse(observedFlag)
}

// gatedLateInitPtr back-fills *desired from observed only when the gating
// use flag is (or will become) true. When the flag is off, the observed
// value is the grid/parent-inherited default rather than something the
// user's spec implies — writing it into spec would silently claim a
// setting that is not actually in effect.
func gatedLateInitPtr[T any](useFlagDesired, useFlagObserved *bool, desired **T, observed *T) bool {
	if !effectiveUseFlag(useFlagDesired, useFlagObserved) {
		return false
	}
	return lateInitPtr(desired, observed)
}

// gatedLateInitStringPtr is the string-field variant of gatedLateInitPtr.
func gatedLateInitStringPtr(useFlagDesired, useFlagObserved *bool, desired **string, observed *string) bool {
	if !effectiveUseFlag(useFlagDesired, useFlagObserved) {
		return false
	}
	return lateInitStringPtr(desired, observed)
}

// gatedLateInitStringSlice is the []string-field variant of gatedLateInitPtr.
func gatedLateInitStringSlice(useFlagDesired, useFlagObserved *bool, desired *[]string, observed []string) bool {
	if !effectiveUseFlag(useFlagDesired, useFlagObserved) {
		return false
	}
	return lateInitStringSlice(desired, observed)
}

// gatedLateInitNestedSlice is the nested-value-bag-slice variant of
// gatedLateInitPtr.
func gatedLateInitNestedSlice[T any](useFlagDesired, useFlagObserved *bool, desired *[]T, observed []T) bool {
	if !effectiveUseFlag(useFlagDesired, useFlagObserved) {
		return false
	}
	return lateInitNestedSlice(desired, observed)
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

// nameServerValueEqual compares two nameServerValue items. The SDK
// documents use_tsig_key_name as the use flag for tsig_key_name: when it is
// off, tsig_key_name is not something the user's spec can drive (the
// appliance does not apply it), so the two sides are unrelated quantities
// and comparing them unconditionally can never converge.
func nameServerValueEqual(a, b nameServerValue) bool {
	if strOrEmpty(a.Address) != strOrEmpty(b.Address) ||
		strOrEmpty(a.Name) != strOrEmpty(b.Name) ||
		boolOrFalse(a.SharedWithMsParentDelegation) != boolOrFalse(b.SharedWithMsParentDelegation) ||
		boolOrFalse(a.Stealth) != boolOrFalse(b.Stealth) ||
		strOrEmpty(a.TsigKey) != strOrEmpty(b.TsigKey) ||
		strOrEmpty(a.TsigKeyAlg) != strOrEmpty(b.TsigKeyAlg) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(a.UseTsigKeyName) != boolOrFalse(b.UseTsigKeyName) {
		return false
	}
	// Only compare tsig_key_name when the flag is on.
	if boolOrFalse(a.UseTsigKeyName) {
		if strOrEmpty(a.TsigKeyName) != strOrEmpty(b.TsigKeyName) {
			return false
		}
	}
	return true
}

// nameServerValuesEqual compares two slices of nameServerValue item by
// item via nameServerValueEqual, so each item's own use_tsig_key_name gate
// applies. Nil and empty both compare equal, matching nestedSliceEqual.
func nameServerValuesEqual(a, b []nameServerValue) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !nameServerValueEqual(a[i], b[i]) {
			return false
		}
	}
	return true
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

// addressAcValueEqual compares two addressAcValue items. The SDK
// documents use_tsig_key_name as the use flag for tsig_key_name: when it is
// off, tsig_key_name is not something the user's spec can drive (the
// appliance does not apply it), so the two sides are unrelated quantities
// and comparing them unconditionally can never converge.
func addressAcValueEqual(a, b addressAcValue) bool {
	if strOrEmpty(a.Address) != strOrEmpty(b.Address) ||
		strOrEmpty(a.Permission) != strOrEmpty(b.Permission) ||
		strOrEmpty(a.TsigKey) != strOrEmpty(b.TsigKey) ||
		strOrEmpty(a.TsigKeyAlg) != strOrEmpty(b.TsigKeyAlg) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(a.UseTsigKeyName) != boolOrFalse(b.UseTsigKeyName) {
		return false
	}
	// Only compare tsig_key_name when the flag is on.
	if boolOrFalse(a.UseTsigKeyName) {
		if strOrEmpty(a.TsigKeyName) != strOrEmpty(b.TsigKeyName) {
			return false
		}
	}
	return true
}

// addressAcValuesEqual compares two slices of addressAcValue item by item
// via addressAcValueEqual, so each item's own use_tsig_key_name gate
// applies. Nil and empty both compare equal, matching nestedSliceEqual.
func addressAcValuesEqual(a, b []addressAcValue) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !addressAcValueEqual(a[i], b[i]) {
			return false
		}
	}
	return true
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
	ResponsesPerSecond *uint32
	Window             *uint32
	Slip               *uint32
}

func responseRateLimitingValueFromSDK(in *ibclient.GridResponseratelimiting) *responseRateLimitingValue {
	if in == nil {
		return nil
	}
	return &responseRateLimitingValue{
		EnableRrl:          boolPtrOrNil(in.EnableRrl),
		LogOnly:            boolPtrOrNil(in.LogOnly),
		ResponsesPerSecond: uint32PtrOrNil(in.ResponsesPerSecond),
		Window:             uint32PtrOrNil(in.Window),
		Slip:               uint32PtrOrNil(in.Slip),
	}
}

func responseRateLimitingValueToSDK(in *responseRateLimitingValue) *ibclient.GridResponseratelimiting {
	if in == nil {
		return nil
	}
	return &ibclient.GridResponseratelimiting{
		EnableRrl:          boolOrFalse(in.EnableRrl),
		LogOnly:            boolOrFalse(in.LogOnly),
		ResponsesPerSecond: uint32OrZero(in.ResponsesPerSecond),
		Window:             uint32OrZero(in.Window),
		Slip:               uint32OrZero(in.Slip),
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
	Every           *uint32
	MinutesPastHour *uint32
	HourOfDay       *uint32
	Year            *uint32
	Month           *uint32
	DayOfMonth      *uint32
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
		Every:           uint32PtrOrNil(in.Every),
		MinutesPastHour: uint32PtrOrNil(in.MinutesPastHour),
		HourOfDay:       uint32PtrOrNil(in.HourOfDay),
		Year:            uint32PtrOrNil(in.Year),
		Month:           uint32PtrOrNil(in.Month),
		DayOfMonth:      uint32PtrOrNil(in.DayOfMonth),
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
		Every:           uint32OrZero(in.Every),
		MinutesPastHour: uint32OrZero(in.MinutesPastHour),
		HourOfDay:       uint32OrZero(in.HourOfDay),
		Year:            uint32OrZero(in.Year),
		Month:           uint32OrZero(in.Month),
		DayOfMonth:      uint32OrZero(in.DayOfMonth),
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
	BlacklistRedirectTTL                *uint32
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
	LameTTL                             *uint32
	UseLameTTL                          *bool
	MaxCacheTTL                         *uint32
	UseMaxCacheTTL                      *bool
	MaxNcacheTTL                        *uint32
	UseMaxNcacheTTL                     *bool
	NotifyDelay                         *uint32
	NxdomainLogQuery                    *bool
	NxdomainRedirect                    *bool
	NxdomainRedirectAddresses           []string
	NxdomainRedirectAddressesV6         []string
	NxdomainRedirectTTL                 *uint32
	NxdomainRulesets                    []string
	UseNxdomainRedirect                 *bool
	Recursion                           *bool
	UseRecursion                        *bool
	UseResponseRateLimiting             *bool
	RpzDropIPRuleEnabled                *bool
	RpzDropIPRuleMinPrefixLengthIPv4    *uint32
	RpzDropIPRuleMinPrefixLengthIPv6    *uint32
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
		BlacklistRedirectTtl:                f.BlacklistRedirectTTL,
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
		LameTtl:                             f.LameTTL,
		UseLameTtl:                          f.UseLameTTL,
		MaxCacheTtl:                         f.MaxCacheTTL,
		UseMaxCacheTtl:                      f.UseMaxCacheTTL,
		MaxNcacheTtl:                        f.MaxNcacheTTL,
		UseMaxNcacheTtl:                     f.UseMaxNcacheTTL,
		NotifyDelay:                         f.NotifyDelay,
		NxdomainLogQuery:                    f.NxdomainLogQuery,
		NxdomainRedirect:                    f.NxdomainRedirect,
		NxdomainRedirectAddresses:           f.NxdomainRedirectAddresses,
		NxdomainRedirectAddressesV6:         f.NxdomainRedirectAddressesV6,
		NxdomainRedirectTtl:                 f.NxdomainRedirectTTL,
		NxdomainRulesets:                    f.NxdomainRulesets,
		UseNxdomainRedirect:                 f.UseNxdomainRedirect,
		Recursion:                           f.Recursion,
		UseRecursion:                        f.UseRecursion,
		UseResponseRateLimiting:             f.UseResponseRateLimiting,
		RpzDropIpRuleEnabled:                f.RpzDropIPRuleEnabled,
		RpzDropIpRuleMinPrefixLengthIpv4:    f.RpzDropIPRuleMinPrefixLengthIPv4,
		RpzDropIpRuleMinPrefixLengthIpv6:    f.RpzDropIPRuleMinPrefixLengthIPv6,
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
		BlacklistRedirectTTL:                v.BlacklistRedirectTtl,
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
		LameTTL:                             v.LameTtl,
		UseLameTTL:                          v.UseLameTtl,
		MaxCacheTTL:                         v.MaxCacheTtl,
		UseMaxCacheTTL:                      v.UseMaxCacheTtl,
		MaxNcacheTTL:                        v.MaxNcacheTtl,
		UseMaxNcacheTTL:                     v.UseMaxNcacheTtl,
		NotifyDelay:                         v.NotifyDelay,
		NxdomainLogQuery:                    v.NxdomainLogQuery,
		NxdomainRedirect:                    v.NxdomainRedirect,
		NxdomainRedirectAddresses:           v.NxdomainRedirectAddresses,
		NxdomainRedirectAddressesV6:         v.NxdomainRedirectAddressesV6,
		NxdomainRedirectTTL:                 v.NxdomainRedirectTtl,
		NxdomainRulesets:                    v.NxdomainRulesets,
		UseNxdomainRedirect:                 v.UseNxdomainRedirect,
		Recursion:                           v.Recursion,
		UseRecursion:                        v.UseRecursion,
		UseResponseRateLimiting:             v.UseResponseRateLimiting,
		RpzDropIPRuleEnabled:                v.RpzDropIpRuleEnabled,
		RpzDropIPRuleMinPrefixLengthIPv4:    v.RpzDropIpRuleMinPrefixLengthIpv4,
		RpzDropIPRuleMinPrefixLengthIPv6:    v.RpzDropIpRuleMinPrefixLengthIpv6,
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
//
//nolint:gocyclo // false positive: this table is data, not control flow — see the doc comment above.
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
	// use_blacklist is the SDK-documented use flag for blacklist_action,
	// blacklist_log_query, blacklist_redirect_addresses,
	// blacklist_redirect_ttl, blacklist_rulesets, and enable_blacklist —
	// off means the Grid's blacklist configuration applies and every one
	// of those fields echoes back the Grid's value, not the submitted one.
	func(desired, observed dnsViewFields) bool {
		return gatedStringEqual(desired.UseBlacklist, desired.BlacklistAction, observed.BlacklistAction)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseBlacklist, desired.BlacklistLogQuery, observed.BlacklistLogQuery)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseBlacklist, desired.BlacklistRedirectAddresses, observed.BlacklistRedirectAddresses)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseBlacklist, desired.BlacklistRedirectTTL, observed.BlacklistRedirectTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseBlacklist, desired.BlacklistRulesets, observed.BlacklistRulesets)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseBlacklist) == boolOrFalse(observed.UseBlacklist)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseBlacklist, desired.EnableBlacklist, observed.EnableBlacklist)
	},
	// use_root_name_server is the use flag for custom_root_name_servers and
	// root_name_server_type.
	func(desired, observed dnsViewFields) bool {
		return gatedStringEqual(desired.UseRootNameServer, desired.RootNameServerType, observed.RootNameServerType)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRootNameServer) == boolOrFalse(observed.UseRootNameServer)
	},
	// use_ddns_force_creation_timestamp_update is the use flag for
	// ddns_force_creation_timestamp_update.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDdnsForceCreationTimestampUpdate, desired.DdnsForceCreationTimestampUpdate, observed.DdnsForceCreationTimestampUpdate)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsForceCreationTimestampUpdate) == boolOrFalse(observed.UseDdnsForceCreationTimestampUpdate)
	},
	// use_ddns_principal_security is the use flag for ddns_restrict_secure,
	// ddns_principal_tracking, and ddns_principal_group.
	func(desired, observed dnsViewFields) bool {
		return gatedStringEqual(desired.UseDdnsPrincipalSecurity, desired.DdnsPrincipalGroup, observed.DdnsPrincipalGroup)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDdnsPrincipalSecurity, desired.DdnsPrincipalTracking, observed.DdnsPrincipalTracking)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsPrincipalSecurity) == boolOrFalse(observed.UseDdnsPrincipalSecurity)
	},
	// use_ddns_patterns_restriction is the use flag for
	// ddns_restrict_patterns_list and ddns_restrict_patterns.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDdnsPatternsRestriction, desired.DdnsRestrictPatterns, observed.DdnsRestrictPatterns)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseDdnsPatternsRestriction, desired.DdnsRestrictPatternsList, observed.DdnsRestrictPatternsList)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsPatternsRestriction) == boolOrFalse(observed.UseDdnsPatternsRestriction)
	},
	// use_ddns_restrict_protected is the use flag for ddns_restrict_protected.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDdnsRestrictProtected, desired.DdnsRestrictProtected, observed.DdnsRestrictProtected)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsRestrictProtected) == boolOrFalse(observed.UseDdnsRestrictProtected)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDdnsPrincipalSecurity, desired.DdnsRestrictSecure, observed.DdnsRestrictSecure)
	},
	// use_ddns_restrict_static is the use flag for ddns_restrict_static.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDdnsRestrictStatic, desired.DdnsRestrictStatic, observed.DdnsRestrictStatic)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDdnsRestrictStatic) == boolOrFalse(observed.UseDdnsRestrictStatic)
	},
	// use_dns64 is the use flag for dns64_enabled and dns64_groups.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDns64, desired.Dns64Enabled, observed.Dns64Enabled)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseDns64, desired.Dns64Groups, observed.Dns64Groups)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDns64) == boolOrFalse(observed.UseDns64)
	},
	// use_dnssec is the use flag for dnssec_enabled,
	// dnssec_expired_signatures_enabled, dnssec_validation_enabled, and
	// dnssec_trusted_keys. dnssec_negative_trust_anchors is NOT in that
	// list — the SDK documents no use flag for it, so it is always
	// user-supplied and compared unconditionally below.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDnssec, desired.DnssecEnabled, observed.DnssecEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDnssec, desired.DnssecExpiredSignaturesEnabled, observed.DnssecExpiredSignaturesEnabled)
	},
	// dnssec_negative_trust_anchors has no use flag — always compared.
	func(desired, observed dnsViewFields) bool {
		return stringSliceEqual(desired.DnssecNegativeTrustAnchors, observed.DnssecNegativeTrustAnchors)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseDnssec, desired.DnssecValidationEnabled, observed.DnssecValidationEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseDnssec) == boolOrFalse(observed.UseDnssec)
	},
	// use_fixed_rrset_order_fqdns is the use flag for fixed_rrset_order_fqdns
	// and enable_fixed_rrset_order_fqdns.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseFixedRrsetOrderFqdns, desired.EnableFixedRrsetOrderFqdns, observed.EnableFixedRrsetOrderFqdns)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseFixedRrsetOrderFqdns) == boolOrFalse(observed.UseFixedRrsetOrderFqdns)
	},
	// enable_match_recursive_only has no use flag — always compared.
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.EnableMatchRecursiveOnly) == boolOrFalse(observed.EnableMatchRecursiveOnly)
	},
	// use_filter_aaaa is the use flag for filter_aaaa and filter_aaaa_list.
	func(desired, observed dnsViewFields) bool {
		return gatedStringEqual(desired.UseFilterAaaa, desired.FilterAaaa, observed.FilterAaaa)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseFilterAaaa) == boolOrFalse(observed.UseFilterAaaa)
	},
	// use_forwarders is the use flag for forwarders and forward_only.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseForwarders, desired.ForwardOnly, observed.ForwardOnly)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseForwarders, desired.Forwarders, observed.Forwarders)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseForwarders) == boolOrFalse(observed.UseForwarders)
	},
	// use_lame_ttl is the use flag for lame_ttl.
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseLameTTL, desired.LameTTL, observed.LameTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseLameTTL) == boolOrFalse(observed.UseLameTTL)
	},
	// use_max_cache_ttl is the use flag for max_cache_ttl.
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseMaxCacheTTL, desired.MaxCacheTTL, observed.MaxCacheTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseMaxCacheTTL) == boolOrFalse(observed.UseMaxCacheTTL)
	},
	// use_max_ncache_ttl is the use flag for max_ncache_ttl.
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseMaxNcacheTTL, desired.MaxNcacheTTL, observed.MaxNcacheTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseMaxNcacheTTL) == boolOrFalse(observed.UseMaxNcacheTTL)
	},
	// notify_delay has no use flag at all in the view object — always
	// compared.
	func(desired, observed dnsViewFields) bool {
		return uint32OrZero(desired.NotifyDelay) == uint32OrZero(observed.NotifyDelay)
	},
	// use_nxdomain_redirect is the use flag for nxdomain_redirect,
	// nxdomain_redirect_addresses, nxdomain_redirect_addresses_v6,
	// nxdomain_redirect_ttl, nxdomain_log_query, and nxdomain_rulesets.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseNxdomainRedirect, desired.NxdomainLogQuery, observed.NxdomainLogQuery)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseNxdomainRedirect, desired.NxdomainRedirect, observed.NxdomainRedirect)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseNxdomainRedirect, desired.NxdomainRedirectAddresses, observed.NxdomainRedirectAddresses)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseNxdomainRedirect, desired.NxdomainRedirectAddressesV6, observed.NxdomainRedirectAddressesV6)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseNxdomainRedirect, desired.NxdomainRedirectTTL, observed.NxdomainRedirectTTL)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedStringSliceEqual(desired.UseNxdomainRedirect, desired.NxdomainRulesets, observed.NxdomainRulesets)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseNxdomainRedirect) == boolOrFalse(observed.UseNxdomainRedirect)
	},
	// use_recursion is the use flag for recursion.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseRecursion, desired.Recursion, observed.Recursion)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRecursion) == boolOrFalse(observed.UseRecursion)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseResponseRateLimiting) == boolOrFalse(observed.UseResponseRateLimiting)
	},
	// use_rpz_drop_ip_rule is the use flag for rpz_drop_ip_rule_enabled,
	// rpz_drop_ip_rule_min_prefix_length_ipv4, and
	// rpz_drop_ip_rule_min_prefix_length_ipv6.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseRpzDropIPRule, desired.RpzDropIPRuleEnabled, observed.RpzDropIPRuleEnabled)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseRpzDropIPRule, desired.RpzDropIPRuleMinPrefixLengthIPv4, observed.RpzDropIPRuleMinPrefixLengthIPv4)
	},
	func(desired, observed dnsViewFields) bool {
		return gatedUint32Equal(desired.UseRpzDropIPRule, desired.RpzDropIPRuleMinPrefixLengthIPv6, observed.RpzDropIPRuleMinPrefixLengthIPv6)
	},
	func(desired, observed dnsViewFields) bool {
		return boolOrFalse(desired.UseRpzDropIPRule) == boolOrFalse(observed.UseRpzDropIPRule)
	},
	// use_rpz_qname_wait_recurse is the use flag for rpz_qname_wait_recurse.
	func(desired, observed dnsViewFields) bool {
		return gatedBoolEqual(desired.UseRpzQnameWaitRecurse, desired.RpzQnameWaitRecurse, observed.RpzQnameWaitRecurse)
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
	// custom_root_name_servers is gated by use_root_name_server; each item
	// also carries its own use_tsig_key_name/tsig_key_name pair, handled by
	// nameServerValuesEqual.
	func(desired, observed dnsViewFields) bool {
		return gatedNestedSliceEqual(desired.UseRootNameServer, desired.CustomRootNameServers, observed.CustomRootNameServers, nameServerValuesEqual)
	},
	// dnssec_trusted_keys is gated by use_dnssec.
	func(desired, observed dnsViewFields) bool {
		return gatedNestedSliceEqual(desired.UseDnssec, desired.DnssecTrustedKeys, observed.DnssecTrustedKeys, nestedSliceEqual[dnssecTrustedKeyValue])
	},
	// fixed_rrset_order_fqdns is gated by use_fixed_rrset_order_fqdns.
	func(desired, observed dnsViewFields) bool {
		return gatedNestedSliceEqual(desired.UseFixedRrsetOrderFqdns, desired.FixedRrsetOrderFqdns, observed.FixedRrsetOrderFqdns, nestedSliceEqual[fixedRrsetOrderFqdnValue])
	},
	// filter_aaaa_list is gated by use_filter_aaaa; each item also carries
	// its own use_tsig_key_name/tsig_key_name pair, handled by
	// addressAcValuesEqual.
	func(desired, observed dnsViewFields) bool {
		return gatedNestedSliceEqual(desired.UseFilterAaaa, desired.FilterAaaaList, observed.FilterAaaaList, addressAcValuesEqual)
	},
	// match_clients/match_destinations have no outer use flag of their own
	// — only each item's use_tsig_key_name/tsig_key_name pair needs
	// gating, handled by addressAcValuesEqual.
	func(desired, observed dnsViewFields) bool {
		return addressAcValuesEqual(desired.MatchClients, observed.MatchClients)
	},
	func(desired, observed dnsViewFields) bool {
		return addressAcValuesEqual(desired.MatchDestinations, observed.MatchDestinations)
	},
	// sortlist is gated by use_sortlist.
	func(desired, observed dnsViewFields) bool {
		return gatedNestedSliceEqual(desired.UseSortlist, desired.Sortlist, observed.Sortlist, nestedSliceEqual[sortlistEntryValue])
	},
	// response_rate_limiting is gated by use_response_rate_limiting.
	func(desired, observed dnsViewFields) bool {
		return gatedPtrDeepEqual(desired.UseResponseRateLimiting, desired.ResponseRateLimiting, observed.ResponseRateLimiting)
	},
	// scavenging_settings is gated by use_scavenging_settings.
	func(desired, observed dnsViewFields) bool {
		return gatedPtrDeepEqual(desired.UseScavengingSettings, desired.ScavengingSettings, observed.ScavengingSettings)
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
//
// Ops for fields gated by a use flag (see dnsViewFieldComparators for the
// full flag -> fields map) use the gatedLateInit* helpers instead of the
// bare lateInit* ones: back-filling a grid/parent-inherited default into
// spec when the flag is off would silently claim a setting that is not
// actually in effect.
//
//nolint:gocyclo // false positive: this table is data, not control flow.
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
		return gatedLateInitStringPtr(desired.UseBlacklist, observed.UseBlacklist, &desired.BlacklistAction, observed.BlacklistAction)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseBlacklist, observed.UseBlacklist, &desired.BlacklistLogQuery, observed.BlacklistLogQuery)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseBlacklist, observed.UseBlacklist, &desired.BlacklistRedirectAddresses, observed.BlacklistRedirectAddresses)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseBlacklist, observed.UseBlacklist, &desired.BlacklistRedirectTTL, observed.BlacklistRedirectTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseBlacklist, observed.UseBlacklist, &desired.BlacklistRulesets, observed.BlacklistRulesets)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseBlacklist, observed.UseBlacklist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseBlacklist, observed.UseBlacklist, &desired.EnableBlacklist, observed.EnableBlacklist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringPtr(desired.UseRootNameServer, observed.UseRootNameServer, &desired.RootNameServerType, observed.RootNameServerType)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRootNameServer, observed.UseRootNameServer)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDdnsForceCreationTimestampUpdate, observed.UseDdnsForceCreationTimestampUpdate, &desired.DdnsForceCreationTimestampUpdate, observed.DdnsForceCreationTimestampUpdate)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsForceCreationTimestampUpdate, observed.UseDdnsForceCreationTimestampUpdate)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringPtr(desired.UseDdnsPrincipalSecurity, observed.UseDdnsPrincipalSecurity, &desired.DdnsPrincipalGroup, observed.DdnsPrincipalGroup)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDdnsPrincipalSecurity, observed.UseDdnsPrincipalSecurity, &desired.DdnsPrincipalTracking, observed.DdnsPrincipalTracking)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsPrincipalSecurity, observed.UseDdnsPrincipalSecurity)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDdnsPatternsRestriction, observed.UseDdnsPatternsRestriction, &desired.DdnsRestrictPatterns, observed.DdnsRestrictPatterns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseDdnsPatternsRestriction, observed.UseDdnsPatternsRestriction, &desired.DdnsRestrictPatternsList, observed.DdnsRestrictPatternsList)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsPatternsRestriction, observed.UseDdnsPatternsRestriction)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDdnsRestrictProtected, observed.UseDdnsRestrictProtected, &desired.DdnsRestrictProtected, observed.DdnsRestrictProtected)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsRestrictProtected, observed.UseDdnsRestrictProtected)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDdnsPrincipalSecurity, observed.UseDdnsPrincipalSecurity, &desired.DdnsRestrictSecure, observed.DdnsRestrictSecure)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDdnsRestrictStatic, observed.UseDdnsRestrictStatic, &desired.DdnsRestrictStatic, observed.DdnsRestrictStatic)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDdnsRestrictStatic, observed.UseDdnsRestrictStatic)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDns64, observed.UseDns64, &desired.Dns64Enabled, observed.Dns64Enabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseDns64, observed.UseDns64, &desired.Dns64Groups, observed.Dns64Groups)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDns64, observed.UseDns64)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDnssec, observed.UseDnssec, &desired.DnssecEnabled, observed.DnssecEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDnssec, observed.UseDnssec, &desired.DnssecExpiredSignaturesEnabled, observed.DnssecExpiredSignaturesEnabled)
	},
	// dnssec_negative_trust_anchors has no use flag — always back-filled.
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitStringSlice(&desired.DnssecNegativeTrustAnchors, observed.DnssecNegativeTrustAnchors)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseDnssec, observed.UseDnssec, &desired.DnssecValidationEnabled, observed.DnssecValidationEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseDnssec, observed.UseDnssec)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseFixedRrsetOrderFqdns, observed.UseFixedRrsetOrderFqdns, &desired.EnableFixedRrsetOrderFqdns, observed.EnableFixedRrsetOrderFqdns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseFixedRrsetOrderFqdns, observed.UseFixedRrsetOrderFqdns)
	},
	// enable_match_recursive_only has no use flag — always back-filled.
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.EnableMatchRecursiveOnly, observed.EnableMatchRecursiveOnly)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringPtr(desired.UseFilterAaaa, observed.UseFilterAaaa, &desired.FilterAaaa, observed.FilterAaaa)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseFilterAaaa, observed.UseFilterAaaa)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseForwarders, observed.UseForwarders, &desired.ForwardOnly, observed.ForwardOnly)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseForwarders, observed.UseForwarders, &desired.Forwarders, observed.Forwarders)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseForwarders, observed.UseForwarders)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseLameTTL, observed.UseLameTTL, &desired.LameTTL, observed.LameTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseLameTTL, observed.UseLameTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseMaxCacheTTL, observed.UseMaxCacheTTL, &desired.MaxCacheTTL, observed.MaxCacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseMaxCacheTTL, observed.UseMaxCacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseMaxNcacheTTL, observed.UseMaxNcacheTTL, &desired.MaxNcacheTTL, observed.MaxNcacheTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseMaxNcacheTTL, observed.UseMaxNcacheTTL)
	},
	// notify_delay has no use flag — always back-filled.
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.NotifyDelay, observed.NotifyDelay)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseNxdomainRedirect, observed.UseNxdomainRedirect, &desired.NxdomainLogQuery, observed.NxdomainLogQuery)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseNxdomainRedirect, observed.UseNxdomainRedirect, &desired.NxdomainRedirect, observed.NxdomainRedirect)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseNxdomainRedirect, observed.UseNxdomainRedirect, &desired.NxdomainRedirectAddresses, observed.NxdomainRedirectAddresses)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseNxdomainRedirect, observed.UseNxdomainRedirect, &desired.NxdomainRedirectAddressesV6, observed.NxdomainRedirectAddressesV6)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseNxdomainRedirect, observed.UseNxdomainRedirect, &desired.NxdomainRedirectTTL, observed.NxdomainRedirectTTL)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitStringSlice(desired.UseNxdomainRedirect, observed.UseNxdomainRedirect, &desired.NxdomainRulesets, observed.NxdomainRulesets)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseNxdomainRedirect, observed.UseNxdomainRedirect)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseRecursion, observed.UseRecursion, &desired.Recursion, observed.Recursion)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRecursion, observed.UseRecursion)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseResponseRateLimiting, observed.UseResponseRateLimiting)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseRpzDropIPRule, observed.UseRpzDropIPRule, &desired.RpzDropIPRuleEnabled, observed.RpzDropIPRuleEnabled)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseRpzDropIPRule, observed.UseRpzDropIPRule, &desired.RpzDropIPRuleMinPrefixLengthIPv4, observed.RpzDropIPRuleMinPrefixLengthIPv4)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseRpzDropIPRule, observed.UseRpzDropIPRule, &desired.RpzDropIPRuleMinPrefixLengthIPv6, observed.RpzDropIPRuleMinPrefixLengthIPv6)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitPtr(&desired.UseRpzDropIPRule, observed.UseRpzDropIPRule)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseRpzQnameWaitRecurse, observed.UseRpzQnameWaitRecurse, &desired.RpzQnameWaitRecurse, observed.RpzQnameWaitRecurse)
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
		return gatedLateInitNestedSlice(desired.UseRootNameServer, observed.UseRootNameServer, &desired.CustomRootNameServers, observed.CustomRootNameServers)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitNestedSlice(desired.UseDnssec, observed.UseDnssec, &desired.DnssecTrustedKeys, observed.DnssecTrustedKeys)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitNestedSlice(desired.UseFixedRrsetOrderFqdns, observed.UseFixedRrsetOrderFqdns, &desired.FixedRrsetOrderFqdns, observed.FixedRrsetOrderFqdns)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitNestedSlice(desired.UseFilterAaaa, observed.UseFilterAaaa, &desired.FilterAaaaList, observed.FilterAaaaList)
	},
	// match_clients/match_destinations have no outer use flag — always
	// back-filled as a whole list (each item's own use_tsig_key_name gate
	// is honored only by the isUpToDate comparator, since a whole-list
	// import from observed is by definition already in agreement).
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.MatchClients, observed.MatchClients)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return lateInitNestedSlice(&desired.MatchDestinations, observed.MatchDestinations)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitNestedSlice(desired.UseSortlist, observed.UseSortlist, &desired.Sortlist, observed.Sortlist)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseResponseRateLimiting, observed.UseResponseRateLimiting, &desired.ResponseRateLimiting, observed.ResponseRateLimiting)
	},
	func(desired *dnsViewFields, observed dnsViewFields) bool {
		return gatedLateInitPtr(desired.UseScavengingSettings, observed.UseScavengingSettings, &desired.ScavengingSettings, observed.ScavengingSettings)
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
// edns_udp_size/use_edns_udp_size/last_queried_acl/max_udp_size/
// use_max_udp_size are deliberately excluded: the provider is pinned to
// WAPI 2.9.7, whose `view` object schema does not define these fields at
// all (confirmed live against the Grid Manager). Requesting them in the
// GET return-fields list fails every Observe() with a 400
// (AdmConProtoError: Unknown argument/field). The controller no longer
// reads, writes, or compares these fields anywhere.
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
