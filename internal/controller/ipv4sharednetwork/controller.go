// Package ipv4sharednetwork implements the Crossplane controller for the
// Infoblox NIOS IPv4SharedNetwork managed resource. Like recorda and
// network, this provider wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateIpv4SharedNetwork/GetIpv4SharedNetworkByRef/
// UpdateIpv4SharedNetwork/DeleteIpv4SharedNetwork) instead of a generic
// HTTP request/response envelope, so there is no internal REST client to
// compose.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package ipv4sharednetwork

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/ipv4sharednetwork/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/ipv4sharednetwork/v1alpha1"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage         = "cannot track ProviderConfig usage"
	errGetPC                = "cannot get ProviderConfig"
	errGetClusterPC         = "cannot get ClusterProviderConfig"
	errUnsupportedKind      = "unsupported provider config kind"
	errGetSecret            = "cannot get credentials secret"
	errNoSecretRef          = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds     = "unsupported credentials source: only Secret is supported"
	errMissingCredKey       = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager     = "cannot create Infoblox NIOS WAPI object manager"
	errObserveIPv4SharedNet = "cannot observe IPv4SharedNetwork"
	errCreateIPv4SharedNet  = "cannot create IPv4SharedNetwork"
	errUpdateIPv4SharedNet  = "cannot update IPv4SharedNetwork"
	errDeleteIPv4SharedNet  = "cannot delete IPv4SharedNetwork"
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

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
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

// stringSliceEqualUnordered reports whether a and b contain the same set
// of strings, ignoring order and treating nil/empty as equal. The
// "networks" field is conceptually a set of member-network CIDRs, and the
// WAPI response ordering is not guaranteed to match the order the user
// supplied in spec.
func stringSliceEqualUnordered(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
	}
	for _, c := range counts {
		if c != 0 {
			return false
		}
	}
	return true
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

// ── DHCP option translation (scope-agnostic intermediate) ──────────────────
//
// sharedNetworkDhcpOption is the scope-agnostic intermediate
// representation of a WAPI Dhcpoption. Each scope's controller converts
// its own generated IPv4SharedNetworkDhcpOption type to/from this
// intermediate at the call site — see observedNetworkMember in the
// network package for why an intermediate type is used instead of a
// direct struct conversion (the cluster and namespaced DHCP option types
// are structurally similar but are distinct named types).
type sharedNetworkDhcpOption struct {
	Name        *string
	Num         *uint32
	VendorClass *string
	Value       *string
	UseOption   *bool
}

// optionsToSDK converts the scope-agnostic intermediate DHCP options into
// the SDK's []*ibclient.Dhcpoption wire type.
func optionsToSDK(opts []sharedNetworkDhcpOption) []*ibclient.Dhcpoption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*ibclient.Dhcpoption, 0, len(opts))
	for _, o := range opts {
		out = append(out, &ibclient.Dhcpoption{
			Name:        strOrEmpty(o.Name),
			Num:         uint32OrZero(o.Num),
			VendorClass: strOrEmpty(o.VendorClass),
			Value:       strOrEmpty(o.Value),
			UseOption:   boolOrFalse(o.UseOption),
		})
	}
	return out
}

// optionsFromSDK converts the SDK's []*ibclient.Dhcpoption response into
// the scope-agnostic intermediate representation.
func optionsFromSDK(opts []*ibclient.Dhcpoption) []sharedNetworkDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]sharedNetworkDhcpOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		item := sharedNetworkDhcpOption{}
		if o.Name != "" {
			v := o.Name
			item.Name = &v
		}
		if o.Num != 0 {
			v := o.Num
			item.Num = &v
		}
		if o.VendorClass != "" {
			v := o.VendorClass
			item.VendorClass = &v
		}
		if o.Value != "" {
			v := o.Value
			item.Value = &v
		}
		if o.UseOption {
			v := o.UseOption
			item.UseOption = &v
		}
		out = append(out, item)
	}
	return out
}

// optionsEqual compares two intermediate DHCP option slices field-by-field
// in order. The WAPI preserves the order DHCP options are supplied in, so
// (unlike "networks") an ordered comparison is used here.
func optionsEqual(a, b []sharedNetworkDhcpOption) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if strOrEmpty(a[i].Name) != strOrEmpty(b[i].Name) {
			return false
		}
		if uint32OrZero(a[i].Num) != uint32OrZero(b[i].Num) {
			return false
		}
		if strOrEmpty(a[i].VendorClass) != strOrEmpty(b[i].VendorClass) {
			return false
		}
		if strOrEmpty(a[i].Value) != strOrEmpty(b[i].Value) {
			return false
		}
		if boolOrFalse(a[i].UseOption) != boolOrFalse(b[i].UseOption) {
			return false
		}
	}
	return true
}

// optionsFromCluster converts the cluster-scoped generated DHCP option
// type into the scope-agnostic intermediate representation.
func optionsFromCluster(opts []*clusterv1alpha1.IPv4SharedNetworkDhcpOption) []sharedNetworkDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]sharedNetworkDhcpOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		out = append(out, sharedNetworkDhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

// optionsToCluster converts the scope-agnostic intermediate representation
// into the cluster-scoped generated DHCP option type.
func optionsToCluster(opts []sharedNetworkDhcpOption) []*clusterv1alpha1.IPv4SharedNetworkDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*clusterv1alpha1.IPv4SharedNetworkDhcpOption, 0, len(opts))
	for _, o := range opts {
		out = append(out, &clusterv1alpha1.IPv4SharedNetworkDhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

// optionsFromNamespaced converts the namespaced generated DHCP option type
// into the scope-agnostic intermediate representation.
func optionsFromNamespaced(opts []*namespacedv1alpha1.IPv4SharedNetworkDhcpOption) []sharedNetworkDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]sharedNetworkDhcpOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		out = append(out, sharedNetworkDhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

// optionsToNamespaced converts the scope-agnostic intermediate
// representation into the namespaced generated DHCP option type.
func optionsToNamespaced(opts []sharedNetworkDhcpOption) []*namespacedv1alpha1.IPv4SharedNetworkDhcpOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]*namespacedv1alpha1.IPv4SharedNetworkDhcpOption, 0, len(opts))
	for _, o := range opts {
		out = append(out, &namespacedv1alpha1.IPv4SharedNetworkDhcpOption{Name: o.Name, Num: o.Num, VendorClass: o.VendorClass, Value: o.Value, UseOption: o.UseOption})
	}
	return out
}

// ── networks (CIDR list) translation ────────────────────────────────────

// networksFromSDK extracts the member-network CIDR/ref strings from the
// SDK's []*ibclient.Ipv4Network response. SharedNetwork.UnmarshalJSON only
// populates each entry's Ref field from the wire "_ref" value — no other
// Ipv4Network fields are ever present on a GET response.
func networksFromSDK(networks []*ibclient.Ipv4Network) []string {
	if len(networks) == 0 {
		return nil
	}
	out := make([]string, 0, len(networks))
	for _, n := range networks {
		if n == nil {
			continue
		}
		out = append(out, n.Ref)
	}
	return out
}

// ── Field comparison / late-init ────────────────────────────────────────
//
// IPv4SharedNetwork's networkView is immutable — live WAPI _schema probing
// found supports=rws (no u), so it is excluded from isUpToDate and is
// never part of the top-level network_view key in updateIPv4SharedNetwork's
// request body (the SDK wrapper never sets SharedNetwork.NetworkView on
// update — see updateIPv4SharedNetwork). All other fields (name, networks,
// comment, extAttrs, disable, useOptions, options) are mutable.

// isUpToDate compares the desired mutable IPv4SharedNetwork fields against
// the observed ibclient.SharedNetwork. networkView is intentionally
// excluded — it is immutable.
func isUpToDate(name *string, networks []string, comment *string, extAttrs map[string]string, disable, useOptions *bool, options []sharedNetworkDhcpOption, sn *ibclient.SharedNetwork) bool {
	if strOrEmpty(name) != strOrEmpty(sn.Name) {
		return false
	}
	if !stringSliceEqualUnordered(networks, networksFromSDK(sn.Networks)) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(sn.Comment) {
		return false
	}
	if !extAttrsEqual(extAttrs, extAttrsFromEA(sn.Ea)) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(sn.Disable) {
		return false
	}
	if boolOrFalse(useOptions) != boolOrFalse(sn.UseOptions) {
		return false
	}
	if !optionsEqual(options, optionsFromSDK(sn.Options)) {
		return false
	}
	return true
}

// observedIPv4SharedNetwork holds the primitive field values extracted
// from a WAPI SharedNetwork response, mirroring the full-mirror AtProvider
// convention. The cluster and namespaced IPv4SharedNetworkObservation
// types are structurally similar but are distinct named types, so they
// are not directly convertible — each scope copies this intermediate
// struct's fields into its own Observation type at the call site.
type observedIPv4SharedNetwork struct {
	ID                    string
	Name                  *string
	Networks              []string
	NetworkView           *string
	Comment               *string
	ExtAttrs              map[string]string
	Disable               *bool
	UseOptions            *bool
	Options               []sharedNetworkDhcpOption
	Ref                   *string
	Authority             *bool
	DdnsTTL               *uint32
	EnableDdns            *bool
	DhcpUtilization       *uint32
	DhcpUtilizationStatus *string
	DynamicHosts          *uint32
}

// observeFromIPv4SharedNetwork extracts the fields mirrored by
// IPv4SharedNetworkObservation (the full-mirror AtProvider convention)
// from a WAPI SharedNetwork response.
func observeFromIPv4SharedNetwork(externalID string, sn *ibclient.SharedNetwork) observedIPv4SharedNetwork {
	o := observedIPv4SharedNetwork{
		ID:         externalID,
		Networks:   networksFromSDK(sn.Networks),
		ExtAttrs:   extAttrsFromEA(sn.Ea),
		Options:    optionsFromSDK(sn.Options),
		Disable:    sn.Disable,
		UseOptions: sn.UseOptions,
		Authority:  sn.Authority,
		DdnsTTL:    sn.DdnsTtl,
		EnableDdns: sn.EnableDdns,
	}
	if sn.Name != nil && *sn.Name != "" {
		v := *sn.Name
		o.Name = &v
	}
	if sn.NetworkView != "" {
		v := sn.NetworkView
		o.NetworkView = &v
	}
	if sn.Comment != nil && *sn.Comment != "" {
		v := *sn.Comment
		o.Comment = &v
	}
	if sn.Ref != "" {
		v := sn.Ref
		o.Ref = &v
	}
	if sn.DhcpUtilization != 0 {
		v := sn.DhcpUtilization
		o.DhcpUtilization = &v
	}
	if sn.DhcpUtilizationStatus != "" {
		v := sn.DhcpUtilizationStatus
		o.DhcpUtilizationStatus = &v
	}
	if sn.DynamicHosts != 0 {
		v := sn.DynamicHosts
		o.DynamicHosts = &v
	}
	return o
}

// lateInitialize back-fills server-defaulted optional primitive fields
// (networkView, comment, disable, useOptions, extAttrs) from the observed
// IPv4SharedNetwork into spec so isUpToDate does not see phantom drift on
// the next reconcile. The required fields (name, networks) are always
// user-supplied and never late-initialized. Options requires
// scope-specific type conversion and is handled separately by each
// scope's Observe (see optionsToCluster/optionsToNamespaced). Returns true
// if any field was changed.
func lateInitialize(networkView, comment **string, disable, useOptions **bool, extAttrs *map[string]string, o observedIPv4SharedNetwork) bool {
	changed := false

	if *networkView == nil && o.NetworkView != nil {
		*networkView = o.NetworkView
		changed = true
	}
	if *comment == nil && o.Comment != nil {
		*comment = o.Comment
		changed = true
	}
	if *disable == nil && o.Disable != nil {
		*disable = o.Disable
		changed = true
	}
	if *useOptions == nil && o.UseOptions != nil {
		*useOptions = o.UseOptions
		changed = true
	}
	if len(*extAttrs) == 0 {
		if len(o.ExtAttrs) > 0 {
			*extAttrs = o.ExtAttrs
			changed = true
		}
	}

	return changed
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createIPv4SharedNetwork issues the WAPI create call.
func createIPv4SharedNetwork(objMgr ibclient.IBObjectManager, name *string, networks []string, networkView *string, comment *string, extAttrs map[string]string, disable, useOptions *bool, options []sharedNetworkDhcpOption) (*ibclient.SharedNetwork, error) {
	return objMgr.CreateIpv4SharedNetwork(strOrEmpty(name), networks, strOrEmpty(networkView), buildEA(extAttrs), strOrEmpty(comment), boolOrFalse(disable), boolOrFalse(useOptions), optionsToSDK(options))
}

// updateIPv4SharedNetwork issues the WAPI update call. networkView is
// threaded through only to correctly associate each network CIDR entry
// with its view when the SDK builds the "networks" wire payload — the
// SDK's UpdateIpv4SharedNetwork never sets the top-level
// SharedNetwork.NetworkView field, so the immutable network_view key is
// never present in the outgoing PUT body regardless of the value passed
// here.
func updateIPv4SharedNetwork(objMgr ibclient.IBObjectManager, ref string, name *string, networks []string, networkView *string, comment *string, extAttrs map[string]string, disable, useOptions *bool, options []sharedNetworkDhcpOption) (*ibclient.SharedNetwork, error) {
	return objMgr.UpdateIpv4SharedNetwork(ref, strOrEmpty(name), networks, strOrEmpty(networkView), strOrEmpty(comment), buildEA(extAttrs), boolOrFalse(disable), boolOrFalse(useOptions), optionsToSDK(options))
}

// deleteIPv4SharedNetwork issues the WAPI delete call (hard delete — a
// subsequent GET on the same ref 404s).
func deleteIPv4SharedNetwork(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteIpv4SharedNetwork(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced
// IPv4SharedNetwork controllers with the SafeStart gate. Each controller
// starts only after its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterIPv4SharedNetwork(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster IPv4SharedNetwork controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("IPv4SharedNetwork"))

	o.Gate.Register(func() {
		if err := setupNamespacedIPv4SharedNetwork(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced IPv4SharedNetwork controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("IPv4SharedNetwork"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced IPv4SharedNetwork
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterIPv4SharedNetwork(mgr, o); err != nil {
		return err
	}
	return setupNamespacedIPv4SharedNetwork(mgr, o)
}
