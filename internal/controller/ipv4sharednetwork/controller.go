// Package ipv4sharednetwork implements the Crossplane controller for the
// Infoblox NIOS IPv4SharedNetwork managed resource. Like recorda and
// network, this provider wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateIpv4SharedNetwork/GetIpv4SharedNetworkByRef/
// UpdateIpv4SharedNetwork/DeleteIpv4SharedNetwork) instead of a generic
// HTTP request/response envelope, so there is no internal REST client to
// compose.
//
// IPv4SharedNetwork is wired to the UID-in-EA object-identity ladder (see
// the internal/clients/identity package doc): the WAPI _ref every create
// call returns is a derived handle — a rendering of the object's own
// name, not a stable backend-assigned ID — so this controller stamps the
// managed resource's own metadata.uid onto the Grid object as an
// extensible attribute and resolves every Observe/Delete through the
// shared identity.Resolve ladder instead of trusting the stored _ref
// alone.
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

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrl "sigs.k8s.io/controller-runtime"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/ipv4sharednetwork/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/ipv4sharednetwork/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage              = "cannot track ProviderConfig usage"
	errPersistExternalName       = "cannot persist refreshed external name"
	errGetPC                     = "cannot get ProviderConfig"
	errGetClusterPC              = "cannot get ClusterProviderConfig"
	errUnsupportedKind           = "unsupported provider config kind"
	errObserveIPv4SharedNet      = "cannot observe IPv4SharedNetwork"
	errCreateIPv4SharedNet       = "cannot create IPv4SharedNetwork"
	errUpdateIPv4SharedNet       = "cannot update IPv4SharedNetwork"
	errDeleteIPv4SharedNet       = "cannot delete IPv4SharedNetwork"
	errEmptyUID                  = "cannot stamp IPv4SharedNetwork identity: managed resource's metadata.uid is empty"
	errDeleteUnverifiedOwnership = "refusing to delete: the resolved object's identity extensible attribute is absent or belongs to a different owner, so ownership cannot be verified before an irreversible delete. " +
		"Reconcile the external-name annotation, verify the Grid object manually, or remove the finalizer to abandon it without deleting."
	errPrerequisiteCheck = "cannot verify the identity extensible attribute definition prerequisite"
)

// unresolvedProbeEndpoint is the identity-prerequisite-probe cache key
// used when an ExternalClient is built without a resolved Grid endpoint.
// Production code always goes through Connect(), which resolves the
// endpoint from the ProviderConfig's spec.host field — required with
// MinLength=1 by the CRD schema — before constructing the client — this
// fallback is only ever reached by this package's own white-box unit
// tests that build clusterExternal/namespacedExternal directly, bypassing
// Connect().
const unresolvedProbeEndpoint = "unresolved-grid-endpoint"

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
// It is used by this package's tests to build mock WAPI request paths —
// the authenticated connector itself is built by the shared credential
// bridge (see internal/controller/config), which pins the same version.
const wapiVersion = "2.9.7"

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
// intermediate at the call site.
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

// networksFromSDK extracts the member-network CIDRs from the SDK's
// []*ibclient.Ipv4Network response. SharedNetwork.UnmarshalJSON only
// populates each entry's Ref field from the wire "_ref" value — no other
// Ipv4Network fields are ever present on a GET response — and on a real
// Grid that Ref is a full WAPI object reference (e.g.
// "network/ZG5zLm5ldHdvcmskMTAwLjY0LjExNi42NC8yOC8w:100.64.116.64/28/default"),
// not the plain CIDR spec.forProvider.networks holds. cidrFromNetworkRef
// normalizes each entry back to a plain CIDR so the result is
// like-for-like comparable (isUpToDate) and mirror-safe
// (observeFromIPv4SharedNetwork/AtProvider) against the desired list.
func networksFromSDK(networks []*ibclient.Ipv4Network) []string {
	if len(networks) == 0 {
		return nil
	}
	out := make([]string, 0, len(networks))
	for _, n := range networks {
		if n == nil {
			continue
		}
		out = append(out, cidrFromNetworkRef(n.Ref))
	}
	return out
}

// cidrFromNetworkRef extracts the plain CIDR embedded in a WAPI network
// object's _ref string. A real Grid _ref for a network object has the
// form "network/<opaque base64 identity>:<cidr>/<network view>" — the
// CIDR is the first path segment of the human-readable portion after the
// ':' delimiter, and the network view is the (possibly multi-segment)
// remainder. If ref does not carry that delimiter — e.g. it is already a
// plain CIDR, as set by the SDK's own Create/Update request wrapper
// before a server response overwrites it — ref is returned unchanged
// rather than mangled or dropped.
func cidrFromNetworkRef(ref string) string {
	idx := strings.Index(ref, ":")
	if idx < 0 || idx == len(ref)-1 {
		return ref
	}
	parts := strings.SplitN(ref[idx+1:], "/", 3)
	if len(parts) < 2 {
		return ref
	}
	return parts[0] + "/" + parts[1]
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
// excluded — it is immutable. The Grid's extattrs map is compared with
// the provider's own identity stamp stripped out (identity.Strip): the
// CRD schema never includes that reserved key, so leaving it in would
// produce a permanent phantom diff.
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
	if !extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(sn.Ea))) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(sn.Disable) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useOptions) != boolOrFalse(sn.UseOptions) {
		return false
	}
	// Only compare options when the flag is on. When it is off, WAPI
	// ignores the submitted DHCP options and returns its own default set
	// on every GET — comparing them against the spec value never
	// converges.
	if boolOrFalse(useOptions) {
		if !optionsEqual(options, optionsFromSDK(sn.Options)) {
			return false
		}
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
// from a WAPI SharedNetwork response. ExtAttrs intentionally mirrors the
// Grid's complete extattrs map, including the provider's own identity
// stamp — unlike isUpToDate and lateInitialize, AtProvider is a read-only
// status mirror, not compared against spec.forProvider, so surfacing the
// stamp there is informative rather than a source of phantom drift.
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
// ibclient.SharedNetwork into spec so isUpToDate does not see phantom
// drift on the next reconcile. The required fields (name, networks) are
// always user-supplied and never late-initialized. Options requires
// scope-specific type conversion and is handled separately by each
// scope's Observe (see optionsToCluster/optionsToNamespaced). extAttrs is
// back-filled with the provider's own identity stamp stripped out
// (identity.Strip) — the same helper every other IPAM controller uses —
// because the CRD schema never includes that reserved key, so
// late-initializing it into spec.forProvider would fail CEL validation
// and produce a permanent diff. Returns true if any field was changed.
func lateInitialize(networkView, comment **string, disable, useOptions **bool, extAttrs *map[string]string, sn *ibclient.SharedNetwork) bool {
	changed := false

	if *networkView == nil && sn.NetworkView != "" {
		v := sn.NetworkView
		*networkView = &v
		changed = true
	}
	if *comment == nil && sn.Comment != nil && *sn.Comment != "" {
		v := *sn.Comment
		*comment = &v
		changed = true
	}
	if *disable == nil && sn.Disable != nil {
		v := *sn.Disable
		*disable = &v
		changed = true
	}
	if *useOptions == nil && sn.UseOptions != nil {
		v := *sn.UseOptions
		*useOptions = &v
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromSN := extAttrsFromEA(identity.Strip(sn.Ea)); len(fromSN) > 0 {
			*extAttrs = fromSN
			changed = true
		}
	}

	return changed
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createIPv4SharedNetwork issues the WAPI create call, stamping the
// owning managed resource's uid into the object's extensible attributes
// in the same request that creates it (identity.Stamp) — there is no
// follow-up call, so there is no window in which the object exists
// without its identity stamp.
func createIPv4SharedNetwork(objMgr ibclient.IBObjectManager, name *string, networks []string, networkView *string, comment *string, extAttrs map[string]string, disable, useOptions *bool, options []sharedNetworkDhcpOption, uid string) (*ibclient.SharedNetwork, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateIpv4SharedNetwork(strOrEmpty(name), networks, strOrEmpty(networkView), ea, strOrEmpty(comment), boolOrFalse(disable), boolOrFalse(useOptions), optionsToSDK(options))
}

// updateIPv4SharedNetwork issues the WAPI update call. networkView is
// threaded through only to correctly associate each network CIDR entry
// with its view when the SDK builds the "networks" wire payload — the
// SDK's UpdateIpv4SharedNetwork never sets the top-level
// SharedNetwork.NetworkView field, so the immutable network_view key is
// never present in the outgoing PUT body regardless of the value passed
// here.
//
// Every call re-asserts the identity stamp (identity.Stamp) in the
// extattrs it sends. Live verification against a real NIOS Grid Manager
// confirmed that a PUT carrying an extattrs object *replaces* the whole
// map — it is not a per-key merge — so omitting the stamp here would wipe
// it off the object on the very first field update after create.
func updateIPv4SharedNetwork(objMgr ibclient.IBObjectManager, ref string, name *string, networks []string, networkView *string, comment *string, extAttrs map[string]string, disable, useOptions *bool, options []sharedNetworkDhcpOption, uid string) (*ibclient.SharedNetwork, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.UpdateIpv4SharedNetwork(ref, strOrEmpty(name), networks, strOrEmpty(networkView), strOrEmpty(comment), ea, boolOrFalse(disable), boolOrFalse(useOptions), optionsToSDK(options))
}

// deleteIPv4SharedNetwork issues the WAPI delete call (hard delete — a
// subsequent GET on the same ref 404s).
func deleteIPv4SharedNetwork(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteIpv4SharedNetwork(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────

// ensureIdentityPrerequisite probes the Grid for the identity extensible
// attribute definition before any call that stamps identity onto a new
// object (identity.Stamp). A *identity.PrerequisiteError is returned
// verbatim — its Error() text is the operator-facing remediation, naming
// the exact WAPI call an administrator should run — so the caller's
// Synced=False condition carries it directly. Any other error (a
// transient failure probing or creating the definition) is wrapped like
// any other WAPI error and is retriable.
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
// first for a managed resource's stored external-name. See recorda's
// doc for the full rationale.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveIPv4SharedNetworkIdentity resolves the IPv4SharedNetwork
// identified by ref/uid through the shared UID-in-EA ladder.
func resolveIPv4SharedNetworkIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.SharedNetwork, identity.Outcome, error) {
	return identity.Resolve[*ibclient.SharedNetwork](ctx, conn, ibclient.NewEmptyIpv4SharedNetwork, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting an
// IPv4SharedNetwork through the identity ladder during Observe — common
// to both scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	sn           *ibclient.SharedNetwork
	obs          observedIPv4SharedNetwork
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeIPv4SharedNetwork runs the identity ladder for Observe and
// late-initializes the given ForProvider field pointers from the
// resolved object.
func observeIPv4SharedNetwork(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, networkView, comment **string, disable, useOptions **bool, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	sn, outcome, err := resolveIPv4SharedNetworkIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return observeResult{}, prereqErr
			}
		}
		return observeResult{}, err
	}
	if outcome == identity.OutcomeNotFound {
		return observeResult{exists: false}, nil
	}

	obs := observeFromIPv4SharedNetwork(sn.Ref, sn)
	res := observeResult{
		exists:  true,
		sn:      sn,
		obs:     obs,
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(networkView, comment, disable, useOptions, extAttrs, sn)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = sn.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteIPv4SharedNetworkIdentity issues the WAPI delete for the
// IPv4SharedNetwork this managed resource owns, resolving through the
// identity ladder first so a stale _ref is never mistaken for a deleted
// object.
func deleteIPv4SharedNetworkIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveIPv4SharedNetworkIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteIPv4SharedNet)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteIPv4SharedNetwork(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteIPv4SharedNet)
	default:
		return errors.New("identity: unresolved IPv4SharedNetwork outcome")
	}
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
