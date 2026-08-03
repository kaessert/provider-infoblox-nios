// Package rangetemplate implements the Crossplane controller for the
// Infoblox NIOS RangeTemplate managed resource. Like recorda, this
// provider wraps the official infoblox-go-client Go SDK directly — the
// SDK's ObjectManager exposes typed CRUD methods
// (CreateRangeTemplate/GetRangeTemplateByRef/UpdateRangeTemplate/
// DeleteRangeTemplate) instead of a generic HTTP request/response
// envelope, so there is no internal REST client to compose.
//
// RangeTemplate is wired to the UID-in-EA object-identity ladder (see the
// internal/clients/identity package doc): the WAPI _ref every create
// call returns is a derived handle — a rendering of the object's own
// name, not a stable backend-assigned ID — so this controller stamps the
// managed resource's own metadata.uid onto the Grid object as an
// extensible attribute and resolves every Observe/Delete through the
// shared identity.Resolve ladder instead of trusting the stored _ref
// alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package rangetemplate

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	controllerpkg "github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/rangetemplate/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/rangetemplate/v1alpha1"
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
	errGetSecret                 = "cannot get credentials secret"
	errNoSecretRef               = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds          = "unsupported credentials source: only Secret is supported"
	errMissingCredKey            = "credentials secret is missing one of the required host/username/password keys"
	errNewObjectManager          = "cannot create Infoblox NIOS WAPI object manager"
	errObserveRangeTemplate      = "cannot observe RangeTemplate"
	errCreateRangeTemplate       = "cannot create RangeTemplate"
	errUpdateRangeTemplate       = "cannot update RangeTemplate"
	errDeleteRangeTemplate       = "cannot delete RangeTemplate"
	errEmptyUID                  = "cannot stamp RangeTemplate identity: managed resource's metadata.uid is empty"
	errDeleteUnverifiedOwnership = "refusing to delete: the resolved object's identity extensible attribute is absent or belongs to a different owner, so ownership cannot be verified before an irreversible delete. " +
		"Reconcile the external-name annotation, verify the Grid object manually, or remove the finalizer to abandon it without deleting."
	errPrerequisiteCheck = "cannot verify the identity extensible attribute definition prerequisite"
)

// unresolvedProbeEndpoint is the identity-prerequisite-probe cache key
// used when an ExternalClient is built without a resolved Grid endpoint.
// See the doc on this constant in the recorda package for the full
// rationale — production code always goes through Connect().
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
func extractCredentials(ctx context.Context, kube k8sclient.Client, source xpv2.CredentialsSource, secretRef *xpv2.SecretKeySelector, fallbackNamespace string) (*nioCredentials, error) {
	if source != xpv2.CredentialsSourceSecret {
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

// newObjectManager constructs an authenticated identity.ManagerAndConnector
// from the given credentials — the SDK's high-level ObjectManager for the
// ordinary CRUD calls, and the lower-level Connector the identity ladder
// needs directly (it operates below ObjectManager's typed methods so it
// can see search match counts). The Connector performs HTTP Basic Auth on
// every request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newObjectManager(creds *nioCredentials, sslVerify bool) (identity.ManagerAndConnector, error) {
	return newObjectManagerWithScheme(creds, sslVerify, "https", "443")
}

// newObjectManagerWithScheme is the scheme/port-parameterized variant of
// newObjectManager used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newObjectManagerWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (identity.ManagerAndConnector, error) {
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
		return identity.ManagerAndConnector{}, errors.Wrap(err, errNewObjectManager)
	}

	return identity.NewManagerAndConnector(conn), nil
}

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

// templateOption is the scope-neutral currency for one DHCP option entry.
// The cluster and namespaced RangeTemplateOption types are structurally
// identical (same field names and primitive types) but are distinct named
// Go types, so this intermediate value lets the comparison/late-init/SDK
// logic below stay scope-agnostic; each scope converts to/from its own
// named type at the call site.
type templateOption struct {
	Name        *string
	Num         *uint32
	VendorClass *string
	Value       *string
	UseOption   *bool
}

// templateMember is the scope-neutral currency for the Grid member that
// will serve ranges created from a RangeTemplate — see templateOption for
// the rationale.
type templateMember struct {
	Ipv4Addr *string
	Ipv6Addr *string
	Name     *string
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

func boolPtrOrFalse(b *bool) bool {
	return boolOrFalse(b)
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func uint32OrZero(v *uint32) uint32 {
	if v == nil {
		return 0
	}
	return *v
}

func uint32PtrOrZero(v *uint32) uint32 {
	return uint32OrZero(v)
}

// buildDhcpOptions converts the scope-neutral option list into the SDK's
// []*ibclient.Dhcpoption request shape. The SDK's Dhcpoption fields are
// plain (non-pointer) primitives, so a nil field is normalized to its zero
// value on the wire — WAPI treats an absent option field the same as its
// documented default.
func buildDhcpOptions(opts []templateOption) []*ibclient.Dhcpoption {
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

// dhcpOptionsToCommon converts an observed []*ibclient.Dhcpoption response
// into the scope-neutral currency. Every field is returned as a non-nil
// pointer reflecting the observed value (including the Go zero value) —
// unlike ARecord's SDK types, Dhcpoption exposes only plain primitives, so
// there is no way to distinguish "unset" from "explicitly zero" once the
// WAPI response has been decoded; the full-mirror AtProvider snapshot
// simply mirrors whatever the server returned.
func dhcpOptionsToCommon(opts []*ibclient.Dhcpoption) []templateOption {
	if len(opts) == 0 {
		return nil
	}
	out := make([]templateOption, 0, len(opts))
	for _, o := range opts {
		if o == nil {
			continue
		}
		name, num, vendorClass, value, useOption := o.Name, o.Num, o.VendorClass, o.Value, o.UseOption
		out = append(out, templateOption{
			Name:        &name,
			Num:         &num,
			VendorClass: &vendorClass,
			Value:       &value,
			UseOption:   &useOption,
		})
	}
	return out
}

// optionsEqual compares two scope-neutral option lists field-by-field,
// normalizing nil pointers to their zero value on both sides (order
// sensitive — WAPI has no documented option re-ordering behavior, so a
// reordered-but-otherwise-identical list is treated as drift).
func optionsEqual(a, b []templateOption) bool {
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

// buildDhcpMember converts the scope-neutral member into the SDK's
// *ibclient.Dhcpmember request shape.
func buildDhcpMember(m *templateMember) *ibclient.Dhcpmember {
	if m == nil {
		return nil
	}
	return &ibclient.Dhcpmember{
		Ipv4Addr: strOrEmpty(m.Ipv4Addr),
		Ipv6Addr: strOrEmpty(m.Ipv6Addr),
		Name:     strOrEmpty(m.Name),
	}
}

// dhcpMemberToCommon converts an observed *ibclient.Dhcpmember response
// into the scope-neutral currency.
func dhcpMemberToCommon(m *ibclient.Dhcpmember) *templateMember {
	if m == nil {
		return nil
	}
	ipv4, ipv6, name := m.Ipv4Addr, m.Ipv6Addr, m.Name
	return &templateMember{Ipv4Addr: &ipv4, Ipv6Addr: &ipv6, Name: &name}
}

// memberEqual compares two scope-neutral members, treating nil as the
// zero-value member so a nil-vs-explicit-zero-value member is not reported
// as drift.
func memberEqual(a, b *templateMember) bool {
	var av, bv templateMember
	if a != nil {
		av = *a
	}
	if b != nil {
		bv = *b
	}
	return strOrEmpty(av.Ipv4Addr) == strOrEmpty(bv.Ipv4Addr) &&
		strOrEmpty(av.Ipv6Addr) == strOrEmpty(bv.Ipv6Addr) &&
		strOrEmpty(av.Name) == strOrEmpty(bv.Name)
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
// than a whole RangeTemplateParameters value — the cluster and namespaced
// RangeTemplateParameters types are structurally identical but distinct
// named Go types, so parameterizing on the field pointers/scope-neutral
// values lets both scopes share this logic unconditionally.

// isUpToDate compares the desired RangeTemplate fields against the
// observed Rangetemplate. name, numberOfAddresses, and offset are required
// fields and always compared. msServer is intentionally excluded: it is a
// write-only convenience parameter that WAPI wraps into a differently
// shaped ms_server.ipv4addr response field, so it is never echoed back
// into AtProvider and cannot be verified for drift — see
// RangeTemplateParameters.MsServer's doc comment.
//
// cloudApiCompatible is intentionally excluded from spec comparison — see
// createRangeTemplate's doc comment for why the wire value is always
// true regardless of what the caller supplied. The comparison target is
// therefore the constant true, not the spec value, so a Grid object
// toggled back to cloud-incompatible out-of-band (e.g. via Grid Manager)
// is still detected as drift and self-heals through Update.
func isUpToDate(name *string, numberOfAddresses, offset *uint32, comment *string, extAttrs map[string]string, options []templateOption, useOptions *bool, serverAssociationType string, failoverAssociation *string, member *templateMember, rec *ibclient.Rangetemplate) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if uint32OrZero(numberOfAddresses) != uint32PtrOrZero(rec.NumberOfAddresses) {
		return false
	}
	if uint32OrZero(offset) != uint32PtrOrZero(rec.Offset) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if !extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rec.Ea))) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useOptions) != boolPtrOrFalse(rec.UseOptions) {
		return false
	}
	// Only compare options when the flag is on. When it is off, WAPI
	// ignores the submitted DHCP options and returns its own default set
	// on every GET — comparing them against the spec value never
	// converges.
	if boolOrFalse(useOptions) {
		if !optionsEqual(options, dhcpOptionsToCommon(rec.Options)) {
			return false
		}
	}
	if serverAssociationType != rec.ServerAssociationType {
		return false
	}
	if strOrEmpty(failoverAssociation) != strOrEmpty(rec.FailoverAssociation) {
		return false
	}
	if !memberEqual(member, dhcpMemberToCommon(rec.Member)) {
		return false
	}
	return boolPtrOrFalse(rec.CloudApiCompatible)
}

// lateInitialize back-fills server-defaulted optional fields into spec so
// isUpToDate does not see phantom drift on the next reconcile. Required
// fields (name, numberOfAddresses, offset) are never late-initialized —
// they are always user-supplied. msServer is never late-initialized either
// — see isUpToDate's doc comment for why it has no observable counterpart.
// Returns true if any field was changed. Each field is delegated to its
// own single-purpose helper (rather than one long if-chain) to keep this
// function's cyclomatic complexity low.
func lateInitialize(comment **string, extAttrs *map[string]string, options *[]templateOption, useOptions **bool, serverAssociationType *string, failoverAssociation **string, member **templateMember, cloudApiCompatible **bool, rec *ibclient.Rangetemplate) bool {
	changed := false
	changed = lateInitComment(comment, rec) || changed
	changed = lateInitExtAttrs(extAttrs, rec) || changed
	changed = lateInitUseOptions(useOptions, rec) || changed
	// Only back-fill options when useOptions is on (post-backfill value
	// above). When it is off, the observed options are WAPI's own
	// default set, not values implied by the user's config.
	if boolOrFalse(*useOptions) {
		changed = lateInitOptions(options, rec) || changed
	}
	changed = lateInitServerAssociationType(serverAssociationType, rec) || changed
	changed = lateInitFailoverAssociation(failoverAssociation, rec) || changed
	changed = lateInitMember(member, rec) || changed
	changed = lateInitCloudApiCompatible(cloudApiCompatible, rec) || changed
	return changed
}

func lateInitComment(comment **string, rec *ibclient.Rangetemplate) bool {
	if *comment != nil || rec.Comment == nil || *rec.Comment == "" {
		return false
	}
	c := *rec.Comment
	*comment = &c
	return true
}

func lateInitExtAttrs(extAttrs *map[string]string, rec *ibclient.Rangetemplate) bool {
	if len(*extAttrs) != 0 {
		return false
	}
	fromRec := extAttrsFromEA(identity.Strip(rec.Ea))
	if len(fromRec) == 0 {
		return false
	}
	*extAttrs = fromRec
	return true
}

func lateInitOptions(options *[]templateOption, rec *ibclient.Rangetemplate) bool {
	if len(*options) != 0 {
		return false
	}
	fromRec := dhcpOptionsToCommon(rec.Options)
	if len(fromRec) == 0 {
		return false
	}
	*options = fromRec
	return true
}

func lateInitUseOptions(useOptions **bool, rec *ibclient.Rangetemplate) bool {
	if *useOptions != nil || rec.UseOptions == nil {
		return false
	}
	u := *rec.UseOptions
	*useOptions = &u
	return true
}

func lateInitServerAssociationType(serverAssociationType *string, rec *ibclient.Rangetemplate) bool {
	if *serverAssociationType != "" || rec.ServerAssociationType == "" {
		return false
	}
	*serverAssociationType = rec.ServerAssociationType
	return true
}

func lateInitFailoverAssociation(failoverAssociation **string, rec *ibclient.Rangetemplate) bool {
	if *failoverAssociation != nil || rec.FailoverAssociation == nil || *rec.FailoverAssociation == "" {
		return false
	}
	f := *rec.FailoverAssociation
	*failoverAssociation = &f
	return true
}

func lateInitMember(member **templateMember, rec *ibclient.Rangetemplate) bool {
	if *member != nil {
		return false
	}
	fromRec := dhcpMemberToCommon(rec.Member)
	if fromRec == nil {
		return false
	}
	*member = fromRec
	return true
}

func lateInitCloudApiCompatible(cloudApiCompatible **bool, rec *ibclient.Rangetemplate) bool {
	if *cloudApiCompatible != nil || rec.CloudApiCompatible == nil {
		return false
	}
	c := *rec.CloudApiCompatible
	*cloudApiCompatible = &c
	return true
}

// observedRangeTemplate holds the primitive field values extracted from a
// WAPI Rangetemplate response. The cluster and namespaced
// RangeTemplateObservation types are structurally similar but are
// distinct named types with distinct nested-struct field types (e.g.
// *RangeTemplateMember), so they are not directly convertible — each
// scope copies this intermediate struct's fields into its own Observation
// type at the call site. MsServer is intentionally absent — see
// isUpToDate's doc comment.
type observedRangeTemplate struct {
	ID                    string
	Name                  *string
	NumberOfAddresses     *uint32
	Offset                *uint32
	Comment               *string
	ExtAttrs              map[string]string
	Options               []templateOption
	UseOptions            *bool
	ServerAssociationType *string
	FailoverAssociation   *string
	Member                *templateMember
	CloudApiCompatible    *bool
	Ref                   *string
}

// observeFromRangeTemplate extracts the fields mirrored by
// RangeTemplateObservation (the full-mirror AtProvider convention) from a
// WAPI Rangetemplate response. NewEmptyRangeTemplate requests the
// extattrs/options/use_options/server_association_type/
// failover_association/member/cloud_api_compatible/ms_server fields in
// addition to the SDK's default return-field set, so
// GetRangeTemplateByRef/GetAllRangeTemplate responses always carry them.
func observeFromRangeTemplate(externalID string, rec *ibclient.Rangetemplate) observedRangeTemplate {
	o := observedRangeTemplate{
		ID:                externalID,
		Name:              rec.Name,
		NumberOfAddresses: rec.NumberOfAddresses,
		Offset:            rec.Offset,
		ExtAttrs:          extAttrsFromEA(rec.Ea),
		Options:           dhcpOptionsToCommon(rec.Options),
		Member:            dhcpMemberToCommon(rec.Member),
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.UseOptions != nil {
		u := *rec.UseOptions
		o.UseOptions = &u
	}
	if rec.ServerAssociationType != "" {
		s := rec.ServerAssociationType
		o.ServerAssociationType = &s
	}
	if rec.FailoverAssociation != nil && *rec.FailoverAssociation != "" {
		f := *rec.FailoverAssociation
		o.FailoverAssociation = &f
	}
	if rec.CloudApiCompatible != nil {
		c := *rec.CloudApiCompatible
		o.CloudApiCompatible = &c
	}
	if rec.Ref != "" {
		r := rec.Ref
		o.Ref = &r
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// wapiCloudAPICompatible is the literal value this controller always
// sends on the wire for a RangeTemplate's cloud_api_compatible field,
// regardless of what the caller's spec supplied.
//
// The identity extensible attribute this controller stamps on every
// Create and Update (identity.Stamp) is itself cloud-API-compatible —
// its definition carries the "C" flag. A Grid enforces a matching
// constraint on template-family objects: it refuses to attach a
// cloud-compatible extensible attribute to a cloud-incompatible template
// object ("Cloud-incompatible template object ... references extensible
// attribute Crossplane Internal ID that is cloud-compatible"). Because
// the identity stamp is unconditional — every RangeTemplate this
// provider manages carries it — every RangeTemplate this provider
// manages must therefore also be cloud-API-compatible. There is no way
// to satisfy both "always stamp identity" and "respect an explicit
// cloud_api_compatible=false" at once, so this field is effectively
// provider-owned once a resource is under management: the wire value is
// always true, and isUpToDate compares the Grid's observed value against
// that same constant rather than against whatever the spec says.
const wapiCloudAPICompatible = true

// createRangeTemplate issues the WAPI create call, stamping the owning
// managed resource's uid into the object's extensible attributes in the
// same request that creates it (identity.Stamp) — there is no follow-up
// call, so there is no window in which the object exists without its
// identity stamp. cloud_api_compatible is always sent as true — see
// wapiCloudAPICompatible's doc comment for why.
func createRangeTemplate(objMgr ibclient.IBObjectManager, name *string, numberOfAddresses, offset *uint32, comment *string, extAttrs map[string]string, options []templateOption, useOptions *bool, serverAssociationType string, failoverAssociation *string, member *templateMember, msServer *string, uid string) (*ibclient.Rangetemplate, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateRangeTemplate(
		strOrEmpty(name),
		uint32OrZero(numberOfAddresses),
		uint32OrZero(offset),
		strOrEmpty(comment),
		ea,
		buildDhcpOptions(options),
		boolOrFalse(useOptions),
		serverAssociationType,
		strOrEmpty(failoverAssociation),
		buildDhcpMember(member),
		wapiCloudAPICompatible,
		strOrEmpty(msServer),
	)
}

// updateRangeTemplate issues the WAPI update call (PUT partial/merge — the
// blueprint records no immutable fields for RangeTemplate, so every
// mutable ForProvider field, including the write-only msServer
// convenience field, is sent on every update). Every call re-asserts the
// identity stamp (identity.Stamp) in the extattrs it sends. Live
// verification against a real NIOS Grid Manager confirmed that a PUT
// carrying an extattrs object *replaces* the whole map — it is not a
// per-key merge — so omitting the stamp here would wipe it off the
// object on the very first field update after create. cloud_api_compatible
// is always sent as true — see wapiCloudAPICompatible's doc comment for
// why; this also self-heals an object adopted from before this provider
// managed it, or one toggled cloud-incompatible out-of-band.
func updateRangeTemplate(objMgr ibclient.IBObjectManager, ref string, name *string, numberOfAddresses, offset *uint32, comment *string, extAttrs map[string]string, options []templateOption, useOptions *bool, serverAssociationType string, failoverAssociation *string, member *templateMember, msServer *string, uid string) (*ibclient.Rangetemplate, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.UpdateRangeTemplate(
		ref,
		strOrEmpty(name),
		uint32OrZero(numberOfAddresses),
		uint32OrZero(offset),
		strOrEmpty(comment),
		ea,
		buildDhcpOptions(options),
		boolOrFalse(useOptions),
		serverAssociationType,
		strOrEmpty(failoverAssociation),
		buildDhcpMember(member),
		wapiCloudAPICompatible,
		strOrEmpty(msServer),
	)
}

// deleteRangeTemplate issues the WAPI delete call.
func deleteRangeTemplate(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteRangeTemplate(ref)
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
// first for a managed resource's stored external-name. See recorda's doc
// for the full rationale.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveRangeTemplateIdentity resolves the RangeTemplate identified by
// ref/uid through the shared UID-in-EA ladder.
func resolveRangeTemplateIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.Rangetemplate, identity.Outcome, error) {
	return identity.Resolve[*ibclient.Rangetemplate](ctx, conn, ibclient.NewEmptyRangeTemplate, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting a
// RangeTemplate through the identity ladder during Observe — common to
// both scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	rec          *ibclient.Rangetemplate
	obs          observedRangeTemplate
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeRangeTemplate runs the identity ladder for Observe. Unlike the
// simpler resources, RangeTemplate's late-init step needs scope-specific
// type conversion for options/member, so the caller (cluster.go /
// namespaced.go) performs lateInitialize itself using the returned
// observeResult.rec — this function only resolves identity and builds the
// observation snapshot.
func observeRangeTemplate(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveRangeTemplateIdentity(ctx, conn, ref, uid)
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

	res := observeResult{
		exists:  true,
		rec:     rec,
		obs:     observeFromRangeTemplate(rec.Ref, rec),
		adopted: outcome == identity.OutcomeAdopted,
	}

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteRangeTemplateIdentity issues the WAPI delete for the RangeTemplate
// this managed resource owns, resolving through the identity ladder first
// so a stale _ref is never mistaken for a deleted object.
func deleteRangeTemplateIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveRangeTemplateIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteRangeTemplate)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteRangeTemplate(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteRangeTemplate)
	default:
		return errors.New("identity: unresolved RangeTemplate outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced
// RangeTemplate controllers with the SafeStart gate. Each controller
// starts only after its respective CRD has been installed in the
// cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controllerpkg.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterRangeTemplate(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster RangeTemplate controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("RangeTemplate"))

	o.Gate.Register(func() {
		if err := setupNamespacedRangeTemplate(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced RangeTemplate controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("RangeTemplate"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced RangeTemplate
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controllerpkg.Options) error {
	if err := setupClusterRangeTemplate(mgr, o); err != nil {
		return err
	}
	return setupNamespacedRangeTemplate(mgr, o)
}
