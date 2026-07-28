// Package rangetemplate implements the Crossplane controller for the
// Infoblox NIOS RangeTemplate managed resource. Like recorda, this
// provider wraps the official infoblox-go-client Go SDK directly — the
// SDK's ObjectManager exposes typed CRUD methods
// (CreateRangeTemplate/GetRangeTemplateByRef/UpdateRangeTemplate/
// DeleteRangeTemplate) instead of a generic HTTP request/response
// envelope, so there is no internal REST client to compose.
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

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	controllerpkg "github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/rangetemplate/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/rangetemplate/v1alpha1"
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
	errObserveRangeTemplate = "cannot observe RangeTemplate"
	errCreateRangeTemplate  = "cannot create RangeTemplate"
	errUpdateRangeTemplate  = "cannot update RangeTemplate"
	errDeleteRangeTemplate  = "cannot delete RangeTemplate"
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
func isUpToDate(name *string, numberOfAddresses, offset *uint32, comment *string, extAttrs map[string]string, options []templateOption, useOptions *bool, serverAssociationType string, failoverAssociation *string, member *templateMember, cloudApiCompatible *bool, rec *ibclient.Rangetemplate) bool {
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
	if !extAttrsEqual(extAttrs, extAttrsFromEA(rec.Ea)) {
		return false
	}
	if !optionsEqual(options, dhcpOptionsToCommon(rec.Options)) {
		return false
	}
	if boolOrFalse(useOptions) != boolPtrOrFalse(rec.UseOptions) {
		return false
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
	return boolOrFalse(cloudApiCompatible) == boolPtrOrFalse(rec.CloudApiCompatible)
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
	changed = lateInitOptions(options, rec) || changed
	changed = lateInitUseOptions(useOptions, rec) || changed
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
	fromRec := extAttrsFromEA(rec.Ea)
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

// createRangeTemplate issues the WAPI create call.
func createRangeTemplate(objMgr ibclient.IBObjectManager, name *string, numberOfAddresses, offset *uint32, comment *string, extAttrs map[string]string, options []templateOption, useOptions *bool, serverAssociationType string, failoverAssociation *string, member *templateMember, cloudApiCompatible *bool, msServer *string) (*ibclient.Rangetemplate, error) {
	return objMgr.CreateRangeTemplate(
		strOrEmpty(name),
		uint32OrZero(numberOfAddresses),
		uint32OrZero(offset),
		strOrEmpty(comment),
		buildEA(extAttrs),
		buildDhcpOptions(options),
		boolOrFalse(useOptions),
		serverAssociationType,
		strOrEmpty(failoverAssociation),
		buildDhcpMember(member),
		boolOrFalse(cloudApiCompatible),
		strOrEmpty(msServer),
	)
}

// updateRangeTemplate issues the WAPI update call (PUT partial/merge — the
// blueprint records no immutable fields for RangeTemplate, so every
// mutable ForProvider field, including the write-only msServer
// convenience field, is sent on every update).
func updateRangeTemplate(objMgr ibclient.IBObjectManager, ref string, name *string, numberOfAddresses, offset *uint32, comment *string, extAttrs map[string]string, options []templateOption, useOptions *bool, serverAssociationType string, failoverAssociation *string, member *templateMember, cloudApiCompatible *bool, msServer *string) (*ibclient.Rangetemplate, error) {
	return objMgr.UpdateRangeTemplate(
		ref,
		strOrEmpty(name),
		uint32OrZero(numberOfAddresses),
		uint32OrZero(offset),
		strOrEmpty(comment),
		buildEA(extAttrs),
		buildDhcpOptions(options),
		boolOrFalse(useOptions),
		serverAssociationType,
		strOrEmpty(failoverAssociation),
		buildDhcpMember(member),
		boolOrFalse(cloudApiCompatible),
		strOrEmpty(msServer),
	)
}

// deleteRangeTemplate issues the WAPI delete call.
func deleteRangeTemplate(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteRangeTemplate(ref)
	return err
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
