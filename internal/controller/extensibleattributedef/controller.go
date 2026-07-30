// Package extensibleattributedef implements the Crossplane controller for
// the Infoblox NIOS ExtensibleAttributeDef managed resource.
//
// Unlike most resources in this provider, the infoblox-go-client SDK's
// ObjectManager exposes only CreateEADefinition and GetEADefinition for
// this object type — there are no UpdateEADefinition or
// DeleteEADefinition wrappers. WAPI itself supports full CRUD (PUT and
// DELETE both work against extensibleattributedef, live-verified), so
// this controller talks to the SDK's lower-level Connector directly
// (CreateObject/GetObject/UpdateObject/DeleteObject, all of which accept
// any type implementing the SDK's IBObject interface) instead of routing
// through the per-object ObjectManager wrappers. Using the same
// Connector for every operation — rather than mixing ObjectManager calls
// for Create/Read with hand-rolled net/http for Update/Delete — keeps
// authentication, session handling, and WAPI error classification
// uniform across all four operations.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared connector plumbing, field comparison, and late-init logic lives
// here.
package extensibleattributedef

import (
	"context"
	"net/http"
	"regexp"
	"strconv"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/extensibleattributedef/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/extensibleattributedef/v1alpha1"
)

// Error constants — all errors must use the crossplane-runtime errors
// package (never fmt.Errorf or the standard library error-construction
// package).
const (
	errTrackPCUsage        = "cannot track ProviderConfig usage"
	errGetPC               = "cannot get ProviderConfig"
	errGetClusterPC        = "cannot get ClusterProviderConfig"
	errUnsupportedKind     = "unsupported provider config kind"
	errGetSecret           = "cannot get credentials secret"
	errNoSecretRef         = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errUnsupportedCreds    = "unsupported credentials source: only Secret is supported"
	errMissingCredKey      = "credentials secret is missing one of the required host/username/password keys"
	errNewConnector        = "cannot create Infoblox NIOS WAPI connector"
	errObserveEADefinition = "cannot observe ExtensibleAttributeDef"
	errCreateEADefinition  = "cannot create ExtensibleAttributeDef"
	errUpdateEADefinition  = "cannot update ExtensibleAttributeDef"
	errDeleteEADefinition  = "cannot delete ExtensibleAttributeDef"
)

// wapiVersion is the NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/ per the provider's base URL convention).
const wapiVersion = "2.9.7"

// eaDefReturnFields is the exact _return_fields set requested on every
// GET. descendants_action is intentionally excluded — the WAPI schema
// marks it write-only (supports=wu) and reading it back returns "Field
// is not readable".
var eaDefReturnFields = []string{
	"name", "type", "comment", "default_value", "min", "max",
	"flags", "list_values", "allowed_object_types", "namespace",
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

// newConnector constructs an authenticated *ibclient.Connector from the
// given credentials. The Connector performs HTTP Basic Auth on every
// request and only validates configuration locally — no network
// round-trip happens until the first Observe/Create/Update/Delete call.
func newConnector(creds *nioCredentials, sslVerify bool) (*ibclient.Connector, error) {
	return newConnectorWithScheme(creds, sslVerify, "https", "443")
}

// newConnectorWithScheme is the scheme/port-parameterized variant of
// newConnector used by unit tests to point the SDK at a plain-HTTP
// httptest.Server instead of a real HTTPS Grid Manager.
func newConnectorWithScheme(creds *nioCredentials, sslVerify bool, scheme, port string) (*ibclient.Connector, error) {
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

// ── SDK <-> CRD field translation helpers (shared by both scopes) ──────────

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// toSDKListValues converts the CRD's simplified []string representation
// of enumerated values into the SDK's []*ibclient.EADefListValue.
func toSDKListValues(values []string) []*ibclient.EADefListValue {
	if len(values) == 0 {
		return nil
	}
	out := make([]*ibclient.EADefListValue, len(values))
	for i, v := range values {
		out[i] = &ibclient.EADefListValue{Value: v}
	}
	return out
}

// fromSDKListValues converts the SDK's []*ibclient.EADefListValue back
// into the CRD's simplified []string representation.
func fromSDKListValues(values []*ibclient.EADefListValue) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, len(values))
	for i, v := range values {
		if v != nil {
			out[i] = v.Value
		}
	}
	return out
}

// descendantsActionToSDK converts the CRD's simplified *string
// representation of descendants_action into the SDK's
// ExtensibleattributedefDescendants struct, populating only
// OptionWithEa (the field this provider exposes). descendants_action is
// write-only in WAPI, so there is no corresponding "fromSDK" conversion
// — it is never present in a GET response.
func descendantsActionToSDK(v *string) *ibclient.ExtensibleattributedefDescendants {
	if v == nil || *v == "" {
		return nil
	}
	return &ibclient.ExtensibleattributedefDescendants{OptionWithEa: *v}
}

// stringSlicesEqualUnordered reports whether two string slices contain
// the same multiset of values, treating a nil slice and an empty slice
// as equal. WAPI is not guaranteed to echo list_values/
// allowed_object_types back in the exact order they were submitted, so
// comparing order-independently avoids a false phantom diff that would
// otherwise trigger an Update on every reconcile.
func stringSlicesEqualUnordered(a, b []string) bool {
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

// ── Field comparison / late-init ────────────────────────────────────────
//
// These helpers take pointers to the individual ForProvider fields rather
// than a whole ExtensibleAttributeDefParameters value. The cluster and
// namespaced ExtensibleAttributeDefParameters types are structurally
// identical (same field names and primitive types) but are distinct
// named Go types, so parameterizing on the field pointers instead lets
// both scopes share this logic.

// isUpToDate compares the desired ExtensibleAttributeDef fields against
// the observed EADefinition. Type, Min, and Max are immutable (WAPI
// rejects a PUT that changes them) and are intentionally excluded from
// this comparison — including them would trigger an Update that WAPI can
// never satisfy, causing an infinite reconcile loop. descendants_action
// is write-only (never present in a GET response) and is also excluded.
func isUpToDate(name string, comment, defaultValue, flags *string, listValues, allowedObjectTypes []string, def *ibclient.EADefinition) bool {
	if name != strOrEmpty(def.Name) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(def.Comment) {
		return false
	}
	if strOrEmpty(defaultValue) != strOrEmpty(def.DefaultValue) {
		return false
	}
	if strOrEmpty(flags) != strOrEmpty(def.Flags) {
		return false
	}
	if !stringSlicesEqualUnordered(listValues, fromSDKListValues(def.ListValues)) {
		return false
	}
	return stringSlicesEqualUnordered(allowedObjectTypes, def.AllowedObjectTypes)
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// defaultValue, flags, listValues, allowedObjectTypes) from the observed
// EADefinition into spec so isUpToDate does not see phantom drift on the
// next reconcile. Required fields (name, type) and the immutable
// min/max fields are never late-initialized: name and type are always
// user-supplied (required on the CRD), and min/max are excluded from
// isUpToDate anyway so late-initializing them would have no observable
// effect on reconciliation — WAPI never invents values for them beyond
// what the user supplied at create time. Returns true if any field was
// changed.
func lateInitialize(comment, defaultValue, flags **string, listValues, allowedObjectTypes *[]string, def *ibclient.EADefinition) bool {
	changed := false

	if *comment == nil && def.Comment != nil && *def.Comment != "" {
		c := *def.Comment
		*comment = &c
		changed = true
	}
	if *defaultValue == nil && def.DefaultValue != nil && *def.DefaultValue != "" {
		d := *def.DefaultValue
		*defaultValue = &d
		changed = true
	}
	if *flags == nil && def.Flags != nil && *def.Flags != "" {
		f := *def.Flags
		*flags = &f
		changed = true
	}
	if len(*listValues) == 0 {
		if fromDef := fromSDKListValues(def.ListValues); len(fromDef) > 0 {
			*listValues = fromDef
			changed = true
		}
	}
	if len(*allowedObjectTypes) == 0 && len(def.AllowedObjectTypes) > 0 {
		*allowedObjectTypes = append([]string{}, def.AllowedObjectTypes...)
		changed = true
	}

	return changed
}

// observedEADefinition holds the primitive field values extracted from a
// WAPI EADefinition response. The cluster and namespaced
// ExtensibleAttributeDefObservation types are structurally identical but
// are distinct named types, so each scope copies this intermediate
// struct's fields into its own Observation type at the call site.
type observedEADefinition struct {
	ID                 string
	Name               *string
	Type               *string
	Comment            *string
	DefaultValue       *string
	Min                *uint32
	Max                *uint32
	Flags              *string
	ListValues         []string
	AllowedObjectTypes []string
	Ref                *string
	Namespace          *string
}

// observeFromEADefinition extracts the fields mirrored by
// ExtensibleAttributeDefObservation (the full-mirror AtProvider
// convention) from a WAPI EADefinition response. descendants_action is
// never populated here — it is write-only and never present in a GET
// response.
func observeFromEADefinition(externalID string, def *ibclient.EADefinition) observedEADefinition {
	o := observedEADefinition{ID: externalID}

	if def.Name != nil && *def.Name != "" {
		n := *def.Name
		o.Name = &n
	}
	if def.Type != "" {
		t := def.Type
		o.Type = &t
	}
	if def.Comment != nil && *def.Comment != "" {
		c := *def.Comment
		o.Comment = &c
	}
	if def.DefaultValue != nil && *def.DefaultValue != "" {
		d := *def.DefaultValue
		o.DefaultValue = &d
	}
	if def.Min != nil {
		m := *def.Min
		o.Min = &m
	}
	if def.Max != nil {
		m := *def.Max
		o.Max = &m
	}
	if def.Flags != nil && *def.Flags != "" {
		f := *def.Flags
		o.Flags = &f
	}
	o.ListValues = fromSDKListValues(def.ListValues)
	if len(def.AllowedObjectTypes) > 0 {
		o.AllowedObjectTypes = append([]string{}, def.AllowedObjectTypes...)
	}
	if def.Ref != "" {
		r := def.Ref
		o.Ref = &r
	}
	if def.Namespace != "" {
		ns := def.Namespace
		o.Namespace = &ns
	}

	return o
}

// ── WAPI call wrappers (shared by both scopes) ──────────────────────────
//
// These issue requests through the SDK's low-level Connector
// (CreateObject/GetObject/UpdateObject/DeleteObject) rather than the
// ObjectManager, because ObjectManager has no Update/Delete wrapper for
// this object type.

// createEADefinition issues the WAPI create call (POST
// extensibleattributedef).
func createEADefinition(conn *ibclient.Connector, name, objType string, comment, defaultValue *string, minVal, maxVal *uint32, flags *string, listValues []*ibclient.EADefListValue, allowedObjectTypes []string, descendantsAction *ibclient.ExtensibleattributedefDescendants) (*ibclient.EADefinition, error) {
	n := name
	obj := &ibclient.EADefinition{
		Name:               &n,
		Type:               objType,
		Comment:            comment,
		DefaultValue:       defaultValue,
		Min:                minVal,
		Max:                maxVal,
		Flags:              flags,
		ListValues:         listValues,
		AllowedObjectTypes: allowedObjectTypes,
		DescendantsAction:  descendantsAction,
	}
	ref, err := conn.CreateObject(obj)
	if err != nil {
		return nil, err
	}
	obj.Ref = ref
	return obj, nil
}

// getEADefinitionByRef issues the WAPI read call (GET <_ref>) requesting
// exactly eaDefReturnFields — descendants_action is never requested
// (write-only, "Field is not readable").
func getEADefinitionByRef(conn *ibclient.Connector, ref string) (*ibclient.EADefinition, error) {
	res := &ibclient.EADefinition{}
	res.SetReturnFields(eaDefReturnFields)
	if err := conn.GetObject(res, ref, ibclient.NewQueryParams(false, nil), res); err != nil {
		return nil, err
	}
	return res, nil
}

// updateEADefinition issues the WAPI update call (PUT <_ref>) with only
// the mutable fields (name, comment, default_value, flags, list_values,
// allowed_object_types, descendants_action). type/min/max are immutable
// and are never sent — WAPI rejects any attempt to change them. WAPI's
// update semantics for this object are partial/merge (omitted fields
// keep their existing server-side value), so omitting the immutable
// fields here does not risk clearing them. Returns the object's
// (possibly new — renaming an ExtensibleAttributeDef changes its _ref)
// _ref.
func updateEADefinition(conn *ibclient.Connector, ref, name string, comment, defaultValue, flags *string, listValues []*ibclient.EADefListValue, allowedObjectTypes []string, descendantsAction *ibclient.ExtensibleattributedefDescendants) (string, error) {
	n := name
	obj := &ibclient.EADefinition{
		Name:               &n,
		Comment:            comment,
		DefaultValue:       defaultValue,
		Flags:              flags,
		ListValues:         listValues,
		AllowedObjectTypes: allowedObjectTypes,
		DescendantsAction:  descendantsAction,
	}
	return conn.UpdateObject(obj, ref)
}

// deleteEADefinition issues the WAPI delete call (DELETE <_ref>).
func deleteEADefinition(conn *ibclient.Connector, ref string) error {
	_, err := conn.DeleteObject(ref)
	return err
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced
// ExtensibleAttributeDef controllers with the SafeStart gate. Each
// controller starts only after its respective CRD has been installed in
// the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterExtensibleAttributeDef(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster ExtensibleAttributeDef controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("ExtensibleAttributeDef"))

	o.Gate.Register(func() {
		if err := setupNamespacedExtensibleAttributeDef(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced ExtensibleAttributeDef controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("ExtensibleAttributeDef"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced
// ExtensibleAttributeDef controllers immediately without SafeStart
// gating (RBAC fallback path, for environments that pre-install CRDs
// before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterExtensibleAttributeDef(mgr, o); err != nil {
		return err
	}
	return setupNamespacedExtensibleAttributeDef(mgr, o)
}
