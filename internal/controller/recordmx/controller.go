// Package recordmx implements the Crossplane controller for the Infoblox
// NIOS MXRecord managed resource. Like the ARecord controller, it wraps
// the official infoblox-go-client Go SDK directly — the SDK's
// ObjectManager exposes typed CRUD methods (CreateMXRecord/
// GetMXRecordByRef/UpdateMXRecord/DeleteMXRecord) instead of a generic
// HTTP request/response envelope, so there is no internal REST client to
// compose.
//
// MXRecord is wired to the UID-in-EA object-identity ladder (see
// recorda's package doc for the full rationale): the WAPI _ref this
// resource's create call returns is a derived handle, not a stable
// backend-assigned ID, so this controller stamps the managed resource's
// own metadata.uid onto the Grid object as an extensible attribute and
// resolves every Observe/Delete through the shared identity.Resolve
// ladder instead of trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordmx

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordmx/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordmx/v1alpha1"
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
	errObserveMXRecord           = "cannot observe MXRecord"
	errCreateMXRecord            = "cannot create MXRecord"
	errUpdateMXRecord            = "cannot update MXRecord"
	errDeleteMXRecord            = "cannot delete MXRecord"
	errEmptyUID                  = "cannot stamp MXRecord identity: managed resource's metadata.uid is empty"
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

// newObjectManager constructs an authenticated ibclient.IBObjectManager
// from the given credentials. The Connector performs HTTP Basic Auth on
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

// uint32PtrOrZero converts an optional *uint32 field (TTL or Preference,
// as returned by the SDK) into a plain uint32 for comparison against
// preferenceOrZero, or used directly for TTL (also *uint32 in
// ForProvider).
func uint32PtrOrZero(v *uint32) uint32 {
	if v == nil {
		return 0
	}
	return *v
}

// preferenceOrZero converts an optional *int64 MX preference into the
// uint32 the SDK expects. Preference is a 0-65535 priority value; values
// outside that range (or unset) clamp to 0 rather than silently wrapping —
// CEL/CRD validation on the ForProvider field is expected to reject
// out-of-range values before they ever reach this helper.
func preferenceOrZero(preference *int64) uint32 {
	if preference == nil || *preference < 0 || *preference > 65535 {
		return 0
	}
	return uint32(*preference)
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
// than a whole MXRecordParameters value. The cluster and namespaced
// MXRecordParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct
// struct conversion between them is not always available once other
// resources in this provider grow reference fields; parameterizing on the
// field pointers instead lets both scopes share this logic unconditionally.

// isUpToDate compares the desired MXRecord fields against the observed
// RecordMX. View is immutable (WAPI ties the object's _ref to
// view+zone+name; the CRD's XValidation rule rejects any spec change to
// it) and is intentionally excluded from this comparison — the API can
// never see a drifted view coming from spec.
func isUpToDate(name, mailExchanger *string, preference *int64, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordMX) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(mailExchanger) != strOrEmpty(rec.MailExchanger) {
		return false
	}
	if preferenceOrZero(preference) != uint32PtrOrZero(rec.Preference) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useTTL) != boolPtrOrFalse(rec.UseTtl) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted ttl and returns the zone default on every
	// GET — comparing it against the spec value never converges.
	if boolOrFalse(useTTL) {
		if uint32PtrOrZero(ttl) != uint32PtrOrZero(rec.Ttl) {
			return false
		}
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rec.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// ttl, useTtl, extAttrs) from the observed RecordMX into spec so
// isUpToDate does not see phantom drift on the next reconcile. Required
// fields (name, mailExchanger, preference) and the immutable view field
// are never late-initialized — view is always user-supplied (required on
// the CRD) and name/mailExchanger/preference are always user-supplied
// too. Returns true if any field was changed.
func lateInitialize(comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordMX) bool {
	changed := false

	if *comment == nil && rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		*comment = &c
		changed = true
	}
	if *useTTL == nil && rec.UseTtl != nil {
		u := *rec.UseTtl
		*useTTL = &u
		changed = true
	}
	// Only back-fill ttl when useTtl is on (post-backfill value above).
	// When it is off, the observed ttl is WAPI's zone default, not a
	// value implied by the user's config — writing it into spec would
	// silently claim a TTL that is not in effect.
	if *ttl == nil && rec.Ttl != nil && boolOrFalse(*useTTL) {
		t := *rec.Ttl
		*ttl = &t
		changed = true
	}
	if len(*extAttrs) == 0 {
		if fromRec := extAttrsFromEA(identity.Strip(rec.Ea)); len(fromRec) > 0 {
			*extAttrs = fromRec
			changed = true
		}
	}

	return changed
}

// observedMXRecord holds the primitive field values extracted from a WAPI
// RecordMX response. The cluster and namespaced MXRecordObservation types
// are structurally similar but are distinct named types, so they are not
// directly convertible — each scope copies this intermediate struct's
// fields into its own Observation type at the call site.
type observedMXRecord struct {
	ID            string
	Name          *string
	MailExchanger *string
	Preference    *int64
	Comment       *string
	TTL           *uint32
	UseTTL        *bool
	ExtAttrs      map[string]string
	View          *string
	Ref           *string
	Zone          *string
}

// observeFromRecordMX extracts the fields mirrored by MXRecordObservation
// (the full-mirror AtProvider convention) from a WAPI RecordMX response.
// The SDK's GetMXRecordByRef/GetMXRecord methods request only a fixed
// subset of fields by default (mail_exchanger, view, name, preference,
// ttl, use_ttl, comment, extattrs, zone); response-only fields outside
// that set (creator, dns_name, cloud_info, etc.) are not requested by
// this provider and are left at their zero value in AtProvider.
func observeFromRecordMX(externalID string, rec *ibclient.RecordMX) observedMXRecord {
	o := observedMXRecord{
		ID:            externalID,
		Name:          rec.Name,
		MailExchanger: rec.MailExchanger,
		ExtAttrs:      extAttrsFromEA(rec.Ea),
	}
	if rec.Preference != nil {
		p := int64(*rec.Preference)
		o.Preference = &p
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Ttl != nil {
		t := *rec.Ttl
		o.TTL = &t
	}
	if rec.UseTtl != nil {
		u := *rec.UseTtl
		o.UseTTL = &u
	}
	if rec.View != nil && *rec.View != "" {
		v := *rec.View
		o.View = &v
	}
	if rec.Ref != "" {
		r := rec.Ref
		o.Ref = &r
	}
	if rec.Zone != "" {
		z := rec.Zone
		o.Zone = &z
	}
	return o
}

// ── SDK call wrappers (shared by both scopes) ───────────────────────────

// createMXRecord issues the WAPI create call, stamping the owning
// managed resource's uid into the object's extensible attributes in the
// same request that creates it (identity.Stamp).
func createMXRecord(objMgr ibclient.IBObjectManager, view, name, mailExchanger *string, preference *int64, ttl *uint32, useTTL *bool, comment *string, extAttrs map[string]string, uid string) (*ibclient.RecordMX, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateMXRecord(
		strOrEmpty(view),
		strOrEmpty(name),
		strOrEmpty(mailExchanger),
		preferenceOrZero(preference),
		uint32PtrOrZero(ttl),
		boolOrFalse(useTTL),
		strOrEmpty(comment),
		ea,
	)
}

// updateMXRecord issues the WAPI update call. Unlike ARecord's
// UpdateARecord (which has no view parameter at all), UpdateMXRecord's
// SDK signature requires a dnsView argument and internally rejects the
// call if it does not match the record's current view ("changing
// 'dns_view' field after object creation is not allowed") — so it is
// always sourced from the CR's own spec.View. This is not a mutation:
// view is immutable (the CRD's XValidation rule rejects any spec change
// to it), so p.View is guaranteed to already equal the record's current
// view; passing it here only satisfies the SDK's required parameter
// without ever changing the value on the wire. Every call re-asserts the
// identity stamp (identity.Stamp) — a WAPI PUT carrying extattrs
// replaces the whole map, not a per-key merge.
func updateMXRecord(objMgr ibclient.IBObjectManager, ref string, view, name, mailExchanger *string, preference *int64, ttl *uint32, useTTL *bool, comment *string, extAttrs map[string]string, uid string) (*ibclient.RecordMX, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.UpdateMXRecord(
		ref,
		strOrEmpty(view),
		strOrEmpty(name),
		strOrEmpty(mailExchanger),
		preferenceOrZero(preference),
		uint32PtrOrZero(ttl),
		boolOrFalse(useTTL),
		strOrEmpty(comment),
		ea,
	)
}

// deleteMXRecord issues the WAPI delete call.
func deleteMXRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteMXRecord(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────

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

func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

func resolveMXRecordIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.RecordMX, identity.Outcome, error) {
	return identity.Resolve[*ibclient.RecordMX](ctx, conn, ibclient.NewEmptyRecordMX, ref, uid)
}

type observeResult struct {
	exists       bool
	rec          *ibclient.RecordMX
	obs          observedMXRecord
	lateInit     bool
	refreshedRef string
	adopted      bool
}

func observeMXRecord(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveMXRecordIdentity(ctx, conn, ref, uid)
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
		obs:     observeFromRecordMX(rec.Ref, rec),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(comment, ttl, useTTL, extAttrs, rec)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
		res.lateInit = true
	}

	return res, nil
}

func deleteMXRecordIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveMXRecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteMXRecord)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteMXRecord(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteMXRecord)
	default:
		return errors.New("identity: unresolved MXRecord outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced MXRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterMXRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster MXRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("MXRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedMXRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced MXRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("MXRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced MXRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterMXRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedMXRecord(mgr, o)
}
