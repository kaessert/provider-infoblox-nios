// Package recordalias implements the Crossplane controller for the
// Infoblox NIOS AliasRecord managed resource. Like the ARecord
// controller, this provider wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateAliasRecord/GetAliasRecordByRef/UpdateAliasRecord/
// DeleteAliasRecord) instead of a generic HTTP request/response envelope,
// so there is no internal REST client to compose.
//
// AliasRecord is wired to the UID-in-EA object-identity ladder (see
// recorda's package doc for the full rationale): the WAPI _ref this
// resource's create call returns is a derived handle, not a stable
// backend-assigned ID, so this controller stamps the managed resource's
// own metadata.uid onto the Grid object as an extensible attribute and
// resolves every Observe/Delete through the shared identity.Resolve
// ladder instead of trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordalias

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordalias/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordalias/v1alpha1"
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
	errObserveAliasRec           = "cannot observe AliasRecord"
	errCreateAliasRecord         = "cannot create AliasRecord"
	errUpdateAliasRecord         = "cannot update AliasRecord"
	errDeleteAliasRecord         = "cannot delete AliasRecord"
	errEmptyUID                  = "cannot stamp AliasRecord identity: managed resource's metadata.uid is empty"
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

// newObjectManager constructs an authenticated
// identity.ManagerAndConnector from the given credentials — the SDK's
// high-level ObjectManager for the ordinary CRUD calls, and the
// lower-level Connector both the identity ladder and Update's
// hand-built partial PUT need directly (see updateAliasRecord's doc
// comment for why the generated UpdateAliasRecord wrapper cannot be
// used).
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

// uint32OrZero converts an optional *uint32 TTL into a plain uint32 for
// comparison/marshaling. Unlike ARecord (whose ForProvider.TTL is an
// *int64 requiring range clamping), AliasRecord's ForProvider.TTL is
// already *uint32 — matching the SDK's Ttl field type directly.
func uint32OrZero(ttl *uint32) uint32 {
	if ttl == nil {
		return 0
	}
	return *ttl
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
// than a whole AliasRecordParameters value. The cluster and namespaced
// AliasRecordParameters types are structurally identical (same field
// names and primitive types) but are distinct named Go types, so a direct
// struct conversion between them is not always available once other
// resources in this provider grow reference fields; parameterizing on the
// field pointers instead lets both scopes share this logic
// unconditionally.

// isUpToDate compares the desired AliasRecord fields against the observed
// RecordAlias. View is soft-immutable (the WAPI schema advertises it as
// updatable, but a live update attempt is rejected at the data level —
// see the immutable-fields table) and is intentionally excluded from this
// comparison so drift there never triggers an infinite reconcile loop.
func isUpToDate(name, targetName, targetType, comment *string, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordAlias) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(targetName) != strOrEmpty(rec.TargetName) {
		return false
	}
	if strOrEmpty(targetType) != rec.TargetType {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
		return false
	}
	if boolOrFalse(disable) != boolOrFalse(rec.Disable) {
		return false
	}
	// Compare the flag first and unconditionally, so a true -> false
	// transition is still detected as drift.
	if boolOrFalse(useTTL) != boolOrFalse(rec.UseTtl) {
		return false
	}
	// Only compare the value when the flag is on. When it is off, WAPI
	// ignores the submitted ttl and returns the zone default on every
	// GET — comparing it against the spec value never converges.
	if boolOrFalse(useTTL) {
		if uint32OrZero(ttl) != uint32OrZero(rec.Ttl) {
			return false
		}
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rec.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// disable, ttl, useTtl, extAttrs) from the observed RecordAlias into spec
// so isUpToDate does not see phantom drift on the next reconcile.
// Required fields (name, targetName, targetType) and the immutable view
// field are never late-initialized — they are always user-supplied
// (required on the CRD). Returns true if any field was changed.
func lateInitialize(comment **string, disable **bool, ttl **uint32, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordAlias) bool {
	changed := false

	if *comment == nil && rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		*comment = &c
		changed = true
	}
	if *disable == nil && rec.Disable != nil {
		d := *rec.Disable
		*disable = &d
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

// observedAliasRecord holds the primitive field values extracted from a
// WAPI RecordAlias response. The cluster and namespaced
// AliasRecordObservation types are structurally similar but are distinct
// named types, so they are not directly convertible — each scope copies
// this intermediate struct's fields into its own Observation type at the
// call site.
type observedAliasRecord struct {
	ID         string
	Name       *string
	TargetName *string
	TargetType *string
	View       *string
	Comment    *string
	Disable    *bool
	TTL        *uint32
	UseTTL     *bool
	ExtAttrs   map[string]string
	Ref        *string
	Zone       *string
}

// observeFromRecordAlias extracts the fields mirrored by
// AliasRecordObservation (the full-mirror AtProvider convention) from a
// WAPI RecordAlias response. NewEmptyAliasRecord requests extattrs,
// cloud_info, comment, disable, dns_name, dns_target_name, creator, ttl,
// use_ttl, view, and zone in addition to the default name/target_name/
// target_type/view fields — response-only fields outside that set
// (creator, cloud_info, aws_rte53_record_info, etc.) are not mirrored
// into AtProvider by this provider.
func observeFromRecordAlias(externalID string, rec *ibclient.RecordAlias) observedAliasRecord {
	o := observedAliasRecord{
		ID:         externalID,
		Name:       rec.Name,
		TargetName: rec.TargetName,
		ExtAttrs:   extAttrsFromEA(rec.Ea),
	}
	if rec.TargetType != "" {
		tt := rec.TargetType
		o.TargetType = &tt
	}
	if rec.View != nil && *rec.View != "" {
		v := *rec.View
		o.View = &v
	}
	if rec.Comment != nil && *rec.Comment != "" {
		c := *rec.Comment
		o.Comment = &c
	}
	if rec.Disable != nil {
		d := *rec.Disable
		o.Disable = &d
	}
	if rec.Ttl != nil {
		t := *rec.Ttl
		o.TTL = &t
	}
	if rec.UseTtl != nil {
		u := *rec.UseTtl
		o.UseTTL = &u
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

// createAliasRecord issues the WAPI create call, stamping the owning
// managed resource's uid into the object's extensible attributes in the
// same request that creates it (identity.Stamp).
func createAliasRecord(objMgr ibclient.IBObjectManager, name, view, targetName, targetType, comment *string, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string, uid string) (*ibclient.RecordAlias, error) {
	if uid == "" {
		return nil, errors.New(errEmptyUID)
	}
	ea := identity.Stamp(buildEA(extAttrs), uid)
	return objMgr.CreateAliasRecord(
		strOrEmpty(name),
		strOrEmpty(view),
		strOrEmpty(targetName),
		strOrEmpty(targetType),
		strOrEmpty(comment),
		boolOrFalse(disable),
		ea,
		uint32OrZero(ttl),
		boolOrFalse(useTTL),
	)
}

// updateAliasRecord issues a partial WAPI PUT against the record's _ref,
// bypassing the generated ObjectManager.UpdateAliasRecord wrapper.
//
// Unlike ARecord's UpdateARecord (which has no view parameter at all,
// naturally excluding it from the wire body), UpdateAliasRecord always
// takes a dnsView argument and unconditionally marshals it as "view" on
// every PUT. The immutable-fields contract requires immutable fields to
// never appear in the update request body, even when the API would
// silently ignore them, so the generated wrapper cannot be used here.
// Building the RecordAlias payload by hand (leaving View nil, which the
// `omitempty` JSON tag then drops) and issuing the PUT directly through
// the IBConnector this ObjectManager wraps is the only way to honor that
// contract with this SDK. Every call re-asserts the identity stamp
// (identity.Stamp) — a WAPI PUT carrying extattrs replaces the whole
// map, not a per-key merge.
func updateAliasRecord(conn ibclient.IBConnector, ref string, name, targetName, targetType, comment *string, disable *bool, ttl *uint32, useTTL *bool, extAttrs map[string]string, uid string) (string, error) {
	if uid == "" {
		return "", errors.New(errEmptyUID)
	}

	n := strOrEmpty(name)
	tn := strOrEmpty(targetName)
	c := strOrEmpty(comment)
	d := boolOrFalse(disable)
	t := uint32OrZero(ttl)
	u := boolOrFalse(useTTL)
	ea := identity.Stamp(buildEA(extAttrs), uid)

	rec := &ibclient.RecordAlias{
		Name:       &n,
		TargetName: &tn,
		TargetType: strOrEmpty(targetType),
		Comment:    &c,
		Disable:    &d,
		Ttl:        &t,
		UseTtl:     &u,
		Ea:         ea,
	}

	return conn.UpdateObject(rec, ref)
}

// deleteAliasRecord issues the WAPI delete call.
func deleteAliasRecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteAliasRecord(ref)
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

// resolveAliasRecordIdentity resolves through the shared UID-in-EA
// ladder. The newEmpty constructor is ibclient.NewEmptyAliasRecord — NOT
// the naming-convention-matching NewEmptyRecordAlias, which does not
// exist in this SDK.
func resolveAliasRecordIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.RecordAlias, identity.Outcome, error) {
	return identity.Resolve[*ibclient.RecordAlias](ctx, conn, ibclient.NewEmptyAliasRecord, ref, uid)
}

type observeResult struct {
	exists       bool
	rec          *ibclient.RecordAlias
	obs          observedAliasRecord
	lateInit     bool
	refreshedRef string
	adopted      bool
}

func observeAliasRecord(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, comment **string, disable **bool, ttl **uint32, useTTL **bool, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveAliasRecordIdentity(ctx, conn, ref, uid)
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
		obs:     observeFromRecordAlias(rec.Ref, rec),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(comment, disable, ttl, useTTL, extAttrs, rec)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
		res.lateInit = true
	}

	return res, nil
}

func deleteAliasRecordIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveAliasRecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteAliasRecord)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteAliasRecord(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteAliasRecord)
	default:
		return errors.New("identity: unresolved AliasRecord outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced AliasRecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterAliasRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster AliasRecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("AliasRecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedAliasRecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced AliasRecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("AliasRecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced AliasRecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterAliasRecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedAliasRecord(mgr, o)
}
