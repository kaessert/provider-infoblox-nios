// Package recordaaaa implements the Crossplane controller for the
// Infoblox NIOS AAAARecord managed resource. Like recorda (its IPv4
// counterpart), this provider wraps the official infoblox-go-client Go SDK
// directly — the SDK's ObjectManager exposes typed CRUD methods
// (CreateAAAARecord/GetAAAARecordByRef/UpdateAAAARecord/DeleteAAAARecord)
// instead of a generic HTTP request/response envelope, so there is no
// internal REST client to compose.
//
// AAAARecord is wired to the UID-in-EA object-identity ladder (see
// recorda's package doc for the full rationale): the WAPI _ref this
// resource's create call returns is a derived handle, not a stable
// backend-assigned ID, so this controller stamps the managed resource's
// own metadata.uid onto the Grid object as an extensible attribute and
// resolves every Observe/Delete through the shared identity.Resolve
// ladder instead of trusting the stored _ref alone.
//
// Dual-scope: cluster-scoped (cluster.go) and namespaced (namespaced.go).
// Shared SDK plumbing, field comparison, and late-init logic lives here.
package recordaaaa

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

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/recordaaaa/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/recordaaaa/v1alpha1"
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
	errObserveAAAARecord         = "cannot observe AAAARecord"
	errCreateAAAARecord          = "cannot create AAAARecord"
	errUpdateAAAARecord          = "cannot update AAAARecord"
	errDeleteAAAARecord          = "cannot delete AAAARecord"
	errCidrIPv6Mutex             = "cidr and ipv6Addr are mutually exclusive"
	errEmptyUID                  = "cannot stamp AAAARecord identity: managed resource's metadata.uid is empty"
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

// ttlOrZero converts an optional *uint32 TTL into the uint32 the SDK
// expects, treating nil as 0.
func ttlOrZero(ttl *uint32) uint32 {
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
// than a whole AAAARecordParameters value. The cluster and namespaced
// AAAARecordParameters types are structurally identical (same field names
// and primitive types) but are distinct named Go types, so a direct struct
// conversion between them is not always available once other resources in
// this provider grow reference fields; parameterizing on the field
// pointers instead lets both scopes share this logic unconditionally.

// isUpToDate compares the desired AAAARecord fields against the observed
// RecordAAAA. View is immutable (WAPI ties the object's _ref to
// view+zone+name; the UpdateAAAARecord SDK method has no view parameter)
// and RemoveAssociatedPtr is write-only (delete-time option, never
// present in a GET response) — both are intentionally excluded from this
// comparison. The Grid's extattrs map is compared with the provider's
// own identity stamp stripped out (identity.Strip): the CRD schema never
// includes that reserved key, so leaving it in would produce a permanent
// phantom diff.
func isUpToDate(name, ipv6Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, rec *ibclient.RecordAAAA) bool {
	if strOrEmpty(name) != strOrEmpty(rec.Name) {
		return false
	}
	if strOrEmpty(ipv6Addr) != strOrEmpty(rec.Ipv6Addr) {
		return false
	}
	if strOrEmpty(comment) != strOrEmpty(rec.Comment) {
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
		if ttlOrZero(ttl) != ttlOrZero(rec.Ttl) {
			return false
		}
	}
	return extAttrsEqual(extAttrs, extAttrsFromEA(identity.Strip(rec.Ea)))
}

// lateInitialize back-fills server-defaulted optional fields (comment,
// ttl, useTtl, extAttrs) from the observed RecordAAAA into spec so
// isUpToDate does not see phantom drift on the next reconcile. Required
// fields (name, ipv6Addr) and the immutable view field are never
// late-initialized — view is always user-supplied (required on the CRD)
// and name/ipv6Addr are always user-supplied too. extAttrs is
// back-filled with the provider's own identity stamp stripped out
// (identity.Strip) — the CRD schema never includes that reserved key, so
// late-initializing it into spec.forProvider would fail CEL validation
// and produce a permanent diff. Returns true if any field was changed.
func lateInitialize(comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string, rec *ibclient.RecordAAAA) bool {
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

// observedAAAARecord holds the primitive field values extracted from a
// WAPI RecordAAAA response. The cluster and namespaced
// AAAARecordObservation types are structurally similar but are distinct
// named types with distinct nested-struct field types (e.g.
// *AAAARecordCloudInfo), so they are not directly convertible — each
// scope copies this intermediate struct's fields into its own
// Observation type at the call site.
type observedAAAARecord struct {
	ID       string
	Name     *string
	IPv6Addr *string
	Comment  *string
	TTL      *uint32
	UseTTL   *bool
	ExtAttrs map[string]string
	View     *string
	Ref      *string
	Zone     *string
}

// observeFromRecordAAAA extracts the fields mirrored by
// AAAARecordObservation (the full-mirror AtProvider convention) from a
// WAPI RecordAAAA response. The SDK's GetAAAARecordByRef method requests
// only a fixed subset of fields by default (extattrs, ipv6addr, name,
// view, zone, comment, ttl, use_ttl); response-only fields outside that
// set (creator, discovered_data, cloud_info, etc.) are not requested by
// this provider and are left at their zero value in AtProvider.
func observeFromRecordAAAA(externalID string, rec *ibclient.RecordAAAA) observedAAAARecord {
	o := observedAAAARecord{
		ID:       externalID,
		Name:     rec.Name,
		IPv6Addr: rec.Ipv6Addr,
		ExtAttrs: extAttrsFromEA(rec.Ea),
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
	if rec.View != "" {
		v := rec.View
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

// validateAAAARecordCreateInputs performs the local, network-free sanity
// checks a create call requires: uid must be set (identity.Stamp cannot
// stamp an empty value), and cidr/ipv6Addr are mutually exclusive.
func validateAAAARecordCreateInputs(ipv6Addr, cidr *string, uid string) error {
	if strings.TrimSpace(uid) == "" {
		return errors.New(errEmptyUID)
	}
	if strOrEmpty(cidr) != "" && strOrEmpty(ipv6Addr) != "" {
		return errors.New(errCidrIPv6Mutex)
	}
	return nil
}

// defaultNetworkView is the NIOS Grid's built-in network view name, used
// when a next-available-IP create call is issued without an explicit
// network view.
const defaultNetworkView = "default"

// createAAAARecord issues the WAPI create call, stamping the owning
// managed resource's uid into the object's extensible attributes in the
// same request that creates it (identity.Stamp) — there is no follow-up
// call, so there is no window in which the object exists without its
// identity stamp. When cidr is set, the WAPI dynamically allocates the
// next available IPv6 address from the given network view
// (func:nextavailableip) instead of using a caller-supplied static
// address — cidr and ipv6Addr are mutually exclusive, enforced above
// before the SDK call is issued. CreateAAAARecord already defaults an
// empty network view to defaultNetworkView internally; this wrapper
// applies the same default explicitly for consistency with createARecord
// (whose SDK counterpart does not self-default).
func createAAAARecord(objMgr ibclient.IBObjectManager, name, view, ipv6Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, cidr, networkView *string, uid string) (*ibclient.RecordAAAA, error) {
	if err := validateAAAARecordCreateInputs(ipv6Addr, cidr, uid); err != nil {
		return nil, err
	}

	cidrVal := strOrEmpty(cidr)
	netView := strOrEmpty(networkView)
	if cidrVal != "" && netView == "" {
		netView = defaultNetworkView
	}

	ea := identity.Stamp(buildEA(extAttrs), uid)

	return objMgr.CreateAAAARecord(
		netView,
		strOrEmpty(view),
		strOrEmpty(name),
		cidrVal,
		strOrEmpty(ipv6Addr),
		boolOrFalse(useTTL),
		ttlOrZero(ttl),
		strOrEmpty(comment),
		ea,
	)
}

// updateAAAARecord issues the WAPI update call. view is intentionally
// never passed — UpdateAAAARecord has no view parameter (immutable
// field). cidr and netView are always empty, mirroring createAAAARecord.
//
// Every call re-asserts the identity stamp (identity.Stamp) in the
// extattrs it sends — a WAPI PUT carrying extattrs replaces the whole
// map, not a per-key merge (live-verified on the ARecord pilot), so
// omitting the stamp here would wipe it off the object on the first
// field update after create.
func updateAAAARecord(objMgr ibclient.IBObjectManager, ref string, name, ipv6Addr, comment *string, ttl *uint32, useTTL *bool, extAttrs map[string]string, uid string) (*ibclient.RecordAAAA, error) {
	if strings.TrimSpace(uid) == "" {
		return nil, errors.New(errEmptyUID)
	}

	ea := identity.Stamp(buildEA(extAttrs), uid)

	return objMgr.UpdateAAAARecord(
		ref,
		"", // netView — not exposed by this provider
		strOrEmpty(name),
		strOrEmpty(ipv6Addr),
		"", // cidr — not exposed by this provider
		boolOrFalse(useTTL),
		ttlOrZero(ttl),
		strOrEmpty(comment),
		ea,
	)
}

// deleteAAAARecord issues the WAPI delete call. removeAssociatedPtr is
// accepted as a ForProvider field for schema completeness, but the SDK's
// DeleteAAAARecord wrapper takes only the object reference — it exposes
// no query-parameter or request-body hook for the WAPI
// remove_associated_ptr delete option, so this provider cannot honor a
// user-set value. NIOS's default PTR-cleanup behavior applies on every
// delete regardless.
func deleteAAAARecord(objMgr ibclient.IBObjectManager, ref string) error {
	_, err := objMgr.DeleteAAAARecord(ref)
	return err
}

// ── Identity EA-definition prerequisite probe (shared by both scopes) ────

// ensureIdentityPrerequisite probes the Grid for the identity extensible
// attribute definition before any call that stamps identity onto a new
// object (identity.Stamp). A *identity.PrerequisiteError is returned
// verbatim — its Error() text is the operator-facing remediation — so the
// caller's Synced=False condition carries it directly. Any other error is
// wrapped like any other WAPI error and is retriable. See recorda's
// ensureIdentityPrerequisite for the full rationale (identical pattern,
// reused per package since each controller package is self-contained).
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
// first for a managed resource's stored external-name. When the
// annotation still holds the framework's NameAsExternalName default (the
// CR's own Kubernetes name) no real WAPI _ref has ever been assigned, so
// this reports "" rather than handing the ladder a value that can never
// resolve.
func observeRefFor(crName, externalName string) string {
	if externalName == crName {
		return ""
	}
	return externalName
}

// resolveAAAARecordIdentity resolves the AAAARecord identified by
// ref/uid through the shared UID-in-EA ladder — see recorda's
// resolveARecordIdentity for the full rationale.
func resolveAAAARecordIdentity(ctx context.Context, conn ibclient.IBConnector, ref, uid string) (*ibclient.RecordAAAA, identity.Outcome, error) {
	return identity.Resolve[*ibclient.RecordAAAA](ctx, conn, ibclient.NewEmptyRecordAAAA, ref, uid)
}

// observeResult bundles the shared parts of resolving and inspecting an
// AAAARecord through the identity ladder during Observe — common to both
// scopes, which differ only in their concrete CRD types.
type observeResult struct {
	exists       bool
	rec          *ibclient.RecordAAAA
	obs          observedAAAARecord
	lateInit     bool
	refreshedRef string
	adopted      bool
}

// observeAAAARecord runs the identity ladder for Observe and
// late-initializes the given ForProvider field pointers from the
// resolved object. See recorda's observeARecord for the full rationale
// (identical pattern).
func observeAAAARecord(ctx context.Context, conn ibclient.IBConnector, prober *identity.Prober, endpoint, crName, externalName, uid string, comment **string, ttl **uint32, useTTL **bool, extAttrs *map[string]string) (observeResult, error) {
	ref := observeRefFor(crName, externalName)

	rec, outcome, err := resolveAAAARecordIdentity(ctx, conn, ref, uid)
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
		obs:     observeFromRecordAAAA(rec.Ref, rec),
		adopted: outcome == identity.OutcomeAdopted,
	}
	res.lateInit = lateInitialize(comment, ttl, useTTL, extAttrs, rec)

	if outcome == identity.OutcomeRotated || outcome == identity.OutcomeFoundByUID {
		res.refreshedRef = rec.Ref
		res.lateInit = true
	}

	return res, nil
}

// deleteAAAARecordIdentity issues the WAPI delete for the AAAARecord this
// managed resource owns, resolving through the identity ladder first so
// a stale _ref is never mistaken for a deleted object. See recorda's
// deleteARecordIdentity for the full rationale (identical pattern).
func deleteAAAARecordIdentity(ctx context.Context, conn ibclient.IBConnector, objMgr ibclient.IBObjectManager, prober *identity.Prober, endpoint, ref, uid string) error {
	obj, outcome, err := resolveAAAARecordIdentity(ctx, conn, ref, uid)
	if err != nil {
		if identity.IsSearchFailure(err) {
			if prereqErr := ensureIdentityPrerequisite(ctx, prober, conn, endpoint); prereqErr != nil {
				return prereqErr
			}
		}
		return errors.Wrap(err, errDeleteAAAARecord)
	}

	switch outcome {
	case identity.OutcomeNotFound:
		return nil
	case identity.OutcomeAdopted:
		return errors.New(errDeleteUnverifiedOwnership)
	case identity.OutcomeResolved, identity.OutcomeRotated, identity.OutcomeFoundByUID:
		delErr := deleteAAAARecord(objMgr, obj.Ref)
		if delErr == nil {
			return nil
		}
		if isNotFound(delErr) {
			return nil
		}
		return errors.Wrap(delErr, errDeleteAAAARecord)
	default:
		return errors.New("identity: unresolved AAAARecord outcome")
	}
}

// ── SafeStart gate registration ─────────────────────────────────────────

// SetupGated registers both the cluster-scoped and namespaced AAAARecord
// controllers with the SafeStart gate. Each controller starts only after
// its respective CRD has been installed in the cluster.
//
// ⚠️ This function MUST call Gate.Register for both GVKs. If either
// registration is omitted, that scope's controller never starts —
// defeating SafeStart silently.
func SetupGated(mgr ctrl.Manager, o controller.Options) error {
	o.Gate.Register(func() {
		if err := setupClusterAAAARecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup cluster AAAARecord controller"))
		}
	}, clusterv1alpha1.SchemeGroupVersion.WithKind("AAAARecord"))

	o.Gate.Register(func() {
		if err := setupNamespacedAAAARecord(mgr, o); err != nil {
			panic(errors.Wrap(err, "cannot setup namespaced AAAARecord controller"))
		}
	}, namespacedv1alpha1.SchemeGroupVersion.WithKind("AAAARecord"))

	return nil
}

// Setup starts both the cluster-scoped and namespaced AAAARecord
// controllers immediately without SafeStart gating (RBAC fallback path,
// for environments that pre-install CRDs before the provider starts).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterAAAARecord(mgr, o); err != nil {
		return err
	}
	return setupNamespacedAAAARecord(mgr, o)
}
