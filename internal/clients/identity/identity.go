// Package identity implements the object-identity resolution ladder for
// NIOS objects whose WAPI reference (_ref) is a derived handle — a
// rendering of the object's own identity fields, not a stable opaque ID.
// Mutating a field that participates in identity does not rotate the
// handle; WAPI computes a different address for the same object, and the
// previous _ref 404s immediately.
//
// Because the only identifier WAPI offers is a function of mutable spec
// fields, this package treats the managed resource's own metadata.uid as
// the durable identity, stamped onto the Grid object as an Extensible
// Attribute in the same call that creates it. The stored external-name
// annotation is demoted to a cache: useful, expected to be correct, and
// never trusted as the sole record of which Grid object a resource owns.
//
// Every Observe (and, transitively, Delete — a 404 on a stale handle is
// never proof the backend object is gone) resolves the object through
// Resolve, the single shared implementation of the ladder. One generic
// implementation replaces the ~22 bespoke per-resource natural-key
// fallbacks that could not distinguish "the object this managed resource
// created" from "an unrelated object with the same field values" — see
// the identity decision this package implements for the analysis that
// ruled natural-key search out.
//
// This package deliberately does not delegate to the vendored SDK's own
// alt-ID search (ObjectManager.SearchObjectByAltId): at the pinned SDK
// revision it silently takes the first match on ambiguity, treats a
// blank internal ID as "accept whatever the ref resolved to", and cannot
// distinguish a UID mismatch (handle reuse) from a 404. All three are
// exactly the failure modes this ladder exists to close, so Resolve
// issues the search itself through the connector, where the match count
// is visible.
package identity

import (
	"context"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/go-logr/logr"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
)

// EAKey is the name of the Extensible Attribute this provider stamps on
// every NIOS object it creates, carrying the owning managed resource's
// metadata.uid. It is reserved: no other field on any cataloged resource
// may reuse this name, and it is always stripped from a Grid object's
// extattrs before comparing them against the CRD's user-facing extAttrs
// field (see Strip).
const EAKey = "Crossplane Internal ID"

// Error message constants. All errors are constructed with
// github.com/crossplane/crossplane-runtime/v2/pkg/errors, never fmt.Errorf.
const (
	errEmptyUID       = "identity: metadata.uid is empty or blank — refusing to resolve or stamp an object without a stable identity to verify ownership against"
	errGetByRef       = "identity: cannot fetch object by its stored reference"
	errSearchByUID    = "identity: cannot search for the object by its stamped identity extensible attribute"
	errNotAPointerFmt = "identity: %T is not a non-nil pointer to a generated NIOS object — Resolve requires the SDK's usual *T shape"
	errNoRefFieldFmt  = "identity: %T has no string Ref field — this object type does not follow the Ref/Ea convention every cataloged NIOS type currently uses, and Resolve cannot verify its identity without one; add explicit field accessors for this type instead of silently skipping identity verification"
	errNoEAFieldFmt   = "identity: %T has no ibclient.EA-typed Ea field — this object type does not follow the Ref/Ea convention every cataloged NIOS type currently uses, and Resolve cannot verify its identity without one; add explicit field accessors for this type instead of silently skipping identity verification"
)

// errStatusRe extracts the HTTP status code from the SDK's generic
// formatted error ("WAPI request error: <status>('<text>')\n..."). The
// SDK wraps HTTP errors this way for everything except object-not-found,
// which it surfaces as a typed *ibclient.NotFoundError instead.
var errStatusRe = regexp.MustCompile(`WAPI request error: (\d{3})`)

// isNotFound reports whether err indicates the WAPI object does not
// exist (HTTP 404), including the empty-result-set case a search returns
// when nothing matches.
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

// Outcome classifies which row of the ADR-IN-0006 §2/§3 resolution
// ladder Resolve took. Combined with the two typed refusal errors
// (HandleReuseError, AmbiguousMatchError), every row of the ladder is
// independently distinguishable by the caller.
type Outcome int

const (
	// OutcomeResolved: the stored reference resolved and its identity EA
	// matches the given uid. The normal, steady-state path.
	OutcomeResolved Outcome = iota
	// OutcomeAdopted: the stored reference resolved but the object
	// carries no identity EA at all. The caller owns it (a bare 404 or
	// a mismatch are the only refusal conditions) but must re-stamp the
	// EA on the object's next mutating call — Resolve never mutates the
	// backend as a side effect of resolving it.
	OutcomeAdopted
	// OutcomeRotated: the stored reference 404'd, but exactly one object
	// matched an identity-EA search for uid. The handle rotated because
	// a field that participates in the object's identity changed. The
	// caller must persist the returned object's new reference somewhere
	// actually persisted — see the external-name convention this
	// package supports (Observe + ResourceLateInitialized, or an
	// explicit conflict-safe patch from Update).
	OutcomeRotated
	// OutcomeFoundByUID: no reference was supplied at all (ref == ""),
	// and exactly one object matched an identity-EA search for uid. This
	// closes the create-crash window: the object is locatable from the
	// managed resource's own uid with zero prior state, even when the
	// external-name annotation was never persisted.
	OutcomeFoundByUID
	// OutcomeNotFound: the reference (if any) 404'd and the identity-EA
	// search matched nothing. The object genuinely does not exist.
	OutcomeNotFound
)

// String renders the Outcome for logs and test failure messages.
func (o Outcome) String() string {
	switch o {
	case OutcomeResolved:
		return "Resolved"
	case OutcomeAdopted:
		return "Adopted"
	case OutcomeRotated:
		return "Rotated"
	case OutcomeFoundByUID:
		return "FoundByUID"
	case OutcomeNotFound:
		return "NotFound"
	default:
		return "Unknown"
	}
}

// Resolve implements the full ADR-IN-0006 §2 (and §3) identity resolution
// ladder for a single NIOS object type T — a pointer to a generated SDK
// struct such as *ibclient.RecordA. One generic implementation serves
// every rotating-identifier resource in the catalog instead of ~22
// bespoke natural-key fallbacks, each of which would have to reinvent
// ownership verification.
//
// newEmpty must return a fresh, empty T on every call (the same value
// the SDK's NewEmpty<Type> constructors return) — Resolve issues more
// than one request and must not reuse a single instance across them.
//
// ref is the object's cached reference (typically the value stored in
// the crossplane.io/external-name annotation); pass "" when no reference
// is known yet, which is always safe and triggers the §3 uid-only path.
//
// uid is the owning managed resource's metadata.uid. It is mandatory: a
// blank uid is a hard configuration error, never license to accept
// whatever the reference resolves to.
//
// The returned Outcome and error together are exhaustive over every row
// of the ladder — see the Outcome constants and HandleReuseError /
// AmbiguousMatchError for the refusal rows.
func Resolve[T ibclient.IBObject](ctx context.Context, conn ibclient.IBConnector, newEmpty func() T, ref, uid string) (T, Outcome, error) {
	var zero T

	uid = strings.TrimSpace(uid)
	if uid == "" {
		return zero, OutcomeNotFound, errors.New(errEmptyUID)
	}

	// Fail loudly, before any network call, if T does not follow the
	// Ref/Ea shape every cataloged NIOS type currently uses. Silently
	// skipping identity verification for an unsupported type is exactly
	// the defect this package exists to prevent.
	probe := newEmpty()
	if _, err := getRef(probe); err != nil {
		return zero, OutcomeNotFound, err
	}
	if _, err := getEA(probe); err != nil {
		return zero, OutcomeNotFound, err
	}

	objType := probe.ObjectType()
	log := ctrllog.FromContext(ctx)

	if ref != "" {
		obj, outcome, err, handled := resolveByRef[T](conn, newEmpty, objType, ref, uid, log)
		if handled {
			return obj, outcome, err
		}
		// Reference 404'd — fall through to the identity-EA search
		// below. Recovering via rotation, not a fresh not-found.
	}

	matches, err := searchByUID(conn, newEmpty, uid)
	if err != nil {
		return zero, OutcomeNotFound, errors.Wrap(err, errSearchByUID)
	}

	switch len(matches) {
	case 0:
		return zero, OutcomeNotFound, nil
	case 1:
		found := matches[0]
		if ref == "" {
			log.V(1).Info("identity: located object by stamped identity attribute with no prior reference",
				"objectType", objType, "uid", uid)
			recordFoundByUID(objType)
			return found, OutcomeFoundByUID, nil
		}
		log.V(1).Info("identity: stored reference 404'd, recovered object via stamped identity attribute (handle rotated)",
			"objectType", objType, "staleRef", ref, "uid", uid)
		recordRotated(objType)
		return found, OutcomeRotated, nil
	default:
		return zero, OutcomeNotFound, &AmbiguousMatchError{ObjectType: objType, UID: uid, Count: len(matches)}
	}
}

// resolveByRef fetches the object at ref and, when the fetch succeeds,
// determines its outcome by comparing the stored identity EA against
// uid. handled reports whether the caller should return outcome/err
// directly (true) or fall through to the identity-EA search (false,
// which only happens on a 404 at ref).
func resolveByRef[T ibclient.IBObject](conn ibclient.IBConnector, newEmpty func() T, objType, ref, uid string, log logr.Logger) (obj T, outcome Outcome, err error, handled bool) {
	var zero T

	fetched := newEmpty()
	getErr := conn.GetObject(fetched, ref, ibclient.NewQueryParams(false, nil), &fetched)
	if getErr != nil {
		if isNotFound(getErr) {
			return zero, OutcomeNotFound, nil, false
		}
		return zero, OutcomeNotFound, errors.Wrap(getErr, errGetByRef), true
	}

	ea, eaErr := getEA(fetched)
	if eaErr != nil {
		return zero, OutcomeNotFound, eaErr, true
	}

	got, has := eaValue(ea, EAKey)
	switch {
	case !has:
		log.V(1).Info("identity: object resolved by reference but carries no stamped identity attribute; adopting",
			"objectType", objType, "ref", ref, "uid", uid)
		recordAdopted(objType)
		return fetched, OutcomeAdopted, nil, true
	case got == uid:
		return fetched, OutcomeResolved, nil, true
	default:
		return zero, OutcomeNotFound, &HandleReuseError{ObjectType: objType, Ref: ref, WantUID: uid, GotUID: got}, true
	}
}

// searchByUID issues the identity-EA search (ADR-IN-0006 §3): every
// object whose EAKey attribute equals uid. An empty result set is
// reported as zero matches, not an error — the SDK connector surfaces a
// no-results search as a NotFoundError.
func searchByUID[T ibclient.IBObject](conn ibclient.IBConnector, newEmpty func() T, uid string) ([]T, error) {
	var list []T
	qp := ibclient.NewQueryParams(false, map[string]string{"*" + EAKey: uid})
	err := conn.GetObject(newEmpty(), "", qp, &list)
	if err != nil {
		if isNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return list, nil
}

// getRef reads the Ref field off a generated SDK object via reflection.
// Every cataloged NIOS type embeds `Ref string` — see the package doc —
// so this works uniformly across all of them without a per-type registry.
// An object type that does not follow that shape is a hard error, never
// a silently-skipped identity check.
func getRef(obj any) (string, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return "", errors.Errorf(errNotAPointerFmt, obj)
	}
	f := v.Elem().FieldByName("Ref")
	if !f.IsValid() || f.Kind() != reflect.String {
		return "", errors.Errorf(errNoRefFieldFmt, obj)
	}
	return f.String(), nil
}

// getEA reads the Ea field off a generated SDK object via reflection.
// See getRef for why reflection is used instead of a type registry.
func getEA(obj any) (ibclient.EA, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return nil, errors.Errorf(errNotAPointerFmt, obj)
	}
	f := v.Elem().FieldByName("Ea")
	if !f.IsValid() {
		return nil, errors.Errorf(errNoEAFieldFmt, obj)
	}
	ea, ok := f.Interface().(ibclient.EA)
	if !ok {
		return nil, errors.Errorf(errNoEAFieldFmt, obj)
	}
	return ea, nil
}

// eaValue reads a single key out of an EA map, distinguishing "absent"
// from "present with a non-string value" (both report has == false —
// this package only ever stamps EAKey as a plain string, so any other
// shape means the attribute was tampered with or collided, which must be
// treated the same as absent rather than silently coerced).
func eaValue(ea ibclient.EA, key string) (value string, has bool) {
	raw, ok := ea[key]
	if !ok || raw == nil {
		return "", false
	}
	s, ok := raw.(string)
	if !ok {
		return "", false
	}
	return s, true
}

// Stamp returns a copy of ea with the identity attribute set to uid. Pass
// the result to the SDK's Create/Update call so the stamp is written in
// the same request that creates or mutates the object — atomic with the
// mutation, never a separate follow-up call that could leave a window
// where the object exists unstamped.
func Stamp(ea ibclient.EA, uid string) ibclient.EA {
	out := make(ibclient.EA, len(ea)+1)
	for k, v := range ea {
		out[k] = v
	}
	out[EAKey] = uid
	return out
}

// Strip returns a copy of ea with the identity attribute removed, for
// comparing a Grid object's extattrs against the CRD's user-facing
// extAttrs field during isUpToDate / lateInitialize — the CRD schema
// never includes the internal identity key, so leaving it in would
// produce a permanent phantom diff.
func Strip(ea ibclient.EA) ibclient.EA {
	if len(ea) == 0 {
		return ea
	}
	out := make(ibclient.EA, len(ea))
	for k, v := range ea {
		if k == EAKey {
			continue
		}
		out[k] = v
	}
	return out
}

// ManagerAndConnector bundles the SDK's high-level ObjectManager together
// with the low-level IBConnector it wraps. ibclient.NewObjectManager
// takes a Connector and discards the caller's reference to it —
// ObjectManager.connector is unexported with no accessor — so any caller
// that also needs identity resolution (which operates on the connector
// directly, since it needs visibility into search match counts that
// ObjectManager's higher-level methods hide) must keep both handles from
// the same construction call.
type ManagerAndConnector struct {
	Manager   ibclient.IBObjectManager
	Connector ibclient.IBConnector
}

// NewManagerAndConnector builds an ObjectManager around conn and returns
// both. Every one of this provider's per-resource newObjectManager
// functions builds a Connector and returns only
// ibclient.NewObjectManager(conn, "", ""), dropping conn on the same
// line. Adopting this helper is a one-line change at each of those call
// sites — replace that return with NewManagerAndConnector(conn) — rather
// than a rewrite of the surrounding credential/TLS plumbing.
func NewManagerAndConnector(conn ibclient.IBConnector) ManagerAndConnector {
	return ManagerAndConnector{
		Manager:   ibclient.NewObjectManager(conn, "", ""),
		Connector: conn,
	}
}
