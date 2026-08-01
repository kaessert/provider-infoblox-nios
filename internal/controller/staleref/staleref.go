// Package staleref centralizes the one behavior every resource's Delete()
// must share: a 404 on the stored external-name (a WAPI _ref) is evidence
// that the handle is stale, never evidence that the object is gone.
//
// The _ref this provider stores as the external-name annotation is a
// derived handle — a rendering of the object's own identity fields, not an
// opaque, backend-assigned ID (see the object identity decision in this
// provider's design history). Any update that changes an identity-composing
// field rotates the handle: WAPI computes a different address for the same
// object, and the old address 404s immediately even though the object is
// unchanged and very much alive. A Delete() that treats that 404 as success
// silently orphans the object on the Grid while reporting the deletion as
// having succeeded.
//
// Every Delete() implementation in this provider therefore follows the same
// two-step resolution before it may conclude an object is gone:
//
//  1. Issue the delete against the stored _ref.
//  2. On 404, resolve the object's natural key (the same identity fields
//     WAPI uses to compute the _ref) against the backend:
//     - no match  -> the object is genuinely absent; report success.
//     - any match -> the handle is merely stale, and the live object's
//     ownership cannot be verified from a natural-key match alone
//     (a match could belong to a different resource entirely). Refuse
//     the delete rather than risk destroying someone else's object.
//
// Observe() must apply the identical resolution before it may report a
// resource as gone: crossplane-runtime's managed reconciler calls Observe()
// first on the deletion path, and if Observe() reports ResourceExists:
// false it never calls Delete() at all — it just unpublishes the managed
// resource and clears the finalizer. A stale-_ref 404 in Observe()
// therefore bypasses every Delete() safeguard above and orphans the
// backend object with no error, no event, and no trace that Delete() was
// ever skipped. Observe() follows the same two-step resolution, with the
// same fail-closed answer on the second step: a natural-key match cannot
// be silently adopted (bound to this managed resource's external name)
// because ownership is unverifiable, so it must refuse rather than
// report the resource as either gone or healthy.
//
// RefusalError is the error every resource's Delete() returns for a
// natural-key match. ObserveRefusalError is the equivalent for Observe().
package staleref

import "github.com/crossplane/crossplane-runtime/v2/pkg/errors"

// refusalMessage explains, to the operator reading the ReconcileError
// condition or the CannotDeleteExternalResource event, exactly what
// happened and what to do about it. It intentionally does not name a
// match count: most of this provider's natural-key searches confirm
// "does at least one live object share this identity" without counting
// beyond one, and the operator's remedy is identical either way.
const refusalMessage = "refusing to delete: stored _ref is stale (404) and an object matching this resource's identity still exists on the Grid. " +
	"Deleting it cannot be proven safe because ownership is unverifiable. " +
	"Reconcile the external-name annotation, or remove the object on the Grid manually, then retry. " +
	"To abandon the object without deleting it, remove the finalizer."

// RefusalError returns the error a Delete() implementation must return
// when the stored _ref 404s but a natural-key search finds a live object
// that could be the same one. Returning this error (rather than nil)
// keeps crossplane-runtime's finalizer in place, surfaces
// ReconcileError/CannotDeleteExternalResource to the operator, and makes
// no mutating call against the backend.
func RefusalError() error {
	return errors.New(refusalMessage)
}

// observeRefusalMessage mirrors refusalMessage for the Observe() path. It
// deliberately does not say "deleting" — Observe() runs on every
// reconcile, not just deletion — and it warns explicitly against
// auto-adopting the match, since Observe() (unlike Delete()) could
// otherwise be tempted to silently rebind the managed resource's identity
// to whatever the search happened to find.
const observeRefusalMessage = "cannot observe: stored external-name (_ref) is stale (404) and an object matching this resource's identity still exists on the Grid. " +
	"Binding this managed resource to it automatically is not safe because ownership is unverifiable. " +
	"Reconcile the external-name annotation to the live _ref, or remove the object on the Grid manually, then retry. " +
	"To abandon the object without deleting it, remove the finalizer."

// ObserveRefusalError returns the error an Observe() implementation must
// return when the stored external-name 404s but a natural-key search
// finds a live object that could be the same one. Returning this error
// (rather than ExternalObservation{ResourceExists: false}) keeps
// crossplane-runtime's finalizer in place — so the deletion path never
// silently unpublishes the managed resource while a matching object is
// still live on the Grid — and surfaces ReconcileError to the operator
// instead of guessing which object this managed resource now owns.
func ObserveRefusalError() error {
	return errors.New(observeRefusalMessage)
}
