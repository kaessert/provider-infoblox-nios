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
// RefusalError is the error every resource returns for the second branch.
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
