// Package externalname provides a helper for persisting a rotated
// external-name annotation to the Kubernetes API server immediately,
// from inside an ExternalClient's Update method.
//
// crossplane-runtime's managed reconciler only persists metadata changes
// (including the external-name annotation) made during Update() when the
// reconciler happens to hit its own late-initialize Update() call later
// in the same run — it is not guaranteed on every path. Any change an
// ExternalClient makes to the managed resource's metadata inside Update
// can therefore be silently discarded once that reconcile ends: the very
// next Observe re-fetches the object from the API server and sees the
// OLD annotation.
//
// This matters for any backend whose object identity is embedded in a
// server-assigned reference that rotates whenever certain fields
// change. When Update detects that the backend minted a new reference
// for the same object, it must persist the refreshed external name
// right away — waiting for the reconciler's normal flush loses the
// mapping, and the next reconcile 404s against the stale reference.
// That can make the controller believe the resource no longer exists,
// attempt to (re)create it, and orphan the original object.
package externalname

import (
	"context"
	"encoding/json"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// errBuildAnnotationPatch is returned when the JSON merge patch document
// cannot be marshalled. This should never happen in practice — the
// document is a fixed shape with a plain string value — but is handled
// explicitly rather than ignored.
const errBuildAnnotationPatch = "cannot build external-name annotation patch"

// Refresh sets the external-name annotation on cr to newName and
// immediately persists that change to the API server.
//
// It persists the change via a JSON merge patch scoped to
// metadata.annotations rather than a whole-object Update. A merge patch
// carries no resourceVersion precondition on the fields it doesn't
// mention, so — unlike a whole-object Update — it is not rejected just
// because some other field (spec, status, an unrelated annotation) was
// changed by another writer since cr was read. That other writer is
// exactly what happened in production: an update-tester patching
// spec.forProvider concurrently with the reconciler's own external-name
// rotation caused a plain Update to lose an optimistic-concurrency race
// with a 409, and the response to that 409 was to give up — dropping the
// only copy of the backend's newly rotated reference.
//
// The patch call is additionally wrapped in a bounded conflict-retry
// loop. A merge patch does not carry the stale-version failure mode a
// whole-object Update does, but retrying on conflict costs nothing and
// protects against any other layer (an admission webhook, a CRD
// conversion path, a future refactor back to Update) reintroducing one.
//
// Losing this write is unrecoverable: the external Update call has
// already succeeded and the backend has already rotated its reference,
// so the new reference exists nowhere else. If it isn't persisted here,
// the next Observe() looks up the OLD reference, gets a 404, believes
// the resource doesn't exist, and — depending on the resource — either
// recreates it (colliding with the still-live original) or treats a
// subsequent Delete as already-done and orphans it.
//
// Call this from Update() whenever the backend returns a different
// identity/reference than the one used to issue the request.
func Refresh(ctx context.Context, kube client.Client, cr client.Object, newName string) error {
	patch, err := annotationMergePatch(newName)
	if err != nil {
		return errors.Wrap(err, errBuildAnnotationPatch)
	}

	rawPatch := client.RawPatch(types.MergePatchType, patch)

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return kube.Patch(ctx, cr, rawPatch)
	}); err != nil {
		return err
	}

	// Reflect the change on the in-memory object too, so callers that
	// keep using cr later in the same reconcile (e.g. to read the
	// external name back) see the refreshed value without needing a
	// fresh Get.
	meta.SetExternalName(cr, newName)
	return nil
}

// annotationMergePatch builds a JSON merge patch document that sets only
// the external-name annotation, leaving every other field (spec, status,
// other annotations/labels) untouched. JSON merge patch semantics merge
// maps key by key: keys present in the document overwrite the
// corresponding key on the server, keys absent from the document are
// left alone.
func annotationMergePatch(newName string) ([]byte, error) {
	doc := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				meta.AnnotationKeyExternalName: newName,
			},
		},
	}
	return json.Marshal(doc)
}
