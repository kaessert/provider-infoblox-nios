// Package externalname provides a helper for persisting a rotated
// external-name annotation to the Kubernetes API server immediately,
// from inside an ExternalClient's Update method.
//
// crossplane-runtime's managed reconciler only persists the status
// subresource after a successful external Update() call — the
// post-Update success path calls only the status client, never a plain
// object update. Any change an ExternalClient makes to the managed
// resource's metadata (including the external-name annotation) inside
// Update is therefore silently discarded once that reconcile ends: the
// very next Observe re-fetches the object from the API server and sees
// the OLD annotation.
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

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Refresh sets the external-name annotation on cr to newName and
// immediately persists that change to the API server via kube.Update.
//
// Call this from Update() whenever the backend returns a different
// identity/reference than the one used to issue the request. A plain
// object update only touches spec/metadata — Kubernetes ignores any
// status field changes in the request body when the CRD has the status
// subresource enabled (true for every managed resource this provider
// defines) — so this cannot race with the reconciler's own status
// update later in the same reconcile.
func Refresh(ctx context.Context, kube client.Client, cr client.Object, newName string) error {
	meta.SetExternalName(cr, newName)
	return kube.Update(ctx, cr)
}
