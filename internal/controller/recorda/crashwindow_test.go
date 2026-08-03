// Deterministic proofs of the create- and update-time "crash window":
// crossplane-runtime persists an external-name annotation change made by
// Create()/Update() as a step separate from — and not guaranteed to
// happen immediately after — the external call that produced it. If the
// process dies (or a write to the API server fails) in the gap between
// "the Grid object was mutated" and "the annotation reflecting that
// mutation was persisted", the next reconcile must still find the object
// through the identity ladder rather than either colliding with it (a
// second Create) or abandoning it (treating a stale ref as "gone").
//
// An earlier attempt to prove this by racing an external polling loop
// against the reconcile could not work: the measured WAPI round trip
// (~83ms) is faster than a curl+python+kubectl poll loop, so the
// annotation was always already written by the first observation. The
// recipe here forces the window open deterministically instead of racing
// it, using only tools every other test in this package already uses —
// no fault-injection hook, no timing, nothing that needs to be proven
// inert in a production build:
//
//   - Create-time window: call the real Create() against the mock Grid
//     (the WAPI POST genuinely lands and the object is genuinely
//     stamped), then drive the "next reconcile" through a SECOND,
//     independent managed-resource value that carries the same
//     metadata.uid but none of Create()'s in-memory annotation mutation.
//     That second value is exactly what a crash between "Create()
//     returns" and "the runtime's annotation write lands" leaves in
//     etcd — Create() itself never talks to the API server, so no fault
//     injection is needed to reproduce the gap.
//   - Update-time window: call the real Update() against the mock Grid
//     (the WAPI PUT genuinely rotates the object's _ref) using a
//     controller-runtime fake client that has never been seeded with the
//     CR being patched, so the annotation-persist call
//     (externalname.Refresh's kube.Patch) genuinely fails with NotFound.
//     This is a real (fake) API server answering truthfully that it does
//     not have the object — not a stub that pretends to fail.
//
// Every scenario below verifies recovery, non-duplication and orphan-free
// cleanup via searchGridByIdentity — a plain HTTP GET issued against the
// mock Grid's real WAPI surface, independent of the code path under
// test — never by reading the mock server's internal map or by trusting
// the managed resource's own status.
//
// Mutation check (recorded manually, not committed as an automated test
// per the guard that a probe must fail when the behavior it proves is
// disabled): identity.Resolve's ref-empty branch —
//
//	if ref == "" {
//	    ...
//	    return found, OutcomeFoundByUID, nil
//	}
//
// — was temporarily replaced with an unconditional early return
// (`return zero, OutcomeNotFound, nil`) before the search is ever issued
// when ref == "", reproducing the exact defect the identity-EA search
// exists to close: a provider that gives up instead of searching when it
// has no prior reference. With that mutation in place,
// TestClusterCreateCrashWindowRecoversByUIDThenDeletesCleanly and
// TestNamespacedCreateCrashWindowRecoversByUIDThenDeletesCleanly both
// failed — "Observe: ResourceExists=false after the crash window — the
// next reconcile would call Create() again and collide with the object
// the first Create() already made" — while every other test in this
// package continued to pass. The mutation was reverted immediately after
// confirming the failure; it was never committed. This proves the two
// tests exercise the ref-empty search path itself, not merely the Grid's
// general tolerance for retries.
package recorda

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/meta"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/identity"
)

// searchGridByIdentity issues a real HTTP GET against the mock Grid's
// identity-EA search endpoint — the same request identity.Resolve's
// searchByUID issues — and returns the matching records. It is
// deliberately independent of both the controller code under test and
// the mock server's internal state (m.records): a genuine WAPI client
// call is the only acceptable evidence for "no second Grid object" and
// "no orphan left behind".
func searchGridByIdentity(t *testing.T, srv *httptest.Server, uid string) []ibclient.RecordA {
	t.Helper()
	q := url.Values{"*" + identity.EAKey: {uid}}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+"/wapi/v"+wapiVersion+"/record:a?"+q.Encode(), nil)
	if err != nil {
		t.Fatalf("WAPI identity search: build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("WAPI identity search: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("WAPI identity search: status %d, want 200", resp.StatusCode)
	}
	var recs []ibclient.RecordA
	if err := json.NewDecoder(resp.Body).Decode(&recs); err != nil {
		t.Fatalf("WAPI identity search: decode response: %v", err)
	}
	return recs
}

// ── create-time crash window ────────────────────────────────────────────

func TestClusterCreateCrashWindowRecoversByUIDThenDeletesCleanly(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &clusterExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector}

	// Step 1 — the create half of the window: a real Create() call, the
	// WAPI POST genuinely lands, and the Grid object is genuinely stamped
	// with this managed resource's uid in the same request.
	created := newClusterARecord("my-arecord", "")
	if _, err := e.Create(context.Background(), created); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	createdRef := meta.GetExternalName(created)
	if createdRef == "" {
		t.Fatal("Create: external-name not set on the in-memory object — cannot proceed with the crash-window recipe")
	}
	if got := searchGridByIdentity(t, srv, testUIDCluster); len(got) != 1 {
		t.Fatalf("post-create WAPI identity search: got %d matches, want exactly 1", len(got))
	}

	// Step 2 — the crash: a second, independent managed-resource value
	// carrying the same uid (newClusterARecord always stamps
	// testUIDCluster) but the pre-create external-name state — the
	// framework's NameAsExternalName initializer backfills the CR's own
	// name before Create ever runs, so "no ref yet" and "ref equals the
	// CR's name" are the same state (see observeRefFor). This is exactly
	// what the API server actually holds after a crash between Create()
	// returning and the runtime persisting its annotation mutation: it
	// has never heard of createdRef.
	crashed := newClusterARecord("my-arecord", "my-arecord")

	// Step 3 — the next reconcile, driven only through Observe(), exactly
	// as crossplane-runtime would drive it after a restart. No Create()
	// call happens here.
	got, err := e.Observe(context.Background(), crashed)
	if err != nil {
		t.Fatalf("Observe: unexpected error recovering from the create crash window: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: ResourceExists=false after the crash window — the next reconcile would call Create() again and collide with the object the first Create() already made")
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: ResourceLateInitialized=false — the recovered reference must be persisted through a path crossplane-runtime actually writes back")
	}
	if gotName := meta.GetExternalName(crashed); gotName != createdRef {
		t.Errorf("Observe: external-name = %q after recovery, want the original created ref %q", gotName, createdRef)
	}

	// Assert the ladder row directly, not just its side effects.
	if _, outcome, rerr := resolveARecordIdentity(context.Background(), mc.Connector, "", testUIDCluster); rerr != nil || outcome != identity.OutcomeFoundByUID {
		t.Fatalf("resolveARecordIdentity(ref=%q, uid=%q) = outcome %v, err %v; want OutcomeFoundByUID, nil", "", testUIDCluster, outcome, rerr)
	}

	// No second Grid object — verified by a WAPI search, not by the MR's
	// own status.
	if got := searchGridByIdentity(t, srv, testUIDCluster); len(got) != 1 {
		t.Fatalf("post-recovery WAPI identity search: got %d matches, want exactly 1 (a second Create() would show up here as 2)", len(got))
	}

	// Step 4 — the subsequent delete, using the now-recovered external
	// name exactly as a real kubectl delete would after the annotation is
	// repopulated.
	if _, err := e.Delete(context.Background(), crashed); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if got := searchGridByIdentity(t, srv, testUIDCluster); len(got) != 0 {
		t.Fatalf("post-delete WAPI identity search: got %d matches, want 0 — the object survived as an orphan", len(got))
	}
}

func TestNamespacedCreateCrashWindowRecoversByUIDThenDeletesCleanly(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	mc := newTestObjectManager(t, srv)
	e := &namespacedExternal{kube: &recordingKubeClient{}, objMgr: mc.Manager, conn: mc.Connector}

	created := newNamespacedARecord("default", "my-arecord", "", "ProviderConfig")
	if _, err := e.Create(context.Background(), created); err != nil {
		t.Fatalf("Create: unexpected error: %v", err)
	}
	createdRef := meta.GetExternalName(created)
	if createdRef == "" {
		t.Fatal("Create: external-name not set on the in-memory object — cannot proceed with the crash-window recipe")
	}
	if got := searchGridByIdentity(t, srv, testUIDNamespaced); len(got) != 1 {
		t.Fatalf("post-create WAPI identity search: got %d matches, want exactly 1", len(got))
	}

	crashed := newNamespacedARecord("default", "my-arecord", "my-arecord", "ProviderConfig")

	got, err := e.Observe(context.Background(), crashed)
	if err != nil {
		t.Fatalf("Observe: unexpected error recovering from the create crash window: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: ResourceExists=false after the crash window — the next reconcile would call Create() again and collide with the object the first Create() already made")
	}
	if !got.ResourceLateInitialized {
		t.Error("Observe: ResourceLateInitialized=false — the recovered reference must be persisted through a path crossplane-runtime actually writes back")
	}
	if gotName := meta.GetExternalName(crashed); gotName != createdRef {
		t.Errorf("Observe: external-name = %q after recovery, want the original created ref %q", gotName, createdRef)
	}

	if _, outcome, rerr := resolveARecordIdentity(context.Background(), mc.Connector, "", testUIDNamespaced); rerr != nil || outcome != identity.OutcomeFoundByUID {
		t.Fatalf("resolveARecordIdentity(ref=%q, uid=%q) = outcome %v, err %v; want OutcomeFoundByUID, nil", "", testUIDNamespaced, outcome, rerr)
	}

	if got := searchGridByIdentity(t, srv, testUIDNamespaced); len(got) != 1 {
		t.Fatalf("post-recovery WAPI identity search: got %d matches, want exactly 1 (a second Create() would show up here as 2)", len(got))
	}

	if _, err := e.Delete(context.Background(), crashed); err != nil {
		t.Fatalf("Delete: unexpected error: %v", err)
	}
	if got := searchGridByIdentity(t, srv, testUIDNamespaced); len(got) != 0 {
		t.Fatalf("post-delete WAPI identity search: got %d matches, want 0 — the object survived as an orphan", len(got))
	}
}

// ── update-time crash window (rotation) ─────────────────────────────────

func TestClusterUpdateCrashWindowRecoversByRotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDCluster),
	})

	mc := newTestObjectManager(t, srv)

	// The persist half of the window: a real controller-runtime fake API
	// server that was never seeded with this CR, so
	// externalname.Refresh's kube.Patch call genuinely fails with
	// NotFound — a truthful fake API server, not a stub pretending to
	// fail. No fault-injection hook.
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	e := &clusterExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	cr := newClusterARecord("my-arecord", oldRef)
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	// Step 1 — the update half of the window: the WAPI PUT lands and the
	// Grid genuinely rotates the object's _ref, but persisting the
	// refreshed annotation fails. Update() must surface that failure
	// rather than swallow it.
	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: want an error when the annotation persist call fails, got nil — a swallowed failure here is exactly how the rotation crash window opens silently")
	}

	rotated := searchGridByIdentity(t, srv, testUIDCluster)
	if len(rotated) != 1 {
		t.Fatalf("post-update WAPI identity search: got %d matches, want exactly 1", len(rotated))
	}
	newRef := rotated[0].Ref
	if newRef == oldRef {
		t.Fatalf("post-update WAPI identity search: ref unchanged (%q) — the rename did not rotate the handle, so this scenario does not exercise the window it claims to", newRef)
	}
	if gotName := meta.GetExternalName(cr); gotName != oldRef {
		t.Fatalf("Update: in-memory external-name = %q after a failed persist, want it unchanged at the stale ref %q — externalname.Refresh must not call meta.SetExternalName before its kube.Patch succeeds", gotName, oldRef)
	}

	// Step 2 — the crash: what the API server actually has is a fresh CR
	// value carrying the same uid and the stale ref, exactly what the
	// failed patch above left behind.
	crashed := newClusterARecord("my-arecord", oldRef)

	// Step 3 — the next reconcile, driven only through Observe().
	got, err := e.Observe(context.Background(), crashed)
	if err != nil {
		t.Fatalf("Observe: unexpected error recovering from the rotation crash window: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: ResourceExists=false after the rotation crash window — the stale ref must recover via the identity search, not be treated as gone")
	}
	if gotName := meta.GetExternalName(crashed); gotName != newRef {
		t.Errorf("Observe: external-name = %q after recovery, want the rotated ref %q", gotName, newRef)
	}

	if _, outcome, rerr := resolveARecordIdentity(context.Background(), mc.Connector, oldRef, testUIDCluster); rerr != nil || outcome != identity.OutcomeRotated {
		t.Fatalf("resolveARecordIdentity(ref=%q, uid=%q) = outcome %v, err %v; want OutcomeRotated, nil", oldRef, testUIDCluster, outcome, rerr)
	}

	if got := searchGridByIdentity(t, srv, testUIDCluster); len(got) != 1 {
		t.Fatalf("post-recovery WAPI identity search: got %d matches, want exactly 1 — the object must not wedge into a duplicate or an orphan", len(got))
	}
}

func TestNamespacedUpdateCrashWindowRecoversByRotation(t *testing.T) {
	m := newMockWapiServer()
	srv := httptest.NewServer(m.handler())
	defer srv.Close()

	oldRef := m.seed(&ibclient.RecordA{
		Name:     stringPtr("host.example.com"),
		Ipv4Addr: stringPtr("10.0.0.1"),
		View:     "default",
		Ea:       identity.Stamp(nil, testUIDNamespaced),
	})

	mc := newTestObjectManager(t, srv)

	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	e := &namespacedExternal{kube: kube, objMgr: mc.Manager, conn: mc.Connector}

	cr := newNamespacedARecord("default", "my-arecord", oldRef, "ProviderConfig")
	cr.Spec.ForProvider.Name = stringPtr("renamed.example.com")

	if _, err := e.Update(context.Background(), cr); err == nil {
		t.Fatal("Update: want an error when the annotation persist call fails, got nil — a swallowed failure here is exactly how the rotation crash window opens silently")
	}

	rotated := searchGridByIdentity(t, srv, testUIDNamespaced)
	if len(rotated) != 1 {
		t.Fatalf("post-update WAPI identity search: got %d matches, want exactly 1", len(rotated))
	}
	newRef := rotated[0].Ref
	if newRef == oldRef {
		t.Fatalf("post-update WAPI identity search: ref unchanged (%q) — the rename did not rotate the handle, so this scenario does not exercise the window it claims to", newRef)
	}
	if gotName := meta.GetExternalName(cr); gotName != oldRef {
		t.Fatalf("Update: in-memory external-name = %q after a failed persist, want it unchanged at the stale ref %q — externalname.Refresh must not call meta.SetExternalName before its kube.Patch succeeds", gotName, oldRef)
	}

	crashed := newNamespacedARecord("default", "my-arecord", oldRef, "ProviderConfig")

	got, err := e.Observe(context.Background(), crashed)
	if err != nil {
		t.Fatalf("Observe: unexpected error recovering from the rotation crash window: %v", err)
	}
	if !got.ResourceExists {
		t.Fatal("Observe: ResourceExists=false after the rotation crash window — the stale ref must recover via the identity search, not be treated as gone")
	}
	if gotName := meta.GetExternalName(crashed); gotName != newRef {
		t.Errorf("Observe: external-name = %q after recovery, want the rotated ref %q", gotName, newRef)
	}

	if _, outcome, rerr := resolveARecordIdentity(context.Background(), mc.Connector, oldRef, testUIDNamespaced); rerr != nil || outcome != identity.OutcomeRotated {
		t.Fatalf("resolveARecordIdentity(ref=%q, uid=%q) = outcome %v, err %v; want OutcomeRotated, nil", oldRef, testUIDNamespaced, outcome, rerr)
	}

	if got := searchGridByIdentity(t, srv, testUIDNamespaced); len(got) != 1 {
		t.Fatalf("post-recovery WAPI identity search: got %d matches, want exactly 1 — the object must not wedge into a duplicate or an orphan", len(got))
	}
}
