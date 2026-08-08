package convergence

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func newTestObj() *metav1.ObjectMeta {
	return &metav1.ObjectMeta{Name: "test-record"}
}

func TestSetAndGetPendingSerial(t *testing.T) {
	obj := newTestObj()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

	if err := SetPendingSerial(obj, "example.com", "Internal", 5, now); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	raw, ok := obj.GetAnnotations()[PendingZoneSerialAnnotation]
	if !ok {
		t.Fatal("expected the pending-zone-serial annotation to be set")
	}
	if raw == "" {
		t.Fatal("annotation value must not be empty")
	}

	pending, ok, err := GetPendingSerial(obj)
	if err != nil {
		t.Fatalf("GetPendingSerial: unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("GetPendingSerial must report ok=true after SetPendingSerial")
	}
	if pending.Zone != "example.com/Internal" || pending.Serial != 5 || !pending.Since.Equal(now) {
		t.Fatalf("GetPendingSerial returned unexpected value: %+v", pending)
	}
}

func TestGetPendingSerialAbsent(t *testing.T) {
	obj := newTestObj()
	_, ok, err := GetPendingSerial(obj)
	if err != nil {
		t.Fatalf("GetPendingSerial: unexpected error: %v", err)
	}
	if ok {
		t.Fatal("GetPendingSerial must report ok=false when no annotation is set")
	}
}

func TestGetPendingSerialCorrupt(t *testing.T) {
	obj := newTestObj()
	obj.SetAnnotations(map[string]string{PendingZoneSerialAnnotation: "{not valid json"})

	_, ok, err := GetPendingSerial(obj)
	if err == nil {
		t.Fatal("expected an error for a corrupt annotation value")
	}
	if ok {
		t.Fatal("GetPendingSerial must report ok=false alongside an error")
	}
}

func TestClearPendingSerial(t *testing.T) {
	obj := newTestObj()
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	ClearPendingSerial(obj)

	if _, ok := obj.GetAnnotations()[PendingZoneSerialAnnotation]; ok {
		t.Fatal("ClearPendingSerial must remove the annotation")
	}
}

func TestClearPendingSerialAnnotationKeyAbsent(t *testing.T) {
	// Annotations map is non-nil and non-empty, but does not contain the
	// pending-zone-serial key — must be a safe no-op that leaves the
	// existing annotations untouched.
	obj := newTestObj()
	obj.SetAnnotations(map[string]string{"other/key": "value"})

	ClearPendingSerial(obj)

	ann := obj.GetAnnotations()
	if len(ann) != 1 || ann["other/key"] != "value" {
		t.Fatalf("ClearPendingSerial must not modify annotations when its key is absent, got %+v", ann)
	}
}

func TestClearPendingSerialNoAnnotations(t *testing.T) {
	// Must not panic on a nil annotations map.
	obj := newTestObj()
	ClearPendingSerial(obj)
	if obj.GetAnnotations() != nil {
		t.Fatal("ClearPendingSerial on an object with no annotations must leave annotations nil")
	}
}

func TestClearPendingSerialPreservesOtherAnnotations(t *testing.T) {
	obj := newTestObj()
	obj.SetAnnotations(map[string]string{"other/key": "value"})
	if err := SetPendingSerial(obj, "example.com", "Internal", 5, time.Now()); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	ClearPendingSerial(obj)

	ann := obj.GetAnnotations()
	if ann["other/key"] != "value" {
		t.Fatal("ClearPendingSerial must not remove unrelated annotations")
	}
	if _, ok := ann[PendingZoneSerialAnnotation]; ok {
		t.Fatal("ClearPendingSerial must remove the pending-zone-serial annotation")
	}
}

func TestZoneKey(t *testing.T) {
	if got := ZoneKey("example.com", "Internal"); got != "example.com/Internal" {
		t.Fatalf("ZoneKey = %q, want %q", got, "example.com/Internal")
	}
}

// TestPendingSerialSurvivesRestart simulates a controller restart: the
// annotation set before "restart" is still readable afterward, since gate
// state lives entirely on the managed resource.
func TestPendingSerialSurvivesRestart(t *testing.T) {
	obj := newTestObj()
	now := time.Now()
	if err := SetPendingSerial(obj, "example.com", "Internal", 7, now); err != nil {
		t.Fatalf("SetPendingSerial: unexpected error: %v", err)
	}

	// Simulate a restart by round-tripping only the annotations map (as
	// if the controller had never seen this object before and just read
	// it fresh from the API server).
	restarted := &metav1.ObjectMeta{Name: obj.Name, Annotations: obj.GetAnnotations()}

	pending, ok, err := GetPendingSerial(restarted)
	if err != nil {
		t.Fatalf("GetPendingSerial after simulated restart: unexpected error: %v", err)
	}
	if !ok || pending.Serial != 7 {
		t.Fatalf("expected the pending serial to survive a restart, got ok=%v pending=%+v", ok, pending)
	}
}

// ── PersistPendingSerial ─────────────────────────────────────────────────
//
// PersistPendingSerial is the fix for the hazard SetPendingSerial alone
// does not cover: crossplane-runtime does not guarantee metadata written
// during an ExternalClient's Update() reaches the API server, only status.
// These tests use a recording client.Client stub — not a full fake
// client — because the point under test is "was Patch called with this
// object", not full merge-patch semantics (already covered by
// externalname.Refresh's own tests, whose pattern this mirrors).

type recordingPatchClient struct {
	client.Client
	patched   client.Object
	patchType client.Patch
}

func (c *recordingPatchClient) Patch(_ context.Context, obj client.Object, patch client.Patch, _ ...client.PatchOption) error {
	c.patched = obj
	c.patchType = patch
	return nil
}

func newPersistTestObj(name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func TestPersistPendingSerialSetsAnnotationAndPatches(t *testing.T) {
	kube := &recordingPatchClient{}
	obj := newPersistTestObj("r1")
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

	if err := PersistPendingSerial(context.Background(), kube, obj, "example.com", "Internal", 9, now); err != nil {
		t.Fatalf("PersistPendingSerial: unexpected error: %v", err)
	}

	// In-memory mutation happened (same contract as SetPendingSerial).
	pending, ok, err := GetPendingSerial(obj)
	if err != nil || !ok {
		t.Fatalf("expected the pending annotation to be set in memory, ok=%v err=%v", ok, err)
	}
	if pending.Serial != 9 || pending.Zone != "example.com/Internal" {
		t.Fatalf("unexpected pending serial: %+v", pending)
	}

	// AND the change was submitted to the API server via Patch — not
	// merely left in memory for crossplane-runtime to maybe flush later.
	if kube.patched == nil {
		t.Fatal("expected PersistPendingSerial to call kube.Patch")
	}
	if kube.patchType == nil {
		t.Fatal("expected a non-nil merge patch")
	}
}

func TestPersistPendingSerialPropagatesPatchError(t *testing.T) {
	kube := &failingPatchClient{err: &boomError{}}
	obj := newPersistTestObj("r1")

	if err := PersistPendingSerial(context.Background(), kube, obj, "example.com", "Internal", 9, time.Now()); err == nil {
		t.Fatal("expected PersistPendingSerial to propagate a Patch error")
	}
}

type failingPatchClient struct {
	client.Client
	err error
}

func (c *failingPatchClient) Patch(context.Context, client.Object, client.Patch, ...client.PatchOption) error {
	return c.err
}

// boomError is a minimal error type used only to prove PersistPendingSerial
// propagates whatever error the Patch call returns.
type boomError struct{}

func (*boomError) Error() string { return "boom" }
