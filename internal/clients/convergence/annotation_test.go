package convergence

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
