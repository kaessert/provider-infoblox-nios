package convergence

import (
	"context"
	"encoding/json"
	"time"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	errMarshalAnnotation    = "cannot marshal pending-zone-serial annotation"
	errUnmarshalAnnotation  = "cannot unmarshal pending-zone-serial annotation"
	errBuildAnnotationPatch = "cannot build pending-zone-serial annotation patch"
)

// PendingZoneSerialAnnotation is the well-known annotation key the
// convergence gate uses to persist a pending write's watermark serial on
// the managed resource, so the gate survives controller restarts.
const PendingZoneSerialAnnotation = "infobloxnios.crossplane.io/pending-zone-serial"

// PendingZoneSerial is the JSON value stored under
// PendingZoneSerialAnnotation. Zone is "<fqdn>/<view>". Since records when
// the watermark was set — used to enforce convergence.timeout even across
// controller restarts, since the annotation (not controller memory) is
// this gate's only state.
type PendingZoneSerial struct {
	Zone   string    `json:"zone"`
	Serial uint32    `json:"serial"`
	Since  time.Time `json:"since"`
}

// ZoneKey formats a zone_auth fqdn+view pair into the "zone" value used in
// PendingZoneSerial and in ReadRouting condition messages.
func ZoneKey(fqdn, view string) string {
	return fqdn + "/" + view
}

// SetPendingSerial records fqdn/view/serial as the pending watermark on
// obj's annotations, called right after a DNS write. now is normally
// time.Now — parameterized so callers (and tests) can control Since
// directly.
func SetPendingSerial(obj metav1.Object, fqdn, view string, serial uint32, now time.Time) error {
	p := PendingZoneSerial{Zone: ZoneKey(fqdn, view), Serial: serial, Since: now}
	raw, err := json.Marshal(p)
	if err != nil {
		return errors.Wrap(err, errMarshalAnnotation)
	}

	ann := obj.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	ann[PendingZoneSerialAnnotation] = string(raw)
	obj.SetAnnotations(ann)
	return nil
}

// GetPendingSerial reads and parses the pending-zone-serial annotation
// from obj. ok is false (with a nil error) when the annotation is simply
// absent — the normal steady-state case once a write has converged. A
// non-nil error means the annotation is present but corrupt (should not
// happen outside manual tampering); callers should treat that the same as
// "not caught up" and route to primary rather than trusting garbage state.
func GetPendingSerial(obj metav1.Object) (pending *PendingZoneSerial, ok bool, err error) {
	ann := obj.GetAnnotations()
	raw, present := ann[PendingZoneSerialAnnotation]
	if !present || raw == "" {
		return nil, false, nil
	}

	var p PendingZoneSerial
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return nil, false, errors.Wrap(err, errUnmarshalAnnotation)
	}
	return &p, true, nil
}

// ClearPendingSerial removes the pending-zone-serial annotation from obj,
// called once the gate confirms the candidate has caught up (or gives up
// after a convergence timeout).
func ClearPendingSerial(obj metav1.Object) {
	ann := obj.GetAnnotations()
	if len(ann) == 0 {
		return
	}
	if _, present := ann[PendingZoneSerialAnnotation]; !present {
		return
	}
	delete(ann, PendingZoneSerialAnnotation)
	obj.SetAnnotations(ann)
}

// PersistPendingSerial calls SetPendingSerial and then immediately
// persists ONLY that annotation to the API server via a JSON merge patch
// scoped to metadata.annotations, wrapped in a bounded conflict-retry
// loop — mirroring the external-name persistence pattern controllers
// already use for other Update()-time metadata changes.
//
// crossplane-runtime's managed reconciler does not guarantee that
// metadata changes made inside an ExternalClient's Update() method are
// written back to the API server: it persists status via a subresource
// update, but the only path that persists metadata/annotations is the
// reconciler's own separate late-initialize Update call, which fires
// only when Observe reported ResourceLateInitialized. A Gate.RecordWrite
// call made directly inside Update() (as opposed to Create(), whose
// annotation changes the reconciler always persists via
// UpdateCriticalAnnotations) can therefore be silently discarded once
// that reconcile ends — wedging the read-after-write convergence gate
// for that write until some unrelated later change happens to trigger a
// late-init persist. Call this from Update() instead of SetPendingSerial
// directly whenever the write happens outside Create()/Observe().
func PersistPendingSerial(ctx context.Context, kube client.Client, obj client.Object, fqdn, view string, serial uint32, now time.Time) error {
	if err := SetPendingSerial(obj, fqdn, view, serial, now); err != nil {
		return err
	}

	value := obj.GetAnnotations()[PendingZoneSerialAnnotation]
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				PendingZoneSerialAnnotation: value,
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, errBuildAnnotationPatch)
	}

	rawPatch := client.RawPatch(types.MergePatchType, patch)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		return kube.Patch(ctx, obj, rawPatch)
	})
}
