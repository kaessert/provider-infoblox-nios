package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// testFieldNotifyDelay is a representative mutable scalar field used across
// the runFieldTest no-op/negative-case tests below.
const testFieldNotifyDelay = "notifyDelay"

// testIPv6NetworkPrefix is the WAPI object-type prefix a dual-object-type
// resource's external-name must carry when it resolved against the IPv6
// object type — shared across the external-name-prefix tests in this file
// and in config_test.go.
const testIPv6NetworkPrefix = "ipv6network/"

// fakeCluster is an in-memory stand-in for a live cluster's view of a single
// managed resource. It implements the subset of kubectl behaviour Runner
// depends on (get -o json, get -o jsonpath=..., get events, patch, wait) so
// runFieldTest can be exercised without a real cluster.
type fakeCluster struct {
	forProvider map[string]interface{}
	atProvider  map[string]interface{}
	generation  int64
	// externalName, when non-empty, is surfaced as the live resource's
	// crossplane.io/external-name annotation — exercised by
	// TestExternalName*. Left empty, metadata carries no annotations map at
	// all (matching a resource observed before Create ever ran), which is
	// the pre-existing default every other test in this file relies on.
	externalName string

	// kind and name identify the resource for event-evidence lookups
	// (kubectl get events ... involvedObject matching). Tests exercising
	// countUpdateEvents/runFieldTest's evidence check set these to match
	// the kind/name passed into runFieldTest.
	kind string
	name string

	// recordUpdateEvent, when true, makes each real field patch (not the
	// ClearConditions status patch) increment the active generation's
	// aggregated count by one — simulating a controller whose Update() call
	// emits an UpdatedExternalResource event. Left false, patches succeed
	// and atProvider still converges, but no event is ever recorded — the
	// "update not evidenced" scenario.
	recordUpdateEvent bool
	// eventBudget caps how many events a single controller "process" (the
	// stretch between restart() calls) accumulates before further
	// increments are silently dropped — simulating client-go's in-process
	// event-spam-filter burst ceiling (see eventBurstCeiling in runner.go).
	// 0 (the default) means unlimited, for tests that are not exercising
	// the ceiling.
	eventBudget int32
	// generations holds one aggregated event count per controller
	// "process": index 0 is the count since the fake was created, and
	// restart() appends a new zeroed entry — mirroring how a real
	// controller restart discards client-go's in-memory event-aggregation
	// cache, so a post-restart event becomes a brand NEW Event object
	// instead of bumping the old one's .count further.
	generations []int32
	// restartCalls counts how many times restart() (wired in as
	// Runner.restartFunc) was invoked, so tests can assert the runner
	// actually triggered a burst reset rather than merely tolerating one.
	restartCalls int
	// emitZeroCountEvent, when true, reports the LAST generation's
	// aggregated .count field as 0 once it reaches at least 1 — mirroring
	// client-go's event recorder, which leaves .count unset (0) for an
	// event's first, unaggregated occurrence. Exercises sumEventOccurrences's
	// zero-guard (a 0 count is 1 occurrence, not 0) through the
	// runFieldTest evidence check rather than only through the standalone
	// sumEventOccurrences unit tests.
	emitZeroCountEvent bool

	patchCalls int
	waitCalls  int
	// nudgeCalls counts NudgeReconcile's metadata-annotation patches,
	// tracked separately from patchCalls (real field patches) so tests can
	// assert the forced second reconcile fired without conflating it with
	// the field patch itself.
	nudgeCalls int
}

// exec implements the Runner.execFunc signature, dispatching on the kubectl
// subcommand (first arg).
func (f *fakeCluster) exec(args []string) (string, error) {
	if len(args) == 0 {
		return "", fmt.Errorf("fakeCluster: no args")
	}
	switch args[0] {
	case "get":
		return f.handleGet(args)
	case "patch":
		return f.handlePatch(args)
	case "wait":
		f.waitCalls++
		return "", nil
	default:
		return "", fmt.Errorf("fakeCluster: unhandled kubectl subcommand %q", args[0])
	}
}

func (f *fakeCluster) handleGet(args []string) (string, error) {
	if len(args) > 1 && args[1] == "events" {
		return f.handleGetEvents()
	}
	for _, a := range args {
		if strings.HasPrefix(a, "jsonpath=") {
			b, err := json.Marshal(f.atProvider)
			if err != nil {
				return "", err
			}
			return string(b), nil
		}
	}
	metadata := map[string]interface{}{"generation": f.generation}
	if f.externalName != "" {
		metadata["annotations"] = map[string]interface{}{
			externalNameAnnotation: f.externalName,
		}
	}
	obj := map[string]interface{}{
		"metadata":    metadata,
		"spec":        map[string]interface{}{"forProvider": f.forProvider},
		jsonKeyStatus: map[string]interface{}{jsonKeyAtProvider: f.atProvider},
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// handleGetEvents backs `kubectl get events --all-namespaces -o json`. It
// emits one aggregated Item per non-empty generation (see f.generations) —
// mirroring how a real controller restart causes client-go to create a
// brand NEW Event object rather than continuing to bump the throttled one —
// so countUpdateEvents/sumEventOccurrences must sum across Items, not just
// read the last one.
func (f *fakeCluster) handleGetEvents() (string, error) {
	list := eventList{}
	for i, count := range f.generations {
		if count <= 0 {
			continue
		}
		reported := count
		if f.emitZeroCountEvent && i == len(f.generations)-1 {
			reported = 0
		}
		item := eventItem{Reason: eventReasonUpdated, Count: reported}
		item.InvolvedObject.Kind = f.kind
		item.InvolvedObject.Name = f.name
		list.Items = append(list.Items, item)
	}
	b, err := json.Marshal(list)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// restart simulates a controller pod restart: it appends a fresh generation
// so subsequent recordUpdateEvent increments accumulate against a new,
// unthrottled budget rather than the exhausted one. Wired in as
// Runner.restartFunc so RunTests's proactive burst-reset (eventBurstCeiling)
// can be exercised without a live cluster.
func (f *fakeCluster) restart() error {
	f.restartCalls++
	f.generations = append(f.generations, 0)
	return nil
}

func (f *fakeCluster) handlePatch(args []string) (string, error) {
	for _, a := range args {
		if a == "--subresource=status" {
			// ClearConditions — no state change needed for these tests.
			return "", nil
		}
	}

	var payload string
	for i, a := range args {
		if a == "-p" && i+1 < len(args) {
			payload = args[i+1]
		}
	}
	if payload == "" {
		return "", fmt.Errorf("fakeCluster: patch called without -p payload")
	}

	var patch map[string]interface{}
	if err := json.Unmarshal([]byte(payload), &patch); err != nil {
		return "", fmt.Errorf("fakeCluster: parsing patch payload: %w", err)
	}

	if _, isMetadataPatch := patch["metadata"]; isMetadataPatch {
		// NudgeReconcile — a metadata-annotation-only patch used to force a
		// reconcile without touching spec.forProvider. Counted separately
		// so it is never mistaken for a real field patch (which would
		// corrupt patchCalls/eventCount assertions below).
		f.nudgeCalls++
		return "", nil
	}

	specRaw, _ := patch["spec"].(map[string]interface{})
	forProviderRaw, _ := specRaw["forProvider"].(map[string]interface{})

	mergeInto(f.forProvider, forProviderRaw)
	// Simulate a controller that converges instantly, so pollField's first
	// read already matches — these tests are about the no-op guard, not
	// about polling behaviour.
	mergeInto(f.atProvider, forProviderRaw)

	f.generation++
	f.patchCalls++
	if f.recordUpdateEvent {
		if len(f.generations) == 0 {
			f.generations = append(f.generations, 0)
		}
		idx := len(f.generations) - 1
		if f.eventBudget <= 0 || f.generations[idx] < f.eventBudget {
			f.generations[idx]++
		}
	}
	return "", nil
}

// mergeInto applies a JSON-merge-patch style merge of patch into dst: nested
// objects are merged recursively, a nil value deletes the key, and any other
// value overwrites it.
func mergeInto(dst, patch map[string]interface{}) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)
			continue
		}
		if m, ok := v.(map[string]interface{}); ok {
			if dm, ok := dst[k].(map[string]interface{}); ok {
				mergeInto(dm, m)
				continue
			}
		}
		dst[k] = v
	}
}

func newFakeRunner(f *fakeCluster) *Runner {
	return &Runner{
		resourceName: "arecord.record.infobloxnios.crossplane.io/example-arecord",
		timeout:      "5s",
		execFunc:     f.exec,
		restartFunc:  f.restart,
	}
}

// TestRunFieldTestNoOpDetection covers the no-op detection path: a field
// whose pre-patch value already equals the target value must be reported as
// a failure (not a PASS), and the patch/wait machinery must never fire since
// there is nothing for it to exercise.
func TestRunFieldTestNoOpDetection(t *testing.T) {
	f := &fakeCluster{
		forProvider: map[string]interface{}{testFieldNotifyDelay: float64(10)},
		atProvider:  map[string]interface{}{testFieldNotifyDelay: float64(10)},
		generation:  1,
		kind:        testKindARecord,
		name:        testNameARecord,
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: testFieldNotifyDelay, Value: 10}
	result, newSnapshot := r.runFieldTest(test, snapshot, testKindARecord, testNameARecord)

	if !result.NoOp {
		t.Fatalf("expected NoOp=true, got %+v", result)
	}
	if result.Passed {
		t.Fatal("no-op result must not be reported as Passed")
	}
	if result.Error == nil {
		t.Fatal("expected a non-nil error naming the no-op field and value")
	}
	const wantSubstr = "no-op: notifyDelay already equals 10"
	if !strings.Contains(result.Error.Error(), wantSubstr) {
		t.Errorf("error = %q, want substring %q", result.Error.Error(), wantSubstr)
	}
	if f.patchCalls != 0 {
		t.Errorf("expected 0 patch calls for a no-op field, got %d", f.patchCalls)
	}
	if f.waitCalls != 0 {
		t.Errorf("expected 0 wait calls for a no-op field, got %d", f.waitCalls)
	}
	if !bytes.Equal(newSnapshot, snapshot) {
		t.Errorf("no-op short-circuit must return the input snapshot unchanged")
	}
}

// TestRunFieldTestExecutesWhenValueDiffers covers the negative case: a field
// whose pre-patch value differs from the target is patched and tested
// normally, producing a genuine PASS backed by positive update-event
// evidence (see TestRunFieldTestNotEvidencedWhenNoUpdateEvent for the
// counter-case), and exercises the forced second reconcile that refreshes
// atProvider independently of the provider's poll interval.
func TestRunFieldTestExecutesWhenValueDiffers(t *testing.T) {
	f := &fakeCluster{
		forProvider:       map[string]interface{}{testFieldNotifyDelay: float64(5)},
		atProvider:        map[string]interface{}{testFieldNotifyDelay: float64(5)},
		generation:        1,
		kind:              testKindARecord,
		name:              testNameARecord,
		recordUpdateEvent: true,
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: testFieldNotifyDelay, Value: 10}
	result, newSnapshot := r.runFieldTest(test, snapshot, testKindARecord, testNameARecord)

	if result.NoOp {
		t.Fatalf("expected NoOp=false when the pre-patch value differs, got %+v", result)
	}
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if !result.Passed {
		t.Fatalf("expected the update test to pass, got %+v", result)
	}
	if !result.UpdateEvidenced {
		t.Errorf("expected UpdateEvidenced=true when the controller emits an update event, got %+v", result)
	}
	if result.NotEvidenced {
		t.Errorf("expected NotEvidenced=false for an evidenced update, got %+v", result)
	}
	if f.patchCalls != 1 {
		t.Errorf("expected exactly 1 patch call, got %d", f.patchCalls)
	}
	if f.nudgeCalls != 1 {
		t.Errorf("expected exactly 1 nudge call (forcing the second reconcile), got %d", f.nudgeCalls)
	}
	if f.waitCalls != 2 {
		t.Errorf("expected exactly 2 wait calls (first reconcile + forced re-observe), got %d", f.waitCalls)
	}
	if bytes.Equal(newSnapshot, snapshot) {
		t.Error("expected the post-patch snapshot to differ from the input snapshot")
	}
	if len(result.SideFx) != 0 {
		t.Errorf("expected no side effects, got %v", result.SideFx)
	}
}

// TestRunFieldTestNotEvidencedWhenNoUpdateEvent covers the failure mode this
// evidence check exists to catch: a field whose observed value ends up
// matching the target (e.g. because something outside the reconciler wrote
// it) but for which the controller never recorded an update event. Without
// the event-count signal this would be indistinguishable from a genuine
// PASS; with it, it must be downgraded to a distinct failure.
func TestRunFieldTestNotEvidencedWhenNoUpdateEvent(t *testing.T) {
	f := &fakeCluster{
		forProvider: map[string]interface{}{testFieldNotifyDelay: float64(5)},
		atProvider:  map[string]interface{}{testFieldNotifyDelay: float64(5)},
		generation:  1,
		kind:        testKindARecord,
		name:        testNameARecord,
		// recordUpdateEvent left false: the patch still "succeeds" (the fake
		// always converges atProvider), but no UpdatedExternalResource event
		// is ever recorded — simulating a reconciler whose Update() path
		// never actually ran.
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: testFieldNotifyDelay, Value: 10}
	result, _ := r.runFieldTest(test, snapshot, testKindARecord, testNameARecord)

	if result.Passed {
		t.Fatalf("expected Passed=false when no update event was recorded, got %+v", result)
	}
	if !result.NotEvidenced {
		t.Fatalf("expected NotEvidenced=true, got %+v", result)
	}
	if result.UpdateEvidenced {
		t.Errorf("expected UpdateEvidenced=false, got %+v", result)
	}
	if result.Error == nil {
		t.Fatal("expected a non-nil error explaining the missing evidence")
	}
	const wantSubstr = "update not evidenced"
	if !strings.Contains(result.Error.Error(), wantSubstr) {
		t.Errorf("error = %q, want substring %q", result.Error.Error(), wantSubstr)
	}
}

// TestRunFieldTestZeroCountEventStillEvidencesUpdate confirms the evidence
// check applies the same zero-guard as sumEventOccurrences: an aggregated
// event whose .count field reads 0 (client-go leaves it unset for a single,
// unaggregated occurrence) must still count as one occurrence and evidence
// the update, not be mistaken for "no event happened".
func TestRunFieldTestZeroCountEventStillEvidencesUpdate(t *testing.T) {
	f := &fakeCluster{
		forProvider:        map[string]interface{}{testFieldNotifyDelay: float64(5)},
		atProvider:         map[string]interface{}{testFieldNotifyDelay: float64(5)},
		generation:         1,
		kind:               testKindARecord,
		name:               testNameARecord,
		recordUpdateEvent:  true,
		emitZeroCountEvent: true,
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: testFieldNotifyDelay, Value: 10}
	result, _ := r.runFieldTest(test, snapshot, testKindARecord, testNameARecord)

	if !result.UpdateEvidenced {
		t.Fatalf("expected UpdateEvidenced=true for a zero-count first occurrence, got %+v", result)
	}
	if !result.Passed {
		t.Fatalf("expected Passed=true, got %+v", result)
	}
	if result.NotEvidenced {
		t.Errorf("expected NotEvidenced=false, got %+v", result)
	}
}

// TestRunFieldTestNoOpUsesExpectOverride confirms the no-op comparison uses
// the patch target value (t.Value), independent of an optional t.Expect
// override used for fields whose observed value differs from what was set.
func TestRunFieldTestNoOpUsesExpectOverride(t *testing.T) {
	f := &fakeCluster{
		forProvider: map[string]interface{}{"dnssecValidationEnabled": true},
		atProvider:  map[string]interface{}{"dnssecValidationEnabled": true},
		generation:  1,
		kind:        testKindARecord,
		name:        testNameARecord,
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: "dnssecValidationEnabled", Value: true, Expect: true}
	result, _ := r.runFieldTest(test, snapshot, testKindARecord, testNameARecord)

	if !result.NoOp {
		t.Fatalf("expected NoOp=true, got %+v", result)
	}
	if f.patchCalls != 0 {
		t.Errorf("expected 0 patch calls, got %d", f.patchCalls)
	}
}

// TestReadCurrentValuePrefersSpecForProvider verifies readCurrentValue reads
// spec.forProvider ahead of status.atProvider — the value a merge patch is
// about to overwrite, not the (possibly stale) observed value.
func TestReadCurrentValuePrefersSpecForProvider(t *testing.T) {
	f := &fakeCluster{
		forProvider: map[string]interface{}{testFieldNotifyDelay: float64(10)},
		atProvider:  map[string]interface{}{testFieldNotifyDelay: float64(999)},
	}
	r := newFakeRunner(f)

	got, err := r.readCurrentValue(testFieldNotifyDelay)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "10" {
		t.Errorf("readCurrentValue = %q, want %q (spec.forProvider value)", got, "10")
	}
}

// TestReadCurrentValueFallsBackToAtProvider verifies readCurrentValue falls
// back to status.atProvider when the field is absent from spec.forProvider.
func TestReadCurrentValueFallsBackToAtProvider(t *testing.T) {
	f := &fakeCluster{
		forProvider: map[string]interface{}{},
		atProvider:  map[string]interface{}{"computedField": "server-value"},
	}
	r := newFakeRunner(f)

	got, err := r.readCurrentValue("computedField")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "server-value" {
		t.Errorf("readCurrentValue = %q, want %q (status.atProvider fallback)", got, "server-value")
	}
}

// TestNavigateJSONPath covers navigateAtProvider and navigateSpecForProvider
// (both backed by navigateJSONPath), including nested fields, missing
// containers, missing leaves, and non-object intermediates.
func TestNavigateJSONPath(t *testing.T) {
	cases := map[string]struct {
		reason  string
		obj     map[string]interface{}
		field   string
		want    interface{}
		wantErr bool
	}{
		"TopLevelField": {
			reason: "a direct child of atProvider resolves",
			obj: map[string]interface{}{
				jsonKeyStatus: map[string]interface{}{
					jsonKeyAtProvider: map[string]interface{}{"name": "example"},
				},
			},
			field: "name",
			want:  "example",
		},
		"NestedField": {
			reason: "a dot-path descends through nested objects",
			obj: map[string]interface{}{
				jsonKeyStatus: map[string]interface{}{
					jsonKeyAtProvider: map[string]interface{}{
						"parent": map[string]interface{}{"child": "value"},
					},
				},
			},
			field: "parent.child",
			want:  "value",
		},
		"MissingContainer": {
			reason: "a missing status key returns nil, nil rather than erroring",
			obj:    map[string]interface{}{},
			field:  "name",
			want:   nil,
		},
		"MissingLeaf": {
			reason: "a leaf absent from an otherwise present object returns nil, nil",
			obj: map[string]interface{}{
				jsonKeyStatus: map[string]interface{}{
					jsonKeyAtProvider: map[string]interface{}{"other": "x"},
				},
			},
			field: "name",
			want:  nil,
		},
		"NonObjectIntermediate": {
			reason: "descending through a scalar intermediate is an error",
			obj: map[string]interface{}{
				jsonKeyStatus: map[string]interface{}{
					jsonKeyAtProvider: map[string]interface{}{"parent": "scalar"},
				},
			},
			field:   "parent.child",
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := navigateAtProvider(tc.obj, tc.field)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("%s: expected error, got nil", tc.reason)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if got != tc.want {
				t.Errorf("%s: got %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

// TestExternalName covers Runner.ExternalName's two observable states: the
// annotation present with a value, and absent entirely (a resource observed
// before Create has ever populated it).
func TestExternalName(t *testing.T) {
	cases := map[string]struct {
		reason       string
		externalName string
		want         string
	}{
		"AnnotationPresent": {
			reason:       "the live crossplane.io/external-name annotation value is returned verbatim",
			externalName: testIPv6NetworkPrefix + "ZG5z...:2001:db8:9f88::/64/default",
			want:         testIPv6NetworkPrefix + "ZG5z...:2001:db8:9f88::/64/default",
		},
		"AnnotationAbsent": {
			reason: "a resource with no external-name annotation yet returns an empty string, not an error",
			want:   "",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := &fakeCluster{externalName: tc.externalName}
			r := newFakeRunner(f)

			got, err := r.ExternalName()
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if got != tc.want {
				t.Errorf("%s: ExternalName() = %q, want %q", tc.reason, got, tc.want)
			}
		})
	}
}

// TestCheckExternalNamePrefix proves the dual-object-type identity guard is
// a real gate, not a check that can only ever pass: the mismatch and
// empty-name cases below must both be classified as failures, not silently
// accepted. This is deliberately a pure-function unit test (no live
// cluster, no deliberately-broken controller build required) — see
// checkExternalNamePrefix's doc comment for why that is sufficient proof.
func TestCheckExternalNamePrefix(t *testing.T) {
	cases := map[string]struct {
		reason         string
		name           string
		expectedPrefix string
		wantOK         bool
		wantReasonHas  string
	}{
		"MatchingPrefix": {
			reason:         "an external-name that starts with the expected object-type prefix passes",
			name:           testIPv6NetworkPrefix + "ZG5z...:2001:db8:9f88::/64/default",
			expectedPrefix: testIPv6NetworkPrefix,
			wantOK:         true,
		},
		"WrongObjectType": {
			reason:         "an external-name resolved against the sibling IPv4 object type (network/) must fail an ipv6network/ expectation — this is the exact silent-duplicate hazard the check exists to catch",
			name:           "network/ZG5z...:2001:db8:9f88::/64/default",
			expectedPrefix: testIPv6NetworkPrefix,
			wantOK:         false,
			wantReasonHas:  "does not have expected prefix",
		},
		"EmptyExternalName": {
			reason:         "a resource with no resolved external-name yet cannot satisfy any prefix expectation",
			name:           "",
			expectedPrefix: testIPv6NetworkPrefix,
			wantOK:         false,
			wantReasonHas:  "absent or empty",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ok, reason := checkExternalNamePrefix(tc.name, tc.expectedPrefix)
			if ok != tc.wantOK {
				t.Fatalf("%s: checkExternalNamePrefix(%q, %q) ok = %v, want %v (reason: %s)",
					tc.reason, tc.name, tc.expectedPrefix, ok, tc.wantOK, reason)
			}
			if !tc.wantOK && !strings.Contains(reason, tc.wantReasonHas) {
				t.Errorf("%s: reason = %q, want substring %q", tc.reason, reason, tc.wantReasonHas)
			}
		})
	}
}

// TestStringifyFieldValue covers the value-to-comparison-string conversion
// used for both status.atProvider and spec.forProvider field reads.
func TestStringifyFieldValue(t *testing.T) {
	cases := map[string]struct {
		reason string
		val    interface{}
		want   string
	}{
		"Nil": {
			reason: "a missing value stringifies to the empty string",
			val:    nil,
			want:   "",
		},
		"String": {
			reason: "strings are returned unquoted",
			val:    "hello",
			want:   "hello",
		},
		"Number": {
			reason: "numbers are returned as canonical JSON",
			val:    float64(10),
			want:   "10",
		},
		"Bool": {
			reason: "booleans are returned as canonical JSON",
			val:    true,
			want:   "true",
		},
		"Map": {
			reason: "maps are returned as canonical JSON, not Go-format",
			val:    map[string]interface{}{"key": "value"},
			want:   `{"key":"value"}`,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := stringifyFieldValue(tc.val, "field")
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.reason, err)
			}
			if got != tc.want {
				t.Errorf("%s: got %q, want %q", tc.reason, got, tc.want)
			}
		})
	}
}

// manifestWithSequentialFieldTests builds a Manifest whose Tests set the
// same field to n distinct, strictly-increasing values in turn. Reusing one
// field (rather than n distinct fields) keeps the fixture small while still
// guaranteeing every test is a genuine change relative to the one before it
// (never a no-op), because runFieldTest's no-op check compares against
// spec.forProvider, which handlePatch updates in place after every patch.
func manifestWithSequentialFieldTests(n int) *Manifest {
	m := &Manifest{Kind: testKindARecord, Name: testNameARecord}
	for i := 1; i <= n; i++ {
		m.Tests = append(m.Tests, UpdateTest{Field: testFieldNotifyDelay, Value: float64(i)})
	}
	return m
}

// TestRunTestsResetsEventBurstBeforeCeiling covers the false-NOT-EVIDENCED
// regression this ticket exists to fix: a resource with more mutable fields
// than the client-go event-spam-filter's ~25-event burst must still report
// every genuinely-converged field as evidenced, because RunTests proactively
// restarts the controller (earning back a fresh burst) before the ceiling is
// reached rather than after events have already been silently dropped.
func TestRunTestsResetsEventBurstBeforeCeiling(t *testing.T) {
	const numFields = eventBurstCeiling + 5 // comfortably past one burst
	f := &fakeCluster{
		forProvider:       map[string]interface{}{testFieldNotifyDelay: float64(0)},
		atProvider:        map[string]interface{}{testFieldNotifyDelay: float64(0)},
		generation:        1,
		kind:              testKindARecord,
		name:              testNameARecord,
		recordUpdateEvent: true,
		// eventBudget mirrors the real client-go burst ceiling: once a
		// generation's aggregated count reaches it, further same-generation
		// increments are silently dropped, exactly as production
		// eventBurstCeiling is calibrated to stay under.
		eventBudget: eventBurstCeiling,
	}
	r := newFakeRunner(f)

	m := manifestWithSequentialFieldTests(numFields)
	results, err := r.RunTests(m)
	if err != nil {
		t.Fatalf("RunTests: unexpected error: %v", err)
	}
	if len(results) != numFields {
		t.Fatalf("got %d results, want %d", len(results), numFields)
	}

	for i, res := range results {
		if res.NotEvidenced {
			t.Errorf("field %d (%s): got NotEvidenced=true, want evidenced (err: %v)", i, res.Field, res.Error)
		}
		if !res.Passed {
			t.Errorf("field %d (%s): got Passed=false, want true (err: %v)", i, res.Field, res.Error)
		}
	}

	if f.restartCalls == 0 {
		t.Error("expected at least one controller restart before the burst ceiling was reached")
	}
	// numFields exceeds one ceiling's worth of attempts, so at least one
	// generation besides the first must have accumulated events — proof the
	// reset actually happened before fields ran out of budget, not merely
	// that restart() was called with no effect on the outcome.
	if len(f.generations) < 2 {
		t.Errorf("expected events to span at least 2 controller generations, got %d: %v", len(f.generations), f.generations)
	}
}

// TestRunTestsWithoutRestartWiringStillDetectsGenuineNonEvidence is the
// counterpart regression case demanded by the ticket: the burst-reset
// machinery must never become vacuously permissive. A controller that truly
// never emits update events (recordUpdateEvent left false, so no generation
// ever accumulates anything for resetEventBurst to rescue) must still fail
// with NotEvidenced, restart or no restart.
func TestRunTestsWithoutRestartWiringStillDetectsGenuineNonEvidence(t *testing.T) {
	const numFields = eventBurstCeiling + 3
	f := &fakeCluster{
		forProvider: map[string]interface{}{testFieldNotifyDelay: float64(0)},
		atProvider:  map[string]interface{}{testFieldNotifyDelay: float64(0)},
		generation:  1,
		kind:        testKindARecord,
		name:        testNameARecord,
		// recordUpdateEvent left false: no field test's patch ever produces
		// an update event, simulating a reconciler whose Update() path
		// never runs at all.
	}
	r := newFakeRunner(f)

	m := manifestWithSequentialFieldTests(numFields)
	results, err := r.RunTests(m)
	if err != nil {
		t.Fatalf("RunTests: unexpected error: %v", err)
	}

	for i, res := range results {
		if !res.NotEvidenced {
			t.Errorf("field %d (%s): got NotEvidenced=false, want true (a controller with no Update() events must still fail)", i, res.Field)
		}
		if res.Passed {
			t.Errorf("field %d (%s): got Passed=true, want false", i, res.Field)
		}
	}
}

// TestRunTestsBelowCeilingNeverRestarts confirms the proactive reset is
// scoped to the burst ceiling: a run whose non-skipped, non-no-op field
// count never reaches eventBurstCeiling must never pay the restart cost.
func TestRunTestsBelowCeilingNeverRestarts(t *testing.T) {
	const numFields = eventBurstCeiling - 1
	f := &fakeCluster{
		forProvider:       map[string]interface{}{testFieldNotifyDelay: float64(0)},
		atProvider:        map[string]interface{}{testFieldNotifyDelay: float64(0)},
		generation:        1,
		kind:              testKindARecord,
		name:              testNameARecord,
		recordUpdateEvent: true,
	}
	r := newFakeRunner(f)

	m := manifestWithSequentialFieldTests(numFields)
	if _, err := r.RunTests(m); err != nil {
		t.Fatalf("RunTests: unexpected error: %v", err)
	}

	if f.restartCalls != 0 {
		t.Errorf("expected 0 restarts for a run below the burst ceiling, got %d", f.restartCalls)
	}
}

// TestRunTestsRestartFailureDoesNotAbortRun confirms a failed burst reset
// degrades gracefully: the run continues (later fields may lose evidence,
// but the whole test suite must not abort) rather than the tool crashing or
// silently swallowing the rest of the manifest.
func TestRunTestsRestartFailureDoesNotAbortRun(t *testing.T) {
	const numFields = eventBurstCeiling + 2
	f := &fakeCluster{
		forProvider:       map[string]interface{}{testFieldNotifyDelay: float64(0)},
		atProvider:        map[string]interface{}{testFieldNotifyDelay: float64(0)},
		generation:        1,
		kind:              testKindARecord,
		name:              testNameARecord,
		recordUpdateEvent: true,
		eventBudget:       eventBurstCeiling,
	}
	r := newFakeRunner(f)
	r.restartFunc = func() error {
		return fmt.Errorf("simulated rollout failure")
	}

	m := manifestWithSequentialFieldTests(numFields)
	results, err := r.RunTests(m)
	if err != nil {
		t.Fatalf("RunTests: unexpected error: %v", err)
	}
	if len(results) != numFields {
		t.Fatalf("got %d results, want %d — a failed restart must not abort the run", len(results), numFields)
	}
}
