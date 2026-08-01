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

// fakeCluster is an in-memory stand-in for a live cluster's view of a single
// managed resource. It implements the subset of kubectl behaviour Runner
// depends on (get -o json, get -o jsonpath=..., patch, wait) so runFieldTest
// can be exercised without a real cluster.
type fakeCluster struct {
	forProvider map[string]interface{}
	atProvider  map[string]interface{}
	generation  int64

	patchCalls int
	waitCalls  int
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
	for _, a := range args {
		if strings.HasPrefix(a, "jsonpath=") {
			b, err := json.Marshal(f.atProvider)
			if err != nil {
				return "", err
			}
			return string(b), nil
		}
	}
	obj := map[string]interface{}{
		"metadata":    map[string]interface{}{"generation": f.generation},
		"spec":        map[string]interface{}{"forProvider": f.forProvider},
		jsonKeyStatus: map[string]interface{}{jsonKeyAtProvider: f.atProvider},
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(b), nil
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
	specRaw, _ := patch["spec"].(map[string]interface{})
	forProviderRaw, _ := specRaw["forProvider"].(map[string]interface{})

	mergeInto(f.forProvider, forProviderRaw)
	// Simulate a controller that converges instantly, so pollField's first
	// read already matches — these tests are about the no-op guard, not
	// about polling behaviour.
	mergeInto(f.atProvider, forProviderRaw)

	f.generation++
	f.patchCalls++
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
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: testFieldNotifyDelay, Value: 10}
	result, newSnapshot := r.runFieldTest(test, snapshot)

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
// normally, producing a genuine PASS.
func TestRunFieldTestExecutesWhenValueDiffers(t *testing.T) {
	f := &fakeCluster{
		forProvider: map[string]interface{}{testFieldNotifyDelay: float64(5)},
		atProvider:  map[string]interface{}{testFieldNotifyDelay: float64(5)},
		generation:  1,
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: testFieldNotifyDelay, Value: 10}
	result, newSnapshot := r.runFieldTest(test, snapshot)

	if result.NoOp {
		t.Fatalf("expected NoOp=false when the pre-patch value differs, got %+v", result)
	}
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if !result.Passed {
		t.Fatalf("expected the update test to pass, got %+v", result)
	}
	if f.patchCalls != 1 {
		t.Errorf("expected exactly 1 patch call, got %d", f.patchCalls)
	}
	if f.waitCalls != 1 {
		t.Errorf("expected exactly 1 wait call, got %d", f.waitCalls)
	}
	if bytes.Equal(newSnapshot, snapshot) {
		t.Error("expected the post-patch snapshot to differ from the input snapshot")
	}
	if len(result.SideFx) != 0 {
		t.Errorf("expected no side effects, got %v", result.SideFx)
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
	}
	r := newFakeRunner(f)
	snapshot, err := json.Marshal(f.atProvider)
	if err != nil {
		t.Fatalf("marshalling snapshot: %v", err)
	}

	test := UpdateTest{Field: "dnssecValidationEnabled", Value: true, Expect: true}
	result, _ := r.runFieldTest(test, snapshot)

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
