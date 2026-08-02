package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Runner executes update tests against a live cluster using kubectl.
type Runner struct {
	kubectl      string
	manifestPath string
	resourceName string // cached: e.g. "arecord.recorda.infobloxnios.crossplane.io/example-arecord"
	namespace    string
	timeout      string

	// execFunc, when set, overrides the kubectl invocation used by exec().
	// Tests inject a fake here to simulate kubectl output without a live
	// cluster; production code leaves it nil and exec() shells out for real.
	execFunc func(args []string) (string, error)
}

// NewRunner creates a Runner for the given manifest file.
func NewRunner(manifestPath string, timeout int) *Runner {
	kubectl := os.Getenv("KUBECTL")
	if kubectl == "" {
		kubectl = "kubectl"
	}
	return &Runner{
		kubectl:      kubectl,
		manifestPath: manifestPath,
		timeout:      fmt.Sprintf("%ds", timeout),
	}
}

// ResolveResource uses kubectl to resolve the full resource name from the
// manifest file.
func (r *Runner) ResolveResource(m *Manifest) error {
	out, err := r.run("get", "-f", r.manifestPath, "-o", "name")
	if err != nil {
		return fmt.Errorf("resolving resource from manifest: %w", err)
	}
	r.resourceName = strings.TrimSpace(out)
	if r.resourceName == "" {
		return fmt.Errorf("kubectl returned empty resource name")
	}
	r.namespace = m.Namespace
	return nil
}

// Snapshot reads the current status.atProvider as raw JSON bytes.
func (r *Runner) Snapshot() ([]byte, error) {
	out, err := r.run("get", r.resourceName, "-o", "jsonpath={.status.atProvider}")
	if err != nil {
		return nil, fmt.Errorf("reading status.atProvider: %w", err)
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" {
		return []byte("{}"), nil
	}
	return []byte(trimmed), nil
}

// ClearConditions clears the status conditions so that a subsequent
// WaitReady blocks until the controller re-establishes them. This is
// the same technique uptest uses in 01-update.yaml.tmpl — it creates
// a reliable signal without timing assumptions.
func (r *Runner) ClearConditions() error {
	_, err := r.run("patch", r.resourceName, "--subresource=status",
		"--type=merge", "-p", `{"status":{"conditions":[]}}`)
	if err != nil {
		return fmt.Errorf("clearing conditions: %w", err)
	}
	return nil
}

// updateTesterNudgeAnnotation is patched onto the resource's metadata by
// NudgeReconcile to force an immediate reconcile. It is deliberately not
// under the crossplane.io/ prefix reserved for the runtime's own
// annotations.
const updateTesterNudgeAnnotation = "update-tester.crossplane.io/nudge"

// NudgeReconcile patches a private metadata annotation with a unique value
// to force an immediate controller reconcile.
//
// A pure status-subresource patch (ClearConditions) is not sufficient for
// this: most generated controllers register their watch with
// resource.DesiredStateChanged(), which only reacts to an annotation,
// label, or generation (i.e. spec) change — a status-only write is
// filtered out and never reaches the reconciler. Changing a metadata
// annotation, by contrast, satisfies that predicate and enqueues a
// reconcile through the same path a real spec edit would, without
// touching spec.forProvider (so it cannot itself trigger another
// Update()).
func (r *Runner) NudgeReconcile() error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:%q}}}`,
		updateTesterNudgeAnnotation, strconv.FormatInt(time.Now().UnixNano(), 10))
	if _, err := r.run("patch", r.resourceName, "--type=merge", "-p", patch); err != nil {
		return fmt.Errorf("nudging reconcile: %w", err)
	}
	return nil
}

// Patch applies a JSON merge patch for the given field and value.
func (r *Runner) Patch(field string, value interface{}) error {
	patchJSON, err := buildMergePatch(field, value)
	if err != nil {
		return fmt.Errorf("building patch: %w", err)
	}
	_, err = r.run("patch", r.resourceName, "--type=merge", "-p", patchJSON)
	if err != nil {
		return fmt.Errorf("patching %s: %w", field, err)
	}
	return nil
}

// WaitReady waits for the resource to become Ready.
func (r *Runner) WaitReady() error {
	_, err := r.run("wait", r.resourceName, "--for=condition=Ready", "--timeout="+r.timeout)
	if err != nil {
		return fmt.Errorf("waiting for Ready: %w", err)
	}
	return nil
}

// GetObject reads the full resource as a decoded JSON object.
func (r *Runner) GetObject() (map[string]interface{}, error) {
	out, err := r.run("get", r.resourceName, "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("reading resource: %w", err)
	}
	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &obj); err != nil {
		return nil, fmt.Errorf("parsing resource JSON: %w", err)
	}
	return obj, nil
}

// GetGeneration reads metadata.generation from the live resource.
func (r *Runner) GetGeneration() (int64, error) {
	obj, err := r.GetObject()
	if err != nil {
		return 0, err
	}
	return extractGeneration(obj)
}

// ReadField reads a single field from status.atProvider via kubectl get -o json
// and Go JSON parsing. For complex types (maps, arrays), it returns canonical
// JSON — avoiding the Go-format output that kubectl jsonpath emits for nested
// objects (e.g. "map[key:value]" instead of {"key":"value"}).
// For scalar types (string, number, bool), it returns the unquoted value.
func (r *Runner) ReadField(field string) (string, error) {
	obj, err := r.GetObject()
	if err != nil {
		return "", fmt.Errorf("reading field %s: %w", field, err)
	}

	val, err := navigateAtProvider(obj, field)
	if err != nil {
		return "", err
	}
	return stringifyFieldValue(val, field)
}

// readCurrentValue returns a field's value as it stands BEFORE a patch is
// applied, for no-op detection. It prefers spec.forProvider — the value the
// upcoming merge patch would overwrite, which is what determines whether the
// patch actually changes anything the controller can react to. If the field
// is absent from spec (e.g. it is only ever populated from the live backend),
// it falls back to the resource's live observed state (status.atProvider).
func (r *Runner) readCurrentValue(field string) (string, error) {
	obj, err := r.GetObject()
	if err != nil {
		return "", fmt.Errorf("reading resource for no-op check on %s: %w", field, err)
	}

	val, err := navigateSpecForProvider(obj, field)
	if err == nil && val != nil {
		return stringifyFieldValue(val, field)
	}

	// Fall back to the live observed state.
	atVal, atErr := navigateAtProvider(obj, field)
	if atErr != nil {
		return "", atErr
	}
	return stringifyFieldValue(atVal, field)
}

// stringifyFieldValue converts a decoded-JSON value into the string
// representation used throughout this package for comparisons: strings are
// returned unquoted (consistent with kubectl jsonpath behaviour and how YAML
// annotation values are represented), everything else (numbers, booleans,
// maps, arrays, and nil) is returned as canonical JSON.
func stringifyFieldValue(val interface{}, field string) (string, error) {
	if val == nil {
		return "", nil
	}
	if s, ok := val.(string); ok {
		return s, nil
	}
	b, err := json.Marshal(val)
	if err != nil {
		return "", fmt.Errorf("marshalling field %s for comparison: %w", field, err)
	}
	return string(b), nil
}

// jsonKeyAtProvider is the status subfield holding the last-observed backend
// state. jsonKeyStatus is the top-level status field. Both are constants
// (rather than repeated literals) because they are referenced from this
// file, converge.go, and their tests.
const (
	jsonKeyStatus     = "status"
	jsonKeyAtProvider = "atProvider"
)

// navigateAtProvider navigates a resource JSON object to
// status.atProvider.<dot-separated-field> and returns the value found there.
// Returns nil, nil when any intermediate segment is missing.
func navigateAtProvider(obj map[string]interface{}, field string) (interface{}, error) {
	return navigateJSONPath(obj, []string{jsonKeyStatus, jsonKeyAtProvider}, field)
}

// navigateSpecForProvider navigates a resource JSON object to
// spec.forProvider.<dot-separated-field> and returns the value found there.
// Returns nil, nil when any intermediate segment is missing.
func navigateSpecForProvider(obj map[string]interface{}, field string) (interface{}, error) {
	return navigateJSONPath(obj, []string{"spec", "forProvider"}, field)
}

// navigateJSONPath descends obj through each key in container (e.g.
// ["status", "atProvider"]), then further descends through the
// dot-separated field path under that container. Returns nil, nil when any
// segment — container or field — is missing, and an error if a
// non-terminal segment resolves to something other than a JSON object.
func navigateJSONPath(obj map[string]interface{}, container []string, field string) (interface{}, error) {
	var curr interface{} = obj
	for _, key := range container {
		m, ok := curr.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("%s is not a JSON object", strings.Join(container, "."))
		}
		v, exists := m[key]
		if !exists {
			return nil, nil
		}
		curr = v
	}

	for _, part := range strings.Split(field, ".") {
		m, ok := curr.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("cannot navigate to %q: parent is not a JSON object", part)
		}
		v, exists := m[part]
		if !exists {
			return nil, nil
		}
		curr = v
	}
	return curr, nil
}

// jsonEqual compares an expected Go value (from a YAML annotation) with an
// actual string returned by ReadField.
//
// For string values, it compares directly because ReadField returns unquoted
// string values — the same representation that YAML gives for string scalars.
//
// For complex types (maps, arrays) and numbers, both expected and actual are
// normalised through JSON (marshal → unmarshal) so that type differences
// introduced by YAML parsing (e.g., int vs float64) do not cause false
// failures. reflect.DeepEqual is used on the normalised values.
//
// Falls back to string comparison when JSON normalisation is not possible.
func jsonEqual(expected interface{}, actual string) bool {
	// String scalars: compare directly.
	// ReadField strips JSON quotes from strings, so actual == "hello" not `"hello"`.
	if s, ok := expected.(string); ok {
		return s == actual
	}

	// Marshal expected to canonical JSON.
	expectedBytes, err := json.Marshal(expected)
	if err != nil {
		// Cannot marshal — fall back to Sprintf comparison.
		return fmt.Sprintf("%v", expected) == actual
	}

	// Normalise both through JSON parsing so that numeric types align
	// (YAML gives int, JSON gives float64 for the same numeric literal).
	var expectedNorm, actualNorm interface{}
	if json.Unmarshal(expectedBytes, &expectedNorm) == nil {
		if json.Unmarshal([]byte(actual), &actualNorm) == nil {
			return reflect.DeepEqual(expectedNorm, actualNorm)
		}
	}

	// Fall back to plain string comparison.
	return string(expectedBytes) == actual
}

// formatExpected converts an expected annotation value to a human-readable
// display string. For strings, the value is returned directly. For complex
// types (maps, arrays, numbers), it returns the canonical JSON representation.
func formatExpected(val interface{}) string {
	if s, ok := val.(string); ok {
		return s
	}
	b, err := json.Marshal(val)
	if err != nil {
		return fmt.Sprintf("%v", val)
	}
	return string(b)
}

// TestResult holds the outcome of a single field update test.
type TestResult struct {
	Field string
	// Skipped marks a test that was never attempted because the manifest
	// annotation explicitly opted it out (see SkipMsg for the reason).
	Skipped bool
	SkipMsg string
	// NoOp marks a test whose pre-patch value already equalled the target
	// value: the patch below could not have exercised the Update() path, so
	// this is reported as a distinct failure rather than a false PASS.
	NoOp     bool
	Passed   bool
	Expected string
	Actual   string
	Duration time.Duration
	Error    error
	SideFx   []FieldChange
	// UpdateEvidenced records whether the aggregated count of
	// UpdatedExternalResource/CannotUpdateExternalResource events for this
	// resource increased between the pre-patch baseline and the post-patch
	// check. This is a wall-clock-independent proof that the reconciler's
	// Update() path actually executed — see NotEvidenced for what it means
	// when this is false but the field value still matched.
	UpdateEvidenced bool
	// NotEvidenced marks a test whose observed value matched the target
	// (Passed would otherwise be true) but for which no update event was
	// ever recorded. Convergence timing alone cannot tell "updated
	// promptly, event observed late" apart from "value happened to already
	// match, Update() never ran" — the event count is the deterministic
	// signal, so a match without it is downgraded from PASS to this
	// distinct failure rather than left indistinguishable from a genuine
	// pass.
	NotEvidenced bool
	// SlowObserve marks a PASSing, evidenced field whose total duration met
	// or exceeded slowObserveThreshold. The runner already forces a second,
	// independent reconcile after Update() so status.atProvider is refreshed
	// by a fresh Observe rather than depending on the provider's background
	// poll tick — so this should be rare. When it does happen, it reflects
	// a genuine backend propagation delay rather than an ambiguous result:
	// UpdateEvidenced is already true, so the slow duration is reported as
	// a labelled variant of PASS, not a reason to doubt the verdict.
	SlowObserve bool
}

// slowObserveThreshold is the duration above which a passing, evidenced
// field test is annotated SlowObserve instead of being reported as a plain,
// fast PASS. Chosen as half of the provider's default 60s poll interval —
// comfortably above the couple of seconds a forced reconcile normally takes,
// while still well under a full poll cycle.
const slowObserveThreshold = 30 * time.Second

// RunTests executes all update tests from the manifest and returns results.
func (r *Runner) RunTests(m *Manifest) ([]TestResult, error) {
	if err := r.ResolveResource(m); err != nil {
		return nil, err
	}

	snapshot, err := r.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("initial snapshot: %w", err)
	}

	var results []TestResult
	for _, t := range m.Tests {
		if t.Skip != "" {
			results = append(results, TestResult{
				Field:   t.Field,
				Skipped: true,
				SkipMsg: t.Skip,
			})
			continue
		}

		var result TestResult
		result, snapshot = r.runFieldTest(t, snapshot, m.Kind, m.Name)
		results = append(results, result)
	}

	return results, nil
}

// runFieldTest executes a single (non-skipped) update test: it patches the
// field, waits for the controller to reconcile, forces a second independent
// reconcile so status.atProvider reflects a fresh Observe rather than a
// stale one, polls status.atProvider for the expected value, checks for
// positive event evidence that Update() actually ran, and runs the
// differential assertion against the prior snapshot. kind and name identify
// the resource for the event-evidence lookup. It returns the test result and
// the snapshot to use for the next test (unchanged from the input snapshot
// if the test aborted early).
func (r *Runner) runFieldTest(t UpdateTest, snapshot []byte, kind, name string) (TestResult, []byte) {
	start := time.Now()
	result := TestResult{Field: t.Field}

	// Determine expected value for comparison and display.
	// Use JSON equality (jsonEqual) to handle complex types (maps, arrays)
	// where fmt.Sprintf("%v") produces Go-format strings (map[key:val])
	// that don't match the JSON returned by kubectl.
	expectedVal := t.Value
	if t.Expect != nil {
		expectedVal = t.Expect
	}
	expected := formatExpected(expectedVal)

	// No-op detection: read the field's current value BEFORE patching. A
	// merge patch that repeats the value already in spec.forProvider makes
	// no change for the API server to persist, so metadata.generation never
	// bumps and the controller's Update() path is never invoked. Left
	// undetected, the poll below would simply re-observe the value that was
	// already there and report a false PASS — indistinguishable from a
	// controller with no Update() implementation at all. Report this as a
	// failure instead, distinct from both PASS and SKIP, so the stale test
	// value gets fixed.
	if before, err := r.readCurrentValue(t.Field); err == nil && jsonEqual(t.Value, before) {
		result.NoOp = true
		result.Expected = expected
		result.Actual = before
		result.Error = fmt.Errorf("no-op: %s already equals %s — patch cannot exercise the update path",
			t.Field, formatExpected(t.Value))
		result.Duration = time.Since(start)
		return result, snapshot
	}

	// Evidence baseline: count update-related events BEFORE patching, so a
	// later delta proves whether Update() executed — a signal that does not
	// depend on wall-clock convergence timing. A failure here does not abort
	// the field test (the value-based assertions below are still useful);
	// it only disables the evidence check for this field. countUpdateEvents
	// sums each Event's aggregated .Count field (with a zero-guard treating
	// an absent/zero .Count as one occurrence) — not a raw Item count.
	eventsBefore, eventsBeforeErr := r.countUpdateEvents(kind, name)

	if err := r.applyPatchAndReconcile(t); err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result, snapshot
	}

	actual, err := r.pollField(t.Field, expectedVal, start)
	if err != nil {
		result.Error = err
	}

	result.Expected = expected
	result.Actual = actual
	result.Passed = jsonEqual(expectedVal, actual)
	result.Duration = time.Since(start)

	// Evidence check: did the aggregated update-event count actually grow?
	// A failure to count (e.g. RBAC denies listing events) leaves checked
	// false without downgrading the result — the evidence check could not
	// run either way, which is different from having run and come back
	// empty.
	r.applyEvidenceCheck(&result, kind, name, t.Field, eventsBefore, eventsBeforeErr)

	if result.Passed && result.Duration >= slowObserveThreshold {
		result.SlowObserve = true
	}

	// Differential assertion
	newSnapshot, err := r.Snapshot()
	if err != nil {
		result.Error = fmt.Errorf("post-update snapshot: %w", err)
		return result, snapshot
	}

	// For nested fields, use only the top-level key for diff exclusion.
	topField := t.Field
	if idx := strings.Index(t.Field, "."); idx != -1 {
		topField = t.Field[:idx]
	}

	changes, err := DiffSnapshots(snapshot, newSnapshot, topField)
	if err != nil {
		result.Error = fmt.Errorf("diff: %w", err)
		return result, snapshot
	}
	result.SideFx = changes

	return result, newSnapshot
}

// applyPatchAndReconcile patches the target field, then drives the
// controller through two independent reconciles. The first is the reconcile
// in which Update() actually runs — but its own Observe() ran BEFORE
// Update(), so status.atProvider is still stale when it completes. The
// second is forced purely to obtain a fresh Observe of the now-updated
// external resource, so atProvider does not depend on the provider's
// background poll tick to refresh (which can be a full poll interval away).
// The second reconcile is triggered by NudgeReconcile rather than a repeat
// of ClearConditions: most generated controllers watch with
// resource.DesiredStateChanged(), which reacts only to an annotation,
// label, or generation (spec) change, so a status-only patch on its own
// would be filtered out and never reach the reconciler — leaving the
// "second reconcile" waiting on the same background poll tick this is
// meant to avoid.
func (r *Runner) applyPatchAndReconcile(t UpdateTest) error {
	if err := r.Patch(t.Field, t.Value); err != nil {
		return err
	}
	if err := r.reconcileOnce(); err != nil {
		return err
	}
	return r.nudgeAndReconcile()
}

// reconcileOnce clears status conditions and waits for Ready, so the caller
// can block on the NEXT reconcile's outcome rather than the stale
// conditions already present. Clearing conditions does not by itself
// trigger that next reconcile — see NudgeReconcile and
// applyPatchAndReconcile for what does.
func (r *Runner) reconcileOnce() error {
	if err := r.ClearConditions(); err != nil {
		return err
	}
	return r.WaitReady()
}

// nudgeAndReconcile clears status conditions BEFORE issuing the nudge, the
// reverse order from reconcileOnce. NudgeReconcile's annotation patch can
// trigger a reconcile that completes (Observe + status write) within
// milliseconds — often faster than this process can issue its own next
// kubectl call. Clearing conditions after the nudge would then have a real
// chance of wiping the fresh Ready condition the nudge just produced,
// forcing WaitReady to fall back on the provider's background poll tick —
// exactly the failure mode this second reconcile exists to avoid. Clearing
// first guarantees the clear has already landed before anything can set a
// new condition, so nothing after it can re-clear a fresh result.
func (r *Runner) nudgeAndReconcile() error {
	if err := r.ClearConditions(); err != nil {
		return err
	}
	if err := r.NudgeReconcile(); err != nil {
		return err
	}
	return r.WaitReady()
}

// applyEvidenceCheck runs the event-based update-evidence check and updates
// result in place: it always sets UpdateEvidenced, downgrades a value-match
// PASS to NotEvidenced when the aggregated event count never grew (a value
// match without an event is not proof Update() ran — see evidenceOutcome),
// and records a counting error without overwriting one already present.
func (r *Runner) applyEvidenceCheck(result *TestResult, kind, name, field string, eventsBefore int, eventsBeforeErr error) {
	checked, evidenced, err := r.evidenceOutcome(kind, name, eventsBefore, eventsBeforeErr)
	result.UpdateEvidenced = evidenced
	if err != nil && result.Error == nil {
		result.Error = err
	}
	if !checked || !result.Passed || evidenced {
		return
	}
	result.Passed = false
	result.NotEvidenced = true
	if result.Error == nil {
		result.Error = fmt.Errorf("update not evidenced: no %s/%s event recorded for %s",
			eventReasonUpdated, eventReasonCannotUpdate, field)
	}
}

// evidenceOutcome counts update-related events for (kind, name) and reports
// whether the aggregated count grew relative to eventsBefore — proof that
// Update() executed, independent of wall-clock convergence timing. checked
// is false when the count could not be established (the pre-patch baseline
// errored, or the post-patch recount errored); in that case evidenced is
// meaningless and err explains what went wrong, but the caller should not
// treat the absence of a count as absence of an update.
func (r *Runner) evidenceOutcome(kind, name string, eventsBefore int, eventsBeforeErr error) (checked, evidenced bool, err error) {
	if eventsBeforeErr != nil {
		return false, false, fmt.Errorf("counting update events before patch: %w", eventsBeforeErr)
	}
	eventsAfter, afterErr := r.countUpdateEvents(kind, name)
	if afterErr != nil {
		return false, false, fmt.Errorf("counting update events after patch: %w", afterErr)
	}
	return true, eventsAfter > eventsBefore, nil
}

// pollField polls status.atProvider for the given field until it matches
// expectedVal or the runner's timeout elapses, whichever comes first.
//
// ClearConditions + WaitReady ensures the controller has run at least one
// reconcile, but atProvider may still be stale: the controller sets Ready
// after Update() but before the next Observe() cycle writes fresh data to
// atProvider. Polling covers the gap between the first Ready
// re-establishment and the subsequent Observe that actually refreshes
// atProvider.
func (r *Runner) pollField(field string, expectedVal interface{}, start time.Time) (string, error) {
	pollInterval := 5 * time.Second
	timeoutDur, _ := time.ParseDuration(r.timeout)
	if timeoutDur == 0 {
		timeoutDur = 120 * time.Second
	}
	deadline := start.Add(timeoutDur)

	for {
		actual, err := r.ReadField(field)
		if err != nil {
			return actual, err
		}
		if jsonEqual(expectedVal, actual) {
			return actual, nil
		}
		if time.Now().After(deadline) {
			return actual, nil
		}
		fmt.Fprintf(os.Stderr, "    poll: %s = %q (want %q), retrying in %s...\n",
			field, actual, formatExpected(expectedVal), pollInterval)
		time.Sleep(pollInterval)
	}
}

// run executes a kubectl command scoped to the resource's namespace (if any)
// and returns combined stdout.
func (r *Runner) run(args ...string) (string, error) {
	if r.namespace != "" {
		args = append(args, "-n", r.namespace)
	}
	return r.exec(args...)
}

// runRaw executes a kubectl command without appending a namespace flag. Used
// for cluster-wide queries (e.g. listing events across all namespaces) where
// namespace-scoping the command would miss events recorded elsewhere.
func (r *Runner) runRaw(args ...string) (string, error) {
	return r.exec(args...)
}

// exec runs the configured kubectl binary with args and returns stdout. If
// execFunc is set (tests only), it is used instead of shelling out.
func (r *Runner) exec(args ...string) (string, error) {
	if r.execFunc != nil {
		return r.execFunc(args)
	}
	// #nosec G204 -- r.kubectl is a controlled config value (default
	// "kubectl", overridable only via the KUBECTL env var), not
	// attacker-controlled input.
	cmd := exec.CommandContext(context.Background(), r.kubectl, args...)
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return "", fmt.Errorf("%w: %s", err, string(exitErr.Stderr))
		}
		return "", err
	}
	return string(out), nil
}

// buildMergePatch constructs a JSON merge patch for a dot-separated field path
// under spec.forProvider.
func buildMergePatch(field string, value interface{}) (string, error) {
	parts := strings.Split(field, ".")

	// Build the innermost value and wrap outward.
	var inner = value
	for i := len(parts) - 1; i >= 0; i-- {
		inner = map[string]interface{}{parts[i]: inner}
	}

	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"forProvider": inner,
		},
	}

	data, err := json.Marshal(patch)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
