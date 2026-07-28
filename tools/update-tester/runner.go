package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
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
	if val == nil {
		return "", nil
	}

	// For strings, return unquoted (consistent with kubectl jsonpath behaviour
	// and how YAML annotation values are represented).
	if s, ok := val.(string); ok {
		return s, nil
	}

	// For numbers (float64 from JSON), booleans, maps, and arrays: return JSON.
	b, err := json.Marshal(val)
	if err != nil {
		return "", fmt.Errorf("marshalling field %s for comparison: %w", field, err)
	}
	return string(b), nil
}

// navigateAtProvider navigates a resource JSON object to
// status.atProvider.<dot-separated-field> and returns the value found there.
// Returns nil, nil when any intermediate segment is missing.
func navigateAtProvider(obj map[string]interface{}, field string) (interface{}, error) {
	status, ok := obj["status"]
	if !ok {
		return nil, nil
	}
	statusMap, ok := status.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("status is not a JSON object")
	}
	atProvider, ok := statusMap["atProvider"]
	if !ok {
		return nil, nil
	}
	atProviderMap, ok := atProvider.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("status.atProvider is not a JSON object")
	}

	parts := strings.Split(field, ".")
	var curr interface{} = atProviderMap
	for _, part := range parts {
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
	Field    string
	Skipped  bool
	SkipMsg  string
	Passed   bool
	Expected string
	Actual   string
	Duration time.Duration
	Error    error
	SideFx   []FieldChange
}

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
		result, snapshot = r.runFieldTest(t, snapshot)
		results = append(results, result)
	}

	return results, nil
}

// runFieldTest executes a single (non-skipped) update test: it patches the
// field, waits for the controller to reconcile, polls status.atProvider for
// the expected value, and runs the differential assertion against the prior
// snapshot. It returns the test result and the snapshot to use for the next
// test (unchanged from the input snapshot if the test aborted early).
func (r *Runner) runFieldTest(t UpdateTest, snapshot []byte) (TestResult, []byte) {
	start := time.Now()
	result := TestResult{Field: t.Field}

	// Patch FIRST — changes spec.forProvider, increments generation.
	if err := r.Patch(t.Field, t.Value); err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result, snapshot
	}

	// Clear status conditions AFTER patching. This forces WaitReady
	// to block until the controller reconciles. The controller will
	// see the new spec (from the patch above) when it re-establishes
	// the conditions. Doing it in this order eliminates a race where
	// the controller could restore Ready with the OLD spec if we
	// cleared before patching.
	if err := r.ClearConditions(); err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result, snapshot
	}

	// Wait for Ready — blocks until controller reconciles with new spec
	if err := r.WaitReady(); err != nil {
		result.Error = err
		result.Duration = time.Since(start)
		return result, snapshot
	}

	// Determine expected value for comparison and display.
	// Use JSON equality (jsonEqual) to handle complex types (maps, arrays)
	// where fmt.Sprintf("%v") produces Go-format strings (map[key:val])
	// that don't match the JSON returned by kubectl.
	expectedVal := t.Value
	if t.Expect != nil {
		expectedVal = t.Expect
	}
	expected := formatExpected(expectedVal)

	actual, err := r.pollField(t.Field, expectedVal, start)
	if err != nil {
		result.Error = err
	}

	result.Expected = expected
	result.Actual = actual
	result.Passed = jsonEqual(expectedVal, actual)
	result.Duration = time.Since(start)

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

// exec runs the configured kubectl binary with args and returns stdout.
func (r *Runner) exec(args ...string) (string, error) {
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
