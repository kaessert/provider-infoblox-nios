package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Event reasons emitted by the crossplane-runtime managed reconciler on
// every external.Update() call.
const (
	eventReasonUpdated      = "UpdatedExternalResource"
	eventReasonCannotUpdate = "CannotUpdateExternalResource"
)

// ConvergeOptions configures a convergence check run.
type ConvergeOptions struct {
	// PollInterval is the provider's poll interval; the check waits
	// PollInterval * 1.5 to guarantee at least one full reconcile cycle.
	PollInterval time.Duration
	// IgnoreFields excludes named atProvider fields from the snapshot diff
	// (e.g. server timestamps, rolling counters).
	IgnoreFields []string
	// Timeout bounds the pre-check that waits for generation to settle to
	// observedGeneration.
	Timeout time.Duration
}

// ConvergeResult holds the outcome of a convergence check.
type ConvergeResult struct {
	Skipped     bool
	SkipMsg     string
	Passed      bool
	Message     string
	Diagnostics []string
}

// RunConverge executes the post-create convergence check: it asserts the
// resource reaches steady state after creation with zero spurious Update
// calls.
//
// Algorithm:
//  1. Resolve the resource from the manifest.
//  2. PRE-CHECK: poll until metadata.generation == status.conditions[].observedGeneration.
//  3. RECORD: snapshot atProvider, generation, and update-event count.
//  4. WAIT: pollInterval * 1.5.
//  5. ASSERT: atProvider unchanged, zero new update events, generation unchanged.
func (r *Runner) RunConverge(m *Manifest, opts ConvergeOptions) (*ConvergeResult, error) {
	if m.ConvergeSkip != "" {
		return &ConvergeResult{Skipped: true, SkipMsg: m.ConvergeSkip}, nil
	}

	if err := r.ResolveResource(m); err != nil {
		return nil, err
	}

	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 120 * time.Second
	}

	settled, gen, obsGen, err := r.waitGenerationSettled(timeout)
	if err != nil {
		return nil, fmt.Errorf("pre-check: %w", err)
	}
	if !settled {
		return &ConvergeResult{
			Passed:  false,
			Message: "RECONCILIATION LOOP DETECTED",
			Diagnostics: []string{
				fmt.Sprintf("pre-check: generation (%d) did not settle to observedGeneration (%d) within %s", gen, obsGen, timeout),
			},
		}, nil
	}

	before, beforeEvents, err := r.recordConvergeBaseline(m)
	if err != nil {
		return nil, err
	}

	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = 60 * time.Second
	}
	waitDur := time.Duration(float64(pollInterval) * 1.5)
	time.Sleep(waitDur)

	after, afterEvents, afterGen, err := r.recordConvergeOutcome(m)
	if err != nil {
		return nil, err
	}

	diff, err := DiffSnapshotsExcluding(before, after, opts.IgnoreFields)
	if err != nil {
		return nil, fmt.Errorf("diff: %w", err)
	}

	return buildConvergeResult(diff, gen, afterGen, beforeEvents, afterEvents), nil
}

// recordConvergeBaseline snapshots atProvider and the update-event count
// before the convergence wait begins.
func (r *Runner) recordConvergeBaseline(m *Manifest) (snapshot []byte, events int, err error) {
	snapshot, err = r.Snapshot()
	if err != nil {
		return nil, 0, fmt.Errorf("recording snapshot: %w", err)
	}
	events, err = r.countUpdateEvents(m.Kind, m.Name)
	if err != nil {
		return nil, 0, fmt.Errorf("counting events: %w", err)
	}
	return snapshot, events, nil
}

// recordConvergeOutcome snapshots atProvider, the update-event count, and the
// generation after the convergence wait completes.
func (r *Runner) recordConvergeOutcome(m *Manifest) (snapshot []byte, events int, gen int64, err error) {
	snapshot, err = r.Snapshot()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("post-wait snapshot: %w", err)
	}
	events, err = r.countUpdateEvents(m.Kind, m.Name)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("counting events: %w", err)
	}
	gen, err = r.GetGeneration()
	if err != nil {
		return nil, 0, 0, fmt.Errorf("reading generation: %w", err)
	}
	return snapshot, events, gen, nil
}

// buildConvergeResult evaluates the pre/post snapshots, event counts, and
// generations, and assembles the final ConvergeResult.
func buildConvergeResult(diff []FieldChange, gen, afterGen int64, beforeEvents, afterEvents int) *ConvergeResult {
	var problems []string
	passed := true

	if len(diff) > 0 {
		passed = false
		problems = append(problems, fmt.Sprintf("atProvider changed: %s", FormatChanges(diff)))
	}
	if afterEvents > beforeEvents {
		passed = false
		problems = append(problems, fmt.Sprintf("%d new update event(s) observed (%s/%s)",
			afterEvents-beforeEvents, eventReasonUpdated, eventReasonCannotUpdate))
	}
	if afterGen != gen {
		passed = false
		problems = append(problems, fmt.Sprintf("generation changed: %d → %d", gen, afterGen))
	}

	result := &ConvergeResult{Passed: passed}
	if passed {
		result.Message = fmt.Sprintf("resource stable (1 cycle observed, %d updates)", afterEvents-beforeEvents)
	} else {
		result.Message = "RECONCILIATION LOOP DETECTED"
		result.Diagnostics = problems
	}
	return result
}

// waitGenerationSettled polls the resource until metadata.generation equals
// the minimum observedGeneration across status.conditions, or timeout
// elapses. crossplane-runtime's ObservedGenerationPropagationManager sets
// every condition's observedGeneration to metadata.generation whenever any
// condition is (re)computed, so once the controller has fully reconciled,
// all conditions carry the same, current generation.
func (r *Runner) waitGenerationSettled(timeout time.Duration) (settled bool, gen int64, obsGen int64, err error) {
	deadline := time.Now().Add(timeout)
	for {
		obj, gerr := r.GetObject()
		if gerr != nil {
			return false, 0, 0, gerr
		}
		g, gerr := extractGeneration(obj)
		if gerr != nil {
			return false, 0, 0, gerr
		}
		gen = g
		if og, found := extractObservedGeneration(obj); found {
			obsGen = og
			if gen == obsGen {
				return true, gen, obsGen, nil
			}
		}
		if time.Now().After(deadline) {
			return false, gen, obsGen, nil
		}
		time.Sleep(2 * time.Second)
	}
}

// countUpdateEvents counts events for the given involvedObject kind/name
// whose reason is UpdatedExternalResource or CannotUpdateExternalResource.
// Queries across all namespaces because cluster-scoped managed resources
// may have their events recorded outside the resource's own namespace.
func (r *Runner) countUpdateEvents(kind, name string) (int, error) {
	out, err := r.runRaw("get", "events", "--all-namespaces", "-o", "json")
	if err != nil {
		return 0, fmt.Errorf("listing events: %w", err)
	}

	var list struct {
		Items []struct {
			Reason         string `json:"reason"`
			InvolvedObject struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"involvedObject"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &list); err != nil {
		return 0, fmt.Errorf("parsing events JSON: %w", err)
	}

	count := 0
	for _, it := range list.Items {
		if it.InvolvedObject.Kind != kind || it.InvolvedObject.Name != name {
			continue
		}
		if it.Reason == eventReasonUpdated || it.Reason == eventReasonCannotUpdate {
			count++
		}
	}
	return count, nil
}

// toInt64 converts a decoded-JSON numeric value (float64 from
// encoding/json, or occasionally int/int64) to int64.
func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case float64:
		return int64(n), nil
	case int64:
		return n, nil
	case int:
		return int64(n), nil
	default:
		return 0, fmt.Errorf("unexpected numeric type %T", v)
	}
}

// extractGeneration reads metadata.generation from a decoded resource object.
func extractGeneration(obj map[string]interface{}) (int64, error) {
	md, ok := obj["metadata"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("metadata not found or not an object")
	}
	genRaw, ok := md["generation"]
	if !ok {
		return 0, fmt.Errorf("metadata.generation not found")
	}
	return toInt64(genRaw)
}

// extractObservedGeneration reads the minimum observedGeneration across all
// status.conditions entries. Returns found=false if status.conditions is
// absent, empty, or none of its entries carry observedGeneration (e.g. the
// resource has not been reconciled yet).
func extractObservedGeneration(obj map[string]interface{}) (min int64, found bool) {
	status, ok := obj["status"].(map[string]interface{})
	if !ok {
		return 0, false
	}
	condsRaw, ok := status["conditions"].([]interface{})
	if !ok || len(condsRaw) == 0 {
		return 0, false
	}

	min = -1
	for _, cRaw := range condsRaw {
		c, ok := cRaw.(map[string]interface{})
		if !ok {
			continue
		}
		ogRaw, ok := c["observedGeneration"]
		if !ok {
			continue
		}
		og, err := toInt64(ogRaw)
		if err != nil {
			continue
		}
		if min == -1 || og < min {
			min = og
		}
	}
	if min == -1 {
		return 0, false
	}
	return min, true
}
