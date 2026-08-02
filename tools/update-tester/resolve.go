package main

import (
	"fmt"
)

// pausedAnnotation is the crossplane-runtime well-known annotation that
// pauses reconciliation for a managed resource when set to the string
// "true".
const pausedAnnotation = "crossplane.io/paused"

// ResolveRecoverResult holds the outcome of a ref-less identity-resolve
// recovery check (see RunResolveRecover).
type ResolveRecoverResult struct {
	Passed      bool
	Message     string
	Diagnostics []string
	// ExternalNameBefore/ExternalNameAfter record the resolved
	// crossplane.io/external-name value before the annotation was
	// stripped and after the recovery reconcile, for the printed report.
	ExternalNameBefore string
	ExternalNameAfter  string
	// CreateEventCount is the total CreatedExternalResource event count
	// for the resource across its entire lifecycle, observed after the
	// recovery reconcile completes.
	CreateEventCount int
}

// RunResolveRecover exercises the ref-less identity-resolve path: a standing
// Create -> Update -> Delete lifecycle for a dual-object-type resource
// always carries a non-empty crossplane.io/external-name annotation, so it
// never drives the code path that resolves identity by search rather than
// by stored ref. This check does, by simulating the scenario that does: it
// pauses reconciliation, strips the external-name annotation, unpauses, and
// asserts the controller recovers to the SAME backend object rather than
// silently creating a duplicate.
//
// Two independent signals distinguish recovery from duplication — either
// one failing fails the whole check:
//
//  1. The recovered external-name still carries the manifest's declared
//     object-type prefix (crossplane.io/expect-external-name-prefix).
//  2. Exactly one CreatedExternalResource event has been recorded for the
//     resource across its ENTIRE lifecycle.
//
// Signal 1 alone is NOT sufficient: a wrong-type search that finds zero
// matches and falls through to a duplicate Create() still derives the WAPI
// object type from the CIDR (not from how identity was searched), so the
// duplicate's external-name is ALSO correctly prefixed. Signal 2 is what
// actually distinguishes "found the existing object via search" from
// "found nothing, created a second one" — a duplicate always emits a
// second CreatedExternalResource event that a genuine recovery never does.
func (r *Runner) RunResolveRecover(m *Manifest) (*ResolveRecoverResult, error) {
	if m.ExpectExternalNamePrefix == "" {
		return nil, fmt.Errorf("manifest has no %s annotation — nothing to recover-check", expectExternalNamePrefixKey)
	}

	if err := r.ResolveResource(m); err != nil {
		return nil, err
	}

	before, err := r.ExternalName()
	if err != nil {
		return nil, fmt.Errorf("reading external-name before recovery: %w", err)
	}
	if before == "" {
		return nil, fmt.Errorf("resource has no external-name to strip — run this check after Create, not before")
	}

	if err := r.pause(); err != nil {
		return nil, fmt.Errorf("pausing reconciliation: %w", err)
	}
	if err := r.stripExternalName(); err != nil {
		return nil, fmt.Errorf("stripping external-name: %w", err)
	}
	if err := r.waitForRecoveryReconcile(); err != nil {
		return nil, fmt.Errorf("waiting for recovery reconcile: %w", err)
	}

	after, err := r.ExternalName()
	if err != nil {
		return nil, fmt.Errorf("reading external-name after recovery: %w", err)
	}

	createEvents, err := r.countCreateEvents(m.Kind, m.Name)
	if err != nil {
		return nil, fmt.Errorf("counting create events: %w", err)
	}

	return buildResolveRecoverResult(before, after, m.ExpectExternalNamePrefix, createEvents), nil
}

// buildResolveRecoverResult evaluates the two independent recovery signals
// documented on RunResolveRecover and assembles the final result.
func buildResolveRecoverResult(before, after, expectedPrefix string, createEvents int) *ResolveRecoverResult {
	result := &ResolveRecoverResult{
		ExternalNameBefore: before,
		ExternalNameAfter:  after,
		CreateEventCount:   createEvents,
	}

	var problems []string
	passed := true

	if ok, reason := checkExternalNamePrefix(after, expectedPrefix); !ok {
		passed = false
		problems = append(problems, fmt.Sprintf("recovered external-name failed prefix check: %s", reason))
	}

	if createEvents != 1 {
		passed = false
		problems = append(problems, fmt.Sprintf(
			"%d %s event(s) recorded across the lifecycle, want exactly 1 — a count other than 1 means the ref-less search did not resolve to the existing backend object",
			createEvents, eventReasonCreated))
	}

	result.Passed = passed
	if passed {
		result.Message = fmt.Sprintf("recovered to %q with exactly 1 %s event", after, eventReasonCreated)
	} else {
		result.Message = "RESOLVE-RECOVERY CHECK FAILED"
		result.Diagnostics = problems
	}
	return result
}

// pause sets the crossplane.io/paused annotation so the controller stops
// reconciling this resource, giving stripExternalName a clean window with
// no in-flight reconcile racing it.
func (r *Runner) pause() error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:"true"}}}`, pausedAnnotation)
	_, err := r.run("patch", r.resourceName, "--type=merge", "-p", patch)
	return err
}

// stripExternalName removes the crossplane.io/external-name annotation,
// forcing the next Observe to re-derive the resource's identity from a
// search rather than a stored ref — the code path
// crossplane.io/expect-external-name-prefix exists to guard, which a
// standing ref-addressed lifecycle never reaches. Must be called while the
// resource is paused (see pause) so the strip cannot race an in-flight
// reconcile.
func (r *Runner) stripExternalName() error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, externalNameAnnotation)
	_, err := r.run("patch", r.resourceName, "--type=merge", "-p", patch)
	return err
}

// waitForRecoveryReconcile clears status conditions BEFORE removing the
// paused annotation, mirroring nudgeAndReconcile's ordering rationale: the
// unpause patch below is what actually triggers the recovery reconcile (an
// annotation change satisfies generated controllers'
// resource.DesiredStateChanged() watch predicate, the same mechanism
// NudgeReconcile relies on), so conditions must already be cleared by the
// time it lands — clearing after would risk wiping the fresh Ready
// condition the unpause trigger just produced, forcing WaitReady to fall
// back on the provider's background poll tick.
func (r *Runner) waitForRecoveryReconcile() error {
	if err := r.ClearConditions(); err != nil {
		return err
	}
	if err := r.unpause(); err != nil {
		return err
	}
	return r.WaitReady()
}

// unpause removes the crossplane.io/paused annotation, re-enabling
// reconciliation.
func (r *Runner) unpause() error {
	patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:null}}}`, pausedAnnotation)
	_, err := r.run("patch", r.resourceName, "--type=merge", "-p", patch)
	return err
}
