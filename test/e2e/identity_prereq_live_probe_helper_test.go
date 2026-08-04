// Package e2e — this file pins down the behavior of
// remove_scratch_build_images(), the scratch-build-image cleanup helper
// inside identity-prereq-live-probe.sh. The rest of that script only runs
// against a live Grid and a kind cluster (see its own header comment), so
// this test isolates and drives just the helper — the one piece that can
// (and, per the defect this test guards against, did) turn a failing
// `make build.vars` into a killed shell under `set -euo pipefail`.
package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// probeScriptPath resolves identity-prereq-live-probe.sh relative to this
// test file, so the test works regardless of the caller's working
// directory.
func probeScriptPath(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd(): %v", err)
	}
	return filepath.Join(wd, "identity-prereq-live-probe.sh")
}

// extractFunction pulls a single top-level bash function definition out of
// the probe script by name, from its "name() {" line up to (and including)
// the next line that is exactly "}". This mirrors the ticket's own repro
// recipe ("source remove_scratch_build_images ... and call it") without
// requiring a full source of the script, which would otherwise execute the
// probe's live-Grid setup and INFOBLOX_* credential checks at the top
// level.
func extractFunction(t *testing.T, script, name string) string {
	t.Helper()
	contents, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("reading %s: %v", script, err)
	}
	lines := strings.Split(string(contents), "\n")
	start := -1
	for i, l := range lines {
		if l == name+"() {" {
			start = i
			break
		}
	}
	if start == -1 {
		t.Fatalf("function %s() not found in %s", name, script)
	}
	for i := start + 1; i < len(lines); i++ {
		if lines[i] == "}" {
			return strings.Join(lines[start:i+1], "\n")
		}
	}
	t.Fatalf("function %s() in %s has no closing \"}\" line", name, script)
	return ""
}

// runHelperAgainst writes a self-contained wrapper script — the extracted
// remove_scratch_build_images() body under the SAME `set -euo pipefail`
// the real probe runs under, plus a no-op log() stub — and runs it against
// the given worktree argument. It returns the combined output and whether
// the wrapper reached its own trailing "SURVIVED" marker, which only
// happens if remove_scratch_build_images returned normally instead of
// aborting the shell partway through.
func runHelperAgainst(t *testing.T, worktree string) (output string, survived bool, exitErr error) {
	t.Helper()
	fn := extractFunction(t, probeScriptPath(t), "remove_scratch_build_images")

	wrapper := "#!/usr/bin/env bash\n" +
		"set -euo pipefail\n" +
		"log() { :; }\n" +
		fn + "\n" +
		"remove_scratch_build_images \"$1\"\n" +
		"echo SURVIVED\n"

	script := filepath.Join(t.TempDir(), "wrapper.sh")
	if err := os.WriteFile(script, []byte(wrapper), 0o755); err != nil { //nolint:gosec // test fixture, not shipped
		t.Fatalf("writing wrapper script: %v", err)
	}

	cmd := exec.CommandContext(t.Context(), "bash", script, worktree)
	out, err := cmd.CombinedOutput()
	return string(out), strings.Contains(string(out), "SURVIVED"), err
}

// TestRemoveScratchBuildImagesSurvivesFailingBuildVars is the regression
// test for the defect: a worktree whose build/ submodule was never
// initialized — the exact shape a SIGKILL between `git worktree add` and
// `git submodule update --init --recursive` leaves behind — makes
// `make -C <worktree> build.vars` fail. Before the fix, that failure
// propagated through the unguarded command substitution and `set -e` killed
// the shell mid-helper; `return 0` at the end of the function was never
// reached. The regression fixture deliberately does NOT use a real git
// worktree with an initialized build/ (that path was already safe — see
// TestRemoveScratchBuildImagesNoOpOnAlreadyCleanRegistry) — it targets the
// `make build.vars` step itself, which is the step the original review
// missed.
func TestRemoveScratchBuildImagesSurvivesFailingBuildVars(t *testing.T) {
	// An existing directory with no Makefile reproduces the same failure
	// class as an uninitialized build/ submodule: `make -C <dir> build.vars`
	// exits non-zero because there is nothing to build the target from.
	worktree := t.TempDir()

	out, survived, err := runHelperAgainst(t, worktree)
	if err != nil {
		t.Fatalf("remove_scratch_build_images killed the shell instead of returning normally "+
			"(this is the exact defect: a failing `make build.vars` propagating through "+
			"`set -euo pipefail`): %v\noutput:\n%s", err, out)
	}
	if !survived {
		t.Fatalf("wrapper script did not reach its trailing SURVIVED marker — "+
			"remove_scratch_build_images did not return normally\noutput:\n%s", out)
	}
}

// TestRemoveScratchBuildImagesNoOpOnAlreadyCleanRegistry is the sibling
// already-safe path noted in the ticket: a worktree that does have a
// working `make build.vars` but resolves to a registry with no matching
// images. This was never the defect (verified in the original review), but
// keeping it alongside the failing-build.vars case makes the difference
// between the two explicit: the failure has to be at the `make build.vars`
// step itself, not merely "the helper found nothing to remove".
func TestRemoveScratchBuildImagesNoOpOnAlreadyCleanRegistry(t *testing.T) {
	worktree := t.TempDir()
	// A Makefile whose build.vars target succeeds but yields no
	// BUILD_REGISTRY line — mirrors "already removed / nothing built yet",
	// not a failing `make` invocation.
	makefile := "build.vars:\n\t@true\n"
	if err := os.WriteFile(filepath.Join(worktree, "Makefile"), []byte(makefile), 0o644); err != nil { //nolint:gosec // test fixture, not shipped
		t.Fatalf("writing fixture Makefile: %v", err)
	}

	out, survived, err := runHelperAgainst(t, worktree)
	if err != nil {
		t.Fatalf("remove_scratch_build_images failed on an empty-registry worktree: %v\noutput:\n%s", err, out)
	}
	if !survived {
		t.Fatalf("wrapper script did not reach its trailing SURVIVED marker\noutput:\n%s", out)
	}
}

// callSiteGuardRe matches a remove_scratch_build_images call site that
// carries the "|| true" defence-in-depth guard the ticket requires at both
// the cleanup trap and the startup sweep.
var callSiteGuardRe = regexp.MustCompile(`remove_scratch_build_images\s+"\$\{[A-Za-z_]+\}"\s+\|\|\s+true`)

// TestRemoveScratchBuildImagesCallSitesCarryDefenceInDepthGuard asserts both
// call sites — the cleanup trap and the startup stale-worktree sweep — keep
// their own "|| true" on top of the helper's internal guard. The helper
// being correct on its own is not sufficient: the trap and the sweep are
// exactly the two places a future edit to the helper (or a call added
// elsewhere) could reintroduce this failure mode, so the outer guard is
// asserted independently of the helper's internals.
func TestRemoveScratchBuildImagesCallSitesCarryDefenceInDepthGuard(t *testing.T) {
	contents, err := os.ReadFile(probeScriptPath(t))
	if err != nil {
		t.Fatalf("reading probe script: %v", err)
	}

	matches := callSiteGuardRe.FindAllString(string(contents), -1)
	if len(matches) < 2 {
		t.Fatalf("expected at least 2 call sites of remove_scratch_build_images guarded with "+
			"\"|| true\" (cleanup trap + startup sweep), found %d: %v", len(matches), matches)
	}
}
