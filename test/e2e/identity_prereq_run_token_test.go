// Package e2e — this file pins down RUN_TOKEN's per-process uniqueness in
// identity-prereq-live-probe.sh. RUN_TOKEN keys every scratch artifact the
// probe creates (kubeconfig path, KIND_CLUSTER_NAME, SCRATCH_KEY, and every
// scratch object name), so two probes launched from the same checkout
// inside the same wall-clock second must never mint an identical token —
// if they did, they would collide on all of those artifacts. This test
// extracts the live RUN_TOKEN assignment verbatim from the script and
// evaluates it in two separate processes, so it tracks the script itself
// instead of a retyped copy of the expression that would keep passing
// after the script regressed.
package e2e

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

// runTokenLineRe matches the RUN_TOKEN assignment line inside
// identity-prereq-live-probe.sh, e.g.
//
//	RUN_TOKEN="$(date -u +%Y%m%d%H%M%S)-$$"
var runTokenLineRe = regexp.MustCompile(`(?m)^RUN_TOKEN=.*$`)

// identityProbeScriptPath resolves identity-prereq-live-probe.sh relative
// to this test file, so the test works regardless of the caller's working
// directory.
func identityProbeScriptPath(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd(): %v", err)
	}
	return filepath.Join(wd, "identity-prereq-live-probe.sh")
}

// extractRunTokenLine pulls the RUN_TOKEN assignment line verbatim out of
// the probe script. Extracting it — rather than retyping the expression
// into the test — is the whole point: this test then tracks the script's
// actual content, so it fails the moment a future edit drops the
// per-process entropy, instead of continuing to pass against a stale copy
// of an expression the script no longer contains.
func extractRunTokenLine(t *testing.T, script string) string {
	t.Helper()
	contents, err := os.ReadFile(script)
	if err != nil {
		t.Fatalf("reading %s: %v", script, err)
	}
	line := runTokenLineRe.FindString(string(contents))
	if line == "" {
		t.Fatalf("no RUN_TOKEN=... assignment line found in %s", script)
	}
	return line
}

// mintRunToken writes a throwaway script containing just the given
// RUN_TOKEN assignment line, runs it as its own OS process, and returns
// the minted token. Each invocation is a distinct process, so a "-$$" PID
// suffix in the assignment naturally differs invocation to invocation —
// exactly the property under test.
func mintRunToken(t *testing.T, assignmentLine string) string {
	t.Helper()
	wrapper := "#!/bin/sh\n" +
		assignmentLine + "\n" +
		"printf '%s' \"$RUN_TOKEN\"\n"

	script := filepath.Join(t.TempDir(), "mint-run-token.sh")
	if err := os.WriteFile(script, []byte(wrapper), 0o755); err != nil { //nolint:gosec // test fixture, not shipped
		t.Fatalf("writing wrapper script: %v", err)
	}

	out, err := exec.CommandContext(t.Context(), "sh", script).CombinedOutput()
	if err != nil {
		t.Fatalf("minting RUN_TOKEN via %q: %v\noutput:\n%s", assignmentLine, err, out)
	}
	return string(out)
}

// timestampComponent returns the fixed-width `date -u +%Y%m%d%H%M%S`
// prefix (14 digits) that both the fixed and pre-fix RUN_TOKEN forms
// share, so a same-second pair can be detected regardless of whether a PID
// suffix follows it.
func timestampComponent(t *testing.T, token string) string {
	t.Helper()
	if len(token) < 14 {
		t.Fatalf("token %q is shorter than the 14-digit `date -u +%%Y%%m%%d%%H%%M%%S` prefix it must start with", token)
	}
	return token[:14]
}

// mintSameSecondPair mints RUN_TOKEN twice from the given assignment line,
// retrying until both mints land in the same wall-clock second. Two
// independent `date` calls can straddle a second boundary purely on
// timing, which would make even the fixed script's mints look "distinct"
// for the wrong reason — the retry bounds that flake instead of leaving it
// in the assertion. Bounded so a persistently broken script (or a broken
// wrapper) fails fast instead of spinning.
func mintSameSecondPair(t *testing.T, assignmentLine string) (a, b string) {
	t.Helper()
	const maxAttempts = 40
	for attempt := 0; attempt < maxAttempts; attempt++ {
		a = mintRunToken(t, assignmentLine)
		b = mintRunToken(t, assignmentLine)
		if timestampComponent(t, a) == timestampComponent(t, b) {
			return a, b
		}
	}
	t.Fatalf("could not mint two same-wall-clock-second RUN_TOKEN values in %d attempts (line under test: %s)",
		maxAttempts, assignmentLine)
	return "", ""
}

// TestIdentityProbeRunTokenPerProcessUnique is the regression test: two
// RUN_TOKEN mints from the SAME assignment line, landing in the SAME
// wall-clock second, must still be distinct tokens. This is the property
// the RUN_TOKEN fix added by widening RUN_TOKEN from a bare
// `date -u +%Y%m%d%H%M%S` timestamp to timestamp-PID. If a future edit
// drops the PID component again, two probes launched from the same
// checkout inside the same second collide on every RUN_TOKEN-keyed
// artifact (kubeconfig path, KIND_CLUSTER_NAME, SCRATCH_KEY, and every
// scratch object name) — silently, and only under concurrency.
func TestIdentityProbeRunTokenPerProcessUnique(t *testing.T) {
	line := extractRunTokenLine(t, identityProbeScriptPath(t))
	a, b := mintSameSecondPair(t, line)
	if a == b {
		t.Fatalf("two RUN_TOKEN mints in the same wall-clock second produced an identical token %q — "+
			"RUN_TOKEN has lost its per-process entropy (e.g. a \"-$$\" PID suffix was dropped); "+
			"line under test: %s", a, line)
	}
}

// TestIdentityProbeRunTokenGuardCatchesPreFixRegression proves that
// TestIdentityProbeRunTokenPerProcessUnique's assertion actually guards
// something, rather than merely having been observed to pass once. It
// applies the identical same-second-pair check to the pre-fix RUN_TOKEN
// line — kept here as a fixture, not read from the script, since the
// script has already moved on — and asserts the two mints DO collide: the
// exact failure the RUN_TOKEN fix eliminated.
func TestIdentityProbeRunTokenGuardCatchesPreFixRegression(t *testing.T) {
	const preFixLine = `RUN_TOKEN="$(date -u +%Y%m%d%H%M%S)"`
	a, b := mintSameSecondPair(t, preFixLine)
	if a != b {
		t.Fatalf("expected the pre-fix RUN_TOKEN line (bare timestamp, no PID) to collide on a "+
			"same-second pair, got distinct tokens %q / %q — this fixture no longer reproduces the "+
			"regression it exists to demonstrate", a, b)
	}
}
