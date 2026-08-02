// update-tester is a CLI tool that reads Crossplane example manifests and
// runs per-field update tests, offline mutable-field coverage checks, and
// post-create convergence checks against a live cluster to validate the
// Update() reconciler path.
//
// Update-path proof: each `run` field test forces a second, independent
// reconcile after the one that calls Update(), so status.atProvider is
// refreshed by a genuine post-update Observe instead of depending on the
// provider's background poll tick. The second reconcile is triggered by
// patching a private metadata annotation (NudgeReconcile) rather than by
// repeating the status-conditions clear used to drive the first one: most
// generated controllers watch with resource.DesiredStateChanged(), which
// only reacts to an annotation, label, or generation (spec) change, so a
// status-only patch alone would be filtered out and never reach the
// reconciler. It also counts the aggregated UpdatedExternalResource /
// CannotUpdateExternalResource events for the resource before and after the
// patch — a field whose value matches the target but whose event count
// never grew is reported as NOT-EVIDENCED, not PASS, because a value match
// without an event is not proof Update() ran. A field that still takes
// poll-interval-scale time to converge despite all of this (result.Duration
// >= slowObserveThreshold) is annotated "slow-observe" rather than left
// looking like an ordinary fast PASS — this should only happen when the
// backend itself is slow to propagate the change, since the forced second
// reconcile removes the timing race that used to produce it. If a
// slow-observe result appears alongside a low update-event count, treat it
// as a real signal and check the controller logs for repeated Update calls
// rather than assuming it is a benign propagation delay.
//
// Usage:
//
//	update-tester run <manifest.yaml> [--timeout 120]
//	update-tester validate <manifest.yaml> --types-file <path_to_types.go>
//	update-tester converge <manifest.yaml> [--poll-interval 60s] [--ignore-fields a,b] [--timeout 120s]
//	update-tester check-external-name-prefix <manifest.yaml> [--timeout 30]
//	update-tester resolve-recover <manifest.yaml> [--timeout 120]
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "run":
		if err := cmdRun(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "validate":
		if err := cmdValidate(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "converge":
		if err := cmdConverge(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "check-external-name-prefix":
		if err := cmdCheckExternalNamePrefix(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "resolve-recover":
		if err := cmdResolveRecover(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `update-tester — Crossplane per-field update E2E tester

Usage:
  update-tester run <manifest.yaml> [--timeout 120]
  update-tester validate <manifest.yaml> --types-file <types.go>
  update-tester converge <manifest.yaml> [--poll-interval 60s] [--ignore-fields a,b] [--timeout 120s]
  update-tester check-external-name-prefix <manifest.yaml> [--timeout 30]
  update-tester resolve-recover <manifest.yaml> [--timeout 120]

Commands:
  run        Execute update tests against a live cluster
  validate   Check annotation coverage against Go type definitions
  converge   Assert the resource reaches steady state after creation
             with zero spurious Update calls
  check-external-name-prefix
             Assert the live resource's crossplane.io/external-name
             annotation has the prefix declared by the manifest's
             crossplane.io/expect-external-name-prefix annotation. For
             dual-object-type resources, where a wrong identity search
             silently resolves against the other WAPI object type.
  resolve-recover
             Pause reconciliation, strip the crossplane.io/external-name
             annotation, unpause, and assert the controller recovers to
             the SAME backend object (exactly one CreatedExternalResource
             event across the resource's lifecycle) rather than silently
             creating a duplicate. Exercises the ref-less identity-search
             path a standing ref-addressed lifecycle never reaches.`)
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	timeout := fs.Int("timeout", 120, "Timeout in seconds for kubectl wait")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: update-tester run <manifest.yaml> [--timeout 120]")
	}
	manifestPath := fs.Arg(0)

	m, err := ParseManifest(manifestPath)
	if err != nil {
		return err
	}
	if len(m.Tests) == 0 {
		return fmt.Errorf("no %s annotation found in manifest", annotationKey)
	}

	// Count tested vs skipped.
	var tested, skipped int
	for _, t := range m.Tests {
		if t.Skip != "" {
			skipped++
		} else {
			tested++
		}
	}

	fmt.Printf("Testing %s/%s (%d fields, %d skipped)\n",
		m.Kind, m.Name, len(m.Tests), skipped)

	runner := NewRunner(manifestPath, *timeout)
	results, err := runner.RunTests(m)
	if err != nil {
		return err
	}

	passed, failed, noop, notEvidenced := printResults(results)

	total := passed + failed
	fmt.Printf("%s: %d/%d tested, %d/%d skipped, %d no-op, %d not-evidenced\n",
		verdict(failed == 0), passed, total, skipped, len(m.Tests), noop, notEvidenced)

	if failed > 0 {
		os.Exit(1)
	}
	return nil
}

// printResults prints one line per test result (plus any side effects) and
// returns the passed/failed counts, plus the no-op and not-evidenced counts
// (both subsets of failed, reported separately so each distinct failure mode
// is easy to spot in the summary line without being confused with a genuine
// PASS or SKIP). A PASS whose field converged at or above slowObserveThreshold
// is annotated "slow-observe" inline — it is still a PASS backed by positive
// update-event evidence, not a reason for a reviewer to suspect the result.
func printResults(results []TestResult) (passed, failed, noop, notEvidenced int) {
	var hasSideFx bool
	for _, r := range results {
		switch {
		case r.Skipped:
			fmt.Printf("  ⊘ %s: SKIPPED (%s)\n", r.Field, r.SkipMsg)
			continue
		case r.NoOp:
			fmt.Printf("  ⦸ %s: NO-OP (%v) (%s)\n", r.Field, r.Error, fmtDuration(r.Duration))
			failed++
			noop++
		case r.NotEvidenced:
			fmt.Printf("  ⚡ %s: NOT-EVIDENCED (%v) (%s)\n", r.Field, r.Error, fmtDuration(r.Duration))
			failed++
			notEvidenced++
		case r.Error != nil:
			fmt.Printf("  ✗ %s: ERROR (%v) (%s)\n", r.Field, r.Error, fmtDuration(r.Duration))
			failed++
		case r.Passed && r.SlowObserve:
			fmt.Printf("  ✓ %s: %q → %q (%s, slow-observe)\n",
				r.Field, r.Expected, r.Actual, fmtDuration(r.Duration))
			passed++
		case r.Passed:
			fmt.Printf("  ✓ %s: %q → %q (%s)\n",
				r.Field, r.Expected, r.Actual, fmtDuration(r.Duration))
			passed++
		default:
			fmt.Printf("  ✗ %s: expected %q, got %q (%s)\n",
				r.Field, r.Expected, r.Actual, fmtDuration(r.Duration))
			failed++
		}
		if len(r.SideFx) > 0 {
			hasSideFx = true
			printSideEffects(r.SideFx)
		}
	}

	fmt.Println()
	if !hasSideFx {
		fmt.Println("  Differential: all non-target fields stable ✓")
		fmt.Println()
	}
	return passed, failed, noop, notEvidenced
}

// printSideEffects prints the fields that changed unexpectedly alongside a
// target field update.
func printSideEffects(changes []FieldChange) {
	fmt.Printf("    ⚠ side effects: ")
	parts := make([]string, 0, len(changes))
	for _, c := range changes {
		parts = append(parts, fmt.Sprintf("%s: %s → %s", c.Field, c.OldValue, c.NewValue))
	}
	fmt.Println(strings.Join(parts, ", "))
}

func cmdValidate(args []string) error {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	typesFile := fs.String("types-file", "", "Path to Go types file containing Parameters struct")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: update-tester validate <manifest.yaml> --types-file <types.go>")
	}
	if *typesFile == "" {
		return fmt.Errorf("--types-file is required")
	}
	manifestPath := fs.Arg(0)

	m, err := ParseManifest(manifestPath)
	if err != nil {
		return err
	}

	fields, err := ParseGoTypes(*typesFile, m.Kind)
	if err != nil {
		return err
	}

	result := ValidateManifest(m, fields)
	PrintValidation(result)

	if !result.AllGood {
		os.Exit(1)
	}
	return nil
}

func cmdConverge(args []string) error {
	fs := flag.NewFlagSet("converge", flag.ExitOnError)
	pollInterval := fs.Duration("poll-interval", 60*time.Second, "Provider poll interval; determines wait duration")
	ignoreFields := fs.String("ignore-fields", "", "Comma-separated atProvider fields excluded from snapshot diff")
	timeout := fs.Duration("timeout", 120*time.Second, "Max time for the pre-check to settle")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: update-tester converge <manifest.yaml> [--poll-interval 60s] [--ignore-fields a,b] [--timeout 120s]")
	}
	manifestPath := fs.Arg(0)

	m, err := ParseManifest(manifestPath)
	if err != nil {
		return err
	}

	var ignore []string
	if *ignoreFields != "" {
		ignore = strings.Split(*ignoreFields, ",")
	}

	runner := NewRunner(manifestPath, int(timeout.Seconds()))
	result, err := runner.RunConverge(m, ConvergeOptions{
		PollInterval: *pollInterval,
		IgnoreFields: ignore,
		Timeout:      *timeout,
	})
	if err != nil {
		return err
	}

	printConvergeResult(m, result)

	if result.Skipped {
		return nil
	}
	if !result.Passed {
		os.Exit(1)
	}
	return nil
}

// cmdCheckExternalNamePrefix asserts that the live resource's
// crossplane.io/external-name annotation has the prefix the manifest
// declares via crossplane.io/expect-external-name-prefix. This exists for
// dual-object-type WAPI resources (e.g. Network models both "network" and
// "ipv6network") where an identity search issued against the wrong type
// returns zero matches and the reconciler silently creates a duplicate —
// a failure invisible to a plain Ready assertion. See
// checkExternalNamePrefix for the underlying (pure, unit-testable) check.
func cmdCheckExternalNamePrefix(args []string) error {
	fs := flag.NewFlagSet("check-external-name-prefix", flag.ExitOnError)
	timeout := fs.Int("timeout", 30, "Timeout in seconds for kubectl calls")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: update-tester check-external-name-prefix <manifest.yaml> [--timeout 30]")
	}
	manifestPath := fs.Arg(0)

	m, err := ParseManifest(manifestPath)
	if err != nil {
		return err
	}
	if m.ExpectExternalNamePrefix == "" {
		return fmt.Errorf("manifest has no %s annotation — nothing to check", expectExternalNamePrefixKey)
	}

	runner := NewRunner(manifestPath, *timeout)
	if err := runner.ResolveResource(m); err != nil {
		return err
	}

	name, err := runner.ExternalName()
	if err != nil {
		return err
	}

	ok, reason := checkExternalNamePrefix(name, m.ExpectExternalNamePrefix)
	fmt.Printf("External-name prefix check: %s/%s\n", m.Kind, m.Name)
	if !ok {
		fmt.Printf("  ✗ %s\n", reason)
		os.Exit(1)
	}
	fmt.Printf("  ✓ external-name %q has expected prefix %q\n", name, m.ExpectExternalNamePrefix)
	return nil
}

// cmdResolveRecover asserts that a dual-object-type resource recovers its
// identity via search (rather than duplicating) when its
// crossplane.io/external-name annotation is stripped and reconciliation is
// resumed. See Runner.RunResolveRecover for the full algorithm and the
// rationale for the two independent pass signals.
func cmdResolveRecover(args []string) error {
	fs := flag.NewFlagSet("resolve-recover", flag.ExitOnError)
	timeout := fs.Int("timeout", 120, "Timeout in seconds for kubectl wait")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: update-tester resolve-recover <manifest.yaml> [--timeout 120]")
	}
	manifestPath := fs.Arg(0)

	m, err := ParseManifest(manifestPath)
	if err != nil {
		return err
	}

	runner := NewRunner(manifestPath, *timeout)
	result, err := runner.RunResolveRecover(m)
	if err != nil {
		return err
	}

	printResolveRecoverResult(m, result)

	if !result.Passed {
		os.Exit(1)
	}
	return nil
}

// printResolveRecoverResult prints the outcome of a resolve-recover check.
func printResolveRecoverResult(m *Manifest, r *ResolveRecoverResult) {
	fmt.Printf("Resolve-recover check: %s/%s\n", m.Kind, m.Name)
	fmt.Printf("  external-name before strip: %q\n", r.ExternalNameBefore)
	fmt.Printf("  external-name after recovery: %q\n", r.ExternalNameAfter)
	fmt.Printf("  %s events across lifecycle: %d\n", eventReasonCreated, r.CreateEventCount)
	switch {
	case r.Passed:
		fmt.Printf("  ✓ recovery: %s\n", r.Message)
	default:
		fmt.Printf("  ✗ recovery: %s\n", r.Message)
		for _, d := range r.Diagnostics {
			fmt.Printf("    - %s\n", d)
		}
	}
}

// printConvergeResult prints the outcome of a convergence check.
func printConvergeResult(m *Manifest, r *ConvergeResult) {
	fmt.Printf("Converge check: %s/%s\n", m.Kind, m.Name)
	switch {
	case r.Skipped:
		fmt.Printf("  ⊘ CONVERGE-SKIP: %s\n", r.SkipMsg)
	case r.Passed:
		fmt.Printf("  ✓ converge: %s\n", r.Message)
	default:
		fmt.Printf("  ✗ converge: %s\n", r.Message)
		for _, d := range r.Diagnostics {
			fmt.Printf("    - %s\n", d)
		}
	}
}

func fmtDuration(d time.Duration) string {
	return fmt.Sprintf("%.0fs", d.Seconds())
}

func verdict(pass bool) string {
	if pass {
		return "PASS"
	}
	return "FAIL"
}
