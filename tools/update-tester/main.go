// update-tester is a CLI tool that reads Crossplane example manifests and
// runs per-field update tests, offline mutable-field coverage checks, and
// post-create convergence checks against a live cluster to validate the
// Update() reconciler path.
//
// Usage:
//
//	update-tester run <manifest.yaml> [--timeout 120]
//	update-tester validate <manifest.yaml> --types-file <path_to_types.go>
//	update-tester converge <manifest.yaml> [--poll-interval 60s] [--ignore-fields a,b] [--timeout 120s]
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

Commands:
  run        Execute update tests against a live cluster
  validate   Check annotation coverage against Go type definitions
  converge   Assert the resource reaches steady state after creation
             with zero spurious Update calls`)
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

	passed, failed, noop := printResults(results)

	total := passed + failed
	fmt.Printf("%s: %d/%d tested, %d/%d skipped, %d no-op\n",
		verdict(failed == 0), passed, total, skipped, len(m.Tests), noop)

	if failed > 0 {
		os.Exit(1)
	}
	return nil
}

// printResults prints one line per test result (plus any side effects) and
// returns the passed/failed counts, plus the no-op count (a subset of
// failed, reported separately so a stale test value is easy to spot in the
// summary line without being confused with a genuine PASS or SKIP).
func printResults(results []TestResult) (passed, failed, noop int) {
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
		case r.Error != nil:
			fmt.Printf("  ✗ %s: ERROR (%v) (%s)\n", r.Field, r.Error, fmtDuration(r.Duration))
			failed++
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
	return passed, failed, noop
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
