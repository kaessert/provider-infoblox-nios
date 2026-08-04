// Package hooks_test proves that every example manifest declaring
// crossplane.io/expect-external-name-prefix actually drives a
// pause/strip-external-name/unpause recovery round-trip against the
// provider, not merely a hook script whose comments happen to mention the
// words "resolve-recover".
//
// The prefix annotation alone is a create-path artifact: Create() derives
// the backend object-type variant straight from spec and stamps the
// external-name annotation from the create response, so a manifest can
// carry a correct-looking prefix whether or not the runtime identity search
// (Observe() with no stored handle) resolves to the right variant. Only
// driving that search — pause the MR, strip crossplane.io/external-name,
// unpause, and check the resource recovers to the SAME backend object
// rather than a duplicate — exercises the code path this guard exists for:
// a variant identity search that silently matches nothing creates a
// duplicate backend object with no error, event, or condition to mark it.
//
// This test drives the REAL generic hook (run-update-tester.sh) and the
// REAL example manifests shipped in examples/, against a fake kubectl that
// models a well-behaved backend (see the crossplane-update-tester module's
// own hack/testdata/fake-kubectl.sh, reused here rather than duplicated).
// No cluster is required. Two things are asserted:
//
//  1. Every example declaring the prefix annotation runs BOTH
//     check-external-name-prefix and resolve-recover as part of its
//     post-assert hook, and the recovery reports exactly one
//     CreatedExternalResource event — the signal that distinguishes
//     "recovered" from "silently created a duplicate".
//  2. That assertion has teeth: driving the identical hook against the
//     identical manifest with only the prefix annotation removed makes
//     both steps disappear. A test that could not observe this
//     difference would not be proving the wiring is real — it would stay
//     green even if the resolve-recover step were deleted entirely, which
//     is the exact false assurance a prefix-only check gives.
package hooks_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// repoRoot returns the absolute path to the provider repository root,
// derived from this test file's own directory (test/hooks) rather than the
// process's working directory, so the test behaves the same regardless of
// how `go test` was invoked.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd(): %v", err)
	}
	root, err := filepath.Abs(filepath.Join(wd, "..", ".."))
	if err != nil {
		t.Fatalf("resolving repo root from %q: %v", wd, err)
	}
	return root
}

// fakeKubectlPath locates the generic fake kubectl the crossplane-update-tester
// module ships for its own smoke test (hack/testdata/fake-kubectl.sh). It is
// reused verbatim here rather than duplicated: it already models the exact
// pause/strip/unpause/re-resolve sequence this test needs, generically over
// whatever apiVersion/kind/forProvider fields a manifest declares, and it is
// the same fake the module's own upstream tests are gated on.
func fakeKubectlPath(t *testing.T, root string) string {
	t.Helper()
	toolDir := filepath.Join(root, "tools", "update-tester")
	out, err := exec.Command("go", "-C", toolDir, "list", "-m", "-f", "{{.Dir}}",
		"github.com/kaessert/crossplane-update-tester").Output()
	if err != nil {
		t.Fatalf("locating the crossplane-update-tester module directory: %v", err)
	}
	modDir := strings.TrimSpace(string(out))
	if modDir == "" {
		t.Fatal("go list -m returned an empty module directory")
	}
	path := filepath.Join(modDir, "hack", "testdata", "fake-kubectl.sh")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("fake kubectl not found at %q: %v", path, err)
	}
	return path
}

// newFakeKubectlBin builds a directory holding one executable named
// "kubectl" — a copy of fakeKubectlPath — suitable for prepending to PATH.
// A copy is used (rather than exec-ing the module's script directly under
// its own name) so the invoked binary is literally named "kubectl", which is
// what the hook's shelled-out `kubectl` calls resolve via PATH lookup.
func newFakeKubectlBin(t *testing.T, root string) string {
	t.Helper()
	src := fakeKubectlPath(t, root)
	contents, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("reading fake kubectl %q: %v", src, err)
	}
	bin := t.TempDir()
	dst := filepath.Join(bin, "kubectl")
	if err := os.WriteFile(dst, contents, 0o755); err != nil {
		t.Fatalf("writing fake kubectl to %q: %v", dst, err)
	}
	return bin
}

// prefixExample is one example manifest that declares
// crossplane.io/expect-external-name-prefix, paired with the resolved
// absolute path of the post-assert-hook script it points at.
type prefixExample struct {
	// manifest is the absolute path to the example YAML file.
	manifest string
	// hook is the absolute path of the post-assert-hook script the manifest
	// declares, resolved relative to the manifest's own directory — exactly
	// how uptest resolves *-hook annotations.
	hook string
}

// name derives a stable, human-readable subtest name from the manifest
// path, e.g. "network/network-v6-namespaced.yaml".
func (p prefixExample) name(root string) string {
	rel, err := filepath.Rel(filepath.Join(root, "examples"), p.manifest)
	if err != nil {
		return p.manifest
	}
	return rel
}

var (
	prefixAnnotationRe = regexp.MustCompile(`crossplane\.io/expect-external-name-prefix:\s*"?([^"\s]+)"?`)
	postAssertHookRe   = regexp.MustCompile(`uptest\.upbound\.io/post-assert-hook:\s*(\S+)`)
)

// discoverPrefixExamples walks examples/ for every manifest declaring
// crossplane.io/expect-external-name-prefix and resolves each one's
// post-assert-hook annotation to an absolute path. It fails the test outright
// if such a manifest has no post-assert-hook annotation at all, or if the
// hook path it names does not exist — both leave the prefix check with
// nothing to run, not merely a gap this test can't measure.
func discoverPrefixExamples(t *testing.T, root string) []prefixExample {
	t.Helper()
	var found []prefixExample

	examplesDir := filepath.Join(root, "examples")
	err := filepath.WalkDir(examplesDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".yaml") {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading %s: %w", path, err)
		}
		if !prefixAnnotationRe.Match(contents) {
			return nil
		}
		m := postAssertHookRe.FindSubmatch(contents)
		if m == nil {
			t.Errorf("%s declares crossplane.io/expect-external-name-prefix but has no "+
				"uptest.upbound.io/post-assert-hook annotation — the prefix check has nothing to run", path)
			return nil
		}
		hookRel := string(m[1])
		hookAbs := filepath.Join(filepath.Dir(path), hookRel)
		if _, err := os.Stat(hookAbs); err != nil {
			t.Errorf("%s declares post-assert-hook %q, resolved to %q, which does not exist: %v",
				path, hookRel, hookAbs, err)
			return nil
		}
		found = append(found, prefixExample{manifest: path, hook: hookAbs})
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", examplesDir, err)
	}
	if len(found) == 0 {
		t.Fatal("no example under examples/ declares crossplane.io/expect-external-name-prefix — " +
			"this test has nothing to exercise; if the provider genuinely has no dual-object-type " +
			"resource left, this test should be revisited, not silently left green")
	}
	return found
}

// hookResult is the outcome of one hook invocation.
type hookResult struct {
	output   string
	exitCode int
}

// runHook executes a post-assert hook script (or a symlink to one) with a
// fake, stateful kubectl on PATH and an isolated state directory, optionally
// overriding manifest derivation via $MANIFEST — exactly the override the
// tool's own README documents for debugging. It runs from a directory that
// is NOT the provider repo root, mirroring how uptest invokes the hook from
// the example manifest's own directory rather than the repo root.
func runHook(t *testing.T, kubectlBin, hookScript, manifestOverride string) hookResult {
	t.Helper()

	state := t.TempDir()
	elsewhere := t.TempDir()

	cmd := exec.Command("bash", hookScript)
	cmd.Dir = elsewhere
	cmd.Env = append(os.Environ(),
		"PATH="+kubectlBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"SMOKE_STATE="+state,
		"UPDATE_TESTER_POLL_INTERVAL=1s",
		"UPDATE_TESTER_TIMEOUT=5",
	)
	if manifestOverride != "" {
		cmd.Env = append(cmd.Env, "MANIFEST="+manifestOverride)
	}

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	done := make(chan error, 1)
	go func() { done <- cmd.Run() }()

	select {
	case err := <-done:
		exitCode := 0
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				t.Fatalf("running hook %s: %v\noutput:\n%s", hookScript, err, out.String())
			}
		}
		return hookResult{output: out.String(), exitCode: exitCode}
	case <-time.After(90 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatalf("hook %s did not finish within 90s\noutput so far:\n%s", hookScript, out.String())
		return hookResult{}
	}
}

// stripPrefixAnnotation returns a copy of a manifest's contents with the
// crossplane.io/expect-external-name-prefix annotation line removed — the
// mutation this test uses to prove the wiring check has teeth. Any other
// line (including the sibling crossplane.io/update-test block) is left
// untouched.
func stripPrefixAnnotation(t *testing.T, contents []byte) []byte {
	t.Helper()
	lines := strings.Split(string(contents), "\n")
	var kept []string
	removed := 0
	for _, l := range lines {
		if strings.Contains(l, "crossplane.io/expect-external-name-prefix") {
			removed++
			continue
		}
		kept = append(kept, l)
	}
	if removed == 0 {
		t.Fatal("stripPrefixAnnotation: no crossplane.io/expect-external-name-prefix line found to remove")
	}
	return []byte(strings.Join(kept, "\n"))
}

// TestPostAssertHookResolvesToGenericRunUpdateTester asserts that every
// example declaring crossplane.io/expect-external-name-prefix points its
// post-assert-hook at (a symlink to) the single generic
// test/hooks/run-update-tester.sh script, rather than a private per-resource
// script that could silently omit the resolve-recover step.
func TestPostAssertHookResolvesToGenericRunUpdateTester(t *testing.T) {
	root := repoRoot(t)
	generic, err := filepath.EvalSymlinks(filepath.Join(root, "test", "hooks", "run-update-tester.sh"))
	if err != nil {
		t.Fatalf("resolving test/hooks/run-update-tester.sh: %v", err)
	}

	for _, ex := range discoverPrefixExamples(t, root) {
		ex := ex
		t.Run(ex.name(root), func(t *testing.T) {
			resolved, err := filepath.EvalSymlinks(ex.hook)
			if err != nil {
				t.Fatalf("resolving symlink %q: %v", ex.hook, err)
			}
			if resolved != generic {
				t.Errorf("post-assert-hook for %s resolves to %q, want the generic hook %q — "+
					"a private hook can silently skip the resolve-recover step", ex.manifest, resolved, generic)
			}
		})
	}
}

// TestResolveRecoverRunsForEveryPrefixExample drives the real post-assert
// hook chain, against a fake but well-behaved backend, for every example
// declaring crossplane.io/expect-external-name-prefix and asserts the
// resolve-recover step actually ran and reported a clean recovery: the
// external-name annotation is stripped and re-resolved to the same backend
// object, evidenced by exactly one CreatedExternalResource event across the
// resource's lifetime (a second event would mean a silent duplicate create).
func TestResolveRecoverRunsForEveryPrefixExample(t *testing.T) {
	root := repoRoot(t)
	kubectlBin := newFakeKubectlBin(t, root)

	for _, ex := range discoverPrefixExamples(t, root) {
		ex := ex
		t.Run(ex.name(root), func(t *testing.T) {
			t.Parallel()
			res := runHook(t, kubectlBin, ex.hook, "")

			if res.exitCode != 0 {
				t.Fatalf("hook exited %d, want 0\noutput:\n%s", res.exitCode, res.output)
			}
			if !strings.Contains(res.output, "==> update-tester: check-external-name-prefix ") {
				t.Errorf("hook output for %s never ran check-external-name-prefix\noutput:\n%s",
					ex.manifest, res.output)
			}
			if !strings.Contains(res.output, "==> update-tester: resolve-recover ") {
				t.Errorf("hook output for %s never ran resolve-recover — the prefix annotation is "+
					"asserted but the ref-less identity search it exists to guard is never exercised\noutput:\n%s",
					ex.manifest, res.output)
			}
			if !strings.Contains(res.output, "CreatedExternalResource event") {
				t.Errorf("hook output for %s recovery step never reported a CreatedExternalResource "+
					"event count — a recovery that doesn't count creates can't tell \"recovered\" apart "+
					"from \"silently duplicated\"\noutput:\n%s", ex.manifest, res.output)
			}
			if !strings.Contains(res.output, "exactly 1 CreatedExternalResource event") {
				t.Errorf("hook output for %s recovery did not confirm exactly 1 CreatedExternalResource "+
					"event — anything else is the silent-duplicate signature this check exists to catch\noutput:\n%s",
					ex.manifest, res.output)
			}
		})
	}
}

// TestResolveRecoverStepDisappearsWithoutPrefixAnnotation is the mutation
// half of the proof: it drives the IDENTICAL hook script against the
// IDENTICAL manifest content with only the crossplane.io/expect-external-name-prefix
// annotation removed, and asserts that both check-external-name-prefix and
// resolve-recover no longer run.
//
// Without this half, TestResolveRecoverRunsForEveryPrefixExample would only
// prove the hook prints the word "resolve-recover" somewhere — which a hook
// that always ran every step regardless of the annotation would also
// satisfy, silently defeating the point of gating the check on the
// annotation at all. This test proves the gate is real: removing the one
// thing the check is supposed to react to removes the check.
func TestResolveRecoverStepDisappearsWithoutPrefixAnnotation(t *testing.T) {
	root := repoRoot(t)
	kubectlBin := newFakeKubectlBin(t, root)

	for _, ex := range discoverPrefixExamples(t, root) {
		ex := ex
		t.Run(ex.name(root), func(t *testing.T) {
			t.Parallel()

			original, err := os.ReadFile(ex.manifest)
			if err != nil {
				t.Fatalf("reading %s: %v", ex.manifest, err)
			}
			stripped := stripPrefixAnnotation(t, original)

			mutated := filepath.Join(t.TempDir(), filepath.Base(ex.manifest))
			if err := os.WriteFile(mutated, stripped, 0o644); err != nil {
				t.Fatalf("writing mutated manifest: %v", err)
			}

			res := runHook(t, kubectlBin, ex.hook, mutated)

			if res.exitCode != 0 {
				t.Fatalf("hook exited %d against the mutated (annotation-stripped) manifest, want 0 — "+
					"a hook that can't even run without the annotation isn't gating on it, it's requiring it\noutput:\n%s",
					res.exitCode, res.output)
			}
			if strings.Contains(res.output, "==> update-tester: check-external-name-prefix ") {
				t.Errorf("check-external-name-prefix still ran after stripping the annotation it is "+
					"supposed to be gated on for %s\noutput:\n%s", ex.manifest, res.output)
			}
			if strings.Contains(res.output, "==> update-tester: resolve-recover ") {
				t.Errorf("resolve-recover still ran after stripping crossplane.io/expect-external-name-prefix "+
					"for %s — the step is not actually gated on the annotation, so "+
					"TestResolveRecoverRunsForEveryPrefixExample passing proves nothing\noutput:\n%s",
					ex.manifest, res.output)
			}
		})
	}
}
