/*
Copyright 2021 Upbound Inc.
*/

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// unwrappedController is a minimal stand-in for a generated zz_controller.go: it
// contains a tjcontroller.NewTerraformPluginSDKAsyncConnector(...) call with the
// >=4 args the wrap pass expects.
const unwrappedController = `package sample

import (
	tjcontroller "github.com/crossplane/upjet/pkg/controller"
)

func Setup(mgr manager, o options) {
	r := managed.NewReconciler(mgr,
		managed.WithExternalConnecter(
			tjcontroller.NewTerraformPluginSDKAsyncConnector(mgr.GetClient(), o.OperationTrackerStore, o.SetupFn, o.Provider.Resources["infoblox_a_record"], opt1, opt2)))
	_ = r
}
`

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	return p
}

// TestWrapConnectorInFileWrapsAndIsIdempotent asserts the wrap pass rewrites the
// connector call once, and that re-running it produces byte-identical output
// (idempotency — safe to re-run without churn).
func TestWrapConnectorInFileWrapsAndIsIdempotent(t *testing.T) {
	p := writeTemp(t, "zz_controller.go", unwrappedController)

	if err := wrapConnectorInFile(p); err != nil {
		t.Fatalf("first wrap: %v", err)
	}
	afterFirst, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(afterFirst), "split.WrapConnector(") {
		t.Fatalf("expected split.WrapConnector wrap after first pass, got:\n%s", afterFirst)
	}
	if !strings.Contains(string(afterFirst), "tjcontroller.NewTerraformPluginSDKAsyncConnector(") {
		t.Fatalf("expected inner connector call to be preserved, got:\n%s", afterFirst)
	}
	// The split package must have been imported.
	if !strings.Contains(string(afterFirst), "internal/clients/split") {
		t.Fatalf("expected split import to be added, got:\n%s", afterFirst)
	}

	if err := wrapConnectorInFile(p); err != nil {
		t.Fatalf("second wrap: %v", err)
	}
	afterSecond, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(afterFirst) != string(afterSecond) {
		t.Fatalf("wrap pass is not idempotent:\n--- first ---\n%s\n--- second ---\n%s", afterFirst, afterSecond)
	}
}

// TestWrapConnectorInFileFailsLoud asserts the pass errors (rather than silently
// no-op'ing) when the expected connector call is absent — e.g. after an upjet
// bump that changes the emitted controller shape.
func TestWrapConnectorInFileFailsLoud(t *testing.T) {
	renamed := strings.ReplaceAll(unwrappedController, "NewTerraformPluginSDKAsyncConnector", "NewSomeOtherConnector")
	p := writeTemp(t, "zz_controller.go", renamed)

	err := wrapConnectorInFile(p)
	if err == nil {
		t.Fatal("expected an error when the connector call is missing, got nil")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected a 'not found' error, got: %v", err)
	}
}

// TestWrapConnectorInFileSignatureGuard asserts the pass errors when the
// connector call has fewer args than the wrapper needs.
func TestWrapConnectorInFileSignatureGuard(t *testing.T) {
	short := strings.Replace(unwrappedController,
		`tjcontroller.NewTerraformPluginSDKAsyncConnector(mgr.GetClient(), o.OperationTrackerStore, o.SetupFn, o.Provider.Resources["infoblox_a_record"], opt1, opt2)`,
		`tjcontroller.NewTerraformPluginSDKAsyncConnector(mgr.GetClient(), o.SetupFn)`, 1)
	p := writeTemp(t, "zz_controller.go", short)

	err := wrapConnectorInFile(p)
	if err == nil {
		t.Fatal("expected a signature error for a too-short connector call, got nil")
	}
	if !strings.Contains(err.Error(), "signature") {
		t.Fatalf("expected a 'signature' error, got: %v", err)
	}
}

// TestWrapConnectorInFileRealControllerIdempotent runs the pass against an
// already-generated (already-wrapped) controller and asserts it is left
// byte-for-byte unchanged.
func TestWrapConnectorInFileRealControllerIdempotent(t *testing.T) {
	real := filepath.Join("..", "..", "internal", "controller", "dns", "arecord", "zz_controller.go")
	src, err := os.ReadFile(real)
	if err != nil {
		t.Skipf("real controller not found (%v); skipping", err)
	}
	p := writeTemp(t, "zz_controller.go", string(src))

	if err := wrapConnectorInFile(p); err != nil {
		t.Fatalf("wrap on already-wrapped controller: %v", err)
	}
	after, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(src) != string(after) {
		t.Fatalf("already-wrapped controller was modified by a re-run (not idempotent)")
	}
}
