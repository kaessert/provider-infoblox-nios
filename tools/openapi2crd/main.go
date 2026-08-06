// Command openapi2crd is the provider-infobloxnios type generator.
//
// Subcommand:
//
//	openapi2crd generate [--output-dir DIR] [--resource SLUG]
//
// generate renders the catalog (pkg/catalog) resource descriptors into Go
// types (apis/common/, apis/cluster/, apis/namespaced/) for both cluster and
// namespaced scopes. Run `make generate` afterwards to produce CRD YAML
// (controller-gen) and deepcopy/managed methodsets (angryjet).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi2crd/pkg/catalog"
	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi2crd/pkg/generator"
)

// providerModulePath is the Go module path declared in the provider root's
// go.mod (the module that owns apis/). A valid --output-dir MUST resolve to
// a directory whose go.mod declares this module.
const providerModulePath = "github.com/crossplane-contrib/provider-infoblox-nios"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "openapi2crd: %v\n", err) //nolint:errcheck
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: openapi2crd generate [flags]")
	}
	switch args[0] {
	case "generate":
		return runGenerate(args[1:])
	default:
		return fmt.Errorf("unknown subcommand %q; expected generate", args[0])
	}
}

// runGenerate implements the generate subcommand. It renders every resource
// currently registered in the catalog, or a single resource when --resource
// is set. New resources are added to the catalog manually as they are
// onboarded (see the /add-resource skill).
//
// --output-dir MUST resolve to the provider root (expected to be
// provider-infobloxnios/, the directory whose go.mod declares
// providerModulePath). It is validated by resolveProviderRoot before
// anything is written — see that function's doc comment for why.
func runGenerate(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ContinueOnError)
	outputDir := fs.String("output-dir", ".", "ABSOLUTE path to the provider root directory that owns apis/ (the go.mod declaring "+providerModulePath+"); must NOT be a tools/ subdirectory")
	resource := fs.String("resource", "", "generate only this resource slug (default: all catalog resources)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	root, err := resolveProviderRoot(*outputDir)
	if err != nil {
		return err
	}

	resources := catalog.All()
	if *resource != "" {
		rd, ok := catalog.FindResource(*resource)
		if !ok {
			return fmt.Errorf("resource %q not found in catalog", *resource)
		}
		resources = []catalog.ResourceDescriptor{*rd}
	}

	for _, rd := range resources {
		if err := generator.Generate(rd, root); err != nil {
			return fmt.Errorf("generate %s: %w", rd.Kind, err)
		}
		fmt.Fprintf(os.Stdout, "openapi2crd: generated types for %s in %s\n", rd.Kind, root) //nolint:errcheck
	}

	// apis/common/driftdetection/driftdetection_types.go is provider-wide,
	// not per-resource — every scope package's Spec imports it, so it is
	// (re)written unconditionally, even when --resource narrows the run to
	// a single catalog entry.
	if err := generator.GenerateDriftDetectionPackage(root); err != nil {
		return fmt.Errorf("generate driftdetection package: %w", err)
	}
	fmt.Fprintf(os.Stdout, "openapi2crd: generated driftdetection package in %s\n", root) //nolint:errcheck

	// apis/common/referencehelpers/zz_referencehelpers.go is a single,
	// static, non-resource-specific file (the generic ExtractField
	// cross-resource reference extractor) — rewrite it unconditionally on
	// every run rather than tying it to any one resource.
	if err := generator.GenerateReferenceHelpers(root); err != nil {
		return fmt.Errorf("generate reference helpers: %w", err)
	}
	fmt.Fprintf(os.Stdout, "openapi2crd: generated reference helpers in %s\n", root) //nolint:errcheck

	return nil
}

// resolveProviderRoot converts dir to an absolute path and validates that it
// looks like the provider root (the module that owns apis/) before the
// generator is allowed to write anything there.
//
// This guards against a known failure mode: running
// `cd tools/openapi2crd && go run . generate --output-dir ..` from inside
// the tools module resolves ".." to the tools/ directory, not the provider
// root, silently creating a phantom tools/apis/ tree while the real apis/
// tree at the provider root stays stale — and looking exactly like a
// "my edits aren't persisting" caching/race bug from the caller's side.
//
// Validation, in order:
//  1. dir must resolve to an absolute path with no path component literally
//     named "tools". This rejects both the tools/openapi2crd directory
//     itself and any output-dir nested underneath a tools/ tree.
//  2. The resolved directory must contain a go.mod file.
//  3. That go.mod's `module` directive must equal providerModulePath.
//
// On success it returns the resolved absolute path (always use this return
// value, not the original dir, as the generator's output root).
func resolveProviderRoot(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolving --output-dir %q: %w", dir, err)
	}

	for _, part := range strings.Split(filepath.ToSlash(abs), "/") {
		if part == "tools" {
			return "", fmt.Errorf(
				"--output-dir %q resolves to %q, which is inside a tools/ directory; "+
					"pass the ABSOLUTE provider root instead (the directory containing "+
					"go.mod with module %q — e.g. the provider-infobloxnios/ checkout root), "+
					"not a tools/ subdirectory",
				dir, abs, providerModulePath)
		}
	}

	goModPath := filepath.Join(abs, "go.mod")
	data, err := os.ReadFile(goModPath) //nolint:gosec // path is the resolved --output-dir flag, validated below
	if err != nil {
		return "", fmt.Errorf(
			"--output-dir %q resolves to %q, which does not contain a go.mod "+
				"(not the provider root that owns apis/): %w",
			dir, abs, err)
	}

	modPath, ferr := parseModulePath(data)
	if ferr != nil {
		return "", fmt.Errorf("--output-dir %q resolves to %q: %w", dir, abs, ferr)
	}
	if modPath != providerModulePath {
		return "", fmt.Errorf(
			"--output-dir %q resolves to %q, whose go.mod declares module %q; "+
				"expected the provider root module %q — this looks like the wrong "+
				"directory (e.g. the tools/openapi2crd module or an unrelated checkout)",
			dir, abs, modPath, providerModulePath)
	}

	return abs, nil
}

// parseModulePath extracts the module path from go.mod content — the token
// following the "module" directive on its own line.
func parseModulePath(data []byte) (string, error) {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			modPath := strings.TrimSpace(strings.TrimPrefix(line, "module"))
			if modPath == "" {
				return "", fmt.Errorf("go.mod has an empty module directive")
			}
			return modPath, nil
		}
	}
	return "", fmt.Errorf("go.mod has no module directive")
}
