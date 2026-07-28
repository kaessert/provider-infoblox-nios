//go:build ignore

// SPDX-FileCopyrightText: 2025 The Crossplane Authors <https://crossplane.io>
//
// SPDX-License-Identifier: Apache-2.0

// generate-registration scans the provider directory structure and emits two
// generated registration files (the auto-generated registration convention):
//
//	apis/zz_generated_register.go              — scheme registration
//	internal/controller/zz_generated_register.go — controller registration
//
// Usage:
//
//	go run hack/generate-registration.go <module-path>
//
// Example:
//
//	go run hack/generate-registration.go github.com/crossplane-contrib/provider-infoblox-nios
package main

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"unicode"
)

// apiEntry describes a discovered API package (one version of one resource).
// A zero-value Scope/Resource identifies the ProviderConfig entry (apis/v1alpha1).
type apiEntry struct {
	Scope      string // "cluster", "namespaced", or "" for ProviderConfig
	Resource   string // e.g. "recorda"; empty for ProviderConfig
	Version    string // e.g. "v1alpha1"
	Alias      string // import alias: scope+resource+version, e.g. "clusterrecordav1alpha1"
	ImportPath string // full import path
}

// titleCase returns the resource name with the first letter upper-cased.
func titleCase(s string) string {
	if s == "" {
		return s
	}
	r := []rune(s)
	r[0] = unicode.ToUpper(r[0])
	return string(r)
}

// discoverAPIPackages scans apis/cluster/ and apis/namespaced/ for resource
// packages that contain a register.go file (the indicator of a registered
// API group version — register.go declares the package's Group/Version
// metadata and SchemeBuilder, the same role groupversion_info.go plays in
// other providers).
//
// Explicitly excluded:
//   - apis/common/  (shared types, no scheme registration)
//   - Any path not matching apis/{cluster,namespaced}/<resource>/<version>/
func discoverAPIPackages(modulePath string) []apiEntry {
	var entries []apiEntry

	for _, scope := range []string{"cluster", "namespaced"} {
		baseDir := filepath.Join("apis", scope)
		if _, err := os.Stat(baseDir); os.IsNotExist(err) {
			continue
		}

		// Walk one level: resource
		resourceDirs, err := os.ReadDir(baseDir)
		if err != nil {
			continue
		}

		for _, rd := range resourceDirs {
			if !rd.IsDir() {
				continue
			}
			resource := rd.Name()
			resourceDir := filepath.Join(baseDir, resource)

			// Walk one more level: version
			versionDirs, err := os.ReadDir(resourceDir)
			if err != nil {
				continue
			}

			for _, vd := range versionDirs {
				if !vd.IsDir() {
					continue
				}
				version := vd.Name()
				pkgDir := filepath.Join(resourceDir, version)

				// Check for register.go as the indicator of a valid API package.
				registerFile := filepath.Join(pkgDir, "register.go")
				if _, err := os.Stat(registerFile); os.IsNotExist(err) {
					continue
				}

				alias := scope + resource + version
				importPath := modulePath + "/apis/" + scope + "/" + resource + "/" + version
				entries = append(entries, apiEntry{
					Scope:      scope,
					Resource:   resource,
					Version:    version,
					Alias:      alias,
					ImportPath: importPath,
				})
			}
		}
	}

	// Sort: cluster before namespaced, then alphabetically by resource+version.
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Scope != entries[j].Scope {
			// cluster < namespaced
			return entries[i].Scope == "cluster"
		}
		if entries[i].Resource != entries[j].Resource {
			return entries[i].Resource < entries[j].Resource
		}
		return entries[i].Version < entries[j].Version
	})

	return entries
}

// providerName derives a short provider name from the module path for use in
// the ProviderConfig import alias, e.g.
// "github.com/crossplane-contrib/provider-infoblox-nios" -> "infobloxnios".
func providerName(modulePath string) string {
	name := filepath.Base(modulePath)
	const prefix = "provider-"
	if len(name) > len(prefix) && name[:len(prefix)] == prefix {
		name = name[len(prefix):]
	}
	// Strip non-alphanumeric separators (e.g. "infoblox-nios" -> "infobloxnios")
	// so the derived alias is a valid Go identifier fragment.
	var b []rune
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b = append(b, r)
		}
	}
	return string(b)
}

// discoverProviderConfig looks for apis/v1alpha1/register.go, the indicator
// that the provider-wide ProviderConfig API package exists (the auto-generated registration convention: this
// package is always present and always scanned first). Returns ok=false if
// the package is not found.
func discoverProviderConfig(modulePath string) (apiEntry, bool) {
	registerFile := filepath.Join("apis", "v1alpha1", "register.go")
	if _, err := os.Stat(registerFile); os.IsNotExist(err) {
		return apiEntry{}, false
	}

	const version = "v1alpha1"
	return apiEntry{
		Scope:      "",
		Resource:   "",
		Version:    version,
		Alias:      providerName(modulePath) + version,
		ImportPath: modulePath + "/apis/" + version,
	}, true
}

// controllerEntry describes a discovered controller package.
type controllerEntry struct {
	Resource   string // e.g. "recorda"
	ImportPath string // full import path
}

// discoverControllers scans internal/controller/ for resource subdirectories
// (excluding "config") that have a matching API package in apis/cluster/ or
// apis/namespaced/.  This prevents false positives from helper directories.
func discoverControllers(modulePath string, apiEntries []apiEntry) []controllerEntry {
	// Build set of known API resources.
	apiResources := make(map[string]bool)
	for _, e := range apiEntries {
		apiResources[e.Resource] = true
	}

	controllerBase := filepath.Join("internal", "controller")
	dirs, err := os.ReadDir(controllerBase)
	if err != nil {
		return nil
	}

	var entries []controllerEntry
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		name := d.Name()
		if name == "config" {
			continue // Always handled separately.
		}
		if !apiResources[name] {
			continue // No matching API package — skip.
		}
		entries = append(entries, controllerEntry{
			Resource:   name,
			ImportPath: modulePath + "/internal/controller/" + name,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Resource < entries[j].Resource
	})

	return entries
}

// writeAPIRegistration writes apis/zz_generated_register.go. pcEntry, when
// non-nil, is always emitted first and registered before any MR types so
// that ProviderConfig resolution never depends on MR registration order.
// The output is fully self-contained (the auto-generated registration convention): it declares its own
// runtime.SchemeBuilder aggregate and AddToScheme entrypoint rather than
// depending on any hand-written file.
func writeAPIRegistration(pcEntry *apiEntry, entries []apiEntry) error {
	var buf bytes.Buffer

	buf.WriteString("// Code generated by hack/generate-registration.go. DO NOT EDIT.\n")
	buf.WriteString("\n")
	buf.WriteString("// SPDX-FileCopyrightText: 2025 The Crossplane Authors <https://crossplane.io>\n")
	buf.WriteString("//\n")
	buf.WriteString("// SPDX-License-Identifier: Apache-2.0\n")
	buf.WriteString("\n")
	buf.WriteString("package apis\n")
	buf.WriteString("\n")

	buf.WriteString("import (\n")
	buf.WriteString("\t\"k8s.io/apimachinery/pkg/runtime\"\n")
	buf.WriteString("\n")
	if pcEntry != nil {
		fmt.Fprintf(&buf, "\t%s %q\n", pcEntry.Alias, pcEntry.ImportPath)
	}
	for _, e := range entries {
		fmt.Fprintf(&buf, "\t%s %q\n", e.Alias, e.ImportPath)
	}
	buf.WriteString(")\n")
	buf.WriteString("\n")

	buf.WriteString("// AddToSchemes may be used to add all resources defined in the project to a Scheme.\n")
	buf.WriteString("var AddToSchemes runtime.SchemeBuilder\n")
	buf.WriteString("\n")

	buf.WriteString("func init() {\n")
	if pcEntry != nil {
		buf.WriteString("\t// Register ProviderConfig types.\n")
		fmt.Fprintf(&buf, "\tAddToSchemes = append(AddToSchemes, %s.SchemeBuilder.AddToScheme)\n", pcEntry.Alias)
	}
	for _, e := range entries {
		var scopeComment string
		if e.Scope == "cluster" {
			scopeComment = "cluster-scoped"
		} else {
			scopeComment = e.Scope
		}
		fmt.Fprintf(&buf, "\t// Register %s %s types.\n", scopeComment, titleCase(e.Resource))
		fmt.Fprintf(&buf, "\tAddToSchemes = append(AddToSchemes, %s.SchemeBuilder.AddToScheme)\n", e.Alias)
	}
	buf.WriteString("}\n")
	buf.WriteString("\n")

	buf.WriteString("// AddToScheme adds all Resources to the Scheme.\n")
	buf.WriteString("func AddToScheme(s *runtime.Scheme) error {\n")
	buf.WriteString("\treturn AddToSchemes.AddToScheme(s)\n")
	buf.WriteString("}\n")

	// Normalize with go/format (equivalent to gofmt).
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("format apis/zz_generated_register.go: %w", err)
	}

	return os.WriteFile(filepath.Join("apis", "zz_generated_register.go"), formatted, 0644)
}

// writeControllerRegistration writes internal/controller/zz_generated_register.go.
func writeControllerRegistration(modulePath string, controllers []controllerEntry) error {
	configPkg := modulePath + "/internal/controller/config"

	var buf bytes.Buffer

	buf.WriteString("// Code generated by hack/generate-registration.go. DO NOT EDIT.\n")
	buf.WriteString("//\n")
	buf.WriteString("// SetupGated defers MR controller startup until CRDs are installed (SafeStart).\n")
	buf.WriteString("// Setup starts all controllers immediately (RBAC fallback path).\n")
	buf.WriteString("// ProviderConfig (config.Setup) is NEVER gated — it must always be available.\n")
	buf.WriteString("// See the gated controller registration convention (SafeStart pattern).\n")
	buf.WriteString("\n")
	buf.WriteString("// SPDX-FileCopyrightText: 2025 The Crossplane Authors <https://crossplane.io>\n")
	buf.WriteString("//\n")
	buf.WriteString("// SPDX-License-Identifier: Apache-2.0\n")
	buf.WriteString("\n")
	buf.WriteString("package controller\n")
	buf.WriteString("\n")

	// Build import list.
	imports := []string{
		`ctrl "sigs.k8s.io/controller-runtime"`,
		`"github.com/crossplane/crossplane-runtime/v2/pkg/controller"`,
		`"` + configPkg + `"`,
	}
	for _, c := range controllers {
		imports = append(imports, `"`+c.ImportPath+`"`)
	}

	buf.WriteString("import (\n")
	for _, imp := range imports {
		fmt.Fprintf(&buf, "\t%s\n", imp)
	}
	buf.WriteString(")\n")
	buf.WriteString("\n")

	// SetupGated
	buf.WriteString("// SetupGated creates all controllers with safe-start (gate) support.\n")
	buf.WriteString("// MR controllers defer startup until CRDs are installed.\n")
	buf.WriteString("func SetupGated(mgr ctrl.Manager, o controller.Options) error {\n")
	buf.WriteString("\tfor _, setup := range []func(ctrl.Manager, controller.Options) error{\n")
	buf.WriteString("\t\t// ProviderConfig — never gated, must always be available.\n")
	buf.WriteString("\t\tconfig.Setup,\n")
	for _, c := range controllers {
		pkg := filepath.Base(c.ImportPath)
		fmt.Fprintf(&buf, "\t\t%s.SetupGated,\n", pkg)
	}
	buf.WriteString("\t} {\n")
	buf.WriteString("\t\tif err := setup(mgr, o); err != nil {\n")
	buf.WriteString("\t\t\treturn err\n")
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n")
	buf.WriteString("\n")

	// Setup
	buf.WriteString("// Setup creates all controllers without gate support (RBAC fallback).\n")
	buf.WriteString("// MR controllers start immediately without waiting for CRD installation.\n")
	buf.WriteString("func Setup(mgr ctrl.Manager, o controller.Options) error {\n")
	buf.WriteString("\tfor _, setup := range []func(ctrl.Manager, controller.Options) error{\n")
	buf.WriteString("\t\t// ProviderConfig — never gated, must always be available.\n")
	buf.WriteString("\t\tconfig.Setup,\n")
	for _, c := range controllers {
		pkg := filepath.Base(c.ImportPath)
		fmt.Fprintf(&buf, "\t\t%s.Setup,\n", pkg)
	}
	buf.WriteString("\t} {\n")
	buf.WriteString("\t\tif err := setup(mgr, o); err != nil {\n")
	buf.WriteString("\t\t\treturn err\n")
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\treturn nil\n")
	buf.WriteString("}\n")

	// Normalize with go/format.
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("format internal/controller/zz_generated_register.go: %w", err)
	}

	return os.WriteFile(filepath.Join("internal", "controller", "zz_generated_register.go"), formatted, 0644)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: go run hack/generate-registration.go <module-path>")
		fmt.Fprintln(os.Stderr, "example: go run hack/generate-registration.go github.com/crossplane-contrib/provider-infoblox-nios")
		os.Exit(1)
	}
	modulePath := os.Args[1]

	pcEntry, hasPC := discoverProviderConfig(modulePath)
	apiEntries := discoverAPIPackages(modulePath)
	controllerEntries := discoverControllers(modulePath, apiEntries)

	var pcEntryPtr *apiEntry
	if hasPC {
		pcEntryPtr = &pcEntry
	}

	if err := writeAPIRegistration(pcEntryPtr, apiEntries); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("wrote apis/zz_generated_register.go")

	if err := writeControllerRegistration(modulePath, controllerEntries); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("wrote internal/controller/zz_generated_register.go")
}
