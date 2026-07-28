package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/catalog"
	"github.com/crossplane-contrib/provider-infoblox-nios/tools/openapi/pkg/report"
)

const defaultOutputPath = "tools/openapi/inventory.md"
const defaultPilotSlug = "record_a"
const defaultSDKCommit = "6a8baf2b625bf611456e5b74bf30b6a3a680ec9c"

// runInventory parses positional/flag arguments and renders inventory.md.
//
// Recognized flags: --pilot=<slug>, --sdk-commit=<sha>, --probe-note=<text>.
// The first non-flag argument, if present, overrides the output path.
func runInventory(args []string) error {
	outputPath := defaultOutputPath
	pilot := defaultPilotSlug
	sdkCommit := defaultSDKCommit
	probeNote := ""

	for _, a := range args {
		switch {
		case strings.HasPrefix(a, "--pilot="):
			pilot = strings.TrimPrefix(a, "--pilot=")
		case strings.HasPrefix(a, "--sdk-commit="):
			sdkCommit = strings.TrimPrefix(a, "--sdk-commit=")
		case strings.HasPrefix(a, "--probe-note="):
			probeNote = strings.TrimPrefix(a, "--probe-note=")
		case strings.HasPrefix(a, "--"):
			// Unknown flag — ignore rather than fail, to keep the tool
			// forward-compatible with additional options.
		default:
			outputPath = a
		}
	}

	// outputPath is an operator-supplied CLI argument (this is a local
	// investigation tool, not a network-facing service) — clean it to
	// normalize any ".." segments before use.
	f, err := os.Create(filepath.Clean(outputPath)) //nolint:gosec // G703: CLI-argument output path, not attacker-controlled input
	if err != nil {
		return err
	}

	opts := report.Options{
		SDKCommit:       sdkCommit,
		PilotSlug:       pilot,
		ActionEndpoints: catalog.ActionEndpoints(),
		ProbeNote:       probeNote,
	}
	if err := report.Write(f, catalog.Resources(), opts); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
