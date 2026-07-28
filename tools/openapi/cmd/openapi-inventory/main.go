// Command openapi-inventory is the Phase 1 investigation tool for
// provider-infobloxnios. Infoblox NIOS does not publish an OpenAPI
// specification (input format rest-nospec-sdk); the resource catalog is
// hand-curated in tools/openapi/pkg/catalog from the pinned
// infoblox-go-client/v2 SDK source (tools/openapi/specs/) plus live WAPI
// probing. This tool renders that catalog into tools/openapi/inventory.md.
//
// Usage:
//
//	openapi-inventory inventory [output-md-path] [--pilot=<slug>] [--sdk-commit=<sha>] [--probe-note=<text>]
package main

import (
	"fmt"
	"os"
)

const usage = `Usage: openapi-inventory <command> [args]

Commands:
  inventory [output-md-path] [--pilot=<slug>] [--sdk-commit=<sha>] [--probe-note=<text>]
      Render the hand-curated catalog (tools/openapi/pkg/catalog) to Markdown.
      Default output path: tools/openapi/inventory.md. Default pilot: record_a.

Example:
  go run ./tools/openapi/cmd/openapi-inventory inventory tools/openapi/inventory.md \
    --pilot=record_a --sdk-commit=6a8baf2b625bf611456e5b74bf30b6a3a680ec9c
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("missing command\n%s", usage)
	}
	switch args[0] {
	case "inventory":
		return runInventory(args[1:])
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}
