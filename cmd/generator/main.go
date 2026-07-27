/*
Copyright 2021 Upbound Inc.
*/

package main

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"

	"github.com/crossplane/upjet/pkg/pipeline"
	"golang.org/x/tools/go/ast/astutil"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/crossplane-contrib/provider-infoblox-nios/config"
)

const (
	// splitPkgPath is the import path of the read/write-split decorator.
	splitPkgPath = "github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/split"
	// splitPkgName is the (explicit) local name used for the split import.
	splitPkgName = "split"

	// connectorSelPkg / connectorSelName identify the generated async
	// connector call that we wrap.
	connectorSelPkg  = "tjcontroller"
	connectorSelName = "NewTerraformPluginSDKAsyncConnector"

	// wrapSelName is the split wrapper function.
	wrapSelName = "WrapConnector"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] == "" {
		panic("root directory is required to be given as argument")
	}
	rootDir := os.Args[1]
	absRootDir, err := filepath.Abs(rootDir)
	if err != nil {
		panic(fmt.Sprintf("cannot calculate the absolute path with %s", rootDir))
	}
	p, err := config.GetProvider(context.Background(), true)
	kingpin.FatalIfError(err, "cannot initialize the provider configuration")
	pipeline.Run(p, absRootDir)

	// Post-generation: wrap every generated async connector with the
	// read/write-split decorator. This is deterministic, idempotent (safe to
	// re-run) and fails loud if the expected connector call is not found, so an
	// upjet bump that changes the emitted controller shape is caught here rather
	// than silently skipping the split.
	kingpin.FatalIfError(wrapGeneratedConnectors(absRootDir), "cannot wrap generated connectors with the read/write split")
}

// wrapGeneratedConnectors rewrites each generated zz_controller.go so that its
// tjcontroller.NewTerraformPluginSDKAsyncConnector(...) call is wrapped by
// split.WrapConnector(mgr.GetClient(), o.Provider.Resources["<tf>"], <call>, o.Logger).
func wrapGeneratedConnectors(rootDir string) error {
	controllerDir := filepath.Join(rootDir, "internal", "controller")
	var files []string
	if err := filepath.Walk(controllerDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Name() == "zz_controller.go" {
			files = append(files, path)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("cannot walk %s: %w", controllerDir, err)
	}
	if len(files) == 0 {
		return fmt.Errorf("no zz_controller.go files found under %s", controllerDir)
	}

	for _, f := range files {
		if err := wrapConnectorInFile(f); err != nil {
			return fmt.Errorf("%s: %w", f, err)
		}
	}
	return nil
}

func wrapConnectorInFile(path string) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("cannot parse: %w", err)
	}

	var (
		found        bool
		alreadyWrap  bool
		replaceError error
	)

	astutil.Apply(file, func(c *astutil.Cursor) bool {
		call, ok := c.Node().(*ast.CallExpr)
		if !ok {
			return true
		}
		if !isSelectorCall(call.Fun, connectorSelPkg, connectorSelName) {
			return true
		}
		// Idempotency / double-wrap guard: if the enclosing call is already
		// split.WrapConnector, leave it alone.
		if parent, ok := c.Parent().(*ast.CallExpr); ok && isSelectorCall(parent.Fun, splitPkgName, wrapSelName) {
			alreadyWrap = true
			return true
		}
		if len(call.Args) < 4 {
			replaceError = fmt.Errorf("unexpected %s.%s signature: want >=4 args, got %d", connectorSelPkg, connectorSelName, len(call.Args))
			return false
		}
		wrapper := &ast.CallExpr{
			Fun: &ast.SelectorExpr{X: ast.NewIdent(splitPkgName), Sel: ast.NewIdent(wrapSelName)},
			Args: []ast.Expr{
				call.Args[0], // mgr.GetClient()
				call.Args[3], // o.Provider.Resources["<tf>"]
				call,         // the original async connector call
				&ast.SelectorExpr{X: ast.NewIdent("o"), Sel: ast.NewIdent("Logger")}, // o.Logger
			},
		}
		c.Replace(wrapper)
		found = true
		return false // do not descend into the wrapper (avoids re-matching call)
	}, nil)

	if replaceError != nil {
		return replaceError
	}
	if alreadyWrap && !found {
		// Already wrapped by a previous run; nothing to do.
		return nil
	}
	if !found {
		return fmt.Errorf("expected connector call %s.%s not found; upjet output shape may have changed", connectorSelPkg, connectorSelName)
	}

	astutil.AddNamedImport(fset, file, splitPkgName, splitPkgPath)

	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return fmt.Errorf("cannot format: %w", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil { //nolint:gosec // generated source file
		return fmt.Errorf("cannot write: %w", err)
	}
	return nil
}

// isSelectorCall reports whether e is a selector expression pkg.name.
func isSelectorCall(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return x.Name == pkg && sel.Sel.Name == name
}
