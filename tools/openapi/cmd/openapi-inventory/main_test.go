package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunInventoryWritesFile(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "inventory.md")

	if err := runInventory([]string{outPath, "--pilot=record_a", "--sdk-commit=deadbeef"}); err != nil {
		t.Fatalf("runInventory() error = %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	content := string(data)
	if !strings.Contains(content, "Infoblox NIOS Provider") {
		t.Error("output missing expected title")
	}
	if !strings.Contains(content, "deadbeef") {
		t.Error("output missing sdk commit")
	}
	if !strings.Contains(content, "Confirmed pilot: ARecord") {
		t.Error("output missing pilot confirmation")
	}
}

func TestRunInventoryDefaultsWhenNoArgs(t *testing.T) {
	dir := t.TempDir()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chdir(wd) }()

	if err := os.MkdirAll(filepath.Dir(defaultOutputPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runInventory(nil); err != nil {
		t.Fatalf("runInventory() error = %v", err)
	}
	if _, err := os.Stat(defaultOutputPath); err != nil {
		t.Errorf("expected default output file to exist: %v", err)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	if err := run([]string{"bogus"}); err == nil {
		t.Error("expected error for unknown command")
	}
}

func TestRunMissingCommand(t *testing.T) {
	if err := run(nil); err == nil {
		t.Error("expected error for missing command")
	}
}
