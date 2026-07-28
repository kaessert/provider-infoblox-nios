package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// FieldInfo holds metadata about a struct field parsed from Go source.
type FieldInfo struct {
	GoName    string
	JSONName  string
	Immutable bool
}

// ValidationResult holds the outcome of validating a manifest against types.
type ValidationResult struct {
	Kind    string
	Fields  []FieldValidation
	AllGood bool
}

// FieldValidation holds the status of a single field in validation.
type FieldValidation struct {
	JSONName string
	Status   string // "tested", "skipped", "immutable", "MISSING"
}

// Regexes for parsing Go struct fields.
var (
	// Matches a struct field line like:
	//   Name string `json:"name" ...`
	//   Path *string `json:"path,omitempty" ...`
	reStructField = regexp.MustCompile(
		`^\s+(\w+)\s+\S+.*` + "`" + `.*json:"([^",]+).*` + "`",
	)

	// Matches XValidation marker for immutability.
	// Looks for: rule="self == oldSelf" in a comment or marker above the field.
	reImmutable = regexp.MustCompile(`self\s*==\s*oldSelf`)

	// Matches the start of a Parameters struct.
	reParamsStruct = regexp.MustCompile(`^type\s+(\w*Parameters)\s+struct\s*\{`)
)

// goTypesParser is a small state machine that scans a Go source file line by
// line looking for the {targetKind}Parameters struct, skipping over any
// other *Parameters structs it encounters along the way (e.g. nested config
// structs). It uses basic regex parsing.
type goTypesParser struct {
	targetStruct string
	fields       []FieldInfo
	inTarget     bool // inside the struct we actually want to parse
	inOther      bool // inside a non-matching *Parameters struct (skipping)
	braceDepth   int
	prevLines    []string // buffer of preceding comment/marker lines
	done         bool
}

// handleLine processes a single line of source, advancing the parser state.
func (p *goTypesParser) handleLine(line string) {
	if !p.inTarget && !p.inOther {
		p.tryEnterStruct(line)
		return
	}

	// Track brace depth to detect the end of the current struct.
	p.braceDepth += strings.Count(line, "{") - strings.Count(line, "}")
	if p.braceDepth <= 0 {
		if p.inTarget {
			// Done parsing the target struct.
			p.done = true
			return
		}
		// Finished skipping a non-target struct; resume searching.
		p.inOther = false
		return
	}

	// Inside a non-target struct — skip everything.
	if p.inOther {
		return
	}

	// Inside the target struct — parse field declarations.
	p.parseFieldLine(line)
}

// tryEnterStruct checks whether line opens a *Parameters struct and, if so,
// starts tracking it (either as the target struct or one to skip past).
func (p *goTypesParser) tryEnterStruct(line string) {
	m := reParamsStruct.FindStringSubmatch(line)
	if m == nil {
		return
	}
	if m[1] == p.targetStruct {
		// Found the struct we want — start parsing its fields.
		p.inTarget = true
		p.braceDepth = 1
		p.prevLines = nil
		return
	}
	// Different *Parameters struct — track depth so we can skip past it.
	p.inOther = true
	p.braceDepth = 1
}

// parseFieldLine handles a single line while inside the target struct: it
// either records a field declaration, accumulates a preceding comment/marker
// line, or resets the comment buffer.
func (p *goTypesParser) parseFieldLine(line string) {
	matches := reStructField.FindStringSubmatch(line)
	switch {
	case matches != nil:
		p.addField(matches[1], matches[2], line)
	case isCommentOrMarkerLine(line):
		// Accumulate comment/marker lines.
		p.prevLines = append(p.prevLines, line)
	default:
		p.prevLines = nil
	}
}

// isCommentOrMarkerLine reports whether line is a comment or a kubebuilder/
// crossplane marker that should be buffered for immutability detection.
func isCommentOrMarkerLine(line string) bool {
	return strings.HasPrefix(strings.TrimSpace(line), "//") ||
		strings.Contains(line, "+kubebuilder") ||
		strings.Contains(line, "+crossplane")
}

// addField records a parsed field declaration, determining immutability from
// the buffered preceding comment lines or the field line itself.
func (p *goTypesParser) addField(goName, jsonName, line string) {
	// Check if any preceding comment lines contain immutability marker.
	immutable := false
	for _, pl := range p.prevLines {
		if reImmutable.MatchString(pl) {
			immutable = true
			break
		}
	}
	// Also check the field line itself (inline markers).
	if reImmutable.MatchString(line) {
		immutable = true
	}

	p.fields = append(p.fields, FieldInfo{
		GoName:    goName,
		JSONName:  jsonName,
		Immutable: immutable,
	})
	p.prevLines = nil
}

// ParseGoTypes reads a Go source file and extracts fields from the
// {targetKind}Parameters struct. It skips other *Parameters structs
// (e.g. nested config structs) until it finds the one matching targetKind.
func ParseGoTypes(path, targetKind string) ([]FieldInfo, error) {
	targetStruct := targetKind + "Parameters"

	// #nosec G304 -- path is an operator-supplied CLI argument (the
	// generated types file to validate), not attacker-controlled input.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening types file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	p := &goTypesParser{targetStruct: targetStruct}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		p.handleLine(scanner.Text())
		if p.done {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning types file: %w", err)
	}

	if len(p.fields) == 0 {
		return nil, fmt.Errorf("no %s struct found in %s", targetStruct, path)
	}

	return p.fields, nil
}

// ValidateManifest checks that the manifest's update-test annotation covers
// all mutable fields from the Go types.
func ValidateManifest(m *Manifest, fields []FieldInfo) *ValidationResult {
	// Build a set of tested/skipped fields from the annotation.
	tested := make(map[string]string) // jsonName → "tested" or "skipped"
	for _, t := range m.Tests {
		if t.Skip != "" {
			tested[t.Field] = "skipped"
		} else {
			tested[t.Field] = "tested"
		}
	}

	result := &ValidationResult{
		Kind:    m.Kind,
		AllGood: true,
	}

	for _, f := range fields {
		var v FieldValidation
		v.JSONName = f.JSONName

		if f.Immutable {
			v.Status = "immutable"
		} else if status, ok := tested[f.JSONName]; ok {
			v.Status = status
		} else {
			v.Status = "MISSING"
			result.AllGood = false
		}
		result.Fields = append(result.Fields, v)
	}

	return result
}

// PrintValidation outputs the validation result to stdout.
func PrintValidation(r *ValidationResult) {
	structName := r.Kind + "Parameters"
	fmt.Printf("Validating %s manifest against %s\n", r.Kind, structName)

	for _, f := range r.Fields {
		var icon string
		var detail string
		switch f.Status {
		case "tested":
			icon = "✓"
			detail = "covered (tested)"
		case "skipped":
			icon = "✓"
			detail = "covered (skipped)"
		case "immutable":
			icon = "✓"
			detail = "immutable (excluded)"
		case "MISSING":
			icon = "✗"
			detail = "MISSING — not covered by update-test annotation"
		}
		fmt.Printf("  %s %s: %s\n", icon, f.JSONName, detail)
	}

	fmt.Println()
	if r.AllGood {
		fmt.Println("All mutable fields covered.")
	} else {
		fmt.Println("FAIL: some mutable fields are not covered.")
	}
}
