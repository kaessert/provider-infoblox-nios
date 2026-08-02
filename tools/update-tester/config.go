package main

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// UpdateTest represents a single field update test parsed from the annotation.
type UpdateTest struct {
	Field  string      `yaml:"field"`
	Value  interface{} `yaml:"value"`
	Expect interface{} `yaml:"expect"`
	Skip   string      `yaml:"skip"`
}

// Manifest holds the parsed Kubernetes manifest metadata needed for testing.
type Manifest struct {
	APIVersion   string
	Kind         string
	Name         string
	Namespace    string
	Tests        []UpdateTest
	ConvergeSkip string
	// ExpectExternalNamePrefix is the value of the
	// crossplane.io/expect-external-name-prefix annotation, when present.
	// Empty means the manifest declares no external-name-prefix
	// expectation — see expectExternalNamePrefixKey.
	ExpectExternalNamePrefix string
}

// manifestDoc is the intermediate YAML structure for parsing.
type manifestDoc struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name        string            `yaml:"name"`
		Namespace   string            `yaml:"namespace"`
		Annotations map[string]string `yaml:"annotations"`
	} `yaml:"metadata"`
}

const annotationKey = "crossplane.io/update-test"

// expectExternalNamePrefixKey names the manifest annotation that declares
// the required prefix of the live resource's crossplane.io/external-name
// annotation (e.g. "ipv6network/" for a dual-object-type resource whose
// identity search could silently resolve against the wrong WAPI object
// type — see cmdCheckExternalNamePrefix). Optional: manifests that do not
// need this guard simply omit it.
const expectExternalNamePrefixKey = "crossplane.io/expect-external-name-prefix"

// ParseManifest reads a YAML manifest file and extracts metadata and update
// test annotations.
func ParseManifest(path string) (*Manifest, error) {
	// #nosec G304 -- path is an operator-supplied CLI argument (the
	// manifest file to test), not attacker-controlled input.
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading manifest: %w", err)
	}
	return ParseManifestBytes(data)
}

// ParseManifestBytes parses manifest YAML bytes.
func ParseManifestBytes(data []byte) (*Manifest, error) {
	var doc manifestDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parsing manifest YAML: %w", err)
	}

	if doc.APIVersion == "" || doc.Kind == "" {
		return nil, fmt.Errorf("manifest missing apiVersion or kind")
	}
	if doc.Metadata.Name == "" {
		return nil, fmt.Errorf("manifest missing metadata.name")
	}

	m := &Manifest{
		APIVersion: doc.APIVersion,
		Kind:       doc.Kind,
		Name:       doc.Metadata.Name,
		Namespace:  doc.Metadata.Namespace,
	}

	m.ExpectExternalNamePrefix = doc.Metadata.Annotations[expectExternalNamePrefixKey]

	annotation, ok := doc.Metadata.Annotations[annotationKey]
	if !ok {
		return m, nil
	}

	tests, convergeSkip, err := ParseAnnotation(annotation)
	if err != nil {
		return nil, fmt.Errorf("parsing %s annotation: %w", annotationKey, err)
	}
	m.Tests = tests
	m.ConvergeSkip = convergeSkip
	return m, nil
}

// ParseAnnotation parses the update-test annotation YAML string into a slice
// of UpdateTest entries, plus an optional top-level "converge-skip" reason.
//
// The annotation format allows a top-level "converge-skip: <reason>" line to
// appear alongside the list of field entries:
//
//	crossplane.io/update-test: |
//	  converge-skip: "atProvider.lastSyncTime changes every observe cycle"
//	  - field: name
//	    value: "updated"
//
// This is not valid as a single YAML document (a mapping key cannot be a
// sibling of top-level sequence items), so the "converge-skip:" line is
// extracted first and the remainder is parsed as a plain YAML sequence.
func ParseAnnotation(annotation string) ([]UpdateTest, string, error) {
	rest, convergeSkip, err := extractConvergeSkip(annotation)
	if err != nil {
		return nil, "", fmt.Errorf("parsing converge-skip: %w", err)
	}

	rest = strings.TrimSpace(rest)
	var tests []UpdateTest
	if rest != "" {
		if err := yaml.Unmarshal([]byte(rest), &tests); err != nil {
			return nil, "", fmt.Errorf("unmarshalling annotation: %w", err)
		}
	}

	for i, t := range tests {
		if t.Field == "" {
			return nil, "", fmt.Errorf("entry %d: field is required", i)
		}
		if t.Value == nil && t.Skip == "" {
			return nil, "", fmt.Errorf("entry %d (%s): value is required unless skip is set", i, t.Field)
		}
	}
	return tests, convergeSkip, nil
}

// extractConvergeSkip scans the annotation text line by line for a top-level
// (unindented) "converge-skip:" mapping entry, removes it from the text, and
// returns the remaining text plus the extracted reason string (empty if
// absent).
func extractConvergeSkip(annotation string) (rest string, convergeSkip string, err error) {
	lines := strings.Split(annotation, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimLeft(line, " \t")
		indent := len(line) - len(trimmed)
		if indent == 0 && strings.HasPrefix(trimmed, "converge-skip:") {
			var single map[string]string
			if uerr := yaml.Unmarshal([]byte(line), &single); uerr != nil {
				return "", "", fmt.Errorf("parsing converge-skip line %q: %w", line, uerr)
			}
			convergeSkip = single["converge-skip"]
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n"), convergeSkip, nil
}

// GroupFromAPIVersion extracts the API group from an apiVersion string.
// For "record.recorda.infobloxnios.crossplane.io/v1alpha1", returns
// "record.recorda.infobloxnios.crossplane.io".
func GroupFromAPIVersion(apiVersion string) string {
	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) < 2 {
		return ""
	}
	return parts[0]
}
