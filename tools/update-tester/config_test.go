package main

import "testing"

// TestParseManifestBytesExpectExternalNamePrefix covers ParseManifestBytes's
// handling of the optional crossplane.io/expect-external-name-prefix
// annotation: present, present alongside a crossplane.io/update-test block,
// and absent (the common case for every manifest that predates this
// annotation).
func TestParseManifestBytesExpectExternalNamePrefix(t *testing.T) {
	cases := map[string]struct {
		reason string
		yaml   string
		want   string
	}{
		"Present": {
			reason: "the annotation value is extracted verbatim",
			yaml: `
apiVersion: network.infobloxnios.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network-v6
  annotations:
    crossplane.io/expect-external-name-prefix: "ipv6network/"
`,
			want: testIPv6NetworkPrefix,
		},
		"PresentAlongsideUpdateTest": {
			reason: "the prefix annotation and the update-test annotation are independent — parsing one must not consume or interfere with the other",
			yaml: `
apiVersion: network.infobloxnios.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network-v6
  annotations:
    crossplane.io/expect-external-name-prefix: "ipv6network/"
    crossplane.io/update-test: |
      - field: comment
        value: "Updated by update-tester"
`,
			want: testIPv6NetworkPrefix,
		},
		"Absent": {
			reason: "manifests without the annotation parse with an empty ExpectExternalNamePrefix, not an error — this is the default for every existing example",
			yaml: `
apiVersion: network.infobloxnios.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network
`,
			want: "",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			m, err := ParseManifestBytes([]byte(tc.yaml))
			if err != nil {
				t.Fatalf("%s: ParseManifestBytes() error = %v", tc.reason, err)
			}
			if m.ExpectExternalNamePrefix != tc.want {
				t.Errorf("%s: ExpectExternalNamePrefix = %q, want %q", tc.reason, m.ExpectExternalNamePrefix, tc.want)
			}
		})
	}
}

// TestParseManifestBytesExpectExternalNamePrefixAlongsideTests confirms that
// when both annotations are present, the update-test annotation's own
// parsing (Tests, ConvergeSkip) still succeeds unaffected.
func TestParseManifestBytesExpectExternalNamePrefixAlongsideTests(t *testing.T) {
	yaml := `
apiVersion: network.infobloxnios.crossplane.io/v1alpha1
kind: Network
metadata:
  name: example-network-v6
  annotations:
    crossplane.io/expect-external-name-prefix: "ipv6network/"
    crossplane.io/update-test: |
      - field: comment
        value: "Updated by update-tester"
`
	m, err := ParseManifestBytes([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseManifestBytes() error = %v", err)
	}
	if len(m.Tests) != 1 {
		t.Fatalf("expected 1 update test, got %d: %+v", len(m.Tests), m.Tests)
	}
	if m.Tests[0].Field != "comment" {
		t.Errorf("Tests[0].Field = %q, want %q", m.Tests[0].Field, "comment")
	}
}
