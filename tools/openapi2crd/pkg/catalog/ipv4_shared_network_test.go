package catalog

import "testing"

// TestFindResourceIPv4SharedNetwork verifies FindResource returns the
// IPv4SharedNetwork descriptor for its slug, with the expected dual-scope
// API groups.
func TestFindResourceIPv4SharedNetwork(t *testing.T) {
	rd, ok := FindResource("ipv4sharednetwork")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "ipv4sharednetwork")
	}
	if rd.Kind != "IPv4SharedNetwork" {
		t.Errorf("Kind = %q, want IPv4SharedNetwork", rd.Kind)
	}
	if rd.ClusterGroup != "ipv4sharednetwork.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want ipv4sharednetwork.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "ipv4sharednetwork.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want ipv4sharednetwork.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestIPv4SharedNetworkFieldCounts pins the request/response/both field
// counts documented in tools/openapi/inventory.md's "### IPv4SharedNetwork"
// section (request=0, response=7, both=8) — a regression guard: uniform or
// drifted counts would indicate a catalog authoring bug.
func TestIPv4SharedNetworkFieldCounts(t *testing.T) {
	rd, ok := FindResource("ipv4sharednetwork")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "ipv4sharednetwork")
	}

	var req, resp, both int
	for _, f := range rd.Fields {
		switch f.Scope {
		case FieldScopeRequest:
			req++
		case FieldScopeResponse:
			resp++
		case FieldScopeBoth:
			both++
		}
	}

	if req != 0 {
		t.Errorf("request-scope field count = %d, want 0", req)
	}
	if resp != 7 {
		t.Errorf("response-scope field count = %d, want 7", resp)
	}
	if both != 8 {
		t.Errorf("both-scope field count = %d, want 8", both)
	}
}

// TestIPv4SharedNetworkImmutableFields verifies networkView is marked
// Immutable (live-verified WAPI supports=rws) and no other field is — the
// CEL self == oldSelf XValidation rule depends on this flag.
func TestIPv4SharedNetworkImmutableFields(t *testing.T) {
	rd, ok := FindResource("ipv4sharednetwork")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "ipv4sharednetwork")
	}

	wantImmutable := map[string]bool{
		fieldNameJSON: false,
		"networks":    false,
		"networkView": true,
		"comment":     false,
		"extAttrs":    false,
		"disable":     false,
		"useOptions":  false,
		"options":     false,
		"ref":         false,
	}

	seen := map[string]bool{}
	for _, f := range rd.Fields {
		want, ok := wantImmutable[f.JSONName]
		if !ok {
			continue
		}
		seen[f.JSONName] = true
		if f.Immutable != want {
			t.Errorf("field %q Immutable = %v, want %v", f.JSONName, f.Immutable, want)
		}
	}
	for name := range wantImmutable {
		if !seen[name] {
			t.Errorf("expected field %q not found in IPv4SharedNetwork descriptor", name)
		}
	}
}

// TestIPv4SharedNetworkSliceFieldsNoOmitEmpty verifies the networks and
// options slice fields, and the extAttrs map field, use raw Go slice/map
// types (never a pointer) so the generator's isOmitEmpty check keeps
// omitempty off — required for CEL round-trip safety.
func TestIPv4SharedNetworkSliceFieldsNoOmitEmpty(t *testing.T) {
	rd, ok := FindResource("ipv4sharednetwork")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "ipv4sharednetwork")
	}

	wantPrefix := map[string]string{
		"networks": "[]",
		"options":  "[]",
		"extAttrs": "map[",
	}

	seen := map[string]bool{}
	for _, f := range rd.Fields {
		prefix, ok := wantPrefix[f.JSONName]
		if !ok {
			continue
		}
		seen[f.JSONName] = true
		if len(f.GoType) < len(prefix) || f.GoType[:len(prefix)] != prefix {
			t.Errorf("field %q GoType = %q, want prefix %q", f.JSONName, f.GoType, prefix)
		}
	}
	for name := range wantPrefix {
		if !seen[name] {
			t.Errorf("expected field %q not found in IPv4SharedNetwork descriptor", name)
		}
	}
}

// TestIPv4SharedNetworkNestedTypes verifies the IPv4SharedNetwork descriptor
// carries the IPv4SharedNetworkDhcpOption nested type used by the options
// field.
func TestIPv4SharedNetworkNestedTypes(t *testing.T) {
	rd, ok := FindResource("ipv4sharednetwork")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "ipv4sharednetwork")
	}
	found := false
	for _, nt := range rd.NestedTypes {
		if nt.TypeName == "IPv4SharedNetworkDhcpOption" {
			found = true
			if len(nt.Fields) == 0 {
				t.Errorf("IPv4SharedNetworkDhcpOption nested type has zero fields")
			}
		}
	}
	if !found {
		t.Errorf("IPv4SharedNetwork descriptor missing IPv4SharedNetworkDhcpOption nested type")
	}
}

// TestIPv4SharedNetworkNetworksReference verifies the networks field now
// carries the cross-resource Reference to Network (cluster scope), wired
// once the Network managed resource merged to main — see ipv4SharedNetwork's
// doc comment. It uses the cidr-match extractor (referencehelpers.ExtractField
// reading "spec.forProvider.network") rather than the default
// reference.ExternalName(), since `networks` entries are CIDRs, not the
// referenced Network's external name.
func TestIPv4SharedNetworkNetworksReference(t *testing.T) {
	rd, ok := FindResource("ipv4sharednetwork")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "ipv4sharednetwork")
	}

	var found bool
	for _, f := range rd.Fields {
		if f.JSONName != "networks" {
			continue
		}
		found = true
		if f.Reference == nil {
			t.Fatalf("networks field has no Reference wired; expected the Network cross-resource reference")
		}
		if f.Reference.TargetKind != "Network" {
			t.Errorf("networks Reference.TargetKind = %q, want %q", f.Reference.TargetKind, "Network")
		}
		if f.Reference.TargetSlug != "network" {
			t.Errorf("networks Reference.TargetSlug = %q, want %q", f.Reference.TargetSlug, "network")
		}
		if f.Reference.TargetScope != testScopeCluster {
			t.Errorf("networks Reference.TargetScope = %q, want %q", f.Reference.TargetScope, testScopeCluster)
		}
		wantExtractor := extractFieldFuncPath + `("spec.forProvider.network")`
		if f.Reference.Extractor != wantExtractor {
			t.Errorf("networks Reference.Extractor = %q, want %q", f.Reference.Extractor, wantExtractor)
		}
	}
	if !found {
		t.Fatalf("networks field not found on IPv4SharedNetwork descriptor")
	}
}
