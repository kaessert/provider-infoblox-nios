package catalog

import "testing"

// TestFindResourceRange verifies FindResource returns the Range descriptor
// for its slug, with the expected cluster/namespaced API groups.
func TestFindResourceRange(t *testing.T) {
	rd, ok := FindResource("range")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "range")
	}
	if rd.Kind != "Range" {
		t.Errorf("Kind = %q, want Range", rd.Kind)
	}
	if rd.ClusterGroup != "range.infobloxnios.crossplane.io" {
		t.Errorf("ClusterGroup = %q, want range.infobloxnios.crossplane.io", rd.ClusterGroup)
	}
	if rd.NamespacedGroup != "range.infobloxnios.m.crossplane.io" {
		t.Errorf("NamespacedGroup = %q, want range.infobloxnios.m.crossplane.io", rd.NamespacedGroup)
	}
}

// TestRangeFieldCounts pins the request/response/both field counts for the
// cataloged Range fields (startAddr, endAddr, networkView, network, comment,
// extAttrs -> both; template -> request; ref -> response) — a regression
// guard against accidental catalog drift.
func TestRangeFieldCounts(t *testing.T) {
	rd, ok := FindResource("range")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "range")
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

	if req != 1 {
		t.Errorf("request-scope field count = %d, want 1 (template)", req)
	}
	if resp != 1 {
		t.Errorf("response-scope field count = %d, want 1 (ref)", resp)
	}
	if both != 6 {
		t.Errorf("both-scope field count = %d, want 6 (startAddr, endAddr, networkView, network, comment, extAttrs)", both)
	}
}

// TestRangeTemplateImmutable verifies template is cataloged as Immutable,
// FieldScopeRequest (create-only), and mirrored into AtProvider from
// ForProvider (informational only) since the WAPI GetNetworkRange response
// never echoes it back.
func TestRangeTemplateImmutable(t *testing.T) {
	rd, ok := FindResource("range")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "range")
	}

	found := false
	for _, f := range rd.Fields {
		if f.Name != "Template" {
			continue
		}
		found = true
		if !f.Immutable {
			t.Errorf("Template: expected Immutable=true")
		}
		if f.Scope != FieldScopeRequest {
			t.Errorf("Template: expected Scope=FieldScopeRequest, got %v", f.Scope)
		}
		if f.OmitFromObservation {
			t.Errorf("Template: expected OmitFromObservation=false")
		}
	}
	if !found {
		t.Errorf("Range descriptor has no Template field")
	}
}

// TestRangeNetworkViewReference verifies networkView carries a cross-resource
// reference targeting NetworkView.
func TestRangeNetworkViewReference(t *testing.T) {
	rd, ok := FindResource("range")
	if !ok {
		t.Fatalf("FindResource(%q): expected found", "range")
	}

	found := false
	for _, f := range rd.Fields {
		if f.Name != kindNetworkView {
			continue
		}
		found = true
		if f.Reference == nil {
			t.Fatalf("NetworkView: expected a Reference, got nil")
		}
		if f.Reference.TargetKind != kindNetworkView {
			t.Errorf("NetworkView.Reference.TargetKind = %q, want NetworkView", f.Reference.TargetKind)
		}
		if f.Reference.TargetSlug != slugNetworkView {
			t.Errorf("NetworkView.Reference.TargetSlug = %q, want networkview", f.Reference.TargetSlug)
		}
	}
	if !found {
		t.Errorf("Range descriptor has no NetworkView field")
	}
}
