/*
Copyright 2022 Upbound Inc.
*/

package config

import (
	"context"
	"sort"
	"testing"
)

// realResources are the 14 Terraform resource names that exist in the embedded
// infoblox provider schema AND have a generated controller under
// internal/controller.
var realResources = []string{
	"infoblox_ip_allocation",
	"infoblox_ip_association",
	"infoblox_ipv4_network",
	"infoblox_ipv4_network_container",
	"infoblox_ipv6_network",
	"infoblox_ipv6_network_container",
	"infoblox_a_record",
	"infoblox_aaaa_record",
	"infoblox_cname_record",
	"infoblox_srv_record",
	"infoblox_txt_record",
	"infoblox_ptr_record",
	"infoblox_mx_record",
	"infoblox_network_view",
}

// TestGetProviderSchema asserts the generation-time provider is built from the
// embedded schema.json (offline, no ConfigureContextFunc / no network) and
// exposes the real resources.
func TestGetProviderSchema(t *testing.T) {
	p, err := getProviderSchema(providerSchema)
	if err != nil {
		t.Fatalf("getProviderSchema: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil *schema.Provider")
	}
	if len(p.ResourcesMap) == 0 {
		t.Fatal("expected non-empty ResourcesMap")
	}
	for _, name := range realResources {
		if _, ok := p.ResourcesMap[name]; !ok {
			t.Errorf("expected schema ResourcesMap to contain %q", name)
		}
	}
}

// TestIncludeListMatchesSchema is the bug #3 guard: every entry in the
// TerraformPluginSDK include list must resolve to a real resource in the
// generated provider. Before the phantom entries were removed this test would
// fail (the 5 non-existent names would not appear in GetProvider(...).Resources).
func TestIncludeListMatchesSchema(t *testing.T) {
	p, err := GetProvider(context.Background(), true)
	if err != nil {
		t.Fatalf("GetProvider: %v", err)
	}

	var missing []string
	for name := range terraformPluginSDKExternalNameConfigs {
		if _, ok := p.Resources[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("include list references resources that do not resolve in the schema: %v", missing)
	}

	if got := len(terraformPluginSDKExternalNameConfigs); got != len(realResources) {
		t.Fatalf("include list has %d entries, want %d (the real resources)", got, len(realResources))
	}

	// Every real resource must be present in the include list too (no drift the
	// other way).
	for _, name := range realResources {
		if _, ok := terraformPluginSDKExternalNameConfigs[name]; !ok {
			t.Errorf("real resource %q missing from the include list", name)
		}
	}
}
