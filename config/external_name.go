/*
Copyright 2022 Upbound Inc.
*/

package config

import "github.com/crossplane/upjet/pkg/config"

// terraformPluginSDKExternalNameConfigs contains all external name
// configurations for the resources reconciled through the no-fork
// (Terraform Plugin SDKv2, in-process) runtime.
var terraformPluginSDKExternalNameConfigs = map[string]config.ExternalName{
	"infoblox_ip_allocation":          config.IdentifierFromProvider,
	"infoblox_ip_association":         config.IdentifierFromProvider,
	"infoblox_ipv4_network":           config.IdentifierFromProvider,
	"infoblox_ipv4_network_container": config.IdentifierFromProvider,
	"infoblox_ipv6_network":           config.IdentifierFromProvider,
	"infoblox_ipv6_network_container": config.IdentifierFromProvider,
	"infoblox_a_record":               config.IdentifierFromProvider,
	"infoblox_aaaa_record":            config.IdentifierFromProvider,
	"infoblox_cname_record":           config.IdentifierFromProvider,
	"infoblox_srv_record":             config.IdentifierFromProvider,
	"infoblox_txt_record":             config.IdentifierFromProvider,
	"infoblox_ptr_record":             config.IdentifierFromProvider,
	"infoblox_mx_record":              config.IdentifierFromProvider,
	"infoblox_network_view":           config.IdentifierFromProvider,
}

// CLIReconciledExternalNameConfigs contains the external name configurations
// for the resources still reconciled through the CLI/Terraform-workspace
// runtime. Empty here: all resources use the no-fork SDK runtime.
var CLIReconciledExternalNameConfigs = map[string]config.ExternalName{}

// resourceConfigurator applies external name configuration and pins the API
// version for a resource found in the given external-name map.
func resourceConfigurator(m map[string]config.ExternalName) config.ResourceOption {
	return func(r *config.Resource) {
		e, ok := m[r.Name]
		if !ok {
			return
		}
		// Preserve the versions these resources currently ship with.
		r.Version = "v1alpha1"
		r.ExternalName = e
	}
}

// TerraformPluginSDKResourceConfigurator returns a ResourceOption that
// configures the resources reconciled through the no-fork SDK runtime.
func TerraformPluginSDKResourceConfigurator() config.ResourceOption {
	return resourceConfigurator(terraformPluginSDKExternalNameConfigs)
}

// CLIReconciledResourceConfigurator returns a ResourceOption that configures
// the resources still reconciled through the CLI runtime.
func CLIReconciledResourceConfigurator() config.ResourceOption {
	return resourceConfigurator(CLIReconciledExternalNameConfigs)
}

// resourceList returns the exact-match (regex-anchored) list of resource names
// contained in the given external-name map.
func resourceList(m map[string]config.ExternalName) []string {
	l := make([]string, len(m))
	i := 0
	for name := range m {
		// $ is added to match the exact string since the format is regex.
		l[i] = name + "$"
		i++
	}
	return l
}
