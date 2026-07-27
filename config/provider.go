/*
Copyright 2021 Upbound Inc.
*/

package config

import (
	"context"
	// Note(turkenh): we are importing this to embed provider schema document
	_ "embed"

	ujconfig "github.com/crossplane/upjet/pkg/config"
	conversiontfjson "github.com/crossplane/upjet/pkg/types/conversion/tfjson"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/pkg/errors"

	infoblox "github.com/infobloxopen/terraform-provider-infoblox/v2/infoblox"

	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/aaaarecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/arecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/cnamerecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/mxrecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/ptrrecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/srvrecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/dns/txtrecord"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ip/allocation"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ip/association"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv4/ipv4allocation"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv4/ipv4association"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv4/ipv4network"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv4/ipv4networkcontainer"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv6/ipv6allocation"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv6/ipv6association"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv6/ipv6network"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/ipv6/ipv6networkcontainer"
	"github.com/crossplane-contrib/provider-infoblox-nios/config/network/view"
)

const (
	resourcePrefix = "infoblox-nios"
	rootGroup      = resourcePrefix + "." + "crossplane.io"
	modulePath     = "github.com/crossplane-contrib/provider-infoblox-nios"
)

//go:embed schema.json
var providerSchema string

//go:embed provider-metadata.yaml
var providerMetadata string

// getProviderSchema builds a Terraform Plugin SDKv2 *schema.Provider from the
// embedded Terraform provider schema JSON. It is used during code generation
// where the real Terraform provider is not (and need not be) instantiated.
func getProviderSchema(s string) (*schema.Provider, error) {
	ps := tfjson.ProviderSchemas{}
	if err := ps.UnmarshalJSON([]byte(s)); err != nil {
		panic(err)
	}
	if len(ps.Schemas) != 1 {
		return nil, errors.Errorf("there should exactly be 1 provider schema but there are %d", len(ps.Schemas))
	}
	var rs map[string]*tfjson.Schema
	for _, v := range ps.Schemas {
		rs = v.ResourceSchemas
		break
	}
	return &schema.Provider{
		ResourcesMap: conversiontfjson.GetV2ResourceMap(rs),
	}, nil
}

// GetProvider returns provider configuration. When generationProvider is true,
// the wrapped Terraform provider is built from the embedded schema JSON (used
// during code generation). Otherwise the real in-process Terraform provider is
// used (no-fork runtime).
func GetProvider(_ context.Context, generationProvider bool) (*ujconfig.Provider, error) {
	var p *schema.Provider
	var err error
	if generationProvider {
		p, err = getProviderSchema(providerSchema)
	} else {
		p = infoblox.Provider()
	}
	if err != nil {
		return nil, errors.Wrap(err, "cannot get the Terraform provider schema")
	}

	pc := ujconfig.NewProvider([]byte(providerSchema), resourcePrefix, modulePath, []byte(providerMetadata),
		ujconfig.WithRootGroup(rootGroup),
		ujconfig.WithIncludeList(resourceList(CLIReconciledExternalNameConfigs)),
		ujconfig.WithTerraformPluginSDKIncludeList(resourceList(terraformPluginSDKExternalNameConfigs)),
		ujconfig.WithTerraformProvider(p),
		ujconfig.WithFeaturesPackage("internal/features"),
		ujconfig.WithDefaultResourceOptions(
			TerraformPluginSDKResourceConfigurator(),
			CLIReconciledResourceConfigurator(),
		))

	for _, configure := range []func(provider *ujconfig.Provider){
		// add custom resource configurators
		allocation.Configure,
		association.Configure,
		ipv4allocation.Configure,
		ipv4association.Configure,
		ipv4network.Configure,
		ipv4networkcontainer.Configure,
		ipv6allocation.Configure,
		ipv6association.Configure,
		ipv6network.Configure,
		ipv6networkcontainer.Configure,
		arecord.Configure,
		aaaarecord.Configure,
		cnamerecord.Configure,
		mxrecord.Configure,
		ptrrecord.Configure,
		srvrecord.Configure,
		txtrecord.Configure,
		view.Configure,
	} {
		configure(pc)
	}

	pc.ConfigureResources()
	return pc, nil
}
