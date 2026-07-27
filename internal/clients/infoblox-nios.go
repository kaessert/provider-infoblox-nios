/*
Copyright 2021 Upbound Inc.
*/

package clients

import (
	"context"
	"encoding/json"

	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tfsdk "github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/crossplane/upjet/pkg/terraform"

	"github.com/crossplane-contrib/provider-infoblox-nios/apis/v1beta1"
)

const (
	// error messages
	errNoProviderConfig      = "no providerConfigRef provided"
	errGetProviderConfig     = "cannot get referenced ProviderConfig"
	errTrackUsage            = "cannot track ProviderConfig usage"
	errExtractCredentials    = "cannot extract credentials"
	errUnmarshalCredentials  = "cannot unmarshal infoblox-nios credentials as JSON"
	errConfigureNoForkClient = "cannot configure the no-fork Terraform provider client"
)

const (
	// Server Key
	Server = "server"
	// Username Key
	Username = "username"
	// Password Key
	Password = "password"
	// SSLmode Key
	SSLmode = "sslmode"
	// Port Key
	Port = "port"
	// ConnectionTimeout Key
	ConnectionTimeout = "connection_timeout"
	// PoolConnections Key
	PoolConnections = "pool_connections"
	// WapiVersion Key
	WapiVersion = "wapi_version"
)

// TerraformSetupBuilder builds Terraform a terraform.SetupFn function which
// returns Terraform provider setup configuration
func TerraformSetupBuilder(tfProvider *schema.Provider) terraform.SetupFn { //nolint:gocyclo
	return func(ctx context.Context, client client.Client, mg resource.Managed) (terraform.Setup, error) {
		ps := terraform.Setup{}

		configRef := mg.GetProviderConfigReference()
		if configRef == nil {
			return ps, errors.New(errNoProviderConfig)
		}
		pc := &v1beta1.ProviderConfig{}
		if err := client.Get(ctx, types.NamespacedName{Name: configRef.Name}, pc); err != nil {
			return ps, errors.Wrap(err, errGetProviderConfig)
		}

		t := resource.NewProviderConfigUsageTracker(client, &v1beta1.ProviderConfigUsage{})
		if err := t.Track(ctx, mg); err != nil {
			return ps, errors.Wrap(err, errTrackUsage)
		}

		data, err := resource.CommonCredentialExtractor(ctx, pc.Spec.Credentials.Source, client, pc.Spec.Credentials.CommonCredentialSelectors)
		if err != nil {
			return ps, errors.Wrap(err, errExtractCredentials)
		}
		creds := map[string]string{}
		if err := json.Unmarshal(data, &creds); err != nil {
			return ps, errors.Wrap(err, errUnmarshalCredentials)
		}

		// Set credentials in Terraform provider configuration.
		ps.Configuration = map[string]any{}
		if v, ok := creds[Server]; ok {
			ps.Configuration[Server] = v
		}
		if v, ok := creds[Username]; ok {
			ps.Configuration[Username] = v
		}
		if v, ok := creds[Password]; ok {
			ps.Configuration[Password] = v
		}
		if v, ok := creds[SSLmode]; ok {
			ps.Configuration[SSLmode] = v
		}
		if v, ok := creds[Port]; ok {
			ps.Configuration[Port] = v
		}
		if v, ok := creds[ConnectionTimeout]; ok {
			ps.Configuration[ConnectionTimeout] = v
		}
		if v, ok := creds[PoolConnections]; ok {
			ps.Configuration[PoolConnections] = v
		}
		if v, ok := creds[WapiVersion]; ok {
			ps.Configuration[WapiVersion] = v
		}

		// Configure the in-process (no-fork) Terraform provider client. The
		// provider is passed by value so that Configure operates on a copy:
		// the SDK configures the provider once and we must avoid a shared
		// ProviderConfig being mutated concurrently across MRs.
		if err := configureNoForkClient(ctx, &ps, *tfProvider); err != nil {
			return ps, errors.Wrap(err, errConfigureNoForkClient)
		}
		return ps, nil
	}
}

// configureNoForkClient configures the given Terraform Plugin SDKv2 provider
// with the credentials populated in ps.Configuration and stores the resulting
// provider meta on ps.Meta for the no-fork runtime.
func configureNoForkClient(ctx context.Context, ps *terraform.Setup, p schema.Provider) error {
	diag := p.Configure(context.WithoutCancel(ctx), &tfsdk.ResourceConfig{
		Config: ps.Configuration,
	})
	if diag != nil && diag.HasError() {
		return errors.Errorf("failed to configure the provider: %v", diag)
	}
	ps.Meta = p.Meta()
	return nil
}
