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

// Optional read-endpoint credential keys. These enable the gridmaster
// read/write split: read traffic (Observe) is routed to a gridmaster
// candidate while write traffic (Create/Update/Delete) stays on the primary
// gridmaster. Each read_* key falls back to its write counterpart when
// absent. If read_server itself is absent, the read setup is identical to the
// write setup and the split is a functional no-op (safe, backward-compatible
// default).
const (
	// ReadServer Key — gridmaster candidate host used for reads.
	ReadServer = "read_server"
	// ReadPort Key — optional read-endpoint port override.
	ReadPort = "read_port"
	// ReadSSLmode Key — optional read-endpoint sslmode override.
	ReadSSLmode = "read_sslmode"
	// ReadWapiVersion Key — optional read-endpoint WAPI version override.
	ReadWapiVersion = "read_wapi_version"
)

// TerraformSetupBuilder builds a terraform.SetupFn that configures an
// in-process (no-fork) Terraform provider client against the *write* endpoint
// (the primary gridmaster, credential key "server"). The name and signature
// are kept stable because it is the default SetupFn used by every controller.
func TerraformSetupBuilder(tfProvider *schema.Provider) terraform.SetupFn {
	return terraformSetup(tfProvider, false)
}

// TerraformReadSetupBuilder builds a terraform.SetupFn that configures an
// in-process (no-fork) Terraform provider client against the *read* endpoint
// (the gridmaster candidate, credential key "read_server", with optional
// read_port/read_sslmode/read_wapi_version overrides). Every read_* value
// falls back to its write counterpart when absent; if "read_server" is not
// set the produced setup is byte-for-byte identical to the write setup, so the
// split degrades to a no-op.
func TerraformReadSetupBuilder(tfProvider *schema.Provider) terraform.SetupFn {
	return terraformSetup(tfProvider, true)
}

// terraformSetup is the shared implementation behind both the write and read
// setup builders. The read flag selects whether read_* credential overrides
// are applied. Everything else (ProviderConfig resolution, usage tracking,
// credential extraction, and provider Configure) is identical so the two
// endpoints stay in sync.
func terraformSetup(tfProvider *schema.Provider, read bool) terraform.SetupFn { //nolint:gocyclo
	return func(ctx context.Context, c client.Client, mg resource.Managed) (terraform.Setup, error) {
		ps := terraform.Setup{}

		configRef := mg.GetProviderConfigReference()
		if configRef == nil {
			return ps, errors.New(errNoProviderConfig)
		}
		pc := &v1beta1.ProviderConfig{}
		if err := c.Get(ctx, types.NamespacedName{Name: configRef.Name}, pc); err != nil {
			return ps, errors.Wrap(err, errGetProviderConfig)
		}

		t := resource.NewProviderConfigUsageTracker(c, &v1beta1.ProviderConfigUsage{})
		if err := t.Track(ctx, mg); err != nil {
			return ps, errors.Wrap(err, errTrackUsage)
		}

		data, err := resource.CommonCredentialExtractor(ctx, pc.Spec.Credentials.Source, c, pc.Spec.Credentials.CommonCredentialSelectors)
		if err != nil {
			return ps, errors.Wrap(err, errExtractCredentials)
		}
		creds := map[string]string{}
		if err := json.Unmarshal(data, &creds); err != nil {
			return ps, errors.Wrap(err, errUnmarshalCredentials)
		}

		ps.Configuration = buildConfiguration(creds, read)

		// Configure the in-process (no-fork) Terraform provider client. The
		// provider is passed by value so that Configure operates on a copy:
		// the SDK stores the resulting meta on the receiver, so a value copy
		// yields an independent meta per invocation. This is what makes the
		// read/write split possible — the same *schema.Provider can be
		// Configured twice (once per endpoint) to obtain two independent
		// ps.Meta values without a shared ProviderConfig being mutated across
		// endpoints or across MRs.
		if err := configureNoForkClient(ctx, &ps, *tfProvider); err != nil {
			return ps, errors.Wrap(err, errConfigureNoForkClient)
		}
		return ps, nil
	}
}

// buildConfiguration assembles the terraform provider configuration map from
// the credentials secret. When read is true and a read_server is present, the
// endpoint-specific keys (server/port/sslmode/wapi_version) are overridden
// with their read_* counterparts, each falling back to the write value when
// its read_* override is absent. Auth and connection-pool settings
// (username/password/connection_timeout/pool_connections) are always shared
// between endpoints.
func buildConfiguration(creds map[string]string, read bool) map[string]any {
	cfg := map[string]any{}

	// Endpoint-specific keys, defaulting to the write (primary) values.
	server := creds[Server]
	port, hasPort := creds[Port]
	sslmode, hasSSLmode := creds[SSLmode]
	wapi, hasWAPI := creds[WapiVersion]

	if read {
		if v, ok := creds[ReadServer]; ok && v != "" {
			server = v
		}
		if v, ok := creds[ReadPort]; ok && v != "" {
			port, hasPort = v, true
		}
		if v, ok := creds[ReadSSLmode]; ok && v != "" {
			sslmode, hasSSLmode = v, true
		}
		if v, ok := creds[ReadWapiVersion]; ok && v != "" {
			wapi, hasWAPI = v, true
		}
	}

	if _, ok := creds[Server]; ok {
		cfg[Server] = server
	}
	if hasPort {
		cfg[Port] = port
	}
	if hasSSLmode {
		cfg[SSLmode] = sslmode
	}
	if hasWAPI {
		cfg[WapiVersion] = wapi
	}

	// Shared (endpoint-independent) keys.
	if v, ok := creds[Username]; ok {
		cfg[Username] = v
	}
	if v, ok := creds[Password]; ok {
		cfg[Password] = v
	}
	if v, ok := creds[ConnectionTimeout]; ok {
		cfg[ConnectionTimeout] = v
	}
	if v, ok := creds[PoolConnections]; ok {
		cfg[PoolConnections] = v
	}

	return cfg
}

// ReadSplitEnabled reports whether the given credentials configure a distinct
// read endpoint (a non-empty read_server). It lets callers decide, per
// ProviderConfig, whether the read/write split is actually active.
func ReadSplitEnabled(creds map[string]string) bool {
	v, ok := creds[ReadServer]
	return ok && v != "" && v != creds[Server]
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
