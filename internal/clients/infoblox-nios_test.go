/*
Copyright 2021 Upbound Inc.
*/

package clients

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/upjet/pkg/resource/fake"
	"github.com/hashicorp/terraform-plugin-sdk/v2/diag"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	tfsdk "github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/crossplane-contrib/provider-infoblox-nios/apis/v1beta1"
)

// TestBuildConfiguration pins the credentials-secret -> Terraform provider
// configuration mapping, including the read/write endpoint overrides and the
// connect_timeout schema-key mapping (bug #1).
func TestBuildConfiguration(t *testing.T) {
	full := map[string]string{
		Server:            "gm-primary",
		Port:              "443",
		SSLmode:           "false",
		WapiVersion:       "2.12.3",
		Username:          "admin",
		Password:          "secret",
		ConnectionTimeout: "60",
		PoolConnections:   "10",
		ReadServer:        "gm-candidate",
		ReadPort:          "8443",
		ReadSSLmode:       "true",
		ReadWapiVersion:   "2.13.1",
	}

	cases := map[string]struct {
		creds map[string]string
		read  bool
		want  map[string]any
	}{
		"WriteEndpointUsesPrimaryValues": {
			creds: full,
			read:  false,
			want: map[string]any{
				"server":           "gm-primary",
				"port":             "443",
				"sslmode":          "false",
				"wapi_version":     "2.12.3",
				"username":         "admin",
				"password":         "secret",
				"connect_timeout":  "60", // bug #1: emitted under the schema key
				"pool_connections": "10",
			},
		},
		"ReadEndpointAppliesReadOverrides": {
			creds: full,
			read:  true,
			want: map[string]any{
				"server":           "gm-candidate",
				"port":             "8443",
				"sslmode":          "true",
				"wapi_version":     "2.13.1",
				"username":         "admin",
				"password":         "secret",
				"connect_timeout":  "60",
				"pool_connections": "10",
			},
		},
		"ReadEndpointFallsBackWhenReadOverridesAbsent": {
			creds: map[string]string{
				Server:            "gm-primary",
				Port:              "443",
				SSLmode:           "false",
				WapiVersion:       "2.12.3",
				Username:          "admin",
				Password:          "secret",
				ConnectionTimeout: "60",
				PoolConnections:   "10",
				ReadServer:        "gm-candidate", // only the host differs
			},
			read: true,
			want: map[string]any{
				"server":           "gm-candidate", // read_server override
				"port":             "443",          // fallback to write port
				"sslmode":          "false",        // fallback
				"wapi_version":     "2.12.3",       // fallback
				"username":         "admin",
				"password":         "secret",
				"connect_timeout":  "60",
				"pool_connections": "10",
			},
		},
		"MinimalWriteOnly": {
			creds: map[string]string{
				Server:   "gm-primary",
				Username: "admin",
				Password: "secret",
			},
			read: false,
			want: map[string]any{
				"server":   "gm-primary",
				"username": "admin",
				"password": "secret",
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := buildConfiguration(tc.creds, tc.read)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("buildConfiguration mismatch:\n got=%#v\nwant=%#v", got, tc.want)
			}
			// Bug #1 regression guard: the deprecated schema-mismatched key must
			// never be emitted.
			if _, bad := got["connection_timeout"]; bad {
				t.Fatalf("buildConfiguration must not emit the phantom key connection_timeout; the schema field is connect_timeout")
			}
		})
	}
}

// TestReadSplitEnabled pins when a distinct read endpoint is considered active.
func TestReadSplitEnabled(t *testing.T) {
	cases := map[string]struct {
		creds map[string]string
		want  bool
	}{
		"NoReadServer":            {creds: map[string]string{Server: "gm"}, want: false},
		"EmptyReadServer":         {creds: map[string]string{Server: "gm", ReadServer: ""}, want: false},
		"ReadServerEqualsServer":  {creds: map[string]string{Server: "gm", ReadServer: "gm"}, want: false},
		"DistinctReadServer":      {creds: map[string]string{Server: "gm", ReadServer: "gm-candidate"}, want: true},
		"ReadServerNoWriteServer": {creds: map[string]string{ReadServer: "gm-candidate"}, want: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ReadSplitEnabled(tc.creds); got != tc.want {
				t.Fatalf("ReadSplitEnabled=%v want %v", got, tc.want)
			}
		})
	}
}

// mirrorProviderSchema mirrors the real infobloxopen/infoblox provider's
// top-level configuration schema field *types* (the ground truth: sslmode is
// TypeBool, connect_timeout / pool_connections are TypeInt, the rest strings).
// It is used to prove, offline and without a grid, how the SDKv2 runtime
// coerces the string values produced by buildConfiguration.
func mirrorProviderSchema(capture func(d *schema.ResourceData)) *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"server":           {Type: schema.TypeString, Optional: true},
			"port":             {Type: schema.TypeString, Optional: true},
			"username":         {Type: schema.TypeString, Optional: true},
			"password":         {Type: schema.TypeString, Optional: true},
			"wapi_version":     {Type: schema.TypeString, Optional: true},
			"sslmode":          {Type: schema.TypeBool, Optional: true},
			"connect_timeout":  {Type: schema.TypeInt, Optional: true},
			"pool_connections": {Type: schema.TypeInt, Optional: true},
		},
		ConfigureContextFunc: func(_ context.Context, d *schema.ResourceData) (any, diag.Diagnostics) {
			capture(d)
			return "sentinel-meta", nil
		},
	}
}

// TestConfigureCoercion is the bug #2 guard. The credentials secret carries
// sslmode as the string "false" (and the timeouts as strings). This test proves
// the SDKv2 schema layer weak-coerces those strings into the correctly typed Go
// values the real provider reads via d.Get(...).(bool)/(int) — i.e. the string
// is NOT silently mishandled, so no code change is warranted. It simultaneously
// proves the bug #1 fix lands the timeout under connect_timeout.
func TestConfigureCoercion(t *testing.T) {
	var got struct {
		ssl     bool
		timeout int
		pool    int
		server  string
	}
	p := mirrorProviderSchema(func(d *schema.ResourceData) {
		got.ssl = d.Get("sslmode").(bool)
		got.timeout = d.Get("connect_timeout").(int)
		got.pool = d.Get("pool_connections").(int)
		got.server = d.Get("server").(string)
	})

	cfg := buildConfiguration(map[string]string{
		Server:            "gm-primary",
		SSLmode:           "false",
		ConnectionTimeout: "60",
		PoolConnections:   "10",
		Username:          "admin",
		Password:          "secret",
	}, false)

	ps := &struct{ Configuration map[string]any }{Configuration: cfg}
	diags := p.Configure(context.Background(), &tfsdk.ResourceConfig{Config: ps.Configuration})
	if diags.HasError() {
		t.Fatalf("Configure returned errors: %v", diags)
	}
	if got.ssl != false {
		t.Fatalf("sslmode string %q coerced to %v, want false", "false", got.ssl)
	}
	if got.timeout != 60 {
		t.Fatalf("connect_timeout string %q coerced to %d, want 60", "60", got.timeout)
	}
	if got.pool != 10 {
		t.Fatalf("pool_connections coerced to %d, want 10", got.pool)
	}
	if got.server != "gm-primary" {
		t.Fatalf("server=%q", got.server)
	}

	// Prove the bug: had the timeout been emitted under the wrong key
	// (connection_timeout), connect_timeout would fall back to its zero value.
	var wrongTimeout int
	pWrong := mirrorProviderSchema(func(d *schema.ResourceData) { wrongTimeout = d.Get("connect_timeout").(int) })
	if d := pWrong.Configure(context.Background(), &tfsdk.ResourceConfig{Config: map[string]any{
		"server": "x", "connection_timeout": "60", // deliberately wrong key
	}}); d.HasError() {
		t.Fatalf("Configure(wrong key): %v", d)
	}
	if wrongTimeout != 0 {
		t.Fatalf("sanity: expected connect_timeout to be unset (0) when only connection_timeout is provided, got %d", wrongTimeout)
	}
}

// TestConfigureCoercionTrueSSL guards the true case too.
func TestConfigureCoercionTrueSSL(t *testing.T) {
	var ssl bool
	p := mirrorProviderSchema(func(d *schema.ResourceData) { ssl = d.Get("sslmode").(bool) })
	cfg := buildConfiguration(map[string]string{Server: "x", SSLmode: "true"}, false)
	if d := p.Configure(context.Background(), &tfsdk.ResourceConfig{Config: cfg}); d.HasError() {
		t.Fatalf("Configure: %v", d)
	}
	if !ssl {
		t.Fatalf("sslmode string %q coerced to %v, want true", "true", ssl)
	}
}

// --- end-to-end setup builders -------------------------------------------------

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.SchemeBuilder.AddToScheme(s); err != nil {
		t.Fatalf("add v1beta1 to scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	return s
}

func credsSecretJSON(t *testing.T) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]string{
		"server":             "gm-primary",
		"read_server":        "gm-candidate",
		"port":               "443",
		"sslmode":            "false",
		"wapi_version":       "2.12.3",
		"username":           "admin",
		"password":           "secret",
		"connection_timeout": "60",
		"pool_connections":   "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func fakeKube(t *testing.T) client.Client {
	t.Helper()
	pc := &v1beta1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: v1beta1.ProviderConfigSpec{
			Credentials: v1beta1.ProviderCredentials{
				Source: xpv1.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv1.CommonCredentialSelectors{
					SecretRef: &xpv1.SecretKeySelector{
						SecretReference: xpv1.SecretReference{Name: "creds", Namespace: "crossplane-system"},
						Key:             "credentials",
					},
				},
			},
		},
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "crossplane-system"},
		Data:       map[string][]byte{"credentials": credsSecretJSON(t)},
	}
	return fakeclient.NewClientBuilder().WithScheme(newScheme(t)).WithObjects(pc, sec).Build()
}

func trackedMR() *fake.Terraformed {
	tr := &fake.Terraformed{}
	tr.TypeMeta = metav1.TypeMeta{APIVersion: "infoblox-nios.crossplane.io/v1alpha1", Kind: "ARecord"}
	tr.SetUID(types.UID("mr-uid-1"))
	tr.SetName("a-record-1")
	tr.SetProviderConfigReference(&xpv1.Reference{Name: "default"})
	return tr
}

// TestSetupBuilders exercises the full write and read setup builders end-to-end
// against a fake kube client: ProviderConfig resolution, usage tracking,
// credential extraction, buildConfiguration and the SDKv2 Configure. It asserts
// the write builder targets the primary and the read builder targets the
// candidate, and that ps.Meta is populated.
func TestSetupBuilders(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		read       bool
		wantServer string
	}{
		{"WriteBuilderTargetsPrimary", false, "gm-primary"},
		{"ReadBuilderTargetsCandidate", true, "gm-candidate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotServer string
			var gotSSL bool
			var gotTimeout int
			p := mirrorProviderSchema(func(d *schema.ResourceData) {
				gotServer = d.Get("server").(string)
				gotSSL = d.Get("sslmode").(bool)
				gotTimeout = d.Get("connect_timeout").(int)
			})

			var fn = TerraformSetupBuilder(p)
			if tc.read {
				fn = TerraformReadSetupBuilder(p)
			}

			ps, err := fn(ctx, fakeKube(t), trackedMR())
			if err != nil {
				t.Fatalf("setup fn: %v", err)
			}
			if ps.Configuration["server"] != tc.wantServer {
				t.Fatalf("server=%v want %v", ps.Configuration["server"], tc.wantServer)
			}
			if ps.Meta != "sentinel-meta" {
				t.Fatalf("ps.Meta not populated by Configure: %#v", ps.Meta)
			}
			if gotServer != tc.wantServer {
				t.Fatalf("Configure saw server=%q want %q", gotServer, tc.wantServer)
			}
			if gotSSL {
				t.Fatalf("sslmode should coerce to false")
			}
			if gotTimeout != 60 {
				t.Fatalf("connect_timeout should be honored (60), got %d", gotTimeout)
			}
		})
	}
}
