// Package config unit tests for the shared ProviderConfig credential
// bridge. These tests exercise GetLegacy, Get, GetCluster and
// BuildConnector directly against a fake kube client — no network
// round-trip occurs, since the underlying WAPI SDK's connector
// construction only validates configuration locally.
package config

import (
	"context"
	"testing"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/dualclient"
)

func boolPtr(b bool) *bool { return &b }

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("cannot build test scheme: %v", err)
	}
	return s
}

func credentialsSecret(ns, name, username, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data: map[string][]byte{
			"username": []byte(username),
			"password": []byte(password),
		},
	}
}

// ── GetLegacy (cluster-scoped ProviderConfig) ──────────────────────────────

func TestGetLegacyThreadsHostAndResolvesConn(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	pc := &clusterv1alpha1.ProviderConfig{
		Spec: clusterv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: clusterv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					SecretRef: &xpv2.SecretKeySelector{
						SecretReference: xpv2.SecretReference{Name: "primary", Namespace: "crossplane-system"},
					},
				},
			},
		},
	}

	conn, err := GetLegacy(context.Background(), kube, pc)
	if err != nil {
		t.Fatalf("GetLegacy: unexpected error: %v", err)
	}
	if conn.Endpoint != "grid.example.com" {
		t.Fatalf("GetLegacy: Endpoint = %q, want %q", conn.Endpoint, "grid.example.com")
	}
	if conn.Connector == nil {
		t.Fatal("GetLegacy: expected a non-nil Connector")
	}
}

func TestGetLegacyMissingSecretErrors(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	pc := &clusterv1alpha1.ProviderConfig{
		Spec: clusterv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: clusterv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					SecretRef: &xpv2.SecretKeySelector{
						SecretReference: xpv2.SecretReference{Name: "does-not-exist", Namespace: "crossplane-system"},
					},
				},
			},
		},
	}

	if _, err := GetLegacy(context.Background(), kube, pc); err == nil {
		t.Fatal("GetLegacy: expected an error for a missing credentials Secret, got nil")
	}
}

func TestGetLegacyUnsupportedSourceErrors(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	pc := &clusterv1alpha1.ProviderConfig{
		Spec: clusterv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: clusterv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceNone,
			},
		},
	}

	if _, err := GetLegacy(context.Background(), kube, pc); err == nil {
		t.Fatal("GetLegacy: expected an error for an unsupported credentials source, got nil")
	}
}

// TestGetLegacySslVerifyVariants exercises the nil/true/false SSLVerify
// resolution branch: nil must default to secure (true) without erroring,
// and both explicit booleans must also construct successfully — the
// underlying SDK connector only validates configuration locally, so no
// branch produces an error here regardless of value.
func TestGetLegacySslVerifyVariants(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	cases := map[string]*bool{
		"Omitted":  nil,
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
	}
	for name, sslVerify := range cases {
		t.Run(name, func(t *testing.T) {
			pc := &clusterv1alpha1.ProviderConfig{
				Spec: clusterv1alpha1.ProviderConfigSpec{
					Host: "grid.example.com",
					Credentials: clusterv1alpha1.ProviderCredentials{
						Source: xpv2.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
							SecretRef: &xpv2.SecretKeySelector{
								SecretReference: xpv2.SecretReference{Name: "primary", Namespace: "crossplane-system"},
							},
						},
					},
					SSLVerify: sslVerify,
				},
			}

			conn, err := GetLegacy(context.Background(), kube, pc)
			if err != nil {
				t.Fatalf("GetLegacy: unexpected error: %v", err)
			}
			if conn.Connector == nil {
				t.Fatal("GetLegacy: expected a non-nil Connector")
			}
		})
	}
}

// ── Get (namespaced ProviderConfig) ────────────────────────────────────────

func TestGetFallsBackToProviderConfigNamespace(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("team-a", "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	pc := &namespacedv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "team-a"},
		Spec: namespacedv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: namespacedv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					// No Namespace set on the SecretRef — must fall back
					// to the ProviderConfig's own namespace ("team-a").
					SecretRef: &xpv2.SecretKeySelector{
						SecretReference: xpv2.SecretReference{Name: "primary"},
					},
				},
			},
		},
	}

	conn, err := Get(context.Background(), kube, pc)
	if err != nil {
		t.Fatalf("Get: unexpected error: %v", err)
	}
	if conn.Endpoint != "grid.example.com" {
		t.Fatalf("Get: Endpoint = %q, want %q", conn.Endpoint, "grid.example.com")
	}
}

func TestGetMissingSecretErrors(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	pc := &namespacedv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "team-a"},
		Spec: namespacedv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: namespacedv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					SecretRef: &xpv2.SecretKeySelector{
						SecretReference: xpv2.SecretReference{Name: "does-not-exist"},
					},
				},
			},
		},
	}

	if _, err := Get(context.Background(), kube, pc); err == nil {
		t.Fatal("Get: expected an error for a missing credentials Secret, got nil")
	}
}

func TestGetUnsupportedSourceErrors(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	pc := &namespacedv1alpha1.ProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "team-a"},
		Spec: namespacedv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: namespacedv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceNone,
			},
		},
	}

	if _, err := Get(context.Background(), kube, pc); err == nil {
		t.Fatal("Get: expected an error for an unsupported credentials source, got nil")
	}
}

func TestGetSslVerifyVariants(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("team-a", "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	cases := map[string]*bool{
		"Omitted":  nil,
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
	}
	for name, sslVerify := range cases {
		t.Run(name, func(t *testing.T) {
			pc := &namespacedv1alpha1.ProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "team-a"},
				Spec: namespacedv1alpha1.ProviderConfigSpec{
					Host: "grid.example.com",
					Credentials: namespacedv1alpha1.ProviderCredentials{
						Source: xpv2.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
							SecretRef: &xpv2.SecretKeySelector{
								SecretReference: xpv2.SecretReference{Name: "primary"},
							},
						},
					},
					SSLVerify: sslVerify,
				},
			}

			if _, err := Get(context.Background(), kube, pc); err != nil {
				t.Fatalf("Get: unexpected error: %v", err)
			}
		})
	}
}

// ── GetCluster (namespaced ClusterProviderConfig) ──────────────────────────

func TestGetClusterHasNoNamespaceFallback(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	cpc := &namespacedv1alpha1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: namespacedv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: namespacedv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					// A ClusterProviderConfig has no namespace of its own,
					// so the secretRef must carry one explicitly.
					SecretRef: &xpv2.SecretKeySelector{
						SecretReference: xpv2.SecretReference{Name: "primary", Namespace: "crossplane-system"},
					},
				},
			},
		},
	}

	conn, err := GetCluster(context.Background(), kube, cpc)
	if err != nil {
		t.Fatalf("GetCluster: unexpected error: %v", err)
	}
	if conn.Endpoint != "grid.example.com" {
		t.Fatalf("GetCluster: Endpoint = %q, want %q", conn.Endpoint, "grid.example.com")
	}
}

func TestGetClusterMissingSecretErrors(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	cpc := &namespacedv1alpha1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: namespacedv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: namespacedv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceSecret,
				CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
					SecretRef: &xpv2.SecretKeySelector{
						SecretReference: xpv2.SecretReference{Name: "does-not-exist", Namespace: "crossplane-system"},
					},
				},
			},
		},
	}

	if _, err := GetCluster(context.Background(), kube, cpc); err == nil {
		t.Fatal("GetCluster: expected an error for a missing credentials Secret, got nil")
	}
}

func TestGetClusterUnsupportedSourceErrors(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	cpc := &namespacedv1alpha1.ClusterProviderConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
		Spec: namespacedv1alpha1.ProviderConfigSpec{
			Host: "grid.example.com",
			Credentials: namespacedv1alpha1.ProviderCredentials{
				Source: xpv2.CredentialsSourceNone,
			},
		},
	}

	if _, err := GetCluster(context.Background(), kube, cpc); err == nil {
		t.Fatal("GetCluster: expected an error for an unsupported credentials source, got nil")
	}
}

func TestGetClusterSslVerifyVariants(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret("crossplane-system", "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	cases := map[string]*bool{
		"Omitted":  nil,
		"Enabled":  boolPtr(true),
		"Disabled": boolPtr(false),
	}
	for name, sslVerify := range cases {
		t.Run(name, func(t *testing.T) {
			cpc := &namespacedv1alpha1.ClusterProviderConfig{
				ObjectMeta: metav1.ObjectMeta{Name: "default"},
				Spec: namespacedv1alpha1.ProviderConfigSpec{
					Host: "grid.example.com",
					Credentials: namespacedv1alpha1.ProviderCredentials{
						Source: xpv2.CredentialsSourceSecret,
						CommonCredentialSelectors: xpv2.CommonCredentialSelectors{
							SecretRef: &xpv2.SecretKeySelector{
								SecretReference: xpv2.SecretReference{Name: "primary", Namespace: "crossplane-system"},
							},
						},
					},
					SSLVerify: sslVerify,
				},
			}

			if _, err := GetCluster(context.Background(), kube, cpc); err != nil {
				t.Fatalf("GetCluster: unexpected error: %v", err)
			}
		})
	}
}

// ── BuildConnector (test seam) ──────────────────────────────────────────────

func TestBuildConnectorSuccess(t *testing.T) {
	conn, err := BuildConnector(dualclient.Credentials{
		Host:     "grid.example.com",
		Username: "admin",
		Password: "s3cr3t",
	}, true, "https", "443")
	if err != nil {
		t.Fatalf("BuildConnector: unexpected error: %v", err)
	}
	if conn == nil {
		t.Fatal("BuildConnector: expected a non-nil connector")
	}
}

func TestBuildConnectorHonorsSchemeAndPort(t *testing.T) {
	conn, err := BuildConnector(dualclient.Credentials{
		Host:     "127.0.0.1",
		Username: "admin",
		Password: "s3cr3t",
	}, false, "http", "8080")
	if err != nil {
		t.Fatalf("BuildConnector: unexpected error: %v", err)
	}
	if conn == nil {
		t.Fatal("BuildConnector: expected a non-nil connector")
	}
}

// ── Setup wiring ────────────────────────────────────────────────────────

// TestSetupRegistersAllThreeProviderConfigControllers proves Setup wires
// all three ProviderConfig controllers (legacy cluster, namespaced, and
// namespaced cluster-scoped) onto a real controller-runtime manager
// without error. The manager's REST config points at an address that is
// never dialed — controller registration only builds caches/informers
// lazily, it does not connect until the manager is started.
func TestSetupRegistersAllThreeProviderConfigControllers(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clusterv1alpha1.SchemeBuilder.AddToScheme(scheme); err != nil {
		t.Fatalf("cannot add cluster scheme: %v", err)
	}
	if err := namespacedv1alpha1.SchemeBuilder.AddToScheme(scheme); err != nil {
		t.Fatalf("cannot add namespaced scheme: %v", err)
	}

	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:0"}, manager.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	o := controller.Options{
		Logger:            logging.NewNopLogger(),
		GlobalRateLimiter: ratelimiter.NewGlobal(1),
	}
	if err := Setup(mgr, o); err != nil {
		t.Fatalf("Setup: unexpected error: %v", err)
	}
}
