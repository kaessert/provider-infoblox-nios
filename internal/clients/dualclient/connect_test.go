package dualclient

import (
	"context"
	"testing"

	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// testNamespace is the namespace used across these tests for both
// credentials Secrets and secretRef lookups.
const testNamespace = "crossplane-system"

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

func TestExtractCredentialsSuccess(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret(testNamespace, "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "primary", Namespace: testNamespace},
	}, "")
	if err != nil {
		t.Fatalf("ExtractCredentials: unexpected error: %v", err)
	}
	if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
		t.Fatalf("ExtractCredentials returned unexpected creds: %+v", creds)
	}
}

// TestExtractCredentialsEmptyHost pins the fail-loudly contract: an empty
// host argument is a caller bug or a ProviderConfig that predates the host
// field, not a malformed Secret, and must be rejected before any Secret
// lookup happens.
func TestExtractCredentialsEmptyHost(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret(testNamespace, "primary", "admin", "s3cr3t")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	_, err := ExtractCredentials(context.Background(), kube, "", xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "primary", Namespace: testNamespace},
	}, "")
	if err == nil {
		t.Fatal("ExtractCredentials: expected an error for an empty host, got nil")
	}
}

// TestExtractCredentialsIgnoresSecretSslVerifyKey pins the sslVerify
// migration: the ssl_verify Secret key is fully ignored — TLS
// verification is a ProviderConfig-level policy field now, not read from
// this Secret at all. Credentials carries no SslVerify field to prove it.
func TestExtractCredentialsIgnoresSecretSslVerifyKey(t *testing.T) {
	scheme := newTestScheme(t)
	secret := credentialsSecret(testNamespace, "primary", "admin", "s3cr3t")
	secret.Data["ssl_verify"] = []byte("false")
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	creds, err := ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "primary", Namespace: testNamespace},
	}, "")
	if err != nil {
		t.Fatalf("ExtractCredentials: unexpected error: %v", err)
	}
	if creds.Host != "grid.example.com" || creds.Username != "admin" || creds.Password != "s3cr3t" {
		t.Fatalf("ExtractCredentials returned unexpected creds: %+v", creds)
	}
}

func TestExtractCredentialsMissingSecret(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()

	_, err := ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "does-not-exist", Namespace: testNamespace},
	}, "")
	if err == nil {
		t.Fatal("expected an error for a missing readEndpoint credentials Secret, got nil")
	}
}

func TestExtractCredentialsMissingKeys(t *testing.T) {
	scheme := newTestScheme(t)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "incomplete", Namespace: testNamespace},
		Data:       map[string][]byte{"username": []byte("admin")}, // missing password
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(secret).Build()

	_, err := ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceSecret, &xpv2.SecretKeySelector{
		SecretReference: xpv2.SecretReference{Name: "incomplete", Namespace: testNamespace},
	}, "")
	if err == nil {
		t.Fatal("expected an error for a credentials Secret missing required keys, got nil")
	}
}

func TestExtractCredentialsNoSecretRef(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	if _, err := ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceSecret, nil, ""); err == nil {
		t.Fatal("expected an error when secretRef is nil, got nil")
	}
}

func TestExtractCredentialsUnsupportedSource(t *testing.T) {
	kube := fake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()
	if _, err := ExtractCredentials(context.Background(), kube, "grid.example.com", xpv2.CredentialsSourceInjectedIdentity, nil, ""); err == nil {
		t.Fatal("expected an error for an unsupported credentials source, got nil")
	}
}

func TestConnectPassthroughWhenNoReadCreds(t *testing.T) {
	var built []Credentials
	factory := func(creds Credentials, _ bool) (ibclient.IBObjectManager, error) {
		built = append(built, creds)
		return newFakeObjMgr(), nil
	}

	c, err := Connect(Credentials{Host: "primary.example.com"}, nil, true, factory, nil)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if c.HasCandidate() {
		t.Fatal("Connect with nil readCreds must produce a passthrough client")
	}
	if len(built) != 1 {
		t.Fatalf("expected exactly one ObjectManager built for a passthrough Connect, got %d", len(built))
	}
}

func TestConnectBuildsCandidateWhenReadCredsPresent(t *testing.T) {
	factory := func(creds Credentials, _ bool) (ibclient.IBObjectManager, error) {
		return newFakeObjMgr(), nil
	}
	readCreds := Credentials{Host: "candidate.example.com"}
	breaker := NewCircuitBreaker(5, 0)

	c, err := Connect(Credentials{Host: "primary.example.com"}, &readCreds, true, factory, breaker)
	if err != nil {
		t.Fatalf("Connect: unexpected error: %v", err)
	}
	if !c.HasCandidate() {
		t.Fatal("Connect with non-nil readCreds must produce a client with a candidate")
	}
	if c.Breaker() != breaker {
		t.Fatal("Connect must attach the supplied circuit breaker to the candidate client")
	}
}

// TestConnectForwardsSslVerifyToBothEndpoints pins the design rule that a
// single ProviderConfig's sslVerify governs both the primary and
// candidate endpoints — Connect must forward the same value to every
// ObjectManagerFactory call.
func TestConnectForwardsSslVerifyToBothEndpoints(t *testing.T) {
	for name, sslVerify := range map[string]bool{"Enabled": true, "Disabled": false} {
		t.Run(name, func(t *testing.T) {
			var seen []bool
			factory := func(_ Credentials, sv bool) (ibclient.IBObjectManager, error) {
				seen = append(seen, sv)
				return newFakeObjMgr(), nil
			}
			readCreds := Credentials{Host: "candidate.example.com"}

			if _, err := Connect(Credentials{Host: "primary.example.com"}, &readCreds, sslVerify, factory, nil); err != nil {
				t.Fatalf("Connect: unexpected error: %v", err)
			}
			if len(seen) != 2 {
				t.Fatalf("expected sslVerify forwarded to both primary and candidate factory calls, got %d calls", len(seen))
			}
			for i, sv := range seen {
				if sv != sslVerify {
					t.Errorf("call %d: got sslVerify=%v, want %v", i, sv, sslVerify)
				}
			}
		})
	}
}

func TestConnectFailsLoudlyOnBadPrimaryCreds(t *testing.T) {
	factory := func(Credentials, bool) (ibclient.IBObjectManager, error) {
		return nil, errNewCandidateObjectManagerTestStub
	}
	if _, err := Connect(Credentials{}, nil, true, factory, nil); err == nil {
		t.Fatal("expected Connect to surface a primary ObjectManager construction failure")
	}
}

func TestConnectFailsLoudlyOnBadCandidateCreds(t *testing.T) {
	calls := 0
	factory := func(Credentials, bool) (ibclient.IBObjectManager, error) {
		calls++
		if calls == 1 {
			// primary succeeds
			return newFakeObjMgr(), nil
		}
		// candidate (readEndpoint) fails — Connect must return an error,
		// not silently fall back to a primary-only passthrough client.
		return nil, errNewCandidateObjectManagerTestStub
	}
	readCreds := Credentials{Host: "candidate.example.com"}

	c, err := Connect(Credentials{Host: "primary.example.com"}, &readCreds, true, factory, nil)
	if err == nil {
		t.Fatal("expected Connect to fail loudly when the readEndpoint ObjectManager cannot be built")
	}
	if c != nil {
		t.Fatal("Connect must not return a partially-built client on candidate failure")
	}
}

// errNewCandidateObjectManagerTestStub is a sentinel error used by the
// fail-loudly tests above; its message is irrelevant.
var errNewCandidateObjectManagerTestStub = errStub("simulated object manager construction failure")

type errStub string

func (e errStub) Error() string { return string(e) }
