// connect.go builds authenticated dual-endpoint Clients from ProviderConfig
// credentials. It centralizes the WAPI credentials Secret contract
// (host/username/password[/ssl_verify] keys) and enforces fail-loudly
// behavior when a readEndpoint is configured but its credentials are
// missing or invalid — misconfiguration must be visible, never silently
// downgraded to primary-only routing.
package dualclient

import (
	"context"

	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Error constants — all errors use the crossplane-runtime errors package.
const (
	errUnsupportedCreds          = "unsupported credentials source: only Secret is supported"
	errNoSecretRef               = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errGetSecret                 = "cannot get credentials secret"
	errMissingCredKey            = "credentials secret is missing one of the required host/username/password keys"
	errNewPrimaryObjectManager   = "cannot create primary Infoblox NIOS WAPI object manager"
	errNewCandidateObjectManager = "cannot create candidate (readEndpoint) Infoblox NIOS WAPI object manager: misconfigured readEndpoint must fail loudly, not silently degrade to primary-only"
)

// Credentials holds the WAPI connection parameters for one endpoint
// (primary or candidate), extracted from a Kubernetes Secret.
type Credentials struct {
	Host      string
	Username  string
	Password  string
	SslVerify bool
}

// ExtractCredentials reads the host/username/password (+ optional
// ssl_verify) keys from the Secret referenced by source/secretRef. This is
// the same Secret shape used for a ProviderConfig's primary credentials,
// reused unchanged for a readEndpoint's credentialsRef.
func ExtractCredentials(ctx context.Context, kube k8sclient.Client, source xpv1.CredentialsSource, secretRef *xpv1.SecretKeySelector, fallbackNamespace string) (Credentials, error) {
	if source != xpv1.CredentialsSourceSecret {
		return Credentials{}, errors.New(errUnsupportedCreds)
	}
	if secretRef == nil {
		return Credentials{}, errors.New(errNoSecretRef)
	}

	ns := secretRef.Namespace
	if ns == "" {
		ns = fallbackNamespace
	}

	secret := &corev1.Secret{}
	if err := kube.Get(ctx, types.NamespacedName{Namespace: ns, Name: secretRef.Name}, secret); err != nil {
		return Credentials{}, errors.Wrap(err, errGetSecret)
	}

	host := string(secret.Data["host"])
	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	if host == "" || username == "" || password == "" {
		return Credentials{}, errors.New(errMissingCredKey)
	}

	sslVerify := true
	if v := string(secret.Data["ssl_verify"]); v == "false" {
		sslVerify = false
	}

	return Credentials{Host: host, Username: username, Password: password, SslVerify: sslVerify}, nil
}

// ObjectManagerFactory builds an authenticated ibclient.IBObjectManager
// from Credentials. Callers inject their own factory (each resource
// controller already owns one, parameterized by WAPI version and, in
// tests, by scheme/port overrides for httptest servers) so this package
// does not duplicate that per-controller SDK connector construction.
type ObjectManagerFactory func(creds Credentials) (ibclient.IBObjectManager, error)

// Connect builds a dual-endpoint Client from primary credentials plus an
// optional read endpoint. When readCreds is nil, the returned Client is a
// pure passthrough to the primary — identical to today's single-endpoint
// behavior. The read endpoint is entirely opt-in.
//
// If readCreds is non-nil and building its ObjectManager fails, Connect
// returns an error rather than falling back to a primary-only Client —
// misconfiguration must fail loudly, never silently degrade. Extracting
// readCreds itself (reading the readEndpoint Secret)
// is the caller's responsibility via ExtractCredentials, for the same
// reason: a missing/invalid Secret must fail Connect(), which callers
// achieve by returning early instead of calling this function with a nil
// readCreds.
func Connect(primaryCreds Credentials, readCreds *Credentials, newObjMgr ObjectManagerFactory, breaker *CircuitBreaker) (*Client, error) {
	primary, err := newObjMgr(primaryCreds)
	if err != nil {
		return nil, errors.Wrap(err, errNewPrimaryObjectManager)
	}

	if readCreds == nil {
		return New(primary), nil
	}

	candidate, err := newObjMgr(*readCreds)
	if err != nil {
		return nil, errors.Wrap(err, errNewCandidateObjectManager)
	}

	return WithCandidate(primary, candidate, breaker), nil
}
