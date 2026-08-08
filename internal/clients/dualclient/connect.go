// connect.go builds authenticated dual-endpoint Clients from ProviderConfig
// credentials. It centralizes the WAPI credentials Secret contract
// (username/password keys only — the Grid Manager host is a
// ProviderConfig-level connection parameter, not a Secret key) and
// enforces fail-loudly behavior when a readEndpoint is configured but its
// credentials are missing or invalid — misconfiguration must be visible,
// never silently downgraded to primary-only routing.
package dualclient

import (
	"context"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"
)

// Error constants — all errors use the crossplane-runtime errors package.
const (
	errEmptyHost                 = "host must not be empty: this indicates a caller bug or a ProviderConfig that predates the host field, not a malformed credentials Secret"
	errUnsupportedCreds          = "unsupported credentials source: only Secret is supported"
	errNoSecretRef               = "credentials secretRef is required for the Infoblox NIOS WAPI client"
	errGetSecret                 = "cannot get credentials secret"
	errMissingCredKey            = "credentials secret is missing one of the required username/password keys"
	errNewPrimaryObjectManager   = "cannot create primary Infoblox NIOS WAPI object manager"
	errNewCandidateObjectManager = "cannot create candidate (readEndpoint) Infoblox NIOS WAPI object manager: misconfigured readEndpoint must fail loudly, not silently degrade to primary-only"
)

// Credentials holds the WAPI connection parameters for one endpoint
// (primary or candidate). Host is a ProviderConfig-level connection
// parameter supplied directly by the caller (see ExtractCredentials);
// Username/Password are extracted from a Kubernetes Secret. TLS
// verification is governed by the owning ProviderConfig's sslVerify spec
// field (passed separately to Connect), not by anything in this struct or
// its source Secret.
type Credentials struct {
	Host     string
	Username string
	Password string
}

// ExtractCredentials reads the username/password keys from the Secret
// referenced by source/secretRef and combines them with the caller-supplied
// host into a fully-populated Credentials. This is the same Secret shape
// used for a ProviderConfig's primary credentials, reused unchanged for a
// readEndpoint's credentialsRef. host is a ProviderConfig-level connection
// parameter (e.g. pc.Spec.Host or pc.Spec.ReadEndpoint.Host) — it is never
// read from the Secret, and an empty host is rejected rather than returned
// for the caller to fill in afterward, so a Credentials value is either
// fully populated or an error. TLS verification is also not read from this
// Secret — it is a ProviderConfig-level policy field (see Connect).
func ExtractCredentials(ctx context.Context, kube k8sclient.Client, host string, source xpv2.CredentialsSource, secretRef *xpv2.SecretKeySelector, fallbackNamespace string) (Credentials, error) {
	if host == "" {
		return Credentials{}, errors.New(errEmptyHost)
	}
	if source != xpv2.CredentialsSourceSecret {
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

	username := string(secret.Data["username"])
	password := string(secret.Data["password"])
	if username == "" || password == "" {
		return Credentials{}, errors.New(errMissingCredKey)
	}

	return Credentials{Host: host, Username: username, Password: password}, nil
}

// ObjectManagerFactory builds an authenticated ibclient.IBObjectManager
// from Credentials and the resolved sslVerify policy. Callers inject
// their own factory (each resource controller already owns one,
// parameterized by WAPI version and, in tests, by scheme/port overrides
// for httptest servers) so this package does not duplicate that
// per-controller SDK connector construction.
type ObjectManagerFactory func(creds Credentials, sslVerify bool) (ibclient.IBObjectManager, error)

// Connect builds a dual-endpoint Client from primary credentials plus an
// optional read endpoint. sslVerify is resolved by the caller from the
// owning ProviderConfig's sslVerify spec field and applies to both the
// primary and candidate endpoints — a single ProviderConfig governs both,
// this is not a per-credential setting. When readCreds is nil, the
// returned Client is a pure passthrough to the primary — identical to
// today's single-endpoint behavior. The read endpoint is entirely opt-in.
//
// If readCreds is non-nil and building its ObjectManager fails, Connect
// returns an error rather than falling back to a primary-only Client —
// misconfiguration must fail loudly, never silently degrade. Extracting
// readCreds itself (reading the readEndpoint Secret)
// is the caller's responsibility via ExtractCredentials, for the same
// reason: a missing/invalid Secret must fail Connect(), which callers
// achieve by returning early instead of calling this function with a nil
// readCreds.
func Connect(primaryCreds Credentials, readCreds *Credentials, sslVerify bool, newObjMgr ObjectManagerFactory, breaker *CircuitBreaker) (*Client, error) {
	primary, err := newObjMgr(primaryCreds, sslVerify)
	if err != nil {
		return nil, errors.Wrap(err, errNewPrimaryObjectManager)
	}

	if readCreds == nil {
		return New(primary), nil
	}

	candidate, err := newObjMgr(*readCreds, sslVerify)
	if err != nil {
		return nil, errors.Wrap(err, errNewCandidateObjectManager)
	}

	return WithCandidate(primary, candidate, breaker), nil
}
