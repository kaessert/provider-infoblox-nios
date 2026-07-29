package v1alpha1

import (
	xpv1 "github.com/crossplane/crossplane-runtime/v2/apis/common/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A ProviderConfigStatus defines the status of a Provider.
type ProviderConfigStatus struct {
	xpv1.ProviderConfigStatus `json:",inline"`
}

// ProviderCredentials required to authenticate.
type ProviderCredentials struct {
	// Source of the provider credentials.
	// +kubebuilder:validation:Enum=None;Secret;InjectedIdentity;Environment;Filesystem
	Source xpv1.CredentialsSource `json:"source"`

	xpv1.CommonCredentialSelectors `json:",inline"`
}

// A ProviderConfigSpec defines the desired state of a ProviderConfig.
type ProviderConfigSpec struct {
	// Credentials required to authenticate to this provider.
	Credentials ProviderCredentials `json:"credentials"`

	// ReadEndpoint configures an optional read-only NIOS endpoint (typically
	// a Grid Master Candidate) for offloading Observe traffic away from the
	// primary Grid Master. When omitted, all traffic (including reads) goes
	// to the primary endpoint — identical to the provider's behavior before
	// this field existed.
	// +optional
	ReadEndpoint *ReadEndpoint `json:"readEndpoint,omitempty"`

	// SSLVerify controls whether the provider verifies the NIOS Grid
	// Manager's TLS certificate for all endpoints (primary and read).
	// Set to false only when the Grid Manager presents a self-signed
	// certificate. Defaults to true (secure).
	// +kubebuilder:default=true
	// +optional
	SSLVerify *bool `json:"sslVerify,omitempty"`
}

// ReadEndpoint configures an optional read-only NIOS endpoint (typically a
// Grid Master Candidate) for offloading Observe traffic.
type ReadEndpoint struct {
	// CredentialsRef references a Secret with the same key format as the
	// primary credentials (host, username, password). Supports
	// least-privilege read-only NIOS accounts.
	CredentialsRef xpv1.SecretReference `json:"credentialsRef"`

	// Convergence configures how the controller detects replication
	// convergence between the primary and the read endpoint.
	// +optional
	Convergence *ConvergenceConfig `json:"convergence,omitempty"`
}

// ConvergenceConfig controls read-after-write convergence detection.
type ConvergenceConfig struct {
	// Mode selects the convergence strategy.
	// "soaSerial" (default for DNS): compare zone SOA serials between
	// endpoints.
	// "primaryOnly": always read from primary (default for IPAM, no serial
	// signal).
	// +kubebuilder:validation:Enum=soaSerial;primaryOnly
	// +kubebuilder:default=soaSerial
	Mode string `json:"mode,omitempty"`

	// PollInterval is how often to check the candidate's serial during
	// convergence.
	// +kubebuilder:default="2s"
	// +optional
	PollInterval *metav1.Duration `json:"pollInterval,omitempty"`

	// Timeout is how long to wait for convergence before falling back to
	// primary.
	// +kubebuilder:default="60s"
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="SECRET-NAME",type="string",JSONPath=".spec.credentials.secretRef.name",priority=1
// +kubebuilder:resource:scope=Cluster,categories={crossplane,provider,infobloxnios}
// A ProviderConfig configures an Infoblox NIOS provider. It is cluster-scoped
// and referenced by legacy cluster-scoped managed resources.
type ProviderConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ProviderConfigSpec   `json:"spec"`
	Status ProviderConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
// ProviderConfigList contains a list of ProviderConfig.
type ProviderConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProviderConfig `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="CONFIG-NAME",type="string",JSONPath=".providerConfigRef.name"
// +kubebuilder:printcolumn:name="RESOURCE-KIND",type="string",JSONPath=".resourceRef.kind"
// +kubebuilder:printcolumn:name="RESOURCE-NAME",type="string",JSONPath=".resourceRef.name"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,provider,infobloxnios}
// A ProviderConfigUsage indicates that a cluster-scoped resource is using a
// ProviderConfig.
type ProviderConfigUsage struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	xpv1.ProviderConfigUsage `json:",inline"`
}

// +kubebuilder:object:root=true
// ProviderConfigUsageList contains a list of ProviderConfigUsage.
type ProviderConfigUsageList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ProviderConfigUsage `json:"items"`
}
