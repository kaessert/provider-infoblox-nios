/*
Copyright 2020 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"context"

	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/providerconfig"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	xpv2 "github.com/crossplane/crossplane/apis/v2/core/v2"
	ibclient "github.com/infobloxopen/infoblox-go-client/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	k8sclient "sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
	"github.com/crossplane-contrib/provider-infoblox-nios/internal/clients/dualclient"
)

// wapiVersion is the Infoblox NIOS WAPI version this provider targets
// (https://<host>/wapi/2.9.7/), pinned identically for every resource
// controller's connector.
const wapiVersion = "2.9.7"

// errNewConnector is returned when the underlying WAPI SDK rejects the
// resolved host/credentials/transport configuration.
const errNewConnector = "cannot create Infoblox NIOS WAPI connector"

// Setup adds controllers that reconcile ProviderConfigs by accounting for
// their current usage. There are three ProviderConfig controllers: the
// legacy cluster-scoped ProviderConfig (plain group, used by LegacyManaged
// MRs), and the namespaced ProviderConfig plus ClusterProviderConfig (.m.
// group, used by ModernManaged MRs — both tracked by the same namespaced
// ProviderConfigUsage).
func Setup(mgr ctrl.Manager, o controller.Options) error {
	if err := setupClusterProviderConfig(mgr, o); err != nil {
		return err
	}
	if err := setupNamespacedProviderConfig(mgr, o); err != nil {
		return err
	}
	return setupNamespacedClusterProviderConfig(mgr, o)
}

// setupClusterProviderConfig reconciles the legacy cluster-scoped
// ProviderConfig (plain group), tracked by the legacy cluster
// ProviderConfigUsage.
func setupClusterProviderConfig(mgr ctrl.Manager, o controller.Options) error {
	name := providerconfig.ControllerName(clusterv1alpha1.ProviderConfigGroupKind)

	of := resource.ProviderConfigKinds{
		Config:    clusterv1alpha1.ProviderConfigGroupVersionKind,
		Usage:     clusterv1alpha1.ProviderConfigUsageGroupVersionKind,
		UsageList: clusterv1alpha1.ProviderConfigUsageListGroupVersionKind,
	}

	r := providerconfig.NewReconciler(mgr, of,
		providerconfig.WithLogger(o.Logger.WithValues("controller", name)),
		//nolint:staticcheck // event.NewAPIRecorder still requires the deprecated record.EventRecorder type; no replacement exists yet in this crossplane-runtime version.
		providerconfig.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(&clusterv1alpha1.ProviderConfig{}).
		Watches(&clusterv1alpha1.ProviderConfigUsage{}, &resource.EnqueueRequestForProviderConfig{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// setupNamespacedProviderConfig reconciles the namespaced ProviderConfig
// (.m. group), tracked by the shared namespaced ProviderConfigUsage.
func setupNamespacedProviderConfig(mgr ctrl.Manager, o controller.Options) error {
	name := providerconfig.ControllerName(namespacedv1alpha1.ProviderConfigGroupKind)

	of := resource.ProviderConfigKinds{
		Config:    namespacedv1alpha1.ProviderConfigGroupVersionKind,
		Usage:     namespacedv1alpha1.ProviderConfigUsageGroupVersionKind,
		UsageList: namespacedv1alpha1.ProviderConfigUsageListGroupVersionKind,
	}

	r := providerconfig.NewReconciler(mgr, of,
		providerconfig.WithLogger(o.Logger.WithValues("controller", name)),
		//nolint:staticcheck // event.NewAPIRecorder still requires the deprecated record.EventRecorder type; no replacement exists yet in this crossplane-runtime version.
		providerconfig.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(&namespacedv1alpha1.ProviderConfig{}).
		Watches(&namespacedv1alpha1.ProviderConfigUsage{}, &resource.EnqueueRequestForProviderConfig{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// setupNamespacedClusterProviderConfig reconciles the cluster-scoped
// ClusterProviderConfig (.m. group), tracked by the SAME shared namespaced
// ProviderConfigUsage as the namespaced ProviderConfig.
func setupNamespacedClusterProviderConfig(mgr ctrl.Manager, o controller.Options) error {
	name := providerconfig.ControllerName(namespacedv1alpha1.ClusterProviderConfigGroupKind)

	of := resource.ProviderConfigKinds{
		Config:    namespacedv1alpha1.ClusterProviderConfigGroupVersionKind,
		Usage:     namespacedv1alpha1.ProviderConfigUsageGroupVersionKind,
		UsageList: namespacedv1alpha1.ProviderConfigUsageListGroupVersionKind,
	}

	r := providerconfig.NewReconciler(mgr, of,
		providerconfig.WithLogger(o.Logger.WithValues("controller", name)),
		//nolint:staticcheck // event.NewAPIRecorder still requires the deprecated record.EventRecorder type; no replacement exists yet in this crossplane-runtime version.
		providerconfig.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(&namespacedv1alpha1.ClusterProviderConfig{}).
		Watches(&namespacedv1alpha1.ProviderConfigUsage{}, &resource.EnqueueRequestForProviderConfig{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}

// ── Credential bridge ───────────────────────────────────────────────────
//
// Conn, GetLegacy, Get, GetCluster and BuildConnector are the single place
// a resource controller resolves its ProviderConfig into an authenticated
// WAPI connection. Host resolution (pc.Spec.Host) and the SSLVerify
// nil-default both live here, and nowhere in internal/controller/<pkg>.

// Conn is everything a controller needs to talk to a Grid. It is a
// struct, not a bare connector, so that adding a field later (read-
// endpoint routing, a convergence gate) does not change a single
// call-site signature across every controller package.
type Conn struct {
	// Connector is the authenticated primary WAPI connector.
	Connector *ibclient.Connector
	// Endpoint is the resolved primary Grid host.
	Endpoint string
}

// GetLegacy resolves an authenticated Conn for a legacy cluster-scoped
// ProviderConfig, used by LegacyManaged resources (cluster-scoped MRs,
// whether they embed xpv1.ResourceSpec or xpv2.ClusterManagedResourceSpec
// — both resolve identically here). The credentials Secret has no
// fallback namespace: a cluster-scoped ProviderConfig has none of its
// own, so secretRef.namespace must be set explicitly.
func GetLegacy(ctx context.Context, kube k8sclient.Client, pc *clusterv1alpha1.ProviderConfig) (*Conn, error) {
	return resolve(ctx, kube, pc.Spec.Host, pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, "", pc.Spec.SSLVerify)
}

// Get resolves an authenticated Conn for a namespaced ProviderConfig,
// used by ModernManaged resources in the same namespace. A secretRef
// with no namespace falls back to the ProviderConfig's own namespace.
func Get(ctx context.Context, kube k8sclient.Client, pc *namespacedv1alpha1.ProviderConfig) (*Conn, error) {
	return resolve(ctx, kube, pc.Spec.Host, pc.Spec.Credentials.Source, pc.Spec.Credentials.SecretRef, pc.GetNamespace(), pc.Spec.SSLVerify)
}

// GetCluster resolves an authenticated Conn for a namespaced
// ClusterProviderConfig, used by ModernManaged resources from any
// namespace. The credentials Secret has no fallback namespace: a
// cluster-scoped config has none of its own, so secretRef.namespace must
// be set explicitly.
func GetCluster(ctx context.Context, kube k8sclient.Client, cpc *namespacedv1alpha1.ClusterProviderConfig) (*Conn, error) {
	return resolve(ctx, kube, cpc.Spec.Host, cpc.Spec.Credentials.Source, cpc.Spec.Credentials.SecretRef, "", cpc.Spec.SSLVerify)
}

// resolve extracts credentials, applies the SSLVerify nil-default exactly
// once, and builds the authenticated Conn against the real Grid endpoint
// (scheme "https", port "443").
func resolve(ctx context.Context, kube k8sclient.Client, host string, source xpv2.CredentialsSource, secretRef *xpv2.SecretKeySelector, fallbackNamespace string, sslVerifyField *bool) (*Conn, error) {
	creds, err := dualclient.ExtractCredentials(ctx, kube, host, source, secretRef, fallbackNamespace)
	if err != nil {
		return nil, err
	}

	// SSLVerify governs TLS verification for all endpoints (primary and
	// read); it is a ProviderConfig policy field, not a per-credential
	// Secret key. Defaults to true (secure) when unset — the kubebuilder
	// default handles the YAML path, but Go code must handle the
	// nil-pointer case too (e.g. objects created before this field
	// existed).
	sslVerify := true
	if sslVerifyField != nil {
		sslVerify = *sslVerifyField
	}

	conn, err := BuildConnector(creds, sslVerify, "https", "443")
	if err != nil {
		return nil, err
	}

	return &Conn{Connector: conn, Endpoint: creds.Host}, nil
}

// BuildConnector constructs an authenticated *ibclient.Connector from the
// given credentials and sslVerify policy. It is the test seam every
// controller package's unit tests use in place of a private per-package
// constructor — pass scheme "http" and the httptest.Server's port to
// point the SDK at a plain-HTTP mock instead of a real HTTPS Grid
// Manager.
func BuildConnector(creds dualclient.Credentials, sslVerify bool, scheme, port string) (*ibclient.Connector, error) {
	hostConfig := ibclient.HostConfig{
		Scheme:  scheme,
		Host:    creds.Host,
		Version: wapiVersion,
		Port:    port,
	}
	authConfig := ibclient.AuthConfig{
		Username: creds.Username,
		Password: creds.Password,
	}
	// sslVerify is resolved by the caller from the ProviderConfig's
	// sslVerify spec field (default: true when nil). Set to false only
	// when the Grid Manager uses a self-signed certificate whose SAN
	// does not match the reachable host address.
	sslVerifyStr := "true"
	if !sslVerify {
		sslVerifyStr = "false"
	}
	transportConfig := ibclient.NewTransportConfig(sslVerifyStr, 60, 10)

	conn, err := ibclient.NewConnector(
		hostConfig,
		authConfig,
		transportConfig,
		&ibclient.WapiRequestBuilder{},
		&ibclient.WapiHttpRequestor{},
	)
	if err != nil {
		return nil, errors.Wrap(err, errNewConnector)
	}

	return conn, nil
}
