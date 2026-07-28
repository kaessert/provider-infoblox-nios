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
	"github.com/crossplane/crossplane-runtime/v2/pkg/controller"
	"github.com/crossplane/crossplane-runtime/v2/pkg/event"
	"github.com/crossplane/crossplane-runtime/v2/pkg/ratelimiter"
	"github.com/crossplane/crossplane-runtime/v2/pkg/reconciler/providerconfig"
	"github.com/crossplane/crossplane-runtime/v2/pkg/resource"
	ctrl "sigs.k8s.io/controller-runtime"

	clusterv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/cluster/v1alpha1"
	namespacedv1alpha1 "github.com/crossplane-contrib/provider-infoblox-nios/apis/namespaced/v1alpha1"
)

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
		providerconfig.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		WithOptions(o.ForControllerRuntime()).
		For(&namespacedv1alpha1.ClusterProviderConfig{}).
		Watches(&namespacedv1alpha1.ProviderConfigUsage{}, &resource.EnqueueRequestForProviderConfig{}).
		Complete(ratelimiter.NewReconciler(name, r, o.GlobalRateLimiter))
}
