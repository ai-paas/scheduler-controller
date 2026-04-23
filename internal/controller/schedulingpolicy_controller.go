/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	schedulerv1alpha1 "github.com/ai-paas/scheduler-controller/api/v1alpha1"
)

const (
	conditionTypeReady = "Ready"
	requeueInterval    = 5 * time.Minute

	managedByPolicyAnnotation = "ai-paas.org/scheduling-policy"
)

// SchedulingPolicyReconciler reconciles a SchedulingPolicy object
type SchedulingPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=ai-paas.org,resources=schedulingpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ai-paas.org,resources=schedulingpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai-paas.org,resources=schedulingpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;replicationcontrollers,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets;replicasets,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=batch,resources=jobs;cronjobs,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=serving.kserve.io,resources=inferenceservices,verbs=get;list;watch;patch;update

func (r *SchedulingPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("schedulingPolicy", req.Name)

	var policy schedulerv1alpha1.SchedulingPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	var allPolicies schedulerv1alpha1.SchedulingPolicyList
	if err := r.List(ctx, &allPolicies); err != nil {
		return r.updateStatusOnError(ctx, &policy, fmt.Errorf("list policies: %w", err))
	}

	matchedTotal := int32(0)
	patchedTotal := int32(0)
	for _, source := range policy.Spec.Sources {
		matched, patched, enforceErr := r.enforceSource(ctx, &policy, allPolicies.Items, source)
		matchedTotal += matched
		patchedTotal += patched
		if enforceErr != nil {
			return r.updateStatusOnError(ctx, &policy, enforceErr)
		}
	}

	policyCopy := policy.DeepCopy()
	now := metav1.Now()
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.MatchedResources = matchedTotal
	policy.Status.PatchedResources = patchedTotal
	policy.Status.LastAppliedTime = &now
	apiMeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionTrue,
		Reason:             "PolicyApplied",
		Message:            fmt.Sprintf("matched=%d patched=%d", matchedTotal, patchedTotal),
		ObservedGeneration: policy.Generation,
		LastTransitionTime: now,
	})
	if err := r.Status().Patch(ctx, &policy, client.MergeFrom(policyCopy)); err != nil {
		logger.Error(err, "unable to patch policy status")
		return ctrl.Result{}, err
	}

	logger.Info("policy reconciled", "matchedResources", matchedTotal, "patchedResources", patchedTotal)
	return ctrl.Result{RequeueAfter: requeueInterval}, nil
}

func (r *SchedulingPolicyReconciler) updateStatusOnError(
	ctx context.Context,
	policy *schedulerv1alpha1.SchedulingPolicy,
	reconcileErr error,
) (ctrl.Result, error) {
	policyCopy := policy.DeepCopy()
	now := metav1.Now()
	policy.Status.ObservedGeneration = policy.Generation
	apiMeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
		Type:               conditionTypeReady,
		Status:             metav1.ConditionFalse,
		Reason:             "ReconcileFailed",
		Message:            reconcileErr.Error(),
		ObservedGeneration: policy.Generation,
		LastTransitionTime: now,
	})
	_ = r.Status().Patch(ctx, policy, client.MergeFrom(policyCopy))
	return ctrl.Result{RequeueAfter: requeueInterval}, reconcileErr
}

func (r *SchedulingPolicyReconciler) enforceSource(
	ctx context.Context,
	policy *schedulerv1alpha1.SchedulingPolicy,
	allPolicies []schedulerv1alpha1.SchedulingPolicy,
	source schedulerv1alpha1.WorkloadSource,
) (int32, int32, error) {
	selector, err := selectorFromSource(source)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid source selector for %s: %w", normalizeTarget(source.APIVersion, source.Kind), err)
	}

	switch normalizeTarget(source.APIVersion, source.Kind) {
	case "v1/Pod":
		return r.enforcePods(ctx, policy, allPolicies, selector, source)
	case "v1/ReplicationController":
		return r.enforceReplicationControllers(ctx, policy, allPolicies, selector, source)
	case "apps/v1/Deployment":
		return r.enforceDeployments(ctx, policy, allPolicies, selector, source)
	case "apps/v1/ReplicaSet":
		return r.enforceReplicaSets(ctx, policy, allPolicies, selector, source)
	case "apps/v1/StatefulSet":
		return r.enforceStatefulSets(ctx, policy, allPolicies, selector, source)
	case "apps/v1/DaemonSet":
		return r.enforceDaemonSets(ctx, policy, allPolicies, selector, source)
	case "batch/v1/Job":
		return r.enforceJobs(ctx, policy, allPolicies, selector, source)
	case "batch/v1/CronJob":
		return r.enforceCronJobs(ctx, policy, allPolicies, selector, source)
	case "serving.kserve.io/v1beta1/InferenceService":
		return r.enforceInferenceServices(ctx, policy, allPolicies, selector, source)
	default:
		return 0, 0, fmt.Errorf("unsupported source resource %s", normalizeTarget(source.APIVersion, source.Kind))
	}
}

func (r *SchedulingPolicyReconciler) enforcePods(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listPods(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchPodScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceReplicationControllers(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listReplicationControllers(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchReplicationControllerScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceDeployments(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listDeployments(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchDeploymentScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceReplicaSets(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listReplicaSets(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchReplicaSetScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceStatefulSets(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listStatefulSets(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchStatefulSetScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceDaemonSets(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listDaemonSets(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchDaemonSetScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceJobs(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listJobs(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchJobScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceCronJobs(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listCronJobs(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchCronJobScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) enforceInferenceServices(ctx context.Context, policy *schedulerv1alpha1.SchedulingPolicy, allPolicies []schedulerv1alpha1.SchedulingPolicy, selector labels.Selector, source schedulerv1alpha1.WorkloadSource) (int32, int32, error) {
	items, err := r.listInferenceServices(ctx, source.Namespaces, selector)
	if err != nil {
		return 0, 0, err
	}
	var matched, patched int32
	for i := range items {
		matched++
		if !r.isWinningPolicy(&items[i], source, allPolicies, policy.Name) {
			continue
		}
		changed, patchErr := r.patchInferenceServiceScheduling(ctx, &items[i], policy)
		if patchErr != nil {
			return matched, patched, patchErr
		}
		if changed {
			patched++
		}
	}
	return matched, patched, nil
}

func (r *SchedulingPolicyReconciler) listDeployments(ctx context.Context, namespaces []string, selector labels.Selector) ([]appsv1.Deployment, error) {
	if len(namespaces) == 0 {
		var list appsv1.DeploymentList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]appsv1.Deployment, 0)
	for _, namespace := range namespaces {
		var list appsv1.DeploymentList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listPods(ctx context.Context, namespaces []string, selector labels.Selector) ([]corev1.Pod, error) {
	if len(namespaces) == 0 {
		var list corev1.PodList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]corev1.Pod, 0)
	for _, namespace := range namespaces {
		var list corev1.PodList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listReplicationControllers(ctx context.Context, namespaces []string, selector labels.Selector) ([]corev1.ReplicationController, error) {
	if len(namespaces) == 0 {
		var list corev1.ReplicationControllerList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]corev1.ReplicationController, 0)
	for _, namespace := range namespaces {
		var list corev1.ReplicationControllerList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listReplicaSets(ctx context.Context, namespaces []string, selector labels.Selector) ([]appsv1.ReplicaSet, error) {
	if len(namespaces) == 0 {
		var list appsv1.ReplicaSetList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]appsv1.ReplicaSet, 0)
	for _, namespace := range namespaces {
		var list appsv1.ReplicaSetList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listStatefulSets(ctx context.Context, namespaces []string, selector labels.Selector) ([]appsv1.StatefulSet, error) {
	if len(namespaces) == 0 {
		var list appsv1.StatefulSetList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]appsv1.StatefulSet, 0)
	for _, namespace := range namespaces {
		var list appsv1.StatefulSetList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listDaemonSets(ctx context.Context, namespaces []string, selector labels.Selector) ([]appsv1.DaemonSet, error) {
	if len(namespaces) == 0 {
		var list appsv1.DaemonSetList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]appsv1.DaemonSet, 0)
	for _, namespace := range namespaces {
		var list appsv1.DaemonSetList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listJobs(ctx context.Context, namespaces []string, selector labels.Selector) ([]batchv1.Job, error) {
	if len(namespaces) == 0 {
		var list batchv1.JobList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]batchv1.Job, 0)
	for _, namespace := range namespaces {
		var list batchv1.JobList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listCronJobs(ctx context.Context, namespaces []string, selector labels.Selector) ([]batchv1.CronJob, error) {
	if len(namespaces) == 0 {
		var list batchv1.CronJobList
		if err := r.List(ctx, &list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]batchv1.CronJob, 0)
	for _, namespace := range namespaces {
		var list batchv1.CronJobList
		if err := r.List(ctx, &list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) listInferenceServices(ctx context.Context, namespaces []string, selector labels.Selector) ([]unstructured.Unstructured, error) {
	newList := func() *unstructured.UnstructuredList {
		list := &unstructured.UnstructuredList{}
		list.SetAPIVersion("serving.kserve.io/v1beta1")
		list.SetKind("InferenceServiceList")
		return list
	}
	if len(namespaces) == 0 {
		list := newList()
		if err := r.List(ctx, list, &client.ListOptions{LabelSelector: selector}); err != nil {
			return nil, err
		}
		return list.Items, nil
	}
	items := make([]unstructured.Unstructured, 0)
	for _, namespace := range namespaces {
		list := newList()
		if err := r.List(ctx, list, &client.ListOptions{Namespace: namespace, LabelSelector: selector}); err != nil {
			return nil, err
		}
		items = append(items, list.Items...)
	}
	return items, nil
}

func (r *SchedulingPolicyReconciler) patchPodScheduling(ctx context.Context, obj *corev1.Pod, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToPod(obj, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchReplicationControllerScheduling(ctx context.Context, obj *corev1.ReplicationController, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	if obj.Spec.Template == nil {
		return false, nil
	}
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, obj.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchDeploymentScheduling(ctx context.Context, obj *appsv1.Deployment, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, &obj.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchReplicaSetScheduling(ctx context.Context, obj *appsv1.ReplicaSet, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, &obj.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchStatefulSetScheduling(ctx context.Context, obj *appsv1.StatefulSet, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, &obj.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchDaemonSetScheduling(ctx context.Context, obj *appsv1.DaemonSet, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, &obj.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchJobScheduling(ctx context.Context, obj *batchv1.Job, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, &obj.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchCronJobScheduling(ctx context.Context, obj *batchv1.CronJob, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	if !applyTargetToWorkloadTemplate(obj, &obj.Spec.JobTemplate.Spec.Template, policy) {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) patchInferenceServiceScheduling(ctx context.Context, obj *unstructured.Unstructured, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	base := obj.DeepCopy()
	changed := applyManagingPolicyAnnotationToObject(obj, policy.Name)
	for _, component := range []string{"predictor", "transformer", "explainer"} {
		componentChanged, err := applyTargetToInferenceServiceComponent(obj, component, policy)
		if err != nil {
			return false, err
		}
		changed = changed || componentChanged
	}
	if !changed {
		return false, nil
	}
	return true, r.Patch(ctx, obj, client.MergeFrom(base))
}

func (r *SchedulingPolicyReconciler) isWinningPolicy(obj client.Object, source schedulerv1alpha1.WorkloadSource, allPolicies []schedulerv1alpha1.SchedulingPolicy, currentName string) bool {
	winner := r.selectWinningPolicy(obj, source, allPolicies)
	return winner != nil && winner.Name == currentName
}

func (r *SchedulingPolicyReconciler) selectWinningPolicy(obj client.Object, source schedulerv1alpha1.WorkloadSource, allPolicies []schedulerv1alpha1.SchedulingPolicy) *schedulerv1alpha1.SchedulingPolicy {
	matched := make([]*schedulerv1alpha1.SchedulingPolicy, 0)
	wantedSource := normalizeTarget(source.APIVersion, source.Kind)
	for i := range allPolicies {
		policy := &allPolicies[i]
		if !policyMatchesObject(policy, wantedSource, obj) {
			continue
		}
		matched = append(matched, policy)
	}
	if len(matched) == 0 {
		return nil
	}
	sort.SliceStable(matched, func(i, j int) bool {
		if matched[i].Spec.Priority != matched[j].Spec.Priority {
			return matched[i].Spec.Priority > matched[j].Spec.Priority
		}
		if !matched[i].CreationTimestamp.Equal(&matched[j].CreationTimestamp) {
			return matched[i].CreationTimestamp.Before(&matched[j].CreationTimestamp)
		}
		return matched[i].Name < matched[j].Name
	})
	return matched[0]
}

func policyMatchesObject(policy *schedulerv1alpha1.SchedulingPolicy, wantedSource string, obj client.Object) bool {
	for _, source := range policy.Spec.Sources {
		if !sourceMatchesObject(source, wantedSource, obj) {
			continue
		}
		return true
	}
	return false
}

func sourceMatchesObject(source schedulerv1alpha1.WorkloadSource, wantedSource string, obj client.Object) bool {
	if normalizeTarget(source.APIVersion, source.Kind) != wantedSource {
		return false
	}
	if len(source.Namespaces) > 0 {
		matchedNamespace := false
		for _, namespace := range source.Namespaces {
			if namespace == obj.GetNamespace() {
				matchedNamespace = true
				break
			}
		}
		if !matchedNamespace {
			return false
		}
	}
	selector, err := selectorFromSource(source)
	if err != nil {
		return false
	}
	return selector.Matches(labels.Set(obj.GetLabels()))
}

func selectorFromSource(source schedulerv1alpha1.WorkloadSource) (labels.Selector, error) {
	if source.Selector == nil {
		return labels.Everything(), nil
	}
	return metav1.LabelSelectorAsSelector(source.Selector)
}

func applyTargetToPod(obj *corev1.Pod, policy *schedulerv1alpha1.SchedulingPolicy) bool {
	changed := applyTargetToPodSpec(&obj.Spec, policy.Spec.Target)
	changed = applyTargetToLabels(&obj.Labels, policy.Spec.Target) || changed
	return applyManagingPolicyAnnotationToObject(obj, policy.Name) || changed
}

func applyTargetToWorkloadTemplate(workload client.Object, template *corev1.PodTemplateSpec, policy *schedulerv1alpha1.SchedulingPolicy) bool {
	changed := applyTargetToPodTemplate(template, policy)
	return applyManagingPolicyAnnotationToObject(workload, policy.Name) || changed
}

func applyTargetToPodTemplate(template *corev1.PodTemplateSpec, policy *schedulerv1alpha1.SchedulingPolicy) bool {
	changed := applyTargetToPodSpec(&template.Spec, policy.Spec.Target)
	changed = applyTargetToLabels(&template.Labels, policy.Spec.Target) || changed
	return applyManagingPolicyAnnotationToMeta(&template.ObjectMeta, policy.Name) || changed
}

func applyTargetToPodSpec(spec *corev1.PodSpec, target schedulerv1alpha1.SchedulingTarget) bool {
	changed := false
	if shouldSetValue(spec.SchedulerName, target.SchedulerName) {
		spec.SchedulerName = target.SchedulerName
		changed = true
	}
	if shouldSetValue(spec.PriorityClassName, target.PriorityClassName) {
		spec.PriorityClassName = target.PriorityClassName
		changed = true
	}
	return changed
}

func applyTargetToLabels(labelSet *map[string]string, target schedulerv1alpha1.SchedulingTarget) bool {
	if len(target.Labels) == 0 {
		return false
	}
	currentLabels := *labelSet
	if currentLabels == nil {
		currentLabels = make(map[string]string)
	}
	changed := false
	for key, value := range target.Labels {
		if !shouldSetValue(currentLabels[key], value) {
			continue
		}
		currentLabels[key] = value
		changed = true
	}
	if !changed {
		return false
	}
	*labelSet = currentLabels
	return true
}

func applyManagingPolicyAnnotationToObject(obj client.Object, policyName string) bool {
	annotations := obj.GetAnnotations()
	changed, annotations := applyManagingPolicyAnnotation(annotations, policyName)
	if changed {
		obj.SetAnnotations(annotations)
	}
	return changed
}

func applyManagingPolicyAnnotationToMeta(meta *metav1.ObjectMeta, policyName string) bool {
	changed, annotations := applyManagingPolicyAnnotation(meta.Annotations, policyName)
	meta.Annotations = annotations
	return changed
}

func applyManagingPolicyAnnotation(annotations map[string]string, policyName string) (bool, map[string]string) {
	if annotations == nil {
		annotations = make(map[string]string)
	}
	if annotations[managedByPolicyAnnotation] == policyName {
		return false, annotations
	}
	annotations[managedByPolicyAnnotation] = policyName
	return true, annotations
}

func applyTargetToInferenceServiceComponent(obj *unstructured.Unstructured, component string, policy *schedulerv1alpha1.SchedulingPolicy) (bool, error) {
	componentMap, found, err := unstructured.NestedMap(obj.Object, "spec", component)
	if err != nil {
		return false, err
	}
	if !found {
		if component != "predictor" {
			return false, nil
		}
		componentMap = map[string]interface{}{}
	}

	changed := false
	if shouldSetValue(asString(componentMap["schedulerName"]), policy.Spec.Target.SchedulerName) {
		componentMap["schedulerName"] = policy.Spec.Target.SchedulerName
		changed = true
	}
	if shouldSetValue(asString(componentMap["priorityClassName"]), policy.Spec.Target.PriorityClassName) {
		componentMap["priorityClassName"] = policy.Spec.Target.PriorityClassName
		changed = true
	}
	if len(policy.Spec.Target.Labels) > 0 {
		labelMap, _ := componentMap["labels"].(map[string]interface{})
		if labelMap == nil {
			labelMap = map[string]interface{}{}
		}
		for key, value := range policy.Spec.Target.Labels {
			if !shouldSetValue(asString(labelMap[key]), value) {
				continue
			}
			labelMap[key] = value
			changed = true
		}
		componentMap["labels"] = labelMap
	}
	annotationsMap, _ := componentMap["annotations"].(map[string]interface{})
	if annotationsMap == nil {
		annotationsMap = map[string]interface{}{}
	}
	if shouldSetValue(asString(annotationsMap[managedByPolicyAnnotation]), policy.Name) {
		annotationsMap[managedByPolicyAnnotation] = policy.Name
		componentMap["annotations"] = annotationsMap
		changed = true
	}
	if !changed {
		return false, nil
	}
	if err := unstructured.SetNestedMap(obj.Object, componentMap, "spec", component); err != nil {
		return false, err
	}
	return true, nil
}

func asString(value interface{}) string {
	stringValue, ok := value.(string)
	if !ok {
		return ""
	}
	return stringValue
}

func shouldSetValue(current, desired string) bool {
	if desired == "" || current == desired {
		return false
	}
	return true
}

func normalizeTarget(apiVersion, kind string) string {
	return fmt.Sprintf("%s/%s", strings.TrimSpace(apiVersion), strings.TrimSpace(kind))
}

// SetupWithManager sets up the controller with the Manager.
func (r *SchedulingPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	inferenceService := &unstructured.Unstructured{}
	inferenceService.SetAPIVersion("serving.kserve.io/v1beta1")
	inferenceService.SetKind("InferenceService")

	return ctrl.NewControllerManagedBy(mgr).
		For(&schedulerv1alpha1.SchedulingPolicy{}).
		Watches(&schedulerv1alpha1.SchedulingPolicy{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&corev1.ReplicationController{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&appsv1.ReplicaSet{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&appsv1.DaemonSet{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&batchv1.Job{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(&batchv1.CronJob{}, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Watches(inferenceService, handler.EnqueueRequestsFromMapFunc(r.enqueueAllPolicies)).
		Named("schedulingpolicy").
		Complete(r)
}

func (r *SchedulingPolicyReconciler) enqueueAllPolicies(ctx context.Context, _ client.Object) []reconcile.Request {
	var policies schedulerv1alpha1.SchedulingPolicyList
	if err := r.List(ctx, &policies); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(policies.Items))
	for _, policy := range policies.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: policy.Name}})
	}
	return requests
}
