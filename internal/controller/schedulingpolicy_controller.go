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
	"sync"

	kservev1beta1 "github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	schedulerv1alpha1 "github.com/ai-paas/scheduler-controller/api/v1alpha1"
)

const (
	conditionTypeReady = "Ready"

	managedByPolicyAnnotation = "ai-paas.org/scheduling-policy"
	workloadRequestSeparator  = "|"
)

// SchedulingPolicyReconciler reconciles a SchedulingPolicy object
type SchedulingPolicyReconciler struct {
	client.Client
	policies *schedulingPolicyCache
}

type workloadReconciler struct {
	client.Client
	policies *schedulingPolicyCache
}

type workloadTarget string

const (
	targetReplicationController workloadTarget = "v1/ReplicationController"
	targetDeployment            workloadTarget = "apps/v1/Deployment"
	targetStatefulSet           workloadTarget = "apps/v1/StatefulSet"
	targetDaemonSet             workloadTarget = "apps/v1/DaemonSet"
	targetCronJob               workloadTarget = "batch/v1/CronJob"
	targetInferenceService      workloadTarget = "serving.kserve.io/v1beta1/InferenceService"
)

type schedulingPolicyCache struct {
	mu       sync.RWMutex
	policies []schedulerv1alpha1.SchedulingPolicy
}

// +kubebuilder:rbac:groups=ai-paas.org,resources=schedulingpolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=ai-paas.org,resources=schedulingpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ai-paas.org,resources=schedulingpolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=replicationcontrollers,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets;daemonsets,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=serving.kserve.io,resources=inferenceservices,verbs=get;list;watch;patch;update

func (r *SchedulingPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("schedulingPolicy", req.Name)
	if r.policies == nil {
		r.policies = newSchedulingPolicyCache()
	}

	var policy schedulerv1alpha1.SchedulingPolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.policies.Delete(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	now := metav1.Now()
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.LastAppliedTime = &now
	unsupportedSources := make([]string, 0)
	for _, source := range policy.Spec.Sources {
		target := sourceWorkloadTarget(source)
		switch target {
		case targetReplicationController, targetDeployment, targetStatefulSet, targetDaemonSet, targetCronJob, targetInferenceService:
		default:
			unsupportedSources = append(unsupportedSources, string(target))
		}
	}
	sort.Strings(unsupportedSources)
	if len(unsupportedSources) > 0 {
		r.policies.Delete(req.Name)
		apiMeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "UnsupportedSource",
			Message:            fmt.Sprintf("unsupported scheduling policy sources: %v", unsupportedSources),
			ObservedGeneration: policy.Generation,
			LastTransitionTime: now,
		})
	} else {
		r.policies.Upsert(policy)
		apiMeta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             "PolicyObserved",
			Message:            "policy observed; existing workloads are not changed by policy create or update",
			ObservedGeneration: policy.Generation,
			LastTransitionTime: now,
		})
	}
	if err := r.Status().Update(ctx, &policy); err != nil {
		logger.Error(err, "unable to patch policy status")
		return ctrl.Result{}, nil
	}

	logger.Info("policy observed")
	return ctrl.Result{}, nil
}

func (r *workloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	targetText, workloadName, ok := strings.Cut(req.Name, workloadRequestSeparator)
	if !ok {
		log.FromContext(ctx).Error(fmt.Errorf("malformed workload request %q", req.Name), "unable to resolve workload target")
		return ctrl.Result{}, nil
	}

	target := workloadTarget(targetText)
	workloadKey := types.NamespacedName{Namespace: req.Namespace, Name: workloadName}
	logger := log.FromContext(ctx).WithValues("workload", workloadKey, "target", target)

	var obj client.Object
	switch target {
	case targetReplicationController:
		obj = &corev1.ReplicationController{}
	case targetDeployment:
		obj = &appsv1.Deployment{}
	case targetStatefulSet:
		obj = &appsv1.StatefulSet{}
	case targetDaemonSet:
		obj = &appsv1.DaemonSet{}
	case targetCronJob:
		obj = &batchv1.CronJob{}
	case targetInferenceService:
		obj = &kservev1beta1.InferenceService{}
	default:
		logger.Error(fmt.Errorf("unsupported workload target %q", target), "unable to create workload object")
		return ctrl.Result{}, nil
	}

	if err := r.Get(ctx, workloadKey, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	policies := r.policies.List()

	var winner *schedulerv1alpha1.SchedulingPolicy
	for i := range policies {
		matched := false
		for _, source := range policies[i].Spec.Sources {
			if sourceWorkloadTarget(source) != target {
				continue
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
					continue
				}
			}
			if source.Selector != nil {
				selector, err := metav1.LabelSelectorAsSelector(source.Selector)
				if err != nil || !selector.Matches(labels.Set(obj.GetLabels())) {
					continue
				}
			}
			matched = true
			break
		}
		if !matched {
			continue
		}
		if winner == nil {
			winner = &policies[i]
			continue
		}
		if policies[i].Spec.Priority != winner.Spec.Priority {
			if policies[i].Spec.Priority > winner.Spec.Priority {
				winner = &policies[i]
			}
			continue
		}
		if !policies[i].CreationTimestamp.Equal(&winner.CreationTimestamp) {
			if policies[i].CreationTimestamp.Before(&winner.CreationTimestamp) {
				winner = &policies[i]
			}
			continue
		}
		if policies[i].Name < winner.Name {
			winner = &policies[i]
		}
	}
	if winner == nil {
		return ctrl.Result{}, nil
	}

	var changed bool
	var err error
	persist := true
	fieldsList := make([]podSchedulingFields, 0, 3)
	switch typed := obj.(type) {
	case *corev1.ReplicationController:
		if typed.Spec.Template == nil {
			return ctrl.Result{}, nil
		}
		fieldsList = append(fieldsList, podSchedulingFields{
			schedulerName:     &typed.Spec.Template.Spec.SchedulerName,
			priorityClassName: &typed.Spec.Template.Spec.PriorityClassName,
			labels:            &typed.Spec.Template.Labels,
		})
	case *appsv1.Deployment:
		fieldsList = append(fieldsList, podSchedulingFields{
			schedulerName:     &typed.Spec.Template.Spec.SchedulerName,
			priorityClassName: &typed.Spec.Template.Spec.PriorityClassName,
			labels:            &typed.Spec.Template.Labels,
		})
	case *appsv1.StatefulSet:
		fieldsList = append(fieldsList, podSchedulingFields{
			schedulerName:     &typed.Spec.Template.Spec.SchedulerName,
			priorityClassName: &typed.Spec.Template.Spec.PriorityClassName,
			labels:            &typed.Spec.Template.Labels,
		})
	case *appsv1.DaemonSet:
		fieldsList = append(fieldsList, podSchedulingFields{
			schedulerName:     &typed.Spec.Template.Spec.SchedulerName,
			priorityClassName: &typed.Spec.Template.Spec.PriorityClassName,
			labels:            &typed.Spec.Template.Labels,
		})
	case *batchv1.CronJob:
		fieldsList = append(fieldsList, podSchedulingFields{
			schedulerName:     &typed.Spec.JobTemplate.Spec.Template.Spec.SchedulerName,
			priorityClassName: &typed.Spec.JobTemplate.Spec.Template.Spec.PriorityClassName,
			labels:            &typed.Spec.JobTemplate.Spec.Template.Labels,
		})
	case *kservev1beta1.InferenceService:
		persist = false
		fieldsList = append(fieldsList, podSchedulingFields{
			schedulerName:     &typed.Spec.Predictor.PodSpec.SchedulerName,
			priorityClassName: &typed.Spec.Predictor.PodSpec.PriorityClassName,
			labels:            &typed.Spec.Predictor.ComponentExtensionSpec.Labels,
		})
		if typed.Spec.Transformer != nil {
			fieldsList = append(fieldsList, podSchedulingFields{
				schedulerName:     &typed.Spec.Transformer.PodSpec.SchedulerName,
				priorityClassName: &typed.Spec.Transformer.PodSpec.PriorityClassName,
				labels:            &typed.Spec.Transformer.ComponentExtensionSpec.Labels,
			})
		}
		if typed.Spec.Explainer != nil {
			fieldsList = append(fieldsList, podSchedulingFields{
				schedulerName:     &typed.Spec.Explainer.PodSpec.SchedulerName,
				priorityClassName: &typed.Spec.Explainer.PodSpec.PriorityClassName,
				labels:            &typed.Spec.Explainer.ComponentExtensionSpec.Labels,
			})
		}
	default:
		err = fmt.Errorf("unsupported workload object %T", obj)
	}
	for _, fields := range fieldsList {
		var fieldChanged bool
		fieldChanged, err = applyPolicyToPodScheduling(ctx, obj, fields, winner, r.Client, persist)
		if err != nil {
			break
		}
		changed = fieldChanged || changed
	}
	if err == nil && !persist && changed {
		err = r.Client.Update(ctx, obj)
	}
	if err != nil {
		logger.Error(err, "unable to patch workload scheduling", "policy", winner.Name)
		return ctrl.Result{}, nil
	}
	if changed {
		logger.Info("workload scheduling policy applied", "policy", winner.Name)
	}
	return ctrl.Result{}, nil
}

func sourceWorkloadTarget(source schedulerv1alpha1.WorkloadSource) workloadTarget {
	return workloadTarget(fmt.Sprintf("%s/%s", source.APIVersion, source.Kind))
}

func workloadRequestForKey(target workloadTarget, key types.NamespacedName) reconcile.Request {
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: key.Namespace,
			Name:      string(target) + workloadRequestSeparator + key.Name,
		},
	}
}

func watchWorkload(b *builder.Builder, object client.Object, target workloadTarget) *builder.Builder {
	return b.Watches(object, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
		return []reconcile.Request{workloadRequestForKey(target, client.ObjectKeyFromObject(obj))}
	}))
}

type podSchedulingFields struct {
	schedulerName     *string
	priorityClassName *string
	labels            *map[string]string
}

func applyPolicyToPodScheduling(
	ctx context.Context,
	obj client.Object,
	fields podSchedulingFields,
	policy *schedulerv1alpha1.SchedulingPolicy,
	patcher client.Client,
	persist bool,
) (bool, error) {
	changed := false
	target := policy.Spec.Target
	if target.SchedulerName != "" && *fields.schedulerName != target.SchedulerName {
		*fields.schedulerName = target.SchedulerName
		changed = true
	}
	if target.PriorityClassName != "" && *fields.priorityClassName != target.PriorityClassName {
		*fields.priorityClassName = target.PriorityClassName
		changed = true
	}
	if len(target.Labels) > 0 {
		currentLabels := *fields.labels
		if currentLabels == nil {
			currentLabels = make(map[string]string)
		}
		labelChanged := false
		for key, value := range target.Labels {
			if value == "" || currentLabels[key] == value {
				continue
			}
			currentLabels[key] = value
			labelChanged = true
		}
		if labelChanged {
			*fields.labels = currentLabels
			changed = true
		}
	}
	objectMetaChanged := false
	objectAnnotations := obj.GetAnnotations()
	objectMetaChanged, objectAnnotations = applyManagingPolicyAnnotation(objectAnnotations, policy.Name)
	if objectMetaChanged {
		obj.SetAnnotations(objectAnnotations)
	}
	changed = objectMetaChanged || changed
	if !changed {
		return false, nil
	}
	if !persist {
		return true, nil
	}

	return true, patcher.Update(ctx, obj)
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

func newSchedulingPolicyCache() *schedulingPolicyCache {
	return &schedulingPolicyCache{}
}

func loadSchedulingPolicyCache(ctx context.Context, reader client.Reader) (*schedulingPolicyCache, error) {
	var list schedulerv1alpha1.SchedulingPolicyList
	if err := reader.List(ctx, &list); err != nil {
		return nil, err
	}

	cache := newSchedulingPolicyCache()
	cache.policies = append(cache.policies, list.Items...)
	return cache, nil
}

func (c *schedulingPolicyCache) Upsert(policy schedulerv1alpha1.SchedulingPolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.policies {
		if c.policies[i].Name != policy.Name {
			continue
		}
		c.policies[i] = policy
		return
	}
	c.policies = append(c.policies, policy)
}

func (c *schedulingPolicyCache) Delete(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.policies {
		if c.policies[i].Name != name {
			continue
		}
		c.policies = append(c.policies[:i], c.policies[i+1:]...)
		break
	}
}

func (c *schedulingPolicyCache) List() []schedulerv1alpha1.SchedulingPolicy {
	c.mu.RLock()
	policies := append([]schedulerv1alpha1.SchedulingPolicy(nil), c.policies...)
	c.mu.RUnlock()
	return policies
}

// SetupWithManager sets up the controller with the Manager.
func (r *SchedulingPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.policies == nil {
		policies, err := loadSchedulingPolicyCache(context.Background(), mgr.GetAPIReader())
		if err != nil {
			return err
		}
		r.policies = policies
	}

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&schedulerv1alpha1.SchedulingPolicy{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("schedulingpolicy").
		Complete(r); err != nil {
		return err
	}

	workloadReconciler := &workloadReconciler{Client: r.Client, policies: r.policies}
	workloadController := ctrl.NewControllerManagedBy(mgr).
		Named("schedulingpolicy-workload").
		For(&appsv1.Deployment{}, builder.WithPredicates(predicate.NewPredicateFuncs(func(client.Object) bool { return false })))
	workloadController = watchWorkload(workloadController, &corev1.ReplicationController{}, targetReplicationController)
	workloadController = watchWorkload(workloadController, &appsv1.Deployment{}, targetDeployment)
	workloadController = watchWorkload(workloadController, &appsv1.StatefulSet{}, targetStatefulSet)
	workloadController = watchWorkload(workloadController, &appsv1.DaemonSet{}, targetDaemonSet)
	workloadController = watchWorkload(workloadController, &batchv1.CronJob{}, targetCronJob)
	workloadController = watchWorkload(workloadController, &kservev1beta1.InferenceService{}, targetInferenceService)

	if err := workloadController.Complete(workloadReconciler); err != nil {
		return err
	}

	return nil
}
