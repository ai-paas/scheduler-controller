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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	schedulerv1alpha1 "github.com/ai-paas/scheduler-controller/api/v1alpha1"
)

var _ = Describe("SchedulingPolicy Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name: resourceName,
		}
		schedulingpolicy := &schedulerv1alpha1.SchedulingPolicy{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind SchedulingPolicy")
			err := k8sClient.Get(ctx, typeNamespacedName, schedulingpolicy)
			if err != nil && errors.IsNotFound(err) {
				resource := &schedulerv1alpha1.SchedulingPolicy{
					ObjectMeta: metav1.ObjectMeta{
						Name: resourceName,
					},
					Spec: schedulerv1alpha1.SchedulingPolicySpec{
						Sources: []schedulerv1alpha1.WorkloadSource{{
							APIVersion: "apps/v1",
							Kind:       "Deployment",
							Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test-resource"}},
						}},
						Target: schedulerv1alpha1.SchedulingTarget{SchedulerName: "kai-scheduler"},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &schedulerv1alpha1.SchedulingPolicy{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance SchedulingPolicy")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &SchedulingPolicyReconciler{
				Client:   k8sClient,
				policies: newSchedulingPolicyCache(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, typeNamespacedName, schedulingpolicy)).To(Succeed())
			Expect(schedulingpolicy.Status.ObservedGeneration).To(Equal(schedulingpolicy.Generation))
		})

		It("should mark pod sources as unsupported", func() {
			podPolicyName := types.NamespacedName{Name: "pod-source-policy"}
			podPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: podPolicyName.Name},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Sources: []schedulerv1alpha1.WorkloadSource{{
						APIVersion: "v1",
						Kind:       "Pod",
						Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test-resource"}},
					}},
					Target: schedulerv1alpha1.SchedulingTarget{SchedulerName: "kai-scheduler"},
				},
			}
			Expect(k8sClient.Create(ctx, podPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, podPolicy)
			})

			controllerReconciler := &SchedulingPolicyReconciler{
				Client:   k8sClient,
				policies: newSchedulingPolicyCache(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: podPolicyName})
			Expect(err).NotTo(HaveOccurred())

			currentPolicy := &schedulerv1alpha1.SchedulingPolicy{}
			Expect(k8sClient.Get(ctx, podPolicyName, currentPolicy)).To(Succeed())
			readyCondition := apiMeta.FindStatusCondition(currentPolicy.Status.Conditions, conditionTypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal("UnsupportedSource"))
			Expect(readyCondition.Message).To(ContainSubstring("v1/Pod"))

			Expect(controllerReconciler.policies.policies).To(BeEmpty())
		})

		It("should mark job sources as unsupported", func() {
			jobPolicyName := types.NamespacedName{Name: "job-source-policy"}
			jobPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: jobPolicyName.Name},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Sources: []schedulerv1alpha1.WorkloadSource{{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test-resource"}},
					}},
					Target: schedulerv1alpha1.SchedulingTarget{SchedulerName: "kai-scheduler"},
				},
			}
			Expect(k8sClient.Create(ctx, jobPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, jobPolicy)
			})

			controllerReconciler := &SchedulingPolicyReconciler{
				Client:   k8sClient,
				policies: newSchedulingPolicyCache(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: jobPolicyName})
			Expect(err).NotTo(HaveOccurred())

			currentPolicy := &schedulerv1alpha1.SchedulingPolicy{}
			Expect(k8sClient.Get(ctx, jobPolicyName, currentPolicy)).To(Succeed())
			readyCondition := apiMeta.FindStatusCondition(currentPolicy.Status.Conditions, conditionTypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal("UnsupportedSource"))
			Expect(readyCondition.Message).To(ContainSubstring("batch/v1/Job"))
		})

		It("should mark replicaset sources as unsupported", func() {
			replicaSetPolicyName := types.NamespacedName{Name: "replicaset-source-policy"}
			replicaSetPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: replicaSetPolicyName.Name},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Sources: []schedulerv1alpha1.WorkloadSource{{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"app": "test-resource"}},
					}},
					Target: schedulerv1alpha1.SchedulingTarget{SchedulerName: "kai-scheduler"},
				},
			}
			Expect(k8sClient.Create(ctx, replicaSetPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, replicaSetPolicy)
			})

			controllerReconciler := &SchedulingPolicyReconciler{
				Client:   k8sClient,
				policies: newSchedulingPolicyCache(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: replicaSetPolicyName})
			Expect(err).NotTo(HaveOccurred())

			currentPolicy := &schedulerv1alpha1.SchedulingPolicy{}
			Expect(k8sClient.Get(ctx, replicaSetPolicyName, currentPolicy)).To(Succeed())
			readyCondition := apiMeta.FindStatusCondition(currentPolicy.Status.Conditions, conditionTypeReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCondition.Reason).To(Equal("UnsupportedSource"))
			Expect(readyCondition.Message).To(ContainSubstring("apps/v1/ReplicaSet"))
		})

		It("should not change existing workloads when reconciling a SchedulingPolicy", func() {
			controllerReconciler := &SchedulingPolicyReconciler{
				Client: k8sClient,
			}

			deploymentName := types.NamespacedName{Namespace: "default", Name: "existing-policy-target"}
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentName.Name,
					Namespace: deploymentName.Namespace,
					Labels:    map[string]string{"app": "test-resource"},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, deployment)
			})

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: typeNamespacedName})
			Expect(err).NotTo(HaveOccurred())

			currentDeployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deploymentName, currentDeployment)).To(Succeed())
			Expect(currentDeployment.Spec.Template.Spec.SchedulerName).NotTo(Equal("kai-scheduler"))
			Expect(currentDeployment.Annotations).NotTo(HaveKey(managedByPolicyAnnotation))
			Expect(currentDeployment.Spec.Template.Annotations).NotTo(HaveKey(managedByPolicyAnnotation))
		})

		It("should not hand off matching workloads after winner policy deletion", func() {
			deploymentName := types.NamespacedName{Namespace: "default", Name: "test-deployment"}
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentName.Name,
					Namespace: deploymentName.Namespace,
					Labels: map[string]string{
						"workflow-id": "example-workflow",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker", "queue": "legacy"}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, deployment)
			})

			backupPolicyName := types.NamespacedName{Name: "z-backup-policy"}
			backupPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: backupPolicyName.Name},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Sources: []schedulerv1alpha1.WorkloadSource{{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"workflow-id": "example-workflow"}},
					}},
					Target: schedulerv1alpha1.SchedulingTarget{
						SchedulerName:     "backup-scheduler",
						PriorityClassName: "backup-priority",
						Labels:            map[string]string{"queue": "batch-z", "team": "ml-platform"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, backupPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, backupPolicy)
			})

			winnerPolicyName := types.NamespacedName{Name: "a-winner-policy"}
			winnerPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: winnerPolicyName.Name},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Sources: []schedulerv1alpha1.WorkloadSource{{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"workflow-id": "example-workflow"}},
					}},
					Target: schedulerv1alpha1.SchedulingTarget{
						SchedulerName:     "winner-scheduler",
						PriorityClassName: "winner-priority",
						Labels:            map[string]string{"queue": "batch-a", "team": "research"},
					},
				},
			}
			winnerPolicy.Spec.Priority = 100
			Expect(k8sClient.Create(ctx, winnerPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, winnerPolicy)
			})

			policyCache, err := loadSchedulingPolicyCache(ctx, k8sClient)
			Expect(err).NotTo(HaveOccurred())
			controllerReconciler := &SchedulingPolicyReconciler{
				Client:   k8sClient,
				policies: policyCache,
			}
			deploymentReconciler := &workloadReconciler{
				Client:   controllerReconciler.Client,
				policies: controllerReconciler.policies,
			}

			_, err = deploymentReconciler.Reconcile(ctx, workloadRequestForKey(targetDeployment, deploymentName))
			Expect(err).NotTo(HaveOccurred())

			currentDeployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deploymentName, currentDeployment)).To(Succeed())
			Expect(currentDeployment.Spec.Template.Spec.SchedulerName).To(Equal("winner-scheduler"))
			Expect(currentDeployment.Spec.Template.Spec.PriorityClassName).To(Equal("winner-priority"))
			Expect(currentDeployment.Spec.Template.Labels).To(HaveKeyWithValue("queue", "batch-a"))
			Expect(currentDeployment.Spec.Template.Labels).To(HaveKeyWithValue("team", "research"))
			Expect(currentDeployment.Annotations).To(HaveKeyWithValue(managedByPolicyAnnotation, winnerPolicyName.Name))
			Expect(currentDeployment.Spec.Template.Annotations).NotTo(HaveKey(managedByPolicyAnnotation))

			Expect(k8sClient.Delete(ctx, winnerPolicy)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: winnerPolicyName})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, deploymentName, currentDeployment)).To(Succeed())
			Expect(currentDeployment.Spec.Template.Spec.SchedulerName).To(Equal("winner-scheduler"))
			Expect(currentDeployment.Spec.Template.Spec.PriorityClassName).To(Equal("winner-priority"))
			Expect(currentDeployment.Spec.Template.Labels).To(HaveKeyWithValue("queue", "batch-a"))
			Expect(currentDeployment.Spec.Template.Labels).To(HaveKeyWithValue("team", "research"))
			Expect(currentDeployment.Annotations).To(HaveKeyWithValue(managedByPolicyAnnotation, winnerPolicyName.Name))
			Expect(currentDeployment.Spec.Template.Annotations).NotTo(HaveKey(managedByPolicyAnnotation))
		})

		It("should select the highest priority matching policy", func() {
			deploymentName := types.NamespacedName{Namespace: "default", Name: "priority-target"}
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentName.Name,
					Namespace: deploymentName.Namespace,
					Labels:    map[string]string{"workflow-id": "example-workflow"},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, deployment)
			})
			source := schedulerv1alpha1.WorkloadSource{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"workflow-id": "example-workflow"}},
			}
			olderTime := metav1.NewTime(time.Date(2026, time.April, 22, 12, 0, 0, 0, time.UTC))
			newerTime := metav1.NewTime(time.Date(2026, time.April, 22, 13, 0, 0, 0, time.UTC))

			lowPriorityPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "older-low-priority", CreationTimestamp: olderTime},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Priority: 10,
					Sources:  []schedulerv1alpha1.WorkloadSource{source},
					Target: schedulerv1alpha1.SchedulingTarget{
						SchedulerName: "low-priority-scheduler",
					},
				},
			}
			highPriorityPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "newer-high-priority", CreationTimestamp: newerTime},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Priority: 20,
					Sources:  []schedulerv1alpha1.WorkloadSource{source},
					Target: schedulerv1alpha1.SchedulingTarget{
						SchedulerName: "high-priority-scheduler",
					},
				},
			}
			Expect(k8sClient.Create(ctx, lowPriorityPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, lowPriorityPolicy)
			})
			Expect(k8sClient.Create(ctx, highPriorityPolicy)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, highPriorityPolicy)
			})

			policyCache, err := loadSchedulingPolicyCache(ctx, k8sClient)
			Expect(err).NotTo(HaveOccurred())
			deploymentReconciler := &workloadReconciler{
				Client:   k8sClient,
				policies: policyCache,
			}

			_, err = deploymentReconciler.Reconcile(ctx, workloadRequestForKey(targetDeployment, deploymentName))
			Expect(err).NotTo(HaveOccurred())

			currentDeployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deploymentName, currentDeployment)).To(Succeed())
			Expect(currentDeployment.Spec.Template.Spec.SchedulerName).To(Equal("high-priority-scheduler"))
			Expect(currentDeployment.Annotations).To(HaveKeyWithValue(managedByPolicyAnnotation, "newer-high-priority"))
		})

		It("should use creation timestamp when matching policy priorities are equal", func() {
			policyCache := newSchedulingPolicyCache()
			deploymentReconciler := &workloadReconciler{
				Client:   k8sClient,
				policies: policyCache,
			}
			deploymentName := types.NamespacedName{Namespace: "default", Name: "timestamp-target"}
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentName.Name,
					Namespace: deploymentName.Namespace,
					Labels:    map[string]string{"workflow-id": "example-workflow"},
				},
				Spec: appsv1.DeploymentSpec{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "worker"}},
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "worker"}},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, deployment)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, deployment)
			})
			source := schedulerv1alpha1.WorkloadSource{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Selector:   &metav1.LabelSelector{MatchLabels: map[string]string{"workflow-id": "example-workflow"}},
			}
			olderTime := metav1.NewTime(time.Date(2026, time.April, 22, 12, 0, 0, 0, time.UTC))
			newerTime := metav1.NewTime(time.Date(2026, time.April, 22, 13, 0, 0, 0, time.UTC))

			olderPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "z-older-policy", CreationTimestamp: olderTime},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Priority: 50,
					Sources:  []schedulerv1alpha1.WorkloadSource{source},
					Target: schedulerv1alpha1.SchedulingTarget{
						SchedulerName: "older-policy-scheduler",
					},
				},
			}
			newerPolicy := &schedulerv1alpha1.SchedulingPolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "a-newer-policy", CreationTimestamp: newerTime},
				Spec: schedulerv1alpha1.SchedulingPolicySpec{
					Priority: 50,
					Sources:  []schedulerv1alpha1.WorkloadSource{source},
					Target: schedulerv1alpha1.SchedulingTarget{
						SchedulerName: "newer-policy-scheduler",
					},
				},
			}
			policyCache.Upsert(*olderPolicy)
			policyCache.Upsert(*newerPolicy)

			_, err := deploymentReconciler.Reconcile(ctx, workloadRequestForKey(targetDeployment, deploymentName))
			Expect(err).NotTo(HaveOccurred())

			currentDeployment := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, deploymentName, currentDeployment)).To(Succeed())
			Expect(currentDeployment.Spec.Template.Spec.SchedulerName).To(Equal("older-policy-scheduler"))
			Expect(currentDeployment.Annotations).To(HaveKeyWithValue(managedByPolicyAnnotation, "z-older-policy"))
		})

	})
})
