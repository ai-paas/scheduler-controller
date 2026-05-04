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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type WorkloadSource struct {
	// +kubebuilder:validation:MinLength=1
	APIVersion string `json:"apiVersion"`
	// +kubebuilder:validation:MinLength=1
	// Supported workload kinds are controller-managed resources such as Deployment,
	// ReplicaSet, StatefulSet, DaemonSet, ReplicationController, Job, CronJob,
	// and serving.kserve.io/v1beta1 InferenceService. Pod is intentionally unsupported.
	Kind string `json:"kind"`
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty"`
}

type SchedulingTarget struct {
	// +kubebuilder:validation:MinLength=1
	SchedulerName string `json:"schedulerName"`
	// +optional
	// +kubebuilder:validation:MinLength=1
	PriorityClassName string `json:"priorityClassName,omitempty"`
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// SchedulingPolicySpec defines the desired state of SchedulingPolicy.
type SchedulingPolicySpec struct {
	// +optional
	// +kubebuilder:default=0
	Priority int32 `json:"priority,omitempty"`
	// +kubebuilder:validation:MinItems=1
	Sources []WorkloadSource `json:"sources"`
	Target  SchedulingTarget `json:"target"`
}

// SchedulingPolicyStatus defines the observed state of SchedulingPolicy.
type SchedulingPolicyStatus struct {
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
	MatchedResources   int32              `json:"matchedResources,omitempty"`
	PatchedResources   int32              `json:"patchedResources,omitempty"`
	LastAppliedTime    *metav1.Time       `json:"lastAppliedTime,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=sp

// SchedulingPolicy is the Schema for the schedulingpolicies API.
type SchedulingPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SchedulingPolicySpec   `json:"spec,omitempty"`
	Status SchedulingPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SchedulingPolicyList contains a list of SchedulingPolicy.
type SchedulingPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SchedulingPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SchedulingPolicy{}, &SchedulingPolicyList{})
}
