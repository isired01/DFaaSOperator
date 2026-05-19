/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EnvironmentPhase tracks the infrastructure lifecycle.
// +kubebuilder:validation:Enum=Idle;ProvisioningVMs;ProvisioningInfra;ProvisioningMonitoring;Ready;Failed
type EnvironmentPhase string

const (
	EnvIdle                   EnvironmentPhase = "Idle"
	EnvProvisioningVMs        EnvironmentPhase = "ProvisioningVMs"
	EnvProvisioningInfra      EnvironmentPhase = "ProvisioningInfra"
	EnvProvisioningMonitoring EnvironmentPhase = "ProvisioningMonitoring"
	EnvReady                  EnvironmentPhase = "Ready"
	EnvFailed                 EnvironmentPhase = "Failed"
)

// EnvironmentNode declares one machine in the federation.
type EnvironmentNode struct {
	// +kubebuilder:validation:Required
	NodeID string `json:"nodeID"`

	// +kubebuilder:validation:Required
	IPAddress string `json:"ipAddress"`

	// +kubebuilder:validation:Required
	Role NodeRole `json:"role"`

	// +kubebuilder:validation:Required
	Capacity NodeCapacity `json:"capacity"`

	// SSH credentials (inline, matches the FE-produced manifests).
	// +kubebuilder:validation:Required
	Username string `json:"username"`
	// +kubebuilder:validation:Required
	Password string `json:"password"`

	// BalancingStrategy is meaningful only for dfaas-worker nodes.
	// Accepted values are the canonical strategy names recognised by
	// dfaas-agent (AGENT_STRATEGY env var). Typos that look like
	// "alllocal" silently fall back to recalcstrategy and crash on
	// missing dfaas.maxrate, so the enum guard is the safety net.
	// +kubebuilder:validation:Enum=staticstrategy;nodemarginstrategy;recalcstrategy;alllocalstrategy;rlagentstrategy
	// +optional
	BalancingStrategy BalancingStrategy `json:"balancingStrategy,omitempty"`

	// Functions to deploy on a dfaas-worker node.
	// +optional
	Functions []Function `json:"functions,omitempty"`
}

// EnvironmentSpec is the desired federation infrastructure.
type EnvironmentSpec struct {
	// +kubebuilder:validation:MinItems=1
	Nodes []EnvironmentNode `json:"nodes"`

	// +optional
	Topology Topology `json:"topology,omitempty"`

	// CleanupOnDelete drives the finalizer's behavior.
	// +kubebuilder:default=false
	CleanupOnDelete bool `json:"cleanupOnDelete,omitempty"`
}

// K6NodeStatus mirrors per-k6-machine connectivity info so the LoadTest
// reconciler can resolve remote clusters without re-walking the spec.
type K6NodeStatus struct {
	NodeID           string `json:"nodeID"`
	IPAddress        string `json:"ipAddress"`
	KubeconfigSecret string `json:"kubeconfigSecret"`
}

// EnvironmentStatus tracks the infrastructure phase observed by the controller.
type EnvironmentStatus struct {
	// +optional
	Phase EnvironmentPhase `json:"phase,omitempty"`

	// ObservedGeneration is the spec.generation observed at the last
	// successful provisioning run. The reconciler skips re-provisioning when
	// metadata.generation == status.observedGeneration and phase == Ready.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// +optional
	K6Nodes []K6NodeStatus `json:"k6Nodes,omitempty"`

	// +optional
	DfaasNodes []string `json:"dfaasNodes,omitempty"`

	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// Environment is the schema for the federation infrastructure resource.
type Environment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              EnvironmentSpec   `json:"spec,omitempty"`
	Status            EnvironmentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type EnvironmentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Environment `json:"items"`
}

// HasNodeWithRole reports whether at least one node in the spec carries the
// requested role.
func (e *Environment) HasNodeWithRole(role NodeRole) bool {
	for _, n := range e.Spec.Nodes {
		if n.Role == role {
			return true
		}
	}
	return false
}

// NodesWithRole returns the subset of spec.nodes matching the role.
func (e *Environment) NodesWithRole(role NodeRole) []EnvironmentNode {
	var out []EnvironmentNode
	for _, n := range e.Spec.Nodes {
		if n.Role == role {
			out = append(out, n)
		}
	}
	return out
}

func init() {
	SchemeBuilder.Register(&Environment{}, &EnvironmentList{})
}
