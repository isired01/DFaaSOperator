/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

// NodeCapacity expresses the relative CPU/RAM budget of a node.
// +kubebuilder:validation:Enum=LOW;MEDIUM;HIGH
type NodeCapacity string

const (
	CapacityLow    NodeCapacity = "LOW"
	CapacityMedium NodeCapacity = "MEDIUM"
	CapacityHigh   NodeCapacity = "HIGH"
)

// NodeRole tags a node either as a DFaaS worker or as a k6 load generator
// running its own k3s + k6-operator.
// +kubebuilder:validation:Enum=dfaas-worker;k6-load-generator
type NodeRole string

const (
	RoleDfaasWorker     NodeRole = "dfaas-worker"
	RoleK6LoadGenerator NodeRole = "k6-load-generator"
)

// BalancingStrategy is meaningful only for dfaas-worker nodes.
type BalancingStrategy string

const (
	StaticStrategy     BalancingStrategy = "staticstrategy"
	NodeMarginStrategy BalancingStrategy = "nodemarginstrategy"
	RecalcStrategy     BalancingStrategy = "recalcstrategy"
	AllLocalStrategy   BalancingStrategy = "alllocalstrategy"
	RLAgentStrategy    BalancingStrategy = "rlagentstrategy"
)

// Function describes one OpenFaaS function deployed on a dfaas-worker node.
type Function struct {
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	ExecTimeout int `json:"execTimeout"`

	// +kubebuilder:default=400
	// +kubebuilder:validation:Minimum=1
	MaxInflight int `json:"maxInflight"`

	// +kubebuilder:default=6000
	// +kubebuilder:validation:Minimum=1
	TimeoutMs int `json:"timeoutMs"`

	// MaxRate is the per-function request rate cap (req/s) consumed by
	// recalcstrategy. Emitted as OpenFaaS label `dfaas.maxrate` at deploy.
	// Required for recalcstrategy; ignored by staticstrategy and
	// alllocalstrategy.
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=1
	MaxRate int32 `json:"maxRate"`
}

// Topology declares inter-node network shaping (latency injection).
//
// NOT IMPLEMENTED: the field is schema-only. Nothing in the operator or in the
// Ansible playbooks reads it, so a declared link's latencyMs is never applied
// to any node. It is kept so existing manifests still validate; treat it as
// documentation of intent, not as a working feature.
type Topology struct {
	// +optional
	Links []Link `json:"links,omitempty"`
}

// Link is one symmetric latency link in the topology.
type Link struct {
	NodeA     string `json:"nodeA"`
	NodeB     string `json:"nodeB"`
	LatencyMs int    `json:"latencyMs"`
}
