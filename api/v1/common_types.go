/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

// NodeCapacity is a size label for a node. The operator sizes nothing with it:
// on a dfaas-worker it becomes the node_type label of the node's Prometheus
// scrape target, and so a label on its federated series and a column of the
// exported metrics CSV.
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
	RandomStrategy     BalancingStrategy = "randomstrategy"
)

// Function describes one OpenFaaS function deployed on a dfaas-worker node.
type Function struct {
	// The dfaas-agent templates the name into HAProxy variable names, which
	// reject hyphens: HAProxy then refuses the whole rendered config and every
	// worker keeps serving the placeholder 503. The name is also a DNS-1123
	// object name, which rejects underscores.

	// Name is the OpenFaaS function name, invoked at /function/<name> on the
	// node's HAProxy entrypoint (NodePort 30080). Lowercase letters and digits
	// only.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]+$`
	Name string `json:"name"`

	// Image is the function's container image. Pin it by digest or an immutable
	// tag for a reproducible campaign.
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// ExecTimeout is the function watchdog's exec_timeout, in seconds.
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	ExecTimeout int `json:"execTimeout"`

	// MaxInflight is the function watchdog's max_inflight: the concurrent
	// requests one replica accepts.
	// +kubebuilder:default=400
	// +kubebuilder:validation:Minimum=1
	MaxInflight int `json:"maxInflight"`

	// TimeoutMs is the per-function timeout in milliseconds, deployed as the
	// OpenFaaS label dfaas.timeout_ms for the dfaas-agent.
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
	// Links are the declared latency links. None of them is applied.
	// +optional
	Links []Link `json:"links,omitempty"`
}

// Link is one symmetric latency link in the topology.
type Link struct {
	// NodeA is one end of the link, a nodeID of spec.nodes. It is not checked
	// against the node list.
	NodeA string `json:"nodeA"`
	// NodeB is the other end of the link, a nodeID of spec.nodes. It is not
	// checked against the node list.
	NodeB string `json:"nodeB"`
	// LatencyMs is the latency declared for the link, in milliseconds. It is
	// not applied to either node.
	LatencyMs int `json:"latencyMs"`
}
