/*
Copyright 2026 Isaia Del Rosso.

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
	RandomStrategy     BalancingStrategy = "randomstrategy"
)

// Function describes one OpenFaaS function deployed on a dfaas-worker node.
type Function struct {
	// Name is templated by the dfaas-agent into HAProxy variable names
	// (`var(req.rate_local_func_<name>)`), and HAProxy variable names reject
	// hyphens. A single function called `dfaas-imgproc` therefore made HAProxy
	// refuse the WHOLE rendered config with a 400 from its Data Plane API —
	// "invalid syntax at char '-imgproc'" — so every worker kept serving the
	// placeholder 503 ("Proxy is running, but the DFaaS agent is not!") while
	// all seven Environment Conditions stayed green and the SSH probe was happy.
	// The name is also the OpenFaaS/DNS-1123 object name, which rejects
	// underscores, so the safe intersection is lowercase alphanumerics only.
	// Rejecting it here turns a silent data-plane death into an apply-time error.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[a-z0-9]+$`
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
