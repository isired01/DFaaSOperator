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
// +kubebuilder:validation:Enum=Idle;ProvisioningVMs;ProvisioningInfra;ProvisioningMonitoring;Ready;Failed;Unreachable
type EnvironmentPhase string

const (
	EnvIdle                   EnvironmentPhase = "Idle"
	EnvProvisioningVMs        EnvironmentPhase = "ProvisioningVMs"
	EnvProvisioningInfra      EnvironmentPhase = "ProvisioningInfra"
	EnvProvisioningMonitoring EnvironmentPhase = "ProvisioningMonitoring"
	EnvReady                  EnvironmentPhase = "Ready"
	EnvFailed                 EnvironmentPhase = "Failed"
	// EnvUnreachable is a NON-terminal state for an Environment whose nodes
	// stopped answering SSH (:22) — during provisioning after the fast
	// sshRetryBudget, or while Ready after healthRetryBudget. Unlike Failed it
	// auto-recovers: the reconciler re-probes indefinitely and, once the nodes
	// answer again, returns to Ready (if already provisioned) or resumes
	// provisioning. Reserved for transient connectivity loss; genuine
	// provisioning failures (Ansible / job-creation) still go to terminal Failed.
	EnvUnreachable EnvironmentPhase = "Unreachable"
)

// Dispatchable reports whether a LoadTest may be created and dispatched
// against an Environment in this phase: Ready only. A Degraded phase (infra
// up, monitoring down) once qualified too; it was dropped because no reconcile
// path ever produced it and a test without metrics is worthless to the user.
// The gateway mirrors this set and must not be looser.
//
// It lives here because the comparison was re-typed at four sites inside one
// reconcile function -- three of them negated -- plus once more in the
// gateway. Adding EnvUnreachable to the set (plausible: Unreachable is
// explicitly non-terminal and auto-recovering) meant finding all five by eye,
// and missing one left the create gate rejecting while the Occupancy gate
// admitted, with no compile error and no test failure.
func (p EnvironmentPhase) Dispatchable() bool {
	return p == EnvReady
}

// Condition Types stamped on Environment.status.conditions (P15).
const (
	EnvCondReady               = "Ready"
	EnvCondVMsReady            = "VMsReady"
	EnvCondDFaaSNodesReady     = "DFaaSNodesReady"
	EnvCondK6Ready             = "K6Ready"
	EnvCondInfrastructureReady = "InfrastructureReady"
	EnvCondMonitoringReady     = "MonitoringReady"
	// EnvCondNodesReachable reflects the per-minute SSH (:22) liveness probe
	// run against every declared node while the Environment is Ready.
	EnvCondNodesReachable = "NodesReachable"
)

// Condition Reasons stamped on Environment.status.conditions (P15).
const (
	EnvReasonSkipped            = "Skipped"
	EnvReasonNoWorkers          = "NoWorkers"
	EnvReasonNoK6Nodes          = "NoK6Nodes"
	EnvReasonVMsProvisioned     = "VMsProvisioned"
	EnvReasonK6Provisioned      = "K6Provisioned"
	EnvReasonSSHReachable       = "SSHReachable"
	EnvReasonSSHUnreachable     = "SSHUnreachable"
	EnvReasonAnsibleRunning     = "AnsibleRunning"
	EnvReasonAnsibleFailed      = "AnsibleFailed"
	EnvReasonJobCreationFailed  = "JobCreationFailed"
	EnvReasonHelmInstalling     = "HelmInstalling"
	EnvReasonHelmFailed         = "HelmFailed"
	EnvReasonWaitingPods        = "WaitingPods"
	EnvReasonPodsRunning        = "PodsRunning"
	EnvReasonInfraReady         = "InfraReady"
	EnvReasonInfraFailed        = "InfraFailed"
	EnvReasonUpdating           = "Updating"
	EnvReasonInitializing       = "Initializing"
	EnvReasonProvisioning       = "Provisioning"
	EnvReasonAllSubsystemsReady = "AllSubsystemsReady"
	EnvReasonFailed             = "Failed"
	EnvReasonJobPending         = "JobPending"
	// EnvReasonCheckFailed marks a readiness probe that could not be evaluated
	// at all (e.g. the Pod List was refused), as opposed to WaitingPods which
	// means "evaluated, not ready yet". Without the split an RBAC regression
	// looks exactly like a slow rollout.
	EnvReasonCheckFailed = "CheckFailed"
)

// EnvironmentNode declares one machine in the federation.
//
// A dfaas-worker must declare at least one function. A worker with none serves
// nothing, and the empty list is worse than useless downstream: buildInventory
// marshals a nil slice to the JSON literal `null`, not `[]`, so the inventory
// carries node_specific_functions='null' and the playbook's prune task runs
// `null | map(attribute='name')` over a four-character string — the play dies
// and the Environment lands in Failed with the function still deployed.
// Scoped to the role: a k6-load-generator runs k6, not OpenFaaS, and the
// inventory emits no function var for it at all.
// +kubebuilder:validation:XValidation:rule="self.role != 'dfaas-worker' || (has(self.functions) && size(self.functions) > 0)",message="a dfaas-worker node must declare at least one function"
type EnvironmentNode struct {
	// NodeID must be DNS-1123 compatible (lowercase): it is embedded in
	// Kubernetes object names (kubeconfig Secret <env>-<nodeID>-kubeconfig,
	// remote TestRuns, k6-log ConfigMaps) — an uppercase ID makes the k6
	// Ansible Job's Secret push fail with a censored 422.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	// Pattern (not CEL XValidation): CEL cost estimation on unbounded
	// node arrays blows the schema budget; the OpenAPI pattern is free.
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	NodeID string `json:"nodeID"`

	// MaxLength is not cosmetic: without it the CEL uniqueness rule on
	// spec.nodes below is rejected, because the cost estimator assumes the
	// largest string the request budget allows for every comparison. 45 is the
	// longest possible textual IP (IPv6 with an embedded IPv4, e.g.
	// ffff:...:255.255.255.255).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=45
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
	// +kubebuilder:validation:Enum=staticstrategy;nodemarginstrategy;recalcstrategy;alllocalstrategy;rlagentstrategy;randomstrategy
	// +optional
	BalancingStrategy BalancingStrategy `json:"balancingStrategy,omitempty"`

	// Functions to deploy on a dfaas-worker node.
	// +optional
	Functions []Function `json:"functions,omitempty"`
}

// EnvironmentSpec is the desired federation infrastructure.
type EnvironmentSpec struct {
	// Nodes is keyed by nodeID: the API server rejects duplicates at admission
	// (listType=map). Without it two entries sharing a nodeID collide on every
	// derived object name (kubeconfig Secret, libp2p key entry, remote TestRun)
	// and the second silently overwrites the first.
	//
	// ipAddress is unique too, enforced by the CEL rule below: one physical
	// machine is one node. Two entries sharing an IP used to be valid (the
	// listMapKey only guards nodeID) and produced two libp2p identities on one
	// box — the Ansible run installs dfaas-agent twice with different keys, and
	// whichever lands last leaves every peer dialling a dead peer ID.
	// MaxItems is what makes the rule admissible at all: CEL cost estimation on
	// an unbounded array blows the schema budget (same reason nodeID uses an
	// OpenAPI Pattern instead of CEL), and this comparison is O(n²).
	// NOTE: scope is one Environment. Nothing stops two *different* Environments
	// from declaring the same machine — that needs a cross-object check the CRD
	// schema cannot express.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=50
	// +kubebuilder:validation:XValidation:rule="self.all(n, self.exists_one(m, m.ipAddress == n.ipAddress))",message="each ipAddress must appear at most once: one machine is one node"
	// +listType=map
	// +listMapKey=nodeID
	Nodes []EnvironmentNode `json:"nodes"`

	// +optional
	Topology Topology `json:"topology,omitempty"`

	// REMOVED: CleanupOnDelete. It was documented as driving the finalizer, but
	// nothing ever read it: the finalizer in environment_lifecycle.go always
	// runs CleanupTargets and then drops itself, on every deletion. Keeping the
	// field made the UI promise a VM teardown that never happened.

	// S3ConfigRef points at a cluster-scoped S3 server configuration
	// registered in namespace "dfaas-s3" (Secret with label
	// "dfaas.io/s3-config=true"). When set, LoadTests targeting this
	// Environment export their metrics CSV to that S3 endpoint under a
	// bucket derived from the Environment name. When nil, LoadTests export
	// to the built-in in-cluster SeaweedFS sink ("seaweedfs-default").
	// +optional
	S3ConfigRef *S3ConfigRef `json:"s3ConfigRef,omitempty"`
}

// S3ConfigRef references one S3 server configuration by name. The actual
// credentials + endpoint live in Secret "dfaas-s3/<name>" with the keys
// "endpoint", "region", "access_key_id", "secret_access_key",
// "force_path_style".
type S3ConfigRef struct {
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
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

	// ObservedGeneration is the spec.generation the last settled run (Ready or
	// Failed) applied, copied from provisioningGeneration at the settle. The
	// reconciler skips re-provisioning when metadata.generation ==
	// status.observedGeneration and phase == Ready.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ProvisioningGeneration is the metadata.generation the current (or last)
	// provisioning run is applying, recorded when the run starts
	// (ProvisioningVMs). A settled phase stamps it into observedGeneration, so
	// an edit that lands mid-run is detected as drift instead of being marked
	// installed without ever reaching Ansible.
	// +optional
	ProvisioningGeneration int64 `json:"provisioningGeneration,omitempty"`

	// LastHealthCheck is the wall-clock time of the most recent Ready-state
	// SSH liveness probe round. Stamped each time the periodic check runs
	// while the Environment is Ready.
	// +optional
	LastHealthCheck *metav1.Time `json:"lastHealthCheck,omitempty"`

	// +optional
	K6Nodes []K6NodeStatus `json:"k6Nodes,omitempty"`

	// +optional
	DfaasNodes []string `json:"dfaasNodes,omitempty"`

	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
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
