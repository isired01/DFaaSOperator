/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// EnvironmentPhase tracks the infrastructure lifecycle.
// +kubebuilder:validation:Enum=ProvisioningVMs;ProvisioningInfra;ProvisioningMonitoring;Ready;Failed;Unreachable
type EnvironmentPhase string

const (
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
func (p EnvironmentPhase) Dispatchable() bool {
	return p == EnvReady
}

// Condition Types stamped on Environment.status.conditions.
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

// Condition Reasons stamped on Environment.status.conditions.
const (
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

// A worker with no function serves nothing, and the empty list breaks the
// playbook: buildInventory marshals a nil slice to the JSON literal `null`, so
// the prune task runs `null | map(attribute='name')` over a string, the play
// dies and the Environment lands in Failed. The rule is scoped to the role: a
// k6-load-generator runs k6, not OpenFaaS, and gets no function var.

// EnvironmentNode declares one machine in the federation. A dfaas-worker must
// declare at least one function.
// +kubebuilder:validation:XValidation:rule="self.role != 'dfaas-worker' || (has(self.functions) && size(self.functions) > 0)",message="a dfaas-worker node must declare at least one function"
type EnvironmentNode struct {
	// The check is an OpenAPI Pattern, not a CEL rule: CEL cost estimation on
	// unbounded node arrays blows the schema budget.

	// NodeID names the node: a lowercase DNS-1123 label, unique within
	// spec.nodes. It is embedded in Kubernetes object names (kubeconfig Secret
	// <env>-<nodeID>-kubeconfig, remote TestRuns, k6-log ConfigMaps).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	NodeID string `json:"nodeID"`

	// MaxLength is what keeps the CEL uniqueness rule on spec.nodes admissible:
	// the cost estimator assumes the largest string the request budget allows
	// for every comparison. 45 is the longest textual IP (IPv6 with an embedded
	// IPv4, e.g. ffff:...:255.255.255.255).

	// IPAddress is the address the operator reaches this machine on: SSH on :22
	// and, for a k6-load-generator, the k3s API on :6443. Unique within
	// spec.nodes.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=45
	IPAddress string `json:"ipAddress"`

	// +kubebuilder:validation:Required
	Role NodeRole `json:"role"`

	// +kubebuilder:validation:Required
	Capacity NodeCapacity `json:"capacity"`

	// Username is the SSH user the operator logs in as. Username and Password
	// are stored in plain text in the spec.
	// +kubebuilder:validation:Required
	Username string `json:"username"`

	// Password is the SSH password of Username.
	// +kubebuilder:validation:Required
	Password string `json:"password"`

	// The enum is the safety net: a misspelt name such as "alllocal" silently
	// falls back to recalcstrategy in the dfaas-agent and crashes on the missing
	// dfaas.maxrate label.

	// BalancingStrategy is the dfaas-agent strategy (AGENT_STRATEGY) of a
	// dfaas-worker; ignored on a k6-load-generator. Supported: staticstrategy,
	// alllocalstrategy, recalcstrategy (uses each function's maxRate) and
	// randomstrategy. nodemarginstrategy and rlagentstrategy pass validation
	// but are not supported by this platform.
	// +kubebuilder:validation:Enum=staticstrategy;nodemarginstrategy;recalcstrategy;alllocalstrategy;rlagentstrategy;randomstrategy
	// +optional
	BalancingStrategy BalancingStrategy `json:"balancingStrategy,omitempty"`

	// Functions to deploy on a dfaas-worker node.
	// +optional
	Functions []Function `json:"functions,omitempty"`
}

// EnvironmentSpec is the desired federation infrastructure.
type EnvironmentSpec struct {
	// The list is keyed by nodeID (listType=map), so the API server rejects
	// duplicates at admission: two entries sharing a nodeID would collide on
	// every derived object name (kubeconfig Secret, libp2p key entry, remote
	// TestRun) and the second would silently overwrite the first.
	// The CEL rule keeps ipAddress unique too. Two entries sharing an IP would
	// give one machine two libp2p identities: Ansible installs dfaas-agent twice
	// with different keys, and whichever lands last leaves every peer dialling a
	// dead peer ID.
	// MaxItems is what makes the rule admissible: CEL cost estimation on an
	// unbounded array blows the schema budget (the same reason nodeID uses an
	// OpenAPI Pattern), and the comparison is O(n²).
	// Two different Environments can still declare the same machine: that needs
	// a cross-object check the CRD schema cannot express.

	// Nodes are the machines of the federation, keyed by nodeID. Each ipAddress
	// may appear once (one machine is one node); at most 50 nodes. Uniqueness
	// is checked within one Environment only.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=50
	// +kubebuilder:validation:XValidation:rule="self.all(n, self.exists_one(m, m.ipAddress == n.ipAddress))",message="each ipAddress must appear at most once: one machine is one node"
	// +listType=map
	// +listMapKey=nodeID
	Nodes []EnvironmentNode `json:"nodes"`

	// S3ConfigRef points at a cluster-scoped S3 server configuration
	// registered in namespace "dfaas-s3" (Secret with label
	// "dfaas.io/s3-config=true"). When set, LoadTests targeting this
	// Environment export their metrics CSV to that S3 endpoint under a
	// bucket derived from the Environment name. When nil, LoadTests export
	// to the built-in in-cluster SeaweedFS sink ("seaweedfs-default"). The
	// Secret must hold the keys endpoint, region, access_key_id,
	// secret_access_key and force_path_style; every key except endpoint is
	// required by the exporter Job.
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

	// ManagementAddress is the management node's address as this generator
	// sees it: the first field of $SSH_CONNECTION during the k6 playbook. The
	// playbook records it only when it is an IPv4 address (the generator's k3s
	// is single-stack, so its runner pods cannot dial IPv6) and the generator
	// could reach the filer NodePort with it (an HTTP answer, or connection
	// refused because SeaweedFS is installed later, in ProvisioningMonitoring),
	// and syncNodeStatus copies it from the kubeconfig Secret annotation
	// dfaas.io/management-address. The LoadTest reconciler builds every URL
	// this generator's runner dials back on it: the filer NodePort (summary
	// upload, GO signal) and the S3 NodePort (assets). Empty means not detected
	// (an older playbook, a generator reached over IPv6, detection failed, or
	// unverified): the filer URLs then fall back to DFAAS_SYNC_PUBLIC_URL, else
	// HOST_IP, and the runner gets no asset base.
	// +optional
	// +kubebuilder:validation:MaxLength=45
	ManagementAddress string `json:"managementAddress,omitempty"`
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

	// DfaasNodes lists the nodeIDs of the dfaas-worker nodes.
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
