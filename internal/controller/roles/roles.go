/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package roles is the one table describing what each NodeRole means to
// provisioning: which playbook, which inventory group, which Job suffix,
// which Condition. Before it, the two-role split was re-encoded at eleven
// branch sites and four role-keyed literals across ansible/, monitoring/ and
// the reconciler, three of which had to agree with no exhaustiveness check —
// and an unmatched role fell through buildInventory's switch to an empty
// inventory with a nil error. Here an unknown role is an error.
//
// Its own package so ansible, monitoring and controller can all import it
// without monitoring having to import the provisioning tool.
package roles

import (
	"fmt"

	dfaasv1 "dfaas-operator/api/v1"
)

// Spec is everything provisioning needs to know about one role.
type Spec struct {
	Role dfaasv1.NodeRole
	// Human is the short label used in logs and Condition messages.
	Human string
	// Playbook is the embedded Ansible playbook file this role runs.
	Playbook string
	// Group is the inventory host group the playbook targets.
	Group string
	// ConfigMapSuffix names the per-role playbook ConfigMap.
	ConfigMapSuffix string
	// JobSuffix keys the Ansible Job + inventory Secret names.
	JobSuffix string
	// CondType is the Environment Condition this role's Job stamps.
	CondType string
	// SkipReason/SkipMessage when the Environment has no nodes of this role.
	SkipReason  string
	SkipMessage string
	// DoneReason on Job success.
	DoneReason string
	// NeedsLibp2pKeys: the inventory carries per-node libp2p identities.
	NeedsLibp2pKeys bool
}

var table = []Spec{
	{
		Role:            dfaasv1.RoleDfaasWorker,
		Human:           "dfaas-worker",
		Playbook:        "setup-nodes.yml",
		Group:           "target_nodes",
		ConfigMapSuffix: "dfaas",
		JobSuffix:       "vms",
		CondType:        dfaasv1.EnvCondDFaaSNodesReady,
		SkipReason:      dfaasv1.EnvReasonNoWorkers,
		SkipMessage:     "no dfaas-worker nodes in spec — phase skipped",
		DoneReason:      dfaasv1.EnvReasonVMsProvisioned,
		NeedsLibp2pKeys: true,
	},
	{
		Role:            dfaasv1.RoleK6LoadGenerator,
		Human:           "k6",
		Playbook:        "setup-k6-nodes.yml",
		Group:           "k6_nodes",
		ConfigMapSuffix: "k6",
		JobSuffix:       "k6",
		CondType:        dfaasv1.EnvCondK6Ready,
		SkipReason:      dfaasv1.EnvReasonNoK6Nodes,
		SkipMessage:     "no k6-load-generator nodes in spec — phase skipped",
		DoneReason:      dfaasv1.EnvReasonK6Provisioned,
	},
}

// For returns the Spec of role, or an error for a role the table does not
// know — never a zero Spec.
func For(role dfaasv1.NodeRole) (Spec, error) {
	for _, s := range table {
		if s.Role == role {
			return s, nil
		}
	}
	return Spec{}, fmt.Errorf("unknown node role %q", role)
}

// All returns every role in a fixed order (workers first). Callers that run
// one thing per role iterate this instead of naming the roles.
func All() []Spec {
	out := make([]Spec, len(table))
	copy(out, table)
	return out
}
