/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// ProvisionedNode is one machine as it was ACTUALLY installed by the last
// fully successful provisioning run. It is the only place the operator keeps a
// node's SSH credentials after that node leaves spec.nodes -- which is what
// makes decommissioning a removed machine possible at all.
//
// Every field is here because some playbook or some verdict reads it; see
// Classify for what each one decides. Notably absent: capacity (reaches no
// playbook -- its only consumer, monitoring.ReconcileTargets, re-runs from spec
// on every pass anyway), topology and s3ConfigRef.
type ProvisionedNode struct {
	NodeID    string           `json:"nodeID"`
	IPAddress string           `json:"ipAddress"`
	Role      dfaasv1.NodeRole `json:"role"`
	Username  string           `json:"username"`
	Password  string           `json:"password"`

	// Position is the node's index in spec.nodes at snapshot time. Stored
	// because buildInventory derives the libp2p bootstrap list *positionally*:
	// the first dfaas-worker is the seed (is_bootstrap=false, empty list) and
	// every later one dials its predecessors. Reordering spec.nodes therefore
	// re-points the whole mesh while changing no other field, and a snapshot
	// keyed by nodeID has no other way to notice.
	Position int `json:"position"`

	// BalancingStrategy reaches only the dfaas-agent Helm task (AGENT_STRATEGY),
	// which carries no `functions` tag: classifying a strategy-only edit as
	// function-only would silently never apply it.
	BalancingStrategy dfaasv1.BalancingStrategy `json:"balancingStrategy,omitempty"`

	// Functions is the sole input to the function-only verdict.
	Functions []dfaasv1.Function `json:"functions,omitempty"`
}

// Verdict says how much of the provisioning run a spec edit actually requires.
type Verdict int

const (
	// VerdictFull re-runs both playbooks end to end. Today's only behaviour,
	// and the fallback for anything the classifier is unsure about.
	VerdictFull Verdict = iota
	// VerdictFunctions re-runs setup-nodes.yml with --tags functions and skips
	// the k6 playbook, which has no function task at all.
	VerdictFunctions
	// VerdictNone: the edit touched no field any playbook reads.
	VerdictNone
)

func (v Verdict) String() string {
	switch v {
	case VerdictFunctions:
		return "functions"
	case VerdictNone:
		return "none"
	default:
		return "full"
	}
}

// Diff is the comparison of the last provisioning snapshot against the current
// spec: how much to re-run, and which machines the edit orphaned.
type Diff struct {
	Verdict Verdict

	// Removed are the snapshot records whose ipAddress appears nowhere in the
	// current spec -- the MACHINES the edit orphaned, sorted by ipAddress for a
	// stable Condition message. Keyed by ipAddress and never by nodeID on
	// purpose: nodeID is an operator-side label, the machine's identity is its
	// address, and renaming a nodeID in place would otherwise report a live
	// machine as removed and run k3s-uninstall.sh on it.
	Removed []ProvisionedNode
}

// Classify compares the last provisioning snapshot with the current spec.
//
// found == false -- no snapshot at all: first ever run, or an Environment
// provisioned before this operator version -- yields VerdictFull with an EMPTY
// Removed set. That is the migration guard, and it is the most important line
// in this file: reading a missing snapshot as "every node was removed" would
// decommission the whole federation on the first reconcile after an upgrade.
// An empty-but-present snapshot is a different statement ("the last run
// installed nothing") and is handled by the normal path.
func Classify(snapshot map[string]ProvisionedNode, found bool,
	spec []dfaasv1.EnvironmentNode) Diff {

	if !found {
		return Diff{Verdict: VerdictFull}
	}

	liveIPs := make(map[string]struct{}, len(spec))
	for _, n := range spec {
		liveIPs[n.IPAddress] = struct{}{}
	}

	var removed []ProvisionedNode
	for _, rec := range snapshot {
		if _, alive := liveIPs[rec.IPAddress]; !alive {
			removed = append(removed, rec)
		}
	}
	sort.Slice(removed, func(i, j int) bool { return removed[i].IPAddress < removed[j].IPAddress })

	// A removal forces a full run, not just a teardown: dropping a node changes
	// AGENT_BOOTSTRAP_NODES_LIST on every survivor.
	if len(removed) > 0 {
		return Diff{Verdict: VerdictFull, Removed: removed}
	}

	functionsChanged := false
	for i, n := range spec {
		rec, known := snapshot[n.NodeID]
		if !known {
			// Added, or renamed in place. Either way the inventory changes.
			return Diff{Verdict: VerdictFull}
		}
		if rec.IPAddress != n.IPAddress ||
			rec.Role != n.Role ||
			rec.Username != n.Username ||
			rec.Password != n.Password ||
			rec.BalancingStrategy != n.BalancingStrategy ||
			rec.Position != i {
			return Diff{Verdict: VerdictFull}
		}
		if !sameFunctions(rec.Functions, n.Functions) {
			functionsChanged = true
		}
	}

	if functionsChanged {
		return Diff{Verdict: VerdictFunctions}
	}
	return Diff{Verdict: VerdictNone}
}

// sameFunctions compares two function lists ignoring order: the playbook loops
// over them with faas_deploy, so a reordered list describes the same node.
func sameFunctions(a, b []dfaasv1.Function) bool {
	if len(a) != len(b) {
		return false
	}
	// append to a nil slice keeps both sides nil when empty, so DeepEqual does
	// not trip over []Function{} vs nil.
	sortedA := append([]dfaasv1.Function(nil), a...)
	sortedB := append([]dfaasv1.Function(nil), b...)
	sort.Slice(sortedA, func(i, j int) bool { return sortedA[i].Name < sortedA[j].Name })
	sort.Slice(sortedB, func(i, j int) bool { return sortedB[i].Name < sortedB[j].Name })
	return reflect.DeepEqual(sortedA, sortedB)
}

// SnapshotSecretName is the per-Environment Secret holding the provisioning
// snapshot. Derived from the Environment name, exactly like the libp2p key
// Secret, so it is always fetched by name and never listed.
func SnapshotSecretName(env *dfaasv1.Environment) string {
	return env.Name + "-provisioned-nodes"
}

func snapshotSecretKey(env *dfaasv1.Environment) client.ObjectKey {
	return client.ObjectKey{Name: SnapshotSecretName(env), Namespace: env.Namespace}
}

// LoadSnapshot returns the last recorded provisioning snapshot, keyed by nodeID.
//
// found=false means NO snapshot exists at all -- a first ever run, or an
// Environment provisioned by an operator predating this Secret. That is NOT the
// same as an empty snapshot and must never be read as "every node was removed";
// Classify short-circuits on it. A read error is returned as an error and must
// not be collapsed into found=false, or a transient API failure disguises
// itself as a missing snapshot.
func (m *Manager) LoadSnapshot(ctx context.Context, env *dfaasv1.Environment) (
	map[string]ProvisionedNode, bool, error) {

	var secret corev1.Secret
	if err := m.Get(ctx, snapshotSecretKey(env), &secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get provisioning snapshot secret: %w", err)
	}

	out := make(map[string]ProvisionedNode, len(secret.Data))
	for nodeID, raw := range secret.Data {
		var rec ProvisionedNode
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, false, fmt.Errorf("decode snapshot record for node %q: %w", nodeID, err)
		}
		out[nodeID] = rec
	}
	return out, true, nil
}

// SaveSnapshot records env.Spec.Nodes as the state now installed on the
// machines. Call it only once a provisioning run has driven every stream to
// success: the invariant Classify leans on is that a snapshot exists if and
// only if every node it records completed its role's playbook.
//
// Replace, not merge. The snapshot is whole state; merging would resurrect
// records for machines already decommissioned, and those would then reappear in
// every later Removed set.
func (m *Manager) SaveSnapshot(ctx context.Context, env *dfaasv1.Environment) error {
	data, err := snapshotData(env)
	if err != nil {
		return err
	}
	return m.writeSnapshot(ctx, env, data, false)
}

// SaveSnapshotIfAbsent writes the snapshot only when none exists: the upgrade
// seed. An Environment settled on its current spec is, by the FSM's own
// invariant, installed exactly as spec.nodes describes, so recording it is
// safe -- and without it the FIRST node removal after an operator upgrade has
// nothing to diff against and silently leaves a live machine in the federation.
func (m *Manager) SaveSnapshotIfAbsent(ctx context.Context, env *dfaasv1.Environment) error {
	data, err := snapshotData(env)
	if err != nil {
		return err
	}
	return m.writeSnapshot(ctx, env, data, true)
}

func snapshotData(env *dfaasv1.Environment) (map[string][]byte, error) {
	data := make(map[string][]byte, len(env.Spec.Nodes))
	for i, n := range env.Spec.Nodes {
		raw, err := json.Marshal(ProvisionedNode{
			NodeID:            n.NodeID,
			IPAddress:         n.IPAddress,
			Role:              n.Role,
			Username:          n.Username,
			Password:          n.Password,
			Position:          i,
			BalancingStrategy: n.BalancingStrategy,
			Functions:         n.Functions,
		})
		if err != nil {
			return nil, fmt.Errorf("encode snapshot record for node %q: %w", n.NodeID, err)
		}
		data[n.NodeID] = raw
	}
	return data, nil
}

// writeSnapshot creates or replaces the snapshot Secret. onlyIfAbsent leaves an
// existing Secret untouched.
func (m *Manager) writeSnapshot(ctx context.Context, env *dfaasv1.Environment,
	data map[string][]byte, onlyIfAbsent bool) error {

	key := snapshotSecretKey(env)
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace},
		Type:       corev1.SecretTypeOpaque,
		Data:       data,
	}
	if err := ctrl.SetControllerReference(env, &secret, m.Scheme); err != nil {
		return fmt.Errorf("set controller ref on provisioning snapshot secret: %w", err)
	}

	err := m.Create(ctx, &secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create provisioning snapshot secret: %w", err)
	}
	if onlyIfAbsent {
		return nil
	}

	if uerr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Secret{}
		if gerr := m.Get(ctx, key, latest); gerr != nil {
			return gerr
		}
		latest.Data = data
		return m.Update(ctx, latest)
	}); uerr != nil {
		return fmt.Errorf("update provisioning snapshot secret: %w", uerr)
	}
	return nil
}
