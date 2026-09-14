/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"sort"
	"testing"

	dfaasv1 "dfaas-operator/api/v1"
)

// --- fixtures -------------------------------------------------------------

func specNode(nodeID, ip string, role dfaasv1.NodeRole) dfaasv1.EnvironmentNode {
	return dfaasv1.EnvironmentNode{
		NodeID:            nodeID,
		IPAddress:         ip,
		Role:              role,
		Capacity:          dfaasv1.NodeCapacity("LOW"),
		Username:          "ubuntu",
		Password:          "pw",
		BalancingStrategy: dfaasv1.RecalcStrategy,
	}
}

func fn(name string, maxRate int32) dfaasv1.Function {
	return dfaasv1.Function{
		Name: name, Image: "ghcr.io/x/" + name + ":latest",
		ExecTimeout: 5, MaxInflight: 400, TimeoutMs: 6000, MaxRate: maxRate,
	}
}

// baseSpec is the reference two-worker + one-generator environment every case
// mutates. Returned fresh each call so a case cannot leak into the next.
func baseSpec() []dfaasv1.EnvironmentNode {
	w0 := specNode("w0", "100.64.0.10", dfaasv1.RoleDfaasWorker)
	w0.Functions = []dfaasv1.Function{fn("imgproc", 100)}
	w1 := specNode("w1", "100.64.0.11", dfaasv1.RoleDfaasWorker)
	w1.Functions = []dfaasv1.Function{fn("figlet", 50)}
	g0 := specNode("g0", "100.64.0.20", dfaasv1.RoleK6LoadGenerator)
	return []dfaasv1.EnvironmentNode{w0, w1, g0}
}

// snapshotOf is the "last run installed exactly this spec" starting point.
func snapshotOf(nodes []dfaasv1.EnvironmentNode) map[string]ProvisionedNode {
	out := make(map[string]ProvisionedNode, len(nodes))
	for i, n := range nodes {
		out[n.NodeID] = ProvisionedNode{
			NodeID: n.NodeID, IPAddress: n.IPAddress, Role: n.Role,
			Username: n.Username, Password: n.Password, Position: i,
			BalancingStrategy: n.BalancingStrategy, Functions: n.Functions,
		}
	}
	return out
}

func removedIPs(d Diff) []string {
	out := make([]string, 0, len(d.Removed))
	for _, r := range d.Removed {
		out = append(out, r.IPAddress)
	}
	sort.Strings(out)
	return out
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- the table ------------------------------------------------------------

// Classify decides how much of the provisioning run a spec edit actually
// requires, and which machines the edit orphaned. Both answers are destructive
// when wrong: too narrow a verdict silently fails to apply a change, and a
// wrong Removed set runs k3s-uninstall.sh on a live machine.
func TestClassify(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(spec []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode
		wantVerdict Verdict
		wantRemoved []string // IP addresses, sorted; nil means none
	}{
		{
			name:        "unchanged spec needs no playbook run",
			mutate:      func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode { return s },
			wantVerdict: VerdictNone,
		},
		{
			name: "function maxRate edited",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].Functions = []dfaasv1.Function{fn("imgproc", 250)}
				return s
			},
			wantVerdict: VerdictFunctions,
		},
		{
			name: "function added",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].Functions = append(s[0].Functions, fn("figlet", 100))
				return s
			},
			wantVerdict: VerdictFunctions,
		},
		{
			name: "function removed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].Functions = nil
				return s
			},
			wantVerdict: VerdictFunctions,
		},
		{
			name: "function image changed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				f := fn("imgproc", 100)
				f.Image = "ghcr.io/x/imgproc:v2"
				s[0].Functions = []dfaasv1.Function{f}
				return s
			},
			wantVerdict: VerdictFunctions,
		},
		{
			name: "node added",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				return append(s, specNode("w2", "100.64.0.12", dfaasv1.RoleDfaasWorker))
			},
			wantVerdict: VerdictFull,
		},
		{
			name: "worker removed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				return []dfaasv1.EnvironmentNode{s[0], s[2]} // drop w1
			},
			wantVerdict: VerdictFull,
			wantRemoved: []string{"100.64.0.11"},
		},
		{
			name: "k6 generator removed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				return []dfaasv1.EnvironmentNode{s[0], s[1]} // drop g0
			},
			wantVerdict: VerdictFull,
			wantRemoved: []string{"100.64.0.20"},
		},
		{
			name: "two nodes removed at once",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				return []dfaasv1.EnvironmentNode{s[0]}
			},
			wantVerdict: VerdictFull,
			wantRemoved: []string{"100.64.0.11", "100.64.0.20"},
		},
		{
			// The machine moved: the old box is orphaned even though its nodeID
			// still exists in spec.
			name: "ipAddress moved on a kept nodeID orphans the old machine",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[1].IPAddress = "100.64.0.99"
				return s
			},
			wantVerdict: VerdictFull,
			wantRemoved: []string{"100.64.0.11"},
		},
		{
			// THE TRAP. A nodeID-keyed diff would report w1 as removed and wipe
			// a machine the spec still uses. Identity is the IP, not the label.
			name: "nodeID renamed at the same ipAddress removes nothing",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[1].NodeID = "worker-one"
				return s
			},
			wantVerdict: VerdictFull,
			wantRemoved: nil,
		},
		{
			name: "username changed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].Username = "root"
				return s
			},
			wantVerdict: VerdictFull,
		},
		{
			name: "password changed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].Password = "hunter2"
				return s
			},
			wantVerdict: VerdictFull,
		},
		{
			name: "role flipped",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[1].Role = dfaasv1.RoleK6LoadGenerator
				s[1].Functions = nil
				return s
			},
			wantVerdict: VerdictFull,
		},
		{
			// Reaches only the agent Helm task, which carries no `functions`
			// tag: classifying it as function-only would never apply it.
			name: "balancingStrategy changed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].BalancingStrategy = dfaasv1.AllLocalStrategy
				return s
			},
			wantVerdict: VerdictFull,
		},
		{
			// capacity reaches no playbook: its only consumer is
			// monitoring.ReconcileTargets, which re-runs every pass anyway.
			name: "capacity alone needs no playbook run",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				s[0].Capacity = dfaasv1.NodeCapacity("HIGH")
				return s
			},
			wantVerdict: VerdictNone,
		},
		{
			// buildInventory derives the bootstrap list positionally: node 0 is
			// the seed with is_bootstrap=false. Reordering moves the seed.
			name: "worker order swapped moves the bootstrap seed",
			mutate: func(s []dfaasv1.EnvironmentNode) []dfaasv1.EnvironmentNode {
				return []dfaasv1.EnvironmentNode{s[1], s[0], s[2]}
			},
			wantVerdict: VerdictFull,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := snapshotOf(baseSpec())
			got := Classify(snap, true, tc.mutate(baseSpec()))

			if got.Verdict != tc.wantVerdict {
				t.Errorf("verdict = %v, want %v", got.Verdict, tc.wantVerdict)
			}
			want := tc.wantRemoved
			if want == nil {
				want = []string{}
			}
			if gotIPs := removedIPs(got); !eqStrings(gotIPs, want) {
				t.Errorf("removed IPs = %v, want %v", gotIPs, want)
			}
		})
	}
}

// faas_deploy loops over the list, so two specs holding the same functions in a
// different order describe the same node and must not re-run anything.
func TestClassifyFunctionOrderIsIrrelevant(t *testing.T) {
	spec := baseSpec()
	spec[0].Functions = []dfaasv1.Function{fn("imgproc", 100), fn("figlet", 50)}
	snap := snapshotOf(spec)

	reordered := baseSpec()
	reordered[0].Functions = []dfaasv1.Function{fn("figlet", 50), fn("imgproc", 100)}

	if got := Classify(snap, true, reordered); got.Verdict != VerdictNone {
		t.Errorf("verdict = %v, want VerdictNone: reordering a function list changes nothing", got.Verdict)
	}
}

// The migration guard, standalone so it cannot be diluted into the table.
//
// An Environment provisioned by an operator predating the snapshot Secret has
// no record at all. Reading that as "every node was removed" would run
// k3s-uninstall.sh across the whole federation on the first reconcile after an
// upgrade. found=false must mean "I don't know", never "nothing is installed".
func TestClassifyWithoutSnapshotNeverRemoves(t *testing.T) {
	got := Classify(nil, false, baseSpec())

	if got.Verdict != VerdictFull {
		t.Errorf("verdict = %v, want VerdictFull: an unknown previous state must re-run everything", got.Verdict)
	}
	if len(got.Removed) != 0 {
		t.Fatalf("removed = %v, want empty: a missing snapshot must NEVER decommission a machine", removedIPs(got))
	}
}

// An empty-but-present snapshot is a different statement from a missing one: it
// says the last run installed nothing, so nothing can be orphaned.
func TestClassifyEmptySnapshotRemovesNothing(t *testing.T) {
	got := Classify(map[string]ProvisionedNode{}, true, baseSpec())

	if got.Verdict != VerdictFull {
		t.Errorf("verdict = %v, want VerdictFull: every node in spec is new", got.Verdict)
	}
	if len(got.Removed) != 0 {
		t.Errorf("removed = %v, want empty", removedIPs(got))
	}
}
