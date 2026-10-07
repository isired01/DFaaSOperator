/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package fake is the in-memory reach adapter for tests. A test declares which
// nodeIDs are down and the probe answers from that set, with no network and no
// wall clock; every probe round is recorded so a test can assert how many times
// the reconciler actually probed.
package fake

import (
	"context"
	"sync"

	dfaasv1 "dfaas-operator/api/v1"
)

// Nodes implements reach.Prober.
type Nodes struct {
	mu sync.Mutex
	// Down is the set of nodeIDs that do not answer. Set it directly, and
	// change it between reconciles to bring a Node back.
	Down []string
	// rounds records the nodeIDs probed on each call, in order.
	rounds [][]string
}

// Unreachable returns the declared-down nodeIDs that this Environment actually
// declares, in spec order -- the same ordering guarantee reach.TCP gives.
func (n *Nodes) Unreachable(_ context.Context, env *dfaasv1.Environment) []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	down := make(map[string]struct{}, len(n.Down))
	for _, id := range n.Down {
		down[id] = struct{}{}
	}

	var probed, unreachable []string
	for _, node := range env.Spec.Nodes {
		probed = append(probed, node.NodeID)
		if _, ok := down[node.NodeID]; ok {
			unreachable = append(unreachable, node.NodeID)
		}
	}
	n.rounds = append(n.rounds, probed)
	return unreachable
}

// Rounds returns the nodeIDs probed on each round so far.
func (n *Nodes) Rounds() [][]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([][]string, len(n.rounds))
	copy(out, n.rounds)
	return out
}
