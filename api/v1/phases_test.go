/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import "testing"

// These three predicates were raw comparisons re-typed inside one 170-line
// Reconcile: Dispatchable at four sites (three of them negated) plus once more
// in the gateway, the pre-execution window at five, and terminal at two -- the
// second an approximation of the first. Naming them is only half the fix; the
// other half is a table that says what the sets are. The phase list is read
// from the +kubebuilder:validation:Enum marker (phasesOf), and every table
// needs an explicit row per phase, so adding a phase to the enum without
// deciding which set it belongs to fails here.

// classify checks pred against table over every phase, and fails on a phase
// the table does not classify.
func classify[P ~string](t *testing.T, pred string, table map[P]bool, all []P, got func(P) bool) {
	t.Helper()
	if len(table) != len(all) {
		t.Errorf("%s: %d rows for %d phases", pred, len(table), len(all))
	}
	for _, p := range all {
		want, ok := table[p]
		if !ok {
			t.Errorf("%q is declared by the Enum marker but not classified for %s", p, pred)
			continue
		}
		if got(p) != want {
			t.Errorf("%s(%q) = %v, want %v", pred, p, got(p), want)
		}
	}
}

func TestEnvironmentPhaseDispatchable(t *testing.T) {
	// Ready only. Degraded (infra up, monitoring down) used to qualify; it was
	// dropped because no reconcile path ever produced it. The gateway mirrors
	// this set, so it must not be looser either.
	dispatchable := map[EnvironmentPhase]bool{
		"": false, EnvProvisioningVMs: false, EnvProvisioningInfra: false,
		EnvProvisioningMonitoring: false, EnvReady: true, EnvFailed: false, EnvUnreachable: false,
	}
	classify(t, "Dispatchable", dispatchable, phasesOf[EnvironmentPhase](t, "EnvironmentPhase"),
		EnvironmentPhase.Dispatchable)

	// Unreachable is explicitly non-terminal and auto-recovering, which makes
	// adding it to this set plausible. It is deliberately NOT in it: a
	// LoadTest dispatched at a machine that stopped answering :22 would fail
	// on the remote apply. Change that decision here, in one place.
	if EnvUnreachable.Dispatchable() {
		t.Error("Unreachable must not be dispatchable — the remote apply would fail")
	}
}

func TestLoadTestPhaseTerminal(t *testing.T) {
	terminal := map[LoadTestPhase]bool{
		"": false, LoadTestPending: false, LoadTestRunning: false, LoadTestExporting: false,
		LoadTestCompleted: true, LoadTestFailed: true, LoadTestAborted: true,
	}
	classify(t, "Terminal", terminal, phasesOf[LoadTestPhase](t, "LoadTestPhase"), LoadTestPhase.Terminal)

	// Exporting is the trap: the k6 run is over, but the metrics export is
	// not, so the reconcile loop must keep running.
	if LoadTestExporting.Terminal() {
		t.Error("Exporting is not terminal — the exporter Job has not finished")
	}
}

func TestLoadTestPhasePreExecution(t *testing.T) {
	// The unreconciled empty phase counts: it is the create-time window, and
	// the reconciler's gates key on it.
	preExecution := map[LoadTestPhase]bool{
		"": true, LoadTestPending: true, LoadTestRunning: false, LoadTestExporting: false,
		LoadTestCompleted: false, LoadTestFailed: false, LoadTestAborted: false,
	}
	classify(t, "PreExecution", preExecution, phasesOf[LoadTestPhase](t, "LoadTestPhase"), LoadTestPhase.PreExecution)

	// Once execution begins the state machine is immutable: spec.suspended is
	// no longer read, which is what the run-once guard depends on.
	if LoadTestRunning.PreExecution() {
		t.Error("Running is past the pre-execution window — the run-once guard depends on it")
	}
}

// The two LoadTest sets must not overlap: a phase cannot be both "not started"
// and "finished for good".
func TestLoadTestPhaseSetsAreDisjoint(t *testing.T) {
	for _, p := range phasesOf[LoadTestPhase](t, "LoadTestPhase") {
		if p.PreExecution() && p.Terminal() {
			t.Errorf("LoadTestPhase(%q) is both pre-execution and terminal", p)
		}
	}
}
