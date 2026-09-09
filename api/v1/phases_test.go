/*
Copyright 2026.

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
// other half is a table that says what the sets are, so adding a phase to the
// enum without deciding which set it belongs to fails here.

func TestEnvironmentPhaseDispatchable(t *testing.T) {
	// Degraded is dispatchable on purpose: it means the monitoring stack is
	// down, which can only fail the metrics export. The operator dispatches
	// against it, so the gateway must not be stricter.
	dispatchable := map[EnvironmentPhase]bool{
		EnvReady:    true,
		EnvDegraded: true,
	}

	// Every phase the enum declares, so a new one has to be classified here.
	all := []EnvironmentPhase{
		"", EnvIdle, EnvProvisioningVMs, EnvProvisioningInfra,
		EnvProvisioningMonitoring, EnvReady, EnvDegraded, EnvFailed, EnvUnreachable,
	}
	for _, p := range all {
		if got, want := p.Dispatchable(), dispatchable[p]; got != want {
			t.Errorf("EnvironmentPhase(%q).Dispatchable() = %v, want %v", p, got, want)
		}
	}

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
		LoadTestCompleted: true,
		LoadTestFailed:    true,
		LoadTestAborted:   true,
	}
	all := []LoadTestPhase{
		"", LoadTestPending, LoadTestRunning, LoadTestExporting,
		LoadTestCompleted, LoadTestFailed, LoadTestAborted,
	}
	for _, p := range all {
		if got, want := p.Terminal(), terminal[p]; got != want {
			t.Errorf("LoadTestPhase(%q).Terminal() = %v, want %v", p, got, want)
		}
	}

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
		"":              true,
		LoadTestPending: true,
	}
	all := []LoadTestPhase{
		"", LoadTestPending, LoadTestRunning, LoadTestExporting,
		LoadTestCompleted, LoadTestFailed, LoadTestAborted,
	}
	for _, p := range all {
		if got, want := p.PreExecution(), preExecution[p]; got != want {
			t.Errorf("LoadTestPhase(%q).PreExecution() = %v, want %v", p, got, want)
		}
	}

	// Once execution begins the state machine is immutable: spec.suspended is
	// no longer read, which is what the run-once guard depends on.
	if LoadTestRunning.PreExecution() {
		t.Error("Running is past the pre-execution window — the run-once guard depends on it")
	}
}

// The two LoadTest sets must not overlap: a phase cannot be both "not started"
// and "finished for good".
func TestLoadTestPhaseSetsAreDisjoint(t *testing.T) {
	all := []LoadTestPhase{
		"", LoadTestPending, LoadTestRunning, LoadTestExporting,
		LoadTestCompleted, LoadTestFailed, LoadTestAborted,
	}
	for _, p := range all {
		if p.PreExecution() && p.Terminal() {
			t.Errorf("LoadTestPhase(%q) is both pre-execution and terminal", p)
		}
	}
}
