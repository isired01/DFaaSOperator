/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
)

// The provisioning run records the generation it installs. Before, the
// settle stamped observedGeneration from the object it re-fetched, so an edit
// that landed after the Ansible Jobs ran (ProvisioningMonitoring) was marked
// installed and never reached Ansible, and drift was acted on only from a
// settled phase.

func generationEnv(phase dfaasv1.EnvironmentPhase, generation, provisioning int64) *dfaasv1.Environment {
	env := faninEnv(true)
	env.Generation = generation
	env.Status.Phase = phase
	env.Status.ProvisioningGeneration = provisioning
	return env
}

func storedEnv(t *testing.T, c client.Client, env *dfaasv1.Environment) *dfaasv1.Environment {
	t.Helper()
	var got dfaasv1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

func TestSettleStampsTheGenerationTheRunApplied(t *testing.T) {
	s := faninScheme(t)
	env := generationEnv(dfaasv1.EnvProvisioningMonitoring, 2, 1) // edited to 2 while run 1 was finishing
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s}

	if _, err := r.phase(context.Background(), env, dfaasv1.EnvReady); err != nil {
		t.Fatal(err)
	}
	if got := storedEnv(t, c, env).Status.ObservedGeneration; got != 1 {
		t.Errorf("observedGeneration = %d, want 1 (the generation the run applied)", got)
	}
}

func TestSettleWithoutARecordFallsBackToTheGeneration(t *testing.T) {
	s := faninScheme(t)
	env := generationEnv(dfaasv1.EnvProvisioningMonitoring, 4, 0) // written by an older operator
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s}

	if _, err := r.phase(context.Background(), env, dfaasv1.EnvReady); err != nil {
		t.Fatal(err)
	}
	if got := storedEnv(t, c, env).Status.ObservedGeneration; got != 4 {
		t.Errorf("observedGeneration = %d, want 4 (fallback, never 0)", got)
	}
}

func TestSpecEditRestartsProvisioningInEveryPhase(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		phase                  dfaasv1.EnvironmentPhase
		observed, provisioning int64
		wantRestart            bool
	}{
		{name: "ProvisioningVMs", phase: dfaasv1.EnvProvisioningVMs, provisioning: 1, wantRestart: true},
		{name: "ProvisioningInfra", phase: dfaasv1.EnvProvisioningInfra, provisioning: 1, wantRestart: true},
		{name: "ProvisioningMonitoring", phase: dfaasv1.EnvProvisioningMonitoring, provisioning: 1, wantRestart: true},
		{name: "Unreachable", phase: dfaasv1.EnvUnreachable, observed: 1, provisioning: 1, wantRestart: true},
		{name: "Ready", phase: dfaasv1.EnvReady, observed: 1, provisioning: 1, wantRestart: true},
		{name: "Failed", phase: dfaasv1.EnvFailed, observed: 1, provisioning: 1, wantRestart: true},
		{name: "no edit mid-run", phase: dfaasv1.EnvProvisioningInfra, provisioning: 2, wantRestart: false},
		{name: "no record (older operator)", phase: dfaasv1.EnvProvisioningInfra, provisioning: 0, wantRestart: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := faninScheme(t)
			env := generationEnv(tc.phase, 2, tc.provisioning)
			env.Status.ObservedGeneration = tc.observed
			c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env).Build()
			r := &EnvironmentReconciler{Client: c, Scheme: s}

			handled, _, err := r.handleGenerationDrift(context.Background(), env)
			if err != nil {
				t.Fatal(err)
			}
			if handled != tc.wantRestart {
				t.Fatalf("restarted = %v, want %v", handled, tc.wantRestart)
			}
			if !tc.wantRestart {
				return
			}
			got := storedEnv(t, c, env)
			if got.Status.Phase != dfaasv1.EnvProvisioningVMs {
				t.Errorf("phase = %q, want ProvisioningVMs", got.Status.Phase)
			}
			if got.Status.ProvisioningGeneration != 2 {
				t.Errorf("provisioningGeneration = %d, want 2 (the new run records it)", got.Status.ProvisioningGeneration)
			}
			if c := meta.FindStatusCondition(got.Status.Conditions, dfaasv1.EnvCondNodesReachable); c == nil ||
				c.Reason != dfaasv1.EnvReasonUpdating {
				t.Errorf("NodesReachable = %+v, want reset with reason Updating", c)
			}
		})
	}
}

func TestProvisioningInfraWaitsForOlderGenerationJobs(t *testing.T) {
	s := faninScheme(t)
	env := generationEnv(dfaasv1.EnvProvisioningInfra, 2, 2)
	old := env.DeepCopy()
	old.Generation = 1
	oldJob := ansibleJob(old, "vms", jobRunning)
	oldJob.Labels = map[string]string{ansible.LabelEnvironment: env.Name, ansible.LabelGeneration: "1"}
	oldSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Name: "bari-old-inventory", Namespace: env.Namespace,
		Labels: map[string]string{ansible.LabelEnvironment: env.Name, ansible.LabelGeneration: "1"},
	}}
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).
		WithObjects(env, oldJob, oldSecret).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s}

	res, err := r.reconcileProvisioningInfra(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter != 10*time.Second {
		t.Errorf("RequeueAfter = %v, want 10s while an older-generation Job runs", res.RequeueAfter)
	}
	var job batchv1.Job
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: env.Namespace,
		Name: ansible.JobNameForRole(env, "vms")}, &job); !apierrors.IsNotFound(err) {
		t.Errorf("generation-2 Job created while the generation-1 one still ran (err=%v)", err)
	}
	cond := meta.FindStatusCondition(storedEnv(t, c, env).Status.Conditions, dfaasv1.EnvCondDFaaSNodesReady)
	if cond == nil || cond.Reason != dfaasv1.EnvReasonJobPending || !strings.Contains(cond.Message, oldJob.Name) {
		t.Errorf("DFaaSNodesReady = %+v, want JobPending naming %s", cond, oldJob.Name)
	}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(oldSecret), &corev1.Secret{}); !apierrors.IsNotFound(err) {
		t.Errorf("older-generation inventory Secret (credentials) not deleted (err=%v)", err)
	}
}
