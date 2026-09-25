/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package k6dispatch

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	dfaasv1 "dfaas-operator/api/v1"
)

func runnerEnvOf(t *testing.T, obj map[string]interface{}) map[string]string {
	t.Helper()
	items, found, err := unstructured.NestedSlice(obj, "spec", "runner", "env")
	if err != nil || !found {
		t.Fatalf("spec.runner.env missing: found=%v err=%v", found, err)
	}
	out := map[string]string{}
	for _, it := range items {
		m := it.(map[string]interface{})
		out[m["name"].(string)] = m["value"].(string)
	}
	return out
}

// The adapter injects exactly what the reconciler decided: the summary URL
// always (empty means "no upload"), the sync URL only when set.
func TestBuildTestRunInjectsRunnerEnv(t *testing.T) {
	lt := &dfaasv1.LoadTest{ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default"}}
	perNode := dfaasv1.PerNodeLoad{NodeID: "node-1", ScriptConfigMap: corev1.LocalObjectReference{Name: "script"}}

	tr, err := buildTestRun(TestRunName(lt, "node-1"), lt, perNode, RunnerEnv{SummaryURL: "http://x/summary.json"})
	if err != nil {
		t.Fatalf("buildTestRun: %v", err)
	}
	if tr.GetName() != "lt-sample-node-1" || tr.GetNamespace() != remoteNamespace {
		t.Errorf("name/ns = %s/%s", tr.GetName(), tr.GetNamespace())
	}
	env := runnerEnvOf(t, tr.Object)
	if env["DFAAS_SUMMARY_URL"] != "http://x/summary.json" {
		t.Errorf("DFAAS_SUMMARY_URL = %q", env["DFAAS_SUMMARY_URL"])
	}
	if _, ok := env["DFAAS_SYNC_URL"]; ok {
		t.Error("DFAAS_SYNC_URL injected without a sync URL")
	}

	tr, _ = buildTestRun(TestRunName(lt, "node-1"), lt, perNode, RunnerEnv{SummaryURL: "", SyncURL: "http://x/go"})
	env = runnerEnvOf(t, tr.Object)
	if v, ok := env["DFAAS_SUMMARY_URL"]; !ok || v != "" {
		t.Errorf("empty summary URL must still be injected, got ok=%v v=%q", ok, v)
	}
	if env["DFAAS_SYNC_URL"] != "http://x/go" {
		t.Errorf("DFAAS_SYNC_URL = %q", env["DFAAS_SYNC_URL"])
	}
}

// The single usability policy: unknown node and missing kubeconfig are both
// errors; a usable node resolves to the Secret in the ENVIRONMENT's namespace.
func TestResolveKubeconfigPolicy(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "envns"},
		Status: dfaasv1.EnvironmentStatus{K6Nodes: []dfaasv1.K6NodeStatus{
			{NodeID: "ok", KubeconfigSecret: "env-ok-kubeconfig"},
			{NodeID: "nosecret", KubeconfigSecret: ""},
		}},
	}
	ref, err := ResolveKubeconfig(env, "ok")
	if err != nil || ref.Name != "env-ok-kubeconfig" || ref.Namespace != "envns" {
		t.Errorf("ok node: ref=%v err=%v", ref, err)
	}
	if _, err := ResolveKubeconfig(env, "nosecret"); err == nil {
		t.Error("node without kubeconfig resolved")
	}
	if _, err := ResolveKubeconfig(env, "gone"); err == nil {
		t.Error("node absent from environment resolved")
	}
}

func TestSanitize(t *testing.T) {
	if got := Sanitize("Gen_A.1"); got != "gen-a-1" {
		t.Errorf("Sanitize = %q", got)
	}
}

// The remote script copy carries the UID of the LoadTest that mirrored it, so
// DeleteScript can tell its own copy from one a later test re-mirrored under
// the same name.
func TestMirrorConfigMapLabelsTheOwningLoadTest(t *testing.T) {
	l := lt("lt", []string{"gen-a"}, nil)
	l.UID = "uid-1"
	src := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "script", Namespace: "default"},
		Data: map[string]string{"script.js": "x"}}
	got := mirrorConfigMap(l, src)
	if got.Labels[ScriptOwnerLabel] != "uid-1" || got.Name != "script" || got.Data["script.js"] != "x" {
		t.Errorf("mirror = %+v", got)
	}
}
