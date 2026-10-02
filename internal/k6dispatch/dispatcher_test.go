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
	// No management address was detected: the script keeps the URL the
	// gateway baked in, so there is no asset base to inject.
	if _, ok := env["DFAAS_ASSET_BASE"]; ok {
		t.Error("DFAAS_ASSET_BASE injected without an asset base")
	}

	tr, _ = buildTestRun(TestRunName(lt, "node-1"), lt, perNode,
		RunnerEnv{SummaryURL: "http://x/summary.json", AssetBase: "http://100.64.0.11:30900"})
	env = runnerEnvOf(t, tr.Object)
	if env["DFAAS_ASSET_BASE"] != "http://100.64.0.11:30900" {
		t.Errorf("DFAAS_ASSET_BASE = %q", env["DFAAS_ASSET_BASE"])
	}
}

// Only a bare IP literal a remote generator can dial back is a management
// address; anything else falls back rather than ending up in a runner URL.
func TestParseManagementAddress(t *testing.T) {
	cases := []struct {
		in     string
		want   string
		wantOK bool
	}{
		{"100.64.0.11", "100.64.0.11", true},
		{"  192.168.252.63\n", "192.168.252.63", true},
		{"fd7a:115c:a1e0::11", "fd7a:115c:a1e0::11", true},
		{"FD7A:115C:A1E0:0:0:0:0:11", "fd7a:115c:a1e0::11", true}, // canonical spelling
		{"::ffff:100.64.0.11", "100.64.0.11", true},               // v4-mapped, unmapped
		{"", "", false},
		{"   ", "", false},
		{"fe80::1%eth0", "", false},         // zone: the runner's URL parser rejects it
		{"100.64.0.11:30901", "", false},    // port
		{"[fd7a:115c:a1e0::11]", "", false}, // brackets
		{"[fd7a::11]:30901", "", false},
		{"0.0.0.0", "", false}, // unspecified
		{"::", "", false},
		{"127.0.0.1", "", false}, // loopback
		{"::1", "", false},
		{"::ffff:127.0.0.1", "", false}, // loopback once unmapped
		{"224.0.0.1", "", false},        // multicast
		{"ff02::1", "", false},
		{"169.254.10.1", "", false}, // link-local
		{"fe80::1", "", false},
		{"mgmt.lab.example", "", false}, // a name, not an address
		{"100.64.0.256", "", false},
		{"not-an-ip", "", false},
	}
	for _, c := range cases {
		got, ok := ParseManagementAddress(c.in)
		if got != c.want || ok != c.wantOK {
			t.Errorf("ParseManagementAddress(%q) = (%q, %v), want (%q, %v)", c.in, got, ok, c.want, c.wantOK)
		}
	}
}

// GeneratorOf reads the address off status through the validator: a node
// that is absent, has none, or has an unusable one gets "" and so falls back.
func TestGeneratorOf(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "env", Namespace: "envns"},
		Status: dfaasv1.EnvironmentStatus{K6Nodes: []dfaasv1.K6NodeStatus{
			{NodeID: "v4", KubeconfigSecret: "env-v4-kubeconfig", ManagementAddress: "100.64.0.11"},
			{NodeID: "v6", KubeconfigSecret: "env-v6-kubeconfig", ManagementAddress: "fd7a:115c:a1e0::11"},
			{NodeID: "mapped", KubeconfigSecret: "env-mapped-kubeconfig", ManagementAddress: "::ffff:100.64.0.12"},
			{NodeID: "none", KubeconfigSecret: "env-none-kubeconfig"},
			{NodeID: "port", KubeconfigSecret: "env-port-kubeconfig", ManagementAddress: "100.64.0.11:30901"},
			{NodeID: "zone", KubeconfigSecret: "env-zone-kubeconfig", ManagementAddress: "fe80::1%eth0"},
			{NodeID: "garbage", KubeconfigSecret: "env-garbage-kubeconfig", ManagementAddress: "n/a"},
		}},
	}
	cases := []struct {
		nodeID string
		want   string
	}{
		{"v4", "100.64.0.11"},
		{"v6", "fd7a:115c:a1e0::11"},
		{"mapped", "100.64.0.12"},
		{"none", ""},
		{"port", ""},
		{"zone", ""},
		{"garbage", ""},
		{"gone", ""},
	}
	for _, c := range cases {
		g := GeneratorOf(env, c.nodeID)
		if g.NodeID != c.nodeID || g.MgmtAddr != c.want {
			t.Errorf("GeneratorOf(%q) = %+v, want {NodeID:%s MgmtAddr:%s}", c.nodeID, g, c.nodeID, c.want)
		}
	}
	// An empty address is no usability fault: the node still resolves.
	if _, err := ResolveKubeconfig(env, "none"); err != nil {
		t.Errorf("a node with no management address must still resolve: %v", err)
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
