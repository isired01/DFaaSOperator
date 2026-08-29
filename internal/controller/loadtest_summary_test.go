/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
)

func summaryTestLT(syncStart bool) *dfaasv1.LoadTest {
	return &dfaasv1.LoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default"},
		Spec:       dfaasv1.LoadTestSpec{SyncStart: syncStart},
	}
}

func TestSummaryPath(t *testing.T) {
	lt := summaryTestLT(false)
	cases := []struct {
		nodeID string
		want   string
	}{
		{"node-1", "/dfaas-k6-summary/default/lt-sample/node-1.json"},
		// sanitize: lowercase, non-[a-z0-9-] → "-"
		{"Node_1", "/dfaas-k6-summary/default/lt-sample/node-1.json"},
	}
	for _, c := range cases {
		if got := summaryPath(lt, c.nodeID); got != c.want {
			t.Errorf("summaryPath(%q) = %q, want %q", c.nodeID, got, c.want)
		}
	}
}

func TestSummaryURL(t *testing.T) {
	lt := summaryTestLT(false)

	t.Run("override", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "http://lab.example:30901")
		t.Setenv("HOST_IP", "")
		want := "http://lab.example:30901/dfaas-k6-summary/default/lt-sample/node-1.json"
		if got := summaryURL(lt, "node-1"); got != want {
			t.Errorf("summaryURL = %q, want %q", got, want)
		}
	})
	t.Run("host ip fallback", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "")
		t.Setenv("HOST_IP", "192.0.2.10")
		want := "http://192.0.2.10:30901/dfaas-k6-summary/default/lt-sample/node-1.json"
		if got := summaryURL(lt, "node-1"); got != want {
			t.Errorf("summaryURL = %q, want %q", got, want)
		}
	})
	t.Run("unresolvable is empty", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "")
		t.Setenv("HOST_IP", "")
		if got := summaryURL(lt, "node-1"); got != "" {
			t.Errorf("summaryURL = %q, want empty", got)
		}
	})
}

// testRunEnv extracts the runner env entries of a built TestRun as name→value.
func testRunEnv(t *testing.T, tr map[string]interface{}) map[string]string {
	t.Helper()
	spec, _ := tr["spec"].(map[string]interface{})
	runner, _ := spec["runner"].(map[string]interface{})
	envList, _ := runner["env"].([]interface{})
	out := map[string]string{}
	for _, e := range envList {
		m, _ := e.(map[string]interface{})
		out[m["name"].(string)] = m["value"].(string)
	}
	return out
}

func TestBuildRemoteTestRunEnv(t *testing.T) {
	t.Setenv("DFAAS_SYNC_PUBLIC_URL", "http://lab.example:30901")
	t.Setenv("HOST_IP", "")
	perNode := dfaasv1.PerNodeLoad{NodeID: "node-1"}

	t.Run("summary URL always injected", func(t *testing.T) {
		tr := buildRemoteTestRun("lt-sample-node-1", summaryTestLT(false), perNode)
		env := testRunEnv(t, tr.Object)
		if env["DFAAS_SUMMARY_URL"] != "http://lab.example:30901/dfaas-k6-summary/default/lt-sample/node-1.json" {
			t.Errorf("DFAAS_SUMMARY_URL = %q", env["DFAAS_SUMMARY_URL"])
		}
		if _, ok := env["DFAAS_SYNC_URL"]; ok {
			t.Error("DFAAS_SYNC_URL injected without syncStart")
		}
	})
	t.Run("sync URL added with syncStart", func(t *testing.T) {
		tr := buildRemoteTestRun("lt-sample-node-1", summaryTestLT(true), perNode)
		env := testRunEnv(t, tr.Object)
		if _, ok := env["DFAAS_SUMMARY_URL"]; !ok {
			t.Error("DFAAS_SUMMARY_URL missing with syncStart")
		}
		if env["DFAAS_SYNC_URL"] != "http://lab.example:30901/dfaas-sync/default/lt-sample.go" {
			t.Errorf("DFAAS_SYNC_URL = %q", env["DFAAS_SYNC_URL"])
		}
	})
}
