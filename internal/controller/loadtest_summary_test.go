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
