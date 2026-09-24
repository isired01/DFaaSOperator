/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package k6dispatch

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	dfaasv1 "dfaas-operator/api/v1"
)

func lt(name string, specNodes, statusNodes []string) *dfaasv1.LoadTest {
	out := &dfaasv1.LoadTest{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	for _, id := range specNodes {
		out.Spec.PerNodeLoad = append(out.Spec.PerNodeLoad, dfaasv1.PerNodeLoad{NodeID: id})
	}
	for _, id := range statusNodes {
		out.Status.TestRuns = append(out.Status.TestRuns, dfaasv1.TestRunRef{
			NodeID: id, Name: TestRunName(out, id),
		})
	}
	return out
}

// The golden name. captureK6Logs writes this ConfigMap and runExporter asks the
// exporter Job to mount it; the projection is Optional, so a mismatch produces
// no error anywhere -- the Job mounts an empty dir, the exporter finds no .log
// files and returns silently, MetricsExported goes True, the LoadTest reports
// Completed, and the per-Generator k6 logs are gone. Both sites now call this
// function, so the compiler holds them together and this test holds the shape.
func TestK6LogConfigMapName(t *testing.T) {
	cases := []struct {
		nodeID string
		want   string
	}{
		{"gen-a", "lt-k6log-gen-a"},
		// The nodeID is spelled the same way everywhere it reaches a
		// Kubernetes object name.
		{"GEN_A", "lt-k6log-gen-a"},
		{"gen.a", "lt-k6log-gen-a"},
	}
	for _, tc := range cases {
		if got := K6LogConfigMap(lt("lt", nil, nil), tc.nodeID); got != tc.want {
			t.Errorf("K6LogConfigMap(%q) = %q, want %q", tc.nodeID, got, tc.want)
		}
	}
}

// The abort sweep and the deletion finalizer's NotFound poll must target the
// identical set. A poll set smaller than the sweep set drops the finalizer
// while a remote TestRun is still live, which is the exact gap the finalizer
// exists to close.
func TestTargetNodeIDsIsTheUnion(t *testing.T) {
	cases := []struct {
		name   string
		spec   []string
		status []string
		want   []string
	}{
		{
			name: "spec and status agree",
			spec: []string{"gen-b", "gen-a"}, status: []string{"gen-a", "gen-b"},
			want: []string{"gen-a", "gen-b"},
		},
		{
			// Partial dispatch: the TestRun exists remotely but the spec entry
			// was removed. The sweep must still reach it.
			name: "a TestRun with no spec entry is still a target",
			spec: nil, status: []string{"gen-a"},
			want: []string{"gen-a"},
		},
		{
			// The reverse race: dispatch was interrupted before status caught
			// up, so a remote TestRun may exist without a status ref.
			name: "a spec entry with no TestRun is still a target",
			spec: []string{"gen-a"}, status: nil,
			want: []string{"gen-a"},
		},
		{
			name: "the union is deduplicated and sorted",
			spec: []string{"gen-c", "gen-a"}, status: []string{"gen-b", "gen-a"},
			want: []string{"gen-a", "gen-b", "gen-c"},
		},
		{
			name: "no nodes at all",
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := TargetNodeIDs(lt("lt", tc.spec, tc.status))
			if len(tc.want) == 0 {
				if len(got) != 0 {
					t.Fatalf("want none, got %v", got)
				}
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// Sorted, so per-node log order does not change between reconciles. Both
// callers used to iterate a Go map.
func TestTargetNodeIDsIsStable(t *testing.T) {
	l := lt("lt", []string{"gen-c", "gen-a", "gen-b"}, []string{"gen-b", "gen-c"})
	first := TargetNodeIDs(l)
	for i := 0; i < 20; i++ {
		if got := TargetNodeIDs(l); !reflect.DeepEqual(got, first) {
			t.Fatalf("order changed between calls: %v then %v", first, got)
		}
	}
}

// k6-operator derives its Job names from the TestRun name, and Kubernetes
// copies a Job's name into the 63-byte job-name pod label. The longest derived
// name is <testrun>-initializer: past 51 bytes its CREATE fails and the TestRun
// polls for an initializer pod forever, so the LoadTest stays Running. The
// gateway caps the LoadTest name at 63 and a nodeID can be 63 too.
func TestTestRunNameFitsK6OperatorJobNames(t *testing.T) {
	// Short names are untouched: every remote call recomputes the name, so a
	// name v3.5.0 dispatched must stay identical.
	if got := TestRunName(lt("lt", nil, nil), "gen-a"); got != "lt-gen-a" {
		t.Errorf("short name = %q, want lt-gen-a unchanged", got)
	}

	ltName := strings.Repeat("l", 62) + "a"
	nodeID := strings.Repeat("n", 62) + "a"
	got := TestRunName(lt(ltName, nil, nil), nodeID)
	if len(got) > 51 {
		t.Errorf("bounded name is %d bytes, want <= 51: %s", len(got), got)
	}
	if errs := validation.IsDNS1123Label(got); len(errs) > 0 {
		t.Errorf("bounded name %q is not a DNS-1123 label: %v", got, errs)
	}
	if n := len(got + "-initializer"); n > 63 {
		t.Errorf("initializer Job name is %d bytes, want <= 63", n)
	}
	if again := TestRunName(lt(ltName, nil, nil), nodeID); again != got {
		t.Errorf("not deterministic: %q then %q", got, again)
	}

	// The uniqueness is in the part that gets cut: generated LoadTest names
	// differ only in their trailing nonce, and nodeIDs in their tail.
	otherLT := strings.Repeat("l", 62) + "b"
	if TestRunName(lt(otherLT, nil, nil), nodeID) == got {
		t.Error("LoadTest names differing only at the end collide")
	}
	otherNode := strings.Repeat("n", 62) + "b"
	if TestRunName(lt(ltName, nil, nil), otherNode) == got {
		t.Error("nodeIDs differing only at the end collide")
	}
}

// maxTestRunName is k6-operator's longest derived suffix, read off the version
// the k6 playbook installs. A chart bump can change that suffix.
func TestK6ChartVersionIsPinned(t *testing.T) {
	raw, err := os.ReadFile("../controller/ansible/templates/setup-k6-nodes.yml")
	if err != nil {
		t.Fatalf("read k6 playbook: %v", err)
	}
	m := regexp.MustCompile(`k6_chart_version:\s*"([^"]+)"`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("k6_chart_version not found in setup-k6-nodes.yml")
	}
	if got := string(m[1]); got != "3.7.0" {
		t.Errorf("k6_chart_version = %q, want 3.7.0: recheck k6-operator's longest derived "+
			"name suffix (maxTestRunName in dispatcher.go) before changing the chart", got)
	}
}
