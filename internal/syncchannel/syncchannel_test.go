/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package syncchannel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/monitoring"
	"dfaas-operator/internal/k6dispatch"
)

func testLT() *dfaasv1.LoadTest {
	return &dfaasv1.LoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default"},
	}
}

// undetected is a generator with no management address: its URLs come from
// the process-wide fallback.
func undetected(nodeID string) k6dispatch.Generator {
	return k6dispatch.Generator{NodeID: nodeID}
}

// recorder answers like the filer and records what it was asked.
type recorder struct {
	mu     sync.Mutex
	status int
	calls  []string // "<METHOD> <path>?<query>"
}

func (rec *recorder) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		rec.mu.Lock()
		target := req.URL.Path
		if req.URL.RawQuery != "" {
			target += "?" + req.URL.RawQuery
		}
		rec.calls = append(rec.calls, req.Method+" "+target)
		code := rec.status
		rec.mu.Unlock()
		if code == 0 {
			code = http.StatusNoContent
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (rec *recorder) got() []string {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]string(nil), rec.calls...)
}

// --- the object layout -----------------------------------------------------

func TestSummaryPath(t *testing.T) {
	lt := testLT()
	cases := []struct {
		nodeID string
		want   string
	}{
		{"node-1", "/dfaas-k6-summary/default/lt-sample/node-1.json"},
		// The nodeID goes through the one definition of how it is spelled:
		// lowercase, non-[a-z0-9-] to "-". Drift here would 404 every fetch.
		{"Node_1", "/dfaas-k6-summary/default/lt-sample/node-1.json"},
	}
	for _, c := range cases {
		if got := SummaryPath(lt, c.nodeID); got != c.want {
			t.Errorf("SummaryPath(%q) = %q, want %q", c.nodeID, got, c.want)
		}
	}
}

// --- which base each operation picks --------------------------------------

func TestFilerPicksTheRightBasePerOperation(t *testing.T) {
	// The operator reaches the filer one way; the VMs reach it another; the
	// exporter Job must always get in-cluster DNS regardless of both.
	f := NewFiler("http://operator.test:30901", "http://vm-facing.test:30901")
	lt := testLT()

	if got, want := f.GoURL(lt, undetected("node-1")), "http://vm-facing.test:30901/dfaas-sync/default/lt-sample.go"; got != want {
		t.Errorf("GoURL = %q, want %q (the VM-facing base)", got, want)
	}
	if got, want := f.SummaryURL(lt, undetected("node-1")),
		"http://vm-facing.test:30901/dfaas-k6-summary/default/lt-sample/node-1.json"; got != want {
		t.Errorf("SummaryURL = %q, want %q (the VM-facing base)", got, want)
	}
	inCluster := f.InClusterSummaryURL(lt, "node-1")
	if !strings.HasPrefix(inCluster, monitoring.FilerInClusterBase()) {
		t.Errorf("InClusterSummaryURL = %q, want the in-cluster base %q even when the operator "+
			"itself reaches the filer another way", inCluster, monitoring.FilerInClusterBase())
	}
}

func TestNoPublicBaseMeansNoVMFacingURLs(t *testing.T) {
	f := NewFiler("http://operator.test:30901", "")
	lt := testLT()

	// GoURL empty is the signal startK6 must fail the LoadTest on, rather than
	// parking every runner on a barrier nobody can open.
	if got := f.GoURL(lt, undetected("node-1")); got != "" {
		t.Errorf("GoURL = %q, want empty when no public base is resolvable", got)
	}
	// SummaryURL empty means "no upload" -- never an error.
	if got := f.SummaryURL(lt, undetected("node-1")); got != "" {
		t.Errorf("SummaryURL = %q, want empty", got)
	}
	// A generator with a detected management address needs no fallback.
	detected := k6dispatch.Generator{NodeID: "node-1", MgmtAddr: "100.64.0.11"}
	if got := f.GoURL(lt, detected); got != "http://100.64.0.11:30901/dfaas-sync/default/lt-sample.go" {
		t.Errorf("GoURL = %q, want the detected address on the filer NodePort", got)
	}
	// The exporter's URL does not depend on the public base at all.
	if got := f.InClusterSummaryURL(lt, "node-1"); got == "" {
		t.Error("InClusterSummaryURL must not depend on the public base")
	}
}

func TestEmptyOperatorBaseFallsBackToInCluster(t *testing.T) {
	f := NewFiler("", "")
	if f.operatorBase != monitoring.FilerInClusterBase() {
		t.Errorf("operatorBase = %q, want the in-cluster filer %q", f.operatorBase, monitoring.FilerInClusterBase())
	}
}

func TestTrailingSlashesAreNormalised(t *testing.T) {
	f := NewFiler("http://operator.test:30901/", "http://vm.test:30901/")
	if got, want := f.GoURL(testLT(), undetected("node-1")), "http://vm.test:30901/dfaas-sync/default/lt-sample.go"; got != want {
		t.Errorf("GoURL = %q, want %q", got, want)
	}
}

// The VM-facing base is per generator: the detected management address beats
// the process-wide fallback, and only with neither is there no URL. Both URLs
// of one generator share its base, and the GO path is the same for every
// generator, so one PublishGo still opens every barrier.
func TestVMFacingBasePrecedence(t *testing.T) {
	lt := testLT()
	const goPath = "/dfaas-sync/default/lt-sample.go"
	const summaryPath = "/dfaas-k6-summary/default/lt-sample/gen-a.json"
	cases := []struct {
		name     string
		fallback string
		mgmtAddr string
		wantBase string
	}{
		{"detected address beats the fallback", "http://vm-facing.test:30901", "100.64.0.11", "http://100.64.0.11:30901"},
		{"detected address with no fallback", "", "100.64.0.11", "http://100.64.0.11:30901"},
		{"detected IPv6 address is bracketed", "http://vm-facing.test:30901", "fd7a:115c:a1e0::11", "http://[fd7a:115c:a1e0::11]:30901"},
		{"no address uses the fallback", "http://vm-facing.test:30901", "", "http://vm-facing.test:30901"},
		{"neither gives no URL", "", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := NewFiler("http://operator.test:30901", c.fallback)
			g := k6dispatch.Generator{NodeID: "gen-a", MgmtAddr: c.mgmtAddr}
			wantGo, wantSummary := "", ""
			if c.wantBase != "" {
				wantGo, wantSummary = c.wantBase+goPath, c.wantBase+summaryPath
			}
			if got := f.GoURL(lt, g); got != wantGo {
				t.Errorf("GoURL = %q, want %q", got, wantGo)
			}
			if got := f.SummaryURL(lt, g); got != wantSummary {
				t.Errorf("SummaryURL = %q, want %q", got, wantSummary)
			}
			// The exporter reads in-cluster whatever the generator dials.
			if got := f.InClusterSummaryURL(lt, "gen-a"); got != monitoring.FilerInClusterBase()+summaryPath {
				t.Errorf("InClusterSummaryURL = %q, want the in-cluster base", got)
			}
		})
	}
}

// --- the HTTP behaviour ---------------------------------------------------

func TestPublishGoPutsTheObjectOnTheOperatorBase(t *testing.T) {
	rec := &recorder{status: http.StatusCreated}
	srv := rec.server(t)
	f := NewFiler(srv.URL, "http://vm.test:30901")

	if err := f.PublishGo(context.Background(), testLT()); err != nil {
		t.Fatalf("PublishGo: %v", err)
	}
	want := []string{"PUT /dfaas-sync/default/lt-sample.go"}
	if got := rec.got(); len(got) != 1 || got[0] != want[0] {
		t.Errorf("filer saw %v, want %v", got, want)
	}
}

// A filer that refuses the PUT must be reported, so the caller can retry on the
// poll cadence while the wait budget still bounds the total time. The old
// httptest handler answered 204 to everything, so this case was unexpressible.
func TestPublishGoReportsAFilerError(t *testing.T) {
	rec := &recorder{status: http.StatusInternalServerError}
	srv := rec.server(t)
	f := NewFiler(srv.URL, "http://vm.test:30901")

	err := f.PublishGo(context.Background(), testLT())
	if err == nil {
		t.Fatal("want an error when the filer returns 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should name the status: %v", err)
	}
}

func TestDeletesAreBestEffort(t *testing.T) {
	// An already-absent object is success: these are hygiene, and a 404 means
	// the work is already done.
	for _, code := range []int{http.StatusNoContent, http.StatusAccepted, http.StatusNotFound} {
		rec := &recorder{status: code}
		srv := rec.server(t)
		f := NewFiler(srv.URL, "")
		if err := f.DeleteGo(context.Background(), testLT()); err != nil {
			t.Errorf("DeleteGo on %d: %v", code, err)
		}
		if err := f.DeleteSummaries(context.Background(), testLT()); err != nil {
			t.Errorf("DeleteSummaries on %d: %v", code, err)
		}
	}

	// A real failure is still an error.
	rec := &recorder{status: http.StatusInternalServerError}
	srv := rec.server(t)
	f := NewFiler(srv.URL, "")
	if err := f.DeleteGo(context.Background(), testLT()); err == nil {
		t.Error("DeleteGo must report a 500")
	}
}

// One recursive DELETE, not N per-node ones: it also sweeps files from
// Generators later removed from the spec.
func TestDeleteSummariesIsRecursive(t *testing.T) {
	rec := &recorder{}
	srv := rec.server(t)
	f := NewFiler(srv.URL, "")

	if err := f.DeleteSummaries(context.Background(), testLT()); err != nil {
		t.Fatalf("DeleteSummaries: %v", err)
	}
	want := "DELETE /dfaas-k6-summary/default/lt-sample/?recursive=true"
	if got := rec.got(); len(got) != 1 || got[0] != want {
		t.Errorf("filer saw %v, want [%s]", got, want)
	}
}

// The GO object and the summaries live outside /buckets, so neither shows up as
// an S3 bucket.
func TestObjectsLiveOutsideBuckets(t *testing.T) {
	lt := testLT()
	for _, p := range []string{GoPath(lt), SummaryDirPath(lt), SummaryPath(lt, "node-1")} {
		if strings.HasPrefix(p, "/buckets") {
			t.Errorf("%q must not live under /buckets", p)
		}
	}
}

// --- env resolution, in exactly one place ---------------------------------

func TestFromEnv(t *testing.T) {
	t.Run("the public override wins", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "http://lab.example:30901")
		t.Setenv("HOST_IP", "192.0.2.10")
		t.Setenv("DFAAS_FILER_URL", "")
		if got, want := FromEnv().GoURL(testLT(), undetected("node-1")),
			"http://lab.example:30901/dfaas-sync/default/lt-sample.go"; got != want {
			t.Errorf("GoURL = %q, want %q", got, want)
		}
	})

	t.Run("a detected management address beats both", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "http://lab.example:30901")
		t.Setenv("HOST_IP", "192.0.2.10")
		t.Setenv("DFAAS_FILER_URL", "")
		g := k6dispatch.Generator{NodeID: "node-1", MgmtAddr: "fd7a:115c:a1e0::11"}
		if got, want := FromEnv().SummaryURL(testLT(), g),
			"http://[fd7a:115c:a1e0::11]:30901/dfaas-k6-summary/default/lt-sample/node-1.json"; got != want {
			t.Errorf("SummaryURL = %q, want %q", got, want)
		}
	})

	t.Run("HOST_IP plus the filer NodePort is the fallback", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "")
		t.Setenv("HOST_IP", "192.0.2.10")
		t.Setenv("DFAAS_FILER_URL", "")
		want := "http://192.0.2.10:30901/dfaas-sync/default/lt-sample.go"
		if got := FromEnv().GoURL(testLT(), undetected("node-1")); got != want {
			t.Errorf("GoURL = %q, want %q", got, want)
		}
		// And that port is the one the Helm values file pins.
		if monitoring.FilerNodePort != 30901 {
			t.Errorf("filer NodePort = %d; the k6 VMs and the values file expect 30901",
				monitoring.FilerNodePort)
		}
	})

	t.Run("an IPv6 HOST_IP is bracketed", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "")
		t.Setenv("HOST_IP", "2001:db8::10")
		t.Setenv("DFAAS_FILER_URL", "")
		want := "http://[2001:db8::10]:30901/dfaas-sync/default/lt-sample.go"
		if got := FromEnv().GoURL(testLT(), undetected("node-1")); got != want {
			t.Errorf("GoURL = %q, want %q", got, want)
		}
	})

	t.Run("neither leaves the VM-facing URLs empty", func(t *testing.T) {
		t.Setenv("DFAAS_SYNC_PUBLIC_URL", "")
		t.Setenv("HOST_IP", "")
		t.Setenv("DFAAS_FILER_URL", "")
		f := FromEnv()
		if got := f.GoURL(testLT(), undetected("node-1")); got != "" {
			t.Errorf("GoURL = %q, want empty", got)
		}
		// The operator still knows where to write: in-cluster DNS.
		if f.operatorBase != monitoring.FilerInClusterBase() {
			t.Errorf("operatorBase = %q, want %q", f.operatorBase, monitoring.FilerInClusterBase())
		}
	})
}
