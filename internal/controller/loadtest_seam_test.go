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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
	"dfaas-operator/internal/k6dispatch/fake"
	syncfake "dfaas-operator/internal/syncchannel/fake"
)

// These specs drive LoadTestReconciler.Reconcile — the module's actual
// interface — against a real API server (envtest) with the in-memory k6
// fleet behind the Dispatcher seam. Before the seam existed, Dispatcher was
// a concrete type with no double, so none of this was reachable by any test:
// not the dispatch protocol, not the observe triage, not abort, not the
// deletion finalizer, and not one of Reconcile's ordering guards.
var _ = Describe("LoadTest reconcile through the Dispatcher seam", func() {
	var (
		ctx   context.Context
		ns    string
		fleet *fake.Fleet
		r     *LoadTestReconciler
		// The object channel shared with the k6 VMs, recorded rather than
		// served: every operation is named, so "GO published exactly once"
		// is an assertion on a call list instead of a counter comparison on
		// undifferentiated HTTP hits.
		channel *syncfake.Channel
	)
	var nsCounter atomic.Int32

	envReady := func(name string, k6Nodes ...string) *dfaasv1.Environment {
		env := &dfaasv1.Environment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
				{NodeID: "worker", IPAddress: "10.0.0.1", Role: dfaasv1.RoleDfaasWorker, Capacity: "LOW",
					Username: "u", Password: "p",
					// A dfaas-worker with no functions is rejected at admission.
					Functions: []dfaasv1.Function{{Name: "figlet", Image: "ghcr.io/openfaas/figlet:latest",
						ExecTimeout: 5, MaxInflight: 400, TimeoutMs: 6000, MaxRate: 100}}},
			}},
		}
		for i, id := range k6Nodes {
			env.Spec.Nodes = append(env.Spec.Nodes, dfaasv1.EnvironmentNode{
				NodeID: id, IPAddress: fmt.Sprintf("10.0.1.%d", i+1), Role: dfaasv1.RoleK6LoadGenerator,
				Capacity: "LOW", Username: "u", Password: "p"})
		}
		Expect(k8sClient.Create(ctx, env)).To(Succeed())
		env.Status.Phase = dfaasv1.EnvReady
		for i, id := range k6Nodes {
			env.Status.K6Nodes = append(env.Status.K6Nodes, dfaasv1.K6NodeStatus{
				NodeID: id, IPAddress: fmt.Sprintf("10.0.1.%d", i+1), KubeconfigSecret: name + "-" + id + "-kubeconfig"})
		}
		Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
		return env
	}

	scriptCM := func(name string) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Data:       map[string]string{"script.js": "export default function(){}"},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	}

	newLT := func(name, envName string, nodeIDs ...string) *dfaasv1.LoadTest {
		lt := &dfaasv1.LoadTest{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: dfaasv1.LoadTestSpec{
				TargetEnvironment: envName,
				MetricsExport: dfaasv1.MetricsExportSpec{
					Metrics: []dfaasv1.MetricExportEntry{{Type: dfaasv1.MetricTypeRaw, Query: "up"}},
				},
			},
		}
		for _, id := range nodeIDs {
			lt.Spec.PerNodeLoad = append(lt.Spec.PerNodeLoad, dfaasv1.PerNodeLoad{
				NodeID: id, VUs: 1, Duration: "10s", ScriptConfigMap: corev1.LocalObjectReference{Name: "script"}})
		}
		return lt
	}

	keyOf := func(lt *dfaasv1.LoadTest) types.NamespacedName {
		return types.NamespacedName{Name: lt.Name, Namespace: lt.Namespace}
	}

	// reconcileUntil drives Reconcile up to max passes and stops when pred
	// holds on the freshly-read LoadTest. Each pass is one reconcile of the
	// real loop; requeues are not honoured because each pass re-reads anyway.
	reconcileUntil := func(lt *dfaasv1.LoadTest, max int, pred func(*dfaasv1.LoadTest) bool) *dfaasv1.LoadTest {
		var fresh dfaasv1.LoadTest
		for i := 0; i < max; i++ {
			_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: keyOf(lt)})
			// controller-runtime requeues a returned error; a 409 is the loop's
			// own stale-copy race (status write, then metadata Update on the
			// object fetched before it) and clears on the next pass.
			if err != nil && !apierrors.IsConflict(err) {
				Expect(err).NotTo(HaveOccurred())
			}
			if gerr := k8sClient.Get(ctx, keyOf(lt), &fresh); gerr != nil {
				if apierrors.IsNotFound(gerr) {
					return nil
				}
				Expect(gerr).NotTo(HaveOccurred())
			}
			if pred(&fresh) {
				return &fresh
			}
		}
		return &fresh
	}
	phaseIs := func(p dfaasv1.LoadTestPhase) func(*dfaasv1.LoadTest) bool {
		return func(lt *dfaasv1.LoadTest) bool { return lt.Status.Phase == p }
	}
	cond := func(lt *dfaasv1.LoadTest, t string) *metav1.Condition {
		return meta.FindStatusCondition(lt.Status.Conditions, t)
	}

	BeforeEach(func() {
		ctx = context.Background()
		// One namespace per spec: the occupancy gate serialises tests per
		// Environment within a namespace, and envtest has no namespace GC.
		ns = fmt.Sprintf("seam-%d", nsCounter.Add(1))
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())

		fleet = fake.New()
		channel = &syncfake.Channel{Public: "http://vm-facing.test:30901"}
		r = &LoadTestReconciler{
			Client: k8sClient, Scheme: scheme.Scheme,
			Dispatcher: fleet, Sync: channel,
			// reconcileUntil does not wait between passes; production paces a
			// failed remote round at remoteRetryInterval.
			retryEvery: time.Nanosecond,
		}
	})

	It("dispatches one TestRun per generator, then goes Running", func() {
		envReady("env", "gen-a", "gen-b")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a", "gen-b")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())

		fresh := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))

		applied := fleet.Applied()
		Expect(applied).To(HaveLen(2))
		Expect(fleet.Mirrored()).To(ConsistOf("gen-a/script", "gen-b/script"))
		Expect(fresh.Status.TestRuns).To(HaveLen(2))
		Expect(fresh.Status.TestRuns[0].Name).To(Equal("lt-gen-a"))
		Expect(fresh.Status.TestRuns[0].Namespace).To(Equal("default"))
		Expect(fresh.Status.StartTime).NotTo(BeNil())

		c := cond(fresh, dfaasv1.LTCondK6Dispatched)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal(dfaasv1.LTReasonAllDispatched))
	})

	It("never re-applies a TestRun that is already live (partial-dispatch resume)", func() {
		envReady("env", "gen-a", "gen-b")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a", "gen-b")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		fleet.FailNext("gen-b", "apply", fmt.Errorf("remote down"))

		// First pass through startK6: gen-a applies, gen-b fails → retry budget.
		reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool { return len(l.Status.TestRuns) == 1 })
		Expect(fleet.Applied()).To(HaveLen(1))
		Expect(fleet.Applied()[0].NodeID).To(Equal("gen-a"))

		// Recovery: gen-b applies, gen-a is NOT touched again.
		fresh := reconcileUntil(lt, 8, phaseIs(dfaasv1.LoadTestRunning))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		Expect(fleet.Applied()).To(HaveLen(2))
		Expect(fleet.Applied()[1].NodeID).To(Equal("gen-b"))
		// gen-a's TestRun was deleted exactly once (the stale-wipe before its
		// own apply), never again on the resume pass.
		deletes := 0
		for _, d := range fleet.Deleted() {
			if d == "gen-a|lt-gen-a" {
				deletes++
			}
		}
		Expect(deletes).To(Equal(1))
	})

	It("observes stages: all finished → Exporting with k6 logs captured", func() {
		envReady("env", "gen-a")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

		// Runner still going: stays Running, K6Healthy=Unknown.
		fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		Expect(cond(fresh, dfaasv1.LTCondK6Healthy).Status).To(Equal(metav1.ConditionUnknown))

		fleet.SetLogs("gen-a", "k6 summary here")
		fleet.SetStage("gen-a", lt, "finished")
		fresh = reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestExporting))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestExporting))
		Expect(fresh.Status.EndTime).NotTo(BeNil())
		Expect(cond(fresh, dfaasv1.LTCondK6Healthy).Reason).To(Equal(dfaasv1.LTReasonAllFinished))

		var cm corev1.ConfigMap
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lt-k6log-gen-a", Namespace: ns}, &cm)).To(Succeed())
		Expect(cm.Data["k6.log"]).To(Equal("k6 summary here"))
	})

	It("fails the LoadTest when every runner reports error", func() {
		envReady("env", "gen-a", "gen-b")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a", "gen-b")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

		fleet.SetStage("gen-a", lt, "error")
		fleet.SetStage("gen-b", lt, "error")
		deletedBefore := len(fleet.Deleted())
		fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
		Expect(cond(fresh, dfaasv1.LTCondK6Healthy).Reason).To(Equal(dfaasv1.LTReasonAllFailed))
		// Runners already observed done are not torn down: their remote logs survive.
		Expect(fleet.Deleted()).To(HaveLen(deletedBefore))
	})

	It("holds a syncStart test on the barrier until every runner has started, then publishes GO", func() {
		envReady("env", "gen-a", "gen-b")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a", "gen-b")
		lt.Spec.SyncStart = true
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())

		// Dispatched, but stage is "created" on both: barrier holds, no GO.
		fresh := reconcileUntil(lt, 10, func(l *dfaasv1.LoadTest) bool {
			c := cond(l, dfaasv1.LTCondSyncReady)
			return c != nil && c.Reason == dfaasv1.LTReasonAwaitingRunners
		})
		Expect(fresh.Status.Phase).NotTo(Equal(dfaasv1.LoadTestRunning))
		Expect(fleet.Applied()).To(HaveLen(2))
		for _, a := range fleet.Applied() {
			Expect(a.Env.SyncURL).To(HavePrefix("http://vm-facing.test:30901"))
		}
		Expect(channel.Count("PublishGo")).To(BeZero(), "GO must not be published while a runner is still creating")

		// One runner parked, one still creating: still held.
		fleet.SetStage("gen-a", lt, "started")
		fresh = reconcileUntil(lt, 2, func(*dfaasv1.LoadTest) bool { return false })
		Expect(fresh.Status.Phase).NotTo(Equal(dfaasv1.LoadTestRunning))
		Expect(cond(fresh, dfaasv1.LTCondSyncReady).Message).To(ContainSubstring("1/2"))

		// Both parked: GO published, test goes Running.
		fleet.SetStage("gen-b", lt, "started")
		fresh = reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestRunning))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		Expect(cond(fresh, dfaasv1.LTCondSyncReady).Reason).To(Equal(dfaasv1.LTReasonGoPublished))
		// Exactly once, and only after every TestRun reported started.
		Expect(channel.Count("PublishGo")).To(Equal(1))
	})

	It("aborts on spec.stop: every remote TestRun deleted, phase Aborted with the user's reason", func() {
		envReady("env", "gen-a", "gen-b")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a", "gen-b")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

		Expect(k8sClient.Patch(ctx, lt, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"stop":true}}`)))).To(Succeed())
		fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestAborted))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestAborted))
		Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
		Expect(fleet.Exists("gen-b", lt)).To(BeFalse())
		ready := cond(fresh, dfaasv1.LTCondReady)
		Expect(ready).NotTo(BeNil())
		Expect(ready.Reason).To(Equal(dfaasv1.LTReasonUserAborted))
		Expect(cond(fresh, dfaasv1.LTCondMetricsExported).Reason).To(Equal(dfaasv1.LTReasonExportSkipped))
	})

	It("deletion finalizer reclaims remote TestRuns before releasing the CR", func() {
		envReady("env", "gen-a")
		scriptCM("script")
		lt := newLT("lt", "env", "gen-a")
		Expect(k8sClient.Create(ctx, lt)).To(Succeed())
		reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
		Expect(fleet.Exists("gen-a", lt)).To(BeTrue())

		Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
		gone := reconcileUntil(lt, 6, func(*dfaasv1.LoadTest) bool { return false })
		Expect(gone).To(BeNil(), "LoadTest should be gone once the finalizer released it")
		Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
	})

	// A generator whose TestRun is applied may still be unable to reach the
	// VM-facing filer: the GO signal and the end-of-test summary upload both
	// go through it. startProbe asks each generator right after dispatch;
	// collectProbes reads the verdicts once they have settled and folds an
	// unreachable one into K6Dispatched as a warning that does not move the
	// dispatch's own status or budget.
	Context("filer reachability probe", func() {
		// awaiting is the "fleet round" Context's helper, duplicated here: it
		// is a closure over that Context's own scope, not a Describe-level
		// helper, so a sibling Context cannot reach it.
		awaiting := func(l *dfaasv1.LoadTest) bool {
			c := cond(l, dfaasv1.LTCondSyncReady)
			return c != nil && c.Reason == dfaasv1.LTReasonAwaitingRunners
		}

		// ageDispatched moves K6Dispatched's LastTransitionTime back by d, as
		// the barrier-budget specs do, so the probe settle window can pass.
		ageDispatched := func(lt *dfaasv1.LoadTest, d time.Duration) {
			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			for i := range fresh.Status.Conditions {
				if fresh.Status.Conditions[i].Type == dfaasv1.LTCondK6Dispatched {
					fresh.Status.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(-d))
				}
			}
			Expect(k8sClient.Status().Update(ctx, &fresh)).To(Succeed())
		}

		// logged sends the reconciler's log into the returned lines, for the
		// outcomes that go to the operator log rather than to a Condition.
		logged := func() *[]string {
			lines := &[]string{}
			ctx = logr.NewContext(ctx, funcr.New(func(_, args string) { *lines = append(*lines, args) }, funcr.Options{}))
			return lines
		}

		It("starts one probe per generator with the summary URL it injects", func() {
			// Every SummaryURL call answers differently, so a probe that
			// recomputed the URL would disagree with the one applied.
			r.Sync = &perCallChannel{Channel: channel}
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			applied := fleet.Applied()
			Expect(applied).To(HaveLen(2))
			for _, a := range applied {
				Expect(a.Env.SummaryURL).NotTo(BeEmpty())
				Expect(fleet.ProbeURL(a.NodeID, running)).To(Equal(a.Env.SummaryURL))
			}
		})

		// countOf counts one call in fleet.Calls().
		countOf := func(call string) int {
			n := 0
			for _, c := range fleet.Calls() {
				if c == call {
					n++
				}
			}
			return n
		}

		// Each probe is two remote calls, each building a fresh client: run
		// between two Applies they would add start skew between the
		// generators. The TestRuns start as they did before the probe.
		It("applies every TestRun of the pass before any probe call", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fleet.Calls()).To(Equal([]string{
				"apply:gen-a", "apply:gen-b",
				"probe-delete:gen-a", "probe:gen-a", "probe-delete:gen-b", "probe:gen-b",
			}))
		})

		It("a failed Apply starts no probe for that generator while the dispatch retries", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fleet.FailNext("gen-b", "apply", fmt.Errorf("remote down"))
			retrying := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool { return len(l.Status.TestRuns) == 1 })
			Expect(cond(retrying, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonApplyFailed))
			Expect(fleet.ProbeExists("gen-b", retrying)).To(BeFalse())
			Expect(fleet.ProbeExists("gen-a", retrying)).To(BeTrue(), "gen-a was applied in the pass that failed on gen-b")

			running := reconcileUntil(lt, 8, phaseIs(dfaasv1.LoadTestRunning))
			Expect(running.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			applied := fleet.Applied()
			Expect(applied).To(HaveLen(2))
			Expect(applied[1].NodeID).To(Equal("gen-b"))
			Expect(fleet.ProbeURL("gen-b", running)).To(Equal(applied[1].Env.SummaryURL))
			// gen-a's probe was started once; the resume pass left it alone.
			Expect(countOf("probe:gen-a")).To(Equal(1))
			Expect(countOf("probe-delete:gen-a")).To(Equal(1))
		})

		// Here the pass applies gen-a, then fails the test on gen-b (absent from
		// the Environment): the run end's teardown has already run when the
		// probes would start, and a probe started after it would outlive the
		// run it belongs to.
		It("a pass that ends the run starts no probe for the generator it applied", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			failed := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestFailed))
			Expect(failed.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(fleet.Applied()).To(HaveLen(1))
			Expect(fleet.ProbeExists("gen-a", failed)).To(BeFalse())
		})

		It("an unreachable filer keeps K6Dispatched True, names generator, URL and error, and deletes the probes", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			before := cond(running, dfaasv1.LTCondK6Dispatched).LastTransitionTime
			fleet.SetProbe("gen-a", running, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable,
				Detail: "can't connect to remote host (192.168.252.70): Connection timed out"})
			fleet.SetProbe("gen-b", running, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeReachable})
			fresh := reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool {
				return cond(l, dfaasv1.LTCondK6Dispatched).Reason == dfaasv1.LTReasonDispatchedUnreachable
			})
			c := cond(fresh, dfaasv1.LTCondK6Dispatched)
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
			Expect(c.LastTransitionTime).To(Equal(before))
			Expect(c.Message).To(ContainSubstring("gen-a"))
			Expect(c.Message).To(ContainSubstring(channel.SummaryURL(fresh, "gen-a")))
			Expect(c.Message).To(ContainSubstring("Connection timed out"))
			Expect(c.Message).NotTo(ContainSubstring("gen-b"))
			// The runners upload to the filer; a plain test has no GO signal
			// for them to fetch, so the warning must not name one.
			Expect(c.Message).To(ContainSubstring("cannot upload the end-of-test summaries"))
			Expect(c.Message).NotTo(ContainSubstring("GO signal"))
			Expect(c.Message).To(ContainSubstring("DFAAS_SYNC_PUBLIC_URL must be an address every generator can reach"))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			Expect(fleet.ProbeExists("gen-a", fresh)).To(BeFalse())
			Expect(fleet.ProbeExists("gen-b", fresh)).To(BeFalse())
		})

		It("a warning whose status write fails keeps the probes, and the next pass records it", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			fleet.SetProbe("gen-a", running, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable,
				Detail: "Connection timed out"})
			// The status write carrying the warning fails once, as an API
			// server hiccup would; RetryOnConflict does not retry a 503.
			var refused atomic.Bool
			wc, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme.Scheme})
			Expect(err).NotTo(HaveOccurred())
			r.Client = interceptor.NewClient(wc, interceptor.Funcs{
				SubResourceUpdate: func(ictx context.Context, c client.Client, sub string,
					obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if l, ok := obj.(*dfaasv1.LoadTest); ok {
						k := cond(l, dfaasv1.LTCondK6Dispatched)
						if k != nil && k.Reason == dfaasv1.LTReasonDispatchedUnreachable && refused.CompareAndSwap(false, true) {
							return apierrors.NewServiceUnavailable("etcdserver: leader changed")
						}
					}
					return c.SubResource(sub).Update(ictx, obj, opts...)
				},
			})
			lines := logged()
			fresh := reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool {
				return cond(l, dfaasv1.LTCondK6Dispatched).Reason == dfaasv1.LTReasonDispatchedUnreachable
			})
			Expect(refused.Load()).To(BeTrue(), "the first warning stamp was refused")
			c := cond(fresh, dfaasv1.LTCondK6Dispatched)
			Expect(c.Reason).To(Equal(dfaasv1.LTReasonDispatchedUnreachable))
			Expect(c.Message).To(ContainSubstring("Connection timed out"))
			Expect(fleet.ProbeExists("gen-a", fresh)).To(BeFalse())
			// The verdict reached the log while no Condition held it yet.
			Expect(*lines).To(ContainElement(SatisfyAll(ContainSubstring(`"node"="gen-a"`),
				ContainSubstring(channel.SummaryURL(fresh, "gen-a")), ContainSubstring("Connection timed out"))))
		})

		It("every generator reachable leaves AllDispatched and deletes the probes", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			fleet.SetProbe("gen-a", running, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeReachable})
			fresh := reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool { return !fleet.ProbeExists("gen-a", l) })
			Expect(fleet.ProbeExists("gen-a", fresh)).To(BeFalse())
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonAllDispatched))
		})

		It("a probe that did not run, or could not be created, leaves AllDispatched", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			fleet.FailNext("gen-b", "probe", fmt.Errorf("pods is forbidden"))
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fleet.ProbeExists("gen-b", running)).To(BeFalse())
			fleet.SetProbe("gen-a", running, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeDidNotRun, Detail: "DeadlineExceeded"})
			fresh := reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool { return !fleet.ProbeExists("gen-a", l) })
			Expect(fleet.ProbeExists("gen-a", fresh)).To(BeFalse())
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonAllDispatched))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		})

		It("no VM-facing filer URL starts no probe, and the operator log says so", func() {
			channel.Public = ""
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			lines := logged()
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(running.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			Expect(fleet.ProbeExists("gen-a", running)).To(BeFalse())
			Expect(*lines).To(ContainElement(SatisfyAll(ContainSubstring("probe skipped"),
				ContainSubstring(`"node"="gen-a"`))))
		})

		It("a probe still pending after the settle time counts as not run and is deleted", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			ageDispatched(lt, probeSettleAfter+time.Second)
			fresh := reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool { return !fleet.ProbeExists("gen-a", l) })
			Expect(fleet.ProbeExists("gen-a", fresh)).To(BeFalse())
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonAllDispatched))
		})

		It("a syncStart test keeps the warning across barrier polls that re-enter the dispatch", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			waiting := reconcileUntil(lt, 10, awaiting)
			fleet.SetProbe("gen-a", waiting, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable, Detail: "download timed out"})
			reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool {
				return cond(l, dfaasv1.LTCondK6Dispatched).Reason == dfaasv1.LTReasonDispatchedUnreachable
			})
			fresh := reconcileUntil(lt, 4, func(*dfaasv1.LoadTest) bool { return false }) // more barrier polls
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonDispatchedUnreachable))
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Status).To(Equal(metav1.ConditionTrue))
			// Only a syncStart runner fetches the GO signal from the filer.
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Message).To(ContainSubstring("cannot fetch the GO signal"))
		})

		It("a stale probe Pod is deleted before the new probe starts", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			fleet.SetProbe("gen-a", lt, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable, URL: "http://old"})
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fleet.ProbeURL("gen-a", running)).To(Equal(channel.SummaryURL(running, "gen-a")))
		})

		It("a SyncTimeout names the failed probe", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			waiting := reconcileUntil(lt, 10, awaiting)
			fleet.SetProbe("gen-a", waiting, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable, Detail: "download timed out"})
			reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool {
				return cond(l, dfaasv1.LTCondK6Dispatched).Reason == dfaasv1.LTReasonDispatchedUnreachable
			})
			ageDispatched(lt, syncWaitBudget+time.Minute)
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			s := cond(fresh, dfaasv1.LTCondSyncReady)
			Expect(s.Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
			Expect(s.Message).To(ContainSubstring("download timed out"))
			Expect(cond(fresh, dfaasv1.LTCondReady).Message).To(ContainSubstring("download timed out"))
		})

		It("a SyncTimeout without a failed probe says nothing about it", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 10, awaiting)
			ageDispatched(lt, syncWaitBudget+time.Minute)
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondSyncReady).Message).NotTo(ContainSubstring("at dispatch"))
		})

		// collectProbes stamps the DispatchedUnreachable warning through condErr,
		// which re-Gets a separate object inside the status writer -- it never
		// mutates the lt pointer awaitSyncBarrier itself holds. If a fail()
		// trigger that does not depend on syncWaitBudget (a runner reporting
		// stage=error) fires on the very same tick the probe verdict first
		// settles unreachable, probeNote(lt) must still see the warning that
		// landed moments earlier in the same pass.
		It("a probe warning landing on the same tick as an unrelated failure still names it", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			waiting := reconcileUntil(lt, 10, awaiting)
			// Both land before the reconciler ever reads either: the probe
			// verdict (unread until this pass) and a runner error (independent
			// of the probe and of syncWaitBudget).
			fleet.SetProbe("gen-a", waiting, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable, Detail: "download timed out"})
			fleet.SetStage("gen-a", waiting, "error")
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonDispatchedUnreachable))
			s := cond(fresh, dfaasv1.LTCondSyncReady)
			Expect(s.Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
			Expect(s.Message).To(ContainSubstring("download timed out"))
		})

		// endOnEnvironmentLost re-Gets the LoadTest into fresh specifically
		// because the reconciler's own lt (fetched once at the top of Reconcile)
		// can be stale -- an informer cache lag in production. probeNote must
		// read fresh, not lt, or the note this function exists to add is lost
		// exactly when the caller's copy predates the probe's stamp.
		It("endOnEnvironmentLost names the probe from a fresh copy, not the reconcile's stale one", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			waiting := reconcileUntil(lt, 10, awaiting)

			// A snapshot from before the probe warning landed -- standing in for
			// the caller's copy of lt, which Reconcile fetches once at the top
			// and never refreshes itself.
			var stale dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &stale)).To(Succeed())

			fleet.SetProbe("gen-a", waiting, k6dispatch.ProbeOutcome{State: k6dispatch.ProbeUnreachable, Detail: "download timed out"})
			reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool {
				return cond(l, dfaasv1.LTCondK6Dispatched).Reason == dfaasv1.LTReasonDispatchedUnreachable
			})

			// The Environment stops being Dispatchable while the test still
			// holds on the barrier with TestRuns recorded -- endOnEnvironmentLost's
			// own branch.
			var env dfaasv1.Environment
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "env", Namespace: ns}, &env)).To(Succeed())
			env.Status.Phase = dfaasv1.EnvFailed
			Expect(k8sClient.Status().Update(ctx, &env)).To(Succeed())

			// Reconcile's own top-level Get returns the pre-warning snapshot
			// exactly once -- the class of staleness an informer cache lag would
			// produce in production; every later Get in the same pass (including
			// endOnEnvironmentLost's own re-Get) sees the real, current object.
			var served atomic.Bool
			wc, err := client.NewWithWatch(cfg, client.Options{Scheme: scheme.Scheme})
			Expect(err).NotTo(HaveOccurred())
			r.Client = interceptor.NewClient(wc, interceptor.Funcs{
				Get: func(ictx context.Context, c client.WithWatch, key client.ObjectKey,
					obj client.Object, opts ...client.GetOption) error {
					if l, ok := obj.(*dfaasv1.LoadTest); ok && key == keyOf(lt) && served.CompareAndSwap(false, true) {
						stale.DeepCopyInto(l)
						return nil
					}
					return c.Get(ictx, key, obj, opts...)
				},
			})

			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(served.Load()).To(BeTrue(), "the stale copy must have been served exactly once")
			s := cond(fresh, dfaasv1.LTCondSyncReady)
			Expect(s.Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
			Expect(s.Message).To(ContainSubstring("download timed out"))
			Expect(cond(fresh, dfaasv1.LTCondReady).Message).To(ContainSubstring("download timed out"))
		})

		It("the run end deletes a probe Pod nobody read", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fleet.ProbeExists("gen-a", running)).To(BeTrue())
			_, err := r.abortLoadTest(ctx, running, dfaasv1.LTReasonUserAborted, "stop")
			Expect(err).NotTo(HaveOccurred())
			Expect(fleet.ProbeExists("gen-a", running)).To(BeFalse())
		})

		It("deleting the LoadTest deletes its probe Pod", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			running.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, running)).To(Succeed())
			Expect(fleet.ProbeExists("gen-a", running)).To(BeTrue())
			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			Expect(reconcileUntil(lt, 8, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.ProbeExists("gen-a", running)).To(BeFalse())
		})

		// Neither of the two specs above exercises reclaimProbes: the abort spec
		// goes through teardownRemoteTestRuns, and the delete spec through the
		// finalizer's own poll -- both have their own DeleteProbe call. The path
		// every short run whose probe was never read takes is the ordinary
		// Exporting -> Completed exit, where endRun's !runnersMayBeLive branch
		// calls reclaimProbes because the phase itself is already Exporting.
		It("the run end deletes a probe Pod when an ordinary run reaches Completed", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fleet.ProbeExists("gen-a", running)).To(BeTrue())

			fleet.SetStage("gen-a", lt, "finished")
			exporting := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestExporting))
			Expect(fleet.ProbeExists("gen-a", exporting)).To(BeTrue(),
				"the probe outlives dispatch: nothing has read or reclaimed it yet")

			// Skip the export cool-down so the exporter Job is created at once.
			past := metav1.NewTime(exporting.Status.EndTime.Add(-(exportCooldown + time.Minute)))
			exporting.Status.EndTime = &past
			Expect(k8sClient.Status().Update(ctx, exporting)).To(Succeed())
			reconcileUntil(lt, 2, func(*dfaasv1.LoadTest) bool { return false })

			var job batchv1.Job
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: ExporterJobName(lt), Namespace: ns}, &job)).To(Succeed())
			job.Status.Succeeded = 1
			Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())

			completed := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestCompleted))
			Expect(completed.Status.Phase).To(Equal(dfaasv1.LoadTestCompleted))
			Expect(fleet.ProbeExists("gen-a", completed)).To(BeFalse())
		})
	})

	Context("ordering guards in Reconcile", func() {
		It("terminal phases are a no-op: nothing is dispatched", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			lt.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, lt)).To(Succeed())

			reconcileUntil(lt, 3, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("missing Environment fails the test and says so on EnvironmentLinked", func() {
			lt := newLT("lt", "nope", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			c := cond(fresh, dfaasv1.LTCondEnvironmentLinked)
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionFalse))
			Expect(c.Reason).To(Equal(dfaasv1.LTReasonEnvNotFound))
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("strict create gate: a non-Ready Environment fails a fresh non-draft test", func() {
			env := envReady("env", "gen-a")
			env.Status.Phase = dfaasv1.EnvProvisioningInfra
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("a test that fails admission is still owned by its Environment", func() {
			env := envReady("env", "gen-a")
			env.Status.Phase = dfaasv1.EnvProvisioningInfra
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			failed := reconcileUntil(lt, 6, phaseIs(dfaasv1.LoadTestFailed))
			Expect(hasOwnerRef(failed, env)).To(BeTrue())
		})

		It("a suspended draft parks at Pending and dispatches nothing", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.Suspended = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestPending))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("a parked draft still gets its Environment ownerRef", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.Suspended = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestPending))
			fresh := reconcileUntil(lt, 3, func(l *dfaasv1.LoadTest) bool { return hasOwnerRef(l, env) })
			Expect(hasOwnerRef(fresh, env)).To(BeTrue())
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("a node that left the Environment mid-test fails the test and deletes the other runners", func() {
			env := envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			env.Status.K6Nodes = env.Status.K6Nodes[:1] // gen-b gone
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse(), "the reachable runner must not keep loading")
			Expect(cond(fresh, dfaasv1.LTCondK6Healthy).Message).To(ContainSubstring("gen-b"))
			Expect(cond(fresh, dfaasv1.LTCondMetricsExported).Reason).To(Equal(dfaasv1.LTReasonExportSkipped))
		})
	})
	// Every way a run ends goes through one module that reclaims what the run
	// may still have live on the generators. Before, only the Sync barrier
	// tore down: four exits to Failed left runners loading the DFaaS nodes
	// while Occupancy freed the Environment for the next queued test.
	Context("run end (every exit reclaims)", func() {
		remoteDown := fmt.Errorf("remote k3s API down")
		k6Healthy := func(l *dfaasv1.LoadTest) *metav1.Condition { return cond(l, dfaasv1.LTCondK6Healthy) }
		exhaustBudget := func(lt *dfaasv1.LoadTest) {
			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:"%d:%d"}}}`,
				dispatchAttemptsAnnotation, fresh.Generation, dispatchRetryBudget-1)
			Expect(k8sClient.Patch(ctx, &fresh, client.RawPatch(types.MergePatchType, []byte(patch)))).To(Succeed())
		}

		It("an exhausted dispatch budget reclaims gen-a and does not hold the Environment for gen-b, which never got a TestRun", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fleet.FailNext("gen-b", "apply", remoteDown)
			reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool { return len(l.Status.TestRuns) == 1 })

			exhaustBudget(lt)
			fleet.FailNext("gen-b", "mirror", remoteDown)
			fleet.FailNext("gen-b", "delete", remoteDown)
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondReady).Reason).To(Equal(dfaasv1.LTReasonDispatchFailed))
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonDispatchFailed))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(k6Healthy(fresh).Reason).NotTo(Equal(dfaasv1.LTReasonRunnersUnreclaimed))
			Expect(k6Healthy(fresh).Message).To(ContainSubstring("gen-b"))

			lt2 := newLT("lt2", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt2)).To(Succeed())
			Expect(reconcileUntil(lt2, 12, phaseIs(dfaasv1.LoadTestRunning)).Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		})

		It("a generator that left the Environment mid-dispatch fails the test and deletes the TestRun already applied", func() {
			env := envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fleet.FailNext("gen-b", "apply", remoteDown)
			reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool { return len(l.Status.TestRuns) == 1 })

			env.Status.K6Nodes = env.Status.K6Nodes[:1]
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(k6Healthy(fresh).Message).To(ContainSubstring("gen-b"))
		})

		It("an unreachable generator no longer blocks the barrier's Failed transition", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 10, func(l *dfaasv1.LoadTest) bool {
				c := cond(l, dfaasv1.LTCondSyncReady)
				return c != nil && c.Reason == dfaasv1.LTReasonAwaitingRunners
			})

			fleet.SetStage("gen-a", lt, "error")
			fleet.FailNext("gen-b", "delete", remoteDown)
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondSyncReady).Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
			Expect(k6Healthy(fresh).Reason).To(Equal(dfaasv1.LTReasonRunnersUnreclaimed))
			Expect(k6Healthy(fresh).Message).To(ContainSubstring("gen-b"))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(channel.Count("PublishGo")).To(BeZero())
		})

		It("an exhausted observe budget fails the test and reclaims every generator", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			var cur dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &cur)).To(Succeed())
			patch := fmt.Sprintf(`{"metadata":{"annotations":{%q:"%d:%d"}}}`,
				fetchMissesAnnotation, cur.Generation, fetchRetryBudget-1)
			Expect(k8sClient.Patch(ctx, &cur, client.RawPatch(types.MergePatchType, []byte(patch)))).To(Succeed())
			fleet.FailNext("gen-a", "stage", remoteDown)
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondReady).Message).To(ContainSubstring("gen-a"))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(fleet.Exists("gen-b", lt)).To(BeFalse())
			Expect(k6Healthy(fresh).Reason).To(Equal(dfaasv1.LTReasonRunnersReclaimed))
		})

		It("abort with an unreachable generator reaches Aborted in one pass, holds the Environment, then releases it", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			Expect(k8sClient.Patch(ctx, lt, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"stop":true}}`)))).To(Succeed())
			fleet.FailNext("gen-b", "delete", remoteDown)
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestAborted))
			Expect(k6Healthy(fresh).Reason).To(Equal(dfaasv1.LTReasonRunnersUnreclaimed))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(fleet.Exists("gen-b", lt)).To(BeTrue())

			lt2 := newLT("lt2", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt2)).To(Succeed())
			held := reconcileUntil(lt2, 6, func(l *dfaasv1.LoadTest) bool { return cond(l, dfaasv1.LTCondQueued) != nil })
			Expect(held.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
			q := cond(held, dfaasv1.LTCondQueued)
			Expect(q).NotTo(BeNil())
			Expect(q.Reason).To(Equal(dfaasv1.LTReasonEnvBusy))
			Expect(q.Message).To(ContainSubstring(`"lt"`))
			for _, a := range fleet.Applied() {
				Expect(a.Name).NotTo(HavePrefix("lt2-"))
			}

			fleet.FailNext("gen-b", "delete", remoteDown)
			still := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(k6Healthy(still).Reason).To(Equal(dfaasv1.LTReasonRunnersUnreclaimed))

			healed := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(k6Healthy(healed).Reason).To(Equal(dfaasv1.LTReasonRunnersReclaimed))
			Expect(fleet.Exists("gen-b", lt)).To(BeFalse())
			Expect(reconcileUntil(lt2, 12, phaseIs(dfaasv1.LoadTestRunning)).Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		})

		It("a Failed exit from the exporter step sweeps the k6 summaries", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			fleet.SetStage("gen-a", lt, "finished")
			exporting := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestExporting))
			deletedBefore := len(fleet.Deleted())

			exporting.Status.StartTime = nil
			Expect(k8sClient.Status().Update(ctx, exporting)).To(Succeed())
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondMetricsExported).Reason).To(Equal(dfaasv1.LTReasonJobFailed))
			Expect(channel.Count("DeleteSummaries")).To(Equal(1))
			Expect(fleet.Deleted()).To(HaveLen(deletedBefore))
		})

		It("deleting a Completed test reclaims its TestRun without rewriting its record", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			fleet.SetStage("gen-a", lt, "finished")
			done := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestExporting))
			done.Status.Phase = dfaasv1.LoadTestCompleted
			meta.SetStatusCondition(&done.Status.Conditions, metav1.Condition{Type: dfaasv1.LTCondMetricsExported,
				Status: metav1.ConditionTrue, Reason: dfaasv1.LTReasonExportSucceeded, Message: "metrics exported"})
			Expect(k8sClient.Status().Update(ctx, done)).To(Succeed())

			fleet.FailNext("gen-a", "stage", remoteDown)
			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			kept := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(kept).NotTo(BeNil())
			Expect(kept.Status.Phase).To(Equal(dfaasv1.LoadTestCompleted))
			Expect(cond(kept, dfaasv1.LTCondMetricsExported).Reason).To(Equal(dfaasv1.LTReasonExportSucceeded))

			Expect(reconcileUntil(lt, 6, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
		})

		It("an unreachable generator holds the deletion finalizer only for the deletion budget", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			fleet.FailNext("gen-b", "delete", remoteDown)
			first := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(first.Status.Phase).To(Equal(dfaasv1.LoadTestAborted))

			fleet.FailNext("gen-b", "stage", remoteDown)
			Expect(reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })).NotTo(BeNil())

			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			past := metav1.NewTime(fresh.DeletionTimestamp.Add(-deletionReclaimBudget - time.Second))
			fresh.DeletionTimestamp = &past
			fleet.FailNext("gen-b", "stage", remoteDown)
			_, err := r.handleLoadTestDeletion(ctx, &fresh)
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, keyOf(lt), &fresh))).To(BeTrue())
		})

		It("deleting a never-dispatched draft makes no remote call", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			lt.Spec.Suspended = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestPending))

			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			Expect(reconcileUntil(lt, 3, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Deleted()).To(BeEmpty())
			Expect(channel.Calls()).To(BeEmpty())
		})

		It("with the Environment already gone, deletion still sweeps the GO object and the summaries", func() {
			lt := newLT("lt", "missing", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false }) // adds the finalizer
			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			fresh.Status.TestRuns = []dfaasv1.TestRunRef{{NodeID: "gen-a", Name: "lt-gen-a", Namespace: "default"}}
			Expect(k8sClient.Status().Update(ctx, &fresh)).To(Succeed())

			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			Expect(reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(channel.Count("DeleteGo")).To(Equal(1))
			Expect(channel.Count("DeleteSummaries")).To(Equal(1))
		})

		It("Environment deletion drains the tests it owns first, and a deleting Environment dispatches nothing new", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Finalizers = append(env.Finalizers, environmentFinalizer)
			Expect(k8sClient.Update(ctx, env)).To(Succeed())
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			Expect(k8sClient.Delete(ctx, env)).To(Succeed())
			lt3 := newLT("lt3", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt3)).To(Succeed())
			reconcileUntil(lt3, 3, func(*dfaasv1.LoadTest) bool { return false })
			for _, a := range fleet.Applied() {
				Expect(a.Name).NotTo(HavePrefix("lt3-"))
			}

			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			Expect(env.Finalizers).To(ContainElement(environmentFinalizer))

			Expect(reconcileUntil(lt, 6, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(reconcileUntil(lt3, 4, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			_, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env))).To(BeTrue())
		})

		It("Environment deletion drains a finished test until its remote TestRun is gone", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Finalizers = append(env.Finalizers, environmentFinalizer)
			Expect(k8sClient.Update(ctx, env)).To(Succeed())
			sec := kubeconfigSecret("env-gen-a-kubeconfig", "env", "gen-a")
			sec.Namespace = env.Namespace
			Expect(k8sClient.Create(ctx, sec)).To(Succeed())

			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			running.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, running)).To(Succeed())
			Expect(fleet.Exists("gen-a", lt)).To(BeTrue()) // finished TestRuns stay: remote logs survive

			Expect(k8sClient.Delete(ctx, env)).To(Succeed())
			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			// Terminal is not enough: the test still exists, its finalizer has not run.
			res, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sec), sec)).To(Succeed())

			Expect(reconcileUntil(lt, 6, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			_, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env))).To(BeTrue())
		})

		It("Environment deletion gives up on a test still finalizing after its drain budget", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Finalizers = append(env.Finalizers, environmentFinalizer)
			Expect(k8sClient.Update(ctx, env)).To(Succeed())
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			Expect(k8sClient.Delete(ctx, env)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			// The API server will not backdate the test's DeletionTimestamp: move
			// the clock past its drain budget instead. The test never ran its
			// finalizer, so it is still there.
			er.now = func() time.Time { return time.Now().Add(envDrainBudget + time.Second) }
			res, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeZero())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), &dfaasv1.Environment{}))).To(BeTrue())
			Expect(k8sClient.Get(ctx, keyOf(lt), &dfaasv1.LoadTest{})).To(Succeed())
		})

		It("Environment deletion that starts past its drain budget still waits for the test it deletes", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Finalizers = append(env.Finalizers, environmentFinalizer)
			Expect(k8sClient.Update(ctx, env)).To(Succeed())
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			// The operator was down when the Environment was deleted: its first
			// drain pass runs long after the request. Age the copy.
			Expect(k8sClient.Delete(ctx, env)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			past := metav1.NewTime(time.Now().Add(-envDrainBudget - time.Second))
			env.DeletionTimestamp = &past
			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			var kept dfaasv1.Environment
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), &kept)).To(Succeed())
			Expect(kept.Finalizers).To(ContainElement(environmentFinalizer))

			Expect(reconcileUntil(lt, 6, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
		})

		// ownedPair gives an Environment, finalizer on, one finished dispatched
		// test ("done", its TestRun still on gen-a) and one live test ("live").
		ownedPair := func() (*dfaasv1.Environment, *dfaasv1.LoadTest, *dfaasv1.LoadTest) {
			env := envReady("env", "gen-a")
			scriptCM("script")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Finalizers = append(env.Finalizers, environmentFinalizer)
			Expect(k8sClient.Update(ctx, env)).To(Succeed())
			done := newLT("done", "env", "gen-a")
			Expect(k8sClient.Create(ctx, done)).To(Succeed())
			running := reconcileUntil(done, 12, phaseIs(dfaasv1.LoadTestRunning))
			running.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, running)).To(Succeed())
			live := newLT("live", "env", "gen-a")
			Expect(k8sClient.Create(ctx, live)).To(Succeed())
			Expect(reconcileUntil(live, 12, phaseIs(dfaasv1.LoadTestRunning)).Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			return env, done, live
		}

		It("Environment deletion with --cascade=orphan deletes and waits for a live test, and keeps a finished one", func() {
			env, done, live := ownedPair()
			orphan := metav1.DeletePropagationOrphan
			Expect(k8sClient.Delete(ctx, env, &client.DeleteOptions{PropagationPolicy: &orphan})).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			var got dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(live), &got)).To(Succeed())
			Expect(got.DeletionTimestamp).NotTo(BeNil())

			Expect(reconcileUntil(live, 6, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Exists("gen-a", live)).To(BeFalse())
			_, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, keyOf(done), &got)).To(Succeed())
			Expect(got.DeletionTimestamp).To(BeNil())
			Expect(fleet.Exists("gen-a", done)).To(BeTrue())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			Expect(env.Finalizers).NotTo(ContainElement(environmentFinalizer)) // "orphan" stays: no GC in envtest
		})

		It("Environment deletion keeps a finished test the garbage collector orphaned, and still waits for a live one", func() {
			env, done, live := ownedPair()
			sec := kubeconfigSecret("env-gen-a-kubeconfig", "env", "gen-a")
			sec.Namespace = env.Namespace
			Expect(k8sClient.Create(ctx, sec)).To(Succeed())
			// --cascade=orphan with the garbage collector done before the first
			// drain pass: no ownerRef on either test, no "orphan" finalizer left.
			for _, lt := range []*dfaasv1.LoadTest{done, live} {
				Expect(k8sClient.Get(ctx, keyOf(lt), lt)).To(Succeed())
				lt.OwnerReferences = nil
				Expect(k8sClient.Update(ctx, lt)).To(Succeed())
			}
			Expect(k8sClient.Delete(ctx, env)).To(Succeed())
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			// One pass of its finalizer ends the live test: terminal, still
			// finalizing, no ownerRef. The drain still waits, with the Secret.
			ended := reconcileUntil(live, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(ended.Status.Phase).To(Equal(dfaasv1.LoadTestAborted))
			res, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(sec), sec)).To(Succeed())

			Expect(reconcileUntil(live, 6, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Exists("gen-a", live)).To(BeFalse())
			_, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env))).To(BeTrue())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(sec), sec))).To(BeTrue())
			var kept dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(done), &kept)).To(Succeed())
			Expect(kept.DeletionTimestamp).To(BeNil())
			Expect(fleet.Exists("gen-a", done)).To(BeTrue())
		})

		It("a deleted test whose budget is spent is released with no remote call", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			running.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, running)).To(Succeed())
			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())

			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			past := metav1.NewTime(time.Now().Add(-deletionReclaimBudget - time.Second))
			fresh.DeletionTimestamp = &past
			// Dispatch itself made one stale-wipe Delete call and one Stage call
			// (to confirm ErrNotFound) before Apply; the budget-spent path must
			// add neither on top of them.
			stagedBefore := len(fleet.Staged())
			deletedBefore := len(fleet.Deleted())
			_, err := r.handleLoadTestDeletion(ctx, &fresh)
			Expect(err).NotTo(HaveOccurred())
			Expect(fleet.Staged()).To(HaveLen(stagedBefore))
			Expect(fleet.Deleted()).To(HaveLen(deletedBefore))
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, keyOf(lt), &dfaasv1.LoadTest{}))).To(BeTrue())
		})

		It("a RunnersUnreclaimed test past its budget still gets one Delete pass before releasing", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			// A terminal RunnersUnreclaimed test still holds its TestRun on the
			// generator (Delete failed on it at run end) — Occupancy is what
			// keeps the Environment busy on it, so at most one such test exists
			// per Environment.
			running.Status.Phase = dfaasv1.LoadTestFailed
			meta.SetStatusCondition(&running.Status.Conditions, metav1.Condition{
				Type: dfaasv1.LTCondK6Healthy, Status: metav1.ConditionFalse,
				Reason: dfaasv1.LTReasonRunnersUnreclaimed, Message: "gen-a held its TestRun",
			})
			Expect(k8sClient.Status().Update(ctx, running)).To(Succeed())
			Expect(fleet.Exists("gen-a", lt)).To(BeTrue())
			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())

			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			past := metav1.NewTime(time.Now().Add(-deletionReclaimBudget - time.Second))
			fresh.DeletionTimestamp = &past
			deletedBefore := len(fleet.Deleted())
			_, err := r.handleLoadTestDeletion(ctx, &fresh)
			Expect(err).NotTo(HaveOccurred())
			Expect(fleet.Deleted()).To(HaveLen(deletedBefore + 1))
			Expect(fleet.Deleted()[deletedBefore]).To(Equal("gen-a|lt-gen-a"))
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, keyOf(lt), &dfaasv1.LoadTest{}))).To(BeTrue())
		})

		It("deleting a finished test deletes its mirrored script on the generator", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			running := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fleet.ScriptExists("gen-a", "script")).To(BeTrue())
			running.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, running)).To(Succeed())

			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			Expect(reconcileUntil(lt, 8, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.ScriptExists("gen-a", "script")).To(BeFalse())
		})

		It("keeps a script another test re-mirrored under the same name", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			first := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			first.Status.Phase = dfaasv1.LoadTestCompleted
			Expect(k8sClient.Status().Update(ctx, first)).To(Succeed())
			lt2 := newLT("lt2", "env", "gen-a") // same "script" name
			Expect(k8sClient.Create(ctx, lt2)).To(Succeed())
			reconcileUntil(lt2, 12, phaseIs(dfaasv1.LoadTestRunning))

			Expect(k8sClient.Delete(ctx, lt)).To(Succeed())
			Expect(reconcileUntil(lt, 8, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.ScriptExists("gen-a", "script")).To(BeTrue())
		})
	})
	// One fleet round per tick: every generator is polled once and the Retry
	// counter is charged once per round. Before, a healthy generator's
	// success reset the counter every pass, so one generator down behind a
	// healthy one never exhausted the budget and the test stayed Running.
	Context("fleet round", func() {
		remoteDown := fmt.Errorf("remote k3s API down")
		awaiting := func(l *dfaasv1.LoadTest) bool {
			c := cond(l, dfaasv1.LTCondSyncReady)
			return c != nil && c.Reason == dfaasv1.LTReasonAwaitingRunners
		}
		runningOn := func(ids ...string) *dfaasv1.LoadTest {
			envReady("env", ids...)
			scriptCM("script")
			lt := newLT("lt", "env", ids...)
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			return lt
		}
		refPhase := func(l *dfaasv1.LoadTest, nodeID string) string {
			for _, ref := range l.Status.TestRuns {
				if ref.NodeID == nodeID {
					return ref.Phase
				}
			}
			return "<none>"
		}

		It("a generator down behind a healthy one exhausts the observe budget", func() {
			lt := runningOn("gen-a", "gen-b")
			var fresh *dfaasv1.LoadTest
			passes := 0
			for passes < fetchRetryBudget+5 {
				fleet.FailNext("gen-b", "stage", remoteDown)
				fresh = reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
				passes++
				if fresh.Status.Phase == dfaasv1.LoadTestFailed {
					break
				}
			}
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(passes).To(BeNumerically("<=", fetchRetryBudget))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
		})

		It("a failed round still persists the stage of every generator that answered", func() {
			lt := runningOn("gen-a", "gen-b")
			fleet.SetStage("gen-b", lt, "finished")
			fleet.FailNext("gen-a", "stage", remoteDown)
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(refPhase(fresh, "gen-b")).To(Equal("finished"))
		})

		It("a fetch failure charges its own counter and leaves K6Dispatched alone", func() {
			lt := runningOn("gen-a", "gen-b")
			fleet.FailNext("gen-a", "stage", remoteDown)
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fresh.Annotations[fetchMissesAnnotation]).To(Equal(fmt.Sprintf("%d:1", fresh.Generation)))
			Expect(fresh.Annotations[dispatchAttemptsAnnotation]).NotTo(HaveSuffix(":1"))
			Expect(cond(fresh, dfaasv1.LTCondK6Dispatched).Reason).To(Equal(dfaasv1.LTReasonAllDispatched))
			h := cond(fresh, dfaasv1.LTCondK6Healthy)
			Expect(h.Status).To(Equal(metav1.ConditionUnknown))
			Expect(h.Reason).To(Equal(dfaasv1.LTReasonFetchFailed))
			Expect(h.Message).To(ContainSubstring("gen-a"))
		})

		It("the next remote round waits for the retry interval after a failed one", func() {
			lt := runningOn("gen-a")
			r.retryEvery = 0 // production pacing
			fleet.FailNext("gen-a", "stage", remoteDown)
			reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: keyOf(lt)})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			var fresh dfaasv1.LoadTest
			Expect(k8sClient.Get(ctx, keyOf(lt), &fresh)).To(Succeed())
			Expect(fresh.Annotations[fetchMissesAnnotation]).To(Equal(fmt.Sprintf("%d:1", fresh.Generation)))
		})

		It("a GO publish that keeps failing past the barrier budget fails the test", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 10, awaiting)
			fleet.SetStage("gen-a", lt, "started")
			fleet.SetStage("gen-b", lt, "started")
			channel.PublishErr = fmt.Errorf("filer down")
			waiting := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(waiting.Status.Phase).NotTo(Equal(dfaasv1.LoadTestFailed))

			for i := range waiting.Status.Conditions {
				if waiting.Status.Conditions[i].Type == dfaasv1.LTCondK6Dispatched {
					waiting.Status.Conditions[i].LastTransitionTime = metav1.NewTime(time.Now().Add(-syncWaitBudget - time.Minute))
				}
			}
			Expect(k8sClient.Status().Update(ctx, waiting)).To(Succeed())
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondSyncReady).Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
		})

		It("a runner that finished before the GO signal fails the test", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 10, awaiting)
			fleet.SetStage("gen-a", lt, "finished")
			fleet.SetStage("gen-b", lt, "started")
			fresh := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondSyncReady).Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
			Expect(channel.Count("PublishGo")).To(BeZero())
		})

		It("a GO already published finishes the dispatch without publishing again", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			parked := reconcileUntil(lt, 10, awaiting)
			meta.SetStatusCondition(&parked.Status.Conditions, metav1.Condition{Type: dfaasv1.LTCondSyncReady,
				Status: metav1.ConditionTrue, Reason: dfaasv1.LTReasonGoPublished, Message: "GO signal published"})
			Expect(k8sClient.Status().Update(ctx, parked)).To(Succeed())
			fleet.SetStage("gen-a", lt, "finished")
			fresh := reconcileUntil(lt, 2, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			Expect(channel.Count("PublishGo")).To(BeZero())
		})
	})

	// A scheduled test is created suspended with a startAt (that is what the
	// SPA sends); the operator lifts the suspension itself at fire time. These
	// pin that the fire lands on a parked test instead of tripping the
	// "startAt without suspended" guard meant for a kubectl apply.
	Context("scheduled start (spec.startAt)", func() {
		scheduledReason := func(l *dfaasv1.LoadTest) string {
			if c := cond(l, dfaasv1.LTCondScheduled); c != nil {
				return c.Reason
			}
			return ""
		}
		newScheduled := func(at time.Time) *dfaasv1.LoadTest {
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.Suspended = true
			lt.Spec.StartAt = &metav1.Time{Time: at}
			return lt
		}
		moveStartAtToPast := func(lt *dfaasv1.LoadTest) {
			patch := fmt.Sprintf(`{"spec":{"startAt":%q}}`, time.Now().Add(-time.Minute).UTC().Format(time.RFC3339))
			Expect(k8sClient.Patch(ctx, lt, client.RawPatch(types.MergePatchType, []byte(patch)))).To(Succeed())
		}

		It("parks an armed scheduled test at Pending, then fires and dispatches it", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newScheduled(time.Now().Add(time.Hour))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			armed := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool {
				return scheduledReason(l) == dfaasv1.LTReasonScheduledArmed
			})
			Expect(armed.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
			Expect(fleet.Applied()).To(BeEmpty())

			moveStartAtToPast(lt)
			fresh := reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			Expect(scheduledReason(fresh)).To(Equal(dfaasv1.LTReasonScheduledFired))
			Expect(fleet.Applied()).NotTo(BeEmpty())
		})

		It("fires a scheduled test whose startAt already passed instead of failing it", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newScheduled(time.Now().Add(-time.Minute))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fresh := reconcileUntil(lt, 12, func(l *dfaasv1.LoadTest) bool {
				return l.Status.Phase == dfaasv1.LoadTestRunning || l.Status.Phase == dfaasv1.LoadTestFailed
			})
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
			Expect(scheduledReason(fresh)).To(Equal(dfaasv1.LTReasonScheduledFired))
			Expect(fleet.Applied()).NotTo(BeEmpty())
		})

		It("waits at Pending when the schedule fires on a non-Ready Environment, then fires once it is Ready", func() {
			env := envReady("env", "gen-a")
			env.Status.Phase = dfaasv1.EnvProvisioningInfra
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			scriptCM("script")
			lt := newScheduled(time.Now().Add(-time.Minute))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			delayed := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool {
				return scheduledReason(l) == dfaasv1.LTReasonScheduledDelayedEnvNot
			})
			Expect(delayed.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
			Expect(fleet.Applied()).To(BeEmpty())

			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Status.Phase = dfaasv1.EnvReady
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			fresh := reconcileUntil(lt, 12, func(l *dfaasv1.LoadTest) bool {
				return l.Status.Phase == dfaasv1.LoadTestRunning || l.Status.Phase == dfaasv1.LoadTestFailed
			})
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		})

		It("Start on an armed scheduled test dispatches it now, startAt left in place", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newScheduled(time.Now().Add(time.Hour))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			armed := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool {
				return scheduledReason(l) == dfaasv1.LTReasonScheduledArmed
			})
			Expect(armed.Status.Phase).To(Equal(dfaasv1.LoadTestPending))

			// What a pre-fix gateway's Activate or a kubectl patch sends.
			Expect(k8sClient.Patch(ctx, lt, client.RawPatch(types.MergePatchType,
				[]byte(`{"spec":{"suspended":false}}`)))).To(Succeed())
			fresh := reconcileUntil(lt, 12, func(l *dfaasv1.LoadTest) bool {
				return l.Status.Phase == dfaasv1.LoadTestRunning || l.Status.Phase == dfaasv1.LoadTestFailed
			})
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestRunning))
		})

		It("an armed scheduled test is owned by its Environment before it fires", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			lt := newScheduled(time.Now().Add(time.Hour))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			armed := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool {
				return scheduledReason(l) == dfaasv1.LTReasonScheduledArmed
			})
			Expect(armed.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
			Expect(hasOwnerRef(armed, env)).To(BeTrue())
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("a scheduled test delayed on a non-Ready Environment is owned by it", func() {
			env := envReady("env", "gen-a")
			env.Status.Phase = dfaasv1.EnvProvisioningInfra
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			scriptCM("script")
			lt := newScheduled(time.Now().Add(-time.Minute))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			delayed := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool {
				return scheduledReason(l) == dfaasv1.LTReasonScheduledDelayedEnvNot
			})
			Expect(hasOwnerRef(delayed, env)).To(BeTrue())
		})

		It("Environment deletion drains an armed scheduled test with no remote call", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			env.Finalizers = append(env.Finalizers, environmentFinalizer)
			Expect(k8sClient.Update(ctx, env)).To(Succeed())
			lt := newScheduled(time.Now().Add(time.Hour))
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool {
				return scheduledReason(l) == dfaasv1.LTReasonScheduledArmed
			})

			Expect(k8sClient.Delete(ctx, env)).To(Succeed())
			er := &EnvironmentReconciler{Client: k8sClient, Scheme: scheme.Scheme}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env)).To(Succeed())
			res, err := er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(reconcileUntil(lt, 4, func(*dfaasv1.LoadTest) bool { return false })).To(BeNil())
			Expect(fleet.Deleted()).To(BeEmpty())
			_, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env))).To(BeTrue())
		})

		It("still fails a test created with startAt but without suspended", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.StartAt = &metav1.Time{Time: time.Now().Add(time.Hour)}
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondReady).Message).To(ContainSubstring("spec.startAt requires spec.suspended=true"))
			Expect(fleet.Applied()).To(BeEmpty())
		})
	})
	// phase "" means "not yet admitted", nothing more. A test that was never
	// queued used to keep "" through its whole dispatch and the Sync barrier,
	// so the create-time gate fired mid-run with the create-time message, and
	// a Pending test holding TestRuns waited forever on an Environment that
	// never recovered.
	Context("admission", func() {
		setEnvPhase := func(p dfaasv1.EnvironmentPhase) {
			var env dfaasv1.Environment
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "env", Namespace: ns}, &env)).To(Succeed())
			env.Status.Phase = p
			Expect(k8sClient.Status().Update(ctx, &env)).To(Succeed())
		}

		It("admits every test at Pending before anything remote happens", func() {
			envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fresh := reconcileUntil(lt, 5, func(l *dfaasv1.LoadTest) bool { return l.Status.Phase != "" })
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestPending))
			Expect(fleet.Applied()).To(BeEmpty())
		})

		It("an Environment lost mid-barrier ends the run with its own failure, not the create-time one", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			lt.Spec.SyncStart = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 10, func(l *dfaasv1.LoadTest) bool {
				c := cond(l, dfaasv1.LTCondSyncReady)
				return c != nil && c.Reason == dfaasv1.LTReasonAwaitingRunners
			})

			setEnvPhase(dfaasv1.EnvUnreachable)
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			msg := cond(fresh, dfaasv1.LTCondReady).Message
			Expect(msg).NotTo(ContainSubstring("before creating a LoadTest"))
			Expect(msg).To(ContainSubstring("Unreachable"))
			Expect(cond(fresh, dfaasv1.LTCondSyncReady).Reason).To(Equal(dfaasv1.LTReasonSyncTimeout))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
			Expect(fleet.Exists("gen-b", lt)).To(BeFalse())
		})

		It("a Pending test holding TestRuns fails and reclaims them when its Environment stops being dispatchable", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			fleet.FailNext("gen-b", "apply", fmt.Errorf("remote down"))
			partial := reconcileUntil(lt, 6, func(l *dfaasv1.LoadTest) bool { return len(l.Status.TestRuns) == 1 })
			if partial.Status.Phase != dfaasv1.LoadTestPending {
				partial.Status.Phase = dfaasv1.LoadTestPending // what a queued-then-dispatched test looks like
				Expect(k8sClient.Status().Update(ctx, partial)).To(Succeed())
			}

			setEnvPhase(dfaasv1.EnvProvisioningVMs)
			fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(fleet.Exists("gen-a", lt)).To(BeFalse())
		})

		It("a test that dispatched nothing waits for its Environment without rewriting its status", func() {
			env := envReady("env", "gen-a")
			env.Status.Phase = dfaasv1.EnvProvisioningInfra
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			lt.Spec.Suspended = true
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestPending))
			Expect(k8sClient.Patch(ctx, lt, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"suspended":false}}`)))).To(Succeed())
			reconcileUntil(lt, 2, func(*dfaasv1.LoadTest) bool { return false })

			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: keyOf(lt)})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(res.Requeue).To(BeFalse())
			Expect(fleet.Applied()).To(BeEmpty())
		})
	})
	// The background re-sweep of an unreclaimed runner is paced like every
	// other failed remote round: it runs off every watch event (its own
	// terminal write, each Environment health write), and each try against a
	// dead k3s API stalls the single LoadTest worker for the remote timeout.
	Context("reclaim pacing", func() {
		It("does not re-sweep an unreclaimed runner before the retry interval", func() {
			envReady("env", "gen-a", "gen-b")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a", "gen-b")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))
			Expect(k8sClient.Patch(ctx, lt, client.RawPatch(types.MergePatchType, []byte(`{"spec":{"stop":true}}`)))).To(Succeed())
			fleet.FailNext("gen-b", "delete", fmt.Errorf("remote k3s API down"))
			aborted := reconcileUntil(lt, 1, func(*dfaasv1.LoadTest) bool { return false })
			Expect(cond(aborted, dfaasv1.LTCondK6Healthy).Reason).To(Equal(dfaasv1.LTReasonRunnersUnreclaimed))

			r.retryEvery = 0 // production pacing
			deletes := len(fleet.Deleted())
			res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: keyOf(lt)})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))
			Expect(fleet.Deleted()).To(HaveLen(deletes), "no remote call inside the retry interval")
		})
	})
})

// perCallChannel answers each SummaryURL call with a URL of its own, so a spec
// can tell the URL a caller was handed from one it recomputed.
type perCallChannel struct {
	*syncfake.Channel
	calls atomic.Int32
}

func (c *perCallChannel) SummaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	return fmt.Sprintf("%s?call=%d", c.Channel.SummaryURL(lt, nodeID), c.calls.Add(1))
}
