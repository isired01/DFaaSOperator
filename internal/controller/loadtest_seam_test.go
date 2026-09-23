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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
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

			exhaustBudget(lt)
			fleet.FailNext("gen-a", "stage", remoteDown) // TestRuns[0]
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
			Expect(cond(fresh, dfaasv1.LTCondReady).Reason).To(Equal(dfaasv1.LTReasonDispatchFailed))
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
			_, err = er.handleEnvDeletion(ctx, env)
			Expect(err).NotTo(HaveOccurred())
			Expect(apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(env), env))).To(BeTrue())
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
			armed := reconcileUntil(lt, 4, func(l *dfaasv1.LoadTest) bool {
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
			delayed := reconcileUntil(lt, 4, func(l *dfaasv1.LoadTest) bool {
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
			armed := reconcileUntil(lt, 4, func(l *dfaasv1.LoadTest) bool {
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
})
