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
				{NodeID: "worker", IPAddress: "10.0.0.1", Role: dfaasv1.RoleDfaasWorker, Capacity: "LOW", Username: "u", Password: "p"},
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
		fresh := reconcileUntil(lt, 4, phaseIs(dfaasv1.LoadTestFailed))
		Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
		Expect(cond(fresh, dfaasv1.LTCondK6Healthy).Reason).To(Equal(dfaasv1.LTReasonAllFailed))
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

		It("a node that left the Environment mid-test fails the test on the next observe", func() {
			env := envReady("env", "gen-a")
			scriptCM("script")
			lt := newLT("lt", "env", "gen-a")
			Expect(k8sClient.Create(ctx, lt)).To(Succeed())
			reconcileUntil(lt, 12, phaseIs(dfaasv1.LoadTestRunning))

			env.Status.K6Nodes = nil
			Expect(k8sClient.Status().Update(ctx, env)).To(Succeed())
			fresh := reconcileUntil(lt, 3, phaseIs(dfaasv1.LoadTestFailed))
			Expect(fresh.Status.Phase).To(Equal(dfaasv1.LoadTestFailed))
		})
	})
})
