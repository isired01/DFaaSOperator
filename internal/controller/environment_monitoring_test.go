package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	monfake "dfaas-operator/internal/controller/monitoring/fake"
)

// ensureMonitoring encodes five outcomes and used to be reachable only through
// a real Helm install, so none of them was asserted. The one that matters most
// is the distinction between "readiness could not be evaluated" and "not ready
// yet": a refused Pod List (an RBAC regression, or the API server down) would
// otherwise be indistinguishable from a slow rollout, and the Environment
// would sit in ProvisioningMonitoring behind a reassuring "pods not Ready yet".

func monEnv(t *testing.T) (client.Client, *dfaasv1.Environment) {
	t.Helper()
	s := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default", Generation: 1},
		Status:     dfaasv1.EnvironmentStatus{Phase: dfaasv1.EnvProvisioningMonitoring},
	}
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env).Build()
	return c, env
}

func monCond(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	var out dfaasv1.Environment
	if err := c.Get(context.Background(), types.NamespacedName{Name: "bari", Namespace: "default"}, &out); err != nil {
		t.Fatalf("get environment: %v", err)
	}
	cond := meta.FindStatusCondition(out.Status.Conditions, dfaasv1.EnvCondMonitoringReady)
	if cond == nil {
		t.Fatal("MonitoringReady was never stamped")
	}
	return cond
}

func TestEnsureMonitoringOutcomes(t *testing.T) {
	helmDown := errors.New("helm: connection refused")
	podsUnlistable := errors.New("pods is forbidden: RBAC")

	cases := []struct {
		name string
		// stack behaviour
		deployErrs   []error
		checkResults []monfake.CheckResult
		// how many failed rounds already happened, as the Retry counter sees it
		priorFailures int
		wantDone      bool
		wantFailed    bool
		wantStatus    metav1.ConditionStatus
		wantReason    string
	}{
		{
			name:       "the stack is up",
			wantDone:   true,
			wantStatus: metav1.ConditionTrue,
			wantReason: dfaasv1.EnvReasonPodsRunning,
		},
		{
			// The pods are not up yet. Ordinary, and must read as progress.
			name:         "pods not ready yet",
			checkResults: []monfake.CheckResult{{Ready: false}},
			wantStatus:   metav1.ConditionFalse,
			wantReason:   dfaasv1.EnvReasonWaitingPods,
		},
		{
			// NOT the same thing: the readiness could not be evaluated at all.
			name:         "readiness could not be evaluated",
			checkResults: []monfake.CheckResult{{Err: podsUnlistable}},
			wantStatus:   metav1.ConditionUnknown,
			wantReason:   dfaasv1.EnvReasonCheckFailed,
		},
		{
			// First ever observation of a failing install is Unknown, not
			// False: one failed Helm attempt is not yet news.
			name:       "the first install failure is Unknown",
			deployErrs: []error{helmDown},
			wantStatus: metav1.ConditionUnknown,
			wantReason: dfaasv1.EnvReasonHelmInstalling,
		},
		{
			name:          "a later install failure is False",
			deployErrs:    []error{helmDown},
			priorFailures: 1,
			wantStatus:    metav1.ConditionFalse,
			wantReason:    dfaasv1.EnvReasonHelmInstalling,
		},
		{
			// The budget is exhausted, so the caller must go terminal.
			name:          "the budget is exhausted",
			deployErrs:    []error{helmDown},
			priorFailures: monitoringRetryBudget - 1,
			wantFailed:    true,
			wantStatus:    metav1.ConditionFalse,
			wantReason:    dfaasv1.EnvReasonHelmFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, env := monEnv(t)
			stack := &monfake.Stack{DeployErrs: tc.deployErrs, CheckResults: tc.checkResults}
			r := &EnvironmentReconciler{Client: c, Monitoring: stack}

			// Seed the Retry counter. It is generation-scoped, so it is written
			// in the "<generation>:<count>" form the status writer owns.
			if tc.priorFailures > 0 {
				seedCounter(t, c, env, monitoringAttemptsAnnotation, tc.priorFailures)
			}

			done, failed := r.ensureMonitoring(context.Background(), env)
			if done != tc.wantDone {
				t.Errorf("done: want %v, got %v", tc.wantDone, done)
			}
			if failed != tc.wantFailed {
				t.Errorf("failed: want %v, got %v", tc.wantFailed, failed)
			}

			cond := monCond(t, c)
			if cond.Status != tc.wantStatus {
				t.Errorf("status: want %s, got %s", tc.wantStatus, cond.Status)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf("reason: want %s, got %s (%q)", tc.wantReason, cond.Reason, cond.Message)
			}

			// Targets are reconciled only once the stack is actually up.
			if got := len(stack.TargetRounds()); tc.wantDone && got != 1 {
				t.Errorf("want the Prometheus targets reconciled once, got %d rounds", got)
			} else if !tc.wantDone && got != 0 {
				t.Errorf("targets reconciled while the stack was not up (%d rounds)", got)
			}
		})
	}
}

// A failing Deploy must never reach Check: the readiness of a stack that was
// not installed is not a question worth asking, and asking it would produce
// the wrong Condition.
func TestEnsureMonitoringDoesNotCheckAfterAFailedDeploy(t *testing.T) {
	c, env := monEnv(t)
	stack := &monfake.Stack{DeployErrs: []error{errors.New("helm: no such host")}}
	r := &EnvironmentReconciler{Client: c, Monitoring: stack}

	r.ensureMonitoring(context.Background(), env)
	if stack.Checks() != 0 {
		t.Errorf("Check was called %d times after a failed Deploy", stack.Checks())
	}
}

// seedCounter writes a generation-scoped Retry counter directly, so a test can
// start from "this has already failed N times" without N reconciles.
func seedCounter(t *testing.T, c client.Client, env *dfaasv1.Environment, key string, count int) {
	t.Helper()
	var live dfaasv1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &live); err != nil {
		t.Fatalf("get environment: %v", err)
	}
	if live.Annotations == nil {
		live.Annotations = map[string]string{}
	}
	live.Annotations[key] = fmt.Sprintf("%d:%d", live.Generation, count)
	if err := c.Update(context.Background(), &live); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
	env.Annotations = live.Annotations
	env.ResourceVersion = live.ResourceVersion
}

// parseFailedTask is a pure string -> string with a documented scan rule -- the
// last "TASK [..]" before the last FAILED!/fatal: line -- and it feeds the
// Condition message the SPA renders verbatim. It had no test.
func TestParseFailedTask(t *testing.T) {
	cases := []struct {
		name string
		log  string
		want string
	}{
		{
			name: "the task before the failure, not the last task in the log",
			log: strings.Join([]string{
				"TASK [install k3s] ***",
				"ok: [10.0.0.1]",
				"TASK [configure haproxy] ***",
				`fatal: [10.0.0.1]: FAILED! => {"msg": "no such file"}`,
				"TASK [never reached] ***",
			}, "\n"),
			want: "configure haproxy",
		},
		{
			name: "the LAST failure wins when a playbook reports several",
			log: strings.Join([]string{
				"TASK [first] ***",
				"fatal: [10.0.0.1]: FAILED! => {}",
				"TASK [second] ***",
				"fatal: [10.0.0.2]: FAILED! => {}",
			}, "\n"),
			want: "second",
		},
		{
			name: "a clean run names no task",
			log:  "TASK [install k3s] ***\nok: [10.0.0.1]\nPLAY RECAP ***",
			want: "",
		},
		{
			name: "a failure with no preceding task names nothing",
			log:  `fatal: [10.0.0.1]: FAILED! => {"msg": "unreachable"}`,
			want: "",
		},
		{
			name: "an empty log names nothing",
			log:  "",
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseFailedTask(tc.log); got != tc.want {
				t.Errorf("parseFailedTask = %q, want %q", got, tc.want)
			}
		})
	}
}
