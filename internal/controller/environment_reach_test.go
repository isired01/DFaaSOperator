package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	reachfake "dfaas-operator/internal/reach/fake"
)

// Auto-recovery from the non-terminal Unreachable phase was referenced in zero
// test files, because the only way to steer reachability was to choose an IP
// address and no test stood up a listener: every spec used the unroutable
// 192.0.2.1, so only the unreachable branch was reachable at all. With the
// probe behind reach.Prober both arms are table cases with no network.

func reachEnv(t *testing.T, generation, observed int64, infraReady bool) (client.Client, *dfaasv1.Environment) {
	t.Helper()
	s := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default", Generation: generation},
		Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
			{NodeID: "w1", IPAddress: "10.0.0.1", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
			{NodeID: "g4", IPAddress: "10.0.0.2", Role: dfaasv1.RoleK6LoadGenerator, Username: "u", Password: "p"},
		}},
		Status: dfaasv1.EnvironmentStatus{
			Phase:              dfaasv1.EnvUnreachable,
			ObservedGeneration: observed,
		},
	}
	if infraReady {
		meta.SetStatusCondition(&env.Status.Conditions, metav1.Condition{
			Type:   dfaasv1.EnvCondInfrastructureReady,
			Status: metav1.ConditionTrue,
			Reason: dfaasv1.EnvReasonSSHReachable, Message: "infra up",
		})
	}
	c := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(env).Build()
	return c, env
}

func reloadEnv(t *testing.T, c client.Client) *dfaasv1.Environment {
	t.Helper()
	var out dfaasv1.Environment
	if err := c.Get(context.Background(), types.NamespacedName{Name: "bari", Namespace: "default"}, &out); err != nil {
		t.Fatalf("get environment: %v", err)
	}
	return &out
}

func condOf(t *testing.T, env *dfaasv1.Environment, condType string) *metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(env.Status.Conditions, condType)
	if c == nil {
		t.Fatalf("condition %s was never stamped", condType)
	}
	return c
}

func TestReconcileUnreachableRecoveryArms(t *testing.T) {
	cases := []struct {
		name       string
		generation int64
		observed   int64
		infraReady bool
		wantPhase  dfaasv1.EnvironmentPhase
	}{
		{
			// The nodes answer again and nothing changed while they were away.
			name:       "infrastructure up and spec unchanged returns to Ready",
			generation: 4, observed: 4, infraReady: true,
			wantPhase: dfaasv1.EnvReady,
		},
		{
			// The spec was edited during the outage, so the playbooks must run
			// against the new spec before the Environment can be Ready again.
			name:       "spec drifted during the outage routes through ProvisioningInfra",
			generation: 5, observed: 4, infraReady: true,
			wantPhase: dfaasv1.EnvProvisioningInfra,
		},
		{
			// Provisioning never finished, so there is nothing to return to.
			name:       "infrastructure never became ready resumes provisioning",
			generation: 4, observed: 4, infraReady: false,
			wantPhase: dfaasv1.EnvProvisioningInfra,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, env := reachEnv(t, tc.generation, tc.observed, tc.infraReady)
			prober := &reachfake.Nodes{} // every node answers
			r := &EnvironmentReconciler{Client: c, Prober: prober}

			if _, err := r.reconcileUnreachable(context.Background(), env); err != nil {
				t.Fatalf("reconcileUnreachable: %v", err)
			}

			got := reloadEnv(t, c)
			if got.Status.Phase != tc.wantPhase {
				t.Errorf("phase: want %s, got %s", tc.wantPhase, got.Status.Phase)
			}
			if rounds := prober.Rounds(); len(rounds) != 1 {
				t.Errorf("want exactly one probe round, got %d", len(rounds))
			} else if len(rounds[0]) != 2 || rounds[0][0] != "w1" || rounds[0][1] != "g4" {
				t.Errorf("probed %v, want the declared nodes in spec order", rounds[0])
			}
		})
	}
}

// The display-truth fix: VMsReady is written by provisioning and used to be
// left untouched by the health loop, so during an outage the panel showed a
// green "VMsReady" directly above a red "NodesReachable". The two must move in
// the same Transition, in both directions.
func TestReachabilityConditionsMoveTogether(t *testing.T) {
	t.Run("recovered", func(t *testing.T) {
		c, env := reachEnv(t, 4, 4, true)
		r := &EnvironmentReconciler{Client: c, Prober: &reachfake.Nodes{}}

		if _, err := r.reconcileUnreachable(context.Background(), env); err != nil {
			t.Fatalf("reconcileUnreachable: %v", err)
		}

		got := reloadEnv(t, c)
		reachable := condOf(t, got, dfaasv1.EnvCondNodesReachable)
		vms := condOf(t, got, dfaasv1.EnvCondVMsReady)
		if reachable.Status != metav1.ConditionTrue || vms.Status != metav1.ConditionTrue {
			t.Errorf("both must be True; NodesReachable=%s VMsReady=%s", reachable.Status, vms.Status)
		}
		if reachable.Reason != vms.Reason {
			t.Errorf("reasons disagree: %s vs %s", reachable.Reason, vms.Reason)
		}
	})

	t.Run("still down", func(t *testing.T) {
		c, env := reachEnv(t, 4, 4, true)
		prober := &reachfake.Nodes{Down: []string{"g4"}}
		r := &EnvironmentReconciler{Client: c, Prober: prober}

		res, err := r.reconcileUnreachable(context.Background(), env)
		if err != nil {
			t.Fatalf("reconcileUnreachable: %v", err)
		}
		if res.RequeueAfter != unreachableRetryInterval {
			t.Errorf("requeue: want %s, got %s", unreachableRetryInterval, res.RequeueAfter)
		}

		got := reloadEnv(t, c)
		if got.Status.Phase != dfaasv1.EnvUnreachable {
			t.Errorf("phase must stay Unreachable, got %s", got.Status.Phase)
		}
		reachable := condOf(t, got, dfaasv1.EnvCondNodesReachable)
		vms := condOf(t, got, dfaasv1.EnvCondVMsReady)
		if reachable.Status != metav1.ConditionFalse || vms.Status != metav1.ConditionFalse {
			t.Errorf("both must be False; NodesReachable=%s VMsReady=%s", reachable.Status, vms.Status)
		}
		if reachable.Message != vms.Message {
			t.Errorf("messages disagree:\n  %s\n  %s", reachable.Message, vms.Message)
		}
		if !containsNode(reachable.Message, "g4") {
			t.Errorf("message does not name the unreachable node: %q", reachable.Message)
		}
	})
}

func containsNode(msg, nodeID string) bool {
	for i := 0; i+len(nodeID) <= len(msg); i++ {
		if msg[i:i+len(nodeID)] == nodeID {
			return true
		}
	}
	return false
}
