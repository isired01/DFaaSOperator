package controller

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	"dfaas-operator/internal/controller/roles"
)

// The ProvisioningInfra fan-in: one Ansible Job per Role, both driven every
// tick, and the Environment advances only once BOTH streams are terminal.
// reconcileProvisioningInfra was referenced in zero test files, which left two
// rules unguarded -- the terminal-state matrix itself, and the ordering rule
// that the success TTL is stamped ONLY after both streams settle. Stamping it
// earlier lets the Job controller delete a finished Job while its sibling is
// still running; the next reconcile then hits NotFound, recreates the Job, and
// re-runs the playbook against machines that were already provisioned.

type jobState int

const (
	jobRunning jobState = iota
	jobComplete
	jobFailed
	jobAbsent
)

func faninScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		dfaasv1.AddToScheme, corev1.AddToScheme, batchv1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func ansibleJob(env *dfaasv1.Environment, suffix string, state jobState) *batchv1.Job {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ansible.JobNameForRole(env, suffix),
			Namespace: env.Namespace,
		},
	}
	switch state {
	case jobComplete:
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
	case jobFailed:
		job.Status.Failed = 1
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}
	case jobRunning:
		job.Status.Active = 1
	case jobAbsent:
	}
	return job
}

func faninEnv(withK6 bool) *dfaasv1.Environment {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bari", Namespace: "default", UID: "uid-fanin", Generation: 1,
		},
		Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
			{NodeID: "w1", IPAddress: "10.0.0.1", Role: dfaasv1.RoleDfaasWorker, Username: "u", Password: "p"},
		}},
		Status: dfaasv1.EnvironmentStatus{Phase: dfaasv1.EnvProvisioningInfra},
	}
	if withK6 {
		env.Spec.Nodes = append(env.Spec.Nodes, dfaasv1.EnvironmentNode{
			NodeID: "g4", IPAddress: "10.0.0.2", Role: dfaasv1.RoleK6LoadGenerator,
			Username: "u", Password: "p",
		})
	}
	return env
}

func jobTTL(t *testing.T, c client.Client, env *dfaasv1.Environment, suffix string) *int32 {
	t.Helper()
	var job batchv1.Job
	key := types.NamespacedName{Name: ansible.JobNameForRole(env, suffix), Namespace: env.Namespace}
	if err := c.Get(context.Background(), key, &job); err != nil {
		t.Fatalf("get job %s: %v", key.Name, err)
	}
	return job.Spec.TTLSecondsAfterFinished
}

func TestProvisioningInfraFanIn(t *testing.T) {
	cases := []struct {
		name      string
		withK6    bool
		vms, k6   jobState
		wantPhase dfaasv1.EnvironmentPhase
		wantWait  time.Duration
		// the aggregate Condition the fan-in stamps once it settles
		wantInfra *metav1.ConditionStatus
	}{
		{
			name:   "both streams succeeded",
			withK6: true, vms: jobComplete, k6: jobComplete,
			wantPhase: dfaasv1.EnvProvisioningMonitoring,
			wantInfra: ptrCondStatus(metav1.ConditionTrue),
		},
		{
			// The load-bearing case: one stream is terminal, the other is not.
			// The Environment must wait, and NEITHER Job may get the success
			// TTL yet.
			name:   "one stream still running holds the fan-in",
			withK6: true, vms: jobComplete, k6: jobRunning,
			wantPhase: dfaasv1.EnvProvisioningInfra,
			wantWait:  10 * time.Second,
		},
		{
			// Both settled, one failed: the Environment fails, and both
			// per-stream Conditions stay coherent ("dfaas failed, k6 OK").
			name:   "one stream failed after both settled",
			withK6: true, vms: jobFailed, k6: jobComplete,
			wantPhase: dfaasv1.EnvFailed,
			wantInfra: ptrCondStatus(metav1.ConditionFalse),
		},
		{
			// A Role with no nodes is skipped, and a skipped stream counts as
			// terminal-and-successful.
			name:   "a role with no nodes does not hold the fan-in",
			withK6: false, vms: jobComplete, k6: jobAbsent,
			wantPhase: dfaasv1.EnvProvisioningMonitoring,
			wantInfra: ptrCondStatus(metav1.ConditionTrue),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := faninScheme(t)
			env := faninEnv(tc.withK6)

			objs := []client.Object{env, ansibleJob(env, "vms", tc.vms)}
			if tc.withK6 && tc.k6 != jobAbsent {
				objs = append(objs, ansibleJob(env, "k6", tc.k6))
			}
			c := crfake.NewClientBuilder().WithScheme(s).
				WithStatusSubresource(env).WithObjects(objs...).Build()
			r := &EnvironmentReconciler{Client: c, Scheme: s}

			res, err := r.reconcileProvisioningInfra(context.Background(), env)
			if err != nil {
				t.Fatalf("reconcileProvisioningInfra: %v", err)
			}
			if res.RequeueAfter != tc.wantWait {
				t.Errorf("requeue: want %s, got %s", tc.wantWait, res.RequeueAfter)
			}

			var got dfaasv1.Environment
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
				t.Fatalf("get environment: %v", err)
			}
			if got.Status.Phase != tc.wantPhase {
				t.Errorf("phase: want %s, got %s", tc.wantPhase, got.Status.Phase)
			}

			infra := meta.FindStatusCondition(got.Status.Conditions, dfaasv1.EnvCondInfrastructureReady)
			if tc.wantInfra == nil {
				if infra != nil {
					t.Errorf("InfrastructureReady stamped before the fan-in settled: %s/%s",
						infra.Status, infra.Reason)
				}
			} else {
				if infra == nil {
					t.Fatal("InfrastructureReady was never stamped")
				}
				if infra.Status != *tc.wantInfra {
					t.Errorf("InfrastructureReady: want %s, got %s", *tc.wantInfra, infra.Status)
				}
			}

			// The success TTL, and only after both streams settle.
			vmsTTL := jobTTL(t, c, env, "vms")
			switch {
			case tc.wantPhase == dfaasv1.EnvProvisioningMonitoring:
				if vmsTTL == nil || *vmsTTL != jobTTLSuccessSeconds {
					t.Errorf("want the success TTL on the settled dfaas-worker Job, got %v", vmsTTL)
				}
				if tc.withK6 {
					if k6TTL := jobTTL(t, c, env, "k6"); k6TTL == nil || *k6TTL != jobTTLSuccessSeconds {
						t.Errorf("want the success TTL on the settled k6 Job, got %v", k6TTL)
					}
				}
			case tc.wantWait > 0:
				if vmsTTL != nil {
					t.Errorf("the success TTL was stamped while a sibling stream was still running (%d); "+
						"the Job controller may delete this Job mid-wait and the playbook re-runs", *vmsTTL)
				}
				if k6TTL := jobTTL(t, c, env, "k6"); k6TTL != nil {
					t.Errorf("TTL stamped on the still-running k6 Job: %d", *k6TTL)
				}
			}
		})
	}
}

// A failed stream keeps its own per-stream Condition, so the panel shows which
// Role broke rather than only that the Environment failed.
func TestProvisioningInfraKeepsPerStreamConditionsCoherent(t *testing.T) {
	s := faninScheme(t)
	env := faninEnv(true)
	c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).WithObjects(
		env,
		ansibleJob(env, "vms", jobFailed),
		ansibleJob(env, "k6", jobComplete),
	).Build()
	r := &EnvironmentReconciler{Client: c, Scheme: s}

	if _, err := r.reconcileProvisioningInfra(context.Background(), env); err != nil {
		t.Fatalf("reconcileProvisioningInfra: %v", err)
	}

	var got dfaasv1.Environment
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(env), &got); err != nil {
		t.Fatalf("get environment: %v", err)
	}

	want := map[string]metav1.ConditionStatus{
		dfaasv1.EnvCondDFaaSNodesReady: metav1.ConditionFalse,
		dfaasv1.EnvCondK6Ready:         metav1.ConditionTrue,
	}
	for condType, status := range want {
		cond := meta.FindStatusCondition(got.Status.Conditions, condType)
		if cond == nil {
			t.Errorf("%s was never stamped", condType)
			continue
		}
		if cond.Status != status {
			t.Errorf("%s: want %s, got %s (%s)", condType, status, cond.Status, cond.Reason)
		}
	}

	// A failed Job gets the 24h grace TTL, not the short success one.
	if ttl := jobTTL(t, c, env, "vms"); ttl == nil || *ttl != jobTTLFailureGraceSeconds {
		t.Errorf("want the failure grace TTL on the failed Job, got %v", ttl)
	}
}

// The role table is what both streams iterate; a third role would silently
// widen the fan-in.
func TestFanInCoversEveryRole(t *testing.T) {
	if len(roles.All()) != 2 {
		t.Fatalf("the fan-in assumes two Provisioning streams, the role table has %d", len(roles.All()))
	}
}

func ptrCondStatus(s metav1.ConditionStatus) *metav1.ConditionStatus { return &s }
