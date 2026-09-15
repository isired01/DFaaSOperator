package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	reachfake "dfaas-operator/internal/reach/fake"
)

// loadSnapshot is the assertion helper: it reports whether the snapshot Secret
// exists and, when it does, its records.
func loadSnapshot(t *testing.T, c client.Client, env *dfaasv1.Environment) (map[string]ansible.ProvisionedNode, bool) {
	t.Helper()
	var sec corev1.Secret
	key := client.ObjectKey{Name: ansible.SnapshotSecretName(env), Namespace: env.Namespace}
	if err := c.Get(context.Background(), key, &sec); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false
		}
		t.Fatalf("get snapshot secret: %v", err)
	}
	m := &ansible.Manager{Client: c}
	got, found, err := m.LoadSnapshot(context.Background(), env)
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	return got, found
}

// The snapshot records what Ansible actually installed, so it may be written
// only once every provisioning stream has succeeded. Writing it while a stream
// is still running -- or after one failed -- would claim machines are installed
// that are not, and a later node removal would then diff against a lie.
func TestProvisioningInfraWritesSnapshotOnlyOnSuccess(t *testing.T) {
	for _, tc := range []struct {
		name         string
		vms, k6      jobState
		wantSnapshot bool
	}{
		{name: "both streams succeeded", vms: jobComplete, k6: jobComplete, wantSnapshot: true},
		{name: "one stream still running", vms: jobComplete, k6: jobRunning, wantSnapshot: false},
		{name: "one stream failed", vms: jobFailed, k6: jobComplete, wantSnapshot: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := faninScheme(t)
			env := faninEnv(true)
			c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).
				WithObjects(env, ansibleJob(env, "vms", tc.vms), ansibleJob(env, "k6", tc.k6)).Build()
			r := &EnvironmentReconciler{Client: c, Scheme: s}

			if _, err := r.reconcileProvisioningInfra(context.Background(), env); err != nil {
				t.Fatalf("reconcileProvisioningInfra: %v", err)
			}

			got, found := loadSnapshot(t, c, env)
			if found != tc.wantSnapshot {
				t.Fatalf("snapshot present = %v, want %v", found, tc.wantSnapshot)
			}
			if !tc.wantSnapshot {
				return
			}
			if len(got) != len(env.Spec.Nodes) {
				t.Fatalf("snapshot holds %d records, want %d", len(got), len(env.Spec.Nodes))
			}
			for i, n := range env.Spec.Nodes {
				rec, ok := got[n.NodeID]
				if !ok {
					t.Fatalf("node %q missing from the snapshot", n.NodeID)
				}
				if rec.IPAddress != n.IPAddress || rec.Password != n.Password || rec.Position != i {
					t.Errorf("node %q recorded as %+v, want it to mirror the spec entry at position %d",
						n.NodeID, rec, i)
				}
			}
		})
	}
}

// An Environment that predates the snapshot Secret has no record, so its FIRST
// node removal after an operator upgrade would have nothing to diff against and
// would silently leave a live machine in the federation. A settled Environment
// is, by the FSM's own invariant, installed exactly as spec.nodes describes, so
// the reconciler seeds the snapshot from the spec — once, without clobbering.
func TestSettledEnvironmentSeedsMissingSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		phase      dfaasv1.EnvironmentPhase
		observed   int64
		generation int64
		wantSeed   bool
	}{
		{name: "Ready and settled", phase: dfaasv1.EnvReady, observed: 3, generation: 3, wantSeed: true},
		{name: "Failed but settled", phase: dfaasv1.EnvFailed, observed: 3, generation: 3, wantSeed: true},
		{
			// Spec edited: the machines no longer match spec.nodes, so seeding
			// from it would record a state that was never installed.
			name: "spec edited after the last run", phase: dfaasv1.EnvReady,
			observed: 2, generation: 3, wantSeed: false,
		},
		{
			// Never provisioned: nothing is installed, so there is nothing to
			// record and a removal must not decommission anything.
			name: "never provisioned", phase: "", observed: 0, generation: 1, wantSeed: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := faninScheme(t)
			env := faninEnv(true)
			env.Generation = tc.generation
			env.Status.Phase = tc.phase
			env.Status.ObservedGeneration = tc.observed
			// Any Environment that has reached a settled phase has had the
			// finalizer stamped on its first ever reconcile; without it here the
			// reconcile returns right after adding one and never reaches the seed.
			env.Finalizers = []string{environmentFinalizer}

			c := crfake.NewClientBuilder().WithScheme(s).WithStatusSubresource(env).
				WithObjects(env).Build()
			r := &EnvironmentReconciler{Client: c, Scheme: s, Prober: &reachfake.Nodes{}}

			if _, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKeyFromObject(env),
			}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			if _, found := loadSnapshot(t, c, env); found != tc.wantSeed {
				t.Errorf("snapshot seeded = %v, want %v", found, tc.wantSeed)
			}
		})
	}
}
