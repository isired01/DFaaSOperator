package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// staleReaderEnv is a client.Reader that always returns the Environment it was
// built with — standing in for the API server while the informer cache still
// holds an older copy.
type staleReaderEnv struct {
	fresh *dfaasv1.Environment
	err   error
}

func (s staleReaderEnv) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	if s.err != nil {
		return s.err
	}
	out, ok := obj.(*dfaasv1.Environment)
	if !ok {
		return nil
	}
	s.fresh.DeepCopyInto(out)
	return nil
}

func (s staleReaderEnv) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return nil
}

func envWithHealthCheck(at *time.Time) *dfaasv1.Environment {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default"},
	}
	if at != nil {
		t := metav1.NewTime(*at)
		env.Status.LastHealthCheck = &t
	}
	return env
}

// The bug this pins down: reconcileReadyHealth throttles on
// status.lastHealthCheck read from the informer CACHE, which lags the
// controller's own status write. A reconcile triggered by that very write saw
// the previous timestamp, judged the interval elapsed and probed again
// milliseconds later — so healthRetryBudget stopped spreading the misses over
// several intervals and collapsed into a couple of seconds. Observed live on a
// stopped node: the counter went 1 → 3 in 22 s, with only 2 s between the last
// two probes.
func TestProbeThrottleIgnoresStaleCachedTimestamp(t *testing.T) {
	const due = 20 * time.Second

	stale := time.Now().Add(-due - time.Second) // cache: interval elapsed, probe away
	fresh := time.Now().Add(-2 * time.Second)   // api server: we probed 2s ago

	r := &EnvironmentReconciler{APIReader: staleReaderEnv{fresh: envWithHealthCheck(&fresh)}}

	wait := r.probeThrottle(context.Background(), envWithHealthCheck(&stale), due)
	if wait <= 0 {
		t.Fatalf("probe allowed on a stale cached timestamp: want a wait > 0, got %v", wait)
	}
	if wait > due {
		t.Fatalf("wait %v exceeds the interval %v", wait, due)
	}
}

func TestProbeThrottleAllowsProbeWhenGenuinelyDue(t *testing.T) {
	const due = 20 * time.Second
	old := time.Now().Add(-due - time.Second)

	r := &EnvironmentReconciler{APIReader: staleReaderEnv{fresh: envWithHealthCheck(&old)}}

	if wait := r.probeThrottle(context.Background(), envWithHealthCheck(&old), due); wait != 0 {
		t.Fatalf("probe blocked although the interval elapsed on both copies: got %v", wait)
	}
}

// The cached copy alone is enough to say "too soon" — no live read needed.
func TestProbeThrottleShortCircuitsOnCachedValue(t *testing.T) {
	const due = 20 * time.Second
	recent := time.Now().Add(-time.Second)

	r := &EnvironmentReconciler{APIReader: staleReaderEnv{err: context.DeadlineExceeded}}

	if wait := r.probeThrottle(context.Background(), envWithHealthCheck(&recent), due); wait <= 0 {
		t.Fatalf("cached timestamp says 1s ago, want a wait, got %v", wait)
	}
}

// The throttle is an optimisation, never a correctness gate: without an
// APIReader (unit tests) or when the live read fails, fall back to the cached
// decision rather than blocking the probe.
func TestProbeThrottleFallsBackWhenLiveReadUnavailable(t *testing.T) {
	const due = 20 * time.Second
	old := time.Now().Add(-due - time.Second)

	for name, r := range map[string]*EnvironmentReconciler{
		"nil APIReader": {},
		"failing read":  {APIReader: staleReaderEnv{err: context.DeadlineExceeded}},
	} {
		t.Run(name, func(t *testing.T) {
			if wait := r.probeThrottle(context.Background(), envWithHealthCheck(&old), due); wait != 0 {
				t.Fatalf("want fallback to the cached decision (0), got %v", wait)
			}
		})
	}
}

func TestProbeThrottleNeverProbedYet(t *testing.T) {
	r := &EnvironmentReconciler{APIReader: staleReaderEnv{fresh: envWithHealthCheck(nil)}}
	if wait := r.probeThrottle(context.Background(), envWithHealthCheck(nil), time.Minute); wait != 0 {
		t.Fatalf("a never-probed Environment must probe immediately, got %v", wait)
	}
}
