package reach

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
)

// A Node with no ipAddress must count as unreachable rather than be skipped:
// there is nothing to dial, and treating it as up would let provisioning
// proceed against a machine the operator cannot reach. No dial happens on this
// path, so this test touches no network.
func TestTCPTreatsMissingAddressAsUnreachable(t *testing.T) {
	env := &dfaasv1.Environment{
		ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default"},
		Spec: dfaasv1.EnvironmentSpec{Nodes: []dfaasv1.EnvironmentNode{
			{NodeID: "w1"},
			{NodeID: "g4"},
		}},
	}

	got := TCP{}.Unreachable(context.Background(), env)
	if len(got) != 2 || got[0] != "w1" || got[1] != "g4" {
		t.Fatalf("want [w1 g4] in spec order, got %v", got)
	}
}

func TestTCPNoNodesIsNoUnreachable(t *testing.T) {
	env := &dfaasv1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "bari"}}
	if got := (TCP{}).Unreachable(context.Background(), env); len(got) != 0 {
		t.Fatalf("want none, got %v", got)
	}
}
