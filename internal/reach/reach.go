// Package reach answers one question about an Environment: which of its Nodes
// do not answer on :22?
//
// It exists because the answer used to come from a package-scope net.DialTimeout
// with no injection point, and the same loop -- for every declared Node, dial
// :22, collect the unreachable nodeIDs -- was written out three times in the
// reconciler. The only way a test could steer it was by choosing an IP address,
// and no test anywhere stood up a listener, so every Environment spec used the
// unroutable 192.0.2.1 and ONLY the unreachable branch was reachable at all.
// The whole auto-recovery path (reconcileUnreachable, both of its arms) was
// referenced in zero test files as a direct consequence, and each unroutable
// dial burned its full timeout.
package reach

import (
	"context"
	"net"
	"time"

	dfaasv1 "dfaas-operator/api/v1"
)

// defaultTimeout caps each per-host dial. Kept here rather than in the
// reconciler so a zero-value TCP{} is usable.
const defaultTimeout = 2 * time.Second

// Prober answers reachability for a whole Environment. It never errors: an
// unreachable Node is data the reconciler acts on, not a failure.
type Prober interface {
	// Unreachable returns the nodeIDs of env.Spec.Nodes that did not answer on
	// :22 within the probe timeout, in spec order. Empty means all up.
	Unreachable(ctx context.Context, env *dfaasv1.Environment) []string
}

// TCP is the production adapter: a plain TCP dial to ip:22. Cheap -- it does
// NOT verify an SSH banner, which would need a real client and credentials.
type TCP struct {
	// Timeout caps each per-host dial. Zero means defaultTimeout.
	Timeout time.Duration
}

// Unreachable implements Prober. A Node with no ipAddress counts as
// unreachable: there is nothing to dial, and treating it as up would let
// provisioning proceed against a machine the operator cannot reach.
func (t TCP) Unreachable(ctx context.Context, env *dfaasv1.Environment) []string {
	timeout := t.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}

	var dialer net.Dialer
	var unreachable []string
	for _, n := range env.Spec.Nodes {
		if n.IPAddress == "" {
			unreachable = append(unreachable, n.NodeID)
			continue
		}
		dialCtx, cancel := context.WithTimeout(ctx, timeout)
		conn, err := dialer.DialContext(dialCtx, "tcp", net.JoinHostPort(n.IPAddress, "22"))
		cancel()
		if err != nil {
			unreachable = append(unreachable, n.NodeID)
			continue
		}
		_ = conn.Close()
	}
	return unreachable
}
