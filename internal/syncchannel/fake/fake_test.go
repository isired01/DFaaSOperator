/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package fake

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
	"dfaas-operator/internal/syncchannel"
)

// The fake stands in for the Filer in every reconciler spec, so its VM-facing
// URLs must follow the same per-generator precedence: a spec that passes on a
// fake with its own policy proves nothing about production.
func TestVMFacingURLsMatchTheFiler(t *testing.T) {
	lt := &dfaasv1.LoadTest{ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default"}}
	for _, fallback := range []string{"", "http://vm-facing.test:30901", "http://vm-facing.test:30901/"} {
		for _, addr := range []string{"", "100.64.0.11", "fd7a:115c:a1e0::11"} {
			c := &Channel{Public: fallback}
			f := syncchannel.NewFiler("", fallback)
			g := k6dispatch.Generator{NodeID: "Gen_A", MgmtAddr: addr}
			if got, want := c.GoURL(lt, g), f.GoURL(lt, g); got != want {
				t.Errorf("fallback %q, address %q: GoURL = %q, the Filer says %q", fallback, addr, got, want)
			}
			if got, want := c.SummaryURL(lt, g), f.SummaryURL(lt, g); got != want {
				t.Errorf("fallback %q, address %q: SummaryURL = %q, the Filer says %q", fallback, addr, got, want)
			}
			// Getters, not operations: a spec asserting "nothing was done on
			// the filer" must not see them.
			if calls := c.Calls(); len(calls) != 0 {
				t.Errorf("URL getters recorded calls %v", calls)
			}
		}
	}
}
