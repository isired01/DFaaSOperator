/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package roles

import (
	"testing"

	dfaasv1 "dfaas-operator/api/v1"
)

func TestForKnowsBothRolesAndRejectsOthers(t *testing.T) {
	w, err := For(dfaasv1.RoleDfaasWorker)
	if err != nil || w.Playbook != "setup-nodes.yml" || w.Group != "target_nodes" || !w.NeedsLibp2pKeys {
		t.Errorf("worker: %+v err=%v", w, err)
	}
	g, err := For(dfaasv1.RoleK6LoadGenerator)
	if err != nil || g.Playbook != "setup-k6-nodes.yml" || g.Group != "k6_nodes" || g.NeedsLibp2pKeys {
		t.Errorf("generator: %+v err=%v", g, err)
	}
	if _, err := For("observer"); err == nil {
		t.Error("unknown role accepted")
	}
	if all := All(); len(all) != 2 || all[0].Role != dfaasv1.RoleDfaasWorker {
		t.Errorf("All() = %+v", all)
	}
}

// The columns three switches used to agree on by hand must be distinct per
// role, or two roles would collide on a Job/ConfigMap name.
func TestSpecsDoNotCollide(t *testing.T) {
	cols := map[string]func(Spec) string{
		"Playbook": func(s Spec) string { return s.Playbook }, "Group": func(s Spec) string { return s.Group },
		"ConfigMapSuffix": func(s Spec) string { return s.ConfigMapSuffix }, "JobSuffix": func(s Spec) string { return s.JobSuffix },
		"CondType": func(s Spec) string { return s.CondType },
	}
	for name, col := range cols {
		seen := map[string]bool{}
		for _, s := range All() {
			if seen[col(s)] {
				t.Errorf("%s %q shared between roles", name, col(s))
			}
			seen[col(s)] = true
		}
	}
}
