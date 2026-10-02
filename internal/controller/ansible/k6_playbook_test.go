/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// The k6 playbook is embedded and only ever runs against a real generator, so
// nothing exercised its structure before these tests. They pin what the
// management address detection relies on and what Go and the playbook must
// agree on: the Secret keys, the Secret name and the filer port hand-off.

const (
	k6TaskReadSSH  = "Read the SSH session addresses on the generator"
	k6TaskTake     = "Take the management node address from the SSH session"
	k6TaskProbe    = "Probe the management filer NodePort from the generator"
	k6TaskKeep     = "Keep the management node address only when the generator reaches it"
	k6TaskK3s      = "Install k3s (single-node, no Traefik, no ServiceLB)"
	k6TaskPushName = "Push kubeconfig Secret to management cluster (server-side apply)"
)

// k6Play parses the embedded k6 playbook: one play.
func k6Play(t *testing.T) map[string]any {
	t.Helper()
	var plays []map[string]any
	if err := yaml.Unmarshal([]byte(k6Playbook), &plays); err != nil {
		t.Fatalf("parse embedded setup-k6-nodes.yml: %v", err)
	}
	if len(plays) != 1 {
		t.Fatalf("want one play in setup-k6-nodes.yml, got %d", len(plays))
	}
	return plays[0]
}

func taskList(t *testing.T, v any) []map[string]any {
	t.Helper()
	raw, _ := v.([]any)
	out := make([]map[string]any, 0, len(raw))
	for i, item := range raw {
		task, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("task %d is a %T, not a mapping", i, item)
		}
		out = append(out, task)
	}
	return out
}

// k6Tasks returns the play's top-level tasks, in order.
func k6Tasks(t *testing.T) []map[string]any {
	t.Helper()
	return taskList(t, k6Play(t)["tasks"])
}

// k6AllTasks returns every task of the play, block children and handlers
// included.
func k6AllTasks(t *testing.T) []map[string]any {
	t.Helper()
	play := k6Play(t)
	var out []map[string]any
	var walk func([]map[string]any)
	walk = func(tasks []map[string]any) {
		for _, task := range tasks {
			out = append(out, task)
			for _, key := range []string{"block", "rescue", "always"} {
				walk(taskList(t, task[key]))
			}
		}
	}
	walk(taskList(t, play["tasks"]))
	walk(taskList(t, play["handlers"]))
	return out
}

// k6Task returns the top-level task named name and its position.
func k6Task(t *testing.T, name string) (int, map[string]any) {
	t.Helper()
	for i, task := range k6Tasks(t) {
		if task["name"] == name {
			return i, task
		}
	}
	t.Fatalf("setup-k6-nodes.yml has no top-level task %q", name)
	return -1, nil
}

func module(t *testing.T, task map[string]any, name string) map[string]any {
	t.Helper()
	m, ok := task[name].(map[string]any)
	if !ok {
		t.Fatalf("task %q does not use %s with a mapping of args", task["name"], name)
	}
	return m
}

// SSH_CONNECTION exists in the SSH session of the user Ansible logs in as. The
// play runs with become: true, and sudo's env_reset drops the variable, so the
// read must opt out; delegated, it would read the Job pod's environment, not
// the generator's.
func TestK6PlaybookReadsSSHConnectionOnTheGeneratorWithoutBecome(t *testing.T) {
	_, task := k6Task(t, k6TaskReadSSH)
	if cmd, _ := task["ansible.builtin.command"].(string); !strings.Contains(cmd, "SSH_CONNECTION") {
		t.Fatalf("task %q does not read SSH_CONNECTION: %v", k6TaskReadSSH, task["ansible.builtin.command"])
	}
	if task["become"] != false {
		t.Errorf("task %q must set become: false, got %v", k6TaskReadSSH, task["become"])
	}
	if _, ok := task["delegate_to"]; ok {
		t.Errorf("task %q must run on the generator, not delegate_to %v", k6TaskReadSSH, task["delegate_to"])
	}
	if task["failed_when"] != false {
		t.Errorf("task %q must never fail the play (failed_when: false), got %v", k6TaskReadSSH, task["failed_when"])
	}
}

// candidateMatch pulls the pattern out of the candidate's Jinja match test.
var candidateMatch = regexp.MustCompile(`is match\('([^']*)'\)`)

// The generator's k3s is single-stack IPv4, so its runner pods cannot dial an
// IPv6 management address, while the probe runs on the host network and would
// reach one: a recorded IPv6 address would replace a working fallback with
// URLs no runner can use. Only an IPv4 literal may become the candidate.
// Jinja's match is Python's re.match; the pattern is plain enough to mean the
// same in RE2.
func TestK6PlaybookKeepsOnlyAnIPv4Candidate(t *testing.T) {
	_, task := k6Task(t, k6TaskTake)
	expr, _ := module(t, task, "ansible.builtin.set_fact")["dfaas_mgmt_candidate"].(string)
	m := candidateMatch.FindStringSubmatch(expr)
	if m == nil {
		t.Fatalf("dfaas_mgmt_candidate is not filtered through a match test: %q", expr)
	}
	re, err := regexp.Compile(m[1])
	if err != nil {
		t.Fatalf("candidate pattern %q: %v", m[1], err)
	}
	for _, tc := range []struct {
		client string
		want   bool
	}{
		{"100.64.0.11", true},
		{"192.168.252.10", true},
		{"fd7a:115c:a1e0::11", false},
		{"::ffff:100.64.0.11", false},
		{"fe80::1%eth0", false},
		{"[100.64.0.11]", false},
		{"100.64.0.11:22", false},
		{"mgmt.lab", false},
		{"", false},
	} {
		if got := re.MatchString(tc.client); got != tc.want {
			t.Errorf("candidate pattern %q on %q: got %v, want %v", m[1], tc.client, got, tc.want)
		}
	}
}

// The probe tests the path a runner takes: from the generator, direct, to the
// filer NodePort the inventory names. Its outcome is data, never a failure.
// ignore_errors would still print a FAILED! line, which is what the operator's
// failed-task scraper looks for in the pod log.
func TestK6PlaybookProbeIsToleratedAndDirect(t *testing.T) {
	_, task := k6Task(t, k6TaskProbe)
	uri := module(t, task, "ansible.builtin.uri")

	if task["failed_when"] != false {
		t.Errorf("probe must set failed_when: false, got %v", task["failed_when"])
	}
	if _, ok := task["ignore_errors"]; ok {
		t.Errorf("probe must not use ignore_errors (it prints FAILED!); use failed_when: false")
	}
	if uri["use_proxy"] != false {
		t.Errorf("probe must set use_proxy: false, got %v", uri["use_proxy"])
	}
	if task["become"] != false {
		t.Errorf("probe must set become: false, got %v", task["become"])
	}
	if _, ok := task["delegate_to"]; ok {
		t.Errorf("probe must run on the generator, not delegate_to %v", task["delegate_to"])
	}
	if url, _ := uri["url"].(string); !strings.Contains(url, "{{ filer_node_port }}") {
		t.Errorf("probe URL must use the filer_node_port host var, got %q", url)
	}
}

// filer_node_port reaches the playbook as an inventory host var
// (buildInventory). Play vars outrank inventory host vars, so a default
// declared in the play would silently replace the operator's value.
func TestK6PlaybookDoesNotShadowFilerNodePort(t *testing.T) {
	vars, _ := k6Play(t)["vars"].(map[string]any)
	if _, ok := vars["filer_node_port"]; ok {
		t.Errorf("filer_node_port must not be a play var: it would shadow the inventory value")
	}
}

// Detection runs before the play installs anything, so it costs seconds and
// sees the network the generator had before k3s added its own interfaces.
func TestK6PlaybookDetectsBeforeInstallingK3s(t *testing.T) {
	k3s, _ := k6Task(t, k6TaskK3s)
	for _, name := range []string{k6TaskReadSSH, k6TaskTake, k6TaskProbe, k6TaskKeep} {
		if i, _ := k6Task(t, name); i >= k3s {
			t.Errorf("task %q (#%d) must come before %q (#%d)", name, i, k6TaskK3s, k3s)
		}
	}
}

// A failing task reaches K6Ready only by its name: parseFailedTask cuts the
// "TASK [..]" header at the first ']', and a templated name prints unrendered.
func TestK6PlaybookTaskNamesAreStatic(t *testing.T) {
	for _, task := range k6AllTasks(t) {
		name, _ := task["name"].(string)
		if name == "" {
			continue
		}
		if strings.Contains(name, "]") || strings.Contains(name, "{{") {
			t.Errorf("task name %q must contain neither ']' nor Jinja", name)
		}
	}
}

// Detection is best-effort by contract: whatever the probe saw, the play goes
// on and the generator falls back. No task that can fail the play may read a
// detection result.
func TestK6PlaybookNoFailureDependsOnDetection(t *testing.T) {
	detection := []string{"dfaas_ssh_connection", "dfaas_ssh_client", "dfaas_mgmt_"}
	for _, task := range k6AllTasks(t) {
		_, fails := task["ansible.builtin.fail"]
		_, asserts := task["ansible.builtin.assert"]
		fw, hasFW := task["failed_when"]
		if !fails && !asserts && (!hasFW || fw == false) {
			continue
		}
		raw, err := json.Marshal(task)
		if err != nil {
			t.Fatalf("marshal task %q: %v", task["name"], err)
		}
		for _, v := range detection {
			if strings.Contains(string(raw), v) {
				t.Errorf("task %q can fail the play and reads the detection result %s*", task["name"], v)
			}
		}
	}
}

// jinjaExpr matches one {{ ... }} expression in the push body.
var jinjaExpr = regexp.MustCompile(`\{\{.*?\}\}`)

// The Secret keys and name are hand-synced between job.go and the playbook,
// which is the source of truth. A key that drifts is not an error anywhere: a
// label read by pruneK6Kubeconfigs or the annotation read by syncNodeStatus
// simply comes back empty. So parse the apply body, every Jinja expression
// replaced by a placeholder, and check each key sits where Go reads it.
func TestK6PlaybookPushCarriesTheSyncedKeys(t *testing.T) {
	_, task := k6Task(t, k6TaskPushName)
	body, _ := module(t, task, "ansible.builtin.uri")["body"].(string)
	if body == "" {
		t.Fatalf("task %q has no body", k6TaskPushName)
	}

	var secret struct {
		Metadata struct {
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := yaml.Unmarshal([]byte(jinjaExpr.ReplaceAllString(body, "X")), &secret); err != nil {
		t.Fatalf("parse the apply body with its Jinja replaced: %v\n%s", err, body)
	}
	for _, key := range []string{LabelKubeconfigEnv, LabelKubeconfigNodeID} {
		if _, ok := secret.Metadata.Labels[key]; !ok {
			t.Errorf("apply body has no metadata.labels[%q]", key)
		}
	}
	if _, ok := secret.Metadata.Annotations[AnnotationKubeconfigManagementAddress]; !ok {
		t.Errorf("apply body has no metadata.annotations[%q]", AnnotationKubeconfigManagementAddress)
	}
	// The value comes from the generator: quoted by to_json, it stays one
	// scalar whatever it holds.
	for _, line := range strings.Split(body, "\n") {
		if strings.Contains(line, AnnotationKubeconfigManagementAddress+":") && !strings.Contains(line, "| to_json }}") {
			t.Errorf("the management address annotation must be rendered through to_json: %q", strings.TrimSpace(line))
		}
	}

	vars, _ := k6Play(t)["vars"].(map[string]any)
	if got, want := vars["mgmt_secret_name"], KubeconfigSecretName("{{ env_name }}", "{{ node_id }}"); got != want {
		t.Errorf("mgmt_secret_name is %q, KubeconfigSecretName builds %q", got, want)
	}
}
