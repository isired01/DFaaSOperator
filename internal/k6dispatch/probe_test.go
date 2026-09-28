package k6dispatch

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
)

func probeLT(name string) *dfaasv1.LoadTest {
	return &dfaasv1.LoadTest{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: "uid-1"}}
}

func TestProbePodIsShortLivedAndBounded(t *testing.T) {
	lt := probeLT(strings.Repeat("a", 63))
	p := probePod(lt, strings.Repeat("n", 63), "http://10.0.0.9:30901/dfaas-sync/default/x.json")
	if len(p.Name) > 63 || p.Name != ProbeName(lt, strings.Repeat("n", 63)) {
		t.Errorf("name %q (%d bytes)", p.Name, len(p.Name))
	}
	if p.Namespace != remoteNamespace || p.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("namespace/restartPolicy = %q/%q", p.Namespace, p.Spec.RestartPolicy)
	}
	if p.Spec.ActiveDeadlineSeconds == nil || *p.Spec.ActiveDeadlineSeconds != probeDeadlineSeconds {
		t.Errorf("activeDeadlineSeconds = %v", p.Spec.ActiveDeadlineSeconds)
	}
	if p.Labels["dfaas.io/loadtest-name"] != lt.Name || p.Labels[ScriptOwnerLabel] != "uid-1" {
		t.Errorf("labels = %v", p.Labels)
	}
	c := p.Spec.Containers[0]
	if c.Image != ProbeImage || c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Errorf("image/termination policy = %q/%q", c.Image, c.TerminationMessagePolicy)
	}
	if len(c.Env) != 1 || c.Env[0].Name != "PROBE_URL" || c.Env[0].Value != "http://10.0.0.9:30901/dfaas-sync/default/x.json" {
		t.Errorf("env = %v", c.Env)
	}
	if c.Resources.Limits.Memory().IsZero() || c.Resources.Requests.Cpu().IsZero() {
		t.Errorf("resources = %v", c.Resources)
	}
}

func terminated(code int32, msg string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: "probe", State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: msg}}}
}

func TestProbeOutcomeReadsThePodStatus(t *testing.T) {
	lt := probeLT("lt")
	for _, tc := range []struct {
		name   string
		phase  corev1.PodPhase
		reason string // the Pod's own status.reason
		status []corev1.ContainerStatus
		uid    string
		want   ProbeState
		detail string // a substring of the wanted Detail
	}{
		{"any HTTP answer", corev1.PodSucceeded, "", []corev1.ContainerStatus{terminated(0, "")}, "uid-1", ProbeReachable, ""},
		{"network error", corev1.PodFailed, "", []corev1.ContainerStatus{terminated(3, "download timed out\n")}, "uid-1", ProbeUnreachable, "download timed out"},
		{"wget missing", corev1.PodFailed, "", []corev1.ContainerStatus{terminated(4, "sh: wget: not found\n")}, "uid-1", ProbeDidNotRun, "wget: not found"},
		{"deadline while pulling", corev1.PodFailed, "", nil, "uid-1", ProbeDidNotRun, ""},
		// A deadline kill leaves the container's termination message empty:
		// the Pod's own reason is then the only detail there is.
		{"deadline while running", corev1.PodFailed, "DeadlineExceeded", []corev1.ContainerStatus{terminated(137, "")}, "uid-1", ProbeDidNotRun, "DeadlineExceeded"},
		{"still running", corev1.PodRunning, "", nil, "uid-1", ProbeRunning, ""},
		{"pending", corev1.PodPending, "", nil, "uid-1", ProbeRunning, ""},
		{"another test's pod", corev1.PodFailed, "", []corev1.ContainerStatus{terminated(3, "x")}, "uid-2", ProbeDidNotRun, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := probePod(lt, "gen-a", "http://u")
			p.Labels[ScriptOwnerLabel] = tc.uid
			p.Status.Phase, p.Status.Reason, p.Status.ContainerStatuses = tc.phase, tc.reason, tc.status
			if tc.reason != "" {
				p.Status.Message = "Pod was active on the node longer than the specified deadline"
			}
			got := probeOutcome(lt, p)
			if got.State != tc.want || got.URL != "http://u" || !strings.Contains(got.Detail, tc.detail) {
				t.Errorf("outcome = %+v, want state %v detail containing %q", got, tc.want, tc.detail)
			}
		})
	}
}

// The script's verdict comes from busybox wget's exit code and output
// (checked against ghcr.io/grafana/k6-operator:latest-runner): run it with a
// fake wget to pin the classification.
func TestProbeScriptTellsHTTPErrorsFromNetworkErrors(t *testing.T) {
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no sh")
	}
	for _, tc := range []struct {
		name   string
		wget   string // body of the fake wget; "" = no wget on PATH
		code   int
		stdout string
		// has: substrings stdout must contain instead of equalling stdout, for
		// a message whose wording is the shell's own.
		has []string
	}{
		{"2xx", "exit 0", 0, "", nil},
		{"404 is reachable", `echo "wget: server returned error: HTTP/1.1 404 Not Found" >&2; exit 1`, 0, "", nil},
		{"refused", `echo "wget: can't connect to remote host (10.0.0.9): Connection refused" >&2; exit 1`, 3, "can't connect to remote host (10.0.0.9): Connection refused", nil},
		{"timeout", `echo "wget: download timed out" >&2; exit 1`, 3, "download timed out", nil},
		// busybox and dash say "wget: not found", bash "wget: command not
		// found": the detail is whatever the shell said, never empty.
		{"no wget", "", 4, "", []string{"wget", "not found"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.wget != "" {
				if err := os.WriteFile(filepath.Join(dir, "wget"), []byte("#!/bin/sh\n"+tc.wget+"\n"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command(sh, "-c", probeScript)
			cmd.Env = []string{"PATH=" + dir, "PROBE_URL=http://10.0.0.9:30901/x"}
			out, _ := cmd.Output()
			got := strings.TrimSpace(string(out))
			ok := got == tc.stdout
			if tc.has != nil {
				ok = true
				for _, sub := range tc.has {
					ok = ok && strings.Contains(got, sub)
				}
			}
			if code := cmd.ProcessState.ExitCode(); code != tc.code || !ok {
				t.Errorf("exit %d stdout %q, want %d %q (or containing %q)", code, out, tc.code, tc.stdout, tc.has)
			}
		})
	}
}
