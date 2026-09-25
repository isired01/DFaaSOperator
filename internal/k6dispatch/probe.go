/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package k6dispatch

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	dfaasv1 "dfaas-operator/api/v1"
)

// ProbeImage runs the reachability probe. It is k6-operator v0.0.15's default
// runner image, so every generator that ran a test already has it (its tag is
// not "latest": pull policy IfNotPresent). It is Alpine with busybox wget,
// checked 2026-09-25; busybox's output is what probeScript parses.
const ProbeImage = "ghcr.io/grafana/k6-operator:latest-runner"

// probeDeadlineSeconds bounds the probe Pod, image pull included.
const probeDeadlineSeconds int64 = 30

// probeScript asks the probe URL once. Any HTTP answer, a 404 included, means
// reachable (exit 0); a connection, DNS or timeout error means unreachable
// (exit 3, the error on stdout, which FallbackToLogsOnError turns into the
// termination message). Anything else is no network verdict (exit 4): the
// probe itself is broken, and that must not read as a network problem.
const probeScript = `out=$(wget -q -O /dev/null -T 10 "$PROBE_URL" 2>&1); rc=$?
[ "$rc" -eq 0 ] && exit 0
case "$out" in *"server returned error"*) exit 0 ;; esac
[ "$rc" -eq 1 ] || exit 4
echo "${out#wget: }"
exit 3`

// ProbeState is what a probe Pod says about the filer, read off its status.
type ProbeState int

const (
	// ProbeAbsent: no probe Pod — never created, or already read and deleted.
	ProbeAbsent ProbeState = iota
	ProbeRunning
	ProbeReachable
	ProbeUnreachable
	// ProbeDidNotRun: the Pod ended without a network verdict (image not
	// pulled in time, no wget, a Pod left by another LoadTest).
	ProbeDidNotRun
)

// ProbeOutcome is one generator's probe verdict, with the URL it tried.
type ProbeOutcome struct {
	State  ProbeState
	URL    string
	Detail string
}

// ProbeName is the probe Pod's name: the TestRun name (≤ 51 bytes) plus
// "-probe", within the 63 bytes every derived name keeps to.
func ProbeName(lt *dfaasv1.LoadTest, nodeID string) string {
	return TestRunName(lt, nodeID) + "-probe"
}

func probePod(lt *dfaasv1.LoadTest, nodeID, url string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: ProbeName(lt, nodeID), Namespace: remoteNamespace,
			Labels: map[string]string{
				"dfaas.io/loadtest-name": lt.Name,
				"dfaas.io/node-id":       nodeID,
				ScriptOwnerLabel:         string(lt.UID),
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                corev1.RestartPolicyNever,
			ActiveDeadlineSeconds:        ptr.To(probeDeadlineSeconds),
			AutomountServiceAccountToken: ptr.To(false),
			Containers: []corev1.Container{{
				Name:                     "probe",
				Image:                    ProbeImage,
				ImagePullPolicy:          corev1.PullIfNotPresent,
				Command:                  []string{"sh", "-c", probeScript},
				Env:                      []corev1.EnvVar{{Name: "PROBE_URL", Value: url}},
				TerminationMessagePolicy: corev1.TerminationMessageFallbackToLogsOnError,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m"), corev1.ResourceMemory: resource.MustParse("16Mi")},
					Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("32Mi")},
				},
			}},
		},
	}
}

func probeOutcome(lt *dfaasv1.LoadTest, p *corev1.Pod) ProbeOutcome {
	o := ProbeOutcome{State: ProbeRunning}
	for _, c := range p.Spec.Containers {
		for _, e := range c.Env {
			if e.Name == "PROBE_URL" {
				o.URL = e.Value
			}
		}
	}
	if p.Labels[ScriptOwnerLabel] != string(lt.UID) {
		o.State, o.Detail = ProbeDidNotRun, "probe Pod left by another LoadTest"
		return o
	}
	switch p.Status.Phase {
	case corev1.PodSucceeded:
		o.State = ProbeReachable
	case corev1.PodFailed:
		o.State, o.Detail = ProbeDidNotRun, strings.TrimSpace(p.Status.Reason+" "+p.Status.Message)
		for _, cs := range p.Status.ContainerStatuses {
			if t := cs.State.Terminated; cs.Name == "probe" && t != nil {
				if t.ExitCode == 3 {
					o.State = ProbeUnreachable
				}
				o.Detail = strings.TrimSpace(t.Message)
			}
		}
	}
	return o
}
