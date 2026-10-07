/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	"dfaas-operator/internal/controller/roles"
)

// jobTTLSuccessSeconds is the TTLSecondsAfterFinished applied to a successful
// Ansible Job so the Job controller cleans it up shortly after completion.
const jobTTLSuccessSeconds int32 = 600

// jobTTLFailureGraceSeconds is the TTLSecondsAfterFinished applied to a failed
// Ansible Job (24h), giving an audit window before auto-cleanup.
const jobTTLFailureGraceSeconds int32 = 86400

// ansibleLogTailLines caps how many trailing log lines we pull from a failed
// Ansible pod. The failure region (TASK header + "fatal: .. FAILED!" + PLAY
// RECAP) sits at the very end, so we tail rather than read from the top.
const ansibleLogTailLines int64 = 400

var ansibleTaskHeaderRe = regexp.MustCompile(`^TASK \[(.+?)\]`)

// patchAnsibleJobTTL looks up the role-suffixed Ansible Job for env and sets
// its post-finish TTL. No-op (logs at V1) if the Job is gone — e.g. a phase
// that was skipped because the role has no nodes, so no Job was ever created.
func (r *EnvironmentReconciler) patchAnsibleJobTTL(ctx context.Context,
	env *dfaasv1.Environment, suffix string, ttlSec int32) {
	var job batchv1.Job
	name := ansible.JobNameForRole(env, suffix)
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: env.Namespace}, &job); err != nil {
		if !apierrors.IsNotFound(err) {
			log.FromContext(ctx).Error(err, "get Ansible Job for TTL patch", "job", name)
		}
		return
	}
	patchJobTTL(ctx, r.Client, &job, ttlSec)
}

// ensureAnsibleJob drives one provisioning stream of the parallel
// ProvisioningInfra fan-in: it creates the role-filtered Ansible Job on first
// sight, then reports its terminal state via done/failed flags while stamping
// the per-stream Condition described by spec. It never changes the phase, so
// the caller owns the FSM transition once both streams settle.
//
// NB: the success TTL is NOT set here. While the sibling Job may still be
// running, ProvisioningInfra keeps re-reconciling every 10s; a short TTL would
// let the Job controller delete this finished Job mid-wait, the next call would
// hit NotFound and recreate it, re-running the playbook. The success TTL is
// applied once, after the fan-in settles, in reconcileProvisioningInfra.
func (r *EnvironmentReconciler) ensureAnsibleJob(ctx context.Context,
	env *dfaasv1.Environment, spec roles.Spec, libp2pKeys map[string]string) (done bool, failed bool, err error) {
	logger := log.FromContext(ctx)

	if !env.HasNodeWithRole(spec.Role) {
		logger.Info("no nodes for role, skipping Ansible phase", "role", spec.Human)
		r.cond(ctx, env, spec.CondType,
			metav1.ConditionTrue, spec.SkipReason, spec.SkipMessage)
		return true, false, nil
	}

	jobName := ansible.JobNameForRole(env, spec.JobSuffix)
	var job batchv1.Job
	getErr := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: env.Namespace}, &job)

	if apierrors.IsNotFound(getErr) {
		// P7: before kicking off the Job, the observed state is "we haven't
		// checked yet" — stamp Unknown/JobPending. Replaced by False/
		// AnsibleRunning once the Job exists.
		r.cond(ctx, env, spec.CondType,
			metav1.ConditionUnknown, dfaasv1.EnvReasonJobPending,
			spec.Human+" Ansible Job not yet created")

		logger.Info("creating Ansible Job", "role", spec.Human, "job", jobName)
		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		newJob, secret, jerr := am.CreateJobForRole(ctx, env, spec.Role, libp2pKeys)
		if jerr != nil {
			// P2: surface CreateJobForRole failure.
			r.cond(ctx, env, spec.CondType,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"build "+spec.Human+" Ansible Job: "+condMessage(jerr))
			return false, false, jerr
		}
		if cerr := r.Create(ctx, secret); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			// P2.
			r.cond(ctx, env, spec.CondType,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create "+spec.Human+" inventory Secret: "+condMessage(cerr))
			return false, false, cerr
		}
		if cerr := r.Create(ctx, newJob); cerr != nil && !apierrors.IsAlreadyExists(cerr) {
			// P2.
			r.cond(ctx, env, spec.CondType,
				metav1.ConditionFalse, dfaasv1.EnvReasonJobCreationFailed,
				"create "+spec.Human+" Ansible Job: "+condMessage(cerr))
			return false, false, cerr
		}
		r.cond(ctx, env, spec.CondType,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning,
			spec.Human+" Ansible Job started")
		return false, false, nil
	}
	if getErr != nil {
		return false, false, getErr
	}

	// Terminal state is driven by the Job's Complete/Failed Conditions, NOT by
	// the raw Failed pod count: with BackoffLimit>0 a failed pod is transient
	// (K8s spawns a retry), so job.Status.Failed counts failed *attempts* while
	// the Job may still recover. We surface those transient attempts (count +
	// last failing task) in the Condition message — visible through to Ready.
	failedCount := job.Status.Failed

	if job.Status.Succeeded > 0 || jobConditionTrue(&job, batchv1.JobComplete) {
		msg := spec.Human + " Ansible Job completed"
		if failedCount > 0 {
			msg = fmt.Sprintf("%s after %d failed attempt(s)%s", msg, failedCount,
				taskSuffix(r.lastFailedTask(ctx, env.Namespace, jobName), " at task: "))
		}
		r.cond(ctx, env, spec.CondType,
			metav1.ConditionTrue, spec.DoneReason, msg)
		return true, false, nil
	}
	if jobConditionTrue(&job, batchv1.JobFailed) {
		patchJobTTL(ctx, r.Client, &job, jobTTLFailureGraceSeconds)
		msg := fmt.Sprintf("%s Ansible Job failed after %d attempt(s)%s; check logs",
			spec.Human, failedCount, taskSuffix(r.lastFailedTask(ctx, env.Namespace, jobName), " at task: "))
		r.cond(ctx, env, spec.CondType,
			metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleFailed, msg)
		return false, true, nil
	}
	// Still running. A failed pod here is mid-backoff, not terminal — keep
	// waiting, but report the transient retry so the UI shows the hiccup.
	logger.Info("Ansible Job still running",
		"role", spec.Human,
		"job", jobName,
		"active", job.Status.Active,
		"succeeded", job.Status.Succeeded,
		"failed", failedCount)
	msg := spec.Human + " Ansible Job in progress"
	if failedCount > 0 {
		msg = fmt.Sprintf("%s (%d failed attempt(s), retrying%s)", msg, failedCount,
			taskSuffix(r.lastFailedTask(ctx, env.Namespace, jobName), "; last task: "))
	}
	r.cond(ctx, env, spec.CondType,
		metav1.ConditionFalse, dfaasv1.EnvReasonAnsibleRunning, msg)
	return false, false, nil
}

// jobConditionTrue reports whether the Job carries condType (JobComplete or
// JobFailed) with status True. With BackoffLimit>0 these Conditions — not the
// raw Failed pod count — define terminal success/failure.
func jobConditionTrue(job *batchv1.Job, condType batchv1.JobConditionType) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == condType && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// taskSuffix renders " <connector><task>" when task is non-empty, else "". Lets
// callers fold the (best-effort) failing-task name into a Condition message
// without branching, degrading cleanly to a count-only message.
func taskSuffix(task, connector string) string {
	if task == "" {
		return ""
	}
	return connector + task
}

// lastFailedTask best-effort extracts the name of the Ansible task that failed
// most recently in a finished pod of the named Job: it lists the Job's pods,
// reads the newest Failed pod's log tail, and returns the task named by the
// last "TASK [..]" line preceding a "fatal: .. FAILED!" marker. Returns "" on
// any error, no match, or when no clientset is wired (e.g. unit tests) — the
// caller then degrades to a count-only message.
func (r *EnvironmentReconciler) lastFailedTask(ctx context.Context, namespace, jobName string) string {
	if r.Clientset == nil {
		return ""
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(namespace),
		client.MatchingLabels{"job-name": jobName}); err != nil {
		return ""
	}
	var newest *corev1.Pod
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase != corev1.PodFailed {
			continue
		}
		if newest == nil || p.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = p
		}
	}
	if newest == nil {
		return ""
	}

	tail := ansibleLogTailLines
	req := r.Clientset.CoreV1().Pods(namespace).GetLogs(newest.Name, &corev1.PodLogOptions{
		Container: "ansible-worker",
		TailLines: &tail,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return ""
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return ""
	}
	return parseFailedTask(string(data))
}

// parseFailedTask returns the name in the last "TASK [..]" header that precedes
// the last "FAILED!"/"fatal:" marker in the Ansible log, or "" if there is no
// failure marker / task header. A failed play aborts at the offending task, so
// that header is the failing task.
func parseFailedTask(logs string) string {
	lines := strings.Split(logs, "\n")
	failIdx := -1
	for i, ln := range lines {
		if strings.Contains(ln, "FAILED!") || strings.HasPrefix(strings.TrimSpace(ln), "fatal:") {
			failIdx = i
		}
	}
	if failIdx < 0 {
		return ""
	}
	for i := failIdx; i >= 0; i-- {
		if m := ansibleTaskHeaderRe.FindStringSubmatch(strings.TrimSpace(lines[i])); m != nil {
			return strings.TrimSpace(m[1])
		}
	}
	return ""
}

// patchJobTTL sets Spec.TTLSecondsAfterFinished on a finished Ansible Job
// to drive Job-controller auto-cleanup. Used with ttlSec=600 on success and
// ttlSec=86400 (24h grace) on failure. Idempotent: skips when TTL is
// already set so re-reconciles do not churn.
func patchJobTTL(ctx context.Context, c client.Client, job *batchv1.Job, ttlSec int32) {
	if job.Spec.TTLSecondsAfterFinished != nil {
		return
	}
	patched := job.DeepCopy()
	ttl := ttlSec
	patched.Spec.TTLSecondsAfterFinished = &ttl
	if err := c.Patch(ctx, patched, client.MergeFrom(job)); err != nil {
		log.FromContext(ctx).Error(err, "patch TTL on Ansible Job",
			"job", job.Name, "ttlSeconds", ttlSec)
	}
}

// cleanupStaleGenJobs deletes the Ansible Jobs of older generations (with
// Foreground propagation, so a Job outlives its pods) and, best-effort, their
// inventory Secrets, which hold node credentials. It returns the names of the
// older-generation Jobs that still exist: ProvisioningInfra waits until none
// does, so two generations of playbooks never run on the same machines. The
// Job List reads uncached when APIReader is wired: the Job informer is not
// ordered with the Environment one, and a missed Job here is a concurrent run.
// Only the List error is returned.
func (r *EnvironmentReconciler) cleanupStaleGenJobs(ctx context.Context,
	env *dfaasv1.Environment) ([]string, error) {
	logger := log.FromContext(ctx)
	selector := []client.ListOption{
		client.InNamespace(env.Namespace),
		client.MatchingLabels{ansible.LabelEnvironment: env.Name},
	}
	var reader client.Reader = r.Client
	if r.APIReader != nil {
		reader = r.APIReader
	}
	var jobs batchv1.JobList
	if err := reader.List(ctx, &jobs, selector...); err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}

	currentGen := fmt.Sprintf("%d", env.Generation)
	stale := func(labels map[string]string) bool {
		g := labels[ansible.LabelGeneration]
		return g != "" && g != currentGen
	}
	propagation := metav1.DeletePropagationForeground
	var older []string
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if !stale(j.Labels) {
			continue
		}
		older = append(older, j.Name)
		if !j.DeletionTimestamp.IsZero() {
			continue
		}
		logger.Info("deleting stale-gen Ansible Job", "job", j.Name,
			"staleGen", j.Labels[ansible.LabelGeneration], "currentGen", currentGen)
		if err := r.Delete(ctx, j, &client.DeleteOptions{PropagationPolicy: &propagation}); err != nil &&
			!apierrors.IsNotFound(err) {
			logger.Error(err, "delete stale Job", "job", j.Name)
		}
	}

	var secrets corev1.SecretList
	if err := r.List(ctx, &secrets, selector...); err != nil {
		logStatusErr(ctx, "list stale-gen inventory Secrets", err)
		return older, nil
	}
	for i := range secrets.Items {
		sec := &secrets.Items[i]
		if stale(sec.Labels) {
			logStatusErr(ctx, "delete stale-gen inventory Secret", client.IgnoreNotFound(r.Delete(ctx, sec)))
		}
	}
	return older, nil
}
