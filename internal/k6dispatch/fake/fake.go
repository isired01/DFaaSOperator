/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package fake is the in-memory k6dispatch adapter for tests. It records every
// remote operation and returns whatever stage the test has set; nothing
// progresses on its own — a test that wants a transition sets it and
// reconciles again.
package fake

import (
	"context"
	"sync"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/k6dispatch"
)

// Applied records one Node.Apply call.
type Applied struct {
	NodeID  string
	Name    string
	PerNode dfaasv1.PerNodeLoad
	Env     k6dispatch.RunnerEnv
}

// Fleet implements k6dispatch.Dispatcher.
type Fleet struct {
	mu       sync.Mutex
	stages   map[string]string // TestRun key → stage; absent = NotFound
	applied  []Applied
	staged   []string             // TestRun keys, in order (every Stage call)
	deleted  []string             // TestRun keys, in order
	mirrored []string             // "<nodeID>/<cm name>"
	scripts  map[string]types.UID // "<nodeID>/<name>" → owner UID
	logs     map[string]string
	probes   map[string]k6dispatch.ProbeOutcome // key(nodeID, lt) → probe verdict
	failNext map[string]error                   // "<nodeID>|<op>" → error, consumed on first use
}

// New returns an empty fleet.
func New() *Fleet {
	return &Fleet{stages: map[string]string{}, scripts: map[string]types.UID{},
		logs: map[string]string{}, probes: map[string]k6dispatch.ProbeOutcome{},
		failNext: map[string]error{}}
}

func key(nodeID string, lt *dfaasv1.LoadTest) string {
	return nodeID + "|" + k6dispatch.TestRunName(lt, nodeID)
}

// Node implements Dispatcher with the same usability policy as Live, so
// tests exercise the real "node left the environment" path.
func (f *Fleet) Node(_ context.Context, env *dfaasv1.Environment, nodeID string) (k6dispatch.Node, error) {
	if _, err := k6dispatch.ResolveKubeconfig(env, nodeID); err != nil {
		return nil, err
	}
	return &node{f: f, nodeID: nodeID}, nil
}

// SetStage sets what Stage reports for lt on nodeID. Also makes the TestRun
// "exist" if it did not.
func (f *Fleet) SetStage(nodeID string, lt *dfaasv1.LoadTest, stage string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stages[key(nodeID, lt)] = stage
}

// SetLogs sets what Logs returns for nodeID.
func (f *Fleet) SetLogs(nodeID, logs string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logs[nodeID] = logs
}

// FailNext makes the next op ("mirror", "apply", "stage", "delete", "logs",
// "script", "probe", "probe-read", "probe-delete") on nodeID return err, once.
func (f *Fleet) FailNext(nodeID, op string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext[nodeID+"|"+op] = err
}

// Applied returns every Apply call in order.
func (f *Fleet) Applied() []Applied {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Applied(nil), f.applied...)
}

// Staged returns every Stage call (TestRun keys) in order, whether or not the
// TestRun was found.
func (f *Fleet) Staged() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.staged...)
}

// Deleted returns every Delete call (TestRun keys) in order.
func (f *Fleet) Deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.deleted...)
}

// Mirrored returns every MirrorConfigMap call as "<nodeID>/<cm name>".
func (f *Fleet) Mirrored() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.mirrored...)
}

// Exists reports whether lt's TestRun is present on nodeID.
func (f *Fleet) Exists(nodeID string, lt *dfaasv1.LoadTest) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.stages[key(nodeID, lt)]
	return ok
}

// ScriptExists reports whether a mirrored script is still on nodeID.
func (f *Fleet) ScriptExists(nodeID, name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.scripts[nodeID+"/"+name]
	return ok
}

// SetProbe scripts nodeID's probe verdict, keeping the URL it was started with.
func (f *Fleet) SetProbe(nodeID string, lt *dfaasv1.LoadTest, o k6dispatch.ProbeOutcome) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := key(nodeID, lt)
	if o.URL == "" {
		o.URL = f.probes[k].URL
	}
	f.probes[k] = o
}

// ProbeExists reports whether nodeID still has a probe Pod for lt.
func (f *Fleet) ProbeExists(nodeID string, lt *dfaasv1.LoadTest) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.probes[key(nodeID, lt)]
	return ok
}

// ProbeURL is the URL nodeID's probe was started with ("" if none).
func (f *Fleet) ProbeURL(nodeID string, lt *dfaasv1.LoadTest) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.probes[key(nodeID, lt)].URL
}

func (f *Fleet) takeFailure(nodeID, op string) error {
	k := nodeID + "|" + op
	if err, ok := f.failNext[k]; ok {
		delete(f.failNext, k)
		return err
	}
	return nil
}

type node struct {
	f      *Fleet
	nodeID string
}

func (n *node) Ref(lt *dfaasv1.LoadTest) dfaasv1.TestRunRef {
	return dfaasv1.TestRunRef{NodeID: n.nodeID, Name: k6dispatch.TestRunName(lt, n.nodeID), Namespace: "default"}
}

func (n *node) MirrorConfigMap(_ context.Context, lt *dfaasv1.LoadTest, cm *corev1.ConfigMap) error {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "mirror"); err != nil {
		return err
	}
	n.f.mirrored = append(n.f.mirrored, n.nodeID+"/"+cm.Name)
	n.f.scripts[n.nodeID+"/"+cm.Name] = lt.UID
	return nil
}

func (n *node) DeleteScript(_ context.Context, lt *dfaasv1.LoadTest, name string) error {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "script"); err != nil {
		return err
	}
	if k := n.nodeID + "/" + name; n.f.scripts[k] == lt.UID {
		delete(n.f.scripts, k)
	}
	return nil
}

func (n *node) Apply(_ context.Context, lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad, env k6dispatch.RunnerEnv) error {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "apply"); err != nil {
		return err
	}
	k := key(n.nodeID, lt)
	n.f.applied = append(n.f.applied, Applied{NodeID: n.nodeID, Name: k6dispatch.TestRunName(lt, n.nodeID), PerNode: perNode, Env: env})
	if _, ok := n.f.stages[k]; !ok {
		n.f.stages[k] = "created"
	}
	return nil
}

func (n *node) Stage(_ context.Context, lt *dfaasv1.LoadTest) (string, error) {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	n.f.staged = append(n.f.staged, key(n.nodeID, lt))
	if err := n.f.takeFailure(n.nodeID, "stage"); err != nil {
		return "", err
	}
	stage, ok := n.f.stages[key(n.nodeID, lt)]
	if !ok {
		return "", k6dispatch.ErrNotFound
	}
	return stage, nil
}

func (n *node) Delete(_ context.Context, lt *dfaasv1.LoadTest) error {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "delete"); err != nil {
		return err
	}
	k := key(n.nodeID, lt)
	n.f.deleted = append(n.f.deleted, k)
	delete(n.f.stages, k)
	return nil
}

func (n *node) Logs(_ context.Context, _ *dfaasv1.LoadTest) (string, error) {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "logs"); err != nil {
		return "", err
	}
	return n.f.logs[n.nodeID], nil
}

// StartProbe refuses a probe already present under the name, as the live
// Create does: only DeleteProbe clears a leftover.
func (n *node) StartProbe(_ context.Context, lt *dfaasv1.LoadTest, url string) error {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "probe"); err != nil {
		return err
	}
	k := key(n.nodeID, lt)
	if _, ok := n.f.probes[k]; ok {
		return apierrors.NewAlreadyExists(corev1.Resource("pods"), k6dispatch.ProbeName(lt, n.nodeID))
	}
	n.f.probes[k] = k6dispatch.ProbeOutcome{State: k6dispatch.ProbeRunning, URL: url}
	return nil
}

func (n *node) ProbeResult(_ context.Context, lt *dfaasv1.LoadTest) (k6dispatch.ProbeOutcome, error) {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "probe-read"); err != nil {
		return k6dispatch.ProbeOutcome{}, err
	}
	return n.f.probes[key(n.nodeID, lt)], nil // zero value = ProbeAbsent
}

func (n *node) DeleteProbe(_ context.Context, lt *dfaasv1.LoadTest) error {
	n.f.mu.Lock()
	defer n.f.mu.Unlock()
	if err := n.f.takeFailure(n.nodeID, "probe-delete"); err != nil {
		return err
	}
	delete(n.f.probes, key(n.nodeID, lt))
	return nil
}
