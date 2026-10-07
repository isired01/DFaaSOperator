/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package k6dispatch is the seam between the LoadTest reconciler and the k6
// fleet: the k3s clusters running on an Environment's k6-load-generator nodes,
// each with its own k6-operator, each reachable through a kubeconfig Secret
// the provisioning playbook pushed into the management cluster.
//
// Dispatcher resolves a node; Node is the handle every remote operation goes
// through. Everything about HOW a node is reached — which Secret, which remote
// namespace, what the TestRun is called — lives behind these two interfaces.
// Two adapters justify the seam: Live (production, client-go against the
// remote k3s) and fake.Fleet (tests, in memory).
package k6dispatch

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/netip"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// Dispatcher reaches the k6 fleet declared on an Environment.
type Dispatcher interface {
	// Node resolves one k6-load-generator by nodeID. It errors when the node
	// is no longer declared on the Environment or has no kubeconfig Secret yet
	// — the single usability policy for every caller: an unusable node is an
	// error, never a silent skip. Callers decide whether that error fails the
	// LoadTest (observe, sync barrier) or is logged and skipped (teardown, log
	// capture — a node that cannot be reached has nothing to reclaim).
	Node(ctx context.Context, env *dfaasv1.Environment, nodeID string) (Node, error)
}

// Node is one k6-load-generator's k3s, addressed by the LoadTest whose TestRun
// it hosts. Remote names are deterministic (TestRunName), so a caller never
// learns or recomputes one.
type Node interface {
	// Ref is the status record for lt's TestRun on this node.
	Ref(lt *dfaasv1.LoadTest) dfaasv1.TestRunRef
	// MirrorConfigMap server-side-applies cm, same name, into the namespace
	// the TestRun lives in, labelled with lt's UID (ScriptOwnerLabel).
	// k6-operator resolves spec.script.configMap in its own cluster, so the
	// script must exist there before Apply.
	MirrorConfigMap(ctx context.Context, lt *dfaasv1.LoadTest, cm *corev1.ConfigMap) error
	// DeleteScript deletes the script ConfigMap name that lt mirrored onto
	// this node. A copy another LoadTest re-mirrored since is kept; an absent
	// one is success.
	DeleteScript(ctx context.Context, lt *dfaasv1.LoadTest, name string) error
	// Apply server-side-applies lt's TestRun for perNode.
	Apply(ctx context.Context, lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad, env RunnerEnv) error
	// Stage returns .status.stage of lt's TestRun. ErrNotFound when the
	// TestRun is absent on the remote — the one outcome callers branch on.
	Stage(ctx context.Context, lt *dfaasv1.LoadTest) (string, error)
	// Delete removes lt's TestRun; an already-absent TestRun is success.
	Delete(ctx context.Context, lt *dfaasv1.LoadTest) error
	// Logs returns the k6 runner Pod log(s) of lt's TestRun.
	Logs(ctx context.Context, lt *dfaasv1.LoadTest) (string, error)
	// StartProbe creates this node's reachability probe for lt: a short-lived
	// Pod that asks url once from the generator's own network. It does not
	// wait for the answer; ProbeResult reads it later.
	StartProbe(ctx context.Context, lt *dfaasv1.LoadTest, url string) error
	// ProbeResult reads the probe Pod's verdict; ProbeAbsent when there is none.
	ProbeResult(ctx context.Context, lt *dfaasv1.LoadTest) (ProbeOutcome, error)
	// DeleteProbe deletes the probe Pod; an absent one is success.
	DeleteProbe(ctx context.Context, lt *dfaasv1.LoadTest) error
}

// RunnerEnv is what the reconciler decides and the adapter injects into the k6
// runner: the URLs the generated script talks to. Every value is per
// generator, built on the management address detected for it when there is
// one (Generator.MgmtAddr). The reconciler owns their computation (it knows
// the filer topology); the adapter owns where they go.
type RunnerEnv struct {
	// SummaryURL is always injected, even empty: scripts without handleSummary
	// ignore it and an unresolvable public base is "no upload", not an error.
	SummaryURL string
	// SyncURL is injected only when non-empty (syncStart tests).
	SyncURL string
	// AssetBase is injected as DFAAS_ASSET_BASE only when non-empty: the S3
	// NodePort base on the management address detected for this generator.
	// Empty means none was detected, and the script fetches its payload
	// assets from the absolute URL the gateway baked into it.
	AssetBase string
}

// ErrNotFound is returned by Node.Stage when the remote TestRun is absent.
var ErrNotFound = errors.New("remote TestRun not found")

// remoteNamespace is where TestRuns and mirrored ConfigMaps live on every k6
// node's k3s.
const remoteNamespace = "default"

// remoteRequestTimeout caps each remote-cluster API request (dial + round
// trip). Without it a dead k6 node blocks on the OS-default TCP connect
// (~30s), stalling the single-threaded reconcile worker.
const remoteRequestTimeout = 10 * time.Second

// TestRunGVK is the GroupVersionKind of k6-operator's TestRun.
var TestRunGVK = schema.GroupVersionKind{
	Group:   "k6.io",
	Version: "v1alpha1",
	Kind:    "TestRun",
}

// maxTestRunName bounds a remote TestRun name. k6-operator names its Jobs
// after the TestRun, the longest being <testrun>-initializer, and Kubernetes
// copies a Job's name into the 63-byte job-name pod label: past this length
// the initializer CREATE fails and the TestRun waits for its pod forever.
// Suffix read off k6-operator v0.0.15, the version chart 3.7.0 installs
// (setup-k6-nodes.yml; TestK6ChartVersionIsPinned holds the two together).
const maxTestRunName = 63 - len("-initializer")

// ScriptOwnerLabel carries the UID of the LoadTest that mirrored a script
// ConfigMap onto a generator. Two LoadTests can share a script name (the YAML
// import accepts any existing ConfigMap), so DeleteScript deletes only the
// copy its own test put there.
const ScriptOwnerLabel = "dfaas.io/loadtest-uid"

// mirrorConfigMap is the remote copy of src for lt: same name and data, in the
// namespace the TestRun lives in, labelled with lt's UID.
func mirrorConfigMap(lt *dfaasv1.LoadTest, src *corev1.ConfigMap) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: src.Name, Namespace: remoteNamespace,
			Labels: map[string]string{ScriptOwnerLabel: string(lt.UID)}},
		Data: src.Data,
	}
}

// TestRunName is the deterministic remote TestRun name for lt on nodeID.
// k6-operator uses "<testrun>-<n>" as the runner pod's hostname, a DNS-1123
// label, so the LoadTest name goes through Sanitize like the nodeID: the
// gateway admits dotted names, and a dot would leave the TestRun at stage
// "initialized" forever. A name that needs no change and fits maxTestRunName
// is returned as is, so every name an older release ran still resolves. Any
// other name keeps its head and ends in the fnv32a hash of the unmapped name:
// that keeps "exp.1" apart from "exp-1", and two generated LoadTest names
// (trailing nonce) or two nodeIDs apart when the cut drops where they differ.
func TestRunName(lt *dfaasv1.LoadTest, nodeID string) string {
	raw := fmt.Sprintf("%s-%s", lt.Name, Sanitize(nodeID))
	name := fmt.Sprintf("%s-%s", Sanitize(lt.Name), Sanitize(nodeID))
	if name == raw && len(name) <= maxTestRunName {
		return name
	}
	h := fnv.New32a()
	h.Write([]byte(raw))
	suffix := fmt.Sprintf("-%08x", h.Sum32())
	if len(name) > maxTestRunName-len(suffix) {
		name = name[:maxTestRunName-len(suffix)]
	}
	return strings.TrimRight(name, "-") + suffix
}

// K6LogConfigMap is the Management cluster ConfigMap holding one Generator's
// k6 end-of-test summary. One definition, because a mismatch between the two
// sides would be invisible: captureK6Logs writes this ConfigMap, runExporter
// asks the exporter Job to mount it, and the projection is Optional so that a
// missing ConfigMap cannot wedge the Pod. A drifted name therefore mounts an
// empty dir, the exporter finds no .log files and returns silently,
// MetricsExported goes True, the LoadTest reports Completed -- and the
// per-Generator k6 logs are gone.
func K6LogConfigMap(lt *dfaasv1.LoadTest, nodeID string) string {
	return fmt.Sprintf("%s-k6log-%s", lt.Name, Sanitize(nodeID))
}

// TargetNodeIDs is spec.perNodeLoad union status.testRuns, sorted. The abort
// sweep and the deletion finalizer's NotFound poll MUST agree on this set: a
// poll set smaller than the sweep set releases the CR while a remote TestRun is
// still live, which is the exact gap the finalizer exists to close. Sorted so
// per-node log order is deterministic across reconciles.
func TargetNodeIDs(lt *dfaasv1.LoadTest) []string {
	seen := make(map[string]struct{}, len(lt.Status.TestRuns)+len(lt.Spec.PerNodeLoad))
	for _, ref := range lt.Status.TestRuns {
		seen[ref.NodeID] = struct{}{}
	}
	for _, perNode := range lt.Spec.PerNodeLoad {
		seen[perNode.NodeID] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Sanitize lowercases and replaces non-DNS-1123 characters with "-", making a
// value safe inside Kubernetes object names. Shared with the reconciler's
// management-cluster names (k6 log ConfigMaps, summary object keys) so the
// two sides never disagree on how a nodeID is spelled.
func Sanitize(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

// k6NodeStatus is nodeID's entry in env.Status.K6Nodes: the one lookup both
// ResolveKubeconfig and GeneratorOf read the fleet through.
func k6NodeStatus(env *dfaasv1.Environment, nodeID string) (dfaasv1.K6NodeStatus, bool) {
	for _, n := range env.Status.K6Nodes {
		if n.NodeID == nodeID {
			return n, true
		}
	}
	return dfaasv1.K6NodeStatus{}, false
}

// ResolveKubeconfig applies the node-usability policy and returns the
// management-cluster Secret holding nodeID's kubeconfig. Shared by Live and
// the fake so both adapters agree on what "unusable" means. The Secret lives
// in the Environment's namespace — the playbook pushes it there. An empty
// managementAddress is not part of the policy: it only moves that
// generator's URLs onto the fallback base (GeneratorOf).
func ResolveKubeconfig(env *dfaasv1.Environment, nodeID string) (types.NamespacedName, error) {
	n, ok := k6NodeStatus(env, nodeID)
	if !ok {
		return types.NamespacedName{}, fmt.Errorf("k6 node %q is no longer part of the environment", nodeID)
	}
	if n.KubeconfigSecret == "" {
		return types.NamespacedName{}, fmt.Errorf("k6 node %q has no kubeconfig Secret on the environment", nodeID)
	}
	return types.NamespacedName{Name: n.KubeconfigSecret, Namespace: env.Namespace}, nil
}

// Generator is one k6-load-generator as the URLs its runner dials back are
// built for it: its nodeID and the management address detected for it at
// provisioning (status.k6Nodes[].managementAddress), already validated. A
// value type rather than two strings, so a caller cannot swap them.
type Generator struct {
	NodeID string
	// MgmtAddr is the canonical management address, or "" when none was
	// detected or the recorded one is not usable (ParseManagementAddress):
	// the URLs then fall back to the process-wide base.
	MgmtAddr string
}

// GeneratorOf reads nodeID's management address off env's status. A node
// absent from status, or one whose address fails ParseManagementAddress, gets
// MgmtAddr "": a fallback URL, never a broken one in the TestRun or probe env.
func GeneratorOf(env *dfaasv1.Environment, nodeID string) Generator {
	g := Generator{NodeID: nodeID}
	if n, ok := k6NodeStatus(env, nodeID); ok {
		g.MgmtAddr, _ = ParseManagementAddress(n.ManagementAddress)
	}
	return g
}

// ParseManagementAddress validates a recorded management address and returns
// its canonical spelling. Only a bare IP literal a remote generator can dial
// back passes: no zone (a URL carrying "%eth0" is one the k6 runner's URL
// parser rejects), no port, no brackets (the URL builders add them), and no
// unspecified, loopback, multicast or link-local address. A v4-mapped IPv6
// address is unmapped to its IPv4 form. Shared with the Environment side,
// which reads the value off the kubeconfig Secret annotation.
func ParseManagementAddress(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	// ParseAddr already refuses a port and brackets; it accepts a zone.
	addr, err := netip.ParseAddr(s)
	if err != nil || addr.Zone() != "" {
		return "", false
	}
	addr = addr.Unmap()
	if addr.IsUnspecified() || addr.IsLoopback() || addr.IsMulticast() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() {
		return "", false
	}
	return addr.String(), true
}

// StageOf reads .status.stage from an unstructured TestRun.
func StageOf(tr *unstructured.Unstructured) string {
	stage, found, err := unstructured.NestedString(tr.Object, "status", "stage")
	if err != nil || !found {
		return ""
	}
	return stage
}

// buildTestRun assembles the k6.io/v1alpha1 TestRun manifest for one
// PerNodeLoad entry. Returns an error if the spec map cannot be nested (a
// TestRun with an empty spec would start no runner at all).
func buildTestRun(name string, lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad, env RunnerEnv) (*unstructured.Unstructured, error) {
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(TestRunGVK)
	tr.SetName(name)
	tr.SetNamespace(remoteNamespace)
	tr.SetLabels(map[string]string{
		"dfaas.io/loadtest-name": lt.Name,
		"dfaas.io/node-id":       perNode.NodeID,
	})
	spec := map[string]interface{}{
		"parallelism": int64(1),
		"script": map[string]interface{}{
			"configMap": map[string]interface{}{
				"name": perNode.ScriptConfigMap.Name,
				"file": "script.js",
			},
		},
		// VUs and duration deliberately do not appear here: k6 takes both from
		// the script's own options, and no TestRun field carries them.
		// spec.perNodeLoad on the LoadTest CR still records both, which is
		// where the UI reads them from.
	}
	runnerEnv := []interface{}{
		map[string]interface{}{"name": "DFAAS_SUMMARY_URL", "value": env.SummaryURL},
	}
	if env.SyncURL != "" {
		// The generated script's setup() blocks polling this URL until the
		// reconciler publishes the GO signal. Scripts without the barrier
		// simply ignore the env var.
		runnerEnv = append(runnerEnv,
			map[string]interface{}{"name": "DFAAS_SYNC_URL", "value": env.SyncURL})
	}
	if env.AssetBase != "" {
		// The generated script fetches a relocatable payload asset from this
		// base plus the object's path; without it, from the URL baked in.
		runnerEnv = append(runnerEnv,
			map[string]interface{}{"name": "DFAAS_ASSET_BASE", "value": env.AssetBase})
	}
	spec["runner"] = map[string]interface{}{"env": runnerEnv}
	if err := unstructured.SetNestedMap(tr.Object, spec, "spec"); err != nil {
		return nil, fmt.Errorf("set TestRun spec: %w", err)
	}
	return tr, nil
}

// ── Live adapter ────────────────────────────────────────────────────────────

// Live is the production Dispatcher: it reads kubeconfig Secrets off the
// management cluster and talks directly to each remote API server, so the
// management ServiceAccount needs no RBAC on the remote TestRun resources.
type Live struct {
	// Local is the management-cluster client, used to read kubeconfig Secrets.
	Local client.Client
}

// Node implements Dispatcher.
func (l *Live) Node(_ context.Context, env *dfaasv1.Environment, nodeID string) (Node, error) {
	secretRef, err := ResolveKubeconfig(env, nodeID)
	if err != nil {
		return nil, err
	}
	return &liveNode{local: l.Local, nodeID: nodeID, secretRef: secretRef}, nil
}

type liveNode struct {
	local     client.Client
	nodeID    string
	secretRef types.NamespacedName
}

func (n *liveNode) Ref(lt *dfaasv1.LoadTest) dfaasv1.TestRunRef {
	return dfaasv1.TestRunRef{NodeID: n.nodeID, Name: TestRunName(lt, n.nodeID), Namespace: remoteNamespace}
}

// restConfig reads the kubeconfig Secret (key "kubeconfig") and parses it
// into a *rest.Config with the remote-request timeout applied. Shared by the
// controller-runtime client and the typed clientset used for logs. The remote
// client is rebuilt on every call on purpose, so a kubeconfig Secret rewritten
// by a re-provision is never served stale.
func (n *liveNode) restConfig(ctx context.Context) (*rest.Config, error) {
	var sec corev1.Secret
	if err := n.local.Get(ctx, n.secretRef, &sec); err != nil {
		return nil, fmt.Errorf("read kubeconfig secret %s/%s: %w", n.secretRef.Namespace, n.secretRef.Name, err)
	}
	raw, ok := sec.Data["kubeconfig"]
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("secret %s/%s missing key \"kubeconfig\"", n.secretRef.Namespace, n.secretRef.Name)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("parse kubeconfig: %w", err)
	}
	cfg.Timeout = remoteRequestTimeout
	return cfg, nil
}

func (n *liveNode) remoteClient(ctx context.Context) (client.Client, error) {
	cfg, err := n.restConfig(ctx)
	if err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{})
}

func (n *liveNode) MirrorConfigMap(ctx context.Context, lt *dfaasv1.LoadTest, src *corev1.ConfigMap) error {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return err
	}
	cm := mirrorConfigMap(lt, src)
	if err := rc.Patch(ctx, cm, client.Apply, client.FieldOwner("dfaas-operator")); err != nil {
		return fmt.Errorf("apply remote ConfigMap %s/%s: %w", remoteNamespace, src.Name, err)
	}
	return nil
}

func (n *liveNode) DeleteScript(ctx context.Context, lt *dfaasv1.LoadTest, name string) error {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return err
	}
	var cm corev1.ConfigMap
	if err := rc.Get(ctx, client.ObjectKey{Namespace: remoteNamespace, Name: name}, &cm); err != nil {
		return client.IgnoreNotFound(err)
	}
	// Mirrored by another test (or by a release that did not label it).
	if cm.Labels[ScriptOwnerLabel] != string(lt.UID) {
		return nil
	}
	// The preconditions keep a copy re-mirrored between the Get and the
	// Delete: server-side apply changes its resourceVersion.
	err = rc.Delete(ctx, &cm, client.Preconditions{UID: &cm.UID, ResourceVersion: &cm.ResourceVersion})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	return err
}

func (n *liveNode) StartProbe(ctx context.Context, lt *dfaasv1.LoadTest, url string) error {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return err
	}
	if err := rc.Create(ctx, probePod(lt, n.nodeID, url)); err != nil {
		return fmt.Errorf("create probe Pod %s/%s: %w", remoteNamespace, ProbeName(lt, n.nodeID), err)
	}
	return nil
}

func (n *liveNode) ProbeResult(ctx context.Context, lt *dfaasv1.LoadTest) (ProbeOutcome, error) {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return ProbeOutcome{}, err
	}
	var p corev1.Pod
	if err := rc.Get(ctx, client.ObjectKey{Namespace: remoteNamespace, Name: ProbeName(lt, n.nodeID)}, &p); err != nil {
		if apierrors.IsNotFound(err) {
			return ProbeOutcome{State: ProbeAbsent}, nil
		}
		return ProbeOutcome{}, err
	}
	return probeOutcome(lt, &p), nil
}

func (n *liveNode) DeleteProbe(ctx context.Context, lt *dfaasv1.LoadTest) error {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return err
	}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: remoteNamespace, Name: ProbeName(lt, n.nodeID)}}
	return client.IgnoreNotFound(rc.Delete(ctx, p))
}

func (n *liveNode) Apply(ctx context.Context, lt *dfaasv1.LoadTest, perNode dfaasv1.PerNodeLoad, env RunnerEnv) error {
	tr, err := buildTestRun(TestRunName(lt, n.nodeID), lt, perNode, env)
	if err != nil {
		return err
	}
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return err
	}
	if err := rc.Patch(ctx, tr, client.Apply, client.FieldOwner("dfaas-operator")); err != nil {
		return fmt.Errorf("apply remote TestRun: %w", err)
	}
	return nil
}

func (n *liveNode) Stage(ctx context.Context, lt *dfaasv1.LoadTest) (string, error) {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return "", err
	}
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(TestRunGVK)
	key := types.NamespacedName{Name: TestRunName(lt, n.nodeID), Namespace: remoteNamespace}
	if err := rc.Get(ctx, key, tr); err != nil {
		if apierrors.IsNotFound(err) {
			return "", ErrNotFound
		}
		return "", err
	}
	return StageOf(tr), nil
}

func (n *liveNode) Delete(ctx context.Context, lt *dfaasv1.LoadTest) error {
	rc, err := n.remoteClient(ctx)
	if err != nil {
		return err
	}
	tr := &unstructured.Unstructured{}
	tr.SetGroupVersionKind(TestRunGVK)
	tr.SetName(TestRunName(lt, n.nodeID))
	tr.SetNamespace(remoteNamespace)
	return client.IgnoreNotFound(rc.Delete(ctx, tr))
}

// k6LogLimitBytes caps the per-pod log read so a runaway runner log cannot
// blow up the operator's memory or the downstream ConfigMap (1 MiB etcd
// limit). A large summary can exceed the cap (283 KB measured on 2026-08-29),
// and LimitBytes keeps the head of the log, so the summary printed last is
// what gets cut.
const k6LogLimitBytes int64 = 262144

// Logs streams the k6 end-of-test summary from the runner Pod(s). k6-operator
// labels runner Pods app=k6,k6_cr=<testRunName>,runner=true and does not set
// cleanup=post, so finished Pods (and their logs) are retained. With more than
// one runner Pod (parallelism > 1) each block is prefixed with a
// "==== pod <name> ====" header.
func (n *liveNode) Logs(ctx context.Context, lt *dfaasv1.LoadTest) (string, error) {
	cfg, err := n.restConfig(ctx)
	if err != nil {
		return "", err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", fmt.Errorf("build remote clientset: %w", err)
	}
	testRunName := TestRunName(lt, n.nodeID)
	selector := fmt.Sprintf("app=k6,k6_cr=%s,runner=true", testRunName)
	pods, err := clientset.CoreV1().Pods(remoteNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", fmt.Errorf("list k6 runner pods (%s): %w", selector, err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no k6 runner pod found for TestRun %q in namespace %q (selector %s)",
			testRunName, remoteNamespace, selector)
	}
	multi := len(pods.Items) > 1
	var b strings.Builder
	for _, pod := range pods.Items {
		logs, lerr := readPodLogs(ctx, clientset, remoteNamespace, pod.Name)
		if lerr != nil {
			return "", fmt.Errorf("stream logs for pod %s/%s: %w", remoteNamespace, pod.Name, lerr)
		}
		if multi {
			fmt.Fprintf(&b, "==== pod %s ====\n", pod.Name)
		}
		b.WriteString(logs)
		if multi && !strings.HasSuffix(logs, "\n") {
			b.WriteByte('\n')
		}
	}
	return b.String(), nil
}

func readPodLogs(ctx context.Context, clientset kubernetes.Interface, namespace, podName string) (string, error) {
	req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		Container:  "k6",
		LimitBytes: ptr.To(k6LogLimitBytes),
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
