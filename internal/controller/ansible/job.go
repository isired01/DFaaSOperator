/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/roles"
)

// libp2pBootstrapPort is the TCP port advertised in the dfaas-agent bootstrap
// multiaddr (/ip4/<node0 ip>/tcp/<port>/p2p/<peerID>). It MUST match the port
// the dfaas-agent-chart actually exposes the agent's libp2p listener on at the
// node host (NodePort / hostPort / hostNetwork) — otherwise non-seed workers
// fail to dial the seed ("all dials failed / dial backoff"). Single source of
// truth: re-point here if the chart's libp2p host exposure changes.
const libp2pBootstrapPort = 31600

// ansibleJobDeadlineSeconds caps a single Ansible run's wall-clock time. Without
// it a playbook that hangs (SSH blackhole after the :22 probe, a stuck
// `curl | sh` install) never fails its pod, so BackoffLimit never trips and the
// Environment FSM waits on Job completion indefinitely. 30 min is generous for a
// full k3s + Helm + OpenFaaS install.
// ponytail: fixed ceiling; lift it if provisioning legitimately runs longer.
const ansibleJobDeadlineSeconds int64 = 1800

// CreateJobForRole builds an Ansible Job + inventory Secret for the subset of
// nodes in env that match role. Which playbook, inventory group and name
// suffix a role uses comes from the roles table, so the two provisioning
// streams run independent Jobs against distinct node sets and a role the
// table does not know is an error rather than a silent fall-through.
func (m *Manager) CreateJobForRole(ctx context.Context, env *dfaasv1.Environment,
	role dfaasv1.NodeRole, libp2pKeys map[string]string) (*batchv1.Job, *corev1.Secret, error) {

	spec, err := roles.For(role)
	if err != nil {
		return nil, nil, err
	}
	nodes := env.NodesWithRole(role)
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes with role %q in environment %q", role, env.Name)
	}

	if err := m.ensureHelmValues(ctx, env); err != nil {
		return nil, nil, fmt.Errorf("ensure helm values: %w", err)
	}
	if err := m.ensurePlaybookConfigMap(ctx, env, spec); err != nil {
		return nil, nil, fmt.Errorf("ensure playbook configmap: %w", err)
	}
	saName, err := m.ensureRBAC(ctx, env)
	if err != nil {
		return nil, nil, fmt.Errorf("ensure RBAC: %w", err)
	}

	playbookFile := spec.Playbook
	playbookCMName := playbookConfigMapName(env, spec)

	inventory, err := buildInventory(env, spec, nodes, libp2pKeys)
	if err != nil {
		return nil, nil, fmt.Errorf("build inventory: %w", err)
	}
	labels := jobLabels(env, role)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      inventorySecretName(env, spec.JobSuffix),
			Namespace: env.Namespace,
			Labels:    labels,
		},
		StringData: map[string]string{"hosts": inventory},
	}

	// PodReplacementPolicy=Failed retains failed Pods for post-mortem debug:
	// Job controller waits for full Pod termination before replacing and does
	// not delete failed Pods on BackoffLimit exceeded (TTL handles cleanup).
	prFailed := batchv1.Failed
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      JobNameForRole(env, spec.JobSuffix),
			Namespace: env.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          ptr.To[int32](3),
			ActiveDeadlineSeconds: ptr.To[int64](ansibleJobDeadlineSeconds),
			PodReplacementPolicy:  &prFailed,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					ServiceAccountName: saName,
					Containers: []corev1.Container{{
						Name:    "ansible-worker",
						Image:   "alpine/ansible:2.18.6",
						Command: []string{"sh", "-c"},
						Args: []string{
							fmt.Sprintf(
								"set -e; "+
									"apk add --no-cache py3-pip && "+
									"pip3 install --break-system-packages --quiet kubernetes && "+
									"ansible-galaxy collection install -r /ansible/playbooks/requirements.yml && "+
									"ansible-playbook -i /etc/ansible/hosts /ansible/playbooks/%s "+
									"--extra-vars \"ansible_ssh_common_args='-o StrictHostKeyChecking=no'\"",
								playbookFile),
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "inventory-volume", MountPath: "/etc/ansible"},
							{Name: "playbook-volume", MountPath: "/ansible/playbooks"},
							{Name: "helm-values-volume", MountPath: "/opt/helm-values"},
						},
					}},
					Volumes: []corev1.Volume{
						{
							Name: "inventory-volume",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{SecretName: secret.Name},
							},
						},
						{
							Name: "playbook-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: playbookCMName},
								},
							},
						},
						{
							Name: "helm-values-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "helm-values-config-" + env.Name},
								},
							},
						},
					},
					// RestartPolicyNever ensures each retry creates a distinct Pod;
					// OnFailure restarts the container in-place and loses prior attempt logs.
					RestartPolicy: corev1.RestartPolicyNever,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(env, secret, m.Scheme); err != nil {
		return nil, nil, fmt.Errorf("set controller ref on inventory secret: %w", err)
	}
	if err := ctrl.SetControllerReference(env, job, m.Scheme); err != nil {
		return nil, nil, fmt.Errorf("set controller ref on ansible job: %w", err)
	}
	return job, secret, nil
}

// buildInventory builds the Ansible inventory text for the given role + node
// subset. Only dfaas-worker nodes get bootstrap-peer + balancing-strategy
// vars; k6 nodes get a minimal inventory.
//
// libp2pKeys maps nodeID → base64 PKCS#8 ed25519 key. Populated by
// EnsureLibp2pKeys for dfaas-worker nodes (Secret-backed, generated on first
// reconcile, optionally pre-applied for BYO peer identity). Authoritative
// source: there is no spec field for the key.
func buildInventory(env *dfaasv1.Environment, spec roles.Spec,
	nodes []dfaasv1.EnvironmentNode, libp2pKeys map[string]string) (string, error) {
	var inv string
	switch spec.Role {
	case dfaasv1.RoleDfaasWorker:
		inv = "[" + spec.Group + "]\n"

		// Every peer ID up front: node i's bootstrap list carries the peer IDs
		// of nodes 0..i-1, so they must all be known before the loop emits a
		// line. A malformed key would poison the mesh, so fail loudly here
		// instead of emitting an empty peer ID that only surfaces as an opaque
		// agent dial-backoff crash-loop later.
		peerIDs := make([]string, len(nodes))
		for i, n := range nodes {
			id, err := derivePeerID(libp2pKeys[n.NodeID])
			if err != nil {
				return "", fmt.Errorf("derive libp2p peer ID for node %q (check its entry in Secret %q): %w",
					n.NodeID, env.Name+"-libp2p-keys", err)
			}
			peerIDs[i] = id
		}
		for i, n := range nodes {
			privKey := libp2pKeys[n.NodeID]
			peerID := peerIDs[i]

			// The bootstrap list IS the federation topology, permanently. The
			// agent dials it once, in kademlia.Initialize, and the Kademlia
			// discovery meant to complete the mesh afterwards never returns a
			// peer: the agent announces only its pod address (10.42.0.0/24 —
			// the SAME subnet on every single-node k3s), so a peer learned
			// through the DHT would be undialable anyway. Measured 2026-09-09
			// on env `lab`: the seed advertised no /ipfs/kad/1.0.0 protocol at
			// all, every node's routing table was empty, and not one agent had
			// ever logged "Found a new peer" — with only the seed's address in
			// the list the result was a star (seed 4 conns, everyone else 1),
			// decaying to 0 peers on each node whose single link dropped.
			//
			// So list nodes 0..i-1 for node i: every pair is dialed exactly
			// once (a full mesh), and no node ever waits on a node that is
			// waiting on it, which matters because AGENT_BOOTSTRAP_FORCE
			// blocks Initialize until every listed peer answers. Comma
			// separated: the agent parses the env var with viper into
			// []string, which splits on commas. Verified live: node l3 given
			// all four addresses reached 4/4 peers in 400ms.
			var addrs []string
			for j := 0; j < i; j++ {
				addrs = append(addrs, fmt.Sprintf("/ip4/%s/tcp/%d/p2p/%s",
					nodes[j].IPAddress, libp2pBootstrapPort, peerIDs[j]))
			}
			isBootstrap := i != 0
			bootstrap := strings.Join(addrs, ",")
			fnJSON, _ := json.Marshal(n.Functions)
			inv += fmt.Sprintf(
				"%s ansible_user=%s ansible_password=%s node_specific_functions='%s' "+
					"node_priv_key='%s' dfaas_agent_id='%s' is_bootstrap=%t bootstrap_address='%s' "+
					"balancing_strategy='%s'\n",
				n.IPAddress, n.Username, n.Password, string(fnJSON),
				privKey, peerID, isBootstrap, bootstrap, string(n.BalancingStrategy),
			)
		}
	case dfaasv1.RoleK6LoadGenerator:
		inv = "[" + spec.Group + "]\n"
		for _, n := range nodes {
			// env_uid feeds the ownerReference the playbook stamps on the
			// kubeconfig Secret it pushes back to the management cluster, so
			// that Secret is garbage-collected with the Environment instead of
			// lingering with dead credentials after a role flip.
			inv += fmt.Sprintf(
				"%s ansible_user=%s ansible_password=%s node_id='%s' env_name='%s' env_namespace='%s' env_uid='%s'\n",
				n.IPAddress, n.Username, n.Password, n.NodeID, env.Name, env.Namespace, env.UID,
			)
		}
	default:
		// The host lines differ structurally per role, so this switch stays;
		// a role without an arm is now an error instead of an empty inventory.
		return "", fmt.Errorf("no inventory layout for node role %q", spec.Role)
	}
	return inv, nil
}

func playbookConfigMapName(env *dfaasv1.Environment, spec roles.Spec) string {
	return fmt.Sprintf("ansible-playbooks-%s-%s", spec.ConfigMapSuffix, env.Name)
}

// shortUID returns the first 8 chars of env.UID, or the whole UID if shorter.
// Embedded into Job + Secret names so the reconciler never recovers a stale
// resource from a previously-deleted Environment that happened to share the
// same name.
func shortUID(env *dfaasv1.Environment) string {
	uid := string(env.UID)
	if len(uid) > 8 {
		return uid[:8]
	}
	return uid
}

// MaxJobNameLen is the Kubernetes label-value limit, which is what actually
// bounds a Job name: with no explicit selector the Job controller copies the
// Job's name verbatim into the auto-generated `job-name` /
// `batch.kubernetes.io/job-name` pod-template labels. Object names may run to
// 253 characters, but a Job named longer than this is rejected at CREATE with
//
//	spec.template.labels: Invalid value: "<name>": must be no more than 63 bytes
//
// and the reconciler then retries forever. Observed in the wild on an exporter
// Job for a 41-character LoadTest name.
const MaxJobNameLen = 63

// BoundedJobName joins prefix+suffix, truncating the PREFIX (the caller-chosen
// resource name) so the result fits MaxJobNameLen. The suffix carries the UID
// and generation that make the name unique, so it is preserved intact — two
// long names sharing a prefix stay distinct. Any trailing '-' left by the cut
// is trimmed so the result remains a valid DNS-1123 name.
func BoundedJobName(prefix, suffix string) string {
	if len(prefix)+len(suffix) <= MaxJobNameLen {
		return prefix + suffix
	}
	keep := MaxJobNameLen - len(suffix)
	if keep < 0 {
		keep = 0
	}
	return strings.TrimRight(prefix[:keep], "-") + suffix
}

// JobNameForRole returns the deterministic Ansible Job name. Includes
// `env.UID[:8]` to scope across delete+recreate of the same env.Name, and
// `env.Generation` so a spec edit gets a fresh Job rather than reusing the
// previous run's status.
func JobNameForRole(env *dfaasv1.Environment, jobSuffix string) string {
	return BoundedJobName(env.Name, fmt.Sprintf("-infra-%s-%s-g%d-job",
		jobSuffix, shortUID(env), env.Generation))
}

// inventorySecretName mirrors JobNameForRole for the per-Job inventory Secret.
func inventorySecretName(env *dfaasv1.Environment, jobSuffix string) string {
	return fmt.Sprintf("%s-ansible-%s-%s-g%d-inventory",
		env.Name, jobSuffix, shortUID(env), env.Generation)
}

// Label keys for Ansible Jobs + inventory Secrets. Used by the operator's
// stale-gen cleanup helper to select previous-generation resources via
// `client.MatchingLabels` on Environment-spec drift.
const (
	LabelEnvironment = "dfaas.io/environment"
	LabelRole        = "dfaas.io/role"
	LabelGeneration  = "dfaas.io/generation"
)

// Label keys the k6 playbook stamps on the per-node kubeconfig Secret it
// pushes back into the management cluster. Note these are NOT LabelEnvironment
// above: that one tags Jobs and inventory Secrets with "dfaas.io/environment",
// while the kubeconfig Secret uses the shorter "dfaas.io/env". Both halves are
// hand-synced with templates/setup-k6-nodes.yml, which is the source of truth.
const (
	LabelKubeconfigEnv    = "dfaas.io/env"
	LabelKubeconfigNodeID = "dfaas.io/node-id"
)

// jobLabels returns the canonical label set for an Ansible Job + its
// inventory Secret. Includes env.Name, role and env.Generation so the
// reconciler can List+Delete previous-gen resources without parsing names.
func jobLabels(env *dfaasv1.Environment, role dfaasv1.NodeRole) map[string]string {
	return map[string]string{
		LabelEnvironment: env.Name,
		LabelRole:        string(role),
		LabelGeneration:  fmt.Sprintf("%d", env.Generation),
	}
}
