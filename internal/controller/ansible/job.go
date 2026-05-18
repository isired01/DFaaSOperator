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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
)

// CreateJobForRole builds an Ansible Job + inventory Secret for the subset of
// nodes in env that match role. jobSuffix becomes part of the Job/Secret/
// playbook-ConfigMap name so VM and K6 phases run independent Jobs against
// distinct node sets. The playbook chosen depends on the role:
//
//   - dfaas-worker        → setup-nodes.yml (base OS + dFaaS install)
//   - k6-load-generator   → setup-k6-nodes.yml (k3s + k6-operator)
func (m *Manager) CreateJobForRole(ctx context.Context, env *dfaasv1.Environment,
	role dfaasv1.NodeRole, jobSuffix string,
	libp2pKeys map[string]string) (*batchv1.Job, *corev1.Secret, error) {

	nodes := env.NodesWithRole(role)
	if len(nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes with role %q in environment %q", role, env.Name)
	}

	if err := m.EnsureHelmValues(ctx, env); err != nil {
		return nil, nil, fmt.Errorf("ensure helm values: %w", err)
	}
	if err := m.EnsurePlaybookConfigMap(ctx, env, role); err != nil {
		return nil, nil, fmt.Errorf("ensure playbook configmap: %w", err)
	}
	saName, err := m.EnsureRBAC(ctx, env)
	if err != nil {
		return nil, nil, fmt.Errorf("ensure RBAC: %w", err)
	}

	playbookFile := playbookFileForRole(role)
	playbookCMName := playbookConfigMapName(env, role)

	inventory := buildInventory(env, role, nodes, libp2pKeys)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-ansible-%s-inventory", env.Name, jobSuffix),
			Namespace: env.Namespace,
		},
		StringData: map[string]string{"hosts": inventory},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-infra-%s-job", env.Name, jobSuffix),
			Namespace: env.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(3),
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
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}

	_ = ctrl.SetControllerReference(env, secret, m.Scheme)
	_ = ctrl.SetControllerReference(env, job, m.Scheme)
	return job, secret, nil
}

// buildInventory builds the Ansible inventory text for the given role + node
// subset. Only dfaas-worker nodes get bootstrap-peer + balancing-strategy
// vars; k6 nodes get a minimal inventory.
//
// libp2pKeys maps nodeID → base64 PKCS#8 ed25519 key, populated by
// EnsureLibp2pKeys for dfaas-worker nodes. Precedence: spec.PrivateKey wins
// when non-empty; otherwise the operator-managed key from libp2pKeys is used.
func buildInventory(env *dfaasv1.Environment, role dfaasv1.NodeRole,
	nodes []dfaasv1.EnvironmentNode, libp2pKeys map[string]string) string {
	var inv string
	switch role {
	case dfaasv1.RoleDfaasWorker:
		inv = "[target_Nodess]\n"

		keyForNode := func(n dfaasv1.EnvironmentNode) string {
			if n.PrivateKey != "" {
				return n.PrivateKey
			}
			return libp2pKeys[n.NodeID]
		}

		firstPeerID, _ := calcolaPeerID(keyForNode(nodes[0]))
		for i, n := range nodes {
			privKey := keyForNode(n)
			peerID, err := calcolaPeerID(privKey)
			if err != nil {
				peerID = "error-key"
			}
			isBootstrap := i != 0
			bootstrap := ""
			if isBootstrap {
				bootstrap = fmt.Sprintf("/ip4/%s/tcp/31600/p2p/%s", nodes[0].IPAddress, firstPeerID)
			}
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
		inv = "[k6_nodes]\n"
		for _, n := range nodes {
			inv += fmt.Sprintf(
				"%s ansible_user=%s ansible_password=%s node_id='%s' env_name='%s' env_namespace='%s'\n",
				n.IPAddress, n.Username, n.Password, n.NodeID, env.Name, env.Namespace,
			)
		}
	}
	return inv
}

func playbookFileForRole(role dfaasv1.NodeRole) string {
	if role == dfaasv1.RoleK6LoadGenerator {
		return "setup-k6-nodes.yml"
	}
	return "setup-nodes.yml"
}

func playbookConfigMapName(env *dfaasv1.Environment, role dfaasv1.NodeRole) string {
	suffix := "dfaas"
	if role == dfaasv1.RoleK6LoadGenerator {
		suffix = "k6"
	}
	return fmt.Sprintf("ansible-playbooks-%s-%s", suffix, env.Name)
}
