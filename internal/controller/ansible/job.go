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

// CreateJob costruisce il Job Ansible per il provisioning dei nodi della
// federazione e il Secret con l'inventory generato dalla Federation.
func (m *Manager) CreateJob(ctx context.Context, exp *dfaasv1.Esperimento) (*batchv1.Job, *corev1.Secret, error) {

	// 1. Assicuriamoci che le ConfigMap dei values esistano PRIMA di definire il Job
	if err := m.EnsureHelmValues(ctx, exp); err != nil {
		return nil, nil, fmt.Errorf("failed to ensure helm values config: %w", err)
	}

	if err := m.EnsureConfigMap(ctx, exp); err != nil {
		return nil, nil, fmt.Errorf("failed to ensure ansible config map: %w", err)
	}

	if len(exp.Spec.Federation.Nodes) == 0 {
		return nil, nil, fmt.Errorf("no nodes specified in federation")
	}

	firstPeerID, _ := calcolaPeerID(exp.Spec.Federation.Nodes[0].PrivateKey)

	var inventory string
	inventory += "[target_Nodess]\n"

	for i, Nodes := range exp.Spec.Federation.Nodes {
		if Nodes.IpAddress != "" {
			peerID, err := calcolaPeerID(Nodes.PrivateKey)
			if err != nil {
				peerID = "error-key"
			}

			isBootstrap := (i != 0)
			bootstrapAddr := fmt.Sprintf("/ip4/%s/tcp/31600/p2p/%s", exp.Spec.Federation.Nodes[0].IpAddress, firstPeerID)
			if !isBootstrap {
				bootstrapAddr = ""
			}

			NodesFunctionsJson, _ := json.Marshal(Nodes.Functions)

			line := fmt.Sprintf("%s ansible_user=%s ansible_password=%s node_specific_functions='%s' node_priv_key='%s' dfaas_agent_id='%s' is_bootstrap=%t bootstrap_address='%s' balancing_strategy='%s'\n",
				Nodes.IpAddress,
				Nodes.Username,
				Nodes.Password,
				string(NodesFunctionsJson),
				Nodes.PrivateKey,
				peerID,
				isBootstrap,
				bootstrapAddr,
				Nodes.BalancingStrategy,
			)
			inventory += line
		}
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-ansible-inventory",
			Namespace: exp.Namespace,
		},
		StringData: map[string]string{
			"hosts": inventory,
		},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-infra-job",
			Namespace: exp.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(3), //default se non specificato è 6
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "ansible-worker",
							Image:   "alpine/ansible:2.20.0",
							Command: []string{"sh", "-c"},
							Args: []string{
								"ansible-playbook -i /etc/ansible/hosts /ansible/playbooks/setup-Nodes.yml " +
									"--extra-vars \"ansible_ssh_common_args='-o StrictHostKeyChecking=no'\"",
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "inventory-volume", MountPath: "/etc/ansible"},
								{Name: "playbook-volume", MountPath: "/ansible/playbooks"},
								{Name: "helm-values-volume", MountPath: "/opt/helm-values"},
							},
						},
					},
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
									LocalObjectReference: corev1.LocalObjectReference{Name: "ansible-playbooks-" + exp.Name},
								},
							},
						},
						{
							Name: "helm-values-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{Name: "helm-values-config-" + exp.Name},
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}

	_ = ctrl.SetControllerReference(exp, secret, m.Scheme)
	_ = ctrl.SetControllerReference(exp, job, m.Scheme)

	return job, secret, nil
}
