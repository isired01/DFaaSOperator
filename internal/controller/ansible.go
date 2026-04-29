package controller

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (r *EsperimentoReconciler) createAnsibleJob(ctx context.Context, exp *dfaasv1.Esperimento) (*batchv1.Job, *corev1.Secret, error) {

	// 1. Assicuriamoci che le ConfigMap dei values esistano PRIMA di definire il Job
	if err := r.ensureHelmValuesConfig(ctx, exp); err != nil {
		return nil, nil, fmt.Errorf("failed to ensure helm values config: %w", err)
	}

	err := r.ensureAnsibleConfigMap(ctx, exp)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to ensure ansible config map: %w", err)
	}

	var inventory string
	inventory += "[target_nodes]\n"

	for i, node := range exp.Spec.Federazione.Nodi {
		if node.IndirizzoIP != "" {
			peerID, err := calcolaPeerID(node.ChiavePrivata)
			if err != nil {
				peerID = "error-key"
			}

			isBootstrap := (i == 0)
			bootstrapAddr := ""
			if !isBootstrap {
				firstNode := exp.Spec.Federazione.Nodi[0]
				firstPeerID, _ := calcolaPeerID(firstNode.ChiavePrivata)
				bootstrapAddr = fmt.Sprintf("/ip4/%s/tcp/31600/p2p/%s", firstNode.IndirizzoIP, firstPeerID)
			}

			nodeFunctionsJson, _ := json.Marshal(node.Funzioni)

			line := fmt.Sprintf("%s ansible_user=%s ansible_password=%s node_specific_functions='%s' node_priv_key='%s' dfaas_agent_id='%s' is_bootstrap=%t bootstrap_address='%s'\n",
				node.IndirizzoIP,
				node.UserName,
				node.Password,
				string(nodeFunctionsJson),
				node.ChiavePrivata,
				peerID,
				isBootstrap,
				bootstrapAddr,
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
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "ansible-worker",
							Image:   "alpine/ansible:2.20.0",
							Command: []string{"sh", "-c"},
							Args: []string{
								"ansible-playbook -i /etc/ansible/hosts /ansible/playbooks/setup-node.yml " +
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

	_ = ctrl.SetControllerReference(exp, secret, r.Scheme)
	_ = ctrl.SetControllerReference(exp, job, r.Scheme)

	return job, secret, nil
}

func (r *EsperimentoReconciler) ensureAnsibleConfigMap(ctx context.Context, exp *dfaasv1.Esperimento) error {
	cmName := "ansible-playbooks-" + exp.Name
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: exp.Namespace,
		},
		Data: map[string]string{
			"setup-node.yml": AnsiblePlaybookYaml,
		},
	}

	if err := ctrl.SetControllerReference(exp, cm, r.Scheme); err != nil {
		return err
	}

	found := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: exp.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		return r.Create(ctx, cm)
	} else if err == nil {
		found.Data = cm.Data
		return r.Update(ctx, found)
	}
	return err
}

func (r *EsperimentoReconciler) ensureHelmValuesConfig(ctx context.Context, exp *dfaasv1.Esperimento) error {
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "helm-values-config-" + exp.Name,
			Namespace: exp.Namespace,
		},
		Data: map[string]string{
			"haproxy.yaml":    HaproxyValues,
			"openfaas.yaml":   OpenfaasValues,
			"prometheus.yaml": PrometheusValues,
		},
	}

	if err := ctrl.SetControllerReference(exp, cm, r.Scheme); err != nil {
		return err
	}

	found := &corev1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Name: cm.Name, Namespace: exp.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		return r.Create(ctx, cm)
	} else if err == nil {
		found.Data = cm.Data
		return r.Update(ctx, found)
	}
	return err
}

func calcolaPeerID(privKeyBase64 string) (string, error) {
	// 1. Decodifica la stringa Base64 della chiave privata
	rawKey, err := base64.StdEncoding.DecodeString(privKeyBase64)
	if err != nil {
		return "", fmt.Errorf("errore decodifica base64: %v", err)
	}

	// 2. Verifica la lunghezza per una chiave Ed25519 (deve essere 64 byte: seed + pub)
	// Se la tua stringa è solo il seed (32 byte), ed25519.NewKeyFromSeed la gestisce
	var priv crypto.PrivKey
	if len(rawKey) == 32 {
		stdPriv := ed25519.NewKeyFromSeed(rawKey)
		priv, err = crypto.UnmarshalEd25519PrivateKey(stdPriv)
	} else {
		priv, err = crypto.UnmarshalEd25519PrivateKey(rawKey)
	}

	if err != nil {
		return "", fmt.Errorf("errore unmarshal chiave privata libp2p: %v", err)
	}

	// 3. Estrai il PeerID dalla chiave pubblica derivata
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("errore generazione PeerID: %v", err)
	}

	// Ritorna la stringa (es. "12D3KooW...")
	return id.String(), nil
}
