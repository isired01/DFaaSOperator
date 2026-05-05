package controller

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
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
			"setup-Nodes.yml": AnsiblePlaybookYaml,
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
	// 1. Decodifica la stringa Base64
	derBytes, err := base64.StdEncoding.DecodeString(privKeyBase64)
	if err != nil {
		return "", fmt.Errorf("errore decodifica base64: %v", err)
	}

	// 2. Parsing della chiave PKCS#8 (formato standard per le chiavi private Ed25519)
	rawKey, err := x509.ParsePKCS8PrivateKey(derBytes)
	if err != nil {
		return "", fmt.Errorf("errore parsing PKCS8: %v", err)
	}

	// 3. Cast alla chiave Ed25519 standard di Go
	edPriv, ok := rawKey.(ed25519.PrivateKey)
	if !ok {
		return "", fmt.Errorf("la chiave non è di tipo Ed25519")
	}

	// 4. Conversione nel formato crypto.PrivKey richiesto da libp2p
	// Libp2p vuole i byte della chiave privata seguiti da quelli della pubblica (64 byte totali)
	priv, err := crypto.UnmarshalEd25519PrivateKey(edPriv)
	if err != nil {
		return "", fmt.Errorf("errore unmarshal per libp2p: %v", err)
	}

	// 5. Generazione del PeerID
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("errore generazione PeerID: %v", err)
	}

	return id.String(), nil
}

// Funzione di utilità per creare un puntatore a int32
func int32Ptr(i int32) *int32 {
	return &i
}
