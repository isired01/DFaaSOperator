package controller

import (
	"context"
	"encoding/json"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"

	dfaasv1 "dfaas-operator/api/v1"
)

func (r *EsperimentoReconciler) createAnsibleJob(exp *dfaasv1.Esperimento) (*batchv1.Job, *corev1.Secret) {
	// 1. Costruiamo l'Inventory di Ansible
	var inventory string
	inventory += "[target_nodes]\n"

	// Usiamo una mappa per raccogliere configurazioni uniche delle funzioni
	// La chiave è il Nome della funzione per evitare duplicati nello stesso esperimento
	uniqueFunctions := make(map[string]dfaasv1.FunzioneConfig)

	for _, node := range exp.Spec.Federazione.Nodi {
		if node.IndirizzoIP != "" {
			// Aggiungiamo il nodo all'inventory
			line := fmt.Sprintf("%s ansible_user=%s ansible_password=%s\n",
				node.IndirizzoIP, node.UserName, node.Password)
			inventory += line

			// Raccogliamo le configurazioni complete delle funzioni associate a questo nodo
			for _, f := range node.Funzioni {
				uniqueFunctions[f.Nome] = f
			}
		}
	}

	// Convertiamo la mappa in una slice di oggetti FunzioneConfig
	var functionConfigs []dfaasv1.FunzioneConfig
	for _, config := range uniqueFunctions {
		functionConfigs = append(functionConfigs, config)
	}

	// Trasformiamo l'intera lista di oggetti in JSON
	// Questo permetterà ad Ansible di accedere a .immagine, .execTimeout, ecc.
	functionsJson, err := json.Marshal(functionConfigs)
	if err != nil {
		// In un caso reale qui dovresti gestire l'errore o loggarlo
		functionsJson = []byte("[]")
	}

	// 2. Creiamo il Secret per l'Inventory (hosts)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-ansible-inventory",
			Namespace: exp.Namespace,
		},
		StringData: map[string]string{
			"hosts": inventory,
		},
	}

	// 3. Definiamo il Job Kubernetes che eseguirà Ansible
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-infra-job",
			Namespace: exp.Namespace,
		},
		Spec: batchv1.JobSpec{
			// Tentativi massimi in caso di fallimento del Pod
			BackoffLimit: ptrInt32(2),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "ansible-worker",
							Image:   "willhallonline/ansible:latest",
							Command: []string{"ansible-playbook"},
							Args: []string{
								"-i", "/etc/ansible/hosts",
								"/ansible/playbooks/setup-node.yml",
								// Passiamo l'oggetto JSON completo come extra-vars
								"--extra-vars", fmt.Sprintf("requested_functions=%s", string(functionsJson)),
								"--extra-vars", "ansible_ssh_common_args='-o StrictHostKeyChecking=no'",
							},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "inventory-volume", MountPath: "/etc/ansible"},
								{Name: "playbook-volume", MountPath: "/ansible/playbooks"},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "inventory-volume",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: secret.Name,
								},
							},
						},
						{
							Name: "playbook-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "ansible-playbooks",
									},
								},
							},
						},
					},
					RestartPolicy: corev1.RestartPolicyOnFailure,
				},
			},
		},
	}

	// Impostiamo l'OwnerReference per la pulizia automatica (Garbage Collection)
	_ = ctrl.SetControllerReference(exp, secret, r.Scheme)
	_ = ctrl.SetControllerReference(exp, job, r.Scheme)

	return job, secret
}

func ptrInt32(i int32) *int32 { return &i }

func (r *EsperimentoReconciler) ensureAnsibleConfigMap(ctx context.Context, exp *dfaasv1.Esperimento) error {
	cm := &corev1.ConfigMap{}
	// Usiamo un nome fisso o derivato, l'importante è che coincida con quello nel Job
	cmName := "ansible-playbooks"

	err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: exp.Namespace}, cm)
	if err != nil && apierrors.IsNotFound(err) {
		// La ConfigMap non esiste, la creiamo
		newCm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      cmName,
				Namespace: exp.Namespace,
			},
			Data: map[string]string{
				"setup-node.yml": AnsiblePlaybookYaml,
			},
		}

		// Impostiamo l'OwnerReference così se cancelli l'esperimento,
		// Kubernetes può pulire (opzionale, dipende se vuoi riutilizzarla)
		if err := ctrl.SetControllerReference(exp, newCm, r.Scheme); err != nil {
			return err
		}

		return r.Create(ctx, newCm)
	}

	return err
}
