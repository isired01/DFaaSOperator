/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"strings"
	"text/template"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"                   // Per ConfigMap
	"k8s.io/apimachinery/pkg/api/errors"          // Per errors.IsAlreadyExists
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1" // Per ObjectMeta
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/yaml"
)

//go:embed monitoring/*.yaml
var monitoringConfig embed.FS

// EsperimentoReconciler reconciles a Esperimento object
type EsperimentoReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type PrometheusTarget struct {
	Targets []string          `json:"targets"`
	Labels  map[string]string `json:"labels"`
}

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=esperimentos,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=esperimentos/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=esperimentos/finalizers,verbs=update

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Esperimento object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.3/pkg/reconcile

// reconcilePrometheusTargets gestisce la registrazione dinamica dei target di monitoraggio.
// Implementa il pattern "File-Based Service Discovery" di Prometheus: invece di modificare
// la configurazione globale, l'operatore inietta un file JSON dedicato all'esperimento
// in una ConfigMap condivisa montata nel pod di monitoraggio.
//
// Flusso operativo:
// 1. Definisce i target (IP e Porte) e applica label identificative (id_esperimento)
//    per permettere a Grafana di filtrare i dati di questo specifico test.
// 2. Serializza la struttura in formato JSON compatibile con Prometheus SD.
// 3. Aggiorna la ConfigMap 'prometheus-targets' nel namespace 'monitoring'.
// 4. Sfrutta il sidecar 'config-reloader' per notificare Prometheus del nuovo file
//    senza causare il riavvio del servizio, garantendo continuità nella raccolta metriche.

func (r *EsperimentoReconciler) reconcilePrometheusTargets(ctx context.Context, exp *dfaasv1.Esperimento) error {
	log := log.FromContext(ctx)

	/*
		// 1. Costruiamo la lista dei target dai nodi della federazione
		var targets []PrometheusTarget


		for _, nodo := range exp.Spec.Federazione.Nodi {
			// Nota: Assumiamo che l'IP sia raggiungibile e che il Node Exporter sia sulla porta 9100
			// Se non hai il campo IP esplicito, dovremo ricavarlo o usare l'ID se risolvibile via DNS
			target := PrometheusTarget{
				Targets: []string{nodo.IdNodo + ":30903"}, // O usa l'IP se l'hai aggiunto allo struct
				Labels: map[string]string{					// 30903 è la porta per tutti i prometeus
					"esperimento": exp.Name,				//su tutti i nodi DFaaS
					"nodo_id":     nodo.IdNodo,
					"tipo_nodo":   nodo.TipoNodo,
				},
			}
			targets = append(targets, target)
		}
	*/

	staticIP := "192.168.64.3:30662"
	targets := []PrometheusTarget{
		{
			Targets: []string{staticIP},
			Labels: map[string]string{
				"esperimento": exp.Name,
				"nodo_id":     "nodo-test-statico",
				"tipo_nodo":   "QEMU-VM",
			},
		},
	}

	// 2. Serializziamo in JSON
	jsonData, err := json.Marshal(targets)
	if err != nil {
		return err
	}

	// 3. Recuperiamo la ConfigMap globale dei target
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := r.Get(ctx, cmKey, cm); err != nil {
		return err
	}

	// 4. Inseriamo il file specifico per questo esperimento
	if cm.Data == nil {
		cm.Data = make(map[string]string)
	}

	fileName := exp.Name + ".json"
	cm.Data[fileName] = string(jsonData)

	// 5. Update su Kubernetes
	if err := r.Update(ctx, cm); err != nil {
		log.Error(err, "Impossibile aggiornare la ConfigMap dei target")
		return err
	}

	log.Info("🎯 Target di monitoraggio aggiornati in Prometheus", "file", fileName)
	return nil
}

// cleanupPrometheusTargets rimuove il file di configurazione specifico dell'esperimento
// dalla ConfigMap di Prometheus. Questa operazione interrompe il monitoraggio dei nodi
// associati a questo test, liberando risorse nel database centrale.
func (r *EsperimentoReconciler) cleanupPrometheusTargets(ctx context.Context, exp *dfaasv1.Esperimento) error {
	log := log.FromContext(ctx)

	// 1. Recuperiamo la ConfigMap globale dei target
	cm := &corev1.ConfigMap{}
	cmKey := client.ObjectKey{Name: "prometheus-targets", Namespace: "monitoring"}
	if err := r.Get(ctx, cmKey, cm); err != nil {
		// Se la ConfigMap non esiste, non c'è nulla da pulire
		return client.IgnoreNotFound(err)
	}

	// 2. Verifichiamo se il file dell'esperimento esiste e lo rimuoviamo
	fileName := exp.Name + ".json"
	if _, esiste := cm.Data[fileName]; esiste {
		delete(cm.Data, fileName)

		// 3. Update della ConfigMap per notificare il Sidecar della rimozione
		if err := r.Update(ctx, cm); err != nil {
			log.Error(err, "Errore durante la rimozione del file JSON da Prometheus targets")
			return err
		}
		log.Info("🗑️ Target di monitoraggio rimossi con successo", "file", fileName)
	}

	return nil
}

const esperimentoFinalizer = "dfaas.dfaas.io/finalizer"

func (r *EsperimentoReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Recuperiamo l'oggetto Esperimento dal cluster
	var esperimento dfaasv1.Esperimento

	if err := r.Get(ctx, req.NamespacedName, &esperimento); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// CONTROLLO SE L'OGGETTO È IN FASE DI CANCELLAZIONE
	if esperimento.ObjectMeta.DeletionTimestamp.IsZero() {
		// L'oggetto NON è in fase di cancellazione, quindi aggiungiamo il finalizer
		if !controllerutil.ContainsFinalizer(&esperimento, esperimentoFinalizer) {
			controllerutil.AddFinalizer(&esperimento, esperimentoFinalizer)
			if err := r.Update(ctx, &esperimento); err != nil {
				return ctrl.Result{}, err
			}
			// FONDAMENTALE: Dopo l'Update, devi uscire dal Reconcile.
			// Kubernetes ti richiamerà subito con l'oggetto aggiornato.
			return ctrl.Result{}, nil
		}
	} else {
		// L'oggetto È in fase di cancellazione (lancio k delete)
		if controllerutil.ContainsFinalizer(&esperimento, esperimentoFinalizer) {
			log.Info("🗑️ Risorsa in cancellazione: avvio pulizia...")

			// --- PULIZIA PROMETHEUS ---
			if err := r.cleanupPrometheusTargets(ctx, &esperimento); err != nil {
				log.Error(err, "Impossibile pulire i target di Prometheus")
				return ctrl.Result{}, err
			}

			// --- PULIZIA VM ---
			log.Info("☁️ Pulizia infrastruttura VM in corso...")

			// Rimuoviamo il finalizer
			controllerutil.RemoveFinalizer(&esperimento, esperimentoFinalizer)
			if err := r.Update(ctx, &esperimento); err != nil {
				return ctrl.Result{}, err
			}
			// Anche qui, dopo aver rimosso il finalizer e fatto Update, usciamo.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, nil
	}

	//Monitoraggio dello spegnimento dell'operatore
	go func() {
		<-ctx.Done() // Si sblocca solo quando premi CTRL+C
		// Qui inserisci la logica di emergenza
		log.Info("⚠️ Shutdown rilevato! Avvio cancellazione d'emergenza VM...")

	}()

	// 2. Log di cortesia per vedere che sta funzionando
	//log.Info("🔍 Rilevato Esperimento:", "Fase Attuale", esperimento.Status.Fase)

	// 3. LOGICA DELL'AUTOMA A STATI

	switch esperimento.Status.Fase {
	case "": // Stato iniziale (Appena creato)
		log.Info("➡️ Inizio transizione: IDLE -> PROVISIONING")
		time.Sleep(3 * time.Second)
		return r.handleInitialState(ctx, &esperimento)

	case "PROVISIONING_INFRA":
		log.Info("☁️ Fase 1: Provisioning Infrastruttura (VM/Nodi)...")
		// Qui in futuro chiamerai la logica Terraform o i tuoi script di creazione VM
		// Per ora simuliamo che sia tutto pronto
		log.Info("✅ Infrastruttura verificata.")
		return r.updateStatus(ctx, &esperimento, "PROVISIONING_MONITORING")

	case "PROVISIONING_MONITORING":
		log.Info("📊 Fase 2: Auto-deploy dello stack di monitoraggio...")

		// 1. Applichiamo gli YAML (Idempotente: se ci sono già non fa nulla)
		if err := r.deployMonitoringStack(ctx); err != nil {
			log.Error(err, "❌ Impossibile installare i manifesti di monitoraggio")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// 2. Verifichiamo se i Pod sono effettivamente pronti (Ready)
		log.Info("🔍 Verifica readiness dei Pod (Prometheus & Grafana)...")
		monReady, err := r.checkMonitoringStack(ctx)
		if err != nil {
			log.Error(err, "❌ Errore durante il check del monitoraggio")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		if !monReady {
			log.Info("⏳ Pod non ancora pronti. Re-check tra 10 secondi...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		log.Info("➡️ Monitoraggio attivo: PROVISIONING -> READY")
		return r.updateStatus(ctx, &esperimento, "READY")

	case "READY":

		log.Info("🚀 Preparazione Prometheus in corso...")

		// 1. Iniezione Target in Prometheus (NUOVO)
		log.Info("🎯 Configurazione monitoraggio Prometheus...")
		if err := r.reconcilePrometheusTargets(ctx, &esperimento); err != nil {
			log.Error(err, "Errore nella configurazione dei target Prometheus")
			return r.updateStatus(ctx, &esperimento, "FAILED")
		}

		log.Info("🚀Devo lanciare k6...")

		log.Info("🛠️ Generazione script k6...")
		scriptJS, err := r.generaScriptK6(&esperimento)
		if err != nil {
			log.Error(err, "Errore nella generazione dello script")
			return r.updateStatus(ctx, &esperimento, "FAILED")
		}

		// 1. Crea la ConfigMap
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      esperimento.Name + "-script-k6",
				Namespace: esperimento.Namespace,
			},
			Data: map[string]string{
				"test.js": scriptJS,
			},
		}

		if err := ctrl.SetControllerReference(&esperimento, cm, r.Scheme); err != nil {
			log.Error(err, "Impossibile impostare l'OwnerReference sulla ConfigMap")
			return r.updateStatus(ctx, &esperimento, "FAILED")
		}

		// 1. Tenta la creazione se no aggiorna
		if err := r.Create(ctx, cm); err != nil {
			if errors.IsAlreadyExists(err) {
				// --- LOGICA DI UPDATE ---
				log.Info("🔄 ConfigMap già esistente, avvio aggiornamento contenuto...")

				// Recuperiamo la versione attuale dal cluster
				existingCm := &corev1.ConfigMap{}
				if err := r.Get(ctx, client.ObjectKey{Name: cm.Name, Namespace: cm.Namespace}, existingCm); err != nil {
					return r.updateStatus(ctx, &esperimento, "FAILED")
				}

				// Sovrascriviamo solo i dati (lo script JS)
				existingCm.Data = cm.Data

				// Applichiamo l'update
				if err := r.Update(ctx, existingCm); err != nil {
					log.Error(err, "Errore durante l'Update della ConfigMap")
					return r.updateStatus(ctx, &esperimento, "FAILED")
				}
				log.Info("✅ ConfigMap aggiornata con l'ultimo script generato")
				// -------------------------
			} else {
				log.Error(err, "Errore fatale creazione ConfigMap")
				return r.updateStatus(ctx, &esperimento, "FAILED")
			}
		}

		// 2. LANCIA IL JOB (Chiamata alla funzione che hai scritto in fondo)
		log.Info("🚢 Lancio del Job k6...")
		if err := r.runK6Job(ctx, &esperimento); err != nil {
			if !errors.IsAlreadyExists(err) {
				log.Error(err, "Errore creazione Job k6")
				return r.updateStatus(ctx, &esperimento, "FAILED")
			}
		}

		log.Info("✅ ConfigMap e Job creati con successo!")
		return r.updateStatus(ctx, &esperimento, "RUNNING")

	case "RUNNING":
		log.Info("⏳ Monitoraggio esecuzione k6...")

		// 1. Recuperiamo il Job dal cluster
		var job batchv1.Job
		jobKey := client.ObjectKey{Name: esperimento.Name + "-k6-job", Namespace: esperimento.Namespace}

		if err := r.Get(ctx, jobKey, &job); err != nil {
			log.Error(err, "Impossibile recuperare lo stato del Job k6")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

		// 2. Controlliamo le condizioni del Job
		if job.Status.Succeeded > 0 {
			log.Info("✅ k6 ha terminato con successo! Passaggio a COOLDOWN.")
			return r.updateStatus(ctx, &esperimento, "COOLDOWN")
		}

		if job.Status.Failed > 0 {
			log.Error(nil, "❌ Il Job k6 è fallito!")
			return r.updateStatus(ctx, &esperimento, "FAILED")
		}

		// 3. Se è ancora in esecuzione, non fare nulla e ricontrolla tra 10 secondi
		log.Info("... k6 sta ancora generando carico ...")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil

	case "COOLDOWN":
		log.Info("⏳ Aspetto che le metriche finiscano di arrivare")
		log.Info("COOLDOWN -> COMPLETED")
		time.Sleep(30 * time.Second)
		return r.updateStatus(ctx, &esperimento, "COMPLETED")

	case "COMPLETED":
		log.Info("Esperimento completato")
		time.Sleep(3 * time.Second) // apro grafana e mostro i risultati
		return r.updateStatus(ctx, &esperimento, "ENDED")

	case "CLEANUP":
		log.Info("🧹 Pulizia risorse...")
		time.Sleep(3 * time.Second)
		//canellare prometesu
		return r.updateStatus(ctx, &esperimento, "ENDED")

	}

	return ctrl.Result{}, nil
}

// Funzione per aggiornare la fase dell'esperimento
func (r *EsperimentoReconciler) updateStatus(ctx context.Context, exp *dfaasv1.Esperimento, fase string) (ctrl.Result, error) {
	// 1. Rileggiamo l'oggetto fresco dal cluster per evitare conflitti di versione
	latestExp := &dfaasv1.Esperimento{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(exp), latestExp); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Aggiorniamo la fase sulla versione appena scaricata
	latestExp.Status.Fase = fase
	if err := r.Status().Update(ctx, latestExp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

// Gestione dello stato iniziale
func (r *EsperimentoReconciler) handleInitialState(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Controllo che non vi siano VM vecchie passo alla fase di PROVISIONING")
	//se ci sono robe da pulire va i cleanUP e chiamo il metodo per pulire
	return r.updateStatus(ctx, exp, "PROVISIONING_INFRA")
}

// SetupWithManager sets up the controller with the Manager.
func (r *EsperimentoReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfaasv1.Esperimento{}).
		Complete(r)
}

const k6Template = `
import http from 'k6/http';
import { sleep } from 'k6';
import { scenario } from 'k6/execution';

{{- $profilo := .Spec.Profilo }}

export const options = {
	tags: {
    esperimento: '{{ .Name }}',
  },
  scenarios: {
    {{- range .Spec.Profilo.Scenari }}
    "{{ .NomeScenario }}": {
      executor: 'ramping-arrival-rate',
      startRate: {{ .StartTime }},
      timeUnit: '1s',
      preAllocatedVUs: {{ .VuAllocati }},
      maxVUs: {{ .VuAllocati }},
      stages: [
        {{- range .Stages }}
        { duration: '{{ .Durata }}', target: {{ .TargetRps }} },
        {{- end }}
      ],
      // Passiamo i dati specifici dello scenario come variabili d'ambiente interne
      env: { 
        TARGET_URL: 'http://{{ .TargetNodeID }}/function/{{ .NomeFunzioneTarget }}',
        BODY_CONTENT: '{{ .Body }}'
      },
    },
    {{- end }}
  },
};

export default function () {
  // Ogni scenario legge le PROPRIE variabili d'ambiente definite sopra
  const url = __ENV.TARGET_URL;
  const payload = __ENV.BODY_CONTENT;
  
  const params = {
    headers: {
      {{ $profilo.CommonHeaders }} 
    },
  };

  http.post(url, payload, params);
}
`

func (r *EsperimentoReconciler) generaScriptK6(exp *dfaasv1.Esperimento) (string, error) {
	// Crea il template
	tmpl, err := template.New("k6").Parse(k6Template)
	if err != nil {
		return "", err
	}

	// Esegue il template usando l'oggetto 'exp' come sorgente dati
	var script bytes.Buffer
	if err := tmpl.Execute(&script, exp); err != nil {
		return "", err
	}

	return script.String(), nil
}

func (r *EsperimentoReconciler) runK6Job(ctx context.Context, exp *dfaasv1.Esperimento) error {
	terminate := int64(3000)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      exp.Name + "-k6-job",
			Namespace: exp.Namespace,
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds: &terminate,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "k6",
							Image: "grafana/k6:1.4.2",
							Args:  []string{"run", "/test/test.js", "--out", "experimental-prometheus-rw"},
							Env: []corev1.EnvVar{
								{
									// URL dell'endpoint write di Prometheus (usando il DNS interno di K8s)
									Name:  "K6_PROMETHEUS_RW_SERVER_URL",
									Value: "http://prometheus-service.monitoring.svc.cluster.local:9090/api/v1/write",
								},
								{
									// Specifichiamo quali statistiche vogliamo
									Name:  "K6_PROMETHEUS_RW_TREND_STATS",
									Value: "p(95),p(99),avg,max",
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "script-volume",
									MountPath: "/test",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "script-volume",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: exp.Name + "-script-k6", // Deve combaciare con la CM creata
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Imposta l'Esperimento come "proprietario" del Job (se cancelli l'esperimento, sparisce il job)
	if err := ctrl.SetControllerReference(exp, job, r.Scheme); err != nil {
		return err
	}

	return r.Create(ctx, job)
}

func (r *EsperimentoReconciler) deployMonitoringStack(ctx context.Context) error {
	log := log.FromContext(ctx)

	// Legge i file dalla cartella monitoring (quella con l'embed)
	entries, err := monitoringConfig.ReadDir("monitoring")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		fileData, err := monitoringConfig.ReadFile("monitoring/" + entry.Name())
		if err != nil {
			return err
		}

		// Decoder per gestire file multi-oggetto (separati da ---)
		decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(fileData), 4096)
		for {
			unstructuredObj := &unstructured.Unstructured{}
			if err := decoder.Decode(unstructuredObj); err != nil {
				break // Fine file
			}

			if unstructuredObj.Object == nil {
				continue
			}

			unstructuredObj.SetNamespace("monitoring")

			// Tenta la creazione. Se esiste già, passa oltre (Idempotenza)
			err = r.Create(ctx, unstructuredObj)
			if err != nil {
				if errors.IsAlreadyExists(err) {
					continue
				}
				if errors.IsInvalid(err) {
					log.Info("⚠️ Risorsa già configurata o porta occupata, salto...", "file", entry.Name())
					continue
				}
				return err
			}
			log.Info("✅ Creato componente monitoraggio:", "kind", unstructuredObj.GetKind(), "name", unstructuredObj.GetName())
		}
	}
	return nil
}

func (r *EsperimentoReconciler) checkMonitoringStack(ctx context.Context) (bool, error) {
	podList := &corev1.PodList{}
	// Prendiamo TUTTI i pod nel namespace monitoring
	opts := []client.ListOption{
		client.InNamespace("monitoring"),
	}

	if err := r.List(ctx, podList, opts...); err != nil {
		return false, err
	}

	promReady := false
	grafanaReady := false

	for _, pod := range podList.Items {
		// Controlliamo se il Pod è in fase Running e se è "Ready"
		isReady := false
		if pod.Status.Phase == corev1.PodRunning {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					isReady = true
					break
				}
			}
		}

		if isReady {
			// Usiamo una ricerca sul nome del Pod (più affidabile se le label cambiano)
			if strings.Contains(pod.Name, "prometheus") {
				promReady = true
			}
			if strings.Contains(pod.Name, "grafana") {
				grafanaReady = true
			}
		}
	}

	return promReady && grafanaReady, nil
}
