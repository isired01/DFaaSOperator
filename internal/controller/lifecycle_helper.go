package controller

import (
	"context"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
	"dfaas-operator/internal/controller/monitoring"
)

func (r *EsperimentoReconciler) handleDeletion(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(exp, esperimentoFinalizer) {
		log.Info("🗑️ Finalizer rilevato, avvio procedure di pulizia...")

		// 1. Pulizia Prometheus
		mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
		_ = mm.CleanupTargets(ctx, exp)

		// 3. Rimuoviamo il finalizer
		controllerutil.RemoveFinalizer(exp, esperimentoFinalizer)
		if err := r.Update(ctx, exp); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *EsperimentoReconciler) reconcileInfra(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	return r.updateStatus(ctx, exp, "INSTALLING_DFAAS")
}

func (r *EsperimentoReconciler) reconcileDFAAS(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {

	log := log.FromContext(ctx)

	// 1. Controlliamo se il Job esiste già
	var job batchv1.Job
	jobKey := client.ObjectKey{Name: exp.Name + "-infra-job", Namespace: exp.Namespace}
	err := r.Get(ctx, jobKey, &job)

	// 2. CASO: IL JOB NON ESISTE -> Lo creiamo
	if apierrors.IsNotFound(err) {
		log.Info("🚀 Fase 1: Creazione Job Ansible per Provisioning...")

		am := &ansible.Manager{Client: r.Client, Scheme: r.Scheme}
		if err := am.EnsureConfigMap(ctx, exp); err != nil {
			return ctrl.Result{}, err
		}

		// Creazione Job e Secret
		newJob, secret, err := am.CreateJob(ctx, exp)
		if err != nil {
			return ctrl.Result{}, err
		}

		if err := r.Create(ctx, secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}

		// Aggiorniamo lo stato e RESTITUIAMO subito per fermare il ciclo frenetico
		r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionFalse,
			"ProvisioningStarted", "Job Ansible avviato per la configurazione nodi")

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if err != nil {
		log.Error(err, "Errore nel recupero del Job")
		return ctrl.Result{}, err
	}

	// 3. CASO: IL JOB È FINITO CON SUCCESSO
	if job.Status.Succeeded > 0 {
		log.Info("✅ Ansible ha finito! Passo al monitoring.")

		r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionTrue,
			"ProvisioningSucceeded", "Nodi configurati correttamente")

		// Passiamo alla fase successiva
		return r.updateStatus(ctx, exp, "PROVISIONING_MONITORING")
	}

	if job.Status.Failed > 0 {
		log.Info("❌ Ansible has failed! Check Logs.(kubectl logs -f -l job-name=" + exp.Name + "-infra-job)")
		r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionFalse,
			"ProvisioningFailed", "Il Job Ansible è fallito, controlla i log")

		return r.updateStatus(ctx, exp, "FAILED")
	}

	// 5. CASO: IL JOB STA ANCORA GIRANDO (Running)
	// IMPORTANTE: Qui NON cambiamo lo status, logghiamo e basta.
	// Senza cambi di status, Kubernetes rispetterà il RequeueAfter di 10 secondi.
	log.Info("⏳ Ansible sta ancora lavorando sulle VM...")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *EsperimentoReconciler) reconcileMonitoring(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
	if err := mm.Deploy(ctx); err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	ready, _ := mm.Check(ctx)

	if !ready {
		// 2. Aggiorna la CONDITION
		r.setCondition(ctx, exp, "MonitoringReady", metav1.ConditionFalse,
			"WaitingPods", "I pod non sono pronti")
		// 3. Mantieni la FASE (Posizione nell'automa)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Se è pronto:
	r.setCondition(ctx, exp, "MonitoringReady", metav1.ConditionTrue,
		"PodsRunning", "Monitoraggio UP")

	_ = mm.ReconcileTargets(ctx, exp)
	// Transizione della FASE
	return r.updateStatus(ctx, exp, "READY")
}

// reconcileReady gestisce la fase READY: cerca un TestRun k6 collegato e, se il test
// è in esecuzione (stage=started), transiziona l'esperimento a RUNNING.
func (r *EsperimentoReconciler) reconcileReady(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Cerca TestRun con la label dfaas.io/experiment-name=<exp.Name>
	testRun, found, err := r.findTestRunForExperiment(ctx, exp)
	if err != nil {
		log.Error(err, "Errore nella ricerca del TestRun")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if !found {
		// Nessun TestRun trovato: il test non è stato ancora lanciato dalla UI.
		// Resta in READY e riprova tra 5 secondi.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Leggi il campo status.stage dal TestRun
	stage := getStageFromTestRun(testRun)
	log.Info("TestRun trovato", "name", testRun.GetName(), "stage", stage)

	switch stage {
	case "started":
		// Il test è in esecuzione → passiamo a RUNNING
		log.Info("🏃 TestRun in esecuzione! Transizione a RUNNING.")
		r.setCondition(ctx, exp, "K6TestRunning", metav1.ConditionTrue,
			"TestStarted", "Il TestRun k6 è in esecuzione")
		return r.updateStatus(ctx, exp, "RUNNING")

	case "error":
		// Il test è fallito
		log.Info("❌ TestRun fallito! Transizione a FAILED.")
		r.setCondition(ctx, exp, "K6TestRunning", metav1.ConditionFalse,
			"TestFailed", "Il TestRun k6 ha riscontrato un errore")
		return r.updateStatus(ctx, exp, "FAILED")

	default:
		// Il TestRun esiste ma non è ancora in stage "started"
		// (potrebbe essere in initialization, initialized, created...)
		log.Info("⏳ TestRun in attesa di una configurazione...", "stage", stage)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
}

// reconcileRunning gestisce la fase RUNNING: monitora il TestRun k6 e, quando
// il test termina (stage=finished/stopped), transiziona l'esperimento a COOLDOWN.
func (r *EsperimentoReconciler) reconcileRunning(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	testRun, found, err := r.findTestRunForExperiment(ctx, exp)
	if err != nil {
		log.Error(err, "Errore nella ricerca del TestRun")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if !found {
		// Il TestRun è scomparso inaspettatamente → FAILED
		log.Info("⚠️ TestRun non trovato durante RUNNING. Transizione a FAILED.")
		r.setCondition(ctx, exp, "K6TestRunning", metav1.ConditionFalse,
			"TestRunDisappeared", "Il TestRun k6 è stato eliminato inaspettatamente")
		return r.updateStatus(ctx, exp, "FAILED")
	}

	stage := getStageFromTestRun(testRun)
	log.Info("Monitoraggio TestRun", "name", testRun.GetName(), "stage", stage)

	switch stage {
	case "finished", "stopped":
		// Il test è terminato → passiamo a COOLDOWN
		log.Info("✅ TestRun terminato! Transizione a COOLDOWN.")
		r.setCondition(ctx, exp, "K6TestRunning", metav1.ConditionTrue,
			"TestFinished", "Il TestRun k6 è terminato con successo")
		return r.updateStatus(ctx, exp, "COOLDOWN")

	case "error":
		log.Info("❌ TestRun fallito durante l'esecuzione! Transizione a FAILED.")
		r.setCondition(ctx, exp, "K6TestRunning", metav1.ConditionFalse,
			"TestFailed", "Il TestRun k6 ha riscontrato un errore durante l'esecuzione")
		return r.updateStatus(ctx, exp, "FAILED")

	default:
		// Il test sta ancora girando (started, created, etc.)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
}

func (r *EsperimentoReconciler) reconcileCooldown(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	testRun, found, _ := r.findTestRunForExperiment(ctx, exp)
	termTime := time.Now()
	if found {
		if t, ok := r.getK6PodTerminationTime(ctx, exp.Namespace, testRun.GetName()); ok {
			termTime = t
		}
	}

	// Controlla se sono passati almeno 30 secondi
	if time.Since(termTime) < 30*time.Second {
		log.Info("⏳ Cooldown in corso, attesa di 30 secondi dalla fine del test K6...")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	log.Info("✅ Cooldown di 30s terminato. Passo a EXPORT_METRICHE.")
	return r.updateStatus(ctx, exp, "EXPORT_METRICHE")
}

func (r *EsperimentoReconciler) reconcileExportMetriche(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Controlliamo se il Job esiste già
	var job batchv1.Job
	jobKey := client.ObjectKey{Name: exp.Name + "-exporter-job", Namespace: exp.Namespace}
	err := r.Get(ctx, jobKey, &job)

	if apierrors.IsNotFound(err) {
		log.Info("🚀 Creazione Job Exporter Metriche...")

		// termTime + queries determinazione dal TestRun (con default).
		termTime := time.Now()
		queries := "node_cpu_seconds_total|node_memory_MemTotal_bytes"

		if testRun, found, _ := r.findTestRunForExperiment(ctx, exp); found {
			if t, ok := r.getK6PodTerminationTime(ctx, exp.Namespace, testRun.GetName()); ok {
				termTime = t
			}
			if val, ok := testRun.GetAnnotations()["dfaas.io/metrics-queries"]; ok && val != "" {
				queries = val
			}
		}

		newJob := r.createExporterJob(exp, queries, termTime)
		if err := r.Create(ctx, newJob); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// 2. Controllo stato Job
	if job.Status.Succeeded > 0 {
		log.Info("✅ Export Metriche completato con successo!")
		if exp.Spec.IsCleanupRequested {
			return r.updateStatus(ctx, exp, "CLEANUP")
		}
		return r.updateStatus(ctx, exp, "COMPLETED")
	}

	if job.Status.Failed > 0 {
		log.Info("❌ Export Metriche fallito!")
		return r.updateStatus(ctx, exp, "FAILED")
	}

	// 3. Job ancora in corso
	log.Info("⏳ Exporter in esecuzione...")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *EsperimentoReconciler) reconcileCleanup(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("🧹 Pulizia risorse post-esperimento...")

	// 1. Iniziamo la pulizia (Status: False)
	r.setCondition(ctx, exp, "MonitoringCleanUP", metav1.ConditionFalse,
		"CleanupStarted", "Rimozione target Prometheus e deallocazione risorse...")

	// 2. Esecuzione pulizia Prometheus
	mm := &monitoring.Manager{Client: r.Client, Scheme: r.Scheme}
	if err := mm.CleanupTargets(ctx, exp); err != nil {
		log.Error(err, "⚠️ Errore durante il cleanup dei target")
		r.setCondition(ctx, exp, "MonitoringCleanUP", metav1.ConditionFalse,
			"CleanupFailed", err.Error())
		// Decidiamo di proseguire comunque verso COMPLETED per non bloccare l'oggetto
		// o potresti restare in CLEANUP per riprovare.
	} else {
		// 3. Pulizia completata con successo (Status: True)
		r.setCondition(ctx, exp, "MonitoringCleanUP", metav1.ConditionTrue,
			"CleanupSucceeded", "Tutte le risorse temporanee sono state rimosse")
	}

	log.Info("🏁 Esperimento terminato con successo")
	return r.updateStatus(ctx, exp, "COMPLETED")
}

// setCondition rifetcha l'oggetto dentro un retry loop e applica la condition
// sul latest, così il bump di ResourceVersion fatto da un eventuale
// Status().Update precedente non causa un 409 Conflict.
func (r *EsperimentoReconciler) setCondition(ctx context.Context,
	exp *dfaasv1.Esperimento, condType string, status metav1.ConditionStatus,
	reason, message string) error {

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &dfaasv1.Esperimento{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(exp), latest); err != nil {
			return err
		}
		meta.SetStatusCondition(&latest.Status.Conditions, metav1.Condition{
			Type:               condType,
			Status:             status,
			Reason:             reason,
			Message:            message,
			LastTransitionTime: metav1.Now(),
		})
		return r.Status().Update(ctx, latest)
	})
}
