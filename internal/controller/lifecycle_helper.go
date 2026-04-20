package controller

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

func (r *EsperimentoReconciler) handleDeletion(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if controllerutil.ContainsFinalizer(exp, esperimentoFinalizer) {
		log.Info("🗑️ Finalizer rilevato, avvio procedure di pulizia...")

		// 1. Pulizia Prometheus
		r.cleanupPrometheusTargets(ctx, exp)

		// 2. CANCELLAZIONE ESPLICITA DEL JOB
		// Questo risolve l'errore "already exists" se riapplichi velocemente
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      exp.Name + "-k6-job",
				Namespace: exp.Namespace,
			},
		}
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			log.Info("Job k6 non trovato o già cancellato")
		}

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
	log := log.FromContext(ctx)
	log.Info("☁️ Fase 1: Provisioning Infrastruttura (VM/Nodi)...")

	// 1. Segnaliamo l'inizio del provisioning (Status: False, Reason: ProvisioningStarted)
	r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionFalse,
		"ProvisioningStarted", "Configurazione nodi DFaaS in corso...")

	// --- Logica Futura (Terraform / Script SSH) ---
	// Qui simuleremo il successo immediato per ora.
	// ----------------------------------------------

	// 2. Provisioning Completato (Status: True)
	log.Info("✅ Infrastruttura verificata.")
	r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionTrue,
		"ProvisioningSucceeded", "Tutti i nodi DFaaS sono raggiungibili")

	return r.updateStatus(ctx, exp, "PROVISIONING_MONITORING")
}

func (r *EsperimentoReconciler) reconcileMonitoring(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	if err := r.deployMonitoringStack(ctx); err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	ready, _ := r.checkMonitoringStack(ctx)

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
	// Transizione della FASE
	return r.updateStatus(ctx, exp, "READY")
}


func (r *EsperimentoReconciler) reconcileCooldown(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 2. Calcoliamo quanto tempo è passato da EndTime
	passato := time.Since(exp.Status.EndTime.Time)
	attesa := 30 * time.Second

	if passato < attesa {
		rimanente := attesa - passato
		log.Info("⏳ Cooldown in corso (basato su EndTime)...",
			"passati", passato.Seconds(),
			"rimanenti", rimanente.Seconds())

		// Se non aggiorniamo lo Status, Kubernetes NON scatena il Reconcile immediato
		// e rispetterà finalmente il RequeueAfter.
		return ctrl.Result{RequeueAfter: rimanente}, nil
	}

	// 3. Se sono passati i 30 secondi, cambiamo fase
	log.Info("✅ Cooldown di 30s terminato. Passo a EXPORT_METRICHE.")
	return r.updateStatus(ctx, exp, "EXPORT_METRICHE")
}
func (r *EsperimentoReconciler) reconcileExportMetrics(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. TENTATIVO DI RECUPERO DEL JOB
	var job batchv1.Job
	// NOTA: Assicurati che il namespace qui sia lo STESSO usato in runExporterJob
	err := r.Get(ctx, client.ObjectKey{Name: exp.Name + "-exporter-job", Namespace: exp.Namespace}, &job)

	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("🚀 Job non trovato, lo sto creando...")
			if errLaunch := r.runExporterJob(ctx, exp); errLaunch != nil {
				// --- FIX QUI: Se il job è stato creato da un'altra reconcile un istante fa, non dare errore ---
				if errors.IsAlreadyExists(errLaunch) {
					log.Info("🏃 Job creato da un'altra istanza proprio ora, attendo il prossimo giro...")
					return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
				}

				log.Error(errLaunch, "❌ Impossibile lanciare il Job di Export")
				r.setCondition(ctx, exp, "MetricsPersistedOnCSV", metav1.ConditionFalse,
					"LaunchFailed", fmt.Sprintf("Errore creazione Job: %v", errLaunch))
				return ctrl.Result{}, errLaunch
			}
			// Job creato con successo, diamogli tempo di apparire in etcd
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		// Altri errori di comunicazione con l'API Server
		return ctrl.Result{}, err
	}

	// 2. ANALISI DELLO STATO DEL JOB (Idempotenza)

	// Caso A: Fallimento
	if job.Status.Failed > 0 {
		log.Error(nil, "❌ Il Job di Export è fallito")
		r.setCondition(ctx, exp, "MetricsPersistedOnCSV", metav1.ConditionFalse,
			"JobFailed", "Il container di export ha terminato con un errore")
		return r.updateStatus(ctx, exp, "FAIL")
	}

	// Caso B: Successo
	if job.Status.Succeeded > 0 {
		log.Info("✅ Job di Export completato con successo")
		r.setCondition(ctx, exp, "MetricsPersistedOnCSV", metav1.ConditionTrue,
			"ExportCompleted", "Il file CSV è stato generato correttamente")

		// Proseguiamo verso la pulizia o i risultati
		return r.updateStatus(ctx, exp, "CLEANUP")
	}

	// Caso C: In esecuzione
	log.Info("⏳ Job di Export ancora in corso...", "Succeeded", job.Status.Succeeded, "Active", job.Status.Active)
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *EsperimentoReconciler) reconcileCleanup(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("🧹 Pulizia risorse post-esperimento...")

	// 1. Iniziamo la pulizia (Status: False)
	r.setCondition(ctx, exp, "MonitoringCleanUP", metav1.ConditionFalse,
		"CleanupStarted", "Rimozione target Prometheus e deallocazione risorse...")

	// 2. Esecuzione pulizia Prometheus
	if err := r.cleanupPrometheusTargets(ctx, exp); err != nil {
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

func (r *EsperimentoReconciler) setCondition(ctx context.Context,
	exp *dfaasv1.Esperimento, condType string, status metav1.ConditionStatus,
	reason, message string) error {
	meta.SetStatusCondition(&exp.Status.Conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	})
	return r.Status().Update(ctx, exp)
}
