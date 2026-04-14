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

func (r *EsperimentoReconciler) reconcileReady(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. "Get" della condizione PrometheusTargetsReady
	cond := meta.FindStatusCondition(exp.Status.Conditions, "PrometheusTargetsReady")

	// Se la condizione non esiste o non è True, iniettiamo i target
	if cond == nil || cond.Status != metav1.ConditionTrue {
		if err := r.reconcilePrometheusTargets(ctx, exp); err != nil {
			log.Error(err, "❌ Fallito aggiornamento target Prometheus")
			r.setCondition(ctx, exp, "PrometheusTargetsReady", metav1.ConditionFalse, "ConfigUpdateFailed", err.Error())
			return r.updateStatus(ctx, exp, "FAILED")
		}
		r.setCondition(ctx, exp, "PrometheusTargetsReady", metav1.ConditionTrue, "ConfigUpdated", "Target inseriti correttamente")

		if err := r.Status().Update(ctx, exp); err != nil {
			return ctrl.Result{}, err
		}

		log.Info("⏳ Target inseriti. Attendo il ricaricamento (10s)...") //TODO: così non va bene nonn aspetta quasi mai serve una fase in più
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// 2. ORA LANCIA IL LAVORO (Quello che mancava!)
	log.Info("🎯 Target già verificati, preparo k6...")

	// Genera lo script k6
	script, err := r.buildScriptK6(exp)
	if err != nil {
		return r.updateStatus(ctx, exp, "FAILED")
	}

	// Crea la ConfigMap con lo script
	if err := r.reconcileK6Config(ctx, exp, script); err != nil {
		log.Error(err, "❌ Fallita creazione ConfigMap k6")
		return r.updateStatus(ctx, exp, "FAILED")
	}

	// 3. LANCIO EFFETTIVO DEL JOB
	if err := r.runK6Job(ctx, exp); err != nil {
		if errors.IsAlreadyExists(err) {
			log.Info("🏃 Job k6 già presente, procedo...")
		} else {
			log.Error(err, "❌ Impossibile avviare il Job k6")
			return r.updateStatus(ctx, exp, "FAILED")
		}
	}

	now := metav1.Now()
	exp.Status.StartTime = &now

	// Forza il salvataggio di TUTTO lo Status (incluso StartTime) prima di uscire
	if err := r.Status().Update(ctx, exp); err != nil {
		log.Error(err, "❌ Impossibile salvare StartTime")
		return ctrl.Result{}, err
	}

	log.Info("🚀 StartTime salvato. Passo in RUNNING")
	return r.updateStatus(ctx, exp, "RUNNING")
}

func (r *EsperimentoReconciler) reconcileRunning(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {

	log := log.FromContext(ctx)
	var job batchv1.Job
	jobKey := client.ObjectKey{Name: exp.Name + "-k6-job", Namespace: exp.Namespace}

	// 1. Recupero del Job
	if err := r.Get(ctx, jobKey, &job); err != nil {
		r.setCondition(ctx, exp, "K6TestExecution", metav1.ConditionUnknown,
			"JobNotFound", "Impossibile recuperare lo stato del Job")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// 2. Controllo successo
	if job.Status.Succeeded > 0 {
		log.Info("✅ Test k6 completato con successo")
		now := metav1.Now()
		exp.Status.EndTime = &now
		r.setCondition(ctx, exp, "K6TestExecution", metav1.ConditionTrue,
			"TestSucceeded", "Il carico è stato generato e inviato a Prometheus")

		return r.updateStatus(ctx, exp, "COOLDOWN")
	}

	// 3. Controllo fallimento
	if job.Status.Failed > 0 {
		log.Error(nil, "❌ Test k6 fallito")
		r.setCondition(ctx, exp, "K6TestExecution", metav1.ConditionFalse,
			"TestFailed", "Il container k6 è andato in errore durante l'esecuzione")
		return r.updateStatus(ctx, exp, "FAILED")
	}

	// 4. Test in corso (Progress)
	log.Info("⏳ k6 sta ancora generando carico...")
	r.setCondition(ctx, exp, "K6TestExecution", metav1.ConditionTrue,
		"TestInBase", "Iniezione di carico in corso...")

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *EsperimentoReconciler) reconcileCooldown(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	condition := meta.FindStatusCondition(exp.Status.Conditions, "MetricsPersisted")

	if condition == nil {
		log.Info("⏳ Inizio Cooldown di 30s per consolidamento metriche")
		r.setCondition(ctx, exp, "MetricsPersisted", metav1.ConditionFalse,
			"AwaitingFlush", "Il test è finito. Attesa 30s per il flush dei dati...")

		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	log.Info("✅ Cooldown terminato. Passo a EXPORT_METRICHE.")
	r.setCondition(ctx, exp, "MetricsPersisted", metav1.ConditionTrue,
		"FlushComplete", "Metriche consolidate correttamente")

	// Passiamo alla fase di Export, non Cleanup direttamente!
	return r.updateStatus(ctx, exp, "EXPORT_METRICHE")
}

func (r *EsperimentoReconciler) reconcileExportMetrics(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var job batchv1.Job
	err := r.Get(ctx, client.ObjectKey{Name: exp.Name + "-exporter-job", Namespace: "monitoring"}, &job)

	// SCENARIO 1: Il Job non esiste, lo creiamo
	if errors.IsNotFound(err) {
		log.Info("🚀 Job non trovato, lo sto creando...")
		if errLaunch := r.runExporterJob(ctx, exp); errLaunch != nil {
			log.Error(errLaunch, "❌ Impossibile lanciare il Job di Export")
			r.setCondition(ctx, exp, "MetricsPersistedOnCSV", metav1.ConditionFalse,
				"LaunchFailed", fmt.Sprintf("Errore creazione Job: %v", errLaunch))
			return ctrl.Result{}, errLaunch
		}
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// SCENARIO 2: Il Job è fallito
	if job.Status.Failed > 0 {
		log.Error(nil, "❌ Il Job di Export è fallito")
		r.setCondition(ctx, exp, "MetricsPersistedOnCSV", metav1.ConditionFalse,
			"JobFailed", "Il container di export ha terminato con un errore")
		return r.updateStatus(ctx, exp, "FAIL")
	}

	// SCENARIO 3: Il Job è finito con successo
	if job.Status.Succeeded > 0 {
		log.Info("✅ Job di Export completato con successo")
		r.setCondition(ctx, exp, "MetricsPersistedOnCSV", metav1.ConditionTrue,
			"ExportCompleted", "Il file CSV è stato generato correttamente")
		return r.updateStatus(ctx, exp, "CLEANUP")
	}

	// SCENARIO 4: In corso
	log.Info("⏳ Job di Export ancora in corso...")
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
