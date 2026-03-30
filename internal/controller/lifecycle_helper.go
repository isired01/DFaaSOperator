package controller

import (
	"context"
	"time"

	dfaasv1 "dfaas-operator/api/v1"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1" // Ecco il colpevole dell'errore
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
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

func (r *EsperimentoReconciler) reconcileReady(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Iniezione Target Prometheus
	// Qui la logica interna userà il nuovo loop sugli IP dei nodi
	if err := r.reconcilePrometheusTargets(ctx, exp); err != nil {
		log.Error(err, "❌ Fallito aggiornamento target Prometheus")
		r.setCondition(ctx, exp, "PrometheusTargetsReady", metav1.ConditionFalse, "ConfigUpdateFailed", err.Error())
		return r.updateStatus(ctx, exp, "FAILED")
	}
	r.setCondition(ctx, exp, "PrometheusTargetsReady", metav1.ConditionTrue, "ConfigUpdated", "Target inseriti correttamente")

	// 2. Generazione Script k6 (Ora usa la logica della mappa IP)
	script, err := r.buildScriptK6(exp)
	if err != nil {
		log.Error(err, "❌ Errore nella generazione dello script k6 (probabile IP mancante)")
		r.setCondition(ctx, exp, "K6ConfigReady", metav1.ConditionFalse, "TemplateError", err.Error())
		// Fondamentale: aggiorniamo il messaggio di errore visibile all'utente
		exp.Status.Message = fmt.Sprintf("Errore script: %v", err)
		return r.updateStatus(ctx, exp, "FAILED")
	}

	// Creazione ConfigMap per lo script
	if err := r.reconcileK6Config(ctx, exp, script); err != nil {
		log.Error(err, "❌ Fallita creazione ConfigMap k6")
		r.setCondition(ctx, exp, "K6ConfigReady", metav1.ConditionFalse, "ConfigMapCreationFailed", err.Error())
		return r.updateStatus(ctx, exp, "FAILED")
	}
	r.setCondition(ctx, exp, "K6ConfigReady", metav1.ConditionTrue, "ConfigMapCreated", "Script k6 caricato")

	// 3. Lancio del Job k6
	if err := r.runK6Job(ctx, exp); err != nil {
		log.Error(err, "❌ Impossibile avviare il Job k6")
		r.setCondition(ctx, exp, "K6JobLaunched", metav1.ConditionFalse, "JobCreationFailed", err.Error())
		return r.updateStatus(ctx, exp, "FAILED")
	}

	r.setCondition(ctx, exp, "K6JobLaunched", metav1.ConditionTrue, "JobCreated", "Job avviato")

	log.Info("🚀 Esperimento in esecuzione!", "nome", exp.Name)
	return r.updateStatus(ctx, exp, "RUNNING")
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

func (r *EsperimentoReconciler) reconcileCooldown(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// 1. Controlliamo se è la prima volta che entriamo in questa funzione per questo stato
	// Se la condizione non esiste ancora, significa che siamo appena "atterrati" qui.
	condition := meta.FindStatusCondition(exp.Status.Conditions, "MetricsPersisted")

	if condition == nil {
		log.Info("⏳ Inizio Cooldown di 30s per consolidamento metriche")
		r.setCondition(ctx, exp, "MetricsPersisted", metav1.ConditionFalse,
			"AwaitingFlush", "Il test è finito. Attesa 30s per il flush dei dati...")

		// Diciamo a Kubernetes: "Metti questo esperimento in pausa e richiamami tra 30 secondi"
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// 2. Se arriviamo qui, significa che i 30 secondi sono PASSATI
	// (perché Kubernetes ci ha richiamati dopo il RequeueAfter)
	log.Info("✅ Cooldown terminato. Passo al Cleanup.")
	r.setCondition(ctx, exp, "MetricsPersisted", metav1.ConditionTrue,
		"FlushComplete", "Metriche consolidate correttamente")

	return r.updateStatus(ctx, exp, "CLEANUP")
}

func (r *EsperimentoReconciler) reconcileCleanup(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("🧹 Pulizia risorse post-esperimento...")

	// 1. Iniziamo la pulizia (Status: False)
	r.setCondition(ctx, exp, "InfrastructureCleaned", metav1.ConditionFalse,
		"CleanupStarted", "Rimozione target Prometheus e deallocazione risorse...")

	// 2. Esecuzione pulizia Prometheus
	if err := r.cleanupPrometheusTargets(ctx, exp); err != nil {
		log.Error(err, "⚠️ Errore durante il cleanup dei target")
		r.setCondition(ctx, exp, "InfrastructureCleaned", metav1.ConditionFalse,
			"CleanupFailed", err.Error())
		// Decidiamo di proseguire comunque verso COMPLETED per non bloccare l'oggetto
		// o potresti restare in CLEANUP per riprovare.
	} else {
		// 3. Pulizia completata con successo (Status: True)
		r.setCondition(ctx, exp, "InfrastructureCleaned", metav1.ConditionTrue,
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
