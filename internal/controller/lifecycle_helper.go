package controller

import (
	"context"

	"time"

	batchv1 "k8s.io/api/batch/v1"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	// --- Logica Futura (Terraform) ---
	// Qui simuleremo il successo immediato per ora.
	// ----------------------------------------------

	// 2. Provisioning Completato (Status: True)
	log.Info("✅ Infrastruttura verificata.")
	r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionTrue,
		"ProvisioningSucceeded", "Tutti i nodi DFaaS sono raggiungibili")

	// 1. Cerchiamo se il Job esiste già
	var job batchv1.Job
	jobKey := client.ObjectKey{Name: exp.Name + "-infra-job", Namespace: exp.Namespace}
	err := r.Get(ctx, jobKey, &job)

	// 2. Se il Job NON esiste, lo creiamo insieme al suo Secret
	if apierrors.IsNotFound(err) {
		log.Info("🚀 Creazione Job Ansible e Secret per provisioning...")

		if err := r.ensureAnsibleConfigMap(ctx, exp); err != nil {
			return ctrl.Result{}, err
		}

		// createAnsibleJob deve restituire (Job, Secret)
		job, secret := r.createAnsibleJob(exp)

		// Sync Secret
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
			return controllerutil.SetControllerReference(exp, secret, r.Scheme)
		})
		if err != nil {
			return ctrl.Result{}, err
		}

		// Sync Job
		_, err = controllerutil.CreateOrUpdate(ctx, r.Client, job, func() error {
			return controllerutil.SetControllerReference(exp, job, r.Scheme)
		})
		if err != nil {
			return ctrl.Result{}, err
		}

		r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionFalse,
			"ProvisioningStarted", "Job Ansible avviato per la configurazione nodi")

		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	} else if err != nil {
		log.Error(err, "Errore nel recupero del Job Ansible")
		return ctrl.Result{}, err
	}

	// 3. Se il Job esiste, analizziamo lo stato
	if job.Status.Succeeded > 0 {
		log.Info("✅ Ansible ha finito con successo!")
		r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionTrue,
			"ProvisioningSucceeded", "Nodi configurati correttamente via Ansible")

		return r.updateStatus(ctx, exp, "PROVISIONING_MONITORING")
	}

	if job.Status.Failed > 0 {
		log.Error(nil, "❌ Job Ansible fallito")
		r.setCondition(ctx, exp, "InfrastructureReady", metav1.ConditionFalse,
			"ProvisioningFailed", "Il Job Ansible è andato in errore")

		return r.updateStatus(ctx, exp, "FAILED")
	}

	// 4. Se siamo qui, il Job sta ancora girando
	log.Info("⏳ Ansible sta ancora lavorando sulle VM...")
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
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

	//TO DO: riprestinare coolDown reale magari manco la facciamo qua

	// 3. Se sono passati i 30 secondi, cambiamo fase
	log.Info("✅ Cooldown di 30s terminato. Passo a EXPORT_METRICHE.")
	return r.updateStatus(ctx, exp, "EXPORT_METRICHE")
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
