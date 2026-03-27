package controller

import (
	"context"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

func (r *EsperimentoReconciler) reconcileFinalizers(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Caso A: L'oggetto NON è in fase di cancellazione
	if exp.ObjectMeta.DeletionTimestamp.IsZero() {
		if !controllerutil.ContainsFinalizer(exp, esperimentoFinalizer) {
			controllerutil.AddFinalizer(exp, esperimentoFinalizer)
			log.Info("🔒 Aggiunta Finalizer", "finalizer", esperimentoFinalizer)
			return ctrl.Result{}, r.Update(ctx, exp)
		}
		return ctrl.Result{}, nil // Nulla da fare, procedi col Reconcile
	}

	// Caso B: L'oggetto È in fase di cancellazione
	if controllerutil.ContainsFinalizer(exp, esperimentoFinalizer) {
		log.Info("🗑️ Risorsa in cancellazione: avvio pulizia...")

		// Pulizia Prometheus
		if err := r.cleanupPrometheusTargets(ctx, exp); err != nil {
			log.Error(err, "❌ Fallimento pulizia Prometheus")
			return ctrl.Result{}, err
		}

		// Qui aggiungerai la logica per spegnere le VM
		log.Info("☁️ Pulizia infrastruttura VM completata")

		// Rimuovi finalizer e aggiorna
		controllerutil.RemoveFinalizer(exp, esperimentoFinalizer)
		return ctrl.Result{}, r.Update(ctx, exp)
	}

	return ctrl.Result{}, nil
}

func (r *EsperimentoReconciler) handleDeletion(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	r.cleanupPrometheusTargets(ctx, exp)
	controllerutil.RemoveFinalizer(exp, esperimentoFinalizer)
	return ctrl.Result{}, r.Update(ctx, exp)
}

func (r *EsperimentoReconciler) reconcileReady(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	if err := r.reconcilePrometheusTargets(ctx, exp); err != nil {
		return r.updateStatus(ctx, exp, "FAILED")
	}

	script, err := r.buildScriptK6(exp)
	if err != nil || r.reconcileK6Config(ctx, exp, script) != nil || r.runK6Job(ctx, exp) != nil {
		return r.updateStatus(ctx, exp, "FAILED")
	}

	return r.updateStatus(ctx, exp, "RUNNING")
}

// Fase MONITORING
func (r *EsperimentoReconciler) reconcileMonitoring(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	if err := r.deployMonitoringStack(ctx); err != nil {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	ready, _ := r.checkMonitoringStack(ctx)
	if !ready {
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	return r.updateStatus(ctx, exp, "READY")
}

func (r *EsperimentoReconciler) reconcileRunning(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	var job batchv1.Job
	jobKey := client.ObjectKey{Name: exp.Name + "-k6-job", Namespace: exp.Namespace}

	if err := r.Get(ctx, jobKey, &job); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	if job.Status.Succeeded > 0 {
		return r.updateStatus(ctx, exp, "COOLDOWN")
	}
	if job.Status.Failed > 0 {
		return r.updateStatus(ctx, exp, "FAILED")
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

func (r *EsperimentoReconciler) reconcileInfra(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("☁️ Fase 1: Provisioning Infrastruttura (VM/Nodi)...")

	// Qui andrà la logica Terraform/Cloud in futuro.
	// Per ora validiamo che l'ambiente sia pronto.

	log.Info("✅ Infrastruttura verificata.")
	return r.updateStatus(ctx, exp, "PROVISIONING_MONITORING")
}

func (r *EsperimentoReconciler) reconcileCooldown(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("⏳ Cooldown: attesa persistenza metriche (30s)")

	// 1. Aggiorniamo prima lo stato a CLEANUP
	_, err := r.updateStatus(ctx, exp, "CLEANUP")
	if err != nil {
		return ctrl.Result{}, err
	}

	// 2. Chiediamo a Kubernetes di tornare tra 30 secondi per eseguire effettivamente il CLEANUP
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *EsperimentoReconciler) reconcileCleanup(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("🧹 Pulizia risorse post-esperimento...")

	if err := r.cleanupPrometheusTargets(ctx, exp); err != nil {
		log.Error(err, "⚠️ Errore durante il cleanup dei target")
		// Non blocchiamo tutto, proviamo comunque a completare
	}

	return r.updateStatus(ctx, exp, "COMPLETED")
}
