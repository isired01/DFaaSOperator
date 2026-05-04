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
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	dfaasv1 "dfaas-operator/api/v1"
)

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

const esperimentoFinalizer = "dfaas.dfaas.io/finalizer"

func (r *EsperimentoReconciler) Reconcile(ctx context.Context,
	req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	var exp dfaasv1.Esperimento

	if err := r.Get(ctx, req.NamespacedName, &exp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if exp.Status.Phase == "" {
		log.Info("Inizio Reconcile")
	}

	// 1. Finalizer & Deletion Logic
	if !exp.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, &exp)
	}

	if !controllerutil.ContainsFinalizer(&exp, esperimentoFinalizer) {
		controllerutil.AddFinalizer(&exp, esperimentoFinalizer)
		return ctrl.Result{}, r.Update(ctx, &exp)
	}

	// 2. State Machine
	switch exp.Status.Phase {

	case "":
		log.Info("Inizio handleInitialState")
		return r.handleInitialState(ctx, &exp)

	case "PROVISIONING_INFRA":
		log.Info("Inizio PROVISIONING_INFRA")
		return r.reconcileInfra(ctx, &exp)

	case "PROVISIONING_MONITORING":
		log.Info("Inizio PROVISIONING_MONITORING")
		return r.reconcileMonitoring(ctx, &exp)

	case "COOLDOWN":
		log.Info("Inizio COOLDOWN")
		return r.reconcileCooldown(ctx, &exp)

	case "CLEANUP":
		log.Info("Inizio CLEANUP")
		return r.reconcileCleanup(ctx, &exp)

	case "COMPLETED":
		log.Info("Esperimento Completo")
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// Funzione per aggiornare la fase dell'esperimento
func (r *EsperimentoReconciler) updateStatus(ctx context.Context,
	exp *dfaasv1.Esperimento, fase string) (ctrl.Result, error) {
	// 1. Rileggiamo l'oggetto fresco dal cluster per evitare conflitti di versione

	latestExp := &dfaasv1.Esperimento{}
	if err := r.Get(ctx, client.ObjectKeyFromObject(exp), latestExp); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Aggiorniamo la fase sulla versione appena scaricata
	latestExp.Status.Phase = dfaasv1.FaseEsperimento(fase)

	if err := r.Status().Update(ctx, latestExp); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{Requeue: true}, nil
}

// Gestione dello stato iniziale
func (r *EsperimentoReconciler) handleInitialState(ctx context.Context,
	exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Controllo che non vi siano VM vecchie passo alla fase di PROVISIONING")
	//se ci sono robe da pulire va i cleanUP e chiamo il metodo per pulire
	//Controllo che posso offrire quello che mi è stato richiesto
	return r.updateStatus(ctx, exp, "PROVISIONING_INFRA")
}

// SetupWithManager sets up the controller with the Manager.
func (r *EsperimentoReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&dfaasv1.Esperimento{}).
		Complete(r)
}
