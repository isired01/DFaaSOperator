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
	"text/template"
	"time"

	corev1 "k8s.io/api/core/v1"                   // Per ConfigMap
	"k8s.io/apimachinery/pkg/api/errors"          // Per errors.IsAlreadyExists
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1" // Per ObjectMeta
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

//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=esperimentos,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=esperimentos/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=dfaas.dfaas.io,resources=esperimentos/finalizers,verbs=update

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
		}
	} else {
		// L'oggetto È in fase di cancellazione(lancio k delete)
		if controllerutil.ContainsFinalizer(&esperimento, esperimentoFinalizer) {
			// ESEGUIAMO LA PULIZIA DELLE VM
			log.Info("🗑️ Risorsa in cancellazione: pulizia VM in corso...")

			// Rimuoviamo il finalizer per permettere a K8s di eliminare l'oggetto
			controllerutil.RemoveFinalizer(&esperimento, esperimentoFinalizer)
			if err := r.Update(ctx, &esperimento); err != nil {
				return ctrl.Result{}, err
			}
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

	case "PROVISIONING":
		//chiamare funz per fare deploy passandoli i dati nel manifest
		//tradurre da oggetti go a terraform tipo?!
		log.Info("➡️ Risorse verificate: PROVISIONING -> READY")
		time.Sleep(3 * time.Second)
		return r.updateStatus(ctx, &esperimento, "READY")

	case "READY":
		log.Info("🚀 L'esperimento è pronto. Devo lanciare k6...")
		log.Info(" Lancio k6: READY -> RUNNING")
		time.Sleep(3 * time.Second)

		log.Info("🛠️ Generazione script k6...")

		scriptJS, err := r.generaScriptK6(&esperimento)
		if err != nil {
			log.Error(err, "Errore nella generazione dello script")
			return r.updateStatus(ctx, &esperimento, "FAILED")
		}

		// Definiamo la ConfigMap
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      esperimento.Name + "-script-k6",
				Namespace: esperimento.Namespace,
			},
			Data: map[string]string{
				"test.js": scriptJS,
			},
		}

		// La creiamo su Kubernetes
		if err := r.Create(ctx, cm); err != nil {
			if !errors.IsAlreadyExists(err) {
				log.Error(err, "Errore creazione ConfigMap")
				return r.updateStatus(ctx, &esperimento, "FAILED")
			}
		}

		log.Info("✅ ConfigMap creata con successo!")
		return r.updateStatus(ctx, &esperimento, "RUNNING")

	case "RUNNING":
		log.Info("⏳ k6 sta generando il carico...")
		log.Info("RUNNING -> COOLDOWN")
		time.Sleep(3 * time.Second)
		// Qui controlleremo se il pod di k6 ha finito
		return r.updateStatus(ctx, &esperimento, "COOLDOWN")

	case "COOLDOWN":
		log.Info("⏳ Aspetto che le metriche finiscano di arrivare")
		log.Info("COOLDOWN -> COMPLETED")
		time.Sleep(3 * time.Second)
		return r.updateStatus(ctx, &esperimento, "COMPLETED")

	case "CLEANUP":
		log.Info("🧹 Pulizia risorse...")
		time.Sleep(3 * time.Second)
		return r.updateStatus(ctx, &esperimento, "")

	case "COMPLETED":
		log.Info("Esperimento completato")
		//da capire che fare qui nel senso che in che stato vado dopo questo?
	}

	return ctrl.Result{}, nil
}

// Funzione per aggiornare la fase dell'esperimento
func (r *EsperimentoReconciler) updateStatus(ctx context.Context, exp *dfaasv1.Esperimento, fase string) (ctrl.Result, error) {
	exp.Status.Fase = fase
	if err := r.Status().Update(ctx, exp); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil // Richiede un nuovo ciclo subito
}

// Gestione dello stato iniziale
func (r *EsperimentoReconciler) handleInitialState(ctx context.Context, exp *dfaasv1.Esperimento) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Controllo che non vi siano VM vecchie passo alla fase di PROVISIONING")
	//se ci sono robe da pulire va i cleanUP e chiamo il metodo per pulire
	return r.updateStatus(ctx, exp, "PROVISIONING")
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

{{- $scenario := index .Spec.Profilo.Scenari 0 }}
{{- $profilo := .Spec.Profilo }}

export const options = {
  scenarios: {
    "{{ $scenario.NomeScenario }}": {
      executor: 'ramping-arrival-rate',
      startRate: {{ $scenario.StartTime }},
      timeUnit: '1s',
      preAllocatedVUs: {{ $scenario.VuAllocati }},
      stages: [
        {{- range $scenario.Stages }}
        { duration: '{{ .Durata }}', target: {{ .TargetRps }} },
        {{- end }}
      ],
    },
  },
};

export default function () {
  const url = 'http://{{ $scenario.TargetNodeID }}/function/{{ $scenario.NomeFunzioneTarget }}';
  const payload = '{{ $scenario.Body }}';
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
