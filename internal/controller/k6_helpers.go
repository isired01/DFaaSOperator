package controller

import (
	"context"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// findTestRunForExperiment cerca un TestRun k6 nel namespace dell'esperimento
// che abbia la label dfaas.io/experiment-name corrispondente.
func (r *EsperimentoReconciler) findTestRunForExperiment(ctx context.Context,
	exp *dfaasv1.Esperimento) (*unstructured.Unstructured, bool, error) {

	testRunList := &unstructured.UnstructuredList{}
	testRunList.SetGroupVersionKind(testRunGVR.GroupVersion().WithKind("TestRunList"))

	selector, _ := labels.Parse("dfaas.io/experiment-name=" + exp.Name)
	opts := &client.ListOptions{
		LabelSelector: selector,
		Namespace:     exp.Namespace,
	}

	if err := r.List(ctx, testRunList, opts); err != nil {
		return nil, false, err
	}

	if len(testRunList.Items) == 0 {
		return nil, false, nil
	}

	// Prendi il più recente (ultimo nella lista, che è il più recente per creazione)
	latest := &testRunList.Items[len(testRunList.Items)-1]
	return latest, true, nil
}

// getStageFromTestRun estrae il campo status.stage da un TestRun unstructured.
func getStageFromTestRun(tr *unstructured.Unstructured) string {
	stage, found, err := unstructured.NestedString(tr.Object, "status", "stage")
	if err != nil || !found {
		return ""
	}
	return stage
}

// getK6PodTerminationTime cerca i pod associati al TestRun e restituisce l'orario di terminazione.
func (r *EsperimentoReconciler) getK6PodTerminationTime(ctx context.Context, namespace, testRunName string) (time.Time, bool) {
	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(namespace)); err == nil {
		for _, pod := range podList.Items {
			// K6-operator crea pod "initializer" e "starter" che terminano subito.
			// Vogliamo prendere l'orario di un pod "runner" vero e proprio.
			if strings.HasPrefix(pod.Name, testRunName) &&
				!strings.Contains(pod.Name, "-initializer-") &&
				!strings.Contains(pod.Name, "-starter-") {

				for _, cs := range pod.Status.ContainerStatuses {
					if cs.State.Terminated != nil {
						return cs.State.Terminated.FinishedAt.Time, true
					}
				}
			}
		}
	}
	return time.Time{}, false
}
