package monitoring

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Check returns true when the prometheus server, grafana, and the SeaweedFS
// sink pods are all in the Running phase with PodReady=True.
func (m *Manager) Check(ctx context.Context) (bool, error) {
	podList := &corev1.PodList{}
	opts := []client.ListOption{
		client.InNamespace("monitoring"),
	}

	if err := m.List(ctx, podList, opts...); err != nil {
		return false, err
	}

	promReady := false
	grafanaReady := false
	seaweedReady := false

	for _, pod := range podList.Items {
		isReady := false
		if pod.Status.Phase == corev1.PodRunning {
			for _, cond := range pod.Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					isReady = true
					break
				}
			}
		}
		if !isReady {
			continue
		}

		// Match the server pod only: the chart's other components are disabled
		// in values/prometheus-values.yaml.
		name := pod.Name
		if strings.Contains(name, "prometheus") && strings.Contains(name, "server") {
			promReady = true
		}
		if strings.Contains(name, "grafana") {
			grafanaReady = true
		}
		if strings.Contains(name, "seaweedfs") {
			seaweedReady = true
		}
	}

	return promReady && grafanaReady && seaweedReady, nil
}
