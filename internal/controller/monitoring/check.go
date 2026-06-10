package monitoring

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Check returns true when both the prometheus and grafana pods are in the
// Running phase with ContainerReady=True.
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

		if isReady {
			if strings.Contains(pod.Name, "prometheus") {
				promReady = true
			}
			if strings.Contains(pod.Name, "grafana") {
				grafanaReady = true
			}
		}
	}

	return promReady && grafanaReady, nil
}
