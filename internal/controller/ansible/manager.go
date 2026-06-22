package ansible

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager runs the Ansible provisioning operations (Job, ConfigMap,
// Helm values) for a DFaaS environment.
type Manager struct {
	client.Client
	Scheme *runtime.Scheme
}
