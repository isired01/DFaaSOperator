package ansible

import (
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager esegue le operazioni di provisioning Ansible (Job, ConfigMap,
// Helm values) per un esperimento dFaaS.
type Manager struct {
	client.Client
	Scheme *runtime.Scheme
}

func int32Ptr(i int32) *int32 { return &i }
