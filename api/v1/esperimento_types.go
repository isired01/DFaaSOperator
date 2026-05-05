/*
Copyright 2026.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- ENUMS ---

type NodeCapacity string

const (
	CapacityLow    NodeCapacity = "LOW"
	CapacityMedium NodeCapacity = "MEDIUM"
	CapacityHigh   NodeCapacity = "HIGH"
)

type BalancingStrategy string

const (
	staticStrategy     BalancingStrategy = "staticstrategy"
	nodeMarginStrategy BalancingStrategy = "nodemarginstrategy"
	recalcStrategy     BalancingStrategy = "recalcstrategy"
	Alllocalstrstegy   BalancingStrategy = "alllocalstrategy"
	RLAgentStrategy    BalancingStrategy = "rlagentstrategy"
)

// --- SOTTO-STRUTTURE (SPEC) ---

type Federation struct {

	// Lista dei nodi della federazione per questo specifico esperimento
	// +kubebuilder:validation:MinItems=1
	Nodes []Node `json:"nodes"`
}

type Function struct {
	// Nome della funzione (es: "figlet")
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Immagine completa (es: "functions/figlet:latest")
	// +kubebuilder:validation:Required
	Image string `json:"image"`

	// Tempo massimo di esecuzione (corrisponde a exec_timeout)
	// +kubebuilder:default=5
	ExecTimeout int `json:"execTimeout"`

	// Massimo numero di richieste parallele (corrisponde a max_inflight)
	// +kubebuilder:default=400
	MaxInflight int `json:"maxInflight"`

	// Timeout logico per l'operatore dFaaS in millisecondi
	// +kubebuilder:default=6000
	TimeoutMs int `json:"timeoutMs"`
}

// --- MODIFICA STRUTTURA NODO ---

type Node struct {
	NodeID            string       `json:"nodeID"`
	IpAddress         string       `json:"ipAddress"`
	Username          string       `json:"username"`
	Password          string       `json:"password"`
	Capacity          NodeCapacity `json:"capacity"`
	BalancingStrategy string       `json:"balancingStrategy"`
	PrivateKey        string       `json:"privateKey"`

	// Lista di configurazioni per le funzioni da deployare su questo nodo
	// +optional
	Functions []Function `json:"functions,omitempty"`
}

type Topology struct {
	// Definizione dei link di rete tra i nodi dell'esperimento
	Links []Link `json:"links"`
}

type Link struct {
	NodeA     string `json:"nodeA"`
	NodeB     string `json:"nodeB"`
	LatencyMs int    `json:"latencyMs"`
}

// --- CORE DELLA CRD ---

type ExperimentSpec struct {
	// Configuration of the dedicated federation
	// +kubebuilder:validation:Required
	Federation Federation `json:"federation"`

	// Flag to request resource cleanup after the test
	// +kubebuilder:default=false
	IsCleanupRequested bool `json:"isCleanupRequested"`

	// Network topology of the experiment
	// +kubebuilder:validation:Required
	Topology Topology `json:"topology"`
}

// EsperimentoStatus definisce lo stato osservato dell'istanza (gestito dal Controller)
type ExperimentStatus struct {
	// Conditions represents the observations of the experiment's current state
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Current execution phase
	// +kubebuilder:default=IDLE
	// +optional
	Phase string `json:"phase,omitempty"`

	// Detailed message (e.g., reason for a FAILED state)
	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"

// Esperimento è lo schema per la risorsa singola
type Esperimento struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ExperimentSpec   `json:"spec,omitempty"`
	Status ExperimentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EsperimentoList è necessario per le operazioni di elenco (es. kubectl get)
type EsperimentoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Esperimento `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Esperimento{}, &EsperimentoList{})
}
