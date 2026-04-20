/*
Copyright 2026.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- ENUMS ---

type FaseEsperimento string

const (
	FaseIdle      FaseEsperimento = "IDLE"
	FaseProvision FaseEsperimento = "PROVISIONING"
	FaseReady     FaseEsperimento = "READY"
	FaseRunning   FaseEsperimento = "RUNNING"
	FaseCooldown  FaseEsperimento = "COOLDOWN"
	FaseCompleted FaseEsperimento = "COMPLETED"
	FaseFailed    FaseEsperimento = "FAILED"
	FaseCleanup   FaseEsperimento = "CLEANUP"
)

type NodeCapacity string

const (
	CapacityLow    NodeCapacity = "LOW"
	CapacityMedium NodeCapacity = "MEDIUM"
	CapacityHigh   NodeCapacity = "HIGH"
)

// --- SOTTO-STRUTTURE (SPEC) ---

type Federazione struct {
	// Strategia di load balancing (es: "ROUND_ROBIN")
	// +kubebuilder:validation:Required
	Strategia string `json:"strategia"`

	// Lista dei nodi della federazione per questo specifico esperimento
	// +kubebuilder:validation:MinItems=1
	Nodi []Nodo `json:"nodi"`
}

type Nodo struct {
	IDNodo      string       `json:"idNodo"`
	IndirizzoIP string       `json:"indirizzoIP"` //se non specificato vuol dire che la macchina va creata
	Capacita    NodeCapacity `json:"capacita"`
	Funzioni    []string     `json:"funzioni"`
}

type Topologia struct {
	// Definizione dei link di rete tra i nodi dell'esperimento
	Links []Link `json:"links"`
}

type Link struct {
	NodoA     string `json:"nodoA"`
	NodoB     string `json:"nodoB"`
	LatenzaMs int    `json:"latenzaMs"`
}

// --- CORE DELLA CRD ---

// EsperimentoSpec definisce i parametri di configurazione del SINGOLO esperimento
type EsperimentoSpec struct {
	// Nome identificativo dell'esperimento (può differire dal nome della risorsa K8s)
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Configurazione della federazione dedicata
	// +kubebuilder:validation:Required
	Federazione Federazione `json:"federazione"`

	// Flag per richiedere la pulizia delle risorse a fine test
	// +kubebuilder:default=false
	IsCleanupRequested bool `json:"isCleanupRequested"`

	// Topologia di rete dell'esperimento
	// +kubebuilder:validation:Required
	Topologia Topologia `json:"topologia"`
}

// EsperimentoStatus definisce lo stato osservato dell'istanza (gestito dal Controller)
type EsperimentoStatus struct {
	// Conditions rappresenta l'osservazione dello stato attuale dell'esperimento
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Stato attuale dell'esecuzione
	// +kubebuilder:default=IDLE
	// +optional
	Fase FaseEsperimento `json:"fase,omitempty"`

	// Messaggio di dettaglio (es. motivo di un FAILED)
	// +optional
	Message string `json:"message,omitempty"`

	// Timestamp di inizio dell'esecuzione effettiva
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// Timestamp di fine dell'esecuzione
	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.fase"

// Esperimento è lo schema per la risorsa singola
type Esperimento struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EsperimentoSpec   `json:"spec,omitempty"`
	Status EsperimentoStatus `json:"status,omitempty"`
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
