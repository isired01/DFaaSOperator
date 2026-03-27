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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- SOTTO-STRUTTURE PER METRICHE ---

type ConfigurazoneMetrica struct {
	Query    string `json:"query"` // La query PromQL o nome metrica
	Commento string `json:"commento,omitempty"`
}

// --- SOTTO-STRUTTURE PER FEDERAZIONE ---

type ConfigurazioneFederazione struct {
	Strategia string               `json:"strategia"` // BASE, EQUAL, NODE_MARGIN, etc.
	Nodi      []ConfigurazioneNodo `json:"nodi"`
}

type ConfigurazioneNodo struct {
	IDNodo   string         `json:"idNodo"`
	TipoNodo string         `json:"tipoNodo"` // LOW, MEDIUM, HIGH
	Funzioni []string       `json:"funzioni"`
	Vicini   map[string]int `json:"idVicini,omitempty"` // IDNodo -> LinkType(in ms)
}

// --- SOTTO-STRUTTURE PER PROFILO CARICO (k6) ---

type ProfiloCarico struct {
	CommonHeaders string     `json:"commonHeaders,omitempty"`
	Scenari       []Scenario `json:"scenari"`
}

type Scenario struct {
	NomeScenario       string    `json:"nomeScenario"`
	TargetNodeID       string    `json:"targetNodeID"`
	NomeFunzioneTarget string    `json:"nomeFunzioneTarget"`
	Body               string    `json:"body,omitempty"`
	DataPath           string    `json:"dataPath,omitempty"`
	Esecutore          string    `json:"esecutore"` // CONSTANT_ARRIVAL_RATE o RAMPING_ARRIVAL_RATE
	Stages             []StageK6 `json:"stages"`
	VuAllocati         int       `json:"vuAllocati"`
	StartTime          int       `json:"startTime,omitempty"`
}

type StageK6 struct {
	Durata    string `json:"durata"`
	TargetRps int    `json:"targetRps"`
}

// --- CORE DELLA CRD ---

// EsperimentoSpec definisce lo stato desiderato di Esperimento
type EsperimentoSpec struct {
	Nome        string                    `json:"nome"`
	Versione    string                    `json:"versione"`
	Metriche    []ConfigurazoneMetrica    `json:"metriche,omitempty"`
	Federazione ConfigurazioneFederazione `json:"federazione"`
	Profilo     ProfiloCarico             `json:"profiloCarico"`
}

// EsperimentoStatus definisce lo stato osservato di Esperimento
type EsperimentoStatus struct {
	// Fase dell'automa: IDLE, VALIDAZIONE_FILE, PROVISIONING, READY, RUNNING, COOL_DOWN, EXPORT_METRICHE, RESULTS, CLEANUP, FAIL
	Fase       string             `json:"fase,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
	// Messaggio di dettaglio (es. motivo di un fallimento)
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Esperimento è lo schema per l'API degli esperimenti
type Esperimento struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EsperimentoSpec   `json:"spec,omitempty"`
	Status EsperimentoStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EsperimentoList contiene una lista di Esperimento
type EsperimentoList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Esperimento `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Esperimento{}, &EsperimentoList{})
}
