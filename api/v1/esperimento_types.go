/*
Copyright 2026.
*/

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// --- SOTTO-STRUTTURE PER METRICHE ---

type ConfigurazoneMetrica struct {
	// Step di campionamento (intervallo tra le rilevazioni)
	// +kubebuilder:default=15
	// +kubebuilder:validation:Minimum=1
	Step int `json:"step"`

	// Lista delle metriche da raccogliere
	// +kubebuilder:validation:MinItems=1
	Metrics []Metric `json:"metrics"`
}

type Metric struct {
	// La query PromQL da eseguire (es: rate(http_requests_total[5m]))
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=3
	Query string `json:"query"`

	// Note opzionali per descrivere la metrica
	// +kubebuilder:validation:MaxLength=100
	Comment string `json:"comment,omitempty"`
}

// --- SOTTO-STRUTTURE PER FEDERAZIONE ---

type ConfigFed struct {
	// Strategia di federazione del carico
	// +kubebuilder:validation:Required
	// +kubebuilder:default=BASE
	Strategy string `json:"strategy"`

	// Configurazione specifica per ogni nodo della federazione
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Nodi []ConfigNode `json:"nodi"`
}

type ConfigNode struct {
	// ID univoco del nodo all'interno della rete
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	IDNodo string `json:"idNodo"`

	// Indirizzo IPv4 del nodo
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$`
	IndirizzoIP string `json:"indirizzoIP"`

	// Classe di potenza del nodo (capacità computazionale)
	// +kubebuilder:validation:Required
	// +kubebuilder:default=MEDIUM
	TipoNodo string `json:"tipoNodo"`

	// Elenco delle funzioni distribuite su questo nodo
	// +kubebuilder:validation:MinItems=1
	Funzioni []string `json:"funzioni"`

	// Mappa delle adiacenze: IDNodo -> Latenza stimata (ms)
	// +optional
	Vicini map[string]int `json:"idVicini,omitempty"`
}

// --- SOTTO-STRUTTURE PER PROFILO CARICO (k6) ---

type LoadProfile struct {
	// Header comuni a tutte le richieste (formato JSON string)
	// +optional
	CommonHeaders string `json:"commonHeaders,omitempty"`

	// Elenco degli scenari di carico da eseguire
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Scenari []Scenario `json:"scenari"`
}

type Scenario struct {
	// Nome identificativo dello scenario
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	NomeScenario string `json:"nomeScenario"`

	// ID del nodo target della federazione
	// +kubebuilder:validation:Required
	TargetNodeID string `json:"targetNodeID"`

	// Nome della funzione da testare sul nodo
	// +kubebuilder:validation:Required
	NomeFunzioneTarget string `json:"nomeFunzioneTarget"`

	// Body della richiesta (opzionale, per POST/PUT)
	// +optional
	Body string `json:"body,omitempty"`

	// Eventuale percorso a un file di dati
	// +optional
	DataPath string `json:"dataPath,omitempty"`

	// Tipo di esecutore k6
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Enum=CONSTANT_ARRIVAL_RATE;RAMPING_ARRIVAL_RATE
	// +kubebuilder:default=CONSTANT_ARRIVAL_RATE
	Esecutore string `json:"esecutore"`

	// Definizione dei carichi (fasi del test)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Stages []StageK6 `json:"stages"`

	// Numero di Virtual Users da allocare per questo scenario
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Minimum=1
	VuAllocati int `json:"vuAllocati"`

	// Offset di inizio dello scenario in secondi
	// +kubebuilder:default=0
	// +kubebuilder:validation:Minimum=0
	StartTime int `json:"startTime,omitempty"`
}

type StageK6 struct {
	// Durata della fase di test (es: "30s", "1m", "2h")
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^[0-9]+(s|m|h)$`
	Durata string `json:"durata"`

	// Target di richieste al secondo (RPS) da raggiungere in questa fase
	// +kubebuilder:validation:Minimum=0
	TargetRps int `json:"targetRps"`
}

// --- CORE DELLA CRD ---

// EsperimentoSpec definisce lo stato desiderato di Esperimento
type EsperimentoSpec struct {
	// Configurazione delle metriche da monitorare ed esportare
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	ConfigMetriche []ConfigurazoneMetrica `json:"metrics"`

	// Configurazione della strategia di federazione tra nodi
	// +kubebuilder:validation:Required
	Federazione ConfigFed `json:"federation"`

	// Profilo di carico e scenari per il test k6
	// +kubebuilder:validation:Required
	Profilo LoadProfile `json:"loadProfile"`
}

// EsperimentoStatus definisce lo stato osservato di Esperimento
type EsperimentoStatus struct {
	// Fase attuale dell'esperimento (Macchina a Stati)
	// +kubebuilder:default=IDLE
	Fase string `json:"fase,omitempty"`

	// Conditions rappresenta l'osservazione dello stato attuale dell'esperimento
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// Messaggio leggibile dall'utente con dettagli sullo stato (es. errore specifico)
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
