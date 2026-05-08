# dFaaS Experiment Controller

Benvenuto nel repository del **dFaaS Experiment Controller**, il cuore orchestrativo del sistema **dFaaS** (distributed Function-as-a-Service). Questo progetto implementa un operatore Kubernetes  per la gestione automatizzata del ciclo di vita di esperimenti FaaS in ambienti Edge/Cloud federati.

## 🚀 Panoramica del Progetto

Il sistema è progettato per semplificare la ricerca e il testing di architetture dFaaS, permettendo di passare dalla definizione astratta di una federazione all'esecuzione di test di carico e raccolta metriche in pochi click.

### Componenti Principali
*   **dFaaS Operator (Core)**: Un operatore Kubernetes sviluppato in Go che gestisce la Custom Resource `Esperimento`. Coordina il provisioning, l'installazione, l'esecuzione dei test e la pulizia finale.
*   **Data Exporter**: Un componente specializzato che estrae metriche granulari da Prometheus alla fine di ogni esperimento e le archivia in formato CSV su **MinIO**.
*   **Ansible**: Utilizzato dall'operatore per configurare dinamicamente le VM esterne e i nodi della federazione.

*   **dFaaS UI & API Gateway**: Un'interfaccia web moderna (**React + Vite**) e un gateway (**Go + Gin**) che permettono di monitorare e comandare il cluster in tempo reale senza interagire direttamente con i manifest YAML.

---

## 🏗️ Architettura e Stato dell'Esperimento

L'operatore implementa un'automa a stati finiti (FSM) per garantire determinismo e resilienza durante l'esecuzione:

| Fase | Descrizione |
| :--- | :--- |
| `INFRASTRUCTURE_PROVISIONING` | Provisioning delle VM tramite Ansible. |
| `INSTALLING_DFAAS` | Installazione degli agent dFaaS e configurazione del routing. |
| `PROVISIONING_MONITORING` | Deployment dello stack di monitoraggio (Prometheus/Grafana/MinIO). |
| `READY` | Sistema pronto, in attesa del comando di avvio test dalla UI. |
| `RUNNING` | Esecuzione del test di carico tramite **k6-operator**. |
| `COOLDOWN` | Periodo di stabilizzazione post-test (30s) per catturare metriche residue. |
| `EXPORT_METRICHE` | Estrazione dati da Prometheus e upload su Object Storage (MinIO). |
| `CLEANUP` | Rimozione automatica delle risorse temporanee se richiesto. |
| `COMPLETED` | Esperimento terminato con successo. |

---

## 🛠️ Requisiti di Sistema

*   **Kubernetes Cluster**: v1.25+
*   **k6-operator**: Installato nel cluster per gestire i test di carico.
*   **Go**: v1.22+ (per sviluppo)
*   **Docker**: Per il build delle immagini.

---

## 🏁 Getting Started

### 1. Installazione dell'Operatore
Per compilare e installare l'operatore nel cluster corrente:

```bash
# Genera i manifesti delle CRD
make manifests

# Installa le CRD nel cluster
make install

# deply locale
make run

# Build e push dell'immagine (sostituisci la tua registry)
make docker-build docker-push IMG=ghcr.io/tuo-user/dfaas-operator:latest

# Deploy del controller
make deploy IMG=ghcr.io/tuo-user/dfaas-operator:latest
```

### 2. Accesso alla UI
La UI si trova a [qui](https://github.com/isired01/DFaaS_UI). Per avviarla localmente in modalità sviluppo:

```bash
# Avvio del Backend (API Gateway)
cd UI
go run ./cmd/server/main.go

# Avvio del Frontend
cd UI/ui
npm install
npm run dev
```
La dashboard sarà accessibile su `http://localhost:5173`.

---

## 📊 Monitoraggio ed Export
L'intero sistema è strumentato per Prometheus. Alla fine dell'esperimento, l'operatore lancia automaticamente un `DataExporter` che:
1.  Recupera le query PromQL specificate nell'UI.
2.  Genera un file CSV con timestamp e `nodo_id`.
3.  Carica il report nel bucket `dfaas-results` su MinIO.

---

## 🤝 Contribuire
Il progetto è parte di un lavoro di tesi focalizzato sulla federazione Edge/Cloud. Feedback e pull request sono benvenuti!

---

> [!IMPORTANT]
> Assicurati che i nodi target siano raggiungibili via SSH e che le chiavi siano correttamente configurate nelle Secret di Kubernetes.
