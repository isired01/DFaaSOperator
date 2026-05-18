# dFaaS Experiment Controller

Kubebuilder-scaffolded Kubernetes operator (Go 1.25, controller-runtime v0.17.3) that orchestrates **dFaaS** (distributed FaaS) experiments end-to-end: from federation provisioning to k6 load tests and metrics export.

The operator ships as a single Deployment that runs **two controllers** against two CRDs in `dfaas.dfaas.io/v1`:

- **`Environment`** — federation infrastructure. Provisions VMs (dfaas-worker nodes + k6-load-generator nodes) and the operator-cluster monitoring stack. Long-lived: once `Ready`, it stays idle until the spec changes.
- **`LoadTest`** — one k6 load test against an `Environment`. Looks up its `targetEnvironment`, waits until it is `Ready`, dispatches one remote k6 `TestRun` per k6-load-generator node, then runs a metrics exporter Job.

## 🚀 What it does

```text
+---------------------------+        +-----------------------------+
| Environment CR |  | LoadTest CR |
| -------------- ||-----------------------------|
| nodes: dfaas-worker, k6   |        | targetEnvironment: env-xyz  |
| topology (latency links)  |        | perNodeLoad[]               |
| openfaas functions        |        | metricsExport.queries (PromQL)|
+-------------+-------------+        +--------------+--------------+
              | reconcile                            | reconcile
              v                                      v
   +----------+----------+               +-----------+------------+
   |  Ansible Jobs (VMs) | ----------->  | Remote k6 TestRun via  |
   |  Helm (Prometheus,  |               | per-node k3s + k6-op   |
   |  Grafana on mgmt)   |               +-----------+------------+
   +----------+----------+                           |
              |                                      v
              v                              +-------+--------+
       Environment.Ready                     | Exporter Job   |
                                             | Prometheus →   |
                                             | CSV (stdout/   |
                                             |  Google Drive) |
                                             +----------------+
```

## 🏗️ State machines

### `Environment`
```
"" / Idle / Failed → ProvisioningVMs → ProvisioningInfra → Ready
                                       ├─ K6 Ansible Job ─┐
                                       └─ Monitoring Helm ┘  (parallel; fan-in)
```
`ProvisioningInfra` runs the K6 Ansible Job and the Helm-based monitoring install concurrently and advances to `Ready` only when **both** terminate. If either fails the surviving stream is still allowed to finish, then the phase becomes `Failed` with per-component `K6Ready` / `MonitoringReady` Conditions for granular diagnosis.

Re-provisioning only runs when `spec.generation` changes. A finalizer (`dfaas.dfaas.io/environment-finalizer`) cleans per-environment Prometheus scrape-target entries on delete. Ansible Jobs auto-cleanup with `TTLSecondsAfterFinished=600` on success and `86400` (24h) on failure.

### `LoadTest`
```
Pending → Running → Exporting → Completed
                              ↘ Failed
```
The reconciler watches the target `Environment`: a Pending LoadTest auto-resumes when the env reaches `Ready`.

## 🧩 Components

| Component                                                | Path                                     | Image                                                                        |
| -------------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| Operator (`Environment` + `LoadTest` controllers)        | `cmd/` + `internal/controller/`          | built from repo root `Dockerfile`                                            |
| Data exporter (Prometheus → CSV + optional Drive upload) | `dataExporter/`                          | `ghcr.io/isired01/dfaas-exporter:latest` (multi-arch, separate `Dockerfile`) |
| Ansible playbooks (worker + k6 provisioning)             | `internal/controller/ansible/templates/` | `alpine/ansible:2.18.6` (see note below)                                     |
| Monitoring stack (Prometheus + Grafana, Helm-managed)    | `internal/controller/monitoring/charts/` | vendored `.tgz` chart bundles                                                |
| Front-end + API gateway (separate repo)                  | `https://github.com/isired01/DFaaS_UI`   | —                                                                            |

## 🛠️ Requirements

- Kubernetes cluster ≥ v1.25 (tested on k3s + standard kubeadm)
- Go ≥ 1.25 for local development
- Docker + buildx (multi-arch exporter image)
- A Helm-compatible cluster (no extra installation needed — the operator drives Helm via the embedded SDK)
- Optional, for Drive export: a Google Cloud project with the Drive API enabled, a service account, and a **Google Workspace Shared Drive** (My Drive doesn't work — service accounts have no personal quota)

## 🏁 Getting Started

```bash
# Generate CRDs + RBAC from kubebuilder markers
make manifests generate

# Install the CRDs into the current cluster
make install

# Run the controller locally against your current kubecontext
make run

# Apply samples
kubectl apply -f config/samples/dfaas_v1_environment.yaml
# Wait until env-sample reaches `Ready`, then:
kubectl apply -f config/samples/dfaas_v1_loadtest.yaml
```

To deploy the operator inside the cluster instead:

```bash
make docker-buildx IMG=ghcr.io/<you>/dfaas-operator:latest   # multi-arch build + push
make deploy        IMG=ghcr.io/<you>/dfaas-operator:latest
```

To rebuild the exporter image (separate from the operator):

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/<you>/dfaas-exporter:latest \
  -f dataExporter/Dockerfile dataExporter --push
```

## 📊 Metrics export

During the `Exporting` phase, the LoadTestReconciler creates `<loadtest>-exporter-job` running the `dfaas-exporter` image. It pulls metrics from the management-cluster Prometheus over `[startTime, endTime]` (queries come from `spec.metricsExport.queries`) and writes them to a CSV.

The destination depends on `spec.metricsExport.googleDrive`:

| Configuration           | Behaviour                                                                                                                                                                                                  |
| ----------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `googleDrive` **unset** | CSV dumped to stdout between `----- BEGIN CSV -----` / `----- END CSV -----` markers. Retrievable with `kubectl logs job/<loadtest>-exporter-job`.                                                         |
| `googleDrive` **set**   | Service-account JSON is read from a Secret you provisioned manually (key `credentials.json`), the file is uploaded into the configured Drive folder via `drive.Files.Create(...).SupportsAllDrives(true)`. |

Provision the credentials Secret out-of-band — the operator does **not** create it:

```bash
kubectl create secret generic <credentialsSecretRef> -n <namespace> \
  --from-file=credentials.json=/path/to/service-account.json
```

The Drive folder **must live inside a Workspace Shared Drive** with the service account added as Content manager, otherwise upload fails with `storageQuotaExceeded` (service accounts have no personal storage quota).

The management-cluster Prometheus federates worker metrics via `/federate?match[]={job!=""}`, so PromQL like `rate(node_cpu_seconds_total[1m])` and `container_memory_working_set_bytes` resolve against each worker's local Prometheus. UIs are exposed as NodePort: Prometheus on `http://<management-node-ip>:30090`, Grafana on `:30300`.

## ⚠️ Known limitations / pins

- **`alpine/ansible:2.18.6`** is pinned ([`internal/controller/ansible/job.go`](internal/controller/ansible/job.go)) instead of `latest` / `2.20.0` because of an upstream bug in the `mschuchard.general` collection (`No module named 'mschuchard'` — broken absolute imports inside `plugins/module_utils/*.py` that escape the `ansible_collections.*` namespace at remote-execution time). Bug reproduces identically on ansible-core 2.18.6 and 2.20.0, so the operator side-steps the collection entirely and shells out to `faas-cli` for OpenFaaS function deployment. The pin makes the failure trivially reproducible if you want to verify against upstream; bump it back once the collection is patched.
- **Worker VMs must have correct clocks** (NTP enabled) — `apt update` rejects Release files dated in the future, which blocks the dfaas-worker provisioning job.
- **Google Drive export requires a Shared Drive.** Personal `@gmail.com` accounts and Workspace tiers that disable Shared Drives cannot use this path; fall back to stdout dump (`googleDrive` unset) or wire an alternative storage (MinIO/S3) if persistence beyond Pod GC is required.
- **Balancing strategies — partial support.** Supported out of the box: `staticstrategy`, `alllocalstrategy`, `recalcstrategy`. The latter requires `Function.maxRate` (emitted as `dfaas.maxrate` OpenFaaS label). `nodemarginstrategy` and `rlagentstrategy` have incomplete upstream documentation and may require additional labels not yet emitted by the operator — use them at your own risk.

> [!IMPORTANT]
> Target VMs must be reachable via SSH from the cluster network and the credentials in `Environment.spec.nodes[].password` must be valid before applying the manifest. The operator does **not** distribute SSH keys. (The dFaaS Agent libp2p identity is handled separately via the operator-managed `<env>-libp2p-keys` Secret — see CLAUDE.md.)
