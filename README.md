# DFaaS Experiment Controller

Kubebuilder-scaffolded Kubernetes operator (Go 1.25, controller-runtime v0.17.3) that orchestrates **DFaaS** (distributed FaaS) experiments end-to-end: from federation provisioning to k6 load tests and metrics export.

The operator ships as a single Deployment that runs **two controllers** against two CRDs in `dfaas.dfaas.io/v1`:

- **`Environment`** — federation infrastructure. Provisions VMs (dfaas-worker nodes + k6-load-generator nodes) and the operator-cluster monitoring stack. Long-lived: once `Ready`, it stays idle until the spec changes.
- **`LoadTest`** — one k6 load test against an `Environment`. Looks up its `targetEnvironment`, waits until it is `Ready`, dispatches one remote k6 `TestRun` per k6-load-generator node, then runs a metrics exporter Job.

## Install via Helm

Prereqs: K8s ≥ 1.25, `kubectl` connected to the cluster, `helm` ≥ 3.8 (for OCI), anonymous pull from `ghcr.io` reachable from the cluster.

### Step 1 — Installa operator + UI via Helm

```bash
helm install dfaas oci://ghcr.io/isired01/charts/dfaas \
  --version 1.0.0 \
  --create-namespace \
  --namespace dfaas-operator-system

# Open the UI:
kubectl -n dfaas-ui port-forward svc/dfaas-ui 8082:8082
open http://localhost:8082
```

### Step 2 — Create your first Custom Resources

Ready-made examples in [`config/samples/`](config/samples/):

- [`dfaas_v1_environment.yaml`](config/samples/dfaas_v1_environment.yaml) — Environment with dfaas-worker + k6-load-generator nodes
- [`dfaas_v1_loadtest.yaml`](config/samples/dfaas_v1_loadtest.yaml) — k6 LoadTest against the Environment above

**Before applying, configure the VM IPs** (the operator does NOT provision machines, it only orchestrates them). Edit `spec.nodes[].ipAddress` with the addresses of Ubuntu VMs that are already running and reachable over SSH:

```yaml
spec:
  nodes:
    - id: dfaas-worker-1
      role: dfaas-worker
      ipAddress: 10.0.0.5      # <— replace with the real IP
      sshUser: ubuntu
      sshPassword: ...
      # ...
    - id: k6-load-generator-1
      role: k6-load-generator
      ipAddress: 10.0.0.6      # <— replace with the real IP
      # ...
```

Poi:

```bash
kubectl apply -f config/samples/dfaas_v1_environment.yaml
kubectl get environment -w   # wait for phase=Ready
kubectl apply -f config/samples/dfaas_v1_loadtest.yaml
```

### Override values

```bash
helm install dfaas oci://ghcr.io/isired01/charts/dfaas --version 1.0.0 \
  --set operator.replicas=2 \
  --set ui.ingress.enabled=true \
  --set ui.ingress.host=dfaas.mio-cluster.example
```

### Upgrade

```bash
# Upgrade ONLY operator + UI:
helm upgrade dfaas oci://ghcr.io/isired01/charts/dfaas --version 1.1.0

# If the new release ships modified CRDs, apply them manually BEFORE the chart upgrade:
kubectl apply -f https://github.com/isired01/DFaaSOperator/releases/download/v1.1.0/dfaas.dfaas.io_environments.yaml
kubectl apply -f https://github.com/isired01/DFaaSOperator/releases/download/v1.1.0/dfaas.dfaas.io_loadtests.yaml
```

### Uninstall

```bash
helm uninstall dfaas -n dfaas-operator-system

# CRDs remain (and with them all existing Environment/LoadTest CRs).
# To delete everything (dangerous cascade-delete):
kubectl delete crd environments.dfaas.dfaas.io loadtests.dfaas.dfaas.io
```

## 🚀 What it does

```text
+---------------------------+        +-----------------------------+
      | Environment CR |  | LoadTest CR |
      | -------------- ||-----------------------------|
| nodes: dfaas-worker, k6   |        | targetEnvironment: env-xyz  |
| topology (latency links)  |        | perNodeLoad[]               |
| openfaas functions        |        | metricsExport.metrics[] (PromQL)|
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
                                             |  S3 bucket)    |
                                             +----------------+
```

## 🏗️ State machines

### `Environment`
```
"" / Idle / Failed → ProvisioningVMs            → ProvisioningInfra                              → ProvisioningMonitoring → Ready
                     (no-op placeholder)          ├─ Ansible dfaas-worker Job ─┐                  (Helm install seq)
                                                  └─ Ansible k6-load-gen Job  ─┘  parallel; fan-in
```
`ProvisioningVMs` is a no-op placeholder for future VM-lifecycle integration (terraform / cloud-init / Cluster API). `ProvisioningInfra` runs the two Ansible Jobs concurrently and advances only when **both** terminate; if either fails the surviving stream is still allowed to finish, then the phase becomes `Failed` with per-component `DfaasWorkersReady` / `K6Ready` Conditions for granular diagnosis. `ProvisioningMonitoring` is sequential: Helm install Prometheus + Grafana + scrape-target reconcile.

Re-provisioning only runs when `spec.generation` changes. A finalizer (`dfaas.dfaas.io/environment-finalizer`) cleans per-environment Prometheus scrape-target entries on delete. Ansible Jobs auto-cleanup with `TTLSecondsAfterFinished=600` on success and `86400` (24h) on failure.

### `LoadTest`
```
Pending (Conditions[Suspended]=True)   ← Save as Draft (spec.suspended=true)
   on PATCH spec.suspended=false (or scheduled spec.startAt fires):
Pending → Running → Exporting → Completed
                              ↘ Failed
   spec.stop=true  OR  kubectl delete (finalizer)  → Aborted   (remote TestRuns deleted)
```
The reconciler watches the target `Environment`: a Pending LoadTest auto-resumes when the env reaches `Ready`. A LoadTest created with `spec.suspended: true` (used by the UI for "Save as Draft") stays at `Pending` with `Conditions[Suspended]=True` until the user PATCHes `spec.suspended: false`; mirrors `batch/v1.Job.spec.suspend`. `spec.startAt` schedules a future start (the reconciler flips `suspended=false` when the time arrives). Run-once: once the LoadTest enters `Running`/`Exporting`/`Completed`/`Failed`, further changes to `spec.suspended` are ignored.

**Abort & deletion.** PATCH `spec.stop=true` (allowed in `Pending`/`Running`) deletes every remote `TestRun` and transitions to the terminal `Aborted`, keeping the CR as an audit record. A finalizer (`dfaas.dfaas.io/loadtest-finalizer`) runs the same remote cleanup on `kubectl delete`, so no remote run is orphaned on any deletion path.

## 📡 Phases & Conditions

The operator surfaces resource state through two complementary signals:

- **`status.phase`** — the FSM driver, a single string the reconciler switches on. Stable across reconciles and recognised by the UI for primary badge colouring.
- **`status.conditions[]`** — `kubernetes/community` API-conventions-style conditions. Each entry carries `type`, `status` (`True` / `False` / `Unknown`), a typed `reason` (machine-readable string from constants in [api/v1](api/v1)), and a `message` (human-readable, sanitized of timestamps / pod UIDs / request IDs so `LastTransitionTime` stays stable across reconciles).

The two signals are kept in sync by `setEnvPhase` / `setLoadTestPhase`, which stamp the aggregator `Ready` condition whenever they patch the phase.

### `Environment` phases

| Phase                    | Meaning                                                                                                                                                                                                       | Exit transitions                                                                                    |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| *empty* / `Idle`         | First observation; reconciler has not yet acted.                                                                                                                                                              | → `ProvisioningVMs` on next tick.                                                                   |
| `ProvisioningVMs`        | TCP-dials `<ip>:22` on every declared node to confirm SSH reachability (2 s timeout per host).                                                                                                                | → `ProvisioningInfra` once all reachable; requeue every 10 s otherwise.                             |
| `ProvisioningInfra`      | Two Ansible Jobs run in parallel: `<env>-infra-vms-job` (dfaas-worker setup) and `<env>-infra-k6-job` (k3s + k6-operator install + kubeconfig push-back).                                                     | → `ProvisioningMonitoring` on both `Succeeded`; → `Failed` if either reports `Failed` after fan-in. |
| `ProvisioningMonitoring` | Helm-installs Prometheus + Grafana on the management cluster, reconciles per-environment scrape targets, then surfaces `status.k6Nodes[]`.                                                                    | → `Ready` on success; → `Degraded` after `monitoringRetryBudget=5` consecutive Helm failures.       |
| `Ready`                  | Infrastructure + monitoring are up; `status.observedGeneration` is stamped, future ticks short-circuit.                                                                                                       | → `ProvisioningVMs` on spec edit (generation drift).                                                |
| `Degraded`               | dfaas + k6 infra are up but the monitoring stack failed terminally. LoadTests are **still permitted** — the exporter step will surface the missing metrics. Recoverable only by spec edit or delete+recreate. | → `ProvisioningVMs` on spec edit.                                                                   |
| `Failed`                 | Terminal failure in `ProvisioningInfra` (one of the Ansible Jobs reported `Failed`).                                                                                                                          | → `ProvisioningVMs` on spec edit; no automatic retry.                                               |

### `Environment` conditions

| Type                  | Purpose                                                                                                  | Reasons (status)                                                                                                                                                                                                                  |
| --------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Ready`               | Top-level aggregator. Lets consumers run `kubectl wait --for=condition=Ready environment/foo`.           | `AllSubsystemsReady` (True), `Provisioning` (False), `Degraded` (False), `Failed` (False), `Initializing` (Unknown).                                                                                                              |
| `VMsReady`            | SSH reachability probe outcome.                                                                          | `SSHReachable` (True), `SSHUnreachable` (False), `Skipped` (True, when `spec.nodes` is empty).                                                                                                                                    |
| `DfaasWorkersReady`   | dfaas-worker Ansible Job lifecycle.                                                                      | `NoWorkers` (True), `JobPending` (Unknown — Job not yet created), `AnsibleRunning` (False), `VMsProvisioned` (True), `AnsibleFailed` (False), `JobCreationFailed` (False — `r.Create(Job)` returned a non-`AlreadyExists` error). |
| `K6Ready`             | k6 Ansible Job lifecycle.                                                                                | Same shape as `DfaasWorkersReady`, swap `VMsProvisioned`→`K6Provisioned` and `NoWorkers`→`NoK6Nodes`.                                                                                                                             |
| `InfrastructureReady` | Roll-up over the two Ansible Jobs.                                                                       | `InfraReady` (True), `InfraFailed` (False).                                                                                                                                                                                       |
| `MonitoringReady`     | Helm install status of the monitoring stack.                                                             | `HelmInstalling` (Unknown on first observation, then False), `WaitingPods` (False), `PodsRunning` (True), `HelmFailed` (False — `monitoringRetryBudget` exhausted; phase moves to `Degraded`).                                    |
| `DependenciesReady`   | Catch-all for cross-cutting failures previously swallowed.                                               | `Libp2pKeyError` (False), `NodeStatusError` (False), `InfraReady` (True, set once libp2p keys exist).                                                                                                                             |
| `Updating`            | Set on generation drift; cleared when the new run settles. UI consumes this for the "Updating…" overlay. | `SpecChanged` (True), `AllSubsystemsReady` (False, cleared on terminal phase).                                                                                                                                                    |

### `LoadTest` phases

| Phase               | Meaning                                                                                                                                                                  | Exit transitions                                                                                               |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| *empty* / `Pending` | Awaiting Environment readiness, draft activation, or scheduled fire. `spec.suspended` / `spec.startAt` are still honoured here.                                          | → `Running` once `spec.suspended=false` AND env is `Ready` / `Degraded`.                                       |
| `Running`           | One remote `TestRun` per `spec.perNodeLoad[]` entry has been server-side-applied to each k6-load-generator's k3s. The reconciler polls every 5 s. Spec is now immutable. | → `Exporting` when every remote `TestRun.status.stage ∈ {finished, stopped}`; → `Failed` on any `stage=error`. |
| `Exporting`         | The in-cluster `<loadtest>-exporter-job` is pulling metrics from management Prometheus over `[startTime, endTime]` and writing the CSV to stdout / S3.                   | → `Completed` on Job success; → `Failed` on Job failure or missing `S3ConfigRef` Secret.                       |
| `Completed`         | Terminal success. `status.endTime` set, `MetricsExported=True/ExportSucceeded`.                                                                                          | none (CR kept as historical record).                                                                           |
| `Failed`            | Terminal failure. Set from any of: env NotFound, dispatch budget exhausted, k6 `stage=error`, exporter Job `Failed`, S3 config missing.                                  | none.                                                                                                          |
| `Aborted`           | Terminal abort. Set by `spec.stop=true` or by deletion finalizer. Remote `TestRun`s have been deleted; the CR is kept as an audit trail.                                 | none.                                                                                                          |

### `LoadTest` conditions

The original single `Ready` condition has been split into composable sub-conditions (see plan `controlla-i-due-reconcile-floating-deer.md`). `Ready` is now a pure roll-up — diagnostic detail lives on the sub-conditions.

| Type                | Purpose                                                                                                         | Reasons (status)                                                                                                                                                                                                                                                                                                                               |
| ------------------- | --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Ready`             | Top-level aggregator. `kubectl wait --for=condition=Ready loadtest/foo` resolves at the terminal phase.         | `Pending` (False), `Running` (False), `ExporterRunning` (False), `Completed` (True), `Failed` (False), `Aborted` (False).                                                                                                                                                                                                                      |
| `EnvironmentLinked` | Outcome of the `targetEnvironment` lookup.                                                                      | `EnvFound` (True), `EnvDegraded` (True, advisory — exporter step may fail), `EnvNotFound` (False), `EnvFailed` (False).                                                                                                                                                                                                                        |
| `Suspended`         | Mirrors `spec.suspended` (draft latch). Stamped only on the True→False edge to avoid steady-state status churn. | `DraftSaved` (True), `Activated` (False).                                                                                                                                                                                                                                                                                                      |
| `Scheduled`         | Lifecycle of `spec.startAt` (scheduled start). Only stamped when `spec.startAt` is set.                         | `ScheduledArmed` (True, fire time still in the future), `ScheduledFired` (True, schedule has fired — the reconciler PATCHed `suspended=false`), `ScheduledDelayedEnvNotReady` (True, fire time elapsed but the env is not ready yet).                                                                                                          |
| `K6Dispatched`      | Remote `TestRun` apply progress.                                                                                | `Pending` (Unknown — nothing applied yet), `InFlight` (False, message carries `N/M`), `AllDispatched` (True), `ScriptMirrorFailed` / `StaleCleanupFailed` / `ApplyFailed` (False — single-attempt sub-causes, retry budget bookkeeping in the message), `DispatchFailed` (False — `dispatchRetryBudget=5` exhausted; phase moves to `Failed`). |
| `K6Healthy`         | k6 execution roll-up. Per-node detail still in `status.testRuns[].phase`.                                       | `Running` (Unknown, message `N/M finished, K error, R running`), `AllFinished` (True), `PartialFailure` (False — some `stage=error`, rest finished), `AllFailed` (False).                                                                                                                                                                      |
| `MetricsExported`   | Exporter Job outcome.                                                                                           | `ExporterRunning` (Unknown), `ExportSucceeded` (True), `JobFailed` (False), `S3ConfigMissing` (False), `Skipped` (False — set on abort, no exporter ran).                                                                                                                                                                                      |
| `SpecLocked`        | Documents that `spec` mutations are silently ignored once the test starts.                                      | `Pending` (False, spec is editable), `PostStart` (True — set as soon as phase leaves the pre-execution window).                                                                                                                                                                                                                                |

> **Reading conditions.** The constants for both condition `type` and `reason` strings live in [`api/v1/environment_types.go`](api/v1/environment_types.go) and [`api/v1/loadtest_types.go`](api/v1/loadtest_types.go) (`EnvCond*` / `EnvReason*` / `LTCond*` / `LTReason*`). UI consumers should pin to these constants — string-literal matches against the values listed above are stable, but the underlying names may shift as the contract evolves.

## 🧩 Components

| Component                                             | Path                                     | Image                                                                        |
| ----------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| Operator (`Environment` + `LoadTest` controllers)     | `cmd/` + `internal/controller/`          | built from repo root `Dockerfile`                                            |
| Data exporter (Prometheus → CSV + optional S3 upload) | `dataExporter/`                          | `ghcr.io/isired01/dfaas-exporter:latest` (multi-arch, separate `Dockerfile`) |
| Ansible playbooks (worker + k6 provisioning)          | `internal/controller/ansible/templates/` | `alpine/ansible:2.18.6` (see note below)                                     |
| Monitoring stack (Prometheus + Grafana, Helm-managed) | `internal/controller/monitoring/charts/` | vendored `.tgz` chart bundles                                                |
| Front-end + API gateway (separate repo)               | `https://github.com/isired01/DFaaS_UI`   | —                                                                            |

## 🛠️ Requirements

- Kubernetes cluster ≥ v1.25 (tested on k3s + standard kubeadm)
- Go ≥ 1.25 for local development
- A Helm-compatible cluster (no extra installation needed — the operator drives Helm via the embedded SDK)
- Optional, for S3 export: an S3-compatible endpoint (AWS S3, MinIO, R2, Wasabi, etc.) and an access-key pair with `s3:HeadBucket`, `s3:CreateBucket`, `s3:PutObject` on the target account

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

## 📊 Metrics export

During the `Exporting` phase, the LoadTestReconciler creates `<loadtest>-exporter-job` running the `dfaas-exporter` image. It pulls metrics from the management-cluster Prometheus over `[startTime, endTime]` (entries come from `spec.metricsExport.metrics`) and writes them to a CSV.

The destination is decided on the **Environment**, not the LoadTest: every LoadTest targeting a given Environment exports to the same place. Set `Environment.spec.s3ConfigRef.name` to point at an S3 server configuration registered as a labeled Secret in the cluster-scoped `dfaas-s3` namespace.

| Configuration           | Behaviour                                                                                                                                          |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `s3ConfigRef` **unset** | CSV dumped to stdout between `----- BEGIN CSV -----` / `----- END CSV -----` markers. Retrievable with `kubectl logs job/<loadtest>-exporter-job`. |
| `s3ConfigRef` **set**   | Exporter `HeadBucket` → `CreateBucket` (when missing) → `PutObject` against the registered endpoint. Bucket name is derived from the Environment.  |

**Bucket per environment.** The exporter computes the bucket name deterministically from the Environment name + the first 6 hex chars of the Environment UID, yielding a name like `my-env-a1b2c3` that satisfies S3's 3–63 char DNS rule and avoids global-namespace collisions. The bucket is created on the first LoadTest export and reused for every subsequent one in that Environment. Object key per upload: `metrics/<loadtest-name>/<UTC RFC3339-compact>.csv`.

At LoadTest export time the operator mirrors the referenced Secret into the LoadTest namespace with an OwnerRef → LoadTest, so the local copy cascades on LoadTest delete (cross-namespace Secret mounts are not supported by Kubernetes — the mirror is required).

## ⚠️ Known limitations / pins
- **Balancing strategies — partial support.** Supported out of the box: `staticstrategy`, `alllocalstrategy`, `recalcstrategy`. The latter requires `Function.maxRate` (emitted as `dfaas.maxrate` OpenFaaS label). `nodemarginstrategy` and `rlagentstrategy` are not supported.

> [!IMPORTANT]
> Target VMs must be reachable via SSH from the cluster network and the credentials in `Environment.spec.nodes[].password` must be valid before applying the manifest. The operator does **not** distribute SSH keys for SSH login.
