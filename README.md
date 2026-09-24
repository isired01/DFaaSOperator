# DFaaS Experiment Controller

Kubebuilder-scaffolded Kubernetes operator (Go 1.25, controller-runtime v0.17.3) that orchestrates **DFaaS** (distributed FaaS) experiments end-to-end: from federation provisioning to k6 load tests and metrics export.

The operator ships as a single Deployment that runs **two controllers** against two CRDs in `dfaas.dfaas.io/v1`:

- **`Environment`** — federation infrastructure. Provisions VMs (dfaas-worker nodes + k6-load-generator nodes) and the operator-cluster monitoring stack. Long-lived: once `Ready` it re-reconciles on spec changes, and a periodic SSH liveness probe keeps watching the nodes — if they stop answering it drops to the non-terminal `Unreachable` phase and auto-recovers when they return.
- **`LoadTest`** — one k6 load test against an `Environment`. Looks up its `targetEnvironment` and requires it `Ready` (a draft or scheduled test may wait for it), dispatches one remote k6 `TestRun` per k6-load-generator node, then runs a metrics exporter Job.

## Install via Helm

Prereqs: K8s ≥ 1.25, `kubectl` connected to the cluster, `helm` ≥ 3.8 (for OCI), anonymous pull from `ghcr.io` reachable from the cluster.

### Step 1 — Install operator + UI via Helm

The chart packages the CRDs, so `helm install` applies them for you. (`helm upgrade` never does — see [Upgrade](#upgrade).)

```bash
helm install dfaas oci://ghcr.io/isired01/charts/dfaas \
  --version 3.5.0 \
  --create-namespace \
  --namespace dfaas-operator-system

# Alternatively, install the chart straight from a repo checkout. Chart.yaml ships
# appVersion 0.0.0, and every image tag defaults to it, so pin a real one:
# helm install dfaas ./charts/dfaas --create-namespace --namespace dfaas-operator-system \
#   --set operator.image.tag=3.5.0 \
#   --set operator.exporterImage.tag=3.5.0 \
#   --set ui.image.tag=3.5.0
```

The UI Service defaults to NodePort `30800`, so the control plane is reachable at
`http://<any-node-ip>:30800/` as soon as the pod is ready. To keep it inside the
cluster instead, install with `--set ui.service.type=ClusterIP` and reach it through
`kubectl -n dfaas-ui port-forward svc/dfaas-ui 8082:8082`.

### Step 2 — Create your first Custom Resources

Ready-made examples in [`config/samples/`](config/samples/):

- [`dfaas_v1_environment.yaml`](config/samples/dfaas_v1_environment.yaml) — Environment with dfaas-worker + k6-load-generator nodes
- [`dfaas_v1_loadtest.yaml`](config/samples/dfaas_v1_loadtest.yaml) — k6 LoadTest against the Environment above

**Before applying, configure the VM IPs** (the operator does NOT provision machines, it only orchestrates them). Edit `spec.nodes[].ipAddress` with the addresses of Ubuntu VMs that are already running and reachable over SSH:

```yaml
spec:
  nodes:
    - nodeID: dfaas-worker-1
      role: dfaas-worker
      capacity: MEDIUM
      ipAddress: 10.0.0.5      # <— replace with the real IP
      username: ubuntu
      password: ...
      # ...
    - nodeID: k6-load-generator-1
      role: k6-load-generator
      capacity: HIGH
      ipAddress: 10.0.0.6      # <— replace with the real IP
      username: ubuntu
      password: ...
      # ...
```

Then:

```bash
kubectl apply -f config/samples/dfaas_v1_environment.yaml
kubectl get environment -w   # wait for phase=Ready
kubectl apply -f config/samples/dfaas_v1_loadtest.yaml
```

### Override values

```bash
helm install dfaas oci://ghcr.io/isired01/charts/dfaas --version 3.5.0 \
  --create-namespace \
  --namespace dfaas-operator-system \
  --set operator.replicas=2 \
  --set ui.ingress.enabled=true \
  --set ui.ingress.host=dfaas.my-cluster.example
```

### Upgrade

```bash
# Helm applies CRDs on install but never on upgrade. Apply the target release's
# CRDs FIRST (replace <version> with it, e.g. 3.5.0):
kubectl apply -f https://github.com/isired01/DFaaSOperator/releases/download/v<version>/dfaas.dfaas.io_environments.yaml
kubectl apply -f https://github.com/isired01/DFaaSOperator/releases/download/v<version>/dfaas.dfaas.io_loadtests.yaml

# Then upgrade operator + UI:
helm upgrade dfaas oci://ghcr.io/isired01/charts/dfaas --version <version> \
  --namespace dfaas-operator-system
```

v3.5.0 adds `status.provisioningGeneration` to the `Environment` CRD. Upgrade the chart without applying the CRD first and the API server prunes that field from every status write: the operator keeps working, but an edit that lands while an Environment is provisioning is no longer detected, and the run settles as if it had installed the new spec.

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
| Environment CR            |        | LoadTest CR                 |
| --------------            |        | -----------                 |
| nodes: dfaas-worker, k6   |        | targetEnvironment: env-xyz  |
| topology (latency links)  |        | perNodeLoad[]               |
| openfaas functions        |        | metricsExport.metrics[]     |
+-------------+-------------+        +--------------+--------------+
              | reconcile                            | reconcile
              v                                      v
   +----------+----------+               +-----------+------------+
   |  Ansible Jobs (VMs) | ----------->  | Remote k6 TestRun via  |
   |  Helm (Prometheus,  |               | per-node k3s + k6-op   |
   |  Grafana, SeaweedFS |               +-----------+------------+
   |  on mgmt)           |                           |
   +----------+----------+                           |
              |                                      v
              v                              +-------+--------+
       Environment.Ready                     | Exporter Job   |
                                             | Prometheus →   |
                                             | CSV → S3 sink  |
                                             | (SeaweedFS by  |
                                             |  default)      |
                                             +----------------+
```

## 🏗️ State machines

### `Environment`
```
"" / Idle / Failed → ProvisioningVMs            → ProvisioningInfra                              → ProvisioningMonitoring → Ready ⇄ Unreachable
                     (SSH :22 reachability)       ├─ Ansible dfaas-worker Job ─┐                  (Helm install seq)         (periodic SSH probe)
                                                  └─ Ansible k6-load-gen Job  ─┘  parallel; fan-in
any phase ── spec edit (generation drift) ──→ ProvisioningVMs
```
`ProvisioningVMs` TCP-dials `<ip>:22` on every declared node to confirm SSH reachability (2 s timeout per host); it advances once all answer, and after `sshRetryBudget=3` consecutive unreachable rounds it drops to the non-terminal `Unreachable` phase (not `Failed`). `ProvisioningInfra` runs the two Ansible Jobs concurrently and advances only when **both** terminate; if either fails the surviving stream is still allowed to finish, then the phase becomes `Failed` with per-component `DFaaSNodesReady` / `K6Ready` Conditions for granular diagnosis. `ProvisioningMonitoring` is sequential: Helm install Prometheus, Grafana and SeaweedFS, then scrape-target reconcile.

Once `Ready` the Environment is **not idle**: `reconcileReadyHealth` re-runs the SSH probe every `healthCheckInterval` (60 s), stamps `status.lastHealthCheck` + the `NodesReachable` and `VMsReady` conditions, and after `healthRetryBudget=3` consecutive misses transitions to `Unreachable`. The probe is throttled on `status.lastHealthCheck`, confirmed against an **uncached** read (`APIReader`) — the informer cache lags the controller's own status write, and reading it stale let a second probe through milliseconds after the first, collapsing the retry budget that exists precisely to tolerate a transient blip. `Unreachable` is non-terminal — the reconciler re-probes every `unreachableRetryInterval` (30 s) indefinitely and auto-recovers to `Ready` (or resumes provisioning) when the nodes answer again.

Re-provisioning only runs when `metadata.generation` changes, and one drift rule covers every phase. A provisioning run records the generation it applies in `status.provisioningGeneration` when it enters `ProvisioningVMs`; a settle (`Ready` or `Failed`) copies it into `status.observedGeneration`. Settled phases compare `observedGeneration` with `metadata.generation`, every other phase compares `provisioningGeneration`, so an edit that lands mid-provisioning restarts the run instead of being stamped installed. On drift the reconciler deletes the older generation's Ansible Jobs (foreground propagation) and inventory Secrets, resets the conditions to `Unknown` with reason `Updating`, and moves back to `ProvisioningVMs`; the new run's `ProvisioningInfra` waits until the older Jobs are gone, so two generations of playbooks never run on the same machines. A finalizer (`dfaas.dfaas.io/environment-finalizer`) cleans the per-environment Prometheus scrape-target entries and the k6 kubeconfig Secrets on delete. A LoadTest is stamped with an OwnerReference to its target Environment on its first pass after admission (a scheduled test only when it fires), so **deleting an Environment cascade-deletes those LoadTests**. A test that ended before that point (failed at admission, or aborted before the stamp, e.g. while armed) carries no OwnerReference and outlives the Environment. The Environment finalizer first deletes, and waits for, the owned tests that have not ended or still hold an unreclaimed runner, so their own finalizers can still reach the remote k3s. `kubectl delete --cascade=foreground` defeats this ordering; the default background propagation keeps it. Ansible Jobs are named `<env>-infra-<vms|k6>-<uid8>-g<generation>-job` and auto-cleanup with `TTLSecondsAfterFinished=600` on success and `86400` (24h) on failure.

### `LoadTest`
```
""  (not yet admitted) → Pending   every test: draft, scheduled or plain, before anything remote happens
""                     → Failed    startAt without suspended  |  non-draft test on an env that is not Ready
Pending   ← Save as Draft (spec.suspended=true)  |  armed schedule  |  waiting for env Ready  |  queued behind a sibling  |  sync barrier
   on PATCH spec.suspended=false (or scheduled spec.startAt fires) AND the env is free:
Pending → Running → Exporting → Completed
   ↘ Failed  ↘ Failed  ↘ Failed
   spec.stop=true (before Exporting)  OR  kubectl delete (finalizer)  → Aborted   (remote TestRuns deleted)
```
Phase `""` means *not yet admitted*. The first pass runs the two create-time checks, then writes `Pending` for every test before anything remote happens. A LoadTest carrying `startAt` without `suspended` fails there, since it would otherwise start at once and ignore the time; so does a non-draft test whose Environment is not `Ready`. (A test an older operator left at `""` with TestRuns already recorded is admitted without the checks.) A LoadTest created with `spec.suspended: true` (used by the UI for "Save as Draft") skips the `Ready` check, so it may target an Environment that is still provisioning, and stays at `Pending` until the user PATCHes `spec.suspended: false`; the draft/waiting distinction is read from `spec.suspended` itself — there is **no** `Suspended` condition. `spec.startAt` schedules a future start, and **requires `spec.suspended: true`**: a scheduled test waits at `Pending`, like a draft, and the reconciler PATCHes `suspended=false` at fire time once the env is `Ready`. The reconciler watches the target `Environment`: a `Pending` test that has dispatched nothing waits for it (re-checked every 10 s, no status write) and resumes when it is `Ready`; one that already has TestRuns applied when the Environment stops being `Ready` is failed and its partial run reclaimed. While the Environment is being deleted, nothing new dispatches. Run-once: once the LoadTest enters `Running` or any later phase, further changes to `spec.suspended` are ignored.

**One test per Environment (FIFO queue).** `envOccupancyGate` serializes LoadTests sharing an `Environment`: while one is mid-dispatch with TestRuns applied, `Running` or `Exporting`, or has ended still holding an unreclaimed runner, newly-eligible siblings are held at `Pending` with `Queued=True` (reason `EnvBusy`, or `QueuedBehind` behind an older waiting sibling; oldest `creationTimestamp` first, re-checked every 10 s) and dispatched one at a time. The test that proceeds gets `Queued=False/Dispatching`.

**Synchronized start (`spec.syncStart`).** Each runner's generated script blocks in `setup()`, polling the `DFAAS_SYNC_URL` the operator injects into the runner. After `K6Dispatched=True` the test stays at `Pending` with `SyncReady=False/AwaitingRunners` until every TestRun reports stage `started`; the operator then publishes a GO object on the SeaweedFS filer (NodePort `30901`, which the k6 VMs must reach — override the address with `DFAAS_SYNC_PUBLIC_URL` via `operator.extraEnv`), stamps `SyncReady=True/GoPublished`, and only then sets `status.startTime` and `Running`. A generator that is no longer usable, a runner in stage `error`, a runner that finished before GO, or `syncWaitBudget` (5 min from `K6Dispatched=True`, which bounds the GO publish too) running out fails the test with `SyncReady=False/SyncTimeout`. Once GO is published, a retry finishes the dispatch without publishing it again. Scripts generated before the barrier existed ignore the URL and start unsynchronized.

**Run end, abort & deletion.** Every way a LoadTest ends — Completed, Failed, Aborted — goes through one run end, `endRun` ([loadtest_end.go](internal/controller/loadtest_end.go)). It re-reads the test uncached and does nothing if it is already terminal. If runners may still be live (dispatch began, the test is not in `Exporting`, and some `perNodeLoad` entry has not been seen `finished`/`stopped`/`error`), it makes one delete pass over every target generator. When dispatch began it also deletes the run's filer objects (the GO signal, the k6 summaries). The terminal phase and its Conditions land in one status write. PATCH `spec.stop=true` (honoured at `""`, `Pending` and `Running`) ends the run as `Aborted`, keeping the CR as an audit record. If the delete fails on a generator holding a TestRun of the run, the test still goes terminal with `K6Healthy=False (RunnersUnreclaimed)`, the Environment stays occupied (`Queued` reason `EnvBusy`) and the operator retries the delete every 30 s until it succeeds or the test is deleted. A finalizer (`dfaas.dfaas.io/loadtest-finalizer`) handles `kubectl delete`: a test that has not ended is ended as `Aborted` first, and a Completed or Failed record is never rewritten. If dispatch began, every generator is then polled until its TestRun is gone, deleting what is still there, for at most 2 min from the deletion timestamp. A test that never dispatched makes no remote call, and one whose Environment is already gone is released at once (the kubeconfig Secrets went with it).

## 📡 Phases & Conditions

The operator surfaces resource state through two complementary signals:

- **`status.phase`** — the FSM driver, a single string the reconciler switches on. Stable across reconciles and recognised by the UI for primary badge colouring.
- **`status.conditions[]`** — `kubernetes/community` API-conventions-style conditions. Each entry carries `type`, `status` (`True` / `False` / `Unknown`), a typed `reason` (machine-readable string from constants in [api/v1](api/v1)), and a `message` (human-readable, sanitized of timestamps / pod UIDs / request IDs so `LastTransitionTime` stays stable across reconciles).

The two signals are kept in sync by the status writer ([statuswriter](internal/controller/statuswriter/writer.go)), which stamps the aggregator `Ready` condition in the same write whenever it sets the phase.

### `Environment` phases

| Phase                    | Meaning                                                                                                                                                                                                       | Exit transitions                                                                                    |
| ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| *empty* / `Idle`         | First observation; reconciler has not yet acted.                                                                                                                                                              | → `ProvisioningVMs` on next tick.                                                                   |
| `ProvisioningVMs`        | The write that enters it records `status.provisioningGeneration`. TCP-dials `<ip>:22` on every declared node to confirm SSH reachability (2 s timeout per host).                                              | → `ProvisioningInfra` once all reachable; requeue every 10 s otherwise; → `Unreachable` after `sshRetryBudget=3`; restarts on spec edit. |
| `ProvisioningInfra`      | First waits until any older generation's Ansible Jobs are gone. Then two Ansible Jobs run in parallel: `<env>-infra-vms-<uid8>-g<gen>-job` (dfaas-worker setup) and `<env>-infra-k6-<uid8>-g<gen>-job` (k3s + k6-operator install + kubeconfig push-back). | → `ProvisioningMonitoring` on both `Succeeded`; → `Failed` if either reports `Failed` after fan-in; → `ProvisioningVMs` on spec edit. |
| `ProvisioningMonitoring` | Helm-installs Prometheus, Grafana and SeaweedFS on the management cluster, reconciles per-environment scrape targets, then surfaces `status.k6Nodes[]`.                                                  | → `Ready` on success; → `Failed` after `monitoringRetryBudget=5` consecutive Helm failures; → `ProvisioningVMs` on spec edit. |
| `Ready`                  | Infrastructure + monitoring are up; `status.observedGeneration` is set from `provisioningGeneration`. **Not idle** — a periodic SSH liveness probe runs (see `NodesReachable` below).                             | → `ProvisioningVMs` on spec edit (generation drift); → `Unreachable` after `healthRetryBudget=3` failed probe rounds. |
| `Unreachable`            | **Non-terminal.** Nodes stopped answering SSH (`:22`) — entered from `ProvisioningVMs` (after `sshRetryBudget=3`) or from `Ready` (after `healthRetryBudget=3`). The reconciler re-probes every 30 s indefinitely. | → `Ready` when the nodes answer again and infra is already up; → `ProvisioningInfra` otherwise; → `ProvisioningVMs` on spec edit. Never self-transitions to `Failed`. |
| `Failed`                 | Terminal failure in `ProvisioningInfra` (an Ansible Job reported `Failed`) or in `ProvisioningMonitoring` (Helm retry budget exhausted). Settles like `Ready`: `observedGeneration` is set from `provisioningGeneration`. | → `ProvisioningVMs` on spec edit; no automatic retry.                                               |

### `Environment` conditions

| Type                  | Purpose                                                                                                  | Reasons (status)                                                                                                                                                                                                                  |
| --------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Ready`               | Top-level aggregator. Lets consumers run `kubectl wait --for=condition=Ready environment/foo`.           | `AllSubsystemsReady` (True), `Provisioning` (False), `SSHUnreachable` (False — set for the `Unreachable` phase), `Failed` (False), `Initializing` (Unknown).                                                                       |
| `VMsReady`            | SSH reachability probe outcome. Kept in step with `NodesReachable` by the Ready-state health loop, so the two never contradict each other. | `SSHReachable` (True), `SSHUnreachable` (False), `Skipped` (True — code path for an empty `spec.nodes`, unreachable since the CRD's `MinItems=1` rejects one).                                                                    |
| `DFaaSNodesReady`     | dfaas-worker Ansible Job lifecycle.                                                                      | `NoWorkers` (True), `JobPending` (Unknown — Job not yet created, or an older generation's Job still terminating), `AnsibleRunning` (False), `VMsProvisioned` (True), `AnsibleFailed` (False), `JobCreationFailed` (False — building the Job, or creating it or its inventory Secret, failed with a non-`AlreadyExists` error; retried with controller-runtime backoff, without a bound). |
| `K6Ready`             | k6 Ansible Job lifecycle.                                                                                | Same shape as `DFaaSNodesReady`, swap `VMsProvisioned`→`K6Provisioned` and `NoWorkers`→`NoK6Nodes`.                                                                                                                               |
| `InfrastructureReady` | Roll-up over the two Ansible Jobs.                                                                       | `InfraReady` (True), `InfraFailed` (False).                                                                                                                                                                                       |
| `MonitoringReady`     | Helm install status of the monitoring stack.                                                             | `HelmInstalling` (Unknown on first observation, then False), `WaitingPods` (False), `CheckFailed` (Unknown — readiness could not be evaluated, e.g. the Pod List was refused; retried every 10 s), `PodsRunning` (True), `HelmFailed` (False — `monitoringRetryBudget` exhausted; phase moves to `Failed`). |
| `NodesReachable`      | Outcome of the periodic Ready-state SSH (`:22`) liveness probe; also stamps `status.lastHealthCheck`.    | `SSHReachable` (True), `SSHUnreachable` (False — after `healthRetryBudget=3` misses the phase moves to `Unreachable`).                                                                                                            |

> On generation drift, in any phase, every condition above (`Ready` / `VMsReady` / `DFaaSNodesReady` / `K6Ready` / `InfrastructureReady` / `MonitoringReady` / `NodesReachable`) is reset to `Unknown` with reason `Updating`; `Ready` is then restamped `False/Provisioning` as the phase moves back to `ProvisioningVMs`. `Updating` is a *reason* constant, not a condition type — the UI derives its "Updating…" overlay from `observedGeneration < generation`, not from a dedicated condition.

### `LoadTest` phases

| Phase               | Meaning                                                                                                                                                                  | Exit transitions                                                                                               |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| *empty*             | Not yet admitted. The first pass runs the create-time checks and writes `Pending`; nothing remote happens at `""`.                                                       | → `Pending` for every test that passes; → `Failed` on `startAt` without `suspended`, a non-draft test on an Environment that is not `Ready`, or env NotFound. |
| `Pending`           | Awaiting Environment readiness, draft activation, scheduled fire, its turn in the per-Environment queue, or the sync barrier. Dispatch runs here. `spec.suspended` / `spec.startAt` are still honoured. | → `Running` once `spec.suspended=false`, env is `Ready`, the queue front is free and every TestRun is applied (with `syncStart`, once GO is published); → `Failed` on dispatch budget exhausted, a sync barrier failure, an Environment no longer `Ready` after TestRuns were applied, env NotFound, a `perNodeLoad` nodeID with no usable generator in the Environment, or `syncStart` with no VM-facing GO URL. |
| `Running`           | One remote `TestRun` per `spec.perNodeLoad[]` entry has been server-side-applied to each k6-load-generator's k3s. The reconciler polls every generator once per round, every 5 s (10 s after a failed round). Changes to `spec.suspended` are ignored; `spec.stop` still aborts. | → `Exporting` when every remote `TestRun.status.stage ∈ {finished, stopped}`; → `Failed` once every runner has ended and any reported `stage=error`, when a generator becomes unusable, after `fetchRetryBudget=15` consecutive failed rounds, or on env NotFound. |
| `Exporting`         | After a 30 s cool-down the in-cluster `<lt>-exporter-<uid8>-g<generation>-job` pulls metrics from management Prometheus over `[startTime, endTime + 15 s]` and writes the CSV to the Environment's S3 sink (the in-cluster `seaweedfs-default` sink unless `spec.s3ConfigRef` overrides it). `spec.stop` is no longer honoured. | → `Completed` on Job success; → `Failed` on Job failure, a missing explicit `S3ConfigRef` Secret, or env NotFound; → `Aborted` only through deletion. |
| `Completed`         | Terminal success. `status.endTime` set, `MetricsExported=True/ExportSucceeded`.                                                                                          | none (CR kept as historical record).                                                                           |
| `Failed`            | Terminal failure. Set from any of: the admission checks, env NotFound, an Environment no longer `Ready` after TestRuns were applied, dispatch budget exhausted, `syncStart` with no VM-facing GO URL, a sync barrier failure, an unusable generator (at dispatch or later), observe budget exhausted, k6 `stage=error`, exporter Job `Failed`, explicit S3 config missing. | none.                                                                                                          |
| `Aborted`           | Terminal abort. Set by `spec.stop=true` or by the deletion finalizer. The run end deletes the remote `TestRun`s when a runner may still be live (`K6Healthy` says whether every generator confirmed it), and on deletion the finalizer then removes any left; with `spec.stop` the CR is kept as an audit trail. | none.                                                                                                          |

### `LoadTest` conditions

The original single `Ready` condition has been split into composable sub-conditions. `Ready` is now a pure roll-up — diagnostic detail lives on the sub-conditions.

| Type                | Purpose                                                                                                         | Reasons (status)                                                                                                                                                                                                                                                                                                                               |
| ------------------- | --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Ready`             | Top-level aggregator. `kubectl wait --for=condition=Ready loadtest/foo` resolves only on `Completed`; the message of a failed or aborted test carries the diagnosis. | `Pending` (False), `Running` (False), `ExporterRunning` (False), `Completed` (True), `Failed` (False), `DispatchFailed` (False — dispatch budget exhausted), `S3ConfigMissing` (False), `UserAborted` (False — every abort, `spec.stop` or deletion).                                                                                     |
| `EnvironmentLinked` | Outcome of the `targetEnvironment` lookup.                                                                      | `EnvFound` (True), `EnvNotFound` (False — the test fails), `EnvFailed` (False — informational; the test itself fails only at admission or mid-dispatch).                                                                                                                             |
| `Scheduled`         | Lifecycle of `spec.startAt` (scheduled start). Only stamped when `spec.startAt` is set.                         | `ScheduledArmed` (True, fire time still in the future), `ScheduledFired` (True, schedule has fired — the reconciler PATCHed `suspended=false`), `ScheduledDelayedEnvNotReady` (True, fire time elapsed but the env is not ready yet).                                                                                                          |
| `Queued`            | Per-Environment FIFO gate, evaluated on a non-draft test before it dispatches.                                  | `EnvBusy` (True — a sibling is dispatching, `Running`/`Exporting`, or ended holding an unreclaimed runner), `QueuedBehind` (True — an older waiting sibling is ahead), `Dispatching` (False — this LoadTest is the queue front and may start, or resumes its own in-flight dispatch). |
| `K6Dispatched`      | Remote `TestRun` apply progress.                                                                                | `Pending` (Unknown — nothing applied yet), `InFlight` (False, message carries `N/M`), `AllDispatched` (True), `ScriptMirrorFailed` / `StaleCleanupFailed` / `ApplyFailed` (False — single-attempt sub-causes, retry budget bookkeeping in the message), `DispatchFailed` (False — `dispatchRetryBudget=15` exhausted; phase moves to `Failed`). A test that ends while this reads `Pending`/`InFlight` gets `False` with its end reason and the applied count. Observe failures do not touch this Condition: they are counted on their own `fetch-misses` counter (15 rounds, one charge per round however many generators missed) and shown on `K6Healthy` as `FetchFailed` (Unknown). Every failed remote round is paced 10 s apart. |
| `K6Healthy`         | k6 execution roll-up. Per-node detail still in `status.testRuns[].phase`.                                       | `Running` (Unknown, message `N/M finished, K error, R running`), `FetchFailed` (Unknown — some generator did not report its stage this round, message `attempt n/15`), `AllFinished` (True), `PartialFailure` (False — some `stage=error`, rest finished), `AllFailed` (False), `RunnersReclaimed` (False — restamped at run end once every target generator confirmed its TestRun deleted, so the condition stops reporting live runners on a test that has none), `RunnersUnreclaimed` (False — the delete failed on a generator holding a TestRun of this run; the Environment stays occupied and the delete is retried). When no Delete failed on a generator holding a TestRun but some generator could not be confirmed (unusable, Environment gone, or a failed Delete where no TestRun was recorded), the run end stamps the run's own reason and names it. |
| `MetricsExported`   | Exporter Job outcome.                                                                                           | `ExportCooldown` (Unknown — the 30 s wait after k6 finishes, with a countdown), `ExporterRunning` (Unknown), `ExportSucceeded` (True), `JobFailed` (False), `S3ConfigMissing` (False — an explicit `s3ConfigRef` Secret is missing; the test fails. A missing *default* sink is not stamped: the export falls back to the Job's stdout and the `ExporterRunning` message says so), `Skipped` (False — the test ended before its export, no exporter ran).                                                                                                                                                                                      |
| `SyncReady`         | Synchronized-start barrier. Only stamped when `spec.syncStart` is true.                                         | `AwaitingRunners` (False — `n/m` runners started, GO held), `GoPublished` (True), `SyncTimeout` (False — the barrier failed and the test with it; also stamped when the Environment stops being `Ready` after TestRuns were applied). A test that ends while `AwaitingRunners` gets `False` with its end reason. |

> The draft latch (`spec.suspended`) and the post-start spec lock are **not** surfaced as conditions — there is no `Suspended` or `SpecLocked` condition type. Read the draft state from `spec.suspended` directly; the run-once immutability is enforced in the reconciler. (`DraftSaved`, `Activated` and `PostStart` were dead constants and have been removed; [api/v1/inventory_test.go](api/v1/inventory_test.go) fails on any reason constant that nothing stamps.)

> **Reading conditions.** The constants for both condition `type` and `reason` strings live in [`api/v1/environment_types.go`](api/v1/environment_types.go) and [`api/v1/loadtest_types.go`](api/v1/loadtest_types.go) (`EnvCond*` / `EnvReason*` / `LTCond*` / `LTReason*`). UI consumers should pin to these constants — string-literal matches against the values listed above are stable, but the underlying names may shift as the contract evolves.

### Resource naming — the 63-byte trap

Ansible Jobs and the LoadTest exporter Job embed `UID[:8]` + `generation` so a delete+recreate or a
spec edit never recovers a stale Job. Both names go through `ansible.BoundedJobName`, which caps the
result at **63 bytes** by truncating the name prefix and keeping that suffix.

The cap is not cosmetic. With no explicit selector the Job controller copies a Job's *name* into the
auto-generated `job-name` pod-template label, and label values are limited to 63 bytes while object
names allow 253. An over-long name is rejected at CREATE:

```
spec.template.labels: Invalid value: "…": must be no more than 63 bytes
```

and the reconciler retries it forever — no terminal state, no event, just a LoadTest wedged in
`Exporting`. A 41-character LoadTest name was enough to trigger it.

The remote k6 TestRun is named `<loadtest>-<sanitized nodeID>` (`k6dispatch.TestRunName`), and
k6-operator derives its own Job names from it; the longest, `<testrun>-initializer`, hits the same
label limit. So the TestRun name is capped at **51 bytes**: a longer one keeps its head and ends in
an 8-hex-digit hash of the full name, and a shorter one is left exactly as before. Past that bound
the initializer CREATE fails on the generator and the TestRun waits for its pod forever, so the
LoadTest stays `Running` (or fails with `SyncTimeout` after 5 min under `syncStart`). The 51 comes
from k6-operator v0.0.15, the version the k6 playbook pins (chart `3.7.0`); a test fails if the pin
moves.

Upgrading from v3.5.0: a test whose TestRun name was over 51 bytes and that has not ended at the
upgrade fails once the new operator runs: a plain test after about 150 s with "TestRun not found", a
`syncStart` test with `SyncTimeout` at its original 5-minute deadline. It keeps the old name in
`status.testRuns`. Its `K6Healthy` Condition then reads `RunnersReclaimed`, which is wrong: the run
end looked only for the new name, so the old TestRun is still on the generator. It has no pods and
loads nothing, and deleting the LoadTest does not remove it. Find it on the generator with
`kubectl -n default get testrun -l dfaas.io/loadtest-name=<lt>` and delete it.

## 🧩 Components

| Component                                             | Path                                     | Image                                                                        |
| ----------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| Operator (`Environment` + `LoadTest` controllers)     | `cmd/` + `internal/controller/`          | built from repo root `Dockerfile`                                            |
| Data exporter (Prometheus → CSV + optional S3 upload) | `dataExporter/`                          | `ghcr.io/isired01/dfaas-exporter:latest` (multi-arch, separate `Dockerfile`) |
| Ansible playbooks (worker + k6 provisioning)          | `internal/controller/ansible/templates/` | `alpine/ansible:2.18.6`                                                      |
| Monitoring stack (Prometheus + Grafana + SeaweedFS, Helm-managed) | `internal/controller/monitoring/charts/` | vendored `.tgz` chart bundles                                    |
| Front-end + API gateway (separate repo)               | `https://github.com/isired01/UI`         | —                                                                            |

## 🛠️ Requirements

- Kubernetes cluster ≥ v1.25 (tested on k3s + standard kubeadm)
- Go ≥ 1.25 for local development
- A Helm-compatible cluster (no extra installation needed — the operator drives Helm via the embedded SDK)
- Optional, for S3 export: an S3-compatible endpoint (AWS S3, SeaweedFS, R2, Wasabi, etc.) and an access-key pair with `s3:HeadBucket`, `s3:CreateBucket`, `s3:PutObject` on the target account

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

During the `Exporting` phase, the LoadTestReconciler creates `<lt>-exporter-<uid8>-g<generation>-job` running the `dfaas-exporter` image. It pulls metrics from the management-cluster Prometheus over `[startTime, endTime + 15 s]` (entries come from `spec.metricsExport.metrics`) and writes three CSVs.

**Export cool-down — 30 s.** Entering `Exporting` does not create the Job immediately: the reconciler holds until 30 s have elapsed since `status.endTime`, reporting `MetricsExported=Unknown (reason=ExportCooldown)` with a countdown while it waits. This is not padding. The management Prometheus does not scrape the workers directly — it **federates** from each worker's own Prometheus every `server.global.scrape_interval`, set to **15 s** in [values/prometheus-values.yaml](internal/controller/monitoring/values/prometheus-values.yaml) (the per-VM Prometheus scrapes at 5 s, but that is only the first hop). Each node is pulled at its own offset, so the last sample inside the k6 window can be up to one interval older than `endTime`, by a different amount per node and per run — which silently makes the tails of two otherwise-identical runs incomparable.

The query window **is** extended: `END_TIME` is `status.endTime` plus `exportTailWindow` (one interval, 15 s), so every node has a sample at or after the k6 finish. The cool-down is `exportCooldown = federationInterval + exportTailWindow`: one interval for the last pull inside the window, one for the pull that lands after `endTime`. The deadline is computed from the persisted `status.endTime`, so an operator restart mid-cool-down resumes the original deadline instead of starting a new one. The constants are in [internal/controller/loadtest_observe.go](internal/controller/loadtest_observe.go), and `federation_interval_test.go` fails when `federationInterval` and the values file disagree.

The destination is decided on the **Environment**, not the LoadTest: every LoadTest targeting a given Environment exports to the same place. Point `Environment.spec.s3ConfigRef.name` at an S3 server configuration registered as a labeled Secret in the cluster-scoped `dfaas-s3` namespace; leave it unset to use the built-in in-cluster SeaweedFS sink.

| Configuration           | Behaviour                                                                                                                                                                                                                     |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `s3ConfigRef` **unset** | Exports to the built-in `seaweedfs-default` config (in-cluster SeaweedFS) — **zero S3 setup required**. Only if that default Secret has been deleted does the exporter fall back to a stdout dump between `----- BEGIN CSV -----` / `----- END CSV -----` markers; the `ExporterRunning` message says so. |
| `s3ConfigRef` **set**   | Exporter `HeadBucket` → `CreateBucket` (when missing) → `PutObject` against the registered endpoint. Bucket name is derived from the Environment. A **missing** explicitly-referenced Secret fails the LoadTest (loud, since it was asked for).                    |

**Bucket per environment.** The exporter computes the bucket name deterministically from the Environment name + the first 6 hex chars of the Environment UID, yielding a name like `my-env-a1b2c3` that satisfies S3's 3–63 char DNS rule and avoids global-namespace collisions. The bucket is created on the first LoadTest export and reused for every subsequent one in that Environment.

**What one export writes.** Five objects, all stamped with the same `<stamp>` — `END_TIME`, the end of the query window, in compact UTC, not the clock — so a retried Job overwrites its own objects and the files of one run pair by name:

| Key | Contents |
| --- | --- |
| `metrics/<loadtest>/<stamp>.csv` | one row per (series, sample); **every Prometheus label is its own column**, the union across the run, empty where a series lacks it |
| `metrics/<loadtest>/query-status-<stamp>.csv` | one row per entry of `spec.metricsExport.metrics`: series and samples returned, Prometheus warnings, error. A query that matches nothing is otherwise invisible |
| `k6/<loadtest>/summary-<stamp>.csv` | the k6 end-of-test summaries, long: `node_id, metric, metric_type, stat, value`, plus one `dfaas_summary_fetched` row per expected Generator (`1`/`0` and the failure reason) |
| `k6/<loadtest>/<nodeID>-summary-<stamp>.json` | each Generator's raw `handleSummary` JSON, verbatim |
| `k6/<loadtest>/<nodeID>-<stamp>.log` | each k6 runner's captured output |

Only the metrics CSV is fatal: it fails the Job when every query came back empty, and the query-status CSV is uploaded first so that failure still leaves the diagnosis behind. Everything k6 warns and continues — the summary CSV is written even with no Generators at all, header only. With no S3 sink each object is dumped between its own stdout markers instead.

At LoadTest export time the operator mirrors the referenced Secret into the LoadTest namespace (cross-namespace Secret mounts are not supported by Kubernetes — the mirror is required). The mirror carries **no OwnerRef**: it is a shared, reusable artifact that any LoadTest against any Environment may consume, so it is not cascade-deleted with a single LoadTest. Orphans are labelled `dfaas.io/s3-config=true` for later cleanup.

## 🖼️ Image payloads for k6 (in-cluster SeaweedFS + `dfaas-imgproc`)

LoadTest scenarios can POST an uploaded image as the request body. The image is stored in the operator-deployed in-cluster SeaweedFS (the `monitoring` namespace, surfaced as the `seaweedfs-default` S3 config) and fetched by the generated k6 script **once** per test (in k6 `setup()`, base64-encoded), then each VU POSTs the raw bytes to a function on a DFaaS node. A ready image-processing target — `dfaas-imgproc` (grayscale + thumbnail, of-watchdog http mode, multi-arch) — ships in [imageFunction/](imageFunction/README.md).

**Use a small image.** It is copied into k6 runner memory and POSTed in full on every request, so a multi-MB payload saturates SeaweedFS and the DFaaS node under load (`unknown format` from failed fetches, `500/504` from a saturated node). The asset URL must be reachable **from the k6 VMs**: the UI gateway builds it from a cluster node's address and the SeaweedFS S3 API NodePort `30900`; set `SEAWEEDFS_PUBLIC_URL` on the gateway when the k6 nodes route to that NodePort through another address. The function is invoked through the DFaaS node's HAProxy entrypoint `http://<dfaas-node-ip>:30080/function/<name>`.

## ✅ What the API server rejects

These are enforced by the CRD schema at `kubectl apply` time — no admission webhook involved. Applying the regenerated CRDs (`make install`) is what activates them.

- **Duplicate `ipAddress`** within one Environment's `spec.nodes[]` — one machine is one node. Two entries on the same box get two libp2p identities: the Ansible run installs dfaas-agent twice with different keys and the last one to land leaves every peer dialling a dead peer ID, while the Environment stays `Ready` (the liveness probe only checks `:22`). Enforced by a CEL rule, which is also why `spec.nodes[]` is capped at **50 items** and `ipAddress` at **45 characters** — an unbounded array or string blows the CRD's validation cost budget and the API server rejects the CRD itself. *Scope is a single Environment*: nothing yet stops two different Environments from declaring the same machine.
- **Duplicate `nodeID`** within `spec.nodes[]` or `spec.perNodeLoad[]` (`listType=map`, `listMapKey=nodeID`). `nodeID` keys both the node's libp2p identity and its kubeconfig Secret, so duplicates used to silently collapse two VMs into one — a LoadTest "across N generators" would quietly hit fewer machines than configured. **Existing CRs carrying duplicate nodeIDs will start failing on update once the new CRDs are applied** — intended.
- **`execTimeout` / `maxInflight` / `timeoutMs` below 1** (`Minimum=1`, matching `maxRate`). `execTimeout: 0` previously deployed as `exec_timeout: "0s"`, corrupting the timing the load test exists to measure.
- **A `dfaas-worker` with no `functions`, or a function name outside `^[a-z0-9]+$`.** A worker with no function serves nothing and breaks the playbook's prune task; a hyphen in the name makes HAProxy reject the whole rendered config, so every worker serves 503 while the Environment stays `Ready`.
- **Malformed durations** in `perNodeLoad[].duration` and `metricsExport.step` — must match Go's duration grammar (`30s`, `5m`, `1h30m`; ASCII units only, no `µs`). `"5 minutes"` used to be accepted and fail much later inside the exporter.

## ⚠️ Known limitations / pins
- **Balancing strategies — partial support.** Supported out of the box: `staticstrategy`, `alllocalstrategy`, `recalcstrategy`, `randomstrategy`. `recalcstrategy` requires `Function.maxRate` (emitted as `dfaas.maxrate` OpenFaaS label). `nodemarginstrategy` and `rlagentstrategy` are not supported.
- **Unbounded retries and leftovers.** Before applying a TestRun the operator waits for a stale one of the same name to disappear from the generator, re-checking every 3 s with no limit. An Ansible Job or inventory Secret the API server refuses to create (`JobCreationFailed`) is retried with controller-runtime backoff, also with no limit. The 10 s pacing between failed remote rounds is kept in memory, so after an operator restart the next round can come early. Script ConfigMaps mirrored onto the generators' k3s are never deleted.

> [!IMPORTANT]
> Target VMs must be reachable via SSH from the cluster network and the credentials in `Environment.spec.nodes[].password` must be valid before applying the manifest. The operator does **not** distribute SSH keys for SSH login.

## 🖥️ Target VM baseline

The operator orchestrates pre-existing VMs; it does not create them. Each declared node must be a Linux host (tested on Ubuntu) that, before you apply the `Environment`:

- runs an **SSH server reachable on `:22`** from the operator cluster, with **password authentication enabled** (the operator authenticates with `spec.nodes[].username` / `password` — it does not distribute keys);
- has the login user set up for **passwordless `sudo`** (provisioning installs k3s / Helm / packages as root);
- has **Python 3** available (required by the Ansible playbooks);
- has a correct clock (NTP) — `apt` rejects `Release` files dated in the future.

The [`cloud-init/`](cloud-init/) directory contains an **example** local-dev setup (Multipass on macOS): [`dfaas-config.yaml`](cloud-init/dfaas-config.yaml) is a reference cloud-init that provisions exactly this baseline (a sudo user with password auth), and [`reset-script.sh`](cloud-init/reset-script.sh) recreates the author's test VMs. Both are machine-specific examples — adapt the hostnames, IPs, image tag, and credentials to your environment; they are not consumed by the operator.
