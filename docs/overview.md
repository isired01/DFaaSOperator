# Overview

## What the system is for

The system runs load tests against a federation of [DFaaS](https://github.com/unimib-datAI/dfaas) nodes and collects the measurements. DFaaS is a research system in which several nodes, each running OpenFaaS, share the load of their functions through a peer-to-peer agent, the dfaas-agent. Measuring such a federation by hand means provisioning several machines, installing the same software stack on each, wiring the peer mesh, starting a monitoring stack, installing a load generator on separate machines, starting the generators at the same moment, and collecting the results from everywhere afterwards. Each step is a chance for two runs that should be identical to differ.

This system turns that into two declarative Kubernetes resources, both in the API group `dfaas.dfaas.io/v1`:

- **`Environment`**: the infrastructure. A list of existing machines with a role each, the functions to deploy on the DFaaS nodes, and optionally the S3 sink for the results. It is long-lived: provision once, run many tests.
- **`LoadTest`**: one k6 test against one `Ready` Environment. It names the generators, the k6 script of each, and the Prometheus metrics to export.

The operator creates no VMs. The machines must exist, accept SSH with a password, and be dedicated to this use ([README](../README.md#requirements)). The operator installs software on them, runs the tests, and exports the results.

If the load generator shared a machine with the system under test, its own CPU use would contaminate the measurement. Generators are therefore separate machines with a role of their own.

## The two repositories

| Repository | What it is |
| --- | --- |
| [DFaaSOperator](https://github.com/isired01/DFaaSOperator) (this one) | The Kubernetes operator (Kubebuilder, Go), the two CRDs, the exporter, the load-test function `dfaas-imgproc`, and the Helm chart `charts/dfaas` that installs both halves. |
| [DFaaS_UI](https://github.com/isired01/DFaaS_UI) | A single Go (Gin) binary that serves a REST gateway over the CRDs and a React single-page app. The UI writes no state of its own: everything it does is also possible with `kubectl apply` on the two CRDs. |

The two are coupled only through the CRDs and a handful of conventions, all listed in [cross-repo-contract.md](cross-repo-contract.md). The gateway does not depend on the operator's Go types.

## Components and machines

```text
                  management cluster
  +------------------------------------------------------------------+
  |  UI gateway + SPA --> Kubernetes API <--> operator                |
  |                                            |   |                  |
  |                                  Ansible Jobs   exporter Job      |
  |  Prometheus (federates the DFaaS nodes) <-----------+             |
  |  Grafana           SeaweedFS (S3 :30900, filer :30901)            |
  +----------+-------------------------------+-----------------------+
             | SSH :22 (provisioning)        | k3s API :6443 (tests)
             v                               v
   DFaaS node(s)                       k6 generator(s)
   k3s, OpenFaaS, HAProxy,             k3s, k6-operator,
   dfaas-agent, Prometheus   <-- HTTP load --  k6 runner pods
        |      ^
        +------+  libp2p mesh between DFaaS nodes
```

**The management cluster** is an existing Kubernetes cluster with a default StorageClass. It runs the operator, the UI, and the monitoring stack that the operator installs: Prometheus, Grafana, and SeaweedFS (the default S3 sink). It never generates load. Every custom resource lives here.

**DFaaS nodes** (role `dfaas-worker`) are the system under test. Each runs its own single-node k3s, OpenFaaS with your functions, HAProxy as the entrypoint to the functions (`/function/<name>` on NodePort 30080), the dfaas-agent that joins the libp2p mesh and decides where each request is served, and its own Prometheus. The management Prometheus pulls ("federates") each node's metrics every 10 s.

**k6 generators** (role `k6-load-generator`) produce the load. Each runs its own single-node k3s and k6-operator. The operator reaches a generator's Kubernetes API directly on port 6443 with a kubeconfig that provisioning stores as a Secret, and applies a `k6.io/v1alpha1 TestRun` there. Nothing runs on a generator over SSH at test time.

SSH is used only by the Ansible Jobs, which run in the management cluster as Kubernetes Jobs and provision each machine with a playbook.

The role names are kebab-case enums (`dfaas-worker`, `k6-load-generator`). In prose a `dfaas-worker` is a "DFaaS node" ([glossary.md](glossary.md)).

## One experiment, end to end

1. **Declare the Environment.** You apply an `Environment` with the machines, each with `nodeID`, `ipAddress`, SSH `username` and `password`, `role` and `capacity`; the functions and the `balancingStrategy` of each DFaaS node; and optionally `s3ConfigRef` and `topology` (latency links between nodes, declared but not applied: see [known-limitations.md](known-limitations.md#spectopology-is-not-applied)). The order of the DFaaS nodes matters: each one bootstraps its peer list from the nodes listed before it, and that list is the whole mesh.
2. **Provisioning.** The operator moves the Environment through `ProvisioningVMs` (can every machine be reached on port 22), `ProvisioningInfra` (two Ansible Jobs in parallel, one per role), and `ProvisioningMonitoring` (Prometheus, Grafana and SeaweedFS with Helm, the scrape targets, the status of each generator), then to `Ready`. This takes several minutes. If Ansible fails, the Condition message names the task. Once `Ready` the Environment stays watched: a node that stops answering SSH moves it to `Unreachable`, and it returns to `Ready` by itself when the node answers again. A spec edit re-provisions from the start. Details: [architecture.md](architecture.md#environment).
3. **Declare the LoadTest.** You apply a `LoadTest` with `targetEnvironment`, one `perNodeLoad` entry per generator (the `nodeID`, and a ConfigMap holding the k6 script `script.js`), and `metricsExport.metrics`, the PromQL queries to export, each with a column name. A draft (`suspended: true`) or a scheduled test (`startAt`) waits; otherwise it starts at once. One test runs per Environment at a time; the others queue in creation order.
4. **Dispatch.** For each generator the operator copies the script ConfigMap onto the generator's k3s, applies a `TestRun` with the runner environment (`DFAAS_SUMMARY_URL`, and `DFAAS_SYNC_URL` and `DFAAS_ASSET_BASE` when they apply), and records it. With `syncStart` it then waits until every runner has started and publishes a GO object on the SeaweedFS filer that releases the barrier in each script's `setup()`. The test is `Running`.
5. **Run.** k6 runs on the generators and loads the DFaaS nodes through HAProxy. The operator polls every generator's TestRun stage every 5 s. Each generated script ends with `handleSummary`, which PUTs the k6 end-of-test summary to the filer. When every TestRun is done, the operator captures each runner's log and moves the test to `Exporting`. If any runner reports `error`, the test fails whole, with no export.
6. **Export.** After a fixed one-minute cool-down (so that the management Prometheus has federated the last samples), the operator runs an exporter Job. It queries the management Prometheus over the test window, fetches the k6 summaries from the filer, and writes CSV files to the S3 sink. When the Job succeeds the test is `Completed`.
7. **Results.** The files are in the S3 sink and the test stays as a record.

## Where the results end up

The destination is decided on the Environment, not the LoadTest: every test on an Environment exports to the same place. With no `spec.s3ConfigRef`, that is the in-cluster SeaweedFS (the config `seaweedfs-default`), and no S3 setup is needed. With an external config, the exporter creates the bucket if missing and writes there.

The bucket is `<environment name>-<first 6 characters of the Environment UID>` (lowercased, at most 63 characters), created on the first export and reused. One export writes five kinds of object, all with the same `<stamp>`, the end of the query window in compact UTC (`20260930T154500Z`), so a retried export overwrites its own files:

| Key | Contents |
| --- | --- |
| `metrics/<loadtest>/<stamp>.csv` | One row per (series, sample). Fixed columns `timestamp, node_id, value, loadtest, query_name, query_type, query_expr, query_comment`, then one column per Prometheus label. |
| `metrics/<loadtest>/query-status-<stamp>.csv` | One row per configured metric: series and samples returned, warnings, error. A query that matched nothing shows here. |
| `k6/<loadtest>/summary-<stamp>.csv` | The k6 end-of-test summaries in long format: `loadtest, environment, window_start, window_end, node_id, metric, metric_type, stat, value, note`, with one `dfaas_summary_fetched` row per generator (`1` or `0`, and the reason). |
| `k6/<loadtest>/<nodeID>-summary-<stamp>.json` | Each generator's raw `handleSummary` JSON. |
| `k6/<loadtest>/<nodeID>-<stamp>.log` | Each runner's captured log. |

Browse the default sink with the SeaweedFS filer at `http://<management-node-ip>:30901/buckets/<bucket>/` (no authentication). A completed test's page in the UI links to the two directories. The management Prometheus keeps 7 days or 16 GB, so after that a run survives only in its CSVs. With no usable S3 config at all, the exporter prints the CSVs on its stdout between `----- BEGIN CSV -----` and `----- END CSV -----` markers (`kubectl logs` on the exporter Job's pod).

Counts and rates can be summed over `node_id`, but percentiles in the k6 summary are per generator and cannot be combined. And the summary CSV is empty of k6 numbers when the script has no `handleSummary`. Both are in [known-limitations.md](known-limitations.md).

## A first experiment

You need the requirements of the README: a management cluster with a default StorageClass, two VMs reachable over SSH with a password and passwordless `sudo`, and the chart installed ([README](../README.md#install-via-helm)). The samples in `config/samples/` use placeholder addresses (`10.0.0.5`, `10.0.0.6`), the user `ubuntu` and the password `CHANGE_ME`.

1. **Edit the Environment sample** (`config/samples/dfaas_v1_environment.yaml`). Set `ipAddress`, `username` and `password` of both nodes. The node `dfaas-worker-1` is a DFaaS node that runs the `figlet` function with the strategy `alllocalstrategy`; the node `k6-gen-1` is the generator.
2. **Edit the LoadTest sample** (`config/samples/dfaas_v1_loadtest.yaml`). In the script inside the ConfigMap, set the URL to your DFaaS node: `http://<dfaas-node-ip>:30080/function/figlet`. Check that `perNodeLoad[].nodeID` is `k6-gen-1`, the generator's `nodeID`. The script defines `handleSummary`, which is what makes the k6 numbers appear in the export; keep it.
3. **Apply the Environment** and wait for `Ready`:

   ```bash
   kubectl apply -f config/samples/dfaas_v1_environment.yaml
   kubectl get environment env-sample -w
   kubectl describe environment env-sample     # the Conditions name the failing task, if any
   ```

4. **Check the DFaaS node serves.** `Ready` does not prove it ([known-limitations.md](known-limitations.md#ready-does-not-mean-the-functions-serve)):

   ```bash
   curl -s http://<dfaas-node-ip>:30080/function/figlet -d 'dfaas'
   ```

5. **Apply the LoadTest** and follow it:

   ```bash
   kubectl apply -f config/samples/dfaas_v1_loadtest.yaml
   kubectl get loadtest loadtest-sample -w     # Pending -> Running -> Exporting -> Completed
   ```

   The sample is not a draft (`suspended` is unset), so it starts at once. Add `suspended: true` to park it.
6. **Read the results.** Open `http://<management-node-ip>:30901/buckets/`, find the bucket named after `env-sample`, and open `metrics/loadtest-sample/` and `k6/loadtest-sample/`. In `k6/loadtest-sample/summary-<stamp>.csv` check that `dfaas_summary_fetched` is `1` for `k6-gen-1`.
7. **Abort or clean up.** `kubectl patch loadtest loadtest-sample --type=merge -p '{"spec":{"stop":true}}'` aborts a running test and keeps the record. `kubectl delete loadtest loadtest-sample` removes the test and its remote TestRun.

The UI at `http://<management-node-ip>:30800/` creates the same resources from forms. Tests it creates are drafts that you start explicitly with **Start**, and the form generates the k6 script from the scenarios you define (the two arrival-rate executors) and shows it before you submit. It has no authentication ([known-limitations.md](known-limitations.md#the-gateway-has-no-authentication)).

## Where to read next

[architecture.md](architecture.md) for how the operator works inside, [cross-repo-contract.md](cross-repo-contract.md) before changing a CRD, [known-limitations.md](known-limitations.md) before trusting a number, and [development.md](development.md) to build and test.
