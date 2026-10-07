# DFaaSOperator

A Kubernetes operator for running load tests against a federation of
[DFaaS](https://github.com/unimib-datAI/dfaas) nodes. You declare the machines
in an `Environment` and a k6 test in a `LoadTest`; the operator installs the
software on those existing VMs over SSH, runs k6 from the generator VMs, and
exports the measurements as CSV files to S3-compatible storage. It creates no
VMs.

The web UI and the REST gateway that sit on top of the two CRDs live in a
separate repository, [DFaaS_UI](https://github.com/isired01/DFaaS_UI). The Helm
chart in this repository installs both. The documentation index is
[docs/README.md](docs/README.md).

Both CRDs are in the group `dfaas.dfaas.io/v1`:

| Kind          | What it describes                                                                                                                                                  |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `Environment` | The machines: `dfaas-worker` nodes (the DFaaS nodes, running k3s, OpenFaaS, HAProxy and the dfaas-agent) and `k6-load-generator` nodes (k3s plus k6-operator). The operator also installs Prometheus, Grafana and SeaweedFS on the management cluster. |
| `LoadTest`    | One k6 test against a `Ready` Environment: one remote k6 `TestRun` per generator, then an exporter Job that writes the results to S3.                               |

```text
+---------------------------+        +-----------------------------+
| Environment CR            |        | LoadTest CR                 |
| nodes: dfaas-worker, k6   |        | targetEnvironment: env-xyz  |
| openfaas functions        |        | perNodeLoad[]               |
| s3ConfigRef (export sink) |        | metricsExport.metrics[]     |
+-------------+-------------+        +--------------+--------------+
              | reconcile                            | reconcile
              v                                      v
   +----------+----------+               +-----------+------------+
   |  Ansible Jobs (VMs) | ----------->  | Remote k6 TestRun via  |
   |  Helm (Prometheus,  |               | per-node k3s + k6-op   |
   |  Grafana, SeaweedFS |               +-----------+------------+
   |  on mgmt)           |                           |
   +----------+----------+                           v
              |                              +-------+--------+
              v                              | Exporter Job   |
       Environment.Ready                     | Prometheus ->  |
                                             | CSV -> S3 sink |
                                             | (SeaweedFS by  |
                                             |  default)      |
                                             +----------------+
```

[docs/overview.md](docs/overview.md) explains the whole system, and
[docs/architecture.md](docs/architecture.md) the phases and conditions of both
resources.

## Requirements

**Management cluster.** A Kubernetes cluster at version 1.25 or later (the
chart's `kubeVersion`), with `kubectl` and Helm 3.8 or later (OCI support). The
operator runs here and installs the monitoring stack here at runtime. That stack
needs a default StorageClass: Prometheus claims a 20Gi volume and SeaweedFS a
10Gi volume, both ReadWriteOnce. k3s ships `local-path`; a kubeadm cluster has
none by default. Without one the Environment stays in `ProvisioningMonitoring`
with the message "monitoring pods not Ready yet".

**VMs.** One per node in `Environment.spec.nodes`. Each one must have:

- a Debian-family Linux with `apt` and systemd (the playbooks use the `apt`
  module; the example below uses Ubuntu);
- an SSH server on port 22 with password authentication enabled, and a user
  whose password goes in `spec.nodes[].username` and `password`. The operator
  does not distribute SSH keys. The user and the password are written unquoted
  into the Ansible inventory, so they must not contain whitespace, `#` or
  quotes;
- passwordless `sudo` for that user (the playbooks run with `become`);
- Python 3;
- a correct clock: `apt` rejects release files dated in the future, and a
  management node whose clock is behind makes Prometheus drop samples.

[cloud-init/dfaas-config.yaml](cloud-init/dfaas-config.yaml) is an example
cloud-init file that produces this baseline (user `ubuntu`, password
`CHANGE_ME`; set your own and use the same pair in the Environment). The
operator does not read it. [cloud-init/reset-script.sh](cloud-init/reset-script.sh)
is an example Multipass helper that deletes and recreates the VMs named in it,
with 2 vCPU, 4 GB of RAM and a 20 GB disk each. No minimum size is enforced or
documented anywhere in the code.

**The machines are taken over.** Provisioning disables UFW and the APT
automatic upgrades, installs k3s, Helm and OpenFaaS, and runs
`k3s-uninstall.sh` when a node's role changes. Use dedicated VMs.

**Network.** The ports below must be open on the machine in the second column,
from the sources in the third.

| Port  | On                          | Must be reachable from                          | Purpose                                                     |
| ----- | --------------------------- | ----------------------------------------------- | ----------------------------------------------------------- |
| 22    | every VM                    | the management cluster (Ansible Job pods, probe) | SSH                                                         |
| 6443  | each generator              | the operator pod                                | k3s API, for dispatching `TestRun`s                         |
| 30909 | each DFaaS node             | the management Prometheus                       | `/federate`                                                 |
| 30080 | each DFaaS node             | the k6 runners and the other DFaaS nodes        | HAProxy, the entrypoint to the functions (`/function/<name>`) |
| 31600 | each DFaaS node             | the other DFaaS nodes                           | libp2p (dfaas-agent)                                        |
| 30900 | management node             | each generator                                  | SeaweedFS S3, payload images                                |
| 30901 | management node             | each generator, and your browser                | SeaweedFS filer: start signal, k6 summaries, result files   |
| 30800 | management node             | your browser                                    | the UI                                                      |
| 30090 | management node             | your browser                                    | Prometheus                                                  |
| 30300 | management node             | your browser                                    | Grafana                                                     |

During provisioning the generators also dial the management node back on 30901,
at the address their SSH session arrives from; see
[docs/cross-repo-contract.md](docs/cross-repo-contract.md) for how that address
is detected and used.

**Internet egress during provisioning.**

- The Ansible Job pods use `alpine/ansible` and download `apk`, PyPI and Ansible
  Galaxy packages on every run.
- Every VM downloads from the distribution `apt` mirrors, `get.k3s.io`, GitHub,
  `raw.githubusercontent.com` (the Helm installer), `cli.openfaas.com`, the
  haproxytech, prometheus-community and openfaas chart repositories, the
  grafana chart repository (`grafana.github.io/helm-charts`, for the k6
  generators), and the image registries `ghcr.io`, `docker.io` and `quay.io`.
- The management cluster pulls images from the same registries.

A Job gets four attempts (`BackoffLimit` 3), so a short DNS or network failure
can fail a provisioning run. Editing the Environment spec starts it again.

**Registry access.** The operator, exporter and UI images and the chart are
pulled from `ghcr.io/isired01` and the packages must be public.
`imagePullSecrets` in the chart reaches only the operator and UI pods: the
exporter Jobs and the VMs pull without credentials.

## Install via Helm

```bash
helm install dfaas oci://ghcr.io/isired01/charts/dfaas \
  --version 4.0.2 \
  --create-namespace \
  --namespace dfaas-operator-system
```

This installs the operator in `dfaas-operator-system`, the UI in `dfaas-ui`,
and the two CRDs (Helm applies the contents of `crds/` on install only; see
[Upgrade](#upgrade)). The `--version` above is the latest published release at
the time of writing; the releases are listed on the
[releases page](https://github.com/isired01/DFaaSOperator/releases).

To install from a checkout, the chart's own version is the placeholder `0.0.0`
(`release.yml` replaces it when it packages a release), so every image tag
defaults to a tag that does not exist. Pin real ones:

```bash
helm install dfaas ./charts/dfaas --create-namespace --namespace dfaas-operator-system \
  --set operator.image.tag=4.0.2 \
  --set operator.exporterImage.tag=4.0.2 \
  --set ui.image.tag=4.0.2
```

### Exposing the UI

The UI Service is a NodePort on 30800 by default, and **the UI has no
authentication**. Anyone who can reach that port can read every Environment,
including the SSH usernames and passwords of its nodes, and can create or delete
Environments and LoadTests. The same holds for the other pieces the operator
installs, which are meant for a trusted lab network:

- Grafana (30300) gives every visitor the Admin role without a login;
- the SeaweedFS filer (30901) has no authentication, so every exported file is
  readable;
- Prometheus (30090) has none either.

Do not expose these on an untrusted network. To keep the UI inside the cluster:

```bash
helm install dfaas oci://ghcr.io/isired01/charts/dfaas --version 4.0.2 \
  --create-namespace --namespace dfaas-operator-system \
  --set ui.service.type=ClusterIP

kubectl -n dfaas-ui port-forward svc/dfaas-ui 8082:8082
```

SSH passwords are stored in plain text in `Environment.spec.nodes[]`, and each
generator's k3s admin kubeconfig is the Secret `<env>-<nodeID>-kubeconfig` in
the Environment's namespace. Creating an Environment is a privileged action:
the node fields go into an Ansible inventory that runs as root. The UI's
ClusterRole can read and write every Secret and ConfigMap in the cluster.

### Upgrade

Helm applies the contents of `crds/` on install and never on upgrade. Apply the
target release's CRDs **first**, then upgrade. An operator started against the
old CRDs has the new status fields pruned by the API server.

```bash
helm pull oci://ghcr.io/isired01/charts/dfaas --version <version> \
  --untar --untardir /tmp/dfaas-<version>
kubectl apply -f /tmp/dfaas-<version>/dfaas/crds/

helm upgrade dfaas oci://ghcr.io/isired01/charts/dfaas --version <version> \
  --namespace dfaas-operator-system
```

From a checkout of the release tag, `kubectl apply -f charts/dfaas/crds/`
does the same as the pull. The release assets on GitHub also carry the two CRD
files, but they return 404 while the repository is private; the `helm pull`
form does not depend on that.

Upgrading from 4.0.x:

- `spec.topology` (Environment) and the phase `Idle` are removed from the CRD.
  Apply the CRDs before the new operator starts, and delete any `topology`
  block from your manifests: `kubectl apply` rejects an unknown field.
- The management Prometheus restarts once, at the next provisioning of an
  Environment after the upgrade.
- Clusters bootstrapped before v2.5, which still have the pre-Helm SeaweedFS
  objects, are no longer migrated. Upgrade through a release that still has
  the migration first.
- Management-address detection (a per-generator address, see
  [docs/cross-repo-contract.md](docs/cross-repo-contract.md)) is covered by
  playbook tests only; it has not been run against a real lab at 4.0.x
  ([#41](https://github.com/isired01/DFaaSOperator/issues/41)). After
  the first provisioning, check
  `kubectl get environment <env> -o jsonpath='{.status.k6Nodes[*].managementAddress}'`.
  Empty means the fallbacks (`DFAAS_SYNC_PUBLIC_URL`, `HOST_IP`) apply.

## Quick start

The samples in [config/samples/](config/samples/) hold placeholder addresses
(`10.0.0.x`), the user `ubuntu` and the password `CHANGE_ME`.

1. Edit [dfaas_v1_environment.yaml](config/samples/dfaas_v1_environment.yaml):
   set `ipAddress`, `username` and `password` of both nodes. The node IDs are
   `dfaas-worker-1` (a DFaaS node running the `figlet` function) and
   `k6-gen-1` (the generator).
2. Edit [dfaas_v1_loadtest.yaml](config/samples/dfaas_v1_loadtest.yaml): the
   URL in the script's ConfigMap must point at your DFaaS node's HAProxy
   entrypoint, `http://<dfaas-node-ip>:30080/function/figlet`. Its
   `perNodeLoad[].nodeID` (`k6-gen-1`) must match the generator's node ID.

```bash
kubectl apply -f config/samples/dfaas_v1_environment.yaml
kubectl get environment -w          # wait for PHASE=Ready
kubectl apply -f config/samples/dfaas_v1_loadtest.yaml
kubectl get loadtest -w             # Pending -> Running -> Exporting -> Completed
```

`handleSummary` in the sample script PUTs the k6 summary to the
`DFAAS_SUMMARY_URL` the operator injects. A script without it still ends
`Completed`, but the exported summary CSV then holds only `dfaas_summary_fetched`
rows with value `0`. Scripts generated by the UI include it.

The UI at `http://<management-node-ip>:30800/` creates the same resources from
forms.

## Where the results are

The export writes to the S3 sink of the Environment: the in-cluster SeaweedFS
(`seaweedfs-default`) unless `Environment.spec.s3ConfigRef` names another
configuration. The bucket is `<environment-name>-<first 6 characters of the
Environment UID>`, created on the first export and reused. One export writes:

| Key                                           | Contents                                                                                       |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `metrics/<loadtest>/<stamp>.csv`              | One row per (series, sample); every Prometheus label is a column.                              |
| `metrics/<loadtest>/query-status-<stamp>.csv` | One row per entry of `metricsExport.metrics`: series and samples returned, warnings, error.    |
| `k6/<loadtest>/summary-<stamp>.csv`           | The k6 end-of-test summaries, one row per generator, metric and statistic.                     |
| `k6/<loadtest>/<nodeID>-summary-<stamp>.json` | Each generator's raw `handleSummary` JSON.                                                     |
| `k6/<loadtest>/<nodeID>-<stamp>.log`          | Each k6 runner's captured output.                                                              |

`<stamp>` is the end of the query window in compact UTC. Browse the default
sink at `http://<management-node-ip>:30901/buckets/<bucket>/`. The exporter
starts one minute after k6 finishes, so that the management Prometheus has
federated the last samples. The management Prometheus keeps 7 days or 16 GB;
after that a run survives only in its CSVs. Details are in
[docs/overview.md](docs/overview.md).

## Uninstall

Both resources carry finalizers that only the running operator removes, so
delete them before removing the operator.

```bash
# 1. While the operator is still running
kubectl delete loadtests.dfaas.dfaas.io --all -A
kubectl delete environments.dfaas.dfaas.io --all -A      # wait until they are gone

# 2. The chart (also removes the dfaas-ui namespace)
helm uninstall dfaas -n dfaas-operator-system
kubectl delete namespace dfaas-operator-system          # created by --create-namespace

# 3. The CRDs, which Helm never removes
kubectl delete crd environments.dfaas.dfaas.io loadtests.dfaas.dfaas.io
```

If the operator is already gone and an object hangs in `Terminating`:

```bash
kubectl patch <kind> <name> -n <namespace> --type=merge -p '{"metadata":{"finalizers":null}}'
```

The chart does not remove what the operator installed at runtime. Copy out any
results you need first: SeaweedFS holds every exported file.

```bash
helm uninstall -n monitoring prometheus grafana seaweedfs
kubectl delete namespace monitoring dfaas-s3
```

Nothing is removed from the VMs. They keep k3s, OpenFaaS, HAProxy, the
dfaas-agent and the k6 stack. Run `sudo /usr/local/bin/k3s-uninstall.sh` on each
one to wipe it.

## Configuration

### Chart values

[charts/dfaas/values.yaml](charts/dfaas/values.yaml) is the reference. The
values most installs touch:

| Value                               | Default                                | Meaning                                                                                              |
| ----------------------------------- | -------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `operator.enabled`, `ui.enabled`    | `true`                                 | Install either half on its own.                                                                      |
| `operator.image.tag`                | the chart's `appVersion`               | Operator image tag.                                                                                  |
| `operator.exporterImage.tag`        | the chart's `appVersion`               | Exporter image tag; the chart passes the full image to the operator as `DFAAS_EXPORTER_IMAGE`.       |
| `operator.leaderElection.enabled`   | `true`                                 | Leader election, for `operator.replicas` above 1.                                                    |
| `operator.extraEnv`                 | `[]`                                   | Extra environment variables for the operator, see below.                                             |
| `operator.resources`, `nodeSelector`, `tolerations`, `affinity` | requests 100m/128Mi, limits 500m/512Mi | Scheduling and sizing of the operator pod.                                           |
| `ui.image.tag`                      | the chart's `appVersion`               | UI image tag.                                                                                        |
| `ui.service.type`, `ui.service.nodePort` | `NodePort`, `30800`               | Use `ClusterIP` to keep the UI inside the cluster.                                                   |
| `ui.ingress.*`                      | disabled, host `dfaas.local`           | Ingress for the UI.                                                                                  |
| `ui.env.*`                          | `CORS_ORIGINS: ""`                     | Environment variables of the gateway. `SEAWEEDFS_ENDPOINT`, `SEAWEEDFS_PUBLIC_URL` and `SEAWEEDFS_FILER_PUBLIC_URL` are described in the comments of `values.yaml`. |
| `imagePullSecrets`                  | `[]`                                   | Pull secrets for the operator and UI pods only.                                                      |
| `fullnameOverride`                  | `""`                                   | Prefix of the resource names (default: the release name).                                            |

### Operator environment variables

Set them with `operator.extraEnv`.

| Variable                | Set by the chart | Meaning                                                                                                                                                                                      |
| ----------------------- | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DFAAS_EXPORTER_IMAGE`  | yes              | Exporter image for the export Jobs. Unset (for example under `make run`) it falls back to `ghcr.io/isired01/dfaas-exporter:latest`, which tracks `main`.                                         |
| `HOST_IP`               | yes (host IP of the operator pod) | Last fallback for the filer address a generator dials (`http://<HOST_IP>:30901`).                                                                                          |
| `DFAAS_SYNC_PUBLIC_URL` | no               | Fallback filer base for a generator on which no management address was detected: `http://<address>:30901`. It must be reachable from every such generator; on a lab spread over several sites, use an address of the management node that all of them can route to (for example a VPN address). Wins over `HOST_IP`. |
| `DFAAS_FILER_URL`       | no               | The filer base the operator itself dials. In the cluster the Service DNS name works; when the operator runs outside it (`make run`) set it to `http://<management-node-ip>:30901`, or every `syncStart` test fails with "GO signal publish failed". |

The operator binary takes the usual controller-runtime flags
(`--metrics-bind-address`, `--health-probe-bind-address`, `--leader-elect`,
`--metrics-secure`, `--enable-http2`, `--zap-*`).

### External S3 sink

Without `spec.s3ConfigRef` an Environment exports to the in-cluster SeaweedFS.
To use another S3 endpoint, register a Secret in the `dfaas-s3` namespace and
name it in `Environment.spec.s3ConfigRef.name`. Every key except `endpoint` is
required; a missing key leaves the exporter Pod in `CreateContainerConfigError`.

```bash
kubectl -n dfaas-s3 create secret generic my-s3 \
  --from-literal=endpoint=https://s3.example.com \
  --from-literal=region=eu-west-1 \
  --from-literal=access_key_id=<access-key> \
  --from-literal=secret_access_key=<secret-key> \
  --from-literal=force_path_style=false
kubectl -n dfaas-s3 label secret my-s3 dfaas.io/s3-config=true
```

The label only makes the UI list the configuration. The credentials need
`s3:HeadBucket`, `s3:CreateBucket` and `s3:PutObject`; uploading payload images
from the UI also needs `s3:PutBucketPolicy` and a bucket that accepts a
public-read policy on `assets/*`.

## Components and images

| Component                             | Source                                   | Image                                                                       |
| ------------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------- |
| Operator (both controllers)           | `cmd/`, `internal/`, root `Dockerfile`   | `ghcr.io/isired01/dfaas-operator:<version>`                                 |
| Exporter (Prometheus to CSV to S3)    | [dataExporter/](dataExporter/)           | `ghcr.io/isired01/dfaas-exporter:<version>`                                 |
| `dfaas-imgproc`, an image-processing function for load tests | [imageFunction/](imageFunction/README.md) | `ghcr.io/isired01/dfaas-imgproc:<version>`              |
| UI and REST gateway                   | [DFaaS_UI](https://github.com/isired01/DFaaS_UI) | `ghcr.io/isired01/dfaas-control-plane:<version>`                    |
| dfaas-agent, the system under test    | built outside this repository, see [docs/dfaas-agent.md](docs/dfaas-agent.md) | `ghcr.io/isired01/dfaas-agent:dev` |

`release.yml` publishes the operator, exporter and imgproc images on every `v*`
tag, under `vX.Y.Z`, `X.Y.Z` and `latest`; the UI repository publishes its own
image on its tag. The chart pins the exporter through `DFAAS_EXPORTER_IMAGE`;
`:latest` is used only by an operator started without it.

The Ansible playbooks, the Helm values for HAProxy, OpenFaaS and Prometheus, and
the embedded Prometheus, Grafana and SeaweedFS charts are compiled into the
operator binary: changing one needs a new operator image. The Ansible Jobs run
the `alpine/ansible:2.18.6` image. Grafana ships one dashboard, `dfaas-live`.

## Documentation

| Document                                                       | Contents                                                                  |
| -------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [docs/overview.md](docs/overview.md)                           | What the system does and how a test runs end to end.                      |
| [docs/architecture.md](docs/architecture.md)                   | Controllers, phases, conditions and the main mechanisms.                  |
| [docs/cross-repo-contract.md](docs/cross-repo-contract.md)     | What couples this repository to DFaaS_UI: change one side, check the other. |
| [docs/glossary.md](docs/glossary.md)                           | Terms used across both repositories.                                      |
| [docs/known-limitations.md](docs/known-limitations.md)         | What to know before trusting a measurement or exposing an install.        |
| [docs/development.md](docs/development.md)                     | Building, testing, changing a CRD, running the operator locally.          |
| [docs/releasing.md](docs/releasing.md)                         | How a release is cut, in both repositories.                               |
| [docs/dfaas-agent.md](docs/dfaas-agent.md)                     | Where the dfaas-agent image and chart come from.                          |
| [docs/adr/README.md](docs/adr/README.md)                       | Index of the architecture decision records.                               |

## Known limitations

- **`Ready` does not mean the functions serve.** The periodic health probe only
  dials TCP port 22, and `K6Healthy` reads the TestRun stage, not k6's failure
  rate; generated scripts set no thresholds. Before a run you intend to keep,
  check `curl -s http://<dfaas-node-ip>:30080/` on each DFaaS node: the body
  `This is a DFaaS node. Proxy is running, but the DFaaS agent is not!` means
  HAProxy still serves its placeholder configuration.
- **The federation is exactly the bootstrap list.** Each DFaaS node dials every
  DFaaS node listed before it in `spec.nodes`, once, at agent start, and the agents
  never re-dial a dropped peer, so a lost link stays down until the agents are
  restarted. Put the most reliable machine first. Reordering `spec.nodes`
  re-provisions every node.
- **Percentiles are per generator.** Counts and rates can be summed over
  `node_id`; `med`, `p90` and `p95` cannot be combined into a run-wide value.
  The start barrier of `syncStart` and the payload-image fetch are ordinary k6
  requests and count in the exported `http_reqs`, `http_req_duration` and
  `http_req_failed`; read the failure rate of the load itself from the `checks`
  metric in scripts generated by the UI.
- **Provisioning is not reproducible by default.** k3s and Helm are installed
  at whatever version is current when missing, the chart repositories are
  re-added on every run, and the agent image is the mutable `:dev` tag. Pin
  versions in `setup-nodes.yml` (embedded, so a new operator image is needed).
- **Removing a node from `spec.nodes` does not uninstall it.** The machine
  keeps its software and, for a DFaaS node, its libp2p links.
- **The clocks must agree.** A management node whose clock is behind makes
  exports fail with "metrics export produced 0 data points".

The full list, with the remedies, is in
[docs/known-limitations.md](docs/known-limitations.md).

## License

Apache-2.0, see [LICENSE](LICENSE), copyright 2026 Isaia Del Rosso. The
exception is three files derived from
[unimib-datAI/dfaas](https://github.com/unimib-datAI/dfaas), which stay under
AGPL-3.0-or-later:
`internal/controller/ansible/templates/haproxy-values.yaml`,
`openfaas-values.yaml` and `prometheus-values.yaml`. The dfaas-agent image is
upstream AGPL-3.0 code and is not part of this repository.
