# Development

How to build, test and change the operator. The UI repository has its own developer guide; what couples the two is in [cross-repo-contract.md](cross-repo-contract.md).

## Toolchain

- Go 1.26. All three Go modules of this repository (the root operator module, `dataExporter/` and `imageFunction/`) declare `go 1.26.0`, and the Dockerfiles build with `golang:1.26`.
- Docker, for `make docker-build`. `kubectl` and `helm` (3.8 or later) for installing and linting the chart.
- Everything else is installed into `./bin/` by `make`, at pinned versions (see the top of the `Makefile`): `controller-gen` v0.20.1 (it matches the generator annotation of the committed CRDs), `kustomize` v5.3.0, `setup-envtest` release-0.19 and the envtest binaries for Kubernetes 1.29.0. `setup-envtest` release-0.17 reads a retired bucket that answers 401, which makes every envtest spec fail in `BeforeSuite` with `exec: "etcd" not found`; keep release-0.19 or later.
- Main dependencies: controller-runtime v0.17.3, `k8s.io/*` v0.29.2, Helm SDK v3.14.4. `go.mod` carries `replace` directives for `k8s.io/kubectl` (without it three modules fall back to v0.29.0), `golang.org/x/sync` and `golang.org/x/tools`. Keep them when you run `go get`; why the last two are pinned down is not recorded and is in the issue tracker.

The repository has no linter configuration. `make fmt` and `make vet` are the gates.

## Make targets

```text
make manifests     regenerate the CRDs (config/crd/bases) and the RBAC role (config/rbac/role.yaml)
make generate      regenerate api/v1/zz_generated.deepcopy.go
make fmt vet       go fmt and go vet for the root module
make test          manifests, generate, fmt, vet, envtest assets, then every Go test (see below)
make build         build bin/manager
make run           run the operator on this machine (see below)
make docker-build  build the operator image for the host platform (IMG=dfaas-operator:dev)
make install       apply config/crd to the cluster of the current kubeconfig context
make uninstall     delete the CRDs from that cluster (and with them every Environment and LoadTest)
make helm-lint     helm lint charts/dfaas
```

`make install` and `make uninstall` render `config/crd` with kustomize and need `kubectl`. They act on the current kubeconfig context.

## Tests

`make test` runs `manifests`, `generate`, `fmt` and `vet`, installs the envtest assets under `bin/k8s/1.29.0-<os>-<arch>`, and then:

```bash
go test ./... -coverprofile cover.out    # root module, including the envtest suite
cd dataExporter && go test ./...
cd imageFunction && go test ./...
```

There is no end-to-end suite. The envtest suite starts a real API server and etcd (no kubelet, so no pods run) and applies the CRDs from `config/crd/bases`; most of the logic is tested without it, through the fakes below.

To run one envtest spec after a first `make test` has installed the assets:

```bash
go test ./internal/controller/ -run TestControllers -ginkgo.focus '<text of the It>'
```

To run everything that does not need envtest, which works without the assets (the Ginkgo specs would fail in `BeforeSuite`):

```bash
go test -skip TestControllers ./api/... ./internal/...
```

What the tests cover:

- `internal/controller/*_test.go` drives both reconcilers through the fakes: `loadtest_seam_test.go` is the large table-driven file for the LoadTest controller and the dispatcher seam; the Environment files cover provisioning, retries, health, deletion, generation drift and the fan-in.
- `crd_validation_test.go` applies manifests that the CRD must reject, including the CEL cost budget.
- `api/v1/phases_test.go` reads the phase lists from the `+kubebuilder:validation:Enum` markers and requires a classification for each phase.
- `api/v1/inventory_test.go` classifies every condition reason (see below).
- `internal/controller/monitoring/*_test.go` pin the embedded values against the address constants, the federation interval and the dashboard.
- `internal/controller/ansible/k6_playbook_test.go` parses the embedded generator playbook and pins its structure. No test runs `ansible-playbook`.
- `dataExporter/main_test.go` checks the CSVs, the object keys and the bucket-name golden table without Prometheus or S3.

Do not run `go test ./...` or a bare `make` target that you have not read against a cluster you care about: the envtest suite uses its own API server, but `make install`, `make uninstall` and `make run` act on the current kubeconfig context.

## Code generation and the CRD copy

The CRDs are generated, and the chart ships its own copy of them. A change to `api/v1/*_types.go` needs:

```bash
make manifests generate
cp config/crd/bases/dfaas.dfaas.io_*.yaml charts/dfaas/crds/
```

No make target performs the copy, and Helm applies `crds/` on install only. The full checklist, including the mirror in the UI repository and the order for applying CRDs on a cluster, is in [cross-repo-contract.md](cross-repo-contract.md#changing-a-crd-field).

If you change a `+kubebuilder:rbac` marker, `make manifests` rewrites `config/rbac/role.yaml`, and you must copy the changed rules into `charts/dfaas/templates/operator-rbac.yaml` by hand. Nothing checks it.

`kubebuilder create api` still scaffolds, but prints errors about the removed `config/default` and `config/rbac` kustomizations (the repository no longer deploys through kustomize; `config/crd`, `config/rbac/role.yaml` and `config/samples` remain).

## What the API server rejects

These rules are enforced by the CRD schema at `kubectl apply` time, with no admission webhook. They take effect when the regenerated CRDs are applied (`make install`, or the chart's `crds/`). The gateway mirrors them by hand ([cross-repo-contract.md](cross-repo-contract.md#validation-mirrors)).

- **Duplicate `ipAddress`** within one Environment's `spec.nodes[]`: one machine is one node. Two entries on one machine would get two libp2p identities, Ansible would install the dfaas-agent twice with different keys, and whichever lands last would leave every peer dialling a dead peer ID while the Environment stays `Ready` (the probe checks only port 22). It is a CEL rule, which is why `spec.nodes` is capped at 50 items and `ipAddress` at 45 characters: an unbounded array or string exceeds the CRD's validation cost budget and the API server then rejects the CRD itself. The two bounds move together with the rule, and `crd_validation_test.go` covers both. The scope is one Environment: nothing stops two different Environments from declaring the same machine.
- **Duplicate `nodeID`** in `spec.nodes[]` or `spec.perNodeLoad[]` (`listType=map`, `listMapKey=nodeID`). `nodeID` keys the node's libp2p identity (`<env>-libp2p-keys`), its kubeconfig Secret and the remote TestRun name, so a duplicate would collapse two machines into one identity.
- **`nodeID`** is a DNS-1123 label of at most 63 characters (an OpenAPI pattern, not CEL, to stay inside the cost budget).
- **`execTimeout`, `maxInflight`, `timeoutMs` and `maxRate` below 1** (`Minimum=1`). An `execTimeout` of 0 would deploy as `exec_timeout: "0s"` and corrupt the timing the test measures.
- **A `dfaas-worker` with no `functions`**, or a function name outside `^[a-z0-9]+$`. A worker with no function serves nothing and breaks the playbook's prune task (an empty list is marshalled as `null`). A hyphen in a function name makes HAProxy reject the whole rendered configuration, so every node serves 503 while the Environment stays `Ready`.
- **Malformed durations** in `perNodeLoad[].duration` and `metricsExport.step`: they must match `^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$` (ASCII units, no `µs`). `"5 minutes"` is rejected at apply time instead of failing in the exporter. (`perNodeLoad[].duration` reaches neither k6 nor the exporter; `step` does.)
- **`metricName` is required** when a metric's `type` is `custom-promql` (a CEL rule).
- **`balancingStrategy`** must be one of the six enum values. `nodemarginstrategy` and `rlagentstrategy` pass but are not supported.
- **`role`** must be `dfaas-worker` or `k6-load-generator`, and `capacity` one of `LOW`, `MEDIUM`, `HIGH`.

The deliberate absence of a CEL rule for "`startAt` requires `suspended`" is explained in [cross-repo-contract.md](cross-repo-contract.md#startat-requires-suspended).

## Running the operator on your machine

`make run` builds and runs the operator against the current kubeconfig context. Leader election is off by default. The cluster you point it at is the management cluster: the operator creates Jobs, installs Prometheus, Grafana and SeaweedFS into it, and needs to reach the VMs and the generators' API servers.

Before you start it:

1. Apply the CRDs: `make install` (or `kubectl apply -f charts/dfaas/crds/`).
2. If the chart is installed in that cluster, scale its operator to zero so that two operators do not fight: `kubectl -n dfaas-operator-system scale deploy/dfaas-operator --replicas=0`.
3. Create the ClusterRole that the monitoring stack reuses; it ships only with the chart:

   ```bash
   helm template dfaas ./charts/dfaas -s templates/monitoring-rbac.yaml | kubectl apply -f -
   ```

4. Set the environment variables below.

| Variable | When | Value |
| --- | --- | --- |
| `DFAAS_FILER_URL` | Always, outside the cluster | `http://<management-node-ip>:30901`. The in-cluster Service name does not resolve outside the cluster. Without it every `syncStart` test fails after 5 minutes with "GO signal publish failed", and the best-effort filer sweeps at the end of a run only log errors. |
| `DFAAS_EXPORTER_IMAGE` | Recommended | `ghcr.io/isired01/dfaas-exporter:<release>`. Unset, the exporter image falls back to `:latest`, which tracks `main`. |
| `DFAAS_SYNC_PUBLIC_URL` or `HOST_IP` | Only for generators without a detected management address | The filer base those generators dial (`http://<address>:30901`). `HOST_IP` is unset outside a Pod. Generators provisioned by a recent release have a detected address and need neither. |

The operator host's clock must agree with the cluster: `status.startTime` and `endTime` come from the operator's clock, and the export window is queried from the cluster's Prometheus.

Embedded files (the playbooks, the HAProxy, OpenFaaS and Prometheus values, the monitoring charts and values, the dashboard) are compiled into the binary. After editing one, restart `make run`, or rebuild the image; then edit the Environment spec to re-provision.

## CI gates

`.github/workflows/test.yml` runs on every pull request and on pushes to `main`. In this order, because each step catches something nothing after it can:

1. **Generated artifacts are committed.** `make manifests generate fmt`, then `git diff --exit-code`. A CRD field that exists only in Go compiles, passes every test, and is then pruned by the API server at run time.
2. **`go.mod` is tidy and verified**, in all three modules (`go mod tidy -diff && go mod verify`).
3. **The chart's CRDs match `config/crd/bases`** (`diff -u` of both files).
4. **The chart lints and renders** (`helm lint`, `helm template`, Helm v3.15.4).
5. **`make test`**.
6. **`go vet`** in `dataExporter` and `imageFunction`.
7. **The three images build** (`dfaas-operator`, `dfaas-exporter`, `dfaas-imgproc`). A Dockerfile is source that no Go check reads: two images once shipped broken because their Dockerfiles named a list of files or packages instead of copying directories. The operator Dockerfile copies `cmd/`, `api/` and `internal/` whole and builds `./cmd`, so a new package or file cannot be missed.

A second job, `publish-exporter`, runs on a push to `main` after the tests pass and publishes `ghcr.io/isired01/dfaas-exporter:latest` (multi-arch). `latest` therefore means "current main" for the exporter, not "last release". A Helm install never uses it (the chart pins the exporter's version); an operator started with `make run` and no `DFAAS_EXPORTER_IMAGE` does.

Wait for CI on a pull request before merging.

## Conventions

- **Commit messages**: `<area>: <what changed>`, in lowercase, for example `loadtest: fail a syncStart test whose generator has no GO URL`. The areas in use are `api`, `ansible`, `ci`, `controller`, `docs`, `exporter`, `k6dispatch`, `loadtest`, `monitoring`, `operator`, `repo`.
- **English** in code comments, error strings, log text and docs. Technical terms are not translated.
- **Roles are kebab-case enums**: `dfaas-worker` and `k6-load-generator`. Never change the casing; the CRD rejects anything else.
- **In prose, a `dfaas-worker` node is a "DFaaS node"** ([glossary.md](glossary.md)).
- **Phase sets are methods on the enums**, never re-typed comparisons: `LoadTestPhase.Terminal()`, `LoadTestPhase.PreExecution()` and `EnvironmentPhase.Dispatchable()` in `api/v1`, each with a table test whose phase lists are read from the enum markers and which needs a row per phase. Two comparisons stay raw on purpose: `isSettledPhase` (`Ready` or `Failed`, a different question) and the `== EnvReady` health route in `Reconcile`.
- **Condition reasons are a protocol.** Every `EnvReason*` and `LTReason*` constant needs a row in `terminalFailure` in `api/v1/inventory_test.go` (true when stamping it means the object failed for good), and `TestNoDeadReasons` fails on a constant that no non-test code stamps. The SPA's curated table follows `terminalFailure`: add a terminal reason to both lists ([cross-repo-contract.md](cross-repo-contract.md#condition-reasons)). A new condition message format is a change visible to users, because the UI renders messages verbatim.
- **A new `internal/` package that reaches outside the management API server** (a machine, the filer, Helm, a remote cluster) gets one narrow interface, one production adapter, a `<pkg>/fake` package and a test through that interface, and the reconciler gets a nil-safe field for it. Wire it in `cmd/main.go`, or production silently runs on the nil-safe default too.
- **Status is written through `statuswriter`.** Do not call `Status().Update` from a handler. Retry limits are `Budget` counters, generation-scoped ([ADR-0002](adr/0002-status-writer-never-decides-requeue.md), [ADR-0003](adr/0003-all-retry-counters-generation-scoped.md)).
- **Build every filer or S3 address through `monitoring/addresses.go`**, every filer path through `internal/syncchannel`, every remote name through `internal/k6dispatch` (`TestRunName`, `K6LogConfigMap`, `TargetNodeIDs`, `Sanitize`). A name formatted one way when it is written and another when it is read fails as "no logs", not as an error.
- **Kubernetes object names built from user input** go through `ansible.BoundedJobName` (Jobs) or an equivalent cap; see [known-limitations.md](known-limitations.md#resource-names-63-bytes-for-a-loadtest-51-for-a-testrun).
- **Kubebuilder RBAC markers** above each `Reconcile` are the source of truth for the manager's ClusterRole. Add the marker before using a new resource, run `make manifests`, then copy the rules into the chart.
- **The operator must never be able to mint ClusterRoles.** The monitoring charts are configured around that ([architecture.md](architecture.md#monitoring-stack)). Bump a chart, and recheck whether its cluster-scoped rules changed.
- **Playbook task names are static** (no Jinja, no `]`): `parseFailedTask` reads them back from the pod log to name the failing task in a Condition message.

## Building and publishing images by hand

The operator image: `make docker-build IMG=<name:tag>` (host platform). The published images are multi-arch and are built by `release.yml` ([releasing.md](releasing.md)). To build one by hand:

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/<owner>/dfaas-exporter:<tag> -f dataExporter/Dockerfile dataExporter --push
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/<owner>/dfaas-imgproc:<tag> -f imageFunction/Dockerfile imageFunction --push
```

`--push` is required for a multi-platform build. A change to the exporter reaches a running operator only through its image tag (`DFAAS_EXPORTER_IMAGE`); a change to the embedded files only through a new operator image. The operator and the exporter image must roll out together when the export format changes: the operator passes the metric list as `METRICS_JSON`, and an exporter built for another format would export the wrong thing.

`dfaas-imgproc` is a small OpenFaaS function (of-watchdog in HTTP mode) used as a load-test target: it reads raw image bytes from the POST body, returns a grayscale thumbnail, and answers `?meta=1` with the dimensions. Deploy it by adding it to a DFaaS node's `functions` (`{name, image, maxRate, timeoutMs}`); k6 reaches it through the node's HAProxy entrypoint at `http://<dfaas-node-ip>:30080/function/<name>`. See `imageFunction/README.md`.

## Changing the monitoring charts

Bumping a chart means replacing the `.tgz` under `internal/controller/monitoring/charts/`, updating the `go:embed` path in `assets.go` (the path carries the version), and rebuilding. For SeaweedFS, while the Helm SDK stays at 3.14.4, the archive in use is the upstream 4.45.0 chart with one template patched: `templates/shared/security-configmap.yaml` calls `fromToml`, which the Helm engine defines only from 3.16 on, so the unpatched chart fails to parse. The call is replaced by `dict`. `assets.go` explains the patch and gives the `helm pull` command for the pristine chart. Upgrading the SDK and dropping the patch is in the issue tracker.
