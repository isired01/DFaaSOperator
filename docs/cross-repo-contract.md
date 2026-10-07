# Cross-repo contract

The system is two repositories:

- **DFaaSOperator** (this repository): the operator, the two CRDs, the chart.
- **[DFaaS_UI](https://github.com/isired01/DFaaS_UI)**: a Go (Gin) gateway that serves a REST API and a React SPA over the CRDs.

The gateway uses a dynamic (unstructured) Kubernetes client. It has no compile-time dependency on the operator's Go types, so a CRD that drifts from its mirror in the gateway never breaks a build. It shows up at run time: a field the gateway does not know is not shown, and a field it writes that the CRD does not know is silently pruned by the API server. Every rule below is a place where one side must be changed by hand when the other changes. Each rule says what to change on the other side. Paths prefixed "UI repo" are in DFaaS_UI.

## What couples the two repositories

| Coupling | Operator side | UI side |
| --- | --- | --- |
| CRD schema | `api/v1/*_types.go`, `charts/dfaas/crds/` | `internal/api/types.go` DTOs, `mappers.go`, `yaml_export.go`, `yaml_import.go` |
| Validation rules | CRD markers and CEL | `internal/api/schema.go`, served at `GET /api/meta/schema`, read by `ui/src/lib/payloads/` |
| Dispatch gate | `EnvironmentPhase.Dispatchable()` | `internal/api/dispatchgate.go`, `crstate.env.dispatchable` |
| Occupancy | `envOccupancyGate`, `runnersUnreclaimed` | `activeLoadTestNames` in `handlers_environment.go`, `lt.occupying` in `crstate.js` |
| Condition reasons | `api/v1` constants, `inventory_test.go` | `ui/src/lib/crstate.js`, `crstate.selfcheck.mjs` |
| Results layout | `dataExporter/main.go` | `internal/api/results.go`, `assets.go` |
| Runner environment | `k6dispatch.buildTestRun`, `syncchannel` | `ui/src/lib/k6Generator.js` |
| RBAC | `charts/dfaas/templates/ui-rbac.yaml` | everything the gateway calls |

## Changing a CRD field

Do all of these, in this order. A step that is skipped fails silently.

1. Edit `api/v1/*_types.go`. Add a marker and a doc comment: the comment is what `kubectl explain` shows. Never hand-edit `zz_generated.deepcopy.go` or `config/crd/bases/`.
2. Run `make manifests generate`. CI runs the same and fails on any diff (`git diff --exit-code`), which catches a field that exists only in Go.
3. Copy the generated CRDs into the chart. There is no make target for it:

   ```bash
   cp config/crd/bases/dfaas.dfaas.io_*.yaml charts/dfaas/crds/
   ```

   The chart's copy is what a Helm install applies. CI diffs the two directories.
4. If a `+kubebuilder:rbac` marker changed, copy the new rules of `config/rbac/role.yaml` into `charts/dfaas/templates/operator-rbac.yaml` by hand. Nothing checks this, and the failure is a 403 on a Helm install, not in CI.
5. Mirror the field in the UI repo:
   - the DTO and the mapper in `internal/api/types.go` and `mappers.go`;
   - `yaml_export.go` and `yaml_import.go` if the field is part of a create path;
   - the rule in `schema.go` if the field has a validation rule (the CRD stays the source of truth);
   - the SPA: the payload builders in `ui/src/lib/payloads/`, the forms, and `ui/src/lib/crstate.js` for a new phase or reason;
   - the selfchecks (`npm run check` in `ui/`), whose assertions pin the mirrors.
6. Build the new operator and UI images and run both repositories' CI.
7. **Apply the new CRDs before the new operator starts**, on every cluster, and put a note in the release. Helm applies `crds/` on install only. An operator that writes a field the live CRD does not know has it pruned by the API server, with only a warning in its log (`unknown field`). The feature then does nothing. This has happened with `status.lastHealthCheck`, `spec.syncStart`, `status.provisioningGeneration` and `status.k6Nodes[].managementAddress`.

The commands are in the README ([Upgrade](../README.md#upgrade)).

A status field that only an operator writes needs only steps 1 to 4, 6 and 7, plus the DTO if the UI shows it. The SPA's Environment edit lock reads a non-zero `status.observedGeneration` below `metadata.generation` as "update in progress"; the operator stamps `observedGeneration` only when a run settles (`Ready` or `Failed`), copying `status.provisioningGeneration`, which is not mirrored in the DTO.

## Node roles are kebab-case enums

`dfaas-worker` and `k6-load-generator`. The CRD rejects anything else, PascalCase included. Do not "fix" the casing on either side. In prose, `dfaas-worker` is called a DFaaS node ([glossary.md](glossary.md)).

## What the gateway touches

The gateway never touches `k6.io/v1alpha1 TestRun`. Those objects live on the generators' k3s clusters and belong to the operator. The gateway reads and writes only `Environment`, `LoadTest`, the script ConfigMaps next to each LoadTest, and the S3 config Secrets in the `dfaas-s3` namespace, and it lists `Node`s.

The ClusterRole in `charts/dfaas/templates/ui-rbac.yaml` lives in this repository but grants what the gateway needs. It must include `get` and `list` on core `nodes`: the gateway lists Nodes to build the baked asset URL, and without that rule every upload to `seaweedfs-default` fails with a 502 unless `SEAWEEDFS_PUBLIC_URL` is set. The Node list is also the last fallback for the result links. When the gateway gains a capability that touches a new resource, add the rule there. The role currently grants more than the gateway uses; narrowing it is tracked in [#35](https://github.com/isired01/DFaaSOperator/issues/35).

## S3 export sink and payload assets

The export sink is configured per Environment, through `Environment.spec.s3ConfigRef`, never on a LoadTest. Unset means the config `seaweedfs-default`, the in-cluster SeaweedFS. The name `seaweedfs-default` is part of the contract and must match on both sides (`DefaultS3ConfigName` in `internal/controller/s3_bootstrap.go` and in `internal/api/assets.go`, UI repo). The configs are Secrets in `dfaas-s3` carrying the label `dfaas.io/s3-config=true` and the keys `endpoint`, `region`, `access_key_id`, `secret_access_key`, `force_path_style`.

**Asset upload.** When a user uploads a payload image, the gateway stores it in the Environment's bucket under `assets/<uuid>-<filename>` and puts a bucket policy that makes the `assets/` prefix anonymously readable on that S3 config. The k6 runners fetch it anonymously over the SeaweedFS S3 NodePort 30900. Whether the shipped SeaweedFS chart honours that policy has not been verified ([#40](https://github.com/isired01/DFaaSOperator/issues/40)). Every upload returns an absolute `url`, built at upload time. An upload to `seaweedfs-default` also returns `relocatable: true` and the object's `path` (`/<bucket>/<key>`). The SPA stores the path as `payloadImagePath`, and the generated script fetches `DFAAS_ASSET_BASE` plus the path when the operator injected that variable on the runner, and the baked `url` otherwise. That `url` is `SEAWEEDFS_PUBLIC_URL` on the gateway, else a node IP, on port 30900. It is therefore a fallback, and only runners without `DFAAS_ASSET_BASE` need to reach it. `SEAWEEDFS_PUBLIC_URL` and `SEAWEEDFS_ENDPOINT` apply to `seaweedfs-default` only: an external config keeps its own endpoint and is never relocated. When a generator cannot fetch the image, `k6Generator.js` aborts the script in `setup()`, naming the scenario, the URL it tried and the HTTP status.

SeaweedFS is a Helm release (the official chart in `allInOne` mode) installed by the operator, so its in-cluster Service is `seaweedfs-all-in-one.monitoring.svc.cluster.local` (8333 for S3, 8888 for the filer) while the NodePorts 30900 and 30901 are fixed. The gateway needs no DNS knowledge: it reads `endpoint` off the `seaweedfs-default` Secret. Operator side, every address comes from `internal/controller/monitoring/addresses.go`, and a test pins the embedded chart values against those constants. Gateway side, the equivalent rules are the methods of `assetAddressing` in `internal/api/assets.go` and `results.go` (UI repo).

**Result links.** The links the browser opens use `SEAWEEDFS_FILER_PUBLIC_URL`, else the host the browser opened the UI on with port 30901 (only when the gateway runs in a Pod; `X-Forwarded-Host` is never read), else a node IP when that host is loopback or unusable or when the gateway runs outside the cluster.

### Bucket and key layout

The exporter and the gateway must agree on where results are. The gateway rebuilds the links, so the bucket name and the key prefixes are a contract:

- bucket: `bucketNameFor(envName, envUID)` in `dataExporter/main.go`, rebuilt by `bucketNameFor` in `internal/api/assets.go` (UI repo): the lowercased Environment name with characters outside `[a-z0-9-]` replaced by `-`, at most 56 characters, then `-` and the first 6 characters of the UID;
- keys: `metrics/<loadtest>/...` and `k6/<loadtest>/...` (the exact names are in [architecture.md](architecture.md#export)). The gateway links to the two directories, `.../buckets/<bucket>/metrics/<loadtest>/` and `.../buckets/<bucket>/k6/<loadtest>/`, because the file names carry a timestamp.

Change either one and the result links break without any error.

## Synchronized start (`spec.syncStart`)

The feature spans both repositories. Change any leg, check the others.

1. **The CRD field.** `syncStart` must exist in the generated CRD, which must be copied into `charts/dfaas/crds/` and applied. Otherwise the API server prunes it and the feature silently does nothing.
2. **The DTO mirror.** `SyncStart` in `internal/api/types.go` (UI repo), and the SPA's default (on with two or more generators).
3. **The script and the filer.** `ui/src/lib/k6Generator.js` emits, in `setup()`, a loop that polls `__ENV.DFAAS_SYNC_URL` every 250 ms until it answers 200. The operator injects that variable into each runner and publishes the GO object on the SeaweedFS filer once every remote TestRun is at stage `started`. The filer is authless, which is why it carries the signal: the operator PUTs through the in-cluster Service, the generators GET through NodePort 30901, and no S3 client or bucket policy is involved. The object lives at `/dfaas-sync/<namespace>/<loadtest>.go`, outside `/buckets`, so no bucket appears. Each generator must reach the management node on port 30901 at the URL the operator built for it: on its detected management address, else on `DFAAS_SYNC_PUBLIC_URL`, else on `HOST_IP`.

A script generated before the feature has no barrier and starts unsynchronized, whatever the CRD says. A `syncStart` test fails before anything is applied when some generator resolves no GO URL, and the message names those generators. Right after dispatch, the operator probes each generator's own summary URL from that generator with a short-lived Pod, and when one cannot reach it, keeps `K6Dispatched` `True` with the reason `DispatchedUnreachable`, naming the generator, the URL and the error; the SPA shows it as a warning. A `SyncTimeout` later repeats the message. The barrier's timing and failure rules are in [architecture.md](architecture.md#synchronized-start); what it does not guarantee is in [known-limitations.md](known-limitations.md#syncstart-does-not-align-runners-to-within-a-fraction-of-a-second).

## Per-generator management address

The management address is the address of the management node as one generator sees it. Every URL that generator's runners dial back is built on it. A deployment spread over several sites has no single address that every generator can route to, but each generator knows the right one: the SSH session that provisions it arrives from the management node. The decision and its consequences are in [ADR-0008](adr/0008-detected-management-address-beats-env-fallbacks.md). The legs are:

1. **The playbook** (`internal/controller/ansible/templates/setup-k6-nodes.yml`, the source of truth). The detection tasks run first, before the role-flip wipe, and take seconds. It reads the first field of `$SSH_CONNECTION` on the generator with `become: false`: the play runs as root through sudo, whose `env_reset` drops `SSH_*`, so the variable has to be read before privilege escalation. It keeps the candidate only if it is a bare IPv4 literal (the generator's k3s is single-stack, so its runner pods cannot dial IPv6 even when a host-network probe would succeed), then probes `http://<candidate>:<filer_node_port>/` from the generator. `filer_node_port` is an inventory host variable set from `monitoring.FilerNodePort`, not a play variable (a play variable would outrank it). The candidate is recorded on any HTTP status or on `Connection refused`; SeaweedFS is installed in `ProvisioningMonitoring`, after this Job, so on a fresh management cluster nothing listens on the port yet. A timeout or no route records nothing. Only 30901 is probed, never 30900. Detection never fails the play. The server-side-apply push of `<env>-<nodeID>-kubeconfig` always writes the annotation `dfaas.io/management-address`, empty when nothing was recorded, so a re-run overwrites a stale value. The Go constant is `ansible.AnnotationKubeconfigManagementAddress`, hand-synced with the playbook and checked by `internal/controller/ansible/k6_playbook_test.go`, which parses the embedded playbook and pins the task shapes, their order, the annotation key and that no failing task depends on the probe.
2. **The status.** `syncNodeStatus` reads each generator's Secret at the end of `ProvisioningMonitoring`, validates the annotation with `k6dispatch.ParseManagementAddress` (a bare IP, no zone, port or brackets; no unspecified, loopback, multicast or link-local address; canonical form) and writes `status.k6Nodes[].managementAddress`, an optional CRD field (at most 45 characters). A value that fails validation is logged and dropped. This is a new status field: regenerate, copy the CRDs and apply them before the new operator runs, or the API server prunes it and every generator falls back without notice.
3. **The runner environment.** Per generator, the operator injects `DFAAS_SUMMARY_URL` and `DFAAS_SYNC_URL` on `http://<address>:30901` and `DFAAS_ASSET_BASE` as `http://<address>:30900`. With no address, the filer URLs use `DFAAS_SYNC_PUBLIC_URL`, else `http://$HOST_IP:30901`, and no `DFAAS_ASSET_BASE` is injected (there is no `HOST_IP` fallback for the asset base: an address that only one site reaches would override the URL the gateway baked).
4. **The script.** The gateway returns `relocatable` and `path` on an upload to `seaweedfs-default`, the SPA stores `path` as `payloadImagePath`, and `k6Generator.js` fetches `DFAAS_ASSET_BASE` plus the path when the variable is set, the baked URL otherwise. Scripts and drafts made before the feature have no path and keep the baked URL.
5. **The DTO.** `K6NodeStatus.ManagementAddress` in `internal/api/types.go`, copied in `mapEnvDetail` and shown on the generator's node card. It is display only.

The rule: a detected and verified address wins over every environment variable. The variables apply only to generators whose address is absent or unverified. An Environment that was `Ready` before the feature existed gets its addresses on its next provisioning run, that is, after a spec edit. After the first provisioning, check:

```bash
kubectl get environment <env> -o jsonpath='{.status.k6Nodes[*].managementAddress}'
```

An empty result means the fallbacks apply. The provisioning Job logs `recorded <addr> as the management address` for each generator that was detected. The detection is covered by static playbook tests only and has not been run against a real multi-site setup ([known-limitations.md](known-limitations.md#management-address-detection-has-no-recorded-run), [#41](https://github.com/isired01/DFaaSOperator/issues/41)).

## perNodeLoad vus and duration

`perNodeLoad[].vus` and `.duration` never reach k6. The runner obeys `options.scenarios` inside the generated script, and the operator sets no TestRun field for either. `duration` survives as the total for the SPA's per-generator progress bar, so the form derives it from the scenarios instead of asking the user: `perNodeTotalMs` in `ui/src/lib/scenarios.js` takes the maximum over the scenarios of the start time plus the executor's own length (the sum of the stages for `ramping-arrival-rate`, the `duration` for `constant-arrival-rate`). The SPA sends `vus: 1`, the CRD minimum.

If the generator lays out scenarios differently, or an executor is added, its `durationMs` entry in `scenarios.js` must still total it. If the operator ever gives `duration` a real use, the UI must stop treating it as advisory.

## Condition reasons

The operator writes Condition messages (for example the name of the failing Ansible task) that the SPA renders verbatim in `ui/src/components/ConditionsList.jsx`. There is no dedicated CRD field; change the message format here and the panel changes with it. The UI cannot import the operator's constants, so it matches strings. These couplings ride on reason strings rather than message text:

- The SPA looks up the abort explanation by `Ready` with the reason `UserAborted`.
- The curated reason table in `ui/src/lib/crstate.js` must give every reason that means "dead" the tone `error`. A terminal reason that is not in the table curates to `neutral`, and then the provisioning row spins forever on a real failure.

Both sides are enforced against one hand-copied list. Operator side, `api/v1/inventory_test.go` classifies every reason constant in its `terminalFailure` table and fails on an unclassified or never-stamped one. UI side, `ui/src/lib/crstate.selfcheck.mjs` asserts that every `true` row is `error`; that `CheckFailed` and `JobCreationFailed` stay `error` (they are retried without bound and need a human); and that the reasons the operator keeps retrying (`RunnersUnreclaimed`, `FetchFailed`, `ApplyFailed`, `StaleCleanupFailed`, `ScriptMirrorFailed`) are not. To add a terminal reason, add it to both lists. To rename a reason, update both by hand.

Related couplings in the same family:

- **k6-operator stage names** (`classifyStage` in `internal/controller/loadtest_fleet.go`: `started`, `finished`, `stopped`, `error`) are mirrored by `ui/src/lib/crstate.js` and `GeneratorProgress.jsx`.
- **The dfaas-agent Helm release name** is `dfaas-agent` (`setup-nodes.yml`). The SPA's default metrics select its pods with `pod=~"dfaas-agent.*"` (`MetricsEditor.jsx`); a selector that matches no series exports an empty column without an error.
- **Prometheus label keys** `node_id` and `node_type` are written on every scrape target by `ReconcileTargets` and read back by the exporter from each series.

## The Ready gate, on both sides

`Ready` alone is the dispatch gate: one implementation per side, plus the SPA's display mirror. Operator: `EnvironmentPhase.Dispatchable()` in `api/v1/environment_types.go`, table-tested over every phase the enum declares. Gateway: `AdmitLoadTest` in `internal/api/dispatchgate.go` (UI repo), which both create paths call (the structured one and the YAML import). The SPA mirror is `crstate.env.dispatchable`. The UI must not be stricter than the operator. The two tables are the contract, not this prose; change the set on one side and change the other. `Unreachable` is not dispatchable even though it is non-terminal and recovers on its own: a remote apply would fail against a machine that stopped answering port 22.

A draft or a scheduled test skips the phase gate on both sides, so it may be created against an Environment that is still provisioning.

## startAt requires suspended

`spec.startAt` requires `spec.suspended=true`, enforced on both sides. The gateway rejects it with a 400 before writing anything; the operator fails such a LoadTest at admission (a `kubectl apply` reaches that path). It is deliberately not a CRD CEL rule, because that would reject the operator's own fire-time patch and freeze every scheduled test. The gateway's Activate ("Start") patches `suspended=false` and `startAt=null` together, so Start means start now.

The Environment edit and delete guard in the gateway (`activeLoadTestNames`, on every PATCH and DELETE) and the SPA's `lt.occupying` count as occupying: a test in `Running` or `Exporting`, a non-suspended test at `""` or `Pending`, and a terminal test with `K6Healthy=RunnersUnreclaimed`. The operator's `envOccupancyGate` uses the same set, and both read the summary's `runnersUnreclaimed` flag.

## Validation mirrors

The CRD enforces: a Go-duration pattern on `perNodeLoad[].duration` and `metricsExport.step`; `Minimum=1` on the `Function` tuning fields; `listMapKey=nodeID` uniqueness on `spec.nodes[]` and `spec.perNodeLoad[]`; unique `ipAddress` within `spec.nodes` (a CEL rule), with `MaxItems=50` on the node list and `MaxLength=45` on `ipAddress` (these two exist only to keep the CEL rule inside the CRD cost budget and move together with it); the function-name pattern `^[a-z0-9]+$`; "a `dfaas-worker` must declare at least one function"; and required username, password and function image.

The gateway's rule set (`internal/api/schema.go`, UI repo) duplicates them by hand, and the SPA's payload builders read the served rules, so a user gets an inline error instead of a raw 422 from the API server ([ADR-0001](adr/0001-gateway-owns-the-validation-rule-set.md)). Both LoadTest create paths go through one `validateLoadTest` and then one `admitAgainstEnvironment`. The sign check on the tuning fields is in the gateway only and is not served: a negative value is rejected, and a 0 is omitted so the CRD default applies. The CRD is the source of truth. The list and the reason for each rule are in [development.md](development.md#what-the-api-server-rejects).

## Export cool-down and the metric windows

The operator waits one minute after k6 finishes before exporting ([architecture.md](architecture.md#export)). The federation interval it is tied to is `server.global.scrape_interval` in `internal/controller/monitoring/values/prometheus-values.yaml`. On the UI side, the preset metrics in `MetricsEditor.jsx` use `[5m]` rate windows, which must stay at least four times that interval. Change the interval and check them.

## Resource names

A LoadTest name is a label value (`dfaas.io/loadtest` on the gateway's script ConfigMaps, `dfaas.io/loadtest-name` on the operator's remote TestRuns and log ConfigMaps), so the gateway rejects a user-set name longer than 63 characters and `generatedLoadTestName` cuts `lt-<env>-<timestamp>[-<suffix>]-<nonce>` to 63, keeping the nonce. The operator does not check it. The remaining limits are in [known-limitations.md](known-limitations.md#resource-names-63-bytes-for-a-loadtest-51-for-a-testrun).

The script ConfigMap that the gateway writes for each `perNodeLoad` entry is named `<loadtest>-<nodeID>-script` (sanitised) in the LoadTest's namespace; `perNodeLoad[].scriptConfigMap` references it, and the key is `script.js`.

## Release coupling

Both repositories are released under the same tag. The chart defaults every image tag, the UI's included, to its `appVersion`. See [releasing.md](releasing.md).
