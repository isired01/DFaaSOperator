# Known limitations

This is the list of what to know before trusting a measurement from this system, before exposing it, and before operating it. Each entry says what happens and what to do about it. Known defects link their GitHub issue.

The headings are stable: other documents link to them.

- [Security model](#security-model)
- [Validity of a measurement](#validity-of-a-measurement)
- [Operational limits](#operational-limits)

## Security model

The system is lab software. It assumes a trusted network and trusted users. The gaps below are design decisions of the lab setting, not oversights, and none of them is fixed by configuration alone.

### The gateway has no authentication

The UI gateway installs no authentication; its only middleware is CORS. The chart exposes it on NodePort 30800 (`ui.service.type: NodePort`). Anyone who can reach that port can:

- read every Environment, including the SSH username and password of every node (`GET /api/environments/<namespace>/<name>` and the YAML export return them);
- create, edit and delete Environments, which makes the operator run Ansible as root against any address in the request;
- create and start LoadTests, register S3 configs with credentials of their choice, and upload objects that become anonymously readable.

With `CORS_ORIGINS=""` (the chart default) the gateway adds no CORS headers. That is not a same-origin check: a "simple" cross-origin POST (a form post or a `fetch` with `Content-Type: text/plain` or `multipart/form-data`) is not preflighted and reaches the handlers, so a web page that a user opens in the same browser can create Environments and LoadTests. Closing this is tracked in [DFaaS_UI#12](https://github.com/isired01/DFaaS_UI/issues/12).

What to do: do not expose the UI on an untrusted network. Use `--set ui.service.type=ClusterIP` and `kubectl -n dfaas-ui port-forward svc/dfaas-ui 8082:8082`, or put an authenticating reverse proxy in front.

### Credentials are stored in the custom resources

SSH passwords are stored in plain text in `Environment.spec.nodes[]`. The operator copies them into the per-generation inventory Secret `<env>-ansible-<suffix>-<uid8>-g<generation>-inventory` that the Ansible Job mounts; stale-generation inventory Secrets are deleted on a spec edit. Each generator's k3s admin kubeconfig is stored in the Secret `<env>-<nodeID>-kubeconfig` in the Environment's namespace. The libp2p private keys of the DFaaS nodes are in `<env>-libp2p-keys`. Compromising one generator's kubeconfig gives admin on that generator's k3s only.

What to do: restrict who can read Secrets and Environments in the Environment's namespace; use dedicated VMs and throwaway passwords.

### Creating an Environment is a privileged action

The node fields (`ipAddress`, `username`, `password`, function names) are written unescaped into an Ansible INI inventory that runs as root on the VMs, and the CRD puts no format validation on them. A value with whitespace, `#` or quotes corrupts the inventory line or injects inventory variables. SSH runs with host-key checking off, and k3s, Helm and `faas-cli` are installed with `curl | sh`. The Ansible worker pod has `get/list/create/update/patch` on Secrets in the Environment's namespace.

What to do: treat "who can create or edit an Environment" as a security boundary. Do not use whitespace, `#` or quotes in node usernames and passwords (a fix is tracked in [#36](https://github.com/isired01/DFaaSOperator/issues/36)).

### Services on the machines are open

| Where | What | Protection |
| --- | --- | --- |
| Each DFaaS node | OpenFaaS gateway (NodePort 31112) | `basic_auth` is off |
| Each DFaaS node | HAProxy Data Plane API (30555), runtime API (30666), and the other HAProxy NodePorts 30443, 30405, 31024 | The Data Plane API uses a static user and password written in `ansible/templates/haproxy-values.yaml`; the runtime API has none |
| Each DFaaS node | Prometheus (30909) | none |
| Management node | Grafana (30300) | every visitor is an anonymous `Admin`; the login form is off |
| Management node | SeaweedFS filer (30901) | none: every exported CSV, summary and log is readable and writable |
| Management node | SeaweedFS S3 (30900) | static keys written in `monitoring/values/seaweedfs-values.yaml` and in the `seaweedfs-default` Secret; payload images under `assets/` are anonymously readable |
| Management node | Prometheus (30090) | none |

What to do: keep these ports on a trusted network segment. Change the static keys if the install is shared.

### The ClusterRoles are broader than needed

The UI's ClusterRole (`charts/dfaas/templates/ui-rbac.yaml`) can create, read and delete every Secret and ConfigMap in the cluster; the gateway uses Secrets only in `dfaas-s3` and ConfigMaps next to LoadTests. The operator's ClusterRole carries scaffold verbs that no code uses. Narrowing them is tracked in [#35](https://github.com/isired01/DFaaSOperator/issues/35) (UI) and [#48](https://github.com/isired01/DFaaSOperator/issues/48) (operator).

### The images must be public

`imagePullSecrets` in the chart reaches only the operator and UI pods. The exporter Jobs and the VMs pull without credentials, so the operator, exporter, UI, imgproc and dfaas-agent images and the agent chart under `ghcr.io/isired01` must be public ([releasing.md](releasing.md#ghcr-packages)).

## Validity of a measurement

### `Ready` does not mean the functions serve

The Environment's health probe is a TCP dial to port 22 on every node and nothing else. `K6Healthy` reads the TestRun stage, not k6's failure rate, and generated scripts set no thresholds. An Environment can therefore be `Ready`, and a LoadTest can end `Completed`, while the system under test serves nothing. A DFaaS node's HAProxy can stay on its placeholder configuration (a static 503 response) until the agent pushes the real one, and a re-provisioning does not repair it when the agent release is unchanged.

What to do before a run you intend to keep:

```bash
curl -s http://<dfaas-node-ip>:30080/
```

The body `This is a DFaaS node. Proxy is running, but the DFaaS agent is not!` means HAProxy still serves the placeholder. Check the Grafana panel "Agent strategy iterations": 0 means the agents stopped recalculating. Check that traffic reaches the neighbours. To restart an agent, run this on each DFaaS node, in `spec.nodes` order, about 20 seconds apart:

```bash
sudo k3s kubectl -n default rollout restart deploy/dfaas-agent
```

A service-level probe is tracked in [#39](https://github.com/isired01/DFaaSOperator/issues/39).

### The agent mesh is exactly the bootstrap list

Each DFaaS node dials every DFaaS node listed before it in `spec.nodes`, once, when its agent starts (`buildInventory` in `ansible/job.go`). The agents never re-dial a dropped peer, and the Kademlia discovery that was meant to complete the mesh does not return a usable peer: the agent announces only its pod address, which is in the same subnet on every single-node k3s. A link that drops stays down until the agents are restarted, and no Condition changes. The symptom is a load balancer that answers "no other DFaaS nodes available" instead of offloading, with every Environment Condition green. A fix exists but is not shipped in any agent image; [#34](https://github.com/isired01/DFaaSOperator/issues/34) carries it ([dfaas-agent.md](dfaas-agent.md)).

What to do: put the most reliable machine first in `spec.nodes`. Reordering `spec.nodes` re-provisions every node. Before a measurement, check that each agent has the peers you expect (the agent logs the connected peers), and restart the agents if a link was lost. Wait a few minutes after an Environment edit before a run you intend to keep: the agents and the per-node Prometheus restart, so the first minutes are not representative.

### Percentiles are per generator

The k6 summary CSV has one row per generator, metric and statistic. Counts and rates can be summed over `node_id`. The median, `p(90)` and `p(95)` of different generators cannot be combined into a run-wide percentile. A run-wide percentile would need k6 to stream raw samples into Prometheus, which is not implemented.

What to do: report percentiles per generator, or run a single generator when a run-wide percentile matters.

### syncStart does not align runners to within a fraction of a second

The operator publishes the GO signal once every TestRun reports the k6-operator stage `started`. That stage is a pod-level fact and can come before the script's `setup()` reaches the barrier (a runner may still be fetching a payload image). The only measurement on record, with two generators, showed a start skew of about 3 s with `syncStart` on, and about the same with it off. It was one lab measurement. Do not assume sub-second alignment.

What to do: use `syncStart` with several generators, and assume the runners begin a few seconds apart. When alignment matters, compare the first requests of each generator in the data. A fix that makes runners report in before the signal is tracked in [#43](https://github.com/isired01/DFaaSOperator/issues/43).

### syncStart barrier polls count in `http_req_failed`

With `syncStart`, the generated script's `setup()` polls the GO URL with an ordinary `http.get`, four times a second per runner, and the answer is 404 until the signal is published. k6 counts a 404 as a failed request by default, and the poll has no tag, so every poll adds to `http_reqs`, `http_req_duration` and `http_req_failed` in the exported summary. The payload image fetch in `setup()` does the same on a smaller scale. `syncStart` is on by default when a test has two or more generators. One lab run read around 10% failed requests in the aggregate against about 0% on the load requests.

What to do: do not read the load's failure rate from `http_req_failed` in such a run. Scripts generated by the UI call `check(res, {'status is 2xx': ...})` on the load requests only, so read the failure rate from the `checks` metric. The fix (tagging the poll and declaring 404 expected) is tracked in [DFaaS_UI#13](https://github.com/isired01/DFaaS_UI/issues/13).

### A raw script without handleSummary exports no k6 numbers

Only scripts generated by the UI end with a `handleSummary` that PUTs the end-of-test summary to `DFAAS_SUMMARY_URL`. A raw script (the form's raw mode, a script POSTed to the API, a YAML import, `kubectl apply`) is sent verbatim, and usually has no such function. The exporter then finds no summary, and the k6 summary CSV holds only `dfaas_summary_fetched` rows with value `0`. The LoadTest still ends `MetricsExported=True` with the reason `ExportSucceeded`: that reason is stamped on any successful exporter Job. Nothing in the status says the k6 numbers are missing, and the Prometheus part of the export looks complete.

What to do: add a `handleSummary` that PUTs `JSON.stringify(data)` to `__ENV.DFAAS_SUMMARY_URL` (see `config/samples/dfaas_v1_loadtest.yaml`), and after every run check that `k6/<loadtest>/summary-<stamp>.csv` has `dfaas_summary_fetched` equal to `1` for each generator. A warning reason for this case is tracked in [#44](https://github.com/isired01/DFaaSOperator/issues/44).

### A test whose runners abort in setup() still ends Completed

When a payload image cannot be fetched, the generated script calls `exec.test.abort()` in `setup()`, with a message that names the scenario, the URL it tried and the HTTP status. That message lands only in the runner's log on the generator. The operator does not read the runner's exit status or its log to decide the outcome. The code path is short: `observeK6` polls each TestRun's `.status.stage`, `classifyStage` sorts it into pending, started, done (`finished`, `stopped`) or errored, and the test moves from `Running` to `Exporting` unless a TestRun reports `error` (`K6Healthy=AllFinished`, then `Exporting`, then `Completed`). A runner that aborted in `setup()` is not reported as `error` by k6-operator, so nothing in this path fails. What a user sees:

- a plain test goes `Running`, `Exporting`, `Completed`, with `K6Healthy=AllFinished` and `MetricsExported=ExportSucceeded`;
- a test with `syncStart` fails with "runner on node ... finished before the GO signal", and the message does not name the failed fetch.

Either way the user sees a finished test with no load, or a generic synchronization error, and not the cause.

How to detect it: read the runner's log. It holds the abort message with the scenario, the URL and the status. After the run it is the object `k6/<loadtest>/<nodeID>-<stamp>.log` in the sink (the "k6 logs & summaries" link of a completed test in the UI) and, until the test is deleted, the ConfigMap `<loadtest>-k6log-<nodeID>` in the LoadTest's namespace. Also look at the k6 summary CSV: a generator that sent no load has no, or an empty, set of load-request metrics, or `dfaas_summary_fetched` is `0` for it. Never treat `Completed` alone as proof that load was sent; the measured request counts are the proof.

What to do: before a campaign, run one short test with the payload and read the runner log. If the fetch fails, the usual causes are an address that the generator cannot reach (check `status.k6Nodes[].managementAddress`, and `SEAWEEDFS_PUBLIC_URL` on the gateway for a generator without one) or an image that was uploaded before the address was fixed (re-upload it). Making the operator stamp a failure for this case is tracked in [#45](https://github.com/isired01/DFaaSOperator/issues/45).

### Mirrored S3 secrets are never deleted

To export, the operator copies the S3 config Secret from `dfaas-s3` into the LoadTest's namespace under the same name. Nothing deletes the copy, and it holds the S3 access key and secret key. It carries no owner reference on purpose, because it is shared by every LoadTest in the namespace that uses that config.

What to do: delete the copies by name when you no longer need them. Do not run a label-selector delete (`dfaas.io/s3-config=true`) in `dfaas-s3`: the same label marks the registry Secrets there, `seaweedfs-default` included. A cleanup is tracked in [#50](https://github.com/isired01/DFaaSOperator/issues/50).

### Uploaded payload assets are never deleted

An uploaded payload image stays in the Environment's bucket under `assets/` and stays anonymously readable after its LoadTest is deleted. What to do: delete the objects in the sink yourself. A cleanup is tracked in [DFaaS_UI#17](https://github.com/isired01/DFaaS_UI/issues/17).

### The export window after an operator outage

`status.endTime` is stamped when the operator sees every runner finished, within about 5 s. After an operator outage it is the time of the restart, and the export window grows to it. The management Prometheus keeps 7 days or 16 GB; after that a run survives only in its CSVs. Take the run length from the raw summary JSON (`state.testRunDurationMs`) where the script exports one.

### The clocks must agree

The operator stamps `status.startTime` and `endTime` from the machine it runs on, while Prometheus runs in the management cluster. If the two clocks disagree (a VM that was suspended can be hours behind), the exporter queries a window the cluster has no data for and the test fails with "metrics export produced 0 data points". A DFaaS node's clock matters too: `apt` rejects release files dated in the future.

What to do: check `date -u` on the operator host (under `make run`), the management node and every VM before a session. Use NTP.

### Rate windows must hold two federated samples

`rate()` over a window that holds fewer than two federated samples returns nothing. The management Prometheus pulls each node once per federation interval (10 s), so use windows of at least four intervals; the preset metrics use `[5m]`. A query that matches nothing does not fail the export: it appears as an empty row in `metrics/<loadtest>/query-status-<stamp>.csv`. The export fails only when every query is empty.

### Payload size, not request rate, saturates the generator

A payload image is fetched once per runner, in `setup()`, held in the runner's memory and POSTed in full on every request. A large image saturates the generator and the DFaaS node before the system under test is the bottleneck. One lab measurement on a two-node setup with `dfaas-imgproc`: a 210 KB PNG at 30 requests per second failed up to about half of the requests, a 1.1 KB JPEG at 60 per second about 0%.

What to do: use small payloads, and check the failure rate (from `checks`) before comparing runs.

### Balancing strategies

Supported: `staticstrategy`, `alllocalstrategy`, `recalcstrategy` (which needs each function's `maxRate`) and `randomstrategy`. `nodemarginstrategy` and `rlagentstrategy` pass CRD validation but are not supported: they may need function labels the operator does not emit.

### perNodeLoad vus and duration are advisory

Neither field reaches k6; the script's own options decide the load and its length ([cross-repo-contract.md](cross-repo-contract.md#pernodeload-vus-and-duration)). A raw script's real duration is whatever the script does; the `duration` field only drives the progress bar.

## Operational limits

### Resource names: 63 bytes for a LoadTest, 51 for a TestRun

- **Job names are capped at 63 bytes, not 253.** Kubernetes copies a Job's name into the auto-generated `job-name` pod-template label, and label values cap at 63. `ansible.BoundedJobName` truncates the prefix and keeps the UID and generation suffix; the Ansible Jobs and the exporter Job both use it. A Job name built without it can pass 63, is rejected at creation, and the LoadTest then waits in `Exporting` forever with nothing surfaced.
- **A LoadTest name is capped at 63 characters, by the gateway only.** The name is copied into the label `dfaas.io/loadtest-name` on the remote TestRuns, probe Pods and log ConfigMaps, and label values cap at 63. The gateway rejects a longer user-set name; a generated name `lt-<env>-<timestamp>[-<suffix>]-<nonce>` is cut to 63, keeping the nonce. The operator and the CRD do not check it, so a LoadTest made with `kubectl` and a longer name fails dispatch (`ApplyFailed`, then `DispatchFailed`).
- **A remote TestRun name is capped at 51 bytes.** k6-operator derives its Job names (`<testrun>-initializer`, `-starter`, `-<n>`) and the pod label `k6_cr=<testrun>` from the TestRun name, so `k6dispatch.TestRunName` (`<loadtest>-<sanitized nodeID>`) caps it at 63 minus `len("-initializer")`. A longer name keeps its head and ends in an fnv32a hash (8 hex digits) of the full name; a shorter one is unchanged. The 51 is read off k6-operator v0.0.15 (chart `3.7.0` in `setup-k6-nodes.yml`); `TestK6ChartVersionIsPinned` fails if the chart moves. A `.` in the LoadTest name becomes `-` (the runner pod's hostname `<testrun>-1` cannot contain a dot) and forces the hash, so `exp.1` and `exp-1` stay apart.
- **A LoadTest name that starts with a digit may break the runner Service.** k6-operator creates a Service named `<testrun>-service-<n>`, which must be a DNS-1035 label, and such a label cannot start with a digit. The gateway admits a leading digit. Whether the generator's k3s rejects it depends on its version; a rejected Service leaves the TestRun at stage `initialized`. This was not reproduced on a cluster.

What to do: start LoadTest names with a letter and keep them at 63 characters or fewer. A check in the operator is tracked in [#42](https://github.com/isired01/DFaaSOperator/issues/42).

### Management address detection has no recorded run

The per-generator detection ([cross-repo-contract.md](cross-repo-contract.md#per-generator-management-address)) is covered by static tests of the playbook only. No test or workflow runs `ansible-playbook`, and the feature has not been run against a real multi-site setup ([#41](https://github.com/isired01/DFaaSOperator/issues/41)). Detection never fails the play, so a broken detection looks like a working one until a runner cannot reach the management node.

What to do: after the first provisioning, run `kubectl get environment <env> -o jsonpath='{.status.k6Nodes[*].managementAddress}'`. An empty value means the fallbacks (`DFAAS_SYNC_PUBLIC_URL`, `HOST_IP`) apply. On a multi-site setup, run a `syncStart` test with an image payload and check `DFAAS_SYNC_URL` and `DFAAS_ASSET_BASE` in the runner's environment.

### Keep one Environment inside one site

All the nodes of an Environment should be in one network site and addressed by stable IP addresses. The operator and the management node are the only things that cross sites. The libp2p mesh of the DFaaS nodes forms from the addresses in `spec.nodes`, and the dfaas-agent publishes the node's registered k3s `node-ip` (the playbook pins it to `ipAddress`) to its peers as "forward requests here".

### Provisioning needs internet access and installs unpinned software

Every run needs outbound access: the Ansible Job pods download `apk`, PyPI and Ansible Galaxy packages (the collections are pinned in `templates/requirements.yml`, the `kubernetes` pip package is not); every VM downloads from the distribution `apt` mirrors, `get.k3s.io`, GitHub, `raw.githubusercontent.com`, `cli.openfaas.com`, the HAProxy, Prometheus, OpenFaaS and (on the generators) grafana chart repositories and the image registries; the management cluster pulls from `ghcr.io`, `docker.io` and `quay.io`. A Job has four attempts (`BackoffLimit: 3`), so a short DNS outage can use them up; a spec edit retries. On freshly created VMs the first provisioning can also lose an attempt to a gateway rollout race ([#38](https://github.com/isired01/DFaaSOperator/issues/38)).

Two runs of the same Environment do not build the same stack: k3s and Helm are installed at whatever version is current when missing; the chart repositories are re-added on every run and the HAProxy, Prometheus and OpenFaaS releases move to their latest chart versions; `faas-cli` is the latest; the dfaas-agent image is the mutable `:dev` tag with `pullPolicy: Always`; the k6 runner image is k6-operator's `latest-runner` (the `k6-operator` chart itself is pinned at `3.7.0`).

What to do: pin versions in `setup-nodes.yml` (the variables `k3s_version`, `helm_version`, `haproxy_chart_version`, `prometheus_chart_version`, `openfaas_chart_version` at the top; empty means latest). The playbooks are embedded in the operator binary, so a change needs a new operator image and a re-provisioning. `setup-k6-nodes.yml` has no k3s or Helm pin. Record the versions of a measurement campaign. Pinning is tracked in [#37](https://github.com/isired01/DFaaSOperator/issues/37).

### The machines are taken over

Provisioning disables UFW and the APT automatic upgrades, installs k3s, Helm and OpenFaaS, and runs `k3s-uninstall.sh` when a node's role changes. Use dedicated VMs.

### Removing a node, or deleting an Environment, does not clean the machine

Removing a node from `spec.nodes` does not uninstall anything: the machine keeps k3s, OpenFaaS, HAProxy and the dfaas-agent. A removed DFaaS node drops out of Prometheus but keeps its existing libp2p links to the DFaaS nodes listed before it. A runner already started on a removed generator keeps loading until its script ends: an unusable generator is named at the run end, never held. Deleting an Environment removes its scrape targets, kubeconfig Secrets and per-Environment objects, and nothing on the VMs.

What to do: wipe a machine with `sudo /usr/local/bin/k3s-uninstall.sh`.

### Uninstalling leaves the monitoring stack and the VM software

`helm uninstall` removes the operator and the UI, not the CRDs, and not the Prometheus, Grafana and SeaweedFS releases and the `monitoring` and `dfaas-s3` namespaces that the operator installed at run time. SeaweedFS holds every exported result. The procedure is in [releasing.md](releasing.md#uninstalling) and in the README.

### Generator kubeconfig certificates expire after a year

The kubeconfig in `<env>-<nodeID>-kubeconfig` carries the k3s admin client certificate, valid for 365 days. k3s renews it only when it starts and the certificate is within 120 days of expiry, and the playbook restarts k3s only for a TLS SAN change. About a year after a generator was installed, every remote call to it fails with an x509 error. This has not been reproduced. A fix is tracked in [#46](https://github.com/isired01/DFaaSOperator/issues/46).

What to do: before the year is out, restart k3s on the generator (`sudo systemctl restart k3s`), then edit the Environment spec so that provisioning copies the new kubeconfig into the Secret.

### Grafana keeps no state

Grafana has no volume. A dashboard edited in the Grafana UI is lost at the next restart. Change the shipped dashboard in `internal/controller/monitoring/dashboards/dfaas-live.json`.

### The management cluster needs a default StorageClass

Prometheus claims 20Gi and SeaweedFS 10Gi, both `ReadWriteOnce`. Without a default StorageClass the Environment waits in `ProvisioningMonitoring` with "monitoring pods not Ready yet" (Helm runs with `Wait=false`). k3s ships `local-path`; a kubeadm cluster has none.

### One reconcile at a time

Each controller handles one object at a time, and the remote rounds poll generators one after another with a 10 s request timeout. K unreachable generators block the worker for K timeouts per round. Two different Environments can declare the same machine, which is destructive (the function prune wipes the other Environment's functions); the CRD can check uniqueness within one Environment only.

### Unbounded waits

A few waits have no bound: a stale TestRun that does not disappear from a generator is rechecked every 3 s forever; `JobCreationFailed` is retried with controller-runtime backoff without a limit; the pacing of failed remote rounds is kept in memory, so after an operator restart one round can come early. The exporter Job has a fixed 10 minute ceiling, and an Ansible Job a fixed 30 minute ceiling.

### Small status inconsistencies

A scheduled test that is started with "Start" keeps the stale `Scheduled=True/ScheduledArmed` Condition while it runs and after it ends. A `spec.stop` that arrives just as a test moves from `Running` to `Exporting` can, on a stale cache read, end the test as `Aborted` although it had collected its data. They are tracked in [#52](https://github.com/isired01/DFaaSOperator/issues/52) and [#47](https://github.com/isired01/DFaaSOperator/issues/47).
