# Glossary

The words below are used the same way in the operator, the gateway and the SPA. Paths without a prefix are in this repository; paths marked "UI repo" are in [DFaaS_UI](https://github.com/isired01/DFaaS_UI). Each entry ends with the words to avoid for that concept.

## Resources

**Environment**
The federation as declared: a set of machines with roles, plus the S3 sink. One custom resource (`dfaas.dfaas.io/v1`), one object in the management cluster, one Ansible provisioning run per spec generation.
Avoid: cluster, federation (the libp2p mesh the agents form is a consequence of an Environment, not the object), deployment.

**LoadTest**
One k6 experiment against a dispatchable Environment: a script per generator, a metrics export, and a lifecycle from `Pending` to `Completed`, `Failed` or `Aborted`.
Avoid: test run (that is the remote k6-operator object), job.

**Condition**
A typed status entry on a custom resource (`type`, `status`, `reason`, `message`). The reason strings are a protocol: the SPA curates them in `ui/src/lib/crstate.js` (UI repo). See [architecture.md](architecture.md#conditions-and-reasons).
Avoid: event, flag, state.

**Phase**
The single coarse lifecycle value in a resource's status. A LoadTest at `""` has not been admitted yet; admission writes `Pending` for every test it admits before anything remote happens, and a test that fails a create-time check goes straight to `Failed`. The sets "terminal", "pre-execution" and "dispatchable" are methods on the phase types in `api/v1` (`Terminal`, `PreExecution`, `Dispatchable`), tested against the CRD enum markers. The gateway mirrors "dispatchable" in `internal/api/dispatchgate.go` and the occupying set in `activeLoadTestNames` (UI repo); the SPA keeps its phase sets in `crstate.js`.
Avoid: state, status (that is the whole subresource).

## Machines and roles

**Node**
One machine declared in `Environment.spec.nodes`, identified by `nodeID`, with exactly one role. One machine is one node: the CRD enforces unique `ipAddress` values within an Environment.
Avoid: host, instance. "VM" is fine for the physical thing, not for the declared object.

**DFaaS node**
A node with the role `dfaas-worker`. It runs k3s, OpenFaaS, HAProxy and the dfaas-agent, and it is the target of the load. In prose, say "DFaaS node"; the enum value stays `dfaas-worker`.
Avoid: worker, in prose.

**Generator**
A node with the role `k6-load-generator`. It runs its own single-node k3s plus k6-operator and hosts one remote TestRun per LoadTest that targets it.
Avoid: k6 node, load generator node, runner (that is the k6 pod inside it).

**Role**
Which of the two kinds a node is. The enum values are kebab-case, `dfaas-worker` and `k6-load-generator`; the CRD rejects any other spelling, PascalCase included. Everything provisioning needs to know per role (playbook, inventory group, Job suffix, Condition type) is one row of the role table in `internal/controller/roles` ([ADR-0004](adr/0004-role-table-in-its-own-package.md)).
Avoid: type, kind, mode.

**Repave**
What happens to a node whose role changed. Each playbook records the node's role in `/etc/dfaas-role` as its last task. When a playbook finds a different role there, it runs `k3s-uninstall.sh` and provisions the machine for the new role. The role flip is the only case in which the operator removes software from a machine.
Avoid: reset, reinstall, migration.

**Provisioning stream**
One Ansible Job per role, run in parallel, each stamping its own Condition (`DFaaSNodesReady`, `K6Ready`). The Environment advances when every stream has finished.
Avoid: phase (that word is the resource-level value), stage, step.

**Management cluster**
The Kubernetes cluster that runs the operator, the gateway, Prometheus, Grafana and SeaweedFS, and where every custom resource lives. Distinct from the k3s on each node.
Avoid: control plane (that is the UI product name), central cluster, master.

**Management node**
The node of the management cluster whose NodePorts the generators dial back: the filer on 30901 and the S3 gateway on 30900. On a multi-node management cluster these ports are reachable on every node.

**Management address**
The address of the management node as one generator sees it, and so the base of every URL that generator's runners dial back to SeaweedFS. It is detected and verified from the generator during provisioning and kept per generator in `status.k6Nodes[].managementAddress`. A generator without one falls back to the global addresses the operator and the gateway are configured with ([ADR-0008](adr/0008-detected-management-address-beats-env-fallbacks.md)).
Avoid: host IP (`HOST_IP` is the node the operator pod runs on), public URL (the global fallbacks), node IP (what the gateway reads off Kubernetes Nodes), `ipAddress` (the other direction: how the management cluster reaches the node).

## Remote execution

**Dispatcher**
The seam between the LoadTest reconciler and the generators' k3s clusters. It resolves a node by `nodeID`; everything about how the node is reached (kubeconfig Secret, remote namespace, TestRun name) lives behind it. Two adapters: `Live` in production and `fake.Fleet` in tests ([ADR-0005](adr/0005-dispatcher-name-kept-for-the-thesis.md), [ADR-0006](adr/0006-dispatcher-seam-at-reach-node-n.md)).
Avoid: client, k6 client, remote API.

**Node handle**
What the Dispatcher returns for one generator. Its methods are `Ref`, `MirrorConfigMap`, `DeleteScript`, `Apply`, `Stage`, `Delete`, `Logs`, `StartProbe`, `ProbeResult` and `DeleteProbe`, all addressed by the LoadTest rather than by remote object names.
Avoid: connection, session, remote.

**TestRun**
The k6-operator `k6.io/v1alpha1 TestRun` object on a generator's k3s, with `parallelism: 1`. It is named deterministically from the LoadTest and the node ID (see [known-limitations.md](known-limitations.md#resource-names-63-bytes-for-a-loadtest-51-for-a-testrun)). The gateway never touches it.
Avoid: run, k6 run, test.

**Runner**
The k6 pod that k6-operator starts on a generator for a TestRun.

**Stage**
The remote TestRun's `.status.stage` (for example `created`, `started`, `finished`, `stopped`, `error`). A stage is a fact about one generator; the LoadTest's phase is derived from all of them. One classifier (`classifyStage` in `internal/controller/loadtest_fleet.go`) sorts every stage into pending, started, done (`finished`, `stopped`) or errored; an unknown stage counts as pending.
Avoid: phase (reserved for the custom resources), state.

**Sync barrier, GO signal**
For `syncStart` tests: every runner parks in `setup()` polling a URL on the SeaweedFS filer until the operator publishes the GO object once every TestRun reports stage `started`. See [cross-repo-contract.md](cross-repo-contract.md#synchronized-start-specsyncstart).
Avoid: lock, gate, semaphore.

**Occupancy**
The rule that one Environment runs one non-draft LoadTest at a time; later ones queue in creation order. A suspended (draft) test occupies nothing. A terminal test whose run end could not delete a runner it applied (`K6Healthy` reason `RunnersUnreclaimed`) keeps occupying until the delete succeeds or the test is deleted.
Avoid: lock, mutex, busy.

**Run end**
The single place where a LoadTest reaches `Completed`, `Failed` or `Aborted` (`endRun` in `internal/controller/loadtest_end.go`). It deletes every TestRun that may still be live, sweeps the filer objects, and writes phase and Conditions as one Transition. The teardown is one bounded pass; what it could not delete it names on `K6Healthy`.
Avoid: cleanup, finish, teardown (that is one step of it).

**Fleet round**
One poll of every generator a LoadTest runs on, per reconcile. Observing and the sync barrier both work from one round. The observe retry counter is charged once per round, never per generator, and a failed round paces the next attempt.
Avoid: poll, sweep.

**Provisioning generation**
The `metadata.generation` that a provisioning run applies. It is recorded as `status.provisioningGeneration` when the run starts and copied into `status.observedGeneration` when the run settles (`Ready` or `Failed`). Spec drift is measured against it in every other phase, so an edit in the middle of a run restarts the run.
Avoid: observed generation (that is the settled copy).

**Dispatchable**
An Environment in `Ready`, the only such phase: a non-draft LoadTest may be created against it and dispatched to it. A test that has applied a TestRun but is not yet `Running` fails if its Environment stops being dispatchable; a `Running` test is not affected. `Unreachable` is deliberately not dispatchable.
Avoid: ready (that is the phase and the Condition type; dispatchable is the rule over phases), healthy, available.

## Federation

**dfaas-agent**
The process, on each DFaaS node, that discovers its peers and decides where to send each request. It is the system under test and comes from upstream, not from this repository ([dfaas-agent.md](dfaas-agent.md)).

**Federation (two meanings)**
The libp2p mesh the dfaas-agents form (the peers are the bootstrap list, see [known-limitations.md](known-limitations.md#the-agent-mesh-is-exactly-the-bootstrap-list)), and the Prometheus federation: the management Prometheus pulls the metrics of every DFaaS node through `/federate` once per `server.global.scrape_interval` (10 s). The word "federation interval" always means the second one.

**Balancing strategy**
The dfaas-agent policy on a DFaaS node, set by `spec.nodes[].balancingStrategy` and passed to the agent as `AGENT_STRATEGY`. Supported: `staticstrategy`, `alllocalstrategy`, `recalcstrategy` and `randomstrategy`. The CRD also accepts `nodemarginstrategy` and `rlagentstrategy`; this platform does not support them.

## Persistence and validation

**Transition**
Everything one status write persists on a resource: an optional phase (and aggregate override), Conditions, and an optional touch of other status fields. The status writer records it in one retrying write, Conditions before the aggregate `Ready` ([ADR-0002](adr/0002-status-writer-never-decides-requeue.md)).
Avoid: update, patch, mutation.

**Retry counter**
A generation-scoped annotation (`"<generation>:<count>"`) bounding the consecutive failures of one operation. A spec edit restarts every budget from zero ([ADR-0003](adr/0003-all-retry-counters-generation-scoped.md)).
Avoid: attempts in new names (the annotations keep their historical keys), backoff.

**Rule set**
The gateway's single validation schema (`internal/api/schema.go`, UI repo), served at `GET /api/meta/schema` ([ADR-0001](adr/0001-gateway-owns-the-validation-rule-set.md)). It is hand-synced with the CRDs, the one remaining hop, since the gateway has no compile-time dependency on the operator.
Avoid: schema in prose (it collides with the Kubernetes OpenAPI schema), validation rules, constraints.

## Results

**Sink**
The S3 destination of an export. It is the config named by `Environment.spec.s3ConfigRef`, or `seaweedfs-default` (the in-cluster SeaweedFS) when that is unset. A config is a Secret in the `dfaas-s3` namespace.

**Export window**
The query range the exporter asks Prometheus for: `status.startTime` to `status.endTime` plus one federation interval. The export itself starts one minute after `endTime` (the cool-down).
