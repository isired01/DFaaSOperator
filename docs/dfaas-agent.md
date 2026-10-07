# The dfaas-agent: provenance of the system under test

The dfaas-agent is the process on each DFaaS node that discovers its peers and decides where each request is served. It is the system under test, not part of this operator. This repository installs it and measures it, but does not contain its source, its chart, or a recipe that builds either. This document records where the pieces came from.

## Upstream

- Project: [unimib-datAI/dfaas](https://github.com/unimib-datAI/dfaas). The agent is the Go code in its `dfaasagent/` directory.
- License: AGPL-3.0. The image therefore contains AGPL-3.0 code, and its source is upstream at the commit below. This repository's Apache-2.0 license does not apply to it. The three Helm values files under `internal/controller/ansible/templates/` that are derived from upstream's `k8s/charts/values-*.yaml` stay AGPL-3.0-or-later for the same reason (they carry an SPDX header).

## The image

The playbook installs `ghcr.io/isired01/dfaas-agent:dev` with `pullPolicy: Always` (`dfaas_agent_image` and `dfaas_agent_image_pull_policy` at the top of `setup-nodes.yml`). The tag `dev` is mutable.

- **`dev`** is upstream `unimib-datAI/dfaas`, directory `dfaasagent/`, at commit `82c50cb99321ad0ea56cb77b5b53b50f8f2ebd5f`, built by hand on 2026-09-11 and pushed under that name.
- **`fix2` and `pre-random`** are an earlier build of 2026-04-30.
- No workflow in either repository builds or publishes the image, and no other tag has a recorded source.

Because `dev` is mutable and pulled on every start, two provisioning runs can run different agents if the tag is moved. Pinning it to a version tag or a digest is part of the reproducibility issue in the issue tracker ([known-limitations.md](known-limitations.md#provisioning-needs-internet-access-and-installs-unpinned-software)).

### Rebuilding the image from upstream

At that commit, upstream builds the agent with `k8s/scripts/build-image.sh`, which runs `buildah build -f k8s/containers/agent -t agent:dev .` from the repository root. The Containerfile is the file `k8s/containers/agent`.

```bash
git clone https://github.com/unimib-datAI/dfaas.git
cd dfaas
git checkout 82c50cb99321ad0ea56cb77b5b53b50f8f2ebd5f
./k8s/scripts/build-image.sh agent tag        # needs buildah; leaves the image agent:dev locally
buildah tag agent:dev ghcr.io/<owner>/dfaas-agent:<tag>
buildah push ghcr.io/<owner>/dfaas-agent:<tag>
```

Build for the CPU architecture of your DFaaS nodes. A different upstream commit is a different agent: its configuration keys and behaviour may differ from what the playbook expects, so run one Environment end to end before relying on it. Then set `dfaas_agent_image` in `internal/controller/ansible/templates/setup-nodes.yml` to your image, rebuild the operator image, and re-provision. The image must be pullable by the VMs without credentials.

## The chart

The playbook installs the agent with the Helm chart `oci://ghcr.io/isired01/dfaas-agent-chart`, version `0.1.3` (`chart_ref` and `chart_version` in `setup-nodes.yml`), into the namespace `default` of each DFaaS node's k3s, as the release `dfaas-agent`. The chart is published only as a package in that registry. **Its source is in no repository.** Upstream has a chart of its own, `k8s/charts/agent` (version `0.1.0` at the commit above), but the published `0.1.3` is not a copy of it, and the values the playbook sets were written for `0.1.3`:

- `image`, `imagePullPolicy`, `privateKey`;
- `config.AGENT_DEBUG`, `AGENT_BOOTSTRAP_NODES`, `AGENT_BOOTSTRAP_NODES_LIST`, `AGENT_BOOTSTRAP_FORCE`, `AGENT_STRATEGY`, `AGENT_RUNTIMEAPI_HOST` and `_PORT`, `AGENT_DATAPLANEAPI_HOST`, `_PORT`, `_USER` and `_PASSWORD`, `AGENT_HAPROXY_PORT`, `AGENT_REJECTOR_HOST` and `_PORT`, `AGENT_RANDOM_REJECT`, `AGENT_RANDOM_SEED`;
- `forecaster.enabled: false`.

Do not lose this package. To keep a copy, or to move the project, fetch it while the registry still serves it:

```bash
helm pull oci://ghcr.io/isired01/dfaas-agent-chart --version 0.1.3
helm push dfaas-agent-chart-0.1.3.tgz oci://ghcr.io/<owner>
```

Then point `chart_ref` at the new location. The archive can be unpacked to recover the chart source and committed to a repository. The upstream chart at `k8s/charts/agent` is the other possible starting point, but it has not been tested with these values.

### The libp2p port contract

The playbooks advertise each DFaaS node to its peers as `/ip4/<node ip>/tcp/<port>/p2p/<peer ID>`. The port is the constant `libp2pBootstrapPort` in `internal/controller/ansible/job.go`, currently **31600**, and it is the only place the port is defined on the operator side. The chart exposes the agent's libp2p listener on the node as a NodePort Service (`dfaas-agent-kademlia`, `nodePort: 31600` in upstream's `kademlia-service.yaml` at the commit above). The two must be equal: the operator only advertises the port and exposes nothing itself, and no task opens a firewall port (UFW is disabled on the VMs).

A mismatch, a Service that is `ClusterIP` only, or a crash-looping agent shows on the peer as `failed to dial ... all dials failed ... dial backoff`: a transport failure, not a peer-ID mismatch. The advertised address is baked into the Ansible inventory when the Job is created, so changing the port needs a re-provision (a spec edit), not just an operator restart.

HAProxy's NodePorts are pinned in `haproxy-values.yaml` for a related reason: an unpinned HAProxy port draws a random number from the NodePort range, which contains 31600, and when HAProxy drew it first the agent install failed with "provided port is already allocated". Keep 31600 out of any other NodePort assignment on a DFaaS node.

### Names other code depends on

- The Helm release name `dfaas-agent` is what the SPA's default metrics select with `pod=~"dfaas-agent.*"`.
- The playbook restarts the agent Deployment `dfaas-agent` (namespace `default`) when the release changed, because a `helm upgrade` that changes only the bootstrap list in the pod environment does not reliably roll the pod.

## The mesh and the unshipped fix

The agents discover each other only through the bootstrap list that the operator writes: DFaaS node *i* lists nodes `0..i-1` of `spec.nodes`, so every pair is dialled exactly once, and `AGENT_BOOTSTRAP_FORCE` makes a non-seed agent wait at start until every listed peer answers. The agent dials the list once, in `Initialize` (`dfaasagent/agent/discovery/kademlia/kademlia.go` upstream), and the Kademlia discovery that was meant to complete the mesh returns no usable peer because the agent announces only its pod address. So a link that drops is never retried ([known-limitations.md](known-limitations.md#the-agent-mesh-is-exactly-the-bootstrap-list)).

A patch that makes the agent keep the bootstrap peers and re-dial the ones that are not connected on every pass of `RunDiscovery` exists. It was written against the 2026-04 copy of upstream and was never built into any published image, so the running agents do not contain it. It is not in any repository; its description and the proposed change are in the issue tracker. A change like it belongs upstream, or in a fork whose image you build and reference as above.
