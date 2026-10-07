# Documentation

Start with the [README](../README.md) for requirements, installation and upgrade. The documents here describe how the system works, how to change it and what to watch out for.

| Document | Contents |
| --- | --- |
| [overview.md](overview.md) | What the system is for, the two repositories, the machines involved, one experiment end to end, where the results are, and a first-experiment walkthrough. |
| [architecture.md](architecture.md) | The operator's code map, the Environment and LoadTest phases and conditions, dispatch, the synchronized start, the export and its one-minute cool-down, the monitoring stack and its ports, and the retry model. |
| [cross-repo-contract.md](cross-repo-contract.md) | Every coupling between this repository and DFaaS_UI, what to change on the other side, and the checklist for changing a CRD field. |
| [glossary.md](glossary.md) | The terms used across both repositories. |
| [known-limitations.md](known-limitations.md) | The security model, what decides whether a measurement is valid, and the operational limits, each with what to do about it. |
| [development.md](development.md) | Toolchain, build, tests, code generation and the CRD copy into the chart, running the operator locally, CI gates and conventions. |
| [releasing.md](releasing.md) | How a release is cut in both repositories, upgrading and uninstalling, and what to change to move the project to another owner. |
| [dfaas-agent.md](dfaas-agent.md) | Where the system under test, the dfaas-agent image and chart, comes from, and how to rebuild it. |
| [adr/README.md](adr/README.md) | Index of the architecture decision records. |

## Suggested reading order

1. [overview.md](overview.md), to see the whole flow.
2. [glossary.md](glossary.md), for the vocabulary the other documents use.
3. [known-limitations.md](known-limitations.md), before you rely on a measurement or expose an install.
4. [architecture.md](architecture.md), then [adr/README.md](adr/README.md), before you read the code.
5. [development.md](development.md) and [cross-repo-contract.md](cross-repo-contract.md), before you change anything.
6. [releasing.md](releasing.md) and [dfaas-agent.md](dfaas-agent.md), before you publish or take over the project.
