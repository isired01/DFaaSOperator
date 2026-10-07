# Architecture decision records

| ADR | Decision |
| --- | --- |
| [0001](0001-gateway-owns-the-validation-rule-set.md) | The gateway owns the single hand-written validation rule set and serves it to the SPA. |
| [0002](0002-status-writer-never-decides-requeue.md) | The status writer persists; each reconciler decides its own requeue. |
| [0003](0003-all-retry-counters-generation-scoped.md) | Every retry counter is scoped to the object generation and resets on a spec edit. |
| [0004](0004-role-table-in-its-own-package.md) | Per-role provisioning facts live in `internal/controller/roles`. |
| [0005](0005-dispatcher-name-kept.md) | The remote-fleet seam keeps the name `Dispatcher`. |
| [0006](0006-dispatcher-seam-at-reach-node-n.md) | The Dispatcher seam sits at "reach node N", not at the HTTP call. |
| [0007](0007-keep-url-template-exports-in-client-js.md) | The SPA's `api/client.js` keeps its one-line route exports. |
| [0008](0008-detected-management-address-beats-env-fallbacks.md) | A generator's runners dial the management address detected for it; env vars are fallbacks. |

ADR 0001 and 0007 govern code in the DFaaS_UI repository. ADR 0008 spans both repositories.
