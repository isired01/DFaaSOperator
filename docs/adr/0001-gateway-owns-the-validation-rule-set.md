# The gateway owns the single validation rule set and serves it to the SPA

The gateway (DFaaS_UI repo) has no compile-time dependency on the operator's Go types, so validation rules cannot be shared as code. They were re-typed across six places, four of them only in the browser. The decision is to write them by hand once in the gateway (`internal/api/schema.go` in the DFaaS_UI repo), enforce them on every write path there, and serve the same value at `GET /api/meta/schema` for the SPA to consume. That leaves exactly one hand-synced hop: gateway to CRD.

## Considered options

Reading the CRD's OpenAPI schema at runtime was rejected: it needs a `customresourcedefinitions` RBAC grant in the operator's chart, JSON-schema validators on both sides, and still cannot express the CEL rules (`ipAddress` uniqueness). A build-time generator in the operator with a vendored JSON file in the UI was rejected because the copy step has the same failure mode as the copy of the CRDs into `charts/dfaas/crds/` (see [development.md](../development.md)): nothing forces it to be redone.

## Consequences

A change to a CRD validation rule (a pattern, a bound, a CEL rule) must be mirrored in `schema.go` by hand. The CRD is the source of truth. [cross-repo-contract.md](../cross-repo-contract.md) lists which rules are mirrored.
