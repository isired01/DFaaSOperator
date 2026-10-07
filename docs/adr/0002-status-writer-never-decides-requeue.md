# The status writer persists; it never returns a ctrl.Result

Two sibling phase setters returned opposite requeue semantics (the Environment one none, the LoadTest one an immediate requeue) and nothing said why. `statuswriter.Writer.Record` now returns only an error. Each reconciler states its own scheduling choice in one `phase()` helper: LoadTest requeues immediately, Environment relies on its handlers' own `RequeueAfter` and the status watch. The policy is visible and per-reconciler, and the persistence package stays small.

LoadTest's `phase()` requeues immediately except after a terminal write, which relies on that write's own watch event. An immediate pass would read the pre-terminal cached copy and stamp in-progress Conditions onto an ended test. The policy still lives in `phase()`.

The same rule applies to `statuswriter.Budget.Attempt` (see [ADR-0003](0003-all-retry-counters-generation-scoped.md)): it returns an outcome, never a `ctrl.Result`. Each caller keeps its own requeue and its own terminal phase.
