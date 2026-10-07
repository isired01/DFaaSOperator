# Every retry counter is generation-scoped

Two annotation encodings coexisted: a plain count (`"7"`), which survived spec edits, and `"<generation>:<count>"`, which reset on a spec edit. Callers had to know which budget they were touching by the function name. Now every counter uses the second form, so a spec edit is uniformly "try again from zero", which is what a user editing a stuck Environment or LoadTest expects.

The counters are the annotations `dfaas.dfaas.io/ssh-attempts`, `health-misses` and `monitoring-attempts` on the Environment, and `dfaas.dfaas.io/dispatch-attempts` and `fetch-misses` on the LoadTest. All five go through `statuswriter.Budget`. A legacy plain value parses as generation 0 and resets on the first bump after an upgrade, a one-time and harmless reset.
