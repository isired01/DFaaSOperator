# `api/client.js` keeps its one-line URL-template exports

This ADR governs the SPA in the DFaaS_UI repo (`ui/src/api/client.js`).

An architecture review listed eleven exports (`fetchLoadTest`, `deleteEnvironment`, and so on) as pass-throughs whose complexity "vanishes" on deletion. It does not: deleting `fetchLoadTest(ns, name)` puts the route string at each of its three callers, so the knowledge spreads rather than disappears, and the function earns its keep by the usual deletion test. Route strings stay in one file. Do not propose removing these exports on line count alone.
