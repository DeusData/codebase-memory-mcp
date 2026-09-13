# Architecture decisions

> Preview example for voting-services. This proposed review note is test content, not an upstream architectural decision.

## 001 — Distinguish declared and observed connections

**Status:** Proposed · **Scope:** Service map review

### Context

A small connected application is useful for checking whether a service visualization makes each relationship understandable.

### Decision

Review deployment dependencies, request relationships, and datastore connections separately. Inspect the source or configuration evidence for each edge. Do not interpret an edge count as traffic volume or a static dependency as an observed runtime call.

### Consequences

The service map can explain what is connected and why without implying measurements that were never collected. Missing connections become concrete questions for further inspection.

### Next review

- [ ] Select a service and inspect its neighboring connections.
- [ ] Verify one connection against source or configuration.
- [ ] Edit this note with the outcome and save it.
