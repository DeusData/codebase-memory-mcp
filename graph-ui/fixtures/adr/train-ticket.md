# Architecture decisions

> Preview examples for the indexed Train Ticket repository. These are proposed review notes for exercising the ADR workspace, not decisions attributed to the upstream maintainers.

## 001 — Review services as deployment boundaries

**Status:** Proposed · **Scope:** Routes review

### Context

A large service graph is difficult to understand when every source file has equal visual weight. Deployment declarations provide another useful level of organization.

### Decision

Use detected service and container boundaries to organize the routes review. Keep the declaration behind each boundary inspectable. Treat a deployment dependency and a source-level request as different kinds of evidence.

### Consequences

- The initial view can show relationships between services before drilling into handlers.
- A declared dependency alone does not prove that a request is sent.
- Services without discovered connections should remain visible.

## 002 — Follow a request through its evidence

**Status:** Proposed · **Scope:** Cross-service investigation

### Context

Matching a caller to a route helps explain the system, but static analysis may not resolve dynamic hosts, route parameters, or runtime configuration.

### Decision

During review, follow a connection from its caller through the route evidence to the destination handler. Keep unresolved destinations visible as unknowns. Use source inspection to verify a suspected relationship before treating it as established.

### Review checklist

- [ ] Inspect an outgoing request in the selected service.
- [ ] Compare its path and method with the destination route.
- [ ] Check relevant configuration and service naming.
- [ ] Record unresolved runtime assumptions here.

### Consequences

This creates a repeatable review path while avoiding claims that a static graph proves runtime behavior.
