# eval-small

A tiny polyglot repository with a hand-verified ground truth, used by the
codebase-memory-mcp evals. Every symbol, call and file in here is
listed in `../expected/eval-small.json`.

The `OrderService` (Python) and `InvoiceService` (Java) classes are the two
entry points. `compute_total` is mentioned here on purpose: it does not exist
in the code, so a search for it must not resolve to a symbol.
