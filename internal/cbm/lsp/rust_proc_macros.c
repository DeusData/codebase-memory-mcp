/* Rust procedural-macro policy boundary.
 *
 * Attribute and derive macros are represented by the generic graph semantic
 * passes as DECORATES plus USAGE. This file intentionally emits no
 * CBMResolvedCall records: without executing rustc and the proc-macro, helper
 * invocations such as Runtime::block_on or Span::enter are not source-proven
 * calls and must not be fabricated.
 *
 * Kept as a separate compilation unit because lsp_all.c includes the historic
 * module directly; the empty policy boundary also makes accidental synthesis
 * conspicuous in review. */
