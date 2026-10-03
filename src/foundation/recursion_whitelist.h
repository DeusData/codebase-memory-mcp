/*
 * recursion_whitelist.h — Functions approved for recursive implementation.
 *
 * These functions use bounded recursion where iterative conversion would
 * add complexity with no practical benefit:
 *
 * Cypher recursive descent parser (bounded by CYPHER_MAX_PARSE_DEPTH; an
 * operator chain is linked, not nested, and UNION branches parse in a loop):
 *   - parse_or_expr, parse_xor_expr, parse_and_expr, parse_not_expr
 *   - parse_atom_expr
 *
 * Cypher expression walkers (bounded by the same parse depth; each follows
 * an operator chain in a loop and recurses only into the operands):
 *   - eval_expr, cypher_expr_selectivity
 *
 * Glob pattern matcher (bounded by pattern nesting ~3):
 *   - glob_match, glob_match_star, glob_match_doublestar
 *   - glob_match_doublestar_slash, glob_match_doublestar_any
 *
 * R import scanner (bounded by source-file AST depth):
 *   - r_collect_imports
 *
 * Extraction descendant search (bounded by AST depth):
 *   - find_first_descendant_by_kind (Verilog/SystemVerilog name wrappers)
 *   - find_first_descendant_of (Dart/Zig import URI/string nesting)
 *
 * To add a function: add it below AND add NOLINT(misc-no-recursion) on
 * the function definition line. The lint gate verifies both match.
 */
#define CBM_RECURSION_WHITELIST                                                               \
    "parse_or_expr", "parse_xor_expr", "parse_and_expr", "parse_not_expr", "parse_atom_expr", \
        "eval_expr", "cypher_expr_selectivity", "glob_match", "glob_match_star",              \
        "glob_match_doublestar", "glob_match_doublestar_slash", "glob_match_doublestar_any",  \
        "parse_bool_expr", "parse_bool_atom", "r_collect_imports",                            \
        "find_first_descendant_by_kind", "find_first_descendant_of"
