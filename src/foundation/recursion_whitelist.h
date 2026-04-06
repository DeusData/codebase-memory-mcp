/*
 * recursion_whitelist.h — Functions approved for recursive implementation.
 *
 * These functions use bounded recursion where iterative conversion would
 * add complexity with no practical benefit:
 *
 * Cypher recursive descent parser (bounded by query nesting depth ~5):
 *   - parse_or_expr, parse_xor_expr, parse_and_expr, parse_not_expr
 *   - parse_atom_expr, parse_post_where, cbm_parse
 *
 * Cypher expression evaluator (bounded by WHERE clause depth ~5):
 *   - eval_expr
 *
 * Glob pattern matcher (bounded by pattern nesting ~3):
 *   - glob_match, glob_match_star, glob_match_doublestar
 *   - glob_match_doublestar_slash, glob_match_doublestar_any
 *
 * To add a function: add it below AND add NOLINT(misc-no-recursion) on
 * the function definition line. The lint gate verifies both match.
 */
#define CBM_RECURSION_WHITELIST                                                               \
    "parse_or_expr", "parse_xor_expr", "parse_and_expr", "parse_not_expr", "parse_atom_expr", \
        "parse_post_where", "cbm_parse", "eval_expr", "glob_match", "glob_match_star",        \
        "glob_match_doublestar", "glob_match_doublestar_slash", "glob_match_doublestar_any",  \
        "parse_bool_expr", "parse_bool_atom"
