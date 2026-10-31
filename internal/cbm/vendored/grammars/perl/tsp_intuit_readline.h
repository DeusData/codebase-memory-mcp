/* tsp_intuit_readline.h
 *
 * Disambiguates `<...>` after an ambiguous bareword: fileglob/readline
 * (`print <*.c>`) vs the relational `<` operator (`CONST < 7 > $x`).  Real
 * perl decides this by PL_expect (was a value or a list-op in front of `<`?),
 * which the scanner can't see, so we hybridise two heuristics whose blind
 * spots are disjoint:
 *
 *   (a) glob-shape: a real target looks like a glob pattern (metachar `*?[]{}~`,
 *       path `/`, filename-dot `foo.c`) or a pure filehandle name; content that
 *       is neither (a number, a spaced scalar, an operand) is relational.  This
 *       catches `CONST < $a > foo`, which (b) alone misreads.
 *   (b) orphan lookahead: a glob is a complete term, so a bare term right after
 *       its `>` would be orphaned -> `<` was the operator.  This catches
 *       `CONST < $a/$b > $c` (glob-shaped content, but `$c` orphans), which (a)
 *       alone misreads.
 *
 * tsp_is_fileglob(content, clen, after, alen): TRUE => emit fileglob; FALSE =>
 * bail to the `<` operator.  See the decision body below.  Best-effort and
 * ASCII-only (an LSP with parser state can refine it); never reads past either
 * buffer.  `after` is a capped window; if it holds only whitespace we treat it
 * as EOF and allow the glob.
 */

#ifndef TSP_INTUIT_READLINE_H
#define TSP_INTUIT_READLINE_H

#include <stdbool.h>
#include <stddef.h>
#include <string.h>

/* --- ASCII classification (mirrors the subset used elsewhere) -------------- */
static inline bool tsp_rl_isdigit(int c) { return c >= '0' && c <= '9'; }
static inline bool tsp_rl_isalpha(int c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
static inline bool tsp_rl_isword(int c) {
  return tsp_rl_isalpha(c) || tsp_rl_isdigit(c) || c == '_';
}
static inline bool tsp_rl_isspace(int c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' ||
         c == '\v';
}

/* The low-precedence operator / statement-modifier keywords that may legally
 * FOLLOW a complete `bareword <glob>` term.  Seeing one of these after the `>`
 * means the glob is complete, not orphaning a term. */
static inline bool tsp_rl_is_trailing_keyword(const char *w, size_t n) {
  static const char *const kw[] = {
      "if", "unless", "while", "until", "for", "foreach", "and", "or", "xor",
      "not", "cmp", "eq", "ne", "lt", "gt", "le", "ge", "x"};
  for (size_t i = 0; i < sizeof(kw) / sizeof(kw[0]); i++) {
    if (strlen(kw[i]) == n && memcmp(w, kw[i], n) == 0) return true;
  }
  return false;
}

/* The infix word-operators that, when appearing INSIDE the `<...>` content as a
 * whitespace-delimited word, prove it is a relational expression rather than a
 * glob pattern (a glob never contains a bare perl operator word). */
static inline bool tsp_rl_is_infix_word(const char *w, size_t n) {
  static const char *const kw[] = {"and", "or", "xor", "not", "cmp", "eq",
                                    "ne",  "lt", "gt",  "le",  "ge",  "x"};
  for (size_t i = 0; i < sizeof(kw) / sizeof(kw[0]); i++) {
    if (strlen(kw[i]) == n && memcmp(w, kw[i], n) == 0) return true;
  }
  return false;
}

/* Scan the `<...>` content for a whitespace-delimited infix operator word.
 * Only alpha runs that start at a whitespace boundary (and are not glued to
 * sigils/digits/punctuation) count; `$x` or `*.c` never qualify. */
static inline bool tsp_rl_content_has_infix_op(const char *s, size_t len) {
  size_t i = 0;
  while (i < len) {
    /* skip leading whitespace */
    while (i < len && tsp_rl_isspace((unsigned char)s[i])) i++;
    size_t start = i;
    bool clean = true;  /* a clean bare word: only alphas */
    while (i < len && !tsp_rl_isspace((unsigned char)s[i])) {
      if (!tsp_rl_isalpha((unsigned char)s[i])) clean = false;
      i++;
    }
    if (clean && i > start && tsp_rl_is_infix_word(s + start, i - start))
      return true;
  }
  return false;
}

/* The glob metacharacters whose presence in the content is decisive evidence
 * of a glob pattern (as opposed to a relational operand). */
static inline bool tsp_rl_is_glob_meta(int c) {
  return c == '*' || c == '?' || c == '[' || c == ']' || c == '{' || c == '}' ||
         c == '~';
}

/* Does the content POSITIVELY look like a glob pattern?  True if it contains a
 * glob metacharacter, a path separator `/`, or a filename-looking `.` (a dot
 * flanked by word characters, e.g. `foo.c`).  A bare number (`7`), a lone
 * scalar (`$a`), or an operator expression has none of these. */
static inline bool tsp_rl_content_is_globshaped(const char *s, size_t len) {
  for (size_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)s[i];
    if (tsp_rl_is_glob_meta(c) || c == '/') return true;
    if (c == '.') {
      bool prev_word = (i > 0) && tsp_rl_isword((unsigned char)s[i - 1]);
      bool next_word = (i + 1 < len) && tsp_rl_isword((unsigned char)s[i + 1]);
      if (prev_word && next_word) return true;
    }
  }
  return false;
}

/* Is the content a pure filehandle / scalar NAME (no leading whitespace): an
 * optional leading `$`, then word characters with `::` or `'` package
 * separators (e.g. `$fh`, `STDIN`, `Foo::BAR`), tolerating only trailing
 * whitespace?  Such content is a readline target, not a relational operand.
 * The leading-vs-trailing whitespace asymmetry is load-bearing: a glob target
 * is written tight against `<` (`<$sner >`), whereas a relational `< EXPR`
 * carries a separating space (`< $a > $b`). */
static inline bool tsp_rl_is_filehandle(const char *buf, size_t len) {
  size_t i = 0;
  if (i >= len || tsp_rl_isspace((unsigned char)buf[i])) return false;
  if (buf[i] == '$') i++;
  if (i >= len) return false; /* bare `$`: not a name */

  bool last_was_word = false;
  while (i < len) {
    if (tsp_rl_isword((unsigned char)buf[i])) {
      last_was_word = true;
      i++;
    } else if (buf[i] == ':') {
      if (!last_was_word) return false;
      if (i + 1 >= len || buf[i + 1] != ':') return false;
      last_was_word = false;
      i += 2;
    } else if (buf[i] == '\'') {
      if (!last_was_word) return false;
      last_was_word = false;
      i++;
    } else if (tsp_rl_isspace((unsigned char)buf[i])) {
      if (!last_was_word) return false;
      for (i++; i < len; i++)
        if (!tsp_rl_isspace((unsigned char)buf[i])) return false;
      return true;
    } else {
      return false;
    }
  }
  return last_was_word;
}

/* --- the decision ---------------------------------------------------------- */
static inline bool tsp_is_fileglob(const char *content, size_t clen,
                                   const char *after, size_t alen) {
  /* The empty diamond `<>` (rare here; usually handled before us). */
  if (clen == 0) return true;

  /* (1) operator-word inside the content => relational expression, not glob. */
  if (tsp_rl_content_has_infix_op(content, clen)) return false;

  /* (2) GLOB-SHAPE GATE.  A real glob/readline target positively looks like a
   * glob pattern (metachar / path / filename-dot) or a pure filehandle name.
   * Content that is neither -- a bare number (`< 7 >`), a lone scalar with a
   * separating space (`< $a > $b`), an arithmetic sub-expression -- is a
   * relational operand, so bail.  This is what keeps `CONST < $a > foo` and
   * `CONST < 7 > $x` relational without any lookahead. */
  if (!tsp_rl_content_is_globshaped(content, clen) &&
      !tsp_rl_is_filehandle(content, clen))
    return false;

  /* (3) post-`>` orphan lookahead.  The content looks glob-shaped, but a real
   * glob is a COMPLETE term: if a bare term follows the `>` with no connecting
   * operator it would be orphaned, which means `<` was the relational operator
   * after all (e.g. `CONST < $a/$b > $c` -- glob-shaped content `$a/$b`, yet
   * the trailing `$c` orphans). */
  size_t i = 0;
  while (i < alen && tsp_rl_isspace((unsigned char)after[i])) i++;

  /* Window exhausted before any non-space byte: treat as EOF-like -> glob ok. */
  if (i >= alen) return true;

  unsigned char nc = (unsigned char)after[i];

  /* An alphabetic word: keyword => complete term (glob ok); otherwise an
   * ambiguous bareword, which we lean toward NOT treating as an orphan. */
  if (tsp_rl_isalpha(nc)) {
    size_t start = i;
    while (i < alen && tsp_rl_isword((unsigned char)after[i])) i++;
    if (tsp_rl_is_trailing_keyword(after + start, i - start)) return true;
    /* Unknown bareword after the glob: lean toward glob (do not orphan). */
    return true;
  }

  /* Bare-term openers with no connecting operator => orphan => bail. */
  if (nc == '$' || nc == '@' || nc == '%' || nc == '"' || nc == '\'' ||
      tsp_rl_isdigit(nc) || nc == '(' || nc == '[' || nc == '{') {
    return false;
  }

  /* Everything else — operators (`+ - * / . < > ! ~ ? : | & ^`), `=`, `=>`,
   * comma, `;`, closing brackets `) ] }` — terminates the term cleanly, so the
   * glob is complete. */
  return true;
}

#endif /* TSP_INTUIT_READLINE_H */
