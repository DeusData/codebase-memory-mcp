/* tsp_intuit_more.h
 *
 * A faithful, stateless port of perl's S_intuit_more() (toke.c) and
 * S_regcurly() (regcomp.c) -- "the one truly awful dwimmer necessary to
 * conflate C and sed".
 *
 * Inside a pattern, a `[` or `{` following a variable is ambiguous:
 *   /$foo[...]/   could be an array subscript OR a character class
 *   /$foo{...}/   could be a hash  subscript OR a {n,m} quantifier
 * Perl resolves this with a weighting heuristic.  We reproduce it so the
 * external scanner can decide whether to emit a literal regex `[` / `{`.
 *
 * tsp_intuit_more(s, len) returns TRUE  => "there is more to the expression",
 * i.e. it's a SUBSCRIPT; FALSE => it's a character class / quantifier.  This
 * matches perl's return-value convention exactly.
 *
 * The caller is the scanner; it has already established that we are inside a
 * pattern and that s[0] is '[' or '{'.  `s` points at that bracket and spans
 * `len` buffered bytes, ideally through the matching close bracket (when one
 * exists in range).  The function never reads past s[len-1].
 *
 * Three branches of perl's heuristic depend on state a stateless lexer can't
 * see; these are intentional best-effort gaps an LSP can refine:
 *   - PL_lex_brackets (parser-stack recursion)      -> handled by the scanner
 *   - gv_fetchpvn_flags (runtime symbol table, -100) -> treated as the -10 case
 *   - keyword() (-150)                               -> backed by our own
 *     regenerated keyword trie (tsp_intuit_keywords.h)
 */

#ifndef TSP_INTUIT_MORE_H
#define TSP_INTUIT_MORE_H

#include <stdbool.h>
#include <stddef.h>
#include "tsp_intuit_keywords.h"

/* --- ASCII classification (perl's is* macros, ASCII subset) ----------------
 * UTF-8 word characters beyond ASCII are a best-effort gap. */
static inline bool tsp_im_isdigit(int c) { return c >= '0' && c <= '9'; }
static inline bool tsp_im_isalpha(int c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}
static inline bool tsp_im_isword(int c) {
  return tsp_im_isalpha(c) || tsp_im_isdigit(c) || c == '_';
}
static inline bool tsp_im_isblank(int c) { return c == ' ' || c == '\t'; }

/* strchr over a literal set, NUL-safe (a NUL `c` never matches). */
static inline bool tsp_im_in(const char *set, int c) {
  if (c == 0) return false;
  for (; *set; set++)
    if ((unsigned char)*set == (unsigned char)c) return true;
  return false;
}

/* --- S_regcurly: does s[0..len) begin a {m,n} quantifier? ------------------
 * Mirrors regcomp.c. We only need the syntactic yes/no, so the result[]
 * out-param is dropped. Accepts {m}, {m,}, {m,n}, and {,n}; rejects {} . */
static bool tsp_regcurly(const char *s, int len) {
  const char *e = s + len;
  bool min_present = false;   /* perl's min_start: digit(s) before the comma */
  bool max_present = false;   /* perl's max_end:   digit(s) after the comma  */

  if (s >= e || *s++ != '{') return false;

  while (s < e && tsp_im_isblank(*s)) s++;

  if (s < e && tsp_im_isdigit(*s)) {
    min_present = true;
    do { s++; } while (s < e && tsp_im_isdigit(*s));
  }

  while (s < e && tsp_im_isblank(*s)) s++;

  if (s < e && *s == ',') {
    s++;
    while (s < e && tsp_im_isblank(*s)) s++;
    if (s < e && tsp_im_isdigit(*s)) {
      max_present = true;
      do { s++; } while (s < e && tsp_im_isdigit(*s));
    }
    while (s < e && tsp_im_isblank(*s)) s++;
  }

  /* Mirrors regcomp.c: must close with '}' and have at least one number,
   * before or after the comma ({3}, {3,}, {3,5}, {,5}); '{}' and '{,}' fail. */
  if (s >= e || *s != '}' || (!min_present && !max_present)) return false;

  return true;
}

/* --- S_intuit_more: subscript (TRUE) vs char-class/quantifier (FALSE) ------ */
static bool tsp_intuit_more(const char *s, int len) {
  const char *e = s + len;

  if (s >= e) return true;                 /* nothing buffered; be safe */

  /* In a pattern, a leading '{' might be a {n,m} quantifier. */
  if (s[0] == '{') {
    return !tsp_regcurly(s, len);
  }

  if (s[0] != '[') return true;            /* caller only sends '[' or '{' */

  s++;                                     /* step past '[' */

  /* '^' implies a char class; empty '[]' isn't legal but means "no more". */
  if (s >= e) return true;                 /* '[' with nothing after -> expr */
  if (s[0] == ']' || s[0] == '^') return false;

  /* Find the matching ']'. No ']' in range => has to be an expression. */
  const char *send = NULL;
  for (const char *p = s; p < e; p++) {
    if (*p == ']') { send = p; break; }
  }
  if (!send) return true;

  /* One or two digits only => subscript, e.g. [12]. */
  if (tsp_im_isdigit(s[0]) && send - s <= 2 &&
      (send - s == 1 || tsp_im_isdigit(s[1]))) {
    return true;
  }

  /* The weighting machine. weight >= 0 => char class, < 0 => subscript. */
  int weight = (s[0] == '$') ? -1 : 2;     /* '$' leans slightly to subscript */

  unsigned char seen[256] = {0};
  unsigned char un_char = 0;
  bool first_time = true;

  for (; s < send; s++, first_time = false) {
    unsigned char prev_un_char = un_char;
    un_char = (unsigned char)s[0];

    switch (s[0]) {
      case '@':
      case '&':
      case '$':
        weight -= seen[un_char] * 10;
        /* A word char following one of these: perl would consult the symbol
         * table for a -100; we lack it, so always take the -10 path (gap). */
        if (s + 1 < e && tsp_im_isword((unsigned char)s[1])) {
          weight -= 10;
        }
        else if (s[0] == '$' && s + 1 < e && s[1] &&
                 tsp_im_in("[#!%*<>()-=", (unsigned char)s[1])) {
          /* Possible punctuation variable. */
          char s2 = (s + 2 < e) ? s[2] : 0;
          if (tsp_im_in("])} =", (unsigned char)s2)) weight -= 10;
          else weight -= 1;
        }
        break;

      case '\\':
        if (s + 1 < e && s[1]) {
          if (tsp_im_in("wds]", (unsigned char)s[1])) weight += 100;
          else if (seen[(unsigned char)'\''] || seen[(unsigned char)'"']) weight += 1;
          else if (tsp_im_in("abcfnrtvx", (unsigned char)s[1])) weight += 40;
          else if (tsp_im_isdigit((unsigned char)s[1])) {
            weight += 40;
            while (s + 1 < send && tsp_im_isdigit((unsigned char)s[1])) s++;
          }
        }
        else weight += 100;              /* '\' at end strongly char class */
        break;

      case '-':
        if (s + 1 < e && s[1] == '\\') weight += 50;
        if (!first_time && tsp_im_in("aA01! ", prev_un_char)) weight += 30;
        if (s + 1 < e && tsp_im_in("zZ79~", (unsigned char)s[1])) weight += 30;
        if (first_time && s + 1 < e &&
            (tsp_im_isdigit((unsigned char)s[1]) || s[1] == '$')) {
          weight -= 5;                   /* negative subscript */
        }
        break;

      default:
        if ((first_time || (!tsp_im_isword(prev_un_char)
                            && prev_un_char != '$'
                            && prev_un_char != '@'
                            && prev_un_char != '&'))
            && tsp_im_isalpha((unsigned char)s[0])
            && s + 1 < send && tsp_im_isalpha((unsigned char)s[1])) {
          /* A run of >= 2 alphas at a boundary: if it spells a keyword it's
           * almost certainly not a character class. (Mirrors perl's pointer
           * walk: s lands on the first non-alpha, which the loop's s++ then
           * skips -- intentional, matching toke.c.) */
          const char *d = s;
          while (s < send && tsp_im_isalpha((unsigned char)s[0])) s++;
          if (tsp_is_perl_keyword(d, (int)(s - d))) weight -= 150;
        }

        /* Ascending runs like ...12... or ...ab... lean char class. */
        if (!first_time && un_char == (unsigned char)(prev_un_char + 1)) {
          weight += 5;
        }

        /* Repeats lean subscript (nobody writes [aba] as a class). */
        weight -= seen[un_char];
        break;
    }

    seen[un_char]++;
  }

  if (weight >= 0) return false;           /* probably a character class */
  return true;                             /* subscript */
}

#endif /* TSP_INTUIT_MORE_H */
