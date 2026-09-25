// Unity build: include simplecpp implementation directly since CGo only
// compiles .cpp files from the immediate package directory, not subdirs.
#include "vendored/simplecpp/simplecpp.cpp"

#include "preprocessor.h"
#include "vendored/simplecpp/simplecpp.h"

#include <sstream>
#include <string>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <cstdint>

extern "C" {

static bool has_preprocessor_work(const char *source, int source_len) {
    if (!source || source_len <= 0) {
        return false;
    }
    for (int i = 0; i < source_len - 1; i++) {
        if (source[i] == '#') {
            // Skip whitespace after #
            int j = i + 1;
            while (j < source_len && (source[j] == ' ' || source[j] == '\t'))
                j++;
            int remaining = source_len - j;
            if (remaining >= 6 && strncmp(source + j, "define", 6) == 0) {
                return true;
            }
            if (remaining >= 5 && strncmp(source + j, "ifdef", 5) == 0) {
                return true;
            }
            if (remaining >= 6 && strncmp(source + j, "ifndef", 6) == 0) {
                return true;
            }
            if (remaining >= 3 && strncmp(source + j, "if ", 3) == 0) {
                return true;
            }
        }
    }
    return false;
}

// ── Export-macro candidates (#1989) ─────────────────────────────────────────
// Build systems define symbol-export macros empty on the compiler command line
// (UE's UBT: `/D "MODULE_API="`; CMake generate_export_header: `<lib>_EXPORT`),
// so they never appear as #define lines in the source. tree-sitter has no
// preprocessor state either, and `class MODULE_API Foo` misparses with the
// macro token as the type name (worse: enums and free functions are lost to
// ERROR regions entirely). To mirror the real compile line, the preprocessed
// second pass predefines the conventional export-macro-shaped identifiers found
// in the file as empty. The shape is deliberately narrow — ALL_CAPS identifier
// ending in a known export suffix, with a non-trivial prefix — and the list is
// capped, so ordinary all-caps identifiers cannot be swept in wholesale.
static const char *kExportMacroSuffixes[] = {"_API",       "_EXPORT",     "_IMPORT",
                                             "_DLLEXPORT", "_DEPRECATED", NULL};

static bool is_export_macro_shape(const char *id, size_t len) {
    if (id[0] < 'A' || id[0] > 'Z') {
        return false;
    }
    for (size_t i = 0; i < len; i++) {
        char c = id[i];
        bool upper = c >= 'A' && c <= 'Z';
        bool digit = c >= '0' && c <= '9';
        if (!upper && !digit && c != '_') {
            return false;
        }
    }
    for (int s = 0; kExportMacroSuffixes[s]; s++) {
        size_t slen = strlen(kExportMacroSuffixes[s]);
        // Require at least two prefix characters before the suffix so a bare
        // "X_API"-style token (single-letter, easily a genuine symbol) stays out.
        if (len >= slen + 2 && strncmp(id + len - slen, kExportMacroSuffixes[s], slen) == 0) {
            return true;
        }
    }
    return false;
}

static bool is_identifier_start(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
}

static bool is_identifier_char(char c) {
    return is_identifier_start(c) || (c >= '0' && c <= '9');
}

// C++ raw string literals (R"delim(...)delim"). Returns the index one past
// the closing quote, or -1 when the form cannot be confidently recognized
// (unterminated, malformed delimiter) — the caller then STOPS collecting so
// the mis-modeled literal cannot poison the scan state of the rest of the
// file (#1989 review).
static int skip_raw_string(const char *source, int source_len, int quote) {
    int j = quote + 1;
    int delim_start = j;
    while (j < source_len) {
        char c = source[j];
        if (c == '(') {
            break;
        }
        // Delimiter chars: the standard d-char set excludes space, parens,
        // backslash, and the line breaks — a double quote IS a legal d-char
        // (R"""(...)""" compiles), so it must not end the delimiter (#1989
        // review round 3).
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ')' || c == '\\' ||
            j - delim_start >= 16) {
            return -1;
        }
        j++;
    }
    if (j >= source_len || source[j] != '(') {
        return -1;
    }
    int delim_len = j - delim_start;
    j++; // past the opening "("
    while (j < source_len) {
        if (source[j] == ')') {
            int k = j + 1;
            int m = 0;
            while (m < delim_len && k + m < source_len &&
                   source[k + m] == source[delim_start + m]) {
                m++;
            }
            if (m == delim_len && k + m < source_len && source[k + m] == '"') {
                return k + m + 1;
            }
        }
        j++;
    }
    return -1; // unterminated raw string: uncertain
}

// If the identifier just scanned is a raw-string prefix (R, LR, uR, UR, u8R)
// immediately followed by a quote, return skip_raw_string(...). Returns 0 for
// an ordinary identifier (including an identifier merely ENDING in R — only a
// standalone prefix token starts a raw string) and -1 on an unrecognizable
// raw form.
static int raw_string_after_prefix(const char *source, int source_len, int start, int end) {
    static const char *prefixes[] = {"R", "LR", "uR", "UR", "u8R", NULL};
    size_t idlen = (size_t)(end - start);
    bool is_prefix = false;
    for (int p = 0; prefixes[p]; p++) {
        if (idlen == strlen(prefixes[p]) && strncmp(source + start, prefixes[p], idlen) == 0) {
            is_prefix = true;
            break;
        }
    }
    if (!is_prefix || end >= source_len || source[end] != '"') {
        return 0;
    }
    return skip_raw_string(source, source_len, end);
}

// Advance past comments and string/char literals so only real code tokens
// consume the candidate budget (#1989 review). Handles line-spliced //
// comments (a backslash before the newline continues the comment — line
// splicing happens before comment recognition) and, via the identifier-scan
// hook in the callers, C++ raw string literals. A digit separator (1'000'000)
// can still over-skip — that fails safe, toward NOT collecting a candidate.
static int skip_non_code(const char *source, int source_len, int i) {
    while (i < source_len) {
        char c = source[i];
        if (c == '/' && i + 1 < source_len && source[i + 1] == '/') {
            i += 2;
            for (;;) {
                while (i < source_len && source[i] != '\n') {
                    i++;
                }
                if (i >= source_len) {
                    break;
                }
                // Line splice: a backslash right before the newline (allowing
                // \r\n) means the comment continues on the next line.
                int back = i - 1;
                if (back >= 0 && source[back] == '\r') {
                    back--;
                }
                if (back >= 0 && source[back] == '\\') {
                    i++; // spliced: keep consuming inside the comment
                    continue;
                }
                i++; // real end-of-comment newline
                break;
            }
        } else if (c == '/' && i + 1 < source_len && source[i + 1] == '*') {
            i += 2;
            while (i + 1 < source_len && !(source[i] == '*' && source[i + 1] == '/')) {
                i++;
            }
            if (i + 1 < source_len) {
                i += 2; // past the closing "*/"
            } else {
                i = source_len; // unterminated comment runs to EOF
            }
        } else if (c == '"' || c == '\'') {
            char quote = c;
            i++;
            while (i < source_len) {
                if (source[i] == '\\') {
                    i += 2;
                    continue;
                }
                if (source[i] == quote) {
                    i++;
                    break;
                }
                i++;
            }
        } else {
            return i;
        }
    }
    return i;
}

// Count-only scan used by the early gate (no storage).
static bool has_export_macro_candidates(const char *source, int source_len) {
    if (!source || source_len <= 0) {
        return false;
    }
    int i = 0;
    while (i < source_len) {
        i = skip_non_code(source, source_len, i);
        if (i >= source_len) {
            break;
        }
        if (!is_identifier_start(source[i])) {
            i++;
            continue;
        }
        int start = i;
        while (i < source_len && is_identifier_char(source[i])) {
            i++;
        }
        // A raw-string prefix consumes everything up to its closing quote;
        // an unrecognizable raw form poisons the scan state, so fail toward
        // NOT running the second pass (raw behavior preserved).
        int raw = raw_string_after_prefix(source, source_len, start, i);
        if (raw < 0) {
            return false;
        }
        if (raw > 0) {
            i = raw;
            continue;
        }
        if (is_export_macro_shape(source + start, (size_t)(i - start))) {
            return true;
        }
    }
    return false;
}

static int collect_export_macro_candidates(const char *source, int source_len,
                                           char (*out)[CBM_EXPORT_MACRO_NAME_MAX], int max_out) {
    if (!source || source_len <= 0 || !out || max_out <= 0) {
        return 0;
    }
    int stored = 0;
    int i = 0;
    while (i < source_len) {
        i = skip_non_code(source, source_len, i);
        if (i >= source_len) {
            break;
        }
        if (!is_identifier_start(source[i])) {
            i++;
            continue;
        }
        int start = i;
        while (i < source_len && is_identifier_char(source[i])) {
            i++;
        }
        // Raw string (see has_export_macro_candidates): skip its body, or stop
        // collecting entirely when the form cannot be confidently recognized —
        // continuing would mis-tokenize the rest of the file and could both
        // mint phantom candidates and mask real ones (#1989 review).
        int raw = raw_string_after_prefix(source, source_len, start, i);
        if (raw < 0) {
            break;
        }
        if (raw > 0) {
            i = raw;
            continue;
        }
        size_t len = (size_t)(i - start);
        /* Length gate FIRST: the dedup below reads out[k][0..len] against rows
         * of CBM_EXPORT_MACRO_NAME_MAX bytes — an over-long candidate must be
         * rejected before any compare touches them. */
        if (len >= CBM_EXPORT_MACRO_NAME_MAX) {
            continue;
        }
        if (!is_export_macro_shape(source + start, len)) {
            continue;
        }
        bool dup = false;
        for (int k = 0; k < stored; k++) {
            if (strncmp(out[k], source + start, len) == 0 && out[k][len] == '\0') {
                dup = true;
                break;
            }
        }
        if (dup) {
            continue;
        }
        if (stored >= max_out) {
            break; // bounded: stop collecting once the cap is reached
        }
        memcpy(out[stored], source + start, len);
        out[stored][len] = '\0';
        stored++;
    }
    return stored;
}

int cbm_export_macro_candidates(const char *source, int source_len,
                                char (*out)[CBM_EXPORT_MACRO_NAME_MAX], int max_out) {
    return collect_export_macro_candidates(source, source_len, out, max_out);
}

static int count_expanded_lines(const std::string &text) {
    int count = 1;
    for (char c : text) {
        if (c == '\n') {
            count++;
        }
    }
    return count;
}

static bool parse_line_directive(const char *line, size_t len, uint32_t *out_line,
                                 std::string *out_file) {
    size_t i = 0;
    while (i < len && (line[i] == ' ' || line[i] == '\t')) {
        i++;
    }
    if (i >= len || line[i++] != '#') {
        return false;
    }
    while (i < len && (line[i] == ' ' || line[i] == '\t')) {
        i++;
    }
    static const char prefix[] = "line";
    if (i + sizeof(prefix) - 1 > len || strncmp(line + i, prefix, sizeof(prefix) - 1) != 0) {
        return false;
    }
    i += sizeof(prefix) - 1;
    if (i >= len || (line[i] != ' ' && line[i] != '\t')) {
        return false;
    }
    while (i < len && (line[i] == ' ' || line[i] == '\t')) {
        i++;
    }
    if (i >= len || line[i] < '0' || line[i] > '9') {
        return false;
    }
    uint64_t parsed_line = 0;
    while (i < len && line[i] >= '0' && line[i] <= '9') {
        parsed_line = parsed_line * 10u + (uint64_t)(line[i] - '0');
        if (parsed_line > UINT32_MAX) {
            return false;
        }
        i++;
    }
    while (i < len && (line[i] == ' ' || line[i] == '\t')) {
        i++;
    }
    if (i >= len || line[i++] != '"') {
        return false;
    }
    size_t file_start = i;
    while (i < len && line[i] != '"') {
        i++;
    }
    if (i >= len) {
        return false;
    }
    *out_line = (uint32_t)parsed_line;
    *out_file = std::string(line + file_start, i - file_start);
    return true;
}

static bool build_line_map(const std::string &expanded, const std::string &main_file,
                           uint32_t *original_line_by_expanded_line,
                           uint8_t *belongs_to_main_file) {
    std::string current_file = main_file;
    uint32_t current_line = 1;
    int expanded_line = 1;
    size_t line_start = 0;

    while (line_start <= expanded.size()) {
        size_t line_end = expanded.find('\n', line_start);
        if (line_end == std::string::npos) {
            line_end = expanded.size();
        }

        uint32_t directive_line = 0;
        std::string directive_file;
        if (parse_line_directive(expanded.c_str() + line_start, line_end - line_start,
                                 &directive_line, &directive_file)) {
            current_file = directive_file;
            current_line = directive_line;
            original_line_by_expanded_line[expanded_line] = 0;
            belongs_to_main_file[expanded_line] = 0;
        } else {
            original_line_by_expanded_line[expanded_line] = current_line;
            belongs_to_main_file[expanded_line] = current_file == main_file ? 1 : 0;
            if (current_line < UINT32_MAX) {
                current_line++;
            }
        }

        if (line_end == expanded.size()) {
            break;
        }
        line_start = line_end + 1;
        expanded_line++;
    }
    return true;
}

CBMPreprocessedSource *cbm_preprocess_with_map(const char *source, int source_len,
                                               const char *filename, const char **extra_defines,
                                               const char **include_paths, int cpp_mode) {
    // Run the second pass when there are directives to evaluate OR when the file
    // carries export-macro-shaped identifiers to predefine empty (#1989) — a UE
    // plugin header with only `#pragma once` + `#include` lines still needs it.
    if (!has_preprocessor_work(source, source_len) &&
        !has_export_macro_candidates(source, source_len)) {
        return NULL; // NULL = no expansion needed, use original
    }

    try {
        simplecpp::DUI dui;
        if (extra_defines) {
            for (int i = 0; extra_defines[i]; i++)
                dui.defines.push_back(extra_defines[i]);
        }
        // Predefine collected export-macro candidates as empty, mirroring the
        // real compile command line (`/D "MODULE_API="`). Names already provided
        // by the caller win — never override an explicit define.
        char export_cands[CBM_EXPORT_MACRO_MAX][CBM_EXPORT_MACRO_NAME_MAX];
        int export_cand_count =
            collect_export_macro_candidates(source, source_len, export_cands, CBM_EXPORT_MACRO_MAX);
        for (int i = 0; i < export_cand_count; i++) {
            bool provided = false;
            for (std::list<std::string>::const_iterator it = dui.defines.begin();
                 it != dui.defines.end(); ++it) {
                const std::string &def = *it;
                size_t eq = def.find('=');
                std::string name = (eq == std::string::npos) ? def : def.substr(0, eq);
                if (name == export_cands[i]) {
                    provided = true;
                    break;
                }
            }
            if (!provided) {
                dui.defines.push_back(std::string(export_cands[i]) + "=");
            }
        }
        if (include_paths) {
            for (int i = 0; include_paths[i]; i++)
                dui.includePaths.push_back(include_paths[i]);
        }
        dui.std = cpp_mode ? "c++17" : "c11";

        std::string src(source, source_len);
        std::istringstream istr(src);
        std::vector<std::string> files;
        files.push_back(filename ? filename : "<input>");

        simplecpp::TokenList rawtokens(istr, files, files[0]);
        simplecpp::TokenList output(files);
        simplecpp::FileDataCache filedata = simplecpp::load(rawtokens, files, dui);

        simplecpp::preprocess(output, rawtokens, files, filedata, dui);

        std::string result = output.stringify();

        // Clean up loaded file data
        simplecpp::cleanup(filedata);

        CBMPreprocessedSource *pp = (CBMPreprocessedSource *)calloc(1, sizeof(*pp));
        if (!pp) {
            return NULL;
        }
        int line_count = count_expanded_lines(result);
        pp->source = (char *)malloc(result.size() + 1);
        pp->original_line_by_expanded_line =
            (uint32_t *)calloc((size_t)line_count + 1u, sizeof(uint32_t));
        pp->belongs_to_main_file = (uint8_t *)calloc((size_t)line_count + 1u, sizeof(uint8_t));
        pp->expanded_line_count = line_count;
        if (!pp->source || !pp->original_line_by_expanded_line || !pp->belongs_to_main_file) {
            cbm_preprocessed_source_free(pp);
            return NULL;
        }
        memcpy(pp->source, result.c_str(), result.size() + 1);
        if (!build_line_map(result, files[0], pp->original_line_by_expanded_line,
                            pp->belongs_to_main_file)) {
            cbm_preprocessed_source_free(pp);
            return NULL;
        }
        return pp;
    } catch (...) {
        // Graceful fallback: return NULL = use original source
        return NULL;
    }
}

char *cbm_preprocess(const char *source, int source_len, const char *filename,
                     const char **extra_defines, const char **include_paths, int cpp_mode) {
    CBMPreprocessedSource *pp = cbm_preprocess_with_map(source, source_len, filename, extra_defines,
                                                        include_paths, cpp_mode);
    if (!pp) {
        return NULL;
    }
    char *out = pp->source;
    pp->source = NULL;
    cbm_preprocessed_source_free(pp);
    return out;
}

void cbm_preprocess_free(char *expanded) {
    free(expanded);
}

void cbm_preprocessed_source_free(CBMPreprocessedSource *pp) {
    if (!pp) {
        return;
    }
    free(pp->source);
    free(pp->original_line_by_expanded_line);
    free(pp->belongs_to_main_file);
    free(pp);
}

} // extern "C"
