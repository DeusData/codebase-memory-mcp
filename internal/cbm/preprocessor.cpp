// Unity build: include simplecpp implementation directly since CGo only
// compiles .cpp files from the immediate package directory, not subdirs.
#include "vendored/simplecpp/simplecpp.cpp"

#include "preprocessor.h"
#include "vendored/simplecpp/simplecpp.h"

#include <deque>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <cstring>
#include <cstdlib>
#include <cstdint>

// ── #include lookup against the indexed repository root (#2993) ────────────
// simplecpp looks a quoted header up as `dirPath(includer) + name` and opens it
// relative to the process working directory, so the expansion depended on the
// directory the indexer was started from. Here every #include and
// __has_include of the file, and of each header it reaches, is resolved before
// simplecpp runs: a quoted name in the including file's directory within the
// repository root, read through the caller's CBMIncludeReader; an angle-bracket
// name resolves to nothing, as no include directories are configured. The
// result is stored in simplecpp's FileDataCache under the exact key its own
// lookup (FileDataCache::get) computes, so simplecpp opens no file itself and
// the expansion is a function of the indexed tree alone.
namespace {

// A computed `#include MACRO` line names its header only after macro expansion,
// which no lookup made ahead of simplecpp can see. Its directive is renamed to
// one simplecpp does not know, and an unknown directive includes nothing.
const char kUnresolvedIncludeDirective[] = "cbm_unresolved_include";

// True when `key`, the normalised name an include resolves to, names a file
// within the indexed repository root: the name was written relative to the
// including file, and every component of the normalised result names an entry
// of the tree. The rule is the same on every platform, so one tree resolves the
// same way everywhere.
bool names_tree_file(const std::string &header, const std::string &key) {
    if (header.empty() || header[0] == '/' || header[0] == '\\' ||
        (header.size() >= 2U && header[1] == ':')) {
        return false;
    }
    if (key.empty() || key[0] == '/' || key.find(':') != std::string::npos ||
        key.find('\\') != std::string::npos || key.find('\0') != std::string::npos) {
        return false;
    }
    std::string::size_type start = 0;
    for (;;) {
        const std::string::size_type end = key.find('/', start);
        const std::string part =
            key.substr(start, end == std::string::npos ? std::string::npos : end - start);
        if (part == "..") {
            return false;
        }
        if (end == std::string::npos) {
            return true;
        }
        start = end + 1U;
    }
}

// The first non-comment token after `tok` on the same line, or nullptr.
simplecpp::Token *next_on_line(simplecpp::Token *tok) {
    simplecpp::Token *next = tok->next;
    while (next && next->comment) {
        next = next->next;
    }
    return sameline(tok, next) ? next : nullptr;
}

class IncludeTree {
  public:
    IncludeTree(const CBMIncludeReader *reader, std::vector<std::string> &files,
                simplecpp::FileDataCache &cache)
        : reader_(reader), files_(files), cache_(cache) {}

    // Resolve everything the main file reaches. Every file name its own
    // tokens registered (itself, and names from its #line directives) is the
    // main file's; every later one belongs to a header.
    void add_main_file(simplecpp::TokenList &tokens) {
        first_header_file_ = static_cast<unsigned int>(files_.size());
        prepare(tokens);
        while (!pending_.empty()) {
            Pending next = std::move(pending_.front());
            pending_.pop_front();
            prepare(next.tokens);
            cache_.insert(simplecpp::FileData{next.key, std::move(next.tokens)});
        }
    }

    // Tokens whose file index is at or above this came from a header.
    unsigned int first_header_file() const {
        return first_header_file_;
    }

  private:
    struct Pending {
        std::string key;
        simplecpp::TokenList tokens;
    };

    // Rewrite one file's __has_include tests and computed includes, and
    // resolve the headers its #include lines name.
    void prepare(simplecpp::TokenList &tokens) {
        for (simplecpp::Token *tok = tokens.front(); tok; tok = tok->next) {
            if (tok->str() == HAS_INCLUDE) {
                rewrite_has_include(tokens, tok);
            } else if (tok->op == '#' && !sameline(tok->previousSkipComments(), tok)) {
                simplecpp::Token *directive = next_on_line(tok);
                if (directive && directive->str() == INCLUDE) {
                    resolve_include(directive);
                }
            }
        }
    }

    // `#include "name"` / `#include <name>` as simplecpp's preprocess reads it:
    // the first non-comment token after the directive, quotes or brackets
    // stripped.
    void resolve_include(simplecpp::Token *directive) {
        const simplecpp::Token *operand = next_on_line(directive);
        if (!operand) {
            return; // no header name: simplecpp reports it and opens nothing
        }
        if (operand->name) {
            directive->setstr(kUnresolvedIncludeDirective);
            return;
        }
        const std::string &spelled = operand->str();
        if (spelled.size() <= 2U) {
            return; // no header name: simplecpp reports it and opens nothing
        }
        (void)lookup(file_of(directive), spelled.substr(1U, spelled.size() - 2U),
                     spelled[0] == '<');
    }

    // `__has_include ( name )` (or without the parentheses) as simplecpp's #if
    // evaluation reads it becomes the literal 1 or 0 of the same lookup. The
    // operand of `defined` / `#ifdef` / `#ifndef` stays as it is. A form that
    // does not close on its line keeps its operand, and its #if still fails to
    // evaluate, as it did before.
    void rewrite_has_include(simplecpp::TokenList &tokens, simplecpp::Token *tok) {
        const simplecpp::Token *before = tok->previousSkipComments();
        if (before && before->op == '(' && sameline(before, tok)) {
            before = before->previousSkipComments();
        }
        if (before && sameline(before, tok) &&
            (before->str() == DEFINED || before->str() == IFDEF || before->str() == IFNDEF)) {
            return;
        }
        simplecpp::Token *operand = next_on_line(tok);
        const bool par = operand && operand->op == '(';
        if (par) {
            operand = next_on_line(operand);
        }
        bool found = false;
        simplecpp::Token *last = operand;
        if (operand && operand->op == '<') {
            while (last && last->op != '>') {
                last = next_on_line(last);
            }
        } else if (operand && operand->str().size() > 2U && operand->str()[0] == '"') {
            const std::string &spelled = operand->str();
            found = lookup(file_of(tok), spelled.substr(1U, spelled.size() - 2U), false);
        }
        if (last && par) {
            last = next_on_line(last);
            if (last && last->op != ')') {
                last = nullptr;
            }
        }
        tok->setstr(found ? "1" : "0");
        if (!last) {
            return;
        }
        while (tok->next != last) {
            tokens.deleteToken(tok->next);
        }
        tokens.deleteToken(last);
    }

    const std::string &file_of(const simplecpp::Token *tok) const {
        static const std::string none;
        return tok->location.fileIndex < files_.size() ? files_[tok->location.fileIndex] : none;
    }

    // Key the name the way simplecpp's FileDataCache::get does and make sure
    // the cache holds that key, so get() finds it instead of opening a file.
    // True when the name resolved to a file of the indexed tree.
    bool lookup(const std::string &sourcefile, const std::string &header, bool system) {
        if (simplecpp::isAbsolutePath(header)) {
            return load(simplecpp::simplifyPath(header), false);
        }
        if (system) {
            return false; // no include directories: get() opens nothing
        }
        const std::string key = simplecpp::simplifyPath(dirPath(sourcefile) + header);
        return load(key, names_tree_file(header, key));
    }

    // Cache `key` once: its tokens when the tree holds it, an empty token list
    // otherwise (the include then adds nothing). The main file never resolves
    // as a header: its tokens are the expansion itself.
    bool load(const std::string &key, bool in_tree) {
        const std::map<std::string, bool>::const_iterator seen = loaded_.find(key);
        if (seen != loaded_.end()) {
            return seen->second;
        }
        std::string text;
        const bool found = in_tree && !names_main_file(key) && read(key, &text);
        loaded_[key] = found;
        if (!found) {
            cache_.insert(simplecpp::FileData{key, simplecpp::TokenList(files_)});
            return false;
        }
        std::istringstream istr(text);
        pending_.push_back(Pending{key, simplecpp::TokenList(istr, files_, key)});
        return true;
    }

    bool names_main_file(const std::string &key) const {
        for (unsigned int i = 0; i < first_header_file_ && i < files_.size(); i++) {
            if (files_[i] == key) {
                return true;
            }
        }
        return false;
    }

    bool read(const std::string &key, std::string *out) const {
        if (!reader_ || !reader_->read) {
            return false;
        }
        size_t len = 0;
        char *data = reader_->read(reader_->userdata, key.c_str(), &len);
        if (!data) {
            return false;
        }
        out->assign(data, len);
        if (reader_->release) {
            reader_->release(reader_->userdata, data);
        }
        return true;
    }

    const CBMIncludeReader *reader_;
    std::vector<std::string> &files_;
    simplecpp::FileDataCache &cache_;
    std::map<std::string, bool> loaded_;
    std::deque<Pending> pending_;
    unsigned int first_header_file_ = 0;
};

} // namespace

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

// True when `prev` and `tok` touched in the source text: same file and line,
// `prev` ending exactly where `tok` starts (simplecpp's own adjacency test),
// and neither produced by a macro expansion (expansion tokens carry the call
// site's location, so their columns prove nothing).
static bool tokens_source_adjacent(const simplecpp::Token *prev, const simplecpp::Token *tok) {
    if (!prev->macro.empty() || !tok->macro.empty() || !prev->location.sameline(tok->location)) {
        return false;
    }
    const std::string &s = prev->str();
    if (s.find_first_of("\r\n") != std::string::npos) {
        return false;
    }
    return prev->location.col + s.size() == tok->location.col;
}

// The expanded buffer in the source's own spelling. Mirrors
// simplecpp::TokenList::stringify() line for line — the same newlines and
// #line directives, so build_line_map and every line-based remap are unchanged —
// except that tokens which touched in the source stay touching. stringify()
// joins every same-line pair with one space, so the second pass spelled a
// multi-token name differently from the raw pass: the explicit specialization
// `template<> struct bitonic<int64_t, AVX2>` became `bitonic < int64_t , AVX2 >`,
// and every QN built from that text (defs adopted by the #961 rescue, call
// scopes, C-LSP class scopes) minted a second node for the same definition, the
// QN dedup (strcmp) missing it. A gap that held whitespace or a comment is still
// one space, as before.
//
// Tokens of an included header's body (file index at or above
// first_header_file) are left out: the header is indexed as its own file, and
// its calls, usages and writes are its own, never the includer's. What the
// header defines still shapes the main file's tokens, since macro expansions
// carry the location of their use. With no header body in the buffer, the main
// file's lines keep their own numbers, as they do when nothing is included.
static std::string stringify_source_spacing(const simplecpp::TokenList &tokens,
                                            unsigned int first_header_file) {
    std::ostringstream ret;
    simplecpp::Location loc;
    loc.line = 1;
    const simplecpp::Token *prev = nullptr;
    for (const simplecpp::Token *tok = tokens.cfront(); tok; tok = tok->next) {
        if (tok->location.fileIndex >= first_header_file) {
            continue;
        }
        if (tok->location.line < loc.line || tok->location.fileIndex != loc.fileIndex) {
            ret << "\n#line " << tok->location.line << " \"" << tokens.file(tok->location)
                << "\"\n";
            loc = tok->location;
        }
        while (tok->location.line > loc.line) {
            ret << '\n';
            loc.line++;
        }
        if (prev && prev->location.sameline(tok->location) && !tokens_source_adjacent(prev, tok)) {
            ret << ' ';
        }
        ret << tok->str();
        loc.adjust(tok->str());
        prev = tok;
    }
    return ret.str();
}

// simplecpp drops the whole expansion when it stops at #error, or at a
// directive it cannot read or evaluate: the output list is cleared and an
// ERROR or SYNTAX_ERROR is reported.
static bool expansion_discarded(const simplecpp::TokenList &output,
                                const simplecpp::OutputList &outputs) {
    if (!output.empty()) {
        return false;
    }
    for (simplecpp::OutputList::const_iterator it = outputs.begin(); it != outputs.end(); ++it) {
        if (it->type == simplecpp::Output::ERROR || it->type == simplecpp::Output::SYNTAX_ERROR) {
            return true;
        }
    }
    return false;
}

// One simplecpp run over the file. Every header its #include and
// __has_include lines reach is resolved through `reader` first (none when it
// is NULL). Returns false when simplecpp discarded the expansion.
static bool preprocess_file(const std::string &src, const std::string &main_name,
                            const simplecpp::DUI &dui, const CBMIncludeReader *reader,
                            std::string *expanded) {
    std::vector<std::string> files;
    files.push_back(main_name);
    std::istringstream istr(src);
    simplecpp::TokenList rawtokens(istr, files, files[0]);
    simplecpp::FileDataCache cache;
    IncludeTree tree(reader, files, cache);
    tree.add_main_file(rawtokens);
    simplecpp::TokenList output(files);
    simplecpp::OutputList outputs;
    simplecpp::preprocess(output, rawtokens, files, cache, dui, &outputs);
    const bool kept = !expansion_discarded(output, outputs);
    *expanded = stringify_source_spacing(output, tree.first_header_file());
    simplecpp::cleanup(cache);
    return kept;
}

// The file's expansion with its includes followed. When what an include
// brought in stops simplecpp (an #error reached inside a header, a header
// directive it cannot evaluate, a failure while reading one), the file keeps
// its expansion without includes instead of losing it.
static std::string preprocess_with_fallback(const std::string &src, const std::string &main_name,
                                            const simplecpp::DUI &dui,
                                            const CBMIncludeReader *reader) {
    std::string expanded;
    if (reader) {
        try {
            if (preprocess_file(src, main_name, dui, reader, &expanded)) {
                return expanded;
            }
        } catch (...) {
            // fall through to the expansion without includes
        }
        expanded.clear();
    }
    (void)preprocess_file(src, main_name, dui, nullptr, &expanded);
    return expanded;
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
                                               const char **include_paths, int cpp_mode,
                                               const CBMIncludeReader *reader) {
    (void)include_paths; // lookups resolve through `reader` only (preprocessor.h)
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
        dui.std = cpp_mode ? "c++17" : "c11";

        const std::string src(source, source_len);
        const std::string main_name(filename ? filename : "<input>");
        std::string result = preprocess_with_fallback(src, main_name, dui, reader);

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
        if (!build_line_map(result, main_name, pp->original_line_by_expanded_line,
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
                                                        include_paths, cpp_mode, NULL);
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
