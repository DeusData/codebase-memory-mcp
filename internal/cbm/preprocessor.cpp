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
    if (!has_preprocessor_work(source, source_len)) {
        return NULL; // NULL = no expansion needed, use original
    }

    try {
        simplecpp::DUI dui;
        if (extra_defines) {
            for (int i = 0; extra_defines[i]; i++)
                dui.defines.push_back(extra_defines[i]);
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
