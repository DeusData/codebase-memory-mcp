#ifndef CBM_PREPROCESSOR_H
#define CBM_PREPROCESSOR_H

#include <stddef.h>
#include <stdint.h>

/* #1989: build-system export macros (UE "<MOD>_API", CMake generate_export_header
 * "<lib>_EXPORT", ...) are defined empty on the real compile command line but are
 * invisible to tree-sitter, so `class MOD_API Foo` misparses with the macro token
 * as the type name. The preprocessed second pass predefines the conventional
 * export-macro-shaped identifiers found in a source file as empty, mirroring the
 * real build line. The candidate list is bounded. */
#define CBM_EXPORT_MACRO_MAX 32
#define CBM_EXPORT_MACRO_NAME_MAX 96

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char *source;
    uint32_t *original_line_by_expanded_line; // 1-based; 0 means directive/unmapped.
    uint8_t *belongs_to_main_file;            // 1-based; true only for the original input file.
    int expanded_line_count;
} CBMPreprocessedSource;

// Collect conventional export-macro-shaped identifiers (ALL_CAPS ending in
// _API/_EXPORT/_IMPORT/_DLLEXPORT/_DEPRECATED) from the source, up to max_out
// unique names. Returns the number of names stored in out.
int cbm_export_macro_candidates(const char *source, int source_len,
                                char (*out)[CBM_EXPORT_MACRO_NAME_MAX], int max_out);

// Preprocess C/C++ source: expand macros, evaluate #ifdef, resolve #include.
// Returns malloc-allocated expanded source, or NULL if no expansion needed/on failure.
// extra_defines: NULL-terminated array of "NAME=VALUE" strings (can be NULL).
// include_paths: NULL-terminated array of directory paths for #include resolution (can be NULL).
// The returned string must be freed with cbm_preprocess_free().
char *cbm_preprocess(const char *source, int source_len, const char *filename,
                     const char **extra_defines, const char **include_paths, int cpp_mode);

// Preprocess and return source plus expanded-line -> original-line ownership map.
// Returns NULL if no expansion is needed or preprocessing fails.
// Free with cbm_preprocessed_source_free().
CBMPreprocessedSource *cbm_preprocess_with_map(const char *source, int source_len,
                                               const char *filename, const char **extra_defines,
                                               const char **include_paths, int cpp_mode);

// Free preprocessed source returned by cbm_preprocess.
void cbm_preprocess_free(char *expanded);

// Free preprocessed source and line maps returned by cbm_preprocess_with_map.
void cbm_preprocessed_source_free(CBMPreprocessedSource *pp);

#ifdef __cplusplus
}
#endif

#endif // CBM_PREPROCESSOR_H
