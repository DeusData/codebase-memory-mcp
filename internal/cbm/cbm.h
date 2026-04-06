#ifndef CBM_H
#define CBM_H

#include <stdint.h>
#include <stdbool.h>
#include "arena.h"
#include "tree_sitter/api.h"

// Language enum mirrors lang.Language in Go.
// Order must match lang_specs.c tables.
typedef enum {
    CBM_LANG_GO = 0,
    CBM_LANG_PYTHON,
    CBM_LANG_JAVASCRIPT,
    CBM_LANG_TYPESCRIPT,
    CBM_LANG_TSX,
    CBM_LANG_RUST,
    CBM_LANG_JAVA,
    CBM_LANG_CPP,
    CBM_LANG_CSHARP,
    CBM_LANG_PHP,
    CBM_LANG_LUA,
    CBM_LANG_SCALA,
    CBM_LANG_KOTLIN,
    CBM_LANG_RUBY,
    CBM_LANG_C,
    CBM_LANG_BASH,
    CBM_LANG_ZIG,
    CBM_LANG_ELIXIR,
    CBM_LANG_HASKELL,
    CBM_LANG_OCAML,
    CBM_LANG_OBJC,
    CBM_LANG_SWIFT,
    CBM_LANG_DART,
    CBM_LANG_PERL,
    CBM_LANG_GROOVY,
    CBM_LANG_ERLANG,
    CBM_LANG_R,
    CBM_LANG_HTML,
    CBM_LANG_CSS,
    CBM_LANG_SCSS,
    CBM_LANG_YAML,
    CBM_LANG_TOML,
    CBM_LANG_HCL,
    CBM_LANG_SQL,
    CBM_LANG_DOCKERFILE,
    // New languages (v0.5 expansion)
    CBM_LANG_CLOJURE,
    CBM_LANG_FSHARP,
    CBM_LANG_JULIA,
    CBM_LANG_VIMSCRIPT,
    CBM_LANG_NIX,
    CBM_LANG_COMMONLISP,
    CBM_LANG_ELM,
    CBM_LANG_FORTRAN,
    CBM_LANG_CUDA,
    CBM_LANG_COBOL,
    CBM_LANG_VERILOG,
    CBM_LANG_EMACSLISP,
    CBM_LANG_JSON,
    CBM_LANG_XML,
    CBM_LANG_MARKDOWN,
    CBM_LANG_MAKEFILE,
    CBM_LANG_CMAKE,
    CBM_LANG_PROTOBUF,
    CBM_LANG_GRAPHQL,
    CBM_LANG_VUE,
    CBM_LANG_SVELTE,
    CBM_LANG_MESON,
    CBM_LANG_GLSL,
    CBM_LANG_INI,
    // Scientific/math languages
    CBM_LANG_MATLAB,
    CBM_LANG_LEAN,
    CBM_LANG_FORM,
    CBM_LANG_MAGMA,
    CBM_LANG_WOLFRAM,
    CBM_LANG_KUSTOMIZE, // kustomization.yaml — Kubernetes overlay tool
    CBM_LANG_K8S,       // Generic Kubernetes manifest (apiVersion: detected)
    CBM_LANG_COUNT
} CBMLanguage;

// --- Extraction result structs ---

typedef struct {
    const char *name;           // short name
    const char *qualified_name; // project.path.name
    const char *label;          // "Function", "Method", "Class", "Variable", "Module"
    const char *file_path;      // relative path
    uint32_t start_line;
    uint32_t end_line;
    const char *signature;     // parameter text (NULL if none)
    const char *return_type;   // return type text (NULL if none)
    const char *receiver;      // Go method receiver (NULL if none)
    const char *docstring;     // leading doc comment (NULL if none)
    const char *parent_class;  // enclosing class QN for methods (NULL if none)
    const char **decorators;   // NULL-terminated array (NULL if none)
    const char **base_classes; // NULL-terminated array (NULL if none)
    const char **param_names;  // NULL-terminated array (NULL if none)
    const char **param_types;  // NULL-terminated array (NULL if none)
    const char **return_types; // NULL-terminated array (NULL if none)
    const char *route_path;    // HTTP route path from decorator (e.g., "/api/users") or NULL
    const char *route_method;  // HTTP method from decorator (e.g., "POST") or NULL
    int complexity;            // cyclomatic complexity
    int lines;                 // body line count
    uint32_t *fingerprint;     // MinHash fingerprint (arena-allocated, K values) or NULL
    int fingerprint_k;         // number of hash values (CBM_MINHASH_K or 0)
    bool is_exported;
    bool is_abstract;
    bool is_test;
    bool is_entry_point;
    const char *structural_profile; // AST structural profile (arena-allocated) or NULL
    const char *body_tokens; // space-separated raw identifier tokens from body (arena) or NULL
} CBMDefinition;

/* Argument captured from a call expression */
typedef struct {
    const char *expr;    // raw expression text ("payload.info", "MY_URL", "'hello'")
    const char *value;   // resolved string value or NULL (constant propagation)
    const char *keyword; // keyword name if keyword arg ("url", "topic_id"), NULL if positional
    int index;           // positional index (0-based)
} CBMCallArg;

#define CBM_MAX_CALL_ARGS 8

typedef struct {
    const char *callee_name;            // raw callee text ("pkg.Func", "foo")
    const char *enclosing_func_qn;      // QN of enclosing function (or module QN)
    const char *first_string_arg;       // first string literal argument (URL, topic, key) or NULL
    const char *second_arg_name;        // second argument identifier (handler ref) or NULL
    CBMCallArg args[CBM_MAX_CALL_ARGS]; // first N arguments with expressions
    int arg_count;                      // number of captured arguments
} CBMCall;

typedef struct {
    const char *local_name;  // local alias or name
    const char *module_path; // resolved module path / QN
} CBMImport;

typedef struct {
    const char *ref_name;          // referenced identifier
    const char *enclosing_func_qn; // QN of enclosing function (or module QN)
} CBMUsage;

typedef struct {
    const char *exception_name;    // exception class/type name
    const char *enclosing_func_qn; // QN of enclosing function
} CBMThrow;

typedef struct {
    const char *var_name;          // variable name
    const char *enclosing_func_qn; // QN of enclosing function
    bool is_write;                 // true = write, false = read
} CBMReadWrite;

typedef struct {
    const char *type_name;         // referenced type/class name
    const char *enclosing_func_qn; // QN of enclosing function
} CBMTypeRef;

typedef struct {
    const char *env_key;           // environment variable key
    const char *enclosing_func_qn; // QN of enclosing function
} CBMEnvAccess;

typedef struct {
    const char *var_name;          // variable being assigned
    const char *type_name;         // class/type name of RHS constructor
    const char *enclosing_func_qn; // QN of enclosing function
} CBMTypeAssign;

// String reference: URL, config key, or async target found in source.
// Extracted from string literals during AST walk.
typedef enum {
    CBM_STRREF_URL = 0,    // REST path or full URL
    CBM_STRREF_CONFIG = 1, // config file path or env var key
} CBMStringRefKind;

typedef struct {
    const char *value;             // the string literal content
    const char *enclosing_func_qn; // QN of enclosing function
    const char *key_path;          // dotted key path from YAML/JSON nesting (NULL if flat)
    CBMStringRefKind kind;         // URL, CONFIG
} CBMStringRef;

/* Infrastructure binding: topic/queue → endpoint URL.
 * Extracted from YAML/HCL/JSON subscription/scheduler configs.
 * Used by pass_route_nodes to connect async Route nodes to handler services. */
typedef struct {
    const char *source_name; // topic, queue, or schedule name
    const char *target_url;  // push_endpoint, uri, or http_target URL
    const char *broker;      // "pubsub", "cloud_tasks", "cloud_scheduler", "sqs", "kafka"
} CBMInfraBinding;

/* Pub/sub channel participation.  One record per emit() or on()/addListener()
 * call detected in source — the receiver (e.g. Socket.IO client, EventEmitter
 * instance) is intentionally NOT identified; matching is by channel_name
 * across files, which captures the common pattern of one logical bus per
 * service.  Transport disambiguates Socket.IO vs EventEmitter vs future
 * detectors (Kafka, Cloud Pub/Sub, etc.). */
typedef enum {
    CBM_CHANNEL_EMIT = 0,
    CBM_CHANNEL_LISTEN = 1,
} CBMChannelDirection;

typedef struct {
    const char *channel_name;      // literal channel name (e.g. "user.created")
    const char *transport;         // "socketio", "event_emitter", ...
    const char *enclosing_func_qn; // QN of the function containing the emit/on call
    CBMChannelDirection direction;
} CBMChannel;

// Rust: impl Trait for Struct
typedef struct {
    const char *trait_name;  // trait name (raw text)
    const char *struct_name; // struct/type name (raw text)
} CBMImplTrait;

// LSP-resolved call: high-confidence type-aware call resolution
typedef struct {
    const char *caller_qn; // enclosing function QN
    const char *callee_qn; // resolved target QN (fully qualified)
    const char *strategy;  // "lsp_type_dispatch", "lsp_direct", etc.
    float confidence;      // 0.90-0.95
    const char *reason;    // diagnostic label for unresolved calls (NULL if resolved)
} CBMResolvedCall;

typedef struct {
    CBMResolvedCall *items;
    int count;
    int cap;
} CBMResolvedCallArray;

// Growable arrays used during extraction.
typedef struct {
    CBMDefinition *items;
    int count;
    int cap;
} CBMDefArray;

typedef struct {
    CBMCall *items;
    int count;
    int cap;
} CBMCallArray;

typedef struct {
    CBMImport *items;
    int count;
    int cap;
} CBMImportArray;

typedef struct {
    CBMUsage *items;
    int count;
    int cap;
} CBMUsageArray;

typedef struct {
    CBMThrow *items;
    int count;
    int cap;
} CBMThrowArray;

typedef struct {
    CBMReadWrite *items;
    int count;
    int cap;
} CBMRWArray;

typedef struct {
    CBMTypeRef *items;
    int count;
    int cap;
} CBMTypeRefArray;

typedef struct {
    CBMEnvAccess *items;
    int count;
    int cap;
} CBMEnvAccessArray;

typedef struct {
    CBMTypeAssign *items;
    int count;
    int cap;
} CBMTypeAssignArray;

typedef struct {
    CBMStringRef *items;
    int count;
    int cap;
} CBMStringRefArray;

typedef struct {
    CBMInfraBinding *items;
    int count;
    int cap;
} CBMInfraBindingArray;

typedef struct {
    CBMImplTrait *items;
    int count;
    int cap;
} CBMImplTraitArray;

typedef struct {
    CBMChannel *items;
    int count;
    int cap;
} CBMChannelArray;

// Full extraction result for one file.
typedef struct {
    CBMArena arena; // owns all string memory

    CBMDefArray defs;
    CBMCallArray calls;
    CBMImportArray imports;
    CBMUsageArray usages;
    CBMThrowArray throws;
    CBMRWArray rw;
    CBMTypeRefArray type_refs;
    CBMEnvAccessArray env_accesses;
    CBMTypeAssignArray type_assigns;
    CBMImplTraitArray impl_traits;       // Rust: impl Trait for Struct pairs
    CBMResolvedCallArray resolved_calls; // LSP-resolved calls (high confidence)
    CBMStringRefArray string_refs;       // URL/config string literals from AST
    CBMInfraBindingArray infra_bindings; // topic→URL pairs from IaC configs
    CBMChannelArray channels;            // Socket.IO / EventEmitter pub/sub participation

    const char *module_qn;    // module qualified name
    const char **exports;     // NULL-terminated (NULL if none)
    const char **constants;   // NULL-terminated (NULL if none)
    const char **global_vars; // NULL-terminated (NULL if none)
    const char **macros;      // NULL-terminated, C/C++ only (NULL if none)

    bool has_error;
    const char *error_msg;
    bool is_test_file;
    int imports_count;
    TSTree *cached_tree;     // retained parse tree (caller frees via cbm_free_tree)
    CBMLanguage cached_lang; // language of cached tree (for parser selection)
} CBMFileResult;

// --- Enclosing function cache ---
// Avoids repeated parent-chain walks for nodes within the same function body.
// Each entry records a function's byte range and its precomputed QN.
#define EFC_SIZE 64 // power of 2 for fast modulo

typedef struct {
    uint32_t start_byte;
    uint32_t end_byte;
    const char *qn;
} EFCEntry;

typedef struct {
    EFCEntry entries[EFC_SIZE];
    int count;
} EFCache;

// --- Extraction context passed to sub-extractors ---

// Module-level string constant map (for constant propagation)
#define CBM_MAX_STRING_CONSTANTS 256
typedef struct {
    const char *names[CBM_MAX_STRING_CONSTANTS];
    const char *values[CBM_MAX_STRING_CONSTANTS];
    int count;
} CBMStringConstantMap;

typedef struct {
    CBMArena *arena;
    CBMFileResult *result;
    const char *source;
    int source_len;
    CBMLanguage language;
    const char *project;
    const char *rel_path;
    const char *module_qn;
    TSNode root;
    EFCache ef_cache;                      // enclosing function cache
    const char *enclosing_class_qn;        // for nested class QN computation
    CBMStringConstantMap string_constants; // module-level NAME = "value" pairs
} CBMExtractCtx;

// --- Public API ---

// Initialize the library. Call once at startup. Returns 0 on success.
int cbm_init(void);

// Extract all data from one file. Caller must call cbm_free_result().
// source must remain valid for the duration of the call.
// timeout_micros: per-file parse timeout in microseconds (0 = no timeout).
CBMFileResult *cbm_extract_file(const char *source, int source_len, CBMLanguage language,
                                const char *project, const char *rel_path, int64_t timeout_micros,
                                const char **extra_defines, // NULL-terminated, or NULL
                                const char **include_paths  // NULL-terminated, or NULL
);

// Free all memory associated with a result.
void cbm_free_result(CBMFileResult *result);

// Free only the cached tree from a result (caller retained it for reuse).
void cbm_free_tree(CBMFileResult *result);

// Free a standalone TSTree pointer (for Go layer cleanup).
void cbm_free_tree_ptr(TSTree *tree);

// Reset the thread-local parser's internal state, releasing slab-allocated
// subtrees. Must be called BEFORE cbm_slab_reset_thread() so the slab rebuild
// doesn't corrupt live parser state.
void cbm_reset_thread_parser(void);

// Destroy the thread-local parser. Call on worker thread exit.
void cbm_destroy_thread_parser(void);

// Shutdown the library. Call once at exit.
void cbm_shutdown(void);

// Profiling: get accumulated parse/extraction times and file count.
typedef struct {
    uint64_t *parse_ns;
    uint64_t *extract_ns;
    uint64_t *files;
} cbm_profile_out_t;
void cbm_get_profile(cbm_profile_out_t out);
uint64_t cbm_get_lsp_ns(void);
uint64_t cbm_get_preprocess_ns(void);
uint64_t cbm_get_files_preprocessed(void);
void cbm_reset_profile(void);

// --- Internal helpers used by extractors ---

// Growable array push functions (arena-allocated, no individual free needed).
void cbm_defs_push(CBMDefArray *arr, CBMArena *a, CBMDefinition def);
void cbm_calls_push(CBMCallArray *arr, CBMArena *a, CBMCall call);
void cbm_imports_push(CBMImportArray *arr, CBMArena *a, CBMImport imp);
void cbm_usages_push(CBMUsageArray *arr, CBMArena *a, CBMUsage usage);
void cbm_throws_push(CBMThrowArray *arr, CBMArena *a, CBMThrow thr);
void cbm_rw_push(CBMRWArray *arr, CBMArena *a, CBMReadWrite rw);
void cbm_typerefs_push(CBMTypeRefArray *arr, CBMArena *a, CBMTypeRef tr);
void cbm_envaccess_push(CBMEnvAccessArray *arr, CBMArena *a, CBMEnvAccess ea);
void cbm_typeassign_push(CBMTypeAssignArray *arr, CBMArena *a, CBMTypeAssign ta);
void cbm_stringref_push(CBMStringRefArray *arr, CBMArena *a, CBMStringRef sr);
void cbm_infrabinding_push(CBMInfraBindingArray *arr, CBMArena *a, CBMInfraBinding ib);
void cbm_impltrait_push(CBMImplTraitArray *arr, CBMArena *a, CBMImplTrait it);
void cbm_resolvedcall_push(CBMResolvedCallArray *arr, CBMArena *a, CBMResolvedCall rc);
void cbm_channels_push(CBMChannelArray *arr, CBMArena *a, CBMChannel ch);

// --- Sub-extractor entry points ---

void cbm_extract_definitions(CBMExtractCtx *ctx);
void cbm_extract_calls(CBMExtractCtx *ctx);
void cbm_extract_imports(CBMExtractCtx *ctx);
void cbm_extract_usages(CBMExtractCtx *ctx);
void cbm_extract_semantic(CBMExtractCtx *ctx);
void cbm_extract_type_refs(CBMExtractCtx *ctx);
void cbm_extract_env_accesses(CBMExtractCtx *ctx);
void cbm_extract_type_assigns(CBMExtractCtx *ctx);
void cbm_extract_channels(CBMExtractCtx *ctx);

// Single-pass unified extraction (replaces the 7 calls above except defs+imports).
void cbm_extract_unified(CBMExtractCtx *ctx);

// K8s / Kustomize semantic extractor (called when language is CBM_LANG_K8S or CBM_LANG_KUSTOMIZE).
void cbm_extract_k8s(CBMExtractCtx *ctx);

#endif // CBM_H
