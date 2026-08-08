/* Reproductions for CBMLanguage-to-spec registry invariants.
 *
 * This ledger classifies raw call-node metadata and generic identifier/reference
 * vocabulary. It does not claim that every call-capable language can produce an
 * exact graph CALL_REFERENCE edge; that narrower semantic contract is covered
 * by the reference-precision suites. */
#include "test_framework.h"
#include "lang_specs.h"
#include "tree_sitter/api.h"

#include <stdio.h>
#include <string.h>

typedef enum {
    CAP_UNSET = 0,
    CAP_CALL_WITH_REFERENCE_VOCAB,
    CAP_CALL_WITHOUT_REFERENCE_VOCAB,
    CAP_NO_CALL,
    CAP_TRANSFORM_ONLY,
    CAP_UNSUPPORTED,
    CAP_COUNT
} LanguageCapability;

typedef struct {
    LanguageCapability capability;
    const char *name;
    const char *reason;
} LanguageCapabilityEntry;

size_t repro_call_argument_matrix_a_copy_language_ids(CBMLanguage *language_ids, size_t capacity);
size_t repro_call_argument_matrix_b_copy_language_ids(CBMLanguage *language_ids, size_t capacity);

#define CALL_WITH_REFERENCE_VOCAB(lang)                        \
    [CBM_LANG_##lang] = {CAP_CALL_WITH_REFERENCE_VOCAB, #lang, \
                         "call metadata and a currently recognized reference node"}
#define CALL_WITHOUT_REFERENCE_VOCAB(lang)                        \
    [CBM_LANG_##lang] = {CAP_CALL_WITHOUT_REFERENCE_VOCAB, #lang, \
                         "call metadata but no reference node recognized by extract_usages"}
#define NO_CALL(lang) \
    [CBM_LANG_##lang] = {CAP_NO_CALL, #lang, "no call-node metadata; callback case inapplicable"}
#define TRANSFORM_ONLY(lang)                        \
    [CBM_LANG_##lang] = {CAP_TRANSFORM_ONLY, #lang, \
                         "source is transformed into another registered language first"}
#define UNSUPPORTED(lang) \
    [CBM_LANG_##lang] = {CAP_UNSUPPORTED, #lang, "enum retained without a registered grammar"}

/* Explicit ledger: no default initializer is intentional. A new enum remains
 * CAP_UNSET until its call-metadata/reference-vocabulary behavior is audited. */
static const LanguageCapabilityEntry LANGUAGE_CAPABILITIES[CBM_LANG_COUNT] = {
    CALL_WITH_REFERENCE_VOCAB(GO),
    CALL_WITH_REFERENCE_VOCAB(PYTHON),
    CALL_WITH_REFERENCE_VOCAB(JAVASCRIPT),
    CALL_WITH_REFERENCE_VOCAB(TYPESCRIPT),
    CALL_WITH_REFERENCE_VOCAB(TSX),
    CALL_WITH_REFERENCE_VOCAB(RUST),
    CALL_WITH_REFERENCE_VOCAB(JAVA),
    CALL_WITH_REFERENCE_VOCAB(CPP),
    CALL_WITH_REFERENCE_VOCAB(CSHARP),
    CALL_WITHOUT_REFERENCE_VOCAB(PHP),
    CALL_WITH_REFERENCE_VOCAB(LUA),
    CALL_WITH_REFERENCE_VOCAB(SCALA),
    CALL_WITH_REFERENCE_VOCAB(KOTLIN),
    CALL_WITH_REFERENCE_VOCAB(RUBY),
    CALL_WITH_REFERENCE_VOCAB(C),
    CALL_WITHOUT_REFERENCE_VOCAB(BASH),
    CALL_WITH_REFERENCE_VOCAB(ZIG),
    CALL_WITH_REFERENCE_VOCAB(ELIXIR),
    CALL_WITH_REFERENCE_VOCAB(HASKELL),
    CALL_WITH_REFERENCE_VOCAB(OCAML),
    CALL_WITH_REFERENCE_VOCAB(OBJC),
    CALL_WITH_REFERENCE_VOCAB(SWIFT),
    CALL_WITH_REFERENCE_VOCAB(DART),
    CALL_WITHOUT_REFERENCE_VOCAB(PERL),
    CALL_WITH_REFERENCE_VOCAB(GROOVY),
    CALL_WITH_REFERENCE_VOCAB(ERLANG),
    CALL_WITH_REFERENCE_VOCAB(R),
    NO_CALL(HTML),
    CALL_WITH_REFERENCE_VOCAB(CSS),
    CALL_WITH_REFERENCE_VOCAB(SCSS),
    NO_CALL(YAML),
    NO_CALL(TOML),
    CALL_WITH_REFERENCE_VOCAB(HCL),
    CALL_WITH_REFERENCE_VOCAB(SQL),
    NO_CALL(DOCKERFILE),
    CALL_WITHOUT_REFERENCE_VOCAB(CLOJURE),
    CALL_WITH_REFERENCE_VOCAB(FSHARP),
    CALL_WITH_REFERENCE_VOCAB(JULIA),
    CALL_WITH_REFERENCE_VOCAB(VIMSCRIPT),
    CALL_WITH_REFERENCE_VOCAB(NIX),
    CALL_WITHOUT_REFERENCE_VOCAB(COMMONLISP),
    CALL_WITHOUT_REFERENCE_VOCAB(ELM),
    CALL_WITH_REFERENCE_VOCAB(FORTRAN),
    CALL_WITH_REFERENCE_VOCAB(CUDA),
    CALL_WITHOUT_REFERENCE_VOCAB(COBOL),
    CALL_WITH_REFERENCE_VOCAB(VERILOG),
    CALL_WITHOUT_REFERENCE_VOCAB(EMACSLISP),
    NO_CALL(JSON),
    NO_CALL(XML),
    NO_CALL(MARKDOWN),
    CALL_WITHOUT_REFERENCE_VOCAB(MAKEFILE),
    CALL_WITH_REFERENCE_VOCAB(CMAKE),
    NO_CALL(PROTOBUF),
    NO_CALL(GRAPHQL),
    NO_CALL(VUE),
    NO_CALL(SVELTE),
    CALL_WITH_REFERENCE_VOCAB(MESON),
    CALL_WITH_REFERENCE_VOCAB(GLSL),
    NO_CALL(INI),
    CALL_WITH_REFERENCE_VOCAB(MATLAB),
    CALL_WITH_REFERENCE_VOCAB(LEAN),
    CALL_WITH_REFERENCE_VOCAB(FORM),
    CALL_WITH_REFERENCE_VOCAB(MAGMA),
    CALL_WITHOUT_REFERENCE_VOCAB(WOLFRAM),
    CALL_WITH_REFERENCE_VOCAB(SOLIDITY),
    CALL_WITHOUT_REFERENCE_VOCAB(TYPST),
    CALL_WITH_REFERENCE_VOCAB(GDSCRIPT),
    CALL_WITH_REFERENCE_VOCAB(GLEAM),
    CALL_WITHOUT_REFERENCE_VOCAB(POWERSHELL),
    CALL_WITH_REFERENCE_VOCAB(PASCAL),
    CALL_WITH_REFERENCE_VOCAB(DLANG),
    UNSUPPORTED(NIM),
    CALL_WITHOUT_REFERENCE_VOCAB(SCHEME),
    CALL_WITHOUT_REFERENCE_VOCAB(FENNEL),
    CALL_WITHOUT_REFERENCE_VOCAB(FISH),
    CALL_WITH_REFERENCE_VOCAB(AWK),
    CALL_WITHOUT_REFERENCE_VOCAB(ZSH),
    CALL_WITHOUT_REFERENCE_VOCAB(TCL),
    CALL_WITH_REFERENCE_VOCAB(ADA),
    CALL_WITHOUT_REFERENCE_VOCAB(AGDA),
    CALL_WITHOUT_REFERENCE_VOCAB(RACKET),
    CALL_WITH_REFERENCE_VOCAB(ODIN),
    CALL_WITH_REFERENCE_VOCAB(RESCRIPT),
    CALL_WITHOUT_REFERENCE_VOCAB(PURESCRIPT),
    CALL_WITHOUT_REFERENCE_VOCAB(NICKEL),
    CALL_WITH_REFERENCE_VOCAB(CRYSTAL),
    CALL_WITH_REFERENCE_VOCAB(TEAL),
    CALL_WITH_REFERENCE_VOCAB(HARE),
    CALL_WITH_REFERENCE_VOCAB(PONY),
    CALL_WITH_REFERENCE_VOCAB(LUAU),
    NO_CALL(JANET),
    CALL_WITH_REFERENCE_VOCAB(SWAY),
    CALL_WITHOUT_REFERENCE_VOCAB(NASM),
    NO_CALL(ASSEMBLY),
    NO_CALL(ASTRO),
    NO_CALL(BLADE),
    CALL_WITH_REFERENCE_VOCAB(JUST),
    CALL_WITH_REFERENCE_VOCAB(GOTEMPLATE),
    CALL_WITH_REFERENCE_VOCAB(TEMPL),
    NO_CALL(LIQUID),
    NO_CALL(JINJA2),
    CALL_WITH_REFERENCE_VOCAB(PRISMA),
    NO_CALL(HYPRLANG),
    NO_CALL(DOTENV),
    NO_CALL(DIFF),
    CALL_WITH_REFERENCE_VOCAB(WGSL),
    NO_CALL(KDL),
    NO_CALL(JSON5),
    CALL_WITH_REFERENCE_VOCAB(JSONNET),
    NO_CALL(RON),
    NO_CALL(THRIFT),
    NO_CALL(CAPNP),
    NO_CALL(PROPERTIES),
    NO_CALL(SSHCONFIG),
    NO_CALL(BIBTEX),
    CALL_WITH_REFERENCE_VOCAB(STARLARK),
    CALL_WITH_REFERENCE_VOCAB(BICEP),
    NO_CALL(CSV),
    NO_CALL(REQUIREMENTS),
    CALL_WITH_REFERENCE_VOCAB(HLSL),
    CALL_WITH_REFERENCE_VOCAB(VHDL),
    CALL_WITH_REFERENCE_VOCAB(SYSTEMVERILOG),
    CALL_WITH_REFERENCE_VOCAB(DEVICETREE),
    CALL_WITHOUT_REFERENCE_VOCAB(LINKERSCRIPT),
    CALL_WITH_REFERENCE_VOCAB(GN),
    NO_CALL(KCONFIG),
    CALL_WITH_REFERENCE_VOCAB(BITBAKE),
    NO_CALL(SMALI),
    NO_CALL(TABLEGEN),
    CALL_WITH_REFERENCE_VOCAB(ISPC),
    CALL_WITH_REFERENCE_VOCAB(CAIRO),
    CALL_WITH_REFERENCE_VOCAB(MOVE),
    CALL_WITH_REFERENCE_VOCAB(SQUIRREL),
    CALL_WITH_REFERENCE_VOCAB(FUNC),
    NO_CALL(REGEX),
    NO_CALL(JSDOC),
    NO_CALL(RST),
    NO_CALL(BEANCOUNT),
    NO_CALL(MERMAID),
    CALL_WITHOUT_REFERENCE_VOCAB(PUPPET),
    NO_CALL(PO),
    NO_CALL(GITATTRIBUTES),
    NO_CALL(GITIGNORE),
    CALL_WITH_REFERENCE_VOCAB(SLANG),
    CALL_WITHOUT_REFERENCE_VOCAB(LLVM_IR),
    NO_CALL(SMITHY),
    NO_CALL(WIT),
    CALL_WITH_REFERENCE_VOCAB(TLAPLUS),
    NO_CALL(PKL),
    NO_CALL(GOMOD),
    CALL_WITH_REFERENCE_VOCAB(APEX),
    NO_CALL(SOQL),
    NO_CALL(SOSL),
    NO_CALL(KUSTOMIZE),
    NO_CALL(K8S),
    CALL_WITH_REFERENCE_VOCAB(PINE),
    CALL_WITH_REFERENCE_VOCAB(QML),
    CALL_WITH_REFERENCE_VOCAB(CFSCRIPT),
    CALL_WITH_REFERENCE_VOCAB(CFML),
    CALL_WITH_REFERENCE_VOCAB(MOJO),
    CALL_WITH_REFERENCE_VOCAB(OBJECTSCRIPT_UDL),
    CALL_WITH_REFERENCE_VOCAB(OBJECTSCRIPT_ROUTINE),
    TRANSFORM_ONLY(OBJECTSCRIPT_EXPORT),
};

#undef CALL_WITH_REFERENCE_VOCAB
#undef CALL_WITHOUT_REFERENCE_VOCAB
#undef NO_CALL
#undef TRANSFORM_ONLY
#undef UNSUPPORTED

_Static_assert(sizeof(LANGUAGE_CAPABILITIES) / sizeof(LANGUAGE_CAPABILITIES[0]) == CBM_LANG_COUNT,
               "language capability ledger must track CBM_LANG_COUNT");

static int grammar_has_symbol(const TSLanguage *language, const char *expected_name) {
    uint32_t symbol_count = ts_language_symbol_count(language);
    for (TSSymbol symbol = 0; symbol < symbol_count; symbol++) {
        const char *actual_name = ts_language_symbol_name(language, symbol);
        if (actual_name && strcmp(actual_name, expected_name) == 0) {
            return 1;
        }
    }
    return 0;
}

/* Every enum value must either be unsupported (NULL) or map to a spec tagged
 * with that exact language. A missing designated initializer must never alias
 * the zero-initialized CBM_LANG_GO slot. */
TEST(repro_language_specs_do_not_alias_other_languages) {
    int failures = 0;
    for (int value = 0; value < CBM_LANG_COUNT; value++) {
        CBMLanguage language = (CBMLanguage)value;
        const CBMLangSpec *spec = cbm_lang_spec(language);
        if (spec && spec->language != language) {
            fprintf(
                stderr,
                "  [language-registry] invariant=spec_language_mismatch requested=%d actual=%d\n",
                value, (int)spec->language);
            failures++;
        }
    }
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_language_capability_ledger_covers_every_enum) {
    int failures = 0;
    int counts[CAP_COUNT] = {0};

    for (int value = 0; value < CBM_LANG_COUNT; value++) {
        const LanguageCapabilityEntry *entry = &LANGUAGE_CAPABILITIES[value];
        const CBMLangSpec *spec = cbm_lang_spec((CBMLanguage)value);
        if (entry->capability <= CAP_UNSET || entry->capability >= CAP_COUNT || !entry->name ||
            !entry->reason) {
            fprintf(stderr, "  [language-registry] invariant=capability_unset requested=%d\n",
                    value);
            failures++;
            continue;
        }
        counts[entry->capability]++;

        if (entry->capability == CAP_UNSUPPORTED || entry->capability == CAP_TRANSFORM_ONLY) {
            if (spec) {
                fprintf(stderr,
                        "  [language-registry] lang=%s "
                        "invariant=non_grammar_capability_spec_nonnull capability=%d actual=%d\n",
                        entry->name, (int)entry->capability, (int)spec->language);
                failures++;
            }
            continue;
        }

        if (!spec || spec->language != (CBMLanguage)value) {
            fprintf(stderr, "  [language-registry] lang=%s invariant=registered_spec_missing\n",
                    entry->name);
            failures++;
            continue;
        }

        int has_call_metadata = spec->call_node_types && spec->call_node_types[0];
        int expects_call_metadata = entry->capability == CAP_CALL_WITH_REFERENCE_VOCAB ||
                                    entry->capability == CAP_CALL_WITHOUT_REFERENCE_VOCAB;
        if (has_call_metadata != expects_call_metadata) {
            fprintf(stderr,
                    "  [language-registry] lang=%s invariant=call_capability_mismatch expected=%d "
                    "actual=%d\n",
                    entry->name, expects_call_metadata, has_call_metadata);
            failures++;
        }
    }

    if (counts[CAP_CALL_WITH_REFERENCE_VOCAB] != 86 ||
        counts[CAP_CALL_WITHOUT_REFERENCE_VOCAB] != 25 || counts[CAP_NO_CALL] != 50 ||
        counts[CAP_TRANSFORM_ONLY] != 1 || counts[CAP_UNSUPPORTED] != 1) {
        fprintf(stderr,
                "  [language-registry] invariant=capability_partition call_ref_vocab=%d ref_gap=%d "
                "no_call=%d transform_only=%d unsupported=%d\n",
                counts[CAP_CALL_WITH_REFERENCE_VOCAB], counts[CAP_CALL_WITHOUT_REFERENCE_VOCAB],
                counts[CAP_NO_CALL], counts[CAP_TRANSFORM_ONLY], counts[CAP_UNSUPPORTED]);
        failures++;
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_argument_matrices_equal_call_capability_ledger) {
    enum {
        EXPECTED_MATRIX_A_ROWS = 67,
        EXPECTED_MATRIX_B_ROWS = 46,
        EXPECTED_CALL_CAPABLE_LANGUAGES = 111,
        EXPECTED_NON_CALL_LANGUAGES = 52,
        EXPECTED_NON_CALL_DOMAIN_CONTROLS = 2,
    };
    CBMLanguage matrix_a_ids[CBM_LANG_COUNT];
    CBMLanguage matrix_b_ids[CBM_LANG_COUNT];
    unsigned int matrix_a_counts[CBM_LANG_COUNT] = {0};
    unsigned int matrix_b_counts[CBM_LANG_COUNT] = {0};
    size_t matrix_a_rows = repro_call_argument_matrix_a_copy_language_ids(
        matrix_a_ids, sizeof(matrix_a_ids) / sizeof(matrix_a_ids[0]));
    size_t matrix_b_rows = repro_call_argument_matrix_b_copy_language_ids(
        matrix_b_ids, sizeof(matrix_b_ids) / sizeof(matrix_b_ids[0]));
    int failures = 0;

    if (matrix_a_rows != EXPECTED_MATRIX_A_ROWS) {
        fprintf(stderr,
                "  [language-registry] matrix=A invariant=row_count expected=%d actual=%zu\n",
                EXPECTED_MATRIX_A_ROWS, matrix_a_rows);
        failures++;
    }
    if (matrix_b_rows != EXPECTED_MATRIX_B_ROWS) {
        fprintf(stderr,
                "  [language-registry] matrix=B invariant=row_count expected=%d actual=%zu\n",
                EXPECTED_MATRIX_B_ROWS, matrix_b_rows);
        failures++;
    }

    size_t matrix_a_copied =
        matrix_a_rows < CBM_LANG_COUNT ? matrix_a_rows : (size_t)CBM_LANG_COUNT;
    for (size_t row = 0; row < matrix_a_copied; row++) {
        int language_id = (int)matrix_a_ids[row];
        if (language_id < 0 || language_id >= CBM_LANG_COUNT) {
            fprintf(stderr,
                    "  [language-registry] matrix=A row=%zu invariant=language_id_range id=%d\n",
                    row, language_id);
            failures++;
            continue;
        }
        matrix_a_counts[language_id]++;
    }

    size_t matrix_b_copied =
        matrix_b_rows < CBM_LANG_COUNT ? matrix_b_rows : (size_t)CBM_LANG_COUNT;
    for (size_t row = 0; row < matrix_b_copied; row++) {
        int language_id = (int)matrix_b_ids[row];
        if (language_id < 0 || language_id >= CBM_LANG_COUNT) {
            fprintf(stderr,
                    "  [language-registry] matrix=B row=%zu invariant=language_id_range id=%d\n",
                    row, language_id);
            failures++;
            continue;
        }
        matrix_b_counts[language_id]++;
    }

    int call_capable_languages = 0;
    int covered_call_capable_languages = 0;
    int explicitly_non_call_languages = 0;
    int covered_non_call_domain_controls = 0;
    for (int language_id = 0; language_id < CBM_LANG_COUNT; language_id++) {
        const LanguageCapabilityEntry *entry = &LANGUAGE_CAPABILITIES[language_id];
        const char *language_name = entry->name ? entry->name : "<unclassified>";
        unsigned int matrix_a_count = matrix_a_counts[language_id];
        unsigned int matrix_b_count = matrix_b_counts[language_id];
        unsigned int total_count = matrix_a_count + matrix_b_count;
        int call_capable = entry->capability == CAP_CALL_WITH_REFERENCE_VOCAB ||
                           entry->capability == CAP_CALL_WITHOUT_REFERENCE_VOCAB;

        if (matrix_a_count > 1) {
            fprintf(stderr,
                    "  [language-registry] matrix=A lang=%s id=%d invariant=duplicate_language "
                    "actual=%u\n",
                    language_name, language_id, matrix_a_count);
            failures++;
        }
        if (matrix_b_count > 1) {
            fprintf(stderr,
                    "  [language-registry] matrix=B lang=%s id=%d invariant=duplicate_language "
                    "actual=%u\n",
                    language_name, language_id, matrix_b_count);
            failures++;
        }
        if (matrix_a_count && matrix_b_count) {
            fprintf(stderr,
                    "  [language-registry] lang=%s id=%d invariant=matrix_overlap matrix_a=%u "
                    "matrix_b=%u\n",
                    language_name, language_id, matrix_a_count, matrix_b_count);
            failures++;
        }

        if (call_capable) {
            call_capable_languages++;
            if (total_count == 0) {
                fprintf(stderr,
                        "  [language-registry] lang=%s id=%d "
                        "invariant=call_capability_missing_from_matrices\n",
                        language_name, language_id);
                failures++;
            } else {
                covered_call_capable_languages++;
                if (total_count != 1) {
                    fprintf(stderr,
                            "  [language-registry] lang=%s id=%d "
                            "invariant=call_capability_matrix_count expected=1 actual=%u\n",
                            language_name, language_id, total_count);
                    failures++;
                }
            }
            continue;
        }

        if (entry->capability != CAP_NO_CALL && entry->capability != CAP_TRANSFORM_ONLY &&
            entry->capability != CAP_UNSUPPORTED) {
            fprintf(stderr,
                    "  [language-registry] lang=%s id=%d "
                    "invariant=remaining_capability_explicit "
                    "expected=NO_CALL_or_TRANSFORM_ONLY_or_UNSUPPORTED "
                    "actual=%d\n",
                    language_name, language_id, (int)entry->capability);
            failures++;
        } else {
            explicitly_non_call_languages++;
        }
        int expected_domain_control =
            language_id == CBM_LANG_DIFF || language_id == CBM_LANG_BIBTEX;
        if (expected_domain_control && total_count == 1) {
            covered_non_call_domain_controls++;
        } else if (total_count != 0 || expected_domain_control) {
            fprintf(stderr,
                    "  [language-registry] lang=%s id=%d "
                    "invariant=non_call_domain_control_coverage expected=%d "
                    "matrix_a=%u matrix_b=%u\n",
                    language_name, language_id, expected_domain_control, matrix_a_count,
                    matrix_b_count);
            failures++;
        }
    }

    if (call_capable_languages != EXPECTED_CALL_CAPABLE_LANGUAGES ||
        covered_call_capable_languages != EXPECTED_CALL_CAPABLE_LANGUAGES ||
        explicitly_non_call_languages != EXPECTED_NON_CALL_LANGUAGES ||
        covered_non_call_domain_controls != EXPECTED_NON_CALL_DOMAIN_CONTROLS) {
        fprintf(stderr,
                "  [language-registry] invariant=matrix_ledger_partition "
                "call_capable_expected=%d call_capable_actual=%d covered_actual=%d "
                "non_call_expected=%d non_call_actual=%d "
                "non_call_controls_expected=%d non_call_controls_actual=%d\n",
                EXPECTED_CALL_CAPABLE_LANGUAGES, call_capable_languages,
                covered_call_capable_languages, EXPECTED_NON_CALL_LANGUAGES,
                explicitly_non_call_languages, EXPECTED_NON_CALL_DOMAIN_CONTROLS,
                covered_non_call_domain_controls);
        failures++;
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

/* A call-node registration that does not exist in the compiled grammar can
 * never match an AST node. Check the compiled language instead of trusting
 * hand-maintained spec strings or the graph index. */
TEST(repro_primary_call_metadata_names_exist_in_grammars) {
    int failures = 0;

    for (int value = 0; value < CBM_LANG_COUNT; value++) {
        const LanguageCapabilityEntry *entry = &LANGUAGE_CAPABILITIES[value];
        const CBMLangSpec *spec = cbm_lang_spec((CBMLanguage)value);
        const TSLanguage *language = cbm_ts_language((CBMLanguage)value);
        if (!spec || !spec->call_node_types || !spec->call_node_types[0]) {
            continue;
        }
        if (!language) {
            fprintf(stderr,
                    "  [language-registry] lang=%s invariant=call_metadata_grammar_missing\n",
                    entry->name);
            failures++;
            continue;
        }

        for (int i = 0; spec->call_node_types[i]; i++) {
            const char *call_node_type = spec->call_node_types[i];
            if (!grammar_has_symbol(language, call_node_type)) {
                fprintf(stderr,
                        "  [language-registry] lang=%s "
                        "invariant=call_node_type_exists node=%s\n",
                        entry->name, call_node_type);
                failures++;
            }
        }
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

/* LinkerScript's compiled grammar root is `linkerscript`, while the language
 * spec currently registers `source_file`. Path-derived module QNs hide the
 * mismatch, so exercise the AST-to-metadata contract directly. */
TEST(repro_linkerscript_root_is_registered_as_module) {
    static const char source[] = "SECTIONS { .text : { *(.text) } }\n";
    const CBMLangSpec *spec = cbm_lang_spec(CBM_LANG_LINKERSCRIPT);
    const TSLanguage *language = cbm_ts_language(CBM_LANG_LINKERSCRIPT);
    TSParser *parser = ts_parser_new();
    int failures = 0;

    if (!spec || !language || !parser || !ts_parser_set_language(parser, language)) {
        fprintf(stderr, "  [language-registry] lang=LINKERSCRIPT invariant=parser_setup\n");
        failures++;
    } else {
        TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
        if (!tree) {
            fprintf(stderr,
                    "  [language-registry] lang=LINKERSCRIPT invariant=parse_tree_nonnull\n");
            failures++;
        } else {
            TSNode root = ts_tree_root_node(tree);
            const char *root_kind = ts_node_type(root);
            int registered = 0;
            if (spec->module_node_types) {
                for (int i = 0; spec->module_node_types[i]; i++) {
                    if (strcmp(spec->module_node_types[i], root_kind) == 0) {
                        registered = 1;
                        break;
                    }
                }
            }
            if (ts_node_has_error(root)) {
                fprintf(stderr,
                        "  [language-registry] lang=LINKERSCRIPT invariant=valid_fixture\n");
                failures++;
            }
            if (!registered) {
                fprintf(stderr,
                        "  [language-registry] lang=LINKERSCRIPT "
                        "invariant=root_registered_as_module root=%s\n",
                        root_kind);
                failures++;
            }
            ts_tree_delete(tree);
        }
    }
    if (parser) {
        ts_parser_delete(parser);
    }
    ASSERT_EQ(failures, 0);
    PASS();
}

SUITE(repro_language_registry) {
    RUN_TEST(repro_language_specs_do_not_alias_other_languages);
    RUN_TEST(repro_language_capability_ledger_covers_every_enum);
    RUN_TEST(repro_call_argument_matrices_equal_call_capability_ledger);
    RUN_TEST(repro_primary_call_metadata_names_exist_in_grammars);
    RUN_TEST(repro_linkerscript_root_is_registered_as_module);
}
