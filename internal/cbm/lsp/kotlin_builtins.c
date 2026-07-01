/*
 * Minimal Kotlin builtin graph targets. kotlin_lsp.c emits lsp_kt_any edges to
 * these QNs; injecting definitions here lets the pipeline mint real target nodes.
 */

typedef struct {
    const char *qn;
    const char *name;
    const char *label;
} KtBuiltinNode;

enum {
    KT_BUILTIN_SYNTHETIC_LINE = 1,
};

static const KtBuiltinNode kKtBuiltinNodes[] = {
    {"kotlin.Any", "Any", "Class"},
    {"kotlin.Any.toString", "toString", "Method"},
    {"kotlin.Any.equals", "equals", "Method"},
    {"kotlin.Any.hashCode", "hashCode", "Method"},
};

static void kt_builtins_inject_defs(CBMFileResult *result, CBMArena *arena) {
    if (!result || !arena) {
        return;
    }

    const size_t node_count = sizeof(kKtBuiltinNodes) / sizeof(kKtBuiltinNodes[0]);
    for (size_t i = 0; i < node_count; i++) {
        const KtBuiltinNode *b = &kKtBuiltinNodes[i];
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = b->name;
        def.qualified_name = b->qn;
        def.label = b->label;
        def.file_path = "<kotlin-builtins>";
        def.start_line = KT_BUILTIN_SYNTHETIC_LINE;
        def.end_line = KT_BUILTIN_SYNTHETIC_LINE;
        cbm_defs_push(&result->defs, arena, def);
    }
}
