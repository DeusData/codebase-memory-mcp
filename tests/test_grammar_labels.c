/*
 * test_grammar_labels.c — Per-grammar node-type LABEL golden snapshot.
 *
 * For every supported grammar, snapshot the deterministic node-type-label
 * histogram that cbm_extract_file() produces for the shared fixture (e.g.
 * "Class:1,Function:2,Module:1"). Asserting the histogram stays fixed catches
 * ANY future change to how a grammar's constructs are labeled — e.g. a class
 * silently downgraded to a Function, a lost Method, or a new spurious node.
 *
 * Node counts are deterministic (unlike edge counts), so an exact histogram
 * golden is stable. Labels come from CBMDefinition.label assigned during
 * extraction (the same labels that become graph-node labels downstream).
 *
 * Capture workflow: a grammar with no golden row prints a `[LABEL-CAPTURE]`
 * line and fails; copy those lines into LABEL_GOLDENS, then the test asserts.
 */
#include "test_framework.h"
#include "cbm.h"
#include "grammar_cases.h"

#include <stdlib.h>
#include <string.h>

/* Build a sorted "Label:count,Label:count" histogram of a result's def labels
 * (includes the always-emitted Module node). Deterministic + stable. */
static void label_histogram(CBMFileResult *r, char *out, size_t out_sz) {
    /* Collect distinct labels + counts (small N — linear scan is fine). */
    enum { MAXL = 32 };
    const char *labels[MAXL];
    int counts[MAXL];
    int nl = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const char *l = r->defs.items[i].label ? r->defs.items[i].label : "(null)";
        int j = 0;
        for (; j < nl; j++) {
            if (strcmp(labels[j], l) == 0) {
                counts[j]++;
                break;
            }
        }
        if (j == nl && nl < MAXL) {
            labels[nl] = l;
            counts[nl] = 1;
            nl++;
        }
    }
    /* Insertion sort labels alphabetically for a canonical string. */
    for (int i = 1; i < nl; i++) {
        const char *lk = labels[i];
        int ck = counts[i];
        int j = i - 1;
        while (j >= 0 && strcmp(labels[j], lk) > 0) {
            labels[j + 1] = labels[j];
            counts[j + 1] = counts[j];
            j--;
        }
        labels[j + 1] = lk;
        counts[j + 1] = ck;
    }
    out[0] = '\0';
    size_t used = 0;
    for (int i = 0; i < nl; i++) {
        int w = snprintf(out + used, out_sz - used, "%s%s:%d", i ? "," : "", labels[i], counts[i]);
        if (w < 0 || (size_t)w >= out_sz - used) {
            break;
        }
        used += (size_t)w;
    }
}

typedef struct {
    const char *name;
    const char *hist;
} LabelGolden;

/* Golden node-type-label histograms — captured from the current (correct)
 * extractor output. A mismatch = a labeling regression for that grammar.
 * (Populated from [LABEL-CAPTURE] output; see header.) */
static const LabelGolden LABEL_GOLDENS[] = {
    {"go", "Function:2,Module:1"},
    {"c", "Function:2,Module:1"},
    {"cpp", "Class:1,Function:1,Module:1"},
    {"cuda", "Function:2,Module:1"},
    {"python", "Class:1,Function:1,Module:1"},
    {"javascript", "Class:1,Function:1,Module:1"},
    {"typescript", "Class:1,Function:1,Module:1"},
    {"tsx", "Function:1,Module:1"},
    {"java", "Class:1,Method:1,Module:1"},
    {"kotlin", "Class:1,Function:1,Module:1"},
    {"rust", "Class:1,Function:1,Module:1"},
    {"ruby", "Class:1,Function:1,Module:1"},
    {"php", "Class:1,Function:1,Module:1"},
    {"c_sharp", "Class:1,Method:1,Module:1"},
    {"bash", "Function:2,Module:1"},
    {"zsh", "Function:2,Module:1"},
    {"lua", "Function:2,Module:1"},
    {"luau", "Function:2,Module:1"},
    {"perl", "Function:2,Module:1"},
    {"dart", "Class:1,Function:1,Module:1"},
    {"swift", "Class:1,Function:1,Module:1"},
    {"scala", "Class:1,Function:1,Method:1,Module:1"},
    {"gdscript", "Function:1,Module:1"},
    {"groovy", "Class:1,Method:1,Module:1"},
    {"zig", "Function:2,Module:1"},
    {"solidity", "Class:1,Function:1,Method:1,Module:1"},
    {"tcl", "Function:2,Module:1"},
    {"powershell", "Function:2,Module:1"},
    {"r", "Function:2,Module:1"},
    {"julia", "Class:1,Function:1,Module:1"},
    {"matlab", "Function:2,Module:1"},
    {"ada", "Function:1,Module:1"},
    {"agda", "Function:1,Module:1"},
    {"apex", "Class:1,Method:1,Module:1"},
    {"awk", "Function:2,Module:1"},
    {"cairo", "Function:2,Module:1"},
    {"clojure", "Function:2,Module:1"},
    {"commonlisp", "Function:2,Module:1"},
    {"emacslisp", "Function:2,Module:1"},
    {"crystal", "Class:1,Function:1,Module:1"},
    {"d", "Function:2,Module:1"},
    {"elixir", "Class:1,Function:1,Module:1"},
    {"erlang", "Function:2,Module:1"},
    {"fennel", "Function:2,Module:1"},
    {"fish", "Function:1,Module:1"},
    {"fortran", "Function:1,Module:1"},
    {"fsharp", "Function:2,Module:1"},
    {"gleam", "Function:2,Module:1"},
    {"glsl", "Function:2,Module:1"},
    {"hare", "Function:2,Module:1"},
    {"haskell", "Function:1,Module:1"},
    {"hlsl", "Function:2,Module:1"},
    {"ispc", "Function:2,Module:1"},
    {"objc", "Class:1,Method:1,Module:1"},
    {"ocaml", "Function:2,Module:1"},
    {"odin", "Function:2,Module:1"},
    {"pascal", "Function:1,Module:1"},
    {"pony", "Class:1,Function:1,Module:1"},
    {"purescript", "Function:1,Module:1"},
    {"racket", "Function:2,Module:1"},
    {"rescript", "Function:2,Module:1"},
    {"scheme", "Function:2,Module:1"},
    {"slang", "Function:2,Module:1"},
    {"squirrel", "Function:2,Module:1"},
    {"starlark", "Function:2,Module:1"},
    {"sway", "Function:2,Module:1"},
    {"teal", "Function:2,Module:1"},
    {"vimscript", "Function:1,Module:1"},
    {"elm", "Class:1,Function:1,Module:1"},
    {"func", "Function:1,Module:1"},
    {"lean", "Function:2,Module:1"},
    {"move", "Function:1,Module:1"},
    {"smali", "Class:1,Function:1,Module:1"},
    {"systemverilog", "Class:1,Function:1,Module:1"},
    {"verilog", "Class:1,Module:1"},
    {"vhdl", "Class:1,Module:1"},
    {"wgsl", "Function:2,Module:1"},
    {"tlaplus", "Function:1,Module:1"},
    {"llvm", "Function:1,Module:1"},
    {"tablegen", "Function:1,Module:1"},
    {"puppet", "Class:1,Module:1"},
    {"assembly", "Function:1,Module:1"},
    {"nasm", "Function:1,Module:1"},
    {"cfml", "Function:1,Module:1"},
    {"cfscript", "Function:1,Module:1"},
    {"cobol", "Function:1,Module:1"},
    {"janet", "Function:2,Module:1"},
    {"magma", "Function:1,Module:1"},
    {"qml", "Function:1,Module:1"},
    {"wolfram", "Function:1,Module:1"},
    {"pine", "Function:1,Module:1"},
    {"form", "Module:1,Variable:1"},
    {"protobuf", "Class:1,Module:1"},
    {"soql", "Module:1"},
    {"sosl", "Module:1"},
    {"dotenv", "Module:1"},
    {"json", "Module:1,Variable:2"},
    {"json5", "Module:1"},
    {"jsonnet", "Module:1"},
    {"jsdoc", "Module:1"},
    {"yaml", "Module:1,Variable:2"},
    {"k8s", "Module:1"},
    {"kustomize", "Module:1"},
    {"toml", "Class:1,Module:1,Variable:1"},
    {"ini", "Class:1,Module:1,Variable:1"},
    {"csv", "Module:1"},
    {"sql", "Module:1,Variable:1"},
    {"xml", "Class:2,Module:1"},
    {"html", "Module:1"},
    {"css", "Module:1"},
    {"scss", "Module:1,Variable:1"},
    {"markdown", "Module:1,Section:1"},
    {"rst", "Module:1"},
    {"dockerfile", "Module:1"},
    {"makefile", "Function:1,Module:1"},
    {"cmake", "Function:1,Module:1"},
    {"meson", "Module:1"},
    {"gn", "Module:1"},
    {"just", "Function:1,Module:1"},
    {"hcl", "Class:1,Module:1"},
    {"nix", "Module:1"},
    {"gomod", "Module:1"},
    {"gotemplate", "Module:1"},
    {"graphql", "Class:1,Module:1"},
    {"prisma", "Class:1,Module:1"},
    {"thrift", "Function:1,Module:1"},
    {"capnp", "Class:1,Module:1"},
    {"smithy", "Class:1,Module:1"},
    {"wit", "Class:2,Function:1,Module:1"},
    {"kdl", "Module:1"},
    {"ron", "Module:1"},
    {"nickel", "Module:1"},
    {"pkl", "Module:1"},
    {"bicep", "Module:1"},
    {"bitbake", "Module:1"},
    {"beancount", "Module:1"},
    {"bibtex", "Module:1"},
    {"po", "Module:1"},
    {"diff", "Module:1"},
    {"regex", "Module:1"},
    {"requirements", "Module:1"},
    {"properties", "Module:1"},
    {"gitignore", "Module:1"},
    {"gitattributes", "Module:1"},
    {"sshconfig", "Module:1"},
    {"hyprlang", "Module:1"},
    {"kconfig", "Class:1,Module:1"},
    {"linkerscript", "Module:1"},
    {"devicetree", "Module:1"},
    {"jinja2", "Module:1"},
    {"liquid", "Module:1"},
    {"blade", "Module:1"},
    {"vue", "Module:1"},
    {"svelte", "Module:1"},
    {"astro", "Module:1"},
    {"templ", "Class:1,Module:1"},
    {"typst", "Module:1"},
    {"mermaid", "Module:1"},
    {NULL, NULL},
};

static const char *golden_for(const char *name) {
    for (int i = 0; LABEL_GOLDENS[i].name; i++) {
        if (strcmp(LABEL_GOLDENS[i].name, name) == 0) {
            return LABEL_GOLDENS[i].hist;
        }
    }
    return NULL;
}

TEST(grammar_label_goldens) {
    int n = (int)CBM_GRAMMAR_CASES_COUNT;
    int failures = 0;
    int missing = 0;
    for (int i = 0; i < n; i++) {
        const GrammarCase *c = &CBM_GRAMMAR_CASES[i];
        CBMFileResult *r =
            cbm_extract_file(c->src, (int)strlen(c->src), c->lang, "lbl", c->path, 0, NULL, NULL);
        if (!r) {
            fprintf(stderr, "  [LABEL] %-14s extract returned NULL\n", c->name);
            failures++;
            continue;
        }
        char hist[256];
        label_histogram(r, hist, sizeof(hist));
        cbm_free_result(r);

        const char *golden = golden_for(c->name);
        if (!golden) {
            fprintf(stderr, "  [LABEL-CAPTURE] {\"%s\", \"%s\"},\n", c->name, hist);
            missing++;
        } else if (strcmp(golden, hist) != 0) {
            fprintf(stderr, "  [LABEL] %-14s MISMATCH golden=[%s] actual=[%s]\n", c->name, golden,
                    hist);
            failures++;
        }
    }
    if (missing > 0) {
        fprintf(stderr,
                "  [LABEL] %d grammar(s) missing a golden — copy the [LABEL-CAPTURE] lines "
                "into LABEL_GOLDENS\n",
                missing);
    }
    ASSERT_EQ(failures, 0);
    ASSERT_EQ(missing, 0);
    PASS();
}

/* Count def nodes that are NOT the structural file/module wrappers. */
static int non_module_defs(CBMFileResult *r) {
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const char *l = r->defs.items[i].label;
        if (l && strcmp(l, "Module") != 0 && strcmp(l, "File") != 0 && strcmp(l, "Folder") != 0 &&
            strcmp(l, "Package") != 0 && strcmp(l, "Directory") != 0) {
            n++;
        }
    }
    return n;
}

/* Code / IDL grammars whose fixture contains a real definition construct
 * (function / type / message / model / module / label) that MUST extract to at
 * least one non-Module node. These currently extract only a Module node — an
 * under-extraction gap reproduced here as a hard failure (fix phase). Data /
 * config / markup grammars (json/yaml/css/…) are intentionally excluded — a
 * lone Module node is correct for them. */
static const char *MUST_EXTRACT_DEFS[] = {
    "agda",   "pony",          "move",     "cobol",    "janet",    "pine",  "smali",  "verilog",
    "vhdl",   "systemverilog", "protobuf", "graphql",  "thrift",   "capnp", "smithy", "wit",
    "prisma", "cmake",         "puppet",   "tablegen", "assembly", "nasm",  NULL};

TEST(grammar_code_extracts_defs) {
    int failures = 0;
    for (int k = 0; MUST_EXTRACT_DEFS[k]; k++) {
        const char *name = MUST_EXTRACT_DEFS[k];
        const GrammarCase *c = NULL;
        for (int i = 0; i < (int)CBM_GRAMMAR_CASES_COUNT; i++) {
            if (strcmp(CBM_GRAMMAR_CASES[i].name, name) == 0) {
                c = &CBM_GRAMMAR_CASES[i];
                break;
            }
        }
        if (!c) {
            fprintf(stderr, "  [CODE-DEFS] FAIL %-14s — no fixture in CBM_GRAMMAR_CASES\n", name);
            failures++;
            continue;
        }
        CBMFileResult *r =
            cbm_extract_file(c->src, (int)strlen(c->src), c->lang, "cd", c->path, 0, NULL, NULL);
        if (!r) {
            fprintf(stderr, "  [CODE-DEFS] FAIL %-14s extract returned NULL\n", name);
            failures++;
            continue;
        }
        int nd = non_module_defs(r);
        cbm_free_result(r);
        if (nd < 1) {
            fprintf(stderr,
                    "  [CODE-DEFS] FAIL %-14s extracts 0 non-Module defs (under-extraction "
                    "gap — only a Module node)\n",
                    name);
            failures++;
        }
    }
    fprintf(stderr, "  [CODE-DEFS] %d code/IDL grammars: %d under-extraction FAILURES\n",
            (int)(sizeof(MUST_EXTRACT_DEFS) / sizeof(MUST_EXTRACT_DEFS[0])) - 1, failures);
    ASSERT_EQ(failures, 0);
    PASS();
}

void suite_grammar_labels(void) {
    RUN_TEST(grammar_label_goldens);
    RUN_TEST(grammar_code_extracts_defs);
}
