/*
 * test_diagrams.c -- diagrams as nodes (internal/cbm/diagram.c): Diagram and
 * DiagramElement definitions, DIAGRAM_ARROW edges (src/pipeline/pass_diagrams.c).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

/* ── extraction ──────────────────────────────────────────────────── */

static const char *const DG_DOC = "# Doc\n"                          /* 1 */
                                  "\n"                               /* 2 */
                                  "Intro.\n"                         /* 3 */
                                  "\n"                               /* 4 */
                                  "## Arch\n"                        /* 5 */
                                  "\n"                               /* 6 */
                                  "```mermaid\n"                     /* 7 */
                                  "---\n"                            /* 8 */
                                  "title: System overview\n"         /* 9 */
                                  "---\n"                            /* 10 */
                                  "flowchart LR\n"                   /* 11 */
                                  "  A[Web app] -->|calls| B(API)\n" /* 12 */
                                  "  B --> C[(DB)] & D{Cache}\n"     /* 13 */
                                  "  B -. retries .-> E\n"           /* 14 */
                                  "  subgraph S [Backend]\n"         /* 15 */
                                  "  end\n"                          /* 16 */
                                  "```\n"                            /* 17 */
                                  "\n"                               /* 18 */
                                  "```mermaid\n"                     /* 19 */
                                  "sequenceDiagram\n"                /* 20 */
                                  "  participant U as User\n"        /* 21 */
                                  "  U->>API: login\n"               /* 22 */
                                  "  API-->>U: token\n"              /* 23 */
                                  "```\n"                            /* 24 */
                                  "\n"                               /* 25 */
                                  "## Model\n"                       /* 26 */
                                  "\n"                               /* 27 */
                                  "```mermaid\n"                     /* 28 */
                                  "classDiagram\n"                   /* 29 */
                                  "  Animal <|-- Dog : is a\n"       /* 30 */
                                  "  class Shop[\"Pet shop\"]\n"     /* 31 */
                                  "  Shop *-- Dog\n"                 /* 32 */
                                  "```\n";                           /* 33 */

static const CBMDefinition *dg_def(const CBMFileResult *r, const char *qn_suffix) {
    size_t sl = strlen(qn_suffix);
    for (int i = 0; i < r->defs.count; i++) {
        const char *qn = r->defs.items[i].qualified_name;
        size_t ql = qn ? strlen(qn) : 0;
        if (ql >= sl && strcmp(qn + ql - sl, qn_suffix) == 0) {
            return &r->defs.items[i];
        }
    }
    printf("  no definition *%s\n", qn_suffix);
    return NULL;
}

static const char *dg_prop(const CBMDefinition *d, const char *key) {
    for (const char **kv = d->extra_props; kv && kv[0]; kv += 2) {
        if (strcmp(kv[0], key) == 0) {
            return kv[1];
        }
    }
    return "";
}

static int dg_arrows(const CBMFileResult *r, const char *from_suffix, const char *to_suffix,
                     const char *style, const char *label) {
    int n = 0;
    size_t fl = strlen(from_suffix);
    size_t tl = strlen(to_suffix);
    for (int i = 0; i < r->diagram_arrows.count; i++) {
        const CBMDiagramArrow *a = &r->diagram_arrows.items[i];
        size_t al = strlen(a->from_qn);
        size_t bl = strlen(a->to_qn);
        if (al >= fl && strcmp(a->from_qn + al - fl, from_suffix) == 0 && bl >= tl &&
            strcmp(a->to_qn + bl - tl, to_suffix) == 0 && strcmp(a->style, style) == 0 &&
            strcmp(a->label ? a->label : "", label) == 0) {
            n++;
        }
    }
    if (n != 1) {
        printf("  arrow %s -> %s [%s \"%s\"]: %d found\n", from_suffix, to_suffix, style, label, n);
    }
    return n;
}

TEST(diagrams_markdown_mermaid) {
    CBMFileResult *r = dm_extract(DG_DOC, CBM_LANG_MARKDOWN, "docs/arch.md");
    ASSERT_NOT_NULL(r);
    /* a flowchart under its section, titled by its front matter */
    const CBMDefinition *d = dg_def(r, ".Arch#diagram-1");
    ASSERT_NOT_NULL(d);
    ASSERT_STR_EQ(d->label, "Diagram");
    ASSERT_STR_EQ(d->name, "System overview");
    ASSERT_STR_EQ(dg_prop(d, "format"), "mermaid");
    ASSERT_STR_EQ(dg_prop(d, "diagram_kind"), "flowchart");
    ASSERT_NOT_NULL(d->parent_class);
    ASSERT(strstr(d->parent_class, ".Arch") != NULL);
    ASSERT(d->docstring && strstr(d->docstring, "Web app") && strstr(d->docstring, "calls"));
    ASSERT_EQ(d->start_line, 7);
    const CBMDefinition *a = dg_def(r, ".Arch#diagram-1.A");
    ASSERT_NOT_NULL(a);
    ASSERT_STR_EQ(a->label, "DiagramElement");
    ASSERT_STR_EQ(a->name, "Web app");
    ASSERT_STR_EQ(a->parent_class, d->qualified_name);
    ASSERT_STR_EQ(dg_prop(a, "element_id"), "A");
    ASSERT_STR_EQ(dg_def(r, ".Arch#diagram-1.C")->name, "DB");
    ASSERT_STR_EQ(dg_def(r, ".Arch#diagram-1.D")->name, "Cache");
    ASSERT_STR_EQ(dg_def(r, ".Arch#diagram-1.E")->name, "E"); /* named only by an arrow */
    ASSERT_STR_EQ(dg_def(r, ".Arch#diagram-1.S")->name, "Backend");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.A", "#diagram-1.B", "arrow", "calls"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.B", "#diagram-1.C", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.B", "#diagram-1.D", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.B", "#diagram-1.E", "dotted", "retries"), 1);
    /* a second diagram of the section: a sequence */
    ASSERT_STR_EQ(dg_prop(dg_def(r, ".Arch#diagram-2"), "diagram_kind"), "sequence");
    ASSERT_STR_EQ(dg_def(r, ".Arch#diagram-2.U")->name, "User");
    ASSERT_EQ(dg_arrows(r, "#diagram-2.U", "#diagram-2.API", "message", "login"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-2.API", "#diagram-2.U", "reply", "token"), 1);
    /* a class diagram under the next section: inheritance points at the parent */
    ASSERT_STR_EQ(dg_def(r, ".Model#diagram-1.Shop")->name, "Pet shop");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.Dog", "#diagram-1.Animal", "inheritance", "is a"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.Shop", "#diagram-1.Dog", "composition", ""), 1);
    cbm_free_result(r);
    PASS();
}

/* A Section's text keeps a diagram fence's labels, not the format's syntax;
 * prose around it stays in order, other fences stay as written. */
TEST(diagrams_section_text_keeps_labels) {
    CBMFileResult *r = dm_extract(DG_DOC, CBM_LANG_MARKDOWN, "docs/arch.md");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *arch = dg_def(r, ".Arch");
    ASSERT_NOT_NULL(arch);
    ASSERT_STR_EQ(arch->label, "Section");
    const char *doc = arch->docstring;
    ASSERT_NOT_NULL(doc);
    const char *kept[] = {"Web app", "API",  "DB",    "Cache", "Backend", "calls",
                          "retries", "User", "login", "token", NULL};
    for (int i = 0; kept[i]; i++) {
        if (!strstr(doc, kept[i])) {
            printf("  label %s missing from: %s\n", kept[i], doc);
        }
        ASSERT(strstr(doc, kept[i]) != NULL);
    }
    const char *dropped[] = {"```",         "mermaid",  "flowchart", "-->", "sequenceDiagram",
                             "participant", "subgraph", NULL};
    for (int i = 0; dropped[i]; i++) {
        if (strstr(doc, dropped[i])) {
            printf("  syntax %s kept in: %s\n", dropped[i], doc);
        }
        ASSERT(strstr(doc, dropped[i]) == NULL);
    }
    ASSERT_STR_EQ(dg_def(r, ".Doc")->docstring, "Intro."); /* no fence: as before */
    cbm_free_result(r);

    r = dm_extract("# Mixed\n"
                   "\n"
                   "Before.\n"
                   "\n"
                   "```mermaid\n"
                   "flowchart LR\n"
                   "  A[Login] --> B[Session]\n"
                   "```\n"
                   "\n"
                   "```python\n"
                   "x = 1\n"
                   "```\n"
                   "\n"
                   "After.\n",
                   CBM_LANG_MARKDOWN, "docs/mixed.md");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, ".Mixed")->docstring,
                  "Before. Login Session ```python x = 1 ``` After.");
    cbm_free_result(r);
    PASS();
}

TEST(diagrams_mermaid_kinds) {
    CBMFileResult *r = dm_extract("erDiagram\n"
                                  "  CUSTOMER ||--o{ ORDER : places\n"
                                  "  ORDER ||..|{ ITEM : contains\n"
                                  "  CUSTOMER {\n"
                                  "    string name\n"
                                  "  }\n",
                                  CBM_LANG_MERMAID, "diagrams/er.mmd");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_prop(dg_def(r, "er#diagram-1"), "diagram_kind"), "er");
    ASSERT_EQ(dg_arrows(r, ".CUSTOMER", ".ORDER", "identifying", "places"), 1);
    ASSERT_EQ(dg_arrows(r, ".ORDER", ".ITEM", "non_identifying", "contains"), 1);
    cbm_free_result(r);

    r = dm_extract("stateDiagram-v2\n"
                   "  [*] --> Idle\n"
                   "  Idle --> Busy : start\n"
                   "  state \"Working hard\" as Busy\n"
                   "  Busy --> [*]\n"
                   "  state Busy {\n"
                   "    [*] --> Step\n"
                   "  }\n",
                   CBM_LANG_MERMAID, "diagrams/state.mmd");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_arrows(r, ".start", ".Idle", "transition", ""), 1);
    ASSERT_EQ(dg_arrows(r, ".Idle", ".Busy", "transition", "start"), 1);
    ASSERT_EQ(dg_arrows(r, ".Busy", ".end", "transition", ""), 1);
    /* a composite state's own start; states named by arrows are states */
    ASSERT_EQ(dg_arrows(r, ".Busy_start", ".Step", "transition", ""), 1);
    ASSERT_STR_EQ(dg_prop(dg_def(r, "state#diagram-1.Idle"), "element_kind"), "state");
    ASSERT_STR_EQ(dg_def(r, "state#diagram-1.Busy")->name, "Working hard");
    cbm_free_result(r);

    r = dm_extract("C4Context\n"
                   "  title Bank\n"
                   "  Person(cust, \"Customer\", \"A customer\")\n"
                   "  System_Ext(mail, \"Mail system\")\n"
                   "  Rel(cust, mail, \"Reads\", \"SMTP\")\n",
                   CBM_LANG_MERMAID, "diagrams/c4.mmd");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_prop(dg_def(r, "c4#diagram-1"), "diagram_kind"), "c4_context");
    ASSERT_STR_EQ(dg_def(r, "c4#diagram-1")->name, "Bank"); /* its title line */
    const CBMDefinition *cust = dg_def(r, "c4#diagram-1.cust");
    ASSERT_NOT_NULL(cust);
    ASSERT_STR_EQ(cust->name, "Customer");
    ASSERT_STR_EQ(dg_prop(cust, "element_kind"), "person");
    ASSERT_STR_EQ(dg_prop(dg_def(r, "c4#diagram-1.mail"), "element_kind"), "system_ext");
    ASSERT_EQ(dg_arrows(r, ".cust", ".mail", "relation", "Reads"), 1);
    cbm_free_result(r);

    r = dm_extract("architecture-beta\n"
                   "  group api(cloud)[API]\n"
                   "  service db(database)[Database] in api\n"
                   "  service server(server)[Server] in api\n"
                   "  db:L -- R:server\n"
                   "  server:T --> B:db\n",
                   CBM_LANG_MERMAID, "diagrams/arch.mmd");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "arch#diagram-1.db")->name, "Database");
    ASSERT_STR_EQ(dg_prop(dg_def(r, "arch#diagram-1.api"), "element_kind"), "group");
    ASSERT_EQ(dg_arrows(r, ".db", ".server", "line", ""), 1);
    ASSERT_EQ(dg_arrows(r, ".server", ".db", "arrow", ""), 1);
    cbm_free_result(r);

    /* a kind without elements is a Diagram all the same */
    r = dm_extract("pie title Pets\n  \"Dogs\" : 386\n", CBM_LANG_MERMAID, "diagrams/pie.mmd");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_prop(dg_def(r, "pie#diagram-1"), "diagram_kind"), "pie");
    ASSERT_STR_EQ(dg_def(r, "pie#diagram-1")->name, "Pets");
    ASSERT_EQ(r->diagram_arrows.count, 0);
    cbm_free_result(r);
    PASS();
}

/* A fenced block of another language is no diagram. */
TEST(diagrams_other_fences) {
    CBMFileResult *r = dm_extract("# T\n\n```js\nA --> B\n```\n", CBM_LANG_MARKDOWN, "a.md");
    ASSERT_NOT_NULL(r);
    for (int i = 0; i < r->defs.count; i++) {
        ASSERT(strcmp(r->defs.items[i].label, "Diagram") != 0);
    }
    ASSERT_EQ(r->diagram_arrows.count, 0);
    cbm_free_result(r);
    PASS();
}

/* ── the other formats ───────────────────────────────────────────── */

static CBMFileResult *dg_file(const char *src, const char *rel) {
    return dm_extract(src, CBM_LANG_DIAGRAM, rel);
}

TEST(diagrams_plantuml) {
    CBMFileResult *r = dg_file("@startuml Components\n"
                               "[Web] --> [API] : REST\n"
                               "component \"Billing\" as B\n"
                               "API ..> B\n"
                               "@enduml\n"
                               "@startuml\n"
                               "class Animal\n"
                               "class Dog {\n"
                               "  +bark()\n"
                               "}\n"
                               "Animal <|-- Dog\n"
                               "Dog \"1\" *-- \"many\" Leg : has\n"
                               "Dog +-- Tail\n"
                               "@enduml\n",
                               "docs/model.puml");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *d = dg_def(r, "model#diagram-1");
    ASSERT_NOT_NULL(d);
    ASSERT_STR_EQ(d->name, "Components");
    ASSERT_STR_EQ(dg_prop(d, "format"), "plantuml");
    ASSERT_STR_EQ(dg_prop(d, "diagram_kind"), "component");
    ASSERT_STR_EQ(dg_def(r, "model#diagram-1.B")->name, "Billing");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.Web", "#diagram-1.API", "arrow", "REST"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.API", "#diagram-1.B", "dependency", ""), 1);
    ASSERT_STR_EQ(dg_prop(dg_def(r, "model#diagram-2"), "diagram_kind"), "class");
    /* classes in packages: a class diagram, whatever line comes first */
    CBMFileResult *pk = dg_file("@startuml\npackage app {\n  class A\n  interface B\n}\n"
                                "A ..|> B\n@enduml\n",
                                "docs/pk.puml");
    ASSERT_STR_EQ(dg_prop(dg_def(pk, "pk#diagram-1"), "diagram_kind"), "class");
    ASSERT_STR_EQ(dg_def(pk, "pk#diagram-1")->name, "pk"); /* untitled: the file */
    cbm_free_result(pk);
    ASSERT_EQ(dg_arrows(r, "#diagram-2.Dog", "#diagram-2.Animal", "inheritance", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-2.Dog", "#diagram-2.Leg", "composition", "has"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-2.Tail", "#diagram-2.Dog", "nesting", ""), 1);
    cbm_free_result(r);

    /* a sequence in a Markdown block */
    r = dm_extract("# Flow\n\n```plantuml\n@startuml\nparticipant Alice\nactor Bob as B\n"
                   "Alice -> B : hello\nB --> Alice : hi\n@enduml\n```\n",
                   CBM_LANG_MARKDOWN, "docs/flow.md");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.Alice", "#diagram-1.B", "message", "hello"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.B", "#diagram-1.Alice", "reply", "hi"), 1);
    ASSERT_STR_EQ(dg_def(r, "Flow#diagram-1.B")->name, "Bob");
    cbm_free_result(r);
    PASS();
}

TEST(diagrams_dot) {
    CBMFileResult *r =
        dg_file("digraph G {\n"
                "  label=\"Pipeline\";\n"
                "  // a comment -> nowhere\n"
                "  a [label=\"Parse\"];\n"
                "  b [label=\"Resolve\"];\n"
                "  a -> b -> c [label=\"next\"];\n"
                "  subgraph cluster_x { d; e; }\n"
                "  c -> {d e};\n"
                "  \"Read every verification output in full before claiming that the step passed, "
                "then record the result in the ledger with its timestamp\" -> b;\n"
                "  y [label=\"two\\nlines\"];\n"
                "}\n",
                "docs/pipe.dot");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *d = dg_def(r, "pipe#diagram-1");
    ASSERT_NOT_NULL(d);
    ASSERT_STR_EQ(d->name, "Pipeline");
    ASSERT_STR_EQ(dg_prop(d, "diagram_kind"), "digraph");
    ASSERT_STR_EQ(dg_def(r, "pipe#diagram-1.a")->name, "Parse");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.a", "#diagram-1.b", "arrow", "next"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.b", "#diagram-1.c", "arrow", "next"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.c", "#diagram-1.d", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.c", "#diagram-1.e", "arrow", ""), 1);
    ASSERT_EQ(r->diagram_arrows.count, 5);
    /* a node named by a sentence too long for an id: kept, under a hashed key */
    int longs = 0;
    for (int i = 0; i < r->diagram_arrows.count; i++) {
        const CBMDiagramArrow *a = &r->diagram_arrows.items[i];
        longs += strstr(a->from_qn, "#diagram-1.Read_every_verification") &&
                 strchr(a->from_qn, '~') && strstr(a->to_qn, "#diagram-1.b");
    }
    ASSERT_EQ(longs, 1);
    ASSERT_STR_EQ(dg_def(r, "pipe#diagram-1.y")->name, "two lines");
    cbm_free_result(r);
    PASS();
}

TEST(diagrams_d2) {
    CBMFileResult *r = dg_file("server: Web Server\n"
                               "db: Database {\n"
                               "  shape: cylinder\n"
                               "}\n"
                               "server -> db: queries\n"
                               "cloud: {\n"
                               "  api -> worker: jobs\n"
                               "}\n",
                               "docs/sys.d2");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "sys#diagram-1.server")->name, "Web Server");
    ASSERT_STR_EQ(dg_def(r, "sys#diagram-1.db")->name, "Database");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.server", "#diagram-1.db", "arrow", "queries"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.cloud_api", "#diagram-1.cloud_worker", "arrow", "jobs"), 1);
    ASSERT_EQ(r->diagram_arrows.count, 2);
    cbm_free_result(r);
    PASS();
}

static const char *const DG_DRAWIO_PAGE =
    "<mxGraphModel><root><mxCell id=\"0\"/><mxCell id=\"1\" parent=\"0\"/>"
    "<mxCell id=\"2\" value=\"Frontend\" style=\"rounded=1;\" vertex=\"1\" parent=\"1\"/>"
    "<mxCell id=\"3\" value=\"&lt;b&gt;Backend&lt;/b&gt;\" style=\"shape=cylinder3;\" vertex=\"1\" "
    "parent=\"1\"/>"
    "<mxCell id=\"4\" value=\"calls\" style=\"endArrow=classic;\" edge=\"1\" parent=\"1\" "
    "source=\"2\" target=\"3\"/>"
    "<mxCell id=\"5\" value=\"\" style=\"\" vertex=\"1\" parent=\"1\"/>"
    "<mxCell id=\"6\" style=\"dashed=1;\" edge=\"1\" parent=\"1\" source=\"2\" target=\"5\"/>"
    "</root></mxGraphModel>";

static char *dg_b64(const unsigned char *in, size_t dn) {
    static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    char *out = malloc(dn * 4 / 3 + 8);
    size_t o = 0;
    for (size_t i = 0; i < dn; i += 3) {
        uint32_t v = (uint32_t)in[i] << 16;
        v |= i + 1 < dn ? (uint32_t)in[i + 1] << 8 : 0;
        v |= i + 2 < dn ? in[i + 2] : 0;
        out[o++] = b64[(v >> 18) & 63];
        out[o++] = b64[(v >> 12) & 63];
        out[o++] = i + 1 < dn ? b64[(v >> 6) & 63] : '=';
        out[o++] = i + 2 < dn ? b64[v & 63] : '=';
    }
    out[o] = '\0';
    return out;
}

/* A draw.io page as the app stores it compressed: URI-encoded, raw deflate,
 * base64. */
static char *dg_drawio_compress(const char *xml) {
    size_t n = strlen(xml);
    char *enc = malloc(n * 3 + 1);
    size_t w = 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)xml[i];
        if (isalnum(c) || strchr("-_.!~*'()", c)) {
            enc[w++] = (char)c;
        } else {
            w += (size_t)snprintf(enc + w, n * 3 + 1 - w, "%%%02X", c);
        }
    }
    unsigned char *def = malloc(w + 64);
    z_stream z;
    memset(&z, 0, sizeof(z));
    deflateInit2(&z, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -MAX_WBITS, 8, Z_DEFAULT_STRATEGY);
    z.next_in = (Bytef *)enc;
    z.avail_in = (uInt)w;
    z.next_out = def;
    z.avail_out = (uInt)(w + 64);
    deflate(&z, Z_FINISH);
    size_t dn = z.total_out;
    deflateEnd(&z);
    char *out = dg_b64(def, dn);
    free(enc);
    free(def);
    return out;
}

TEST(diagrams_drawio) {
    char file[4096];
    snprintf(file, sizeof(file),
             "<mxfile><diagram name=\"Overview\" id=\"x\">%s</diagram></mxfile>", DG_DRAWIO_PAGE);
    CBMFileResult *r = dg_file(file, "docs/sys.drawio");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *d = dg_def(r, "sys#diagram-1");
    ASSERT_NOT_NULL(d);
    ASSERT_STR_EQ(d->name, "Overview");
    ASSERT_STR_EQ(dg_def(r, "sys#diagram-1.2")->name, "Frontend");
    const CBMDefinition *b = dg_def(r, "sys#diagram-1.3");
    ASSERT_STR_EQ(b->name, "Backend"); /* its HTML dropped */
    ASSERT_STR_EQ(dg_prop(b, "element_kind"), "cylinder3");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.2", "#diagram-1.3", "arrow", "calls"), 1);
    ASSERT_EQ(r->diagram_arrows.count, 1); /* the edge to a shape without text is none */
    cbm_free_result(r);

    /* the compressed form, two pages */
    char *packed = dg_drawio_compress(DG_DRAWIO_PAGE);
    size_t big_cap = strlen(packed) * 2 + 256;
    char *big = malloc(big_cap);
    snprintf(big, big_cap,
             "<mxfile><diagram name=\"A\">%s</diagram><diagram name=\"B\">%s</diagram></mxfile>",
             packed, packed);
    r = dg_file(big, "docs/two.drawio");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "two#diagram-1")->name, "A");
    ASSERT_STR_EQ(dg_def(r, "two#diagram-2")->name, "B");
    ASSERT_EQ(dg_arrows(r, "#diagram-2.2", "#diagram-2.3", "arrow", "calls"), 1);
    cbm_free_result(r);
    free(big);
    free(packed);

    /* draw.io's editable SVG keeps the file in its content attribute */
    r = dg_file("<svg xmlns=\"http://www.w3.org/2000/svg\" content=\"&lt;mxfile&gt;&lt;diagram "
                "name=&quot;S&quot;&gt;&lt;mxGraphModel&gt;&lt;root&gt;&lt;mxCell id=&quot;2&quot; "
                "value=&quot;One&quot; vertex=&quot;1&quot;/&gt;&lt;mxCell id=&quot;3&quot; "
                "value=&quot;Two&quot; vertex=&quot;1&quot;/&gt;&lt;mxCell id=&quot;4&quot; "
                "edge=&quot;1&quot; source=&quot;2&quot; target=&quot;3&quot;/&gt;&lt;/root&gt;&lt;"
                "/mxGraphModel&gt;&lt;/diagram&gt;&lt;/mxfile&gt;\"><g/></svg>",
                "docs/edit.drawio.svg");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "#diagram-1")->name, "S");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.2", "#diagram-1.3", "arrow", ""), 1);
    cbm_free_result(r);
    PASS();
}

TEST(diagrams_excalidraw) {
    CBMFileResult *r =
        dg_file("{\"type\":\"excalidraw\",\"elements\":["
                "{\"id\":\"r1\",\"type\":\"rectangle\",\"isDeleted\":false},"
                "{\"id\":\"t1\",\"type\":\"text\",\"text\":\"User\",\"containerId\":\"r1\"},"
                "{\"id\":\"r2\",\"type\":\"ellipse\"},"
                "{\"id\":\"t2\",\"type\":\"text\",\"text\":\"Service\",\"containerId\":\"r2\"},"
                "{\"id\":\"a1\",\"type\":\"arrow\",\"startBinding\":{\"elementId\":\"r1\"},"
                "\"endBinding\":{\"elementId\":\"r2\"}},"
                "{\"id\":\"t3\",\"type\":\"text\",\"text\":\"calls\",\"containerId\":\"a1\"},"
                "{\"id\":\"r3\",\"type\":\"rectangle\",\"isDeleted\":true},"
                "{\"id\":\"t5\",\"type\":\"text\",\"text\":\"Gone\",\"containerId\":\"r3\","
                "\"isDeleted\":true},"
                "{\"id\":\"t4\",\"type\":\"text\",\"text\":\"Note here\"}]}",
                "docs/flow.excalidraw");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *d = dg_def(r, "flow#diagram-1");
    ASSERT_NOT_NULL(d);
    ASSERT(d->docstring && strstr(d->docstring, "Note here"));
    ASSERT_STR_EQ(dg_def(r, "flow#diagram-1.r1")->name, "User");
    ASSERT_STR_EQ(dg_prop(dg_def(r, "flow#diagram-1.r2"), "element_kind"), "ellipse");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.r1", "#diagram-1.r2", "arrow", "calls"), 1);
    for (int i = 0; i < r->defs.count; i++) {
        ASSERT(!strstr(r->defs.items[i].qualified_name, ".r3"));
    }
    cbm_free_result(r);
    PASS();
}

TEST(diagrams_d2_blocks) {
    /* class and style blocks around containers (their braces once restored
     * the path from an unset level), block-string labels, dotted attributes */
    CBMFileResult *r = dg_file("direction: down\n"
                               "classes: {\n"
                               "  card: {\n"
                               "    shape: text\n"
                               "  }\n"
                               "}\n"
                               "label: Hierarchy\n"
                               "consumers: \"\" {\n"
                               "  style: {\n"
                               "    stroke-dash: 4\n"
                               "  }\n"
                               "  app: ||md\n"
                               "    <b>App</b>\n"
                               "    Cross-platform: everywhere\n"
                               "  || {\n"
                               "    class: card\n"
                               "  }\n"
                               "  cli: |md # CLI|\n"
                               "}\n"
                               "core.style.fill: red\n"
                               "core.label: Core\n"
                               "consumers.app -> core: |md uses|\n"
                               "consumers.cli -- core\n",
                               "docs/deps.d2");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "deps#diagram-1")->name, "Hierarchy");
    ASSERT_STR_EQ(dg_def(r, "deps#diagram-1.consumers_app")->name, "App");
    ASSERT_STR_EQ(dg_def(r, "deps#diagram-1.consumers_cli")->name, "CLI");
    ASSERT_STR_EQ(dg_def(r, "deps#diagram-1.core")->name, "Core");
    const char *doc = dg_def(r, "deps#diagram-1")->docstring;
    ASSERT(doc && strstr(doc, "Cross-platform: everywhere"));
    ASSERT_EQ(dg_arrows(r, "#diagram-1.consumers_app", "#diagram-1.core", "arrow", "uses"), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.consumers_cli", "#diagram-1.core", "line", ""), 1);
    ASSERT_EQ(r->diagram_arrows.count, 2);
    for (int i = 0; i < r->defs.count; i++) { /* no attribute or block text became a shape */
        const char *qn = r->defs.items[i].qualified_name;
        ASSERT(!strstr(qn, "style") && !strstr(qn, "card") && !strstr(qn, "class") &&
               !strstr(qn, "Cross"));
    }
    cbm_free_result(r);
    PASS();
}

TEST(diagrams_excalidraw_layout) {
    /* text laid over shapes and arrows drawn up to them, nothing bound */
    CBMFileResult *r = dg_file(
        "{\"type\":\"excalidraw\",\"elements\":["
        "{\"id\":\"f1\",\"type\":\"rectangle\",\"x\":0,\"y\":0,\"width\":900,\"height\":400},"
        "{\"id\":\"t0\",\"type\":\"text\",\"text\":\"System\",\"x\":10,\"y\":5,\"width\":60,"
        "\"height\":20},"
        "{\"id\":\"r1\",\"type\":\"rectangle\",\"x\":20,\"y\":50,\"width\":100,\"height\":60},"
        "{\"id\":\"t2\",\"type\":\"text\",\"text\":\"browser\",\"x\":30,\"y\":95,\"width\":40,"
        "\"height\":10},"
        "{\"id\":\"t1\",\"type\":\"text\",\"text\":\"Client\",\"x\":40,\"y\":70,\"width\":60,"
        "\"height\":20},"
        "{\"id\":\"r2\",\"type\":\"ellipse\",\"x\":250,\"y\":50,\"width\":100,\"height\":60},"
        "{\"id\":\"t3\",\"type\":\"text\",\"text\":\"Server\",\"x\":270,\"y\":70,\"width\":60,"
        "\"height\":20},"
        "{\"id\":\"a1\",\"type\":\"arrow\",\"x\":122,\"y\":80,\"points\":[[0,0],[126,0]]},"
        "{\"id\":\"l1\",\"type\":\"line\",\"x\":122,\"y\":90,\"points\":[[0,0],[126,0]]},"
        "{\"id\":\"a2\",\"type\":\"arrow\",\"x\":30,\"y\":60,\"points\":[[0,0],[70,40]]},"
        "{\"id\":\"a3\",\"type\":\"arrow\",\"x\":122,\"y\":200,\"points\":[[0,0],[500,400]]},"
        /* from r2 deep into the frame around it all: at nothing named */
        "{\"id\":\"a4\",\"type\":\"arrow\",\"x\":300,\"y\":112,\"points\":[[0,0],[0,90]]},"
        /* from r1 to the frame's border: at the frame */
        "{\"id\":\"a5\",\"type\":\"arrow\",\"x\":70,\"y\":112,\"points\":[[0,0],[0,284]]},"
        /* two lanes, each with a named badge: an end deep in one is at the lane */
        "{\"id\":\"L1\",\"type\":\"rectangle\",\"x\":500,\"y\":0,\"width\":100,\"height\":300},"
        "{\"id\":\"tl1\",\"type\":\"text\",\"text\":\"Lane A\",\"x\":520,\"y\":10,\"width\":60,"
        "\"height\":20},"
        "{\"id\":\"b1\",\"type\":\"ellipse\",\"x\":540,\"y\":200,\"width\":20,\"height\":20},"
        "{\"id\":\"tb1\",\"type\":\"text\",\"text\":\"1\",\"containerId\":\"b1\"},"
        "{\"id\":\"L2\",\"type\":\"rectangle\",\"x\":700,\"y\":0,\"width\":100,\"height\":300},"
        "{\"id\":\"tl2\",\"type\":\"text\",\"text\":\"Lane B\",\"x\":720,\"y\":10,\"width\":60,"
        "\"height\":20},"
        "{\"id\":\"b2\",\"type\":\"rectangle\",\"x\":745,\"y\":150,\"width\":10,\"height\":40},"
        "{\"id\":\"a6\",\"type\":\"arrow\",\"x\":555,\"y\":120,\"points\":[[0,0],[195,0]]},"
        /* bound to the text that names r1: at r1 */
        "{\"id\":\"a7\",\"type\":\"arrow\",\"startBinding\":{\"elementId\":\"r2\"},"
        "\"endBinding\":{\"elementId\":\"t1\"}},"
        /* bound to free text that names no shape: at the text */
        "{\"id\":\"t9\",\"type\":\"text\",\"text\":\"Note\",\"x\":440,\"y\":350,\"width\":40,"
        "\"height\":20},"
        "{\"id\":\"a8\",\"type\":\"arrow\",\"startBinding\":{\"elementId\":\"L2\"},"
        "\"endBinding\":{\"elementId\":\"t9\"}}]}",
        "docs/layout.excalidraw");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "layout#diagram-1.f1")->name, "System");
    ASSERT_STR_EQ(dg_def(r, "layout#diagram-1.r1")->name,
                  "Client"); /* the top text, not the frame */
    ASSERT_STR_EQ(dg_def(r, "layout#diagram-1.r2")->name, "Server");
    const char *doc = dg_def(r, "layout#diagram-1")->docstring;
    ASSERT(doc && strstr(doc, "browser"));
    ASSERT_EQ(dg_arrows(r, "#diagram-1.r1", "#diagram-1.r2", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.r1", "#diagram-1.f1", "arrow", ""), 1);
    /* the loose line, the loop inside r1, the dangling end and the end deep in
     * the frame are no arrows */
    ASSERT_EQ(dg_arrows(r, "#diagram-1.L1", "#diagram-1.L2", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.r2", "#diagram-1.r1", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-1.L2", "#diagram-1.t9", "arrow", ""), 1);
    ASSERT_STR_EQ(dg_prop(dg_def(r, "layout#diagram-1.t9"), "element_kind"), "text");
    ASSERT_EQ(r->diagram_arrows.count, 5);
    ASSERT_STR_EQ(dg_def(r, "layout#diagram-1")->name, "layout"); /* untitled: the file */
    cbm_free_result(r);
    PASS();
}

/* An Excalidraw SVG export as the app writes it: the scene zlib-compressed,
 * wrapped in JSON as a byte string, base64 in a payload comment. */
static char *dg_excalidraw_svg(const char *scene) {
    uLongf zn = compressBound((uLong)strlen(scene));
    unsigned char *z = malloc(zn);
    compress2(z, &zn, (const Bytef *)scene, (uLong)strlen(scene), Z_DEFAULT_COMPRESSION);
    size_t cap = zn * 6 + 128;
    char *json = malloc(cap);
    size_t w = (size_t)snprintf(json, cap,
                                "{\"version\":\"1\",\"encoding\":\"bstring\",\"compressed\":true,"
                                "\"encoded\":\"");
    for (uLongf i = 0; i < zn; i++) {
        unsigned char b = z[i];
        if (b == '"' || b == '\\') {
            json[w++] = '\\';
            json[w++] = (char)b;
        } else if (b < 0x20) {
            w += (size_t)snprintf(json + w, cap - w, "\\u%04x", b);
        } else {
            json[w++] = (char)b; /* a byte as one character: the wrapper is no UTF-8 */
        }
    }
    w += (size_t)snprintf(json + w, cap - w, "\"}");
    char *b64 = dg_b64((const unsigned char *)json, w);
    size_t sc = strlen(b64) + 512;
    char *svg = malloc(sc);
    snprintf(svg, sc,
             "<svg version=\"1.1\" xmlns=\"http://www.w3.org/2000/svg\">\n"
             "  <!-- svg-source:excalidraw -->\n"
             "  <metadata><!-- payload-type:application/vnd.excalidraw+json -->"
             "<!-- payload-version:2 --><!-- payload-start -->%s<!-- payload-end --></metadata>\n"
             "  <g/>\n</svg>\n",
             b64);
    free(b64);
    free(json);
    free(z);
    return svg;
}

TEST(diagrams_excalidraw_svg) {
    char *svg = dg_excalidraw_svg(
        "{\"type\":\"excalidraw\",\"elements\":["
        "{\"id\":\"r1\",\"type\":\"rectangle\"},"
        "{\"id\":\"t1\",\"type\":\"text\",\"text\":\"Queue \xc3\xa9v\xc3\xa9nements\","
        "\"containerId\":\"r1\"},"
        "{\"id\":\"r2\",\"type\":\"rectangle\"},"
        "{\"id\":\"t2\",\"type\":\"text\",\"text\":\"Worker\",\"containerId\":\"r2\"},"
        "{\"id\":\"a1\",\"type\":\"arrow\",\"startBinding\":{\"elementId\":\"r1\"},"
        "\"endBinding\":{\"elementId\":\"r2\"}}]}");
    CBMFileResult *r = dg_file(svg, "docs/flow.excalidraw.svg");
    ASSERT_NOT_NULL(r);
    ASSERT_STR_EQ(dg_def(r, "#diagram-1.r1")->name, "Queue \xc3\xa9v\xc3\xa9nements");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.r1", "#diagram-1.r2", "arrow", ""), 1);
    cbm_free_result(r);
    free(svg);
    PASS();
}

TEST(diagrams_rst) {
    CBMFileResult *r = dm_extract("Guide\n"
                                  "=====\n"
                                  "\n"
                                  ".. mermaid::\n"
                                  "   :caption: Request flow\n"
                                  "\n"
                                  "   flowchart LR\n"
                                  "     A --> B\n"
                                  "\n"
                                  ".. digraph:: deps\n"
                                  "\n"
                                  "   core -> util;\n"
                                  "   app -> core;\n"
                                  "\n"
                                  ".. graphviz:: other.dot\n"
                                  "\n"
                                  ".. uml::\n"
                                  "\n"
                                  "   @startuml\n"
                                  "   [Web] --> [API]\n"
                                  "   @enduml\n",
                                  CBM_LANG_RST, "docs/guide.rst");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *m = dg_def(r, "guide#diagram-1");
    ASSERT_NOT_NULL(m);
    ASSERT_STR_EQ(m->name, "Request flow");
    ASSERT_STR_EQ(dg_prop(m, "format"), "mermaid");
    ASSERT_EQ(dg_arrows(r, "guide#diagram-1.A", "guide#diagram-1.B", "arrow", ""), 1);
    /* Sphinx's digraph: the statements inside the graph it names */
    ASSERT_STR_EQ(dg_prop(dg_def(r, "guide#diagram-2"), "format"), "dot");
    ASSERT_EQ(dg_arrows(r, "guide#diagram-2.core", "guide#diagram-2.util", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "guide#diagram-2.app", "guide#diagram-2.core", "arrow", ""), 1);
    /* a graphviz file argument is no diagram here (the file is one itself) */
    ASSERT_STR_EQ(dg_prop(dg_def(r, "guide#diagram-3"), "format"), "plantuml");
    ASSERT_EQ(dg_arrows(r, "guide#diagram-3.Web", "guide#diagram-3.API", "arrow", ""), 1);
    ASSERT_NULL(dg_def(r, "guide#diagram-4"));
    cbm_free_result(r);
    PASS();
}

/* The Diagram named `name`, NULL for none. */
static const CBMDefinition *dg_named(const CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *d = &r->defs.items[i];
        if (d->label && strcmp(d->label, "Diagram") == 0 && d->name && strcmp(d->name, name) == 0) {
            return d;
        }
    }
    return NULL;
}

TEST(diagrams_asciidoc) {
    CBMFileResult *r = dm_extract("= Guide\n"
                                  "\n"
                                  "== Flow\n"
                                  "\n"
                                  ".Login sequence\n"
                                  "[plantuml, target=login, format=svg]\n"
                                  "----\n"
                                  "participant Alice\n"
                                  "Alice -> Bob : hello\n"
                                  "----\n"
                                  "\n"
                                  ".Steps\n"
                                  "[source,mermaid]\n"
                                  "....\n"
                                  "flowchart TD\n"
                                  "  X --> Y\n"
                                  "....\n"
                                  "\n"
                                  "[source,python]\n"
                                  "----\n"
                                  "a -> b\n"
                                  "----\n"
                                  "\n"
                                  "[graphviz]\n"
                                  "----\n"
                                  "digraph { a -> b }\n"
                                  "----\n",
                                  CBM_LANG_ASCIIDOC, "docs/guide.adoc");
    ASSERT_NOT_NULL(r);
    const CBMDefinition *p = dg_named(r, "Login sequence");
    ASSERT_NOT_NULL(p);
    ASSERT_NOT_NULL(p->parent_class); /* under its Section */
    ASSERT(strncmp(p->qualified_name, p->parent_class, strlen(p->parent_class)) == 0);
    ASSERT_STR_EQ(dg_prop(p, "diagram_kind"), "sequence");
    ASSERT_EQ(dg_arrows(r, "#diagram-1.Alice", "#diagram-1.Bob", "message", "hello"), 1);
    ASSERT_STR_EQ(dg_prop(dg_named(r, "Steps"), "format"), "mermaid");
    ASSERT_EQ(dg_arrows(r, "#diagram-2.X", "#diagram-2.Y", "arrow", ""), 1);
    ASSERT_EQ(dg_arrows(r, "#diagram-3.a", "#diagram-3.b", "arrow", ""), 1); /* not python */
    ASSERT_EQ(r->diagram_arrows.count, 3);
    cbm_free_result(r);
    PASS();
}

/* The doc-link records of one syntax whose source ends in `src` and whose
 * reference is `raw`. */
static int dg_links(const CBMFileResult *r, int syntax, const char *src, const char *raw) {
    int n = 0;
    size_t sl = strlen(src);
    for (int i = 0; i < r->doc_links.count; i++) {
        const CBMDocLink *l = &r->doc_links.items[i];
        size_t ql = strlen(l->source_qn);
        n += l->syntax == syntax && ql >= sl && strcmp(l->source_qn + ql - sl, src) == 0 &&
             strcmp(l->raw, raw) == 0;
    }
    if (n != 1) {
        printf("  link %d %s -> %s: %d found\n", syntax, src, raw, n);
    }
    return n;
}

TEST(diagrams_links) {
    CBMFileResult *r =
        dm_extract("# Flow\n\n```mermaid\nflowchart LR\n  A[Api] --> B\n"
                   "  click A \"src/api.py\" \"the API\"\n  click B call cb()\n```\n",
                   CBM_LANG_MARKDOWN, "docs/flow.md");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_MERMAID_CLICK, "#diagram-1.A", "src/api.py"), 1);
    ASSERT_EQ(r->doc_links.count, 1); /* a callback links nothing */
    cbm_free_result(r);

    r = dg_file("@startuml\n!include common.iuml\n!include <C4/C4_Container>\n"
                "class Foo [[src/foo.py]]\nclass Bar\nurl of Bar is [[docs/bar.md{the bar}]]\n"
                "Foo --> Bar\n@enduml\n",
                "docs/m.puml");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_PLANTUML_INCLUDE, "m#diagram-1", "common.iuml"), 1);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_PLANTUML_URL, "#diagram-1.Foo", "src/foo.py"), 1);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_PLANTUML_URL, "#diagram-1.Bar", "docs/bar.md"), 1);
    ASSERT_EQ(r->doc_links.count, 3); /* the standard library is none of the repo's */
    cbm_free_result(r);

    r = dg_file("digraph { a [label=\"A\", URL=\"src/a.c\"]; b [href=\"#top\"]; a -> b; }\n",
                "docs/g.dot");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_DOT_URL, "#diagram-1.a", "src/a.c"), 1);
    ASSERT_EQ(r->doc_links.count, 1); /* an anchor links nothing */
    cbm_free_result(r);

    r = dg_file("x: X\nx.link: src/x.go\ny: Y {\n  link: https://example.com/y\n}\n", "docs/d.d2");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_D2_LINK, "#diagram-1.x", "src/x.go"), 1);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_D2_LINK, "#diagram-1.y", "https://example.com/y"), 1);
    cbm_free_result(r);

    r = dg_file("<mxfile><diagram name=\"P\"><mxGraphModel><root>"
                "<object label=\"Svc\" link=\"src/svc.py\" id=\"7\"><mxCell vertex=\"1\"/></object>"
                "<object label=\"Page\" link=\"data:page/id,abc\" id=\"8\"><mxCell vertex=\"1\"/>"
                "</object></root></mxGraphModel></diagram></mxfile>",
                "docs/s.drawio");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_DRAWIO_LINK, "#diagram-1.7", "src/svc.py"), 1);
    ASSERT_EQ(r->doc_links.count, 1); /* a page link links nothing of the repo */
    cbm_free_result(r);

    r = dg_file("{\"type\":\"excalidraw\",\"elements\":["
                "{\"id\":\"r1\",\"type\":\"rectangle\",\"link\":\"src/e.ts\"},"
                "{\"id\":\"t1\",\"type\":\"text\",\"text\":\"E\",\"containerId\":\"r1\"},"
                "{\"id\":\"r2\",\"type\":\"rectangle\",\"link\":null}]}",
                "docs/e.excalidraw");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dg_links(r, CBM_DOCLINK_DG_EXCALIDRAW_LINK, "#diagram-1.r1", "src/e.ts"), 1);
    ASSERT_EQ(r->doc_links.count, 1);
    cbm_free_result(r);
    PASS();
}

static int dg_db_edges(const char *db, const char *type, const char *src_suffix,
                       const char *tgt_suffix, const char *props_part);

TEST(diagrams_links_graph) {
    /* the families are held until their audit: shipped here */
    dm_ship_held_families();
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dglinks_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "src/api.py"), "def handle():\n    return 1\n");
    th_write_file(TH_PATH(repo, "diagrams/a.puml"),
                  "@startuml\nclass Api [[../src/api.py]]\nclass Db\nApi --> Db\n@enduml\n");
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* the element names the file it links to */
    ASSERT_EQ(dg_db_edges(db, "MENTIONS", "a#diagram-1.Api", "", NULL), 1);
    th_cleanup(tmp);
    cbm_doclink_test_reset_ships();
    PASS();
}

/* ── the graph ───────────────────────────────────────────────────── */

static int dg_db_edges(const char *db, const char *type, const char *src_suffix,
                       const char *tgt_suffix, const char *props_part) {
    sqlite3 *h = NULL;
    int n = 0;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return -1;
    }
    sqlite3_stmt *st = NULL;
    const char *sql = "SELECT e.properties FROM edges e JOIN nodes s ON s.id = e.source_id "
                      "JOIN nodes t ON t.id = e.target_id WHERE e.type = ?1 "
                      "AND s.qualified_name LIKE '%' || ?2 AND t.qualified_name LIKE '%' || ?3";
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, type, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, src_suffix, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, tgt_suffix, -1, SQLITE_TRANSIENT);
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *p = (const char *)sqlite3_column_text(st, 0);
            n += !props_part || (p && strstr(p, props_part));
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    if (n != 1) {
        printf("  %s %s -> %s (%s): %d found\n", type, src_suffix, tgt_suffix,
               props_part ? props_part : "", n);
    }
    return n;
}

TEST(diagrams_graph) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dg_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    th_write_file(TH_PATH(repo, "docs/arch.md"), DG_DOC);
    th_write_file(TH_PATH(repo, "diagrams/flow.mmd"), "flowchart TD\n  X[Start] --> Y[Stop]\n");
    th_write_file(TH_PATH(repo, "diagrams/c.puml"), "@startuml\n[A] --> [B]\n@enduml\n");
    /* an image by its last extension, a diagram by its name */
    th_write_file(TH_PATH(repo, "diagrams/e.drawio.svg"),
                  "<svg content=\"&lt;mxfile&gt;&lt;diagram&gt;&lt;mxGraphModel&gt;&lt;root&gt;"
                  "&lt;mxCell id=&quot;p&quot; value=&quot;P&quot; vertex=&quot;1&quot;/&gt;"
                  "&lt;mxCell id=&quot;q&quot; value=&quot;Q&quot; vertex=&quot;1&quot;/&gt;"
                  "&lt;mxCell id=&quot;pq&quot; edge=&quot;1&quot; source=&quot;p&quot; "
                  "target=&quot;q&quot;/&gt;&lt;/root&gt;&lt;/mxGraphModel&gt;&lt;/diagram&gt;"
                  "&lt;/mxfile&gt;\"><g/></svg>\n");
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    /* containment: the section holds the diagram, the diagram its elements */
    ASSERT_EQ(dg_db_edges(db, "DEFINES", "docs.arch.Arch", "docs.arch.Arch#diagram-1", NULL), 1);
    ASSERT_EQ(dg_db_edges(db, "DEFINES", "Arch#diagram-1", "Arch#diagram-1.A", NULL), 1);
    /* the arrows, with their text */
    ASSERT_EQ(dg_db_edges(db, "DIAGRAM_ARROW", "Arch#diagram-1.A", "Arch#diagram-1.B",
                          "\"label\":\"calls\""),
              1);
    ASSERT_EQ(dg_db_edges(db, "DIAGRAM_ARROW", "Model#diagram-1.Dog", "Model#diagram-1.Animal",
                          "\"style\":\"inheritance\""),
              1);
    /* a diagram file: its diagram under the file */
    ASSERT_EQ(dg_db_edges(db, "DIAGRAM_ARROW", "flow#diagram-1.X", "flow#diagram-1.Y", NULL), 1);
    /* a diagram file of another format: discovered, its arrows in the graph */
    ASSERT_EQ(dg_db_edges(db, "DIAGRAM_ARROW", "c#diagram-1.A", "c#diagram-1.B", NULL), 1);
    ASSERT_EQ(dg_db_edges(db, "DIAGRAM_ARROW", "#diagram-1.p", "#diagram-1.q", NULL), 1);
    th_cleanup(tmp);
    PASS();
}

/* ── incremental == full ─────────────────────────────────────────── */

/* The diagram layer of a graph as sorted lines: its nodes, their containment,
 * the arrows, and the edges the elements' links resolved to. */
static char *dg_state(const char *db) {
    sqlite3 *h = NULL;
    if (sqlite3_open_v2(db, &h, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(h);
        return NULL;
    }
    const char *sql =
        "SELECT 'N|' || label || '|' || qualified_name || '|' || name || '|' || "
        "COALESCE(properties, '') AS l FROM nodes WHERE label IN ('Diagram', 'DiagramElement') "
        "UNION ALL SELECT 'E|' || e.type || '|' || s.qualified_name || '|' || t.qualified_name || "
        "'|' || COALESCE(e.properties, '') FROM edges e JOIN nodes s ON s.id = e.source_id "
        "JOIN nodes t ON t.id = e.target_id WHERE e.type = 'DIAGRAM_ARROW' OR (e.type = "
        "'DEFINES' AND t.label IN ('Diagram', 'DiagramElement')) OR (e.type = 'MENTIONS' AND "
        "s.label IN ('Diagram', 'DiagramElement')) ORDER BY 1";
    sqlite3_stmt *st = NULL;
    size_t cap = 4096;
    size_t len = 0;
    char *buf = malloc(cap);
    buf[0] = '\0';
    if (sqlite3_prepare_v2(h, sql, -1, &st, NULL) == SQLITE_OK) {
        while (sqlite3_step(st) == SQLITE_ROW) {
            const char *line = (const char *)sqlite3_column_text(st, 0);
            size_t l = strlen(line);
            if (len + l + 2 > cap) {
                cap = (len + l + 2) * 2;
                buf = realloc(buf, cap);
            }
            memcpy(buf + len, line, l);
            len += l;
            buf[len++] = '\n';
            buf[len] = '\0';
        }
    }
    sqlite3_finalize(st);
    sqlite3_close(h);
    return buf;
}

/* An incremental index of `repo` into inc_db, a full one into a fresh
 * full_db: the same diagram layer. 0, or -1 after printing the difference. */
static int dg_step(const char *repo, const char *inc_db, const char *full_db, const char *what) {
    if (dm_index(repo, inc_db, NULL) != 0) {
        printf("  %s: incremental index failed\n", what);
        return -1;
    }
    dm_unlink_db(full_db);
    if (dm_index(repo, full_db, NULL) != 0) {
        printf("  %s: full index failed\n", what);
        return -1;
    }
    char *inc = dg_state(inc_db);
    char *full = dg_state(full_db);
    int rc = inc && full && strcmp(inc, full) == 0 && full[0] ? 0 : -1;
    if (rc != 0) {
        printf("  %s: incremental != full\n--- incremental\n%s--- full\n%s", what,
               inc ? inc : "(null)", full ? full : "(null)");
    }
    free(inc);
    free(full);
    return rc;
}

static void dg_write_all(const char *repo, int round) {
    char text[1024];
    snprintf(text, sizeof(text),
             "# Arch%s\n\n```mermaid\nflowchart LR\n  A[Api] --> B[Db%d]\n"
             "  click A \"../src/api.py\"\n```\n",
             round == 2 ? "itecture" : "", round);
    th_write_file(TH_PATH(repo, "docs/arch.md"), text);
    snprintf(text, sizeof(text),
             "@startuml\nclass Api [[../src/api.py]]\nclass Db\nApi --> Db : r%d\n"
             "@enduml\n",
             round);
    th_write_file(TH_PATH(repo, "diagrams/m.puml"), text);
    snprintf(text, sizeof(text), "digraph { a -> b [label=\"r%d\"]; }\n", round);
    th_write_file(TH_PATH(repo, "diagrams/g.dot"), text);
    snprintf(text, sizeof(text), "x: X\ny: Y%d\nx -> y\n", round);
    th_write_file(TH_PATH(repo, "diagrams/s.d2"), text);
    snprintf(text, sizeof(text),
             "<mxfile><diagram name=\"P\"><mxGraphModel><root>"
             "<mxCell id=\"2\" value=\"One\" vertex=\"1\"/><mxCell id=\"3\" value=\"Two%d\" "
             "vertex=\"1\"/><mxCell id=\"4\" edge=\"1\" source=\"2\" target=\"3\"/>"
             "</root></mxGraphModel></diagram></mxfile>",
             round);
    th_write_file(TH_PATH(repo, "diagrams/p.drawio"), text);
    snprintf(text, sizeof(text),
             "Guide\n=====\n\n.. mermaid::\n\n   flowchart LR\n     G --> H%d\n", round);
    th_write_file(TH_PATH(repo, "docs/guide.rst"), text);
    snprintf(text, sizeof(text), "= Guide\n\n== Flow\n\n[plantuml]\n----\n[P] --> [Q%d]\n----\n",
             round);
    th_write_file(TH_PATH(repo, "docs/flow.adoc"), text);
    if (round < 2) {
        snprintf(text, sizeof(text),
                 "{\"type\":\"excalidraw\",\"elements\":["
                 "{\"id\":\"r1\",\"type\":\"rectangle\"},{\"id\":\"t1\",\"type\":\"text\","
                 "\"text\":\"U%d\",\"containerId\":\"r1\"},{\"id\":\"r2\",\"type\":\"rectangle\"},"
                 "{\"id\":\"t2\",\"type\":\"text\",\"text\":\"V\",\"containerId\":\"r2\"},"
                 "{\"id\":\"a\",\"type\":\"arrow\",\"startBinding\":{\"elementId\":\"r1\"},"
                 "\"endBinding\":{\"elementId\":\"r2\"}}]}",
                 round);
        th_write_file(TH_PATH(repo, "diagrams/e.excalidraw"), text);
    }
}

TEST(diagrams_incremental_equals_full) {
    dm_ship_held_families();
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dg_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    th_write_file(TH_PATH(repo, "src/api.py"), "def handle():\n    return 1\n");
    dg_write_all(repo, 0);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    /* every diagram's text edited */
    dg_write_all(repo, 1);
    ASSERT_EQ(dg_step(repo, inc_db, full_db, "edit"), 0);
    /* a heading renamed (the diagram's parent with it), a diagram file gone */
    dg_write_all(repo, 2);
    char gone[600];
    snprintf(gone, sizeof(gone), "%s/diagrams/e.excalidraw", repo);
    unlink(gone);
    ASSERT_EQ(dg_step(repo, inc_db, full_db, "rename + delete"), 0);
    /* and back */
    dg_write_all(repo, 1);
    ASSERT_EQ(dg_step(repo, inc_db, full_db, "re-add"), 0);
    th_cleanup(tmp);
    cbm_doclink_test_reset_ships();
    PASS();
}

SUITE(diagrams) {
    RUN_TEST(diagrams_markdown_mermaid);
    RUN_TEST(diagrams_section_text_keeps_labels);
    RUN_TEST(diagrams_mermaid_kinds);
    RUN_TEST(diagrams_other_fences);
    RUN_TEST(diagrams_plantuml);
    RUN_TEST(diagrams_dot);
    RUN_TEST(diagrams_d2);
    RUN_TEST(diagrams_drawio);
    RUN_TEST(diagrams_excalidraw);
    RUN_TEST(diagrams_d2_blocks);
    RUN_TEST(diagrams_excalidraw_layout);
    RUN_TEST(diagrams_excalidraw_svg);
    RUN_TEST(diagrams_rst);
    RUN_TEST(diagrams_asciidoc);
    RUN_TEST(diagrams_links);
    RUN_TEST(diagrams_links_graph);
    RUN_TEST(diagrams_incremental_equals_full);
    RUN_TEST(diagrams_graph);
}
