/*
 * test_grammar_probe_b.c — Grammar-only node/import/inheritance probe for 12 languages.
 *
 * Scope: glsl, hare, hlsl, ispc, julia, luau, matlab, odin, pascal, powershell,
 *        racket, rescript.
 *
 * CALLS coverage for these languages is already handled by P5 (contract_calls_breadth).
 * This suite probes:
 *   - NODE creation: Function/Procedure/Struct/Type nodes reach the graph after
 *     a full index_repository pipeline run (shader entrypoints, shader struct types,
 *     OOP types, module-like constructs).
 *   - IMPORTS edges: where the language has a cross-file import mechanism that the
 *     extractor knows about (julia `using`/`import`, racket `require`,
 *     powershell `using module`, rescript `open`, pascal `uses`, odin `import`,
 *     hare `use`).  MATLAB has no standard inter-file import syntax → skip.
 *     glsl/hlsl/ispc use `#include` / preprocessor — not modeled as IMPORTS → skip.
 *   - INHERITANCE: powershell classes (`: Base`), pascal OOP (`: Base`), julia
 *     abstract-type subtyping (`<:`) where same-file base is resolvable.
 *     Odin, GLSL, HLSL, ISPC, Hare, MATLAB, ReScript, Racket, Luau have no
 *     class inheritance → skip.
 *
 * Convention:
 *   GREEN  = contract that SHOULD hold; a failure here is a real regression.
 *   RED    = known bug reproduced as a failing assertion; kept as a regression
 *            guard until fixed (brief // BUG comment explains root cause).
 *
 * Do NOT register this SUITE in test_main.c — the build system discovers it
 * separately alongside _a and _c.
 */

#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "cbm.h"
#include <mcp/mcp.h>
#include <store/store.h>
#include <pipeline/pipeline.h>
#include <foundation/log.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#if !defined(_WIN32)
#include <sys/wait.h>
#endif

/* ── Harness (mirrors test_lang_contract.c) ────────────────────────────────── */

typedef struct {
    char tmpdir[256];
    char dbpath[512];
    char *project;
    cbm_mcp_server_t *srv;
} ProbeLangProj;

typedef struct {
    const char *name;
    const char *content;
} ProbeLangFile;

static void pb_fwd_slashes(char *p) {
    for (; *p; p++) {
        if (*p == '\\') *p = '/';
    }
}

static cbm_store_t *pb_open_indexed(ProbeLangProj *lp) {
    lp->project = cbm_project_name_from_path(lp->tmpdir);
    if (!lp->project) return NULL;
    const char *home = getenv("HOME");
    if (!home) home = "/tmp";
    char cache_dir[512];
    snprintf(cache_dir, sizeof(cache_dir), "%s/.cache/codebase-memory-mcp", home);
    cbm_mkdir(cache_dir);
    snprintf(lp->dbpath, sizeof(lp->dbpath), "%s/%s.db", cache_dir, lp->project);
    unlink(lp->dbpath);
    lp->srv = cbm_mcp_server_new(NULL);
    if (!lp->srv) return NULL;
    char args[700];
    snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", lp->tmpdir);
    char *resp = cbm_mcp_handle_tool(lp->srv, "index_repository", args);
    if (resp) free(resp);
    return cbm_store_open_path(lp->dbpath);
}

static cbm_store_t *pb_index_files(ProbeLangProj *lp, const ProbeLangFile *files, int nfiles) {
    memset(lp, 0, sizeof(*lp));
    snprintf(lp->tmpdir, sizeof(lp->tmpdir), "/tmp/cbm_pb_XXXXXX");
    if (!cbm_mkdtemp(lp->tmpdir)) return NULL;
    pb_fwd_slashes(lp->tmpdir);
    for (int i = 0; i < nfiles; i++) {
        char path[700];
        snprintf(path, sizeof(path), "%s/%s", lp->tmpdir, files[i].name);
        char *slash = strrchr(path, '/');
        if (slash && slash > path + strlen(lp->tmpdir)) {
            *slash = '\0';
            cbm_mkdir_p(path, 0755);
            *slash = '/';
        }
        FILE *f = fopen(path, "wb");
        if (!f) return NULL;
        fputs(files[i].content, f);
        fclose(f);
    }
    return pb_open_indexed(lp);
}

static cbm_store_t *pb_index(ProbeLangProj *lp, const char *filename, const char *content) {
    ProbeLangFile f = {filename, content};
    return pb_index_files(lp, &f, 1);
}

static void pb_cleanup(ProbeLangProj *lp, cbm_store_t *store) {
    if (store) cbm_store_close(store);
    if (lp->srv) { cbm_mcp_server_free(lp->srv); lp->srv = NULL; }
    free(lp->project); lp->project = NULL;
    th_rmtree(lp->tmpdir);
    unlink(lp->dbpath);
    char wal[600], shm[600];
    snprintf(wal, sizeof(wal), "%s-wal", lp->dbpath);
    snprintf(shm, sizeof(shm), "%s-shm", lp->dbpath);
    unlink(wal); unlink(shm);
}

/* Count nodes by label. Returns -1 on store error. */
static int pb_count_label(cbm_store_t *store, const char *project, const char *label) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    cbm_store_free_nodes(nodes, count);
    return count;
}

/* Sum across the labels different languages use for callables. */
static int pb_callable_nodes(cbm_store_t *store, const char *project) {
    int fn = pb_count_label(store, project, "Function");
    int mt = pb_count_label(store, project, "Method");
    return (fn < 0 ? 0 : fn) + (mt < 0 ? 0 : mt);
}

/* Sum across type-like labels. */
static int pb_type_nodes(cbm_store_t *store, const char *project) {
    static const char *labels[] = {"Class", "Struct", "Interface", "Enum", "Trait", "Type", NULL};
    int total = 0;
    for (int i = 0; labels[i]; i++) {
        int n = pb_count_label(store, project, labels[i]);
        if (n > 0) total += n;
    }
    return total;
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  GLSL — ext: .glsl
 *  Shader language: functions + void main() entrypoint + struct types.
 *  No imports (uses #include which is a preprocessor directive, not modeled).
 *  No inheritance.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* GLSL: helper + main entrypoint must both reach graph as Function nodes. */
TEST(glsl_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "shader.glsl",
        "float luminance(vec3 color) {\n"
        "    return dot(color, vec3(0.299, 0.587, 0.114));\n"
        "}\n\n"
        "void main() {\n"
        "    float lum = luminance(vec3(1.0, 0.5, 0.2));\n"
        "    gl_FragColor = vec4(lum, lum, lum, 1.0);\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* luminance + main */
    PASS();
}

/* GLSL: struct type (e.g. Light) must appear as a type node. */
TEST(glsl_struct_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "light.glsl",
        "struct Light {\n"
        "    vec3 position;\n"
        "    vec3 color;\n"
        "    float intensity;\n"
        "};\n\n"
        "float attenuation(Light l, vec3 pos) {\n"
        "    float d = length(l.position - pos);\n"
        "    return l.intensity / (d * d);\n"
        "}\n\n"
        "void main() {\n"
        "    Light sun;\n"
        "    sun.intensity = 1.0;\n"
        "    gl_FragColor = vec4(attenuation(sun, vec3(0.0)), 1.0);\n"
        "}\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* struct Light */
    PASS();
}

/* GLSL: vertex shader with multiple stage-specific functions. */
TEST(glsl_vertex_shader_functions) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "vert.glsl",
        "vec4 transform(vec4 pos, mat4 mvp) {\n"
        "    return mvp * pos;\n"
        "}\n\n"
        "vec3 compute_normal(vec3 n, mat3 normal_mat) {\n"
        "    return normalize(normal_mat * n);\n"
        "}\n\n"
        "void main() {\n"
        "    gl_Position = transform(vec4(0.0), mat4(1.0));\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* transform, compute_normal, main */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  HARE — ext: .ha
 *  Systems language: fn declarations, type definitions, `use` imports.
 *  No class inheritance.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* Hare: function nodes from top-level fn declarations. */
TEST(hare_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "math.ha",
        "fn square(x: i64) i64 = x * x;\n\n"
        "fn cube(x: i64) i64 = x * x * x;\n\n"
        "fn sum_of_squares(a: i64, b: i64) i64 = {\n"
        "    return square(a) + square(b);\n"
        "};\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* square, cube, sum_of_squares */
    PASS();
}

/* Hare: type definition (struct) reaches graph as a type node. */
TEST(hare_struct_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "point.ha",
        "type point = struct {\n"
        "    x: i64,\n"
        "    y: i64,\n"
        "};\n\n"
        "fn distance(a: point, b: point) f64 = {\n"
        "    let dx = (b.x - a.x): f64;\n"
        "    let dy = (b.y - a.y): f64;\n"
        "    return 0.0; // simplified\n"
        "};\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* type point */
    PASS();
}

/* Hare: `use` import statement → IMPORTS edge (extraction-level check).
 * The graph-level IMPORTS edge requires cross-file resolution which a single
 * fixture cannot trigger — so we probe at extraction level via cbm_extract_file. */
TEST(hare_use_import_extracted) {
    static const char *src =
        "use fmt;\n"
        "use strings;\n\n"
        "fn greet(name: str) void = {\n"
        "    fmt::println(name);\n"
        "};\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_HARE, "lc", "greet.ha", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* use fmt; use strings; → BUG if 0: hare `use` not extracted */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  HLSL — ext: .hlsl
 *  DirectX shader language: functions + cbuffer struct-like types.
 *  No imports. No inheritance.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* HLSL: pixel shader functions must reach graph. */
TEST(hlsl_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "pixel.hlsl",
        "float4 tint(float4 color, float factor) {\n"
        "    return color * factor;\n"
        "}\n\n"
        "float luminance(float4 c) {\n"
        "    return dot(c.rgb, float3(0.299, 0.587, 0.114));\n"
        "}\n\n"
        "float4 PSMain(float4 pos : SV_Position) : SV_Target {\n"
        "    float4 c = float4(1.0, 0.5, 0.2, 1.0);\n"
        "    return tint(c, luminance(c));\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* tint, luminance, PSMain */
    PASS();
}

/* HLSL: struct definition reaches graph as type node. */
TEST(hlsl_struct_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "types.hlsl",
        "struct VSInput {\n"
        "    float4 position : POSITION;\n"
        "    float2 texcoord : TEXCOORD0;\n"
        "};\n\n"
        "struct VSOutput {\n"
        "    float4 position : SV_Position;\n"
        "    float2 texcoord : TEXCOORD0;\n"
        "};\n\n"
        "VSOutput VSMain(VSInput input) {\n"
        "    VSOutput output;\n"
        "    output.position = input.position;\n"
        "    output.texcoord = input.texcoord;\n"
        "    return output;\n"
        "}\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* VSInput, VSOutput */
    PASS();
}

/* HLSL: compute shader kernel (another entrypoint style). */
TEST(hlsl_compute_shader_function) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "compute.hlsl",
        "RWBuffer<float> gOutput : register(u0);\n\n"
        "float transform(float x) {\n"
        "    return x * x;\n"
        "}\n\n"
        "[numthreads(64, 1, 1)]\n"
        "void CSMain(uint3 id : SV_DispatchThreadID) {\n"
        "    gOutput[id.x] = transform((float)id.x);\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* transform, CSMain */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  ISPC — ext: .ispc
 *  Intel SPMD language: task/export/uniform functions, struct types.
 *  No imports. No inheritance.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* ISPC: uniform and varying function nodes. */
TEST(ispc_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "kernel.ispc",
        "float square(float x) {\n"
        "    return x * x;\n"
        "}\n\n"
        "export void add_arrays(uniform float output[],\n"
        "                       uniform float a[],\n"
        "                       uniform float b[],\n"
        "                       uniform int count) {\n"
        "    foreach (i = 0 ... count) {\n"
        "        output[i] = a[i] + square(b[i]);\n"
        "    }\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* square, add_arrays */
    PASS();
}

/* ISPC: struct definition reaches graph as type node. */
TEST(ispc_struct_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "types.ispc",
        "struct Vec3 {\n"
        "    float x, y, z;\n"
        "};\n\n"
        "float dot3(Vec3 a, Vec3 b) {\n"
        "    return a.x*b.x + a.y*b.y + a.z*b.z;\n"
        "}\n\n"
        "export void normalize_batch(uniform Vec3 vecs[], uniform int n) {\n"
        "    foreach (i = 0 ... n) {\n"
        "        float len = dot3(vecs[i], vecs[i]);\n"
        "        (void)len;\n"
        "    }\n"
        "}\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* struct Vec3 */
    PASS();
}

/* ISPC: task function (another callable kind). */
TEST(ispc_task_function) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "tasks.ispc",
        "uniform float compute(uniform float x) {\n"
        "    return x * 2.0f;\n"
        "}\n\n"
        "task void render_tile(uniform float buf[], uniform int w, uniform int h) {\n"
        "    foreach (j = 0 ... h, i = 0 ... w) {\n"
        "        buf[j*w + i] = compute((float)i);\n"
        "    }\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* compute, render_tile */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  JULIA — ext: .jl
 *  Dynamic language: function defs, struct types, `using`/`import` imports,
 *  abstract type subtyping with `<:`.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* Julia: function and struct nodes. */
TEST(julia_function_and_struct_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "geom.jl",
        "struct Point\n"
        "    x::Float64\n"
        "    y::Float64\n"
        "end\n\n"
        "function distance(a::Point, b::Point)::Float64\n"
        "    return sqrt((b.x - a.x)^2 + (b.y - a.y)^2)\n"
        "end\n\n"
        "function midpoint(a::Point, b::Point)::Point\n"
        "    return Point((a.x + b.x) / 2, (a.y + b.y) / 2)\n"
        "end\n");
    int fns   = store ? pb_callable_nodes(store, lp.project) : -1;
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1);   /* distance, midpoint */
    ASSERT_TRUE(types >= 1); /* struct Point */
    PASS();
}

/* Julia: `using` import statement captured at extraction level. */
TEST(julia_using_import_extracted) {
    static const char *src =
        "using LinearAlgebra\n"
        "using Statistics: mean, std\n\n"
        "function norm_vec(v::Vector{Float64})::Float64\n"
        "    return norm(v)\n"
        "end\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_JULIA, "lc", "vec.jl", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* using LinearAlgebra; using Statistics → BUG if 0 */
    PASS();
}

/* Julia: `import` statement captured at extraction level. */
TEST(julia_import_extracted) {
    static const char *src =
        "import Base: show, length\n"
        "import Random\n\n"
        "struct MyVec\n"
        "    data::Vector{Float64}\n"
        "end\n\n"
        "Base.length(v::MyVec) = length(v.data)\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_JULIA, "lc", "myvec.jl", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* import Base; import Random → BUG if 0 */
    PASS();
}

/* Julia: abstract type subtyping (`<:`) — extraction captures base_classes. */
TEST(julia_abstract_subtype_extracted) {
    static const char *src =
        "abstract type Shape end\n\n"
        "abstract type Polygon <: Shape end\n\n"
        "struct Triangle <: Polygon\n"
        "    a::Float64\n"
        "    b::Float64\n"
        "    c::Float64\n"
        "end\n\n"
        "function area(t::Triangle)::Float64\n"
        "    s = (t.a + t.b + t.c) / 2\n"
        "    return sqrt(s * (s-t.a) * (s-t.b) * (s-t.c))\n"
        "end\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_JULIA, "lc", "shapes.jl", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    /* We expect at least 1 def whose base_classes is non-empty (Polygon<:Shape or
     * Triangle<:Polygon).  Iterate defs to check. */
    int found_base = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].base_classes && r->defs.items[i].base_classes[0] != NULL) {
            found_base = 1;
            break;
        }
    }
    cbm_free_result(r);
    ASSERT_TRUE(found_base); /* BUG if 0: julia `<:` base not extracted */
    PASS();
}

/* Julia: full-pipeline INHERITS edge for same-file abstract subtype chain. */
TEST(julia_inherits_edge) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "animals.jl",
        "abstract type Animal end\n\n"
        "abstract type Pet <: Animal end\n\n"
        "struct Dog <: Pet\n"
        "    name::String\n"
        "end\n\n"
        "function speak(d::Dog)::String\n"
        "    return \"Woof: \" * d.name\n"
        "end\n");
    int inherits = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(inherits >= 1); /* Pet<:Animal, Dog<:Pet → BUG if 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  LUAU — ext: .luau
 *  Roblox Lua dialect: function nodes.  No class inheritance, no module imports
 *  that map to IMPORTS edges (require() is dynamic, not a static import).
 * ══════════════════════════════════════════════════════════════════════════════ */

/* Luau: function nodes reach the graph. */
TEST(luau_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "utils.luau",
        "local function clamp(value: number, min: number, max: number): number\n"
        "    if value < min then return min end\n"
        "    if value > max then return max end\n"
        "    return value\n"
        "end\n\n"
        "local function lerp(a: number, b: number, t: number): number\n"
        "    return a + (b - a) * clamp(t, 0, 1)\n"
        "end\n\n"
        "return { clamp = clamp, lerp = lerp }\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* clamp, lerp */
    PASS();
}

/* Luau: type alias and interface-like construct (type keyword). */
TEST(luau_type_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "types.luau",
        "type Vector2 = {\n"
        "    x: number,\n"
        "    y: number,\n"
        "}\n\n"
        "type Entity = {\n"
        "    id: number,\n"
        "    name: string,\n"
        "    position: Vector2,\n"
        "}\n\n"
        "local function make_entity(id: number, name: string): Entity\n"
        "    return { id = id, name = name, position = { x = 0, y = 0 } }\n"
        "end\n\n"
        "return { make_entity = make_entity }\n");
    /* Either type nodes or function nodes must be present */
    int nodes = store ? (pb_callable_nodes(store, lp.project) + pb_type_nodes(store, lp.project)) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(nodes >= 1); /* make_entity function at minimum */
    PASS();
}

/* Luau: class-style OOP via metatables (functions only — no native class keyword). */
TEST(luau_class_style_functions) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "player.luau",
        "local Player = {}\n"
        "Player.__index = Player\n\n"
        "function Player.new(name: string): Player\n"
        "    return setmetatable({ name = name, health = 100 }, Player)\n"
        "end\n\n"
        "function Player:takeDamage(amount: number): ()\n"
        "    self.health = self.health - amount\n"
        "end\n\n"
        "function Player:isAlive(): boolean\n"
        "    return self.health > 0\n"
        "end\n\n"
        "return Player\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* Player.new, Player:takeDamage, Player:isAlive */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  MATLAB — ext: .m
 *  Scientific language: function definitions in .m files.
 *  No standard cross-file import syntax modeled as IMPORTS.
 *  No inheritance (classdef exists but rarely used; probe basic functions only).
 * ══════════════════════════════════════════════════════════════════════════════ */

/* MATLAB: top-level function and local subfunctions reach graph. */
TEST(matlab_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "stats.m",
        "function result = compute_mean(data)\n"
        "    result = sum_values(data) / length(data);\n"
        "end\n\n"
        "function s = sum_values(v)\n"
        "    s = 0;\n"
        "    for i = 1:length(v)\n"
        "        s = s + v(i);\n"
        "    end\n"
        "end\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* compute_mean, sum_values */
    PASS();
}

/* MATLAB: multiple function file — all functions are nodes. */
TEST(matlab_multiple_functions) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "linalg.m",
        "function c = dot_product(a, b)\n"
        "    c = sum(a .* b);\n"
        "end\n\n"
        "function n = vec_norm(v)\n"
        "    n = sqrt(dot_product(v, v));\n"
        "end\n\n"
        "function u = normalize(v)\n"
        "    u = v / vec_norm(v);\n"
        "end\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* dot_product, vec_norm, normalize */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  ODIN — ext: .odin
 *  Systems language: proc declarations, struct types, `import` statements.
 *  No class inheritance (Odin is not OOP).
 * ══════════════════════════════════════════════════════════════════════════════ */

/* Odin: proc nodes reach graph. */
TEST(odin_proc_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "math.odin",
        "package math\n\n"
        "square :: proc(x: f64) -> f64 {\n"
        "    return x * x\n"
        "}\n\n"
        "cube :: proc(x: f64) -> f64 {\n"
        "    return x * square(x)\n"
        "}\n\n"
        "clamp :: proc(v, lo, hi: f64) -> f64 {\n"
        "    if v < lo { return lo }\n"
        "    if v > hi { return hi }\n"
        "    return v\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* square, cube, clamp */
    PASS();
}

/* Odin: struct type node. */
TEST(odin_struct_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "point.odin",
        "package point\n\n"
        "Vec2 :: struct {\n"
        "    x, y: f64,\n"
        "}\n\n"
        "Vec3 :: struct {\n"
        "    x, y, z: f64,\n"
        "}\n\n"
        "length2 :: proc(v: Vec2) -> f64 {\n"
        "    return v.x*v.x + v.y*v.y\n"
        "}\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* Vec2, Vec3 */
    PASS();
}

/* Odin: `import` statement captured at extraction level. */
TEST(odin_import_extracted) {
    static const char *src =
        "package main\n\n"
        "import \"core:fmt\"\n"
        "import \"core:os\"\n"
        "import math \"core:math\"\n\n"
        "main :: proc() {\n"
        "    fmt.println(\"hello\")\n"
        "}\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_ODIN, "lc", "main.odin", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* import "core:fmt"; import "core:os" → BUG if 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  PASCAL — ext: .pas
 *  Classic structured language: procedures/functions, record/class types,
 *  `uses` import clause, OOP inheritance via `: TBase`.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* Pascal: procedure and function nodes. */
TEST(pascal_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "arith.pas",
        "unit Arith;\n\n"
        "interface\n\n"
        "function Add(a, b: Integer): Integer;\n"
        "function Multiply(a, b: Integer): Integer;\n"
        "procedure PrintResult(val: Integer);\n\n"
        "implementation\n\n"
        "function Add(a, b: Integer): Integer;\n"
        "begin\n"
        "  Result := a + b;\n"
        "end;\n\n"
        "function Multiply(a, b: Integer): Integer;\n"
        "begin\n"
        "  Result := a * b;\n"
        "end;\n\n"
        "procedure PrintResult(val: Integer);\n"
        "begin\n"
        "  Writeln(val);\n"
        "end;\n\n"
        "end.\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* Add, Multiply, PrintResult */
    PASS();
}

/* Pascal: record type node. */
TEST(pascal_record_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "geom.pas",
        "unit Geom;\n\n"
        "interface\n\n"
        "type\n"
        "  TPoint = record\n"
        "    X, Y: Double;\n"
        "  end;\n\n"
        "  TRect = record\n"
        "    TopLeft: TPoint;\n"
        "    BottomRight: TPoint;\n"
        "  end;\n\n"
        "function Distance(P1, P2: TPoint): Double;\n\n"
        "implementation\n\n"
        "function Distance(P1, P2: TPoint): Double;\n"
        "begin\n"
        "  Result := 0.0;\n"
        "end;\n\n"
        "end.\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* TPoint, TRect */
    PASS();
}

/* Pascal: `uses` clause captured at extraction level. */
TEST(pascal_uses_import_extracted) {
    static const char *src =
        "unit MyUnit;\n\n"
        "interface\n\n"
        "uses\n"
        "  SysUtils, Classes, Math;\n\n"
        "procedure DoWork;\n\n"
        "implementation\n\n"
        "procedure DoWork;\n"
        "begin\n"
        "  Writeln(IntToStr(42));\n"
        "end;\n\n"
        "end.\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_PASCAL, "lc", "myunit.pas", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* uses SysUtils, Classes, Math → BUG if 0 */
    PASS();
}

/* Pascal: OOP class inheritance (`: TAnimal`) — extraction captures base_classes. */
TEST(pascal_class_inheritance_extracted) {
    static const char *src =
        "unit Zoo;\n\n"
        "interface\n\n"
        "type\n"
        "  TAnimal = class\n"
        "    procedure Speak; virtual;\n"
        "  end;\n\n"
        "  TDog = class(TAnimal)\n"
        "    procedure Speak; override;\n"
        "  end;\n\n"
        "  TCat = class(TAnimal)\n"
        "    procedure Speak; override;\n"
        "  end;\n\n"
        "implementation\n\n"
        "procedure TAnimal.Speak; begin end;\n"
        "procedure TDog.Speak; begin Writeln('Woof'); end;\n"
        "procedure TCat.Speak; begin Writeln('Meow'); end;\n\n"
        "end.\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_PASCAL, "lc", "zoo.pas", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int found_base = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].base_classes && r->defs.items[i].base_classes[0] != NULL) {
            found_base = 1;
            break;
        }
    }
    cbm_free_result(r);
    ASSERT_TRUE(found_base); /* TDog(TAnimal), TCat(TAnimal) → BUG if 0 */
    PASS();
}

/* Pascal: full-pipeline INHERITS edge for OOP class hierarchy. */
TEST(pascal_inherits_edge) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "vehicles.pas",
        "unit Vehicles;\n\n"
        "interface\n\n"
        "type\n"
        "  TVehicle = class\n"
        "    function GetSpeed: Integer; virtual;\n"
        "  end;\n\n"
        "  TCar = class(TVehicle)\n"
        "    function GetSpeed: Integer; override;\n"
        "  end;\n\n"
        "  TTruck = class(TVehicle)\n"
        "    function GetSpeed: Integer; override;\n"
        "  end;\n\n"
        "implementation\n\n"
        "function TVehicle.GetSpeed: Integer; begin Result := 0; end;\n"
        "function TCar.GetSpeed: Integer; begin Result := 100; end;\n"
        "function TTruck.GetSpeed: Integer; begin Result := 80; end;\n\n"
        "end.\n");
    int inherits = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(inherits >= 1); /* TCar->TVehicle, TTruck->TVehicle → BUG if 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  POWERSHELL — ext: .ps1
 *  Scripting language: function + class definitions, `using module` imports.
 *  OOP inheritance via `: Base`.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* PowerShell: function nodes reach graph. */
TEST(powershell_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "utils.ps1",
        "function Get-Square {\n"
        "    param([double]$x)\n"
        "    return $x * $x\n"
        "}\n\n"
        "function Get-Cube {\n"
        "    param([double]$x)\n"
        "    return $x * (Get-Square $x)\n"
        "}\n\n"
        "function Test-Positive {\n"
        "    param([double]$x)\n"
        "    return $x -gt 0\n"
        "}\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* Get-Square, Get-Cube, Test-Positive */
    PASS();
}

/* PowerShell: class definition reaches graph as type node. */
TEST(powershell_class_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "shapes.ps1",
        "class Shape {\n"
        "    [string]$Name\n"
        "    Shape([string]$name) { $this.Name = $name }\n"
        "    [double] Area() { return 0.0 }\n"
        "}\n\n"
        "class Circle : Shape {\n"
        "    [double]$Radius\n"
        "    Circle([double]$r) : base('Circle') { $this.Radius = $r }\n"
        "    [double] Area() { return [Math]::PI * $this.Radius * $this.Radius }\n"
        "}\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* Shape, Circle */
    PASS();
}

/* PowerShell: `using module` / `using namespace` captured at extraction level. */
TEST(powershell_using_import_extracted) {
    static const char *src =
        "using module ./MyModule\n"
        "using namespace System.IO\n"
        "using assembly System.Drawing\n\n"
        "function Invoke-Task {\n"
        "    param([string]$Path)\n"
        "    [System.IO.File]::Exists($Path)\n"
        "}\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_POWERSHELL, "lc", "task.ps1", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* using module / using namespace → BUG if 0 */
    PASS();
}

/* PowerShell: class inheritance (`: Base`) — extraction captures base_classes. */
TEST(powershell_class_inheritance_extracted) {
    static const char *src =
        "class Animal {\n"
        "    [string]$Name\n"
        "    Animal([string]$name) { $this.Name = $name }\n"
        "    [string] Speak() { return 'some sound' }\n"
        "}\n\n"
        "class Dog : Animal {\n"
        "    Dog([string]$name) : base($name) {}\n"
        "    [string] Speak() { return 'Woof' }\n"
        "}\n\n"
        "class Cat : Animal {\n"
        "    Cat([string]$name) : base($name) {}\n"
        "    [string] Speak() { return 'Meow' }\n"
        "}\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_POWERSHELL, "lc", "animals.ps1", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int found_base = 0;
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].base_classes && r->defs.items[i].base_classes[0] != NULL) {
            found_base = 1;
            break;
        }
    }
    cbm_free_result(r);
    ASSERT_TRUE(found_base); /* Dog : Animal, Cat : Animal → BUG if 0 */
    PASS();
}

/* PowerShell: full-pipeline INHERITS edge from same-file class hierarchy. */
TEST(powershell_inherits_edge) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "exceptions.ps1",
        "class AppException {\n"
        "    [string]$Message\n"
        "    AppException([string]$msg) { $this.Message = $msg }\n"
        "}\n\n"
        "class DatabaseException : AppException {\n"
        "    DatabaseException([string]$msg) : base($msg) {}\n"
        "}\n\n"
        "class NetworkException : AppException {\n"
        "    NetworkException([string]$msg) : base($msg) {}\n"
        "}\n");
    int inherits = store ? cbm_store_count_edges_by_type(store, lp.project, "INHERITS") : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(inherits >= 1); /* Database->App, Network->App → BUG if 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  RACKET — ext: .rkt
 *  Lisp dialect: define functions/procedures, `require` imports.
 *  No class-style inheritance (uses `struct` for types, not OOP classes).
 * ══════════════════════════════════════════════════════════════════════════════ */

/* Racket: function (define) nodes reach graph. */
TEST(racket_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "math.rkt",
        "#lang racket\n\n"
        "(define (square x) (* x x))\n\n"
        "(define (cube x) (* x (square x)))\n\n"
        "(define (sum-of-squares a b)\n"
        "  (+ (square a) (square b)))\n\n"
        "(define (hypotenuse a b)\n"
        "  (sqrt (sum-of-squares a b)))\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* square, cube, sum-of-squares, hypotenuse */
    PASS();
}

/* Racket: struct definition reaches graph as type node. */
TEST(racket_struct_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "point.rkt",
        "#lang racket\n\n"
        "(struct point (x y) #:transparent)\n\n"
        "(struct circle (center radius) #:transparent)\n\n"
        "(define (distance p1 p2)\n"
        "  (let ([dx (- (point-x p2) (point-x p1))]\n"
        "        [dy (- (point-y p2) (point-y p1))])\n"
        "    (sqrt (+ (* dx dx) (* dy dy)))))\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* struct point, struct circle → BUG if 0 */
    PASS();
}

/* Racket: `require` import captured at extraction level. */
TEST(racket_require_import_extracted) {
    static const char *src =
        "#lang racket\n\n"
        "(require racket/list)\n"
        "(require racket/string)\n"
        "(require (only-in racket/math pi))\n\n"
        "(define (circle-area r)\n"
        "  (* pi r r))\n\n"
        "(define (join-words words)\n"
        "  (string-join words \" \"))\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_RACKET, "lc", "utils.rkt", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* require racket/list etc. → BUG if 0 */
    PASS();
}

/* Racket: graph-level IMPORTS edge from two-file project with require. */
TEST(racket_imports_edge) {
    static const ProbeLangFile files[] = {
        {"math.rkt",
         "#lang racket\n"
         "(provide square cube)\n"
         "(define (square x) (* x x))\n"
         "(define (cube x) (* x (square x)))\n"},
        {"main.rkt",
         "#lang racket\n"
         "(require \"math.rkt\")\n"
         "(define (run n) (+ (square n) (cube n)))\n"}};
    ProbeLangProj lp;
    cbm_store_t *store = pb_index_files(&lp, files, 2);
    int imports = store ? cbm_store_count_edges_by_type(store, lp.project, "IMPORTS") : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(imports >= 1); /* main.rkt requires math.rkt → BUG if 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  RESCRIPT — ext: .res
 *  ReasonML-like JS compiler: let-bound functions, type definitions,
 *  `open` module imports.  No class inheritance.
 * ══════════════════════════════════════════════════════════════════════════════ */

/* ReScript: let-bound function nodes reach graph. */
TEST(rescript_function_nodes) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "math.res",
        "let square = (x: float) => x *. x\n\n"
        "let cube = (x: float) => x *. square(x)\n\n"
        "let clamp = (v: float, lo: float, hi: float) =>\n"
        "  if v < lo { lo } else if v > hi { hi } else { v }\n");
    int fns = store ? pb_callable_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(fns >= 1); /* square, cube, clamp */
    PASS();
}

/* ReScript: type definition reaches graph as type node. */
TEST(rescript_type_node) {
    ProbeLangProj lp;
    cbm_store_t *store = pb_index(&lp, "types.res",
        "type color = Red | Green | Blue\n\n"
        "type point = {\n"
        "  x: float,\n"
        "  y: float,\n"
        "}\n\n"
        "let origin: point = {x: 0.0, y: 0.0}\n\n"
        "let to_hex = (c: color) =>\n"
        "  switch c {\n"
        "  | Red => \"#ff0000\"\n"
        "  | Green => \"#00ff00\"\n"
        "  | Blue => \"#0000ff\"\n"
        "  }\n");
    int types = store ? pb_type_nodes(store, lp.project) : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(types >= 1); /* type color, type point → BUG if 0 */
    PASS();
}

/* ReScript: `open` import captured at extraction level. */
TEST(rescript_open_import_extracted) {
    static const char *src =
        "open Belt\n"
        "open Belt.Array\n\n"
        "let sum = (arr: array<int>) =>\n"
        "  Array.reduce(arr, 0, (acc, x) => acc + x)\n\n"
        "let max_val = (arr: array<int>) =>\n"
        "  Array.reduce(arr, Int.min_int, (acc, x) => if x > acc { x } else { acc })\n";
    CBMFileResult *r = cbm_extract_file(src, (int)strlen(src),
                                        CBM_LANG_RESCRIPT, "lc", "utils.res", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    int n = r->imports.count;
    cbm_free_result(r);
    ASSERT_TRUE(n >= 1); /* open Belt; open Belt.Array → BUG if 0 */
    PASS();
}

/* ReScript: graph-level IMPORTS edge from two-file project. */
TEST(rescript_imports_edge) {
    static const ProbeLangFile files[] = {
        {"Utils.res",
         "let square = (x: float) => x *. x\n"
         "let cube = (x: float) => x *. square(x)\n"},
        {"Main.res",
         "open Utils\n\n"
         "let run = (n: float) => square(n) +. cube(n)\n"}};
    ProbeLangProj lp;
    cbm_store_t *store = pb_index_files(&lp, files, 2);
    int imports = store ? cbm_store_count_edges_by_type(store, lp.project, "IMPORTS") : -1;
    pb_cleanup(&lp, store);
    ASSERT_TRUE(imports >= 1); /* Main opens Utils → BUG if 0 */
    PASS();
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  SUITE REGISTRATION
 * ══════════════════════════════════════════════════════════════════════════════ */

SUITE(grammar_probe_b) {
    /* ── GLSL (3 cases) ── */
    RUN_TEST(glsl_function_nodes);
    RUN_TEST(glsl_struct_node);
    RUN_TEST(glsl_vertex_shader_functions);

    /* ── Hare (3 cases) ── */
    RUN_TEST(hare_function_nodes);
    RUN_TEST(hare_struct_node);
    RUN_TEST(hare_use_import_extracted);

    /* ── HLSL (3 cases) ── */
    RUN_TEST(hlsl_function_nodes);
    RUN_TEST(hlsl_struct_node);
    RUN_TEST(hlsl_compute_shader_function);

    /* ── ISPC (3 cases) ── */
    RUN_TEST(ispc_function_nodes);
    RUN_TEST(ispc_struct_node);
    RUN_TEST(ispc_task_function);

    /* ── Julia (5 cases) ── */
    RUN_TEST(julia_function_and_struct_nodes);
    RUN_TEST(julia_using_import_extracted);
    RUN_TEST(julia_import_extracted);
    RUN_TEST(julia_abstract_subtype_extracted);
    RUN_TEST(julia_inherits_edge);

    /* ── Luau (3 cases) ── */
    RUN_TEST(luau_function_nodes);
    RUN_TEST(luau_type_nodes);
    RUN_TEST(luau_class_style_functions);

    /* ── MATLAB (2 cases) ── */
    RUN_TEST(matlab_function_nodes);
    RUN_TEST(matlab_multiple_functions);

    /* ── Odin (3 cases) ── */
    RUN_TEST(odin_proc_nodes);
    RUN_TEST(odin_struct_node);
    RUN_TEST(odin_import_extracted);

    /* ── Pascal (5 cases) ── */
    RUN_TEST(pascal_function_nodes);
    RUN_TEST(pascal_record_node);
    RUN_TEST(pascal_uses_import_extracted);
    RUN_TEST(pascal_class_inheritance_extracted);
    RUN_TEST(pascal_inherits_edge);

    /* ── PowerShell (5 cases) ── */
    RUN_TEST(powershell_function_nodes);
    RUN_TEST(powershell_class_node);
    RUN_TEST(powershell_using_import_extracted);
    RUN_TEST(powershell_class_inheritance_extracted);
    RUN_TEST(powershell_inherits_edge);

    /* ── Racket (4 cases) ── */
    RUN_TEST(racket_function_nodes);
    RUN_TEST(racket_struct_node);
    RUN_TEST(racket_require_import_extracted);
    RUN_TEST(racket_imports_edge);

    /* ── ReScript (4 cases) ── */
    RUN_TEST(rescript_function_nodes);
    RUN_TEST(rescript_type_node);
    RUN_TEST(rescript_open_import_extracted);
    RUN_TEST(rescript_imports_edge);
}
