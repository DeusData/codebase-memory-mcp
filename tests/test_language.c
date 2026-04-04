/*
 * test_language.c — Tests for language detection (filename + extension).
 *
 * RED phase: These tests define the expected behavior for all 64 languages.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "discover/discover.h"

/* ── Extension-based detection ─────────────────────────────────── */

TEST(lang_ext_go) {
    ASSERT_EQ(cbm_language_for_extension(".go"), CBM_LANG_GO);
    PASS();
}
TEST(lang_ext_python) {
    ASSERT_EQ(cbm_language_for_extension(".py"), CBM_LANG_PYTHON);
    PASS();
}
TEST(lang_ext_javascript) {
    ASSERT_EQ(cbm_language_for_extension(".js"), CBM_LANG_JAVASCRIPT);
    PASS();
}
TEST(lang_ext_jsx) {
    ASSERT_EQ(cbm_language_for_extension(".jsx"), CBM_LANG_JAVASCRIPT);
    PASS();
}
TEST(lang_ext_typescript) {
    ASSERT_EQ(cbm_language_for_extension(".ts"), CBM_LANG_TYPESCRIPT);
    PASS();
}
TEST(lang_ext_tsx) {
    ASSERT_EQ(cbm_language_for_extension(".tsx"), CBM_LANG_TSX);
    PASS();
}
TEST(lang_ext_rust) {
    ASSERT_EQ(cbm_language_for_extension(".rs"), CBM_LANG_RUST);
    PASS();
}
TEST(lang_ext_java) {
    ASSERT_EQ(cbm_language_for_extension(".java"), CBM_LANG_JAVA);
    PASS();
}
TEST(lang_ext_cpp) {
    ASSERT_EQ(cbm_language_for_extension(".cpp"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_hpp) {
    ASSERT_EQ(cbm_language_for_extension(".hpp"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_cc) {
    ASSERT_EQ(cbm_language_for_extension(".cc"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_cxx) {
    ASSERT_EQ(cbm_language_for_extension(".cxx"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_hxx) {
    ASSERT_EQ(cbm_language_for_extension(".hxx"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_hh) {
    ASSERT_EQ(cbm_language_for_extension(".hh"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_h) {
    ASSERT_EQ(cbm_language_for_extension(".h"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_ixx) {
    ASSERT_EQ(cbm_language_for_extension(".ixx"), CBM_LANG_CPP);
    PASS();
}
TEST(lang_ext_csharp) {
    ASSERT_EQ(cbm_language_for_extension(".cs"), CBM_LANG_CSHARP);
    PASS();
}
TEST(lang_ext_php) {
    ASSERT_EQ(cbm_language_for_extension(".php"), CBM_LANG_PHP);
    PASS();
}
TEST(lang_ext_lua) {
    ASSERT_EQ(cbm_language_for_extension(".lua"), CBM_LANG_LUA);
    PASS();
}
TEST(lang_ext_scala) {
    ASSERT_EQ(cbm_language_for_extension(".scala"), CBM_LANG_SCALA);
    PASS();
}
TEST(lang_ext_sc) {
    ASSERT_EQ(cbm_language_for_extension(".sc"), CBM_LANG_SCALA);
    PASS();
}
TEST(lang_ext_kotlin) {
    ASSERT_EQ(cbm_language_for_extension(".kt"), CBM_LANG_KOTLIN);
    PASS();
}
TEST(lang_ext_kts) {
    ASSERT_EQ(cbm_language_for_extension(".kts"), CBM_LANG_KOTLIN);
    PASS();
}
TEST(lang_ext_ruby) {
    ASSERT_EQ(cbm_language_for_extension(".rb"), CBM_LANG_RUBY);
    PASS();
}
TEST(lang_ext_rake) {
    ASSERT_EQ(cbm_language_for_extension(".rake"), CBM_LANG_RUBY);
    PASS();
}
TEST(lang_ext_gemspec) {
    ASSERT_EQ(cbm_language_for_extension(".gemspec"), CBM_LANG_RUBY);
    PASS();
}
TEST(lang_ext_c) {
    ASSERT_EQ(cbm_language_for_extension(".c"), CBM_LANG_C);
    PASS();
}
TEST(lang_ext_bash) {
    ASSERT_EQ(cbm_language_for_extension(".sh"), CBM_LANG_BASH);
    PASS();
}
TEST(lang_ext_bash2) {
    ASSERT_EQ(cbm_language_for_extension(".bash"), CBM_LANG_BASH);
    PASS();
}
TEST(lang_ext_zig) {
    ASSERT_EQ(cbm_language_for_extension(".zig"), CBM_LANG_ZIG);
    PASS();
}
TEST(lang_ext_elixir) {
    ASSERT_EQ(cbm_language_for_extension(".ex"), CBM_LANG_ELIXIR);
    PASS();
}
TEST(lang_ext_exs) {
    ASSERT_EQ(cbm_language_for_extension(".exs"), CBM_LANG_ELIXIR);
    PASS();
}
TEST(lang_ext_haskell) {
    ASSERT_EQ(cbm_language_for_extension(".hs"), CBM_LANG_HASKELL);
    PASS();
}
TEST(lang_ext_ocaml) {
    ASSERT_EQ(cbm_language_for_extension(".ml"), CBM_LANG_OCAML);
    PASS();
}
TEST(lang_ext_mli) {
    ASSERT_EQ(cbm_language_for_extension(".mli"), CBM_LANG_OCAML);
    PASS();
}
TEST(lang_ext_swift) {
    ASSERT_EQ(cbm_language_for_extension(".swift"), CBM_LANG_SWIFT);
    PASS();
}
TEST(lang_ext_dart) {
    ASSERT_EQ(cbm_language_for_extension(".dart"), CBM_LANG_DART);
    PASS();
}
TEST(lang_ext_perl) {
    ASSERT_EQ(cbm_language_for_extension(".pl"), CBM_LANG_PERL);
    PASS();
}
TEST(lang_ext_pm) {
    ASSERT_EQ(cbm_language_for_extension(".pm"), CBM_LANG_PERL);
    PASS();
}
TEST(lang_ext_groovy) {
    ASSERT_EQ(cbm_language_for_extension(".groovy"), CBM_LANG_GROOVY);
    PASS();
}
TEST(lang_ext_gradle) {
    ASSERT_EQ(cbm_language_for_extension(".gradle"), CBM_LANG_GROOVY);
    PASS();
}
TEST(lang_ext_erlang) {
    ASSERT_EQ(cbm_language_for_extension(".erl"), CBM_LANG_ERLANG);
    PASS();
}
TEST(lang_ext_r) {
    ASSERT_EQ(cbm_language_for_extension(".r"), CBM_LANG_R);
    PASS();
}
TEST(lang_ext_R) {
    ASSERT_EQ(cbm_language_for_extension(".R"), CBM_LANG_R);
    PASS();
}

/* Tier 2 programming */
TEST(lang_ext_clojure) {
    ASSERT_EQ(cbm_language_for_extension(".clj"), CBM_LANG_CLOJURE);
    PASS();
}
TEST(lang_ext_cljs) {
    ASSERT_EQ(cbm_language_for_extension(".cljs"), CBM_LANG_CLOJURE);
    PASS();
}
TEST(lang_ext_cljc) {
    ASSERT_EQ(cbm_language_for_extension(".cljc"), CBM_LANG_CLOJURE);
    PASS();
}
TEST(lang_ext_fsharp) {
    ASSERT_EQ(cbm_language_for_extension(".fs"), CBM_LANG_FSHARP);
    PASS();
}
TEST(lang_ext_fsi) {
    ASSERT_EQ(cbm_language_for_extension(".fsi"), CBM_LANG_FSHARP);
    PASS();
}
TEST(lang_ext_fsx) {
    ASSERT_EQ(cbm_language_for_extension(".fsx"), CBM_LANG_FSHARP);
    PASS();
}
TEST(lang_ext_julia) {
    ASSERT_EQ(cbm_language_for_extension(".jl"), CBM_LANG_JULIA);
    PASS();
}
TEST(lang_ext_vim) {
    ASSERT_EQ(cbm_language_for_extension(".vim"), CBM_LANG_VIMSCRIPT);
    PASS();
}
TEST(lang_ext_nix) {
    ASSERT_EQ(cbm_language_for_extension(".nix"), CBM_LANG_NIX);
    PASS();
}
TEST(lang_ext_commonlisp) {
    ASSERT_EQ(cbm_language_for_extension(".lisp"), CBM_LANG_COMMONLISP);
    PASS();
}
TEST(lang_ext_lsp) {
    ASSERT_EQ(cbm_language_for_extension(".lsp"), CBM_LANG_COMMONLISP);
    PASS();
}
TEST(lang_ext_cl) {
    ASSERT_EQ(cbm_language_for_extension(".cl"), CBM_LANG_COMMONLISP);
    PASS();
}
TEST(lang_ext_elm) {
    ASSERT_EQ(cbm_language_for_extension(".elm"), CBM_LANG_ELM);
    PASS();
}
TEST(lang_ext_fortran) {
    ASSERT_EQ(cbm_language_for_extension(".f90"), CBM_LANG_FORTRAN);
    PASS();
}
TEST(lang_ext_f95) {
    ASSERT_EQ(cbm_language_for_extension(".f95"), CBM_LANG_FORTRAN);
    PASS();
}
TEST(lang_ext_f03) {
    ASSERT_EQ(cbm_language_for_extension(".f03"), CBM_LANG_FORTRAN);
    PASS();
}
TEST(lang_ext_f08) {
    ASSERT_EQ(cbm_language_for_extension(".f08"), CBM_LANG_FORTRAN);
    PASS();
}
TEST(lang_ext_cuda) {
    ASSERT_EQ(cbm_language_for_extension(".cu"), CBM_LANG_CUDA);
    PASS();
}
TEST(lang_ext_cuh) {
    ASSERT_EQ(cbm_language_for_extension(".cuh"), CBM_LANG_CUDA);
    PASS();
}
TEST(lang_ext_cobol) {
    ASSERT_EQ(cbm_language_for_extension(".cob"), CBM_LANG_COBOL);
    PASS();
}
TEST(lang_ext_cbl) {
    ASSERT_EQ(cbm_language_for_extension(".cbl"), CBM_LANG_COBOL);
    PASS();
}
TEST(lang_ext_verilog) {
    ASSERT_EQ(cbm_language_for_extension(".v"), CBM_LANG_VERILOG);
    PASS();
}
TEST(lang_ext_sv) {
    ASSERT_EQ(cbm_language_for_extension(".sv"), CBM_LANG_VERILOG);
    PASS();
}
TEST(lang_ext_emacslisp) {
    ASSERT_EQ(cbm_language_for_extension(".el"), CBM_LANG_EMACSLISP);
    PASS();
}

/* Scientific/math */
TEST(lang_ext_matlab) {
    ASSERT_EQ(cbm_language_for_extension(".matlab"), CBM_LANG_MATLAB);
    PASS();
}
TEST(lang_ext_mlx) {
    ASSERT_EQ(cbm_language_for_extension(".mlx"), CBM_LANG_MATLAB);
    PASS();
}
TEST(lang_ext_lean) {
    ASSERT_EQ(cbm_language_for_extension(".lean"), CBM_LANG_LEAN);
    PASS();
}
TEST(lang_ext_form) {
    ASSERT_EQ(cbm_language_for_extension(".frm"), CBM_LANG_FORM);
    PASS();
}
TEST(lang_ext_prc) {
    ASSERT_EQ(cbm_language_for_extension(".prc"), CBM_LANG_FORM);
    PASS();
}
TEST(lang_ext_magma) {
    ASSERT_EQ(cbm_language_for_extension(".mag"), CBM_LANG_MAGMA);
    PASS();
}
TEST(lang_ext_magma2) {
    ASSERT_EQ(cbm_language_for_extension(".magma"), CBM_LANG_MAGMA);
    PASS();
}
TEST(lang_ext_wolfram) {
    ASSERT_EQ(cbm_language_for_extension(".wl"), CBM_LANG_WOLFRAM);
    PASS();
}
TEST(lang_ext_wls) {
    ASSERT_EQ(cbm_language_for_extension(".wls"), CBM_LANG_WOLFRAM);
    PASS();
}

/* Helper languages */
TEST(lang_ext_html) {
    ASSERT_EQ(cbm_language_for_extension(".html"), CBM_LANG_HTML);
    PASS();
}
TEST(lang_ext_htm) {
    ASSERT_EQ(cbm_language_for_extension(".htm"), CBM_LANG_HTML);
    PASS();
}
TEST(lang_ext_css) {
    ASSERT_EQ(cbm_language_for_extension(".css"), CBM_LANG_CSS);
    PASS();
}
TEST(lang_ext_scss) {
    ASSERT_EQ(cbm_language_for_extension(".scss"), CBM_LANG_SCSS);
    PASS();
}
TEST(lang_ext_yaml) {
    ASSERT_EQ(cbm_language_for_extension(".yml"), CBM_LANG_YAML);
    PASS();
}
TEST(lang_ext_yaml2) {
    ASSERT_EQ(cbm_language_for_extension(".yaml"), CBM_LANG_YAML);
    PASS();
}
TEST(lang_ext_toml) {
    ASSERT_EQ(cbm_language_for_extension(".toml"), CBM_LANG_TOML);
    PASS();
}
TEST(lang_ext_hcl) {
    ASSERT_EQ(cbm_language_for_extension(".tf"), CBM_LANG_HCL);
    PASS();
}
TEST(lang_ext_hcl2) {
    ASSERT_EQ(cbm_language_for_extension(".hcl"), CBM_LANG_HCL);
    PASS();
}
TEST(lang_ext_sql) {
    ASSERT_EQ(cbm_language_for_extension(".sql"), CBM_LANG_SQL);
    PASS();
}
TEST(lang_ext_dockerfile) {
    ASSERT_EQ(cbm_language_for_extension(".dockerfile"), CBM_LANG_DOCKERFILE);
    PASS();
}
TEST(lang_ext_json) {
    ASSERT_EQ(cbm_language_for_extension(".json"), CBM_LANG_JSON);
    PASS();
}
TEST(lang_ext_xml) {
    ASSERT_EQ(cbm_language_for_extension(".xml"), CBM_LANG_XML);
    PASS();
}
TEST(lang_ext_xsl) {
    ASSERT_EQ(cbm_language_for_extension(".xsl"), CBM_LANG_XML);
    PASS();
}
TEST(lang_ext_xsd) {
    ASSERT_EQ(cbm_language_for_extension(".xsd"), CBM_LANG_XML);
    PASS();
}
TEST(lang_ext_svg) {
    ASSERT_EQ(cbm_language_for_extension(".svg"), CBM_LANG_XML);
    PASS();
}
TEST(lang_ext_markdown) {
    ASSERT_EQ(cbm_language_for_extension(".md"), CBM_LANG_MARKDOWN);
    PASS();
}
TEST(lang_ext_mdx) {
    ASSERT_EQ(cbm_language_for_extension(".mdx"), CBM_LANG_MARKDOWN);
    PASS();
}
TEST(lang_ext_makefile) {
    ASSERT_EQ(cbm_language_for_extension(".mk"), CBM_LANG_MAKEFILE);
    PASS();
}
TEST(lang_ext_cmake) {
    ASSERT_EQ(cbm_language_for_extension(".cmake"), CBM_LANG_CMAKE);
    PASS();
}
TEST(lang_ext_protobuf) {
    ASSERT_EQ(cbm_language_for_extension(".proto"), CBM_LANG_PROTOBUF);
    PASS();
}
TEST(lang_ext_graphql) {
    ASSERT_EQ(cbm_language_for_extension(".graphql"), CBM_LANG_GRAPHQL);
    PASS();
}
TEST(lang_ext_gdscript) {
    ASSERT_EQ(cbm_language_for_extension(".gd"), CBM_LANG_GDSCRIPT);
    PASS();
}
TEST(lang_ext_gql) {
    ASSERT_EQ(cbm_language_for_extension(".gql"), CBM_LANG_GRAPHQL);
    PASS();
}
TEST(lang_ext_vue) {
    ASSERT_EQ(cbm_language_for_extension(".vue"), CBM_LANG_VUE);
    PASS();
}
TEST(lang_ext_svelte) {
    ASSERT_EQ(cbm_language_for_extension(".svelte"), CBM_LANG_SVELTE);
    PASS();
}
TEST(lang_ext_meson) {
    ASSERT_EQ(cbm_language_for_extension(".meson"), CBM_LANG_MESON);
    PASS();
}
TEST(lang_ext_glsl) {
    ASSERT_EQ(cbm_language_for_extension(".glsl"), CBM_LANG_GLSL);
    PASS();
}
TEST(lang_ext_vert) {
    ASSERT_EQ(cbm_language_for_extension(".vert"), CBM_LANG_GLSL);
    PASS();
}
TEST(lang_ext_frag) {
    ASSERT_EQ(cbm_language_for_extension(".frag"), CBM_LANG_GLSL);
    PASS();
}
TEST(lang_ext_ini) {
    ASSERT_EQ(cbm_language_for_extension(".ini"), CBM_LANG_INI);
    PASS();
}
TEST(lang_ext_cfg) {
    ASSERT_EQ(cbm_language_for_extension(".cfg"), CBM_LANG_INI);
    PASS();
}
TEST(lang_ext_conf) {
    ASSERT_EQ(cbm_language_for_extension(".conf"), CBM_LANG_INI);
    PASS();
}

/* Unknown extension */
TEST(lang_ext_unknown) {
    ASSERT_EQ(cbm_language_for_extension(".xyz"), CBM_LANG_COUNT);
    PASS();
}
TEST(lang_ext_null) {
    ASSERT_EQ(cbm_language_for_extension(""), CBM_LANG_COUNT);
    PASS();
}

/* ── Filename-based detection ──────────────────────────────────── */

TEST(lang_fn_makefile) {
    ASSERT_EQ(cbm_language_for_filename("Makefile"), CBM_LANG_MAKEFILE);
    PASS();
}
TEST(lang_fn_gnumakefile) {
    ASSERT_EQ(cbm_language_for_filename("GNUmakefile"), CBM_LANG_MAKEFILE);
    PASS();
}
TEST(lang_fn_makefile_lower) {
    ASSERT_EQ(cbm_language_for_filename("makefile"), CBM_LANG_MAKEFILE);
    PASS();
}
TEST(lang_fn_cmake) {
    ASSERT_EQ(cbm_language_for_filename("CMakeLists.txt"), CBM_LANG_CMAKE);
    PASS();
}
TEST(lang_fn_dockerfile) {
    ASSERT_EQ(cbm_language_for_filename("Dockerfile"), CBM_LANG_DOCKERFILE);
    PASS();
}
TEST(lang_fn_meson_build) {
    ASSERT_EQ(cbm_language_for_filename("meson.build"), CBM_LANG_MESON);
    PASS();
}
TEST(lang_fn_meson_opts) {
    ASSERT_EQ(cbm_language_for_filename("meson.options"), CBM_LANG_MESON);
    PASS();
}
TEST(lang_fn_meson_opts_txt) {
    ASSERT_EQ(cbm_language_for_filename("meson_options.txt"), CBM_LANG_MESON);
    PASS();
}
TEST(lang_fn_vimrc) {
    ASSERT_EQ(cbm_language_for_filename(".vimrc"), CBM_LANG_VIMSCRIPT);
    PASS();
}

/* Filename with extension falls through to extension lookup */
TEST(lang_fn_main_go) {
    ASSERT_EQ(cbm_language_for_filename("main.go"), CBM_LANG_GO);
    PASS();
}
TEST(lang_fn_test_py) {
    ASSERT_EQ(cbm_language_for_filename("test.py"), CBM_LANG_PYTHON);
    PASS();
}
TEST(lang_fn_unknown) {
    ASSERT_EQ(cbm_language_for_filename("README"), CBM_LANG_COUNT);
    PASS();
}

/* ── Language name ─────────────────────────────────────────────── */

TEST(lang_name_go) {
    ASSERT_STR_EQ(cbm_language_name(CBM_LANG_GO), "Go");
    PASS();
}
TEST(lang_name_python) {
    ASSERT_STR_EQ(cbm_language_name(CBM_LANG_PYTHON), "Python");
    PASS();
}
TEST(lang_name_cpp) {
    ASSERT_STR_EQ(cbm_language_name(CBM_LANG_CPP), "C++");
    PASS();
}
TEST(lang_name_gdscript) {
    ASSERT_STR_EQ(cbm_language_name(CBM_LANG_GDSCRIPT), "GDScript");
    PASS();
}
TEST(lang_name_csharp) {
    ASSERT_STR_EQ(cbm_language_name(CBM_LANG_CSHARP), "C#");
    PASS();
}
TEST(lang_name_unknown) {
    ASSERT_STR_EQ(cbm_language_name(CBM_LANG_COUNT), "Unknown");
    PASS();
}

/* ── .m disambiguation ─────────────────────────────────────────── */

/* These tests need temp files with content markers */
TEST(lang_m_objc) {
    /* Write a temp file with Objective-C markers */
    char path[256]; snprintf(path, sizeof(path), "%s/test_lang_objc.m", cbm_tmpdir());
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "#import <Foundation/Foundation.h>\n@interface Foo : NSObject\n@end\n");
    fclose(f);

    ASSERT_EQ(cbm_disambiguate_m(path), CBM_LANG_OBJC);
    remove(path);
    PASS();
}

TEST(lang_m_magma) {
    char path[256]; snprintf(path, sizeof(path), "%s/test_lang_magma.m", cbm_tmpdir());
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "function MyFunc(x)\n  return x^2;\nend function;\n");
    fclose(f);

    ASSERT_EQ(cbm_disambiguate_m(path), CBM_LANG_MAGMA);
    remove(path);
    PASS();
}

TEST(lang_m_matlab) {
    char path[256]; snprintf(path, sizeof(path), "%s/test_lang_matlab.m", cbm_tmpdir());
    FILE *f = fopen(path, "w");
    ASSERT_NOT_NULL(f);
    fprintf(f, "function y = square(x)\n  y = x.^2;\nend\n");
    fclose(f);

    ASSERT_EQ(cbm_disambiguate_m(path), CBM_LANG_MATLAB);
    remove(path);
    PASS();
}

TEST(lang_m_default_on_read_fail) {
    /* Non-existent file defaults to MATLAB */
    ASSERT_EQ(cbm_disambiguate_m("/tmp/nonexistent_file_12345.m"), CBM_LANG_MATLAB);
    PASS();
}

/* --- Ported from lang_test.go: TestForLanguage --- */
TEST(lang_all_have_names) {
    /* Every language enum value from 0 to CBM_LANG_COUNT-1
     * should have a non-"Unknown" name. */
    for (int i = 0; i < CBM_LANG_COUNT; i++) {
        const char *name = cbm_language_name((CBMLanguage)i);
        ASSERT_NOT_NULL(name);
        ASSERT_TRUE(strcmp(name, "Unknown") != 0);
    }
    PASS();
}

/* ── Suite ─────────────────────────────────────────────────────── */

SUITE(language) {
    /* Extension: Tier 1 programming */
    RUN_TEST(lang_ext_go);
    RUN_TEST(lang_ext_python);
    RUN_TEST(lang_ext_javascript);
    RUN_TEST(lang_ext_jsx);
    RUN_TEST(lang_ext_typescript);
    RUN_TEST(lang_ext_tsx);
    RUN_TEST(lang_ext_rust);
    RUN_TEST(lang_ext_java);
    RUN_TEST(lang_ext_cpp);
    RUN_TEST(lang_ext_hpp);
    RUN_TEST(lang_ext_cc);
    RUN_TEST(lang_ext_cxx);
    RUN_TEST(lang_ext_hxx);
    RUN_TEST(lang_ext_hh);
    RUN_TEST(lang_ext_h);
    RUN_TEST(lang_ext_ixx);
    RUN_TEST(lang_ext_csharp);
    RUN_TEST(lang_ext_php);
    RUN_TEST(lang_ext_lua);
    RUN_TEST(lang_ext_scala);
    RUN_TEST(lang_ext_sc);
    RUN_TEST(lang_ext_kotlin);
    RUN_TEST(lang_ext_kts);
    RUN_TEST(lang_ext_ruby);
    RUN_TEST(lang_ext_rake);
    RUN_TEST(lang_ext_gemspec);
    RUN_TEST(lang_ext_c);
    RUN_TEST(lang_ext_bash);
    RUN_TEST(lang_ext_bash2);
    RUN_TEST(lang_ext_zig);
    RUN_TEST(lang_ext_elixir);
    RUN_TEST(lang_ext_exs);
    RUN_TEST(lang_ext_haskell);
    RUN_TEST(lang_ext_ocaml);
    RUN_TEST(lang_ext_mli);
    RUN_TEST(lang_ext_swift);
    RUN_TEST(lang_ext_dart);
    RUN_TEST(lang_ext_perl);
    RUN_TEST(lang_ext_pm);
    RUN_TEST(lang_ext_groovy);
    RUN_TEST(lang_ext_gradle);
    RUN_TEST(lang_ext_erlang);
    RUN_TEST(lang_ext_r);
    RUN_TEST(lang_ext_R);

    /* Extension: Tier 2 programming */
    RUN_TEST(lang_ext_clojure);
    RUN_TEST(lang_ext_cljs);
    RUN_TEST(lang_ext_cljc);
    RUN_TEST(lang_ext_fsharp);
    RUN_TEST(lang_ext_fsi);
    RUN_TEST(lang_ext_fsx);
    RUN_TEST(lang_ext_julia);
    RUN_TEST(lang_ext_vim);
    RUN_TEST(lang_ext_nix);
    RUN_TEST(lang_ext_commonlisp);
    RUN_TEST(lang_ext_lsp);
    RUN_TEST(lang_ext_cl);
    RUN_TEST(lang_ext_elm);
    RUN_TEST(lang_ext_fortran);
    RUN_TEST(lang_ext_f95);
    RUN_TEST(lang_ext_f03);
    RUN_TEST(lang_ext_f08);
    RUN_TEST(lang_ext_cuda);
    RUN_TEST(lang_ext_cuh);
    RUN_TEST(lang_ext_cobol);
    RUN_TEST(lang_ext_cbl);
    RUN_TEST(lang_ext_verilog);
    RUN_TEST(lang_ext_sv);
    RUN_TEST(lang_ext_emacslisp);

    /* Extension: Scientific/math */
    RUN_TEST(lang_ext_matlab);
    RUN_TEST(lang_ext_mlx);
    RUN_TEST(lang_ext_lean);
    RUN_TEST(lang_ext_form);
    RUN_TEST(lang_ext_prc);
    RUN_TEST(lang_ext_magma);
    RUN_TEST(lang_ext_magma2);
    RUN_TEST(lang_ext_wolfram);
    RUN_TEST(lang_ext_wls);

    /* Extension: Helper languages */
    RUN_TEST(lang_ext_html);
    RUN_TEST(lang_ext_htm);
    RUN_TEST(lang_ext_css);
    RUN_TEST(lang_ext_scss);
    RUN_TEST(lang_ext_yaml);
    RUN_TEST(lang_ext_yaml2);
    RUN_TEST(lang_ext_toml);
    RUN_TEST(lang_ext_hcl);
    RUN_TEST(lang_ext_hcl2);
    RUN_TEST(lang_ext_sql);
    RUN_TEST(lang_ext_dockerfile);
    RUN_TEST(lang_ext_json);
    RUN_TEST(lang_ext_xml);
    RUN_TEST(lang_ext_xsl);
    RUN_TEST(lang_ext_xsd);
    RUN_TEST(lang_ext_svg);
    RUN_TEST(lang_ext_markdown);
    RUN_TEST(lang_ext_mdx);
    RUN_TEST(lang_ext_makefile);
    RUN_TEST(lang_ext_cmake);
    RUN_TEST(lang_ext_protobuf);
    RUN_TEST(lang_ext_graphql);
    RUN_TEST(lang_ext_gql);
    RUN_TEST(lang_ext_gdscript);
    RUN_TEST(lang_ext_vue);
    RUN_TEST(lang_ext_svelte);
    RUN_TEST(lang_ext_meson);
    RUN_TEST(lang_ext_glsl);
    RUN_TEST(lang_ext_vert);
    RUN_TEST(lang_ext_frag);
    RUN_TEST(lang_ext_ini);
    RUN_TEST(lang_ext_cfg);
    RUN_TEST(lang_ext_conf);

    /* Unknown/edge cases */
    RUN_TEST(lang_ext_unknown);
    RUN_TEST(lang_ext_null);

    /* Filename-based */
    RUN_TEST(lang_fn_makefile);
    RUN_TEST(lang_fn_gnumakefile);
    RUN_TEST(lang_fn_makefile_lower);
    RUN_TEST(lang_fn_cmake);
    RUN_TEST(lang_fn_dockerfile);
    RUN_TEST(lang_fn_meson_build);
    RUN_TEST(lang_fn_meson_opts);
    RUN_TEST(lang_fn_meson_opts_txt);
    RUN_TEST(lang_fn_vimrc);
    RUN_TEST(lang_fn_main_go);
    RUN_TEST(lang_fn_test_py);
    RUN_TEST(lang_fn_unknown);

    /* Language names */
    RUN_TEST(lang_name_go);
    RUN_TEST(lang_name_python);
    RUN_TEST(lang_name_cpp);
    RUN_TEST(lang_name_gdscript);
    RUN_TEST(lang_name_csharp);
    RUN_TEST(lang_name_unknown);

    /* .m disambiguation */
    RUN_TEST(lang_m_objc);
    RUN_TEST(lang_m_magma);
    RUN_TEST(lang_m_matlab);
    RUN_TEST(lang_m_default_on_read_fail);

    /* Go test ports */
    RUN_TEST(lang_all_have_names);
}
