#!/usr/bin/env bash
set -euo pipefail

# Clone benchmark repositories for MCP vs Explorer quality comparison.
# Uses shallow clones (--depth 1) to minimize disk usage.
# Shared repos are cloned once and symlinked for secondary languages.

BENCH_DIR="${1:-/tmp/bench}"

clone() {
    local lang="$1" repo="$2" subdir="${3:-}"
    local dest="$BENCH_DIR/$lang"
    if [ -d "$dest" ]; then
        echo "SKIP: $lang (exists)"
        return
    fi
    echo "CLONE: $lang <- $repo"
    git clone --depth 1 --quiet "https://github.com/$repo.git" "$dest"
    echo "  OK: $(du -sh "$dest" | cut -f1)"
}

symlink() {
    local lang="$1" source_lang="$2"
    local dest="$BENCH_DIR/$lang"
    if [ -d "$dest" ] || [ -L "$dest" ]; then
        echo "SKIP: $lang (exists)"
        return
    fi
    echo "LINK: $lang -> $source_lang"
    ln -s "$BENCH_DIR/$source_lang" "$dest"
}

mkdir -p "$BENCH_DIR"

# Programming languages — Tier 1 (44 languages)
# Target: 100K+ LOC per repo for meaningful performance benchmarks
clone go          "kubernetes/kubernetes"          # 3.5M LOC, the Go benchmark
clone python      "django/django"                  # 350K+ LOC, web framework
clone javascript  "vercel/next.js"                 # 500K+ LOC, React framework
clone typescript  "microsoft/TypeScript"           # 1M+ LOC, the TS compiler
clone tsx         "shadcn-ui/ui"                   # 728K LOC (already large)
clone java        "elastic/elasticsearch"          # 2M+ LOC, search engine
clone kotlin      "JetBrains/Exposed"              # 977K LOC (already large)
clone scala       "apache/spark"                   # 1M+ LOC, big data
clone rust        "meilisearch/meilisearch"        # 409K LOC (already large)
clone c           "redis/redis"                    # 546K LOC (already large)
clone cpp         "protocolbuffers/protobuf"       # 500K+ LOC, real .cpp files
clone csharp      "dotnet/runtime"                 # Massive C# runtime
clone php         "koel/koel"                      # 189K LOC (OK)
clone ruby        "rails/rails"                    # 500K+ LOC, the Ruby framework
clone lua         "neovim/neovim"                  # 500K+ LOC, editor
clone bash        "ohmyzsh/ohmyzsh"                # 100K+ .sh files
clone zig         "tigerbeetle/tigerbeetle"        # 224K LOC (already large)
clone haskell     "jgm/pandoc"                     # 433K LOC (already large)
clone ocaml       "ocaml/dune"                     # 345K LOC (already large)
clone elixir      "plausible/analytics"            # 677K LOC (already large)
clone erlang      "emqx/emqx"                      # 500K+ LOC, MQTT broker
clone objc        "realm/realm-cocoa"              # 200K+ LOC, database SDK
clone swift       "Alamofire/Alamofire"            # 370K LOC (already large)
clone dart        "felangel/bloc"                  # 285K LOC (already large)
clone perl        "movabletype/movabletype"        # 300K+ LOC, CMS
clone groovy      "spockframework/spock"           # 137K LOC (OK)
clone r           "tidyverse/ggplot2"              # 150K+ LOC, visualization
clone clojure     "clojure/clojure"                # 108K LOC (OK)
clone fsharp      "dotnet/fsharp"                  # 500K+ LOC, the F# compiler
clone julia       "JuliaLang/julia"                # 1M+ LOC, the Julia runtime
clone vimscript   "SpaceVim/SpaceVim"              # 2.6M LOC (already huge)
clone nix         "NixOS/nixpkgs"                  # 6M LOC (already huge)
clone commonlisp  "lem-project/lem"                # 1.2M LOC (already large)
clone elm         "elm/compiler"                   # 57K LOC (largest Elm repo available)
clone fortran     "cp2k/cp2k"                      # 5.9M LOC (already huge)
clone cobol       "OCamlPro/gnucobol"              # 540K LOC (already large)
clone verilog     "YosysHQ/yosys"                  # 517K LOC (already large)
clone emacslisp   "emacs-mirror/emacs"             # 5.3M LOC (already huge)
clone matlab      "acristoffers/tree-sitter-matlab" # 133K LOC (best available)
clone lean        "leanprover-community/mathlib4"  # 2.3M LOC (already huge)
clone form        "vermaseren/form"                # 221K LOC (already large)
clone wolfram     "WolframResearch/WolframLanguageForJupyter" # 4K LOC (largest public Wolfram repo)

# Helper languages — Tier 2 (22 languages)
clone yaml        "kubernetes/examples"            # K8s manifests
clone hcl         "hashicorp/terraform-provider-aws" # 1M+ LOC, massive HCL
clone scss        "twbs/bootstrap"                 # 120K LOC (OK)
clone dockerfile  "docker-library/official-images" # Docker configs
clone cmake       "Kitware/CMake"                  # 1.7M LOC (already huge)
clone protobuf    "googleapis/googleapis"          # 2.2M LOC (already huge)
clone graphql     "graphql/graphql-spec"           # 23K LOC (largest pure GraphQL)
clone vue         "vuejs/core"                     # 200K+ LOC, Vue 3 core
clone svelte      "sveltejs/svelte"                # 267K LOC (already large)
clone meson       "mesonbuild/meson"               # 237K LOC (already large)

# Shared repos (symlinked — language uses same repo as primary)
symlink html      javascript    # Express views contain HTML
symlink css       tsx           # shadcn-ui styles
symlink toml      rust          # meilisearch Cargo.toml + config
symlink sql       java          # spring-petclinic SQL schemas
clone cuda        "NVIDIA/cuda-samples"
symlink json      typescript    # trpc JSON configs
symlink xml       java          # spring-petclinic XML configs
symlink markdown  python        # httpie docs
symlink makefile  c             # redis Makefile
clone glsl        "repalash/Open-Shaders"
symlink ini       python        # httpie .cfg/.ini files
symlink magma     lean          # .m files — disambiguated via content markers
symlink kubernetes yaml         # YAML subtype — Deployment/Service manifests
symlink kustomize yaml          # YAML subtype — kustomization.yaml

echo ""
echo "=== Clone complete ==="
ls -1 "$BENCH_DIR/" | wc -l | xargs printf "%s repos ready in $BENCH_DIR\n"
