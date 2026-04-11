# Third-Party Licenses

This project vendors third-party code. We are grateful to the authors and
maintainers of these projects for making their work freely available.

## Tree-sitter Runtime

The tree-sitter C runtime is vendored in `internal/cbm/vendored/ts_runtime/`.

- **Project:** [tree-sitter](https://github.com/tree-sitter/tree-sitter)
- **License:** MIT
- **Copyright:** (c) 2018 Max Brunsfeld

## Tree-sitter Grammars

Pre-generated parser sources are vendored in `internal/cbm/vendored/grammars/<lang>/`.
Each grammar is the work of its upstream authors; we vendor only the generated
`parser.c` (and `scanner.c` where applicable) for static compilation.

| Grammar | Upstream Repository | License | Copyright |
|---------|-------------------|---------|-----------|
| go | [tree-sitter/tree-sitter-go](https://github.com/tree-sitter/tree-sitter-go) | MIT | (c) 2014 Max Brunsfeld |
| bash | [tree-sitter/tree-sitter-bash](https://github.com/tree-sitter/tree-sitter-bash) | MIT | (c) 2017 Max Brunsfeld |
| c | [tree-sitter/tree-sitter-c](https://github.com/tree-sitter/tree-sitter-c) | MIT | (c) 2014 Max Brunsfeld |
| clojure | [tree-sitter-grammars/tree-sitter-clojure](https://github.com/tree-sitter-grammars/tree-sitter-clojure) | EPL-1.0 | (c) Amaan Qureshi |
| cmake | [tree-sitter-grammars/tree-sitter-cmake](https://github.com/tree-sitter-grammars/tree-sitter-cmake) | MIT | (c) Amaan Qureshi |
| cobol | [tree-sitter-grammars/tree-sitter-cobol](https://github.com/tree-sitter-grammars/tree-sitter-cobol) | MIT | (c) Amaan Qureshi |
| commonlisp | [tree-sitter-grammars/tree-sitter-commonlisp](https://github.com/tree-sitter-grammars/tree-sitter-commonlisp) | MIT | (c) Amaan Qureshi |
| cpp | [tree-sitter/tree-sitter-cpp](https://github.com/tree-sitter/tree-sitter-cpp) | MIT | (c) 2014 Max Brunsfeld |
| c_sharp | [tree-sitter/tree-sitter-c-sharp](https://github.com/tree-sitter/tree-sitter-c-sharp) | MIT | (c) 2014 Max Brunsfeld |
| css | [tree-sitter/tree-sitter-css](https://github.com/tree-sitter/tree-sitter-css) | MIT | (c) 2017 Max Brunsfeld |
| cuda | [tree-sitter-grammars/tree-sitter-cuda](https://github.com/tree-sitter-grammars/tree-sitter-cuda) | MIT | (c) Amaan Qureshi |
| dart | [tree-sitter/tree-sitter-dart](https://github.com/tree-sitter/tree-sitter-dart) | MIT | (c) Amaan Qureshi |
| dockerfile | [tree-sitter/tree-sitter-dockerfile](https://github.com/tree-sitter/tree-sitter-dockerfile) | MIT | (c) Max Brunsfeld |
| elisp | [tree-sitter-grammars/tree-sitter-elisp](https://github.com/tree-sitter-grammars/tree-sitter-elisp) | MIT | (c) Amaan Qureshi |
| elixir | [tree-sitter/tree-sitter-elixir](https://github.com/tree-sitter/tree-sitter-elixir) | MIT | (c) Max Brunsfeld |
| elm | [tree-sitter-grammars/tree-sitter-elm](https://github.com/tree-sitter-grammars/tree-sitter-elm) | MIT | (c) Amaan Qureshi |
| erlang | [tree-sitter/tree-sitter-erlang](https://github.com/tree-sitter/tree-sitter-erlang) | MIT | (c) Nemo Anzai |
| form | Custom grammar | MIT | (c) 2026 DeusData |
| fortran | [tree-sitter-grammars/tree-sitter-fortran](https://github.com/tree-sitter-grammars/tree-sitter-fortran) | MIT | (c) Amaan Qureshi |
| fsharp | [tree-sitter-grammars/tree-sitter-fsharp](https://github.com/tree-sitter-grammars/tree-sitter-fsharp) | MIT | (c) Amaan Qureshi |
| glsl | [tree-sitter-grammars/tree-sitter-glsl](https://github.com/tree-sitter-grammars/tree-sitter-glsl) | MIT | (c) Amaan Qureshi |
| graphql | [tree-sitter-grammars/tree-sitter-graphql](https://github.com/tree-sitter-grammars/tree-sitter-graphql) | MIT | (c) Amaan Qureshi |
| groovy | [tree-sitter/tree-sitter-groovy](https://github.com/tree-sitter/tree-sitter-groovy) | MIT | (c) Amaan Qureshi |
| haskell | [tree-sitter/tree-sitter-haskell](https://github.com/tree-sitter/tree-sitter-haskell) | MIT | (c) 2017 Max Brunsfeld |
| hcl | [tree-sitter-grammars/tree-sitter-hcl](https://github.com/tree-sitter-grammars/tree-sitter-hcl) | MIT | (c) Amaan Qureshi |
| html | [tree-sitter/tree-sitter-html](https://github.com/tree-sitter/tree-sitter-html) | MIT | (c) 2017 Max Brunsfeld |
| ini | [tree-sitter-grammars/tree-sitter-ini](https://github.com/tree-sitter-grammars/tree-sitter-ini) | MIT | (c) Amaan Qureshi |
| java | [tree-sitter/tree-sitter-java](https://github.com/tree-sitter/tree-sitter-java) | MIT | (c) 2017 Max Brunsfeld |
| javascript | [tree-sitter/tree-sitter-javascript](https://github.com/tree-sitter/tree-sitter-javascript) | MIT | (c) 2014 Max Brunsfeld |
| json | [tree-sitter/tree-sitter-json](https://github.com/tree-sitter/tree-sitter-json) | MIT | (c) Max Brunsfeld |
| julia | [tree-sitter-grammars/tree-sitter-julia](https://github.com/tree-sitter-grammars/tree-sitter-julia) | MIT | (c) Amaan Qureshi |
| kotlin | [tree-sitter/tree-sitter-kotlin](https://github.com/tree-sitter/tree-sitter-kotlin) | MIT | (c) Max Brunsfeld |
| lean | [Julian/tree-sitter-lean](https://github.com/Julian/tree-sitter-lean) | MIT | (c) Julian Samarrasinghe |
| lua | [tree-sitter/tree-sitter-lua](https://github.com/tree-sitter/tree-sitter-lua) | MIT | (c) Amaan Qureshi |
| magma | Custom grammar | MIT | (c) 2026 DeusData |
| make | [tree-sitter/tree-sitter-make](https://github.com/tree-sitter/tree-sitter-make) | MIT | (c) Amaan Qureshi |
| markdown | [tree-sitter-grammars/tree-sitter-markdown](https://github.com/tree-sitter-grammars/tree-sitter-markdown) | MIT | (c) Amaan Qureshi |
| matlab | [acristoffers/tree-sitter-matlab](https://github.com/acristoffers/tree-sitter-matlab) | MIT | (c) Alan Cristoffers |
| meson | [tree-sitter-grammars/tree-sitter-meson](https://github.com/tree-sitter-grammars/tree-sitter-meson) | MIT | (c) Amaan Qureshi |
| nix | [tree-sitter-grammars/tree-sitter-nix](https://github.com/tree-sitter-grammars/tree-sitter-nix) | MIT | (c) Amaan Qureshi |
| objc | [tree-sitter-grammars/tree-sitter-objective-c](https://github.com/tree-sitter-grammars/tree-sitter-objective-c) | MIT | (c) Max Brunsfeld |
| ocaml | [tree-sitter/tree-sitter-ocaml](https://github.com/tree-sitter/tree-sitter-ocaml) | MIT | (c) Max Brunsfeld |
| perl | [tree-sitter/tree-sitter-perl](https://github.com/tree-sitter/tree-sitter-perl) | MIT | (c) Ganesh Tiwari |
| php | [tree-sitter/tree-sitter-php](https://github.com/tree-sitter/tree-sitter-php) | MIT | (c) Josh Vera, Max Brunsfeld, Amaan Qureshi |
| protobuf | [tree-sitter-grammars/tree-sitter-protobuf](https://github.com/tree-sitter-grammars/tree-sitter-protobuf) | MIT | (c) Amaan Qureshi |
| python | [tree-sitter/tree-sitter-python](https://github.com/tree-sitter/tree-sitter-python) | MIT | (c) 2016 Max Brunsfeld |
| r | [tree-sitter/tree-sitter-r](https://github.com/tree-sitter/tree-sitter-r) | MIT | (c) Max Brunsfeld |
| ruby | [tree-sitter/tree-sitter-ruby](https://github.com/tree-sitter/tree-sitter-ruby) | MIT | (c) 2017 Max Brunsfeld |
| rust | [tree-sitter/tree-sitter-rust](https://github.com/tree-sitter/tree-sitter-rust) | MIT | (c) 2017 Max Brunsfeld |
| scala | [tree-sitter/tree-sitter-scala](https://github.com/tree-sitter/tree-sitter-scala) | MIT | (c) Amaan Qureshi |
| scss | [tree-sitter/tree-sitter-scss](https://github.com/tree-sitter/tree-sitter-scss) | MIT | (c) Max Brunsfeld |
| sql | [tree-sitter/tree-sitter-sql](https://github.com/tree-sitter/tree-sitter-sql) | MIT | (c) Amaan Qureshi |
| svelte | [tree-sitter-grammars/tree-sitter-svelte](https://github.com/tree-sitter-grammars/tree-sitter-svelte) | MIT | (c) Amaan Qureshi |
| swift | [tree-sitter/tree-sitter-swift](https://github.com/tree-sitter/tree-sitter-swift) | MIT | (c) Max Brunsfeld |
| toml | [tree-sitter-grammars/tree-sitter-toml](https://github.com/tree-sitter-grammars/tree-sitter-toml) | MIT | (c) Amaan Qureshi |
| typescript | [tree-sitter/tree-sitter-typescript](https://github.com/tree-sitter/tree-sitter-typescript) | MIT | (c) Max Brunsfeld |
| tsx | [tree-sitter/tree-sitter-typescript](https://github.com/tree-sitter/tree-sitter-typescript) | MIT | (c) Max Brunsfeld |
| verilog | [tree-sitter/tree-sitter-verilog](https://github.com/tree-sitter/tree-sitter-verilog) | MIT | (c) Aman Hardikar |
| vim | [tree-sitter-grammars/tree-sitter-vim](https://github.com/tree-sitter-grammars/tree-sitter-vim) | MIT | (c) Amaan Qureshi |
| vue | [tree-sitter-grammars/tree-sitter-vue](https://github.com/tree-sitter-grammars/tree-sitter-vue) | MIT | (c) Amaan Qureshi |
| wolfram | [LumaKernel/tree-sitter-wolfram](https://github.com/LumaKernel/tree-sitter-wolfram) | MIT | (c) LumaKernel |
| xml | [tree-sitter-grammars/tree-sitter-xml](https://github.com/tree-sitter-grammars/tree-sitter-xml) | MIT | (c) Amaan Qureshi |
| yaml | [tree-sitter/tree-sitter-yaml](https://github.com/tree-sitter/tree-sitter-yaml) | MIT | (c) Max Brunsfeld |
| zig | [tree-sitter/tree-sitter-zig](https://github.com/tree-sitter/tree-sitter-zig) | MIT | (c) Max Brunsfeld |

## Vendored C Libraries

All C dependencies are vendored in `vendored/` — zero system library dependencies required.

| Library | License | Project |
|---------|---------|---------|
| SQLite 3 | Public Domain | [sqlite.org](https://www.sqlite.org/) |
| mimalloc | MIT | [microsoft/mimalloc](https://github.com/microsoft/mimalloc) |
| Mongoose | Dual GPLv2 / Commercial | [cesanta/mongoose](https://github.com/cesanta/mongoose) |
| yyjson | MIT | [ibireme/yyjson](https://github.com/ibireme/yyjson) |
| xxHash | BSD-2-Clause | [Cyan4973/xxHash](https://github.com/Cyan4973/xxHash) |
| TRE | BSD-2-Clause | [laurikari/tre](https://github.com/laurikari/tre) |

## Notes

- **FORM** and **Magma** grammars are custom tree-sitter grammars created by DeusData
  for this project (MIT-licensed under the project's own license).
- **Clojure** uses the Eclipse Public License 1.0, which is compatible with MIT
  for downstream use.
- All other grammars are MIT-licensed.
