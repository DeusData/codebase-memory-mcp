{
  description = "codebase-memory-mcp — C11 MCP server for codebase indexing";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "aarch64-darwin" "x86_64-darwin" "aarch64-linux" "x86_64-linux" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      # Cross dev shell (macOS only): build the *other* darwin arch locally.
      # The native clang already cross-emits object code via -arch (scripts/env.sh
      # exports ARCHFLAGS), so the only thing missing for a cross link is zlib:
      # it is the product's single link-time dependency (`-lz`), and the host-arch
      # copy cannot satisfy an x86_64 link on arm64 or vice versa. This shell puts
      # the TARGET-arch zlib on the include and link paths, and nothing else.
      #
      # Building needs no Rosetta; *running* an x86_64 binary on Apple Silicon
      # does. The target-arch libs come from prebuilt substitutes, so entering the
      # shell is a download rather than a cross-build of the dependency tree.
      #
      # Returns {} off macOS, where there is no second darwin arch to target.
      crossDevShells = pkgs:
        let
          inherit (nixpkgs) lib;
          crossTarget =
            {
              "aarch64-darwin" = "x86_64-darwin";
              "x86_64-darwin" = "aarch64-darwin";
            }
            .${pkgs.system} or null;
          mkCrossShell =
            targetSystem:
            let
              tpkgs = nixpkgs.legacyPackages.${targetSystem};
              targetArch = if targetSystem == "x86_64-darwin" then "x86_64" else "arm64";
            in
            pkgs.mkShell {
              # Host toolchain only — clang comes from the shell stdenv. Deliberately
              # NOT inputsFrom the default package: that would put the HOST-arch zlib
              # on the link path, which is exactly the wrong-arch copy this shell
              # exists to displace.
              nativeBuildInputs = [ pkgs.gnumake ];
              shellHook = ''
                # zlib is consumed as a bare `-lz` / `#include <zlib.h>`, so its
                # include dir (the header is arch-independent) and lib dir are
                # supplied by hand. NIX_LDFLAGS is searched ahead of the link's own
                # -L, so the target-arch slice wins; a host-arch copy that still
                # reaches ld is skipped as "wrong architecture" rather than
                # producing a silently mislinked binary.
                export NIX_CFLAGS_COMPILE="-I${lib.getDev tpkgs.zlib}/include ''${NIX_CFLAGS_COMPILE:-}"
                export NIX_LDFLAGS="-L${lib.getLib tpkgs.zlib}/lib ''${NIX_LDFLAGS:-}"
                echo "[cross] target ${targetSystem} — build with: scripts/build.sh --arch ${targetArch}"
              '';
            };
        in
        lib.optionalAttrs (crossTarget != null) {
          # `nix develop .#cross` then `scripts/build.sh --arch <target>`.
          cross = mkCrossShell crossTarget;
        };
    in
    {
      packages = forAllSystems (pkgs: rec {
        default = pkgs.stdenv.mkDerivation {
          pname = "codebase-memory-mcp";
          version = "0.10.8";

          src = ./.;

          nativeBuildInputs = [ pkgs.gnumake ];
          buildInputs = [ pkgs.zlib ];

          # scripts/build.sh verifies the compiler via `file`, which fails on Nix
          # because CC is a bash wrapper script rather than a binary. Call make
          # directly to bypass that check; the Nix stdenv already guarantees the
          # correct compiler and target architecture.
          buildPhase = ''
            make -j$NIX_BUILD_CORES -f Makefile.cbm cbm
          '';

          installPhase = ''
            install -Dm755 build/c/codebase-memory-mcp $out/bin/codebase-memory-mcp
          '';

          meta = {
            description = "MCP server that builds and queries a semantic graph of your codebase";
            homepage = "https://github.com/DeusData/codebase-memory-mcp";
            license = nixpkgs.lib.licenses.mit;
            mainProgram = "codebase-memory-mcp";
            platforms = systems;
          };
        };

        # The graph-ui React frontend, built to static assets (graph-ui/dist).
        # buildNpmPackage fetches npm deps offline via the pinned package-lock.json;
        # bump npmDepsHash whenever that lockfile changes:
        #   nix run nixpkgs#prefetch-npm-deps -- graph-ui/package-lock.json
        graph-ui = pkgs.buildNpmPackage {
          pname = "codebase-memory-graph-ui";
          version = "0.1.0";

          src = ./graph-ui;

          npmDepsHash = "sha256-P1JVo+GFr+Gsq88dnn9OsedZqrTaj3DDGbej+nHbp4U=";

          # `npm run build` runs `tsc -b && vite build`, emitting to dist/.
          installPhase = ''
            runHook preInstall
            cp -r dist $out
            runHook postInstall
          '';
        };

        # Same server binary as `default`, but with the graph-ui assets embedded
        # so `--ui=true` starts the HTTP server. The frontend is built separately
        # by the graph-ui package above; here we drop its dist/ into place and run
        # the embed + link path (cbm-with-ui) — no network access needed.
        codebase-memory-mcp-ui = default.overrideAttrs (old: {
          pname = "codebase-memory-mcp-ui";

          buildPhase = ''
            # cbm-with-ui depends on `embed`, which depends on `frontend`, which
            # runs `npm ci && npm run build` — needs network access, unavailable
            # in the sandbox. Supply the pre-built assets, run the embed script
            # directly, then link with `cbm-with-ui` while telling make its
            # `frontend`/`embed` prerequisites are already up to date (-o) so it
            # won't re-run the npm build.
            mkdir -p graph-ui/dist
            cp -r ${graph-ui}/. graph-ui/dist/
            chmod -R u+w graph-ui/dist

            bash scripts/embed-frontend.sh graph-ui/dist build/c/embedded

            make -j$NIX_BUILD_CORES -f Makefile.cbm \
              -o frontend -o embed \
              cbm-with-ui
          '';

          meta = old.meta // {
            description = "codebase-memory-mcp with the embedded graph UI (--ui)";
          };
        });
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.system}.default ];
        };
      } // crossDevShells pkgs);
    };
}
