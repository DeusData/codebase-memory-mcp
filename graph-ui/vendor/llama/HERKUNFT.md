# Optional llama.cpp runtime provenance

The binaries are not committed. This document records the source and build procedure
for the optional Apple Silicon sidecar used by the GGUF evaluation. Browser inference
uses the separately documented ONNX runtime.

## Source and build

- Source: https://github.com/ggml-org/llama.cpp
- Recorded tag: **b10675**, commit `90c26fc`
- Built from source on 2026-08-28 for Darwin arm64
- Reported binary version: `0.3.0-dev (build 1, commit 90c26fc)`; the build lacked
  the Git metadata used for the numeric build counter

```sh
cmake -B build -DGGML_METAL=ON -DGGML_RPC=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release --target llama-server
```

Metal enables local GPU inference. Disabling GGML RPC omits the remote-compute backend.
The local scripts `llm/start.sh` and `llm/stop.sh` manage the sidecar at
`127.0.0.1:4141`. The recorded build includes `llama-server` and nine dynamic libraries.

Copy the executable and libraries identified by `otool -L`, then replace their build
paths and sign the modified files locally:

```sh
install_name_tool -rpath <build-directory>/bin @executable_path vendor/llama/llama-server
install_name_tool -rpath <build-directory>/bin @loader_path vendor/llama/<library>
codesign --force --sign - vendor/llama/<file>
```

These operations change the bytes. Independent rebuilds need not reproduce the
recorded hashes; compare source revision and build options as well.

## External dependencies

The recorded build loads Homebrew OpenSSL from these absolute paths; the libraries
are not copied into this directory:

```text
/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib
/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib
```

It therefore requires that OpenSSL installation. Other dependencies are system
libraries, including CoreFoundation, Security, libc++ and libSystem.

## Recorded SHA-256 values

These identify the locally patched and signed files, not the original build output.
Symlink aliases are omitted.

```text
b5b95c7da3d6222f509e2c844f503ff039f16d7f07d10fa318f432d7e6a71b1b  llama-server
193d4b04a4f21d2b80cd1efc3958891b20f82ab066e79aeca0fd37ac3eed86fb  libggml-base.0.22.0.dylib
89f483e236860145196722226a88bf2802cb215ac8d98bc901c3ad7a9c1dab5e  libggml-blas.0.22.0.dylib
d9609e1c389e27b42281a64159d0120a29083ecac1f6d5ca740084b2b3e8a653  libggml-cpu.0.22.0.dylib
f46ca279250dd7f594c4ee6792b565d74ee6de47045b779a42df801999137588  libggml-metal.0.22.0.dylib
152efd99a02db26a5e0b45a4e3a5bfdf18f6e6aa52c60bbc7dfaf4e88173c427  libggml.0.22.0.dylib
5737e61e1e119f613d15c74c5abb8a6175a03053bd34c1c49d48c88c1f3d7077  libllama-common.0.3.0.dylib
a9802e0717a1154cbb42f856b740db1c905e5f5c7db660274af9e37d320f75f0  libllama-server-impl.dylib
15abeed752e583896a196d308de082ae3cc50297126a4bbdd7c4b7dc79ad3fea  libllama.0.3.0.dylib
6c91a2c43bcaa7e77607348c1ec45c65617767393b3dbf751e70332f82ac2750  libmtmd.0.3.0.dylib
```

## License

llama.cpp is MIT licensed. Its full license is in `LICENSE` at the source revision
above; preserve it when redistributing a built runtime.
