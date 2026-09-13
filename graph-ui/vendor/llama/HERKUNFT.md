# Herkunft der Inferenz-Runtime in vendor/llama/

Diese Datei ist committet, das Verzeichnis daneben nicht. Was hier steht, muss
reichen, um den Inhalt von vendor/llama/ auf einer anderen Maschine identisch
herzustellen; die 19 MB Binaerdaten selbst gehoeren nicht in die Historie.

## Was hier liegt

`llama-server` aus llama.cpp, gebaut fuer Apple Silicon (Darwin arm64), plus
die neun dynamischen Bibliotheken, gegen die er gelinkt ist. Gestartet wird er
ueber `llm/start.sh`, beendet ueber `llm/stop.sh`; er bindet ausschliesslich
127.0.0.1:4141.

## Bezugsquelle und Version

- Quelle: https://github.com/ggml-org/llama.cpp
- Tag: **b10675** (`git describe --tags` im Bauverzeichnis; der Tag zeigt auf
  Commit `90c26fc`, "Vulkan: add hoisting support for row IDs and expert count
  in shaders (#26686)")
- Bezogen und gebaut am: 2026-08-28
- Gebaut aus den Quellen, nicht aus einem Release-Artefakt

Die Version, die das Binary selbst nennt, lautet
`version: 0.3.0-dev (build 1, commit 90c26fc)`. Sie nennt den Tag NICHT: der
Bauvorgang lief in einem Baum ohne die Git-Metadaten, die llama.cpp fuer seine
Build-Nummer liest, und faellt dann auf `build 1` zurueck. Der Tag oben ist
deshalb die belastbare Angabe, und der Commit-Hash ist die Bruecke zwischen
beiden.

## Baubefehl

```sh
cmake -B build -DGGML_METAL=ON -DGGML_RPC=OFF -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release --target llama-server
```

Warum diese beiden Schalter, und nicht die Vorgaben:

- **`GGML_METAL=ON`**: die ganze Modellwahl in ADR 0001 rechnet mit der GPU
  dieser Maschine. Ein CPU-Build waere ein anderes Produkt mit denselben
  Dateinamen.
- **`GGML_RPC=OFF`**: das RPC-Backend von ggml oeffnet einen Weg, Rechenarbeit
  an einen anderen Rechner zu geben. In einem Projekt, dessen erste Zusage
  "nichts verlaesst diese Maschine" ist, wird diese Faehigkeit nicht
  mitgeliefert und dann diszipliniert nicht benutzt; sie wird gar nicht erst
  gebaut.

Aus dem Bauverzeichnis uebernommen wurden `bin/llama-server` und die dylibs,
die `otool -L` an ihm und an `libllama-server-impl.dylib` nennt.

## Anpassung nach dem Kopieren

Der Linker schreibt in `LC_RPATH` den absoluten Pfad des Bauverzeichnisses.
Ein Binary mit diesem Pfad laeuft nur, solange das Bauverzeichnis existiert.
Deshalb wurde der Pfad ersetzt und danach ad-hoc neu signiert (ohne beides
verweigert macOS auf arm64 den Start):

```sh
install_name_tool -rpath <bauverzeichnis>/bin @executable_path vendor/llama/llama-server
install_name_tool -rpath <bauverzeichnis>/bin @loader_path  vendor/llama/<jede-dylib>
codesign --force --sign - vendor/llama/<jede-datei>
```

Die Pruefsummen unten sind die der so angepassten Dateien, also genau der
Dateien, die hier liegen und laufen. Sie stimmen NICHT mit denen im
Bauverzeichnis ueberein; wer nachbaut, bekommt andere Werte und sollte statt
der Summen den Tag und den Baubefehl vergleichen.

## Fremde Abhaengigkeiten

`llama-server` linkt gegen OpenSSL aus Homebrew:

```
/opt/homebrew/opt/openssl@3/lib/libssl.3.dylib
/opt/homebrew/opt/openssl@3/lib/libcrypto.3.dylib
```

Beide werden ueber ihren absoluten Pfad geladen und liegen NICHT in diesem
Verzeichnis. Auf einer Maschine ohne `brew install openssl@3` startet das
Binary nicht. Das ist eine echte Grenze dieser Auslieferung und steht hier,
statt in einem Startfehler entdeckt zu werden.

Alles Weitere sind Systembibliotheken (CoreFoundation, Security, libc++,
libSystem).

## SHA256 der ausgelieferten Dateien

Symlinks (`libggml.dylib` -> `libggml.0.dylib` -> `libggml.0.22.0.dylib`) sind
weggelassen; aufgefuehrt ist jeweils die reale Datei.

```
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

## Lizenz

llama.cpp steht unter der MIT-Lizenz. Der Volltext liegt im Quellbaum
(`LICENSE`) und ist mit dem Tag oben eindeutig bestimmt.
