#!/bin/sh
# Ein Modell von Hugging Face in das Cache-Verzeichnis dieses Projekts holen.
#
# Aufruf:
#   llm/fetch-model.sh <user>/<repo>[:<quant>]   holt das Modell
#   llm/fetch-model.sh --print <user>/<repo>     zeigt nur den Befehl
#   llm/fetch-model.sh --list                    zeigt, was im Cache liegt
#
# ## Warum dieses Skript und nicht ein Knopf in der Oberflaeche
#
# Die Oberflaeche ist eine SPA ohne eigenes Backend. Sie startet keinen Prozess
# und schreibt nichts auf die Platte, also kann sie auch nichts herunterladen.
# Was sie kann, tut sie: sie zeigt die Vorschlaege mit ihren gemessenen Zahlen
# und den fertigen Aufruf zum Kopieren. Der Aufruf ist dieser hier.
#
# ## Was hier wirklich passiert
#
# llama-server kann selbst laden: `-hf user/repo[:quant]` holt die Datei von
# huggingface.co in das Verzeichnis, das llama.cpp als Cache liest
# (Umgebungsvariable LLAMA_CACHE, ohne sie auf macOS $HOME/Library/Caches/
# llama.cpp). Dieses Skript setzt LLAMA_CACHE auf das Cache-Verzeichnis dieses
# Projekts, damit ein geholtes Modell in derselben Liste auftaucht wie die schon
# vorhandenen und llm/start.sh es ohne weitere Angabe findet.
#
# Der Quant ist optional; ohne ihn nimmt llama-server Q4_K_M, und wenn es den
# nicht gibt, die erste Datei des Repositories. Er wird ohne Ansehen der
# Gross- und Kleinschreibung gesucht.
#
# ## Der eine Satz, der vor jedem Lauf dasteht
#
# Dieser Befehl GEHT INS NETZ. Er ist damit die eine Stelle, an der dieses
# ansonsten abgeschottete Produkt eine fremde Adresse anspricht, und er sagt das
# vorher, samt Ziel und Ablageort. Ein Beweislauf fuehrt ihn nicht aus.

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
SERVER="$ROOT/vendor/llama/llama-server"

# Dasselbe Verzeichnis wie in llm/start.sh, mit derselben Reihenfolge. Zwei
# Vorgaben fuer denselben Ort waeren ein geholtes Modell, das in der Liste des
# Starters nicht auftaucht.
MODELS_DIR=${ATLAS_MODELS_DIR:-${LLAMA_CACHE:-$ROOT/models}}

usage() {
    echo "Aufruf: llm/fetch-model.sh <user>/<repo>[:<quant>]" >&2
    echo "        llm/fetch-model.sh --print <user>/<repo>[:<quant>]" >&2
    echo "        llm/fetch-model.sh --list" >&2
    echo "" >&2
    echo "Beispiele (die sechs gemessenen Kandidaten stehen in docs/adr/0001-modellwahl.md):" >&2
    echo "  llm/fetch-model.sh unsloth/Qwen3.5-2B-GGUF" >&2
    echo "  llm/fetch-model.sh LiquidAI/LFM2.5-1.2B-Instruct-GGUF:Q4_K_M" >&2
    echo "" >&2
    echo "Cache-Verzeichnis: $MODELS_DIR (ueber ATLAS_MODELS_DIR aenderbar)" >&2
}

PRINT_ONLY=0
case "${1:-}" in
    --list)
        echo "Cache-Verzeichnis: $MODELS_DIR"
        FOUND=0
        for CANDIDATE in "$MODELS_DIR"/*.gguf; do
            if [ -f "$CANDIDATE" ]; then
                FOUND=1
                echo "  $(basename "$CANDIDATE")"
            fi
        done
        if [ "$FOUND" -eq 0 ]; then
            echo "  (leer)"
            echo "Ein Modell holen: llm/fetch-model.sh unsloth/Qwen3.5-2B-GGUF"
        fi
        exit 0
        ;;
    --print)
        PRINT_ONLY=1
        shift
        ;;
    -h|--help|'')
        usage
        exit 2
        ;;
esac

REPO=${1:-}
if [ -z "$REPO" ]; then
    usage
    exit 2
fi

# Eine reine Formpruefung, keine Netzabfrage: user/repo, wahlweise mit :quant.
# Ob es das Repository gibt, weiss nur huggingface.co, und dieses Skript
# behauptet darueber nichts.
if ! printf '%s' "$REPO" | grep -Eq '^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+(:[A-Za-z0-9._-]+)?$'; then
    echo "Das ist keine Repo-Kennung: $REPO" >&2
    echo "Erwartet wird user/repo oder user/repo:quant, zum Beispiel" >&2
    echo "unsloth/Qwen3.5-2B-GGUF oder unsloth/Qwen3.5-2B-GGUF:Q4_K_M." >&2
    exit 2
fi

COMMAND="LLAMA_CACHE=\"$MODELS_DIR\" $SERVER -hf $REPO --no-warmup -c 512"

echo "Dieser Aufruf geht ins Netz: er laedt $REPO von huggingface.co"
echo "und legt die Datei in $MODELS_DIR ab."
echo "Die Anwendung selbst laedt nichts herunter; sie zeigt nur diesen Aufruf."
echo ""
echo "$COMMAND"

if [ "$PRINT_ONLY" -eq 1 ]; then
    exit 0
fi

if [ ! -x "$SERVER" ]; then
    echo "" >&2
    echo "llama-server fehlt: $SERVER" >&2
    echo "vendor/llama/ ist gitignoriert; Herkunft und Baubefehl stehen in vendor/llama/HERKUNFT.md." >&2
    exit 4
fi

mkdir -p "$MODELS_DIR"

# Im Vordergrund und ohne Kunstgriffe.
#
# `-hf` laedt und startet danach den Dienst; einen Modus "nur laden" gibt es
# nicht. Dieses Skript raet deshalb nicht, wann der Download fertig ist, sondern
# sagt, woran man es sieht, und ueberlaesst das Beenden dem Leser. Ein Skript,
# das nach einer geschaetzten Frist zuschlaegt, waere ein Skript, das einen
# halben Download fuer fertig erklaeren kann.
echo ""
echo "Der Server laedt jetzt und startet danach von selbst. Sobald in seiner"
echo "Ausgabe \"server is listening\" steht, ist die Datei im Cache: dann mit"
echo "Ctrl-C beenden. Die Datei bleibt liegen, und llm/start.sh findet sie."
echo ""
exec env LLAMA_CACHE="$MODELS_DIR" "$SERVER" -hf "$REPO" --no-warmup -c 512
