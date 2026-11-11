#!/bin/sh
# Den lokalen Modell-Sidecar starten. Ein Prozess, ein Port, eine PID-Datei.
#
# Warum es dieses Skript gibt und nicht einen Knopf in der Oberflaeche: die SPA
# hat kein eigenes Backend und kann keinen Prozess starten (ADR 0001,
# Abschnitt "Engine-Entscheidung"). Ein Knopf, der einen Prozess verspricht,
# waere eine Luege in Knopfform. Der Manager im Browser erkennt, zeigt und
# leitet an; gestartet wird hier, von Hand oder aus einem Beweislauf.
#
# ## Die Vorgabe ist seit W10 der Router, nicht ein Dateiname
#
# Ohne Argument faehrt dieses Skript llama-server ueber ein CACHE-VERZEICHNIS:
#
#     --models-dir "$ATLAS_MODELS_DIR" --models-max N --models-autoload
#
# Damit listet /v1/models, was in dem Verzeichnis liegt, und das Feld "model"
# einer Chat-Anfrage waehlt daraus aus. Das Einstellungen-Panel kann so zwischen
# Modellen umschalten, ohne dass jemand einen Prozess neu startet, und ohne dass
# die Oberflaeche einen Prozess anfassen muesste, was sie nicht darf.
#
# Der Grund fuer die Umstellung steht im Nachtrag W10 von docs/adr/0001: das
# Programm liefert kein Modell mehr aus und nennt auch keins als gesetzt. Welche
# Datei laeuft, entscheidet der Leser.
#
# ## Woher das Cache-Verzeichnis kommt
#
# ATLAS_MODELS_DIR, sonst $LLAMA_CACHE (die Umgebungsvariable, die llama.cpp
# selbst fuer seinen Cache liest), sonst <repo>/models. Das letzte ist kein
# Notbehelf: models/ IST das lokale Cache-Verzeichnis dieses Projekts. Es ist
# gitignoriert, sein Inhalt gehoert dem Leser, und llm/fetch-model.sh laedt in
# genau dieses Verzeichnis, damit ein geholtes Modell in derselben Liste
# auftaucht wie die schon vorhandenen.
#
# ## Aufruf
#
#     llm/start.sh                      Router ueber das Cache-Verzeichnis
#     llm/start.sh <pfad>.gguf          eine bestimmte Datei
#     llm/start.sh <name>               eine Datei aus dem Cache-Verzeichnis
#     llm/start.sh hf:user/repo[:quant] direkt von Hugging Face (geht ins Netz)
#     llm/start.sh class-a              die Reproduktionsgriffe der ADR-Eval
#     llm/start.sh --print-command [..] zeigt die Befehlszeile und startet nichts
#
# Aus der Umgebung: ATLAS_MODEL (wie das Argument), ATLAS_CTX (Kontextfenster),
# ATLAS_MODELS_DIR, ATLAS_MODELS_MAX (wie viele Instanzen gleichzeitig).
#
# ## --print-command, und warum es das gibt
#
# Ein Beweislauf soll pruefen koennen, WOMIT dieses Skript den Server aufruft,
# ohne dafuer ein Modell zu laden. Ohne diesen Schalter muesste er Gigabyte in
# den Speicher heben, um eine Zeichenkette zu sehen. Er schreibt die Zeile auf
# stdout, startet nichts und fasst weder Port noch PID-Datei an.
#
# Idempotent: laeuft der eigene Sidecar schon, passiert nichts und das Skript
# endet mit 0. Lauscht etwas ANDERES auf dem Port, wird nichts gestartet und
# nichts abgeschossen; das Skript sagt, was es vorgefunden hat, und endet mit 3.

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
PORT=4141
HOST=127.0.0.1
PID_FILE="$HERE/.pid"
LOG_FILE="$HERE/llama-server.log"
SERVER="$ROOT/vendor/llama/llama-server"

PRINT_ONLY=0
if [ "${1:-}" = "--print-command" ]; then
    PRINT_ONLY=1
    shift
fi

# Das Cache-Verzeichnis. Siehe Kopf: die Reihenfolge ist Argument-Umgebung,
# llama.cpp-Umgebung, Projektverzeichnis.
MODELS_DIR=${ATLAS_MODELS_DIR:-${LLAMA_CACHE:-$ROOT/models}}
# Wie viele Instanzen der Router gleichzeitig geladen halten darf. Zwei, weil
# genau zwei gebraucht werden, um ohne Neustart umzuschalten: die laufende und
# die gewaehlte. Jede weitere kostet den Speicher eines ganzen Modells fuer eine
# Auswahl, die niemand getroffen hat. Die Vorgabe von llama-server waere 4.
MODELS_MAX=${ATLAS_MODELS_MAX:-2}

CHOICE=${1:-${ATLAS_MODEL:-}}

# ---------------------------------------------------------------------------
# Die Reproduktionsgriffe der ADR-Eval.
#
# Das ist KEINE Vorgabe mehr, und die Reihenfolge in dieser Datei sagt das: die
# Vorgabe ist der Router weiter unten. Diese sechs Namen sind die Griffe, mit
# denen die Laeufe von W5 wiederholbar bleiben (tools/smoke-w5a.mjs faehrt
# class-a und erwartet Qwen3.5-2B mit Kontext 3072, tools/smoke-w5b.mjs und
# tools/smoke-w6-full.mjs fahren die anderen, tools/eval-check.mjs erwartet die
# Dateien unter models/). Sie sind Belege und keine Empfehlung; welches Modell
# gut ist, sagt die Eval-Tabelle im ADR, und welches laeuft, sagt der Leser.
#
# Sie loesen darum ausdruecklich gegen <repo>/models auf und nicht gegen
# $ATLAS_MODELS_DIR: die Dateien der Eval liegen dort, und ein gesetztes
# LLAMA_CACHE wuerde sonst einen Reproduktionslauf ins Leere schicken.
#
# Die Wahlen hiessen bis zum 2026-08-29 "1b" und "4b". Befund 17 des
# unabhaengigen Audits: der Sieger der Klasse A ist ein 2B-Modell, und eine
# Beschriftung, die es "1b" nennt, sagt etwas Falsches ueber die Datei, die sie
# laedt. Die Klasse haengt ohnehin am Kontextfenster und nicht an der
# Parameterzahl (siehe modelClassOf in src/llm/sidecar.ts). Die alten Namen
# bleiben als Alias gueltig.
# ---------------------------------------------------------------------------
ALIAS_FILE=''
ALIAS_CTX=''
case "$CHOICE" in
    class-a|1b)                 ALIAS_FILE=Qwen3.5-2B-Q4_K_M.gguf                   ; ALIAS_CTX=3072 ;;
    class-a-lfm|1b-lfm)         ALIAS_FILE=LFM2.5-1.2B-Instruct-Q4_K_M.gguf         ; ALIAS_CTX=3072 ;;
    class-a-minicpm|1b-minicpm) ALIAS_FILE=MiniCPM5-1B-Q4_K_M.gguf                  ; ALIAS_CTX=3072 ;;
    class-a-coder|1b-coder)     ALIAS_FILE=qwen2.5-coder-1.5b-instruct-q4_k_m.gguf  ; ALIAS_CTX=3072 ;;
    class-b|4b)                 ALIAS_FILE=Qwen3.5-4B-Q4_K_M.gguf                   ; ALIAS_CTX=8192 ;;
    class-b-gemma|4b-gemma)     ALIAS_FILE=gemma-4-E4B-it-Q4_K_M.gguf               ; ALIAS_CTX=8192 ;;
esac

case "$CHOICE" in
    1b|1b-lfm|1b-minicpm|1b-coder|4b|4b-gemma)
        echo "Hinweis: \"$CHOICE\" ist der alte Name dieser Wahl. Sie heisst jetzt nach ihrer" >&2
        echo "Klasse; die Klassen sind class-a (Kontext 3072) und class-b (Kontext 8192)." >&2
        ;;
esac

# Der Kontext: die Umgebung schlaegt den Alias, der Alias die Vorgabe. 3072 ist
# das Budget der Klasse A aus PLAN Paragraph 5 und damit die konservative Wahl
# fuer ein Modell, ueber das dieses Skript nichts weiss.
CTX=${ATLAS_CTX:-${ALIAS_CTX:-3072}}

# ---------------------------------------------------------------------------
# Die Argumente des Servers, in genau einer Variablen.
#
# Sie entstehen hier und werden weiter unten entweder gedruckt oder gefahren.
# Zwei Stellen, an denen die Zeile entsteht, waeren zwei Zeilen: der Beweislauf
# saehe die eine und der Leser bekaeme die andere.
# ---------------------------------------------------------------------------
MODE=''
DESCRIPTION=''
set -- --host "$HOST" --port "$PORT"

if [ -n "$ALIAS_FILE" ]; then
    MODE=eval-alias
    MODEL_PATH="$ROOT/models/$ALIAS_FILE"
    if [ "$PRINT_ONLY" -eq 0 ] && [ ! -f "$MODEL_PATH" ]; then
        echo "Modell fehlt: $MODEL_PATH" >&2
        echo "\"$CHOICE\" ist ein Reproduktionsgriff der ADR-Eval und erwartet die Datei aus" >&2
        echo "models/SHA256SUMS. Holen: llm/fetch-model.sh --list" >&2
        exit 4
    fi
    # Der relative Pfad, byte-identisch zu dem, was dieses Skript vor W10
    # uebergeben hat: /props meldet ihn woertlich zurueck, und die Beweislaeufe
    # von W5 und W6 lesen ihn dort.
    set -- "$@" -m "models/$ALIAS_FILE" -c "$CTX"
    DESCRIPTION="$ALIAS_FILE (Reproduktionsgriff $CHOICE), Kontext $CTX"
elif [ -z "$CHOICE" ]; then
    MODE=router
    set -- "$@" --models-dir "$MODELS_DIR" --models-max "$MODELS_MAX" --models-autoload -c "$CTX"
    DESCRIPTION="Router ueber $MODELS_DIR (hoechstens $MODELS_MAX geladen), Kontext $CTX"
else
    case "$CHOICE" in
        hf:*)
            MODE=hf
            REPO=${CHOICE#hf:}
            set -- "$@" -hf "$REPO" -c "$CTX"
            DESCRIPTION="$REPO von huggingface.co, Kontext $CTX"
            ;;
        *)
            MODE=file
            # Ein Pfad, oder ein Name im Cache-Verzeichnis. Erst wird gefragt,
            # ob die Datei so existiert; erst dann wird sie im Cache gesucht.
            if [ -f "$CHOICE" ]; then
                MODEL_PATH="$CHOICE"
            elif [ -f "$MODELS_DIR/$CHOICE" ]; then
                MODEL_PATH="$MODELS_DIR/$CHOICE"
            elif [ -f "$MODELS_DIR/$CHOICE.gguf" ]; then
                MODEL_PATH="$MODELS_DIR/$CHOICE.gguf"
            else
                echo "unbekannte Modellwahl: $CHOICE" >&2
                echo "Erwartet wird eines von:" >&2
                echo "  ein Pfad auf eine .gguf-Datei" >&2
                echo "  ein Dateiname aus $MODELS_DIR" >&2
                echo "  hf:user/repo[:quant] (laedt von huggingface.co)" >&2
                echo "  ein Reproduktionsgriff der ADR-Eval, davon" >&2
                # Diese Zeile ist eine Naht: src/llm/strings.test.ts liest sie aus
                # diesem Skript und vergleicht sie mit LLM_MODEL_CHOICES. Die
                # Liste steht damit einmal in der Wahrheit und einmal in der
                # Anzeige, und ein Test haelt beide zusammen (Audit-Befund 17).
                echo "erlaubt: class-a | class-a-lfm | class-a-minicpm | class-a-coder | class-b | class-b-gemma" >&2
                echo "  (die alten Namen 1b, 1b-lfm, 1b-minicpm, 1b-coder, 4b, 4b-gemma gelten weiter)" >&2
                echo "Ohne Argument faehrt dieses Skript den Router ueber $MODELS_DIR." >&2
                exit 2
            fi
            set -- "$@" -m "$MODEL_PATH" -c "$CTX"
            DESCRIPTION="$MODEL_PATH, Kontext $CTX"
            ;;
    esac
fi

if [ "$PRINT_ONLY" -eq 1 ]; then
    printf '%s' "$SERVER"
    for ARG in "$@"; do
        printf ' %s' "$ARG"
    done
    printf '\n'
    exit 0
fi

if [ ! -x "$SERVER" ]; then
    echo "llama-server fehlt: $SERVER" >&2
    echo "vendor/llama/ ist gitignoriert; Herkunft und Baubefehl stehen in vendor/llama/HERKUNFT.md." >&2
    exit 4
fi

# Ein leeres Cache-Verzeichnis ist kein Start, sondern eine Auskunft.
#
# Ohne diese Pruefung liefe der Router an und antwortete auf jede Anfrage mit
# einer leeren Modell-Liste, und aus der Ferne saehe das aus wie ein Defekt.
# Gesagt wird darum, was fehlt und wie man es holt.
if [ "$MODE" = router ]; then
    # Ein Glob und kein find: `find -quit` gibt es nicht ueberall, und die Frage
    # ist ohnehin nur, ob EINE Datei da ist.
    HAVE_GGUF=0
    for CANDIDATE in "$MODELS_DIR"/*.gguf; do
        if [ -f "$CANDIDATE" ]; then
            HAVE_GGUF=1
            break
        fi
    done
    if [ "$HAVE_GGUF" -eq 0 ]; then
        echo "Kein Modell im Cache-Verzeichnis: $MODELS_DIR" >&2
        echo "Dieses Programm liefert keines mit. So holen Sie eines (geht ins Netz):" >&2
        echo "  llm/fetch-model.sh unsloth/Qwen3.5-2B-GGUF" >&2
        echo "oder direkt:" >&2
        echo "  LLAMA_CACHE=\"$MODELS_DIR\" $SERVER -hf unsloth/Qwen3.5-2B-GGUF:Q4_K_M" >&2
        echo "Ein anderes Verzeichnis: ATLAS_MODELS_DIR=/pfad/zum/cache llm/start.sh" >&2
        echo "Die Vorschlaege mit ihren gemessenen Zahlen stehen im Einstellungen-Panel." >&2
        exit 5
    fi
fi

# Laeuft der eigene Prozess schon? Die PID-Datei allein reicht als Auskunft
# nicht: sie ueberlebt einen Absturz. Gefragt wird deshalb das Betriebssystem.
if [ -f "$PID_FILE" ]; then
    OLD=$(cat "$PID_FILE" 2>/dev/null || echo '')
    if [ -n "$OLD" ] && kill -0 "$OLD" 2>/dev/null; then
        echo "Sidecar laeuft bereits (pid $OLD) auf $HOST:$PORT. Nichts zu tun."
        exit 0
    fi
    echo "verwaiste PID-Datei ($OLD) wird entfernt."
    rm -f "$PID_FILE"
fi

# Der Port gehoert dem Sidecar. Wer sonst dort lauscht, wird nicht abgeschossen:
# ein Startskript, das fremde Prozesse beendet, ist ein Startskript, dem man
# nicht mehr zusehen kann.
if command -v lsof >/dev/null 2>&1; then
    BUSY=$(lsof -ti "tcp:$PORT" 2>/dev/null || true)
    if [ -n "$BUSY" ]; then
        echo "Port $PORT ist belegt (pid $BUSY), aber nicht von diesem Skript." >&2
        echo "Nichts gestartet und nichts beendet." >&2
        exit 3
    fi
fi

if [ "$MODE" = hf ]; then
    echo "Dieser Aufruf geht ins Netz (huggingface.co) und laedt nach ${LLAMA_CACHE:-$MODELS_DIR}."
fi

cd "$ROOT"
nohup "$SERVER" "$@" > "$LOG_FILE" 2>&1 &
PID=$!
echo "$PID" > "$PID_FILE"

echo "Sidecar gestartet: pid $PID, $DESCRIPTION, http://$HOST:$PORT"
if [ "$MODE" = router ]; then
    echo "Die Modelle im Cache: curl -s http://$HOST:$PORT/v1/models"
    echo "Umschalten ohne Neustart: im Einstellungen-Panel der Oberflaeche."
fi
echo "Bereitschaft: curl -s http://$HOST:$PORT/health  ->  {\"status\":\"ok\"}"
echo "Log: $LOG_FILE"
echo "Beenden: $HERE/stop.sh"
