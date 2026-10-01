#!/bin/sh
# Den eigenen Sidecar beenden, und nur den eigenen.
#
# Beendet wird ausschliesslich die PID aus llm/.pid. Kein pkill auf einen
# Namen, kein "was auch immer auf 4141 lauscht": beides wuerde fremde Prozesse
# treffen, die dieses Skript nie gestartet hat. Steht in der Datei eine PID, die
# es nicht mehr gibt, wird die Datei aufgeraeumt und das gesagt.
#
# Aufruf: llm/stop.sh

set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
PID_FILE="$HERE/.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "keine PID-Datei ($PID_FILE): dieses Skript hat gerade nichts laufen."
    exit 0
fi

PID=$(cat "$PID_FILE" 2>/dev/null || echo '')
if [ -z "$PID" ]; then
    echo "PID-Datei war leer, wird entfernt."
    rm -f "$PID_FILE"
    exit 0
fi

if ! kill -0 "$PID" 2>/dev/null; then
    echo "pid $PID laeuft nicht mehr; die PID-Datei wird entfernt."
    rm -f "$PID_FILE"
    exit 0
fi

kill "$PID" 2>/dev/null || true

# Erst hoeflich, dann nicht mehr. Fuenf Sekunden reichen dem Sidecar, um seine
# Slots abzuraeumen; laenger zu warten hiesse, jedem Beweislauf eine Pause
# aufzuzwingen, die nur der Hoeflichkeit dient.
i=0
while [ "$i" -lt 50 ]; do
    if ! kill -0 "$PID" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "Sidecar beendet (pid $PID)."
        exit 0
    fi
    sleep 0.1
    i=$((i + 1))
done

echo "pid $PID reagierte nicht auf SIGTERM, sende SIGKILL." >&2
kill -9 "$PID" 2>/dev/null || true
sleep 0.3
rm -f "$PID_FILE"
echo "Sidecar hart beendet (pid $PID)."
