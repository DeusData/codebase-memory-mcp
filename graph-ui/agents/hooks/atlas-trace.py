#!/usr/bin/env python3
"""Eine JSON-Zeile je Werkzeugaufruf, im Format, das die Bruecke liest.

Aufruf: als PostToolUse-Hook. Das Werkzeug reicht sein Ereignis als JSON auf
der Standardeingabe herein; dieses Skript haengt eine Zeile an die
Ereignisdatei und endet.

## Drei Regeln, und keine davon ist Geschmack

1. **Es endet immer mit 0, unter allen Umstaenden.** Ein Protokoll, das die
   Arbeit aufhalten kann, ist schlimmer als kein Protokoll: ein Hook, der einen
   Fehler wirft, bricht den Werkzeugaufruf ab, den er nur beobachten sollte.
   Jeder Zweig hier faellt darum still durch.

2. **Keine Dateiinhalte, nie.** Was in eine Zeile kommt, ist: wann, wer, welcher
   Lauf, welche Nummer, welches Werkzeug, welcher Pfad, welcher Zeilenbereich
   und der Befehl beziehungsweise das Suchmuster. Kein Quelltext, kein
   Ausgabetext, kein Diff. Das ist die Grenze des Formats und nicht eine
   Einstellung daran: wer die Ereignisdatei liest, liest, WO gearbeitet wurde,
   und nicht, WAS dort steht.

3. **Der Zeilenbereich wird gemessen, nicht geraten.** Bei `Read` steht er im
   Aufruf (`offset`, `limit`). Bei `Edit` steht er nirgends, also wird er
   gesucht: die Stelle des ersetzten Textes in der Datei, umgerechnet in
   Zeilennummern. Dafuer wird die Datei gelesen und sofort wieder vergessen;
   in die Zeile kommen zwei Zahlen und kein Zeichen des Inhalts. Findet sich
   die Stelle nicht, fehlt das Feld, statt eine Zahl zu erfinden.

## Warum `seq` fortlaufend ist und nicht aus der Uhr kommt

Weil die Oberflaeche daran erkennt, dass ihr etwas fehlt. Bei einem Abriss
nimmt sie mit der zuletzt gesehenen Nummer je Lauf wieder auf, und aus der
Differenz wird die Meldung "2 Ereignisse verpasst". Eine Nummer aus der Uhr
(etwa Millisekunden modulo einer Million) sieht fortlaufend aus, laeuft aber
ueber und springt zurueck; die Oberflaeche muesste dann raten, ob eine Luecke
eine Luecke ist. Der Zaehler steht deshalb neben der Ereignisdatei in
`<datei>.seq` und gilt je Lauf.

## Einrichtung

In der Hook-Konfiguration des Agenten-Werkzeugs (eine `settings.json` in
dessen Heimatverzeichnis):

    {
      "hooks": {
        "PostToolUse": [
          { "matcher": "*",
            "hooks": [{ "type": "command",
                        "command": "python3 <repo>/agents/hooks/atlas-trace.py" }] }
        ]
      }
    }

Wohin geschrieben wird, sagt `ATLAS_TRACE_FILE`; ohne die Variable ist es
`~/.atlas-trace/events.jsonl`. Wie der Agent heisst, sagt `ATLAS_AGENT_NAME`;
ohne sie steht dort `agent`.
"""
import json
import os
import sys
import time

DEFAULT_FILE = os.path.join(os.path.expanduser('~'), '.atlas-trace', 'events.jsonl')

# Was von einem Befehl oder einem Suchmuster aufgeschrieben wird. Laenger waere
# in einem Instrument von 320 Pixeln ohnehin nicht zu lesen, und kuerzer
# verloere die Datei, um die es geht.
DETAIL_CAP = 180


def trace_file():
    return os.environ.get('ATLAS_TRACE_FILE') or DEFAULT_FILE


def next_seq(path, run):
    """Die naechste Nummer dieses Laufs, fortlaufend und ohne Ueberlauf.

    Der Zaehler liegt als kleines JSON neben der Ereignisdatei. Zwei Agenten,
    die gleichzeitig schreiben, koennen sich hier ueberholen; das ist genau der
    Fall, den die Oberflaeche als Luecke meldet, und eine Sperre dafuer waere
    ein Protokoll, das die Arbeit anhaelt.
    """
    counter_path = path + '.seq'
    counters = {}
    try:
        with open(counter_path, 'r', encoding='utf-8') as handle:
            loaded = json.load(handle)
        if isinstance(loaded, dict):
            counters = loaded
    except Exception:
        counters = {}
    value = counters.get(run)
    number = (value if isinstance(value, int) and value > 0 else 0) + 1
    counters[run] = number
    try:
        with open(counter_path, 'w', encoding='utf-8') as handle:
            json.dump(counters, handle)
    except Exception:
        pass
    return number


def line_span(file_path, args):
    """Der beruehrte Zeilenbereich, gemessen oder gar nicht.

    `Read` nennt ihn selbst. `Edit` und `Write` nennen ihn nicht, also wird die
    Stelle des alten Textes in der Datei gesucht und in Zeilennummern
    umgerechnet. Nichts davon kommt in die Zeile ausser den zwei Zahlen.
    """
    offset = args.get('offset')
    limit = args.get('limit')
    if isinstance(offset, int) and offset > 0:
        if isinstance(limit, int) and limit > 0:
            return [offset, offset + limit - 1]
        return [offset, offset]

    needle = args.get('old_string')
    if not isinstance(needle, str) or len(needle) == 0 or not file_path:
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as handle:
            body = handle.read()
    except Exception:
        return None
    at = body.find(needle)
    if at < 0:
        return None
    start = body.count('\n', 0, at) + 1
    return [start, start + needle.count('\n')]


def main():
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0
    try:
        path = trace_file()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        tool = payload.get('tool_name') or ''
        args = payload.get('tool_input') or {}
        if not isinstance(args, dict):
            args = {}
        file_path = args.get('file_path') or args.get('notebook_path') or ''
        detail = (
            args.get('command')
            or args.get('pattern')
            or args.get('description')
            or ''
        )
        run = str(payload.get('session_id') or 'session')[:8]

        row = {
            'ts': int(time.time() * 1000),
            'agent': os.environ.get('ATLAS_AGENT_NAME', 'agent'),
            'run': run,
            'seq': next_seq(path, run),
            'phase': 'end',
            'tool': tool,
            'path': str(file_path or args.get('path') or ''),
            'detail': str(detail)[:DETAIL_CAP],
        }
        span = line_span(file_path, args)
        if span is not None:
            row['lines'] = span

        with open(path, 'a', encoding='utf-8') as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + '\n')
    except Exception:
        pass
    return 0


if __name__ == '__main__':
    sys.exit(main())
