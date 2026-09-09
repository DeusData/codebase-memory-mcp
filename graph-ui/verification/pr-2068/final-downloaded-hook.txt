#!/usr/bin/env python3
"""Record tool metadata in a local outbox and POST it to the existing daemon.

Configure ATLAS_PROJECT to the indexed project name and ATLAS_DAEMON_URL when
using a nondefault daemon port. No bridge, listener or external transmission.
Pending events survive downtime and retry on the next hook or --flush.
The hook always returns zero so observing work cannot break the tool call.
"""
import json
import os
import sys
import time
import sqlite3
import uuid
import urllib.request
import urllib.parse
import argparse
import pathlib
import shlex
import tempfile

DEFAULT_FILE = os.path.join(os.path.expanduser('~'), '.atlas-trace', 'events.jsonl')

# Was von einem Befehl oder einem Suchmuster aufgeschrieben wird. Laenger waere
# in einem Instrument von 320 Pixeln ohnehin nicht zu lesen, und kuerzer
# verloere die Datei, um die es geht.
DETAIL_CAP = 180


def trace_file():
    return os.environ.get('ATLAS_TRACE_FILE') or DEFAULT_FILE


def open_outbox():
    path = trace_file() + '.outbox.sqlite3'
    os.makedirs(os.path.dirname(os.path.abspath(path)), mode=0o700, exist_ok=True)
    db = sqlite3.connect(path, timeout=0.5)
    os.chmod(path, 0o600)
    db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS counters(run TEXT PRIMARY KEY, seq INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS pending(id INTEGER PRIMARY KEY, project TEXT NOT NULL,
                                            payload TEXT NOT NULL);
    """)
    return db


def enqueue(db, project, row):
    # Counter allocation and enqueue are one SQLite transaction: parallel hooks
    # cannot assign the same run/sequence pair and silently discard an event.
    with db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('INSERT INTO counters VALUES(?,1) ON CONFLICT(run) DO UPDATE SET seq=seq+1',
                   (row['run'],))
        row['seq'] = db.execute('SELECT seq FROM counters WHERE run=?', (row['run'],)).fetchone()[0]
        db.execute('INSERT INTO pending(project,payload) VALUES(?,?)',
                   (project, json.dumps(row, ensure_ascii=False)))
        # A prolonged daemon outage cannot fill the disk without bound. Sequence
        # gaps remain visible when older undelivered metadata expires.
        db.execute('DELETE FROM pending WHERE id IN (SELECT id FROM pending ORDER BY id DESC LIMIT -1 OFFSET 10000)')


def flush(db):
    origin = os.environ.get('ATLAS_DAEMON_URL', 'http://127.0.0.1:9749')
    parsed = urllib.parse.urlsplit(origin)
    if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost')
            or parsed.username or parsed.password or parsed.path not in ('', '/')
            or parsed.query or parsed.fragment):
        return  # Never forward local metadata to an external endpoint.
    first = db.execute('SELECT project FROM pending ORDER BY id LIMIT 1').fetchone()
    if not first:
        return
    rows = db.execute('SELECT id,payload FROM pending WHERE project=? ORDER BY id LIMIT 100',
                      first).fetchall()
    data = json.dumps({'project': first[0], 'events': [json.loads(row[1]) for row in rows]}).encode()
    request = urllib.request.Request(origin.rstrip('/') + '/api/agent-events', data=data,
                                     headers={'Content-Type': 'application/json'})
    # No environment proxy, redirects, or second server; post only to loopback.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(request, timeout=0.75) as response:
        result = json.load(response)
        if response.status != 200 or result.get('accepted', 0) + result.get('duplicates', 0) != len(rows):
            return
    with db:
        db.executemany('DELETE FROM pending WHERE id=?', [(row[0],) for row in rows])


def line_span(file_path, args):
    """Use an explicit requested source range, never infer it after an edit.

    PostToolUse runs after mutation. Searching old_string could locate a different,
    unchanged occurrence and falsely attribute that line to the observed edit.
    """
    offset = args.get('offset')
    limit = args.get('limit')
    if isinstance(offset, int) and not isinstance(offset, bool) and offset > 0:
        if isinstance(limit, int) and not isinstance(limit, bool) and limit > 0:
            return [offset, offset + limit - 1]
        return [offset, offset]

    return None


def daemon_origin(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != 'http' or parsed.hostname not in ('127.0.0.1', 'localhost')
            or parsed.username or parsed.password or parsed.path not in ('', '/')
            or parsed.query or parsed.fragment):
        raise ValueError('The daemon URL must be a loopback HTTP origin.')
    return value.rstrip('/')


def install_claude(root, project, origin):
    """Explicit, project-local install; preserve unrelated settings and hooks.

    No package downloads or network requests. Refuse symlinks, malformed settings,
    and conflicting older installations rather than replace user configuration.
    """
    root = pathlib.Path(root).resolve(strict=True)
    if not root.is_dir() or not project or len(project.encode()) > 255:
        raise ValueError('Choose the indexed repository root and exact project name.')
    origin = daemon_origin(origin)
    directory = root / '.claude'
    hook_dir = directory / 'hooks'
    settings = directory / 'settings.local.json'
    target = hook_dir / 'cbm-atlas-trace.py'
    if any(path.is_symlink() for path in (directory, hook_dir, settings, target)):
        raise ValueError('Refusing symlinked hook or settings paths.')
    original = settings.read_bytes() if settings.exists() else None
    config = json.loads(original) if original is not None else {}
    if not isinstance(config, dict):
        raise ValueError('Existing settings must be a JSON object.')
    hooks = config.setdefault('hooks', {})
    if not isinstance(hooks, dict):
        raise ValueError('Existing hooks must be an object.')
    entries = hooks.setdefault('PostToolUse', [])
    if not isinstance(entries, list):
        raise ValueError('Existing PostToolUse hooks must be an array.')
    command = ('ATLAS_PROJECT=' + shlex.quote(project) + ' ATLAS_AGENT_NAME=' + shlex.quote('Claude Code')
               + ' ATLAS_DAEMON_URL=' + shlex.quote(origin)
               + ' python3 ' + shlex.quote(str(target)))
    proposed = {'matcher': '', 'hooks': [{'type': 'command', 'command': command, 'timeout': 3}]}
    installed = proposed in entries
    for entry in entries:
        for handler in entry.get('hooks', []) if isinstance(entry, dict) else []:
            if not isinstance(handler, dict):
                continue
            try:
                words = shlex.split(str(handler.get('command', '')))
            except ValueError:
                words = []
            references_target = any(word in (str(target), '.claude/hooks/cbm-atlas-trace.py',
                                             './.claude/hooks/cbm-atlas-trace.py') for word in words)
            if references_target and entry != proposed:
                raise ValueError('An existing Atlas hook differs. Review its matcher, command and timeout before changing it.')
    source = pathlib.Path(__file__).read_bytes()
    if target.exists() and target.read_bytes() != source:
        raise ValueError('An existing Atlas hook file differs. Review it before replacing it.')
    if not installed:
        entries.append(proposed)
    encoded = (json.dumps(config, indent=2, ensure_ascii=False) + '\n').encode()
    # A changed file is never overwritten after our read. Directory operations
    # happen only after the complete, reviewable configuration was validated.
    directory.mkdir(mode=0o700, exist_ok=True)
    hook_dir.mkdir(mode=0o700, exist_ok=True)
    if (settings.read_bytes() if settings.exists() else None) != original:
        raise ValueError('Settings changed during setup; retry after reviewing them.')
    created_hook = not target.exists()
    if created_hook:
        with target.open('xb') as handle:
            handle.write(source)
        target.chmod(0o600)
    if not installed:
        descriptor, temporary = tempfile.mkstemp(prefix='.cbm-atlas-settings-', dir=directory)
        try:
            with os.fdopen(descriptor, 'wb') as handle:
                handle.write(encoded)
            if (settings.read_bytes() if settings.exists() else None) != original:
                raise ValueError('Settings changed during setup; nothing was replaced.')
            os.replace(temporary, settings)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    return {'installed': True, 'changed': not installed or created_hook,
            'settings': str(settings), 'hook': str(target), 'project': project,
            'daemon': origin, 'next': 'Start a new Claude Code session in this repository, perform a tool call, then load activity.'}


def setup_main():
    parser = argparse.ArgumentParser(description='Install the local Code Atlas Claude Code tool hook.')
    parser.add_argument('--install-claude', action='store_true', required=True)
    parser.add_argument('--root', required=True, help='Exact indexed repository root')
    parser.add_argument('--project', required=True, help='Exact indexed project name')
    parser.add_argument('--daemon-url', default='http://127.0.0.1:9749')
    args = parser.parse_args()
    try:
        result = install_claude(args.root, args.project, args.daemon_url)
    except Exception as error:
        print('Atlas setup: ' + str(error), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0


def main():
    if '--flush' in sys.argv:
        try:
            with open_outbox() as db:
                flush(db)
        except Exception:
            pass
        return 0
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
        run = str(payload.get('session_id') or uuid.uuid4())
        root = str(payload.get('cwd') or os.getcwd())
        project = os.environ.get('ATLAS_PROJECT', os.path.basename(root.rstrip(os.sep)))

        row = {
            'ts': int(time.time() * 1000),
            'agent': os.environ.get('ATLAS_AGENT_NAME', 'agent'),
            'run': run,
            'seq': 0,
            'phase': 'end',
            'tool': tool,
            'path': str(file_path or args.get('path') or ''),
            'detail': str(detail)[:DETAIL_CAP],
            'source': 'tool-hook',
        }
        span = line_span(file_path, args) if tool == 'Read' else None
        if span is not None:
            row['lines'] = span

        if os.path.isabs(row['path']):
            row['path'] = os.path.relpath(row['path'], root)
        with open_outbox() as db:
            enqueue(db, project, row)
            flush(db)
    except Exception:
        pass
    return 0


if __name__ == '__main__':
    sys.exit(setup_main() if '--install-claude' in sys.argv else main())
