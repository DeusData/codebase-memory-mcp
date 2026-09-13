"""Reproducible local producer tests; no external network or agent accounts."""
import concurrent.futures
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('atlas_trace', Path(__file__).with_name('atlas-trace.py'))
hook = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hook)


class TraceTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.environment = patch.dict(os.environ, {
            'ATLAS_TRACE_FILE': os.path.join(self.directory.name, 'events'),
            'ATLAS_DAEMON_URL': 'http://127.0.0.1:9749',
        })
        self.environment.start()

    def tearDown(self):
        self.environment.stop()
        self.directory.cleanup()

    def record(self, number):
        with hook.open_outbox() as db:
            hook.enqueue(db, 'fixture', {'run': 'concurrent-run', 'seq': 0, 'ts': number,
                                        'agent': 'fixture', 'tool': 'Read', 'phase': 'end'})

    def test_concurrent_hooks_allocate_unique_sequences_and_keep_pending_after_failure(self):
        with hook.open_outbox():
            pass
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(self.record, range(20)))
        with hook.open_outbox() as db:
            rows = db.execute('SELECT payload FROM pending').fetchall()
            self.assertEqual(sorted(json.loads(row[0])['seq'] for row in rows), list(range(1, 21)))
            with patch.object(hook.urllib.request, 'build_opener', side_effect=OSError('offline')):
                with self.assertRaises(OSError):
                    hook.flush(db)
            self.assertEqual(db.execute('SELECT count(*) FROM pending').fetchone()[0], 20)

    def test_external_destinations_and_credentials_are_never_contacted(self):
        self.record(1)
        for origin in ('https://example.com', 'http://example.com',
                       'http://127.0.0.1.evil.test', 'http://secret@localhost:9749',
                       'http://localhost:9749/redirect', 'http://localhost:9749?secret=1'):
            with patch.dict(os.environ, {'ATLAS_DAEMON_URL': origin}), \
                    patch.object(hook.urllib.request, 'build_opener') as network, \
                    hook.open_outbox() as db:
                hook.flush(db)
                network.assert_not_called()
                self.assertEqual(db.execute('SELECT count(*) FROM pending').fetchone()[0], 1)

    def test_explicit_setup_preserves_settings_is_idempotent_and_escapes_names(self):
        root = Path(self.directory.name) / "repo with 'quote'"
        (root / '.claude').mkdir(parents=True)
        settings = root / '.claude/settings.local.json'
        original = {'permissions': {'allow': ['Read']}, 'hooks': {'PostToolUse': [
            {'matcher': 'Read', 'hooks': [{'type': 'command', 'command': 'existing-hook'}]}]}}
        settings.write_text(json.dumps(original))
        with patch.object(hook.urllib.request, 'build_opener') as network:
            result = hook.install_claude(root, "project'; echo bad", 'http://127.0.0.1:9749')
            again = hook.install_claude(root, "project'; echo bad", 'http://127.0.0.1:9749')
            network.assert_not_called()
        self.assertTrue(result['changed'])
        self.assertFalse(again['changed'])
        installed = json.loads(settings.read_text())
        self.assertEqual(installed['permissions'], original['permissions'])
        self.assertEqual(installed['hooks']['PostToolUse'][0], original['hooks']['PostToolUse'][0])
        self.assertEqual(len(installed['hooks']['PostToolUse']), 2)
        command = installed['hooks']['PostToolUse'][1]['hooks'][0]['command']
        self.assertEqual(hook.shlex.split(command)[0], "ATLAS_PROJECT=project'; echo bad")
        self.assertEqual(hook.shlex.split(command)[-1], str(root.resolve() / '.claude/hooks/cbm-atlas-trace.py'))
        before = settings.read_bytes()
        with self.assertRaises(ValueError):
            hook.install_claude(root, 'another-project', 'http://127.0.0.1:9749')
        self.assertEqual(settings.read_bytes(), before)

    def test_setup_refuses_symlinks_malformed_settings_and_external_daemons_before_writes(self):
        root = Path(self.directory.name) / 'repo'
        root.mkdir()
        with self.assertRaises(ValueError):
            hook.install_claude(root, 'fixture', 'https://example.com')
        self.assertFalse((root / '.claude').exists())
        (root / '.claude').mkdir()
        settings = root / '.claude/settings.local.json'
        settings.write_text('malformed user data')
        with self.assertRaises(ValueError):
            hook.install_claude(root, 'fixture', 'http://127.0.0.1:9749')
        self.assertEqual(settings.read_text(), 'malformed user data')
        settings.unlink()
        target = root / 'external-settings.json'
        target.write_text('{}')
        settings.symlink_to(target)
        with self.assertRaises(ValueError):
            hook.install_claude(root, 'fixture', 'http://127.0.0.1:9749')
        self.assertEqual(target.read_text(), '{}')
        self.assertFalse((root / '.claude/hooks').exists())

    def test_setup_refuses_a_second_execution_with_a_different_matcher_or_timeout(self):
        root = Path(self.directory.name) / 'TEST fixture'
        root.mkdir()
        hook.install_claude(root, 'fixture', 'http://127.0.0.1:9749')
        settings = root / '.claude/settings.local.json'
        pristine = json.loads(settings.read_text())
        for change in ('matcher', 'timeout', 'relative-command'):
            config = json.loads(json.dumps(pristine))
            entry = config['hooks']['PostToolUse'][0]
            if change == 'matcher':
                entry['matcher'] = 'Read'
            elif change == 'timeout':
                entry['hooks'][0]['timeout'] = 5
            else:
                entry['hooks'][0]['command'] = 'python3 .claude/hooks/cbm-atlas-trace.py'
            settings.write_text(json.dumps(config))
            before = settings.read_bytes()
            with self.assertRaisesRegex(ValueError, 'existing Atlas hook differs'):
                hook.install_claude(root, 'fixture', 'http://127.0.0.1:9749')
            self.assertEqual(settings.read_bytes(), before)
            self.assertEqual(len(json.loads(settings.read_text())['hooks']['PostToolUse']), 1)

    def test_post_edit_range_is_unknown_instead_of_an_unmodified_duplicate_occurrence(self):
        source = Path(self.directory.name) / 'edited.txt'
        source.write_text('new value\nold value\n')
        with patch('builtins.open', side_effect=AssertionError('Metadata hook must not read source')):
            self.assertIsNone(hook.line_span(str(source), {'old_string': 'old value', 'new_string': 'new value'}))
            self.assertEqual(hook.line_span(str(source), {'offset': 8, 'limit': 3}), [8, 10])
            self.assertIsNone(hook.line_span(str(source), {'offset': True}))

    def test_acknowledged_duplicate_retry_removes_only_acknowledged_rows(self):
        self.record(1)
        self.record(2)
        class Response:
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def read(self): return b'{"accepted":0,"duplicates":2}'
        class Opener:
            def open(self, request, timeout):
                self_body = json.loads(request.data)
                assert request.full_url == 'http://127.0.0.1:9749/api/agent-events'
                assert self_body['project'] == 'fixture'
                assert len(self_body['events']) == 2
                return Response()
        with patch.object(hook.urllib.request, 'build_opener', return_value=Opener()), \
                hook.open_outbox() as db:
            hook.flush(db)
            self.assertEqual(db.execute('SELECT count(*) FROM pending').fetchone()[0], 0)


if __name__ == '__main__':
    unittest.main()
