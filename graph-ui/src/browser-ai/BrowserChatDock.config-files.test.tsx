// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';
import { dockHarness, readerOf } from './chat-dock.fixture';

const dock = dockHarness();
const TOX = '[tox]\nminversion = 4.0\nenvlist =\n    py3\n    flake8\n\n[testenv]\nchangedir = tests\ncommands =\n    {envpython} runtests.py {posargs}\n';

/** "was macht diese datei?" on tox.ini went to the model, which wrote "Testverwendungen" (B6, B7). */
describe('what an INI file does, answered from the file (B6, B7)', () => {
    it('answers from tox.ini with its purpose and sections, without the model', async () => {
        const { props, runtime } = dock.setup();
        await dock.render({ ...props, selectionScope: 'django-demo:explore', readerContext: readerOf(TOX, 'tox.ini') }); await dock.load();
        await dock.ask('was macht diese datei?');
        expect(runtime.chat).not.toHaveBeenCalled();
        const answer = dock.answerOf(dock.last());
        expect(answer).toContain('tox.ini: INI-Konfiguration, 10 Zeilen.');
        expect(answer).toContain('tox-Konfiguration: die Testumgebungen, die tox anlegt, und die Befehle, die es in jeder ausführt.');
        expect(answer).toContain('[tox]: minversion 4.0; envlist: py3, flake8');
        expect(answer).toContain('[testenv]: changedir tests; commands {envpython} runtests.py {posargs}');
        expect(answer).toContain('Aus der Datei gelesen, nicht vom Modell erzeugt.');
        expect(dock.buttonsOf(dock.last())).toEqual(['Modell fragen']);
    });
});
