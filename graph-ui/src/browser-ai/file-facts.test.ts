import { describe, expect, it } from 'vitest';
import { fileFactLines, isDataFile, readerFacts } from './file-facts';
import type { BrowserChatReaderContext } from './chat-model';

/*
 * K12: the automatic Explore card of a configuration or text file shows what is read from
 * the file, counted, and nothing the model would have to guess.
 */

const reader = (path: string, text: string, kind: 'file' | 'selection' = 'file', lines?: [number, number], partial?: string): BrowserChatReaderContext => ({
    project: 'demo', path, status: 'ready',
    source: { id: 'r', text, path, project: 'demo', startLine: lines?.[0] ?? 1, startColumn: 1, endLine: lines?.[1] ?? text.split('\n').length, endColumn: 1, sourceVersion: 'sha256:1', kind,
        ...partial ? { partial } : {} },
});

describe('facts read from a configuration or text file (K12)', () => {
    it('tells data and text files from program code by their name', () => {
        for (const path of ['a.yml', 'b.yaml', 'package.json', 'pyproject.toml', 'setup.cfg', 'tox.ini', 'nginx.conf', '.env', 'app.properties', 'README.md', 'docs/index.rst',
            'LICENSE.txt', 'pom.xml', '.gitignore', '.editorconfig', '.github/workflows/ci.yml']) expect(isDataFile(path), path).toBe(true);
        for (const path of ['src/main.c', 'manage.py', 'app.ts', 'Makefile', 'run.sh', 'index.html']) expect(isDataFile(path), path).toBe(false);
    });

    it('lists the top-level keys of a YAML file with the keys right below them', () => {
        expect(fileFactLines('docker-compose.yml', 'version: "3"\nservices:\n  web:\n    image: nginx\n  db:\n    image: postgres\nvolumes:\n  data: {}\n')).toEqual([
            'File kind: YAML configuration, not program code; 8 lines.',
            'Top-level keys (3): `version`, `services` (2 keys: `web`, `db`), `volumes` (1 key: `data`).',
        ]);
    });

    it('counts a list of mappings as a list, and the keys of its items as item keys, not as keys of the parent', () => {
        // django's .pre-commit-config.yaml: one key holding 5 items, each with repo, rev and hooks.
        const preCommit = ['repos:', ...['black', 'blacken-docs', 'isort', 'flake8', 'eslint'].flatMap(hook => [
            `  - repo: https://github.com/example/${hook}`, '    rev: 1.0.0', '    hooks:', `      - id: ${hook}`])].join('\n') + '\n';
        expect(fileFactLines('.pre-commit-config.yaml', preCommit)).toEqual([
            'File kind: YAML configuration, not program code; 21 lines.',
            'Top-level keys (1): `repos` (list of 5; item keys: `repo`, `rev`, `hooks`).',
        ]);
        // The items' first key on the dash line and the rest below it; mappings that differ in their keys.
        expect(fileFactLines('steps.yml', 'steps:\n  - name: a\n    run: x\n  - name: b\n    uses: y\n    with:\n      key: 1\n')).toContain(
            'Top-level keys (1): `steps` (list of 2; item keys: `name`, `run`, `uses`, `with`).');
        // A list written at its key's own indent, next to a mapping.
        expect(fileFactLines('x.yaml', 'repos:\n- repo: a\n  rev: 1\n- repo: b\n  rev: 2\nci:\n  autofix: true\n')).toContain(
            'Top-level keys (2): `repos` (list of 2; item keys: `repo`, `rev`), `ci` (1 key: `autofix`).');
        // A list of plain values has no item keys.
        expect(fileFactLines('tags.yml', 'tags:\n  - a\n  - b\n  - c\n')).toContain('Top-level keys (1): `tags` (list of 3).');
    });

    it('reads a JSON file with JSON.parse: keys and what their values are', () => {
        expect(fileFactLines('package.json', '{"name": "demo", "private": true, "scripts": {"build": "tsc", "test": "vitest"}, "files": ["dist", "src"]}')).toEqual([
            'File kind: JSON data, not program code; 1 line.',
            'Top-level keys (4): `name` (text), `private` (true or false), `scripts` (2 keys: `build`, `test`), `files` (list of 2).',
        ]);
        expect(fileFactLines('data.json', '[1, 2, 3]')).toEqual(['File kind: JSON data, not program code; 1 line.', 'A list of 3 items.']);
        expect(fileFactLines('broken.json', '{"name": ')).toEqual(['File kind: JSON data, not program code; 1 line.', 'The text is not valid JSON, so no keys are counted.']);
    });

    it('lists the tables of a TOML file and the keys before the first one', () => {
        expect(fileFactLines('pyproject.toml', 'requires-python = ">=3.10"\n\n[project]\nname = "demo"\nversion = "1.0"\n\n[tool.ruff]\nline-length = 88\n\n[[tool.mypy.overrides]]\nmodule = "a"\n')).toEqual([
            'File kind: TOML configuration, not program code; 11 lines.',
            'Top-level keys (1): `requires-python`.',
            'Tables (3): `[project]` (2 keys), `[tool.ruff]` (1 key), `[[tool.mypy.overrides]]` (1 key).',
        ]);
    });

    it('lists the sections of an INI file, and the keys of a file without sections', () => {
        expect(fileFactLines('setup.cfg', '[metadata]\nname = demo\nversion = 1\n\n; a comment\n[options]\npackages = find:\n')).toEqual([
            'File kind: INI configuration, not program code; 7 lines.',
            'Sections (2): `[metadata]` (2 keys), `[options]` (1 key).',
        ]);
        expect(fileFactLines('.env', '# local\nDEBUG=1\nSECRET_KEY=x\n')).toContain('Keys (2): `DEBUG`, `SECRET_KEY`.');
    });

    it('names the title and sections of a Markdown file, not the lines of its code blocks', () => {
        expect(fileFactLines('README.md', '# Demo\n\nA demo.\n\n## Install\n\n```sh\n# not a heading\npip install demo\n```\n\n## Usage\n\n### Options\n')).toEqual([
            'File kind: Markdown text, not program code; 14 lines.',
            'Title: `Demo`.',
            'Sections (2): `Install`, `Usage`.',
            '1 code block.',
        ]);
    });

    it('counts the patterns of an ignore file and names the root element of an XML file', () => {
        expect(fileFactLines('.gitignore', '*.pyc\n# comment\n\n/build\n')).toEqual(['File kind: ignore patterns, not program code; 4 lines.', '2 patterns: `*.pyc`, `/build`.']);
        expect(fileFactLines('pom.xml', '<?xml version="1.0"?>\n<!-- c -->\n<project>\n  <modelVersion>4</modelVersion>\n</project>\n')).toContain('Root element: `<project>`.');
    });

    it('says nothing about program code and keeps the workflow facts for a workflow', () => {
        expect(fileFactLines('manage.py', 'import os\n')).toEqual([]);
        const workflow = 'name: CI\non: [push]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n';
        expect(fileFactLines('.github/workflows/ci.yml', workflow)).toEqual(['Workflow name: `CI`.', 'Trigger: `push`.', '1 job: `test` (runs on ubuntu-latest, 1 step).']);
    });

    it('describes a marked part or a cut excerpt as what it is', () => {
        expect(readerFacts(reader('docker-compose.yml', 'services:\n  web:\n    image: nginx\n', 'selection', [2, 4]))).toEqual([
            'Selected lines 2-4 of the file.',
            'Top-level keys (1): `services` (1 key: `web`).',
        ]);
        expect(readerFacts(reader('big.json', '{"a": 1', 'file', [1, 1], 'Only the first 6,000 characters were read.'))).toEqual([
            'File kind: JSON data, not program code.',
            'Only the first 6,000 characters were read.',
        ]);
        expect(readerFacts(reader('manage.py', 'import os\n'))).toEqual([]);
    });
});
