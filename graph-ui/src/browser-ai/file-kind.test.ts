import { describe, expect, it } from 'vitest';
import { fileKind } from './file-kind';

describe('the kind of an open file (K12)', () => {
    it.each([
        ['.github/workflows/new_contributor_pr.yml', 'GitHub Actions workflow (YAML configuration, not program code)'],
        ['docker-compose.yaml', 'YAML configuration, not program code'],
        ['package.json', 'JSON data, not program code'],
        ['pyproject.toml', 'TOML configuration, not program code'],
        ['tox.ini', 'INI configuration, not program code'],
        ['README.md', 'Markdown text, not program code'],
        ['docs/intro.rst', 'reStructuredText, not program code'],
        ['django/db/models/query.py', 'Python source file'],
        ['graph-ui/src/App.tsx', 'TypeScript source file'],
        ['src/main.c', 'C source file'],
        ['django/conf/project_template/manage.py-tpl', 'Python source template'],
    ])('names %s', (path, kind) => {
        expect(fileKind(path)).toBe(kind);
    });

    it('says nothing for an unknown extension', () => {
        expect(fileKind('LICENSE')).toBeUndefined();
        expect(fileKind('data.bin')).toBeUndefined();
    });
});
