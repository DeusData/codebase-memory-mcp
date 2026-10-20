import { describe, expect, it } from 'vitest';
import { fileOutline } from './file-outline';

/** django's pyproject.toml, its first table and the start of [project]. */
const PYPROJECT = `[build-system]
requires = ["setuptools>=75.8.1"]
build-backend = "setuptools.build_meta"

[project]
name = "Django"
requires-python = ">= 3.10"
dependencies = [
    "asgiref>=3.8.1",
    "sqlparse>=0.3.1",
    "tzdata; sys_platform == 'win32'",
]
authors = [
  {name = "Django Software Foundation", email = "foundation@djangoproject.com"},
]
license = {text = "BSD-3-Clause"}
`;

/** "was macht diese datei?" on pyproject.toml showed "dependencies [; authors [" for arrays over several lines. */
describe('TOML arrays over several lines in the outline', () => {
    it('lists the items of a multi-line array, and keeps commas inside an inline table', () => {
        const outline = fileOutline('pyproject.toml', PYPROJECT, 'en')!;
        expect(outline).toContain('- `[project]`: name `Django`; requires-python `>= 3.10`; dependencies: `asgiref>=3.8.1`, `sqlparse>=0.3.1`, `tzdata; sys_platform == \'win32\'`; '
            + 'authors: `{name = "Django Software Foundation", email = "foundation@djangoproject.com"}`; license `{text = "BSD-3-Clause"}`');
        expect(outline).not.toMatch(/dependencies \[|authors \[/);
        expect(outline).toContain('- `[build-system]`: requires: `setuptools>=75.8.1`; build-backend `setuptools.build_meta`');
    });
});
