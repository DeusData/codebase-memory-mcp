// W12d acceptance: the Settings scroller keeps the cue defined for it.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const css = readFileSync(resolve(ROOT, 'src/styles/terminal.css'), 'utf8');

test('AC3: Settings keeps the shared local scroll cue', () => {
  const blocks = [...css.matchAll(/\.atlas-settings\s*\{([^}]*)\}/g)].map((match) => match[1]);
  const positioned = blocks.find((block) => /position:\s*absolute/.test(block));
  assert.ok(positioned, 'der eigentliche .atlas-settings-Block fehlt');
  assert.match(positioned, /background-color:\s*var\(--atlas-bg\)/,
    'die Grundfarbe muss den vorher definierten Scroll-Verlauf stehen lassen');
  assert.doesNotMatch(positioned, /(^|[;\s])background\s*:/,
    'background-Shorthand loescht background-image und background-attachment');
});
