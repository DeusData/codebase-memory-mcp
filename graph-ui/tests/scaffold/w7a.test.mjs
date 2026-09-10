// W7a acceptance tests: nothing in the chrome promises what this product
// cannot do, and [?]help opens a real, offline help page that names the
// limits first. Run: node --test tests/scaffold/w7a.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');

test('AC1: the menu row carries no decoys', () => {
  const h = JSON.parse(read('verification/w7/help.json'));
  const menu = h.menuItems.join(' ');
  for (const gone of ['[f]ile', '[e]dit', '[v]iew', '[t]erminal']) {
    assert.ok(!menu.includes(gone), `${gone} muss aus der Menuezeile verschwunden sein`);
  }
  for (const kept of ['[a]tlas', '[?]help']) {
    assert.ok(menu.includes(kept), `${kept} muss bleiben`);
  }
  assert.equal(h.noNotWiredTooltip, true, 'der not-wired-Tooltip darf nicht mehr existieren');
  assert.equal(h.everyMenuItemWired, true, 'jeder Menuepunkt muss verdrahtet sein');
  const catalog = read('src/i18n/messages.ts');
  assert.ok(!catalog.includes('notWired'), 'notWired muss aus dem Katalog verschwinden');
});

test('AC2: the help page is real, offline and names the limits', () => {
  const h = JSON.parse(read('verification/w7/help.json'));
  assert.equal(h.helpOpensByClick, true);
  assert.equal(h.helpOpensByKey, true, 'die Taste ? muss die Hilfe oeffnen');
  assert.equal(h.helpEscCloses, true);
  assert.ok(h.helpSectionsShown >= 6, `>= 6 Hilfe-Abschnitte erwartet, war ${h.helpSectionsShown}`);
  assert.equal(h.helpListsEveryShortcut, true,
    'die Hilfe muss jede verdrahtete Taste nennen, und keine, die es nicht gibt');
  assert.equal(h.helpNamesLimits, true,
    'read-only, kein Ausfuehren und kein Terminal muessen woertlich dastehen');
  assert.equal(h.helpHasNoWebLinks, true, 'die Hilfe darf offline nicht ins Leere verlinken');
  assert.equal(h.darkStyled, true);
  assert.equal(h.overlapViolations, 0);
  assert.equal(h.clippingViolations, 0);
  assert.ok(h.port >= 4370, `Port >= 4370 erwartet, war ${h.port}`);
  assert.equal(h.leftoverProcesses, 0);
  for (const shot of ['menu.png', 'help.png']) {
    const p = join(ROOT, 'verification', 'w7', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
});

test('AC3: no sentence promises a feature for later', () => {
  const strings = read('src/twin/strings.ts');
  assert.ok(!strings.includes('Import runtime traces'),
    'der Verweis auf einen nicht existierenden Menuepunkt muss weg');
  assert.ok(!/ARRIVES_LATER|ARRIVE_LATER/.test(strings)
    || /Ausnahme|exception/i.test(strings),
    'die arrives-later-Saetze muessen ersetzt oder als Ausnahme begruendet sein');
  const h = JSON.parse(read('verification/w7/help.json'));
  assert.equal(h.twinRuntimeSentenceHonest, true,
    'die Runtime-Zeile muss auf BUG-Wizard und CLI zeigen, nicht auf ein fehlendes Menue');
  assert.equal(h.twinChangesSentenceHonest, true,
    'die Changes-Zeile muss auf das Impact-Panel zeigen');
});

test('AC8/AC9/AC10: typing always lands in the command line', () => {
  const h = JSON.parse(read('verification/w7/help.json'));
  assert.equal(h.typingWithoutClickReachesLine, true,
    'Tippen ohne vorherigen Klick muss sichtbar in der Zeile ankommen');
  assert.equal(h.typingNeverOpensPanelAccidentally, true,
    'ein Wortanfang darf niemals unbemerkt ein Panel oeffnen');
  assert.equal(h.focusVisible, true, 'die Zeile muss zeigen, ob sie den Fokus hat');
  assert.equal(h.focusKeyWorks, true, 'eine Taste muss die Zeile global fokussieren');
  assert.equal(h.oneCharHintInResultRow, true,
    'bei einem Zeichen muss der Hinweis dort stehen, wo der Nutzer hinsieht');
  assert.equal(h.noNativeTooltipOnCommandLine, true,
    'der native Tooltip darf die Zeile nicht mehr verdecken');
});

test('AC4: the promise scan is clean', () => {
  const p = JSON.parse(read('verification/w7/promises.json'));
  assert.equal(p.promiseHits, 0, 'kein Versprechen-Muster im Produktpfad');
  assert.equal(p.deadAffordances, 0, 'kein klickbares Element ohne Handler');
  assert.ok(p.filesScanned >= 20, `>= 20 gescannte Dateien erwartet, war ${p.filesScanned}`);
  assert.ok(existsSync(join(ROOT, 'tools/promise-scan.mjs')), 'tools/promise-scan.mjs fehlt');
});

test('AC7: wiring and net deny', () => {
  const nd = JSON.parse(read('verification/w7/netdeny.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w7a/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  for (const script of ['smoke:w7a', 'check:promises']) {
    assert.ok(pkg.scripts?.[script], `Script ${script} fehlt`);
  }
  const all = { ...(pkg.dependencies ?? {}), ...(pkg.devDependencies ?? {}) };
  for (const [name, version] of Object.entries(all)) {
    assert.ok(!/[\^~><|*x]/.test(version), `${name} muss exakt gepinnt sein, war ${version}`);
  }
});
