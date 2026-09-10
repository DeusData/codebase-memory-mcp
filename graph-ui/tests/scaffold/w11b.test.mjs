// W11b acceptance tests: an agent does not teleport, its path stays readable
// behind it, three filmic states have a rule each, and the cinema fills the
// screen. Run: node --test tests/scaffold/w11b.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync, statSync, readdirSync } from 'node:fs';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const read = (p) => readFileSync(join(ROOT, p), 'utf8');
const motion = () => JSON.parse(read('verification/w11/motion.json'));

test('AC1: the move is a path, not a jump', () => {
  const m = motion();
  assert.equal(m.transitionIsCurved, true,
    'in der Mitte muss der Koerper messbar ABSEITS der geraden Verbindung liegen');
  assert.ok(Array.isArray(m.transitionSamples) && m.transitionSamples.length >= 3,
    'die drei gemessenen Positionen gehoeren ins Artefakt');
  assert.ok(m.transitionMs >= 250 && m.transitionMs <= 900,
    `Uebergang im erwarteten Band, war ${m.transitionMs}`);
  assert.equal(m.cometTailPresent, true, 'der kurze Schweif muss da sein');
  assert.equal(m.neverInTwoPlaces, true,
    'kein Bild, in dem derselbe Koerper an zwei Orten steht');
});

test('AC2: the trail shows the way, and is never mistaken for an edge', () => {
  const m = motion();
  assert.ok(m.trailNodes >= 6 && m.trailNodes <= 10,
    `zwischen 6 und 10 Knoten erwartet, waren ${m.trailNodes}`);
  assert.equal(m.trailBelowEdges, true, 'die Spur liegt unter den echten Graphkanten');
  assert.equal(m.trailDashed, true, 'und ist gestrichelt, weil sie keine Beziehung ist');
  assert.equal(m.trailInLegend, true, 'der Unterschied steht in der Legende');
  assert.equal(m.trailWindowSwitchable, true, '60s, 5m, 15m, unbegrenzt');
  assert.equal(m.trailsToggleOff, true, 'TRAILS schaltet sie ganz ab');
});

test('AC3: three filmic states, each with a rule behind it', () => {
  const m = motion();
  assert.equal(m.sameNodeDifferentRadii, true,
    'zwei Agenten am selben Symbol umkreisen es auf verschiedenen Radien');
  assert.equal(m.radiiDeterministic, true,
    'die Radien haengen an der Kennung, damit dasselbe Bild gleich aussieht');
  assert.equal(m.burstMakesOneWave, true,
    'fuenf Schreibereignisse in zwei Sekunden ergeben GENAU EINE Welle');
  assert.equal(m.followSpringNoOvershoot, true,
    'die Kamera folgt weich, ohne ueber die gemessene Schwelle hinauszuschiessen');
  assert.equal(m.followLineShowsMeasuredOnly, true,
    'die eingeblendete Zeile nennt nur Gemessenes: Akteur, Art, Symbol, Zeilen');
});

test('AC4: the timeline is honest about what it shows', () => {
  const m = motion();
  assert.ok(m.timelineTracksPerActor >= 2, 'eine Spur je Akteur');
  assert.equal(m.timelinePauseKeepsEvents, true,
    'die Pause haelt das Nachlaufen an, ohne Ereignisse zu verlieren');
  assert.equal(m.timelineScrubMarkedAsReplay, true,
    'ein Sprung in die Vergangenheit ist als Wiedergabe gekennzeichnet');
});

test('AC5: cinema fills the screen and gives the camera back', () => {
  const m = motion();
  assert.equal(m.cinemaFillsViewport, true, 'der Graph nimmt den ganzen Schirm');
  assert.equal(m.cinemaHasTicker, true, 'der Ereignis-Ticker im Klartext');
  assert.equal(m.cinemaEscapeKeepsCamera, true,
    'Escape fuehrt zurueck, und die Kameralage bleibt erhalten');
  assert.ok(Array.isArray(m.cinemaSizes) && m.cinemaSizes.length >= 2,
    'in mindestens zwei Fenstergroessen gemessen');
  assert.ok(m.cinemaSizes.every((s) => s.overlapViolations === 0 && s.clippingViolations === 0),
    'auch im Vollbild darf nichts kollidieren');
});

test('AC6/AC7: quiet when nothing happens, and it stays fluid', () => {
  const m = motion();
  assert.equal(m.idleHasNoAnimation, true,
    'ohne Ereignisse keine Bewegung: kein Pulsieren ins Nichts');
  assert.equal(m.idleAgentFadesNotDisappears, true,
    'ein Agent ohne Ereignis wird blass, verschwindet aber nicht');
  assert.ok(Number.isFinite(m.framesPerSecondMin) && m.framesPerSecondMin > 0,
    'die Untergrenze der Bildrate muss gemessen sein');
  assert.ok(Number.isInteger(m.drawnBodiesCap) && m.drawnBodiesCap > 0,
    'die Obergrenze gezeichneter Koerper gehoert ins Artefakt');
  assert.equal(m.capReported, true,
    'wird der Deckel erreicht, sagt das Instrument es, statt still das Aelteste fallen zu lassen');
});

test('AC7b: every expensive effect can be switched off, centrally', () => {
  const m = motion();
  assert.equal(m.effectsToggleable, true,
    'Schweife, Spuren, Wellen und Zeitstrahl haben je einen Schalter in den Einstellungen');
  assert.equal(m.thriftProfileKeepsCinemaUsable, true,
    'das Sparprofil schaltet sie gemeinsam ab, und der Kinomodus laeuft ruhiger weiter');
});

test('AC7c: motion is proven as a frame series, and the sheet is looked at', () => {
  const m = motion();
  assert.ok(m.frameSeriesCount >= 12,
    `mindestens 12 Einzelbilder erwartet, waren ${m.frameSeriesCount}`);
  assert.equal(m.contactSheetWritten, true, 'der Kontaktabzug muss geschrieben sein');
  assert.equal(m.videoWritten, true, 'die Aufnahme fuer den Menschen ebenso');
  assert.equal(m.framesDifferBetweenSteps, true,
    'eine Bildserie ohne Aenderung zwischen zwei Bildern ist ein Befund, kein Erfolg');
  const sheet = join(ROOT, 'verification', 'w11', 'contact-sheet.png');
  assert.ok(existsSync(sheet), 'contact-sheet.png fehlt');
  assert.ok(statSync(sheet).size > 30 * 1024, 'der Kontaktabzug ist verdaechtig klein');
  const video = join(ROOT, 'verification', 'w11', 'live.webm');
  assert.ok(existsSync(video), 'live.webm fehlt');
  const frames = readdirSync(join(ROOT, 'verification', 'w11', 'frames'));
  assert.ok(frames.length >= 12, `mindestens 12 Einzelbilder erwartet, waren ${frames.length}`);
});

test('AC8/AC9: proof run and wiring', () => {
  const m = motion();
  assert.equal(m.overlapViolations, 0);
  assert.equal(m.clippingViolations, 0);
  assert.equal(m.cutWithoutHint, 0);
  assert.ok(m.port >= 4520, `Port >= 4520 erwartet, war ${m.port}`);
  assert.equal(m.leftoverProcesses, 0);
  for (const shot of ['cinema.png', 'trails.png', 'follow.png', 'timeline.png']) {
    const p = join(ROOT, 'verification', 'w11', shot);
    assert.ok(existsSync(p), `${shot} fehlt`);
    assert.ok(statSync(p).size > 30 * 1024, `${shot} verdaechtig klein`);
  }
  const nd = JSON.parse(read('verification/w11/netdeny-w11b.json'));
  assert.equal(nd.outboundViolations, 0);
  assert.ok(/smoke-w11b/.test(nd.command));
  const pkg = JSON.parse(read('package.json'));
  assert.ok(pkg.scripts?.['smoke:w11b'], 'Script smoke:w11b fehlt');
});
