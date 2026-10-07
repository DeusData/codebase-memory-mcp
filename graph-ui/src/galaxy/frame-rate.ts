/**
 * Die Bildrate der Szene, einmal gemessen und von allen gelesen.
 *
 * ## Warum es diese Datei gibt
 *
 * AC9 des W10-Contracts verlangt, dass jede Einstellung, die Rechenzeit kostet,
 * ihren Effekt in GEMESSENEN Zahlen nennt: Bildrate vorher, Bildrate nachher,
 * auf dieser Maschine an derselben Szene. Damit braucht es zwei Leser fuer eine
 * Zahl, das Panel und den Beweislauf, und genau da faengt der Fehler an, gegen
 * den diese Datei geschnitten ist: eine zweite Rechnung fuer dieselbe Zahl waere
 * eine zweite Wahrheit. Gemessen wird darum an einer Stelle (der Zaehler in der
 * Szene, src/galaxy/GraphScene.tsx), abgelegt wird an einer Stelle (hier), und
 * beide Leser lesen dasselbe Fenster.
 *
 * ## Was ein Fenster ist und warum es eines ist
 *
 * Frames werden gezaehlt und ueber {@link FRAME_WINDOW_MS} gemittelt. Die
 * Alternative, den Abstand zwischen zwei Bildern zu nehmen, misst genau ein
 * Bild: eine einzelne Ruckelphase (ein Garbage-Lauf, ein Fenster, das den Fokus
 * wechselt) waere dann die Messung. Ein halbsekuendiges Fenster ist lang genug,
 * dass eine Ausreisserzeit darin untergeht, und kurz genug, dass ein Leser, der
 * einen Schalter umlegt, nicht wartet.
 *
 * ## Warum das Rauschband hier steht
 *
 * Weil eine Behauptung ohne Streuung keine Messung ist. Zwei Fenster derselben
 * Szene ohne jede Aenderung unterscheiden sich auf dieser Maschine um mehrere
 * Bilder je Sekunde; wer daraus "schneller" macht, hat das Rauschen gemessen.
 * {@link noiseBandOf} nimmt die Spanne mehrerer Fenster OHNE Aenderung, und
 * {@link verdictOf} nennt einen Unterschied innerhalb dieser Spanne genau das,
 * was er ist: kein messbarer Unterschied. Das ist kein Makel des Produkts,
 * sondern die Anforderung (AC9: "Wo eine Einstellung nichts messbar bringt,
 * sagt das Panel genau das").
 *
 * ## Und wenn gar nichts gezeichnet wird
 *
 * Dann kommt kein Fenster an, und das ist eine Auskunft und kein Wert. Der
 * Renderloop steht still, sobald das Panel zugeklappt ist (`frameloop="never"`
 * in GalaxyPanel). {@link frameRateSnapshot} meldet dann `running: false`, und
 * das Panel sagt, dass an einem stehenden Bild nichts zu messen ist, statt eine
 * Null als Bildrate zu zeigen.
 */

/** Die Laenge eines Messfensters. Siehe Kopf. */
export const FRAME_WINDOW_MS = 500;

/**
 * Wie lange ein Fenster als aktuell gilt.
 *
 * Zwei Fenster plus Luft: ein einzelnes ausgefallenes Fenster (ein langer
 * Layout-Durchgang, ein Reiterwechsel) soll nicht sofort "der Graph laeuft
 * nicht" heissen, und ein wirklich angehaltener Renderloop soll nicht sekunden-
 * lang eine Zahl von vorhin zeigen.
 */
export const FRAME_STALE_MS = 1500;

/** Wie viele Fenster aufgehoben werden. Genug fuer ein Rauschband, nicht mehr. */
export const FRAME_HISTORY = 16;

/** Ein Messfenster. */
export interface FrameSample {
    /** Bilder je Sekunde in diesem Fenster. */
    fps: number;
    /** Wie viele Bilder gezaehlt wurden. */
    frames: number;
    /** Wie lang das Fenster wirklich war, in Millisekunden. */
    windowMs: number;
    /** Wann es geschlossen wurde (Date.now). */
    at: number;
}

/**
 * Die Naht, an der ein Beweislauf die laufende Bildrate liest.
 *
 * Genau die Zahl, die auch das Panel zeigt. Sie steht auf `globalThis`, weil ein
 * Lauf im Browser keinen anderen Weg in eine React-Komponente hat, und sie wird
 * bei jedem Fenster neu geschrieben, damit sie nie eine Lage von vorhin
 * beschreibt.
 */
export interface AtlasGalaxyPerfSeam {
    /** Bilder je Sekunde im letzten Fenster. 0, solange keines geschlossen wurde. */
    fps: number;
    /** Die Bilder dieses Fensters. */
    frames: number;
    /** Die Laenge dieses Fensters in Millisekunden. */
    windowMs: number;
    /** Wie viele Fenster seit dem Laden der Seite geschlossen wurden. */
    samples: number;
    /** Ob das letzte Fenster jung genug ist, um von einem laufenden Graphen zu sprechen. */
    running: boolean;
    /** Wie lange das letzte Fenster her ist, in Millisekunden. */
    sinceLastMs: number;
    /** Ab wann ein Fenster als veraltet gilt. */
    staleAfterMs: number;
    /** Die Szene, an der gemessen wurde. */
    nodes: number;
    edges: number;
    /** Der Bildratendeckel, der gerade gilt. 0 heisst: keiner. */
    cap: number;
    /** Die Spanne der letzten Fenster: das Rauschen dieser Maschine. */
    noiseBand: number;
    /** Die letzten Fenster, aeltestes zuerst. */
    recent: number[];
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasGalaxyPerf: AtlasGalaxyPerfSeam | undefined;
}

/** Was die Szene ueber sich mitgeteilt hat, ausser der Bildrate. */
interface SceneFacts {
    nodes: number;
    edges: number;
    cap: number;
}

const history: FrameSample[] = [];
const scene: SceneFacts = { nodes: 0, edges: 0, cap: 0 };
let samples = 0;
const listeners = new Set<() => void>();
let staleCheck: ReturnType<typeof setTimeout> | undefined;

/**
 * Ein einziger Wecker, der die Naht ehrlich haelt, wenn das Zeichnen aufhoert.
 *
 * `running` haengt an der Uhr und nicht an einem Ereignis: hoert die Szene auf
 * zu zeichnen, kommt kein Fenster mehr, und ohne diesen Wecker bliebe in
 * `globalThis.__atlasGalaxyPerf` fuer immer die letzte Bildrate mit
 * `running: true` stehen. Ein Beweislauf, der kurz nach dem Zuklappen liest,
 * bekaeme dann eine Zahl ueber ein Bild, das niemand mehr zeichnet.
 *
 * Ein Wecker und kein Intervall: jedes neue Fenster stellt ihn neu, und wenn
 * die Fenster ausbleiben, laeuft er genau einmal ab und schreibt die Naht ein
 * letztes Mal. Danach steht dort `running: false`, und es tickt nichts mehr.
 */
function scheduleStaleCheck(): void {
    if (typeof setTimeout !== 'function') {
        return;
    }
    if (staleCheck !== undefined) {
        clearTimeout(staleCheck);
    }
    staleCheck = setTimeout(() => {
        staleCheck = undefined;
        frameRateSnapshot();
    }, FRAME_STALE_MS + 100);
    // In Node haelt ein Timer den Prozess am Leben. Im Browser gibt es unref
    // nicht, und dort waere es auch egal.
    (staleCheck as unknown as { unref?: () => void }).unref?.();
}

/** Bilder je Sekunde aus einem gezaehlten Fenster. Total ergibt 0 und nicht NaN. */
export function frameRateOf(frames: number, windowMs: number): number {
    if (!Number.isFinite(frames) || !Number.isFinite(windowMs) || windowMs <= 0 || frames <= 0) {
        return 0;
    }
    return (frames * 1000) / windowMs;
}

/**
 * Das Rauschband einer Reihe von Fenstern: die Spanne zwischen dem hoechsten und
 * dem niedrigsten.
 *
 * Die Spanne und nicht die Standardabweichung, und das ist eine Entscheidung
 * gegen die elegantere Zahl: eine Streuung ueber vier Fenster ist eine Statistik
 * mit vier Werten, und der Leser soll die Schranke nachrechnen koennen, indem er
 * auf die Zahlen sieht. Weniger als zwei Fenster ergeben keine Spanne, und dann
 * gibt es kein Band, sondern ein `undefined`: ein Band von 0 waere die
 * Behauptung, diese Maschine messe exakt.
 */
export function noiseBandOf(values: readonly number[]): number | undefined {
    if (values.length < 2) {
        return undefined;
    }
    return Math.max(...values) - Math.min(...values);
}

/** Wie ein Vorher-Nachher-Vergleich ausgegangen ist. */
export type FrameVerdict = 'not-measured' | 'not-drawing' | 'no-difference' | 'higher' | 'lower';

/**
 * Das Urteil ueber zwei Messungen, mit dem Rauschen als Schranke.
 *
 * Die Reihenfolge der Faelle ist die Aussage: ohne Zahlen gibt es kein Urteil,
 * ohne Band gibt es keins, und innerhalb des Bandes gibt es ausdruecklich
 * "kein messbarer Unterschied" statt einer Richtung. Erst was das Rauschen
 * dieser Maschine ueberschreitet, bekommt eine Richtung, und auch die ist eine
 * Beobachtung ueber zwei Zahlen und kein Versprechen ueber die naechste.
 */
export function verdictOf(
    before: number | undefined,
    after: number | undefined,
    band: number | undefined,
): FrameVerdict {
    if (before === undefined || after === undefined || before <= 0 || after <= 0) {
        return 'not-measured';
    }
    if (band === undefined) {
        return 'not-measured';
    }
    const difference = after - before;
    if (Math.abs(difference) <= band) {
        return 'no-difference';
    }
    return difference > 0 ? 'higher' : 'lower';
}

/** Der Mittelwert einer Reihe. Leer ergibt `undefined` und nicht 0. */
export function meanOf(values: readonly number[]): number | undefined {
    if (values.length === 0) {
        return undefined;
    }
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

/** Die Fenster, die nicht aelter sind als `ms`. */
export function recentSamples(ms: number, now = Date.now()): FrameSample[] {
    return history.filter((sample) => now - sample.at <= ms);
}

/** Alle aufgehobenen Fenster, aeltestes zuerst. */
export function frameHistory(): FrameSample[] {
    return [...history];
}

/**
 * Ein geschlossenes Fenster melden.
 *
 * Der einzige Weg, auf dem eine Bildrate in dieses Modul kommt. Er wird aus der
 * Szene gerufen und aus nichts sonst; ein zweiter Melder waere ein zweiter
 * Zaehler, und die Naht wuerde von zwei Zaehlern beschrieben.
 */
export function recordFrameWindow(frames: number, windowMs: number, now = Date.now()): FrameSample {
    const sample: FrameSample = { fps: frameRateOf(frames, windowMs), frames, windowMs, at: now };
    history.push(sample);
    while (history.length > FRAME_HISTORY) {
        history.shift();
    }
    samples += 1;
    frameRateSnapshot(now);
    scheduleStaleCheck();
    for (const listener of listeners) {
        listener();
    }
    return sample;
}

/** Was die Szene ueber sich sagt: wie gross sie ist und mit welchem Deckel sie laeuft. */
export function recordSceneFacts(nodes: number, edges: number, cap: number): void {
    scene.nodes = nodes;
    scene.edges = edges;
    scene.cap = cap;
}

/**
 * Der Stand, wie er jetzt ist, und derselbe Stand auf `globalThis`.
 *
 * Beides in einem Aufruf, damit die Naht nicht hinter dem zurueckbleiben kann,
 * was das Panel zeigt: `running` haengt an der Uhr und nicht an einem Ereignis,
 * also waere eine Naht, die nur beim Messen geschrieben wird, nach dem
 * Zuklappen des Panels fuer immer `true`.
 */
export function frameRateSnapshot(now = Date.now()): AtlasGalaxyPerfSeam {
    const last = history[history.length - 1];
    const sinceLastMs = last === undefined ? Number.POSITIVE_INFINITY : now - last.at;
    const running = last !== undefined && sinceLastMs <= FRAME_STALE_MS;
    const recent = history.map((sample) => sample.fps);
    const seam: AtlasGalaxyPerfSeam = {
        fps: running && last !== undefined ? last.fps : 0,
        frames: last?.frames ?? 0,
        windowMs: last?.windowMs ?? 0,
        samples,
        running,
        sinceLastMs: Number.isFinite(sinceLastMs) ? sinceLastMs : -1,
        staleAfterMs: FRAME_STALE_MS,
        nodes: scene.nodes,
        edges: scene.edges,
        cap: scene.cap,
        noiseBand: noiseBandOf(recent) ?? 0,
        recent,
    };
    globalThis.__atlasGalaxyPerf = seam;
    return seam;
}

/** Sich benachrichtigen lassen, sobald ein Fenster geschlossen wurde. */
export function subscribeFrameRate(listener: () => void): () => void {
    listeners.add(listener);
    return () => {
        listeners.delete(listener);
    };
}

/** Alles vergessen. Nur fuer Tests: zwei Testfaelle teilen sich sonst eine Historie. */
export function resetFrameRate(): void {
    history.length = 0;
    samples = 0;
    scene.nodes = 0;
    scene.edges = 0;
    scene.cap = 0;
    if (staleCheck !== undefined) {
        clearTimeout(staleCheck);
        staleCheck = undefined;
    }
}
