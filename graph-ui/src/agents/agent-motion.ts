/**
 * Die Bewegung: wie ein Koerper von einem Symbol zum naechsten kommt, wie er
 * atmet, und was hinter ihm stehen bleibt.
 *
 * Alles hier ist reine Rechnung ohne Zustand und ohne DOM. Es steht in einer
 * eigenen Datei, weil genau diese Zahlen im Beweislauf noch einmal
 * nachgerechnet werden: eine Bahn, die nur in einem `useFrame` existiert, ist
 * nicht pruefbar, und eine zweite Formel daneben waere eine zweite Wahrheit
 * ueber dieselbe Bewegung.
 *
 * ## Die eine Regel, die diese Datei zusammenhaelt
 *
 * **Bewegung entsteht aus Ereignissen, nie aus der Uhr allein.** Ein Ortswechsel
 * ist die Folge eines Ereignisses, das einen anderen Knoten nennt. Der Puls ist
 * die Zahl der Ereignisse der letzten Minute. Eine Welle ist ein Schreib-Bruch.
 * Eine Spur sind besuchte Knoten. Kommt nichts mehr, hoert alles auf: der
 * Koerper steht, blass, und das ist die Auskunft "hier arbeitet gerade niemand"
 * statt einer Leerlauf-Animation, die Arbeit behauptet, die nicht stattfindet.
 *
 * ## Warum der Uebergang gebogen ist
 *
 * Weil man an einer Geraden nicht sieht, ob sie eine Bewegung war. Zwei Koerper,
 * die zwischen denselben zwei Symbolen hin und her springen, zeichnen dieselbe
 * Strecke; auf einer gebogenen Bahn sieht man die Richtung, in die geflogen
 * wurde, und der kurze Schweif dahinter sagt, wie schnell. Die Kruemmung ist
 * darum keine Verzierung, sondern das, was den Weg lesbar macht: der Beweislauf
 * misst sie als Abstand der Bahnmitte von der geraden Verbindung.
 */

/** Ein Punkt im Raum der Szene. */
export interface MotionPoint {
    x: number;
    y: number;
    z: number;
}

/**
 * Wie lange ein Ortswechsel dauert, in Millisekunden.
 *
 * Die Zahl kommt aus der Gegenrede zum Entwurf (etwa 450 ms) und sie ist eine
 * Wahl mit einer Begruendung: kuerzer, und der Flug ist ein Zucken, das man
 * nicht verfolgen kann; laenger, und der Koerper haengt zwischen zwei Symbolen,
 * waehrend das naechste Ereignis schon da ist. Die Aufzeichnungen dieses
 * Projekts haben Abstaende ab etwa 700 ms zwischen zwei Ereignissen desselben
 * Akteurs; 450 ms passen dazwischen, ohne dass zwei Fluege einander einholen.
 */
export const TRANSITION_MS = 450;

/**
 * Wie weit die Bahn von der geraden Verbindung abweicht.
 *
 * Als Anteil der Sehnenlaenge und nicht in Welteinheiten: ein fester Bogen
 * waere bei einem Sprung ueber die halbe Galaxie ein Strich und bei zwei
 * benachbarten Symbolen eine Schleife. Der Steuerpunkt liegt in der Mitte der
 * Sehne, senkrecht dazu um `TRANSITION_BEND * Sehnenlaenge` versetzt; die
 * groesste Abweichung der quadratischen Bezierkurve von der Sehne ist die
 * Haelfte davon, also 14 Prozent der Sehnenlaenge.
 */
export const TRANSITION_BEND = 0.28;

/** Wie viele Punkte der Kometenschweif traegt. */
export const COMET_TAIL_POINTS = 12;

/** Wie lange der Schweif nach dem Ankommen noch nachlaeuft, in Millisekunden. */
export const COMET_TAIL_FADE_MS = 220;

/**
 * Hoechstens so viele Knoten stehen in der Spur eines Akteurs.
 *
 * Die Gegenrede nennt sechs bis zehn. Zehn ist der Deckel, sechs ist keine
 * Untergrenze, die dieses Modul herstellen koennte: wer erst drei Symbole
 * beruehrt hat, hat drei, und drei erfundene dazu waeren ein Weg, den niemand
 * gegangen ist.
 */
export const TRAIL_NODE_LIMIT = 10;

/** Hoechstens so viele Spursegmente werden insgesamt gezeichnet. */
export const TRAIL_SEGMENT_CAP = 120;

/** Das Fenster, aus dem der Puls seine Zahl nimmt. */
export const PULSE_WINDOW_MS = 60000;

/** Ab so vielen Ereignissen im Fenster schlaegt der Puls am schnellsten. */
export const PULSE_BUSY_EVENTS = 12;

/** Die kuerzeste Pulsdauer, in Millisekunden. Viel Arbeit. */
export const PULSE_FAST_MS = 700;

/** Die laengste Pulsdauer. Ein einzelnes Ereignis im Fenster. */
export const PULSE_SLOW_MS = 2600;

/** Der kleinste Ausschlag, als Anteil der Koerpergroesse. */
export const PULSE_MIN_AMPLITUDE = 0.14;

/** Der groesste Ausschlag. */
export const PULSE_MAX_AMPLITUDE = 0.55;

/**
 * Ab wann ein Akteur als ruhig gilt, in Millisekunden.
 *
 * Eine Minute, und die Zahl steht so im Contract (AC6). Was danach passiert,
 * ist der ganze Punkt: der Koerper haelt an, wird blass und rutscht im
 * Instrument nach unten. Er verschwindet nicht, denn sein Lauf laeuft weiter
 * (siehe ACTIVE_WINDOW_MS in agent-store.ts), und ein Koerper, der bei der
 * ersten Pause verschwaende, waere die Behauptung, der Agent sei fertig.
 */
export const IDLE_MS = 60000;

/**
 * Wie weit zwei Schreibereignisse auseinanderliegen duerfen, um ein Bruch zu
 * sein.
 *
 * Ein Schreib-Bruch ist eine Folge von Aenderungen an DEMSELBEN Knoten, bei der
 * zwischen zwei Aenderungen weniger als diese Zeit liegt. Fuenf Aenderungen in
 * zwei Sekunden sind damit ein Bruch und ergeben EINE Welle; fuenf Aenderungen
 * ueber eine Minute verteilt sind fuenf Brueche und ergeben fuenf. Der
 * Unterschied ist genau der, den ein Leser sieht: einmal arbeitet jemand an
 * einer Stelle, einmal kommt er immer wieder zurueck.
 */
export const BURST_GAP_MS = 2500;

/** Wie lange die Welle eines Bruchs laeuft, in Millisekunden. */
export const WAVE_LIFETIME_MS = 1600;

/**
 * Hoechstens so viele Koerper werden gleichzeitig gezeichnet.
 *
 * Der Deckel ist eine Zusicherung ueber die Rechenzeit und keine ueber die
 * Wahrheit: wer nicht gezeichnet wird, steht weiter im Instrument, und das
 * Instrument sagt, wie viele es sind. Still das Aelteste fallen zu lassen waere
 * ein Bild, dem man nicht ansieht, dass es unvollstaendig ist.
 */
export const DRAWN_BODIES_CAP = 8;

/**
 * Die Periode der Federdaempfung der FOLLOW-Kamera, in Millisekunden.
 *
 * Die Feder ist KRITISCH gedaempft (Daempfungsgrad genau 1). Das ist der
 * schnellste Anflug, der ohne Ueberschwingen auskommt, und "ohne
 * Ueberschwingen" ist hier keine Vorliebe: eine Kamera, die ueber ihr Ziel
 * hinausfaehrt und zurueckkommt, zeigt eine Bewegung, die der Agent nicht
 * gemacht hat.
 */
export const FOLLOW_SPRING_PERIOD_MS = 760;

/* ------------------------------------------------------------- die Bahn --- */

/** Weich anfangen, weich aufhoeren. */
export function easeInOutCubic(t: number): number {
    const clamped = t <= 0 ? 0 : t >= 1 ? 1 : t;
    return clamped < 0.5
        ? 4 * clamped * clamped * clamped
        : 1 - Math.pow(-2 * clamped + 2, 3) / 2;
}

/**
 * Auf welche Seite dieser Akteur ausweicht.
 *
 * Aus der Kennung und nicht aus dem Zufall: zwei Akteure, die denselben Weg
 * fliegen, sollen ihn nicht uebereinander fliegen, und derselbe Akteur soll
 * zweimal dieselbe Bahn nehmen. Sonst waere jeder zweite Flug ein anderes Bild
 * derselben Bewegung.
 */
export function bendSignOf(hash: number): number {
    return hash % 2 === 0 ? 1 : -1;
}

/**
 * Der Steuerpunkt der Bahn: die Mitte der Sehne, senkrecht dazu versetzt.
 *
 * Senkrecht in der Bildebene (x/y). Die Tiefe wird linear interpoliert: ein
 * Bogen durch die z-Achse waere in der Ansicht von vorn keine Kruemmung,
 * sondern ein Koerper, der kurz naeher steht.
 */
export function controlPointOf(from: MotionPoint, to: MotionPoint, sign: number): MotionPoint {
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const length = Math.hypot(dx, dy);
    const midX = (from.x + to.x) / 2;
    const midY = (from.y + to.y) / 2;
    const midZ = (from.z + to.z) / 2;
    if (length === 0) {
        return { x: midX, y: midY, z: midZ };
    }
    const offset = TRANSITION_BEND * length * sign;
    return {
        x: midX + (-dy / length) * offset,
        y: midY + (dx / length) * offset,
        z: midZ,
    };
}

/** Ein Punkt der Bahn, bei `t` zwischen 0 und 1. Quadratische Bezierkurve. */
export function transitionPointAt(
    from: MotionPoint,
    to: MotionPoint,
    control: MotionPoint,
    t: number,
): MotionPoint {
    const e = t <= 0 ? 0 : t >= 1 ? 1 : t;
    const inv = 1 - e;
    const a = inv * inv;
    const b = 2 * inv * e;
    const c = e * e;
    return {
        x: a * from.x + b * control.x + c * to.x,
        y: a * from.y + b * control.y + c * to.y,
        z: a * from.z + b * control.z + c * to.z,
    };
}

/**
 * Der Abstand eines Punktes von der Geraden durch zwei andere, in der
 * Bildebene.
 *
 * Die Zahl, an der der Beweislauf die Kruemmung misst. Sie steht hier, damit
 * die Ansicht und der Beweis dieselbe Rechnung benutzen.
 */
export function distanceFromChord(
    point: MotionPoint,
    from: MotionPoint,
    to: MotionPoint,
): number {
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const length = Math.hypot(dx, dy);
    if (length === 0) {
        return Math.hypot(point.x - from.x, point.y - from.y);
    }
    return Math.abs(dy * point.x - dx * point.y + to.x * from.y - to.y * from.x) / length;
}

/* -------------------------------------------------------------- der Puls --- */

/** Wie ein Koerper atmet. */
export interface Pulse {
    /** Die Dauer eines Atemzugs, in Millisekunden. `0` heisst: kein Puls. */
    periodMs: number;
    /** Der Ausschlag, als Anteil der Koerpergroesse. `0` heisst: keiner. */
    amplitude: number;
    /** Die Zahl, aus der beides kommt. */
    events: number;
}

/** Der Puls ohne jede Arbeit dahinter. */
export const NO_PULSE: Pulse = { periodMs: 0, amplitude: 0, events: 0 };

/**
 * Der Puls aus der Zahl der Ereignisse im Fenster.
 *
 * Null Ereignisse ergeben KEINEN Puls, und das ist die Zusicherung, um die es
 * geht: der Puls ist die Arbeit und nicht ihr Abbild. Zwischen einem Ereignis
 * und {@link PULSE_BUSY_EVENTS} laeuft beides linear, weil eine Kurve hier eine
 * Aussage ueber die Bedeutung von "viel" waere, die niemand gemessen hat.
 */
export function pulseOf(events: number): Pulse {
    if (!Number.isFinite(events) || events <= 0) {
        return NO_PULSE;
    }
    const share = Math.min(1, (events - 1) / Math.max(1, PULSE_BUSY_EVENTS - 1));
    return {
        periodMs: Math.round(PULSE_SLOW_MS + (PULSE_FAST_MS - PULSE_SLOW_MS) * share),
        amplitude: Number(
            (PULSE_MIN_AMPLITUDE + (PULSE_MAX_AMPLITUDE - PULSE_MIN_AMPLITUDE) * share).toFixed(4),
        ),
        events,
    };
}

/**
 * Die Groesse des Koerpers zu dieser Zeit, als Vielfaches seiner Ruhegroesse.
 *
 * Ein Kosinus und keine Sprungfunktion: ein Knoten leuchtet gleichmaessig, ein
 * Agent atmet, und ein Atemzug hat keine Kanten. Ohne Puls kommt genau 1
 * heraus, bei jedem `elapsed`: ein ruhiger Koerper aendert seine Groesse nicht.
 */
export function pulseScaleAt(pulse: Pulse, elapsedMs: number): number {
    if (pulse.periodMs <= 0 || pulse.amplitude <= 0) {
        return 1;
    }
    const phase = (elapsedMs % pulse.periodMs) / pulse.periodMs;
    return 1 + pulse.amplitude * (0.5 - 0.5 * Math.cos(phase * Math.PI * 2));
}

/* ------------------------------------------------------------- die Welle --- */

/** Ein Schreib-Bruch: mehrere Aenderungen an demselben Knoten in kurzer Folge. */
export interface WriteBurst {
    /** Die Kennung des Bruchs. Sie bleibt gleich, solange der Bruch waechst. */
    key: string;
    nodeId: number;
    /** Die Zeit des ersten Ereignisses des Bruchs. */
    from: number;
    /** Die Zeit des letzten. */
    to: number;
    /** Wie viele Ereignisse darin liegen. */
    events: number;
}

/** Ein Ereignis, so weit dieses Modul es braucht. */
export interface BurstInput {
    ts: number;
    nodeId: number;
    /** Ob es ein Schreibereignis war. Nur die zaehlen. */
    write: boolean;
    /** Lauf und Nummer, fuer die Kennung des Bruchs. */
    key: string;
}

/**
 * Die Schreib-Brueche einer Ereignisfolge, aelteste zuerst.
 *
 * Ein Bruch endet, wenn das naechste Schreibereignis desselben Knotens mehr als
 * {@link BURST_GAP_MS} spaeter kommt, oder wenn dazwischen an einem ANDEREN
 * Knoten geschrieben wurde. Der zweite Fall ist wichtiger, als er aussieht: wer
 * zwischen zwei Dateien hin und her aendert, arbeitet an zwei Stellen, und
 * beide sollen ihre eigene Welle bekommen.
 */
export function writeBurstsOf(events: readonly BurstInput[]): WriteBurst[] {
    const bursts: WriteBurst[] = [];
    let open: WriteBurst | undefined;
    for (const event of events) {
        if (!event.write || event.nodeId < 0) {
            continue;
        }
        if (open !== undefined && open.nodeId === event.nodeId
            && event.ts - open.to <= BURST_GAP_MS) {
            open.to = event.ts;
            open.events += 1;
            continue;
        }
        open = {
            key: event.key,
            nodeId: event.nodeId,
            from: event.ts,
            to: event.ts,
            events: 1,
        };
        bursts.push(open);
    }
    return bursts;
}

/**
 * Der Bruch, der gerade eine Welle traegt: hoechstens einer, und nur der
 * laufende.
 *
 * "Laufend" heisst: das LETZTE Ereignis dieses Akteurs gehoert zu diesem Bruch.
 * Er arbeitet also gerade an dieser Stelle. Sobald er etwas anderes tut oder
 * ruhig wird, ist die Welle weg.
 *
 * Es ist ausdruecklich KEINE Frist auf der Wanduhr, und das ist eine
 * Entscheidung mit einem Grund: die Wiedergabe schiebt die Zeitstempel einer
 * Aufzeichnung einmal auf die Gegenwart, und danach altern sie mit jeder
 * Sekunde des Zusehens. Eine Welle, die zwei Sekunden nach dem Ereignis
 * ausliefe, waere in einer Wiedergabe nie zu sehen, obwohl der Leser den Bruch
 * genau jetzt vor sich hat. Die Regel "das ist gerade seine Arbeit" gilt in
 * beiden Betriebsarten gleich.
 *
 * Der Rueckgabewert ist eine Liste mit null oder einem Eintrag, weil die
 * Oberflaeche sie so zeichnet: eine Welle je Bruch, nicht eine je Ereignis.
 */
export function currentBursts(
    bursts: readonly WriteBurst[],
    lastEventTs: number,
): WriteBurst[] {
    const last = bursts[bursts.length - 1];
    return last !== undefined && last.to === lastEventTs ? [last] : [];
}

/* ------------------------------------------------------------- die Feder --- */

/** Der Zustand einer Feder auf einer Achse. */
export interface SpringState {
    value: number;
    velocity: number;
}

/**
 * Ein Schritt der kritisch gedaempften Feder.
 *
 * Halb-implizit integriert (erst die Geschwindigkeit, dann der Ort): das
 * explizite Verfahren pumpt bei grossen Zeitschritten Energie in die Feder und
 * schwingt genau da ueber, wo diese Bewegung es nicht darf. Der Zeitschritt
 * wird ausserdem gedeckelt, damit ein einzelnes sehr langes Bild (ein Reiter,
 * der zurueckkommt) nicht einen ganzen Anflug in einem Sprung erledigt.
 */
export function springStep(
    state: SpringState,
    goal: number,
    deltaSeconds: number,
    periodMs: number = FOLLOW_SPRING_PERIOD_MS,
): SpringState {
    const dt = Math.min(0.1, Math.max(0, deltaSeconds));
    if (dt === 0 || periodMs <= 0) {
        return state;
    }
    const omega = (2 * Math.PI) / (periodMs / 1000);
    const velocity = state.velocity
        + (-2 * omega * state.velocity - omega * omega * (state.value - goal)) * dt;
    return { value: state.value + velocity * dt, velocity };
}
