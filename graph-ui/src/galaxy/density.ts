/*
 * MIT License. Copyright (c) 2025 DeusData.
 *
 * Uebernommen am 2026-08-28 aus DeusData/codebase-memory-mcp, Branch
 * feat/atlas-r1, Datei graph-ui/src/lib/density.ts. Der Lizenztext und die
 * Liste aller uebernommenen Dateien stehen in THIRD_PARTY.md.
 *
 * Aenderungen gegenueber dem Original:
 *  - loadDisplaySettings, saveDisplaySettings, loadTooltipDelayMs,
 *    clampDisplaySettings, DISPLAY_LIMITS und der Speicherschluessel
 *    "cbm-display" waren bis W10 gestrichen. Sie bedienten ein
 *    Einstellungsmenue, das dieses Projekt nicht uebernommen hatte, und sie
 *    waren die einzige Stelle der Uebernahme, die localStorage angefasst hat.
 *  - Die vier Skalenfunktionen und DisplaySettings sind unveraendert: sie
 *    entscheiden, wie die Szene bei wachsender Dichte aussieht, und eine
 *    eigene Kurve waere eine zweite Wahrheit ueber dasselbe Bild.
 *
 * ## Warum das Laden und Speichern seit W10 zurueckkehrt
 *
 * Weil es das Menue jetzt gibt. Der Nutzerwunsch vom 2026-08-29, woertlich:
 * "2D/3D oder sowas sollte immer zentral in einem Settings-Menue drin sein,
 * nicht alles auf einer Oberflaeche, wegen Rechenleistung falls jemand keine so
 * starke Maschine hat." Damit hat die Frage, die die beiden Funktionen
 * beantworten, wieder einen Ort: eine Entscheidung ueber Rechenzeit, die man bei
 * jedem Laden neu treffen muesste, ist keine Einstellung, sondern eine Geste.
 *
 * Es sind nicht dieselben Funktionen wie im Original, und das steht hier, damit
 * niemand sie fuer eine Rueckuebernahme haelt. Der Schluessel ist
 * `atlas-display:<projekt>` (die Form dieses Projekts, siehe
 * src/llm/preference.ts) und nicht "cbm-display", und was gespeichert wird, ist
 * {@link GraphDisplaySettings}: nicht die vier Regler des Originals, sondern die
 * Schalter, die auf dieser Maschine wirklich Rechenzeit kosten, samt der
 * Projektion und dem Bildratendeckel, die es im Original nicht gab.
 */

/* Visual density compensation.
 *
 * The white-blob-at-scale failure is dominated by EDGES: they blend
 * additively and a 15k-node graph carries ~80k long lines crossing the
 * center, so their glow stacks into an opaque wash. Nodes are far fewer and
 * discrete: their bloom halos are the whole "wow", so we keep those bright.
 *
 * Strategy: dim edges hard (they cause the blob), keep nodes and bloom near
 * full so the bright stars still pop; only ease nodes/bloom back on genuinely
 * huge clouds where even discrete points overlap. Highlighted elements are
 * never scaled: a selection stays bright against the dimmed rest. */

/* Edge counts at/below this render exactly as before. */
export const EDGE_REFERENCE_COUNT = 2500;
const EDGE_MIN_SCALE = 0.05;

export function edgeIntensityScale(edgeCount: number): number {
    if (edgeCount <= EDGE_REFERENCE_COUNT) return 1;
    /* ~1/sqrt(n): 4x the edges gives each ~half the brightness, total glow ~flat. */
    return Math.max(EDGE_MIN_SCALE, Math.sqrt(EDGE_REFERENCE_COUNT / edgeCount));
}

/* Node glow and bloom stay at full strength up to here: this covers the
 * common "load the whole repo" case (tens of thousands of nodes) so the
 * bright-star look is preserved. */
export const NODE_REFERENCE_COUNT = 25000;
/* then ease gently toward these floors as the cloud grows past the fade end,
 * where even discrete point sprites start to overlap and over-bloom. */
const NODE_FADE_END = 250000;
const BLOOM_FLOOR = 0.7;
const NODE_BOOST_FLOOR = 0.8;

function fadeFactor(nodeCount: number): number {
    if (nodeCount <= NODE_REFERENCE_COUNT) return 0;
    return Math.min(
        1,
        (nodeCount - NODE_REFERENCE_COUNT) / (NODE_FADE_END - NODE_REFERENCE_COUNT),
    );
}

export function bloomIntensityScale(nodeCount: number): number {
    return 1 - fadeFactor(nodeCount) * (1 - BLOOM_FLOOR);
}

/* Per-node glow boost: full up to the reference count, then a gentle,
 * high-floored fade so large clouds don't merge into mush while moderate
 * graphs keep their halos. */
export function nodeBoostScale(nodeCount: number): number {
    return 1 - fadeFactor(nodeCount) * (1 - NODE_BOOST_FLOOR);
}

/* Colour-aware glow multiplier applied to each node before bloom.
 *
 * Nodes are coloured as star classes by degree: blue giants (high-degree
 * hubs) to white/yellow (mid) to red dwarfs (leaves). Bloom is luminance-
 * thresholded, and blue has a tiny luminance weight, so a naive
 * brightness-based boost makes white/yellow blow out while blue and red stay
 * flat. Instead we boost by *channel dominance*: a blue-dominant node gets the
 * strongest boost (the important hubs shine brightest), a red-dominant node a
 * modest one, and white/yellow, which need no help to bloom, the least.
 *
 * r, g, b are 0..1 colour channels. Returns a multiplier >= 1. */
const GLOW_BASE = 1.35;
const GLOW_BLUE_GAIN = 2.4;
const GLOW_RED_GAIN = 0.9;

export function nodeGlowBoost(r: number, g: number, b: number): number {
    /* Blue that exceeds both red and green is a cool hub. Red that exceeds both
     * green and blue is a warm leaf (the green cutoff keeps yellow/orange out
     * of the red term so they are not boosted). */
    const blueness = Math.max(0, b - Math.max(r, g));
    const redness = Math.max(0, r - Math.max(g, b));
    return GLOW_BASE + blueness * GLOW_BLUE_GAIN + redness * GLOW_RED_GAIN;
}

/* User-facing multipliers layered ON TOP of the adaptive scales above: the
 * adaptive scale picks a sane default for the current density, the settings
 * let the user push contrast/brightness around it. */

export interface DisplaySettings {
    /** Edge brightness multiplier (0.1-3, default 1). */
    edgeBrightness: number;
    /** Node glow-boost multiplier (0-2, default 1). */
    nodeGlow: number;
    /** Bloom intensity multiplier (0-2, default 1). */
    bloom: number;
    /** Hover dwell before help cards appear, ms (150-800, default 350). */
    tooltipDelayMs: number;
}

export const DEFAULT_DISPLAY_SETTINGS: DisplaySettings = {
    edgeBrightness: 1,
    nodeGlow: 1,
    bloom: 1,
    tooltipDelayMs: 350,
};

/* ------------------------------------------------------------------------- *
 * Was der Leser einstellt, und was es kostet (W10)
 * ------------------------------------------------------------------------- */

/**
 * Wie der Graph gezeichnet wird: raeumlich oder von oben.
 *
 * `flat` ist eine orthografische Ansicht die z-Achse hinunter. Sie laesst die
 * dritte Achse fallen, statt sie zu verkuerzen: eine perspektivische Kamera von
 * oben waere immer noch eine 3D-Szene mit allem, was sie kostet, und saehe nur
 * so aus wie eine Karte.
 */
export type GraphProjection = 'spatial' | 'flat';

/**
 * Wie viel von den Kanten gezeichnet wird.
 *
 * `off` zeichnet die Ebene GAR NICHT, statt sie auf Helligkeit null zu setzen.
 * Der Unterschied ist genau der Punkt dieser Einstellung: eine unsichtbare
 * Linie kostet dieselbe Zeit wie eine sichtbare, und ein Schalter, der nur die
 * Farbe aendert und "spart" heisst, waere ein Versprechen.
 */
export type EdgeDensity = 'full' | 'dim' | 'off';

/** Die Helligkeit, die zu einer Kantenstufe gehoert. */
export const EDGE_DENSITY_BRIGHTNESS: Readonly<Record<EdgeDensity, number>> = {
    full: 1,
    dim: 0.4,
    off: 0,
};

/**
 * Ab welcher Entfernung Namen verschwinden, als Vielfaches des Szenenradius.
 *
 * Als Vielfaches und nicht als Zahl in Welteinheiten: der Radius einer Galaxie
 * haengt am Layout des Servers und ist bei jedem Projekt ein anderer. Eine feste
 * Entfernung waere in einem kleinen Projekt "immer an" und in einem grossen
 * "immer aus", und der Schalter haette in beiden nichts zu tun.
 *
 * `0` heisst: keine Grenze. Das ist das Verhalten der Uebernahme, bei dem die
 * Sichtbarkeit rein zahlbasiert ist (die groessten `maxLabels` Knoten).
 */
export const LABEL_DISTANCE_FACTORS: readonly number[] = [0, 2, 1];

/** Die Bildraten, auf die sich der Renderloop deckeln laesst. `0` heisst: kein Deckel. */
export const FRAME_CAPS: readonly number[] = [0, 60, 30];

/**
 * Was der Leser im Einstellungen-Panel entscheidet.
 *
 * Alles hier kostet Rechenzeit, und das ist die Aufnahmebedingung: was nur die
 * Ansicht betrifft (die Detailstufe des Twins, die Legende auf und zu, der
 * Galaxy/Hierarchie-Umschalter, die Kantenart-Filter der Legende) bleibt, wo es
 * ist. Der Wunsch war die Buendelung des Teuren und nicht ein Menue, in dem
 * alles verschwindet.
 */
export interface GraphDisplaySettings {
    projection: GraphProjection;
    /** Die Leuchthoefe auf den groessten Knoten (HaloLayer). */
    halos: boolean;
    /** Der Bloom-Durchgang der Nachbearbeitung. */
    bloom: boolean;
    /** Wie viel von den Kanten gezeichnet wird. */
    edges: EdgeDensity;
    /** Namen nur innerhalb dieses Vielfachen des Szenenradius. 0 heisst: alle. */
    labelDistanceFactor: number;
    /** Der Bildratendeckel. 0 heisst: kein Deckel. */
    frameCap: number;
    /**
     * Ob die Agentenebene gezeichnet wird (W11a).
     *
     * Sie gehoert hierher und nicht an den Graphen, weil sie Rechenzeit kostet:
     * je Akteur ein DOM-Element, das in jedem Bild neu gestellt wird, dazu die
     * Pings einer Suche und die gestrichelte Linie eines Testlaufs. Was sie auf
     * DIESER Maschine kostet, misst das Panel selbst und schreibt die zwei
     * Zahlen daneben.
     *
     * Aus heisst hier wirklich aus: kein Koerper, kein Ping, keine Linie. Das
     * Instrument bleibt und sagt, dass die Ebene abgeschaltet ist; es ist die
     * Erklaerung zum Bild, und eine Erklaerung, die mit dem Bild verschwindet,
     * laesst den Leser mit der Frage allein, warum nichts mehr zu sehen ist.
     */
    agents: boolean;
    /**
     * Die Kometenschweife hinter den fliegenden Koerpern (W11b).
     *
     * Je Akteur eine Linie aus zwoelf Punkten, die waehrend eines Fluges in
     * jedem Bild neue Punkte bekommt. Der billigste der vier Posten hier, und er
     * steht trotzdem einzeln da: die Gruppe verspricht, dass JEDER teure Effekt
     * seinen eigenen Schalter hat, und ein Effekt, den man nur gemeinsam mit
     * anderen abschalten kann, waere eine Ausnahme, die man nachlesen muss.
     */
    agentTails: boolean;
    /**
     * Die gestrichelten Spuren der zuletzt besuchten Symbole (W11b).
     *
     * Bis zu zehn Knoten je Akteur, gedeckelt auf 120 Segmente insgesamt. Sie
     * sind gestrichelt, und eine gestrichelte Linie kostet eine eigene
     * Laengenrechnung je Punkt.
     */
    agentTrails: boolean;
    /** Die konzentrischen Wellen der Schreib-Brueche (W11b). */
    agentWaves: boolean;
    /**
     * Der Zeitstrahl unter dem Graphen (W11b).
     *
     * Er zeichnet einen Strich je Ereignis je Akteur, also bei acht Agenten mit
     * je achtzig behaltenen Ereignissen bis zu 640 Elemente, und er wird bei
     * jedem Takt der Instrumentenuhr neu gestellt.
     */
    agentTimeline: boolean;
}

/** Die Vorgabe: genau das Bild, das dieses Panel vor W10 gezeichnet hat. */
export const DEFAULT_GRAPH_DISPLAY: GraphDisplaySettings = {
    projection: 'spatial',
    halos: true,
    bloom: true,
    edges: 'full',
    labelDistanceFactor: 0,
    frameCap: 0,
    agents: true,
    agentTails: true,
    agentTrails: true,
    agentWaves: true,
    agentTimeline: true,
};

/**
 * Das Sparprofil: mehrere Schalter in einem Zug.
 *
 * Es verspricht nichts. Was es auf DIESER Maschine bringt, misst das Panel
 * selbst und schreibt die zwei Zahlen daneben; auf einer anderen Maschine sind
 * es andere. Zusammengestellt ist es aus dem, was in einer Szene aus tausenden
 * Punkten teuer ist: die Nachbearbeitung, die additiv gemischten langen Linien,
 * die Textur-Sprites der Namen und die dritte Achse.
 */
export const THRIFTY_GRAPH_DISPLAY: GraphDisplaySettings = {
    projection: 'flat',
    halos: false,
    bloom: false,
    edges: 'dim',
    labelDistanceFactor: 1,
    frameCap: 30,
    /*
     * Die Agentenebene geht mit aus.
     *
     * Sie ist der billigste Posten dieser Liste (je Akteur ein Element, also
     * bei fuenf Agenten fuenf, gegen tausende Linien und einen zweiten
     * Renderdurchgang darueber), und sie steht trotzdem darin: das Sparprofil
     * legt JEDEN Schalter dieser Gruppe um, und ein Profil mit einer stillen
     * Ausnahme waere ein Profil, dessen Wirkung man nachlesen muss. Und es ist
     * hier nicht still: das Instrument bleibt stehen und sagt in einer eigenen
     * Zeile, dass die Ebene in den Einstellungen aus ist, statt einen Graphen
     * zu zeigen, auf dem aus unerklaerten Gruenden niemand mehr kreist.
     */
    agents: false,
    /*
     * Und die vier Wirkungen der Bewegung gehen mit (W11b AC7b).
     *
     * Der Vollbildmodus laeuft danach weiter, nur ruhiger: die Koerper stehen
     * an ihren Symbolen, sie fliegen weiter dorthin, und was wegfaellt, sind
     * die Schweife, die Spuren, die Wellen und der Zeitstrahl. Ein Rechner, der
     * die Bewegung nicht traegt, soll die Ansicht trotzdem benutzen koennen.
     */
    agentTails: false,
    agentTrails: false,
    agentWaves: false,
    agentTimeline: false,
};

/** Ob diese Einstellungen die Vorgabe sind. */
export function isDefaultDisplay(settings: GraphDisplaySettings): boolean {
    return (Object.keys(DEFAULT_GRAPH_DISPLAY) as (keyof GraphDisplaySettings)[])
        .every((key) => settings[key] === DEFAULT_GRAPH_DISPLAY[key]);
}

function asOneOf<T>(value: unknown, allowed: readonly T[], fallback: T): T {
    return allowed.includes(value as T) ? (value as T) : fallback;
}

/**
 * Was aus dem Speicher kam, auf zulaessige Werte zurechtstutzen.
 *
 * Jeder unbekannte Wert faellt auf die Vorgabe zurueck und nicht auf den
 * naechstbesten: ein gespeicherter Bildratendeckel von 7, den eine spaetere
 * Fassung nicht mehr kennt, soll die Vorgabe ergeben und nicht 30. Die Vorgabe
 * ist die einzige Zahl, ueber die dieses Modul etwas weiss.
 */
export function clampDisplaySettings(raw: unknown): GraphDisplaySettings {
    const record = typeof raw === 'object' && raw !== null ? (raw as Record<string, unknown>) : {};
    return {
        projection: asOneOf<GraphProjection>(
            record['projection'], ['spatial', 'flat'], DEFAULT_GRAPH_DISPLAY.projection,
        ),
        halos: typeof record['halos'] === 'boolean' ? record['halos'] : DEFAULT_GRAPH_DISPLAY.halos,
        bloom: typeof record['bloom'] === 'boolean' ? record['bloom'] : DEFAULT_GRAPH_DISPLAY.bloom,
        edges: asOneOf<EdgeDensity>(record['edges'], ['full', 'dim', 'off'], DEFAULT_GRAPH_DISPLAY.edges),
        labelDistanceFactor: asOneOf(
            record['labelDistanceFactor'], LABEL_DISTANCE_FACTORS, DEFAULT_GRAPH_DISPLAY.labelDistanceFactor,
        ),
        frameCap: asOneOf(record['frameCap'], FRAME_CAPS, DEFAULT_GRAPH_DISPLAY.frameCap),
        agents: typeof record['agents'] === 'boolean'
            ? record['agents']
            : DEFAULT_GRAPH_DISPLAY.agents,
        agentTails: typeof record['agentTails'] === 'boolean'
            ? record['agentTails']
            : DEFAULT_GRAPH_DISPLAY.agentTails,
        agentTrails: typeof record['agentTrails'] === 'boolean'
            ? record['agentTrails']
            : DEFAULT_GRAPH_DISPLAY.agentTrails,
        agentWaves: typeof record['agentWaves'] === 'boolean'
            ? record['agentWaves']
            : DEFAULT_GRAPH_DISPLAY.agentWaves,
        agentTimeline: typeof record['agentTimeline'] === 'boolean'
            ? record['agentTimeline']
            : DEFAULT_GRAPH_DISPLAY.agentTimeline,
    };
}

/** Praefix des Schluessels, unter dem die Wahl eines Projekts liegt. */
export const DISPLAY_KEY_PREFIX = 'atlas-display:';

/** Der Schluessel, unter dem die Wahl eines Projekts liegt. */
export function displayKey(project: string): string {
    return `${DISPLAY_KEY_PREFIX}${project}`;
}

/** Nur das, was diese Datei von einem Speicher braucht. */
export interface DisplayStore {
    getItem(key: string): string | null;
    setItem(key: string, value: string): void;
}

/**
 * Die gespeicherte Wahl dieses Browsers fuer dieses Projekt.
 *
 * Jeder Zweifel endet bei der Vorgabe: kein Eintrag, unlesbares JSON, ein
 * Speicher, der die Lesung verweigert. Anders als beim lokalen Modell ist die
 * Vorgabe hier nicht "aus", sondern das volle Bild: ein Leser, der nie etwas
 * eingestellt hat, soll die Galaxie sehen, fuer die dieses Panel gebaut ist.
 */
export function loadDisplaySettings(store: DisplayStore, project: string): GraphDisplaySettings {
    if (project.length === 0) {
        return { ...DEFAULT_GRAPH_DISPLAY };
    }
    let raw: string | null;
    try {
        raw = store.getItem(displayKey(project));
    } catch {
        return { ...DEFAULT_GRAPH_DISPLAY };
    }
    if (raw === null) {
        return { ...DEFAULT_GRAPH_DISPLAY };
    }
    try {
        return clampDisplaySettings(JSON.parse(raw));
    } catch {
        return { ...DEFAULT_GRAPH_DISPLAY };
    }
}

/** Die Wahl speichern. Liefert, was jetzt gilt. */
export function saveDisplaySettings(
    store: DisplayStore,
    project: string,
    settings: GraphDisplaySettings,
): GraphDisplaySettings {
    if (project.length === 0) {
        return settings;
    }
    try {
        store.setItem(displayKey(project), JSON.stringify(settings));
    } catch {
        // Ein verweigerter Speicher kostet den Leser das erneute Einstellen beim
        // naechsten Laden. Dasselbe Abwaegen wie bei der LLM-Praeferenz.
    }
    return settings;
}

/**
 * Die Wahl des Lesers auf die Vorgabe einer Ansicht legen.
 *
 * Multiplikativ und nicht ersetzend, und das ist der Unterschied zwischen einem
 * Schalter und einer zweiten Wahrheit: die Hierarchie-Ansicht setzt Bloom auf 0
 * und den Knotenglanz auf die Haelfte, weil dort sechzig Punkte in einem Raster
 * stehen (siehe HIERARCHY_DISPLAY in GalaxyPanel.tsx). Wuerde die Wahl des
 * Lesers diese Werte ersetzen, braechte ein eingeschaltetes Bloom das Leuchten
 * in einer Ansicht zurueck, in der es als Befund weggenommen wurde.
 */
export function displayWith(base: DisplaySettings, choice: GraphDisplaySettings): DisplaySettings {
    return {
        ...base,
        edgeBrightness: base.edgeBrightness * EDGE_DENSITY_BRIGHTNESS[choice.edges],
        bloom: choice.bloom ? base.bloom : 0,
    };
}
