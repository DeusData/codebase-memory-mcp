/**
 * Die vier Masse des Layouts: wie breit, wie hoch, wie weit darf das gehen, und
 * was davon ueberlebt einen Reload.
 *
 * Der Befund vom 2026-08-29 (Screenshot mit drei gleichzeitig offenen Flaechen)
 * hat zwei Haelften. Die eine ist, WO etwas liegt, und die beantwortet
 * src/app/AtlasChrome.tsx mit festen Zonen statt gestapelter Fenster. Die andere
 * ist, WIE GROSS es ist, und die beantwortet diese Datei: der Leser zieht die
 * Grenzen selbst, und was er gezogen hat, steht beim naechsten Oeffnen noch da.
 *
 * ## Warum die Grenzen aus dem Fenster kommen und nicht aus einer Tabelle
 *
 * Eine feste Hoechsthoehe ("das Erklaeren-Feld darf 620 Pixel hoch sein") ist
 * auf einem grossen Bildschirm eine willkuerliche Fessel und auf einem kleinen
 * eine Zusage, die die Oberflaeche nicht halten kann: 620 Pixel Erklaeren in
 * einem 600 Pixel hohen Fenster heisst kein Reader mehr. Die Hoechstmasse sind
 * darum ANTEILE des Fensters mit einem Deckel darauf, und die Mindestmasse sind
 * absolute Zahlen: eine Zone, die kleiner als ihr Mindestmass ist, zeigt nichts
 * mehr, und das haengt an der Schriftgroesse und nicht an der Fenstergroesse.
 *
 * Damit gilt die Zusage aus AC2 ("keine Zone darf wegziehbar sein") an jeder
 * Fenstergroesse, und sie gilt zweifach: hier als Zahl, damit der Griff seine
 * `aria-valuemin`/`aria-valuemax` ehrlich melden kann, und in terminal.css als
 * `min-height`/`max-height`, damit auch ein Fenster, das zwischen zwei Bildern
 * kleiner wird, keine Zone verschluckt. Zwei Stellen fuer dieselbe Grenze sind
 * hier kein Widerspruch, sondern Guertel und Hosentraeger: die eine spricht,
 * die andere zeichnet.
 *
 * ## Was hier von src/chat/chat-height.ts uebernommen wurde
 *
 * W7c hatte dem Antwort-Panel eine eigene ziehbare Hoehe gegeben, mit Grenzen,
 * einer Schrittweite und einem Gedaechtnis in localStorage. Seit W8 ist der Chat
 * ein Reiter und erbt die Hoehe seiner Zone; die drei Zusagen gelten unveraendert
 * weiter, sie stehen nur nicht mehr neben dem Chat, sondern hier, wo sie fuer
 * alle vier Grenzen gelten. Eine zweite Hoehe neben der der Zone waere genau der
 * Stapel, den dieser Zyklus abschafft.
 */

/** Die vier Grenzen, die der Leser ziehen kann. */
export const LAYOUT_KEYS = ['leftWidth', 'explainHeight', 'rightWidth', 'twinHeight'] as const;

/** Der Name einer Grenze. */
export type LayoutKey = (typeof LAYOUT_KEYS)[number];

/** Die vier Masse in Pixeln. */
export type LayoutSizes = Record<LayoutKey, number>;

/** Wie gross das Fenster gerade ist. Nur daraus kommen die Hoechstmasse. */
export interface LayoutFrame {
    width: number;
    height: number;
}

/**
 * Die Vorgabe, mit der ein Projekt aufgeht.
 *
 * `leftWidth` und `rightWidth` sind die Zahlen, die seit W3 beziehungsweise W5c
 * im Raster standen (260 und 440). Sie zu aendern waere eine Aenderung an jedem
 * bisherigen Beweisbild fuer nichts.
 */
export const LAYOUT_DEFAULT: LayoutSizes = {
    leftWidth: 260,
    explainHeight: 340,
    rightWidth: 440,
    twinHeight: 360,
};

/**
 * Die Mindestmasse.
 *
 * Jede Zahl ist die Groesse, bei der die Zone noch etwas zeigt und nicht nur
 * noch da ist: der Explorer eine Zeile mit Pfad und Punkt, das Erklaeren-Feld
 * seine Reiterleiste plus zwei Zeilen Inhalt, die rechte Spalte einen
 * Twin-Abschnitt, der Twin seinen Kopf plus eine Zeile.
 */
export const LAYOUT_MIN: LayoutSizes = {
    leftWidth: 180,
    explainHeight: 150,
    rightWidth: 320,
    twinHeight: 170,
};

/**
 * Der Deckel, ab dem mehr nicht mehr besser wird.
 *
 * Ein Explorer, der 900 Pixel breit ist, zeigt keine tieferen Pfade, sondern
 * mehr Leerraum rechts davon; ein Erklaeren-Feld, das den Reader auf eine Zeile
 * druecken darf, macht aus der Lese-IDE ein Panel mit Editor-Andeutung.
 */
export const LAYOUT_CEILING: LayoutSizes = {
    leftWidth: 520,
    explainHeight: 720,
    rightWidth: 760,
    twinHeight: 720,
};

/**
 * Der Anteil des Fensters, den eine Zone hoechstens nimmt.
 *
 * Die beiden Breiten zusammen sind 0.70; die restlichen 0.30 gehoeren dem
 * Reader und sind der Grund, aus dem die Summe unter eins liegt. Bei den
 * Hoehen gibt es diese Kopplung nicht, weil sie in verschiedenen Spalten
 * liegen: die Erklaeren-Hoehe streitet mit dem Reader, die Twin-Hoehe mit dem
 * Graphen.
 */
export const LAYOUT_SHARE: LayoutSizes = {
    leftWidth: 0.28,
    explainHeight: 0.55,
    rightWidth: 0.42,
    /*
     * Der kleinste der vier Anteile, und der Grund steht in terminal.css: unter
     * dem Twin liegt das Graph-Panel mit einer Mindesthoehe von 280 Pixeln, die
     * seit W5c eine Zusicherung ist. Ein Twin, der bis zur Haelfte des Fensters
     * wachsen darf, drueckt sie in einer 1050 Pixel hohen Ansicht kaputt; mit
     * 0.42 bleibt der Zusage auch am Anschlag ihr Platz.
     */
    twinHeight: 0.42,
};

/** Ob diese Grenze an der Fensterbreite oder an der Fensterhoehe haengt. */
export function layoutAxisOf(key: LayoutKey): 'width' | 'height' {
    return key === 'leftWidth' || key === 'rightWidth' ? 'width' : 'height';
}

/**
 * Ein Pfeiltastendruck.
 *
 * Sechzehn Pixel, also ungefaehr eine Zeile dieser Schrift: klein genug, um
 * genau zu treffen, gross genug, dass ein Druck sichtbar etwas tut. Ein Griff,
 * dessen Taste nichts Sichtbares bewegt, ist fuer den Leser kaputt, auch wenn
 * die Zahl sich geaendert hat.
 */
export const LAYOUT_STEP = 16;

/** Derselbe Druck mit Shift. Vier Schritte, damit man eine Strecke ueberbrueckt. */
export const LAYOUT_BIG_STEP = 64;

/** Das Fenster, das keine Grenze mehr sinnvoll teilen kann. */
const TINY_FRAME = 240;

/**
 * Die Grenzen dieser einen Zahl bei diesem Fenster.
 *
 * `max` ist nie kleiner als `min`: in einem sehr kleinen Fenster faellt der
 * Anteil unter das Mindestmass, und dann gibt es nichts mehr zu ziehen. Die
 * Zone bleibt trotzdem bei ihrem Mindestmass stehen, statt zu verschwinden, und
 * der Griff meldet `min === max` statt eines umgekehrten Bereichs.
 */
export function layoutBounds(key: LayoutKey, frame: LayoutFrame): { min: number; max: number } {
    const axis = layoutAxisOf(key);
    const raw = axis === 'width' ? frame.width : frame.height;
    const available = Number.isFinite(raw) && raw > TINY_FRAME ? raw : TINY_FRAME;
    const min = LAYOUT_MIN[key];
    const max = Math.max(min, Math.min(LAYOUT_CEILING[key], Math.round(available * LAYOUT_SHARE[key])));
    return { min, max };
}

/** Eine einzelne Zahl in ihre Grenzen bringen. Unsinn wird zur Vorgabe. */
export function clampLayoutValue(key: LayoutKey, value: number, frame: LayoutFrame): number {
    const { min, max } = layoutBounds(key, frame);
    if (!Number.isFinite(value)) {
        return Math.max(min, Math.min(max, LAYOUT_DEFAULT[key]));
    }
    return Math.round(Math.max(min, Math.min(max, value)));
}

/** Alle vier Zahlen in ihre Grenzen bringen. */
export function clampLayout(sizes: LayoutSizes, frame: LayoutFrame): LayoutSizes {
    const out = {} as LayoutSizes;
    for (const key of LAYOUT_KEYS) {
        out[key] = clampLayoutValue(key, sizes[key], frame);
    }
    return out;
}

/** Die Vorgabe fuer dieses Fenster: dieselbe Zahl, nur nie ausserhalb. */
export function defaultLayout(frame: LayoutFrame): LayoutSizes {
    return clampLayout(LAYOUT_DEFAULT, frame);
}

/** Ob zwei Layouts dieselben vier Zahlen tragen. */
export function sameLayout(a: LayoutSizes, b: LayoutSizes): boolean {
    return LAYOUT_KEYS.every((key) => a[key] === b[key]);
}

/**
 * Nur das, was diese Datei von einem Speicher braucht.
 *
 * Als eigene Form und nicht als `Storage`, damit ein Test einen Speicher
 * hinstellen kann, ohne ein halbes DOM zu bauen, und damit ein Browser, der
 * localStorage verweigert (privates Fenster, gesperrte Seite), hier nur eine
 * Ausnahme wirft, die abgefangen wird.
 */
export interface LayoutStore {
    getItem: (key: string) => string | null;
    setItem: (key: string, value: string) => void;
    removeItem?: (key: string) => void;
}

/** Der Praefix des Schluessels. Die Fassung steht darin, damit ein alter Eintrag stirbt. */
export const LAYOUT_STORAGE_PREFIX = 'codeatlas.layout.v1';

/**
 * Der Schluessel, unter dem dieses Projekt seine Masse fuehrt.
 *
 * Pro Projekt, wie der Contract es verlangt, und das ist keine Formalie: ein
 * Repository mit tiefen Pfaden braucht einen breiten Explorer, ein flaches
 * nicht, und wer zwischen beiden wechselt, will nicht jedes Mal nachziehen.
 * Ohne Projekt gibt es einen eigenen Schluessel statt gar keinem, damit die
 * Masse auch dann ueberleben, wenn die Oberflaeche gerade kein Projekt kennt.
 */
export function layoutStorageKey(project: string): string {
    const name = project.trim();
    return `${LAYOUT_STORAGE_PREFIX}:${name.length === 0 ? '(no project)' : name}`;
}

/**
 * Was gespeichert war, in den Grenzen dieses Fensters.
 *
 * Fehlt der Eintrag, ist er kaputt oder traegt er Unsinn, kommt die Vorgabe
 * zurueck. Ein halb gelesener Eintrag waere schlimmer als keiner: eine Zone mit
 * einer Zahl aus einer anderen Fassung sieht aus wie ein Layoutfehler.
 */
export function readLayout(store: LayoutStore | undefined, project: string, frame: LayoutFrame): LayoutSizes {
    if (store === undefined) {
        return defaultLayout(frame);
    }
    let raw: string | null = null;
    try {
        raw = store.getItem(layoutStorageKey(project));
    } catch {
        return defaultLayout(frame);
    }
    if (raw === null || raw.length === 0) {
        return defaultLayout(frame);
    }
    let parsed: unknown;
    try {
        parsed = JSON.parse(raw);
    } catch {
        return defaultLayout(frame);
    }
    if (parsed === null || typeof parsed !== 'object') {
        return defaultLayout(frame);
    }
    const record = parsed as Partial<Record<LayoutKey, unknown>>;
    const sizes = { ...LAYOUT_DEFAULT };
    for (const key of LAYOUT_KEYS) {
        const value = record[key];
        if (typeof value === 'number' && Number.isFinite(value)) {
            sizes[key] = value;
        }
    }
    return clampLayout(sizes, frame);
}

/** Die vier Zahlen merken. Ein Speicher, der nicht will, wird nicht erzwungen. */
export function writeLayout(store: LayoutStore | undefined, project: string, sizes: LayoutSizes): void {
    if (store === undefined) {
        return;
    }
    try {
        store.setItem(layoutStorageKey(project), JSON.stringify(sizes));
    } catch {
        // Ein Fenster ohne Speicher ist kein Fehler dieser Oberflaeche. Die
        // Masse gelten dann fuer diese Sitzung, und das steht dem Leser
        // nirgends als Zusage entgegen.
    }
}
