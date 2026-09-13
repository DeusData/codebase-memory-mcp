/**
 * Die Farbe und der Buchstabe eines Akteurs.
 *
 * ## Warum die Farbe aus der Kennung kommt und nicht aus einer Liste
 *
 * Weil derselbe Agent nach einem Reload dieselbe Farbe haben muss. Eine Liste,
 * die der Reihe nach vergeben wird, gibt dem Agenten die Farbe des Platzes, auf
 * dem er gerade steht: kommt ein zweiter frueher an, tauschen beide die Farbe,
 * und der Leser sieht eine Bewegung, die es nicht gab. Die Farbe hier ist eine
 * Funktion der Kennung, sonst nichts. Zwei Fenster, zwei Laeufe, zwei Tage:
 * dieselbe Kennung, dieselbe Farbe.
 *
 * ## Warum es ein Farbton-Band ist und keine sechs festen Farben
 *
 * Sechs feste Farben sind bei sieben Agenten zweimal dieselbe. Das Band gibt
 * jeder Kennung ihren eigenen Ton und laesst trotzdem die Familie erkennen.
 *
 * Und warum GENAU dieses Band: die Knotenfarben dieses Graphen sind Sternfarben
 * und liegen im warmen Viertel (in der Fixture gemessen: #ff6050 bis #fff0c0,
 * also Farbton 15 bis 45). Das Band der Agenten beginnt bei 140 und endet bei
 * 340, also Gruen ueber Cyan und Blau bis Magenta. Damit kann ein Agentenkoerper
 * nie die Farbe eines Knotens tragen, und die Ebene faerbt keinen einzigen
 * Knoten und keine einzige Kante um: sie legt sich darueber.
 *
 * Die Helligkeit ist fest und hoch (72 Prozent bei 92 Prozent Saettigung). Ein
 * kleiner Koerper von zehn Pixeln auf einem dunklen Grund muss leuchten, um
 * ueberhaupt eine Farbe zu haben, und ein Band, in dem auch die Helligkeit
 * variiert, haette dunkle Mitglieder, die niemand sieht.
 *
 * ## Der Buchstabe
 *
 * Er ist der zweite Unterschied, damit die Unterscheidung nicht allein an der
 * Farbe haengt: wer Farben nicht trennen kann, liest den Buchstaben. Er kommt
 * aus dem Namen, und wenn zwei Namen mit demselben Zeichen beginnen, bekommt der
 * zweite das naechste Zeichen seines eigenen Namens, das noch frei ist. Vergeben
 * wird in der Ordnung der Kennungen und nicht in der Reihenfolge des Eintreffens,
 * damit auch das den Reload ueberlebt.
 */

/** Der Anfang des Farbtonbandes, in Grad. Gruen. */
export const AGENT_HUE_START = 140;

/** Das Ende des Bandes, in Grad. Magenta. */
export const AGENT_HUE_END = 340;

/** Die Saettigung jedes Agentenkoerpers, in Prozent. */
export const AGENT_SATURATION = 92;

/** Die Helligkeit jedes Agentenkoerpers, in Prozent. */
export const AGENT_LIGHTNESS = 72;

/**
 * Die Farbe des Lesers.
 *
 * Fast weiss und ausdruecklich ausserhalb des Bandes: der Leser ist kein Agent,
 * und seine Spur soll auch dann nicht wie eine fremde aussehen, wenn zufaellig
 * ein Agent in der Naehe seines Farbtons liegt.
 */
export const YOU_COLOR = '#F2F5FF';

/** Die Kennung, unter der die eigene Navigation laeuft. */
export const YOU_ID = 'you';

/** Der Buchstabe des Lesers. */
export const YOU_LETTER = 'Y';

/**
 * Ein 32-Bit-Streuwert einer Zeichenkette (FNV-1a).
 *
 * Ausgeschrieben und nicht aus einer Bibliothek, weil er Teil der Zusicherung
 * ist: die Farbe eines Agenten haengt an dieser Rechnung, und sie darf sich
 * nicht mit einer Abhaengigkeit aendern.
 */
export function hashOf(value: string): number {
    let hash = 0x811c9dc5;
    for (let i = 0; i < value.length; i += 1) {
        hash ^= value.charCodeAt(i);
        hash = Math.imul(hash, 0x01000193) >>> 0;
    }
    return hash >>> 0;
}

/** Der Farbton dieser Kennung, in Grad, im Band. */
export function agentHue(id: string): number {
    const span = AGENT_HUE_END - AGENT_HUE_START;
    return AGENT_HUE_START + (hashOf(id) % span);
}

/** Die Farbe dieser Kennung, als CSS-Farbe. */
export function agentColor(id: string): string {
    if (id === YOU_ID) {
        return YOU_COLOR;
    }
    return `hsl(${agentHue(id)} ${AGENT_SATURATION}% ${AGENT_LIGHTNESS}%)`;
}

/** Die Kandidaten fuer den Buchstaben eines Namens, in der Reihenfolge des Namens. */
function letterCandidates(name: string): string[] {
    const letters = [...name.toUpperCase()].filter((character) => /[A-Z0-9]/.test(character));
    return letters.length > 0 ? letters : ['?'];
}

/**
 * Je Kennung ein Buchstabe, alle verschieden, in der Ordnung der Kennungen.
 *
 * Erschoepfen sich die Zeichen eines Namens (zwei Agenten mit demselben Namen),
 * bekommt der spaetere eine Ziffer. Sie ist haesslich und sie ist ehrlich: zwei
 * Koerper mit demselben Buchstaben waeren zwei, die man nicht unterscheiden
 * kann.
 */
export function agentLetters(actors: readonly { id: string; name: string }[]):
Map<string, string> {
    const out = new Map<string, string>();
    const taken = new Set<string>();
    // Der Buchstabe des Lesers wird vorab belegt, nicht in der Reihe vergeben:
    // sonst haette ein Agent namens "Yara" das Y, und der Leser bekaeme ein A.
    if (actors.some((actor) => actor.id === YOU_ID)) {
        taken.add(YOU_LETTER);
    }
    const ordered = [...actors].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    for (const actor of ordered) {
        if (actor.id === YOU_ID) {
            out.set(actor.id, YOU_LETTER);
            taken.add(YOU_LETTER);
            continue;
        }
        const candidate = letterCandidates(actor.name).find((letter) => !taken.has(letter));
        if (candidate !== undefined) {
            out.set(actor.id, candidate);
            taken.add(candidate);
            continue;
        }
        let digit = 2;
        while (taken.has(String(digit)) && digit < 10) {
            digit += 1;
        }
        out.set(actor.id, String(digit));
        taken.add(String(digit));
    }
    return out;
}
