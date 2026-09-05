/**
 * Die sechs Vorschlaege, mit den Zahlen, die dieses Projekt an ihnen gemessen
 * hat, und dem Aufruf, mit dem man einen davon holt.
 *
 * ## Warum eine Liste im Quelltext und keine Abfrage
 *
 * Es gibt nichts abzufragen. Hugging Face kennt Millionen Repositories und
 * dieses Fenster erreicht keines davon: die Oberflaeche ist per Vorgabe
 * abgeschottet (PLAN Abschnitt 3). Was sie anbieten kann, ist das, was dieses
 * Projekt selbst GEMESSEN hat, und das sind genau diese sechs Dateien: 44
 * goldene Fragen, Temperatur 0, Seed 42, eine Maschine, ein Tag
 * (verification/w5/eval.json, Tabelle in docs/adr/0001-modellwahl.md).
 *
 * ## Warum jede Zahl hier gegen ihre Quelle geprueft wird
 *
 * Eine Zahl im Quelltext, die niemand gegen ihre Quelle haelt, ist eine
 * Behauptung. src/settings/model-catalog.test.ts liest verification/w5/eval.json
 * und docs/adr/0001-modellwahl.md und vergleicht jeden Eintrag dieser Tabelle
 * Feld fuer Feld: Trefferquote, Zitattreue und Tempo gegen die Aufzeichnung des
 * Laufs, die Bytes gegen die Groessenangabe der ADR, und jede Repo-Kennung
 * gegen ihren woertlichen Vorkommen in einer der beiden Papierdateien.
 * Verschiebt sich eine Zahl in der Quelle, faellt dieser Katalog um, statt
 * still weiter etwas anderes zu behaupten.
 *
 * ## Was diese Liste ausdruecklich NICHT ist
 *
 * Sie ist keine Rangliste des Feldes und keine Aussage darueber, was auf einer
 * anderen Maschine passiert. Die Eval hat gemessen, was sie messen konnte; was
 * ein 9B-Modell auf einem groesseren Rechner tut, hat niemand gemessen, und der
 * Katalog behauptet darueber nichts (docs/adr/0001, Nachtrag W10). Er ist die
 * Auskunft, die ein Leser bekommt, bevor er selbst entscheidet, und das freie
 * Feld daneben ist die Zusicherung, dass er nicht auf diese sechs festgelegt
 * ist.
 */

/** Die Kontextklasse, in der ein Kandidat gemessen wurde. */
export type ModelClassLabel = 'A' | 'B';

/** Ein Vorschlag: der Name, die Herkunft und was an ihm gemessen wurde. */
export interface ModelSuggestion {
    /** Der Griff im DOM und im Testgriff. Stabil, klein geschrieben. */
    id: string;
    /** Der Name, unter dem die Eval ihn gefuehrt hat. */
    name: string;
    /** Die Datei, die die Eval geladen hat. */
    file: string;
    /** Das Hugging-Face-Repository. Steht woertlich in einer der zwei Papierdateien. */
    repo: string;
    /** Die Quantisierung, die gemessen wurde. */
    quant: string;
    /** Die Kontextklasse: A ist 3072 Token, B ist 8192 (PLAN Paragraph 5). */
    modelClass: ModelClassLabel;
    /** Anteil bestandener Fragen, aus verification/w5/eval.json. */
    passRate: number;
    /** Anteil der Antworten, deren Behauptungszeilen alle zitiert haben. */
    citationCompliance: number;
    /** Antworttempo im Mittel, in Token je Sekunde. */
    tokensPerSecond: number;
    /** Die Datei in Byte, aus der Groessenangabe der ADR. */
    bytes: number;
    /**
     * Wie viele Antworten die Zitatpruefung nicht messen konnte.
     *
     * Fehlt hier bei allen sechs, und das ist kein Versaeumnis, sondern der
     * Stand der Quelle: verification/w5/eval.json ist am 2026-08-29 VOR der
     * Aenderung an der Zitatpruefung aufgezeichnet worden (W10, AC8) und fuehrt
     * das Feld nicht. Es wird auch nicht nachgetragen, denn dazu muesste die
     * Eval neu laufen, und eine nachtraeglich eingesetzte Null waere die
     * Behauptung, es habe keine ungemessene Antwort gegeben. Das Panel sagt
     * darum an dieser Stelle, dass der aufgezeichnete Lauf sie nicht ausweist.
     */
    citationUnmeasured?: number;
}

/**
 * Die sechs Kandidaten der ADR, in der Reihenfolge der Eval-Tabelle.
 *
 * Die Reihenfolge ist die der ADR und nicht die der Trefferquote: die Tabelle
 * dort fuehrt erst die Klasse A und dann die Klasse B, und eine nach Punkten
 * sortierte Liste haette den 4B-Kandidaten oben, der ein anderes Kontextbudget
 * und einen anderen Speicherbedarf hat. Zwei Klassen als eine Rangliste zu
 * zeichnen waere ein Vergleich, den die Eval nicht gemacht hat.
 */
export const MODEL_SUGGESTIONS: readonly ModelSuggestion[] = [
    {
        id: 'qwen3.5-2b',
        name: 'Qwen3.5-2B',
        file: 'Qwen3.5-2B-Q4_K_M.gguf',
        repo: 'unsloth/Qwen3.5-2B-GGUF',
        quant: 'Q4_K_M',
        modelClass: 'A',
        passRate: 0.682,
        citationCompliance: 0.932,
        tokensPerSecond: 86.214,
        bytes: 1280835840,
    },
    {
        id: 'lfm2.5-1.2b',
        name: 'LFM2.5-1.2B',
        file: 'LFM2.5-1.2B-Instruct-Q4_K_M.gguf',
        repo: 'LiquidAI/LFM2.5-1.2B-Instruct-GGUF',
        quant: 'Q4_K_M',
        modelClass: 'A',
        passRate: 0.295,
        citationCompliance: 0.432,
        tokensPerSecond: 109.945,
        bytes: 730895168,
    },
    {
        id: 'minicpm5-1b',
        name: 'MiniCPM5-1B',
        file: 'MiniCPM5-1B-Q4_K_M.gguf',
        repo: 'openbmb/MiniCPM5-1B-GGUF',
        quant: 'Q4_K_M',
        modelClass: 'A',
        passRate: 0.25,
        citationCompliance: 0.545,
        tokensPerSecond: 170.848,
        bytes: 688065920,
    },
    {
        id: 'qwen2.5-coder-1.5b',
        name: 'Qwen2.5-Coder-1.5B',
        file: 'qwen2.5-coder-1.5b-instruct-q4_k_m.gguf',
        repo: 'Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF',
        quant: 'Q4_K_M',
        modelClass: 'A',
        passRate: 0.227,
        citationCompliance: 0.705,
        tokensPerSecond: 84.048,
        bytes: 1117320768,
    },
    {
        id: 'qwen3.5-4b',
        name: 'Qwen3.5-4B',
        file: 'Qwen3.5-4B-Q4_K_M.gguf',
        repo: 'unsloth/Qwen3.5-4B-MTP-GGUF',
        quant: 'Q4_K_M',
        modelClass: 'B',
        passRate: 0.818,
        citationCompliance: 0.955,
        tokensPerSecond: 37.836,
        bytes: 2834975040,
    },
    {
        id: 'gemma-4-e4b',
        name: 'gemma-4-E4B',
        file: 'gemma-4-E4B-it-Q4_K_M.gguf',
        repo: 'unsloth/gemma-4-E4B-it-GGUF',
        quant: 'Q4_K_M',
        modelClass: 'B',
        passRate: 0.841,
        citationCompliance: 1,
        tokensPerSecond: 31.831,
        bytes: 4977171584,
    },
];

/**
 * Die Form einer Hugging-Face-Repo-Kennung.
 *
 * `user/repo` mit einem wahlweisen `:quant`. Dieselbe Form wie die Pruefung in
 * llm/fetch-model.sh, und beide sind eine reine FORMPRUEFUNG: ob es das
 * Repository gibt, weiss nur huggingface.co, und diese Oberflaeche spricht mit
 * huggingface.co nicht. Ein Feld, das "gibt es nicht" sagen wuerde, wuerde eine
 * Auskunft vortaeuschen, die es nicht hat.
 */
export const HF_REPO_PATTERN = /^[A-Za-z0-9._-]+\/[A-Za-z0-9._-]+(:[A-Za-z0-9._-]+)?$/;

/** Wie eine eingegebene Repo-Kennung gelesen wurde. */
export interface RepoReading {
    /** Ob die Form stimmt. Nur die Form. */
    ok: boolean;
    /** Der Teil vor dem Doppelpunkt. Leer, wenn die Form nicht stimmt. */
    repo: string;
    /** Der Teil dahinter. Leer, wenn keiner angegeben war. */
    quant: string;
    /** Was an der Eingabe nicht der Form entspricht. Leer, wenn sie stimmt. */
    problem: 'empty' | 'shape' | '';
}

/**
 * Eine Eingabe in das freie Feld lesen.
 *
 * Was nicht der Form entspricht, wird als solches benannt und nicht
 * stillschweigend geschluckt: ein Feld, das jede Eingabe annimmt und daraus
 * einen Befehl baut, gibt dem Leser eine Zeile in die Hand, die in seiner
 * Kommandozeile scheitert, und laesst ihn den Fehler dort suchen.
 */
export function readRepoInput(value: string): RepoReading {
    const trimmed = value.trim();
    if (trimmed.length === 0) {
        return { ok: false, repo: '', quant: '', problem: 'empty' };
    }
    if (!HF_REPO_PATTERN.test(trimmed)) {
        return { ok: false, repo: '', quant: '', problem: 'shape' };
    }
    const colon = trimmed.indexOf(':');
    return colon === -1
        ? { ok: true, repo: trimmed, quant: '', problem: '' }
        : { ok: true, repo: trimmed.slice(0, colon), quant: trimmed.slice(colon + 1), problem: '' };
}

/**
 * Der fertige Aufruf, mit dem der Leser ein Modell holt.
 *
 * Es ist das Skript dieses Projekts und nicht der nackte llama-server-Aufruf,
 * und der Grund ist der Ablageort: `-hf` laedt in das Verzeichnis, das
 * llama.cpp aus `LLAMA_CACHE` liest, und ohne diese Variable auf macOS nach
 * `$HOME/Library/Caches/llama.cpp`, wo llm/start.sh es dann nicht findet. Das
 * Skript setzt die Variable auf das Cache-Verzeichnis dieses Projekts, sagt vor
 * dem Lauf, dass es ins Netz geht, und zeigt danach, was im Cache liegt. Ein
 * kopierter Befehl, der die Datei woanders ablegt als der Starter sie sucht,
 * waere ein Befehl, der laeuft und trotzdem nicht hilft.
 */
export function fetchCommand(repo: string, quant = ''): string {
    return `llm/fetch-model.sh ${quant.length > 0 ? `${repo}:${quant}` : repo}`;
}

/**
 * Ein Anteil als Prozentangabe, mit einer Nachkommastelle, wo sie etwas sagt.
 *
 * `0.682` wird `68.2%` und `1` wird `100%`. Auf ganze Prozent zu runden waere
 * bei 44 Fragen eine Rundung ueber eine halbe Frage hinweg, und der Unterschied
 * zwischen zwei Kandidaten liegt gelegentlich genau dort.
 */
export function percentText(share: number): string {
    if (!Number.isFinite(share)) {
        return '';
    }
    const value = share * 100;
    const rounded = Math.round(value * 10) / 10;
    return `${Number.isInteger(rounded) ? rounded : rounded.toFixed(1)}%`;
}

/** Das Tempo als ganze Zahl. Nachkommastellen bei Token je Sekunde sind Rauschen. */
export function speedText(tokensPerSecond: number): string {
    return Number.isFinite(tokensPerSecond) ? String(Math.round(tokensPerSecond)) : '';
}

/** Das Cache-Verzeichnis, so wie llm/start.sh es ohne Umgebungsvariable waehlt. */
export const MODEL_CACHE_PATH = 'models/';

/**
 * Die beiden Umgebungsvariablen, die diesen Ort verschieben.
 *
 * Sie stehen im Ehrlichkeitssatz mit dabei, weil die Oberflaeche den wirklichen
 * Ort nicht kennt: sie hat kein Backend und kann kein Verzeichnis lesen. Was
 * sie ehrlich sagen kann, ist die VORGABE und wodurch sie sich aendert; ein
 * absoluter Pfad an dieser Stelle waere geraten.
 */
export const MODEL_CACHE_ENV = 'ATLAS_MODELS_DIR, LLAMA_CACHE';
