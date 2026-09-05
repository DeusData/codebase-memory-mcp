/**
 * Die Lese-Flaeche: Monaco, read-only, mit dem Quelltext, den der Server
 * geliefert hat.
 *
 * Read-only steht hier doppelt, `readOnly` und `domReadOnly`, und das ist kein
 * Guertel-und-Hosentraeger. `readOnly` haelt Monacos eigene Bearbeitungsbefehle
 * an; `domReadOnly` setzt `contenteditable` im DOM auf false, so dass auch ein
 * Weg an Monaco vorbei (Eingabemethode, Einfuegen per Browser-Menue) nichts
 * hineinschreibt. Der Beweislauf prueft beides: er liest die Option UND tippt.
 *
 * Es gibt keine Bearbeitungsabsicht in diesem Produkt. Dies ist eine Lese-IDE
 * (PLAN.md Abschnitt 1): man guckt, versteht und navigiert; editiert wird
 * woanders.
 */
import type { JSX } from 'react';
import { useEffect, useRef } from 'react';
import { ATLAS_THEME, prepareMonaco } from './monaco-setup';
import type { ReaderDocument } from './file-source';
import type { StepBadge } from '../core/step-badge-decorator';
import { badgeDecorations, highlightDecorations } from './step-badges';

/**
 * Die Lagen der Lese-Flaeche.
 *
 * `unavailable` ist seit W4d eine eigene Lage und ausdruecklich nicht
 * `failed`: eine Datei, die der Index uebersprungen oder nach Absicht
 * ausgeschlossen hat, ist kein Fehler dieser Oberflaeche, sondern eine Grenze
 * der heutigen Server-Flaeche. Sie in Alarmfarbe zu zeigen hiesse, dem Leser
 * einen Ausfall zu melden, wo der Server genau das getan hat, was in seinen
 * Regeln steht.
 */
export type ReaderStatus = 'idle' | 'loading' | 'ready' | 'failed' | 'unavailable';

export interface MonacoReaderProps {
    status: ReaderStatus;
    /** Was gerade zu lesen ist. Fehlt, solange nichts offen ist. */
    document?: ReaderDocument | undefined;
    /** Der Satz, der statt des Editors steht, wenn es nichts zu zeigen gibt. */
    message: string;
    /**
     * Die numerierten Aufrufstellen des Symbols, in dem der Caret gerade steht.
     * Leer heisst: keine Badges, nicht "dieses Symbol ruft nichts".
     */
    badges?: readonly StepBadge[];
    /** 1-basierte Zeile, deren Badge pulst. Der Caret, wenn er auf einer sitzt. */
    pulseLine?: number | undefined;
    /** 1-basierte Zeile, die hervorgehoben wird, weil das Panel auf sie zeigt. */
    highlightLine?: number | undefined;
    /** Die Zeile, in der der Caret jetzt steht, 1-basiert. Ungedaempft. */
    onCursorLine?: ((line: number) => void) | undefined;
    /** 1-basierte Zeile, zu der gesprungen werden soll. */
    revealLine?: number | undefined;
    /**
     * Zaehler, der einen Sprung ausloest.
     *
     * Ohne ihn waere zweimal zur selben Zeile springen ein Nichts-Tun: die
     * Eigenschaft haette sich nicht geaendert. Ein Leser, der zweimal auf
     * denselben Schritt klickt, erwartet aber zweimal denselben Sprung.
     */
    revealNonce?: number;
}

/**
 * Der Griff, an dem der Beweislauf den Editor anfasst.
 *
 * Ein Beweislauf, der nur durch das DOM guckt, kann "ist read-only" nicht von
 * "sieht read-only aus" unterscheiden: Monaco malt seinen Text in Spans, und
 * Spans sind immer unveraenderlich. Ueber diesen Griff liest der Lauf die
 * Option am Editor selbst und tippt danach dagegen. Der Griff ist absichtlich
 * schmal und traegt nur, was gefragt wird.
 */
export interface AtlasReaderSeam {
    /** Der Editor, oder undefined solange keiner steht. */
    editor?: unknown;
    /** Der Inhalt, den der Editor gerade haelt. */
    value(): string;
    /** Ob der Editor die Option read-only fuehrt. */
    readOnly(): boolean;
    /** Das geladene Dokument, mit Kappungsbefund. */
    document?: ReaderDocument | undefined;
    status: ReaderStatus;
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasReader: AtlasReaderSeam | undefined;
}

export default function MonacoReader(props: MonacoReaderProps): JSX.Element {
    const hostRef = useRef<HTMLDivElement | null>(null);
    const editorRef = useRef<ReturnType<typeof createEditor> | null>(null);
    // Zwei getrennte Mengen, weil sie unterschiedlich oft wechseln: die Badges
    // einmal je Symbol, die Hervorhebung bei jeder Mausbewegung ueber die
    // Schrittliste. Eine gemeinsame Menge wuerde die jeweils andere bei jedem
    // Setzen loeschen.
    const badgeSetRef = useRef<ReturnType<NonNullable<typeof editorRef.current>['createDecorationsCollection']> | null>(null);
    const highlightSetRef = useRef<typeof badgeSetRef.current>(null);
    // Der Rueckruf liegt in einem Ref, damit der Abonnent des Editors einmal
    // angelegt wird und nicht bei jedem Bild neu: ein Abonnement je Render
    // waere ein Leck, das man erst nach einer Stunde Lesen bemerkt.
    const cursorRef = useRef(props.onCursorLine);
    cursorRef.current = props.onCursorLine;

    function createEditor(host: HTMLDivElement) {
        const monaco = prepareMonaco();
        return monaco.editor.create(host, {
            value: '',
            theme: ATLAS_THEME,
            readOnly: true,
            domReadOnly: true,
            automaticLayout: true,
            fontFamily:
                'ui-monospace, SFMono-Regular, "SF Mono", "JetBrains Mono", Menlo, Consolas, monospace',
            fontSize: 13,
            lineHeight: 20,
            minimap: { enabled: false },
            scrollBeyondLastLine: false,
            renderLineHighlight: 'line',
            lineNumbersMinChars: 4,
            glyphMargin: true,
            contextmenu: false,
            roundedSelection: false,
            padding: { top: 8, bottom: 8 },
            scrollbar: { verticalScrollbarSize: 10, horizontalScrollbarSize: 10 },
        });
    }

    // Der Editor wird einmal gebaut und lebt so lange wie die Flaeche. Ein
    // Editor je Datei waere ein Wegwerf-Fenster pro Klick.
    useEffect(() => {
        const host = hostRef.current;
        if (host === null || editorRef.current !== null) {
            return;
        }
        const editor = createEditor(host);
        editorRef.current = editor;
        badgeSetRef.current = editor.createDecorationsCollection([]);
        highlightSetRef.current = editor.createDecorationsCollection([]);
        const cursor = editor.onDidChangeCursorPosition((event) => {
            // Monaco zaehlt Zeilen 1-basiert, der Graph auch. Hier wird nichts
            // umgerechnet, und genau darum steht es hier und nicht in
            // positions.ts.
            cursorRef.current?.(event.position.lineNumber);
        });
        return () => {
            cursor.dispose();
            const model = editor.getModel();
            editor.dispose();
            model?.dispose();
            badgeSetRef.current = null;
            highlightSetRef.current = null;
            editorRef.current = null;
            globalThis.__atlasReader = undefined;
        };
        // Absichtlich einmalig: der Editor haengt an der Flaeche, nicht am Dokument.
    }, []);

    // Das Modell traegt den Pfad als URI. Daran erkennt Monaco die Sprache, und
    // zwar ueber dieselbe Endungstabelle, die auch der Rest von Monaco benutzt.
    // Eine eigene Endung-zu-Sprache-Tabelle waere eine zweite Wahrheit gewesen.
    useEffect(() => {
        const editor = editorRef.current;
        if (editor === null) {
            return;
        }
        const monaco = prepareMonaco();
        const previous = editor.getModel();
        if (props.document === undefined) {
            editor.setModel(null);
            previous?.dispose();
            return;
        }
        const uri = monaco.Uri.parse(`atlas:///${props.document.path}`);
        const existing = monaco.editor.getModel(uri);
        existing?.dispose();
        const model = monaco.editor.createModel(props.document.source, undefined, uri);
        editor.setModel(model);
        editor.setScrollTop(0);
        if (previous !== null && previous !== model) {
            previous.dispose();
        }
    }, [props.document]);

    // Die Badges im Rand. Sie haengen an den Schritten UND am Dokument: ein
    // Dateiwechsel tauscht das Modell aus, und eine Menge, die nicht neu gesetzt
    // wuerde, malte die Aufrufstellen der alten Datei in die neue.
    useEffect(() => {
        badgeSetRef.current?.set(badgeDecorations(props.badges ?? [], props.pulseLine));
    }, [props.badges, props.pulseLine, props.document]);

    // Die Zeile, auf die das Panel gerade zeigt.
    useEffect(() => {
        highlightSetRef.current?.set(highlightDecorations(props.highlightLine));
    }, [props.highlightLine, props.document]);

    // Der Sprung zu einer Zeile. Haengt am Zaehler UND am Dokument, weil beim
    // Folgen in eine andere Datei die Bitte vor dem Inhalt da ist: sie muss
    // noch einmal greifen, wenn der Inhalt nachkommt.
    useEffect(() => {
        const editor = editorRef.current;
        const line = props.revealLine;
        if (editor === null || line === undefined || props.document === undefined) {
            return;
        }
        const model = editor.getModel();
        if (model === null) {
            return;
        }
        const clamped = Math.min(Math.max(1, line), model.getLineCount());
        editor.setPosition({ lineNumber: clamped, column: 1 });
        editor.revealLineInCenter(clamped);
    }, [props.revealNonce, props.revealLine, props.document]);

    // Den Griff frisch halten. Er wird bei jedem Zustandswechsel neu gesetzt,
    // damit er nie eine Lage von vorhin beschreibt.
    useEffect(() => {
        const monaco = prepareMonaco();
        globalThis.__atlasReader = {
            editor: editorRef.current ?? undefined,
            value: () => editorRef.current?.getValue() ?? '',
            // Die Option am Editor selbst, nicht die, die beim Bauen mitgegeben
            // wurde: nur die erste ist eine Aussage darueber, was der Editor
            // gerade tut.
            readOnly: () => editorRef.current?.getOption(monaco.editor.EditorOption.readOnly) === true,
            document: props.document,
            status: props.status,
        };
    }, [props.document, props.status]);

    const showPlaceholder = props.status !== 'ready' || props.document === undefined;

    return (
        <>
            <div
                className="atlas-reader-editor"
                data-testid="atlas-reader-editor"
                data-status={props.status}
                ref={hostRef}
            />
            {showPlaceholder && (
                <p
                    className="atlas-reader-placeholder"
                    data-testid="atlas-reader-placeholder"
                    data-state={props.status === 'failed' ? 'failed' : props.status}
                >
                    {props.message}
                </p>
            )}
        </>
    );
}
