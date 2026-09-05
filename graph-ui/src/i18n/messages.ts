/**
 * Der typisierte Katalog der sichtbaren Chrome-Strings.
 *
 * Die Mechanik ist die des PR-Frontends (INVENTAR.md Abschnitt 1): EIN Objekt,
 * `as const`, nach Bereichen geordnet, und alles, was eine Zahl oder einen Namen
 * einsetzen muss, ist eine Funktion statt einer Zeichenkette mit Platzhaltern.
 * Eine Funktion kann der Uebersetzer nicht falsch zusammensetzen, und der
 * Aufrufer bekommt die Stelligkeit vom Typ gesagt.
 *
 * ## Was hier steht und was ausdruecklich nicht
 *
 * Hier stehen die Saetze des RAHMENS: Kopfzeile, Menue, Tabs, Statusleiste,
 * Explorer, Kommandozeile, Reader-Notizen, Suchfenster, Why-Karten, Tour-Karte
 * und die Rahmen von Einstiegsdialog, BUG-Assistent, Impact-Ansicht,
 * Sidecar-Karte und Chat.
 *
 * Hier stehen NICHT die Fachtexte der portierten Module. `src/twin/strings.ts`,
 * `src/pseudocode/pseudocode-strings.ts`, `src/impact/impact-strings.ts`,
 * `src/traces/bug-wizard-strings.ts`, `src/chat/chat-strings.ts`,
 * `src/llm/strings.ts`, `src/app/coverage-strings.ts` und `src/why/why-model.ts`
 * bleiben die Quelle ihrer eigenen Saetze. Das ist kein Versaeumnis, sondern der
 * Standard, aus dem dieses Muster kommt: ein Fachtext gehoert neben die Regel,
 * die ihn begruendet, und ein zweiter Ort fuer denselben Satz waere ein zweiter
 * Ort, an dem ihn jemand schaerft.
 *
 * ## Ein Baum, eine Sprache, und keine erfundene zweite
 *
 * Es gibt genau `en`. Ein `zh`-Baum mit maschinell erzeugten Saetzen waere eine
 * unuebersetzte Luege in einer Struktur, die behauptet, uebersetzt zu sein. Die
 * Struktur ist erweiterbar: ein zweiter Baum derselben Form neben `messages` und
 * eine Wahl darueber, und keine Aufrufstelle aendert sich.
 *
 * ## Der eine Satz, der hier nicht englisch war
 *
 * `llm.readFrom` lautete "gelesen aus ...", als einziger Eintrag des Katalogs.
 * W6a hat ihn hierher verschoben und ausdruecklich nicht umgeschrieben, weil
 * jener Zyklus Saetze verschob; aufgefallen ist er, weil der Katalog ihn neben
 * seine Nachbarn stellte, und genau dafuer ist ein Katalog da. Das unabhaengige
 * Audit hat ihn als Befund 14 aufgeschrieben, und seit dem 2026-08-29 heisst er
 * "read from ...". Der Katalog ist jetzt in einer Sprache, was die Voraussetzung
 * dafuer ist, dass ein zweiter Baum daneben eine Uebersetzung waere und nicht
 * eine zweite Mischung.
 */

/** Ein Menuepunkt: sein Buchstaben-Kuerzel und der Rest des Wortes. */
export interface MenuItemMessage {
    readonly key: string;
    readonly rest: string;
}

/*
 * Drei Saetze stehen an mehr als einer Stelle im Baum, und sie stehen darum
 * genau einmal hier. Sie zweimal auszuschreiben waere zweimal die Gelegenheit,
 * einen davon zu schaerfen, bis die beiden verschieden sind.
 */
const NO_PROJECT = 'no project';
const CLOSE = 'close';
const TOUR_FAILED_TEXT = 'the walk could not be prepared';

export const messages = {
    /** Die Kopfzeile und die Flaechen, die zu keinem Panel gehoeren. */
    app: {
        brand: 'CODEATLAS',
        /** Steht in der Tab-Leiste und im Breadcrumb, wenn nichts offen ist. */
        noFileOpen: 'no file open',
        /** Der Name, den eine Oberflaeche ohne Projekt traegt. */
        noProject: NO_PROJECT,
        closeTab: (name: string): string => `close ${name}`,
    },

    /**
     * Die Menuezeile und die Titel ihrer Punkte.
     *
     * Seit dem 2026-08-29 steht hier NUR noch, wofuer es eine Verdrahtung gibt.
     * Bis dahin trug die Zeile die fuenf Punkte des Terminal-Vorbilds
     * (`[f]ile [e]dit [v]iew [t]erminal` neben `[a]tlas`) und haengte den
     * unverdrahteten einen `title` an, der sagte, dass sie nichts tun. Das war
     * die Form des Vorbilds ohne dessen Inhalt: vier Woerter, die eine Bedienung
     * anbieten, die es in einer Lese-Oberflaeche nicht gibt und nicht geben
     * soll. Ein Tooltip macht aus einer Attrappe keine Auskunft; er macht sie zu
     * einer Attrappe mit Fussnote.
     *
     * Was bleibt, hat etwas dahinter: `[a]tlas` schaltet die Galaxie und traegt
     * die vier Aktionen der Atlas-Zeile, `[?]help` schlaegt die Hilfe auf. Die
     * Regel dazu wird strukturell geprueft und nicht per Liste gepflegt: jeder
     * Eintrag hier muss seinen Buchstaben in WIRED_MENU_SHORTCUTS haben
     * (src/app/shortcuts.ts, Test src/app/shortcuts.test.ts).
     */
    menu: {
        ariaLabel: 'main menu',
        items: [
            { key: 'a', rest: 'tlas' },
            { key: '?', rest: 'help' },
        ] as readonly MenuItemMessage[],
        /**
         * Was die Klammer um den Buchstaben bedeutet, in der Zeile selbst.
         *
         * Seit dem 2026-08-29 tragen die Buchstaben Alt/Option (siehe
         * src/app/keyboard.ts), und ein `[a]` ohne diese Angabe waere ein
         * Versprechen auf eine Taste, die etwas anderes tut. Die Angabe steht in
         * der Zeile und nicht nur in der Hilfe, weil sie dort gebraucht wird, wo
         * die Klammern stehen.
         */
        legend: 'alt + letter',
        atlasHide: 'atlas: hide the galaxy panel (alt+a)',
        atlasShow: 'atlas: show the galaxy panel (alt+a)',
        settings: 'atlas: which model answers, and everything that costs computing time (alt+s)',
        agentsOn:
            'atlas: live mode is on. The bridge is being read and the actors are drawn on the graph '
            + '(alt+g)',
        agentsOff:
            'atlas: live mode is off, and not one request goes to the bridge. Turn it on to see the '
            + 'agents working in this repository (alt+g)',
        why: 'atlas: put the question again and pick a way in (alt+w)',
        bug: 'atlas: compare the expected path into a symbol with the observed one (alt+b)',
        impact: 'atlas: what a change would reach, and what covers it (alt+c)',
        helpOpen: 'help: what this reads, what it cannot do, and every key it listens to (?)',
        helpClose: 'help: close this page (?)',
    },

    /**
     * Die Hilfeseite, ganz.
     *
     * Sie steht im Katalog und nicht in der Komponente, weil sie der laengste
     * zusammenhaengende Text der Oberflaeche ist: verteilt auf JSX waere sie der
     * eine Ort, an dem niemand mehr sieht, was das Produkt ueber sich selbst
     * behauptet. Hier steht es an einem Stueck und ist an einem Stueck lesbar.
     *
     * Zwei Dinge sind ausdruecklich so gebaut und nicht anders:
     *
     * 1. **Die Grenzen stehen vor den Faehigkeiten.** Wer die Hilfe aufschlaegt,
     *    soll zuerst erfahren, was hier nicht geht, samt Grund. Ein Produkt, das
     *    seine Grenzen ans Ende stellt, laesst den Leser sie selbst finden, und
     *    das ist die teuerste Art, sie zu erfahren.
     * 2. **Kein Netz-Link, nirgends.** Die Oberflaeche ist per Vorgabe
     *    abgeschottet (PLAN Abschnitt 3); ein Verweis ins Web waere im
     *    Normalbetrieb ein toter Knopf. Die Verweise sind darum Pfade in diesem
     *    Repository.
     */
    help: {
        title: 'CODEATLAS HELP',
        subtitle: 'a reading surface over an indexed repository',
        close: '[esc] close',
        closeLabel: CLOSE,

        whatTitle: 'What this is',
        what: [
            'CodeAtlasWeb reads a repository that the analysis server has indexed and explains it: '
            + 'the tree and the source, a twin of the symbol under the caret, a walk along the calls, '
            + 'the scope of a change, and the answers of a local model.',
            'Everything on this screen comes from that index or from a file the server handed over, '
            + 'and every panel says which of the two it is.',
        ],

        limitsTitle: 'What it cannot do',
        limits: [
            'The reader is read-only: you cannot edit a file here, and what you type into it changes nothing.',
            'It cannot run code, and there is no terminal: no build, no test run, no shell, '
            + 'no process started from this window.',
            'It reaches no cloud. The only model it can use is a sidecar on this machine, '
            + 'started by hand, and it stays off until it answers.',
        ],
        limitsWhy:
            'That is the shape of the product and not a missing release: this surface has no backend of '
            + 'its own, and the analysis server offers a read-only surface. Everything that writes, builds '
            + 'or executes happens where the repository lives, on the command line.',

        panelsTitle: 'The panels, and what each one answers',
        panels: [
            { name: 'explorer and reader', answers: 'what is in this project, and what does this file say' },
            { name: 'twin', answers: 'what does the symbol under the caret hold, call, raise and touch' },
            { name: 'flow', answers: 'in what order does this symbol call what it calls' },
            { name: '[w]hy am I here', answers: 'where do I start, and what is the walk from there' },
            { name: '[b]ug hunt', answers: 'how does the expected path into a symbol differ from the observed one' },
            { name: '[c]hange scope', answers: 'what would a change here reach, and what covers it' },
            { name: '[a]tlas', answers: 'where does this symbol sit among the others' },
            { name: '[l]ocal llm', answers: 'is a local model answering, and out of which context' },
            {
                name: '[s]ettings',
                answers:
                    'which model in your cache answers, how to get another one, and what the drawing '
                    + 'of the graph costs on this machine',
            },
            {
                name: 'command line',
                answers:
                    'which symbol means this: the words are matched against symbol names and their paths '
                    + 'while you type. A line that ends with a question mark builds an answer from indexed '
                    + 'cards and their source lines. Use the visible AI button on that answer only if you want an '
                    + 'optional local-model wording. This line runs no commands.',
            },
        ],

        shortcutsTitle: 'Every key this window listens to',
        shortcutsNote:
            'This table is derived from the wiring itself, not written beside it: the menu keys come from '
            + 'the list the window handler reads, the walk and search keys from the functions that decide '
            + 'what a key means. A key that stopped working could not stay in this table.',
        typingNote:
            'Everything else you type goes into the command line, even when nothing has the cursor: the '
            + 'line takes the focus and the character lands in it, where you can see it. That is why the '
            + 'menu keys carry alt (option on an Apple keyboard): a bare letter is text, not a command.',
        columnKey: 'key',
        columnWhere: 'where it counts',
        columnDoes: 'what it does',

        /*
         * Der Tastentest.
         *
         * Er steht in der Hilfe und nicht in einem Entwicklermodus, weil er den
         * Nutzer beantwortet, der einen Fehler meldet, den niemand nachstellen
         * kann. Am 2026-08-29 kam "alt plus letter funktioniert nur fuer
         * atlas"; an der laufenden Vorschau loesten alle fuenf Kuerzel aus. Statt
         * an einer Ursache zu raten, zeigt die Seite, was am Geraet des Lesers
         * wirklich ankommt, und was diese Oberflaeche daraus gemacht haette.
         */
        keyProbeTitle: 'Test a key combination',
        keyProbeIntro:
            'Press a combination now, with this page open, and read below what arrived. This is not a '
            + 'simulation: it is the same reading the window handler makes, from the same event.',
        keyProbeIdle: 'nothing pressed yet',
        keyProbeFields: {
            code: 'physical key (code)',
            key: 'character produced (key)',
            modifiers: 'modifiers',
            defaultPrevented: 'already consumed',
            consumedBy: 'consumed by',
            target: 'arrived at',
            shortcut: 'recognised as a menu key',
        } as Readonly<Record<string, string>>,
        keyProbeConsumers: {
            nobody: 'nobody: it arrived untouched',
            'this-window': 'this window: its own shortcut handler took it',
            'something-else':
                'something else, before this window saw it: a browser extension, the keyboard layout, '
                + 'or another handler on the page',
        } as Readonly<Record<string, string>>,
        keyProbeNoShortcut: 'no: this combination is not a menu key',
        keyProbeShortcut: (letter: string): string => `yes: the menu key for [${letter}]`,
        keyProbeNone: '-',
        keyProbeNoModifier: 'none',
        keyProbeNote:
            'What to look for: a menu key needs alt (option) and a code of the form KeyW. On an Apple '
            + 'keyboard option+w produces a different character, which is why the wiring reads the code '
            + 'and not the character. If nothing appears here at all when you press, the combination '
            + 'never reached this page, and the cause is outside it.',

        /** Der Modifikator vor einem Menuekuerzel. Auf Apple-Tastaturen: option. */
        altPrefix: 'alt+',
        scopes: {
            mnemonic: 'anywhere, also while you type',
            bare: 'while the command line does not have the cursor',
            line: 'while the command line does not have the cursor',
            walk: 'during a walk',
            search: 'while the search window is open',
        },
        keyNames: {
            Enter: 'enter',
            Escape: 'esc',
            ArrowUp: 'up',
            ArrowDown: 'down',
            ArrowLeft: 'left',
            ArrowRight: 'right',
        } as Readonly<Record<string, string>>,
        /**
         * Was eine Taste tut, je Bereich und Taste (`bereich:taste`).
         *
         * Der Schluessel ist zusammengesetzt, weil dieselbe Taste in zwei
         * Bereichen zwei Dinge tut: Enter geht im Walk einen Schritt weiter und
         * oeffnet im Suchfenster den Treffer.
         */
        shortcutDoes: {
            'mnemonic:a': 'show or hide the galaxy',
            'mnemonic:w': 'ask again where to start, and walk from there',
            'mnemonic:b': 'open the BUG hunt on the symbol in the twin',
            'mnemonic:c': 'open the change scope',
            'mnemonic:l': 'turn the local model on or off',
            'mnemonic:r': 'put every zone back to the width and height it starts with',
            'mnemonic:s': 'open or close the settings: the model, and what the drawing costs',
            'mnemonic:g': 'turn the live agent mode on or off. Off asks the bridge nothing at all',
            'bare:?': 'open or close this page',
            'line:/': 'put the cursor in the command line, without typing the slash itself',
            'walk:Enter': 'go to the next step of the walk, and finish it on the last one',
            'walk:ArrowLeft': 'go back one step',
            'walk:q': 'leave the walk',
            'walk:d': 'draw the flow of this step, when the step has a symbol',
            'search:ArrowUp': 'move the selection up',
            'search:ArrowDown': 'move the selection down',
            'search:Enter': 'open the selected hit, or ask the atlas when nothing matched',
            'search:Escape': 'close the search window',
        } as Readonly<Record<string, string>>,

        operationsTitle: 'Running it',
        operations: [
            'Index a project with the CLI of the analysis server, then open this page with the project '
            + 'name in the address (?project=<name>). This window indexes nothing itself.',
            'Start the local model with llm/start.sh class-a. It listens on the loopback address of this '
            + 'machine; the [l]lm card turns green as soon as it answers, and says why when it does not.',
            'Bring recorded runs in with ingest_traces on the command line. There is no importer on this '
            + 'surface, and the twin does not carry them: they show up on the BUG hunt hops and in the flow.',
        ],

        honestyTitle: 'The rules this surface holds itself to',
        honesty: [
            'Every claim comes from the index or from the file the server sent, and the panel names which.',
            'What the index does not record is named as a gap, never filled in with a plausible guess.',
            'An affordance that does nothing does not exist here: a button or a key either acts, or it is '
            + 'not on the screen.',
        ],

        referencesTitle: 'Where to read further, in this repository',
        references: [
            { path: 'PLAN.md', about: 'the plan this surface was built against, and its honesty rules' },
            { path: 'INVENTAR.md', about: 'what the analysis server delivers, and what it does not' },
            { path: 'UPSTREAM-ASKS.md', about: 'the gaps that were handed back to the server' },
            { path: 'docs/adr/0001-modellwahl.md', about: 'why this local model and no other' },
            { path: 'verification/', about: 'the proof runs: screenshots, measurements, gate reports' },
        ],
        referencesNote:
            'Paths in this repository, not links. This window is offline by design, so a link into the '
            + 'web would be a button that leads nowhere.',
    },

    /** Die Chips der Kopfzeile und der Statusleiste. */
    statusbar: {
        chipProject: 'project',
        chipSymbols: 'sym',
        chipEdges: 'edg',
        chipServer: 'server',
        chipPort: 'port',
        chipTree: 'tree',
        chipSource: 'source',
        chipGalaxy: 'galaxy',
        /*
         * Nutzerwunsch vom 2026-08-29: das Ding heisst ueberall gleich.
         *
         * Der Chip hiess "llm", der Menuepunkt "[l]lm on", die Karte
         * "LOCAL_MODEL": drei Namen fuer eine Sache, und der kuerzeste davon
         * sagte nicht, dass sie auf diesem Rechner laeuft. Seit W7c heisst sie
         * an allen drei Stellen "local llm"; der Buchstabe des Kuerzels bleibt
         * `l` (src/llm/strings.ts).
         */
        chipLlm: 'local llm',
        chipCoverage: 'coverage',
        chipExplored: 'explored',
        chipWalk: 'walk',
        /** Der Wert eines Chips, dessen Zahl der Server nicht genannt hat. */
        noCount: 'no count',
        serverContacting: 'contacting',
        serverReady: 'ready',
        serverUnreachable: 'unreachable',
        noFileLoaded: 'no file loaded',
        noProjectIndexed: 'no project: this server has nothing indexed',
        noProjectError: (detail: string): string => `no project: ${detail}`,
    },

    /** Der Explorer: seine Ueberschrift und die eine Zeile seiner Ordner-Legende. */
    explorer: {
        title: 'EXPLORER',
        folderLabel: 'folder',
        folderDescription: 'the worst stage of anything below it',
    },

    /** Die Notizen um den Editor herum. */
    reader: {
        /** Die Marke vor der Kappungszeile unter dem Editor. */
        incompleteLabel: 'incomplete: ',
        pickFile: 'pick a file in the explorer',
        loading: (path: string, tool: string): string => `loading ${path} over ${tool} ...`,
    },

    /**
     * Die Kommandozeile: Platzhalter und die beiden Hinweise am rechten Rand.
     *
     * Der `title` ist seit dem 2026-08-29 weg. Ein nativer Tooltip legt sich in
     * dieser Oberflaeche ueber den Anfang der Zeile, also ueber genau den Text,
     * den der Leser gerade tippt (Nutzer-Screenshot); er ist ausserdem nicht zu
     * gestalten und kommt zu spaet, um eine Frage zu beantworten, die man beim
     * Hinsehen hat. Was er sagte, steht jetzt in der Hilfe unter "the panels,
     * and what each one answers".
     */
    command: {
        ariaLabel: 'command line',
        /*
         * Der Rueckfall, wenn noch kein Index geladen ist.
         *
         * Steht einer, nennt die Zeile ein echtes Beispiel mit einem Symbol
         * dieses Projekts (src/search/command-examples.ts). Der Nutzer am
         * 2026-08-29: "anstelle von 'type a command or ask the atlas' explizite
         * Beispiele, wie man den Chat nutzt, sonst weiss niemand, wie man es
         * nutzt."
         */
        placeholder: 'type a command or ask the atlas...',
        examplesLabel: 'what you can type here',
        hintIdle: 'type 2 letters to search by meaning',
        hintOpen: 'up/down picks, enter opens, esc closes',
    },

    /** Das Suchfenster ueber der Kommandozeile. */
    search: {
        resultsLabel: 'search results',
        searching: (query: string): string => `searching for "${query}" ...`,
        failed: 'the search could not be answered',
        noFile: 'this hit carries no file in the index, so there is nothing to open',
        /**
         * Was im Trefferfenster steht, solange erst ein Zeichen getippt ist.
         *
         * Der Hinweis stand bis zum 2026-08-29 nur klein am rechten Rand der
         * Zeile ("type 2 letters to search by meaning"). Wer tippt, sieht aber
         * dorthin, wo die Treffer erscheinen, und dort stand nichts: aus
         * Nutzersicht antwortet die Suche auf den ersten Buchstaben mit
         * Schweigen. Jetzt steht der Satz an der Stelle, an der die Antwort
         * erwartet wird.
         */
        oneMoreLetter: 'one more letter, then the atlas searches by meaning',
        /*
         * Die Sofort-Vorschlaege, und dass sie vorlaeufig sind.
         *
         * Nutzerbefund vom 2026-08-29: die Vorschlaege erscheinen zu langsam.
         * Die Antwort darauf sind Zeilen aus dem, was der Browser schon geladen
         * hat, sichtbar bevor der Index geantwortet hat. Sie muessen als das
         * erkennbar sein, was sie sind, sonst waeren sie eine leise Luege ueber
         * die Vollstaendigkeit der Suche: hier steht nicht "das findet der
         * Index", sondern "das kennt dieses Fenster schon".
         */
        provisionalHeadline: (query: string, shown: number): string =>
            shown === 1
                ? `1 suggestion for "${query}" from what is already loaded, the index is answering ...`
                : `${shown} suggestions for "${query}" from what is already loaded, the index is answering ...`,
        provisionalEmpty: (query: string): string =>
            `nothing loaded answers "${query}" yet, asking the index ...`,
        /** Die Marke an einer vorlaeufigen Zeile. Kurz, weil sie an jeder steht. */
        provisionalMark: 'loaded',
        provisionalMarkTitle:
            'from the tree and the graph this window already holds, not from an answer of the index',
    },

    /** Die Frage nach dem Warum. Die Karten selbst stehen in why/why-model.ts. */
    why: {
        /** Was in der Fusszeile der Karte steht, wenn kein Projekt geladen ist. */
        noProject: NO_PROJECT,
    },

    /** Die Schrittkarte einer Fuehrung. */
    tour: {
        prev: '[<-] prev',
        prevUnavailable: 'this is the first step, so there is no previous step to return to',
        nextPrefix: '[Enter] ',
        next: 'next',
        finish: 'finish',
        exit: '[q] exit',
        /** Die vierte Aktion der Karte (PLAN Abschnitt 4, Audit-Befund 13). */
        diagram: '[d] diagram',
        diagramTitle: 'draw the flow of this step: what it calls, in order, and what each call raises',
        /**
         * Was dort steht, wenn dieser Schritt kein Symbol hat.
         *
         * Der Satz nennt den Grund und nicht die Wirkung: "nothing to draw"
         * klaenge wie ein Befund ueber die Datei, und dies ist eine Auskunft
         * ueber den Schritt.
         */
        diagramUnavailable:
            'this step points at a file, not at a symbol, so there is no call flow to draw',
        failed: TOUR_FAILED_TEXT,
        failedNoFile: (name: string): string =>
            `${TOUR_FAILED_TEXT}: the index names no file for ${name}`,
    },

    /** Der Einstiegsdialog. */
    entry: {
        title: 'Where do you want to start?',
        subline:
            'The walk starts at what you pick here and goes forward over the calls the index recorded.',
        searchPlaceholder: 'or search for a name ...',
        searchLabel: 'search for a symbol by meaning',
        close: '[esc] close',
        closeLabel: CLOSE,
        searching: 'searching ...',
        notOpenable: 'the index names this way in but no file for it, so there is nothing to open',
        startWalkAt: (name: string): string => `start the walk at ${name}`,
        overviewFailed: (detail: string): string =>
            `the summary of ways in could not be read: ${detail}`,
        noAnswer: (query: string): string => `no symbol answers "${query}"`,
    },

    /** Der Rahmen des BUG-Assistenten. Seine Saetze stehen in bug-wizard-strings.ts. */
    wizard: {
        closeLabel: CLOSE,
        unresolvedHop: (name: string): string =>
            `the index does not resolve "${name}", so there is nothing to open.`,
    },

    /** Der Rahmen der Aenderungsansicht. Ihre Saetze stehen in impact-strings.ts. */
    impact: {
        close: '[esc] close',
        closeLabel: CLOSE,
        openRow: (name: string): string => `open ${name}`,
        rowNotOpenable: 'the index names no file for this row, so there is nothing to open',
        tileChangedFiles: 'changed files',
        tileDirectSymbols: 'symbols in them',
        tileEndpoints: 'endpoints',
        tileTestsAffected: 'tests affected',
        tileUntestedAffected: 'untested affected',
        /** Ein Abstandsband, ein- und mehrzahlig. */
        stepsOut: (distance: number): string =>
            distance === 1 ? 'one step out' : `${distance} steps out`,
        noTestCaller: 'no test caller found',
    },

    /** Die Karte des lokalen Modells. Ihre Zustandssaetze stehen in llm/strings.ts. */
    llm: {
        panelLabel: 'local model',
        on: 'on',
        off: 'off',
        toggleBlocked: 'denied by the project policy: this switch has no effect',
        toggleOff: 'turn the local model off',
        toggleOn: 'turn the local model on',
        /** Siehe Kopf dieser Datei: bis zum 2026-08-29 der eine deutsche Eintrag. */
        readFrom: (source: string): string => `read from ${source}`,
        rowModel: 'model',
        rowContext: 'context',
        rowWeights: 'weights',
        rowSlots: 'slots',
        sourceModel: 'GET /props (model_path, model_ftype)',
        sourceContext: 'GET /props (n_ctx), GET /v1/models (n_ctx_train)',
        sourceWeights: 'GET /v1/models (meta.size, meta.n_params)',
        sourceSlots: 'GET /props (total_slots), Port aus llm/start.sh',
        contextValue: (tokens: number, trained: number | undefined): string =>
            trained === undefined ? `${tokens} tokens` : `${tokens} of ${trained} trained tokens`,
        weightsValue: (size: string, billions: string | undefined): string =>
            billions === undefined ? size : `${size} for ${billions}B parameters`,
        slotsValue: (slots: number, port: number): string => `${slots} on 127.0.0.1:${port}`,
    },

    /**
     * Die Zonen, ihre Griffe und der Erklaeren-Bereich mit seinen Reitern.
     *
     * Der Bereich ist neu in W8 und ersetzt drei Flaechen, die sich bis dahin um
     * denselben Platz stritten (Flow-Erklaerer, Antwort-Panel, Schrittkarte).
     * Zwei Sorten Saetze stehen darum hier, und der Unterschied zwischen ihnen
     * ist der Kern dieses Zyklus:
     *
     *  - `disabled` sagt, warum ein Reiter gerade nichts zu zeigen hat. Er
     *    verschwindet nicht: ein Reiter, der mal da ist und mal nicht, laesst
     *    den Leser suchen, wo vorher etwas war. Er steht gedimmt da und sagt
     *    seinen Grund IM FELD, nicht in einem Tooltip. Ein Tooltip macht aus
     *    einer Attrappe keine Auskunft (dieselbe Regel wie in der Menuezeile).
     *  - `note` ist die eine Zeile, die der eingeklappte Streifen zeigt. Sie
     *    beantwortet die Frage, die W7c fuer den Chat schon beantwortet hatte:
     *    "ist mein Verlauf noch da?". Seit W8 gilt sie fuer jeden Reiter.
     */
    layout: {
        explainLabel: 'explain',
        explainTabsLabel: 'what explains the code in front of you',
        explainEmptyLabel: 'nothing to explain here yet',
        collapse: '[esc] fold',
        collapseTooltip:
            'fold this area away. Nothing in it is lost: the chat keeps its history, the walk its '
            + 'step and the flow its place. Only "clear" in the chat deletes anything.',
        expand: 'unfold',
        expandTooltip: 'unfold this area again, on the tab that was open last',
        tab: {
            flow: 'flow',
            walk: 'walk',
            chat: 'chat',
            bug: 'bug hunt',
            change: 'change scope',
        },
        tabTitle: {
            flow: 'the recorded calls of the symbol in the twin, as a sequence and as steps',
            walk: 'the walk in front of you: one step, what it is, and where it sits',
            chat: 'what was asked in this session, with the cards each answer was given',
            bug: 'the expected path into a symbol against the observed one',
            change: 'what a change would reach, and what covers it',
        },
        disabled: {
            flow:
                'No symbol in the twin yet, so there is no sequence to draw. Open a file and put '
                + 'the caret in a function; the twin follows it, and this tab follows the twin.',
            walk:
                'No walk is running. Start one from the atlas menu ("why am I here") and its steps '
                + 'appear here.',
            chat:
                'Nothing was asked in this session yet. Type a question in the command line and '
                + 'press enter; the answer and the cards it was given show up here.',
            bug:
                'No project is open, and this assistant reads a project index. Open one and the '
                + 'expected path against the observed one is drawn here.',
            change:
                'No project is open, and this view reads a project index. Open one and the reach '
                + 'of a change is drawn here.',
        },
        note: {
            flow: (subject: string): string => `flow of ${subject}`,
            flowIdle: 'no symbol in the twin',
            walk: (step: number, steps: number): string => `walk, step ${step} of ${steps}`,
            walkIdle: 'no walk running',
            chat: (turns: number): string =>
                turns === 1 ? '1 question in this session' : `${turns} questions in this session`,
            chatIdle: 'nothing asked yet',
            bug: 'bug hunt',
            change: 'change scope',
        },
        /**
         * Die vier Griffe.
         *
         * Jeder traegt seinen eigenen Namen, weil vier Trennlinien mit demselben
         * Etikett fuer eine Vorlesehilfe vier gleiche Elemente sind. Dazu die
         * Lage in Worten (`value`), damit ein Vorleseprogramm nicht nur weiss,
         * was es anfasst, sondern auch, wo es steht.
         *
         * Der vierzeilige Tooltip, der bis W10b an allen vier Griffen hing, ist
         * ersatzlos weg. Nutzerauftrag vom 2026-08-29: "diese Meldung nicht
         * anzeigen, das wird klar durch alles andere. Bitte an allen Bordern die
         * Meldung entfernen." Seit W8b traegt jeder Griff eine sichtbare Marke;
         * der Kasten erklaerte damit etwas, das man sieht, und verdeckte dabei
         * Inhalt. Der Grund steht ausfuehrlich im Kopf von src/layout/Splitter.tsx.
         */
        splitter: {
            left: 'width of the explorer',
            explain: 'height of the explain area',
            right: 'width of the understanding column',
            twin: 'height of the twin against the graph',
            value: (pixels: number): string => `${pixels} pixels`,
        },
        /**
         * Der Weg zurueck.
         *
         * Ein Befehl und ein Menuepunkt, und beide gehen durch dieselbe Funktion:
         * zwei Wege, die dasselbe TUN sollen, aber es getrennt tun, sind zwei
         * Wege, die auseinanderlaufen koennen.
         */
        reset: 'reset layout',
        resetMenuLabel: '[r]eset layout',
        resetTooltip: 'atlas: put every zone back to the width and height it starts with (alt+r)',
        resetCommandHint: 'press enter to put every zone back to its default size',
    },

    /** Der Rahmen des Antwort-Panels. Seine Saetze stehen in chat-strings.ts. */
    chat: {
        panelLabel: 'atlas chat',
        depthGroupLabel: 'neighbourhood depth',
        /*
         * `clear` bleibt der einzige Knopf, der loescht, und sagt es.
         *
         * Nutzerbefund vom 2026-08-29: das Panel liess sich nur ueber "clear"
         * loswerden, und damit war der Verlauf weg. Wer es nur wegklappen
         * wollte, hat ihn dafuer bezahlt. W7c hat daneben einen zweiten Knopf
         * gestellt, der nur zuklappt; seit W8 ist der Chat ein Reiter, und das
         * Zuklappen gehoert der Zone (messages.layout.collapse). Der
         * Unterschied, um den es dem Nutzer ging, ist damit nicht verschwunden,
         * sondern schaerfer geworden: die Zone klappt zu und behaelt alles,
         * dieser eine Knopf hier loescht, und er steht als einziger im Kopf des
         * Chats.
         */
        clear: 'clear',
        clearTooltip:
            'forget this session: every question and every answer in this panel is deleted. '
            + 'The history is in memory and nowhere else, so this cannot be undone. Folding the '
            + 'explain area away costs nothing; only this button deletes.',
        readPrefix: 'read: ',
        openCard: (path: string, line: number | undefined): string =>
            `open ${path}${line === undefined ? '' : `:${line}`}`,
        /** Die Ueberschrift ueber der Kandidatenliste. Ihr Satz steht in chat-strings.ts. */
        candidatesLabel: 'symbols this name reached',
    },

    /** Was die Statusleiste ueber das Graph-Panel sagt. */
    galaxy: {
        noLayout: 'no layout',
        nodeCount: (nodes: number, hidden: boolean): string =>
            `${nodes} nodes${hidden ? ' (hidden)' : ''}`,
    },

    /**
     * Das Einstellungen-Panel (W10), ganz.
     *
     * Wie die Hilfe steht sein Text hier und nicht in der Komponente: es ist die
     * zweite lange zusammenhaengende Flaeche dieses Produkts, und verteilt auf
     * JSX waere sie wieder der Ort, an dem niemand mehr sieht, was das Produkt
     * ueber sich behauptet. Drei Dinge sind ausdruecklich so formuliert und
     * nicht anders:
     *
     * 1. **Kein Satz verspricht eine Wirkung.** Was eine Einstellung bringt,
     *    misst das Panel auf DIESER Maschine und schreibt zwei Zahlen daneben.
     *    Die Saetze hier beschreiben, was eine Einstellung TUT (welche Ebene
     *    wegfaellt, welcher Durchgang entfaellt), nicht, was sie einbringt.
     * 2. **Der Ehrlichkeitssatz sagt drei Dinge auf einmal**: dass der Befehl
     *    ins Netz geht, wohin er laedt, und dass diese Anwendung selbst nichts
     *    herunterlaedt. Alle drei zusammen, weil jeder fuer sich die Frage offen
     *    laesst, die der naechste beantwortet.
     * 3. **Der Grund fuer "kein Router" ist eine Messung und keine Vermutung.**
     *    Ein Einzel-Server ignoriert ein fremdes `model`-Feld stillschweigend;
     *    das wurde am 2026-08-29 gemessen, und der Satz sagt es so.
     */
    settings: {
        title: 'CODEATLAS SETTINGS',
        subtitle: 'which model answers, and everything that costs computing time',
        panelLabel: 'settings',
        close: '[esc] close',
        closeLabel: CLOSE,
        /** Der Eintrag in der Atlas-Zeile. Der Buchstabe steht in Klammern. */
        menuLabel: '[s]ettings',
        /** Was die Kommandozeile am rechten Rand sagt, wenn `settings` dasteht. */
        commandHint: 'press enter to open the settings',

        /* ------------------------------------------------- das laufende Modell */

        modelTitle: 'The model that answers',
        modelIntro:
            'Every number in this block comes from the process that loaded the file. Point at one and '
            + 'it says which request it came from. A table of model sizes kept in this code would be '
            + 'right for a month and a claim about a file somebody swapped out after that.',
        rowName: 'name',
        rowQuantization: 'quantisation',
        rowContext: 'context',
        rowWeights: 'weights',
        sourceName: 'GET /props (model_path)',
        sourceQuantization: 'GET /props (model_ftype)',
        sourceContext: 'GET /props (n_ctx), GET /v1/models (meta.n_ctx_train)',
        sourceWeights: 'GET /v1/models (meta.size, meta.n_params)',
        sourceNameRouter: 'GET /props?model=<id> (model_path)',
        sourceQuantizationRouter: 'GET /props?model=<id> (model_ftype)',
        sourceContextRouter: 'GET /props?model=<id> (n_ctx), GET /v1/models (meta.n_ctx_train)',
        contextValue: (tokens: number, trained: number | undefined): string =>
            trained === undefined ? `${tokens} tokens` : `${tokens} of ${trained} trained tokens`,
        weightsValue: (size: string, billions: string | undefined): string =>
            billions === undefined ? size : `${size} for ${billions}B parameters`,
        /** Wenn der Prozess eine Angabe schuldig bleibt. Kein Platzhalter, ein Satz. */
        valueUnreported: 'the process did not report this',

        offTitle: 'The local model is off, and this panel asks it nothing',
        offText:
            'Not one request goes to the sidecar while the model is off, not even to find out whether '
            + 'one is running. That is the whole meaning of the switch: an opt-out and not a different '
            + 'colour. Turn it on in the atlas menu with [l]ocal llm, and this block fills with the '
            + 'models in your cache, which one is answering, and a click to switch between them.',
        offStillWorks:
            'Everything below about drawing works either way. It has nothing to do with a model.',
        notRunningTitle: 'The local model is on and nothing is listening',
        notRunningText:
            'This page has no backend and cannot start a process, so the sidecar is started from a '
            + 'shell. Without an argument it runs over your cache directory, and then the list below '
            + 'is a choice:',
        startingText: 'A sidecar is loading its model. The numbers appear as soon as it answers.',
        blockedText:
            'The committed policy of the indexed project denies the local model. This panel shows what '
            + 'it would show and asks nothing.',

        /* ------------------------------------------------- die Modelle im Cache */

        cacheTitle: 'The models in your cache',
        cacheIntro:
            'What the running process lists in its cache directory. It is read every time this panel '
            + 'opens, and again when you ask for it.',
        cacheEmpty: 'the process lists no model in its cache directory',
        cacheCount: (count: number): string =>
            count === 1 ? '1 model in the cache' : `${count} models in the cache`,
        modelAnswering: 'answering now',
        modelLoaded: 'loaded',
        modelUnloaded: 'in the cache, not loaded',
        selectTitle: (name: string): string =>
            `send the next questions to ${name}. It stays chosen after a reload.`,
        selectedTitle: (name: string): string => `${name} is the model the next question goes to`,
        refresh: 'read the list again',
        refreshTitle:
            'ask the process for its model list once more. This is the only request this panel sends '
            + 'on its own, and it counts in the same probe counter as the card in the sidebar.',

        noRouterTitle: 'This sidecar serves one fixed file, so there is nothing to switch',
        noRouterText:
            'It was started with -m and a path. A sidecar in that shape ignores a different "model" '
            + 'field in a request SILENTLY: measured on 2026-08-29 against this build, a request naming '
            + 'another model came back with a normal answer from the loaded one, no error and no '
            + 'warning. A list to click here would therefore do nothing and say nothing about it, '
            + 'which is worse than no list.',
        noRouterHow:
            'Start it over your cache directory instead. Then /v1/models lists what lies there, the '
            + '"model" field really chooses, and this block becomes a choice:',

        /* ------------------------------------------------- eins dazuholen */

        fetchTitle: 'Getting another model',
        honesty:
            'This line goes out to the network: it downloads from huggingface.co and writes the file '
            + 'into the model cache of this project, models/ next to the repository (ATLAS_MODELS_DIR '
            + 'or LLAMA_CACHE move it elsewhere). This application downloads nothing itself. It has no '
            + 'backend, it starts no process and it writes no file; what it can do is hand you the '
            + 'line and read the cache again afterwards.',
        noProgressBar:
            'There is no progress bar here because this window cannot see the download. A bar over a '
            + 'transfer that another process is running would be an animation, not a measurement.',
        suggestionsTitle: 'The six this project measured',
        suggestionsIntro:
            'Six files, 44 questions, temperature 0, seed 42, one machine, one day '
            + '(verification/w5/eval.json, the table in docs/adr/0001-modellwahl.md). That is what is '
            + 'known about these six and it is not a statement about any other model or any other '
            + 'machine. The field below takes any repository.',
        columnModel: 'model',
        columnClass: 'class',
        columnPass: 'questions passed',
        columnCitation: 'citations kept',
        columnSpeed: 'speed',
        columnSize: 'size',
        classValue: (label: string): string =>
            label === 'B' ? 'B, 8192 tokens of context' : 'A, 3072 tokens of context',
        passValue: (percent: string, questions: number): string => `${percent} of ${questions}`,
        citationValue: (percent: string): string => `${percent} of the answers`,
        unmeasuredValue: (count: number): string =>
            count === 1
                ? '1 answer could not be measured for citations'
                : `${count} answers could not be measured for citations`,
        /*
         * Der Fall, den es heute wirklich gibt: verification/w5/eval.json stammt
         * von vor der Aenderung an der Zitatpruefung (W10, AC8) und fuehrt das
         * Feld nicht. Eine Null waere hier die Behauptung, es habe keine
         * ungemessene Antwort gegeben, und genau das weiss niemand.
         */
        unmeasuredMissing: 'the recorded run does not report unmeasured answers',
        speedValue: (tokens: string): string => `${tokens} tokens/s`,
        repoTitle: (repo: string): string => `on Hugging Face: ${repo}`,
        copy: 'copy',
        copying: 'copying...',
        copyTitle: 'copy this line to the clipboard',
        copied: 'copied to the clipboard',
        copyFailed: 'the clipboard refused. The line stands above as text and can be selected.',

        freeTitle: 'Any other repository',
        freeIntro:
            'The shape is user/repo, with an optional :quant behind it. Only the shape is checked '
            + 'here. Whether the repository exists is something only huggingface.co knows, and this '
            + 'window does not talk to it.',
        freeLabel: 'a Hugging Face repository',
        freePlaceholder: 'user/repo or user/repo:quant',
        freeEmpty: 'type a repository, for example unsloth/Qwen3.5-9B-GGUF',
        freeShape:
            'that is not the shape of a repository id. It is user/repo, with an optional :quant '
            + 'behind it, and letters, digits, dots, dashes and underscores in the two names.',
        freeOk: 'the shape is right. Whether it exists is decided by huggingface.co, not by this field.',

        /* ------------------------------------------------- Darstellung und Leistung */

        displayTitle: 'Drawing, and what it costs',
        displayIntro:
            'Everything that costs computing time is in this block and only here. Each switch says '
            + 'what it does to the scene, and once you use it, what it did on THIS machine: frames per '
            + 'second before, frames per second after, on the scene that was on the screen. No switch '
            + 'here promises anything.',
        displayStored: (key: string): string => `kept in this browser under ${key}`,
        liveValue: (fps: string): string => `${fps} frames/s right now`,
        liveIdle:
            'the graph is not drawing right now, so there is nothing to measure. Open the graph panel '
            + 'with [a]tlas and the counter starts.',
        liveScene: (nodes: number, edges: number): string => `${nodes} nodes, ${edges} edges`,

        measureIdle: 'not measured yet',
        measureRunning: 'measuring, one window before and one after',
        measureNotDrawing:
            'nothing was drawn while this changed, so nothing was measured: the graph panel is folded '
            + 'away and its render loop stands still.',
        measureNoDifference: (before: string, after: string, band: string): string =>
            `before ${before} frames/s, after ${after} frames/s. That is inside the spread this `
            + `machine shows without any change at all (${band} frames/s), so there is no measurable `
            + 'difference here.',
        measureHigher: (before: string, after: string, band: string): string =>
            `before ${before} frames/s, after ${after} frames/s, past the ${band} frames/s this `
            + 'machine wobbles by on its own.',
        measureLower: (before: string, after: string, band: string): string =>
            `before ${before} frames/s, after ${after} frames/s, past the ${band} frames/s this `
            + 'machine wobbles by on its own.',
        measureScene: (nodes: number, edges: number, at: string): string =>
            `measured on ${nodes} nodes and ${edges} edges at ${at}`,

        settingProjection: 'view',
        settingProjectionDetail:
            'the flat view is an orthographic camera looking down the z axis. It drops the third axis '
            + 'instead of foreshortening it, and the controls lose their rotation; panning and zooming '
            + 'stay.',
        valueSpatial: '3D',
        valueFlat: '2D',
        settingHalos: 'landmark halos',
        settingHalosDetail:
            'the glow around the biggest hubs. It is an extra sprite layer drawn over the cloud.',
        settingBloom: 'bloom',
        settingBloomDetail:
            'the post-processing pass that makes bright points glow. It renders the scene a second '
            + 'time into a smaller buffer and blurs it.',
        settingEdges: 'edges',
        settingEdgesDetail:
            'none does not draw the edge layer at all. Dimming it to nothing would cost the same and '
            + 'only look cheaper.',
        valueEdgesFull: 'all',
        valueEdgesDim: 'dim',
        valueEdgesOff: 'none',
        settingLabels: 'labels',
        settingLabelsDetail:
            'names beyond this distance from the camera are not drawn. Each name carries its own '
            + 'texture and its own draw call. Until now their visibility depended on node size alone '
            + 'and the camera did not come into it.',
        valueLabelsAll: 'all of them',
        valueLabelsFar: 'within twice the scene radius',
        valueLabelsNear: 'within the scene radius',
        settingFrameCap: 'frame ceiling',
        settingFrameCapDetail:
            'with a ceiling the render loop is driven by a clock instead of by the screen, so a '
            + 'machine that cannot reach 60 stops trying. Without one it runs exactly as it did '
            + 'before this panel existed.',
        valueCapOff: 'none',
        valueCap: (fps: number): string => `${fps} per second`,
        settingAgents: 'live agent layer',
        settingAgentsDetail:
            'the glowing bodies that orbit the symbol an agent is working on, and their search pings '
            + 'and test lines. Each actor is one element that is placed again in every frame. Off '
            + 'draws none of it; the instrument in the corner stays and says so.',
        settingAgentTails: 'agent comet tails',
        settingAgentTailsDetail:
            'the short trail right behind a body while it flies from one symbol to the next. One '
            + 'line of twelve points per actor, refilled in every frame of the flight. Off leaves '
            + 'the flight itself untouched.',
        settingAgentTrails: 'agent trails',
        settingAgentTrailsDetail:
            'the dashed line through the last symbols an actor touched, drawn under the real edges. '
            + 'Up to ten nodes per actor and 120 segments in total; a dashed line needs its own '
            + 'length computation per point. It is the reading that shows whether someone follows a '
            + 'call path or jumps across the repository, so it is the last one to switch off.',
        settingAgentWaves: 'agent write waves',
        settingAgentWavesDetail:
            'one concentric wave per burst of writes on the same symbol. One per burst, not one per '
            + 'event: a wave per event would be a firework and say nothing.',
        settingAgentTimeline: 'agent timeline',
        settingAgentTimelineDetail:
            'the strip under the graph: one track per actor, one tick per event, redrawn on every '
            + 'tick of the instrument clock. It only appears when the graph area is at least 600 px '
            + 'wide; narrower than that, one tick cannot be told from the next.',
        switchOn: 'on',
        switchOff: 'off',

        profileTitle: 'Both at once',
        profileThrifty: 'thrifty profile',
        profileThriftyDetail:
            'sets the flat view, no halos, no bloom, dim edges, labels within the scene radius, a '
            + 'ceiling of 30 and no agent tails, trails, waves or timeline, in one step. Fullscreen '
            + 'keeps working after it, only quieter.',
        profileDefault: 'back to the default',
        profileDefaultDetail: 'puts every switch in this block back to what it starts with.',
        profileIsDefault: 'every switch is on its default',
        profileChanged: 'this differs from the default',

        keepsTitle: 'What stays where it is',
        keepsText:
            'Switches that only change what you see and cost nothing to draw stay in their own '
            + 'panels: the detail level of the twin, the legend, the galaxy and hierarchy switch, and '
            + 'the edge-kind filters inside the legend. The point was to bundle the expensive things, '
            + 'not to build a menu that everything disappears into.',
    },

    /** Der Rahmen des Twins. Seine Fakten-Saetze stehen in twin/strings.ts. */
    twin: {
        /** Das Etikett des Leseprofils, mit dem dieses Produkt startet. */
        profileLabel: 'CodeAtlasWeb Twin',
        /** Der Kopf des Panels, solange kein Symbol vor dem Leser steht. */
        noSymbol: 'no symbol',
    },
} as const;

export type Messages = typeof messages;
