/**
 * Die Verdrahtung: Projekt aus der Adresszeile, Baum aus dem Graphen, Datei in
 * den Reader.
 *
 * Hier steht der Zustand und sonst nichts Darstellendes. Das Chrome
 * (src/app/AtlasChrome.tsx) bekommt fertige Werte, der Reader
 * (src/reader/MonacoReader.tsx) bekommt ein fertiges Dokument. Diese Datei ist
 * die einzige, die weiss, dass es einen Server gibt.
 *
 * Drei Entscheidungen, die man sonst raten muesste:
 *
 * 1. **Das Projekt kommt aus `?project=`.** Fehlt der Parameter, wird das erste
 *    Projekt aus `list_projects` genommen. Gibt es keins, sagt die Oberflaeche
 *    "no project" und zeigt keinen Baum, statt einen leeren zu zeigen: ein
 *    leerer Baum waere die Behauptung, das Projekt sei leer.
 * 2. **Der Baum wird beim Laden aufgeklappt.** `/api/tree` antwortet je Anfrage
 *    mit einer Ebene, und ein Explorer, der mit zwei zugeklappten Ordnern
 *    startet, zeigt von einem indizierten Projekt so gut wie nichts. Es wird
 *    breitenweise nachgeladen, bis ein Deckel erreicht ist; was danach noch
 *    offen ist, bleibt zugeklappt und laedt beim Klick. Der Deckel wird
 *    genannt, wenn er greift.
 * 3. **Eine Datei kommt ueber ihren Modul-Knoten.** Warum es keinen anderen Weg
 *    gibt und was das kostet, steht in src/reader/file-source.ts.
 *
 * Dazu kommt seit W2b die Caret-Strecke des Twins, und die hat drei eigene
 * Entscheidungen:
 *
 * 4. **Aufloesen wird entprellt, Hervorheben nicht.** Ein Caret, der durch eine
 *    Funktion wandert, soll das Badge sofort mitnehmen, aber nicht bei jedem
 *    Tastendruck den Graphen fragen. Also wandert `caretLine` sofort und
 *    `settledCaret` nach 250 ms, und nur das zweite loest auf.
 * 5. **Dasselbe Symbol wird nicht zweimal geholt.** Der Cache in
 *    src/twin/ir-cache.ts schluesselt ueber den qualifizierten Namen und legt
 *    gleichzeitige Anfragen zusammen; hier kommt nur die Regel dazu, bei
 *    unveraendertem Subjekt gar nicht erst in den Ladezustand zu gehen, damit
 *    das Panel beim Wandern des Carets nicht blinkt.
 * 6. **Folgen wechselt das Subjekt sofort und laesst den Editor nachkommen.**
 *    Der Kopf des Panels nennt ab dem Klick das neue Symbol und der Koerper
 *    sagt, dass gelesen wird. Die Fakten kommen erst, wenn der Caret in der
 *    Zieldatei aufgeloest ist: eine IR, die aus dem Klickziel statt aus einer
 *    Aufloesung gebaut waere, traege eine erfundene Symbolart, und "This is a
 *    piece of code" ueber einer Funktion ist genau die Sorte kleiner Luege,
 *    gegen die dieses Panel gebaut ist.
 *
 * Seit W3 kommen die Galaxie und die Bedeutungssuche dazu, mit drei weiteren
 * Entscheidungen:
 *
 * 7. **Der Fokus laeuft in beide Richtungen ueber genau einen Schluessel.**
 *    Das Twin-Subjekt reist als qualifizierter Name zur Galaxie, die Galaxie
 *    schickt einen angeklickten Knoten als Ziel zurueck durch denselben
 *    `followTarget`, den auch der Twin benutzt. Es gibt also keinen zweiten
 *    Navigationsweg, den man vergessen koennte zu pflegen, und keine zweite
 *    Stelle, an der ein Symbol identifiziert wird.
 * 8. **Ein Klick in die Galaxie behauptet nichts ueber das Symbol.** Er
 *    oeffnet die Datei an der gemeldeten Zeile; was dort steht, sagt die
 *    Aufloesung, so wie beim Folgen aus dem Twin. Der Grund ist derselbe wie
 *    in Entscheidung 6.
 * 9. **Getippt wird ohne Anfrage, gefragt wird nach 200 ms Ruhe.** Die
 *    Kommandozeile ist eine Suche und kein Terminal: sie schickt keine Zeile
 *    ab, sie zeigt waehrend des Tippens, was der Index kennt. Ohne die
 *    Entprellung waere jede Taste ein Sweep ueber den Graphen.
 *
 * Seit W4a kommen die Einstiegsmodi dazu, und mit ihnen vier weitere:
 *
 * 10. **Die Frage nach dem Warum ist eine Flaeche, kein Dialog.** Sie fuellt
 *     den leeren Editorbereich, wenn keine Datei offen ist und dieser Browser
 *     fuer dieses Projekt noch keine Antwort kennt. Ein Modal beim ersten
 *     Oeffnen wuerde die Oberflaeche und jeden automatisierten Lauf durch sie
 *     blockieren; das ist die Begruendung des Referenzprojekts und sie gilt
 *     hier unveraendert.
 * 11. **Eine Antwort setzt genau eine Sache und wird dann vergessen.** Sie
 *     setzt das Leseprofil des Twins (Tiefe und Linsen, Zuordnung in
 *     why/why-model.ts) und wird gespeichert, damit die Frage nicht wieder
 *     kommt. Niemand fragt den Intent spaeter zurueck: ein weitergereichter
 *     Intent waere ein Modus, und ein Modus ist ein zweites Produkt im ersten.
 * 12. **Ein Schritt bewegt den Reader ueber dieselbe Strecke wie ein Klick.**
 *     Ein Symbolschritt geht durch `followTarget`, ein Dateischritt durch
 *     `openFile`. Es gibt keinen zweiten Navigationsweg fuer Fuehrungen, also
 *     auch keine zweite Stelle, an der ein Symbol identifiziert wird, und der
 *     Twin folgt einem Schritt aus demselben Grund, aus dem er einem Klick
 *     folgt.
 * 13. **Die stille Anzeige zaehlt Wege, nie Zeit.** Beim Betreten eines
 *     Schrittes und beim Folgen einer Zeile wird ein Checklisten-Item als
 *     besucht vermerkt. Verweildauer ist keine Evidenz, und die Statusleiste
 *     zeigt gar keinen Zaehler, solange es keine Checkliste zu zaehlen gibt,
 *     statt "0 of 0" zu behaupten.
 *
 * Seit W4b sind die beiden letzten Karten der Frage scharf, und mit ihnen
 * kommen drei weitere Entscheidungen:
 *
 * 14. **Der BUG-Assistent uebernimmt das Subjekt des Twins und schiebt
 *     nichts zurueck.** Er liest neu, wenn ein anderes Symbol vor dem Leser
 *     steht, und veroeffentlicht selbst nichts: wer zwei Pfade vergleicht, hat
 *     nicht darum gebeten, dass Reader, Twin und Galaxie unter ihm wegwandern.
 *     Nur ein Klick auf einen Hop navigiert, und der loest den Namen in diesem
 *     Moment auf, statt einen gespeicherten Verweis zu benutzen.
 * 15. **Ein kaputter Ref wird hier abgelehnt und nicht dort.** `detect_changes`
 *     nimmt ein unbekanntes `since` kommentarlos an und antwortet ueber den
 *     Arbeitsbaum. Ein Tippfehler ergaebe also eine plausible Antwort auf eine
 *     andere Frage, und deshalb prueft `refRejection` die Form im Browser und
 *     der Aufruf unterbleibt. Der Beweislauf zaehlt die Anfragen am Proxy und
 *     liest diese Zahl als die Behauptung.
 * 16. **Die beiden Flaechen schliessen einander aus.** Beide brauchen den
 *     ganzen Platz ueber dem Editor, und zwei ganze Antworten uebereinander
 *     waeren keine. Escape schliesst, was offen ist; das [a]tlas-Menue macht
 *     beim Oeffnen der einen die andere zu.
 *
 * Seit W4e reicht diese Datei eine Antwort weiter, die sie bisher wegwarf:
 *
 * 17. **Der Walk bleibt liegen, solange seine Fuehrung laeuft.** `getClosure`
 *     liefert einen Graphen; daraus eine Schrittliste zu machen und den Graphen
 *     zu vergessen hiess, die Tiefe wegzuwerfen, die in ihm steht. Also haelt
 *     `walk` ihn, und das Graph-Panel zeigt ihn als Hierarchie ab dem
 *     gewaehlten Einstiegspunkt. Er endet, wenn die Fuehrung endet: ein Bild
 *     einer beendeten Frage waere eine Antwort, nach der niemand mehr fragt.
 */
import type { JSX, KeyboardEvent } from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';

import AtlasChrome, { COMMAND_PLACEHOLDER } from './app/AtlasChrome';
import type { Chip, MenuExtra, MenuWiring, TabDescriptor } from './app/AtlasChrome';
import { AtlasApi, TREE_ROUTE } from './app/atlas-api';
import { ATLAS_BUILD_SUFFIX, ATLAS_VERSION } from './app/build-info';
import { messages } from './i18n/messages';
import {
    commandLineIntent,
    KEY_LISTENER_OPTIONS,
    menuShortcutFor,
    tourKeyForEvent,
} from './app/keyboard';
import { markHandled } from './app/key-probe';
import { WIRED_MENU_SHORTCUTS } from './app/shortcuts';
import { baseName, pathSegments } from './app/module-qn';
import HelpOverlay from './help/HelpOverlay';
import {
    directoryPaths,
    EMPTY_COVERAGE,
    flattenTree,
    mergeCoverageIntoLevels,
    moveCursor,
    parentPath,
    treeIntent,
} from './app/tree-model';
import type { CoverageIndex, CoverageState, TreeLevel, TreeRow } from './app/tree-model';
import { loadCoverage, loadPathCoverage } from './app/coverage-source';
import { keepsReadyTwinForTarget } from './app/ready-twin-target';
import type { CoveragePathAnswer } from './app/tree-model';
import {
    freshnessNote,
    freshnessNoteNeeded,
    partialFileNote,
    readerUnavailableNote,
} from './app/coverage-strings';
import { RpcIntelligenceClient } from './provider/rpc-client';
import { CbmRpcProvider, symbolKindOf } from './provider/cbm-rpc-provider';
import MonacoReader from './reader/MonacoReader';
import type { ReaderStatus } from './reader/MonacoReader';
import { FileNotReadableError, loadFileDocument, READER_RPC_TOOL } from './reader/file-source';
import type { ReaderDocument } from './reader/file-source';
import { badgesForLines } from './core/step-badge-decorator';
import type { SymbolRef } from './core/focus-protocol';
import type { SemanticIR } from './core/semantic-ir';
import { buildIr } from './ir/semantic-ir-builder';
import TwinPanel from './twin/TwinPanel';
import type { TwinBodyView, TwinStatus } from './twin/TwinPanel';
import { IrCache } from './twin/ir-cache';
import { ATLAS_WORKSPACE_ROOT, twinLocationOf, twinTargetOf, workspacePathOf } from './twin/twin-target';
import GalaxyPanel from './galaxy/GalaxyPanel';
import { targetRefOfNode } from './galaxy/galaxy-model';
import type { GraphData, GraphNode } from './galaxy/types';
import SearchOverlay from './search/SearchOverlay';
import type { SearchOverlayStatus } from './search/SearchOverlay';
import {
    SEARCH_DEBOUNCE_MS,
    SEARCH_MIN_QUERY,
    findByMeaning,
    searchByMeaning,
} from './search/find-by-meaning';
import type { RankedHit } from './search/semantic-search';
import { rankHits } from './search/semantic-search';
import {
    commandExamplesFor,
    commandPlaceholderFor,
    exampleSymbolOf,
} from './search/command-examples';
import { EMPTY_LOCAL_INDEX, localCandidates } from './search/local-suggestions';
import type { LocalIndex } from './search/local-suggestions';
import {
    isSearchable,
    MAX_SEARCH_ROWS,
    moveSelection,
    overlayIntent,
    searchHeadline,
    searchRows,
} from './search/overlay-model';
import type { SearchRow, SearchRowSource } from './search/overlay-model';
import { Facet, resolvePresentation } from './twin/presentation-profile';
import type { PresentationOverrides, PresentationProfile } from './twin/presentation-profile';
import type { TwinRow } from './twin/twin-view-model';
import WhyPanel from './why/WhyPanel';
import { WHY_MENU_LABEL, profileFor } from './why/why-model';
import type { WhyIntent } from './why/why-model';
import { readWhyAnswer, recordWhyAnswer } from './why/why-store';
import type { WhyAnswer } from './why/why-store';
import TourCard from './tours/TourCard';
import { generateProjectTour } from './tours/tour-source';
import { entryWalkTour } from './tours/entry-walk';
import type { ActiveTour } from './tours/entry-walk';
import { isLastStep, playerIntent, stepMove } from './tours/tour-player';
import { markableItemId } from './tours/tour-model';
import { getClosure } from './provider/closure';
import type { ClosureResult } from './provider/closure';
import FlowOverlay from './pseudocode/FlowOverlay';
import { buildFlowView, FLOW_CLOSURE_CAP, FLOW_CLOSURE_DEPTH } from './pseudocode/flow-view';
import type { FlowView } from './pseudocode/flow-view';
import { buildImportsGroup } from './pseudocode/imports-group';
import type { ImportsGroup } from './pseudocode/imports-group';
import { fileImportsFor } from './pseudocode/imports-source';
import { buildPseudocode, closureDocumentOf } from './pseudocode/pseudocode-builder';
import type { PseudocodeDocument, PseudocodeSourceRef } from './pseudocode/pseudocode-builder';
import { FLOW_LOADING, FLOW_UNAVAILABLE } from './pseudocode/pseudocode-strings';
import EntryPointDialog from './entry/EntryPointDialog';
import type { EntrySearchStatus } from './entry/EntryPointDialog';
import { entryHeadline, entryRows, routeNote } from './entry/entry-model';
import type { ArchitectureOverviewDto, SymbolSearchHit } from './core/intelligence-provider';
import {
    browserStore,
    exploredLabel,
    markVisited,
    readUnderstanding,
    totalMarks,
    understandingOf,
} from './checklist/understanding-store';
import type { UnderstandingRecord } from './checklist/understanding-store';
import {
    TWIN_EMPTY_HINT,
    TWIN_EMPTY_MESSAGE,
    TWIN_FILE_NOT_INDEXED,
    TWIN_FILE_NOT_INDEXED_HINT,
    TWIN_LOADING,
    TWIN_LOAD_FAILED,
} from './twin/strings';
import BugWizard from './traces/BugWizard';
import type { BugWizardStatus } from './traces/BugWizard';
import { bugPaths, resolveHop } from './traces/bug-paths';
import type { BugPathNode, BugPathsDto } from './traces/bug-paths';
import { BUG_WIZARD_MENU_LABEL } from './traces/bug-wizard-strings';
import ImpactPanel from './impact/ImpactPanel';
import type { ImpactMode, ImpactStatus } from './impact/ImpactPanel';
import { readImpact } from './impact/impact-source';
import { refRejection } from './impact/impact-model';
import type { ImpactModel, ImpactTarget } from './impact/impact-model';
import { IMPACT_MENU_LABEL, impactRefRejected } from './impact/impact-strings';
import SidecarPanel from './llm/SidecarPanel';
import { probeSidecar, SIDECAR_ORIGIN, SIDECAR_POLL_MS } from './llm/sidecar';
import type { CacheModel, SidecarReading, SidecarState } from './llm/sidecar';
import AtlasChatPanel from './chat/AtlasChatPanel';
import { askAtlas } from './chat/ask-atlas';
import type { ChatTurn } from './chat/ask-atlas';
import { askModel } from './chat/chat-client';
import { commandIntent } from './chat/command-intent';
import {
    CHAT_HINT_OFF,
    CHAT_HINT_READY,
    REFINE_APPLIED,
    REFINE_RUNNING,
    refineRejected,
} from './chat/chat-strings';
import ExplainZone from './layout/ExplainZone';
import Splitter from './layout/Splitter';
import { explainTabs } from './layout/explain-tabs';
import type { ExplainTabId, ExplainTabState } from './layout/explain-tabs';
import { lineCommandOf } from './layout/layout-command';
import {
    LAYOUT_KEYS,
    clampLayout,
    clampLayoutValue,
    defaultLayout,
    layoutBounds,
    layoutStorageKey,
    readLayout,
    sameLayout,
    writeLayout,
} from './layout/layout-model';
import type { LayoutFrame, LayoutKey, LayoutSizes } from './layout/layout-model';
import { NEIGHBOR_DEPTHS, NEIGHBOR_DEPTH_DEFAULT } from './compiler/fact-recipes';
import type { NeighborDepth, ObservedFact, SubjectCandidate } from './compiler/fact-recipes';
import { modelClassOf } from './compiler/card-compiler';
import type { CardSource } from './compiler/card-compiler';
import { buildRefinePrompt, nonThinkingFor, REFINE_SYSTEM_PROMPT } from './compiler/prompt-contract';
import { applyRefinement, refineMaxTokens, refineSubjectText } from './pseudocode/refine';
import { readLlmPolicy } from './llm/policy';
import type { PolicyReading } from './llm/policy';
import { resolveLlmState } from './llm/llm-state';
import { readLlmPreference, recordLlmPreference } from './llm/preference';
import { llmChipValue, llmMenuLabel, llmMenuTitle } from './llm/strings';
import SettingsPanel from './settings/SettingsPanel';
import type { SettingsMeasurement } from './settings/SettingsPanel';
import ProjectsPanel from './projects/ProjectsPanel';
import type { ProjectsSource } from './projects/ProjectsPanel';
import { projectHref } from './projects/projects-model';
import { MODEL_SUGGESTIONS, fetchCommand } from './settings/model-catalog';
import { modelKey, readModelPreference, recordModelPreference } from './settings/model-preference';
import {
    DEFAULT_GRAPH_DISPLAY,
    displayKey,
    isDefaultDisplay,
    loadDisplaySettings,
    saveDisplaySettings,
} from './galaxy/density';
import type { GraphDisplaySettings } from './galaxy/density';
import { bridgePortFromSearch, useAgentStream } from './agents/agent-source';
import { agentsMenuLabel } from './agents/agent-strings';

/**
 * Wie viele Ebenen beim Start von selbst nachgeladen werden.
 *
 * Der Deckel ist da, weil jede Ebene eine eigene Anfrage ist: ein Repository
 * mit tausend Verzeichnissen waere sonst tausend Anfragen beim ersten Bild.
 * Vierzig Ebenen decken die Fixtures und jedes normale Projekt ab; was darueber
 * liegt, klappt der Benutzer selbst auf und die Oberflaeche sagt es ihm.
 */
export const EAGER_LEVEL_BUDGET = 40;

/**
 * Wie lange der Caret stehen muss, bevor gefragt wird.
 *
 * 250 ms ist die Spanne, in der ein Leser mit den Pfeiltasten durch einen
 * Rumpf geht, ohne stehenzubleiben. Kuerzer, und jede Zeile waere eine Frage an
 * den Graphen; laenger, und das Panel haengt spuerbar hinter dem Caret.
 */
export const TWIN_CARET_DEBOUNCE_MS = 250;

/**
 * Womit das Panel startet.
 *
 * Kein eingebautes Profil, und das ist eine Aussage: die fuenf Presets des
 * Referenzprojekts hoeren zu einer Werkbank mit fuenf Panels, und jedes von
 * ihnen schaltet Linsen ab, die dort ein anderes Panel beantwortet. Hier gibt
 * es nur den Twin, also waere jedes der fuenf eine Startlage, in der eine
 * Sektion fehlt und niemand sagt, warum. Die Tiefe zwei zeigt die
 * aufgezeichneten Fakten, so wie design/design.png es zeigt; die Linsen sind
 * alle, die dieses Backend beantworten kann. Runtime und Changes sind aus, weil
 * sie hier nur ihre eigene Unfertigkeit melden koennten, und der Leser kann sie
 * mit einem Klick einschalten und genau das lesen.
 */
export const TWIN_PROFILE: PresentationProfile = {
    id: 'custom:codeatlasweb-twin',
    label: messages.twin.profileLabel,
    depth: 2,
    facets: [Facet.Logic, Facet.Calls, Facet.Data, Facet.Errors, Facet.Tests],
    terminology: 'technical',
    conceptCallouts: false,
    panels: [],
    aiChangeWatcher: false,
};

/**
 * Die Adressparameter, mit denen die Grenzen des Vorwaerts-Walks von aussen
 * gesetzt werden koennen.
 *
 * Dieselben zwei Namen wie im Referenzprojekt (pseudocode-service.ts), und sie
 * sind kein Debug-Schalter: die Karte sagt am Ende eines gekappten Walks, dass
 * sie gekappt wurde, und dieser Satz muss an einem echten Lauf zu sehen sein,
 * nicht nur in einem Unit-Test. An einem Projekt mit zehn Dateien kappt der
 * Vorgabe-Deckel von fuenfzehn Symbolen nichts; mit `?codeatlasClosureCap=3`
 * kappt er, und der Beweislauf liest den Satz dort, wo ein Leser ihn saehe.
 */
export const CLOSURE_DEPTH_PARAM = 'codeatlasClosureDepth';
export const CLOSURE_CAP_PARAM = 'codeatlasClosureCap';

/*
 * Die sichtbaren Saetze dieser Datei stehen seit W6a im Katalog
 * (src/i18n/messages.ts) und werden hier nur noch unter ihren bisherigen Namen
 * weitergereicht. Die Namen bleiben, weil sie exportiert sind und anderswo
 * gelesen werden; der Wortlaut hat jetzt genau eine Quelle.
 */

/** Was die Statusleiste sagt, wenn eine Fuehrung nicht zustande kam. */
export const TOUR_FAILED = messages.tour.failed;

/** Was in der Kommandozeile steht, wenn nichts gesucht wird. */
export const COMMAND_HINT_IDLE = messages.command.hintIdle;
/** Was dort steht, waehrend das Fenster offen ist. */
export const COMMAND_HINT_OPEN = messages.command.hintOpen;
/** Was das Trefferfenster sagt, solange erst ein Zeichen getippt ist. */
export const COMMAND_ONE_MORE_LETTER = messages.search.oneMoreLetter;
/** Was das Fenster sagt, wenn die Suche selbst gescheitert ist. */
export const SEARCH_FAILED = messages.search.failed;
/** Was es sagt, wenn ein gewaehlter Treffer keine Datei traegt. */
export const SEARCH_NO_FILE = messages.search.noFile;

const emptyLevels = new Map<string, TreeLevel>();
const noBadges: ReturnType<typeof badgesForLines> = [];
const noHits: RankedHit[] = [];
/** Das Trefferfenster ohne Treffer, als stabile Referenz. */
const noRows: SearchRow[] = [];

/**
 * Die beiden Griffe, an denen der Beweislauf die Fuehrung und die stille
 * Anzeige anfasst.
 *
 * Absichtlich schmal, wie die Griffe des Readers, des Twins und der Galaxie:
 * alles davon steht auch im DOM, der Griff spart dem Lauf das Parsen und macht
 * zwei Dinge ueberhaupt messbar, naemlich "dasselbe Dokument zweimal" und "die
 * Zahl der Vermerke ist gestiegen".
 */
export interface AtlasTourSeam {
    id: string;
    kind: 'project' | 'entry';
    title: string;
    steps: number;
    index: number;
    paths: string[];
    titles: string[];
    endNote: string;
    /** Das Dokument, wie es gerade gelaufen wird. */
    json: string;
    /** Noch einmal erzeugen und serialisieren. Nur fuer die Projekt-Fuehrung. */
    regenerate?: () => Promise<string>;
}

/**
 * Der Griff, an dem der Beweislauf den Coverage-Join anfasst.
 *
 * Er traegt genau das, was auch im DOM steht, und spart dem Lauf das Parsen.
 * Die Zeilen kommen mit ihrer Stufe, weil "der Baum zeigt exakt das Gemessene"
 * sonst nur ueber Screenshot-Vergleiche pruefbar waere, und ein Bild ist keine
 * Behauptung ueber Pfade.
 */
export interface AtlasCoverageSeam {
    rows: { path: string; kind: string; coverage: string; reason: string; sources: string[] }[];
    /** Je Pfad genau ein Befund, so wie der Join ihn haelt. */
    records: { path: string; kind: string; state: string; reason: string; sources: string[] }[];
    truncations: string[];
    counts: CoverageIndex['counts'];
    /** Die Metadaten des Coverage-Stores, inklusive der ignored_files-Zahlen. */
    metadata: Record<string, unknown>;
    /** Was ueber die offene Datei gerade bekannt ist. */
    open: { path: string; status: string; freshness: string; action: string; note: string } | undefined;
    /** Leer, solange nichts schiefging. */
    error: string;
}

export interface AtlasChecklistSeam {
    /** Das Symbol, ueber das die Statusleiste gerade zaehlt. Leer, wenn keines. */
    symbol: string;
    explored: number;
    total: number;
    /** Genau das, was in der Statusleiste steht. Leer heisst: kein Zaehler. */
    label: string;
    /** Vermerke dieses Projekts insgesamt. Die eine Zahl, die nur steigt. */
    marks: number;
}

/**
 * Der Griff, an dem der Beweislauf das lokale Modell anfasst.
 *
 * Er traegt zwei Dinge, die im DOM nicht stehen und ohne die der Lauf raten
 * muesste: das Urteil der Policy samt Begruendung, und die Zahl der Proben, die
 * ueberhaupt losgeschickt wurden. Die zweite ist der Gegenbeweis zur ersten
 * Behauptung dieses Zyklus: solange das LLM aus ist, bleibt sie null, und der
 * Netz-Mitschnitt sagt dasselbe aus der anderen Richtung.
 */
/**
 * Der Anfangswert der Probe: nichts gefragt, nichts gefunden.
 *
 * Als Konstante und nicht als Literal an drei Stellen: er wird beim ersten Bild
 * gesetzt, beim Projektwechsel und beim Ausschalten, und drei Fassungen davon
 * waeren drei Gelegenheiten, eine zu vergessen.
 */
const EMPTY_SIDECAR_READING: SidecarReading = {
    state: 'not-running',
    router: false,
    models: [],
    detail: '',
};

/** Eine leere Liste mit fester Identitaet, damit sie keine Effekte anstoesst. */
const NO_CACHE_MODELS: readonly CacheModel[] = [];

export interface AtlasLlmSeam {
    state: SidecarState;
    /** Ob dieser Browser fuer dieses Projekt eingeschaltet hat. */
    preferenceOn: boolean;
    /** Das Urteil der committeten Policy. Leer, solange noch nicht gelesen. */
    policyVerdict: string;
    policyPath: string;
    policyDetail: string;
    /** Der Modellname, so wie er im Panel steht. Leer, wenn keiner bekannt ist. */
    model: string;
    /** Was in der Statusleiste steht. */
    chip: string;
    /** Wie oft ueberhaupt zum Sidecar gefragt wurde, seit die Seite geladen ist. */
    probes: number;
}

/**
 * Der Griff, an dem der Beweislauf den Chat und die Umformulierung anfasst.
 *
 * Zwei Dinge stehen hier, die im DOM nicht stehen und ohne die ein Lauf raten
 * muesste: die Kartennummern, die eine Antwort wirklich zitiert hat (im Text
 * stehen sie als Knoepfe, aber nicht die Liste der gegebenen Karten daneben),
 * und `validateRefine`. Das zweite ist kein Nachbau: es ruft denselben
 * Validator, den der Knopf ruft, mit einer absichtlich kaputten Antwort. Ein
 * Lauf, der die Regel nachbaut, prueft seine Kopie und nicht das Produkt.
 */
export interface AtlasChatSeam {
    turns: {
        id: number;
        question: string;
        status: string;
        klass: string;
        rule: string;
        depth: number;
        cards: number;
        cardIds: string[];
        sources: number;
        citations: string[];
        unknownCitations: string[];
        uncitedLines: number;
        answer: string;
        refusal: string;
        tokens: number;
        budget: number;
        tokensPerSecond: number;
        /** Die Kandidaten, wenn dieser Zug auf eine Wahl wartet. */
        candidates: { name: string; qualifiedName: string; filePath: string; line: number }[];
        /** Der Name, der mehrdeutig war oder nicht gefunden wurde. Leer, wenn keiner. */
        askedName: string;
        /** Das Fokus-Symbol, das statt eines nicht gefundenen Namens geantwortet hat. */
        focusFallbackUsed: string;
    }[];
    /**
     * Ob der Chat gerade sichtbar ist.
     *
     * Seit W8 heisst das: der Erklaeren-Bereich ist offen UND sein Reiter ist
     * `chat`. Eingeklappt oder auf einem anderen Reiter bleiben die Zuege
     * trotzdem stehen, und genau das ist die Zusicherung, die W7c erkaempft hat
     * und die hier weiter gemessen wird.
     */
    open: boolean;
    /** Die Hoehe, die der Chat gerade hat. Seit W8 ist es die Hoehe seiner Zone. */
    height: number;
    /** Der Deckel, den dieses Fenster gerade zulaesst. Ebenfalls der der Zone. */
    maxHeight: number;
    depth: number;
    depthDefault: number;
    depthOptions: number[];
    refineAvailable: boolean;
    refineState: string;
    refineMessage: string;
    validateRefine: (answer: string) => { applied: boolean; reason: string };
}

/**
 * Der Griff, an dem der Beweislauf die Suche und die Menuezeile anfasst.
 *
 * Er traegt genau das, was im DOM nicht steht und was ein Lauf sonst aus
 * Wartezeiten raten muesste: wie lange der Serverweg gedauert hat, wie oft ein
 * Verlaengern des Wortes aus dem Praefix-Cache beantwortet wurde, wie oft eine
 * ueberholte Runde abgebrochen wurde, und ob je eine ueberholte Antwort die
 * neuere ueberschrieben hat. Die letzte Zahl ist die einzige, die falsch sein
 * darf: sie ist der Gegenbeweis zur Zusicherung dieses Zyklus und muss null
 * bleiben.
 *
 * `activatedMenus` ist die Liste der Menuepunkte, die wirklich GELAUFEN sind,
 * je Buchstabe. Sie wird an der Stelle geschrieben, an der die Handlung
 * ausgefuehrt wird, und nicht dort, wo geklickt wurde: ein Lauf, der einen
 * Klick zaehlt, hat den Klick bewiesen und nicht die Handlung dahinter.
 */
export interface AtlasSearchSeam {
    /** Die Entprellung, wie sie wirklich gilt. */
    debounceMs: number;
    minQuery: number;
    /** Was gerade in der Zeile steht. */
    currentQuery: string;
    /** Die Anfrage, die die gezeigten Zeilen beantworten. */
    shownQuery: string;
    /** Woher die gezeigten Zeilen kommen. */
    shownSource: SearchRowSource;
    shownRows: number;
    /** Wie oft vorlaeufige Zeilen VOR einer Antwort des Index dastanden. */
    localFirst: number;
    /** Wie oft ein verlaengertes Wort aus dem Praefix-Cache beantwortet wurde. */
    prefixCacheHits: number;
    /** Runden, die wirklich zum Server gingen. */
    serverRequests: number;
    /** Der letzte gemessene Serverweg, in Millisekunden. */
    serverRoundtripMs: number;
    /** Jeder gemessene Serverweg dieser Sitzung. */
    roundtrips: number[];
    /** Runden, die abgebrochen wurden, weil ein Tastendruck sie ueberholt hat. */
    abortedRequests: number;
    /** Antworten, die zu spaet kamen und deshalb verworfen wurden. */
    staleDropped: number;
    /** Muss false bleiben: eine ueberholte Antwort hat die neuere ueberschrieben. */
    staleAnswerWins: boolean;
    /** Kandidaten, die ohne Serverweg zur Verfuegung stehen. */
    localCandidates: number;
    /** Die Buchstaben der Menuepunkte, deren Handlung gelaufen ist, in ihrer Reihenfolge. */
    activatedMenus: string[];
    /** Ob der Griff am Fenster in der einfangenden Phase haengt. */
    keyListenerCapture: boolean;
}

/**
 * Der Griff, an dem der Beweislauf das Layout anfasst.
 *
 * Er traegt drei Dinge, die im DOM nicht oder nur als Pixel stehen, und jedes
 * davon ist eine Frage, die ein Lauf sonst raten muesste:
 *
 *  - **Die Zahlen mit ihren Grenzen.** Ein Lauf kann ein Rechteck messen; ob
 *    das gemessene Rechteck am Anschlag steht oder nur zufaellig so gross ist,
 *    sagt ihm erst `bounds`. Genau daran haengt AC2 ("keine Zone darf
 *    wegziehbar sein"): gemessen wird gegen die Grenze, die die Oberflaeche
 *    selbst nennt, und nicht gegen eine im Lauf abgeschriebene Zahl.
 *  - **Der Zustand hinter den Reitern.** AC5 verlangt, dass ein Reiterwechsel
 *    keinen Zustand kostet. Der Chat-Verlauf steht im DOM, der Walk-Schritt und
 *    die Flow-Stelle stehen dort nur, solange ihr Reiter offen ist. Hier stehen
 *    alle drei jederzeit, und deshalb kann ein Lauf sie VOR und NACH einem
 *    Wechsel vergleichen, statt zu behaupten, sie seien noch da.
 *  - **Der Schluessel im Speicher.** Damit ein Lauf pruefen kann, dass die
 *    Masse pro Projekt gefuehrt werden, ohne den Schluessel nachzubauen.
 */
export interface AtlasLayoutSeam {
    /** Die Masse, die WIRKLICH gezeichnet werden. Gegen sie misst der Lauf Rechtecke. */
    sizes: LayoutSizes;
    /**
     * Die Masse, die der Leser gezogen hat.
     *
     * Sie koennen groesser sein als `sizes`: ein kleines Fenster begrenzt die
     * Darstellung und nicht den Wunsch. Ohne diese zweite Zahl saehe ein Lauf
     * nur, dass eine Spalte schmal ist, und nicht, ob sie es bleiben wird.
     */
    requested: LayoutSizes;
    bounds: Record<LayoutKey, { min: number; max: number }>;
    defaults: LayoutSizes;
    /** Ob alle vier Zahlen auf ihrer Vorgabe stehen. */
    isDefault: boolean;
    frame: LayoutFrame;
    explainOpen: boolean;
    explainTab: string;
    tabs: { id: string; enabled: boolean; reason: string; note: string }[];
    storageKey: string;
    /** Was hinter den Reitern liegt, unabhaengig davon, welcher offen ist. */
    state: {
        chatTurns: number;
        walkStep: number;
        walkSteps: number;
        flowStep: number;
        flowSteps: number;
    };
}

/**
 * Der Griff, an dem der Beweislauf das Einstellungen-Panel anfasst (W10).
 *
 * Er traegt drei Dinge, die im DOM nicht oder nur als Text stehen, und jedes
 * davon ist eine Frage, die ein Lauf sonst raten muesste:
 *
 *  - **Die Wahl und ihr Speicherort.** Ob eine Wahl den Reload ueberlebt, kann
 *    ein Lauf nur pruefen, wenn er den Schluessel kennt, ohne ihn nachzubauen.
 *  - **Was der Prozess wirklich gemeldet hat.** Die Liste der Cache-Modelle
 *    steht auch im DOM, aber ohne die Angabe, ob der Prozess ein Router ist,
 *    und genau daran haengt, ob eine Auswahl ueberhaupt wirkt.
 *  - **Die letzten Messungen.** Sie stehen als Saetze auf dem Schirm; hier
 *    stehen sie als Zahlen, mit dem Rauschband daneben, damit ein Lauf das
 *    Urteil nachrechnen kann, statt einen Satz zu lesen.
 */
export interface AtlasSettingsSeam {
    open: boolean;
    /** Ob das lokale Modell an ist. Ist es aus, hat dieses Panel nichts gefragt. */
    llmOn: boolean;
    /** Wie oft ueberhaupt zum Sidecar gefragt wurde. Dieselbe Zahl wie in __atlasLlm. */
    probes: number;
    /** Ob der Prozess als Router ueber ein Cache-Verzeichnis laeuft. */
    router: boolean;
    /** Die id, an die die naechste Frage geht. Leer heisst: keine Wahl getroffen. */
    selectedModel: string;
    /** Was `/v1/models` gelistet hat, mit der aktiven Zeile markiert. */
    cacheModels: { id: string; name: string; loaded: boolean; status: string; active: boolean }[];
    /** Die vier Zahlen des laufenden Modells, so wie das Panel sie zeigt. */
    running: {
        model: string;
        modelPath: string;
        quantization: string;
        contextTokens: number;
        trainedContextTokens: number;
        weightsBytes: number;
        parameters: number;
    };
    /** Die kuratierten Vorschlaege mit ihren gemessenen Zahlen und ihrem Befehl. */
    suggestions: {
        id: string;
        name: string;
        repo: string;
        modelClass: string;
        passRate: number;
        citationCompliance: number;
        /** `null` heisst: der aufgezeichnete Lauf weist die Zahl nicht aus. */
        citationUnmeasured: number | null;
        tokensPerSecond: number;
        bytes: number;
        command: string;
    }[];
    /** Die Anzeige-Einstellungen, ihre Vorgabe und ob beides dasselbe ist. */
    display: GraphDisplaySettings;
    displayDefault: GraphDisplaySettings;
    isDefaultDisplay: boolean;
    /** Die zwei Schluessel im Speicher dieses Browsers. */
    storageKeys: { model: string; display: string };
    /** Die letzten abgeschlossenen Messungen, eine je Einstellung. */
    measurements: SettingsMeasurement[];
}

declare global {
    // eslint-disable-next-line no-var
    var __atlasSettings: AtlasSettingsSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasLayout: AtlasLayoutSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasSearch: AtlasSearchSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasChat: AtlasChatSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasTour: AtlasTourSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasChecklist: AtlasChecklistSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasCoverage: AtlasCoverageSeam | undefined;
    // eslint-disable-next-line no-var
    var __atlasLlm: AtlasLlmSeam | undefined;
}

export default function App(): JSX.Element {
    const client = useMemo(() => new RpcIntelligenceClient({}), []);
    const api = useMemo(() => new AtlasApi({}), []);

    /*
     * What the projects panel asks the server. The list comes over /rpc like
     * every other read of this window; the rest are the /api routes the panel
     * names in its own text. Opening a project reloads the page with the
     * query the start-up code reads, because every panel of this window is
     * keyed to the project it started with.
     */
    const projectsSource = useMemo<ProjectsSource>(
        () => ({
            listProjects: () => client.listProjects().then((result) => result.projects),
            projectHealth: (name) => api.projectHealth(name),
            deleteProject: (name) => api.deleteProject(name),
            browse: (path) => api.browse(path),
            startIndex: (rootPath, projectName) => api.startIndex(rootPath, projectName),
            indexJobs: () => api.indexJobs(),
            adr: (name) => api.adr(name),
            saveAdr: (name, content) => api.saveAdr(name, content),
            logs: (lines) => api.logs(lines),
            processes: () => api.processes(),
        }),
        [client, api],
    );
    const openProject = useCallback((name: string) => {
        window.location.assign(projectHref(name));
    }, []);

    const [project, setProject] = useState('');
    const [projectDetail, setProjectDetail] = useState('resolving project ...');
    const [serverOk, setServerOk] = useState<boolean | undefined>(undefined);
    const [counts, setCounts] = useState<{ nodes?: number; edges?: number }>({});

    const [levels, setLevels] = useState<ReadonlyMap<string, TreeLevel>>(emptyLevels);
    const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set<string>());
    const [budgetHit, setBudgetHit] = useState(false);
    const [treeError, setTreeError] = useState('');
    const [cursor, setCursor] = useState(0);

    /*
     * Der Coverage-Befund: die zweite Haelfte der Baumquelle.
     *
     * `EMPTY_COVERAGE` heisst "noch nicht gefragt" und nicht "keine Luecken";
     * der Unterschied steht in der Notiz unter dem Baum. Ein gescheiterter Lauf
     * wird darum ausdruecklich gemeldet, statt einen leeren Befund zu setzen.
     */
    const [coverage, setCoverage] = useState<CoverageIndex>(EMPTY_COVERAGE);
    const [coverageMeta, setCoverageMeta] = useState<Record<string, unknown>>({});
    const [coverageAsked, setCoverageAsked] = useState(false);
    const [coverageError, setCoverageError] = useState('');
    /** Was der Store ueber die offene Datei sagt, mit Frische. */
    const [openCoverage, setOpenCoverage] = useState<CoveragePathAnswer | undefined>(undefined);

    const [tabs, setTabs] = useState<string[]>([]);
    const [activePath, setActivePath] = useState('');
    const [document, setDocument] = useState<ReaderDocument | undefined>(undefined);
    const [readerStatus, setReaderStatus] = useState<ReaderStatus>('idle');
    const [readerMessage, setReaderMessage] = useState('pick a file in the explorer');
    const [command, setCommand] = useState('');

    // Zaehler gegen Wettlaeufe: klickt jemand schnell durch zwei Dateien, darf
    // die langsamere Antwort die schnellere nicht ueberschreiben.
    const loadTicket = useRef(0);

    // Der Coverage-Befund als Ref, damit `openFile` ihn lesen kann, ohne bei
    // jeder neuen Antwort seine Identitaet zu wechseln: an `openFile` haengen
    // die Fuehrungen, der Twin und die Galaxie, und ein Wechsel dort haengt
    // eine Handvoll Effekte neu auf.
    const coverageRef = useRef<CoverageIndex>(EMPTY_COVERAGE);
    coverageRef.current = coverage;
    const coverageTicket = useRef(0);

    // ------------------------------------------------------- Twin ----------

    const provider = useMemo(() => new CbmRpcProvider(client, { generation: 1 }), [client]);
    const [overrides, setOverrides] = useState<PresentationOverrides>({});
    // Das Profil ist seit W4a beweglich: die Antwort auf "warum bist du hier"
    // setzt es. Die Regler des Lesers liegen als Overlay darauf, so wie vorher,
    // und werden beim Profilwechsel geleert: eine abgeschaltete Linse, die ueber
    // ein neues Profil hinweg abgeschaltet bliebe, waere eine Sektion, die
    // fehlt, ohne dass jemand sagt warum.
    const [profile, setProfile] = useState<PresentationProfile>(TWIN_PROFILE);
    const presentation = useMemo(() => resolvePresentation(profile, overrides), [profile, overrides]);

    const [twinSymbol, setTwinSymbol] = useState<SymbolRef | undefined>(undefined);
    const [twinIr, setTwinIr] = useState<SemanticIR | undefined>(undefined);
    const [twinStatus, setTwinStatus] = useState<TwinStatus>('empty');
    const [twinMessage, setTwinMessage] = useState(TWIN_EMPTY_MESSAGE);
    const [twinHint, setTwinHint] = useState(TWIN_EMPTY_HINT);
    const [twinName, setTwinName] = useState('no symbol');
    const [caretLine, setCaretLine] = useState<number | undefined>(undefined);
    const [settledCaret, setSettledCaret] = useState<number | undefined>(undefined);
    const [pointedLine, setPointedLine] = useState<number | undefined>(undefined);
    const [reveal, setReveal] = useState<{ line: number; nonce: number } | undefined>(undefined);

    /*
     * Der Erklaeren-Bereich: welcher Reiter gilt, und ob der Bereich offen ist.
     *
     * Zwei Angaben und nicht fuenf Schalter, und das ist der ganze Umbau von W8.
     * Bis dahin hatte jede der fuenf Flaechen ihren eigenen "offen"-Zustand, und
     * genau daraus entstand der Nutzerbefund vom 2026-08-29: drei davon standen
     * gleichzeitig auf `true`, lagen uebereinander und waren alle drei
     * angeschnitten. Mit EINEM Reiter kann das nicht mehr vorkommen; es ist
     * keine Regel, die jemand einhalten muss, sondern eine Form, in der der
     * Fehler nicht formulierbar ist.
     *
     * Eingeklappt per Vorgabe (AC4): eine Flaeche, die beim Oeffnen eines
     * Projekts von selbst aufgeht, erklaert etwas, wonach niemand gefragt hat.
     */
    const [explainOpen, setExplainOpen] = useState(false);
    const [explainTab, setExplainTab] = useState<ExplainTabId>('flow');
    /*
     * Der aktive Reiter, auch fuer die Rueckrufe, die ihn nicht als Abhaengigkeit
     * haben duerfen. Derselbe Umweg wie bei `menuActionsRef` und aus demselben
     * Grund: ein Griff, der bei jedem Reiterwechsel ab- und wieder angemeldet
     * wird, ist ein Griff, der zwischendurch taub ist.
     */
    const explainTabRef = useRef<ExplainTabId>('flow');
    explainTabRef.current = explainTab;
    /*
     * Die Reiter, wie sie das letzte Bild gezeichnet hat, fuer den Griff des
     * Beweislaufs. Sie entstehen unten aus Tatsachen; hier steht nur die
     * Ablage, damit der Effekt, der den Griff schreibt, sie sehen kann.
     */
    const explainTabsRef = useRef<ExplainTabState[]>([]);
    /*
     * Ob der Flow gerade zu sehen ist.
     *
     * Abgeleitet und nicht gespeichert: ein zweiter Schalter neben dem Reiter
     * waere die Stelle, an der beide auseinanderlaufen. Der Schritt selbst steht
     * auf -1, weil "noch keiner" kein Schritt ist; eine 0 hier waere der erste
     * Schritt und wuerde beim Oeffnen ungefragt den Editor bewegen.
     */
    const flowOpen = explainOpen && explainTab === 'flow';
    /*
     * Die Hilfeseite (W7a).
     *
     * Zu per Vorgabe, aus demselben Grund wie der Erklaerer: eine Flaeche ueber
     * dem Editor geht nicht ungefragt auf. Ein Zustand und kein Router-Pfad,
     * weil diese Oberflaeche keine Adressen hat, hinter denen Seiten liegen; sie
     * hat Flaechen, die auf- und zugehen.
     */
    const [helpOpen, setHelpOpen] = useState(false);
    /*
     * Das Einstellungen-Panel (W10), aus denselben Gruenden zu per Vorgabe wie
     * die Hilfe: eine Flaeche ueber dem Editor geht nicht ungefragt auf.
     */
    const [settingsOpen, setSettingsOpen] = useState(false);
    const [projectsOpen, setProjectsOpen] = useState(false);
    /*
     * Die Anzeige-Einstellungen des Lesers.
     *
     * Sie stehen HIER und nicht im Panel, weil zwei Flaechen sie brauchen: das
     * Panel stellt sie ein, das Graph-Panel zeichnet danach. Ein Zustand im
     * Einstellungen-Panel waere weg, sobald jemand es zuklappt, und genau das
     * darf eine Einstellung nicht sein.
     */
    const [display, setDisplay] = useState<GraphDisplaySettings>(DEFAULT_GRAPH_DISPLAY);
    /*
     * Der Live-Modus der Agenten (W11a).
     *
     * AUS per Vorgabe, und er wird auch nicht gespeichert. Dieselbe Regel wie
     * beim lokalen Modell: solange er aus ist, geht keine einzige Anfrage an die
     * Bruecke, und eine Seite, die nach dem naechsten Laden von selbst wieder zu
     * reden anfaengt, waere ein Schalter, ueber den der Leser nur einmal
     * entschieden hat. Er steht hier und nicht im Graph-Panel, weil ihn zwei
     * Wege umlegen: der Menuepunkt und die Kommandozeile.
     */
    const [liveAgentsOn, setLiveAgentsOn] = useState(false);
    /*
     * Die Bitte um das Vollbild, als Zaehler (W11b).
     *
     * Dieselbe Form wie `graphRefit`: die Wahl selbst liegt im Panel, weil sie
     * dort gespeichert wird; was von hier kommt, ist die Bitte. Ein zweiter Ort
     * fuer denselben Zustand waere zwei Wahrheiten darueber, ob das Bild gerade
     * das ganze Fenster fuellt.
     */
    const [fullscreenToggle, setFullscreenToggle] = useState(0);
    /*
     * Der Port der Bruecke, aus der Adresszeile.
     *
     * `?agents=<port>` und sonst 4142. Aus der Adresse und nicht aus einer
     * Einstellung, weil es keine Wahl des Lesers ist, sondern die Lage seiner
     * Maschine: wer eine zweite Bruecke auf einem anderen Port faehrt, sagt es
     * beim Aufrufen, und ein Beweislauf tut dasselbe.
     */
    const bridgePort = useMemo(
        () => bridgePortFromSearch(typeof location === 'undefined' ? '' : location.search),
        [],
    );
    const agentStream = useAgentStream({ on: liveAgentsOn, port: bridgePort });
    const agents = useMemo(
        () => ({ ...agentStream, port: bridgePort, on: liveAgentsOn }),
        [agentStream, bridgePort, liveAgentsOn],
    );
    /** Die letzten Messungen des Panels, je Einstellung. Nur fuer die Naht. */
    const [measurements, setMeasurements] = useState<Record<string, SettingsMeasurement>>({});
    const [flowView, setFlowView] = useState<FlowView | undefined>(undefined);
    const [flowStep, setFlowStep] = useState(-1);
    const [flowMessage, setFlowMessage] = useState(FLOW_LOADING);
    const [twinView, setTwinView] = useState<TwinBodyView>('facts');
    const [imports, setImports] = useState<ImportsGroup | undefined>(undefined);
    const [pseudocode, setPseudocode] = useState<PseudocodeDocument | undefined>(undefined);
    const flowTicket = useRef(0);
    /**
     * Fuer welches Symbol der Walk schon geholt ist.
     *
     * Neu in W8, und der Grund ist AC5: der Flow behaelt seine Stelle ueber einen
     * Reiterwechsel hinweg. Bis dahin holte der Effekt den Walk jedes Mal neu,
     * wenn der Erklaerer aufging, und setzte den Schritt dabei auf -1 zurueck.
     * Als Overlay war das richtig (jedes Aufschlagen war eine neue Sitzung), als
     * Reiter waere es der Verlust, den dieser Zyklus verbietet. Der Vermerk
     * unterscheidet die beiden Faelle: dasselbe Symbol wird nicht noch einmal
     * geholt, ein anderes schon, und der Effekt darueber leert ihn.
     */
    const flowLoadedFor = useRef('');
    const importsTicket = useRef(0);
    /*
     * Waehrend einer Stepper-Sitzung folgt der Twin dem Caret nicht.
     *
     * Ein Schritt kann in eine andere Datei zeigen (ein erhobener Fehlertyp
     * steht fast immer im Aufgerufenen), und der Editor folgt ihm. Ohne diese
     * Klammer wuerde die Aufloesung am Zielort das Subjekt wechseln, der Kasten
     * waere der eines anderen Symbols, und der Leser haette die Sitzung
     * verloren, die er gerade fuehrt. Escape beendet sie und gibt den Caret
     * wieder frei.
     */
    /**
     * Das Subjekt des Twins als Zeichenkette.
     *
     * Der Schluessel und nicht das Objekt, und das ist keine Feinheit: die
     * Aufloesung liefert bei jedem Lauf ein NEUES `SymbolRef`, auch wenn es
     * dasselbe Symbol meint. Ein Effekt, der an der Objektgleichheit haengt,
     * wirft dann den geladenen Walk weg, sobald der Caret sich noch einmal
     * setzt, und der Erklaerer stuende dauerhaft auf "wird geladen". Genau das
     * ist beim ersten Lauf dieses Zyklus passiert, und der Schluessel ist die
     * Antwort darauf: gleiches Symbol, gleicher Schluessel, kein Wegwerfen.
     */
    const twinKey = twinSymbol === undefined ? '' : (twinSymbol.qualifiedName ?? twinSymbol.name);

    const flowPinned = useRef(false);
    /*
     * Der Reiter und nicht die Sichtbarkeit haelt die Klammer.
     *
     * Bis W8 stand hier `flowOpen`, also "das Overlay ist zu sehen". Seit der
     * Erklaerer ein Reiter ist, gibt es einen dritten Zustand: der Reiter ist
     * gewaehlt, aber die Zone ist eingeklappt (Escape). Die Sitzung ist dann
     * NICHT beendet, denn Escape kostet seit W8 nichts, und eine Klammer, die
     * sich beim Einklappen loeste, gaebe den Caret frei und wechselte hinter dem
     * eingeklappten Bereich das Subjekt. Der Leser klappte wieder auf und faende
     * das Bild eines anderen Symbols.
     */
    flowPinned.current = explainTab === 'flow' && flowStep >= 0;

    /**
     * Was jeder verdrahtete Menuebuchstabe tut, immer auf dem Stand des Bildes.
     *
     * Geschrieben wird es unten bei den Menuepunkten, gelesen oben im
     * Tastatur-Griff, und der Grund fuer den Umweg ueber ein Ref steht an beiden
     * Stellen. Der Schluessel ist der Buchstabe aus {@link WIRED_MENU_SHORTCUTS}.
     */
    const menuActionsRef = useRef<Record<string, () => void>>({});

    /**
     * Was der Tastatur-Griff ueber den Bildschirm wissen muss, ohne ihn neu
     * aufzuhaengen.
     *
     * Zwei Fragen, und beide aendern sich mit jedem Bild: liegt gerade eine
     * Flaeche ueber dem Editor, und laeuft eine Fuehrung. Sie in die
     * Abhaengigkeitsliste des Effekts zu schreiben hiesse, den Griff bei jedem
     * geoeffneten Panel ab- und wieder anzumelden; sie in seine Schliessung zu
     * schreiben hiesse, dass er die Lage von vor hundert Bildern liest.
     */
    const keyboardGuardRef = useRef<{ overlayOpen: boolean; walkRunning: boolean }>({
        overlayOpen: false,
        walkRunning: false,
    });

    /*
     * Das geladene Dokument als Ref.
     *
     * Die Imports-Lesung braucht den Text der Datei, die schon offen ist, und
     * soll dafuer nicht jedesmal neu aufgehaengt werden, wenn der Reader ein
     * Zeichen mehr geladen hat. Der Effekt haengt am Pfad und an der IR; der
     * Text wird zum Zeitpunkt der Frage gelesen.
     */
    const documentRef = useRef<ReaderDocument | undefined>(undefined);
    documentRef.current = document;

    const twinTicket = useRef(0);
    // Das aktuelle Subjekt als Ref, damit die Aufloesung es lesen kann, ohne
    // dass der Effekt bei jedem Subjektwechsel neu aufgehaengt wird.
    const shownSymbol = useRef<SymbolRef | undefined>(undefined);
    shownSymbol.current = twinSymbol;
    // Die IR ebenfalls als Ref: der Klick auf eine Zeile muss wissen, aus
    // welcher Checkliste die Zeile stammt, und darf dafuer nicht neu gebunden
    // werden, sooft das Panel neu zeichnet.
    const shownIr = useRef<SemanticIR | undefined>(undefined);
    shownIr.current = twinIr;

    const irCache = useMemo(
        () =>
            new IrCache(
                async (symbol) => buildIr(provider, ATLAS_WORKSPACE_ROOT, symbol, {
                    projectName: project,
                    generation: 1,
                }),
                () => {
                    // Nur hier, und nur bei einer echten Anfrage. Der Beweislauf
                    // misst an dieser Zahl, ob ein Caret-Wechsel nachgeladen hat.
                    window.__atlasTwinFetches = (window.__atlasTwinFetches ?? 0) + 1;
                },
            ),
        [provider, project],
    );

    // ------------------------------------------------- Galaxie und Suche ---

    const [galaxyOn, setGalaxyOn] = useState(true);
    const [layout, setLayout] = useState<GraphData | undefined>(undefined);
    const [hits, setHits] = useState<RankedHit[]>(noHits);
    const [selectedHit, setSelectedHit] = useState(0);
    const [searchStatus, setSearchStatus] = useState<SearchOverlayStatus>('ready');
    const [searchMessage, setSearchMessage] = useState('');
    const [answeredQuery, setAnsweredQuery] = useState('');
    /** Woher die gezeigten Zeilen kommen: aus der Antwort des Index, oder vorlaeufig. */
    const [hitSource, setHitSource] = useState<SearchRowSource>('index');
    const searchTicket = useRef(0);
    /**
     * Der Abbruch der laufenden Runde.
     *
     * Die Ticketpruefung allein reicht seit W7b nicht mehr: sie verhindert, dass
     * eine ueberholte Antwort die neuere ueberschreibt, laesst die ueberholte
     * Runde aber zu Ende laufen. Bei einem Wort je Anfrage sind das mehrere
     * Anfragen, die niemand mehr liest, und sie stehen in der Warteschlange vor
     * denen, die noch jemand liest. Beides bleibt: der Abbruch spart den Weg,
     * das Ticket ist die Zusicherung.
     */
    const searchAbort = useRef<AbortController | undefined>(undefined);
    /**
     * Die letzte vollstaendige Antwort des Index, roh.
     *
     * Sie traegt das Verlaengern eines Wortes: wer `valid` gefragt hat und dann
     * `validate` tippt, sieht sofort die neu gerankten Kandidaten der vorigen
     * Runde, statt auf einen zweiten Weg zum Server zu warten.
     *
     * Sie ERSETZT den Weg nicht, und das ist eine bewusste Grenze. Die Antwort
     * des Servers auf ein laengeres Wort ist nicht garantiert eine Teilmenge
     * seiner Antwort auf das kuerzere: die BM25-Suche zerlegt Bezeichner in
     * Woerter, und `validate` kann ein Symbol treffen, das `valid` nicht traf.
     * Ein Cache, der die Anfrage einspart, wuerde solche Treffer still
     * verschlucken. Er spart also die WARTEZEIT und nicht die Wahrheit: die
     * Zeilen stehen sofort da und tragen ihre Marke, und die Antwort des Index
     * ersetzt sie, sobald sie da ist.
     */
    const prefixCache = useRef<
        { query: string; candidates: SymbolSearchHit[]; complete: boolean } | undefined
    >(undefined);
    /** Die Zahlen, die der Beweislauf liest. Siehe AtlasSearchSeam. */
    const searchStats = useRef({
        localFirst: 0,
        prefixCacheHits: 0,
        serverRequests: 0,
        serverRoundtripMs: 0,
        roundtrips: [] as number[],
        abortedRequests: 0,
        staleDropped: 0,
        staleAnswerWins: false,
        appliedTicket: 0,
    });
    /** Die Menuepunkte, deren Handlung gelaufen ist. Siehe AtlasSearchSeam. */
    const activatedMenus = useRef<string[]>([]);

    // ------------------------------- Warum, Fuehrungen und die Checkliste ---

    /**
     * Der Speicher dieses Browsers.
     *
     * Einmal geholt und weitergereicht, damit jede Stelle, die etwas merkt oder
     * nachschlaegt, denselben Speicher benutzt und ein Test ihn ersetzen kann,
     * ohne globale Objekte zu verbiegen.
     */
    const store = useMemo(() => browserStore(), []);

    // ------------------------------------------------ Die Zonen (W8) -------

    /*
     * Wie gross das Fenster ist, als Zustand und nicht als Abfrage.
     *
     * Die Hoechstmasse der vier Zonen haengen daran (src/layout/layout-model.ts),
     * und ein Griff, dessen `aria-valuemax` aus der Fenstergroesse von vor dem
     * letzten Ziehen stammt, meldet einen Anschlag, den es nicht gibt. Der
     * Anfangswert wird beim ersten Bild gelesen und im Effekt darunter sofort
     * korrigiert: `window.innerHeight` ist beim ersten Bild noch die Hoehe vor
     * dem Layout. Dieselbe Ueberlegung wie beim Deckel des Antwort-Panels in
     * W7c, nur fuer alle vier Masse.
     */
    const [frame, setFrame] = useState<LayoutFrame>(() => ({
        width: window.innerWidth,
        height: window.innerHeight,
    }));
    const [zones, setZones] = useState<LayoutSizes>(() =>
        defaultLayout({ width: window.innerWidth, height: window.innerHeight }));
    const frameRef = useRef(frame);
    frameRef.current = frame;
    const zonesRef = useRef(zones);
    zonesRef.current = zones;
    const projectRef = useRef('');

    useEffect(() => {
        const measure = (): void => {
            setFrame({ width: window.innerWidth, height: window.innerHeight });
        };
        measure();
        window.addEventListener('resize', measure);
        return () => window.removeEventListener('resize', measure);
    }, []);

    /**
     * Die vier Zahlen setzen und merken, an einer Stelle.
     *
     * Beides zusammen und nicht in zwei Effekten: ein Effekt, der bei jeder
     * Aenderung schreibt, schreibt beim Laden zuerst die Vorgabe ueber den
     * gemerkten Stand und danach den gemerkten Stand zurueck. Das geht gut, bis
     * jemand das Fenster genau dazwischen schliesst.
     */
    const applyZones = useCallback((next: LayoutSizes) => {
        setZones(next);
        /*
         * Gespeichert wird nur, was sich wirklich geaendert hat.
         *
         * Ein Zug schickt zwanzig Ereignisse zwischen zwei Bildern, und die
         * meisten davon tragen dieselbe Zahl (der Griff steht am Anschlag, oder
         * die Bewegung war kleiner als ein Pixel). Sie alle zu speichern hiesse,
         * zwanzigmal in localStorage zu schreiben, um nichts zu aendern.
         *
         * Der Zustand wird trotzdem IMMER gesetzt, und das ist keine
         * Unachtsamkeit: `setZones` mit einem neuen Objekt zeichnet ein Bild,
         * und an diesem Bild haengen die Naehte, an denen der Beweislauf liest.
         * Ein "reset layout" auf ein Layout, das schon die Vorgabe ist, aendert
         * nichts und muss trotzdem sichtbar stattgefunden haben; ohne das Bild
         * meldete der Griff des Laufs, der Menuepunkt habe nicht ausgeloest.
         * Gemessen an genau diesem Fall (smoke:w7b, `a-layout`).
         */
        if (!sameLayout(next, zonesRef.current)) {
            writeLayout(store, projectRef.current, next);
        }
    }, [store]);

    /** Eine Grenze verschieben. Die Grenzen der Grenze kommen aus dem Modell. */
    const changeZone = useCallback((key: LayoutKey, value: number) => {
        applyZones({
            ...zonesRef.current,
            [key]: clampLayoutValue(key, value, frameRef.current),
        });
    }, [applyZones]);

    /** Doppelklick auf einen Griff: genau diese eine Grenze zurueck. */
    const resetZone = useCallback((key: LayoutKey) => {
        changeZone(key, defaultLayout(frameRef.current)[key]);
    }, [changeZone]);

    /**
     * Die Bitte an das Graph-Panel, sein Bild wieder einzupassen (W10b, AC5).
     *
     * Eine Zahl und kein Zustand: jede Erhoehung ist eine neue Bitte. Sie steht
     * hier, weil "reset layout" sie ausloest; der Knopf in der Szene hat seinen
     * eigenen Zaehler, und beide gehen im Panel durch dieselbe Rechnung.
     */
    const [graphRefit, setGraphRefit] = useState(0);

    /**
     * Der Weg zurueck aus AC3: alle vier Grenzen auf die Vorgabe.
     *
     * Eine Funktion fuer beide Wege (Menuepunkt und Kommandozeile), damit sie
     * nicht auseinanderlaufen koennen. Der Erklaeren-Bereich klappt dabei NICHT
     * zu: "reset layout" ist eine Aussage ueber Groessen und nicht darueber, was
     * der Leser gerade erklaert haben will.
     *
     * Seit W10b passt der Graph sein Bild dabei wieder ein. Das ist kein zweiter
     * Gegenstand, sondern derselbe: die Zonen bekommen ihre Vorgabegroesse
     * zurueck, und die eingepasste Ansicht IST die Vorgabe des Graphen. Wer sich
     * in der Wolke verdreht hat, kommt sonst nur ueber den Knopf in der Szene
     * zurueck, und "reset layout" liesse ausgerechnet die Flaeche stehen, in der
     * man sich am leichtesten verliert.
     */
    const resetLayout = useCallback(() => {
        applyZones(defaultLayout(frameRef.current));
        setGraphRefit((count) => count + 1);
    }, [applyZones]);

    /**
     * Was gezeichnet wird: der Wunsch des Lesers, so weit das Fenster ihn traegt.
     *
     * Zwei Zahlen und nicht eine, und der Unterschied ist ein Nutzerbefund im
     * Kleinen: `zones` ist, was der Leser GEZOGEN hat, `shownZones` ist, was
     * davon in dieses Fenster passt. Bis dahin hatte ein Effekt die gezogene
     * Zahl beim Verkleinern des Fensters einfach ueberschrieben, und beim
     * Vergroessern kam sie nicht zurueck: wer sein Fenster kurz klein macht,
     * findet danach schmale Spalten vor und muss alles neu ziehen. Gemessen im
     * Beweislauf von W5c, der fuer seinen Tab-Test das Fenster verkleinert und
     * wieder vergroessert: die rechte Spalte blieb bei 402 statt 439 Pixeln
     * stehen, und der Graph verlor seinen Anteil an der Spalte.
     *
     * Der Wunsch bleibt also stehen, und begrenzt wird nur die Darstellung.
     */
    const shownZones = useMemo(() => clampLayout(zones, frame), [zones, frame]);

    /*
     * Die gemerkten Masse dieses Projekts, beim Projektwechsel neu gelesen.
     *
     * Pro Projekt, wie der Contract es verlangt: ein Repository mit tiefen
     * Pfaden braucht einen breiten Explorer, ein flaches nicht, und wer zwischen
     * beiden wechselt, will nicht jedes Mal nachziehen.
     */
    useEffect(() => {
        projectRef.current = project;
        setZones(readLayout(store, project, frameRef.current));
    }, [project, store]);

    const [whyAnswer, setWhyAnswer] = useState<WhyAnswer>({ asked: false });
    /** Von Hand wieder aufgerufen, ueber das [a]tlas-Menue. */
    const [whyReopened, setWhyReopened] = useState(false);
    /**
     * Ob der Leser die Frage in dieser Sitzung weggelegt hat, indem er anfing zu
     * lesen.
     *
     * Nutzerbefund vom 2026-08-29 mit zwei Screenshots: "wenn ich eine Klasse
     * anklicke, zum Beispiel orderService.ts, bleibt das und nicht der Code wird
     * gezeigt." Die Frage verschwand von selbst nur, solange KEINE Datei offen
     * war; von Hand wieder aufgerufen blieb sie ueber allem stehen, auch ueber
     * einer Datei. Das war Absicht fuer den Fall "mitten im Lesen den Modus
     * wechseln", und es ist die falsche Absicht, sobald der Leser danach etwas
     * anderes tut: der Klick im Explorer OEFFNETE die Datei, Tab und Breadcrumb
     * wechselten, und sichtbar blieb die Frage. Eine Aktion, die wirkt, ohne
     * dass man ihre Wirkung sieht, ist die schlimmste Sorte stillen Fehlschlags.
     *
     * Wer eine Datei oeffnet, ein Symbol waehlt oder eine Frage stellt, hat die
     * Frage beantwortet, indem er anfing zu lesen. Nur fuer diese Sitzung und
     * nicht im Speicher des Browsers: was er dauerhaft beantwortet, beantwortet
     * er mit einer der vier Karten oder mit dem Knopf darunter.
     */
    const [whyDismissed, setWhyDismissed] = useState(false);

    const [tour, setTour] = useState<ActiveTour | undefined>(undefined);
    const [tourStep, setTourStep] = useState(0);
    const [tourMessage, setTourMessage] = useState('');
    const tourStepRef = useRef(0);
    tourStepRef.current = tourStep;

    /*
     * Der Walk hinter einem Vorwaerts-Gang, aufgehoben statt weggeworfen.
     *
     * Bis W4d wurde die Antwort von `getClosure` sofort in eine Fuehrung
     * uebersetzt und danach vergessen. Die Fuehrung ist aber eine Liste, und
     * eine Liste zeigt keine Tiefe: sie hat die Symbole ohne Datei schon
     * weggelassen und aus dem Graphen eine Reihe gemacht. Das Graph-Panel
     * bekommt deshalb den Walk selbst (W4e) und projiziert ihn, statt aus
     * Schritten einen Graphen zurueckzurechnen, den es nie gab.
     */
    const [walk, setWalk] = useState<ClosureResult | undefined>(undefined);

    const [entryOpen, setEntryOpen] = useState(false);
    const [overview, setOverview] = useState<ArchitectureOverviewDto | undefined>(undefined);
    const [entryQuery, setEntryQuery] = useState('');
    const [entryHits, setEntryHits] = useState<RankedHit[]>(noHits);
    const [entryStatus, setEntryStatus] = useState<EntrySearchStatus>('idle');
    const [entryMessage, setEntryMessage] = useState('');
    const entryTicket = useRef(0);

    const [marks, setMarks] = useState<UnderstandingRecord>({ visited: {}, confirmed: {} });

    // -------------------------------------- Der BUG-Assistent und die Aenderung

    /*
     * Ob der Assistent zu sehen ist, abgeleitet aus dem Reiter.
     *
     * Kein eigener Schalter mehr, und das ist der Kern von W8: ein Schalter
     * neben dem Reiter waere die Stelle, an der beide auseinanderlaufen, und
     * zwei Flaechen mit je einem eigenen Schalter sind genau der Zustand, in
     * dem der Nutzer sie am 2026-08-29 uebereinander liegen sah.
     */
    const bugOpen = explainOpen && explainTab === 'bug';
    const [bugTarget, setBugTarget] = useState<BugPathNode | undefined>(undefined);
    const [bugPathsDto, setBugPathsDto] = useState<BugPathsDto | undefined>(undefined);
    const [bugStatus, setBugStatus] = useState<BugWizardStatus>('idle');
    const [bugMessage, setBugMessage] = useState('');
    const bugTicket = useRef(0);

    const impactOpen = explainOpen && explainTab === 'change';
    const [impactMode, setImpactMode] = useState<ImpactMode>('worktree');
    const [impactModel, setImpactModel] = useState<ImpactModel | undefined>(undefined);
    const [impactStatus, setImpactStatus] = useState<ImpactStatus>('idle');
    const [impactMessage, setImpactMessage] = useState('');
    const [impactRouteNote, setImpactRouteNote] = useState('');
    const [refDraft, setRefDraft] = useState('');
    const [refError, setRefError] = useState('');
    /**
     * Der Vergleichspunkt, gegen den die angezeigte Lesung wirklich gefahren
     * wurde.
     *
     * Getrennt vom Feldinhalt, und das ist der Punkt: wer tippt, aendert nicht
     * die Frage, die auf dem Bildschirm beantwortet ist. Erst der Knopf setzt
     * ihn, und nur ein Ref, dessen Form geprueft wurde, kommt hier an.
     */
    const [appliedRef, setAppliedRef] = useState<string | undefined>(undefined);
    const impactTicket = useRef(0);

    // ------------------------------------------ Das lokale Modell (W5a) ----

    /*
     * Drei Zustaende und kein vierter: was das Projekt erlaubt, was dieser
     * Browser eingestellt hat, und was der Prozess auf 4141 gerade von sich
     * gibt. Die ersten beiden entscheiden ueber das Fragen, der dritte ist die
     * Antwort auf das Gefragte.
     *
     * `llmPolicy` bleibt undefined, bis die Datei des Zielprojekts gelesen ist.
     * Genau so lange ist das LLM aus, auch wenn die Praeferenz an sagt: siehe
     * src/llm/llm-state.ts, Regel 3.
     */
    const [llmPolicy, setLlmPolicy] = useState<PolicyReading | undefined>(undefined);
    const [llmPreferenceOn, setLlmPreferenceOn] = useState(false);
    const [llmProbe, setLlmProbe] = useState<SidecarReading>(EMPTY_SIDECAR_READING);
    const llmProbes = useRef(0);

    /*
     * Die Modellwahl dieses Browsers (W10), und der Griff, der sofort nachfragt.
     *
     * Die Wahl steht als Ref DANEBEN, weil der Probe-Timer sie liest: haenge er
     * an ihr, wuerde jede Wahl den Timer ab- und neu aufhaengen. Der Griff geht
     * die andere Richtung: wer waehlt, will die Zahlen des gewaehlten Modells
     * sehen, ohne drei Sekunden auf die naechste Runde zu warten. Er ruft
     * dieselbe Probe wie der Timer und landet damit im selben Zaehler; ein
     * zweiter Weg zum Sidecar waere eine Anfrage, die `__atlasLlm.probes` nicht
     * mitzaehlt, und der Beweislauf misst an der neuen Flaeche vorbei.
     */
    const [selectedModel, setSelectedModel] = useState('');
    const selectedModelRef = useRef('');
    selectedModelRef.current = selectedModel;
    const askSidecarRef = useRef<((model?: string) => void) | undefined>(undefined);

    // ------------------------------------------- Der Atlas-Chat (W5b) ------

    /*
     * Der Verlauf dieser Sitzung, und nur dieser.
     *
     * Im Speicher und nirgends sonst: kein localStorage, keine Datei, kein
     * Server. Das ist keine fehlende Funktion, sondern Non-Goal des Contracts.
     * Ein Verlauf, der den Reload ueberlebt, ist ein Protokoll darueber, was
     * jemand nicht verstanden hat, und dieses Produkt legt keines an.
     */
    const [chatTurns, setChatTurns] = useState<ChatTurn[]>([]);
    /** Martins Kontextregel als Einstellung. Vorgabe: Fokus plus erste Nachbarschaft. */
    const [chatDepth, setChatDepth] = useState<NeighborDepth>(NEIGHBOR_DEPTH_DEFAULT);
    const chatTicket = useRef(0);
    /*
     * Ob der Chat zu sehen ist, abgeleitet aus dem Reiter.
     *
     * Der Zustand, den W7c hier eingefuehrt hat, ist nicht verschwunden, er ist
     * eine Ebene hoeher gewandert: "hat dieser Leser schon gefragt" steht in
     * `chatTurns`, "will er die Antworten gerade sehen" im Reiter des
     * Erklaeren-Bereichs. Der Nutzerbefund vom 2026-08-29 dahinter gilt
     * unveraendert: Zumachen laesst den Verlauf stehen, und nur `clear` loescht.
     */
    const chatOpen = explainOpen && explainTab === 'chat';

    /*
     * Die Umformulierung des Pseudocode-Blocks.
     *
     * `refined` haelt das Ergebnis getrennt vom deterministischen Block, statt
     * ihn zu ersetzen: das Original muss jederzeit zurueckholbar sein, und ein
     * ueberschriebener Block waere genau das nicht mehr.
     */
    const [refined, setRefined] = useState<PseudocodeDocument | undefined>(undefined);
    const [refineState, setRefineState] = useState<'idle' | 'running' | 'applied' | 'refused'>('idle');
    const [refineMessage, setRefineMessage] = useState('');

    /** Der Griff auf das Eingabefeld, fuer den einen Weg zurueck in die Suche. */
    const commandInputRef = useRef<HTMLInputElement | null>(null);

    /**
     * Die Grenzen des Vorwaerts-Walks, einmal aus der Adresszeile gelesen.
     *
     * Fehlt ein Parameter, entscheidet der Vorgabewert in provider/closure.ts.
     * Gelesen wird nur beim Start: eine Grenze, die sich waehrend eines Walks
     * aendert, waere ein Walk, der auf halber Strecke eine andere Frage
     * beantwortet.
     */
    const closureBounds = useMemo(() => {
        const params = new URLSearchParams(window.location.search);
        const read = (name: string): number | undefined => {
            const raw = params.get(name);
            if (raw === null) {
                return undefined;
            }
            const value = Number.parseInt(raw, 10);
            return Number.isFinite(value) ? value : undefined;
        };
        return { depth: read(CLOSURE_DEPTH_PARAM), cap: read(CLOSURE_CAP_PARAM) };
    }, []);

    /*
     * Der Vorwaerts-Walk aus dem Symbol im Fokus (W10b, AC3).
     *
     * Nutzerbefund vom 2026-08-30: "und hierarchies werden auch nicht
     * angezeigt." Die Ursache stand in GalaxyPanel.tsx: die Projektion hing am
     * WALK, und einen Walk gab es nur nach einem Einstiegs-Spaziergang oder in
     * einer Fuehrung. Ein Symbol im Fokus reichte nicht, obwohl der Leser genau
     * dann seinen Aufrufbaum sehen will.
     *
     * Drei Entscheidungen stecken darin:
     *
     *  1. **Derselbe Closure, dieselben Grenzen.** Gerechnet wird mit
     *     `getClosure` und mit `closureBounds`, also Zeichen fuer Zeichen wie
     *     beim Einstiegs-Spaziergang. Ein zweiter, eigener Weg waere ein zweites
     *     Bild derselben Frage.
     *  2. **Nur ohne laufenden Spaziergang.** Laeuft einer, ist er gemeint; ihn
     *     zu ueberschreiben, weil der Leser in ihm gerade woanders hinsieht,
     *     waere das Ende des Spaziergangs ohne Ansage.
     *  3. **Das Ergebnis waehlt keine Ansicht.** Es macht die Hierarchie
     *     moeglich, mehr nicht; die Vorgabe bleibt die Galaxie (Entscheidung 17
     *     im Kopf von GalaxyPanel.tsx).
     */
    const [focusWalk, setFocusWalk] = useState<ClosureResult | undefined>(undefined);
    useEffect(() => {
        if (walk !== undefined || twinSymbol === undefined || project.length === 0) {
            setFocusWalk(undefined);
            return;
        }
        let cancelled = false;
        getClosure(provider, ATLAS_WORKSPACE_ROOT, twinSymbol, {
            projectName: project,
            generation: 1,
            ...(closureBounds.depth === undefined ? {} : { depth: closureBounds.depth }),
            ...(closureBounds.cap === undefined ? {} : { cap: closureBounds.cap }),
        })
            .then((closure) => {
                if (!cancelled) {
                    setFocusWalk(closure);
                }
            })
            .catch(() => {
                /*
                 * Ein misslungener Closure ist hier keine Meldung wert: niemand
                 * hat ihn bestellt. Der Knopf bleibt dann grau und sagt, was
                 * fehlt, statt eine Fehlermeldung ueber eine Frage zu zeigen,
                 * die der Leser nicht gestellt hat.
                 */
                if (!cancelled) {
                    setFocusWalk(undefined);
                }
            });
        return () => {
            cancelled = true;
        };
    }, [walk, twinSymbol, provider, project, closureBounds]);

    // ------------------------------------------------------- Projekt -------

    useEffect(() => {
        const fromUrl = new URLSearchParams(window.location.search).get('project') ?? '';
        if (fromUrl.length > 0) {
            setProject(fromUrl);
            setProjectDetail('');
            return;
        }
        let cancelled = false;
        client
            .listProjects()
            .then((result) => {
                if (cancelled) {
                    return;
                }
                const first = result.projects[0];
                if (first === undefined) {
                    setProjectDetail(messages.statusbar.noProjectIndexed);
                    return;
                }
                setProject(first.name);
                setProjectDetail('');
            })
            .catch((error: unknown) => {
                if (!cancelled) {
                    setServerOk(false);
                    setProjectDetail(messages.statusbar.noProjectError(String(error)));
                }
            });
        return () => {
            cancelled = true;
        };
    }, [client]);

    // ------------------------------------------------------- Kennzahlen ----

    useEffect(() => {
        if (project.length === 0) {
            return;
        }
        let cancelled = false;
        client
            .indexStatus(project)
            .then((status) => {
                if (cancelled) {
                    return;
                }
                setServerOk(true);
                const next: { nodes?: number; edges?: number } = {};
                if (status.nodes !== undefined) {
                    next.nodes = status.nodes;
                }
                if (status.edges !== undefined) {
                    next.edges = status.edges;
                }
                setCounts(next);
            })
            .catch(() => {
                if (!cancelled) {
                    setServerOk(false);
                    setCounts({});
                }
            });
        return () => {
            cancelled = true;
        };
    }, [client, project]);

    // ------------------------------------------------------- Baum ----------

    useEffect(() => {
        if (project.length === 0) {
            return;
        }
        let cancelled = false;
        (async () => {
            const root = await api.tree(project, '');
            const nextLevels = new Map<string, TreeLevel>([['', root]]);
            const nextExpanded = new Set<string>();
            const queue = directoryPaths(root);
            while (queue.length > 0 && nextLevels.size < EAGER_LEVEL_BUDGET) {
                const dir = queue.shift();
                if (dir === undefined || nextLevels.has(dir)) {
                    continue;
                }
                const level = await api.tree(project, dir);
                if (cancelled) {
                    return;
                }
                nextLevels.set(dir, level);
                nextExpanded.add(dir);
                queue.push(...directoryPaths(level));
            }
            if (cancelled) {
                return;
            }
            setLevels(nextLevels);
            setExpanded(nextExpanded);
            setBudgetHit(queue.length > 0);
            setTreeError('');
            setServerOk(true);
        })().catch((error: unknown) => {
            if (!cancelled) {
                setLevels(emptyLevels);
                setExpanded(new Set<string>());
                setTreeError(String(error instanceof Error ? error.message : error));
                setServerOk(false);
            }
        });
        return () => {
            cancelled = true;
        };
    }, [api, project]);

    /*
     * Die zweite Baumquelle: der Coverage-Store.
     *
     * Ein eigener Effekt und nicht ein Anhaengsel des Baum-Effekts, aus zwei
     * Gruenden. Erstens haengen die beiden an verschiedenen Dingen: der Baum an
     * `/api`, die Coverage an `/rpc`, und ein Ausfall der einen darf die andere
     * nicht mitnehmen. Zweitens ist die Reihenfolge egal: der Join ist eine
     * reine Funktion ueber beide Ergebnisse, also darf ankommen, was zuerst
     * fertig ist.
     */
    useEffect(() => {
        if (project.length === 0) {
            return;
        }
        let cancelled = false;
        setCoverageAsked(false);
        loadCoverage(client, project)
            .then((reading) => {
                if (cancelled) {
                    return;
                }
                setCoverage(reading.index);
                setCoverageMeta({ ...reading.answer.metadata });
                setCoverageAsked(true);
                setCoverageError('');
            })
            .catch((error: unknown) => {
                if (cancelled) {
                    return;
                }
                // Kein stiller Rueckfall auf "keine Luecken": ein Baum, der
                // Vollstaendigkeit behauptet, weil er nicht nachfragen konnte,
                // waere die teuerste Luege dieses Zyklus.
                setCoverage(EMPTY_COVERAGE);
                setCoverageMeta({});
                setCoverageAsked(false);
                setCoverageError(error instanceof Error ? error.message : String(error));
            });
        return () => {
            cancelled = true;
        };
    }, [client, project]);

    /**
     * Die Vereinigung, aus der der Explorer zeichnet.
     *
     * Der Nur-Graph-Weg ist damit weg: `rows` haengt an dieser Karte und nicht
     * mehr an `levels`. Solange die Coverage-Antwort fehlt, ist die Vereinigung
     * genau der Graph, und die Notiz unter dem Baum sagt, dass noch niemand
     * gefragt hat.
     */
    const mergedLevels = useMemo(() => mergeCoverageIntoLevels(levels, coverage), [levels, coverage]);

    const rows = useMemo(() => flattenTree(mergedLevels, expanded), [mergedLevels, expanded]);

    const loadLevel = useCallback(
        async (path: string) => {
            const level = await api.tree(project, path);
            setLevels((current) => {
                const next = new Map(current);
                next.set(path, level);
                return next;
            });
        },
        [api, project],
    );

    const toggleDirectory = useCallback(
        (row: TreeRow) => {
            setExpanded((current) => {
                const next = new Set(current);
                if (next.has(row.path)) {
                    next.delete(row.path);
                } else {
                    next.add(row.path);
                }
                return next;
            });
            if (!levels.has(row.path)) {
                void loadLevel(row.path).catch((error: unknown) => {
                    // Ein Ordner, den nur die Coverage-Listen kennen, hat im
                    // Graphen keine Ebene. Dass /api/tree ihn nicht kennt, ist
                    // dann der erwartete Fall und kein Baumfehler; die Zeilen
                    // darunter kommen ohnehin aus der Vereinigung.
                    if (mergedLevels.get(row.path)?.synthetic === true) {
                        return;
                    }
                    setTreeError(String(error instanceof Error ? error.message : error));
                });
            }
        },
        [levels, loadLevel, mergedLevels],
    );

    // ------------------------------------------------------- Datei oeffnen -

    /**
     * Eine Datei oeffnen, mit dem Coverage-Befund als erster Instanz.
     *
     * Steht die Datei in einer der Listen, die "kein Inhalt" bedeuten, wird gar
     * nicht erst geladen: der Server liefert Inhalt ausschliesslich ueber einen
     * Modul-Knoten, und den gibt es fuer eine uebersprungene Datei nicht. Ein
     * Aufruf, dessen Ausgang schon feststeht, waere eine Anfrage, um eine
     * Fehlermeldung zu erzeugen, die man vorher kannte.
     *
     * Scheitert das Laden trotzdem an einem fehlenden Modul-Knoten, ist das
     * dieselbe Grenze und nicht ein Ausfall: `unavailable` statt `failed`, mit
     * der Begruendung, die der Lader selbst geschrieben hat.
     */
    const openFile = useCallback(
        (path: string) => {
            /*
             * Wer eine Datei oeffnet, hat die Frage beantwortet, indem er
             * anfing zu lesen (Nutzerbefund 2026-08-29, AC6f). Hier und nicht
             * im Explorer, weil dieselbe Funktion auch die Suche, die Zitate
             * des Chats, der Walk und jeder Sprung des Twin benutzen: eine
             * Regel an einer Stelle statt sechs Aufrufen derselben Regel.
             */
            setWhyReopened(false);
            setWhyDismissed(true);
            setTabs((current) => (current.includes(path) ? current : [...current, path]));
            setActivePath(path);
            setDocument(undefined);
            loadTicket.current += 1;
            const ticket = loadTicket.current;

            const record = coverageRef.current.records.get(path);
            const state: CoverageState = record?.state ?? 'indexed';
            if (state === 'skipped' || state === 'not-indexed' || state === 'ignored') {
                setReaderStatus('unavailable');
                setReaderMessage(readerUnavailableNote(path, state, record?.reason ?? ''));
                return;
            }

            setReaderStatus('loading');
            setReaderMessage(messages.reader.loading(path, READER_RPC_TOOL));
            loadFileDocument(client, project, path)
                .then((loaded) => {
                    if (ticket !== loadTicket.current) {
                        return;
                    }
                    setDocument(loaded);
                    setReaderStatus('ready');
                })
                .catch((error: unknown) => {
                    if (ticket !== loadTicket.current) {
                        return;
                    }
                    setDocument(undefined);
                    setReaderStatus(error instanceof FileNotReadableError ? 'unavailable' : 'failed');
                    setReaderMessage(error instanceof Error ? error.message : String(error));
                });
        },
        [client, project],
    );

    const closeTab = useCallback(
        (path: string) => {
            setTabs((current) => {
                const next = current.filter((entry) => entry !== path);
                if (path === activePath) {
                    const fallback = next[next.length - 1];
                    if (fallback === undefined) {
                        setActivePath('');
                        setDocument(undefined);
                        setReaderStatus('idle');
                        setReaderMessage(messages.reader.pickFile);
                    } else {
                        openFile(fallback);
                    }
                }
                return next;
            });
        },
        [activePath, openFile],
    );

    /*
     * Die Frische der offenen Datei.
     *
     * Eine eigene Frage je Datei, weil die Antwort nur beim Fragen entsteht:
     * der Server vergleicht mtime und Groesse der Datei auf der Platte mit dem,
     * was beim Indexlauf notiert wurde (mcp.c, coverage_path_freshness). Genau
     * darum meldet sie eine Datei, die seit dem Index geaendert wurde, und
     * genau darum steht sie nicht schon in den Listen, die beim Start geholt
     * wurden.
     *
     * Ein Fehler hier loescht die Notiz, statt eine alte stehenzulassen: eine
     * Frische-Angabe ueber die vorige Datei waere schlimmer als keine.
     */
    useEffect(() => {
        if (project.length === 0 || activePath.length === 0) {
            setOpenCoverage(undefined);
            return;
        }
        coverageTicket.current += 1;
        const ticket = coverageTicket.current;
        let cancelled = false;
        setOpenCoverage(undefined);
        loadPathCoverage(client, project, activePath)
            .then((answer) => {
                if (!cancelled && ticket === coverageTicket.current) {
                    setOpenCoverage(answer);
                }
            })
            .catch(() => {
                if (!cancelled && ticket === coverageTicket.current) {
                    setOpenCoverage(undefined);
                }
            });
        return () => {
            cancelled = true;
        };
    }, [activePath, client, project]);

    // ------------------------------------------------------- Caret-Strecke -

    // Der Caret wandert sofort in die Anzeige (Badge-Puls, data-current) und
    // erst nach der Ruhezeit in die Frage an den Graphen.
    useEffect(() => {
        if (caretLine === undefined) {
            return;
        }
        const timer = window.setTimeout(() => setSettledCaret(caretLine), TWIN_CARET_DEBOUNCE_MS);
        return () => window.clearTimeout(timer);
    }, [caretLine]);

    // Ein Dateiwechsel loescht die alte Lage. Der Caret der neuen Datei meldet
    // sich von selbst, sobald Monaco sein Modell getauscht hat; bis dahin
    // stehenzulassen, was ueber die vorige Datei bekannt war, waere ein Panel,
    // das ueber die falsche Datei spricht.
    useEffect(() => {
        setCaretLine(undefined);
        setSettledCaret(undefined);
        setPointedLine(undefined);
    }, [activePath]);

    useEffect(() => {
        if (project.length === 0 || activePath.length === 0 || settledCaret === undefined) {
            return;
        }
        // Waehrend einer Stepper-Sitzung bleibt das Subjekt stehen. Warum,
        // steht bei `flowPinned`.
        if (flowPinned.current) {
            return;
        }
        let cancelled = false;
        twinTicket.current += 1;
        const ticket = twinTicket.current;
        const current = () => !cancelled && ticket === twinTicket.current;

        (async () => {
            const resolved = await provider.resolveSymbolAt(
                ATLAS_WORKSPACE_ROOT,
                activePath,
                settledCaret,
                { projectName: project },
            );
            if (!current()) {
                return;
            }
            if (resolved.kind !== 'ok') {
                // Jeder der drei Faelle sagt etwas anderes, und keiner davon
                // heisst "dieses Symbol hat keine Fakten".
                setTwinSymbol(undefined);
                setTwinIr(undefined);
                setTwinName(messages.twin.noSymbol);
                if (resolved.kind === 'file-not-indexed') {
                    setTwinStatus('not-indexed');
                    setTwinMessage(TWIN_FILE_NOT_INDEXED);
                    setTwinHint(TWIN_FILE_NOT_INDEXED_HINT);
                } else if (resolved.kind === 'engine-unavailable') {
                    setTwinStatus('failed');
                    setTwinMessage(TWIN_LOAD_FAILED);
                    setTwinHint(resolved.reason);
                } else {
                    setTwinStatus('empty');
                    setTwinMessage(TWIN_EMPTY_MESSAGE);
                    setTwinHint(TWIN_EMPTY_HINT);
                }
                return;
            }

            const symbol = resolved.symbol;
            const unchanged =
                shownSymbol.current?.qualifiedName !== undefined &&
                shownSymbol.current.qualifiedName === symbol.qualifiedName;
            if (unchanged && twinIr !== undefined) {
                // Gleiches Subjekt, neuer Caret: nichts zu holen und nichts zu
                // melden. Auch nur kurz in den Ladezustand zu gehen waere ein
                // Blinken, das behauptet, es passiere etwas.
                return;
            }
            setTwinSymbol(symbol);
            setTwinName(symbol.name);
            setTwinStatus('loading');
            setTwinMessage(TWIN_LOADING);
            setTwinHint('');
            const entry = await irCache.load(symbol);
            if (!current()) {
                return;
            }
            setTwinIr(entry.ir);
            setTwinStatus('ready');
        })().catch((error: unknown) => {
            if (!current()) {
                return;
            }
            setTwinIr(undefined);
            setTwinStatus('failed');
            setTwinMessage(TWIN_LOAD_FAILED);
            setTwinHint(error instanceof Error ? error.message : String(error));
        });

        return () => {
            cancelled = true;
        };
        // `twinIr` steht bewusst nicht in der Liste: es aendert sich als Folge
        // dieses Effekts, und eine Abhaengigkeit darauf waere eine Schleife.
        // Gelesen wird es nur, um zu entscheiden, ob schon etwas dasteht.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [provider, irCache, project, activePath, settledCaret]);

    /**
     * Einem Ziel folgen: das Subjekt wechselt sofort, der Editor kommt nach.
     *
     * Die Fakten kommen aus der Aufloesung in der Zieldatei und nicht aus dem
     * Klickziel. Warum, steht oben im Kopf dieser Datei.
     */
    /**
     * Die stille Anzeige beim Folgen: das Item, dem gefolgt wurde, ist besucht.
     *
     * Vermerkt wird am Symbol, dessen Checkliste die Zeile traegt, und nicht am
     * Ziel: die Verpflichtung "verstehe den Aufruf von validateUser" gehoert
     * demjenigen, der aufruft. Findet sich keine Zeile, die auf dieses Ziel
     * zeigt, wird nichts vermerkt; ein Klick auf etwas, das nicht auf der Liste
     * steht, ist kein abgearbeiteter Punkt.
     */
    const markFollowed = useCallback(
        (target: SymbolRef) => {
            const ir = shownIr.current;
            const owner = ir?.symbol.qualifiedName;
            if (ir === undefined || owner === undefined || project.length === 0) {
                return;
            }
            const item = ir.checklist.find((entry) =>
                entry.target !== undefined
                && ((target.qualifiedName !== undefined
                    && entry.target.qualifiedName === target.qualifiedName)
                    || (target.qualifiedName === undefined && entry.target.uri === target.uri)));
            if (item === undefined) {
                return;
            }
            setMarks(markVisited(store, project, owner, item.id));
        },
        [project, store],
    );

    const followTarget = useCallback(
        (target: SymbolRef) => {
            markFollowed(target);
            const location = twinLocationOf(target);
            const keepReadyTwin = location.path.length > 0
                && keepsReadyTwinForTarget(target, shownSymbol.current, shownIr.current, twinStatus);
            setTwinName(target.name);
            if (!keepReadyTwin) {
                setTwinIr(undefined);
                setTwinSymbol(undefined);
                setTwinStatus('loading');
                setTwinMessage(TWIN_LOADING);
                setTwinHint('');
            }
            if (location.path.length === 0) {
                setTwinStatus('empty');
                setTwinMessage(TWIN_EMPTY_MESSAGE);
                setTwinHint(TWIN_EMPTY_HINT);
                return;
            }
            if (location.path !== activePath) {
                openFile(location.path);
            }
            setReveal((previous) => ({ line: location.line, nonce: (previous?.nonce ?? 0) + 1 }));
        },
        [activePath, markFollowed, openFile, twinStatus],
    );

    const pointRow = useCallback((row: TwinRow | undefined) => {
        setPointedLine(row?.siteLine);
    }, []);

    // ------------------------------------- Flow-Erklaerer und Pseudocode ---

    /**
     * Eine Zeile oeffnen: Datei anfahren, Zeile offenbaren, Zeile anleuchten.
     *
     * Derselbe Weg, den `followTarget` fuer eine Twin-Zeile geht, nur ohne den
     * Subjektwechsel: eine Pseudocode-Zeile zeigt auf eine Stelle im Text und
     * behauptet nicht, dass dort ein anderes Symbol das Thema wird. Was dort
     * steht, sagt der Reader; wer dort steht, sagt die Aufloesung, sobald der
     * Leser die Sitzung beendet.
     */
    const openLine = useCallback(
        (ref: PseudocodeSourceRef) => {
            const path = workspacePathOf(ref.uri);
            if (path.length === 0) {
                return;
            }
            if (path !== activePath) {
                openFile(path);
            }
            setPointedLine(ref.line);
            setReveal((previous) => ({ line: ref.line, nonce: (previous?.nonce ?? 0) + 1 }));
        },
        [activePath, openFile],
    );

    /**
     * Einen Schritt anlaufen: Kasten, STEPS-Liste und Editor bewegen sich
     * zusammen.
     *
     * Der Kasten und die Liste folgen dem Index (das Panel rechnet beides aus
     * demselben Modell), der Editor folgt der Quellangabe der Zeile. Eine
     * Zeile ohne Quellangabe bewegt den Editor nicht, statt irgendwohin zu
     * springen.
     */
    const stepFlow = useCallback(
        (index: number) => {
            const step = flowView?.steps[index];
            if (step === undefined) {
                return;
            }
            setFlowStep(index);
            if (step.line.sourceRef !== undefined) {
                openLine(step.line.sourceRef);
            }
        },
        [flowView, openLine],
    );

    /**
     * Der flow()-Kopf im Twin schaltet den Reiter.
     *
     * Steht der Bereich schon auf `flow`, klappt er zu; steht er woanders oder
     * ist er zu, geht er auf `flow` auf. Das ist dieselbe Bedienung wie vorher
     * (ein Kopf, ein Klick, auf und zu), und sie kostet seit W8 nichts mehr: der
     * Schritt bleibt, wo er war, und ein zweiter Klick zeigt wieder dieselbe
     * Stelle. Bis W8 setzte dieses Zumachen den Stepper zurueck, weil ein
     * Overlay eine Sitzung war; ein Reiter ist keine.
     */
    const toggleFlow = useCallback(() => {
        setExplainOpen((open) => !(open && explainTabRef.current === 'flow'));
        setExplainTab('flow');
    }, []);

    /**
     * Etwas erklaeren lassen: den Bereich aufmachen, auf DIESEM Reiter.
     *
     * Der eine Weg, auf dem jede der fuenf Flaechen aufgeht (AC4). Bis W8 hatte
     * jede ihren eigenen, und drei davon machten dabei die anderen zu, was der
     * Grund war, aus dem der Zustand jedes Mal verlorenging. Hier geht nichts zu:
     * es wird nur umgeschaltet.
     */
    const openExplain = useCallback((tab: ExplainTabId) => {
        setExplainTab(tab);
        setExplainOpen(true);
    }, []);

    /**
     * Den Bereich zuklappen, ohne irgendetwas zu kosten.
     *
     * Escape und der Knopf im Kopf gehen denselben Weg. Was hier NICHT steht,
     * ist der Punkt: kein `setChatTurns([])`, kein `setFlowStep(-1)`, kein
     * `setTour(undefined)`. Zumachen ist nicht loeschen, und das war der
     * Nutzerbefund, aus dem W7c den zweiten Knopf des Chats gebaut hat.
     */
    const collapseExplain = useCallback(() => {
        setExplainOpen(false);
    }, []);

    /*
     * Der Fakten-Block des aktiven Symbols. Rein aus dem, was schon dasteht:
     * die Pseudocode-Ansicht kostet keine einzige weitere Anfrage.
     *
     * Seit W8c sind es ZWEI Dinge, die schon dastehen, und das zweite ist der
     * Punkt: die IR des Symbols, und das geladene Layout der Galaxie. Aus dem
     * Layout liest der Block, was der Index ueber die AUFGERUFENEN Symbole
     * aufgezeichnet hat (was sie erheben koennen, was sie ihrerseits rufen, was
     * sie aus der Umgebung lesen), und schreibt es an die Schrittzeile. Es ist
     * dieselbe Quelle, aus der die Bedeutungssuche ihre Kandidaten nimmt
     * (`localIndex` weiter oben), und aus demselben Grund: sie ist da, sie ist
     * eine Antwort fuer das ganze Projekt, und sie noch einmal zu holen waere
     * ein zweiter Serverweg fuer eine Antwort, die im Speicher liegt. Was das
     * Layout NICHT hergibt, wird nicht nachgeladen und nicht erfunden; es steht
     * als `enrichmentAvailable.missing` im Dokument (src/pseudocode/
     * step-insights.ts).
     *
     * Ein Subjektwechsel wirft die Umformulierung weg. Sie gehoert zu genau
     * einem Block; sie ueber einen Symbolwechsel hinweg stehenzulassen waere
     * eine Umschreibung der Fakten eines anderen Symbols.
     */
    useEffect(() => {
        const ir = twinIr;
        setRefined(undefined);
        setRefineState('idle');
        setRefineMessage('');
        if (ir === undefined) {
            setPseudocode(undefined);
            return;
        }
        setPseudocode(buildPseudocode(
            { kind: 'symbol', label: ir.symbol.name },
            { irs: [ir], ...(layout === undefined ? {} : { graph: layout }) },
        ));
    }, [layout, twinIr]);

    // Was die Datei um das Symbol herum hereinholt. Zwei Lesungen, eine davon
    // ist der Text, den der Reader ohnehin schon geladen hat; siehe
    // src/pseudocode/imports-source.ts.
    useEffect(() => {
        const ir = twinIr;
        if (ir === undefined || project.length === 0 || activePath.length === 0) {
            setImports(undefined);
            return;
        }
        importsTicket.current += 1;
        const ticket = importsTicket.current;
        let cancelled = false;
        void fileImportsFor(provider, ATLAS_WORKSPACE_ROOT, {
            filePath: activePath,
            source: documentRef.current?.path === activePath ? documentRef.current.source : undefined,
            opts: { projectName: project, generation: 1 },
        })
            .then((answer) => {
                if (cancelled || ticket !== importsTicket.current) {
                    return;
                }
                setImports(buildImportsGroup({ imports: answer, irs: [ir], uri: ir.symbol.uri }));
            })
            .catch(() => {
                if (!cancelled && ticket === importsTicket.current) {
                    // Eine fehlgeschlagene Import-Lesung ist kein Grund, die
                    // Fakten zu verlieren. Der Data-Block steht ohne sie.
                    setImports(undefined);
                }
            });
        return () => {
            cancelled = true;
        };
    }, [activePath, document, project, provider, twinIr]);

    /*
     * Der Walk hinter dem Kasten.
     *
     * Er laeuft, sobald der Kasten offen ist und ein Subjekt dasteht: das
     * Design zeigt den Kasten sichtbar, also muss er sich selbst fuellen und
     * nicht auf einen zweiten Klick warten. Die IRs der erreichten Symbole
     * kommen aus dem IR-Cache; ein Symbol, das der Leser eben erst besucht hat,
     * kostet dabei keine Anfrage.
     */
    /*
     * Ein Subjektwechsel wirft den Walk weg, und zwar HIER und nicht im Lader.
     *
     * Bis W8 tat es der Lader mit, weil er ohnehin bei jedem Aufschlagen lief.
     * Seit der Erklaerer ein Reiter ist, laeuft der Lader nicht mehr bei jedem
     * Aufschlagen (sonst waere der Schritt bei jedem Reiterwechsel weg), und
     * damit braucht das Wegwerfen eine eigene Stelle. Ohne sie stuende nach
     * einem Symbolwechsel das Bild des vorigen Symbols da, und das waere die
     * schlimmste Sorte Fehler dieser Oberflaeche: eine richtige Antwort auf eine
     * andere Frage.
     */
    useEffect(() => {
        flowLoadedFor.current = '';
        setFlowView(undefined);
        setFlowStep(-1);
        setFlowMessage(twinKey.length === 0 ? FLOW_UNAVAILABLE : FLOW_LOADING);
    }, [twinKey]);

    useEffect(() => {
        const symbol = twinSymbol;
        if (!flowOpen || symbol === undefined || project.length === 0) {
            return;
        }
        // Dasselbe Symbol wird nicht zweimal geholt: sonst kostete jeder
        // Reiterwechsel den Schritt, an dem der Leser steht.
        if (flowLoadedFor.current === twinKey) {
            return;
        }
        flowLoadedFor.current = twinKey;
        flowTicket.current += 1;
        const ticket = flowTicket.current;
        let cancelled = false;
        const current = () => !cancelled && ticket === flowTicket.current;
        setFlowView(undefined);
        setFlowStep(-1);
        setFlowMessage(FLOW_LOADING);

        (async () => {
            const walk = await getClosure(provider, ATLAS_WORKSPACE_ROOT, symbol, {
                projectName: project,
                generation: 1,
                depth: FLOW_CLOSURE_DEPTH,
                cap: FLOW_CLOSURE_CAP,
            });
            if (!current()) {
                return;
            }
            const closure = closureDocumentOf(walk);
            // Was ankommt, kommt an. Ein Symbol, dessen IR nicht zu holen war,
            // wird vom Builder als Gruppe ohne Zeilen gezeigt und im ehrlichen
            // Block genannt; es wird nicht stillschweigend weggelassen.
            const irs: SemanticIR[] = [];
            for (const reached of closure.symbols) {
                const entry = await irCache.load(reached).catch(() => undefined);
                if (!current()) {
                    return;
                }
                if (entry !== undefined) {
                    irs.push(entry.ir);
                }
            }
            setFlowView(buildFlowView({
                closure,
                irs,
                depth: FLOW_CLOSURE_DEPTH,
                cap: FLOW_CLOSURE_CAP,
            }));
        })().catch(() => {
            if (current()) {
                // Der Vermerk faellt zurueck, damit ein zweiter Versuch moeglich
                // ist. Ein gemerkter Fehlschlag waere ein Symbol, das dieses
                // Fenster nie wieder zeichnet.
                flowLoadedFor.current = '';
                setFlowView(undefined);
                setFlowMessage(FLOW_UNAVAILABLE);
            }
        });

        return () => {
            cancelled = true;
        };
    }, [flowOpen, irCache, project, provider, twinKey, twinSymbol]);

    // ------------------------------------------------------- Galaxie -------

    const onLayout = useCallback((loaded: GraphData) => {
        setLayout(loaded);
    }, []);

    /**
     * Ein Klick in die Galaxie.
     *
     * Er geht durch denselben `followTarget` wie ein Klick im Twin: Datei
     * oeffnen, Zeile anfahren, und was dort steht, sagt die Aufloesung. Ein
     * Knoten ohne Datei kommt hier gar nicht an; das Panel sagt selbst, warum.
     */
    const openGalaxyNode = useCallback(
        (node: GraphNode) => {
            const target = targetRefOfNode(node);
            if (target === undefined) {
                return;
            }
            followTarget(target);
        },
        [followTarget],
    );

    // ------------------------------------------------- Bedeutungssuche -----

    /**
     * Wie oft der Index ein Symbol erreicht, aus dem geladenen Layout.
     *
     * Die flache Suchform des Servers traegt keine solche Spalte
     * (UPSTREAM-ASKS.md, Ask 5), das Layout traegt sie an jedem Knoten. Solange
     * keines geladen ist, hat kein Kandidat eine Zahl, und das entscheidet nur
     * bei Gleichstand.
     */
    const fanIn = useMemo(() => {
        const readings = new Map<string, number>();
        for (const node of layout?.nodes ?? []) {
            const qualifiedName = node.qualified_name;
            if (qualifiedName !== undefined && node.in_calls !== undefined) {
                readings.set(qualifiedName, node.in_calls);
            }
        }
        return readings;
    }, [layout]);

    const fanInOf = useCallback(
        (hit: { qualifiedName?: string }) => fanIn.get(hit.qualifiedName ?? '') ?? 0,
        [fanIn],
    );

    /**
     * Was dieses Fenster schon weiss, als Kandidatenmenge fuer die Suche.
     *
     * Zwei Quellen, beide schon im Speicher, beide ohne einen Serverweg: die
     * Knoten der Galaxie (Name, qualifizierter Name, Datei, Zeile) und die
     * Dateien des Baums. Warum daraus Vorschlaege werden duerfen und was sie
     * NICHT behaupten, steht in src/search/local-suggestions.ts.
     *
     * Gemerkt, weil daran der Sofort-Vorschlag zu jedem Tastendruck haengt: die
     * Menge neu zu bauen, sooft jemand einen Buchstaben tippt, waere die eine
     * Rechnung, die diesen Weg wieder langsam machen wuerde.
     */
    const localIndex = useMemo<LocalIndex>(() => {
        const nodes = layout?.nodes ?? [];
        if (nodes.length === 0 && rows.length === 0) {
            return EMPTY_LOCAL_INDEX;
        }
        return {
            symbols: nodes
                .filter((node) => node.name.length > 0)
                .map((node) => ({
                    name: node.name,
                    qualifiedName: node.qualified_name,
                    kind: symbolKindOf(node.label),
                    filePath: node.file_path,
                    line: node.start_line,
                })),
            files: rows.filter((row) => row.kind === 'file').map((row) => row.path),
        };
    }, [layout, rows]);

    const localPool = useMemo(() => localCandidates(localIndex), [localIndex]);

    /*
     * Woran die Beispiele der Kommandozeile haengen (AC6c).
     *
     * Ein Name aus dem geladenen Index und keine feste Liste: ein Beispiel mit
     * einem Symbol, das dieses Projekt nicht hat, waere eine Einladung ins
     * Leere. Gerechnet aus `localIndex.symbols`, also aus den Knoten des
     * Layouts, weil die den Namen UND die Art tragen; die Dateien des Baums
     * bleiben aussen vor, denn eine Datei ruft nichts, und zwei der drei
     * Beispiele fragen nach Aufrufen. Steht nichts, gibt es keine Beispiele.
     */
    const exampleSymbol = useMemo(
        () => exampleSymbolOf(localIndex.symbols.map((hit) => ({ name: hit.name, kind: hit.kind }))),
        [localIndex],
    );
    const commandExamples = useMemo(
        () => (exampleSymbol === undefined ? [] : commandExamplesFor(exampleSymbol)),
        [exampleSymbol],
    );

    /**
     * Die Zeilen, die sofort dastehen koennen, ohne dass jemand gefragt wird.
     *
     * Erst der Praefix-Cache (die vorige Antwort des Index, neu gerankt), dann
     * das schon Geladene, und beides in EINEN Rang: zwei Listen zu mischen
     * hiesse, eine zweite Ordnung neben der einen zu haben, die dieses Produkt
     * hat.
     */
    const instantHits = useCallback(
        (query: string): RankedHit[] => {
            const cached = prefixCache.current;
            const extendsCached =
                cached !== undefined
                && cached.complete
                && query.length > cached.query.length
                && query.startsWith(cached.query);
            if (extendsCached) {
                searchStats.current.prefixCacheHits += 1;
            }
            const pool = extendsCached
                ? [...(cached?.candidates ?? []), ...localPool]
                : localPool;
            return pool.length === 0
                ? noHits
                : rankHits(pool, query, fanInOf).slice(0, MAX_SEARCH_ROWS);
        },
        [fanInOf, localPool],
    );

    /*
     * Der Sofort-Vorschlag als Ref, und nicht als Abhaengigkeit des Effekts.
     *
     * Er haengt am Baum und an der Galaxie, und beide wachsen waehrend des
     * Ladens weiter. Stuende er in der Abhaengigkeitsliste, wuerde jede
     * nachgeladene Baumebene die laufende Suche zuruecksetzen und noch einmal
     * fragen. Gelesen wird er beim Tastendruck, und dann ist er auf dem Stand
     * dieses Bildes.
     */
    const instantRef = useRef(instantHits);
    instantRef.current = instantHits;

    /*
     * Die Suche, in zwei Schritten statt in einem.
     *
     * Nutzerbefund vom 2026-08-29: die Vorschlaege erscheinen zu langsam. Bis
     * dahin lief alles hintereinander: tippen, 200 ms Entprellung, ein Weg zum
     * Server je Wort, dann die erste Zeile. Der Bildschirm blieb in dieser Zeit
     * leer, obwohl die Antwort auf einen guten Teil der Frage schon im Browser
     * lag.
     *
     * Jetzt passieren zwei Dinge, und die Reihenfolge ist die Aussage:
     *
     *  1. **Sofort**, im selben Bild wie der Tastendruck, stehen die
     *     vorlaeufigen Zeilen da: aus der vorigen Antwort des Index, wenn das
     *     Wort nur verlaengert wurde, sonst aus Baum und Galaxie. Sie tragen
     *     ihre Marke, damit niemand sie fuer die Antwort haelt.
     *  2. **Nach der Entprellung** geht die Frage an den Index, mit einem
     *     Abbruch am Hals. Ihre Antwort ersetzt die vorlaeufigen Zeilen an Ort
     *     und Stelle; der Kasten hat feste Hoehe, also springt dabei nichts.
     *
     * Die Ticketpruefung bleibt und ist die eigentliche Zusicherung: eine
     * Antwort, die zu spaet kommt, wird verworfen und nicht angezeigt. Der
     * Abbruch spart zusaetzlich den Weg, den niemand mehr liest.
     */
    useEffect(() => {
        const query = command.trim();
        if (project.length === 0 || !isSearchable(query, SEARCH_MIN_QUERY)) {
            searchAbort.current?.abort();
            searchAbort.current = undefined;
            setHits(noHits);
            setAnsweredQuery('');
            setHitSource('index');
            setSearchStatus('ready');
            setSearchMessage('');
            return;
        }

        // Schritt 1: was ohne Serverweg zu haben ist, und zwar jetzt.
        const instant = instantRef.current(query);
        setHits(instant);
        setSelectedHit(0);
        setAnsweredQuery(query);
        setHitSource('loaded');
        setSearchStatus('searching');
        setSearchMessage('');
        if (instant.length > 0) {
            searchStats.current.localFirst += 1;
        }

        // Schritt 2: die Frage an den Index, sobald die Zeile still steht.
        const timer = window.setTimeout(() => {
            searchAbort.current?.abort();
            const controller = new AbortController();
            searchAbort.current = controller;
            searchTicket.current += 1;
            const ticket = searchTicket.current;
            searchStats.current.serverRequests += 1;
            const started = performance.now();
            searchByMeaning(
                provider,
                ATLAS_WORKSPACE_ROOT,
                query,
                { projectName: project, signal: controller.signal },
                fanInOf,
            )
                .then((answer) => {
                    const elapsed = Math.round(performance.now() - started);
                    if (answer.aborted) {
                        searchStats.current.abortedRequests += 1;
                        return;
                    }
                    if (ticket !== searchTicket.current) {
                        searchStats.current.staleDropped += 1;
                        return;
                    }
                    if (ticket < searchStats.current.appliedTicket) {
                        // Kann nicht passieren, solange die Zeile darueber
                        // greift. Steht trotzdem hier, weil eine Zusicherung,
                        // die niemand nachrechnet, keine ist.
                        searchStats.current.staleAnswerWins = true;
                        return;
                    }
                    searchStats.current.appliedTicket = ticket;
                    searchStats.current.serverRoundtripMs = elapsed;
                    searchStats.current.roundtrips.push(elapsed);
                    prefixCache.current = {
                        query,
                        candidates: answer.candidates,
                        complete: answer.complete,
                    };
                    setHits(answer.hits);
                    setSelectedHit(0);
                    setAnsweredQuery(query);
                    setHitSource('index');
                    setSearchStatus('ready');
                    setSearchMessage('');
                })
                .catch((error: unknown) => {
                    if (controller.signal.aborted) {
                        searchStats.current.abortedRequests += 1;
                        return;
                    }
                    if (ticket !== searchTicket.current) {
                        searchStats.current.staleDropped += 1;
                        return;
                    }
                    setHits(noHits);
                    setAnsweredQuery(query);
                    setHitSource('index');
                    setSearchStatus('failed');
                    setSearchMessage(
                        `${SEARCH_FAILED}: ${error instanceof Error ? error.message : String(error)}`,
                    );
                });
        }, SEARCH_DEBOUNCE_MS);
        return () => window.clearTimeout(timer);
    }, [command, project, provider, fanInOf]);

    const closeSearch = useCallback(() => {
        searchAbort.current?.abort();
        searchAbort.current = undefined;
        setCommand('');
        setHits(noHits);
        setAnsweredQuery('');
        setHitSource('index');
        setSearchStatus('ready');
        setSearchMessage('');
    }, []);

    /**
     * Einen Treffer waehlen: Datei oeffnen, Twin folgen, Galaxie hinterher.
     *
     * Die Kamerafahrt steht hier nicht: sie haengt am Twin-Subjekt, und das
     * wechselt, sobald die Aufloesung in der Zieldatei da ist. Sie von hier aus
     * zusaetzlich anzustossen hiesse, zweimal auf dasselbe zu zeigen und beim
     * zweiten Mal auf den Treffer statt auf das aufgeloeste Symbol.
     */
    const chooseHit = useCallback(
        (index: number) => {
            const chosen = hits[index];
            if (chosen === undefined) {
                return;
            }
            const target = twinTargetOf({
                name: chosen.hit.name,
                qualifiedName: chosen.hit.qualifiedName,
                kind: chosen.hit.kind,
                filePath: chosen.hit.filePath,
                startLine: chosen.hit.line,
            });
            if (target === undefined) {
                setSearchMessage(SEARCH_NO_FILE);
                return;
            }
            closeSearch();
            followTarget(target);
        },
        [closeSearch, followTarget, hits],
    );

    // ------------------------------- Warum, Fuehrungen und die Checkliste ---

    // Was dieser Browser ueber dieses Projekt weiss, beim Projektwechsel neu
    // gelesen. Beides ist an das Projekt gebunden und an nichts sonst.
    useEffect(() => {
        setWhyAnswer(readWhyAnswer(store, project));
        setMarks(readUnderstanding(store, project));
        setWhyReopened(false);
        setTour(undefined);
        setTourStep(0);
        setTourMessage('');
        setWalk(undefined);
    }, [project, store]);

    /**
     * Wann die Frage dasteht.
     *
     * Von selbst nur im leeren Editorbereich und nur, solange dieser Browser
     * fuer dieses Projekt keine Antwort kennt. Waehrend einer laufenden
     * Fuehrung nie: die Fuehrung ist die Antwort, die gerade ausgefuehrt wird.
     * Von Hand ueber das Menue jederzeit, und dann auch ueber einer offenen
     * Datei, weil der Wiederaufruf sonst genau dann nicht ginge, wenn jemand
     * mitten im Lesen den Modus wechseln will.
     */
    const whyVisible =
        tour === undefined
        && !entryOpen
        && !bugOpen
        && !impactOpen
        && project.length > 0
        && !whyDismissed
        && (whyReopened || (!whyAnswer.asked && activePath.length === 0));

    /*
     * Der Stand fuer den Tastatur-Griff, bei jedem Bild neu geschrieben.
     *
     * Hier und nicht weiter unten, weil `whyVisible` hier entsteht: die Liste
     * soll an der Stelle stehen, an der ihr letzter Bestandteil bekannt ist,
     * damit niemand sie fuer vollstaendig haelt, waehrend eine Flaeche fehlt.
     */
    keyboardGuardRef.current = {
        /*
         * Nur die zwei Flaechen, die die Tastatur wirklich fuehren: die Hilfe
         * nimmt Escape, der Einstiegsdialog hat sein eigenes Suchfeld.
         *
         * Der Erklaeren-Bereich stand hier bis W8 mit drin, solange er ein
         * Overlay ueber dem Editor war. Als ZONE gehoert er nicht mehr dazu: er
         * liegt neben dem Reader, die Kommandozeile ist sichtbar, und wer bei
         * offenem Flow lostippt, meint die Zeile, die er sieht. Genau diese
         * Lesart hatten die Frage nach dem Warum, der Assistent und die
         * Aenderungsansicht schon vorher, und aus demselben Grund.
         */
        /*
         * Seit W10 sind es drei: das Einstellungen-Panel fuehrt die Tastatur
         * genauso wie die Hilfe (Escape schliesst es) und hat ausserdem ein
         * eigenes Eingabefeld. Ohne diesen Eintrag fielen Buchstaben, die
         * jemand vor dem offenen Panel tippt, in die Kommandozeile dahinter.
         */
        overlayOpen: helpOpen || entryOpen || settingsOpen || projectsOpen,
        walkRunning: tour !== undefined,
    };

    const closeTour = useCallback(() => {
        setTour(undefined);
        setTourStep(0);
        // Mit der Fuehrung geht auch der Walk: das Graph-Panel wuerde sonst die
        // Hierarchie einer Frage zeigen, die der Leser beendet hat.
        setWalk(undefined);
    }, []);

    /** Die Fuehrung durch das ganze Projekt, im Browser erzeugt. */
    const startProjectTour = useCallback(async () => {
        setTourMessage('');
        // Die Fuehrung durchs Projekt hat keinen gewaehlten Einstiegspunkt und
        // damit keinen Subgraphen. Ein stehengebliebener Walk waere die
        // Hierarchie der vorigen Frage unter der neuen Fuehrung.
        setWalk(undefined);
        try {
            const active = await generateProjectTour(provider, ATLAS_WORKSPACE_ROOT, {
                projectName: project,
                generation: 1,
            });
            setTour(active);
            setTourStep(0);
            // Eine Fuehrung, die laeuft und deren Karte eingeklappt bliebe, waere
            // eine Fuehrung ohne Fuehrer. Das ist der Fall aus AC4, in dem der
            // Bereich von selbst aufgeht: der Leser hat danach gefragt.
            openExplain('walk');
        } catch (error) {
            setTour(undefined);
            setTourMessage(`${TOUR_FAILED}: ${error instanceof Error ? error.message : String(error)}`);
        }
    }, [openExplain, project, provider]);

    /**
     * Der Vorwaerts-Walk ab einem gewaehlten Symbol.
     *
     * Zuerst wird die Wahl aufgeloest und erst danach gelaufen. Eine Suchzeile
     * und ein Eintrag der Zusammenfassung nennen eine Stelle, kein Symbol; der
     * Walk fragt den Graphen nach den Aufrufen eines qualifizierten Namens, und
     * die Wurzel muss deshalb das sein, was der Index an dieser Stelle kennt.
     * Scheitert die Aufloesung, wird mit dem gewaehlten Ziel gelaufen, damit
     * eine Wahl nicht folgenlos bleibt; was dabei fehlt, sagt der Walk selbst.
     */
    const startEntryWalk = useCallback(
        async (chosen: { name: string; qualifiedName?: string; kind: SymbolRef['kind']; filePath: string; line?: number }) => {
            setEntryOpen(false);
            setTourMessage('');
            const fallback = twinTargetOf({
                name: chosen.name,
                qualifiedName: chosen.qualifiedName,
                kind: chosen.kind,
                filePath: chosen.filePath,
                startLine: chosen.line,
            });
            try {
                let root = fallback;
                if (chosen.line !== undefined) {
                    const resolved = await provider.resolveSymbolAt(
                        ATLAS_WORKSPACE_ROOT,
                        chosen.filePath,
                        chosen.line,
                        { projectName: project },
                    );
                    if (resolved.kind === 'ok') {
                        root = resolved.symbol;
                    }
                }
                if (root === undefined) {
                    setTourMessage(messages.tour.failedNoFile(chosen.name));
                    return;
                }
                const closure = await getClosure(provider, ATLAS_WORKSPACE_ROOT, root, {
                    projectName: project,
                    generation: 1,
                    ...(closureBounds.depth === undefined ? {} : { depth: closureBounds.depth }),
                    ...(closureBounds.cap === undefined ? {} : { cap: closureBounds.cap }),
                });
                setWalk(closure);
                setTour(entryWalkTour(closure));
                setTourStep(0);
                openExplain('walk');
            } catch (error) {
                setTour(undefined);
                setWalk(undefined);
                setTourMessage(`${TOUR_FAILED}: ${error instanceof Error ? error.message : String(error)}`);
            }
        },
        [closureBounds, openExplain, project, provider],
    );

    const chooseIntent = useCallback(
        (intent: WhyIntent) => {
            setProfile(profileFor(intent, TWIN_PROFILE));
            setOverrides({});
            setWhyAnswer(recordWhyAnswer(store, project, intent));
            setWhyReopened(false);
            if (intent === 'understand') {
                void startProjectTour();
            } else if (intent === 'entry') {
                setEntryOpen(true);
            } else if (intent === 'bug') {
                openExplain('bug');
            } else if (intent === 'change') {
                openExplain('change');
            }
        },
        [openExplain, project, startProjectTour, store],
    );

    const declineWhy = useCallback(() => {
        // Der Knopf merkt sich nur die Antwort. Kein Profilwechsel: wer nichts
        // gesagt hat, hat nichts ueber seine Lesetiefe gesagt.
        setWhyAnswer(recordWhyAnswer(store, project));
        setWhyReopened(false);
        setWhyDismissed(true);
    }, [project, store]);

    /**
     * Die Frage weglegen, weil der Leser angefangen hat zu lesen.
     *
     * Anders als der Knopf darunter schreibt das NICHTS in den Speicher dieses
     * Browsers: eine Datei zu oeffnen ist keine Antwort auf "warum bist du
     * hier", sondern der Beweis, dass die Frage gerade nicht dran ist. Beim
     * naechsten Projekt oder nach dem Menuepunkt steht sie wieder da.
     */
    const dismissWhy = useCallback(() => {
        setWhyReopened(false);
        setWhyDismissed(true);
    }, []);

    // Ein Schritt bewegt den Reader. Die beiden Rueckrufe liegen in Refs, damit
    // dieser Effekt nur an Schritt und Fuehrung haengt: `followTarget` wechselt
    // seine Identitaet, sobald die geoeffnete Datei wechselt, was genau die
    // Folge dieses Effekts ist, und eine Abhaengigkeit darauf waere eine
    // Schleife.
    const followRef = useRef(followTarget);
    followRef.current = followTarget;
    const openRef = useRef(openFile);
    openRef.current = openFile;

    useEffect(() => {
        const step = tour?.document.steps[tourStep];
        if (step === undefined) {
            return;
        }
        const primary = step.primary;
        if (primary.kind === 'symbol') {
            const target = twinTargetOf({
                name: primary.name,
                qualifiedName: primary.qualifiedName,
                kind: primary.symbolKind,
                filePath: primary.filePath,
                startLine: primary.line,
            });
            if (target !== undefined) {
                followRef.current(target);
                return;
            }
        }
        openRef.current(primary.filePath);
    }, [tour, tourStep]);

    /**
     * Ein betretener Schritt ist ein besuchtes Item.
     *
     * Vermerkt wird erst, wenn die IR des Schritt-Symbols da ist: vorher gibt es
     * keine Checkliste, und ein Vermerk auf eine Id, die dieses Symbol gar nicht
     * hat, waere ein Haken an nichts. Welches Item, entscheidet
     * `markableItemId` aus dem portierten tour-model: das erste core-logic-Item,
     * so wie im Referenzprojekt.
     */
    useEffect(() => {
        if (tour === undefined || twinIr === undefined || project.length === 0) {
            return;
        }
        const owner = twinIr.symbol.qualifiedName;
        if (owner === undefined) {
            return;
        }
        const state = understandingOf(readUnderstanding(store, project), owner, twinIr.checklist);
        const item = markableItemId(state.items);
        if (item === undefined) {
            return;
        }
        setMarks(markVisited(store, project, owner, item));
    }, [tour, twinIr, project, store]);

    /** Die Zusammenfassung, sobald der Einstiegsdialog sie braucht. */
    useEffect(() => {
        if (!entryOpen || project.length === 0 || overview !== undefined) {
            return;
        }
        let cancelled = false;
        provider
            .architectureOverview(ATLAS_WORKSPACE_ROOT, { projectName: project, generation: 1 })
            .then((loaded) => {
                if (!cancelled) {
                    setOverview(loaded);
                }
            })
            .catch((error: unknown) => {
                if (!cancelled) {
                    setEntryMessage(messages.entry.overviewFailed(
                        error instanceof Error ? error.message : String(error),
                    ));
                }
            });
        return () => {
            cancelled = true;
        };
    }, [entryOpen, overview, project, provider]);

    // Die Suche des Dialogs ist dieselbe Suche wie in der Kommandozeile, mit
    // derselben Entprellung. Zwei Rangfolgen fuer dasselbe Wort waeren zwei
    // Antworten auf eine Frage.
    useEffect(() => {
        const query = entryQuery.trim();
        if (!entryOpen || project.length === 0 || !isSearchable(query, SEARCH_MIN_QUERY)) {
            setEntryHits(noHits);
            setEntryStatus('idle');
            return;
        }
        setEntryStatus('searching');
        const timer = window.setTimeout(() => {
            entryTicket.current += 1;
            const ticket = entryTicket.current;
            findByMeaning(provider, ATLAS_WORKSPACE_ROOT, query, { projectName: project }, fanInOf)
                .then((ranked) => {
                    if (ticket !== entryTicket.current) {
                        return;
                    }
                    setEntryHits(ranked);
                    setEntryStatus('ready');
                    setEntryMessage(ranked.length === 0 ? messages.entry.noAnswer(query) : '');
                })
                .catch((error: unknown) => {
                    if (ticket !== entryTicket.current) {
                        return;
                    }
                    setEntryHits(noHits);
                    setEntryStatus('failed');
                    setEntryMessage(
                        `${SEARCH_FAILED}: ${error instanceof Error ? error.message : String(error)}`,
                    );
                });
        }, SEARCH_DEBOUNCE_MS);
        return () => window.clearTimeout(timer);
    }, [entryOpen, entryQuery, fanInOf, project, provider]);

    const offered = useMemo(() => entryRows(overview), [overview]);
    const entryHitRows = useMemo(() => searchRows(entryHits), [entryHits]);

    const chooseFlagged = useCallback(
        (key: string) => {
            const row = offered.rows.find((entry) => entry.key === key);
            const target = row?.target;
            if (target?.filePath === undefined) {
                return;
            }
            void startEntryWalk({
                name: target.name,
                qualifiedName: target.qualifiedName,
                kind: target.kind,
                filePath: target.filePath,
                line: target.startLine,
            });
        },
        [offered, startEntryWalk],
    );

    const chooseEntryHit = useCallback(
        (index: number) => {
            const chosen = entryHits[index];
            if (chosen === undefined) {
                return;
            }
            if (chosen.hit.filePath === undefined || chosen.hit.filePath.length === 0) {
                setEntryMessage(SEARCH_NO_FILE);
                return;
            }
            void startEntryWalk({
                name: chosen.hit.name,
                qualifiedName: chosen.hit.qualifiedName,
                kind: chosen.hit.kind,
                filePath: chosen.hit.filePath,
                line: chosen.hit.line,
            });
        },
        [entryHits, startEntryWalk],
    );

    // ------------------------- Der BUG-Assistent: Ziel, Lesung, Hop-Klick ---

    /**
     * Das Subjekt, das der Assistent uebernimmt.
     *
     * Das des Twins, und nur bei einem echten Symbolwechsel: ein Assistent, der
     * sich das erste Symbol merkt, das er gesehen hat, vergliche noch Pfade in
     * eine Funktion, die der Leser vor zehn Minuten verlassen hat, und nichts auf
     * dem Bildschirm sagte es. Umgekehrt darf ein Caret, der innerhalb desselben
     * Symbols wandert, die Lesung nicht neu anstossen.
     */
    useEffect(() => {
        if (!bugOpen) {
            return;
        }
        const next: BugPathNode | undefined =
            twinSymbol === undefined
                ? undefined
                : {
                    name: twinSymbol.name,
                    qualifiedName: twinSymbol.qualifiedName,
                    filePath: workspacePathOf(twinSymbol.uri),
                };
        setBugTarget((current) =>
            current?.name === next?.name && current?.qualifiedName === next?.qualifiedName
                ? current
                : next);
    }, [bugOpen, twinSymbol]);

    useEffect(() => {
        if (!bugOpen || project.length === 0 || bugTarget === undefined) {
            return;
        }
        bugTicket.current += 1;
        const ticket = bugTicket.current;
        setBugStatus('loading');
        setBugMessage('');
        bugPaths(client, api, bugTarget, { project })
            .then((paths) => {
                if (ticket !== bugTicket.current) {
                    return;
                }
                setBugPathsDto(paths);
                setBugStatus('ready');
            })
            .catch((error: unknown) => {
                if (ticket !== bugTicket.current) {
                    return;
                }
                setBugPathsDto(undefined);
                setBugStatus('failed');
                setBugMessage(error instanceof Error ? error.message : String(error));
            });
    }, [api, bugOpen, bugTarget, client, project]);

    /**
     * Ein Klick auf einen Hop.
     *
     * Der Name wird jetzt aufgeloest und nicht vorher: was in einer Aufzeichnung
     * steht, hat irgendwessen Rekorder geschrieben, und ein gespeicherter
     * SymbolRef ohne nodeId waere die Behauptung, dieses Symbol sei nicht
     * indiziert. Loest der Index nichts auf, wird nichts geoeffnet und der
     * Assistent sagt es.
     */
    const openHop = useCallback(
        (hop: BugPathNode) => {
            setBugMessage('');
            void resolveHop(provider, ATLAS_WORKSPACE_ROOT, hop, { projectName: project })
                .then((symbol) => {
                    if (symbol === undefined) {
                        setBugMessage(messages.wizard.unresolvedHop(hop.name));
                        return;
                    }
                    followRef.current(symbol);
                })
                .catch((error: unknown) => {
                    setBugMessage(error instanceof Error ? error.message : String(error));
                });
        },
        [project, provider],
    );

    // ----------------------------- Die Aenderungsansicht: Modus, Ref, Lesung -

    const chooseImpactMode = useCallback((mode: ImpactMode) => {
        setImpactMode(mode);
        setRefError('');
        if (mode === 'worktree') {
            setAppliedRef(undefined);
        }
    }, []);

    /**
     * Der Knopf neben dem Ref-Feld.
     *
     * Die Form wird hier geprueft und der Aufruf unterbleibt, wenn sie nicht
     * stimmt. Warum nicht die Engine gefragt wird, steht bei `refRejection`: sie
     * nimmt ein unbekanntes `since` kommentarlos an und antwortet ueber den
     * Arbeitsbaum, also waere ein Tippfehler eine plausible Antwort auf eine
     * andere Frage.
     */
    const applyRef = useCallback(() => {
        const value = refDraft.trim();
        const rejection = refRejection(value);
        if (rejection !== undefined) {
            setRefError(impactRefRejected(value, rejection));
            return;
        }
        setRefError('');
        setAppliedRef(value);
    }, [refDraft]);

    useEffect(() => {
        if (!impactOpen || project.length === 0) {
            return;
        }
        impactTicket.current += 1;
        const ticket = impactTicket.current;
        setImpactStatus('loading');
        setImpactMessage('');
        readImpact(provider, ATLAS_WORKSPACE_ROOT, {
            projectName: project,
            generation: 1,
            ...(appliedRef === undefined ? {} : { sinceRef: appliedRef }),
            // Der Routen-Scan liest den Quelltext ueber denselben Weg wie der
            // Reader. Einen zweiten Weg zu einer Datei gaebe es hier nicht, und
            // ein zweiter waere eine zweite Stelle, an der eine Kappung
            // interpretiert wird.
            readSource: async (filePath) => {
                const loaded = await loadFileDocument(client, project, filePath).catch(() => undefined);
                return loaded === undefined
                    ? undefined
                    : { source: loaded.source, truncated: loaded.truncated };
            },
        })
            .then((reading) => {
                if (ticket !== impactTicket.current) {
                    return;
                }
                setImpactModel(reading.model);
                setImpactRouteNote(reading.routeNote);
                setImpactStatus('ready');
            })
            .catch((error: unknown) => {
                if (ticket !== impactTicket.current) {
                    return;
                }
                setImpactModel(undefined);
                setImpactStatus('failed');
                setImpactMessage(error instanceof Error ? error.message : String(error));
            });
    }, [appliedRef, client, impactOpen, project, provider]);

    /** Eine Zeile der Aenderungsansicht oeffnen: derselbe Weg wie jeder Klick. */
    const openImpactRow = useCallback(
        (target: ImpactTarget) => {
            const ref = twinTargetOf({
                name: target.name,
                qualifiedName: target.qualifiedName,
                kind: 'unknown',
                filePath: target.filePath,
                startLine: target.line,
            });
            if (ref === undefined) {
                return;
            }
            followRef.current(ref);
        },
        [],
    );

    // ------------------------------------------ Das lokale Modell (W5a) ----

    /*
     * Die Praeferenz dieses Browsers, beim Projektwechsel neu gelesen, so wie
     * die Antwort auf die Warum-Frage und die Vermerke. Sie haengt am Projekt
     * und an nichts sonst.
     */
    useEffect(() => {
        setLlmPreferenceOn(readLlmPreference(store, project).on);
        setLlmPolicy(undefined);
        setLlmProbe(EMPTY_SIDECAR_READING);
        // Die Modellwahl und die Anzeige haengen am selben Projekt und werden
        // deshalb hier mitgelesen. Beides ist pro Projekt gefuehrt, wie die
        // Zonenmasse und die Vermerke: ein anderes Repository ist eine andere
        // Frage und darf eine andere Antwort haben.
        setSelectedModel(readModelPreference(store, project).id);
        setDisplay(loadDisplaySettings(store, project));
    }, [project, store]);

    /*
     * Die committete Policy des ZIELprojekts, einmal je Projekt.
     *
     * Sie wird gelesen, sobald ein Projekt dasteht, und nicht erst, wenn jemand
     * den Schalter anfasst. Der Grund ist AC4 selbst: der Schalter soll
     * WIRKUNGSLOS sein und das auch sagen, bevor ihn jemand drueckt. Eine
     * Policy, die erst beim Einschalten gelesen wird, waere eine, die man einmal
     * umgehen kann.
     *
     * Das ist eine Anfrage an den Graph-Server, nicht an den Sidecar: sie geht
     * ueber denselben Weg wie jede Datei dieses Frontends und sagt ueber 4141
     * nichts.
     */
    useEffect(() => {
        if (project.length === 0) {
            return;
        }
        let cancelled = false;
        readLlmPolicy(client, project)
            .then((reading) => {
                if (!cancelled) {
                    setLlmPolicy(reading);
                }
            })
            .catch((error: unknown) => {
                if (!cancelled) {
                    // Kein stiller Rueckfall auf "erlaubt": wer die Datei nicht
                    // lesen konnte, weiss nicht, ob dort eine Sperre steht.
                    setLlmPolicy({
                        verdict: 'unreadable',
                        path: '.codeatlas/policy.json',
                        detail: error instanceof Error ? error.message : String(error),
                    });
                }
            });
        return () => {
            cancelled = true;
        };
    }, [client, project]);

    const llmMode = resolveLlmState(llmPolicy?.verdict, llmPreferenceOn);

    /*
     * Die Probe. Der ganze Opt-out haengt an der ersten Zeile dieses Effekts.
     *
     * Ist das LLM nicht an, wird der Timer gar nicht erst aufgehaengt und
     * `probeSidecar` nie gerufen. Es gibt keinen zweiten Aufrufer: der Zaehler
     * `llmProbes` steht hier und nur hier, und der Beweislauf liest ihn neben
     * dem Netz-Mitschnitt, damit "null Anfragen" von zwei Seiten belegt ist.
     */
    useEffect(() => {
        if (llmMode !== 'on') {
            askSidecarRef.current = undefined;
            return;
        }
        let cancelled = false;
        const ask = (model = selectedModelRef.current): void => {
            llmProbes.current += 1;
            /*
             * Die Modellwahl geht mit, und nur im Router-Modus wirkt sie
             * (src/llm/sidecar.ts). Sie kommt aus dem Ref und nicht aus der
             * Abhaengigkeitsliste: an ihr zu haengen hiesse, den Timer bei jeder
             * Wahl ab- und wieder aufzuhaengen.
             */
            void probeSidecar((url) => window.fetch(url), SIDECAR_ORIGIN, {
                model,
            })
                .then((reading) => {
                    if (!cancelled) {
                        setLlmProbe(reading);
                    }
                })
                .catch(() => {
                    if (!cancelled) {
                        setLlmProbe(EMPTY_SIDECAR_READING);
                    }
                });
        };
        // Der Griff fuer den Aktualisieren-Knopf des Panels. Er zeigt auf
        // dieselbe Funktion, damit jede Anfrage des Panels im selben Zaehler
        // landet wie die des Managers.
        askSidecarRef.current = ask;
        ask();
        const timer = window.setInterval(ask, SIDECAR_POLL_MS);
        return () => {
            cancelled = true;
            askSidecarRef.current = undefined;
            window.clearInterval(timer);
        };
    }, [llmMode]);

    const llmState: SidecarState =
        llmMode === 'disabled-by-policy' ? 'disabled-by-policy' : llmMode === 'off' ? 'off' : llmProbe.state;
    const llmFacts = llmState === 'ready' ? llmProbe.facts : undefined;
    const llmModel = llmFacts?.model ?? '';
    /*
     * Die Liste des Cache-Verzeichnisses, und ob eine Auswahl daraus ueberhaupt
     * wirkt. Beides gilt nur, solange der Prozess wirklich antwortet: eine Liste
     * von vorhin unter einem gestorbenen Sidecar waere eine Auswahl ohne
     * Gegenstelle.
     */
    const llmRouter = llmState === 'ready' && llmProbe.router;
    const llmModels: readonly CacheModel[] = llmState === 'ready' ? llmProbe.models : NO_CACHE_MODELS;

    /**
     * Das Modell, das eine Chat-Anfrage mitschickt, oder nichts.
     *
     * Nur im Router-Modus: ein Einzel-Server ignoriert ein fremdes `model`-Feld
     * stillschweigend (gemessen, siehe src/llm/sidecar.ts), und ein Feld, das
     * nichts bewirkt, mitzuschicken hiesse, sich auf eine Wirkung zu verlassen,
     * die es nicht gibt. Ohne Wahl bleibt der Rumpf der Anfrage genau der, den
     * er vor W10 hatte.
     */
    const requestModel = llmRouter && selectedModel.length > 0 ? selectedModel : undefined;
    const selectedModelName = requestModel === undefined
        ? llmModel
        : llmModels.find((model) => model.id === requestModel)?.name ?? llmModel;

    /** Die Wahl umlegen: gemerkt, und sofort nachgefragt. */
    const chooseModel = useCallback(
        (id: string) => {
            const selected = recordModelPreference(store, project, id).id;
            /* The immediate probe below runs before React necessarily renders
             * the new state. Keep its request and the next explicit AI request
             * on the same router id. */
            selectedModelRef.current = selected;
            setSelectedModel(selected);
            askSidecarRef.current?.(selected);
        },
        [project, store],
    );

    /** Eine Anzeige-Einstellung umlegen: gemerkt, und sofort wirksam. */
    const changeDisplay = useCallback(
        (next: GraphDisplaySettings) => {
            setDisplay(saveDisplaySettings(store, project, next));
        },
        [project, store],
    );

    /**
     * Der Schalter.
     *
     * Gegen eine Sperre tut er nichts, und zwar sichtbar nichts: die Praeferenz
     * wird dann nicht einmal geschrieben. Eine gespeicherte Praeferenz, die
     * niemals wirkt, waere ein Wert, der beim naechsten Projekt ohne Policy
     * ploetzlich etwas einschaltet, um das an dieser Stelle niemand gebeten hat.
     */
    const toggleLlm = useCallback(() => {
        if (llmMode === 'disabled-by-policy') {
            return;
        }
        const next = !llmPreferenceOn;
        setLlmPreferenceOn(recordLlmPreference(store, project, next).on);
        if (!next) {
            setLlmProbe(EMPTY_SIDECAR_READING);
        }
    }, [llmMode, llmPreferenceOn, project, store]);

    // ------------------------------------------- Der Atlas-Chat (W5b) ------

    /**
     * Was eine Aufzeichnung auf dem Weg in ein Symbol gesehen hat.
     *
     * Eine Anfrage je Aufrufer an /api/trace, und keine fuenfte: die Route
     * haengt `observed` an einen Hop einer Antwort, die aus dem STATISCHEN
     * Graphen gerechnet ist (siehe src/traces/bug-paths.ts). Es gibt also keine
     * Liste beobachteter Aufrufe, die man abfragen koennte, sondern nur die
     * Frage "was wurde auf diesem Weg gesehen", und genau die wird hier
     * gestellt. Ohne importierte Aufzeichnung kommt nichts zurueck, und das
     * Rezept schreibt das als Notiz unter die Antwort statt es zu verschweigen.
     */
    const observedInto = useCallback(
        async (subject: SymbolRef, callers: readonly { qualifiedName?: string; name: string }[]) => {
            const to = subject.qualifiedName;
            if (to === undefined || project.length === 0) {
                return [] as ObservedFact[];
            }
            const found: ObservedFact[] = [];
            for (const caller of callers) {
                const from = caller.qualifiedName;
                if (from === undefined) {
                    continue;
                }
                const answer = await api.trace(project, from, to).catch(() => undefined);
                if (answer === undefined || !answer.reachable) {
                    continue;
                }
                answer.path.forEach((node, index) => {
                    const observed = node.observed;
                    const previous = answer.path[index - 1];
                    if (observed !== undefined && previous !== undefined) {
                        found.push({
                            from: previous.name,
                            to: node.name,
                            count: observed.count,
                            lastSeen: observed.lastSeen,
                        });
                    }
                });
            }
            return found;
        },
        [api, project],
    );

    /** Eine Karte oeffnen: derselbe Weg, den jeder andere Klick dieser Oberflaeche geht. */
    const openCardSource = useCallback(
        (source: CardSource) => {
            const target = twinTargetOf({
                name: source.name,
                qualifiedName: source.qualifiedName,
                kind: 'unknown',
                filePath: source.filePath,
                startLine: source.line,
            });
            if (target === undefined) {
                return;
            }
            followRef.current(target);
        },
        [],
    );

    /**
     * Eine Frage an den Atlas.
     *
     * Der Compiler steht vor dem optionalen Modellzug: auch wenn das Modell
     * aus, gesperrt oder nicht gestartet ist, bleiben Karten und ihre Quellen
     * lesbar. Der Zug nennt den echten Grund und schickt nichts an den Sidecar.
     */
    const askQuestion = useCallback(
        (question: string, chosenSubject?: string) => {
            chatTicket.current += 1;
            const id = chatTicket.current;
            const base: ChatTurn = {
                id,
                question,
                status: 'compiling',
                depth: chatDepth,
                cards: [],
                gaps: [],
                sources: [],
                answer: '',
                message: '',
            };
            const put = (turn: ChatTurn): void => {
                setChatTurns((current) => current.map((entry) => (entry.id === id ? turn : entry)));
            };
            setChatTurns((current) => [...current, base]);
            // Wer fragt, hat angefangen. Siehe AC6f und `dismissWhy`.
            setWhyReopened(false);
            setWhyDismissed(true);
            // Eine Antwort hinter einer eingeklappten Flaeche waere genau die
            // Stille, gegen die dieses Panel gebaut ist. Seit W8 heisst das: der
            // Erklaeren-Bereich geht auf, und zwar auf DEM Reiter, der die
            // Antwort traegt, statt einen zweiten Kasten aufzumachen.
            openExplain('chat');

            void askAtlas({
                question,
                source: provider,
                root: ATLAS_WORKSPACE_ROOT,
                origin: SIDECAR_ORIGIN,
                fetch: (url, init) => window.fetch(url, init),
                modelName: llmModel,
                modelClass: modelClassOf(llmFacts?.contextTokens),
                /*
                 * Die Modellwahl des Lesers, wenn sie wirkt. Siehe
                 * `requestModel`: ohne Router bleibt sie weg, und der Rumpf der
                 * Anfrage ist dann derselbe wie vor W10.
                 */
                depth: chatDepth,
                context: {
                    focusName: twinSymbol?.name,
                    focusQualifiedName: twinSymbol?.qualifiedName,
                    mode: bugOpen ? 'bug' : impactOpen ? 'change' : entryOpen ? 'entry'
                        : tour !== undefined ? 'tour' : 'none',
                },
                ...(twinSymbol === undefined ? {} : { focus: twinSymbol }),
                ...(chosenSubject === undefined ? {} : { chosenSubject }),
                observed: observedInto,
                opts: { projectName: project, generation: 1 },
                /* The normal turn always stops at the built card path. The
                 * model is an explicit choice in the rendered answer. */
                useModel: false,
                ...(llmState === 'ready' ? {} : {
                    modelUnavailableReason: llmState === 'disabled-by-policy'
                        ? 'policy'
                        : llmState === 'not-running' ? 'not-running' : 'off',
                }),
            }, id).then(put);
        },
        [
            bugOpen, chatDepth, entryOpen, impactOpen, llmFacts, llmMode, llmModel, llmState,
            observedInto, project, provider, tour, twinSymbol,
        ],
    );

    /**
     * Der Leser hat aus einer Kandidatenliste gewaehlt.
     *
     * Dieselbe Frage noch einmal, mit dem qualifizierten Namen daneben. Ein
     * NEUER Zug und keine Aenderung am alten: der Zug mit der Liste bleibt
     * stehen, damit sichtbar bleibt, dass es die Wahl gab und wie sie ausging.
     * Ein Zug, der sich unter der Hand in eine Antwort verwandelt, waere ein
     * Verlauf, dem man nicht ansieht, was passiert ist.
     */
    const pickCandidate = useCallback(
        (turn: ChatTurn, candidate: SubjectCandidate) => {
            askQuestion(turn.question, candidate.qualifiedName ?? candidate.name);
        },
        [askQuestion],
    );

    /**
     * Der Knopf am Pseudocode-Block.
     *
     * Er existiert nur, wenn das Modell bereit ist (die Anzeige entscheidet
     * das), und er ersetzt nichts: das Ergebnis liegt neben dem Original, und
     * eine abgelehnte Antwort laesst das Original stehen und sagt den Grund.
     */
    const refinePseudocode = useCallback(() => {
        const document = pseudocode;
        if (document === undefined || llmState !== 'ready') {
            return;
        }
        setRefineState('running');
        setRefineMessage(REFINE_RUNNING);
        void askModel({
            origin: SIDECAR_ORIGIN,
            system: REFINE_SYSTEM_PROMPT,
            user: buildRefinePrompt(refineSubjectText(document)),
            chatTemplateKwargs: nonThinkingFor(llmModel).chatTemplateKwargs,
            maxTokens: refineMaxTokens(document),
            fetch: (url, init) => window.fetch(url, init),
            // Dieselbe Wahl wie im Chat: es ist derselbe Sidecar und dieselbe
            // Route, und ein Pfad, der ein anderes Modell benutzt als der
            // andere, waere ein Panel, das aus einem zweiten Modell erklaert.
            ...(requestModel === undefined ? {} : { model: requestModel }),
        })
            .then((reply) => {
                const outcome = applyRefinement(document, reply.content);
                if (outcome.kind === 'applied') {
                    setRefined(outcome.document);
                    setRefineState('applied');
                    setRefineMessage(REFINE_APPLIED);
                    return;
                }
                setRefined(undefined);
                setRefineState('refused');
                setRefineMessage(refineRejected(outcome.reason));
            })
            .catch((error: unknown) => {
                setRefined(undefined);
                setRefineState('refused');
                setRefineMessage(
                    refineRejected(error instanceof Error ? error.message : String(error)),
                );
            });
    }, [llmModel, llmState, pseudocode, requestModel]);

    const restorePseudocode = useCallback(() => {
        setRefined(undefined);
        setRefineState('idle');
        setRefineMessage('');
    }, []);

    /*
     * Der Layout-Griff, bei jedem Bild neu geschrieben.
     *
     * Ohne Abhaengigkeitsliste, wie die anderen Naehte dieser Datei: eine Liste
     * waere eine zweite Stelle, an der jemand vergisst, ein Feld nachzutragen,
     * und ein Griff, der eine Lage von vorhin meldet, ist schlimmer als keiner.
     */
    useEffect(() => {
        globalThis.__atlasLayout = {
            sizes: { ...shownZones },
            requested: { ...zones },
            bounds: Object.fromEntries(
                LAYOUT_KEYS.map((key) => [key, layoutBounds(key, frame)]),
            ) as Record<LayoutKey, { min: number; max: number }>,
            defaults: defaultLayout(frame),
            isDefault: sameLayout(shownZones, defaultLayout(frame)),
            frame: { ...frame },
            explainOpen,
            explainTab,
            tabs: explainTabsRef.current.map((tab) => ({
                id: tab.id,
                enabled: tab.enabled,
                reason: tab.reason,
                note: tab.note,
            })),
            storageKey: layoutStorageKey(project),
            state: {
                chatTurns: chatTurns.length,
                walkStep: tour === undefined ? -1 : tourStep,
                walkSteps: tour?.document.steps.length ?? 0,
                flowStep,
                flowSteps: flowView?.steps.length ?? 0,
            },
        };
    });

    useEffect(() => {
        globalThis.__atlasChat = {
            turns: chatTurns.map((turn) => ({
                id: turn.id,
                question: turn.question,
                status: turn.status,
                klass: turn.klass ?? '',
                rule: turn.rule ?? '',
                depth: turn.depth,
                cards: turn.cards.length,
                cardIds: turn.cards.map((card) => card.id),
                sources: turn.sources.length,
                citations: turn.check?.cited ?? [],
                unknownCitations: turn.check?.unknown ?? [],
                uncitedLines: (turn.check?.violations ?? [])
                    .filter((entry) => entry.reason === 'no-citation').length,
                answer: turn.answer,
                refusal: turn.refusal ?? '',
                tokens: turn.tokens ?? 0,
                budget: turn.budget ?? 0,
                tokensPerSecond: turn.tokensPerSecond ?? 0,
                candidates: (turn.choice?.candidates ?? []).map((candidate) => ({
                    name: candidate.name,
                    qualifiedName: candidate.qualifiedName ?? '',
                    filePath: candidate.filePath ?? '',
                    line: candidate.line ?? 0,
                })),
                askedName: turn.choice?.name ?? turn.focusFallback?.asked ?? '',
                focusFallbackUsed: turn.focusFallback?.used ?? '',
            })),
            open: chatOpen,
            height: shownZones.explainHeight,
            maxHeight: layoutBounds('explainHeight', frame).max,
            depth: chatDepth,
            depthDefault: NEIGHBOR_DEPTH_DEFAULT,
            depthOptions: [...NEIGHBOR_DEPTHS],
            refineAvailable: llmState === 'ready',
            refineState,
            refineMessage,
            // Der echte Validator, an derselben Stelle, an der ihn der Knopf
            // ruft. Ein Beweislauf, der eine kaputte Antwort nachbaut, prueft
            // sonst eine Kopie der Regel statt der Regel.
            validateRefine: (answer: string) => {
                if (pseudocode === undefined) {
                    return { applied: false, reason: 'no block is shown' };
                }
                const outcome = applyRefinement(pseudocode, answer);
                return outcome.kind === 'applied'
                    ? { applied: true, reason: '' }
                    : { applied: false, reason: outcome.reason };
            },
        };
    });

    useEffect(() => {
        globalThis.__atlasLlm = {
            state: llmState,
            preferenceOn: llmPreferenceOn,
            policyVerdict: llmPolicy?.verdict ?? '',
            policyPath: llmPolicy?.path ?? '',
            policyDetail: llmPolicy?.detail ?? '',
            model: llmModel,
            chip: llmChipValue(llmState, llmModel),
            probes: llmProbes.current,
        };
    });

    /*
     * Der Griff des Einstellungen-Panels, bei jedem Bild neu.
     *
     * Er steht HIER und nicht im Panel, obwohl das Panel die Messungen macht:
     * die Wahl, die Anzeige-Einstellungen und die Modell-Liste gehoeren der
     * Anwendung und ueberleben das Zuklappen. Ein Griff, den nur ein offenes
     * Panel schreibt, waere nach dem Schliessen eine Beschreibung von vorhin,
     * und `llmOffMakesNoRequests` muss auch bei geschlossenem Panel lesbar sein.
     */
    useEffect(() => {
        globalThis.__atlasSettings = {
            open: settingsOpen,
            llmOn: llmMode === 'on',
            probes: llmProbes.current,
            router: llmRouter,
            selectedModel,
            cacheModels: llmModels.map((model) => ({
                id: model.id,
                name: model.name,
                loaded: model.loaded,
                status: model.status,
                active: model.id === selectedModel
                    || (selectedModel.length === 0 && model.name === llmModel),
            })),
            running: {
                model: llmModel,
                modelPath: llmFacts?.modelPath ?? '',
                quantization: llmFacts?.quantization ?? '',
                contextTokens: llmFacts?.contextTokens ?? 0,
                trainedContextTokens: llmFacts?.trainedContextTokens ?? 0,
                weightsBytes: llmFacts?.weightsBytes ?? 0,
                parameters: llmFacts?.parameters ?? 0,
            },
            suggestions: MODEL_SUGGESTIONS.map((entry) => ({
                id: entry.id,
                name: entry.name,
                repo: entry.repo,
                modelClass: entry.modelClass,
                passRate: entry.passRate,
                citationCompliance: entry.citationCompliance,
                citationUnmeasured: entry.citationUnmeasured ?? null,
                tokensPerSecond: entry.tokensPerSecond,
                bytes: entry.bytes,
                command: fetchCommand(entry.repo, entry.quant),
            })),
            display: { ...display },
            displayDefault: { ...DEFAULT_GRAPH_DISPLAY },
            isDefaultDisplay: isDefaultDisplay(display),
            storageKeys: { model: modelKey(project), display: displayKey(project) },
            measurements: Object.values(measurements),
        };
    });

    // ------------------------------------------------------- Tastatur ------

    const overlayOpen = isSearchable(command, SEARCH_MIN_QUERY);
    /**
     * Ob genau ein Zeichen dasteht: zu wenig zum Suchen, genug fuer eine
     * Antwort auf die Frage "warum passiert nichts".
     */
    const oneCharTyped = command.trim().length > 0 && !overlayOpen;
    const hitRows = useMemo(() => searchRows(hits, MAX_SEARCH_ROWS, hitSource), [hits, hitSource]);

    /*
     * Der Griff der Suche und der Menuezeile, bei jedem Bild neu geschrieben.
     *
     * Bei jedem Bild und nicht nur bei einer Aenderung: die Zaehler stehen in
     * Refs, also weiss React nichts von ihnen, und ein Effekt mit
     * Abhaengigkeitsliste wuerde sie nur zufaellig mitnehmen.
     */
    useEffect(() => {
        globalThis.__atlasSearch = {
            debounceMs: SEARCH_DEBOUNCE_MS,
            minQuery: SEARCH_MIN_QUERY,
            currentQuery: command.trim(),
            shownQuery: answeredQuery,
            shownSource: hitSource,
            shownRows: hitRows.length,
            localFirst: searchStats.current.localFirst,
            prefixCacheHits: searchStats.current.prefixCacheHits,
            serverRequests: searchStats.current.serverRequests,
            serverRoundtripMs: searchStats.current.serverRoundtripMs,
            roundtrips: [...searchStats.current.roundtrips],
            abortedRequests: searchStats.current.abortedRequests,
            staleDropped: searchStats.current.staleDropped,
            staleAnswerWins: searchStats.current.staleAnswerWins,
            localCandidates: localPool.length,
            activatedMenus: [...activatedMenus.current],
            keyListenerCapture: KEY_LISTENER_OPTIONS.capture,
        };
    });

    /**
     * Die Tasten des Suchfensters.
     *
     * Nur solange es offen ist, und nur die vier, die ihm gehoeren. Ein
     * verbrauchtes Ereignis wird als verbraucht markiert, damit das Chrome
     * Escape nicht ein zweites Mal liest und das Feld den Fokus verliert,
     * waehrend der Leser noch tippt.
     */
    /**
     * Was Enter in der Kommandozeile bedeutet.
     *
     * Die Entscheidung selbst steht in src/chat/command-intent.ts, samt
     * Begruendung: die Suche behaelt den Vorrang, und eine Zeile mit
     * Fragezeichen, eine Zeile mit @ und eine Zeile, auf die die Suche mit
     * nichts geantwortet hat, sind Fragen. Hier steht nur, was mit der
     * Entscheidung passiert.
     */
    const enterIntent = useMemo(
        () => commandIntent({
            line: command,
            hitCount: hitRows.length,
            answered: answeredQuery === command.trim() && searchStatus === 'ready',
        }),
        [answeredQuery, command, hitRows.length, searchStatus],
    );

    const onCommandKeyDown = useCallback(
        (event: KeyboardEvent<HTMLInputElement>) => {
            /*
             * Der eine Befehl steht VOR der Suche und vor der Frage.
             *
             * Nicht, weil er wichtiger waere, sondern weil er sonst nie
             * ankaeme: "reset layout" findet kein Symbol, also waere die Zeile
             * eine Frage an das Modell, und der Leser bekaeme eine Antwort
             * darueber, dass es kein Symbol dieses Namens gibt. Die Erkennung
             * ist absichtlich eng (src/layout/layout-command.ts); jede
             * Unschaerfe hier waere eine Suche, die stattdessen das Layout
             * umbaut.
             */
            if (event.key === 'Enter') {
                const line = lineCommandOf(command);
                if (line === 'reset-layout') {
                    event.preventDefault();
                    resetLayout();
                    setCommand('');
                    closeSearch();
                    return;
                }
                if (line === 'open-settings') {
                    event.preventDefault();
                    setSettingsOpen(true);
                    setCommand('');
                    closeSearch();
                    return;
                }
                if (line === 'toggle-live-agents') {
                    event.preventDefault();
                    setLiveAgentsOn((on) => !on);
                    setCommand('');
                    closeSearch();
                    return;
                }
                /*
                 * "fullscreen" (W11b). Der Modus ist der Rahmen der
                 * Agenten-Ansicht; ist der Live-Modus aus, geht er mit an,
                 * statt dass die Zeile still nichts tut.
                 */
                if (line === 'toggle-fullscreen') {
                    event.preventDefault();
                    setLiveAgentsOn(true);
                    setFullscreenToggle((count) => count + 1);
                    setCommand('');
                    closeSearch();
                    return;
                }
            }
            // Enter gilt auch dann, wenn kein Suchfenster offen ist: eine Frage
            // von zwei Zeichen Laenge oeffnet keines, und sie muss trotzdem
            // abschickbar sein.
            if (event.key === 'Enter' && enterIntent === 'ask') {
                event.preventDefault();
                const question = command.trim();
                closeSearch();
                askQuestion(question);
                return;
            }
            if (!overlayOpen) {
                return;
            }
            const intent = overlayIntent(event.key);
            if (intent === 'none') {
                return;
            }
            event.preventDefault();
            switch (intent) {
                case 'up':
                    setSelectedHit((current) => moveSelection(hitRows.length, current, -1));
                    return;
                case 'down':
                    setSelectedHit((current) => moveSelection(hitRows.length, current, 1));
                    return;
                case 'close':
                    closeSearch();
                    return;
                case 'choose':
                    chooseHit(selectedHit);
                    return;
                default:
                    return;
            }
        },
        [
            askQuestion, chooseHit, closeSearch, command, enterIntent, hitRows.length, overlayOpen,
            resetLayout, selectedHit,
        ],
    );

    /*
     * Der eine Griff am Fenster, der entscheidet, wer eine Taste bekommt.
     *
     * Er liegt am Fenster und nicht am Menue oder an der Zeile: beides soll
     * gelten, waehrend der Fokus im Baum, auf einem Knopf oder nirgends steht.
     * WANN eine Taste was bedeutet, entscheidet src/app/keyboard.ts (dort steht
     * auch, warum die Kuerzel seit dem 2026-08-29 Alt/Option tragen); WAS ein
     * Kuerzel tut, steht in `menuActionsRef`, das weiter unten bei den
     * Menuepunkten geschrieben wird.
     *
     * Die Reihenfolge ist die ganze Aussage, und sie steht hier einmal:
     *
     *  1. **Ein Menuekuerzel zuerst.** Alt/Option + Buchstabe, oder die
     *     reservierte Hilfetaste.
     *  2. **Eine Flaeche, die die Tastatur fuehrt, behaelt sie.** Das sind seit
     *     W8 noch zwei, die Hilfe und der Einstiegsdialog (siehe
     *     `keyboardGuardRef`); hinter ihnen faellt kein Zeichen in eine Zeile,
     *     die der Leser gerade nicht ansieht.
     *  3. **Einer laufenden Fuehrung gehoeren ihre vier Tasten.** Welche das
     *     sind, sagt `playerIntent` und nicht eine zweite Liste hier.
     *  4. **Alles andere Druckbare gehoert der Kommandozeile.** Sie holt sich
     *     den Fokus und bekommt das Zeichen, damit Tippen sichtbar ankommt,
     *     statt im Hintergrund Panels zu oeffnen (Nutzerbefund 2026-08-29).
     *
     * Seit W7b haengt der Griff in der EINFANGENDEN Phase. Der Grund steht bei
     * KEY_LISTENER_OPTIONS in src/app/keyboard.ts: der Nutzerbefund "alt plus
     * letter funktioniert nur fuer atlas" liess sich nicht nachstellen, also
     * wird nicht an einer Ursache geraten, sondern die eine Klasse von Ursachen
     * ausgeschlossen, die diese Anwendung selbst in der Hand hat, naemlich ein
     * Griff auf dem Weg nach oben, der die Taste vorher verbraucht. In der
     * einfangenden Phase sieht das Fenster den Druck als erster, und niemand
     * kann ihm zuvorkommen.
     *
     * Ein verbrauchtes Ereignis wird zusaetzlich gezeichnet (`markHandled`).
     * Das ist nicht fuer die Verdrahtung, sondern fuer den Tastentest der
     * Hilfeseite: `defaultPrevented` allein kann nicht sagen, ob DIESER Griff
     * die Taste genommen hat oder jemand vor ihm, und genau das ist die Frage,
     * die der Nutzerbefund stellt.
     */
    useEffect(() => {
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            const target = event.target as Element | null;
            const shortcut = menuShortcutFor(event, target, WIRED_MENU_SHORTCUTS);
            if (shortcut !== undefined) {
                const act = menuActionsRef.current[shortcut];
                if (act !== undefined) {
                    event.preventDefault();
                    markHandled(event, shortcut);
                    act();
                }
                return;
            }
            const guard = keyboardGuardRef.current;
            if (guard.overlayOpen) {
                return;
            }
            if (guard.walkRunning && playerIntent(event.key) !== 'none') {
                return;
            }
            const intent = commandLineIntent(event, target);
            if (intent === undefined) {
                return;
            }
            // Abbestellt, damit das Zeichen nicht zweimal ankommt: einmal von
            // hier und einmal von der Vorgabe, sobald das Feld den Fokus hat.
            event.preventDefault();
            commandInputRef.current?.focus();
            if (intent.kind === 'type') {
                setCommand((current) => current + intent.text);
            }
        };
        window.addEventListener('keydown', onKeyDown, KEY_LISTENER_OPTIONS);
        return () => window.removeEventListener('keydown', onKeyDown, KEY_LISTENER_OPTIONS);
    }, []);

    /*
     * Escape schliesst die Hilfe, auch von ausserhalb der Hilfe.
     *
     * Die Seite faengt Escape schon selbst ab (sie holt sich den Fokus beim
     * Aufschlagen). Dieser Griff ist fuer den Fall danach: wer im Baum oder in
     * der Kommandozeile geklickt hat, waehrend die Hilfe offen steht, drueckt
     * Escape und meint trotzdem die Hilfe. Er haengt nur, solange sie offen ist,
     * damit Escape sonst genau so wenig bedeutet wie vorher.
     */
    useEffect(() => {
        if (!helpOpen) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) {
                return;
            }
            event.preventDefault();
            setHelpOpen(false);
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [helpOpen]);

    // Dasselbe fuer das Einstellungen-Panel, und aus demselben Grund: wer im
    // Baum oder in der Kommandozeile geklickt hat, waehrend es offen steht,
    // drueckt Escape und meint trotzdem das Panel.
    useEffect(() => {
        if (!settingsOpen) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) {
                return;
            }
            event.preventDefault();
            setSettingsOpen(false);
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [settingsOpen]);

    // And for the projects panel, which has input fields of its own as well.
    useEffect(() => {
        if (!projectsOpen) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) {
                return;
            }
            event.preventDefault();
            setProjectsOpen(false);
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [projectsOpen]);

    /**
     * Die vier Tasten einer laufenden Fuehrung.
     *
     * Am Fenster und nicht an der Karte, weil der Fokus waehrend einer Fuehrung
     * fast immer im Editor steht: waeren die Tasten an der Karte, muesste der
     * Leser vor jedem Schritt erst dorthin klicken. Wo sie NICHT gelten, sagt
     * `tourKeyForEvent`; in Kuerze: in einem echten Eingabefeld.
     *
     * `d` ist die vierte und bewegt die Fuehrung nicht: sie schlaegt das Bild
     * zum Schritt auf. Es gibt es nur, wenn der Schritt ein Symbol hat, und
     * ohne Symbol passiert nichts und die Taste bleibt unverbraucht, damit ein
     * gedrueckte `d` nicht so aussieht, als haette es etwas getan.
     */
    useEffect(() => {
        const steps = tour?.document.steps.length ?? 0;
        if (steps === 0) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            const key = tourKeyForEvent(event, event.target as Element | null);
            if (key === undefined) {
                return;
            }
            const intent = playerIntent(key);
            if (intent === 'none') {
                return;
            }
            if (intent === 'diagram') {
                const step = tour?.document.steps[tourStepRef.current];
                if (step?.primary.kind !== 'symbol') {
                    return;
                }
                event.preventDefault();
                openExplain('flow');
                return;
            }
            event.preventDefault();
            if (intent === 'exit') {
                closeTour();
                return;
            }
            if (intent === 'prev') {
                setTourStep(stepMove(steps, tourStepRef.current, -1));
                return;
            }
            if (isLastStep(tourStepRef.current, steps)) {
                closeTour();
                return;
            }
            setTourStep(stepMove(steps, tourStepRef.current, 1));
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [closeTour, openExplain, tour]);

    // Escape schliesst den Einstiegsdialog, auch aus seinem eigenen Suchfeld
    // heraus: dort ist Escape sonst nichts, und ein Dialog ohne Ausgang ueber
    // die Tastatur ist ein Dialog, aus dem man nur mit der Maus herauskommt.
    useEffect(() => {
        if (!entryOpen) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape') {
                return;
            }
            event.preventDefault();
            setEntryOpen(false);
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [entryOpen]);

    /*
     * Escape schliesst die Frage nach dem Warum.
     *
     * Sie stand bis W8b NICHT im Escape-Griff dieser Datei: dort standen Hilfe,
     * Flow, Einstieg, Assistent, Aenderungsansicht und Suchfenster, und die
     * einzige Flaeche, die ueber dem Editor liegen und dort bleiben konnte,
     * hatte keinen Ausgang ueber die Tastatur (Nutzerbefund 2026-08-29, AC6f).
     * Der einzige Ausweg war ein Knopf, der "Not now" hiess.
     *
     * Die Stelle in der Reihenfolge ist die richtige und keine beliebige: die
     * Hilfe und der Einstiegsdialog liegen UEBER der Frage, das Suchfenster
     * gehoert der Kommandozeile, und der Erklaeren-Bereich liegt darunter. Der
     * Griff nimmt die Taste also nach den drei ersten und vor dem letzten, und
     * er markiert sie als verbraucht, damit der Bereich sie liegenlaesst.
     */
    useEffect(() => {
        if (!whyVisible) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) {
                return;
            }
            if (helpOpen || entryOpen || overlayOpen) {
                return;
            }
            event.preventDefault();
            dismissWhy();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [dismissWhy, entryOpen, helpOpen, overlayOpen, whyVisible]);

    /*
     * Escape klappt den Erklaeren-Bereich ein, und zwar zuletzt.
     *
     * Ein Griff fuer alle fuenf Reiter, wo bis W8 drei Griffe standen (Chat,
     * Flow-Overlay, und die Sammelklausel fuer Assistent und Aenderungsansicht).
     * Was dabei gilt, ist genau die Zusage aus W7c, nur breiter: Escape kostet
     * NICHTS. Kein Zug wird geloescht, kein Schritt zurueckgesetzt, keine
     * Fuehrung beendet; der Streifen darunter sagt weiter, was noch dasteht, und
     * ein Klick auf den Reiter bringt alles zurueck. Der Nutzerbefund vom
     * 2026-08-29, aus dem diese Trennung entstanden ist, gilt damit fuer jede
     * der fuenf Flaechen statt nur fuer den Chat.
     *
     * Der Griff nimmt die Taste nur, wenn sie sonst niemand braucht, und das ist
     * keine Hoeflichkeit, sondern die Reihenfolge: die Hilfe und der
     * Einstiegsdialog liegen ueber dem Editor, das Suchfenster steht ueber der
     * Kommandozeile, und eine Zone weiter unten, die ihnen Escape wegnaehme,
     * waere eine Zone, die eine offene Flaeche stehenlaesst. Dazu das Feld unter
     * dem Zeiger: wer in der Kommandozeile steht, verlaesst mit Escape die
     * Zeile, und dieser Griff hat dort nichts zu suchen.
     */
    useEffect(() => {
        if (!explainOpen) {
            return;
        }
        const onKeyDown = (event: globalThis.KeyboardEvent): void => {
            if (event.key !== 'Escape' || event.defaultPrevented) {
                return;
            }
            if (helpOpen || entryOpen || overlayOpen) {
                return;
            }
            /*
             * Genau EIN Feld behaelt Escape fuer sich, und es ist die
             * Kommandozeile: wer dort steht, verlaesst mit Escape die Zeile.
             * Nicht `isTypingTarget`, obwohl der Chat-Griff aus W7c das so
             * hatte: der Editor faellt darunter, und der Flow-Erklaerer schickt
             * bei jedem Schritt den Caret dorthin. Mit der weiten Pruefung waere
             * Escape genau dann taub, wenn der Leser gerade Schritte gegangen
             * ist, und das war bis W8 der Fall, den das Overlay mit einem
             * eigenen Griff ohne diese Pruefung abgedeckt hat.
             */
            if (event.target === commandInputRef.current) {
                return;
            }
            event.preventDefault();
            collapseExplain();
        };
        window.addEventListener('keydown', onKeyDown);
        return () => window.removeEventListener('keydown', onKeyDown);
    }, [collapseExplain, entryOpen, explainOpen, helpOpen, overlayOpen]);

    /**
     * Der Griff, an dem der Beweislauf die Fuehrung anfasst.
     *
     * `regenerate` ist der Determinismus-Beweis und steht deshalb hier und nicht
     * im Lauf: der Lauf soll dieselbe Erzeugung noch einmal ausloesen, die die
     * Anwendung benutzt, und nicht eine nachgebaute daneben. Angeboten wird sie
     * nur fuer die Projekt-Fuehrung; ein Vorwaerts-Walk haengt an einer Wahl und
     * ist nicht dieselbe Frage zweimal.
     */
    useEffect(() => {
        globalThis.__atlasTour =
            tour === undefined
                ? undefined
                : {
                    id: tour.document.id,
                    kind: tour.kind,
                    title: tour.document.title,
                    steps: tour.document.steps.length,
                    index: tourStep,
                    paths: tour.document.steps.map((step) => step.primary.filePath),
                    titles: tour.document.steps.map((step) => step.title),
                    endNote: tour.endNote,
                    json: JSON.stringify(tour.document),
                    regenerate:
                        tour.kind === 'project'
                            ? async () =>
                                JSON.stringify(
                                    (
                                        await generateProjectTour(provider, ATLAS_WORKSPACE_ROOT, {
                                            projectName: project,
                                            generation: 1,
                                        })
                                    ).document,
                                )
                            : undefined,
                };
    });

    const onTreeKeyDown = useCallback(
        (event: KeyboardEvent<HTMLUListElement>) => {
            const row = rows[cursor];
            const intent = treeIntent(event.key, row);
            if (intent === 'none') {
                return;
            }
            event.preventDefault();
            switch (intent) {
                case 'down':
                    setCursor(moveCursor(rows.length, cursor, 1));
                    return;
                case 'up':
                    setCursor(moveCursor(rows.length, cursor, -1));
                    return;
                case 'open':
                    if (row !== undefined) {
                        openFile(row.path);
                    }
                    return;
                case 'expand':
                case 'collapse':
                    if (row !== undefined) {
                        toggleDirectory(row);
                    }
                    return;
                case 'toParent': {
                    if (row === undefined) {
                        return;
                    }
                    const parent = parentPath(row.path);
                    const index = rows.findIndex((candidate) => candidate.path === parent);
                    if (index >= 0) {
                        setCursor(index);
                    }
                    return;
                }
                default:
                    return;
            }
        },
        [cursor, openFile, rows, toggleDirectory],
    );

    // ------------------------------------------------------- Anzeige -------

    /**
     * Die stille Anzeige fuer das Symbol vor dem Leser.
     *
     * Undefiniert, solange es keine Checkliste gibt, und die Statusleiste laesst
     * den Punkt dann ganz weg. "explored 0 of 0" waere ein Befund ueber das
     * Symbol, wo in Wahrheit noch niemand gefragt hat.
     */
    const understanding = useMemo(() => {
        const owner = twinIr?.symbol.qualifiedName;
        return owner === undefined || twinIr === undefined
            ? undefined
            : understandingOf(marks, owner, twinIr.checklist);
    }, [marks, twinIr]);
    const explored = exploredLabel(understanding);

    useEffect(() => {
        globalThis.__atlasChecklist = {
            symbol: understanding?.symbolQualifiedName ?? '',
            explored: understanding?.exploration.visited ?? 0,
            total: understanding?.exploration.total ?? 0,
            label: explored ?? '',
            marks: totalMarks(marks),
        };
    });

    const rootLevel = levels.get('');
    const treeNoteParts: string[] = [];
    if (treeError.length > 0) {
        treeNoteParts.push(`tree unavailable: ${treeError}`);
    } else if (rootLevel !== undefined) {
        treeNoteParts.push(`${rootLevel.files} files, ${rootLevel.symbols} symbols from ${TREE_ROUTE}`);
        const dropped = [...levels.values()].reduce((sum, level) => sum + level.droppedNonPaths, 0);
        if (dropped > 0) {
            treeNoteParts.push(
                dropped === 1
                    ? '1 index entry names no path and is not shown'
                    : `${dropped} index entries name no path and are not shown`,
            );
        }
        if (budgetHit) {
            treeNoteParts.push(`only ${EAGER_LEVEL_BUDGET} folders opened up front, the rest load on click`);
        }
    } else if (project.length === 0) {
        treeNoteParts.push(projectDetail.length > 0 ? projectDetail : 'no project');
    } else {
        treeNoteParts.push('loading the tree ...');
    }
    // Die zweite Quelle bekommt ihren eigenen Satz. "Noch nicht gefragt" und
    // "gefragt, nichts gefunden" sehen im Baum gleich aus und sind es nicht.
    if (coverageError.length > 0) {
        treeNoteParts.push(`coverage unavailable: ${coverageError}`);
    } else if (!coverageAsked) {
        treeNoteParts.push('joining the coverage lists ...');
    } else {
        treeNoteParts.push(
            `${coverage.counts.partial} partial, ${coverage.counts.skipped} skipped, `
            + `${coverage.counts.notIndexedFiles} files and ${coverage.counts.notIndexedDirs} folders `
            + `not indexed, ${coverage.counts.scopeEntries} coverage entries joined in`,
        );
    }

    const chips: Chip[] = [
        {
            label: messages.statusbar.chipProject,
            value: project.length > 0 ? project : messages.app.noProject,
            state: project.length > 0 ? 'plain' : 'absent',
        },
        {
            label: messages.statusbar.chipSymbols,
            value: counts.nodes === undefined ? messages.statusbar.noCount : String(counts.nodes),
            state: counts.nodes === undefined ? 'absent' : 'plain',
        },
        {
            label: messages.statusbar.chipEdges,
            value: counts.edges === undefined ? messages.statusbar.noCount : String(counts.edges),
            state: counts.edges === undefined ? 'absent' : 'plain',
        },
    ];

    const status: Chip[] = [
        {
            label: messages.statusbar.chipProject,
            value: project.length > 0 ? project : messages.app.noProject,
            state: project.length > 0 ? 'ok' : 'absent',
        },
        {
            label: messages.statusbar.chipServer,
            value: serverOk === undefined
                ? messages.statusbar.serverContacting
                : serverOk ? messages.statusbar.serverReady : messages.statusbar.serverUnreachable,
            state: serverOk === true ? 'ok' : serverOk === false ? 'absent' : 'plain',
        },
        {
            label: messages.statusbar.chipPort,
            value: window.location.port.length > 0 ? window.location.port : '80',
        },
        { label: messages.statusbar.chipTree, value: TREE_ROUTE },
        {
            label: messages.statusbar.chipSource,
            value: document === undefined ? messages.statusbar.noFileLoaded : READER_RPC_TOOL,
            state: document === undefined ? 'absent' : 'ok',
        },
        {
            label: messages.statusbar.chipGalaxy,
            value:
                layout === undefined
                    ? messages.galaxy.noLayout
                    : messages.galaxy.nodeCount(layout.nodes.length, !galaxyOn),
            state: layout === undefined ? 'absent' : galaxyOn ? 'ok' : 'plain',
        },
        /*
         * Der Chip des lokalen Modells. Er steht IMMER da, auch im Zustand aus,
         * und das ist der Punkt: "es gibt hier ein lokales Modell und es ist
         * aus" ist eine andere Aussage als ein Chip, der erst auftaucht, wenn
         * etwas laeuft. Der zweite waere aus der Ferne nicht von "dieses Produkt
         * kann das nicht" zu unterscheiden.
         */
        {
            label: messages.statusbar.chipLlm,
            value: llmChipValue(llmState, llmModel),
            state: llmState === 'ready' ? 'ok' : llmState === 'starting' ? 'plain' : 'absent',
        },
    ];
    /*
     * Die Frische-Notiz der offenen Datei.
     *
     * Sie steht nur da, wenn der Server etwas zu sagen hat: `metadata_match`
     * zusammen mit `no_recorded_issue` ist die Lage, in der die Datei seit dem
     * Index unveraendert ist und keine Quelle etwas gemeldet hat. Ein Chip, der
     * auch das anzeigt, waere eine Ampel, die immer leuchtet.
     */
    const coverageAnswer = openCoverage;
    const freshnessLabel =
        coverageAnswer !== undefined
        && freshnessNoteNeeded(coverageAnswer.status, coverageAnswer.freshness)
            ? freshnessNote(
                coverageAnswer.status,
                coverageAnswer.freshness,
                coverageAnswer.recommendedAction,
            )
            : '';
    if (freshnessLabel.length > 0) {
        status.push({ label: messages.statusbar.chipCoverage, value: freshnessLabel, state: 'absent' });
    }
    // Die stille Anzeige steht nur da, wenn sie etwas zu zaehlen hat.
    if (explored !== undefined) {
        status.push({ label: messages.statusbar.chipExplored, value: explored, state: 'ok' });
    }

    /*
     * Die Notiz ueber dem Editor.
     *
     * Nur fuer die eine Lage, in der der Text dasteht und trotzdem
     * unvollstaendig gelesen wurde: eine partiell geparste Datei. Die Faelle
     * ohne Inhalt sagen ihre Begruendung an der Stelle, an der sonst der Text
     * stuende, und brauchen darum keine zweite Zeile darueber.
     */
    const openRecord = coverage.records.get(activePath);
    const coverageNote =
        readerStatus === 'ready' && openRecord?.state === 'partial'
            ? { state: 'partially parsed', text: partialFileNote(activePath, openRecord.reason) }
            : undefined;

    useEffect(() => {
        globalThis.__atlasCoverage = {
            rows: rows.map((row) => ({
                path: row.path,
                kind: row.kind,
                coverage: row.coverage ?? 'indexed',
                reason: row.coverageReason ?? '',
                sources: row.coverageSources ?? [],
            })),
            records: [...coverage.records.values()].map((record) => ({
                path: record.path,
                kind: record.kind,
                state: record.state,
                reason: record.reason,
                sources: [...record.sources],
            })),
            truncations: [...coverage.truncations],
            counts: { ...coverage.counts },
            metadata: { ...coverageMeta },
            open:
                coverageAnswer === undefined
                    ? undefined
                    : {
                        path: coverageAnswer.path,
                        status: coverageAnswer.status,
                        freshness: coverageAnswer.freshness,
                        action: coverageAnswer.recommendedAction,
                        note: freshnessLabel,
                    },
            error: coverageError,
        };
    });
    if (tourMessage.length > 0) {
        status.push({ label: messages.statusbar.chipWalk, value: tourMessage, state: 'absent' });
    }

    const tabDescriptors: TabDescriptor[] = tabs.map((path) => ({
        path,
        name: baseName(path),
        active: path === activePath,
    }));

    // Badges nur, solange der Reader die Datei zeigt, in der das Symbol steht.
    // Ohne diese Bedingung malte ein Dateiwechsel fuer einen Wimpernschlag die
    // Aufrufstellen des vorigen Symbols in die neue Datei.
    const twinPath = twinSymbol === undefined ? '' : workspacePathOf(twinSymbol.uri);
    // Gemerkt, damit der Editor seine Dekorationen nur dann neu setzt, wenn sich
    // die Schritte wirklich geaendert haben, und nicht bei jedem Bild.
    const badges = useMemo(
        () =>
            twinIr !== undefined && twinPath === activePath
                ? badgesForLines(
                      twinIr.steps.value.map((call) => ({ line: call.line, label: call.targetName })),
                  )
                : noBadges,
        [twinIr, twinPath, activePath],
    );

    /*
     * Die vier Eintraege der Atlas-Zeile, EINMAL.
     *
     * Sie stehen als Liste da und nicht zweimal ausgeschrieben, weil sie seit
     * dem 2026-08-29 zwei Leser haben: das Menue zeichnet sie, und die Tastatur
     * ruft sie. Zwei Fassungen derselben Liste waeren zwei Gelegenheiten, ein
     * Etikett zu aendern und das Kuerzel dabei zu vergessen, und ein Menuepunkt,
     * dessen Buchstabe etwas anderes tut, als daneben steht, ist schlimmer als
     * einer ohne Buchstaben.
     *
     * `key` ist das Suffix von `data-menu` (also `a-why`) und bleibt, was es
     * war: die Beweislaeufe fassen die Eintraege daran an. `shortcut` ist der
     * Buchstabe, und er steht im Etikett in Klammern, weil ein Terminal keine
     * Unterstreichung hat.
     */
    const atlasEntries: readonly (MenuExtra & { shortcut: string })[] = [
        {
            key: 'why',
            shortcut: 'w',
            label: WHY_MENU_LABEL,
            title: messages.menu.why,
            onSelect: () => {
                // Die Frage neu zu stellen heisst, von vorn anzufangen: die
                // laufende Fuehrung endet, und der Erklaeren-Bereich klappt zu,
                // damit die Frage die leere Editorflaeche bekommt.
                closeTour();
                setEntryOpen(false);
                collapseExplain();
                setWhyDismissed(false);
                setWhyReopened(true);
            },
        },
        {
            key: 'bug',
            shortcut: 'b',
            label: BUG_WIZARD_MENU_LABEL,
            title: messages.menu.bug,
            onSelect: () => {
                /*
                 * Kein `closeTour()` mehr, und das ist eine Aenderung mit Grund.
                 * Bis W8 deckte der Assistent den Editor ab und die Schrittkarte
                 * lag darunter, also musste die Fuehrung weichen. Jetzt teilen
                 * sich beide einen Platz und wechseln ueber einen Reiter; eine
                 * Fuehrung zu beenden, weil jemand kurz in den Assistenten
                 * schaut, waere genau der Zustandsverlust, den dieser Zyklus
                 * abschafft.
                 */
                setEntryOpen(false);
                setWhyReopened(false);
                openExplain('bug');
            },
        },
        {
            key: 'impact',
            shortcut: 'c',
            label: IMPACT_MENU_LABEL,
            title: messages.menu.impact,
            onSelect: () => {
                setEntryOpen(false);
                setWhyReopened(false);
                openExplain('change');
            },
        },
        /*
         * Der Weg zurueck aus AC3, als Menuepunkt.
         *
         * Er steht in derselben Zeile wie die anderen vier und traegt wie sie
         * einen Buchstaben: eine Handlung, die es nur in der Kommandozeile gibt,
         * ist in einer Oberflaeche, deren Vorbild die Tastatur ist, eine halbe
         * Handlung.
         */
        {
            key: 'layout',
            shortcut: 'r',
            label: messages.layout.resetMenuLabel,
            title: messages.layout.resetTooltip,
            onSelect: resetLayout,
        },
        /*
         * Der Schalter im Menue, mit demselben Ausgang wie der im Panel.
         * Ein Extra traegt keinen Zustand, also traegt ihn sein Etikett:
         * `[l]lm off` heisst "es ist aus" und nicht "hier ausschalten".
         * Diese Lesart ist dieselbe wie bei [a]tlas, dessen `data-state`
         * ebenfalls die Lage und nicht die Wirkung faerbt.
         */
        {
            key: 'llm',
            shortcut: 'l',
            label: llmMenuLabel(llmState),
            title: llmMenuTitle(llmState),
            onSelect: toggleLlm,
        },
        /*
         * Das Einstellungen-Panel (W10), als Eintrag der Atlas-Zeile.
         *
         * Es schaltet, wie [a]tlas und [?]help: der Buchstabe steht fuer die
         * Flaeche, und ein zweiter Druck macht sie wieder zu. Ein Eintrag, der
         * nur oeffnet, waere in dieser Zeile der einzige.
         */
        {
            key: 'settings',
            shortcut: 's',
            label: messages.settings.menuLabel,
            title: messages.menu.settings,
            onSelect: () => setSettingsOpen((open) => !open),
        },
        /*
         * The projects panel, as an entry of the atlas row. It toggles like
         * [s]ettings next to it: the letter stands for the surface, and a
         * second press closes it again.
         */
        {
            key: 'projects',
            shortcut: 'p',
            label: messages.projects.menuLabel,
            title: messages.menu.projects,
            onSelect: () => setProjectsOpen((open) => !open),
        },
        /*
         * Der Live-Modus der Agenten (W11a), als Eintrag der Atlas-Zeile.
         *
         * Er schaltet, wie [l]ocal llm daneben, und sein Etikett traegt den
         * ZUSTAND und nicht die Wirkung: "off" heisst "es ist aus". Es gibt ihn
         * hier und in der Kommandozeile ("live agents"), weil ein Modus, der per
         * Vorgabe aus ist, sonst nur denen gehoert, die das Kuerzel kennen.
         */
        {
            key: 'agents',
            shortcut: 'g',
            label: agentsMenuLabel(liveAgentsOn),
            title: liveAgentsOn ? messages.menu.agentsOn : messages.menu.agentsOff,
            onSelect: () => setLiveAgentsOn((on) => !on),
        },
    ];

    /*
     * Was ein Buchstabe tut, in einem Ref und nicht in einer Abhaengigkeit.
     *
     * Der Griff am Fenster wird einmal eingehaengt (unten, mit leerer
     * Abhaengigkeitsliste), weil ein Menuekuerzel keinen Grund hat, bei jedem
     * Bild ab- und wieder anzumelden. Die Handlungen darin haengen aber an
     * `llmState`, an `tour` und an drei Zustandssetzern; sie in die Schliessung
     * des Effekts zu schreiben hiesse, dass der Buchstabe die Lage von vor
     * hundert Bildern schaltet. Das Ref ist die Stelle, an der beides
     * zusammengeht: bei jedem Bild neu geschrieben, vom Griff bei jedem
     * Tastendruck frisch gelesen.
     */
    /**
     * Eine Handlung, mit einem Vermerk davor.
     *
     * Der Vermerk steht dort, wo die Handlung LAEUFT, und nicht dort, wo
     * geklickt wird: ein Beweislauf, der Klicks zaehlt, hat Klicks bewiesen. Er
     * ist auch der Grund, aus dem Knopf und Taste dieselbe Funktion rufen statt
     * zwei gleich aussehender: dass die Maus und die Tastatur dasselbe tun,
     * soll keine Behauptung sein, sondern eine Eigenschaft der Verdrahtung.
     */
    const menuAct = (letter: string, act: () => void) => () => {
        activatedMenus.current = [...activatedMenus.current, letter];
        act();
    };

    menuActionsRef.current = {
        a: menuAct('a', () => setGalaxyOn((current) => !current)),
        '?': menuAct('?', () => setHelpOpen((current) => !current)),
        ...Object.fromEntries(
            atlasEntries.map((entry) => [entry.shortcut, menuAct(entry.shortcut, entry.onSelect)]),
        ),
    };

    const menus: Partial<Record<string, MenuWiring>> = {
        a: {
            title: galaxyOn ? messages.menu.atlasHide : messages.menu.atlasShow,
            state: galaxyOn ? 'on' : 'off',
            onSelect: menuActionsRef.current['a'] ?? (() => undefined),
            extras: atlasEntries.map((entry) => ({
                ...entry,
                onSelect: menuActionsRef.current[entry.shortcut] ?? entry.onSelect,
            })),
        },
        /*
         * `[?]help` schaltet, statt nur zu oeffnen, und zwar aus demselben
         * Grund wie `[a]tlas`: der Buchstabe steht fuer die Flaeche, und ein
         * zweiter Druck auf denselben Buchstaben macht sie wieder zu. Ein
         * Menuepunkt, der nur oeffnet, waere der einzige der Zeile, dessen
         * `data-state` eine Lage anzeigt, die er selbst nicht umkehren kann.
         */
        '?': {
            title: helpOpen ? messages.menu.helpClose : messages.menu.helpOpen,
            state: helpOpen ? 'on' : 'off',
            onSelect: menuActionsRef.current['?'] ?? (() => undefined),
        },
    };

    const commandOverlay = overlayOpen ? (
        <SearchOverlay
            /*
             * Drei Saetze und nicht zwei, seit die Liste vor der Antwort
             * dasteht. "searching for x" waere ueber einer gefuellten Liste
             * eine halbe Auskunft: es stimmt, dass gesucht wird, und es
             * verschweigt, woher die Zeilen darunter kommen.
             */
            headline={
                hitSource === 'loaded'
                    ? hitRows.length === 0
                        ? messages.search.provisionalEmpty(answeredQuery)
                        : messages.search.provisionalHeadline(answeredQuery, hitRows.length)
                    : searchStatus === 'searching' && answeredQuery !== command.trim()
                        ? messages.search.searching(command.trim())
                        : searchHeadline(answeredQuery, hits.length, hitRows.length)
            }
            rows={hitRows}
            selected={selectedHit}
            status={searchStatus}
            message={searchMessage}
            onChoose={chooseHit}
            onPoint={setSelectedHit}
        />
    ) : oneCharTyped ? (
        /*
         * Ein einziges Zeichen sucht noch nicht, und das steht jetzt DORT, wo
         * die Treffer stehen wuerden.
         *
         * Nutzerbefund vom 2026-08-29: der Grund stand nur klein am rechten Rand
         * der Zeile, und wer tippt, sieht nach oben, wo die Antwort erscheint.
         * Dort stand nichts, also sah es aus, als antworte die Suche gar nicht.
         * Dasselbe Fenster, dieselbe Stelle, ein Satz statt einer Leere.
         */
        <SearchOverlay
            headline={COMMAND_ONE_MORE_LETTER}
            rows={noRows}
            selected={-1}
            status="ready"
            message=""
            onChoose={chooseHit}
            onPoint={setSelectedHit}
        />
    ) : undefined;

    /*
     * Wo die Fuehrung steht, als qualifizierter Name.
     *
     * Nur fuer den Vorwaerts-Gang: die Fuehrung durchs Projekt hat keinen
     * Subgraphen, in dem ein Schritt liegen koennte. Ein Dateischritt hat kein
     * Symbol und liefert deshalb nichts; das Bild sagt dann, dass nichts von
     * diesem Walk im Fokus steht, statt auf den naechstbesten Punkt zu zeigen.
     */
    const walkStep = tour?.kind === 'entry' ? tour.document.steps[tourStep]?.primary : undefined;
    const walkStepQualifiedName =
        walkStep !== undefined && walkStep.kind === 'symbol' ? walkStep.qualifiedName : undefined;

    const galaxy = (
        <GalaxyPanel
            project={project}
            visible={galaxyOn}
            focusQualifiedName={twinSymbol?.qualifiedName}
            focusName={twinSymbol?.name}
            onOpenNode={openGalaxyNode}
            onLayout={onLayout}
            walk={walk}
            focusWalk={focusWalk}
            refit={graphRefit}
            stepQualifiedName={walkStepQualifiedName}
            onToggleVisible={() => setGalaxyOn((current) => !current)}
            display={display}
            agents={agents}
            agentLayer={display.agents}
            agentEffects={{
                tails: display.agentTails,
                trails: display.agentTrails,
                waves: display.agentWaves,
                timeline: display.agentTimeline,
            }}
            /*
             * Der Vollbildmodus nimmt Escape nur, wenn sonst niemand sie
             * braucht. Dieselbe Reihenfolge wie ueberall in dieser Datei: was
             * ueber dem Panel liegt, hat den Vortritt.
             */
            escapeTaken={helpOpen || entryOpen || overlayOpen || settingsOpen || projectsOpen}
            fullscreenToggle={fullscreenToggle}
        />
    );

    const llm = (
        <SidecarPanel
            state={llmState}
            facts={llmFacts}
            project={project}
            policyDetail={llmPolicy?.detail ?? ''}
            detail={llmProbe.detail}
            onToggle={toggleLlm}
        />
    );

    const twin = (
        <TwinPanel
            status={twinStatus}
            message={twinMessage}
            hint={twinHint}
            symbolName={twinName}
            symbolQualifiedName={twinSymbol?.qualifiedName}
            ir={twinIr}
            presentation={presentation}
            caretLine={caretLine}
            onDepth={(depth) => setOverrides((current) => ({ ...current, depth }))}
            onToggleFacet={(facet) =>
                setOverrides((current) => {
                    const on = presentation.facets.has(facet);
                    const added = (current.facetsAdded ?? []).filter((entry) => entry !== facet);
                    const removed = (current.facetsRemoved ?? []).filter((entry) => entry !== facet);
                    return {
                        ...current,
                        facetsAdded: on ? added : [...added, facet],
                        facetsRemoved: on ? [...removed, facet] : removed,
                    };
                })
            }
            onFollow={followTarget}
            onPointRow={pointRow}
            imports={imports}
            flow={flowView}
            flowOpen={flowOpen}
            onToggleFlow={toggleFlow}
            flowStep={flowStep}
            view={twinView}
            onView={setTwinView}
            pseudocode={refined ?? pseudocode}
            onOpenLine={openLine}
            refineAvailable={llmState === 'ready'}
            refineState={refineState}
            refineMessage={refineMessage}
            onRefine={refinePseudocode}
            onRestoreOriginal={restorePseudocode}
            voiceModel={selectedModelName}
            voiceRequestModel={requestModel}
        />
    );

    /*
     * Die fuenf Reiter des Erklaeren-Bereichs, und was gerade dahinter liegt.
     *
     * Die Liste entsteht aus Tatsachen und nicht aus Schaltern
     * (src/layout/explain-tabs.ts): ein Reiter ist bedienbar, WEIL etwas
     * dahinterliegt, und nicht, weil ihn jemand aufgeschlagen hat.
     */
    const explainTabList = explainTabs({
        hasProject: project.length > 0,
        flowSubject: twinSymbol === undefined ? '' : twinName,
        flowStep,
        walkRunning: tour !== undefined,
        walkStep: tourStep,
        walkSteps: tour?.document.steps.length ?? 0,
        chatTurns: chatTurns.length,
    });
    explainTabsRef.current = explainTabList;

    /*
     * Der Inhalt des aktiven Reiters, und NUR er.
     *
     * Fuenf Flaechen gleichzeitig im Baum zu halten und vier davon
     * wegzublenden waere die bequeme Loesung: kein Zustand ginge verloren, und
     * niemand muesste darueber nachdenken, wo er liegt. Genau darum ist es die
     * falsche: der Zustand laege dann in fuenf Komponenten, das Wegblenden
     * waere eine CSS-Regel, an der man sich vertun kann, und die Zusicherung
     * "immer nur eine Flaeche" waere eine Behauptung ueber eine Regel statt
     * eine Eigenschaft der Form. Der Zustand liegt oben in dieser Datei; ein
     * Reiterwechsel haengt nur die Darstellung aus.
     */
    const explainPanel =
        explainTab === 'flow' ? (
            <FlowOverlay
                symbolName={twinName}
                flow={flowView}
                message={flowMessage}
                step={flowStep}
                onStep={stepFlow}
                onOpenLine={openLine}
                aiAvailable={llmState === 'ready'}
                modelName={selectedModelName}
                requestModel={requestModel}
            />
        ) : explainTab === 'walk' && tour !== undefined ? (
            <TourCard
                title={tour.document.title}
                steps={tour.document.steps}
                index={tourStep}
                endNote={tour.endNote}
                onPrev={() => setTourStep(stepMove(tour.document.steps.length, tourStep, -1))}
                onNext={() =>
                    isLastStep(tourStep, tour.document.steps.length)
                        ? closeTour()
                        : setTourStep(stepMove(tour.document.steps.length, tourStep, 1))
                }
                onExit={closeTour}
                /*
                 * Das Bild zum Schritt. Der Kasten steht schon auf dem Symbol
                 * dieses Schrittes: die Fuehrung setzt bei jedem Schritt das
                 * Subjekt des Twins (der Effekt mit `followRef` weiter oben),
                 * und der Flow haengt am Subjekt. Aufzuschlagen ist also genau
                 * dieser Reiter, und ein zweiter Weg, dasselbe Symbol noch
                 * einmal zu setzen, waere ein zweiter Weg, an dem es abweichen
                 * kann. Die Fuehrung laeuft dabei weiter: sie hat ihren Reiter
                 * und ihren Schritt, und beide stehen noch, wenn der Leser
                 * zurueckwechselt.
                 */
                onDiagram={() => openExplain('flow')}
            />
        ) : explainTab === 'chat' && chatTurns.length > 0 ? (
            <AtlasChatPanel
                turns={chatTurns}
                depth={chatDepth}
                onDepth={setChatDepth}
                onOpenCard={openCardSource}
                /*
                 * Loeschen leert den Verlauf. Zuklappen tut es nicht, und das
                 * ist seit W7c der ganze Unterschied zwischen den beiden; seit
                 * W8 gehoert das Zuklappen der Zone, und der Knopf hier ist der
                 * einzige, der loescht.
                 */
                onClear={() => setChatTurns([])}
                onPickCandidate={pickCandidate}
                /*
                 * Das Angebot aus AC6b: dieselbe Frage noch einmal, mit der
                 * Tiefe, die jetzt eingestellt ist. `askQuestion` liest
                 * `chatDepth` selbst, also braucht der Aufruf keine Zahl; und
                 * er legt einen NEUEN Zug an, waehrend der alte mit seiner
                 * alten Tiefe stehenbleibt. Genau darum geht es: zwei Antworten
                 * nebeneinander, von denen jede sagt, womit sie gerechnet
                 * wurde, statt einer, die sich unter der Hand aendert und ihre
                 * Zitate dabei entwertet.
                 */
                onAskAgain={(turn) => askQuestion(turn.question)}
                aiAvailable={llmState === 'ready'}
                modelName={selectedModelName}
                requestModel={requestModel}
            />
        ) : explainTab === 'bug' && project.length > 0 ? (
            <BugWizard
                project={project}
                target={bugTarget}
                paths={bugPathsDto}
                status={bugStatus}
                message={bugMessage}
                onHop={openHop}
                onChangeTarget={() => {
                    // Derselbe Weg wie jede andere Suche dieser Oberflaeche.
                    // Der Assistent bleibt offen und uebernimmt das neue
                    // Subjekt, sobald der Twin es hat.
                    commandInputRef.current?.focus();
                }}
                onClose={collapseExplain}
            />
        ) : explainTab === 'change' && project.length > 0 ? (
            <ImpactPanel
                project={project}
                mode={impactMode}
                onMode={chooseImpactMode}
                refDraft={refDraft}
                onRefDraft={(value) => {
                    setRefDraft(value);
                    setRefError('');
                }}
                onGo={applyRef}
                refError={refError}
                model={impactModel}
                status={impactStatus}
                message={impactMessage}
                routeNote={impactRouteNote}
                onOpen={openImpactRow}
                onClose={collapseExplain}
            />
        ) : undefined;

    /** Ein Griff, mit seinen Grenzen aus dem Modell. */
    const splitter = (
        key: LayoutKey,
        testId: string,
        orientation: 'vertical' | 'horizontal',
        label: string,
        invert = false,
    ): JSX.Element => {
        const bounds = layoutBounds(key, frame);
        return (
            <Splitter
                testId={testId}
                orientation={orientation}
                label={label}
                value={shownZones[key]}
                min={bounds.min}
                max={bounds.max}
                invert={invert}
                onChange={(value) => changeZone(key, value)}
                onReset={() => resetZone(key)}
            />
        );
    };

    return (
        <AtlasChrome
            version={ATLAS_VERSION}
            buildSuffix={ATLAS_BUILD_SUFFIX}
            chips={chips}
            tabs={tabDescriptors}
            onSelectTab={openFile}
            onCloseTab={closeTab}
            tree={{
                projectName: project.length > 0 ? project : messages.app.noProject,
                rows,
                cursor,
                activePath,
                note: treeNoteParts.join('; '),
                noteIsAbsence: treeError.length > 0 || coverageError.length > 0 || project.length === 0,
                truncations: coverage.truncations,
                onCursorChange: setCursor,
                onOpen: (row) => openFile(row.path),
                onToggle: toggleDirectory,
                onKeyDown: onTreeKeyDown,
            }}
            breadcrumb={activePath.length > 0 ? pathSegments(activePath) : []}
            truncationNote={document?.truncationNote ?? ''}
            coverageNote={coverageNote}
            commandValue={command}
            onCommandChange={setCommand}
            onCommandKeyDown={onCommandKeyDown}
            commandInputRef={commandInputRef}
            commandOverlay={commandOverlay}
            commandPlaceholder={commandPlaceholderFor(exampleSymbol, COMMAND_PLACEHOLDER)}
            commandExamples={commandExamples}
            /*
             * Ein Beispiel wird in die Zeile GESCHRIEBEN und nicht
             * abgeschickt: der Leser soll den Namen darin gegen seinen
             * eigenen tauschen koennen, bevor er drueckt. Der Fokus geht
             * dabei zurueck ins Feld, weil er sonst auf einem Knopf staende,
             * der im selben Moment verschwindet.
             */
            onCommandExample={(text) => {
                setCommand(text);
                commandInputRef.current?.focus();
            }}
            commandHint={
                enterIntent === 'ask'
                    ? llmState === 'ready' ? CHAT_HINT_READY : CHAT_HINT_OFF
                    : overlayOpen ? COMMAND_HINT_OPEN : COMMAND_HINT_IDLE
            }
            menus={menus}
            status={status}
            llm={llm}
            twin={twin}
            galaxy={galaxy}
            zones={{
                leftWidth: shownZones.leftWidth,
                rightWidth: shownZones.rightWidth,
                twinHeight: shownZones.twinHeight,
            }}
            splitLeft={splitter('leftWidth', 'atlas-split-left', 'vertical', messages.layout.splitter.left)}
            splitExplain={splitter(
                'explainHeight',
                'atlas-split-explain',
                'horizontal',
                messages.layout.splitter.explain,
                // Nach oben ziehen macht den Bereich groesser: die Kante folgt
                // dem Zeiger, statt vor ihm wegzulaufen.
                true,
            )}
            splitRight={splitter(
                'rightWidth',
                'atlas-split-right',
                'vertical',
                messages.layout.splitter.right,
                true,
            )}
            splitTwin={galaxyOn
                ? splitter('twinHeight', 'atlas-split-twin', 'horizontal', messages.layout.splitter.twin)
                : undefined}
            explain={
                <ExplainZone
                    tabs={explainTabList}
                    active={explainTab}
                    onSelect={(id) => {
                        setExplainTab(id);
                        setExplainOpen(true);
                    }}
                    open={explainOpen}
                    onToggle={() => setExplainOpen((open) => !open)}
                    height={shownZones.explainHeight}
                >
                    {explainPanel}
                </ExplainZone>
            }
        >
            {/*
              * Die Frage und der Einstiegsdialog liegen UEBER dem Editor statt
              * an seiner Stelle, aus demselben Grund wie der Platzhalter des
              * Readers: ein Editor, der nur manchmal im Layout haengt, misst
              * sich beim Auftauchen auf null und zeichnet nichts, bis ihn etwas
              * anstoesst.
              */}
            {whyVisible && (
                <WhyPanel project={project} onChoose={chooseIntent} onDecline={declineWhy} />
            )}
            {/*
              * Der Einstiegsdialog liegt aus demselben Grund ueber dem Editor
              * wie die Frage: eine Flaeche, die nur manchmal im Layout haengt,
              * misst sich beim Auftauchen auf null.
              *
              * Der Assistent und die Aenderungsansicht standen bis W8 hier
              * daneben. Sie sind seit W8 Reiter des Erklaeren-Bereichs: sie
              * ERKLAEREN den gelesenen Code, also gehoeren sie an den Platz, an
              * dem erklaert wird, und nicht vor den Text, den sie erklaeren.
              */}
            {entryOpen && (
                <EntryPointDialog
                    headline={entryHeadline(offered.total, offered.rows.length)}
                    rows={offered.rows}
                    query={entryQuery}
                    onQueryChange={setEntryQuery}
                    hits={entryHitRows}
                    status={entryStatus}
                    message={entryMessage}
                    routeNote={routeNote(overview)}
                    onChooseFlagged={chooseFlagged}
                    onChooseHit={chooseEntryHit}
                    onClose={() => setEntryOpen(false)}
                />
            )}
            <MonacoReader
                status={readerStatus}
                document={document}
                message={readerMessage}
                badges={badges}
                pulseLine={caretLine}
                highlightLine={pointedLine}
                onCursorLine={setCaretLine}
                revealLine={reveal?.line}
                revealNonce={reveal?.nonce}
            />
            {/*
              * Die Hilfe liegt als letztes Kind ueber allem anderen dieser
              * Flaeche, aus demselben Grund wie der Erklaerer: bei gleicher
              * Ebene entscheidet die Reihenfolge im DOM. Sie ist die einzige
              * Flaeche, die der Leser aufschlaegt, WEIL er nicht weiterweiss;
              * sie darf nicht hinter dem stehen, was ihn ratlos gemacht hat.
              */}
            {helpOpen && <HelpOverlay onClose={() => setHelpOpen(false)} />}
            {/*
              * Das Einstellungen-Panel liegt neben der Hilfe auf derselben
              * Ebene und aus demselben Grund: es ist eine Flaeche, die jemand
              * aufschlaegt, WEIL etwas nicht stimmt (das Modell antwortet nicht,
              * die Maschine kommt nicht mit). Sie hinter das zu legen, was ihn
              * hergefuehrt hat, waere die falsche Reihenfolge.
              */}
            {settingsOpen && (
                <SettingsPanel
                    project={project}
                    state={llmState}
                    facts={llmFacts}
                    router={llmRouter}
                    models={llmModels}
                    selectedModel={selectedModel}
                    onSelectModel={chooseModel}
                    /*
                     * Der Aktualisieren-Knopf gibt es nur, solange das lokale
                     * Modell an ist. Ohne den Griff steht er nicht da: ein Knopf,
                     * der nichts tut, existiert in dieser Oberflaeche nicht, und
                     * ein Knopf, der bei ausgeschaltetem Modell doch etwas taete,
                     * waere der Bruch der Aus-heisst-aus-Regel.
                     */
                    onRefresh={
                        llmMode === 'on' ? () => askSidecarRef.current?.() : undefined
                    }
                    display={display}
                    onDisplay={changeDisplay}
                    onMeasurement={(measurement) =>
                        setMeasurements((current) => ({
                            ...current,
                            [measurement.setting]: measurement,
                        }))}
                    onClose={() => setSettingsOpen(false)}
                />
            )}
            {/*
              * The projects panel sits on the same level as the settings and
              * for the same reason: a reader opens it BECAUSE something is
              * missing (no project, a stale index), and it must not sit
              * behind what sent them there.
              */}
            {projectsOpen && (
                <ProjectsPanel
                    project={project}
                    source={projectsSource}
                    onOpenProject={openProject}
                    onClose={() => setProjectsOpen(false)}
                />
            )}
        </AtlasChrome>
    );
}
