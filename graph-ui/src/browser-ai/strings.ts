import type { DroppedReason } from './explanation-response';
import { galaxyNodeNameText, type BranchNameWords } from '../galaxy/galaxy-strings';

export const browserAiText = {
    title: 'Browser AI',
    subtitle: 'Optional explanations, generated on this device.',
    close: 'Close browser AI',
    model: 'Model',
    download: 'Download & enable',
    prepare: 'Enable from cache / download',
    preparing: 'Preparing the browser model...',
    ready: 'Ready on this device',
    generating: 'Writing a short explanation...',
    off: 'Off',
    cancel: 'Cancel',
    disable: 'Disable',
    remove: 'Remove downloaded model',
    removing: 'Removing the model cache...',
    explain: 'Explain source',
    failed: 'Browser AI could not complete this step.',
    optIn: 'Enabling downloads about 566 MB from Hugging Face if the files are not already cached. Model files stay in this browser. Opening this panel makes no download request.',
    device: 'Requires WebGPU with shader-f16 and enough free GPU memory. Runtime code is bundled with CBM. The local-model sidecar remains available separately.',
    privacy: 'Source is processed on this device. It is not sent to Hugging Face or another inference service.',
    storage: 'Downloads use a dedicated browser cache. Disable stops the worker; Remove also deletes this model cache. Closing the panel disables the model.',
    excerpt: 'Source to explain',
    truncated: 'Only the first 6,000 characters of this excerpt will be used.',
    noSource: 'Open a file or focus a function in Explore, then return here.',
    output: 'Unverified explanation',
    outputNote: 'Generated text can be wrong. Check it against the source and indexed evidence.',
    provenance: 'Model details',
    revision: 'Pinned revision',
    format: 'q4f16 · 160 output tokens maximum',
    progress: (loaded: number, total: number) => `${(loaded / 1_000_000).toFixed(1)} / ${(total / 1_000_000).toFixed(1)} MB`,
    location: (path: string, line: number) => `${path}:${line}`,
};

/** Words for graph facts in the prompt (always English) and for the listed
 * relationship answer, which follows the language of the question. Numbers are
 * written as the language writes them: 5,548 and 5.548 (C1). */
const en = (value: number) => value.toLocaleString('en-US');
const de = (value: number) => value.toLocaleString('de-DE');
/** Node kinds of the index in German: the noun, its plural and its gender (W8, W9). */
const GERMAN_KINDS: Readonly<Record<string, readonly [string, string, 'f' | 'm' | 'n']>> = {
    project: ['Projekt', 'Projekte', 'n'], package: ['Paket', 'Pakete', 'n'], folder: ['Ordner', 'Ordner', 'm'], file: ['Datei', 'Dateien', 'f'],
    module: ['Modul', 'Module', 'n'], class: ['Klasse', 'Klassen', 'f'], function: ['Funktion', 'Funktionen', 'f'], method: ['Methode', 'Methoden', 'f'],
    interface: ['Schnittstelle', 'Schnittstellen', 'f'], enum: ['Aufzählung', 'Aufzählungen', 'f'], type: ['Typ', 'Typen', 'm'], variable: ['Variable', 'Variablen', 'f'],
    route: ['Route', 'Routen', 'f'], resource: ['Ressource', 'Ressourcen', 'f'], section: ['Abschnitt', 'Abschnitte', 'm'], field: ['Feld', 'Felder', 'n'],
    struct: ['Struktur', 'Strukturen', 'f'], trait: ['Trait', 'Traits', 'm'], macro: ['Makro', 'Makros', 'n'], constant: ['Konstante', 'Konstanten', 'f'],
    namespace: ['Namensraum', 'Namensräume', 'm'], property: ['Eigenschaft', 'Eigenschaften', 'f'], decorator: ['Dekorator', 'Dekoratoren', 'm'],
    test: ['Test', 'Tests', 'm'], channel: ['Kanal', 'Kanäle', 'm'], node: ['Knoten', 'Knoten', 'm'], symbol: ['Symbol', 'Symbole', 'n'],
    branch: ['Branch-Knoten', 'Branch-Knoten', 'm'],
};
/** The Branch node in the words of the answer: "django-demo · detached HEAD" (round 4, N1). A German
 * answer keeps the words of the Galaxy label, so the node it names can be found in the picture. */
const englishNodeNames: BranchNameWords = galaxyNodeNameText;
const germanNodeNames: BranchNameWords = {
    detached: galaxyNodeNameText.detached,
    workingTree: galaxyNodeNameText.workingTree,
    branch: (name: string) => `Branch ${name}`,
    inProject: (project: string, what: string) => `${project} · ${what}`,
};
const englishRelationshipWords = {
    more: (count: number) => `+${en(count)} more`,
    hops: (depth: number) => depth === 0 ? 'the selection only' : depth === 1 ? '1 hop' : `${depth} hops`,
    both: 'in both directions', inbound: 'incoming only', outbound: 'outgoing only',
    allTypes: 'all relationship types',
    onlyTypes: (types: readonly string[]) => types.length ? `only ${types.join(', ')}` : 'no relationship types',
    size: (nodes: number, edges: number) => `${en(nodes)} ${nodes === 1 ? 'symbol' : 'symbols'} and ${en(edges)} ${edges === 1 ? 'relationship' : 'relationships'}`,
    /** The scope finished loading; it is not the whole graph (W4). */
    complete: 'fully loaded',
    loading: 'still loading, so this is a partial preview',
    partial: (error?: string) => `incomplete${error ? `: ${error}` : ''}`,
    /** A layer stopped at the render limit: the layers inside it are whole, it and those further out are not (C1). As the
     * Galaxy tooltip says it, loading stops after the request that passes the limit (5,548 of 5,000), and the scene draws up
     * to the limit. The limit gets a sentence of its own, so nodes never stand beside the symbols of the scope size (W1). */
    renderLimited: (layer: number, limit: number, kind: 'nodes' | 'edges') => `partial. Layer ${layer} stopped loading after the request that took it past `
        + `the render limit of ${en(limit)} ${kind}; the scene draws at most ${en(limit)} ${kind}, `
        + (layer > 1 ? 'so counts and names further out can be incomplete' : 'so counts and names can be incomplete, the direct relationships included'),
    exhausted: 'nothing further beyond this depth',
    /** A node kind as the answer's language writes it: "Class", "Klasse" (W8). */
    kindName: (kind: string) => kind,
    /** How a Branch node is named (round 4, N1). */
    nodeNames: englishNodeNames,
    scope: (shape: string, size: string, state: string) => `Scope: ${shape}; ${size}; ${state}.`,
    /** Each number with its unit: "Incoming: 23 relationships from 12 symbols", never "23 from 12", which reads like a score (W2). */
    incoming: (total: number, symbols?: number) => `Incoming: ${en(total)} ${total === 1 ? 'relationship' : 'relationships'}`
        + `${symbols === undefined ? '' : ` from ${en(symbols)} ${symbols === 1 ? 'symbol' : 'symbols'}`}.`,
    outgoing: (total: number, symbols?: number) => `Outgoing: ${en(total)} ${total === 1 ? 'relationship' : 'relationships'}`
        + `${symbols === undefined ? '' : ` to ${en(symbols)} ${symbols === 1 ? 'symbol' : 'symbols'}`}.`,
    noIncoming: 'Incoming: no relationships in this scope.',
    noOutgoing: 'Outgoing: no relationships in this scope.',
    incomingNotLoaded: 'Incoming: not loaded; the scope does not follow incoming edges.',
    outgoingNotLoaded: 'Outgoing: not loaded; the scope does not follow outgoing edges.',
    cut: (side: 'incoming' | 'outgoing') => `${side === 'incoming' ? 'Incoming' : 'Outgoing'}: relationships left out of this snapshot.`,
    truncated: 'the snapshot left part of its relationships out, so counts and names can be incomplete',
    moreTypes: (count: number) => `+${en(count)} more relationship ${count === 1 ? 'type' : 'types'}`,
    internal: (summary: string) => `Between the selected symbols: ${summary}.`,
    beyond: (summary: string) => `Further out in the scope: ${summary}.`,
    selected: (what: string) => `Selected: ${what}.`,
    documentation: (text: string) => `Documentation: ${text}`,
    notInScope: (label: string, kind: string) => `Selected: ${label} (${kind}); its symbols are not in the loaded scope.`,
    selectedGroup: (kind: string, label: string, count: number, listed: string, omitted: number) =>
        `Selected ${kind}: ${label} with ${en(count)} symbols: ${listed}${omitted > 0 ? `; +${en(omitted)} more` : ''}.`,
    /** The heading of a listed answer counts what it lists, the CALLS edges; every other relationship
     * of that side is counted apart, so 23 relationships never head a list of 11 callers (W3). */
    callers: (name: string, count: number) => `${en(count)} ${count === 1 ? 'caller' : 'callers'} (CALLS) of ${name} in the loaded graph.`,
    callees: (name: string, count: number) => `${name} calls ${en(count)} ${count === 1 ? 'symbol' : 'symbols'} (CALLS) in the loaded graph.`,
    all: (side: 'incoming' | 'outgoing', total: number, symbols?: number) => `All ${side}: ${en(total)} ${total === 1 ? 'relationship' : 'relationships'}`
        + `${symbols === undefined ? '' : ` ${side === 'incoming' ? 'from' : 'to'} ${en(symbols)} ${symbols === 1 ? 'symbol' : 'symbols'}`}.`,
    /** Why a side cannot be listed; the reason follows. */
    unlisted: (side: 'incoming' | 'outgoing', name: string) => side === 'incoming' ? `The callers of ${name} cannot be listed from this scope.`
        : `What ${name} calls cannot be listed from this scope.`,
    nothing: (side: 'incoming' | 'outgoing', name: string) => side === 'incoming' ? `${name} has no callers and no other incoming relationships in this scope.`
        : `${name} calls nothing and has no other outgoing relationships in this scope.`,
    cutListed: (side: 'incoming' | 'outgoing') => `The snapshot left the ${side} relationships out.`,
    /** One edge type of a listed answer with the number of its symbols: "TESTS (11)". */
    typeCount: (type: string, count: number) => `${type} (${en(count)})`,
    /** What an incoming DEFINES edge from a file, module or class says about the selection. */
    definer: (kind: string, count: number, name: string) => {
        const noun = kind.toLowerCase();
        return count === 1 ? `the ${noun} that defines ${name}` : `the ${/(?:s|x|ch|sh)$/.test(noun) ? `${noun}es` : `${noun}s`} that define ${name}`;
    },
    noCalls: (name: string, side: 'incoming' | 'outgoing') => side === 'incoming'
        ? `No CALLS edge reaches ${name} in this scope.` : `${name} has no outgoing CALLS edge in this scope.`,
    otherRelationships: (side: 'incoming' | 'outgoing') => `Other ${side} relationships:`,
    /** The Galaxy controls as the UI labels them: the direction select and Expand +1 (W8). */
    notLoaded: (side: 'incoming' | 'outgoing'): string => side === 'incoming'
        ? 'The current scope does not follow incoming relationships. Choose "Incoming" or "Both directions" in Galaxy, then ask again.'
        : 'The current scope does not follow outgoing relationships. Choose "Outgoing" or "Both directions" in Galaxy, then ask again.',
    notExpanded: 'The current scope shows the selection only. Click "Expand +1" in Galaxy, then ask again.',
    stillLoading: 'The scope is still loading; this list can grow.',
    listedFromGraph: 'Listed from the indexed graph; not generated by the model.',
    didYouMean: (side: 'incoming' | 'outgoing', name: string) => side === 'incoming' ? `Did you mean: callers of ${name}?` : `Did you mean: what ${name} calls?`,
    didYouMeanBoth: (name: string) => `Did you mean: callers of ${name} and what it calls?`,
    /** A choice, not an order: the buttons "Show the list" and "Ask the model" follow (W9). */
    uncertain: 'The question was not recognized for certain. You can show the list from the indexed graph or ask the model.',
    uncertainName: (typed: string, name: string) => `${typed} does not match the name of the selection (${name}). You can show the list from the indexed graph or ask the model.`,
    showList: 'Show the list',
    /** Offered under a listed answer or a suggestion, which the model did not write. */
    askModel: 'Ask the model',
};
export type RelationshipWords = typeof englishRelationshipWords;

export const relationshipWords: { en: RelationshipWords; de: RelationshipWords } = {
    en: englishRelationshipWords,
    de: {
        more: (count: number) => `+${de(count)} weitere`,
        hops: (depth: number) => depth === 0 ? 'nur die Auswahl' : depth === 1 ? '1 Schritt' : `${depth} Schritte`,
        both: 'in beide Richtungen', inbound: 'nur eingehend', outbound: 'nur ausgehend',
        allTypes: 'alle Beziehungstypen',
        onlyTypes: (types: readonly string[]) => types.length ? `nur ${types.join(', ')}` : 'keine Beziehungstypen',
        size: (nodes: number, edges: number) => `${de(nodes)} ${nodes === 1 ? 'Symbol' : 'Symbole'} und ${de(edges)} ${edges === 1 ? 'Beziehung' : 'Beziehungen'}`,
        complete: 'vollständig geladen',
        loading: 'lädt noch, das ist eine Vorschau',
        partial: (error?: string) => `unvollständig${error ? `: ${error}` : ''}`,
        renderLimited: (layer: number, limit: number, kind: 'nodes' | 'edges') => {
            const unit = kind === 'nodes' ? 'Knoten' : 'Kanten';
            return `unvollständig. Ebene ${layer} hörte nach der Anfrage auf zu laden, die sie über das Darstellungslimit von ${de(limit)} ${unit} brachte; `
                + `die Szene zeichnet höchstens ${de(limit)} ${unit}, `
                + (layer > 1 ? 'daher können Anzahlen und Namen weiter außen fehlen' : 'daher können Anzahlen und Namen fehlen, auch bei den direkten Beziehungen');
        },
        exhausted: 'dahinter folgt nichts mehr',
        kindName: (kind: string) => GERMAN_KINDS[kind.toLowerCase()]?.[0] ?? kind,
        nodeNames: germanNodeNames,
        scope: (shape: string, size: string, state: string) => `Ausschnitt: ${shape}; ${size}; ${state}.`,
        incoming: (total: number, symbols?: number) => `Eingehend: ${de(total)} ${total === 1 ? 'Beziehung' : 'Beziehungen'}`
            + `${symbols === undefined ? '' : ` von ${de(symbols)} ${symbols === 1 ? 'Symbol' : 'Symbolen'}`}.`,
        outgoing: (total: number, symbols?: number) => `Ausgehend: ${de(total)} ${total === 1 ? 'Beziehung' : 'Beziehungen'}`
            + `${symbols === undefined ? '' : ` zu ${de(symbols)} ${symbols === 1 ? 'Symbol' : 'Symbolen'}`}.`,
        noIncoming: 'Eingehend: keine Beziehungen in diesem Ausschnitt.',
        noOutgoing: 'Ausgehend: keine Beziehungen in diesem Ausschnitt.',
        incomingNotLoaded: 'Eingehend: nicht geladen; der Ausschnitt folgt keinen eingehenden Kanten.',
        outgoingNotLoaded: 'Ausgehend: nicht geladen; der Ausschnitt folgt keinen ausgehenden Kanten.',
        cut: (side: 'incoming' | 'outgoing') => `${side === 'incoming' ? 'Eingehend' : 'Ausgehend'}: Beziehungen in diesem Schnappschuss ausgelassen.`,
        truncated: 'der Schnappschuss hat einen Teil der Beziehungen ausgelassen, Anzahlen und Namen können unvollständig sein',
        moreTypes: (count: number) => `+${de(count)} weitere ${count === 1 ? 'Beziehungstyp' : 'Beziehungstypen'}`,
        internal: (summary: string) => `Zwischen den ausgewählten Symbolen: ${summary}.`,
        beyond: (summary: string) => `Weiter außen im Ausschnitt: ${summary}.`,
        selected: (what: string) => `Ausgewählt: ${what}.`,
        documentation: (text: string) => `Dokumentation: ${text}`,
        notInScope: (label: string, kind: string) => `Ausgewählt: ${label} (${kind}); die Symbole der Auswahl sind nicht im geladenen Ausschnitt.`,
        selectedGroup: (kind: string, label: string, count: number, listed: string, omitted: number) =>
            `Ausgewählt (${kind}): ${label} mit ${de(count)} Symbolen: ${listed}${omitted > 0 ? `; +${de(omitted)} weitere` : ''}.`,
        callers: (name: string, count: number) => `${de(count)} Aufrufer (CALLS) von ${name} im geladenen Graphen.`,
        callees: (name: string, count: number) => `${name} ruft im geladenen Graphen ${de(count)} ${count === 1 ? 'Symbol' : 'Symbole'} auf (CALLS).`,
        all: (side: 'incoming' | 'outgoing', total: number, symbols?: number) => `Alle ${side === 'incoming' ? 'eingehenden' : 'ausgehenden'}: ${de(total)} ${total === 1 ? 'Beziehung' : 'Beziehungen'}`
            + `${symbols === undefined ? '' : ` ${side === 'incoming' ? 'von' : 'zu'} ${de(symbols)} ${symbols === 1 ? 'Symbol' : 'Symbolen'}`}.`,
        unlisted: (side: 'incoming' | 'outgoing', name: string) => side === 'incoming' ? `Die Aufrufer von ${name} lassen sich aus diesem Ausschnitt nicht auflisten.`
            : `Was ${name} aufruft, lässt sich aus diesem Ausschnitt nicht auflisten.`,
        nothing: (side: 'incoming' | 'outgoing', name: string) => side === 'incoming' ? `${name} hat in diesem Ausschnitt keine Aufrufer und keine anderen eingehenden Beziehungen.`
            : `${name} ruft in diesem Ausschnitt nichts auf und hat keine anderen ausgehenden Beziehungen.`,
        cutListed: (side: 'incoming' | 'outgoing') => `Der Schnappschuss hat die ${side === 'incoming' ? 'eingehenden' : 'ausgehenden'} Beziehungen ausgelassen.`,
        typeCount: (type: string, count: number) => `${type} (${de(count)})`,
        /** Article and relative pronoun follow the gender of the kind: "der Ordner, der", "die Schnittstelle, die" (W9). */
        definer: (kind: string, count: number, name: string) => {
            const known = GERMAN_KINDS[kind.toLowerCase()];
            const [noun, plural, gender] = known ?? [`Symbol (${kind})`, `Symbole (${kind})`, 'n'];
            const article = gender === 'f' ? 'die' : gender === 'm' ? 'der' : 'das';
            return count === 1 ? `${article} ${noun}, ${article} ${name} definiert` : `die ${plural}, die ${name} definieren`;
        },
        noCalls: (name: string, side: 'incoming' | 'outgoing') => side === 'incoming'
            ? `Keine CALLS-Kante führt in diesem Ausschnitt zu ${name}.` : `${name} hat in diesem Ausschnitt keine ausgehende CALLS-Kante.`,
        /** "Andere": the types other than CALLS, also where no CALLS edge exists (W3). */
        otherRelationships: (side: 'incoming' | 'outgoing') => `Andere ${side === 'incoming' ? 'eingehende' : 'ausgehende'} Beziehungen:`,
        notLoaded: (side: 'incoming' | 'outgoing') => side === 'incoming'
            ? 'Der aktuelle Ausschnitt folgt keinen eingehenden Beziehungen. Wähle in Galaxy „Incoming“ oder „Both directions“ und frage dann noch einmal.'
            : 'Der aktuelle Ausschnitt folgt keinen ausgehenden Beziehungen. Wähle in Galaxy „Outgoing“ oder „Both directions“ und frage dann noch einmal.',
        notExpanded: 'Der aktuelle Ausschnitt zeigt nur die Auswahl. Klicke in Galaxy auf „Expand +1“ und frage dann noch einmal.',
        stillLoading: 'Der Ausschnitt lädt noch; die Liste kann wachsen.',
        listedFromGraph: 'Aus dem indizierten Graphen gelistet, nicht vom Modell erzeugt.',
        didYouMean: (side: 'incoming' | 'outgoing', name: string) => side === 'incoming' ? `Meintest du: Aufrufer von ${name}?` : `Meintest du: von ${name} aufgerufene Symbole?`,
        didYouMeanBoth: (name: string) => `Meintest du: Aufrufer von ${name} und was ${name} aufruft?`,
        uncertain: 'Die Frage wurde nicht sicher erkannt. Du kannst dir die Liste aus dem indizierten Graphen anzeigen lassen oder das Modell fragen.',
        uncertainName: (typed: string, name: string) => `${typed} entspricht nicht dem Namen der Auswahl (${name}). Du kannst dir die Liste aus dem indizierten Graphen anzeigen lassen oder das Modell fragen.`,
        showList: 'Liste anzeigen',
        askModel: 'Modell fragen',
    },
};

/** Notes of the local chat dock about how an answer was produced or bounded. */
const tokens = (value: number) => value.toLocaleString('en-US');
const code = (text: string) => `\`${text.replace(/`/g, "'")}\``;
export const browserChatText = {
    shortened: 'Token limit reached: the answer was cut short',
    /** What the expanded token-limit note says, with the limits the answer ran into. */
    limitReached: (input: number, output: number) => `This answer used all ${tokens(output)} output tokens it was allowed. The input limit is ${tokens(input)} tokens for the question, its source and earlier messages.`,
    /** `cap` is what automatic explanations write at most; below it they stop at the reader's own limit,
     * which is no design choice to keep them short (W6). */
    limitAutomatic: (automaticInput: number, automatic: number, input: number, output: number, cap: number) => `Automatic explanations stop after ${tokens(automatic)} output tokens`
        + `${automatic < cap ? ', your output limit,' : ' so they stay short,'} and read at most ${tokens(automaticInput)} input tokens of source and facts. `
        + `A question in the chat may read up to ${tokens(input)} input tokens and answer with up to ${tokens(output)} output tokens.${output > automatic ? ' Ask in the chat for a longer answer.' : ''}`,
    automaticRoom: (cap: number) => `Raise the output limit in the agent configuration and automatic explanations can use up to ${tokens(cap)} output tokens.`,
    outputRoom: (output: number, max: number) => output < max ? `You can raise the output limit up to ${tokens(max)} tokens in the agent configuration.`
        : `The output limit is at its maximum of ${tokens(max)} tokens.`,
    /** At the maximum a larger model writes no longer answer: every model stops at the same limit (W6). */
    narrower: (sameForLarger: boolean) => `${sameForLarger ? 'Larger models have the same limit. ' : ''}Ask about one part of the code, or ask for the rest of the answer.`,
    changeOutputLimit: 'Change the output limit',
    largerModels: (sameLimit: boolean) => `A larger model may stay closer to the question${sameLimit ? ', but its output limit is the same' : ''}. `
        + 'Each is a one-time download and needs more memory than its download size:',
    modelDownload: (name: string, size: string) => `${name} · ${size} download`,
    newConversation: 'New conversation',
    /** The model's state in the agent configuration (K10). */
    cached: 'Cached',
    loadCached: 'Load model (cached, no download)',
    autoLoad: 'Load the chosen model on start when it is cached',
    autoLoadNote: 'Only from this browser\'s cache; nothing is downloaded on start. A project switch stays in this page and keeps a loaded model without loading it again, so this option only matters when the page is opened or reloaded.',
    resumeFailed: 'The cached model could not be loaded without a download. Load it in the agent configuration.',
    /** Under an answer that names what its source and graph facts do not contain (K12). */
    /** Six names and the rest counted; with many unknown names the answer is most likely invented (W7). */
    unsupportedNames: (names: readonly string[]) => {
        const listed = `${names.slice(0, 6).join(', ')}${names.length > 6 ? `, +${(names.length - 6).toLocaleString('en-US')} more` : ''}`;
        return names.length >= 8 ? `${names.length.toLocaleString('en-US')} names in this answer are not in the source or graph facts it was given: ${listed}. The answer is likely made up; do not rely on it.`
            : `Not in the source or graph facts this answer was given: ${listed}. Check these names before relying on them.`;
    },
    /** Who wrote which part of a grounded automatic explanation (K7). */
    factsAndSentence: 'Facts listed from the indexed graph; the last sentence is generated by the model.',
    factsOnly: 'Listed from the indexed graph; not generated by the model.',
    /** What the left out sentence claimed or named, and what it was checked against (W5). */
    sentenceDropped: (reason?: DroppedReason) => `Listed from the indexed graph. The model's sentence ${!reason ? 'was not supported by the source or the facts'
        : reason.kind === 'claim' ? `claimed something the source does not show (here: "${reason.text}")`
            : `named something that is in neither the source nor the facts (here: ${code(reason.text)})`} and was left out.`,
    explanationDropped: (reason?: DroppedReason) => `The model's explanation ${!reason ? 'was not supported by the source'
        : `${reason.kind === 'claim' ? 'claimed' : 'named'} something the source does not show (here: ${reason.kind === 'claim' ? `"${reason.text}"` : code(reason.text)})`}`
        + ' and was left out. Ask a question about the code instead.',
    /** The same for facts read from an open workflow file (K12). */
    fileFactsAndSentence: 'Facts read from the file; the text after them is generated by the model.',
    fileFactsOnly: 'Read from the file; not generated by the model.',
    fileSentenceDropped: (reason?: DroppedReason) => `Read from the file. The model's text ${!reason ? 'was not supported by the file'
        : `${reason.kind === 'claim' ? 'claimed' : 'named'} something the file does not show (here: ${reason.kind === 'claim' ? `"${reason.text}"` : code(reason.text)})`}`
        + ' and was left out.',
    writingSentence: 'Reading the source; the model is adding one sentence…',
    readingFacts: 'Listing the facts…',
    /** Above the first question about another file or selection (K17); topicText has it in both languages (W8). */
    topicBreak: (label: string) => `New topic: ${label}. Earlier messages are not sent with these questions.`,
    capacity: (nodes: number, edges: number, model: string, shown: number) =>
        `${nodes} nodes / ${edges} edges: too large for the local ${model} model; showing ${shown}`,
    historyTrimmed: (count: number) => `${count} earlier ${count === 1 ? 'message was' : 'messages were'} left out to fit the input limit.`,
    waitingForScope: 'Waiting for the complete scope before explaining…',
    partialScope: 'This scope did not load completely, so it is not explained automatically.',
    tokenLimits: (model: string) => `Token limits for ${model}`,
    inputLimit: 'Input (context)',
    outputLimit: 'Output (answer)',
    limitRange: (min: number, max: number) => `${min.toLocaleString('en-US')} to ${max.toLocaleString('en-US')} tokens`,
    limitsNote: (input: number, output: number) => `Stored in this browser for each model. Automatic explanations use at most ${input.toLocaleString('en-US')} input and ${output.toLocaleString('en-US')} output tokens.`,
};

/** The divider above the first question about another file or selection, in the language of that question (K17, W8). */
export const topicText = {
    en: { topicBreak: browserChatText.topicBreak },
    de: { topicBreak: (label: string) => `Neues Thema: ${label}. Frühere Nachrichten werden bei diesen Fragen nicht mitgeschickt.` },
};

/** Who wrote which part of the answer to a general question about a selection, in its language (C5). */
export const groundedText = {
    en: { factsAndSentence: browserChatText.factsAndSentence, factsOnly: browserChatText.factsOnly, sentenceDropped: browserChatText.sentenceDropped,
        writingSentence: browserChatText.writingSentence },
    de: {
        factsAndSentence: 'Fakten aus dem indizierten Graphen gelistet; der letzte Satz ist vom Modell erzeugt.',
        factsOnly: 'Aus dem indizierten Graphen gelistet, nicht vom Modell erzeugt.',
        sentenceDropped: (reason?: DroppedReason) => `Aus dem indizierten Graphen gelistet. Der Satz des Modells ${!reason ? 'wurde weggelassen, weil Quelltext und Fakten ihn nicht stützen'
            : reason.kind === 'claim' ? `behauptete etwas, das der Quelltext nicht zeigt (hier: „${reason.text}“), und wurde weggelassen`
                : `nannte etwas, das weder im Quelltext noch in den Fakten steht (hier: ${code(reason.text)}), und wurde weggelassen`}.`,
        writingSentence: 'Der Quelltext wird gelesen; das Modell fügt einen Satz hinzu…',
    },
};

/** The reply to a prompt that asks nothing ("test", "hallo"): questions it could ask (C6). */
export const noQuestionText = {
    en: {
        heading: (typed: string) => `No question was recognized in "${typed}". You can ask, for example:`,
        note: 'Answered without the model.',
        whatDoes: (name: string) => `What does ${name} do?`,
        whoCalls: (name: string) => `Who calls ${name}?`,
        whatCalls: (name: string) => `What does ${name} call?`,
        inDetail: (name: string) => `Explain ${name} in detail.`,
        /** For a class: what it is, who uses it and what it inherits from; for a file or folder: what it contains (W9). */
        whatIs: (name: string) => `What is ${name}?`,
        whoUses: (name: string) => `Who uses ${name}?`,
        whatInherits: (name: string) => `What does ${name} inherit from?`,
        whatContains: (name: string) => `What does ${name} contain?`,
        markedDoes: 'What does the marked code do?',
        markedInDetail: 'Explain the marked code line by line.',
    },
    de: {
        heading: (typed: string) => `In „${typed}“ wurde keine Frage erkannt. Du kannst zum Beispiel fragen:`,
        note: 'Ohne das Modell beantwortet.',
        whatDoes: (name: string) => `Was macht ${name}?`,
        whoCalls: (name: string) => `Wer ruft ${name} auf?`,
        whatCalls: (name: string) => `Was ruft ${name} auf?`,
        inDetail: (name: string) => `Erklär ${name} ausführlich.`,
        whatIs: (name: string) => `Was ist ${name}?`,
        whoUses: (name: string) => `Wer verwendet ${name}?`,
        whatInherits: (name: string) => `Wovon erbt ${name}?`,
        whatContains: (name: string) => `Was enthält ${name}?`,
        markedDoes: 'Was macht der markierte Code?',
        markedInDetail: 'Erklär den markierten Code Zeile für Zeile.',
    },
};

/** The chat's own reply when a question has no code or graph context (K11), in the language of the question. */
export const browserChatContextText = {
    en: {
        nothingSelected: 'Nothing is selected for me to explain. Select a node in Galaxy or a part in Architecture, or open a file in Explore, then ask again.',
        noFileOpen: 'No file is open in Explore. Open a file, or mark code in it, then ask again.',
        sourceUnavailable: (path: string) => `The source of \`${path.replace(/`/g, "'")}\` is not available. Open the file again or choose another one, then ask again.`,
        notAsked: 'Answered without the model: without code or graph facts it could only guess.',
    },
    de: {
        nothingSelected: 'Es ist nichts ausgewählt, das ich erklären könnte. Wähle einen Knoten in Galaxy oder einen Teil in Architecture, oder öffne eine Datei in Explore, und frage dann noch einmal.',
        noFileOpen: 'In Explore ist keine Datei geöffnet. Öffne eine Datei oder markiere Code darin und frage dann noch einmal.',
        sourceUnavailable: (path: string) => `Der Quelltext von \`${path.replace(/`/g, "'")}\` ist nicht verfügbar. Öffne die Datei noch einmal oder wähle eine andere und frage dann noch einmal.`,
        notAsked: 'Ohne das Modell beantwortet: ohne Code oder Graph-Fakten könnte es nur raten.',
    },
};

/** The notes of an ⓘ Source block in plain words (W10). The prompt reads them in English; an
 * answer shows them in its language. */
const evidenceNotes = {
    staticGraph: { en: 'The relationships here come from reading the code; they do not show what runs at runtime.',
        de: 'Die Beziehungen hier wurden aus dem Code gelesen; sie zeigen nicht, was zur Laufzeit ausgeführt wird.' },
};
export const staticGraphNote = evidenceNotes.staticGraph.en;
/** A known note in the language of the answer; any other note as it is. */
export const evidenceNote = (note: string, language: 'en' | 'de'): string => Object.values(evidenceNotes).find(item => item.en === note)?.[language] ?? note;

const count = (value: number) => value.toLocaleString('en-US');
const plural = (value: number, one: string, many: string) => `${count(value)} ${value === 1 ? one : many}`;
/** Architecture selections as sentences: for the prompt and for the explanation card. */
export const architectureWords = {
    kinds: { area: 'source area', file: 'file', symbol: 'symbol', route: 'route' },
    more: (value: number) => `+${count(value)} more`,
    selected: (kind: string, name: string, detail?: string) => `Selected ${kind}: ${name}${detail ? ` (${detail})` : ''}.`,
    selectedSymbol: (symbol: string) => `Selected symbol: ${symbol}.`,
    openedArea: (path: string) => `Opened source area: ${path}.`,
    openedFile: (path: string) => `Opened file: ${path}.`,
    openedHotspots: (path: string) => `Opened hotspot area: ${path}.`,
    scopePart: (name: string, kind: string, files?: number, lines?: number) =>
        `${name} (${kind}${files !== undefined ? `, ${plural(files, 'file', 'files')}` : ''}${lines !== undefined ? `, ${plural(lines, 'indexed line', 'indexed lines')}` : ''})`,
    /** The parts a view shows of an opened area or file: the ones inside it, largest first, and the ones outside it. */
    parts: (items: readonly string[], total: number, scope: 'area' | 'file' = 'area', outside: readonly string[] = [], outsideTotal = outside.length) => {
        const more = (listed: readonly string[], all: number) => all > listed.length ? `, +${count(all - listed.length)} more` : '';
        if (!outsideTotal) return `Parts shown (${count(total)}, largest first): ${items.join(', ')}${total > items.length ? `; ${count(total - items.length)} more` : ''}.`;
        const inside = total ? `${count(total)} inside the ${scope}, largest first: ${items.join(', ')}${more(items, total)}; ` : '';
        return `Parts shown (${count(total + outsideTotal)}): ${inside}${count(outsideTotal)} outside it: ${outside.join(', ')}${more(outside, outsideTotal)}.`;
    },
    outside: (name: string) => `${name} (outside)`,
    partConnections: (described: readonly string[], omitted: number) => `Connections of its parts: ${described.join('; ')}${omitted ? `; ${plural(omitted, 'more connection', 'more connections')} not listed` : ''}.`,
    measured: (lines: number, measured: number, files: number, languages: readonly string[]) =>
        `${plural(lines, 'indexed line', 'indexed lines')} in ${plural(measured, 'measured file', 'measured files')} of ${count(files)}${languages.length ? `; files by language: ${languages.join(', ')}` : ''}.`,
    fanIn: (value: number) => `fan-in ${count(value)}`,
    complexity: (value: number) => `complexity ${count(value)}`,
    hotspots: (total: number, ranked: readonly string[]) => `${plural(total, 'hotspot finding', 'hotspot findings')}: ${ranked.join(', ')}.`,
    members: (listed: string) => `Indexed members include ${listed}.`,
    to: (name: string) => `to ${name}`,
    from: (name: string) => `from ${name}`,
    connections: (described: readonly string[], omitted: number) => `Connections ${described.join('; ')}${omitted ? `; ${plural(omitted, 'more connection', 'more connections')} not listed` : ''}.`,
    view: (view: string, nodes: number, edges: number) => `The ${view} view shows ${plural(nodes, 'part', 'parts')} and ${plural(edges, 'connection', 'connections')}.`,
    connection: (source: string, target: string, type: string, total?: number) => `Selected connection: ${source} → ${target}, ${type}${total !== undefined ? ` ×${count(total)}` : ''}.`,
    example: (from: string, type: string, to: string, where?: string) => `${from} ${type} ${to}${where ? ` (${where})` : ''}`,
    examples: (items: readonly string[]) => `For example: ${items.join('; ')}.`,
    operation: (symbol: string) => `Starting operation: ${symbol}.`,
    call: (caller: string, callee: string, where?: string) => `Selected call: ${caller} calls ${callee}${where ? ` at ${where}` : ''}.`,
    callAt: (line: number, args: readonly string[], callee?: string) => `line ${line}${callee ? ` calls ${callee}` : ''}${args.length ? ` with ${args.join(', ')}` : ''}`,
    directCalls: (total: number, calls: readonly string[]) => `${plural(total, 'direct call', 'direct calls')} with call-site evidence${calls.length ? `: ${calls.join('; ')}` : ''}.`,
    path: (names: readonly string[]) => `Call chain: ${names.join(' → ')}.`,
    component: (name: string, members?: number, files?: number) => `Component: ${name}${members !== undefined ? `, ${plural(members, 'member', 'members')}` : ''}${files !== undefined ? ` in ${plural(files, 'file', 'files')}` : ''}.`,
    part: (kind: 'component' | 'group', name: string, members?: number, files?: number, components?: number, role?: string) =>
        `Selected ${kind}: ${name}${components !== undefined ? `, ${plural(components, 'component', 'components')}` : ''}${members !== undefined ? `, ${plural(members, 'member', 'members')}` : ''}${files !== undefined ? ` in ${plural(files, 'file', 'files')}` : ''}${role === 'test' ? ', tests' : ''}.`,
    representatives: (listed: string) => `Representative symbols: ${listed}.`,
    incoming: (items: readonly string[], total: number) => `Incoming from ${items.join(', ')}${total > items.length ? `; ${count(total - items.length)} more` : ''}.`,
    outgoing: (items: readonly string[], total: number) => `Outgoing to ${items.join(', ')}${total > items.length ? `; ${count(total - items.length)} more` : ''}.`,
};

const counted = (value: number, one: string, many: string) => `${value.toLocaleString('en-US')} ${value === 1 ? one : many}`;
const gezaehlt = (value: number, one: string, many: string) => `${value.toLocaleString('de-DE')} ${value === 1 ? one : many}`;
/** A GitHub Actions workflow as facts counted from its keys: for the prompt and the explanation card (K12). */
export const workflowWords = {
    heading: 'Facts read from the file (counted, not guessed):',
    name: (name: string) => `Workflow name: ${name}.`,
    triggers: (items: readonly string[]) => `${items.length === 1 ? 'Trigger' : 'Triggers'}: ${items.join(', ')}.`,
    trigger: (event: string, types: readonly string[]) => `${event}${types.length ? ` (types: ${types.join(', ')})` : ''}`,
    jobs: (total: number, items: readonly string[]) => `${counted(total, 'job', 'jobs')}: ${items.join('; ')}${total > items.length ? `; +${(total - items.length).toLocaleString('en-US')} more` : ''}.`,
    job: (id: string, name: string | undefined, runsOn: string | undefined, steps: number) =>
        `${id} (${[name ? `"${name}"` : '', runsOn ? `runs on ${runsOn}` : '', counted(steps, 'step', 'steps')].filter(Boolean).join(', ')})`,
    uses: (items: readonly string[]) => `Actions used: ${items.join(', ')}.`,
};
export type WorkflowWords = typeof workflowWords;
/** The same facts for an outline asked for in German (C7). */
export const germanWorkflowWords: WorkflowWords = {
    heading: 'Aus der Datei gelesene Fakten (gezählt, nicht geraten):',
    name: (name: string) => `Name des Workflows: ${name}.`,
    triggers: (items: readonly string[]) => `Auslöser: ${items.join(', ')}.`,
    trigger: (event: string, types: readonly string[]) => `${event}${types.length ? ` (Typen: ${types.join(', ')})` : ''}`,
    jobs: (total: number, items: readonly string[]) => `${gezaehlt(total, 'Job', 'Jobs')}: ${items.join('; ')}${total > items.length ? `; +${(total - items.length).toLocaleString('de-DE')} weitere` : ''}.`,
    job: (id: string, name: string | undefined, runsOn: string | undefined, steps: number) =>
        `${id} (${[name ? `"${name}"` : '', runsOn ? `läuft auf ${runsOn}` : '', gezaehlt(steps, 'Schritt', 'Schritte')].filter(Boolean).join(', ')})`,
    uses: (items: readonly string[]) => `Verwendete Actions: ${items.join(', ')}.`,
};

/** The outline of a configuration file, answered from the file instead of the model (C7). */
const englishOutlineWords = {
    heading: (name: string, kind: string, lines: number) => `${name}: ${kind}, ${counted(lines, 'line', 'lines')}. Read from the file:`,
    kinds: { yaml: 'YAML configuration', json: 'JSON data', toml: 'TOML configuration', workflow: 'GitHub Actions workflow' },
    list: (total: number) => `list of ${total.toLocaleString('en-US')}`,
    keys: (total: number) => counted(total, 'key', 'keys'),
    empty: 'empty',
    text: 'text block',
    more: (total: number) => `+${total.toLocaleString('en-US')} more`,
    cut: 'The rest of the file is left out here.',
    note: 'Read from the file; not generated by the model.',
};
export const fileOutlineWords: { en: typeof englishOutlineWords; de: typeof englishOutlineWords } = {
    en: englishOutlineWords,
    de: {
        heading: (name: string, kind: string, lines: number) => `${name}: ${kind}, ${gezaehlt(lines, 'Zeile', 'Zeilen')}. Aus der Datei gelesen:`,
        kinds: { yaml: 'YAML-Konfiguration', json: 'JSON-Daten', toml: 'TOML-Konfiguration', workflow: 'GitHub-Actions-Workflow' },
        list: (total: number) => `Liste mit ${gezaehlt(total, 'Eintrag', 'Einträgen')}`,
        keys: (total: number) => gezaehlt(total, 'Schlüssel', 'Schlüssel'),
        empty: 'leer',
        text: 'Textblock',
        more: (total: number) => `+${total.toLocaleString('de-DE')} weitere`,
        cut: 'Der Rest der Datei ist hier ausgelassen.',
        note: 'Aus der Datei gelesen, nicht vom Modell erzeugt.',
    },
};

const listed = (items: readonly string[], total: number) => `${items.join(', ')}${total > items.length ? `, +${(total - items.length).toLocaleString('en-US')} more` : ''}`;
/** A configuration or text file as facts read from its text: for the prompt and the explanation card (K12). */
export const fileWords = {
    kind: (kind: string, lines?: number) => `File kind: ${kind}${lines === undefined ? '' : `; ${counted(lines, 'line', 'lines')}`}.`,
    selected: (start: number, end: number) => start === end ? `Selected line ${start} of the file.` : `Selected lines ${start}-${end} of the file.`,
    topKeys: (total: number, items: readonly string[]) => `Top-level keys (${total.toLocaleString('en-US')}): ${listed(items, total)}.`,
    keys: (total: number, items: readonly string[]) => `Keys (${total.toLocaleString('en-US')}): ${listed(items, total)}.`,
    /** What a key holds: "(2 keys: `web`, `db`)", "(list of 2)", "(text)". */
    children: (total: number, items: readonly string[]) => total ? `${counted(total, 'key', 'keys')}: ${listed(items, total)}` : 'no keys',
    keyCount: (total: number) => counted(total, 'key', 'keys'),
    list: (total: number) => `list of ${total.toLocaleString('en-US')}`,
    /** The keys the items of a list have: "item keys: `repo`, `rev`, `hooks`". */
    itemKeys: (total: number, items: readonly string[]) => `item keys: ${listed(items, total)}`,
    value: { text: 'text', number: 'number', boolean: 'true or false', empty: 'null' },
    items: (total: number) => `A list of ${counted(total, 'item', 'items')}.`,
    invalidJson: 'The text is not valid JSON, so no keys are counted.',
    tables: (total: number, items: readonly string[]) => `Tables (${total.toLocaleString('en-US')}): ${listed(items, total)}.`,
    sections: (total: number, items: readonly string[]) => `Sections (${total.toLocaleString('en-US')}): ${listed(items, total)}.`,
    title: (title: string) => `Title: ${title}.`,
    codeBlocks: (total: number) => `${counted(total, 'code block', 'code blocks')}.`,
    patterns: (total: number, items: readonly string[]) => `${counted(total, 'pattern', 'patterns')}: ${listed(items, total)}.`,
    root: (name: string) => `Root element: ${name}.`,
};

/** Third hand test round (B1 to B7): the chat's own words for an answer handed to the model,
 * a topic the reader comes back to, a follow-up without context, facts read from code and the
 * purpose of well-known files. New strings only; the ones above keep their own wording. */
const englishRound3Text = {
    /** Under the question of a turn asked of the model from an answer the chat gave itself (B1). */
    askedModel: 'Asked the local model',
    /** Under every answer the model wrote freely (B1). */
    modelNote: 'Generated by the local model; it can be wrong.',
    askAgain: 'Ask again',
    /** Above the first question about a file or selection that has earlier turns (B4). */
    backTo: (label: string) => `Back to: ${label}. Earlier messages about it are sent again.`,
    /** A follow-up ("und was noch?") right after a change of topic (B4). */
    followUp: (typed: string, label: string) => `"${typed}" refers to earlier messages. Those were about another topic and are not sent with questions about ${label}. Please ask the full question, for example:`,
    /** Above the lines read from the selected code (B2). */
    inSource: 'In the source:',
    moreMembers: (total: number) => `+${total.toLocaleString('en-US')} more ${total === 1 ? 'attribute or method' : 'attributes and methods'}`,
    moreLines: (total: number) => `+${counted(total, 'more line', 'more lines')}`,
    /** An open code file or marked code as facts read from it, for a short general question (B3). */
    codeKind: (language: string) => language ? `${language} source file` : 'source file',
    marked: (start: number, end: number, name: string, kind: string) => `${start === end ? `Marked line ${start}` : `Marked lines ${start}-${end}`} of ${name} (${kind}).`,
    moduleDocstring: (text: string) => `Module docstring: "${text}"`,
    definitions: (total: number, items: readonly string[], where: 'file' | 'marked') =>
        `${where === 'file' ? 'Top-level definitions' : 'Definitions in the marked code'} (${total.toLocaleString('en-US')}): ${items.join(', ')}${total > items.length ? `, +${(total - items.length).toLocaleString('en-US')} more` : ''}.`,
    /** Who wrote which part of an answer about a file (K12), in the language of the question. */
    fileNotes: { factsAndSentence: browserChatText.fileFactsAndSentence, factsOnly: browserChatText.fileFactsOnly, sentenceDropped: browserChatText.fileSentenceDropped },
    /** The first line of a file outline; the note under it says where it was read (B7). */
    outlineHeading: (name: string, kind: string, lines: number) => `${name}: ${kind}, ${counted(lines, 'line', 'lines')}.`,
    iniKind: 'INI configuration',
    /** What a well-known file is for, by its name and structure, before its outline (B7). */
    purposes: {
        preCommit: 'pre-commit configuration: hooks that run before each commit.',
        workflow: 'GitHub runs the jobs of this workflow when one of its triggers occurs.',
        npmPackage: 'npm package manifest: the name, scripts and dependencies of a JavaScript package.',
        pyproject: 'Python project configuration: how the package is built, its metadata and the settings of tools.',
        tox: 'tox configuration: the test environments tox creates and the commands it runs in each.',
        setupCfg: 'setuptools configuration: package metadata and options, often also settings of other tools.',
        compose: 'Docker Compose file: the services that Docker Compose starts together.',
        readTheDocs: 'Read the Docs configuration: how readthedocs.org builds the documentation.',
        editorConfig: 'EditorConfig: indentation, line endings and similar editor settings per file pattern.',
        tsconfig: 'TypeScript compiler configuration: which files are compiled and with which options.',
        flake8: 'flake8 configuration: the rules of the Python linter flake8.',
        pytest: 'pytest configuration: the options pytest runs the tests with.',
        coverage: 'coverage.py configuration: which code test coverage measures and how it is reported.',
    },
};
export const chatRound3Text: { en: typeof englishRound3Text; de: typeof englishRound3Text } = {
    en: englishRound3Text,
    de: {
        askedModel: 'An das lokale Modell gestellt',
        modelNote: 'Vom lokalen Modell erzeugt; kann falsch sein.',
        askAgain: 'Erneut fragen',
        backTo: (label: string) => `Zurück zu: ${label}. Die früheren Nachrichten dazu werden wieder mitgeschickt.`,
        followUp: (typed: string, label: string) => `„${typed}“ bezieht sich auf frühere Nachrichten. Die betrafen ein anderes Thema und werden mit Fragen zu ${label} nicht mitgeschickt. Stell die Frage bitte vollständig, zum Beispiel:`,
        inSource: 'Im Quelltext:',
        moreMembers: (total: number) => `+${total.toLocaleString('de-DE')} ${total === 1 ? 'weiteres Attribut oder weitere Methode' : 'weitere Attribute und Methoden'}`,
        moreLines: (total: number) => `+${gezaehlt(total, 'weitere Zeile', 'weitere Zeilen')}`,
        codeKind: (language: string) => language ? `${language}-Quelltext` : 'Quelltext',
        marked: (start: number, end: number, name: string, kind: string) => `${start === end ? `Markierte Zeile ${start}` : `Markierte Zeilen ${start} bis ${end}`} von ${name} (${kind}).`,
        moduleDocstring: (text: string) => `Docstring des Moduls: "${text}"`,
        definitions: (total: number, items: readonly string[], where: 'file' | 'marked') =>
            `${where === 'file' ? 'Definitionen auf oberster Ebene' : 'Definitionen im markierten Code'} (${total.toLocaleString('de-DE')}): ${items.join(', ')}${total > items.length ? `, +${(total - items.length).toLocaleString('de-DE')} weitere` : ''}.`,
        fileNotes: {
            factsAndSentence: 'Fakten aus der Datei gelesen; der Text danach ist vom Modell erzeugt.',
            factsOnly: 'Aus der Datei gelesen, nicht vom Modell erzeugt.',
            sentenceDropped: (reason?: DroppedReason) => `Aus der Datei gelesen. Der Text des Modells ${!reason ? 'wurde weggelassen, weil die Datei ihn nicht stützt'
                : reason.kind === 'claim' ? `behauptete etwas, das die Datei nicht zeigt (hier: „${reason.text}“), und wurde weggelassen`
                    : `nannte etwas, das die Datei nicht zeigt (hier: ${code(reason.text)}), und wurde weggelassen`}.`,
        },
        outlineHeading: (name: string, kind: string, lines: number) => `${name}: ${kind}, ${gezaehlt(lines, 'Zeile', 'Zeilen')}.`,
        iniKind: 'INI-Konfiguration',
        purposes: {
            preCommit: 'pre-commit-Konfiguration: Hooks, die vor jedem Commit laufen.',
            workflow: 'GitHub führt die Jobs dieses Workflows aus, wenn einer seiner Auslöser eintritt.',
            npmPackage: 'npm-Paketmanifest: Name, Skripte und Abhängigkeiten eines JavaScript-Pakets.',
            pyproject: 'Python-Projektkonfiguration: wie das Paket gebaut wird, seine Metadaten und die Einstellungen von Werkzeugen.',
            tox: 'tox-Konfiguration: die Testumgebungen, die tox anlegt, und die Befehle, die es in jeder ausführt.',
            setupCfg: 'setuptools-Konfiguration: Paketmetadaten und Optionen, oft auch Einstellungen anderer Werkzeuge.',
            compose: 'Docker-Compose-Datei: die Services, die Docker Compose zusammen startet.',
            readTheDocs: 'Read-the-Docs-Konfiguration: wie readthedocs.org die Dokumentation baut.',
            editorConfig: 'EditorConfig: Einrückung, Zeilenenden und ähnliche Editor-Einstellungen je Dateimuster.',
            tsconfig: 'TypeScript-Compilerkonfiguration: welche Dateien mit welchen Optionen kompiliert werden.',
            flake8: 'flake8-Konfiguration: die Regeln des Python-Linters flake8.',
            pytest: 'pytest-Konfiguration: die Optionen, mit denen pytest die Tests ausführt.',
            coverage: 'coverage.py-Konfiguration: welcher Code bei der Testabdeckung gemessen und wie darüber berichtet wird.',
        },
    },
};

type ViewDirection = 'both' | 'inbound' | 'outbound';
/** The listed answer to a question about the current Galaxy view (H1), and the note for a model
 * answer that only restated its question (H2), in the language of the question. */
const englishViewText = {
    center: (name: string, kind: string, roots: number) => `${name} (${kind}) is in the middle${roots > 1 ? ` with ${en(roots)} symbols` : ''}`,
    reach: (hops: string, direction: string, both: boolean) => `the scope reaches ${hops}${both ? ' ' : ', '}${direction}`,
    selectionOnly: 'the scope shows the selection only',
    /** The heading of a side; the hierarchy draws incoming on the left and outgoing on the right. */
    side: (side: 'incoming' | 'outgoing', hierarchy: boolean): string => side === 'incoming' ? hierarchy ? 'Left, incoming:' : 'Incoming:' : hierarchy ? 'Right, outgoing:' : 'Outgoing:',
    /** How the hierarchy is read, as its hint says it: from two layers on each column is one layer further,
     * and nodes reached through both directions stand in the dashed band below. */
    readHierarchy: (direction: ViewDirection, columns: boolean) => direction === 'both'
        ? `How to read the hierarchy: incoming relationships stand on the left, outgoing on the right${columns ? ', each column one layer further in the same direction' : ''}.`
            + (columns ? ' Nodes reached through both directions, such as a callee of a caller, stand in the dashed band "Mixed directions" below.' : '')
        : `How to read the hierarchy: the scope follows ${direction === 'inbound' ? 'incoming' : 'outgoing'} relationships only, so everything stands on the ${direction === 'inbound' ? 'left' : 'right'} of the middle${columns ? ', each column one layer further' : ''}.`,
    readGalaxy: 'The galaxy view shows this scope as a cloud. "hierarchy" at the top right lays the same scope out in columns: incoming on the left, outgoing on the right.',
    size: (size: string, types: string, state: string) => `${size}, ${types}; ${state}.`,
    noAnswer: (echoed: string) => `The model gave no answer; it only restated the question ("${echoed}").`,
    factsFollow: 'This is what the indexed graph lists for the selection:',
    rephrase: 'Ask the question in other words, or ask again.',
};
export const viewText: { en: typeof englishViewText; de: typeof englishViewText } = {
    en: englishViewText,
    de: {
        center: (name: string, kind: string, roots: number) => `${name} (${kind}) steht${roots > 1 ? ` mit ${de(roots)} Symbolen` : ''} in der Mitte`,
        reach: (hops: string, direction: string, both: boolean) => `der Ausschnitt geht ${hops}${both ? ' ' : ', '}${direction}`,
        selectionOnly: 'der Ausschnitt zeigt nur die Auswahl',
        side: (side: 'incoming' | 'outgoing', hierarchy: boolean) => side === 'incoming' ? hierarchy ? 'Links, eingehend:' : 'Eingehend:' : hierarchy ? 'Rechts, ausgehend:' : 'Ausgehend:',
        readHierarchy: (direction: ViewDirection, columns: boolean) => direction === 'both'
            ? `So liest du die Hierarchie: eingehende Beziehungen stehen links, ausgehende rechts${columns ? ', jede Spalte eine Ebene weiter in derselben Richtung' : ''}.`
                + (columns ? ' Was über beide Richtungen erreicht wird, etwa etwas, das ein Aufrufer sonst noch aufruft, steht im gestrichelten Band „Mixed directions“ darunter.' : '')
            : `So liest du die Hierarchie: der Ausschnitt folgt nur ${direction === 'inbound' ? 'eingehenden' : 'ausgehenden'} Beziehungen, daher steht alles ${direction === 'inbound' ? 'links' : 'rechts'} der Mitte${columns ? ', jede Spalte eine Ebene weiter' : ''}.`,
        readGalaxy: 'Die Galaxy-Ansicht zeigt diesen Ausschnitt als Wolke. „hierarchy“ oben rechts ordnet denselben Ausschnitt in Spalten: eingehend links, ausgehend rechts.',
        size: (size: string, types: string, state: string) => `${size}, ${types}; ${state}.`,
        noAnswer: (echoed: string) => `Das Modell hat keine Antwort gegeben, es hat nur die Frage wiederholt („${echoed}“).`,
        factsFollow: 'Das listet der indizierte Graph zur Auswahl:',
        rephrase: 'Stell die Frage mit anderen Worten oder frag erneut.',
    },
};
