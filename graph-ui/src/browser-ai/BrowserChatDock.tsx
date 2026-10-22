import { useEffect, useMemo, useRef, useState, type FormEvent, type JSX, type ReactNode } from 'react';
import type { BrowserAiProgress } from './browser-ai-controller';
import { createBrowserChatRuntime, type BrowserChatRuntime } from './browser-ai-runtime';
import { BROWSER_MODELS, getBrowserModel, isBrowserModelCached, removeBrowserModelCache, type BrowserModel } from './model-policy';
import { keepAgent, loadProgress, offerAgent, peekAgent, takeAgent, type AgentHandover, type LoadProgress } from './agent-handover';
import { buildChatMessages, echoRetryRequest, selectionLocation, sentInHistory, snapshotAttachment, snapshotReaderContext, trimChatHistory, type BrowserChatAttachment, type BrowserChatContext, type BrowserChatReaderContext, type BrowserChatSource, type BrowserChatTurn } from './chat-model';
import { explanationInput, EXPLANATION_DELAY_MS, type ExplanationInput } from './proactive-selection';
import { prepareExplanationContext, selectionSummary, type PreparedExplanationContext } from './explanation-context';
import { AUTO_INPUT_TOKENS, AUTO_OUTPUT_TOKENS, citedInterpretation, explanationMode, explanationSentence, namesNotIn, parseExplanationResponse, explanationMessages, formatExplanationEvidence, type DroppedReason } from './explanation-response';
import { carriedSource, selectionSubject, SYMBOL_SOURCE_LINES, sourceTargetOf, symbolSource, type SymbolSourceReader } from './symbol-source';
import { codeFacts, codeFactsMarkdown, codeSourceFacts } from './code-facts';
import { relationshipAnswer, relationshipSuggestion } from './relationship-answer';
import { clampTokenLimits, tokenLimitBounds, tokenLimitsFor, useAgentPreferences, type TokenLimits } from './agent-preferences';
import { browserChatText, chatRound3Text, evidenceNote, groundedText, relationshipWords, topicText, viewText } from './strings';
import { isGpuRuntimeFailure, BrowserRuntimeFatalError } from './runtime-fault';
import ChatMarkdown from './ChatMarkdown';
import AgentSettingsDialog from './AgentSettingsDialog';
import { useChatHistory } from './use-chat-history';
import { chatTopic, contextFreeFollowUp, followedTopic, missingContextAnswer, questionLanguage, topicDivider, topicHistory } from './chat-context';
import { isDataFile, readerFacts } from './file-facts';
import { fileOutline } from './file-outline';
import { readGalaxyEvidence, type GalaxyEvidence } from './galaxy-evidence';
import { followUpAnswer, generalQuestion, knownNames, noQuestion, noQuestionAnswer, viewQuestion, type ExampleSubject } from './question-intent';
import { viewAnswer } from './view-answer';
import { echoAnswer } from './echo-answer';
import './browser-chat.css';

export type { BrowserChatAttachment, BrowserChatContext, BrowserChatReaderContext, BrowserChatSource } from './chat-model';
export interface BrowserChatDockProps {
    proactiveSelection?: BrowserChatContext;
    selectionScope?: string;
    historyKey?: string;
    proactive?: boolean;
    onAgentStateChange?: (state: 'off' | 'loading' | 'active' | 'busy' | 'error') => void;
    settingsRequest?: number;
    onAgentModelChange?: (name: string) => void;
    open: boolean;
    onClose: () => void;
    showCollapsed?: boolean;
    onOpen?: () => void;
    attachment?: BrowserChatAttachment;
    readerContext?: BrowserChatReaderContext;
    context?: readonly BrowserChatContext[];
    pendingContext?: BrowserChatContext;
    onContextConsumed?: (id: string) => void;
    onContextRemoved?: (id: string) => void;
    onAttachmentConsumed: (id: string) => void;
    onAttachmentRemoved?: (id: string) => void;
    createRuntime?: (modelId: string) => BrowserChatRuntime;
    removeCache?: (modelId: string) => Promise<void>;
    /** Reads a selected symbol's source (get_code_snippet), so explanations stand on code, not names. */
    readSource?: SymbolSourceReader;
    /** Whether a model's files are in the browser cache (K10). */
    isCached?: (modelId: string) => Promise<boolean>;
}

type Phase = 'off' | 'preparing' | 'ready' | 'counting' | 'generating' | 'removing';
type ChatTurn = BrowserChatTurn & { evidence?: PreparedExplanationContext };
/** `grounded`: the facts are listed in the card, so the prompt's name budget is not the reader's limit. */
type Explanation = { key: string; label: string; answer: string; status: string; error?: string; packet?: PreparedExplanationContext; citation?: ReturnType<typeof citedInterpretation>; mode?: 'interpretation'; shortened?: boolean; limit?: TokenLimits; evidence?: string; grounded?: boolean;
    /** The facts of a configuration or text file, which the model was not asked about (K12). */
    askable?: boolean;
    /** The answer is the model's text alone, without listed facts: it says so (B1). */
    generated?: boolean };
/** The shape of an explanation for a short general question: facts listed from the graph or read
 * from an open code file or marked code, the lines of the selected code, one checked sentence (C5, B3). */
type Grounded = { summary: string[]; code?: string; subject?: { name: string; kind?: string } }
    & ({ from: 'graph'; graph: BrowserChatContext; reader?: undefined } | { from: 'file'; reader: BrowserChatReaderContext; graph?: undefined });
/** Finished explanations per selection, so returning to one does not run the model again. */
const EXPLANATION_CACHE_SIZE = 32;
const initialModel = BROWSER_MODELS.find(model => model.availability === 'available')!;
const cachedInBrowser = (modelId: string) => isBrowserModelCached(getBrowserModel(modelId)).catch(() => false);
const messageOf = (error: unknown): string => error instanceof Error ? error.message : String(error);
/** The names a Galaxy selection goes by: its label and its symbols with their qualified names. */
const galaxyNames = (galaxy: GalaxyEvidence): string[] => [galaxy.label, ...galaxy.roots.flatMap(root => [root.name, root.qualifiedName ?? ''])].filter(Boolean);
/** Answers the chat gives itself, which "Ask the model" sends to the model as the question they answer.
 * Not the hint to a prompt without a question: the model only echoed "test" or "hallo" (B5). */
const ASKABLE = new Set(['graph', 'suggestion', 'grounded', 'file']);
const sizeLabel = (bytes: number): string => bytes >= 1_000_000_000 ? `${(bytes / 1_000_000_000).toFixed(2)} GB` : `${Math.ceil(bytes / 1_000_000)} MB`;
/** Symbol sources read for explanations and questions, newest last. */
const SOURCE_CACHE_SIZE = 32;

/** The listed facts first, then the lines read from the selected code, then the model's sentence
 * if it stood the check, then who wrote what (K7). The facts come from the indexed graph, or for
 * an open workflow from the file itself (K12). The answer to a general question says it in the
 * language of that question (C5). `code` says what the code declares when the sentence is
 * dropped, as it was for JSONBAgg every time (B2). The note names what the left out sentence
 * claimed or named, where the check says it (W5). */
function groundedExplanation(summary: readonly string[], sentence: string | undefined, dropped: boolean | DroppedReason, from: 'graph' | 'file' = 'graph', language: 'en' | 'de' = 'en', code?: string): string {
    const words = from === 'file' ? chatRound3Text[language].fileNotes : groundedText[language];
    const reason = typeof dropped === 'object' ? dropped : undefined;
    const note = sentence ? words.factsAndSentence : dropped ? words.sentenceDropped(reason) : words.factsOnly;
    return [summary.map(line => `- ${line}`).join('\n'), ...code ? [code] : [], ...sentence ? [sentence] : [], `_${note}_`].join('\n\n');
}
/** What a turn says when the model only restated the question: that it gave no answer, then the
 * facts of the selection or open file the question was about, as a general question lists them (H2). */
function echoedAnswer(output: string, language: 'en' | 'de', graph: BrowserChatContext | undefined, reader: BrowserChatReaderContext | undefined): string {
    const text = viewText[language];
    const echoed = output.replace(/\s+/g, ' ').trim();
    const shown = echoed.length > 80 ? `${echoed.slice(0, 79)}…` : echoed;
    const source = reader?.source;
    const summary = graph ? selectionSummary(graph, language) : source ? isDataFile(source.path) ? readerFacts(reader) : codeSourceFacts(source, language).summary : [];
    if (!summary.length) return `${text.noAnswer(shown)} ${text.rephrase}`;
    return `${text.noAnswer(shown)} ${text.factsFollow}\n\n${groundedExplanation(summary, undefined, false, graph ? 'graph' : 'file', language)}`;
}
/** The same while the model writes its sentence. */
const writingExplanation = (summary: readonly string[], note: string, code?: string): string =>
    summary.length ? [summary.map(line => `- ${line}`).join('\n'), ...code ? [code] : [], `_${note}_`].join('\n\n') : '';

function Attachment({ attachment, label }: { attachment: BrowserChatAttachment; label?: string }): JSX.Element {
    return <details className="cbm-chat-attachment">
        <summary><span aria-hidden="true">⌁</span> {label && `${label} · `}{selectionLocation(attachment)}</summary>
        <pre>{attachment.text}</pre>
        <small>{attachment.project} · Source {attachment.sourceVersion}</small>
    </details>;
}

function SourceDisclosure({ children, title }: { children?: ReactNode; title?: string }): JSX.Element {
    return children ? <details className="cbm-chat-response-source">
        <summary aria-label="Source for this answer"><span className="cbm-chat-speaker" title={title}>Agent</span><span className="cbm-chat-source-toggle">ⓘ Source</span></summary>
        <div className="cbm-chat-source-content">{children}</div>
    </details> : <span className="cbm-chat-speaker" title={title}>Agent</span>;
}

/** Where an answer stopped and what can be changed: the limits it ran into, the way to
 * the output limit and the larger models with their download. Only what helps is offered:
 * an automatic explanation writes at most AUTO_OUTPUT_TOKENS whatever the limit, and at the
 * maximum a larger model stops at the same limit (W6). */
interface LimitNote { limit: TokenLimits; automatic?: boolean; chat: TokenLimits; model: BrowserModel; onChangeOutput: () => void }
function TokenLimitNote({ limit, automatic, chat, model, onChangeOutput }: LimitNote): JSX.Element {
    const larger = BROWSER_MODELS.filter(candidate => candidate.availability === 'available' && candidate.bytes > model.bytes);
    const sameLimit = larger.every(candidate => candidate.maxOutputTokens <= model.maxOutputTokens);
    const atMaximum = chat.outputTokens >= model.maxOutputTokens;
    const raise = automatic ? chat.outputTokens < AUTO_OUTPUT_TOKENS : !atMaximum;
    return <details className="cbm-chat-limit-note">
        <summary>{browserChatText.shortened}</summary>
        <p>{automatic ? browserChatText.limitAutomatic(limit.inputTokens, limit.outputTokens, chat.inputTokens, chat.outputTokens, AUTO_OUTPUT_TOKENS) : browserChatText.limitReached(limit.inputTokens, limit.outputTokens)}</p>
        {automatic ? raise && <p>{browserChatText.automaticRoom(AUTO_OUTPUT_TOKENS)}</p> : <p>{browserChatText.outputRoom(chat.outputTokens, model.maxOutputTokens)}</p>}
        {!automatic && atMaximum && sameLimit && <p>{browserChatText.narrower(larger.length > 0)}</p>}
        {/* At its maximum the limit cannot be raised, so there is nothing to change (C8). */}
        {raise && <button type="button" onClick={onChangeOutput}>{browserChatText.changeOutputLimit}</button>}
        {!automatic && larger.length > 0 && (!atMaximum || !sameLimit) && <><p>{browserChatText.largerModels(sameLimit)}</p>
            <ul>{larger.map(candidate => <li key={candidate.id}>{browserChatText.modelDownload(candidate.displayName, sizeLabel(candidate.bytes))}</li>)}</ul></>}
    </details>;
}

/** How an answer was bounded: cut at the output limit, scope too large, history left out,
 * and which of its names the model was not given. */
function AnswerNotes({ shortened, packet, model, historyOmitted, unsupported = [] }: { shortened?: LimitNote; packet?: PreparedExplanationContext; model: string; historyOmitted?: number; unsupported?: readonly string[] }): JSX.Element {
    const capacity = packet?.capacity;
    return <>
        {unsupported.length > 0 && <small className="cbm-chat-answer-note">{browserChatText.unsupportedNames(unsupported)}</small>}
        {shortened && <TokenLimitNote {...shortened} />}
        {capacity && <small className="cbm-chat-answer-note">{browserChatText.capacity(capacity.nodes, capacity.edges, model, capacity.shown)}</small>}
        {!!historyOmitted && <small className="cbm-chat-answer-note">{browserChatText.historyTrimmed(historyOmitted)}</small>}
    </>;
}

/** `codeOnly` where the listed facts already stand above it, in the explanation card. The
 * source does not push the first graph facts out: a listed answer shows both (K14). */
function PacketSource({ packet, citation, codeOnly = false, language = 'en' }: { packet: PreparedExplanationContext; citation?: ReturnType<typeof citedInterpretation>; codeOnly?: boolean; language?: 'en' | 'de' }): JSX.Element {
    const code = packet.evidence.filter(item => item.source === 'code').slice(0, 3);
    const facts = codeOnly ? [] : packet.evidence.filter(item => item.source !== 'code').slice(0, 3);
    return <>
        {packet.limitations.map((limit, index) => <p className="cbm-chat-evidence-note" key={index}>{evidenceNote(limit, language)}</p>)}
        {citation ? <pre>{citation.quote}</pre> : [...code, ...facts].map(item => <div key={item.id}>
            {item.location && <small>{item.location.path}:{item.location.startLine}-{item.location.endLine}</small>}<pre>{item.text}</pre>
        </div>)}
    </>;
}

/** A typed limit applies on blur or Enter, clamped into the model policy. */
function TokenLimitField({ id, label, value, min, max, step, onCommit }: { id: string; label: string; value: number; min: number; max: number; step: number; onCommit: (value: number) => void }): JSX.Element {
    const [text, setText] = useState(String(value));
    useEffect(() => { setText(String(value)); }, [value]);
    const commit = (): void => {
        const typed = Number.parseInt(text, 10);
        const next = Number.isFinite(typed) ? Math.min(max, Math.max(min, typed)) : value;
        setText(String(next));
        if (next !== value) onCommit(next);
    };
    return <div className="cbm-chat-token-limit">
        <label htmlFor={id}>{label}</label>
        <input id={id} type="number" inputMode="numeric" min={min} max={max} step={step} value={text} aria-describedby={`${id}-range`}
            onChange={event => setText(event.target.value)} onBlur={commit} onKeyDown={event => { if (event.key === 'Enter') { event.preventDefault(); commit(); } }} />
        <small id={`${id}-range`}>{browserChatText.limitRange(min, max)}</small>
    </div>;
}

function ContextSnapshot({ context }: { context: BrowserChatContext }): JSX.Element {
    return <details className="cbm-chat-attachment"><summary>{context.label}</summary><pre>{context.text}</pre></details>;
}

/** Keep mounted when collapsed: state and worker lifetime are independent of visibility. */
export default function BrowserChatDock({ proactiveSelection, selectionScope = "", historyKey, proactive = true, onAgentStateChange, onAgentModelChange, settingsRequest = 0, open, onClose, showCollapsed = false, onOpen, attachment, readerContext, context = [], pendingContext, onContextConsumed, onContextRemoved, onAttachmentConsumed, onAttachmentRemoved, createRuntime = createBrowserChatRuntime, removeCache = removeBrowserModelCache, readSource, isCached = cachedInBrowser }: BrowserChatDockProps): JSX.Element {
    const { preferences, setPreferences } = useAgentPreferences();
    const automatic = preferences.automatic;
    const [explanation, setExplanation] = useState<Explanation>();
    const explanations = useRef(new Map<string, Explanation>());
    const symbolSources = useRef(new Map<string, Promise<BrowserChatSource | undefined>>());
    /** The same sources once read, for answers listed at once from the graph. */
    const readSources = useRef(new Map<string, BrowserChatSource>());
    const [retryExplanation, setRetryExplanation] = useState(0);
    const autoRun = useRef<{ key: string; cancelled: boolean; settled: Promise<void> } | undefined>(undefined);
    const manualRequest = useRef<{ cancelled: boolean } | undefined>(undefined);
    const operationSettled = useRef<Promise<void> | undefined>(undefined);
    const historyProject = useRef(historyKey);
    const [questionQueued, setQuestionQueued] = useState(false);
    const [newExplanation, setNewExplanation] = useState(false);
    const lastAttempt = useRef<string | undefined>(undefined);
    /** Files whose card the reader asked the model to write about ("Ask the model", K12). */
    const modelAsked = useRef(new Set<string>());
    const selected = useMemo(() => explanationInput(selectionScope, readerContext, proactiveSelection), [selectionScope, readerContext, proactiveSelection]);
    const selectionRef = useRef(selected); selectionRef.current = selected;
    const settingsSeen = useRef(settingsRequest);
    const modelId = preferences.modelId;
    const model = BROWSER_MODELS.find(candidate => candidate.id === modelId) ?? initialModel;
    const limits = tokenLimitsFor(preferences, model);
    // Short automatic explanations never exceed the configured limits either.
    const autoOutput = Math.min(AUTO_OUTPUT_TOKENS, limits.outputTokens);
    const autoInput = Math.min(AUTO_INPUT_TOKENS, limits.inputTokens, model.contextTokens - autoOutput);
    // Evidence for a manual question grows with the input limit: about 1.5 characters per
    // token leave room for the question, history and instructions (2048 tokens: 3200).
    const chatEvidence = Math.max(800, Math.floor(limits.inputTokens * 25 / 16));
    // A model handed over by the dock of the last project is loaded from the first frame (K24).
    const [phase, setPhase] = useState<Phase>(() => { const handover = peekAgent(model.id); return !handover ? 'off' : handover.ready ? 'preparing' : handover.settled ? 'counting' : 'ready'; });
    const { draft, setDraft, turns, setTurns, ready: historyReady, historyNotice, clearHistory } = useChatHistory<ChatTurn>(historyKey);
    const [error, setError] = useState<string>();
    const [runtimeFailed, setRuntimeFailed] = useState(false);
    const [notice, setNotice] = useState<string>();
    // A download handed over unfinished shows how far it got from the first frame (K24).
    const [progress, setProgress] = useState<BrowserAiProgress | undefined>(() => peekAgent(model.id)?.progress?.latest);
    /** Models whose files are in the browser cache: checked on start, not remembered per session (K10). */
    const [cached, setCached] = useState<ReadonlySet<string>>(() => new Set());
    const [settingsOpen, setSettingsOpen] = useState(false);
    /** The token-limit note opens the configuration at its output limit. */
    const focusOutputLimit = useRef(false);
    const [newOutput, setNewOutput] = useState(false);
    const [stopping, setStopping] = useState(false);
    const [selectedContext, setSelectedContext] = useState<BrowserChatContext[]>([]);
    const [handledContextId, setHandledContextId] = useState<string>();
    const graphSelection = pendingContext?.id !== handledContextId ? pendingContext : undefined;
    const manualAttachment = readerContext === undefined ? attachment : undefined;
    const currentReader = snapshotReaderContext(readerContext);
    const runtime = useRef<BrowserChatRuntime | undefined>(undefined);
    /** The model the worker was created for; the stored choice can change in another tab. */
    const runtimeModel = useRef<string | undefined>(undefined);
    /** The model still loading and its progress, so a project switch can hand it over unfinished (K24). */
    const loading = useRef<{ ready: Promise<void>; progress: LoadProgress } | undefined>(undefined);
    const epoch = useRef(0);
    const pending = useRef(false);
    const stopRequested = useRef(false);
    const activeTurn = useRef<string | undefined>(undefined);
    const transcript = useRef<HTMLDivElement>(null);
    const followOutput = useRef(true);
    const followExplanation = useRef(false);
    const input = useRef<HTMLTextAreaElement>(null);
    const busy = phase === 'preparing' || phase === 'counting' || phase === 'generating' || phase === 'removing';

    useEffect(() => {
        // A project switch hands this dock the model of the last project's dock (K24).
        const handover = takeAgent(model.id);
        if (handover) adopt(handover);
        const withdraw = offerAgent(handOver);
        return () => {
            withdraw(); epoch.current += 1;
            const current = runtime.current, id = runtimeModel.current;
            runtime.current = undefined; runtimeModel.current = undefined;
            // Inside a switch (also React's development double effect) the model stays for the next dock.
            if (current && !(id && keepAgent(handoverOf(current, id)))) current.dispose();
        };
    }, []);
    useEffect(() => {
        // The chosen model loads on start when asked to, only from the cache (K10).
        let alive = true;
        void Promise.all(BROWSER_MODELS.filter(candidate => candidate.availability === 'available').map(async candidate => [candidate.id, await isCached(candidate.id)] as const))
            .then(entries => {
                if (!alive) return;
                const found = new Set(entries.filter(([, inCache]) => inCache).map(([id]) => id));
                setCached(found);
                if (preferences.autoLoad && found.has(model.id) && !runtime.current && !pending.current) void prepare(true);
            });
        return () => { alive = false; };
    }, []);
    useEffect(() => {
        if (historyProject.current === historyKey) return;
        const previous = historyProject.current;
        historyProject.current = historyKey;
        // The first project of this window is no switch: there is nothing of another project to drop.
        if (previous === undefined) return;
        explanations.current.clear(); symbolSources.current.clear(); readSources.current.clear(); resetProject();
        setSelectedContext([]); setHandledContextId(undefined); setNewExplanation(false);
    }, [historyKey]);
    useEffect(() => {
        // Navigation must not pull the reader away from a question being answered.
        if (manualRequest.current) { setNewExplanation(!!selected); return; }
        followExplanation.current = open && proactive && automatic && !!selected;
        if (followExplanation.current) {
            if (transcript.current) transcript.current.scrollTop = 0;
            setNewOutput(false); setNewExplanation(false);
        }
    }, [selected?.key, open, proactive, automatic]);
    useEffect(() => {
        // The current explanation precedes manual turns, so its beginning is at the top.
        // Keep it there as it completes unless the reader has deliberately scrolled away.
        if (open && followExplanation.current && transcript.current) transcript.current.scrollTop = 0;
    }, [explanation, open, phase]);
    useEffect(() => {
        const element = transcript.current;
        if (!element || !open || followExplanation.current) return;
        if (followOutput.current) { element.scrollTop = element.scrollHeight; setNewOutput(false); }
        else if (turns.some(turn => turn.status === 'generating')) setNewOutput(true);
    }, [turns, open]);
    useEffect(() => { if (open && (manualAttachment || graphSelection)) input.current?.focus(); }, [open, manualAttachment?.id, graphSelection?.id]);
    useEffect(() => {
        const element = input.current;
        if (!element || !open) return;
        element.style.height = 'auto';
        element.style.height = `${Math.min(104, Math.max(32, element.scrollHeight))}px`;
    }, [draft, open, phase]);

    const agentState = phase === 'off' ? error ? 'error' : 'off' : phase === 'preparing' || phase === 'removing' ? 'loading' : phase === 'ready' ? 'active' : 'busy';
    useEffect(() => { onAgentStateChange?.(agentState); }, [agentState, onAgentStateChange]);
    useEffect(() => { onAgentModelChange?.(model.displayName); }, [model.displayName, onAgentModelChange]);
    useEffect(() => {
        // Explanations belong to the model that wrote them, and a model chosen elsewhere
        // never answers on the worker of the previous one.
        explanations.current.clear();
        if (runtimeModel.current !== undefined && runtimeModel.current !== model.id) release();
    }, [model.id]);
    useEffect(() => {
        if (settingsSeen.current !== settingsRequest) { settingsSeen.current = settingsRequest; setSettingsOpen(true); }
    }, [settingsRequest]);
    useEffect(() => {
        // Runs after the dialog's own showModal, which focuses its first control.
        if (!settingsOpen || !focusOutputLimit.current) return;
        focusOutputLimit.current = false;
        const field = document.getElementById('cbm-chat-output-tokens');
        if (field instanceof HTMLInputElement) { field.focus({ preventScroll: true }); field.select(); field.scrollIntoView?.({ block: 'nearest' }); }
    }, [settingsOpen]);
    const openOutputLimit = (): void => { focusOutputLimit.current = true; setSettingsOpen(true); };
    const limitNote = (shortened: boolean | undefined, limit: TokenLimits | undefined, automatic?: boolean): LimitNote | undefined => shortened
        ? { limit: limit ?? (automatic ? { inputTokens: autoInput, outputTokens: autoOutput } : limits), automatic, chat: limits, model, onChangeOutput: openOutputLimit } : undefined;
    useEffect(() => {
        const run = autoRun.current;
        if (run && !run.cancelled && (run.key !== selected?.key || !automatic || !proactive)) {
            run.cancelled = true; lastAttempt.current = undefined; runtime.current?.stop();
        }
    }, [selected?.key, automatic, proactive]);
    useEffect(() => {
        // Returning to an explained selection shows what was already written, unless its
        // complete scope now carries other evidence, as after a re-index.
        const cached = selected && explanations.current.get(selected.key);
        if (!cached) return;
        if (selected.evidence && cached.evidence !== selected.evidence) {
            explanations.current.delete(selected.key); lastAttempt.current = undefined;
            setExplanation(previous => previous?.key === selected.key ? undefined : previous);
            return;
        }
        lastAttempt.current = cached.key;
        setExplanation(previous => previous?.key === cached.key ? previous : cached);
    }, [selected?.key, selected?.evidence]);
    useEffect(() => {
        if (!historyReady || !proactive || !automatic || !selected || selected.waiting || phase !== 'ready' || manualRequest.current || pending.current
            || lastAttempt.current === selected.key || explanations.current.has(selected.key)) return;
        const snapshot = selected;
        const timer = setTimeout(() => { void explain(snapshot); }, EXPLANATION_DELAY_MS);
        return () => clearTimeout(timer);
    }, [selected?.key, selected?.waiting, selected?.evidence, phase, automatic, proactive, retryExplanation, historyReady]);
    /** The selection's own source: what the view already read, or the selected symbol read
     * once through get_code_snippet. A failed read is retried on the next request. */
    const sourceFor = (context: BrowserChatContext | undefined): Promise<BrowserChatSource | undefined> => {
        const carried = carriedSource(context);
        const target = carried ? undefined : sourceTargetOf(context);
        if (carried || !target || !readSource) return Promise.resolve(carried);
        const cache = symbolSources.current;
        let read = cache.get(target.qualifiedName);
        if (!read) {
            read = readSource(target.qualifiedName, { maxLines: SYMBOL_SOURCE_LINES }).then(snippet => {
                const source = symbolSource(target, snippet, 'indexed-snippet');
                if (source) readSources.current.set(target.qualifiedName, source);
                return source;
            }, () => { cache.delete(target.qualifiedName); return undefined; });
            cache.set(target.qualifiedName, read);
            while (cache.size > SOURCE_CACHE_SIZE) cache.delete(cache.keys().next().value!);
        }
        return read;
    };
    /** A source already at hand, so a listed answer does not claim "Source unavailable". */
    const knownSource = (context: BrowserChatContext): BrowserChatSource | undefined => {
        const target = sourceTargetOf(context);
        return carriedSource(context) ?? (target ? readSources.current.get(target.qualifiedName) : undefined);
    };
    /** A listed answer or suggestion stands on the selection's source as an explanation does:
     * read once, also when no automatic explanation read it first (K14). */
    const listSource = (id: string, context: BrowserChatContext): void => {
        if (knownSource(context) || !sourceTargetOf(context)) return;
        const budget = chatEvidence;
        void sourceFor(context).then(symbol => {
            if (symbol) setTurns(previous => previous.map(item => item.id === id && (item.answeredFrom === 'graph' || item.answeredFrom === 'suggestion')
                ? { ...item, evidence: prepareExplanationContext(undefined, context, budget, symbol) } : item));
        });
    };
    const remember = (entry: Explanation): void => {
        const cache = explanations.current;
        cache.delete(entry.key); cache.set(entry.key, entry);
        while (cache.size > EXPLANATION_CACHE_SIZE) cache.delete(cache.keys().next().value!);
    };

    const explain = async (snapshot: ExplanationInput): Promise<void> => {
        const currentRuntime = runtime.current;
        if (!historyReady || !currentRuntime || pending.current || manualRequest.current || selectionRef.current?.key !== snapshot.key) return;
        // A configuration or text file is explained by what is read from it. The model writes
        // about it only when asked, here or in the chat (K12).
        if (snapshot.reader?.source && isDataFile(snapshot.reader.source.path) && !modelAsked.current.has(snapshot.key)) {
            lastAttempt.current = snapshot.key;
            const complete: Explanation = { key: snapshot.key, label: snapshot.label, answer: groundedExplanation(readerFacts(snapshot.reader), undefined, false, 'file'), status: 'complete',
                packet: prepareExplanationContext(snapshot.reader, snapshot.graph, 3200), grounded: true, askable: true };
            remember(complete); setExplanation(complete);
            if (!followExplanation.current) setNewExplanation(true);
            return;
        }
        pending.current = true; stopRequested.current = false; setStopping(false);
        const ticket = ++epoch.current;
        let settle!: () => void;
        const settled = new Promise<void>(resolve => { settle = resolve; });
        operationSettled.current = settled;
        const run = { key: snapshot.key, cancelled: false, settled }; autoRun.current = run; lastAttempt.current = snapshot.key;
        const valid = () => epoch.current === ticket && !run.cancelled && !stopRequested.current && selectionRef.current?.key === snapshot.key;
        // Graph selections show their listed facts at once; the model only adds a sentence.
        const summary = snapshot.reader ? readerFacts(snapshot.reader) : selectionSummary(snapshot.graph);
        const from = snapshot.reader ? 'file' : 'graph';
        const writing = summary.length ? `${summary.map(line => `- ${line}`).join('\n')}\n\n_${snapshot.reader || sourceTargetOf(snapshot.graph) || carriedSource(snapshot.graph) ? browserChatText.writingSentence : browserChatText.readingFacts}_` : '';
        setExplanation({ key: snapshot.key, label: snapshot.label, answer: writing, status: 'generating' });
        setPhase('counting');
        try {
            const symbol = snapshot.reader ? undefined : await sourceFor(snapshot.graph);
            if (!valid()) return;
            let packet = prepareExplanationContext(snapshot.reader, snapshot.graph, 3200, symbol);
            // Without source the model could only restate the facts or guess from names (an area
            // became "a high-level web framework"): the listed facts are the explanation (K7).
            if (summary.length && explanationMode(packet) === 'graph') {
                const complete: Explanation = { key: snapshot.key, label: snapshot.label, answer: groundedExplanation(summary, undefined, false), status: 'complete', packet, grounded: true,
                    ...snapshot.evidence ? { evidence: snapshot.evidence } : {} };
                remember(complete); setExplanation(complete);
                if (!followExplanation.current) setNewExplanation(true);
                return;
            }
            const subject = snapshot.reader ? undefined : selectionSubject(snapshot.graph);
            // What the selected symbol's code declares stands under the facts, whatever the model writes (B2).
            const facts = summary.length && symbol && subject ? codeFacts(symbol, subject) : undefined;
            const code = facts ? codeFactsMarkdown(facts, 'en') : undefined;
            let request = explanationMessages(packet, subject);
            let count = await currentRuntime.countTokens(request);
            if (!valid()) return;
            for (let budget = 1900; count > autoInput && budget >= 300; budget = Math.floor(budget * .6)) {
                packet = prepareExplanationContext(snapshot.reader, snapshot.graph, budget, symbol);
                request = explanationMessages(packet, subject);
                count = await currentRuntime.countTokens(request);
                if (!valid()) return;
            }
            if (!packet.evidence.length || count > autoInput) {
                setExplanation({ key: snapshot.key, label: snapshot.label, answer: '', status: 'error', packet, error: !packet.evidence.length ? 'No source or graph evidence is available for this selection.' : 'This selection is too large for the local agent. Select a smaller code range and try again.' });
                return;
            }
            setExplanation({ key: snapshot.key, label: snapshot.label, answer: code ? writingExplanation(summary, browserChatText.writingSentence, code) : writing, status: 'generating', packet });
            setPhase('generating');
            // Keep an explanation together and ignore output from superseded selections.
            let shortened = false;
            const answer = await currentRuntime.chat(request, () => {}, { maxOutputTokens: autoOutput, generationProfile: 'automatic-explanation',
                onComplete: ({ stopReason }) => { shortened = stopReason === 'length'; } });
            if (valid()) {
                const result = parseExplanationResponse(answer, packet);
                if (!followExplanation.current) setNewExplanation(true);
                // One sentence beside the facts (two for reader code), and none that names what the evidence lacks.
                const checked = result.status === 'generated' ? explanationSentence(result.markdown, packet, request.map(message => message.content).join('\n')) : {};
                if (summary.length || (result.status === 'generated' && checked.dropped)) {
                    const markdown = summary.length ? groundedExplanation(summary, checked.sentence, checked.reason ?? checked.dropped !== undefined, from, 'en', code) : `_${browserChatText.explanationDropped(checked.reason)}_`;
                    const complete: Explanation = { key: snapshot.key, label: snapshot.label, answer: markdown, status: 'complete', mode: 'interpretation', packet, grounded: summary.length > 0,
                        ...shortened && checked.sentence ? { shortened, limit: { inputTokens: autoInput, outputTokens: autoOutput } } : {}, ...snapshot.evidence ? { evidence: snapshot.evidence } : {} };
                    remember(complete); setExplanation(complete);
                } else if (result.status === 'generated') {
                    const complete: Explanation = { key: snapshot.key, label: snapshot.label, answer: checked.sentence ?? result.markdown, status: 'complete', mode: 'interpretation', packet, citation: result.citation, generated: true,
                        ...shortened ? { shortened, limit: { inputTokens: autoInput, outputTokens: autoOutput } } : {}, ...snapshot.evidence ? { evidence: snapshot.evidence } : {} };
                    remember(complete); setExplanation(complete);
                } else setExplanation({ key: snapshot.key, label: snapshot.label, answer: '', status: 'error', packet, error: result.reason });
            }
        } catch (failure) {
            if (epoch.current === ticket && isGpuRuntimeFailure(failure)) { invalidateRuntime(failure); return; }
            if (valid()) setExplanation({ key: snapshot.key, label: snapshot.label, answer: '', status: 'error', error: messageOf(failure) });
        } finally {
            if (epoch.current === ticket) {
                if (run.cancelled || stopRequested.current) setExplanation(previous => previous?.key === snapshot.key ? { ...previous, status: 'stopped' } : previous);
                autoRun.current = undefined; pending.current = false; setStopping(false); setPhase('ready');
            }
            // Cancellation only asks the worker to stop. A waiting question must not
            // use the worker until token counting / generation has actually settled.
            if (operationSettled.current === settled) operationSettled.current = undefined;
            settle();
        }
    };
    const resetProject = (): void => {
        if (!runtime.current || phase === 'preparing' || phase === 'removing') { release(); return; }
        const retainedRuntime = runtime.current;
        const settled = operationSettled.current;
        if (autoRun.current) autoRun.current.cancelled = true;
        if (manualRequest.current) manualRequest.current.cancelled = true;
        autoRun.current = undefined; manualRequest.current = undefined;
        lastAttempt.current = undefined; activeTurn.current = undefined;
        setQuestionQueued(false); setExplanation(undefined); setError(undefined);
        const ticket = ++epoch.current;
        pending.current = !!settled; stopRequested.current = !!settled; setStopping(!!settled);
        if (!settled) { setPhase('ready'); return; }
        // Retain downloaded weights across projects, but drain the old worker request
        // before making the new project's context eligible for inference.
        setPhase('counting'); retainedRuntime.stop();
        void settled.then(() => {
            if (epoch.current !== ticket || runtime.current !== retainedRuntime) return;
            pending.current = false; stopRequested.current = false; setStopping(false); setPhase('ready');
        });
    };
    const handoverOf = (current: BrowserChatRuntime, modelId: string): AgentHandover => ({ runtime: current, modelId,
        ...operationSettled.current ? { settled: operationSettled.current } : {}, ...loading.current ? { ready: loading.current.ready, progress: loading.current.progress } : {} });
    /** Gives the model to the dock of the next project (K24). What this dock was doing stops,
     * and its late results land nowhere: its epoch has moved on. */
    const handOver = (): AgentHandover | undefined => {
        const current = runtime.current, id = runtimeModel.current;
        if (!current || !id) return undefined;
        if (autoRun.current) autoRun.current.cancelled = true;
        if (manualRequest.current) manualRequest.current.cancelled = true;
        epoch.current += 1; current.stop();
        const handover = handoverOf(current, id);
        runtime.current = undefined; runtimeModel.current = undefined;
        return handover;
    };
    /** The model the last project's dock handed over: no new worker and no second load. A
     * stopped answer settles, and a model still loading finishes, before this project uses it. */
    const adopt = (handover: AgentHandover): void => {
        const adopted = handover.runtime;
        runtime.current = adopted; runtimeModel.current = handover.modelId;
        adopted.setFatalHandler?.(failure => { if (runtime.current === adopted) invalidateRuntime(failure); });
        // A further project switch must carry the same unfinished operation with it.
        operationSettled.current = handover.settled;
        const waiting = handover.ready ?? handover.settled;
        if (!waiting) { setPhase('ready'); return; }
        const ticket = epoch.current;
        pending.current = true;
        if (handover.ready) {
            const progress = handover.progress ?? loadProgress();
            loading.current = { ready: handover.ready, progress }; setPhase('preparing');
            progress.follow(value => { if (epoch.current === ticket && runtime.current === adopted) setProgress(value); });
        } else { stopRequested.current = true; setStopping(true); setPhase('counting'); }
        void waiting.then(() => {
            if (epoch.current !== ticket || runtime.current !== adopted) return;
            if (operationSettled.current === handover.settled) operationSettled.current = undefined;
            pending.current = false; stopRequested.current = false; loading.current = undefined; setStopping(false);
            if (handover.ready) { setCached(previous => new Set(previous).add(handover.modelId)); setProgress(undefined); }
            setPhase('ready');
        }, (failure: unknown) => {
            if (epoch.current !== ticket || runtime.current !== adopted) return;
            release(); setError(messageOf(failure));
        });
    };
    const release = (): void => {
        if (manualRequest.current) manualRequest.current.cancelled = true;
        manualRequest.current = undefined; operationSettled.current = undefined; setQuestionQueued(false);
        if (autoRun.current) autoRun.current.cancelled = true;
        autoRun.current = undefined; lastAttempt.current = undefined; setExplanation(undefined);
        const id = activeTurn.current;
        if (id) setTurns(previous => previous.map(turn => turn.id === id ? { ...turn, status: 'stopped' } : turn));
        epoch.current += 1; pending.current = false; activeTurn.current = undefined;
        stopRequested.current = false; setStopping(false);
        runtime.current?.dispose(); runtime.current = undefined; runtimeModel.current = undefined; loading.current = undefined;
        setProgress(undefined); setPhase('off'); setRuntimeFailed(false);
    };
    const invalidateRuntime = (failure: unknown): void => {
        const id = activeTurn.current;
        const message = new BrowserRuntimeFatalError(messageOf(failure)).message;
        release(); setRuntimeFailed(true); setError(message);
        if (id) setTurns(previous => previous.map(turn => turn.id === id ? { ...turn, status: 'error', error: message } : turn));
    };
    const prepare = async (cacheOnly = cached.has(model.id)): Promise<void> => {
        if (pending.current || model.availability !== 'available') return;
        release(); pending.current = true;
        const ticket = epoch.current;
        setError(undefined); setNotice(undefined); setPhase('preparing');
        try {
            const nextRuntime = createRuntime(model.id);
            runtime.current = nextRuntime; runtimeModel.current = model.id;
            nextRuntime.setFatalHandler?.(failure => { if (runtime.current === nextRuntime) invalidateRuntime(failure); });
            const progress = loadProgress();
            progress.follow(value => { if (epoch.current === ticket) setProgress(value); });
            const loaded = nextRuntime.prepare(value => progress.report(value), cacheOnly ? { cacheOnly } : undefined);
            loading.current = { ready: loaded, progress };
            await loaded;
            if (epoch.current !== ticket) return;
            loading.current = undefined;
            setCached(previous => new Set(previous).add(model.id));
            setPhase('ready'); setProgress(undefined); setSettingsOpen(false); pending.current = false;
        } catch (failure) {
            if (epoch.current !== ticket) return;
            release();
            if (cacheOnly) { setCached(previous => { const next = new Set(previous); next.delete(model.id); return next; }); setError(browserChatText.resumeFailed); }
            else setError(messageOf(failure));
        }
    };
    const stop = (): void => {
        if (phase === 'preparing') { release(); setNotice('Download stopped. Cached files can be reused or deleted.'); return; }
        if (manualRequest.current && autoRun.current) {
            manualRequest.current.cancelled = true; manualRequest.current = undefined; setQuestionQueued(false);
            lastAttempt.current = selectionRef.current?.key;
        }
        if ((phase !== 'generating' && phase !== 'counting') || stopRequested.current) return;
        stopRequested.current = true; setStopping(true);
        if (phase === 'generating') runtime.current?.stop();
        // Stay busy until the in-flight GPU operation settles; a second send must not overlap it.
    };
    const send = async (retry?: ChatTurn): Promise<void> => {
        const currentRuntime = runtime.current;
        const automaticRun = autoRun.current;
        if (!historyReady || !currentRuntime || manualRequest.current || stopping || (!automaticRun && (phase !== 'ready' || pending.current)) || (!retry && !draft.trim())) return;
        if (!retry && readerContext?.status === 'loading') return;
        // A manual question switches from reading the current explanation to its reply.
        // Preserve a deliberate history scroll; only automatic explanation following resets it.
        if (followExplanation.current) followOutput.current = true;
        followExplanation.current = false;
        // Capture the question and every source before yielding to cancellation.
        // Later navigation changes the live explainer, never this request.
        const prompt = retry?.prompt ?? draft;
        const source = snapshotAttachment(retry ? retry.attachment : manualAttachment);
        const reader = snapshotReaderContext(retry ? retry.readerContext : readerContext);
        const selectedGraph = !retry && graphSelection ? { ...graphSelection } : undefined;
        const nextContext = selectedGraph ? [...selectedContext.filter(item => item.id !== selectedGraph.id), selectedGraph] : selectedContext;
        const extra = (retry ? retry.context ?? [] : nextContext).map(item => ({ ...item }));
        let packet = retry?.evidence;
        // A listed answer or a suggestion asked again goes to the model: the same question and evidence in a fresh request.
        const ask = retry !== undefined && ASKABLE.has(retry.answeredFrom ?? '');
        // The graph evidence it was listed from, read again with its source (K14).
        const askFrom = ask ? retry.listedFrom ?? retry.suggestion?.context : undefined;
        const currentGraph = !retry && !reader && proactiveSelection ? [{ ...proactiveSelection }] : [];
        const packetGraph = askFrom ? [askFrom] : currentGraph;
        const consume = (): void => {
            setDraft(previous => previous === prompt ? '' : previous);
            setSelectedContext(previous => previous.filter(item => !extra.some(sent => sent.id === item.id && sent.text === item.text)));
            if (source) onAttachmentConsumed(source.id);
            if (selectedGraph) { setHandledContextId(selectedGraph.id); onContextConsumed?.(selectedGraph.id); }
        };
        // A question without its own context may follow up on attached code in this view.
        const topic = retry ? retry.topic : chatTopic(selectionScope, { reader, graph: currentGraph[0], attachment: source, context: extra })
            ?? followedTopic(turns, selectionScope);
        // Without code or graph facts the model can only guess: say what to select instead.
        if (!retry && !topic) {
            setTurns(previous => [...previous, { id: `local-turn-${crypto.randomUUID()}`, prompt, modelId: model.id, request: [],
                answer: missingContextAnswer(prompt, reader), status: 'complete', answeredFrom: 'local' }]);
            consume();
            return;
        }
        // Callers and callees of the selection come from the loaded graph, complete and
        // without the model: a small model drops and repeats names in long lists.
        const listed = !retry && !source ? relationshipAnswer(prompt, [...currentGraph, ...extra]) : undefined;
        if (listed) {
            const id = `local-turn-${crypto.randomUUID()}`;
            setTurns(previous => [...previous, { id, prompt, context: extra, topic, listedFrom: listed.context, replyLanguage: listed.language,
                evidence: prepareExplanationContext(undefined, listed.context, chatEvidence, knownSource(listed.context)), modelId: model.id, request: [],
                answer: listed.markdown, status: 'complete', answeredFrom: 'graph' }]);
            consume(); listSource(id, listed.context);
            return;
        }
        // Sounds like callers or callees but is not certain: offer the list, do not guess with the model.
        const suggested = !retry && !source ? relationshipSuggestion(prompt, [...currentGraph, ...extra]) : undefined;
        if (suggested) {
            const id = `local-turn-${crypto.randomUUID()}`;
            setTurns(previous => [...previous, { id, prompt, context: extra, topic, replyLanguage: suggested.language,
                evidence: prepareExplanationContext(undefined, suggested.context, chatEvidence, knownSource(suggested.context)), modelId: model.id, request: [],
                answer: suggested.markdown, status: 'complete', answeredFrom: 'suggestion', suggestion: { question: suggested.question, context: suggested.context } }]);
            consume(); listSource(id, suggested.context);
            return;
        }
        const language = questionLanguage(prompt);
        const galaxy = currentGraph[0] ? readGalaxyEvidence(currentGraph[0].text) : undefined;
        const fileSource = reader?.source;
        // A question about the current view ("erkläre die aktuelle Hierarchie", "what am I looking at")
        // is answered from the loaded scope: the model only restated it (H1).
        const view = !retry && !source && !extra.length && galaxy && viewQuestion(prompt, galaxyNames(galaxy)) === 'general' ? viewAnswer(galaxy, language) : undefined;
        if (view) {
            const id = `local-turn-${crypto.randomUUID()}`, scope = currentGraph[0];
            setTurns(previous => [...previous, { id, prompt, context: extra, topic, listedFrom: scope, replyLanguage: language,
                evidence: prepareExplanationContext(undefined, scope, chatEvidence, knownSource(scope)), modelId: model.id, request: [],
                answer: view, status: 'complete', answeredFrom: 'graph' }]);
            consume(); listSource(id, scope);
            return;
        }
        // A prompt that asks nothing ("test", "hallo") gets questions it could ask, not an echo of the model (C6).
        const known = knownNames({ galaxy, names: galaxy ? galaxyNames(galaxy) : fileSource ? [fileSource.path] : [],
            texts: [fileSource?.text ?? '', source?.text ?? '', ...currentGraph.filter(() => !galaxy).map(item => item.label), ...extra.map(item => item.label)] });
        const subject: ExampleSubject = galaxy ? { kind: 'galaxy', name: galaxy.label, evidence: galaxy } : fileSource?.kind === 'selection' || source ? { kind: 'marked' }
            : { kind: 'other', name: fileSource ? fileSource.path.split('/').pop()! : topic?.label ?? '' };
        // "und was noch?" right after a change of topic would reach the model without context:
        // this topic has no earlier turn, and those of other topics are not sent (B4).
        const hint = !retry && noQuestion(prompt, known) ? noQuestionAnswer(prompt, language, subject)
            : !retry && topic && contextFreeFollowUp(prompt) && !topicHistory(turns, topic).some(sentInHistory) && turns.some(item => item.topic && item.topic.key !== topic.key)
                ? followUpAnswer(prompt, language, subject, topic.label) : undefined;
        if (hint) {
            setTurns(previous => [...previous, { id: `local-turn-${crypto.randomUUID()}`, prompt, attachment: source, readerContext: reader, context: extra, topic, replyLanguage: language,
                ...currentGraph[0] ? { listedFrom: currentGraph[0] } : {}, modelId: model.id, request: [], answer: hint, status: 'complete', answeredFrom: 'hint' }]);
            consume();
            return;
        }
        // What a configuration file holds is read from it: the model hung its keys on the wrong items (C7).
        const outline = !retry && !source && !extra.length && fileSource?.kind === 'file' && !fileSource.partial
            && generalQuestion(prompt, [fileSource.path]) ? fileOutline(fileSource.path, fileSource.text, language) : undefined;
        if (outline) {
            setTurns(previous => [...previous, { id: `local-turn-${crypto.randomUUID()}`, prompt, readerContext: reader, context: extra, topic, replyLanguage: language,
                modelId: model.id, request: [], answer: outline, status: 'complete', answeredFrom: 'file' }]);
            consume();
            return;
        }
        // A short general question about a Galaxy selection gets the shape of its explanation: the
        // listed facts and one checked model sentence, in the language of the question (C5). So
        // does one about an open code file or marked code, with the facts read from it (B3).
        const codeSource: BrowserChatSource | undefined = !retry && !extra.length && !galaxy
            ? fileSource ? !isDataFile(fileSource.path) && !fileSource.partial ? fileSource : undefined : source ? { ...source, kind: 'selection' } : undefined : undefined;
        const codeFound = codeSource ? codeSourceFacts(codeSource, language) : undefined;
        const grounded: Grounded | undefined = !retry && !source && !extra.length && galaxy && generalQuestion(prompt, galaxyNames(galaxy)) === 'general'
            ? { from: 'graph', graph: currentGraph[0], summary: selectionSummary(currentGraph[0], language), subject: selectionSubject(currentGraph[0]) }
            : codeSource && codeFound && generalQuestion(prompt, [codeSource.path, ...codeFound.names]) === 'general'
                ? { from: 'file', reader: { project: codeSource.project, path: codeSource.path, status: 'ready', source: codeSource }, summary: codeFound.summary, code: codeFound.code } : undefined;
        const groundedPacket = (budget: number, symbol?: BrowserChatSource) => grounded?.from === 'file' ? prepareExplanationContext(grounded.reader, [], budget)
            : prepareExplanationContext(undefined, packetGraph, budget, symbol);
        // Answers about another file or selection stay out (K17); those of this topic come along,
        // also from before the reader went elsewhere and came back (B4).
        const earlier = topicHistory(ask ? turns.filter(item => item.id !== retry.id) : turns, topic);
        let history: ChatTurn[] = earlier;
        const makeRequest = () => buildChatMessages(history, prompt, source, extra, reader, currentGraph, packet ? formatExplanationEvidence(packet) : undefined, questionLanguage(prompt));
        const queued = { cancelled: false }; manualRequest.current = queued;
        const waitingEpoch = epoch.current;
        // The selected symbol's source grounds a question as it grounds the explanation (K14).
        const symbol = (!retry || askFrom) && !reader && packetGraph.length ? await sourceFor(packetGraph[0]) : undefined;
        if (queued.cancelled || manualRequest.current !== queued || epoch.current !== waitingEpoch || runtime.current !== currentRuntime) {
            if (manualRequest.current === queued) manualRequest.current = undefined;
            return;
        }
        if (grounded) packet = groundedPacket(3200, symbol);
        else if ((!retry || askFrom) && ((reader?.source?.text.length ?? 0) > 5000 || packetGraph.length)) packet = prepareExplanationContext(reader, packetGraph, chatEvidence, symbol);
        // What the selected symbol's code declares, whatever the model's sentence says (B2).
        const declared = grounded?.from === 'graph' && symbol && grounded.subject ? codeFacts(symbol, grounded.subject) : undefined;
        const code = grounded?.code ?? (declared ? codeFactsMarkdown(declared, language) : undefined);
        // Without source the model could only restate the facts: they are the answer (K7).
        if (grounded && explanationMode(packet!) === 'graph') {
            if (manualRequest.current === queued) manualRequest.current = undefined;
            setTurns(previous => [...previous, { id: `local-turn-${crypto.randomUUID()}`, prompt, context: extra, topic, ...grounded.graph ? { listedFrom: grounded.graph } : {}, replyLanguage: language, evidence: packet,
                modelId: model.id, request: [], answer: groundedExplanation(grounded.summary, undefined, false, grounded.from, language), status: 'complete', answeredFrom: 'grounded' }]);
            consume();
            return;
        }
        // An echo asked again is told not to restate the question: the same request gets the same echo (H2).
        let request = grounded ? explanationMessages(packet!, grounded.subject, language) : retry && !ask ? retry.echo ? echoRetryRequest(retry.request) : retry.request.map(message => ({ ...message })) : makeRequest();
        if (automaticRun) {
            automaticRun.cancelled = true; lastAttempt.current = undefined;
            setQuestionQueued(true); currentRuntime.stop();
            await automaticRun.settled;
            if (queued.cancelled || manualRequest.current !== queued || epoch.current !== waitingEpoch || runtime.current !== currentRuntime) return;
        }
        setQuestionQueued(false);
        pending.current = true; stopRequested.current = false; setStopping(false);
        const ticket = ++epoch.current;
        let settle!: () => void;
        const settled = new Promise<void>(resolve => { settle = resolve; });
        operationSettled.current = settled;
        setError(undefined); setNotice(undefined); setPhase('counting');
        try {
            let count = await currentRuntime.countTokens(request);
            if (epoch.current !== ticket || stopRequested.current) return;
            const limit = grounded ? autoInput : Math.min(limits.inputTokens, model.contextTokens - limits.outputTokens);
            // The sentence of a general question is bounded like that of an automatic explanation.
            for (let budget = 1900; grounded && count > limit && budget >= 300; budget = Math.floor(budget * .6)) {
                packet = groundedPacket(budget, symbol);
                request = explanationMessages(packet, grounded.subject, language); count = await currentRuntime.countTokens(request);
                if (epoch.current !== ticket || stopRequested.current) return;
            }
            // Earlier turns give way first, oldest first; the question and its evidence stay.
            while (!grounded && (!retry || ask) && count > limit && history.length) {
                const characters = request.reduce((sum, message) => sum + message.content.length, 0);
                history = trimChatHistory(history, Math.ceil((count - limit) * characters / Math.max(1, count)));
                request = makeRequest(); count = await currentRuntime.countTokens(request);
                if (epoch.current !== ticket || stopRequested.current) return;
            }
            for (let budget = packet ? Math.floor(chatEvidence * .6) : chatEvidence; !grounded && (!retry || askFrom) && count > limit && (reader?.source || packetGraph.length) && budget >= 300; budget = Math.floor(budget * .6)) {
                packet = prepareExplanationContext(reader, packetGraph, budget, symbol);
                request = makeRequest(); count = await currentRuntime.countTokens(request);
                if (epoch.current !== ticket || stopRequested.current) return;
            }
            if (count > limit) {
                setError(`This prompt needs ${count.toLocaleString()} input tokens; the local working limit is ${limit.toLocaleString()}. Select less code or start a new conversation. Nothing was sent or shortened without disclosure.`);
                return;
            }
            // The model's answer to a listed, grounded or file answer is a turn of its own below it:
            // the facts stay where they were (B1). Asking again repeats a model answer in place.
            const id = retry && !ask ? retry.id : `local-turn-${crypto.randomUUID()}`;
            const historyOmitted = retry && !ask ? retry.historyOmitted
                : earlier.filter(item => item.status !== 'error' && item.status !== 'generating' && !history.includes(item)).length;
            // A general question shows its facts at once; the model only adds a sentence.
            const writing = grounded ? writingExplanation(grounded.summary, groundedText[language].writingSentence, code) : '';
            const turn: ChatTurn = { id, prompt, attachment: source, readerContext: reader, context: extra, evidence: packet, modelId: model.id, request, answer: writing, status: 'generating', topic,
                ...historyOmitted ? { historyOmitted } : {}, ...grounded ? { answeredFrom: 'grounded' as const, ...grounded.graph ? { listedFrom: grounded.graph } : {}, replyLanguage: language } : {},
                ...ask || retry?.askedModel ? { askedModel: true } : {} };
            activeTurn.current = id;
            if (retry && !ask) setTurns(previous => previous.map(item => item.id === id ? turn : item));
            else { setTurns(previous => [...previous, turn]); if (!retry) consume(); }
            setPhase('generating');
            let shortened = false;
            const onComplete = ({ stopReason }: { stopReason: string }) => { shortened = stopReason === 'length'; };
            const output = await currentRuntime.chat(request, chunk => {
                if (grounded || epoch.current !== ticket || stopRequested.current) return;
                setTurns(previous => previous.map(item => item.id === id ? { ...item, answer: item.answer + chunk } : item));
            }, grounded ? { maxOutputTokens: autoOutput, generationProfile: 'automatic-explanation', onComplete } : { maxOutputTokens: limits.outputTokens, onComplete });
            if (epoch.current !== ticket) return;
            if (grounded) {
                // One sentence beside the facts, checked like the sentence of the explanation card.
                const result = parseExplanationResponse(output, packet!);
                const checked = !stopRequested.current && result.status === 'generated' ? explanationSentence(result.markdown, packet!, request.map(message => message.content).join('\n')) : {};
                // A sentence that only restates the question says nothing beside the facts (H2).
                const echoed = checked.sentence !== undefined && echoAnswer(checked.sentence, prompt);
                const answer = groundedExplanation(grounded.summary, echoed ? undefined : checked.sentence, checked.reason ?? (echoed || checked.dropped !== undefined), grounded.from, language, code);
                setTurns(previous => previous.map(item => item.id === id ? { ...item, answer, status: stopRequested.current ? 'stopped' : 'complete' } : item));
                return;
            }
            // An answer that only restates the question is none: the turn says so, with the facts it was about (H2).
            if (!stopRequested.current && echoAnswer(output, prompt)) {
                const about = packetGraph[0] ?? retry?.listedFrom;
                setTurns(previous => previous.map(item => item.id === id ? { ...item, answer: echoedAnswer(output, language, about, about ? undefined : reader), status: 'complete', echo: true,
                    replyLanguage: language, ...about ? { listedFrom: about } : {} } : item));
                return;
            }
            setTurns(previous => previous.map(item => item.id === id ? { ...item, answer: stopRequested.current ? item.answer : output, status: stopRequested.current ? 'stopped' : 'complete',
                ...shortened && !stopRequested.current ? { shortened, limit: { inputTokens: limits.inputTokens, outputTokens: limits.outputTokens } } : {} } : item));
        } catch (failure) {
            if (epoch.current !== ticket) return;
            if (isGpuRuntimeFailure(failure)) { invalidateRuntime(failure); return; }
            const explanation = messageOf(failure);
            const id = activeTurn.current;
            if (id) setTurns(previous => previous.map(turn => turn.id === id ? { ...turn, status: stopRequested.current ? 'stopped' : 'error', error: stopRequested.current ? undefined : explanation,
                ...grounded ? { answer: groundedExplanation(grounded.summary, undefined, false, grounded.from, language, code) } : {} } : turn));
            else if (!stopRequested.current) setError(explanation);
        } finally {
            if (manualRequest.current === queued) manualRequest.current = undefined;
            if (epoch.current === ticket) { activeTurn.current = undefined; pending.current = false; setStopping(false); setPhase('ready'); }
            if (operationSettled.current === settled) operationSettled.current = undefined;
            settle();
        }
    };
    /** The suggested list replaces the suggestion; the question stays as typed. */
    const showSuggestedList = (turn: ChatTurn): void => {
        const listed = turn.suggestion && relationshipAnswer(turn.suggestion.question, [turn.suggestion.context]);
        if (!listed) return;
        setTurns(previous => previous.map(item => item.id === turn.id ? { ...item, answer: listed.markdown, answeredFrom: 'graph', suggestion: undefined, listedFrom: listed.context, replyLanguage: listed.language,
            evidence: prepareExplanationContext(undefined, listed.context, chatEvidence, knownSource(listed.context)) } : item));
        listSource(turn.id, listed.context);
    };
    const deleteCache = async (): Promise<void> => {
        if (pending.current) return;
        release(); pending.current = true;
        const ticket = epoch.current;
        setPhase('removing'); setError(undefined); setNotice(undefined);
        try {
            await removeCache(model.id);
            if (epoch.current !== ticket) return;
            setCached(previous => { const next = new Set(previous); next.delete(model.id); return next; });
            setNotice('Cached model files deleted. Your conversation is still here.');
        } catch (failure) { if (epoch.current === ticket) setError(messageOf(failure)); }
        finally { if (epoch.current === ticket) { pending.current = false; setPhase('off'); } }
    };
    const clear = (): void => {
        if (busy || !historyReady || (!turns.length && !draft) || !window.confirm('Clear this project’s local conversation and saved history? This removes its messages, draft and attached code snapshots.')) return;
        void clearHistory(); setError(undefined); setNewOutput(false); followOutput.current = true;
    };
    const submit = (event: FormEvent): void => { event.preventDefault(); void send(); };
    const status = stopping ? 'Stopping' : phase === 'preparing' ? 'Loading' : phase === 'counting' ? 'Checking context' : phase === 'generating' ? 'Answering' : phase === 'removing' ? 'Deleting cache' : phase === 'ready' ? 'Loaded' : 'Off';
    const needsEnable = phase === 'off';
    const showConversation = turns.length > 0 || draft.length > 0 || phase === 'ready' || phase === 'counting' || phase === 'generating';

    return <>
        {settingsOpen && <AgentSettingsDialog onClose={() => setSettingsOpen(false)}>
        <section id="cbm-chat-model-settings" className="cbm-chat-settings" aria-label="Local model settings">
            {proactive && <label><input type="checkbox" checked={automatic} onChange={event => setPreferences({ automatic: event.target.checked })} /> Explain selections automatically</label>}
            <label title={browserChatText.autoLoadNote}><input id="cbm-chat-auto-load" type="checkbox" checked={preferences.autoLoad} onChange={event => setPreferences({ autoLoad: event.target.checked })} /> {browserChatText.autoLoad}</label>
            <span className="cbm-chat-status" role="status">{status}</span>
            <label htmlFor="cbm-chat-model">Model</label>
            <select id="cbm-chat-model" value={modelId} disabled={busy} onChange={event => { release(); setPreferences({ modelId: event.target.value }); setError(undefined); setNotice(undefined); }}>
                {BROWSER_MODELS.map(candidate => <option key={candidate.id} value={candidate.id}>{candidate.displayName} · {candidate.availability === 'unsupported' ? 'Requires runtime support' : candidate.id === model.id && phase === 'ready' ? 'Loaded' : cached.has(candidate.id) ? browserChatText.cached : 'Available'}</option>)}
            </select>
            <p>{sizeLabel(model.bytes)} download · {model.license}. Memory use is higher.</p>
            {model.compatibilityNote && <p>{model.compatibilityNote}</p>}
            <fieldset className="cbm-chat-token-limits">
                <legend>{browserChatText.tokenLimits(model.displayName)}</legend>
                <TokenLimitField id="cbm-chat-input-tokens" label={browserChatText.inputLimit} value={limits.inputTokens} {...tokenLimitBounds(model, limits.outputTokens).input} step={64}
                    onCommit={inputTokens => setPreferences(current => ({ limits: { ...current.limits, [model.id]: clampTokenLimits(model, limits, { inputTokens }) } }))} />
                <TokenLimitField id="cbm-chat-output-tokens" label={browserChatText.outputLimit} value={limits.outputTokens} {...tokenLimitBounds(model, limits.outputTokens).output} step={32}
                    onCommit={outputTokens => setPreferences(current => ({ limits: { ...current.limits, [model.id]: clampTokenLimits(model, limits, { outputTokens }) } }))} />
                <p>{browserChatText.limitsNote(autoInput, autoOutput)}</p>
            </fieldset>
            <p><a href={model.modelCard} target="_blank" rel="noreferrer">Model details</a> · Downloads come from Hugging Face. No model downloads automatically.</p>
            <div className="cbm-chat-model-actions">
                {phase === 'off' && <button type="button" className="cbm-chat-primary" disabled={model.availability !== 'available'} onClick={() => { void prepare(); }}>{runtimeFailed ? 'Reload model' : cached.has(model.id) ? browserChatText.loadCached : 'Download & load'}</button>}
                {(phase === 'ready' || phase === 'generating' || phase === 'counting') && <button type="button" onClick={() => { release(); setNotice('Model unloaded. Conversation and cached files retained.'); }}>Unload model</button>}
                <button type="button" disabled={busy} onClick={() => { void deleteCache(); }}>Delete cached model</button>
            </div>
        </section>
        {historyKey && <section className="cbm-chat-settings" aria-label="Chat history settings">
            <p>Chat history · 24 hours in this browser, per project. Expired history is removed on the next visit.</p>
            <button type="button" disabled={busy || !historyReady || (!turns.length && !draft)} onClick={clear}>Clear history</button>
            {historyNotice && <p role="status">{historyNotice}</p>}
        </section>}
        {phase === 'preparing' && <div className="cbm-chat-loading" role="status"><progress max={100} value={progress?.progress} /><span>{progress?.file ?? 'Preparing browser model…'}</span><button type="button" onClick={stop}>Stop download</button></div>}
        {error && <div className="cbm-chat-error" role="alert">{error}</div>}
        {notice && <p className="cbm-chat-notice" role="status">{notice}</p>}
        </AgentSettingsDialog>}
        {!open && showCollapsed ? <aside className="cbm-chat-dock is-collapsed" aria-label="Local chat">
            <header className="cbm-chat-header"><h2>Chat</h2>
                <button type="button" className="cbm-chat-primary" aria-expanded={false} onClick={onOpen}>Open chat</button>
            </header>
        </aside> : <aside className="cbm-chat-dock" hidden={!open} aria-label="Local chat">
        <header className="cbm-chat-header">
            <h2>Chat</h2>
            {turns.length > 0 && <button type="button" className="cbm-chat-new" disabled={busy} onClick={clear}>{browserChatText.newConversation}</button>}
            <button type="button" className="cbm-chat-icon" aria-label="Collapse local chat" title="Collapse chat; keep conversation" onClick={onClose}>›</button>
        </header>
        {needsEnable && <div className="cbm-chat-enable"><p>Enable the local agent to explain selections and chat.</p><button type="button" onClick={() => setSettingsOpen(true)}>Enable agent</button></div>}
        {(phase === 'preparing' || phase === 'removing') && <p className="cbm-chat-notice" role="status">Preparing agent…</p>}
        {error && !settingsOpen && <div className="cbm-chat-error" role="alert">{error}</div>}
        {showConversation && <div className="cbm-chat-transcript" ref={transcript} role="log" aria-label="Conversation" aria-live="off" onScroll={() => {
            const element = transcript.current;
            if (!element) return;
            if (element.scrollTop > 48) followExplanation.current = false;
            followOutput.current = element.scrollHeight - element.scrollTop - element.clientHeight < 48;
            if (followOutput.current) setNewOutput(false);
        }}>
            {showConversation && !needsEnable && phase !== 'preparing' && phase !== 'removing' && proactive && automatic && selected && <section className="cbm-chat-explanation" aria-label="Current selection explanation">
                <SourceDisclosure title="Generated explanation; check source for exact behavior.">
                    {explanation?.key === selected.key && explanation.packet ? <PacketSource packet={explanation.packet} citation={explanation.citation} codeOnly={!selected.reader} /> : null}
                </SourceDisclosure>
                {explanation?.key === selected.key ? <>
                    <ChatMarkdown text={explanation.answer || (explanation.status === 'generating' ? 'Explaining selection…' : explanation.status === 'stopped' ? 'Explanation stopped.' : '')} />
                    {explanation.generated && explanation.status === 'complete' && <small className="cbm-chat-model-note">{chatRound3Text.en.modelNote}</small>}
                    {explanation.status !== 'generating' && <AnswerNotes shortened={limitNote(explanation.shortened, explanation.limit, true)} packet={explanation.grounded ? undefined : explanation.packet} model={model.displayName} />}
                    {explanation.error && <p className="cbm-chat-turn-error" role="alert">{explanation.error}</p>}
                    {explanation.status !== 'generating' && <button type="button" className="cbm-chat-retry" disabled={phase !== 'ready' || !!selected.waiting} onClick={() => {
                        if (explanation.askable) modelAsked.current.add(selected.key);
                        lastAttempt.current = undefined; explanations.current.delete(selected.key); setRetryExplanation(value => value + 1);
                    }}>{explanation.askable ? relationshipWords.en.askModel : 'Explain again'}</button>}
                </> : <p>{selected.waiting === 'loading' ? browserChatText.waitingForScope : selected.waiting === 'partial' ? browserChatText.partialScope
                    : manualRequest.current ? 'This selection will be explained after your answer.' : 'Preparing explanation…'}</p>}
            </section>}
            {turns.length === 0 && !(proactive && automatic && selected) && <div className="cbm-chat-empty"><span aria-hidden="true">⌁</span><h3>Ask about the code.</h3><p>{readerContext ? 'The current file is included automatically. Mark code to focus your next message on that exact selection.' : 'Ask a question, or add source and graph context to your next message.'}</p></div>}
            {turns.map((turn, index) => { const replyWords = relationshipWords[turn.replyLanguage === 'de' ? 'de' : 'en'];
                // The model's own answers carry their note in the language of the question (B1).
                const ownWords = chatRound3Text[turn.replyLanguage ?? questionLanguage(turn.prompt)];
                const divider = topicDivider(turns, index);
                return <article className="cbm-chat-turn" key={turn.id}>
                {turn.topic && divider && <p className="cbm-chat-topic-break">{divider === 'back' ? ownWords.backTo(turn.topic.label) : topicText[turn.replyLanguage ?? questionLanguage(turn.prompt)].topicBreak(turn.topic.label)}</p>}
                <div className="cbm-chat-question"><span className="cbm-chat-speaker">You</span><ChatMarkdown text={turn.prompt} />
                    {turn.askedModel && <small className="cbm-chat-asked-model">{ownWords.askedModel}</small>}</div>
                <div className="cbm-chat-answer"><SourceDisclosure>
                    {turn.evidence || turn.attachment || turn.readerContext?.source || turn.context?.length ? <>
                        {turn.evidence ? <PacketSource packet={turn.evidence} language={turn.replyLanguage ?? questionLanguage(turn.prompt)} /> : <>
                            {turn.attachment && <Attachment attachment={turn.attachment} />}
                            {turn.readerContext?.source && <><Attachment attachment={turn.readerContext.source} label={turn.readerContext.source.kind === 'selection' ? 'Selection snapshot' : 'File snapshot'} />{turn.readerContext.source.partial && <p className="cbm-chat-evidence-note">{turn.readerContext.source.partial}</p>}</>}
                        </>}
                        {turn.context?.map(item => <ContextSnapshot key={item.id} context={item} />)}
                    </> : null}
                </SourceDisclosure><div className="cbm-chat-answer-text"><ChatMarkdown text={turn.answer || (turn.status === 'generating' ? 'Thinking…' : turn.status === 'stopped' ? 'Stopped before an answer.' : '')} /></div>
                    {turn.status === 'stopped' && turn.answer && <small>Stopped · partial answer</small>}
                    {!turn.answeredFrom && !turn.echo && turn.answer && (turn.status === 'complete' || turn.status === 'stopped') && <small className="cbm-chat-model-note">{ownWords.modelNote}</small>}
                    {turn.status !== 'generating' && !turn.answeredFrom && !turn.echo && <AnswerNotes shortened={limitNote(turn.shortened, turn.limit)} packet={turn.evidence}
                        unsupported={turn.answer ? namesNotIn(turn.answer, turn.request.map(message => message.content).join('\n')) : []} model={BROWSER_MODELS.find(candidate => candidate.id === turn.modelId)?.displayName ?? turn.modelId} historyOmitted={turn.historyOmitted} />}
                    {turn.status === 'error' && <p className="cbm-chat-turn-error" role="alert">{turn.error}</p>}
                    {index === turns.length - 1 && turn.answeredFrom === 'suggestion' && turn.suggestion && <button type="button" className="cbm-chat-retry"
                        onClick={() => showSuggestedList(turn)}>{replyWords.showList}</button>}
                    {index === turns.length - 1 && turn.status !== 'generating' && (!turn.answeredFrom || ASKABLE.has(turn.answeredFrom)) && <button type="button" className="cbm-chat-retry" disabled={phase !== 'ready'} onClick={() => { void send(turn); }}>{turn.answeredFrom ? replyWords.askModel : turn.status === 'error' ? 'Retry' : ownWords.askAgain}</button>}
                </div>
            </article>; })}
        </div>}
        {newOutput && <button type="button" className="cbm-chat-jump" onClick={() => { followOutput.current = true; setNewOutput(false); if (transcript.current) transcript.current.scrollTop = transcript.current.scrollHeight; }}>Latest answer ↓</button>}
        {newExplanation && proactive && automatic && selected && <button type="button" className="cbm-chat-jump" onClick={() => { followExplanation.current = true; setNewExplanation(false); if (transcript.current) transcript.current.scrollTop = 0; }}>Current selection ↑</button>}
        <form className="cbm-chat-composer" onSubmit={submit}>
            {questionQueued && <p className="cbm-chat-source-state" role="status">Your question is next…</p>}
            {currentReader && !currentReader.source && <p className="cbm-chat-source-state" role="status" title={currentReader.path}>{currentReader.status === 'loading' ? 'Loading current file…' : currentReader.status === 'empty' ? 'Open a file to include its code.' : 'Current file source is unavailable.'}</p>}
            {manualAttachment && <div className="cbm-chat-pending"><div className="cbm-chat-pending-title"><span>Attached to next message</span><button type="button" aria-label="Remove code attachment" disabled={!onAttachmentRemoved} onClick={() => onAttachmentRemoved?.(manualAttachment.id)}>×</button></div><Attachment attachment={manualAttachment} /></div>}
            {graphSelection && <div className="cbm-chat-pending"><div className="cbm-chat-pending-title"><span>Graph selection for next message</span><button type="button" aria-label="Remove graph selection" onClick={() => { setHandledContextId(graphSelection.id); onContextRemoved?.(graphSelection.id); }}>×</button></div><ContextSnapshot context={graphSelection} /></div>}
            {selectedContext.map(item => <div className="cbm-chat-pending" key={item.id}><div className="cbm-chat-pending-title"><span>Context for next message</span><button type="button" aria-label={`Remove ${item.label}`} onClick={() => setSelectedContext(previous => previous.filter(selected => selected.id !== item.id))}>×</button></div><ContextSnapshot context={item} /></div>)}
            {context.length > 0 && <details className="cbm-chat-context-options"><summary>Add context</summary><fieldset><legend>Include in the next message</legend>{context.map(item => <label key={item.id}><input type="checkbox" checked={selectedContext.some(selected => selected.id === item.id)} onChange={event => {
                const checked = event.target.checked;
                setSelectedContext(previous => checked ? [...previous.filter(selected => selected.id !== item.id), { ...item }] : previous.filter(selected => selected.id !== item.id));
            }} />{item.label}</label>)}</fieldset></details>}
            {showConversation && <><label className="cbm-chat-visually-hidden" htmlFor="cbm-chat-prompt">Message local model</label>
            <div className="cbm-chat-input-row"><textarea ref={input} id="cbm-chat-prompt" value={draft} placeholder="Ask about this code…" rows={1} onChange={event => setDraft(event.target.value)} onKeyDown={event => {
                if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); void send(); }
            }} />
                {(phase === 'counting' || phase === 'generating') && !(autoRun.current && !manualRequest.current && draft.trim() && !stopping) ? <button type="button" className="cbm-chat-send" aria-label={stopping ? 'Stopping…' : 'Stop'} title="Stop generating" disabled={stopping} onClick={stop}>■</button> : <button className="cbm-chat-primary cbm-chat-send" type="submit" aria-label="Send ↑" title="Send message" disabled={!historyReady || (!autoRun.current && phase !== 'ready') || !!manualRequest.current || !draft.trim() || readerContext?.status === 'loading'}>↑</button>}
            </div>

            </>}
        </form>
    </aside>}
    </>;
}
