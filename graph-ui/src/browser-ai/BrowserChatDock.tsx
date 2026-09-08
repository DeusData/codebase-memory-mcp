import { useEffect, useRef, useState, type FormEvent, type JSX } from 'react';
import type { BrowserAiProgress } from './browser-ai-controller';
import { createBrowserChatRuntime, type BrowserChatRuntime } from './browser-ai-runtime';
import { BROWSER_MODELS, removeBrowserModelCache } from './model-policy';
import { buildChatMessages, selectionLocation, snapshotAttachment, type BrowserChatAttachment, type BrowserChatContext, type BrowserChatTurn } from './chat-model';
import ChatMarkdown from './ChatMarkdown';
import './browser-chat.css';

export type { BrowserChatAttachment, BrowserChatContext } from './chat-model';
export interface BrowserChatDockProps {
    open: boolean;
    onClose: () => void;
    attachment?: BrowserChatAttachment;
    context?: readonly BrowserChatContext[];
    pendingContext?: BrowserChatContext;
    onContextConsumed?: (id: string) => void;
    onContextRemoved?: (id: string) => void;
    onAttachmentConsumed: (id: string) => void;
    onAttachmentRemoved?: (id: string) => void;
    createRuntime?: (modelId: string) => BrowserChatRuntime;
    removeCache?: (modelId: string) => Promise<void>;
}

type Phase = 'off' | 'preparing' | 'ready' | 'counting' | 'generating' | 'removing';
const initialModel = BROWSER_MODELS.find(model => model.availability === 'available')!;
const messageOf = (error: unknown): string => error instanceof Error ? error.message : String(error);
const sizeLabel = (bytes: number): string => bytes >= 1_000_000_000 ? `${(bytes / 1_000_000_000).toFixed(2)} GB` : `${Math.ceil(bytes / 1_000_000)} MB`;

function Attachment({ attachment }: { attachment: BrowserChatAttachment }): JSX.Element {
    return <details className="cbm-chat-attachment">
        <summary><span aria-hidden="true">⌁</span> {selectionLocation(attachment)}</summary>
        <pre>{attachment.text}</pre>
        <small>{attachment.project} · Source {attachment.sourceVersion}</small>
    </details>;
}

function ContextSnapshot({ context }: { context: BrowserChatContext }): JSX.Element {
    return <details className="cbm-chat-attachment"><summary>{context.label}</summary><pre>{context.text}</pre></details>;
}

/** Keep mounted when collapsed: state and worker lifetime are independent of visibility. */
export default function BrowserChatDock({ open, onClose, attachment, context = [], pendingContext, onContextConsumed, onContextRemoved, onAttachmentConsumed, onAttachmentRemoved, createRuntime = createBrowserChatRuntime, removeCache = removeBrowserModelCache }: BrowserChatDockProps): JSX.Element {
    const [modelId, setModelId] = useState(initialModel.id);
    const model = BROWSER_MODELS.find(candidate => candidate.id === modelId) ?? initialModel;
    const [phase, setPhase] = useState<Phase>('off');
    const [draft, setDraft] = useState('');
    const [turns, setTurns] = useState<BrowserChatTurn[]>([]);
    const [error, setError] = useState<string>();
    const [notice, setNotice] = useState<string>();
    const [progress, setProgress] = useState<BrowserAiProgress>();
    const [downloaded, setDownloaded] = useState<Set<string>>(() => new Set());
    const [tokenCount, setTokenCount] = useState<number>();
    const [showModels, setShowModels] = useState(true);
    const [newOutput, setNewOutput] = useState(false);
    const [stopping, setStopping] = useState(false);
    const [selectedContext, setSelectedContext] = useState<BrowserChatContext[]>([]);
    const [handledContextId, setHandledContextId] = useState<string>();
    const graphSelection = pendingContext?.id !== handledContextId ? pendingContext : undefined;
    const runtime = useRef<BrowserChatRuntime | undefined>(undefined);
    const epoch = useRef(0);
    const pending = useRef(false);
    const stopRequested = useRef(false);
    const nextTurn = useRef(0);
    const activeTurn = useRef<string | undefined>(undefined);
    const transcript = useRef<HTMLDivElement>(null);
    const followOutput = useRef(true);
    const input = useRef<HTMLTextAreaElement>(null);
    const busy = phase === 'preparing' || phase === 'counting' || phase === 'generating' || phase === 'removing';

    useEffect(() => () => { epoch.current += 1; runtime.current?.dispose(); runtime.current = undefined; }, []);
    useEffect(() => {
        const element = transcript.current;
        if (!element || !open) return;
        if (followOutput.current) { element.scrollTop = element.scrollHeight; setNewOutput(false); }
        else if (phase === 'generating') setNewOutput(true);
    }, [turns, open, phase]);
    useEffect(() => { if (open && (attachment || graphSelection)) input.current?.focus(); }, [open, attachment?.id, graphSelection?.id]);

    const release = (): void => {
        const id = activeTurn.current;
        if (id) setTurns(previous => previous.map(turn => turn.id === id ? { ...turn, status: 'stopped' } : turn));
        epoch.current += 1; pending.current = false; activeTurn.current = undefined;
        stopRequested.current = false; setStopping(false);
        runtime.current?.dispose(); runtime.current = undefined;
        setProgress(undefined); setTokenCount(undefined); setPhase('off');
    };
    const prepare = async (): Promise<void> => {
        if (pending.current || model.availability !== 'available') return;
        release(); pending.current = true;
        const ticket = epoch.current;
        setError(undefined); setNotice(undefined); setPhase('preparing');
        try {
            const nextRuntime = createRuntime(model.id);
            runtime.current = nextRuntime;
            await nextRuntime.prepare(value => { if (epoch.current === ticket) setProgress(value); });
            if (epoch.current !== ticket) return;
            setDownloaded(previous => new Set(previous).add(model.id));
            setPhase('ready'); setProgress(undefined); setShowModels(false); pending.current = false;
        } catch (failure) {
            if (epoch.current !== ticket) return;
            release(); setError(messageOf(failure));
        }
    };
    const stop = (): void => {
        if (phase === 'preparing') { release(); setNotice('Download stopped. Cached files can be reused or deleted.'); return; }
        if ((phase !== 'generating' && phase !== 'counting') || stopRequested.current) return;
        stopRequested.current = true; setStopping(true);
        if (phase === 'generating') runtime.current?.stop();
        // Stay busy until the in-flight GPU operation settles; a second send must not overlap it.
    };
    const send = async (retry?: BrowserChatTurn): Promise<void> => {
        const currentRuntime = runtime.current;
        if (!currentRuntime || phase !== 'ready' || pending.current || (!retry && !draft.trim())) return;
        pending.current = true; stopRequested.current = false; setStopping(false);
        const ticket = ++epoch.current;
        const prompt = retry?.prompt ?? draft;
        const source = snapshotAttachment(retry ? retry.attachment : attachment);
        const selectedGraph = !retry && graphSelection ? { ...graphSelection } : undefined;
        const nextContext = selectedGraph ? [...selectedContext.filter(item => item.id !== selectedGraph.id), selectedGraph] : selectedContext;
        const extra = (retry ? retry.context ?? [] : nextContext).map(item => ({ ...item }));
        const request = retry ? retry.request.map(message => ({ ...message })) : buildChatMessages(turns, prompt, source, extra);
        setError(undefined); setNotice(undefined); setPhase('counting');
        try {
            const count = await currentRuntime.countTokens(request);
            if (epoch.current !== ticket) return;
            if (stopRequested.current) return;
            setTokenCount(count);
            if (count + model.maxOutputTokens > model.contextTokens) {
                setError(`This prompt needs ${count.toLocaleString()} input tokens; ${Math.max(0, model.contextTokens - model.maxOutputTokens).toLocaleString()} fit with space for the answer. Select less code, start a new conversation, or choose a larger-context model. Nothing was sent or shortened.`);
                return;
            }
            const id = retry?.id ?? `local-turn-${++nextTurn.current}`;
            const turn: BrowserChatTurn = { id, prompt, attachment: source, context: extra, modelId: model.id, request, answer: '', status: 'generating' };
            activeTurn.current = id;
            if (retry) setTurns(previous => previous.map(item => item.id === id ? turn : item));
            else {
                setTurns(previous => [...previous, turn]);
                setDraft(previous => previous === prompt ? '' : previous);
                setSelectedContext(previous => previous.filter(item => !extra.some(sent => sent.id === item.id && sent.text === item.text)));
                if (source) onAttachmentConsumed(source.id);
                if (selectedGraph) { setHandledContextId(selectedGraph.id); onContextConsumed?.(selectedGraph.id); }
            }
            setPhase('generating');
            const output = await currentRuntime.chat(request, chunk => {
                if (epoch.current !== ticket || stopRequested.current) return;
                setTurns(previous => previous.map(item => item.id === id ? { ...item, answer: item.answer + chunk } : item));
            });
            if (epoch.current !== ticket) return;
            setTurns(previous => previous.map(item => item.id === id ? { ...item, answer: stopRequested.current ? item.answer : output, status: stopRequested.current ? 'stopped' : 'complete' } : item));
        } catch (failure) {
            if (epoch.current !== ticket) return;
            const explanation = messageOf(failure);
            const id = activeTurn.current;
            if (id) setTurns(previous => previous.map(turn => turn.id === id ? { ...turn, status: stopRequested.current ? 'stopped' : 'error', error: stopRequested.current ? undefined : explanation } : turn));
            else if (!stopRequested.current) setError(explanation);
        } finally {
            if (epoch.current === ticket) { activeTurn.current = undefined; pending.current = false; setStopping(false); setPhase('ready'); }
        }
    };
    const deleteCache = async (): Promise<void> => {
        if (pending.current) return;
        release(); pending.current = true;
        const ticket = epoch.current;
        setPhase('removing'); setError(undefined); setNotice(undefined);
        try {
            await removeCache(model.id);
            if (epoch.current !== ticket) return;
            setDownloaded(previous => { const next = new Set(previous); next.delete(model.id); return next; });
            setNotice('Cached model files deleted. Your conversation is still here.');
        } catch (failure) { if (epoch.current === ticket) setError(messageOf(failure)); }
        finally { if (epoch.current === ticket) { pending.current = false; setPhase('off'); } }
    };
    const clear = (): void => {
        if (busy || !turns.length || !window.confirm('Clear this local conversation? This removes its messages and attached code snapshots.')) return;
        setTurns([]); setTokenCount(undefined); setError(undefined); setNewOutput(false); followOutput.current = true;
    };
    const submit = (event: FormEvent): void => { event.preventDefault(); void send(); };
    const status = stopping ? 'Stopping' : phase === 'preparing' ? 'Loading' : phase === 'counting' ? 'Checking context' : phase === 'generating' ? 'Answering' : phase === 'removing' ? 'Deleting cache' : phase === 'ready' ? 'Loaded' : 'Off';

    return <aside className="cbm-chat-dock" hidden={!open} aria-label="Local chat">
        <header className="cbm-chat-header">
            <div><h2>Local chat</h2><span className="cbm-chat-subtitle">Code stays in this browser</span></div>
            <button type="button" className="cbm-chat-icon" aria-label="Collapse local chat" title="Collapse chat; keep conversation" onClick={onClose}>›</button>
        </header>
        <div className="cbm-chat-modelbar">
            <button type="button" aria-expanded={showModels} aria-controls="cbm-chat-model-settings" onClick={() => setShowModels(value => !value)}><span>{model.displayName}</span><span aria-hidden="true">⌄</span></button>
            <span className="cbm-chat-status" data-ready={phase === 'ready'} role="status">{status}</span>
        </div>
        {showModels && <section id="cbm-chat-model-settings" className="cbm-chat-settings" aria-label="Local model settings">
            <label htmlFor="cbm-chat-model">Model</label>
            <select id="cbm-chat-model" value={modelId} disabled={busy} onChange={event => { release(); setModelId(event.target.value); setError(undefined); setNotice(undefined); }}>
                {BROWSER_MODELS.map(candidate => <option key={candidate.id} value={candidate.id}>{candidate.displayName} · {candidate.availability === 'unsupported' ? 'Requires runtime support' : candidate.id === model.id && phase === 'ready' ? 'Loaded' : downloaded.has(candidate.id) ? 'Downloaded this session' : 'Available'}</option>)}
            </select>
            <p>{sizeLabel(model.bytes)} download · {model.license}. Memory use is higher.</p>
            {model.compatibilityNote && <p>{model.compatibilityNote}</p>}
            <p><a href={model.modelCard} target="_blank" rel="noreferrer">Model details</a> · Downloads come from Hugging Face. No model downloads automatically.</p>
            <div className="cbm-chat-model-actions">
                {phase === 'off' && <button type="button" className="cbm-chat-primary" disabled={model.availability !== 'available'} onClick={() => { void prepare(); }}>{downloaded.has(model.id) ? 'Load model' : 'Download & load'}</button>}
                {(phase === 'ready' || phase === 'generating' || phase === 'counting') && <button type="button" onClick={() => { release(); setNotice('Model unloaded. Conversation and cached files retained.'); }}>Unload model</button>}
                <button type="button" disabled={busy} onClick={() => { void deleteCache(); }}>Delete cached model</button>
            </div>
        </section>}
        {phase === 'preparing' && <div className="cbm-chat-loading" role="status"><progress max={100} value={progress?.progress} /><span>{progress?.file ?? 'Preparing browser model…'}</span><button type="button" onClick={stop}>Stop download</button></div>}
        {error && <div className="cbm-chat-error" role="alert">{error}</div>}
        {notice && <p className="cbm-chat-notice" role="status">{notice}</p>}
        <div className="cbm-chat-transcript" ref={transcript} role="log" aria-label="Conversation" aria-live="off" onScroll={() => {
            const element = transcript.current;
            if (!element) return;
            followOutput.current = element.scrollHeight - element.scrollTop - element.clientHeight < 48;
            if (followOutput.current) setNewOutput(false);
        }}>
            {turns.length === 0 && <div className="cbm-chat-empty"><span aria-hidden="true">⌁</span><h3>Ask about the code.</h3><p>Select code in the reader, then choose “Ask about selection”. Its exact text will travel with your next message.</p><p>You can also ask a question without an attachment.</p></div>}
            {turns.map((turn, index) => <article className="cbm-chat-turn" key={turn.id}>
                <div className="cbm-chat-question"><span className="cbm-chat-speaker">You</span><ChatMarkdown text={turn.prompt} />{turn.attachment && <Attachment attachment={turn.attachment} />}{turn.context?.map(item => <ContextSnapshot key={item.id} context={item} />)}</div>
                <div className="cbm-chat-answer"><span className="cbm-chat-speaker">{BROWSER_MODELS.find(candidate => candidate.id === turn.modelId)?.displayName ?? turn.modelId}</span><div className="cbm-chat-answer-text"><ChatMarkdown text={turn.answer || (turn.status === 'generating' ? 'Thinking…' : turn.status === 'stopped' ? 'Stopped before an answer.' : '')} /></div>
                    {turn.status === 'stopped' && turn.answer && <small>Stopped · partial answer</small>}
                    {turn.status === 'error' && <p className="cbm-chat-turn-error" role="alert">{turn.error}</p>}
                    {index === turns.length - 1 && turn.status !== 'generating' && <button type="button" className="cbm-chat-retry" disabled={phase !== 'ready'} onClick={() => { void send(turn); }}>Retry</button>}
                </div>
            </article>)}
        </div>
        {newOutput && <button type="button" className="cbm-chat-jump" onClick={() => { followOutput.current = true; setNewOutput(false); if (transcript.current) transcript.current.scrollTop = transcript.current.scrollHeight; }}>Latest answer ↓</button>}
        <form className="cbm-chat-composer" onSubmit={submit}>
            {attachment && <div className="cbm-chat-pending"><div className="cbm-chat-pending-title"><span>Attached to next message</span><button type="button" aria-label="Remove code attachment" disabled={!onAttachmentRemoved} onClick={() => onAttachmentRemoved?.(attachment.id)}>×</button></div><Attachment attachment={attachment} /></div>}
            {graphSelection && <div className="cbm-chat-pending"><div className="cbm-chat-pending-title"><span>Graph selection for next message</span><button type="button" aria-label="Remove graph selection" onClick={() => { setHandledContextId(graphSelection.id); onContextRemoved?.(graphSelection.id); }}>×</button></div><ContextSnapshot context={graphSelection} /></div>}
            {selectedContext.map(item => <div className="cbm-chat-pending" key={item.id}><div className="cbm-chat-pending-title"><span>Context for next message</span><button type="button" aria-label={`Remove ${item.label}`} onClick={() => setSelectedContext(previous => previous.filter(selected => selected.id !== item.id))}>×</button></div><ContextSnapshot context={item} /></div>)}
            {context.length > 0 && <details className="cbm-chat-context-options"><summary>Add context</summary><fieldset><legend>Include in the next message</legend>{context.map(item => <label key={item.id}><input type="checkbox" checked={selectedContext.some(selected => selected.id === item.id)} onChange={event => {
                const checked = event.target.checked;
                setSelectedContext(previous => checked ? [...previous.filter(selected => selected.id !== item.id), { ...item }] : previous.filter(selected => selected.id !== item.id));
            }} />{item.label}</label>)}</fieldset></details>}
            <label className="cbm-chat-visually-hidden" htmlFor="cbm-chat-prompt">Message local model</label>
            <textarea ref={input} id="cbm-chat-prompt" value={draft} placeholder="Ask about this code…" rows={3} onChange={event => setDraft(event.target.value)} onKeyDown={event => {
                if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); void send(); }
            }} />
            <div className="cbm-chat-compose-actions"><button type="button" className="cbm-chat-clear" disabled={busy || !turns.length} onClick={clear}>New conversation</button>
                {phase === 'counting' || phase === 'generating' ? <button type="button" disabled={stopping} onClick={stop}>{stopping ? 'Stopping…' : 'Stop'}</button> : <button className="cbm-chat-primary" type="submit" disabled={phase !== 'ready' || !draft.trim()}>Send ↑</button>}
            </div>
            <p className="cbm-chat-context">{tokenCount !== undefined ? `${tokenCount.toLocaleString()} input tokens in last check · ` : ''}{turns.length ? 'Earlier messages stay in context. ' : ''}Session only · Verify model answers.</p>
        </form>
    </aside>;
}
