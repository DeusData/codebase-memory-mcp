import { useEffect, useId, useRef, useState } from 'react';
import type { JSX } from 'react';
import ChatMarkdown from '../browser-ai/ChatMarkdown';
import type { ProjectsSource } from '../projects/ProjectsPanel';
import type { AdrRecord } from '../projects/projects-model';
import { ADR_CONTENT_LIMIT, adrSize, appendDecision, clearAdrDraft, readAdrDraft, sameAdr, storeAdrDraft } from './adr-model';
import { adrStrings as s } from './adr-strings';
import './adr.css';

export interface AdrWorkspaceProps {
    project: string;
    active: boolean;
    source: Pick<ProjectsSource, 'adr' | 'saveAdr'>;
}

function busyError(error: unknown): boolean {
    return typeof error === 'object' && error !== null && 'status' in error && error.status === 423;
}

interface EditorState {
    base?: AdrRecord;
    content: string;
    editing: boolean;
    preview: boolean;
    phase: 'idle' | 'loading' | 'ready' | 'error';
    error: string;
    notice: string;
    conflict?: AdrRecord;
    reviewing: boolean;
}

function ProjectAdrWorkspace({ project, active, source }: AdrWorkspaceProps): JSX.Element {
    const [state, setState] = useState<EditorState>(() => {
        const draft = readAdrDraft(project);
        return { base: draft?.base, content: draft?.content ?? '', editing: !!draft, preview: false,
            phase: 'idle', error: '', notice: draft ? s.restored : '', reviewing: false };
    });
    const [saving, setSaving] = useState(false);
    const [storageAvailable, setStorageAvailable] = useState(true);
    const [reload, setReload] = useState(0);
    const stateRef = useRef(state);
    stateRef.current = state;
    const mounted = useRef(true);
    const savingRef = useRef(false);
    const loadTicket = useRef(0);
    const lastReload = useRef(0);
    const hasLoaded = useRef(false);
    const editor = useRef<HTMLTextAreaElement>(null);
    const document = useRef<HTMLDivElement>(null);
    const headingPrefix = useId();
    const [outline, setOutline] = useState<{ id: string; label: string; level: number }[]>([]);
    const dirty = !!state.base && state.content !== state.base.content;
    const size = adrSize(project, state.content);

    useEffect(() => { mounted.current = true; return () => { mounted.current = false; loadTicket.current += 1; }; }, []);

    useEffect(() => {
        const current = stateRef.current;
        const forced = reload !== lastReload.current;
        lastReload.current = reload;
        if (!active || savingRef.current || (hasLoaded.current && current.phase === 'ready' && current.base && current.content !== current.base.content && !forced)) return;
        const ticket = ++loadTicket.current;
        setState(value => ({ ...value, phase: 'loading', error: '' }));
        void source.adr(project).then(record => {
            if (!mounted.current || ticket !== loadTicket.current) return;
            hasLoaded.current = true;
            setState(value => {
                if (value.base && value.content !== value.base.content) {
                    return { ...value, phase: 'ready', error: '', conflict: sameAdr(value.base, record) ? undefined : record };
                }
                return { ...value, base: record, content: record.content, phase: 'ready', error: '', conflict: undefined };
            });
        }, error => {
            if (mounted.current && ticket === loadTicket.current) setState(value => ({ ...value, phase: 'error', error: busyError(error) ? s.busy : s.readFailed }));
        });
        return () => { if (ticket === loadTicket.current) loadTicket.current += 1; };
    }, [active, project, source, reload]);

    useEffect(() => {
        if (state.base && dirty) setStorageAvailable(storeAdrDraft(project, { version: 1, base: state.base, content: state.content }));
        else if (state.base) clearAdrDraft(project);
    }, [project, state.base, state.content, dirty]);

    useEffect(() => {
        if (!dirty) return;
        const warn = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ''; };
        window.addEventListener('beforeunload', warn);
        return () => window.removeEventListener('beforeunload', warn);
    }, [dirty]);

    useEffect(() => {
        const headings = [...(document.current?.querySelectorAll<HTMLElement>('h1, h2, h3, h4, h5, h6') ?? [])];
        const entries = headings.map((heading, index) => {
            heading.id = `${headingPrefix}-heading-${index}`;
            heading.tabIndex = -1;
            return { id: heading.id, label: heading.textContent ?? '', level: Number(heading.tagName.slice(1)) };
        });
        const level = entries.some(heading => heading.level === 2) ? 2 : Math.min(...entries.map(heading => heading.level));
        setOutline(entries.filter(heading => heading.level === level));
    }, [state.content, state.editing, state.preview, state.phase, headingPrefix]);

    useEffect(() => { if (state.editing && active) editor.current?.focus(); }, [state.editing, active]);

    const acceptSaved = (record: AdrRecord): void => {
        clearAdrDraft(project);
        setState(value => ({ ...value, base: record, content: record.content, phase: 'ready', editing: false,
            error: '', notice: s.saved, conflict: undefined, reviewing: false }));
    };
    const save = async (): Promise<void> => {
        const current = stateRef.current;
        if (savingRef.current || current.phase !== 'ready' || !current.base || current.conflict
            || current.content === current.base.content || !adrSize(project, current.content).valid) return;
        savingRef.current = true;
        loadTicket.current += 1;
        setSaving(true);
        setState(value => ({ ...value, error: '', notice: '' }));
        let stage: 'check' | 'write' | 'verify' = 'check';
        try {
            const latest = await source.adr(project);
            if (!mounted.current) return;
            if (!sameAdr(current.base, latest)) {
                if (latest.content === current.content && (latest.hasAdr || current.content === '')) acceptSaved(latest);
                else setState(value => ({ ...value, conflict: latest, reviewing: false }));
                return;
            }
            stage = 'write';
            await source.saveAdr(project, current.content);
            if (!mounted.current) return;
            stage = 'verify';
            const verified = await source.adr(project);
            if (!mounted.current) return;
            if (verified.content !== current.content || (!verified.hasAdr && current.content !== '')) {
                setState(value => ({ ...value, conflict: verified, reviewing: false, error: s.mismatch }));
                return;
            }
            acceptSaved(verified);
        } catch (error) {
            if (mounted.current) setState(value => ({ ...value, error: busyError(error) ? s.busy
                : stage === 'check' ? s.checkFailed : stage === 'verify' ? s.verifyFailed : s.saveFailed }));
        } finally {
            savingRef.current = false;
            if (mounted.current) setSaving(false);
        }
    };
    const startEditing = (append = false): void => setState(value => ({ ...value, editing: true,
        content: append ? appendDecision(value.content) : value.content, error: '', notice: '' }));
    const cancel = (): void => {
        clearAdrDraft(project);
        setState(value => ({ ...value, base: value.conflict ?? value.base, content: (value.conflict ?? value.base)?.content ?? '', editing: false,
            preview: false, conflict: undefined, reviewing: false, error: '', notice: '' }));
    };
    const useLatest = (discard: boolean): void => setState(value => value.conflict ? ({ ...value,
        base: value.conflict, content: discard ? value.conflict.content : value.content,
        editing: !discard, conflict: undefined, reviewing: false, phase: 'ready', error: '', notice: discard ? '' : s.rebased }) : value);
    const ready = state.phase === 'ready';
    const documentVisible = !state.editing || state.preview;

    return <section className="adr-workspace" aria-label={s.label} aria-busy={state.phase === 'loading' || saving}>
        <header className="adr-toolbar">
            <div className="adr-project"><strong>{project}</strong><span role="status">{saving ? s.saving : dirty ? s.draft : state.notice || (state.base?.updatedAt ? s.edited : s.noDate)}</span>
                {!dirty && !state.notice && state.base?.updatedAt && <time dateTime={state.base.updatedAt}>{state.base.updatedAt}</time>}</div>
            <div className="adr-actions">{state.editing ? <>
                <button type="button" aria-pressed={state.preview} disabled={saving} onClick={() => setState(value => ({ ...value, preview: !value.preview }))}>{state.preview ? s.hidePreview : s.preview}</button>
                <button type="button" disabled={saving} onClick={cancel}>{s.cancel}</button>
                <button type="button" className="adr-primary" disabled={!ready || !dirty || !size.valid || saving || !!state.conflict} onClick={() => { void save(); }}>{saving ? s.saving : s.save}</button>
            </> : <>
                <button type="button" disabled={state.phase === 'loading'} onClick={() => setReload(value => value + 1)}>{s.refresh}</button>
                {ready && state.base?.hasAdr && <button type="button" onClick={() => startEditing()}>{s.edit}</button>}
                {ready && state.base?.hasAdr && <button type="button" className="adr-primary" onClick={() => startEditing(true)}>{s.add}</button>}
            </>}</div>
        </header>
        {state.error && <div className="adr-notice" role="alert"><span>{state.error}</span>
            {state.phase === 'error' && <button type="button" onClick={() => setReload(value => value + 1)}>{s.retry}</button>}</div>}
        {dirty && state.notice && <p className="adr-notice" role="status">{state.notice}</p>}
        {dirty && !storageAvailable && <p className="adr-notice" role="status">{s.storageUnavailable}</p>}
        {state.conflict && <div className="adr-conflict">
            <div className="adr-notice" role="alert"><span>{s.conflict}</span><button type="button" disabled={saving}
                aria-expanded={state.reviewing} onClick={() => setState(value => ({ ...value, reviewing: !value.reviewing }))}>{state.reviewing ? s.hideReview : s.review}</button></div>
            {state.reviewing && <><section className="adr-latest" aria-label={s.latest}>
                <p className="adr-section-label">{s.latest}</p>{state.conflict.content ? <ChatMarkdown text={state.conflict.content} /> : <p>{s.latestEmpty}</p>}
            </section><div className="adr-conflict-actions"><button type="button" onClick={() => useLatest(false)}>{s.rebase}</button>
                <button type="button" onClick={() => useLatest(true)}>{s.reload}</button></div></>}
        </div>}
        {state.phase === 'loading' && !state.base && <p className="adr-empty" role="status">{s.loading}</p>}
        {state.editing && <div className="adr-edit-info"><span className={size.valid ? '' : 'adr-limit-exceeded'}>{s.size(size.bytes)}</span>
            {!size.valid && <span role="alert">{size.bytes > ADR_CONTENT_LIMIT ? s.sizeExceeded : s.bodyExceeded}</span>}</div>}
        {state.base && (state.editing || state.base.hasAdr) && <div className="adr-body" data-editing={state.editing} data-preview={state.preview} data-outline={outline.length > 0 && documentVisible}>
            {state.editing && <textarea ref={editor} className="adr-editor" aria-label={s.editor} spellCheck={false} disabled={saving}
                value={state.content} onChange={event => setState(value => ({ ...value, content: event.target.value, error: '', notice: '' }))} />}
            {documentVisible && <>
                <div ref={document} className="adr-document-area" aria-label={s.document}><article className="adr-document"><ChatMarkdown text={state.content} /></article></div>
                {outline.length > 0 && <nav className="adr-outline" aria-label={s.outline}><p>{s.outline}</p>{outline.map(heading => <button
                    key={heading.id} type="button" data-level={heading.level} onClick={() => {
                        const node = document.current?.querySelector<HTMLElement>(`[id="${heading.id}"]`);
                        node?.scrollIntoView({ block: 'start', behavior: 'smooth' }); node?.focus({ preventScroll: true });
                    }}>{heading.label}</button>)}</nav>}
            </>}
        </div>}
        {ready && !state.editing && !state.base?.hasAdr && <div className="adr-empty"><h2>{s.emptyTitle}</h2><p>{s.emptyBody}</p>
            <button type="button" className="adr-primary" onClick={() => startEditing(true)}>{s.create}</button></div>}
    </section>;
}

export default function AdrWorkspace(props: AdrWorkspaceProps): JSX.Element {
    return props.project ? <ProjectAdrWorkspace key={props.project} {...props} />
        : <section className="adr-workspace" aria-label={s.label}><p className="adr-empty">{s.noProject}</p></section>;
}
