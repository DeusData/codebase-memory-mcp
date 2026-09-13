import { useEffect, useRef, useState } from 'react';
import type { JSX } from 'react';
import { BrowserAiController } from './browser-ai-controller';
import type { BrowserAiRuntime, BrowserAiSource, BrowserAiState } from './browser-ai-controller';
import { createBrowserAiRuntime } from './browser-ai-runtime';
import { BROWSER_MODEL, removeBrowserModelCache } from './model-policy';
import { browserAiText as text } from './strings';
import './browser-ai.css';

export interface BrowserAiPanelProps {
    source?: BrowserAiSource;
    onClose: () => void;
    /** Injected only by deterministic lifecycle tests. No worker exists before Enable. */
    createRuntime?: () => BrowserAiRuntime;
    removeCache?: () => Promise<void>;
}

export default function BrowserAiPanel({ source, onClose, createRuntime = createBrowserAiRuntime, removeCache = removeBrowserModelCache }: BrowserAiPanelProps): JSX.Element {
    const dialog = useRef<HTMLDialogElement>(null);
    const [state, setState] = useState<BrowserAiState>({ phase: 'off' });
    const [controller] = useState(() => new BrowserAiController(createRuntime, removeCache, setState));
    useEffect(() => {
        const element = dialog.current;
        if (element && !element.open) {
            if (typeof element.showModal === 'function') element.showModal();
            else element.setAttribute('open', '');
        }
        return () => controller.dispose();
    }, [controller]);
    const close = () => { controller.dispose(); onClose(); };
    const busy = state.phase === 'preparing' || state.phase === 'generating';
    const phaseText = state.phase === 'preparing' ? text.preparing : state.phase === 'generating' ? text.generating
        : state.phase === 'ready' ? text.ready : state.phase === 'removing' ? text.removing : text.off;
    const progress = state.progress;
    return <dialog ref={dialog} className="atlas-browser-ai" aria-labelledby="atlas-browser-ai-title" onCancel={event => { event.preventDefault(); close(); }}>
        <header><div><h2 id="atlas-browser-ai-title">{text.title}</h2><p>{text.subtitle}</p></div>
            <button className="atlas-browser-ai-close" aria-label={text.close} onClick={close}>×</button></header>
        <div className="atlas-browser-ai-body">
            <div className="atlas-browser-ai-model"><span className="atlas-browser-ai-icon" aria-hidden="true">✦</span><div>
                <strong>{BROWSER_MODEL.displayName}</strong><p>{text.format}</p></div><span className="atlas-browser-ai-status" data-phase={state.phase}>{phaseText}</span></div>
            {(state.phase === 'off' || state.phase === 'error') && <><p>{text.optIn}</p><p className="atlas-browser-ai-muted">{text.device}</p></>}
            <p className="atlas-browser-ai-privacy">{text.privacy}</p>
            {state.phase === 'error' && <div role="alert" className="atlas-browser-ai-error"><strong>{text.failed}</strong><p>{state.error}</p></div>}
            {busy && <div role="status" className="atlas-browser-ai-progress"><p>{phaseText}</p>
                {state.phase === 'preparing' && <><progress max="100" value={progress?.progress} />
                    <span>{progress?.file}{progress?.loaded !== undefined && progress.total ? ` · ${text.progress(progress.loaded, progress.total)}` : ''}</span></>}
            </div>}
            <div className="atlas-browser-ai-actions">
                {(state.phase === 'off' || state.phase === 'error') && <button className="atlas-browser-ai-primary" onClick={() => { void controller.prepare(); }}>{text.download}</button>}
                {state.phase === 'ready' && <button className="atlas-browser-ai-primary" disabled={!source?.text.trim()} onClick={() => { if (source) void controller.explain(source); }}>{text.explain}</button>}
                {busy && <button onClick={() => controller.cancel()}>{text.cancel}</button>}
                {state.phase === 'ready' && <button onClick={() => controller.cancel()}>{text.disable}</button>}
                <button disabled={state.phase === 'removing'} onClick={() => { void controller.remove(); }}>{text.remove}</button>
            </div>
            {source ? <details className="atlas-browser-ai-source"><summary>{text.excerpt} · {source.project} · {text.location(source.path, source.startLine)}</summary>
                <pre>{source.text}</pre></details> : <p className="atlas-browser-ai-empty">{text.noSource}</p>}
            {state.output && <section className="atlas-browser-ai-output"><h3>{text.output}</h3>
                {state.outputSource && <p>{state.outputSource.project} · {text.location(state.outputSource.path, state.outputSource.startLine)}</p>}
                <div>{state.output}</div><p>{text.outputNote}</p></section>}
            <details className="atlas-browser-ai-details"><summary>{text.provenance}</summary><dl>
                <dt>{text.model}</dt><dd><a href={BROWSER_MODEL.modelCard} target="_blank" rel="noreferrer">{BROWSER_MODEL.id}</a></dd>
                <dt>{text.revision}</dt><dd>{BROWSER_MODEL.revision}</dd>
            </dl><p><a href={BROWSER_MODEL.baseModelCard} target="_blank" rel="noreferrer">{BROWSER_MODEL.license}</a></p><p>{text.storage}</p></details>
        </div>
    </dialog>;
}
