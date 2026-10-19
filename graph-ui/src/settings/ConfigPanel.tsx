import { useCallback, useEffect, useRef, useState } from 'react';
import type { KeyboardEvent } from 'react';
import BrowserConfig, { type BrowserConfigProps } from './BrowserConfig';
import ConfigReference from './ConfigReference';
import { CONFIG_TEXT, configValueError, type ConfigService, type ConfigSetting, type ConfigSnapshot } from './config-model';
import './configuration.css';

interface Props extends BrowserConfigProps { service: ConfigService; onClose: () => void; embedded?: boolean; active?: boolean }
const displayValue = (value: string | null) => value === null ? 'Automatic' : value === 'true' ? 'On' : value === 'false' ? 'Off' : value || '(empty)';
const title = (value: string) => value.replace(/[_-]/g, ' ').replace(/^./, char => char.toUpperCase());
const timing = (value: string) => ({ restart: 'Process restart', 'next-session': 'Next session', 'next-request': 'Next request', live: 'Immediate', startup: 'After restart', 'next-index': 'Next index' }[value] ?? title(value));

function SettingRow({ setting, draft, changed, onChange, disabled }: { setting: ConfigSetting; draft?: string | null; changed: boolean; onChange: (value: string | null) => void; disabled: boolean }) {
    const value = changed ? draft ?? null : setting.override ?? setting.value;
    const error = changed ? configValueError(setting, value) : undefined;
    const id = `config-${setting.key}`;
    const legacyInput = !changed && value !== null && !!configValueError(setting, value);
    const effectiveLabel = setting.effectiveKnown === false ? `Runtime interprets ${displayValue(setting.effective)}` : `Using ${displayValue(setting.effective)}`;
    return <article className={`cbm-config-setting${changed ? ' is-changed' : ''}`}>
        <div className="cbm-config-setting-main"><div><label htmlFor={id}>{setting.label}</label><p>{setting.description}</p></div>
            <div className="cbm-config-value">
                {!setting.editable ? <span>{displayValue(setting.effective)}</span>
                    : setting.type === 'boolean' || setting.type === 'enum' ? <select id={id} value={value ?? ''} disabled={disabled} aria-invalid={!!error} onChange={event => onChange(event.target.value || null)}>
                        <option value="">Inherited / automatic</option>{(setting.type === 'boolean' ? ['true', 'false'] : setting.options ?? []).map(option => <option key={option} value={option}>{displayValue(option)}</option>)}
                    </select> : <input id={id} type={setting.type === 'integer' || setting.type === 'number' ? 'number' : 'text'}
                        value={legacyInput ? '' : value ?? ''} placeholder="Inherited / automatic" min={setting.minimum} max={setting.maximum} step={setting.type === 'number' ? 'any' : 1}
                        disabled={disabled} aria-invalid={!!error} onChange={event => onChange(event.target.value)} />}
                {setting.editable && <button type="button" disabled={disabled || (!changed && setting.override === null)} onClick={() => onChange(null)}>Reset</button>}
            </div>
        </div>
        {error && <p className="cbm-config-error" role="alert">{error}</p>}
        <div className="cbm-config-setting-meta"><span>{setting.pendingRestart ? 'Restart pending' : timing(setting.applyMode)}</span><span>{effectiveLabel}</span><span>{title(setting.source)}</span>{changed && <span>{draft === null ? 'Reset staged' : 'Unsaved'}</span>}</div>
        {setting.readOnlyReason && <p className="cbm-config-readonly">{setting.readOnlyReason}</p>}
        <details className="cbm-config-setting-details"><summary>Source & default</summary><dl>
            <div><dt>Key</dt><dd>{setting.key}</dd></div><div><dt>Default</dt><dd>{displayValue(setting.defaultValue)}</dd></div>
            <div><dt>Saved override</dt><dd>{setting.override === null ? 'None' : displayValue(setting.override)}</dd></div>
            {setting.environment && <div><dt>Environment</dt><dd>{setting.environment}</dd></div>}
            {setting.cli && <div><dt>CLI</dt><dd>{setting.cli}</dd></div>}
            {setting.sourceFile && <div><dt>Consumer</dt><dd>{setting.sourceFile}</dd></div>}
        </dl></details>
    </article>;
}

export default function ConfigPanel({ service, onClose, embedded = false, active = true, ...browser }: Props) {
    const [snapshot, setSnapshot] = useState<ConfigSnapshot>();
    const [category, setCategory] = useState('');
    const [draft, setDraft] = useState<Record<string, string | null>>({});
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState('');
    const [notice, setNotice] = useState('');
    const [confirmClose, setConfirmClose] = useState(false);
    const dialog = useRef<HTMLDivElement>(null);
    const generation = useRef(0);
    const pending = useRef(false);
    const count = Object.keys(draft).length;
    const draftCount = useRef(count);
    draftCount.current = count;
    const references = snapshot?.settings.filter(setting => setting.category.toLowerCase() === 'reference') ?? [];
    const categories = [...new Set(snapshot?.settings.filter(setting => setting.category.toLowerCase() !== 'reference').map(setting => setting.category) ?? [])];
    const activeCategory = category || categories[0] || 'browser';
    const invalid = snapshot?.settings.some(setting => Object.hasOwn(draft, setting.key) && configValueError(setting, draft[setting.key]));
    const reload = useCallback(async () => {
        const ticket = ++generation.current;
        setLoading(true); setError(''); setNotice('');
        try { const next = await service.configuration(); if (ticket === generation.current) { setSnapshot(next); setDraft({}); setConfirmClose(false); } }
        catch (failure) { if (ticket === generation.current) setError(failure instanceof Error ? failure.message : 'Could not read configuration.'); }
        finally { if (ticket === generation.current) setLoading(false); }
    }, [service]);
    useEffect(() => () => { generation.current++; }, [service]);
    useEffect(() => { if (active && draftCount.current === 0 && !pending.current) void reload(); }, [active, reload]);
    useEffect(() => {
        if (embedded) return;
        const previous = document.activeElement;
        dialog.current?.focus();
        return () => { if (previous instanceof HTMLElement && previous.isConnected) previous.focus(); };
    }, [embedded]);
    const close = () => { if (pending.current) return; if (count) setConfirmClose(true); else onClose(); };
    const keyDown = (event: KeyboardEvent<HTMLDivElement>) => {
        if (event.key === 'Escape') { event.stopPropagation(); close(); }
        if (event.key !== 'Tab') return;
        const focusable = [...(dialog.current?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), select:not(:disabled), summary, [tabindex="0"]') ?? [])].filter(element => element.getClientRects().length);
        const first = focusable[0], last = focusable.at(-1);
        if (!first) { event.preventDefault(); return; }
        if (event.shiftKey && (document.activeElement === first || document.activeElement === dialog.current)) { event.preventDefault(); last?.focus(); }
        else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    };
    const save = async () => {
        if (!snapshot || pending.current || !count || invalid) return;
        pending.current = true; setSaving(true); setError(''); setNotice('');
        const ticket = ++generation.current;
        try {
            const next = await service.saveConfiguration(snapshot.revision, draft);
            if (ticket === generation.current) {
                setSnapshot(next); setDraft({}); setConfirmClose(false);
                setNotice(next.settings.some(setting => setting.pendingRestart) ? 'Saved. Restart CBM to apply pending settings throughout.' : 'Saved. Changes apply at the times shown below.');
            }
        } catch (failure) { if (ticket === generation.current) setError(failure instanceof Error ? failure.message : 'Could not save configuration. Your edits are retained.'); }
        finally { pending.current = false; if (ticket === generation.current) setSaving(false); }
    };
    const navigate = (action: () => void) => { if (count) { setError('Save or discard your daemon changes before opening another settings panel.'); return; } action(); };
    const panel = <div className={`cbm-config-dialog${embedded ? ' cbm-config-embedded' : ''}`} ref={dialog} role={embedded ? 'region' : 'dialog'} aria-modal={embedded ? undefined : true} aria-labelledby="cbm-config-title" tabIndex={embedded ? undefined : -1} onKeyDown={embedded ? undefined : keyDown}>
            <header className="cbm-config-heading"><div><h2 id="cbm-config-title">{embedded ? 'Configuration' : CONFIG_TEXT.title}</h2><p>Daemon settings and browser preferences</p></div>{!embedded && <button type="button" onClick={close} disabled={saving} aria-label={CONFIG_TEXT.close}>×</button>}</header>
            <nav className="cbm-config-categories" aria-label="Configuration categories">{[...categories, 'browser', 'reference'].map(value => <button type="button" key={value} aria-pressed={activeCategory === value} onClick={() => setCategory(value)}>{value === 'reference' ? 'External inputs' : title(value)}</button>)}</nav>
            <div className="cbm-config-content">
                {error && <div className="cbm-config-error" role="alert">{error}<p>Your edits have not been applied. Reload values to review a newer daemon configuration.</p></div>}
                {notice && <p className="cbm-config-notice" role="status">{notice}</p>}
                {confirmClose && <div className="cbm-config-unsaved" role="alert"><p>Discard {count} unsaved daemon {count === 1 ? 'change' : 'changes'}?</p><button type="button" onClick={() => setConfirmClose(false)}>Keep editing</button><button type="button" onClick={onClose}>Discard & close</button></div>}
                {activeCategory === 'browser' ? <BrowserConfig {...browser} onOpenBrowserModels={() => navigate(browser.onOpenBrowserModels)} onOpenDisplay={() => navigate(browser.onOpenDisplay)} />
                    : activeCategory === 'reference' ? <><ConfigReference />{references.length > 0 && <details className="cbm-config-setting-details"><summary>Daemon reference values</summary>{references.map(setting => <SettingRow key={setting.key} setting={setting} changed={false} disabled onChange={() => undefined} />)}</details>}</>
                    : <><p className="cbm-config-scope">Applies to this CBM installation, across projects. Saved overrides take precedence when each process starts; restart CBM to apply them throughout. Reset restores inherited values.</p>
                        {loading && <p role="status">Reading configuration…</p>}
                        {snapshot?.settings.filter(setting => setting.category === activeCategory).map(setting => <SettingRow key={setting.key} setting={setting} draft={draft[setting.key]} changed={Object.hasOwn(draft, setting.key)} disabled={saving || loading} onChange={value => {
                            setNotice(''); setDraft(previous => { const next = { ...previous }; if (value === setting.override || (setting.override === null && value === setting.value)) delete next[setting.key]; else next[setting.key] = value; return next; });
                        }} />)}
                    </>}
            </div>
            <footer className="cbm-config-footer"><span>{count ? `${count} unsaved ${count === 1 ? 'change' : 'changes'}` : snapshot ? `${snapshot.settings.length} daemon settings` : 'Daemon configuration unavailable'}</span><div>
                <button type="button" disabled={loading || saving} onClick={() => { if (count) { setDraft({}); setNotice('Unsaved edits discarded.'); } else void reload(); }}>{count ? 'Discard edits' : 'Reload values'}</button>
                <button type="button" className="cbm-config-save" disabled={!count || !!invalid || loading || saving} onClick={() => { void save(); }}>{saving ? 'Saving…' : 'Save changes'}</button>
            </div></footer>
        </div>;
    return embedded ? panel : <div className="cbm-config-backdrop" onClick={event => { if (event.target === event.currentTarget) close(); }}>{panel}</div>;
}
