import { useEffect, useRef, useState } from 'react';
import type { JSX } from 'react';
import type { ProjectEntry } from '../provider/rpc-schemas';
import type { ProjectsSource } from './ProjectsPanel';
import { childPath, projectNameFor } from './projects-model';
import type { BrowseLevel } from './projects-model';
import './add-project-index.css';

export interface AddProjectIndexActivity {
    name: string;
    status: 'indexing' | 'done' | 'error';
}
export interface AddProjectIndexDialogProps {
    open: boolean;
    source: Pick<ProjectsSource, 'browse' | 'startIndex' | 'indexJobs' | 'listProjects' | 'projectHealth'>;
    onClose: () => void;
    onOpenProject: (name: string) => void;
    onActivityChange?: (activity: AddProjectIndexActivity | undefined) => void;
    pollMs?: number;
}
interface TrackedIndex extends AddProjectIndexActivity {
    id: number;
    slot: number;
    path: string;
    phase: 'running' | 'verifying' | 'unavailable' | 'lost' | 'finished' | 'failed';
    message: string;
}

function comparablePath(path: string): string {
    const normal = path.replace(/\\/g, '/').replace(/\/+$/, '') || '/';
    return /^[A-Za-z]:/.test(normal) || normal.startsWith('//') ? normal.toLowerCase() : normal;
}
function samePath(left: string | undefined, right: string): boolean {
    return !!left && comparablePath(left) === comparablePath(right);
}
/** Mirror portable name mapping, using the existing basename suggestion. */
function normalizedName(name: string): string {
    let result = '';
    for (const byte of new TextEncoder().encode(name)) {
        const character = String.fromCharCode(byte);
        result += /[A-Za-z0-9._-]/.test(character) ? character : byte >= 128 ? byte.toString(16).padStart(2, '0') : '-';
    }
    return result.replace(/-+/g, '-').replace(/\.+/g, '.').replace(/^[-.]+/, '').replace(/-+$/, '') || 'root';
}
function validName(name: string): boolean {
    return name.length > 0 && name.length <= 200 && /^[A-Za-z0-9_-][A-Za-z0-9._-]*$/.test(name)
        && !name.includes('..') && normalizedName(name) === name;
}
const nameHint = 'Use up to 200 letters, numbers, underscores, dots or single dashes. No spaces or leading dots.';

export default function AddProjectIndexDialog({ open, source, onClose, onOpenProject, onActivityChange, pollMs = 1500 }: AddProjectIndexDialogProps): JSX.Element | null {
    const [step, setStep] = useState<'browse' | 'confirm' | 'progress'>('browse');
    const [level, setLevel] = useState<BrowseLevel>();
    const [pathInput, setPathInput] = useState('');
    const [browsing, setBrowsing] = useState(false);
    const [browseError, setBrowseError] = useState('');
    const [entries, setEntries] = useState<ProjectEntry[]>([]);
    const [listStatus, setListStatus] = useState<'idle' | 'loading' | 'ready' | 'error'>('idle');
    const [selectedPath, setSelectedPath] = useState('');
    const [name, setName] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [submitError, setSubmitError] = useState('');
    const [job, setJob] = useState<TrackedIndex>();
    const [readingStatus, setReadingStatus] = useState(false);
    const alive = useRef(true);
    const browseTicket = useRef(0);
    const listTicket = useRef(0);
    const submitPending = useRef(false);
    const pollPending = useRef(false);
    const nextJob = useRef(0);
    const jobRef = useRef<TrackedIndex | undefined>(undefined);
    const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
    const pollRef = useRef<() => Promise<void>>(async () => {});
    const activityCallback = useRef(onActivityChange);
    activityCallback.current = onActivityChange;
    const panel = useRef<HTMLElement>(null);
    const pathField = useRef<HTMLInputElement>(null);
    const nameField = useRef<HTMLInputElement>(null);
    const opener = useRef<HTMLElement | null>(null);
    const closeCallback = useRef(onClose);
    closeCallback.current = onClose;

    const updateJob = (next: TrackedIndex | undefined): void => { jobRef.current = next; setJob(next); };
    useEffect(() => {
        alive.current = true;
        return () => { alive.current = false; browseTicket.current += 1; listTicket.current += 1; clearTimeout(timer.current); };
    }, []);
    useEffect(() => {
        activityCallback.current?.(job ? { name: job.name, status: job.phase === 'unavailable' ? 'error' : job.status } : undefined);
    }, [job?.name, job?.status, job?.phase]);

    const browse = async (path: string): Promise<void> => {
        const ticket = ++browseTicket.current;
        setBrowsing(true); setBrowseError(''); setPathInput(path);
        try {
            const next = await source.browse(path);
            if (!alive.current || ticket !== browseTicket.current) return;
            setLevel(next); setPathInput(previous => previous === path ? next.path : previous);
        } catch {
            if (alive.current && ticket === browseTicket.current) setBrowseError('Could not open this folder. Check the path and try again.');
        } finally {
            if (alive.current && ticket === browseTicket.current) setBrowsing(false);
        }
    };
    const loadProjects = async (): Promise<void> => {
        const ticket = ++listTicket.current;
        setListStatus('loading');
        try {
            const projects = await source.listProjects();
            if (alive.current && ticket === listTicket.current) { setEntries(projects); setListStatus('ready'); }
        } catch { if (alive.current && ticket === listTicket.current) setListStatus('error'); }
    };
    useEffect(() => {
        if (!open) return;
        if (!level && !jobRef.current) void browse('');
        if (!jobRef.current) void loadProjects();
        // Reopening retains the folder/form and a job already being tracked.
    }, [open, source]);

    useEffect(() => {
        if (!open) return;
        opener.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        (pathField.current ?? panel.current)?.focus();
        const keys = (event: KeyboardEvent): void => {
            if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); closeCallback.current(); return; }
            if (event.key !== 'Tab') return;
            const controls = [...(panel.current?.querySelectorAll<HTMLElement>('button:not(:disabled), input:not(:disabled), [tabindex="0"]') ?? [])];
            const first = controls[0], last = controls[controls.length - 1];
            if (!first) { event.preventDefault(); panel.current?.focus(); return; }
            if (event.shiftKey && (document.activeElement === first || document.activeElement === panel.current)) { event.preventDefault(); last.focus(); }
            else if (!event.shiftKey && (document.activeElement === last || !panel.current?.contains(document.activeElement))) { event.preventDefault(); first.focus(); }
        };
        window.addEventListener('keydown', keys, true);
        return () => { window.removeEventListener('keydown', keys, true); if (opener.current?.isConnected) opener.current.focus(); };
    }, [open]);
    useEffect(() => {
        if (open) (step === 'confirm' ? nameField.current : step === 'browse' ? pathField.current : panel.current)?.focus();
    }, [open, step]);

    const poll = async (): Promise<void> => {
        const tracked = jobRef.current;
        if (!tracked || !Number.isInteger(tracked.slot) || tracked.slot < 0 || pollPending.current || tracked.status === 'done' || tracked.phase === 'failed') return;
        clearTimeout(timer.current);
        pollPending.current = true; setReadingStatus(true);
        const current = () => alive.current && jobRef.current?.id === tracked.id;
        try {
            const jobs = await source.indexJobs();
            if (!current()) return;
            const found = jobs.find(candidate => candidate.slot === tracked.slot);
            if (!found || !samePath(found.path, tracked.path) || found.status === 'unknown') {
                updateJob({ ...tracked, status: 'error', phase: 'lost', message: 'This job is no longer identifiable in the server status. Completion is unconfirmed.' });
                return;
            }
            if (found.status === 'error') {
                updateJob({ ...tracked, status: 'error', phase: 'failed', message: found.error || 'The server reported an indexing failure.' });
                return;
            }
            if (found.status === 'indexing') {
                updateJob({ ...tracked, status: 'indexing', phase: 'running', message: 'Indexing source and relationships…' });
                return;
            }
            updateJob({ ...tracked, status: 'indexing', phase: 'verifying', message: 'Indexing finished. Checking the project…' });
            const [projects, health] = await Promise.all([source.listProjects(), source.projectHealth(tracked.name)]);
            if (!current()) return;
            const project = projects.find(candidate => candidate.name === tracked.name);
            setEntries(projects); setListStatus('ready');
            if (project && samePath(project.root_path, tracked.path) && health.status === 'healthy') {
                updateJob({ ...tracked, status: 'done', phase: 'finished', message: 'The project index is ready to explore.' });
            } else if ((project?.root_path && !samePath(project.root_path, tracked.path)) || health.status === 'corrupt') {
                updateJob({ ...tracked, status: 'error', phase: 'failed', message: health.status === 'corrupt'
                    ? 'The index could not be verified as healthy. Inspect its errors in System.'
                    : 'The returned project belongs to a different folder. This index was not verified.' });
            } else {
                updateJob({ ...tracked, status: 'indexing', phase: 'unavailable', message: 'The job finished, but its project index could not be verified yet. Check again.' });
            }
        } catch {
            if (current()) updateJob({ ...tracked, phase: 'unavailable', message: 'Status is unavailable. The job may still be running; retry the status check.' });
        } finally {
            pollPending.current = false;
            if (alive.current) setReadingStatus(false);
            const latest = jobRef.current;
            if (alive.current && latest?.id === tracked.id && latest.status === 'indexing' && (latest.phase === 'running' || latest.phase === 'verifying')) {
                timer.current = setTimeout(() => { void pollRef.current(); }, Math.max(1, pollMs));
            }
        }
    };
    pollRef.current = poll;

    const folderDuplicate = entries.find(entry => samePath(entry.root_path, selectedPath));
    const nameDuplicate = entries.find(entry => entry.name.toLowerCase() === name.trim().toLowerCase());
    const duplicate = folderDuplicate ?? nameDuplicate;
    const useFolder = (): void => {
        if (!level || browsing || browseError || !level.path || !samePath(pathInput.trim(), level.path)) return;
        setSelectedPath(level.path);
        setName(normalizedName(projectNameFor(level.path)).slice(0, 200).replace(/-+$/, ''));
        setSubmitError(''); setStep('confirm');
    };
    const start = async (): Promise<void> => {
        const projectName = name.trim();
        if (submitPending.current || !selectedPath || !validName(projectName) || duplicate || listStatus !== 'ready' || jobRef.current?.status === 'indexing') return;
        const path = selectedPath;
        submitPending.current = true; setSubmitting(true); setSubmitError('');
        try {
            const projects = await source.listProjects();
            if (!alive.current) return;
            setEntries(projects); setListStatus('ready');
            if (projects.some(entry => samePath(entry.root_path, path) || entry.name.toLowerCase() === projectName.toLowerCase())) return;
            const started = await source.startIndex(path, projectName);
            if (!alive.current) return;
            const valid = Number.isInteger(started.slot) && started.slot >= 0 && samePath(started.path, path);
            const tracked: TrackedIndex = { id: ++nextJob.current, name: projectName, path, slot: started.slot,
                status: valid ? 'indexing' : 'error', phase: valid ? 'running' : 'lost', message: valid
                    ? 'Indexing source and relationships…' : 'The server accepted the request without a matching job identifier. Completion is unconfirmed.' };
            updateJob(tracked); setStep('progress');
            if (valid) void pollRef.current();
        } catch {
            if (alive.current) setSubmitError('The index request was not confirmed. Check the server in System before trying again.');
        } finally { submitPending.current = false; if (alive.current) setSubmitting(false); }
    };
    const another = (): void => {
        if (jobRef.current?.status === 'indexing' || pollPending.current) return;
        clearTimeout(timer.current); updateJob(undefined); setStep('browse'); setSubmitError('');
        void loadProjects(); if (!level) void browse('');
    };
    const openProject = (projectName: string): void => { onClose(); onOpenProject(projectName); };
    if (!open) return null;

    return <div className="add-index-backdrop" onPointerDown={event => { if (event.target === event.currentTarget) onClose(); }}>
        <section ref={panel} className="add-index-dialog" role="dialog" aria-modal="true" aria-labelledby="add-index-title" tabIndex={-1}>
            <header><div><p>Local repository</p><h2 id="add-index-title">Add project index</h2></div><button type="button" className="add-index-close" aria-label="Close add project index" onClick={onClose}>×</button></header>
            {step === 'browse' && <>
                <form className="add-index-path" onSubmit={event => { event.preventDefault(); void browse(pathInput.trim()); }}>
                    <label htmlFor="add-index-folder">Folder</label><div><input ref={pathField} id="add-index-folder" value={pathInput} spellCheck={false}
                        placeholder="Enter a local folder path" onChange={event => setPathInput(event.target.value)} /><button type="submit">Go</button></div>
                </form>
                <div className="add-index-folder-tools">{level && <button type="button" disabled={browsing || !level.parent || samePath(level.parent, level.path)} onClick={() => { void browse(level.parent); }}>↑ Up</button>}
                    {level?.roots.map(root => <button type="button" key={root} onClick={() => { void browse(root); }}>{root}</button>)}</div>
                <div className="add-index-folders" aria-label="Folders" aria-busy={browsing}>
                    {browsing && <p role="status">Opening folder…</p>}
                    {browseError && <p role="alert">{browseError}</p>}
                    {!browsing && !browseError && level?.dirs.length === 0 && <p>No visible subfolders.</p>}
                    {!browseError && <ul>{level?.dirs.slice().sort((left, right) => left.localeCompare(right)).map(dir => <li key={dir}><button type="button" aria-label={`Open folder ${dir}`} disabled={browsing} onClick={() => { void browse(childPath(level, dir)); }}><span aria-hidden="true">▱</span>{dir}<span aria-hidden="true">›</span></button></li>)}</ul>}
                </div>
                <footer><span>Choose a folder to index. Nothing starts while browsing.</span><button type="button" className="add-index-primary" disabled={!level?.path || browsing || !!browseError || !samePath(pathInput.trim(), level.path)} onClick={useFolder}>Use this folder</button></footer>
            </>}
            {step === 'confirm' && <form className="add-index-confirm" onSubmit={event => { event.preventDefault(); void start(); }}>
                <p className="add-index-chosen-path">{selectedPath}</p>
                <label htmlFor="add-index-name">Project name</label><input ref={nameField} id="add-index-name" value={name} spellCheck={false} disabled={submitting}
                    onChange={event => { setName(event.target.value); setSubmitError(''); }} aria-describedby="add-index-name-hint" />
                <p id="add-index-name-hint" className="add-index-hint">{nameHint}</p>
                {!validName(name.trim()) && <p role="status">Choose a valid project name before indexing.</p>}
                {listStatus === 'loading' && <p role="status">Checking existing projects…</p>}
                {listStatus === 'error' && <div className="add-index-warning" role="alert"><p>Could not check existing projects.</p><button type="button" onClick={() => { void loadProjects(); }}>Retry project check</button></div>}
                {duplicate && <div className="add-index-warning" role="status"><p>{folderDuplicate ? `This folder is already indexed as ${duplicate.name}.` : `The name ${duplicate.name} is already in use. Choose another name.`}</p>
                    {folderDuplicate && <button type="button" onClick={() => openProject(duplicate.name)}>Open project</button>}</div>}
                {submitError && <p className="add-index-warning" role="alert">{submitError}</p>}
                <footer><button type="button" disabled={submitting} onClick={() => setStep('browse')}>Choose another folder</button>
                    <button type="submit" className="add-index-primary" disabled={submitting || !validName(name.trim()) || !!duplicate || listStatus !== 'ready'}>{submitting ? 'Starting…' : 'Start indexing'}</button></footer>
            </form>}
            {step === 'progress' && job && <div className="add-index-progress" data-status={job.status}>
                <span className="add-index-progress-icon" aria-hidden="true">{job.status === 'done' ? '✓' : job.status === 'error' || job.phase === 'unavailable' ? '!' : '◎'}</span>
                <h3>{job.status === 'done' ? 'Ready to explore' : job.phase === 'failed' ? 'Indexing failed' : job.phase === 'lost' ? 'Tracking interrupted' : job.phase === 'unavailable' ? 'Status unavailable' : job.phase === 'verifying' ? 'Checking the index' : 'Indexing repository'}</h3>
                <strong>{job.name}</strong><p className="add-index-chosen-path">{job.path}</p>
                {(job.phase === 'running' || job.phase === 'verifying') && <progress aria-label="Indexing progress" />}
                <p role={job.status === 'error' || job.phase === 'unavailable' ? 'alert' : 'status'}>{job.message}</p>
                {job.status === 'indexing' && <p className="add-index-hint">You can close this window. Indexing continues in the daemon.</p>}
                <div className="add-index-progress-actions">
                    {(job.phase === 'unavailable' || job.phase === 'lost') && Number.isInteger(job.slot) && job.slot >= 0 && <button type="button" disabled={readingStatus} onClick={() => { void pollRef.current(); }}>{readingStatus ? 'Checking…' : 'Retry status'}</button>}
                    {job.status === 'done' && <button type="button" className="add-index-primary" onClick={() => openProject(job.name)}>Open project</button>}
                    {job.status !== 'indexing' && <button type="button" disabled={readingStatus} onClick={another}>Add another project</button>}
                    {job.status === 'indexing' && <button type="button" onClick={onClose}>Close for now</button>}
                </div>
            </div>}
        </section>
    </div>;
}
