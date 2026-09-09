import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import type { JSX } from 'react';
import type { ProjectEntry } from '../provider/rpc-schemas';
import { projectSwitcherStrings as text } from './project-switcher-strings';
import { projectPickerPlacement } from './project-picker-placement';
import './project-switcher.css';

interface ProjectSwitcherProps {
    currentProject: string;
    listProjects: () => Promise<readonly ProjectEntry[]>;
    onSelectProject: (name: string) => void;
    onManageProjects: () => void;
}

export default function ProjectSwitcher(props: ProjectSwitcherProps): JSX.Element {
    const [open, setOpen] = useState(false);
    const [query, setQuery] = useState('');
    const [entries, setEntries] = useState<readonly ProjectEntry[]>([]);
    const [status, setStatus] = useState<'loading' | 'ready' | 'error'>('loading');
    const [revision, setRevision] = useState(0);
    const [placement, setPlacement] = useState({ top: 64, left: 12, width: 370, maxHeight: 520 });
    const disclosure = useRef<HTMLDetailsElement>(null);
    const trigger = useRef<HTMLElement>(null);
    const search = useRef<HTMLInputElement>(null);
    const close = (restoreFocus = false): void => {
        setOpen(false);
        if (restoreFocus) trigger.current?.focus();
    };

    useLayoutEffect(() => {
        if (!open) return;
        const reposition = (): void => {
            const bounds = trigger.current?.getBoundingClientRect();
            if (bounds) setPlacement(projectPickerPlacement(bounds, { width: window.innerWidth, height: window.innerHeight }));
        };
        reposition();
        const observer = typeof ResizeObserver === 'undefined' ? undefined : new ResizeObserver(reposition);
        const header = disclosure.current?.closest('.atlas-header') ?? trigger.current;
        if (header) observer?.observe(header);
        window.addEventListener('resize', reposition);
        window.addEventListener('scroll', reposition, true);
        return () => {
            observer?.disconnect();
            window.removeEventListener('resize', reposition);
            window.removeEventListener('scroll', reposition, true);
        };
    }, [open]);

    useEffect(() => {
        if (!open) return;
        let current = true;
        setStatus('loading');
        void Promise.resolve().then(props.listProjects).then(
            (projects) => {
                if (!current) return;
                setEntries(projects);
                setStatus('ready');
            },
            () => { if (current) setStatus('error'); },
        );
        return () => { current = false; };
    }, [open, props.listProjects, revision]);

    useEffect(() => {
        if (!open) return;
        search.current?.focus();
        const outside = (event: PointerEvent): void => {
            if (event.target instanceof Node && !disclosure.current?.contains(event.target)) setOpen(false);
        };
        document.addEventListener('pointerdown', outside);
        return () => document.removeEventListener('pointerdown', outside);
    }, [open]);

    const needle = query.trim().toLocaleLowerCase();
    const projects = Array.from(new Map(entries.map(entry => [entry.name, entry])).values())
        .filter(entry => entry.name.length > 0 &&
            `${entry.name}\n${entry.root_path ?? ''}`.toLocaleLowerCase().includes(needle))
        .sort((left, right) => Number(right.name === props.currentProject) - Number(left.name === props.currentProject)
            || left.name.localeCompare(right.name));

    return <details ref={disclosure} open={open} className="atlas-project-switcher"
        onKeyDown={(event) => {
            if (event.key !== 'Escape' || !open) return;
            event.preventDefault();
            event.stopPropagation();
            close(true);
        }}
        onBlur={(event) => {
            if (event.relatedTarget instanceof Node && !event.currentTarget.contains(event.relatedTarget)) close();
        }}>
        <summary ref={trigger} aria-label={text.switchProject(props.currentProject || text.choose)}
            title={props.currentProject || text.choose}
            onClick={(event) => {
                event.preventDefault();
                if (!open) { setQuery(''); setStatus('loading'); }
                setOpen(value => !value);
            }}>
            <span className="atlas-project-switcher-label">{text.project}</span>
            <span className="atlas-project-switcher-name">{props.currentProject || text.choose}</span>
            <span aria-hidden="true">⌄</span>
        </summary>
        {open && <div className="atlas-project-picker" style={placement}>
            <input ref={search} type="search" aria-label={text.search} placeholder={text.searchPlaceholder}
                value={query} onChange={event => setQuery(event.target.value)} />
            <div className="atlas-project-results" aria-busy={status === 'loading'}>
                {status === 'loading' && <p role="status">{text.loading}</p>}
                {status === 'error' && <div role="status"><p>{text.failed}</p>
                    <button type="button" onClick={() => setRevision(value => value + 1)}>{text.retry}</button>
                </div>}
                {status === 'ready' && projects.length === 0 && <p role="status">{entries.length === 0 ? text.empty : text.noMatches}</p>}
                {status === 'ready' && <ul>{projects.map(entry => <li key={entry.name}>
                    <button type="button" aria-current={entry.name === props.currentProject ? 'true' : undefined}
                        onClick={() => {
                            close(true);
                            if (entry.name !== props.currentProject) props.onSelectProject(entry.name);
                        }}>
                        <span className="atlas-project-result-name">{entry.name}
                            {entry.name === props.currentProject && <small>{text.current}</small>}</span>
                        {entry.root_path && <span className="atlas-project-result-path">{entry.root_path}</span>}
                    </button>
                </li>)}</ul>}
            </div>
            <footer>
                <button type="button" onClick={() => { close(); props.onManageProjects(); }}>{text.manage}</button>
                <button type="button" disabled={status === 'loading'} onClick={() => setRevision(value => value + 1)}>{text.refresh}</button>
            </footer>
        </div>}
    </details>;
}
