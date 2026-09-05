/**
 * The projects panel: what this server has indexed, how to give it more, and
 * what the server is doing.
 *
 * It is the one surface of this frontend that asks the server to write, and
 * it says so in every place where it does: index a repository (POST
 * /api/index), remove an index (DELETE /api/project), store a decision record
 * (POST /api/adr). The server does the work in its own process; this window
 * only asks, then reads the answer back through the same routes it would read
 * anyway.
 *
 * Three rules shape it, and they are the rules of the rest of this surface:
 *
 *  1. **Every block names its source.** The list says it comes from
 *     list_projects, the jobs say they come from /api/index-status, and a
 *     number the server did not send is shown as not sent, never as zero.
 *  2. **A control that cannot act is not on the screen.** The index button
 *     appears once a path and a name are there; a project whose root the
 *     server did not report has no reindex button and a sentence instead.
 *  3. **Deleting takes two steps, in place.** No browser dialog: the row turns
 *     into the question, with the answer and the way back next to it.
 *
 * Why this panel exists at all, given that the help page describes a
 * read-only product: the previous frontend of this repository let a reader
 * index a project from the browser, and the first thing a new install shows is
 * a server with nothing indexed. Sending that reader to the command line would
 * be a capability the replacement took away. The panel keeps it, and keeps the
 * writing confined to these three requests.
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import type { JSX, ReactNode } from 'react';

import { AtlasApiError } from '../app/atlas-api';
import { messages } from '../i18n/messages';
import type { ProjectEntry } from '../provider/rpc-schemas';
import { anyIndexing, childPath, finishedSince, megabytes, projectNameFor } from './projects-model';
import type {
    AdrRecord,
    BrowseLevel,
    IndexJob,
    IndexStarted,
    LogTail,
    ProcessReport,
    ProjectHealth,
} from './projects-model';

const text = messages.projects;

/** How often the job table is read while a job runs. */
export const POLL_MS = 1500;

/** How much of the log the panel asks for. */
export const LOG_LINES = 200;

/**
 * Everything the panel asks the server, as functions and not as a client, so
 * a test can hand in answers without a network and App can bind the list to
 * /rpc and the rest to /api.
 */
export interface ProjectsSource {
    listProjects(): Promise<ProjectEntry[]>;
    projectHealth(name: string): Promise<ProjectHealth>;
    deleteProject(name: string): Promise<void>;
    browse(path: string): Promise<BrowseLevel>;
    startIndex(rootPath: string, projectName: string): Promise<IndexStarted>;
    indexJobs(): Promise<IndexJob[]>;
    adr(project: string): Promise<AdrRecord>;
    saveAdr(project: string, content: string): Promise<void>;
    logs(lines: number): Promise<LogTail>;
    processes(): Promise<ProcessReport>;
}

export interface ProjectsPanelProps {
    /** The project this window has open; empty when none is. */
    project: string;
    source: ProjectsSource;
    /** Called with a project name when the reader wants to open it here. */
    onOpenProject: (name: string) => void;
    onClose: () => void;
    /** The polling interval, so a test does not have to wait a second and a half. */
    pollMs?: number;
}

function describeError(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
}

function Section(props: { name: string; title: string; source?: string; children: ReactNode }): JSX.Element {
    return (
        <section
            className="atlas-projects-section"
            data-testid="atlas-projects-section"
            data-section={props.name}
        >
            <h3 className="atlas-projects-section-title">{props.title}</h3>
            {props.source !== undefined && (
                <p className="atlas-projects-source" data-testid="atlas-projects-source">{props.source}</p>
            )}
            {props.children}
        </section>
    );
}

type HealthState = ProjectHealth | 'asking';

function healthSentence(health: ProjectHealth): string {
    switch (health.status) {
        case 'healthy':
            return text.healthHealthy(
                health.nodes ?? 0,
                health.edges ?? 0,
                megabytes(health.sizeBytes ?? 0),
            );
        case 'missing':
            return text.healthMissing;
        case 'corrupt':
            return text.healthCorrupt(health.reason);
        default:
            return text.healthUnknown;
    }
}

function jobSentence(job: IndexJob): string {
    switch (job.status) {
        case 'indexing':
            return text.jobIndexing;
        case 'done':
            return text.jobDone;
        case 'error':
            return text.jobError(job.error);
        default:
            return text.jobUnknown;
    }
}

export default function ProjectsPanel(props: ProjectsPanelProps): JSX.Element {
    const { source, project } = props;
    const pollMs = props.pollMs ?? POLL_MS;
    const root = useRef<HTMLDivElement | null>(null);
    const mounted = useRef(true);

    // ------------------------------------------------------------ the list --
    const [projects, setProjects] = useState<ProjectEntry[] | undefined>(undefined);
    const [listError, setListError] = useState('');
    const [listNotice, setListNotice] = useState('');
    const [health, setHealth] = useState<Record<string, HealthState>>({});
    const [armed, setArmed] = useState('');

    // ------------------------------------------------------------ the form --
    const [path, setPath] = useState('');
    const [name, setName] = useState('');
    const [nameTouched, setNameTouched] = useState(false);
    const [browseLevel, setBrowseLevel] = useState<BrowseLevel | undefined>(undefined);
    const [browseError, setBrowseError] = useState('');
    const [startNotice, setStartNotice] = useState('');
    const [jobs, setJobs] = useState<IndexJob[]>([]);
    const jobsRef = useRef<IndexJob[]>([]);

    // ------------------------------------------------------------- the adr --
    const [adr, setAdr] = useState<{ state: 'idle' | 'loading' | 'ready' | 'error'; record?: AdrRecord; detail: string }>({
        state: 'idle',
        detail: '',
    });
    const [adrDraft, setAdrDraft] = useState('');
    const [adrNotice, setAdrNotice] = useState('');

    // ---------------------------------------------------------- the server --
    const [processes, setProcesses] = useState<ProcessReport | undefined>(undefined);
    const [processesError, setProcessesError] = useState('');
    const [logs, setLogs] = useState<LogTail | undefined>(undefined);
    const [logsError, setLogsError] = useState('');

    useEffect(() => {
        mounted.current = true;
        root.current?.focus();
        return () => {
            mounted.current = false;
        };
    }, []);

    const loadProjects = useCallback(async () => {
        try {
            const list = await source.listProjects();
            if (!mounted.current) {
                return;
            }
            setProjects(list);
            setListError('');
        } catch (error) {
            if (mounted.current) {
                setListError(text.listError(describeError(error)));
            }
        }
    }, [source]);

    const loadJobs = useCallback(async () => {
        try {
            const next = await source.indexJobs();
            if (!mounted.current) {
                return;
            }
            const finished = finishedSince(jobsRef.current, next);
            jobsRef.current = next;
            setJobs(next);
            if (finished.length > 0) {
                void loadProjects();
            }
        } catch {
            /* The job table is a courtesy while something runs; its absence is
             * visible as an unchanged list, and the next poll asks again. */
        }
    }, [source, loadProjects]);

    const loadAdr = useCallback(async () => {
        if (project.length === 0) {
            setAdr({ state: 'idle', detail: '' });
            setAdrDraft('');
            return;
        }
        setAdr({ state: 'loading', detail: '' });
        try {
            const record = await source.adr(project);
            if (!mounted.current) {
                return;
            }
            setAdr({ state: 'ready', record, detail: '' });
            setAdrDraft(record.content);
        } catch (error) {
            if (mounted.current) {
                setAdr({ state: 'error', detail: text.adrReadError(describeError(error)) });
            }
        }
    }, [source, project]);

    const loadServer = useCallback(async () => {
        try {
            const report = await source.processes();
            if (mounted.current) {
                setProcesses(report);
                setProcessesError('');
            }
        } catch (error) {
            if (mounted.current) {
                setProcessesError(text.processesError(describeError(error)));
            }
        }
        try {
            const tail = await source.logs(LOG_LINES);
            if (mounted.current) {
                setLogs(tail);
                setLogsError('');
            }
        } catch (error) {
            if (mounted.current) {
                setLogsError(text.logsError(describeError(error)));
            }
        }
    }, [source]);

    useEffect(() => {
        void loadProjects();
        void loadJobs();
        void loadServer();
    }, [loadProjects, loadJobs, loadServer]);

    useEffect(() => {
        void loadAdr();
    }, [loadAdr]);

    // The poll runs only while a job runs: an idle panel sends nothing.
    useEffect(() => {
        if (!anyIndexing(jobs)) {
            return undefined;
        }
        const timer = window.setTimeout(() => {
            void loadJobs();
        }, pollMs);
        return () => window.clearTimeout(timer);
    }, [jobs, pollMs, loadJobs]);

    // ------------------------------------------------------------ actions --

    const check = async (entry: ProjectEntry): Promise<void> => {
        setHealth((current) => ({ ...current, [entry.name]: 'asking' }));
        try {
            const verdict = await source.projectHealth(entry.name);
            if (mounted.current) {
                setHealth((current) => ({ ...current, [entry.name]: verdict }));
            }
        } catch (error) {
            if (mounted.current) {
                setHealth((current) => ({
                    ...current,
                    [entry.name]: { status: 'unknown', reason: describeError(error) },
                }));
            }
        }
    };

    const remove = async (entry: ProjectEntry): Promise<void> => {
        setArmed('');
        try {
            await source.deleteProject(entry.name);
            if (!mounted.current) {
                return;
            }
            setListNotice(text.removed(entry.name));
            setHealth((current) => {
                const next = { ...current };
                delete next[entry.name];
                return next;
            });
            void loadProjects();
        } catch (error) {
            if (mounted.current) {
                setListNotice(text.removeError(describeError(error)));
            }
        }
    };

    const start = async (rootPath: string, projectName: string): Promise<void> => {
        try {
            const started = await source.startIndex(rootPath, projectName);
            if (!mounted.current) {
                return;
            }
            setStartNotice(text.started(started.path.length > 0 ? started.path : rootPath));
            void loadJobs();
        } catch (error) {
            if (mounted.current) {
                setStartNotice(text.startError(describeError(error)));
            }
        }
    };

    const browse = async (at: string): Promise<void> => {
        try {
            const level = await source.browse(at);
            if (mounted.current) {
                setBrowseLevel(level);
                setBrowseError('');
            }
        } catch (error) {
            if (mounted.current) {
                setBrowseError(text.browseError(describeError(error)));
            }
        }
    };

    const changePath = (value: string): void => {
        setPath(value);
        if (!nameTouched) {
            setName(projectNameFor(value));
        }
    };

    const useFolder = (folder: string): void => {
        changePath(folder);
        setBrowseLevel(undefined);
    };

    const saveAdr = async (): Promise<void> => {
        try {
            await source.saveAdr(project, adrDraft);
            if (!mounted.current) {
                return;
            }
            setAdrNotice(text.adrSaved);
            void loadAdr();
        } catch (error) {
            if (!mounted.current) {
                return;
            }
            if (error instanceof AtlasApiError && error.status === 423) {
                setAdrNotice(text.adrBusy);
            } else {
                setAdrNotice(text.adrError(describeError(error)));
            }
        }
    };

    const canStart = path.trim().length > 0 && name.trim().length > 0;
    const others = processes?.processes.filter((entry) => !entry.isSelf) ?? [];

    return (
        <div
            className="atlas-projects"
            data-testid="atlas-projects"
            role="dialog"
            aria-label={text.panelLabel}
            aria-modal="false"
            tabIndex={-1}
            ref={root}
            onKeyDown={(event) => {
                if (event.key === 'Escape') {
                    event.preventDefault();
                    props.onClose();
                }
            }}
        >
            <header className="atlas-projects-head">
                <span className="atlas-projects-title" data-testid="atlas-projects-title">
                    {text.title}
                </span>
                <span className="atlas-projects-subtitle">{text.subtitle}</span>
                <button
                    type="button"
                    className="atlas-projects-close"
                    data-testid="atlas-projects-close"
                    aria-label={text.closeLabel}
                    onClick={props.onClose}
                >
                    {text.close}
                </button>
            </header>

            {/* ------------------------------------------------- the list */}
            <Section name="list" title={text.listTitle} source={text.listSource}>
                {listError.length > 0 && (
                    <p className="atlas-projects-notice" data-testid="atlas-projects-list-error">{listError}</p>
                )}
                {projects === undefined && listError.length === 0 && (
                    <p className="atlas-projects-text">{text.listLoading}</p>
                )}
                {projects !== undefined && projects.length === 0 && (
                    <p className="atlas-projects-text" data-testid="atlas-projects-empty">{text.listEmpty}</p>
                )}
                {projects !== undefined && projects.length > 0 && (
                    <ul className="atlas-projects-list" data-testid="atlas-projects-list">
                        {projects.map((entry) => {
                            const verdict = health[entry.name];
                            const isOpen = entry.name === project;
                            const isArmed = armed === entry.name;
                            return (
                                <li
                                    key={entry.name}
                                    className="atlas-projects-row"
                                    data-testid="atlas-projects-row"
                                    data-project={entry.name}
                                    data-open={isOpen}
                                >
                                    <div className="atlas-projects-row-line">
                                        <span className="atlas-projects-row-name">{entry.name}</span>
                                        <span className="atlas-projects-row-meta">
                                            {entry.nodes !== undefined && entry.edges !== undefined
                                                ? text.counts(entry.nodes, entry.edges)
                                                : text.countsUnknown}
                                        </span>
                                        {isOpen && <span className="atlas-projects-row-open">{text.openNow}</span>}
                                    </div>
                                    {entry.root_path !== undefined && entry.root_path.length > 0 && (
                                        <div className="atlas-projects-row-path">{entry.root_path}</div>
                                    )}
                                    {!isArmed && (
                                        <div className="atlas-projects-row-actions">
                                            {!isOpen && (
                                                <button
                                                    type="button"
                                                    className="atlas-projects-action"
                                                    data-testid="atlas-projects-open"
                                                    title={text.openTitle(entry.name)}
                                                    onClick={() => props.onOpenProject(entry.name)}
                                                >
                                                    {text.open}
                                                </button>
                                            )}
                                            <button
                                                type="button"
                                                className="atlas-projects-action"
                                                data-testid="atlas-projects-check"
                                                title={text.checkTitle}
                                                onClick={() => {
                                                    void check(entry);
                                                }}
                                            >
                                                {text.check}
                                            </button>
                                            {entry.root_path !== undefined && entry.root_path.length > 0 ? (
                                                <button
                                                    type="button"
                                                    className="atlas-projects-action"
                                                    data-testid="atlas-projects-reindex"
                                                    title={text.reindexTitle(entry.root_path)}
                                                    onClick={() => {
                                                        void start(entry.root_path ?? '', entry.name);
                                                    }}
                                                >
                                                    {text.reindex}
                                                </button>
                                            ) : (
                                                <span className="atlas-projects-row-note">{text.noRoot}</span>
                                            )}
                                            <button
                                                type="button"
                                                className="atlas-projects-action"
                                                data-danger="true"
                                                data-testid="atlas-projects-remove"
                                                title={text.removeTitle}
                                                onClick={() => setArmed(entry.name)}
                                            >
                                                {text.remove}
                                            </button>
                                        </div>
                                    )}
                                    {isArmed && (
                                        <div className="atlas-projects-armed" data-testid="atlas-projects-armed">
                                            <span>{text.removeArmed(entry.name)}</span>
                                            <button
                                                type="button"
                                                className="atlas-projects-action"
                                                data-danger="true"
                                                data-testid="atlas-projects-remove-confirm"
                                                onClick={() => {
                                                    void remove(entry);
                                                }}
                                            >
                                                {text.removeConfirm}
                                            </button>
                                            <button
                                                type="button"
                                                className="atlas-projects-action"
                                                data-testid="atlas-projects-remove-cancel"
                                                onClick={() => setArmed('')}
                                            >
                                                {text.cancel}
                                            </button>
                                        </div>
                                    )}
                                    {verdict !== undefined && (
                                        <div className="atlas-projects-row-health" data-testid="atlas-projects-health">
                                            {verdict === 'asking' ? text.listLoading : healthSentence(verdict)}
                                        </div>
                                    )}
                                </li>
                            );
                        })}
                    </ul>
                )}
                {listNotice.length > 0 && (
                    <p className="atlas-projects-notice" data-testid="atlas-projects-list-notice">{listNotice}</p>
                )}
                <div className="atlas-projects-row-actions">
                    <button
                        type="button"
                        className="atlas-projects-action"
                        data-testid="atlas-projects-refresh"
                        onClick={() => {
                            void loadProjects();
                        }}
                    >
                        {text.refresh}
                    </button>
                </div>
            </Section>

            {/* ------------------------------------------------- the form */}
            <Section name="index" title={text.indexTitle}>
                <p className="atlas-projects-text">{text.indexIntro}</p>
                <div className="atlas-projects-form">
                    <label className="atlas-projects-field">
                        <span className="atlas-projects-label">{text.pathLabel}</span>
                        <input
                            className="atlas-projects-input"
                            data-testid="atlas-projects-path"
                            value={path}
                            placeholder={text.pathPlaceholder}
                            spellCheck={false}
                            onChange={(event) => changePath(event.target.value)}
                        />
                    </label>
                    <label className="atlas-projects-field">
                        <span className="atlas-projects-label">{text.nameLabel}</span>
                        <input
                            className="atlas-projects-input"
                            data-testid="atlas-projects-name"
                            value={name}
                            spellCheck={false}
                            onChange={(event) => {
                                setName(event.target.value);
                                setNameTouched(true);
                            }}
                        />
                        <span className="atlas-projects-note">{text.nameHint}</span>
                    </label>
                    <div className="atlas-projects-row-actions">
                        <button
                            type="button"
                            className="atlas-projects-action"
                            data-testid="atlas-projects-browse"
                            title={text.browseTitle}
                            onClick={() => {
                                void browse(path.trim());
                            }}
                        >
                            {text.browse}
                        </button>
                        {canStart && (
                            <button
                                type="button"
                                className="atlas-projects-action"
                                data-primary="true"
                                data-testid="atlas-projects-start"
                                title={text.startTitle}
                                onClick={() => {
                                    void start(path.trim(), name.trim());
                                }}
                            >
                                {text.start}
                            </button>
                        )}
                    </div>
                    {startNotice.length > 0 && (
                        <p className="atlas-projects-notice" data-testid="atlas-projects-start-notice">{startNotice}</p>
                    )}
                    {browseError.length > 0 && (
                        <p className="atlas-projects-notice" data-testid="atlas-projects-browse-error">{browseError}</p>
                    )}
                    {browseLevel !== undefined && (
                        <div className="atlas-projects-browse" data-testid="atlas-projects-browse-level">
                            <div className="atlas-projects-browse-head">
                                <span>{text.browseAt(browseLevel.path)}</span>
                                <button
                                    type="button"
                                    className="atlas-projects-action"
                                    data-testid="atlas-projects-browse-up"
                                    onClick={() => {
                                        void browse(browseLevel.parent);
                                    }}
                                >
                                    {text.browseUp}
                                </button>
                                <button
                                    type="button"
                                    className="atlas-projects-action"
                                    data-testid="atlas-projects-browse-use"
                                    onClick={() => useFolder(browseLevel.path)}
                                >
                                    {text.browseUse}
                                </button>
                                <button
                                    type="button"
                                    className="atlas-projects-action"
                                    data-testid="atlas-projects-browse-close"
                                    onClick={() => setBrowseLevel(undefined)}
                                >
                                    {text.browseClose}
                                </button>
                            </div>
                            {browseLevel.roots.length > 0 && (
                                <ul className="atlas-projects-browse-list">
                                    {browseLevel.roots.map((entry) => (
                                        <li key={entry}>
                                            <button
                                                type="button"
                                                className="atlas-projects-browse-dir"
                                                onClick={() => {
                                                    void browse(entry);
                                                }}
                                            >
                                                {entry}
                                            </button>
                                        </li>
                                    ))}
                                </ul>
                            )}
                            {browseLevel.dirs.length === 0 ? (
                                <p className="atlas-projects-note">{text.browseEmpty}</p>
                            ) : (
                                <ul className="atlas-projects-browse-list" data-testid="atlas-projects-browse-dirs">
                                    {browseLevel.dirs.map((entry) => (
                                        <li key={entry}>
                                            <button
                                                type="button"
                                                className="atlas-projects-browse-dir"
                                                data-testid="atlas-projects-browse-dir"
                                                onClick={() => {
                                                    void browse(childPath(browseLevel, entry));
                                                }}
                                            >
                                                {entry}
                                            </button>
                                        </li>
                                    ))}
                                </ul>
                            )}
                        </div>
                    )}
                </div>
                <h4 className="atlas-projects-subtitle-inline">{text.jobsTitle}</h4>
                <p className="atlas-projects-source">{text.jobsSource((pollMs / 1000).toFixed(1))}</p>
                {jobs.length === 0 ? (
                    <p className="atlas-projects-note" data-testid="atlas-projects-jobs-none">{text.jobsNone}</p>
                ) : (
                    <ul className="atlas-projects-jobs" data-testid="atlas-projects-jobs">
                        {jobs.map((job) => (
                            <li
                                key={job.slot}
                                className="atlas-projects-job"
                                data-testid="atlas-projects-job"
                                data-status={job.status}
                            >
                                <span className="atlas-projects-job-path">{job.path}</span>
                                <span className="atlas-projects-job-state">{jobSentence(job)}</span>
                            </li>
                        ))}
                    </ul>
                )}
            </Section>

            {/* -------------------------------------------------- the adr */}
            <Section name="adr" title={text.adrTitle}>
                {project.length === 0 && (
                    <p className="atlas-projects-text" data-testid="atlas-projects-adr-none">{text.adrNoProject}</p>
                )}
                {project.length > 0 && (
                    <>
                        <p className="atlas-projects-text">{text.adrIntro(project)}</p>
                        {adr.state === 'loading' && <p className="atlas-projects-note">{text.adrLoading}</p>}
                        {adr.state === 'error' && (
                            <p className="atlas-projects-notice" data-testid="atlas-projects-adr-error">{adr.detail}</p>
                        )}
                        {adr.state === 'ready' && (
                            <>
                                <p className="atlas-projects-note" data-testid="atlas-projects-adr-state">
                                    {adr.record?.hasAdr === true
                                        ? text.adrUpdated(adr.record.updatedAt)
                                        : text.adrNone}
                                </p>
                                <textarea
                                    className="atlas-projects-adr-text"
                                    data-testid="atlas-projects-adr-text"
                                    aria-label={text.adrLabel}
                                    value={adrDraft}
                                    spellCheck={false}
                                    onChange={(event) => {
                                        setAdrDraft(event.target.value);
                                        setAdrNotice('');
                                    }}
                                />
                                <div className="atlas-projects-row-actions">
                                    <button
                                        type="button"
                                        className="atlas-projects-action"
                                        data-primary="true"
                                        data-testid="atlas-projects-adr-save"
                                        title={text.adrSaveTitle}
                                        onClick={() => {
                                            void saveAdr();
                                        }}
                                    >
                                        {text.adrSave}
                                    </button>
                                    {adrNotice.length > 0 && (
                                        <span className="atlas-projects-note" data-testid="atlas-projects-adr-notice">
                                            {adrNotice}
                                        </span>
                                    )}
                                </div>
                            </>
                        )}
                    </>
                )}
            </Section>

            {/* ------------------------------------------------ the server */}
            <Section name="server" title={text.serverTitle} source={text.processesSource}>
                {processesError.length > 0 && <p className="atlas-projects-notice">{processesError}</p>}
                {processes !== undefined && (
                    <div className="atlas-projects-procs" data-testid="atlas-projects-processes">
                        <p className="atlas-projects-text">
                            {text.processesSelf(processes.selfPid, processes.selfRssMb.toFixed(1))}
                        </p>
                        <p className="atlas-projects-note">
                            {others.length === 0 ? text.processesNone : text.processesOthers(others.length)}
                        </p>
                        {others.length > 0 && (
                            <ul className="atlas-projects-proc-list">
                                {others.map((entry) => (
                                    <li key={entry.pid} data-testid="atlas-projects-process">
                                        {text.processRow(entry.pid, entry.cpu.toFixed(1), entry.rssMb.toFixed(1), entry.elapsed)}
                                    </li>
                                ))}
                            </ul>
                        )}
                    </div>
                )}
                <h4 className="atlas-projects-subtitle-inline">{text.logsTitle}</h4>
                {logsError.length > 0 && <p className="atlas-projects-notice">{logsError}</p>}
                {logs !== undefined && (
                    <>
                        <p className="atlas-projects-source">{text.logsSource(logs.lines.length, logs.total)}</p>
                        {logs.lines.length === 0 ? (
                            <p className="atlas-projects-note">{text.logsEmpty}</p>
                        ) : (
                            <pre className="atlas-projects-logs" data-testid="atlas-projects-logs">
                                {logs.lines.join('\n')}
                            </pre>
                        )}
                    </>
                )}
                <div className="atlas-projects-row-actions">
                    <button
                        type="button"
                        className="atlas-projects-action"
                        data-testid="atlas-projects-server-reload"
                        onClick={() => {
                            void loadServer();
                        }}
                    >
                        {text.reload}
                    </button>
                </div>
            </Section>
        </div>
    );
}
