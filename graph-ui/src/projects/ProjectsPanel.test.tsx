// @vitest-environment jsdom
/*
 * The projects panel in jsdom: what it shows from the server's answers, what
 * it asks the server when a control is used, and what it withholds.
 *
 * The three cases where the panel leaves something OUT are the ones a
 * screenshot cannot show:
 *
 *  1. The index button is absent until a path and a name are there.
 *  2. A project whose root the server did not send has no reindex button.
 *  3. Deleting asks in place first; the request goes out only on the answer.
 *
 * The server is a set of recorded functions. Nothing here touches a network,
 * and the poll interval is one millisecond so a finished job is seen without
 * waiting for the product's second and a half.
 */

import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import ProjectsPanel from './ProjectsPanel';
import type { ProjectsPanelProps, ProjectsSource } from './ProjectsPanel';
import type { IndexJob } from './projects-model';
import { AtlasApiError } from '../app/atlas-api';
import { messages } from '../i18n/messages';
import type { ProjectEntry } from '../provider/rpc-schemas';

let container: HTMLDivElement;
let root: Root;

const PROJECTS: ProjectEntry[] = [
    { name: 'atlas-sample', root_path: '/repos/atlas-sample', nodes: 76, edges: 179 },
    { name: 'rootless' },
];

function fakeSource(jobs: () => IndexJob[] = () => []): ProjectsSource & { calls: Record<string, unknown[][]> } {
    const calls: Record<string, unknown[][]> = {};
    const record = <T,>(name: string, impl: (...args: never[]) => T) => (...args: never[]): T => {
        (calls[name] ??= []).push(args);
        return impl(...args);
    };
    return {
        calls,
        listProjects: record('listProjects', async () => PROJECTS),
        projectHealth: record('projectHealth', async () => ({
            status: 'healthy' as const,
            nodes: 76,
            edges: 179,
            sizeBytes: 1572864,
            reason: '',
        })),
        deleteProject: record('deleteProject', async () => undefined),
        browse: record('browse', async (path: string) => ({
            path: path.length > 0 ? path : '/home',
            dirs: ['repo-a', 'repo-b'],
            parent: '/',
            roots: [],
        })),
        startIndex: record('startIndex', async (path: string) => ({ slot: 0, path })),
        indexJobs: record('indexJobs', async () => jobs()),
        adr: record('adr', async () => ({ hasAdr: true, content: '# old record', updatedAt: '2026-09-05 10:00' })),
        saveAdr: record('saveAdr', async () => undefined),
        logs: record('logs', async () => ({ lines: ['line one', 'line two'], total: 40 })),
        processes: record('processes', async () => ({
            selfPid: 42,
            selfRssMb: 120.5,
            processes: [
                { pid: 42, cpu: 0.2, rssMb: 120.5, elapsed: '00:10', command: 'codebase-memory-mcp', isSelf: true },
                { pid: 43, cpu: 12.5, rssMb: 900, elapsed: '01:00', command: 'codebase-memory-mcp', isSelf: false },
            ],
        })),
    };
}

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div');
    document.body.appendChild(container);
    root = createRoot(container);
});

afterEach(async () => {
    await act(async () => {
        root.unmount();
    });
    container.remove();
});

/** Let every pending fetch answer and every effect after it run. */
async function settle(): Promise<void> {
    await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
    });
}

async function render(overrides: Partial<ProjectsPanelProps> = {}): Promise<ProjectsPanelProps> {
    const props: ProjectsPanelProps = {
        project: 'atlas-sample',
        source: fakeSource(),
        onOpenProject: vi.fn(),
        onClose: vi.fn(),
        pollMs: 1,
        ...overrides,
    };
    await act(async () => {
        root.render(<ProjectsPanel {...props} />);
    });
    await settle();
    return props;
}

const byTestId = (id: string): HTMLElement | null => container.querySelector(`[data-testid="${id}"]`);
const allByTestId = (id: string): HTMLElement[] => [...container.querySelectorAll<HTMLElement>(`[data-testid="${id}"]`)];

async function click(element: HTMLElement | null): Promise<void> {
    expect(element, 'the control must exist').not.toBeNull();
    await act(async () => {
        element?.click();
    });
    await settle();
}

async function type(element: HTMLElement | null, value: string): Promise<void> {
    expect(element, 'the field must exist').not.toBeNull();
    const field = element as HTMLInputElement | HTMLTextAreaElement;
    const proto = field instanceof HTMLTextAreaElement ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    Object.getOwnPropertyDescriptor(proto, 'value')?.set?.call(field, value);
    await act(async () => {
        field.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await settle();
}

describe('the list', () => {
    it('shows every project with its counts, marks the open one, and names its source', async () => {
        await render();
        const rows = allByTestId('atlas-projects-row');
        expect(rows.map((row) => row.dataset['project'])).toEqual(['atlas-sample', 'rootless']);
        expect(rows[0]?.dataset['open']).toBe('true');
        expect(rows[0]?.textContent).toContain(messages.projects.counts(76, 179));
        expect(rows[1]?.textContent).toContain(messages.projects.countsUnknown);
        expect(container.textContent).toContain(messages.projects.listSource);
    });

    it('offers open only for a project that is not open, and reindex only with a root', async () => {
        await render();
        const [open, rootless] = allByTestId('atlas-projects-row');
        expect(open?.querySelector('[data-testid="atlas-projects-open"]')).toBeNull();
        expect(rootless?.querySelector('[data-testid="atlas-projects-open"]')).not.toBeNull();
        expect(open?.querySelector('[data-testid="atlas-projects-reindex"]')).not.toBeNull();
        expect(rootless?.querySelector('[data-testid="atlas-projects-reindex"]')).toBeNull();
        expect(rootless?.textContent).toContain(messages.projects.noRoot);
    });

    it('says when nothing is indexed, and points at the form', async () => {
        const source = fakeSource();
        source.listProjects = async () => [];
        await render({ source, project: '' });
        expect(byTestId('atlas-projects-empty')?.textContent).toBe(messages.projects.listEmpty);
        expect(byTestId('atlas-projects-adr-none')?.textContent).toBe(messages.projects.adrNoProject);
    });

    it('hands the name to the window when open is pressed', async () => {
        const props = await render();
        await click(byTestId('atlas-projects-open'));
        expect(props.onOpenProject).toHaveBeenCalledWith('rootless');
    });

    it('asks for the health verdict on check and prints it as a sentence', async () => {
        const props = await render();
        await click(allByTestId('atlas-projects-check')[0] ?? null);
        expect((props.source as ReturnType<typeof fakeSource>).calls['projectHealth']).toEqual([['atlas-sample']]);
        expect(byTestId('atlas-projects-health')?.textContent).toBe(messages.projects.healthHealthy(76, 179, '1.5'));
    });

    it('reindexes with the root the server sent', async () => {
        const props = await render();
        await click(byTestId('atlas-projects-reindex'));
        expect((props.source as ReturnType<typeof fakeSource>).calls['startIndex']).toEqual([['/repos/atlas-sample', 'atlas-sample']]);
    });

    it('deletes only after the question in place is answered, then reloads the list', async () => {
        const props = await render();
        const source = props.source as ReturnType<typeof fakeSource>;
        await click(allByTestId('atlas-projects-remove')[1] ?? null);
        expect(byTestId('atlas-projects-armed')?.textContent).toContain(messages.projects.removeArmed('rootless'));
        expect(source.calls['deleteProject']).toBeUndefined();

        await click(byTestId('atlas-projects-remove-cancel'));
        expect(byTestId('atlas-projects-armed')).toBeNull();
        expect(source.calls['deleteProject']).toBeUndefined();

        await click(allByTestId('atlas-projects-remove')[1] ?? null);
        const listReads = source.calls['listProjects']?.length ?? 0;
        await click(byTestId('atlas-projects-remove-confirm'));
        expect(source.calls['deleteProject']).toEqual([['rootless']]);
        expect(source.calls['listProjects']?.length).toBe(listReads + 1);
        expect(byTestId('atlas-projects-list-notice')?.textContent).toBe(messages.projects.removed('rootless'));
    });
});

describe('the form', () => {
    it('suggests the name from the path and shows the index button only once both are there', async () => {
        await render();
        expect(byTestId('atlas-projects-start')).toBeNull();
        await type(byTestId('atlas-projects-path'), '/repos/new-thing/');
        expect((byTestId('atlas-projects-name') as HTMLInputElement).value).toBe('new-thing');
        expect(byTestId('atlas-projects-start')).not.toBeNull();
        await type(byTestId('atlas-projects-name'), '');
        expect(byTestId('atlas-projects-start')).toBeNull();
    });

    it('keeps a name the reader typed when the path changes again', async () => {
        await render();
        await type(byTestId('atlas-projects-path'), '/repos/one');
        await type(byTestId('atlas-projects-name'), 'chosen');
        await type(byTestId('atlas-projects-path'), '/repos/two');
        expect((byTestId('atlas-projects-name') as HTMLInputElement).value).toBe('chosen');
    });

    it('starts the job, shows the acknowledgement, polls while it runs, and reloads the list when it is done', async () => {
        let status: IndexJob['status'] = 'indexing';
        const source = fakeSource(() => [{ slot: 0, status, path: '/repos/new-thing', error: '' }]);
        const props = await render({ source });
        await type(byTestId('atlas-projects-path'), '/repos/new-thing');
        await click(byTestId('atlas-projects-start'));
        expect(source.calls['startIndex']).toEqual([['/repos/new-thing', 'new-thing']]);
        expect(byTestId('atlas-projects-start-notice')?.textContent).toBe(messages.projects.started('/repos/new-thing'));
        expect(allByTestId('atlas-projects-job')[0]?.dataset['status']).toBe('indexing');

        const polled = source.calls['indexJobs']?.length ?? 0;
        const listReads = source.calls['listProjects']?.length ?? 0;
        status = 'done';
        await settle();
        await settle();
        expect(source.calls['indexJobs']?.length).toBeGreaterThan(polled);
        expect(allByTestId('atlas-projects-job')[0]?.dataset['status']).toBe('done');
        expect(source.calls['listProjects']?.length).toBe(listReads + 1);
        void props;
    });

    it('shows the server refusal as it came', async () => {
        const source = fakeSource();
        source.startIndex = async () => {
            throw new AtlasApiError('/api/index', 403, 'root outside the workspace');
        };
        await render({ source });
        await type(byTestId('atlas-projects-path'), '/elsewhere');
        await click(byTestId('atlas-projects-start'));
        expect(byTestId('atlas-projects-start-notice')?.textContent).toBe(
            messages.projects.startError('root outside the workspace'),
        );
    });

    it('browses folders and puts the chosen one into the path', async () => {
        const props = await render();
        await click(byTestId('atlas-projects-browse'));
        expect(byTestId('atlas-projects-browse-level')?.textContent).toContain(messages.projects.browseAt('/home'));
        expect(allByTestId('atlas-projects-browse-dir').map((entry) => entry.textContent)).toEqual(['repo-a', 'repo-b']);
        await click(allByTestId('atlas-projects-browse-dir')[1] ?? null);
        expect((props.source as ReturnType<typeof fakeSource>).calls['browse']).toEqual([[''], ['/home/repo-b']]);
        await click(byTestId('atlas-projects-browse-use'));
        expect((byTestId('atlas-projects-path') as HTMLInputElement).value).toBe('/home/repo-b');
        expect((byTestId('atlas-projects-name') as HTMLInputElement).value).toBe('repo-b');
        expect(byTestId('atlas-projects-browse-level')).toBeNull();
    });
});

describe('the decision record', () => {
    it('loads the record of the open project into the editor and saves what was typed', async () => {
        const props = await render();
        const source = props.source as ReturnType<typeof fakeSource>;
        expect(byTestId('atlas-projects-adr-state')?.textContent).toBe(messages.projects.adrUpdated('2026-09-05 10:00'));
        expect((byTestId('atlas-projects-adr-text') as HTMLTextAreaElement).value).toBe('# old record');
        await type(byTestId('atlas-projects-adr-text'), '# new record');
        await click(byTestId('atlas-projects-adr-save'));
        expect(source.calls['saveAdr']).toEqual([['atlas-sample', '# new record']]);
        expect(byTestId('atlas-projects-adr-notice')?.textContent).toBe(messages.projects.adrSaved);
    });

    it('says busy when the server is indexing, and refused otherwise', async () => {
        const source = fakeSource();
        source.saveAdr = async () => {
            throw new AtlasApiError('/api/adr', 423, 'project is busy');
        };
        await render({ source });
        await click(byTestId('atlas-projects-adr-save'));
        expect(byTestId('atlas-projects-adr-notice')?.textContent).toBe(messages.projects.adrBusy);
    });

    it('names the absence of a record instead of showing an empty timestamp', async () => {
        const source = fakeSource();
        source.adr = async () => ({ hasAdr: false, content: '', updatedAt: '' });
        await render({ source });
        expect(byTestId('atlas-projects-adr-state')?.textContent).toBe(messages.projects.adrNone);
    });
});

describe('the server block', () => {
    it('shows the serving process, the others, and the log tail with its source', async () => {
        await render();
        expect(byTestId('atlas-projects-processes')?.textContent).toContain(messages.projects.processesSelf(42, '120.5'));
        expect(allByTestId('atlas-projects-process').map((entry) => entry.textContent)).toEqual([
            messages.projects.processRow(43, '12.5', '900.0', '01:00'),
        ]);
        expect(byTestId('atlas-projects-logs')?.textContent).toBe('line one\nline two');
        expect(container.textContent).toContain(messages.projects.logsSource(2, 40));
    });

    it('asks again on reload', async () => {
        const props = await render();
        const source = props.source as ReturnType<typeof fakeSource>;
        const before = source.calls['logs']?.length ?? 0;
        await click(byTestId('atlas-projects-server-reload'));
        expect(source.calls['logs']?.length).toBe(before + 1);
        expect(source.calls['processes']?.length).toBe(before + 1);
    });
});

describe('the frame', () => {
    it('closes on escape and on the close button', async () => {
        const props = await render();
        await act(async () => {
            byTestId('atlas-projects')?.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
        });
        expect(props.onClose).toHaveBeenCalledTimes(1);
        await click(byTestId('atlas-projects-close'));
        expect(props.onClose).toHaveBeenCalledTimes(2);
    });

    it('carries every section a reader can look for', async () => {
        await render();
        expect(allByTestId('atlas-projects-section').map((section) => section.dataset['section'])).toEqual([
            'list',
            'index',
            'adr',
            'server',
        ]);
    });
});
