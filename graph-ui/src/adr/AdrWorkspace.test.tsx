// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import AdrWorkspace, { type AdrWorkspaceProps } from './AdrWorkspace';
import type { AdrRecord } from '../projects/projects-model';
import { clearAdrDraft, readAdrDraft } from './adr-model';

let host: HTMLDivElement;
let root: Root;
const record = (content = '# Decisions\n\n## Storage\n\nUse a local database.'): AdrRecord => ({ hasAdr: true, content, updatedAt: '2026-09-14T12:00:00Z' });
const empty: AdrRecord = { hasAdr: false, content: '', updatedAt: '' };
const projects = ['adr-test-alpha', 'adr-test-beta', 'adr-test-restored'];
const project = projects[0];

beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    for (const name of projects) clearAdrDraft(name);
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => {
    await act(async () => root.unmount()); host.remove();
    for (const name of projects) clearAdrDraft(name);
    vi.restoreAllMocks();
});

function source(initial = record()) {
    let current = initial;
    return {
        adr: vi.fn(async (_project: string) => ({ ...current })),
        saveAdr: vi.fn(async (_project: string, content: string) => { current = record(content); }),
        replace(next: AdrRecord) { current = next; },
    };
}
async function render(api: AdrWorkspaceProps['source'], overrides: Partial<AdrWorkspaceProps> = {}) {
    await act(async () => root.render(<AdrWorkspace project={project} active source={api} {...overrides} />));
}
function button(name: string): HTMLButtonElement {
    const found = [...host.querySelectorAll('button')].find(candidate => candidate.textContent === name);
    if (!found) throw new Error(`Missing button: ${name}`);
    return found;
}
async function click(name: string) { await act(async () => button(name).click()); }
async function edit(content: string) {
    if (!host.querySelector('textarea')) await click('Edit');
    await act(async () => {
        const input = host.querySelector('textarea')!;
        Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(input, content);
        input.dispatchEvent(new Event('input', { bubbles: true }));
    });
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

it('loads only while active and refreshes the same project when a clean reader is reopened', async () => {
    const api = source();
    await render(api, { active: false });
    expect(api.adr).not.toHaveBeenCalled();
    await render(api);
    expect(api.adr).toHaveBeenCalledWith(project);
    expect(host.querySelector('article')?.textContent).toContain('Use a local database.');
    await render(api, { active: false });
    api.replace(record('# New decision'));
    await render(api);
    expect(api.adr).toHaveBeenCalledTimes(2);
    expect(host.querySelector('article')?.textContent).toBe('New decision');
});

it('preflights and verifies an explicit save; editing never writes automatically', async () => {
    const api = source();
    await render(api);
    await edit('# Decisions\n\nUse SQLite.');
    expect(api.saveAdr).not.toHaveBeenCalled();
    expect(readAdrDraft(project)?.content).toContain('Use SQLite.');
    await click('Save');
    expect(api.adr).toHaveBeenCalledTimes(3);
    expect(api.saveAdr).toHaveBeenCalledExactlyOnceWith(project, '# Decisions\n\nUse SQLite.');
    expect(host.querySelector('textarea')).toBeNull();
    expect(host.querySelector('[role="status"]')?.textContent).toBe('Saved');
    expect(readAdrDraft(project)).toBeUndefined();
});

it('retains dirty edits when hidden, switched to another project, and remounted', async () => {
    const api = source();
    await render(api);
    await edit('# My unsaved choice');
    await render(api, { active: false });
    await render(api);
    expect(api.adr).toHaveBeenCalledTimes(1);
    expect(host.querySelector('textarea')?.value).toBe('# My unsaved choice');
    await render(api, { project: projects[1] });
    expect(host.querySelector('textarea')).toBeNull();
    await render(api);
    expect(host.querySelector('textarea')?.value).toBe('# My unsaved choice');
    expect(host.querySelector('strong')?.textContent).toBe(project);
    await act(async () => root.unmount());
    root = createRoot(host);
    await render(api);
    expect(host.querySelector('textarea')?.value).toBe('# My unsaved choice');
    expect(api.saveAdr).not.toHaveBeenCalled();
});

it('restores a session-stored draft and its base after reload, then catches an intervening change', async () => {
    const name = projects[2];
    sessionStorage.setItem(`cbm.adr.draft:${encodeURIComponent(name)}`, JSON.stringify({ version: 1, base: record(), content: '# Restored draft' }));
    const api = source(record('# Updated elsewhere'));
    await render(api, { project: name });
    expect(host.querySelector('textarea')?.value).toBe('# Restored draft');
    expect(host.textContent).toContain('The server document changed.');
    expect(button('Save').disabled).toBe(true);
    expect(readAdrDraft(name)?.base.content).toBe(record().content);
});

it('does not replace a new project with the previous project’s late read', async () => {
    const pending = deferred<AdrRecord>();
    const api = source();
    api.adr.mockImplementationOnce(() => pending.promise);
    await render(api);
    await render(api, { project: projects[1] });
    await act(async () => pending.resolve(record('# Wrong old project')));
    expect(host.querySelector('strong')?.textContent).toBe(projects[1]);
    expect(host.textContent).not.toContain('Wrong old project');
});

it('shows a retry on read failure and prevents editing or saving an unread record', async () => {
    const api = source();
    api.adr.mockRejectedValueOnce(new Error('offline'));
    await render(api);
    expect(host.textContent).toContain('Could not load the decision record.');
    expect(host.querySelector('textarea')).toBeNull();
    expect([...host.querySelectorAll('button')].map(value => value.textContent)).not.toContain('Save');
    await click('Try again');
    await edit('# Recovered');
    expect(button('Save').disabled).toBe(false);
});

it('keeps the draft through a busy save and succeeds on an explicit retry', async () => {
    const api = source();
    api.saveAdr.mockRejectedValueOnce({ status: 423 });
    await render(api);
    await edit('# Pending decision');
    await click('Save');
    expect(host.textContent).toContain('The project is busy.');
    expect(host.querySelector('textarea')?.value).toBe('# Pending decision');
    expect(readAdrDraft(project)?.content).toBe('# Pending decision');
    expect(host.querySelector('[role="status"]')?.textContent).not.toBe('Saved');
    await click('Save');
    expect(host.querySelector('[role="status"]')?.textContent).toBe('Saved');
});

it('does not write when its preflight read fails', async () => {
    const api = source();
    await render(api);
    await edit('# Keep until the server is readable');
    api.adr.mockRejectedValueOnce(new Error('offline'));
    await click('Save');
    expect(api.saveAdr).not.toHaveBeenCalled();
    expect(host.textContent).toContain('Could not check the latest version.');
    expect(readAdrDraft(project)?.content).toBe('# Keep until the server is readable');
});

it('does not refresh a dirty draft on reentry after an earlier read retry', async () => {
    const api = source();
    api.adr.mockRejectedValueOnce(new Error('offline'));
    await render(api);
    await click('Try again');
    await edit('# Keep my draft');
    await render(api, { active: false });
    await render(api);
    expect(api.adr).toHaveBeenCalledTimes(2);
    expect(host.querySelector('textarea')?.value).toBe('# Keep my draft');
});

it('keeps a draft across project switches when session storage is unavailable', async () => {
    const api = source();
    await render(api);
    // Node's Web Storage can survive jsdom global setup; target the storage actually used.
    const write = vi.spyOn(Object.getPrototypeOf(sessionStorage), 'setItem')
        .mockImplementation(() => { throw new Error('storage unavailable'); });
    await edit('# In-memory draft');
    expect(write).toHaveBeenCalled();
    expect(host.textContent).toContain('browser storage is unavailable');
    await render(api, { project: projects[1] });
    await render(api);
    expect(host.querySelector('textarea')?.value).toBe('# In-memory draft');
    expect(host.textContent).toContain('browser storage is unavailable');
});

it('blocks conflicting writes until the latest document has been explicitly reviewed', async () => {
    const api = source();
    await render(api);
    await edit('# My change');
    api.replace(record('# Another writer\n\nPreserve this decision.'));
    await click('Save');
    expect(api.saveAdr).not.toHaveBeenCalled();
    expect(button('Save').disabled).toBe(true);
    expect(host.querySelector('textarea')?.value).toBe('# My change');
    expect([...host.querySelectorAll('button')].map(value => value.textContent)).not.toContain('Use latest as edit base');
    await click('Review latest version');
    expect(host.querySelector('[aria-label="Latest server version"]')?.textContent).toContain('Preserve this decision.');
    await click('Use latest as edit base');
    await edit('# Another writer\n\nPreserve this decision.\n\n## My change');
    await click('Save');
    expect(api.saveAdr).toHaveBeenCalledExactlyOnceWith(project, '# Another writer\n\nPreserve this decision.\n\n## My change');
});

it('can discard its conflicting draft and load the reviewed server document', async () => {
    const api = source();
    await render(api);
    await edit('# My change');
    api.replace(record('# New server record'));
    await click('Save');
    await click('Review latest version');
    await click('Discard draft and load latest');
    expect(host.querySelector('textarea')).toBeNull();
    expect(host.querySelector('article')?.textContent).toBe('New server record');
    expect(readAdrDraft(project)).toBeUndefined();
    expect(api.saveAdr).not.toHaveBeenCalled();
});

it('does not report Saved after a verification failure and retains the draft for retry', async () => {
    const api = source();
    await render(api);
    await edit('# Awaiting verification');
    api.adr.mockResolvedValueOnce(record()).mockRejectedValueOnce(new Error('read-back failed'));
    await click('Save');
    expect(host.textContent).toContain('The save could not be verified.');
    expect(readAdrDraft(project)?.content).toBe('# Awaiting verification');
    expect(host.querySelector('[role="status"]')?.textContent).not.toBe('Saved');
    await click('Save');
    expect(api.saveAdr).toHaveBeenCalledTimes(1);
    expect(host.querySelector('[role="status"]')?.textContent).toBe('Saved');
});

it('keeps a draft if the verified server content differs from what was submitted', async () => {
    const api = source();
    await render(api);
    await edit('# Submitted');
    api.adr.mockResolvedValueOnce(record()).mockResolvedValueOnce(record('# Concurrent replacement'));
    await click('Save');
    expect(host.textContent).toContain('The server returned different content after saving.');
    expect(host.querySelector('textarea')?.value).toBe('# Submitted');
    expect(button('Save').disabled).toBe(true);
    expect(readAdrDraft(project)?.content).toBe('# Submitted');
});

it('disables edits during a save and ignores its completion after a project switch', async () => {
    const pending = deferred<void>();
    const api = source();
    api.saveAdr.mockImplementationOnce(() => pending.promise);
    await render(api);
    await edit('# Original project draft');
    await click('Save');
    expect(host.querySelector('textarea')?.disabled).toBe(true);
    expect(button('Cancel').disabled).toBe(true);
    await render(api, { project: projects[1] });
    await act(async () => pending.resolve());
    expect(host.querySelector('strong')?.textContent).toBe(projects[1]);
    expect(host.querySelector('[role="status"]')?.textContent).not.toBe('Saved');
    expect(readAdrDraft(project)?.content).toBe('# Original project draft');
});

it('enforces UTF-8 content and complete JSON request limits', async () => {
    const api = source();
    await render(api);
    await edit('🧭'.repeat(2001));
    expect(button('Save').disabled).toBe(true);
    expect(host.textContent).toContain('8,004 / 8,000 bytes');
    await edit('\u0001'.repeat(3000));
    expect(button('Save').disabled).toBe(true);
    expect(host.textContent).toContain('The encoded save request is too large.');
    await edit('a'.repeat(8000));
    expect(button('Save').disabled).toBe(false);
});

it('renders Markdown safely and builds a navigable outline from actual headings', async () => {
    const api = source(record('# Decisions\n\n## Same title\n\n| Choice | Reason |\n| --- | --- |\n| A | B |\n\n## Same title\n\n```js\nrun();\n```\n\n[unsafe](javascript:alert(1))\n\n<strong>Raw HTML stays text</strong>\n\n![remote](https://example.com/tracker.png)'));
    const scroll = vi.fn();
    const original = HTMLElement.prototype.scrollIntoView;
    HTMLElement.prototype.scrollIntoView = scroll;
    try {
        await render(api);
        expect(host.querySelector('table')).not.toBeNull();
        expect(host.querySelector('pre code')?.textContent).toBe('run();\n');
        expect(host.querySelector('article strong, img, a[href^="javascript:"]')).toBeNull();
        const nav = host.querySelector('[aria-label="On this page"]')!;
        expect([...nav.querySelectorAll('button')].map(value => value.textContent)).toEqual(['Same title', 'Same title']);
        const headings = [...host.querySelectorAll('article h1, article h2')];
        expect(new Set(headings.map(value => value.id)).size).toBe(3);
        await act(async () => nav.querySelectorAll('button')[1].click());
        expect(scroll).toHaveBeenCalledOnce();
        expect(document.activeElement).toBe(headings[2]);
    } finally { HTMLElement.prototype.scrollIntoView = original; }
});

it('does not reserve an outline column for documents without headings', async () => {
    await render(source(record('A short decision without a heading.')));
    expect(host.querySelector('[aria-label="On this page"]')).toBeNull();
    expect(host.querySelector('.adr-body')?.getAttribute('data-outline')).toBe('false');
});

it('creates a generic decision scaffold without saving until requested and cancels explicitly', async () => {
    const api = source(empty);
    await render(api);
    await click('Create decision record');
    const text = host.querySelector('textarea')?.value ?? '';
    expect(text).toContain('### Context');
    expect(text).toContain('### Alternatives');
    expect(text).not.toContain(project);
    expect(api.saveAdr).not.toHaveBeenCalled();
    await click('Preview');
    expect(host.querySelector('article h1')?.textContent).toBe('Architecture decisions');
    await click('Cancel');
    expect(readAdrDraft(project)).toBeUndefined();
    expect(host.querySelector('textarea')).toBeNull();
});

it('warns before unloading only while a draft is dirty', async () => {
    const api = source();
    await render(api);
    let event = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(event);
    expect(event.defaultPrevented).toBe(false);
    await edit('# Draft');
    event = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(event);
    expect(event.defaultPrevented).toBe(true);
    await click('Cancel');
    event = new Event('beforeunload', { cancelable: true });
    window.dispatchEvent(event);
    expect(event.defaultPrevented).toBe(false);
});

it('does not request or offer writes without a selected project', async () => {
    const api = source();
    await render(api, { project: '' });
    expect(api.adr).not.toHaveBeenCalled();
    expect(host.textContent).toContain('Select a project');
    expect(host.querySelector('button')).toBeNull();
});
