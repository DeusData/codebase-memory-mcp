// @vitest-environment jsdom
/*
 * Review of K42 ("other Refresh buttons the same way"): Refresh in ADR read
 * the decision record again and nothing on screen changed (measured in the
 * browser: no visible state for a single frame). It now says that it runs and
 * then when it ran and whether anything changed, as the Architecture
 * Refresh buttons do.
 */
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import AdrWorkspace from './AdrWorkspace';
import type { AdrRecord } from '../projects/projects-model';
import { clearAdrDraft } from './adr-model';

let host: HTMLDivElement, root: Root;
const project = 'adr-refresh-test';
const record = (content: string): AdrRecord => ({ hasAdr: true, content, updatedAt: '2026-09-14T12:00:00Z' });
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    clearAdrDraft(project);
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); clearAdrDraft(project); });

function deferred<T>() {
    let resolve!: (value: T) => void, reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
const refresh = () => [...host.querySelectorAll('.adr-actions button')][0] as HTMLButtonElement;
const status = () => host.querySelector('.adr-actions .atlas-refresh-status')?.textContent;
const TIME = '\\d\\d:\\d\\d:\\d\\d';

it('K42: Refresh in ADR says that it runs, then when it ran and whether the record changed, or that it failed', async () => {
    let next = deferred<AdrRecord>();
    const adr = vi.fn().mockResolvedValueOnce(record('# Decisions')).mockImplementation(() => next.promise);
    await act(async () => root.render(<AdrWorkspace project={project} active source={{ adr, saveAdr: vi.fn() }} />));
    expect(refresh().textContent).toBe('Refresh');
    expect(status()).toBe('');

    await act(async () => refresh().click());
    expect(refresh().textContent).toBe('Refreshing…');
    expect(refresh().getAttribute('aria-disabled')).toBe('true');
    await act(async () => next.resolve(record('# Decisions')));
    expect(refresh().textContent).toBe('Refresh');
    expect(status()).toMatch(new RegExp(`^Up to date at ${TIME}: no changes since the last load$`));

    next = deferred<AdrRecord>();
    await act(async () => refresh().click());
    await act(async () => next.resolve(record('# Decisions\n\n## Storage')));
    expect(status()).toMatch(new RegExp(`^Decisions refreshed at ${TIME}$`));

    next = deferred<AdrRecord>();
    await act(async () => refresh().click());
    await act(async () => next.reject(new Error('offline')));
    // The alert below the toolbar says what failed and offers Try again; the status beside the button only says when.
    expect(status()).toMatch(new RegExp(`^Refresh failed at ${TIME}$`));
    expect(host.querySelector('.adr-notice[role="alert"]')?.textContent).toContain('Could not load the decision record.');
});
