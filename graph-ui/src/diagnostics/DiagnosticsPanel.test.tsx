// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RpcIntelligenceClient } from '../provider/rpc-client';
import { buildCoverageIndex } from '../app/tree-model';
import DiagnosticsPanel, { type DiagnosticsPanelProps } from './DiagnosticsPanel';

let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
});
afterEach(async () => { await act(async () => root.unmount()); container.remove(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });
async function click(text: string) {
    const button = [...container.querySelectorAll('button')].find((item) => item.textContent === text);
    expect(button).toBeDefined(); await act(async () => button!.click());
}

function clientFixture() {
    const calls: { url: string; tool: string; args: Record<string, unknown> }[] = [];
    const fetch = vi.fn(async (url: string | URL | Request, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body));
        calls.push({ url: String(url), tool: body.params.name, args: body.params.arguments });
        if (String(url) !== '/rpc' || init?.method !== 'POST') throw new Error('Unexpected external request');
        let payload: unknown;
        if (body.params.name === 'index_status') payload = { parse_partial: { count: 1, files: [{ path: 'src/broken.ts', error_ranges: '3-5' }] } };
        else if (body.params.name === 'check_index_coverage') payload = body.params.arguments.paths
            ? { paths: [{ path: 'src/broken.ts', status: 'partial', freshness: 'metadata_changed', recommended_action: 'read_source_and_reindex', coverage: [{ path: 'src/broken.ts', kind: 'parse_partial', detail: '3-5' }] }] }
            : { metadata: { generation: 'fixture-generation', generation_matches: true, recording_status: 'complete' }, scopes: [{ scope: '.', total: 1, has_more: false, entries: [{ path: 'src/broken.ts', kind: 'parse_partial', detail: '3-5' }] }] };
        else throw new Error('Unexpected tool/agent action');
        return { ok: true, status: 200, text: async () => JSON.stringify({ jsonrpc: '2.0', id: body.id, result: { content: [{ type: 'text', text: JSON.stringify(payload) }] } }) } as Response;
    });
    return { client: new RpcIntelligenceClient({ fetch: fetch as typeof globalThis.fetch }), calls, fetch };
}

describe('Local diagnosis user boundary', () => {
    it('does no read until requested, uses existing tools, and only copies the edited report', async () => {
        const fixture = clientFixture(); const outside = vi.fn(); vi.stubGlobal('fetch', outside);
        const writeText = vi.fn().mockResolvedValue(undefined);
        Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
        const onNavigate = vi.fn(); const onCoverage = vi.fn();
        await act(async () => root.render(<DiagnosticsPanel project="fixture" client={fixture.client} active onClose={vi.fn()} onNavigate={onNavigate} path="src/broken.ts" onCoverage={onCoverage} />));
        expect(fixture.calls).toHaveLength(0); expect(container.querySelector('textarea')).toBeNull();
        await click('Run local diagnosis');
        expect(fixture.calls.map((call) => call.tool)).toEqual(['index_status', 'check_index_coverage', 'check_index_coverage']);
        expect(fixture.calls.every((call) => call.url === '/rpc')).toBe(true);
        expect(container.textContent).toContain('File size or modification time changed');
        expect(container.textContent).toContain('fixture-generation'); expect(onCoverage).toHaveBeenCalledOnce();
        const area = container.querySelector('textarea')!;
        await act(async () => {
            Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')!.set!.call(area, 'Reviewed, redacted draft');
            area.dispatchEvent(new Event('input', { bubbles: true }));
        });
        await click('Copy edited report'); expect(writeText).toHaveBeenCalledWith('Reviewed, redacted draft');
        const createObjectURL = vi.fn().mockReturnValue('blob:local-diagnostic-report');
        const revokeObjectURL = vi.fn();
        Object.defineProperty(URL, 'createObjectURL', { configurable: true, value: createObjectURL });
        Object.defineProperty(URL, 'revokeObjectURL', { configurable: true, value: revokeObjectURL });
        const linkClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(function (this: HTMLAnchorElement) {
            expect(this.href).toBe('blob:local-diagnostic-report'); expect(this.download).toBe('local-index-diagnosis.txt');
        });
        await click('Download edited report'); expect(createObjectURL).toHaveBeenCalledOnce(); expect(linkClick).toHaveBeenCalledOnce(); expect(revokeObjectURL).toHaveBeenCalledWith('blob:local-diagnostic-report');
        await click('src/broken.ts'); expect(onNavigate).toHaveBeenCalledWith('src/broken.ts');
        expect(fixture.calls).toHaveLength(3); expect(outside).not.toHaveBeenCalled();
        expect(container.querySelector('a[href^="http"]')).toBeNull();
        expect([...container.querySelectorAll('button')].some((button) => /upload|publish|create issue/i.test(button.textContent ?? ''))).toBe(false);
    });
    it('never presents a failed read as complete coverage', async () => {
        const fixture = clientFixture(); fixture.fetch.mockRejectedValue(new Error('SQLite busy'));
        await act(async () => root.render(<DiagnosticsPanel project="fixture" client={fixture.client} active onClose={vi.fn()} onNavigate={vi.fn()} />));
        await click('Run local diagnosis');
        expect(container.querySelector('[role="alert"]')?.textContent).toContain('SQLite busy');
        expect(container.textContent).toContain('Completeness is unknown');
        expect(container.querySelector('textarea')).toBeNull();
    });
    it('renders shared coverage reasons and an honest empty state without refetching', async () => {
        const fixture = clientFixture(); const props: DiagnosticsPanelProps = { project: 'fixture', client: fixture.client, active: true, onClose: vi.fn(), onNavigate: vi.fn(), coverage: buildCoverageIndex({}) };
        await act(async () => root.render(<DiagnosticsPanel {...props} />));
        expect(container.textContent).toContain('does not prove complete indexing');
        expect(fixture.calls).toHaveLength(0);
        await act(async () => root.render(<DiagnosticsPanel {...props} coverage={buildCoverageIndex({ scopes: [{ scope: '.', requestedScope: '.', status: 'complete', total: 1, hasMore: false, entries: [{ path: 'vendor/', kind: 'not_indexed_dir', detail: 'gitignore' }] }] })} />));
        expect(container.textContent).toContain('Excluded by index rules'); expect(container.textContent).toContain('gitignore');
    });
    it('discards a diagnosis from a previous project and does not continue its path query', async () => {
        let resolve!: (data: unknown) => void;
        const fixture = clientFixture();
        vi.spyOn(fixture.client, 'indexStatusPayload').mockImplementationOnce(() => new Promise((done) => { resolve = done; }));
        const onCoverage = vi.fn();
        const props = { project: 'old-project', client: fixture.client, active: true, path: 'old.ts', onClose: vi.fn(), onNavigate: vi.fn(), onCoverage };
        await act(async () => root.render(<DiagnosticsPanel {...props} />));
        await click('Run local diagnosis');
        await act(async () => root.render(<DiagnosticsPanel {...props} project="new-project" path="new.ts" />));
        await act(async () => resolve({}));
        expect(container.textContent).toContain('new-project'); expect(container.querySelector('textarea')).toBeNull();
        expect(onCoverage).not.toHaveBeenCalled();
        // loadCoverage owns its bounded scope lookup; the obsolete selected
        // path must never be requested or committed afterward.
        expect(fixture.calls.some((call) => call.args['paths'] !== undefined)).toBe(false);
    });
});
