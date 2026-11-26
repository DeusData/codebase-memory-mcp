// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it } from 'vitest';
import { clockTime, RefreshControl, useRefreshFeedback, type RefreshReading } from './refresh-feedback';

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

const labels = { idle: 'Refresh', busy: 'Refreshing…', done: (time: string) => `Refreshed at ${time}` };
let begin: () => void = () => {};
function Fixture({ reading }: { reading: RefreshReading }) {
    const refresh = useRefreshFeedback(reading, () => new Date(2026, 9, 4, 7, 3, 9).getTime());
    begin = refresh.begin;
    return <RefreshControl labels={labels} feedback={refresh.feedback} onRefresh={refresh.begin} />;
}
const show = (reading: RefreshReading) => act(async () => root.render(<Fixture reading={reading} />));
const status = () => host.querySelector('[role="status"]')?.textContent;

it('names the wall clock with two digits each', () => {
    expect(clockTime(new Date(2026, 9, 4, 7, 3, 9).getTime())).toBe('07:03:09');
});

it('waits for the reading the refresh started, not the one shown when it was pressed', async () => {
    await show({ key: 'a', settled: true, value: { routes: 1 } });
    await act(async () => begin());
    // Still the earlier reading: nothing is decided yet.
    await show({ key: 'a', settled: true, value: { routes: 1 } });
    expect(host.querySelector('button')?.textContent).toBe('Refreshing…');
    await show({ key: 'b', settled: false });
    expect(status()).toBe('');
    await show({ key: 'b', settled: true, value: { routes: 1 } });
    expect(status()).toBe('Up to date at 07:03:09: no changes since the last load');
});

it('drops the status once another reading than the refreshed one is shown', async () => {
    await show({ key: 'a', settled: true, value: 1 });
    await act(async () => begin());
    await show({ key: 'b', settled: true, value: 2 });
    expect(status()).toBe('Refreshed at 07:03:09');
    // Another start or a reindex: the status would speak about data no longer shown.
    await show({ key: 'c', settled: true, value: 3 });
    expect(status()).toBe('');
});
