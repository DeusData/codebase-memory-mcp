// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import AgentSetup, { agentInstallCommand } from './AgentSetup';
let host: HTMLDivElement; let root: Root;
beforeEach(() => { (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true; host = document.createElement('div'); document.body.append(host); root = createRoot(host); });
afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.restoreAllMocks(); });
const open = async () => { await act(async () => { const details = host.querySelector('details')!; details.open = true; details.dispatchEvent(new Event('toggle')); }); };
it('exposes actionable setup without querying or changing anything until opened', async () => {
    const api = { repoInfo: vi.fn().mockResolvedValue({ rootPath: '/repo with spaces', branch: 'main', remoteUrl: '' }) };
    await act(async () => root.render(<AgentSetup api={api} project="project-a" port={9749} />));
    expect(host.querySelector('summary')?.textContent).toBe('Connect your agent');
    expect(api.repoInfo).not.toHaveBeenCalled();
    await open();
    expect(api.repoInfo).toHaveBeenCalledWith('project-a');
    expect(host.querySelector('pre')?.textContent).toContain("--root '/repo with spaces' --project 'project-a'");
    expect(host.textContent).toContain('no file contents');
    expect(host.textContent).toContain('does not configure your coding client');
    expect(host.textContent).toContain('Other coding clients need their own event adapter');
});
it('never presents the previous repository command when a new root fails to load', async () => {
    const api = { repoInfo: vi.fn().mockResolvedValueOnce({ rootPath: '/repo-a' }).mockRejectedValueOnce(new Error('repository unavailable')) };
    await act(async () => root.render(<AgentSetup api={api} project="a" port={9749} />)); await open();
    expect(host.querySelector('pre')?.textContent).toContain('/repo-a');
    await act(async () => root.render(<AgentSetup api={api} project="b" port={9749} />));
    expect(host.querySelector('pre')).toBeNull();
    expect(host.querySelector('[role="alert"]')?.textContent).toContain('repository unavailable');
});
it('quotes project and root values as literal shell words', () => {
    const command = agentInstallCommand("/repo'; touch /tmp/unwanted", '$(env)', 9749);
    expect(command).toContain("--root '/repo'\\''; touch /tmp/unwanted'");
    expect(command).toContain("--project '$(env)'");
});
