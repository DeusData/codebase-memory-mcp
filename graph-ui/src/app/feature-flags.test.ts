import { afterEach, expect, it, vi } from 'vitest';

afterEach(() => { vi.unstubAllEnvs(); vi.resetModules(); });

it.each([undefined, '', 'false', '1', 'TRUE'])('keeps Agents unavailable for build value %s', async value => {
    vi.stubEnv('VITE_EXPERIMENTAL_AGENTS', value);
    vi.resetModules();
    const { experimentalAgentsEnabled } = await import('./feature-flags');
    const { initialWorkspace, allowedWorkspace, availableWorkspaces } = await import('./workspace-strings');
    const { lineCommandOf } = await import('../layout/layout-command');
    const { WIRED_MENU_SHORTCUTS } = await import('./shortcuts');
    expect(experimentalAgentsEnabled).toBe(false);
    expect(availableWorkspaces().map(item => item.id)).not.toContain('agents');
    expect(initialWorkspace('agents', 'system')).toBe('explore');
    expect(initialWorkspace(null, 'agents')).toBe('explore');
    expect(allowedWorkspace('agents')).toBe('explore');
    expect(initialWorkspace('coverage', 'agents')).toBe('coverage');
    expect(initialWorkspace('adr', null)).toBe('adr');
    expect(initialWorkspace(null, 'adr')).toBe('adr');
    expect(initialWorkspace(null, null)).toBe('architecture');
    expect(lineCommandOf('live agents')).toBe('none');
    expect(lineCommandOf('fullscreen')).toBe('none');
    expect(WIRED_MENU_SHORTCUTS).not.toContain('g');
    expect(WIRED_MENU_SHORTCUTS).toContain('l');
});

it('restores the experimental workspace and controls only with explicit build opt-in', async () => {
    vi.stubEnv('VITE_EXPERIMENTAL_AGENTS', 'true');
    vi.resetModules();
    const { experimentalAgentsEnabled } = await import('./feature-flags');
    const { initialWorkspace, allowedWorkspace, availableWorkspaces } = await import('./workspace-strings');
    const { lineCommandOf } = await import('../layout/layout-command');
    const { WIRED_MENU_SHORTCUTS } = await import('./shortcuts');
    expect(experimentalAgentsEnabled).toBe(true);
    expect(availableWorkspaces().map(item => item.id)).toContain('agents');
    expect(initialWorkspace('agents', null)).toBe('agents');
    expect(initialWorkspace(null, 'agents')).toBe('agents');
    expect(allowedWorkspace('agents')).toBe('agents');
    expect(lineCommandOf('live agents')).toBe('toggle-live-agents');
    expect(lineCommandOf('fullscreen')).toBe('toggle-fullscreen');
    expect(WIRED_MENU_SHORTCUTS).toContain('g');
});
