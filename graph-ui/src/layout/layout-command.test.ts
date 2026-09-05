import { describe, expect, it } from 'vitest';

import {
    LIVE_AGENTS_COMMAND,
    RESET_LAYOUT_COMMAND,
    SETTINGS_COMMAND,
    lineCommandOf,
} from './layout-command';

describe('die Befehle der Kommandozeile', () => {

    it('erkennt die Zeile, die das Layout zuruecksetzt', () => {
        expect(lineCommandOf(RESET_LAYOUT_COMMAND)).toBe('reset-layout');
        expect(lineCommandOf('  Reset   Layout ')).toBe('reset-layout');
        expect(lineCommandOf('RESET LAYOUT')).toBe('reset-layout');
    });

    it('erkennt die Zeile, die die Einstellungen aufschlaegt', () => {
        expect(lineCommandOf(SETTINGS_COMMAND)).toBe('open-settings');
        expect(lineCommandOf('  Settings ')).toBe('open-settings');
        expect(lineCommandOf('SETTINGS')).toBe('open-settings');
    });

    it('erkennt die Zeile, die den Live-Modus der Agenten umlegt', () => {
        expect(lineCommandOf(LIVE_AGENTS_COMMAND)).toBe('toggle-live-agents');
        expect(lineCommandOf('  Live   Agents ')).toBe('toggle-live-agents');
        expect(lineCommandOf('LIVE AGENTS')).toBe('toggle-live-agents');
    });

    /*
     * Der Punkt der Enge: diese Zeile sucht auch. Jede Unschaerfe hier ist eine
     * Suche, die dem Leser stattdessen das Layout umbaut oder ein Panel
     * aufschlaegt.
     */
    it('nimmt kein Wort, nach dem jemand suchen wuerde', () => {
        for (const line of [
            'reset', 'layout', 'resetLayout', 'reset layouts', 'reset layout now', '',
            'setting', 'settings panel', 'open settings', 'getSettings', 'SettingsPanel',
            'agents', 'live', 'liveAgents', 'live agent', 'show live agents',
        ]) {
            expect(lineCommandOf(line), line).toBe('none');
        }
    });
});
