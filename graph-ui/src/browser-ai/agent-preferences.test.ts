// @vitest-environment jsdom
import { beforeEach, describe, expect, it } from 'vitest';
import {
    AGENT_PREFERENCES_KEY, clampTokenLimits, DEFAULT_AGENT_PREFERENCES, defaultTokenLimits, MIN_INPUT_TOKENS, MIN_OUTPUT_TOKENS,
    readAgentPreferences, setAgentPreferences, tokenLimitBounds, tokenLimitsFor,
} from './agent-preferences';
import { BROWSER_MODELS } from './model-policy';

const [first, second] = BROWSER_MODELS;
const store = (preferences: unknown, version = 1) => window.localStorage.setItem(AGENT_PREFERENCES_KEY, JSON.stringify({ version, preferences }));
beforeEach(() => window.localStorage.clear());

describe('browser-local agent preferences', () => {
    it('defaults to the first available model, automatic explanations and the policy limits', () => {
        expect(readAgentPreferences()).toEqual(DEFAULT_AGENT_PREFERENCES);
        expect(DEFAULT_AGENT_PREFERENCES.modelId).toBe(first.id);
        expect(tokenLimitsFor(readAgentPreferences(), first)).toEqual({ inputTokens: 2048, outputTokens: first.maxOutputTokens });
        expect(tokenLimitBounds(first, 512)).toEqual({ input: { min: MIN_INPUT_TOKENS, max: first.contextTokens - 512 }, output: { min: MIN_OUTPUT_TOKENS, max: first.maxOutputTokens } });
    });

    it('restores a saved model, automatic flag and limits per model', () => {
        store({ modelId: second.id, automatic: false, limits: { [second.id]: { inputTokens: 4096, outputTokens: 256 } } });
        const restored = readAgentPreferences();
        expect(restored).toEqual({ modelId: second.id, automatic: false, autoLoad: false, limits: { [second.id]: { inputTokens: 4096, outputTokens: 256 } } });
        expect(readAgentPreferences()).toBe(restored);
        expect(tokenLimitsFor(restored, second)).toEqual({ inputTokens: 4096, outputTokens: 256 });
        expect(tokenLimitsFor(restored, first)).toEqual(defaultTokenLimits(first));
    });

    it('rejects malformed data, unknown versions, unknown models and limits outside the model policy', () => {
        window.localStorage.setItem(AGENT_PREFERENCES_KEY, '{bad json');
        expect(readAgentPreferences()).toEqual(DEFAULT_AGENT_PREFERENCES);
        store({ modelId: second.id }, 2);
        expect(readAgentPreferences()).toEqual(DEFAULT_AGENT_PREFERENCES);
        store({ modelId: 'someone/else', automatic: 'no', limits: {
            [first.id]: { inputTokens: first.contextTokens, outputTokens: 512 },
            [second.id]: { inputTokens: 2048, outputTokens: second.maxOutputTokens + 1 },
            'someone/else': { inputTokens: 1024, outputTokens: 64 },
        } });
        expect(readAgentPreferences()).toEqual(DEFAULT_AGENT_PREFERENCES);
        store({ limits: { [first.id]: { inputTokens: 1024.5, outputTokens: 64 }, [second.id]: { inputTokens: 1024, outputTokens: 64 } } });
        expect(readAgentPreferences().limits).toEqual({ [second.id]: { inputTokens: 1024, outputTokens: 64 } });
    });

    it('loads the chosen model on start only when asked to, off by default (K10)', () => {
        expect(DEFAULT_AGENT_PREFERENCES.autoLoad).toBe(false);
        store({ modelId: second.id, autoLoad: 'yes' });
        expect(readAgentPreferences().autoLoad).toBe(false);
        setAgentPreferences({ autoLoad: true });
        expect(JSON.parse(window.localStorage.getItem(AGENT_PREFERENCES_KEY)!).preferences.autoLoad).toBe(true);
        expect(readAgentPreferences()).toMatchObject({ modelId: second.id, autoLoad: true });
    });

    it('saves a versioned record and clamps requested limits into the model policy', () => {
        const limits = clampTokenLimits(first, defaultTokenLimits(first), { inputTokens: 99_999, outputTokens: 1 });
        expect(limits).toEqual({ inputTokens: first.contextTokens - MIN_OUTPUT_TOKENS, outputTokens: MIN_OUTPUT_TOKENS });
        expect(clampTokenLimits(first, limits, { outputTokens: 512 })).toEqual({ inputTokens: first.contextTokens - 512, outputTokens: 512 });
        setAgentPreferences({ automatic: false, limits: { [first.id]: limits } });
        expect(JSON.parse(window.localStorage.getItem(AGENT_PREFERENCES_KEY)!)).toEqual({ version: 1,
            preferences: { modelId: first.id, automatic: false, autoLoad: false, limits: { [first.id]: limits } } });
        expect(readAgentPreferences()).toMatchObject({ automatic: false, limits: { [first.id]: limits } });
    });
});
