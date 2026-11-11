import { useCallback, useSyncExternalStore } from 'react';
import { CHAT_INPUT_TOKENS } from './explanation-response';
import { BROWSER_MODELS, type BrowserModel } from './model-policy';

/** Below these the local agent cannot hold a question with its evidence or a sentence of answer. */
export const MIN_INPUT_TOKENS = 512;
export const MIN_OUTPUT_TOKENS = 32;

export interface TokenLimits { inputTokens: number; outputTokens: number }
export interface AgentPreferences {
    modelId: string;
    automatic: boolean;
    /** Load the chosen model on start when its files are cached; off unless chosen (K10). */
    autoLoad: boolean;
    /** Only models whose limits were changed; every other model uses its defaults. */
    limits: Readonly<Record<string, TokenLimits>>;
}

const AVAILABLE = BROWSER_MODELS.filter(model => model.availability === 'available');
export const DEFAULT_AGENT_PREFERENCES: Readonly<AgentPreferences> = Object.freeze({ modelId: AVAILABLE[0].id, automatic: true, autoLoad: false, limits: Object.freeze({}) });
export const AGENT_PREFERENCES_KEY = 'cbm-agent-preferences-v1';
const CHANGE = 'cbm-agent-preferences-change';

/** The model policy bounds both limits: the answer stays within the model's output
 * ceiling and the question plus the answer within its browser context. */
export function tokenLimitBounds(model: BrowserModel, outputTokens: number) {
    return { input: { min: MIN_INPUT_TOKENS, max: model.contextTokens - outputTokens }, output: { min: MIN_OUTPUT_TOKENS, max: model.maxOutputTokens } };
}

export function defaultTokenLimits(model: BrowserModel): TokenLimits {
    return { inputTokens: Math.min(CHAT_INPUT_TOKENS, model.contextTokens - model.maxOutputTokens), outputTokens: model.maxOutputTokens };
}

export function tokenLimitsFor(preferences: Readonly<AgentPreferences>, model: BrowserModel): TokenLimits {
    return preferences.limits[model.id] ?? defaultTokenLimits(model);
}

/** Clamp a requested change into the policy, so a saved value is always one validation accepts. */
export function clampTokenLimits(model: BrowserModel, current: TokenLimits, change: Partial<TokenLimits>): TokenLimits {
    const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, Math.round(value)));
    const outputTokens = clamp(change.outputTokens ?? current.outputTokens, MIN_OUTPUT_TOKENS, model.maxOutputTokens);
    return { outputTokens, inputTokens: clamp(change.inputTokens ?? current.inputTokens, MIN_INPUT_TOKENS, model.contextTokens - outputTokens) };
}

const record = (value: unknown): Record<string, unknown> | undefined => typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown> : undefined;
const within = (value: unknown, min: number, max: number): value is number => Number.isSafeInteger(value) && (value as number) >= min && (value as number) <= max;

function validated(value: unknown): Readonly<AgentPreferences> {
    const row = record(value) ?? {};
    const limits: Record<string, TokenLimits> = {};
    for (const model of BROWSER_MODELS) {
        const stored = record(record(row.limits)?.[model.id]);
        if (!stored || !within(stored.outputTokens, MIN_OUTPUT_TOKENS, model.maxOutputTokens)) continue;
        if (within(stored.inputTokens, MIN_INPUT_TOKENS, model.contextTokens - stored.outputTokens)) {
            limits[model.id] = Object.freeze({ inputTokens: stored.inputTokens, outputTokens: stored.outputTokens });
        }
    }
    return Object.freeze({
        modelId: BROWSER_MODELS.some(model => model.id === row.modelId) ? row.modelId as string : DEFAULT_AGENT_PREFERENCES.modelId,
        automatic: typeof row.automatic === 'boolean' ? row.automatic : DEFAULT_AGENT_PREFERENCES.automatic,
        autoLoad: typeof row.autoLoad === 'boolean' ? row.autoLoad : DEFAULT_AGENT_PREFERENCES.autoLoad,
        limits: Object.freeze(limits),
    });
}

function decode(raw: string | null): Readonly<AgentPreferences> {
    try {
        const stored = record(JSON.parse(raw ?? 'null'));
        if (stored?.version === 1) return validated(stored.preferences);
    } catch { /* Invalid storage is equivalent to no saved preference. */ }
    return DEFAULT_AGENT_PREFERENCES;
}

let snapshot: { raw: string | null; value: Readonly<AgentPreferences>; sessionOnly: boolean } | undefined;

/** Stable snapshots: an unchanged store returns the same object. */
export function readAgentPreferences(): Readonly<AgentPreferences> {
    if (snapshot?.sessionOnly) return snapshot.value;
    let raw: string | null;
    try { raw = window.localStorage.getItem(AGENT_PREFERENCES_KEY); }
    catch { return snapshot?.value ?? DEFAULT_AGENT_PREFERENCES; }
    if (snapshot?.raw === raw) return snapshot.value;
    snapshot = { raw, value: decode(raw), sessionOnly: false };
    return snapshot.value;
}

export function setAgentPreferences(update: Partial<AgentPreferences> | ((current: Readonly<AgentPreferences>) => Partial<AgentPreferences>)): void {
    const current = readAgentPreferences();
    const value = validated({ ...current, ...(typeof update === 'function' ? update(current) : update) });
    if (JSON.stringify(value) === JSON.stringify(current)) return;
    const raw = JSON.stringify({ version: 1, preferences: value });
    let sessionOnly = false;
    try { window.localStorage.setItem(AGENT_PREFERENCES_KEY, raw); }
    catch { sessionOnly = true; }
    snapshot = { raw, value, sessionOnly };
    window.dispatchEvent(new CustomEvent(CHANGE));
}

/** Browser-local: the chosen model, automatic explanations, loading on start and token limits per model. */
export function useAgentPreferences(): { preferences: Readonly<AgentPreferences>; setPreferences: typeof setAgentPreferences } {
    const subscribe = useCallback((listener: () => void) => {
        const stored = (event: StorageEvent) => { if (event.key === null || event.key === AGENT_PREFERENCES_KEY) listener(); };
        window.addEventListener(CHANGE, listener);
        window.addEventListener('storage', stored);
        return () => { window.removeEventListener(CHANGE, listener); window.removeEventListener('storage', stored); };
    }, []);
    const preferences = useSyncExternalStore(subscribe, readAgentPreferences, () => DEFAULT_AGENT_PREFERENCES);
    return { preferences, setPreferences: setAgentPreferences };
}
