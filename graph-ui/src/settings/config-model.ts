/** The daemon owns the registry, validation, provenance and apply timing. */
export interface ConfigSetting {
    key: string;
    label: string;
    description: string;
    category: string;
    type: 'boolean' | 'integer' | 'number' | 'enum' | 'string';
    options?: string[];
    minimum?: number;
    maximum?: number;
    defaultValue: string | null;
    value: string | null;
    override: string | null;
    effective: string | null;
    effectiveKnown?: boolean;
    source: string;
    applyMode: string;
    pendingRestart: boolean;
    editable: boolean;
    readOnlyReason?: string;
    environment?: string;
    cli?: string;
    sourceFile?: string;
}
export interface ConfigSnapshot { revision: string; settings: ConfigSetting[] }
export interface ConfigService {
    configuration(): Promise<ConfigSnapshot>;
    saveConfiguration(revision: string, changes: Record<string, string | null>): Promise<ConfigSnapshot>;
}
const record = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const nullableString = (value: unknown): value is string | null => value === null || typeof value === 'string';

export function readConfigSnapshot(value: unknown): ConfigSnapshot {
    if (!record(value) || typeof value.revision !== 'string' || !Array.isArray(value.settings)) throw new Error('The daemon returned an invalid configuration snapshot.');
    const keys = new Set<string>();
    for (const item of value.settings) {
        if (!record(item) || typeof item.key !== 'string' || !item.key || ['__proto__', 'constructor', 'prototype'].includes(item.key) || keys.has(item.key)
            || ['label', 'description', 'category', 'source', 'applyMode'].some(key => typeof item[key] !== 'string')
            || !['boolean', 'integer', 'number', 'enum', 'string'].includes(String(item.type))
            || ['value', 'override', 'effective', 'defaultValue'].some(key => !nullableString(item[key]))
            || typeof item.editable !== 'boolean' || typeof item.pendingRestart !== 'boolean'
            || (item.effectiveKnown !== undefined && typeof item.effectiveKnown !== 'boolean')
            || (item.options !== undefined && (!Array.isArray(item.options) || item.options.some(option => typeof option !== 'string')))
            || (item.type === 'enum' && (!Array.isArray(item.options) || !item.options.length))
            || ['minimum', 'maximum'].some(key => item[key] !== undefined && (typeof item[key] !== 'number' || !Number.isFinite(item[key])))) {
            throw new Error('The daemon returned an invalid setting. Configuration was not changed.');
        }
        keys.add(item.key);
    }
    return value as unknown as ConfigSnapshot;
}

export function configValueError(setting: ConfigSetting, value: string | null): string | undefined {
    if (value === null) return;
    if (!setting.editable) return 'This setting is controlled outside the running daemon.';
    if (setting.type === 'boolean' && !['true', 'false'].includes(value)) return 'Choose on or off.';
    if (setting.type === 'enum' && !setting.options?.includes(value)) return 'Choose a listed value.';
    if (setting.type === 'integer' || setting.type === 'number') {
        if (!value.trim() || (setting.type === 'integer' && !/^-?\d+$/.test(value)) || !Number.isFinite(Number(value))) return 'Enter a valid number.';
        const number = Number(value);
        if (setting.type === 'integer' && !Number.isSafeInteger(number)) return 'Enter a safe whole number.';
        if (setting.minimum !== undefined && number < setting.minimum) return `Minimum: ${setting.minimum}.`;
        if (setting.maximum !== undefined && number > setting.maximum) return `Maximum: ${setting.maximum}.`;
    }
}

export const CONFIG_TEXT = { title: 'Config', open: 'Open configuration', close: 'Close configuration' } as const;
