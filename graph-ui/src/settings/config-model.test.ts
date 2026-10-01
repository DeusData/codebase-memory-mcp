import { describe, expect, it, vi } from 'vitest';
import { AtlasApi } from '../app/atlas-api';
import { configValueError, readConfigSnapshot, type ConfigSetting } from './config-model';

export const workersSetting: ConfigSetting = { key: 'CBM_WORKERS', label: 'Index workers', description: 'Parallel indexing workers.', category: 'resources', type: 'integer', minimum: 1, maximum: 256,
    defaultValue: null, value: '4', override: null, effective: '4', source: 'environment', applyMode: 'restart', pendingRestart: false, editable: true };
describe('configuration contract', () => {
    it('keeps false, zero and reset distinct and checks numeric bounds', () => {
        expect(configValueError(workersSetting, '0')).toBe('Minimum: 1.');
        expect(configValueError(workersSetting, '257')).toBe('Maximum: 256.');
        expect(configValueError(workersSetting, '1.5')).toBe('Enter a valid number.');
        expect(configValueError(workersSetting, '')).toBe('Enter a valid number.');
        expect(configValueError(workersSetting, null)).toBeUndefined();
        expect(configValueError({ ...workersSetting, minimum: 0 }, '0')).toBeUndefined();
        expect(configValueError({ ...workersSetting, type: 'boolean' }, 'false')).toBeUndefined();
    });
    it('rejects malformed or duplicate daemon descriptors', () => {
        expect(readConfigSnapshot({ revision: '7', settings: [workersSetting] }).settings[0]).toEqual(workersSetting);
        for (const settings of [[workersSetting, workersSetting], [{ ...workersSetting, key: '__proto__' }], [{ ...workersSetting, effective: undefined }], [{ ...workersSetting, type: 'enum' }], [{ ...workersSetting, maximum: Infinity }], [{ ...workersSetting, effectiveKnown: 'yes' }]]) {
            expect(() => readConfigSnapshot({ revision: '7', settings })).toThrow();
        }
    });
    it('sends a revision-bound JSON patch and preserves null reset', async () => {
        const fetcher = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({ revision: '8', settings: [workersSetting] }), { status: 200 }));
        const api = new AtlasApi({ fetch: fetcher });
        await api.saveConfiguration('7', { CBM_WORKERS: null });
        expect(fetcher).toHaveBeenCalledWith('/api/config', { method: 'POST', headers: { Accept: 'application/json', 'Content-Type': 'application/json' }, body: JSON.stringify({ revision: '7', changes: { CBM_WORKERS: null } }) });
    });
    it('surfaces a revision conflict instead of treating it as saved', async () => {
        const api = new AtlasApi({ fetch: vi.fn<typeof fetch>().mockResolvedValue(new Response('{"error":"Configuration changed; reload."}', { status: 409 })) });
        await expect(api.saveConfiguration('old', { CBM_WORKERS: '8' })).rejects.toMatchObject({ status: 409 });
    });
});
