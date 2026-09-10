/**
 * The HTTP leg of the frontend log: POST /api/ui-log, same origin.
 *
 * Two ways to send, chosen by the buffer: an ordinary fetch while the page
 * lives, and `navigator.sendBeacon` when it is being closed, because a fetch
 * started from pagehide is cancelled with the page while a beacon is handed
 * to the browser and survives it. Both carry `application/json`, which the
 * server requires on every POST under /api (the same header the projects
 * panel sends); the beacon sets it through the Blob's type.
 *
 * No error is thrown from here and nothing is written to the console: a
 * transport that logged its own failures would feed the buffer it serves.
 */

import { UI_LOG_ROUTE } from './ui-log';
import type { UiLogPayload, UiLogTransport } from './ui-log';

export interface HttpUiLogTransportOptions {
    /** Origin of the server, no trailing slash. Empty means same origin. */
    base?: string;
    fetch?: typeof globalThis.fetch;
    /** Replaceable beacon so a test sees what the page sends on its way out. */
    beacon?: (url: string, body: Blob) => boolean;
}

function defaultBeacon(): ((url: string, body: Blob) => boolean) | undefined {
    const nav = (globalThis as { navigator?: Navigator }).navigator;
    if (nav === undefined || typeof nav.sendBeacon !== 'function') {
        return undefined;
    }
    return (url, body) => nav.sendBeacon(url, body);
}

export function httpUiLogTransport(options: HttpUiLogTransportOptions = {}): UiLogTransport {
    const url = `${options.base ?? ''}${UI_LOG_ROUTE}`;
    return {
        async send(payload: UiLogPayload, final: boolean): Promise<boolean> {
            const body = JSON.stringify(payload);
            if (final) {
                const beacon = options.beacon ?? defaultBeacon();
                if (beacon !== undefined) {
                    try {
                        return beacon(url, new Blob([body], { type: 'application/json' }));
                    } catch {
                        return false;
                    }
                }
            }
            const doFetch = options.fetch ?? globalThis.fetch;
            try {
                const response = await doFetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body,
                    keepalive: final,
                });
                return response.ok;
            } catch {
                return false;
            }
        },
    };
}
