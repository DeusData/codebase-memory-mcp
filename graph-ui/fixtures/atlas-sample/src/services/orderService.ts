// Order use cases. getOrder wraps the repository call in try/catch so the
// fixture has a recoverable failure path to render.

import { insert, query } from '../repo/db';
import type { Order } from '../types';
import { validateId } from '../util/validate';

export function getOrder(id: string): Order | undefined {
    try {
        const key = validateId(id);
        const rows = query('orders', 100);
        const row = rows.find((candidate) => candidate.id === key);
        if (!row) {
            return undefined;
        }
        return {
            id: row.id,
            createdAt: String(row.payload.createdAt ?? ''),
            customerId: String(row.payload.customerId ?? ''),
            total: Number(row.payload.total ?? 0),
        };
    } catch (error) {
        console.error(`[orders] lookup failed for ${id}: ${String(error)}`);
        return undefined;
    }
}

// Same bare name as userService.create on purpose: the pair is the ambiguity
// fixture for path tracing.
export function create(customerId: string, total: number): Order {
    const createdAt = new Date(0).toISOString();
    const id = `order-${customerId}-${total}`;
    insert('orders', id, { customerId, total, createdAt });
    return { id, createdAt, customerId, total };
}
