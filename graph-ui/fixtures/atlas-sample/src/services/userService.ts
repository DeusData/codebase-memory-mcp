// User use cases. Kept small on purpose so the facts the graph reports for
// createUser stay stable: it calls, throws and reads exactly one thing each.

import { insert, query, Row } from '../repo/db';
import { UserEntity } from '../types';
import type { User } from '../types';
import { ValidationError, validateUser } from '../util/validate';

function toUser(row: Row): User {
    return {
        id: row.id,
        createdAt: String(row.payload.createdAt ?? ''),
        email: String(row.payload.email ?? ''),
        name: String(row.payload.name ?? ''),
    };
}

export function listUsers(limit = 50): User[] {
    const rows = query('users', limit);
    return rows.map(toUser);
}

export function createUser(input: unknown): User {
    const parsed = validateUser(input);
    const dsn = process.env.DB_URL ?? '';
    if (dsn.length === 0) {
        throw new ValidationError('DB_URL', 'DB_URL must be set before writing users');
    }
    const entity = new UserEntity(`user-${listUsers().length + 1}`, parsed.email, parsed.name);
    const row = insert('users', entity.id, {
        email: entity.email,
        name: entity.name,
        createdAt: entity.createdAt,
    });
    return toUser(row);
}

// Thin alias kept on purpose: orderService exports a function with the same
// bare name, which makes "create" an ambiguous trace target.
export function create(input: unknown): User {
    return createUser(input);
}
