// Minimal fixture test. It exercises listUsers and nothing else, so the graph
// is expected to show a test edge for that function only.

import test from 'node:test';
import assert from 'node:assert/strict';
import { listUsers } from '../src/services/userService';

test('listUsers returns an array', () => {
    const users = listUsers();
    assert.ok(Array.isArray(users));
});

test('listUsers honours the limit', () => {
    const users = listUsers(1);
    assert.ok(users.length <= 1);
});
