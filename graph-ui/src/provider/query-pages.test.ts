import { expect, it } from 'vitest';
import { RpcIntelligenceClient } from './rpc-client';
import { FakeRpc } from '../test-support/rpc-recordings';

const first = 'rows: 1  (cols: n.name)\n  first\nreturned: 1\ntotal: 2\ntotal_relation: eq\nhas_more: true\ntruncated: true\ntruncation_reason: output_budget\nnext_cursor: q1.next\nnext_offset: 1\n';
const last = 'rows: 1  (cols: n.name)\n  last\nreturned: 1\ntotal: 2\noffset: 1\ntotal_relation: eq\nhas_more: false\ntruncated: false\n';
function client(end = last) {
    const rpc = new FakeRpc([
        { tool: 'query_graph', when: args => args.cursor === 'q1.next', text: end },
        { tool: 'query_graph', text: first },
    ]);
    return { rpc, client: new RpcIntelligenceClient({ fetch: rpc.fetch }) };
}

it('reads cursor pages without losing the project, query or row order', async () => {
    const { client: c, rpc } = client();
    expect(await c.queryRows('project-a', 'MATCH query')).toEqual([{ 'n.name': 'first' }, { 'n.name': 'last' }]);
    expect(rpc.calls[1].args).toEqual({ project: 'project-a', query: 'MATCH query', cursor: 'q1.next' });
});

it('keeps truncation metadata on the single-page API', async () => {
    expect(await client().client.queryGraph('p', 'q')).toMatchObject({ total: 2, returned: 1, truncated: true, nextCursor: 'q1.next' });
});

it.each([last.replace('offset: 1', 'offset: 0'), last.replace('total: 2', 'total: 3'), last.replace('n.name', 'n.file_path')])(
    'does not merge incompatible pages into a plausible complete answer', async end => {
        await expect(client(end).client.queryRows('p', 'q')).rejects.toThrow('result snapshot');
    },
);

it('does not turn a bounded engine result without continuation into complete rows', async () => {
    const rpc = new FakeRpc([{ tool: 'query_graph', text: first.replace('next_cursor: q1.next\n', '') }]);
    await expect(new RpcIntelligenceClient({ fetch: rpc.fetch }).queryRows('p', 'q')).rejects.toThrow('Incomplete query results');
});

it('rejects a repeated cursor', async () => {
    await expect(client(last.replace('has_more: false', 'has_more: true') + 'next_cursor: q1.next\n').client.queryRows('p', 'q'))
        .rejects.toThrow('Incomplete query results');
});
