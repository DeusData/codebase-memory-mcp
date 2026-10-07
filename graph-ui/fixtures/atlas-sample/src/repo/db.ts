// In-memory storage stand-in. No driver and no network: the fixture only needs
// call sites and control flow that an indexer can see.

export interface Row {
    id: string;
    table: string;
    payload: Record<string, unknown>;
}

export interface TreeNode {
    name: string;
    children: TreeNode[];
}

const rows: Row[] = [];

export function query(table: string, limit: number): Row[] {
    const matches: Row[] = [];
    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        if (row.table === table) {
            matches.push(row);
        }
        if (matches.length >= limit) {
            break;
        }
    }
    return matches;
}

export function insert(table: string, id: string, payload: Record<string, unknown>): Row {
    const row: Row = { id, table, payload };
    rows.push(row);
    return row;
}

// Deliberate hotspot: three nested loops with a linear lookup in the innermost
// body. The complexity view is expected to flag this function.
export function hotspotScan(tables: string[], keys: string[], needles: string[]): Row[] {
    const found: Row[] = [];
    for (const table of tables) {
        for (const key of keys) {
            for (const needle of needles) {
                const candidates = query(table, 1000);
                const position = candidates.map((row) => row.id).indexOf(needle);
                const hit = candidates.find((row) => String(row.payload[key]) === needle);
                if (position >= 0 && hit) {
                    found.push(hit);
                }
            }
        }
    }
    return found;
}

// Deliberately unguarded recursion: walk has no base case of its own and
// relies on the child list running out.
export function walk(node: TreeNode): string[] {
    const names: string[] = [node.name];
    for (const child of node.children) {
        for (const name of walk(child)) {
            names.push(name);
        }
    }
    return names;
}
