/*
 * The readers of the projects panel: what they keep, what they refuse, and
 * the two decisions the panel makes on top of them (when a job has finished,
 * which name a path suggests).
 */

import { describe, expect, it } from 'vitest';

import {
    anyIndexing,
    childPath,
    finishedSince,
    megabytes,
    projectHref,
    projectNameFor,
    readAdr,
    readBrowse,
    readHealth,
    readIndexJobs,
    readIndexStarted,
    readLogs,
    readProcesses,
} from './projects-model';

describe('readIndexJobs', () => {
    it('reads the job table as the server writes it', () => {
        const jobs = readIndexJobs([
            { slot: 0, status: 'indexing', path: '/repo/a', error: '' },
            { slot: 1, status: 'done', path: '/repo/b', error: '' },
            { slot: 2, status: 'error', path: '/repo/c', error: 'parse failed' },
        ]);
        expect(jobs).toEqual([
            { slot: 0, status: 'indexing', path: '/repo/a', error: '' },
            { slot: 1, status: 'done', path: '/repo/b', error: '' },
            { slot: 2, status: 'error', path: '/repo/c', error: 'parse failed' },
        ]);
    });

    it('names a status it does not know instead of guessing one', () => {
        expect(readIndexJobs([{ slot: 0, status: 'queued', path: '/x', error: '' }])[0]?.status).toBe('unknown');
    });

    it('reads anything that is not a list as no jobs', () => {
        expect(readIndexJobs({ jobs: [] })).toEqual([]);
        expect(readIndexJobs('nope')).toEqual([]);
        expect(readIndexJobs([1, 'two', null])).toEqual([]);
    });

    it('knows whether anything is still running', () => {
        expect(anyIndexing([])).toBe(false);
        expect(anyIndexing(readIndexJobs([{ slot: 0, status: 'done', path: '/a', error: '' }]))).toBe(false);
        expect(anyIndexing(readIndexJobs([{ slot: 0, status: 'indexing', path: '/a', error: '' }]))).toBe(true);
    });

    it('reports the paths that finished between two readings, and only those', () => {
        const before = readIndexJobs([
            { slot: 0, status: 'indexing', path: '/a', error: '' },
            { slot: 1, status: 'indexing', path: '/b', error: '' },
            { slot: 2, status: 'done', path: '/old', error: '' },
        ]);
        const after = readIndexJobs([
            { slot: 0, status: 'done', path: '/a', error: '' },
            { slot: 1, status: 'indexing', path: '/b', error: '' },
            { slot: 2, status: 'done', path: '/old', error: '' },
        ]);
        expect(finishedSince(before, after)).toEqual(['/a']);
        expect(finishedSince(after, after)).toEqual([]);
    });

    it('reads the acknowledgement of a started job', () => {
        expect(readIndexStarted({ status: 'indexing', slot: 2, path: '/repo' })).toEqual({ slot: 2, path: '/repo' });
        expect(readIndexStarted(null)).toEqual({ slot: -1, path: '' });
    });
});

describe('readBrowse', () => {
    it('reads a level with its parent and the optional drive roots', () => {
        expect(readBrowse({ path: '/Users/x', dirs: ['a', 'b'], parent: '/Users' })).toEqual({
            path: '/Users/x',
            dirs: ['a', 'b'],
            parent: '/Users',
            roots: [],
        });
        expect(readBrowse({ path: 'C:/', dirs: [], parent: 'C:/', roots: ['C:/', 'D:/'] }).roots).toEqual(['C:/', 'D:/']);
    });

    it('drops entries that are not strings', () => {
        expect(readBrowse({ path: '/', dirs: ['ok', 3, null] }).dirs).toEqual(['ok']);
    });

    it('joins a folder onto the level without doubling the separator', () => {
        expect(childPath(readBrowse({ path: '/Users/x', dirs: [] }), 'repo')).toBe('/Users/x/repo');
        expect(childPath(readBrowse({ path: '/', dirs: [] }), 'Users')).toBe('/Users');
        expect(childPath(readBrowse({ path: '', dirs: [] }), 'repo')).toBe('repo');
    });
});

describe('readHealth', () => {
    it('reads the three verdicts and their numbers', () => {
        expect(readHealth({ status: 'healthy', nodes: 76, edges: 178, size_bytes: 1048576 })).toEqual({
            status: 'healthy',
            nodes: 76,
            edges: 178,
            sizeBytes: 1048576,
            reason: '',
        });
        expect(readHealth({ status: 'missing' }).status).toBe('missing');
        expect(readHealth({ status: 'corrupt', reason: 'cannot open' })).toMatchObject({ status: 'corrupt', reason: 'cannot open' });
    });

    it('leaves a number the server did not send undefined', () => {
        const health = readHealth({ status: 'healthy' });
        expect(health.nodes).toBeUndefined();
        expect(health.sizeBytes).toBeUndefined();
    });

    it('names an unknown verdict as unknown', () => {
        expect(readHealth({ status: 'fine' }).status).toBe('unknown');
        expect(readHealth(undefined).status).toBe('unknown');
    });

    it('prints megabytes with one decimal', () => {
        expect(megabytes(1048576)).toBe('1.0');
        expect(megabytes(1572864)).toBe('1.5');
        expect(megabytes(0)).toBe('0.0');
    });
});

describe('readAdr', () => {
    it('reads a record with its content and timestamp', () => {
        expect(readAdr({ has_adr: true, content: '# ADR\n', updated_at: '2026-09-05 12:00' })).toEqual({
            hasAdr: true,
            content: '# ADR\n',
            updatedAt: '2026-09-05 12:00',
        });
    });

    it('reads the absence of a record as empty, not as an error', () => {
        expect(readAdr({ has_adr: false })).toEqual({ hasAdr: false, content: '', updatedAt: '' });
    });
});

describe('readLogs and readProcesses', () => {
    it('reads the log tail and its total', () => {
        expect(readLogs({ lines: ['a', 'b'], total: 400 })).toEqual({ lines: ['a', 'b'], total: 400 });
    });

    it('falls back to the shown count when the total is missing', () => {
        expect(readLogs({ lines: ['a'] }).total).toBe(1);
        expect(readLogs(null)).toEqual({ lines: [], total: 0 });
    });

    it('reads the process report with the flag for the serving process', () => {
        const report = readProcesses({
            self_pid: 42,
            self_rss_mb: 120.5,
            self_user_cpu_s: 1.2,
            self_sys_cpu_s: 0.3,
            processes: [
                { pid: 42, cpu: 0.5, rss_mb: 120.5, elapsed: '01:02:03', command: 'codebase-memory-mcp', is_self: true },
                { pid: 43, cpu: 12.0, rss_mb: 900.0, elapsed: '00:00:10', command: 'codebase-memory-mcp', is_self: false },
            ],
        });
        expect(report.selfPid).toBe(42);
        expect(report.selfRssMb).toBe(120.5);
        expect(report.processes.map((entry) => [entry.pid, entry.isSelf])).toEqual([[42, true], [43, false]]);
    });

    it('reads an empty or malformed report as no processes', () => {
        expect(readProcesses({}).processes).toEqual([]);
        expect(readProcesses('x').selfPid).toBe(-1);
    });
});

describe('projectNameFor and projectHref', () => {
    it('suggests the last path segment as the project name', () => {
        expect(projectNameFor('/Users/x/repo')).toBe('repo');
        expect(projectNameFor('/Users/x/repo/')).toBe('repo');
        expect(projectNameFor('C:\\code\\thing\\')).toBe('thing');
    });

    it('suggests nothing for an empty path or a bare root', () => {
        expect(projectNameFor('')).toBe('');
        expect(projectNameFor('/')).toBe('');
    });

    it('addresses a project through the query the page reads at start', () => {
        expect(projectHref('atlas sample')).toBe('?project=atlas%20sample');
    });
});
