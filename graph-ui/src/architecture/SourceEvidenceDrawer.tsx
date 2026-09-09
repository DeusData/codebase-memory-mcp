import { useEffect, useRef, useState } from 'react';
import type { RpcIntelligenceClient } from '../provider/rpc-client';
import type { CodeSnippetResult } from '../provider/rpc-schemas';
import type { GraphData } from '../galaxy/types';
import { COLUMNS, fileNodeForPath, moduleForFile } from '../provider/cypher';
import './source-evidence.css';

export interface SourceEvidenceTarget { filePath: string; line?: number; name?: string; kind?: 'callsite' | 'declaration' | 'file' }
const PAGE_LINES = 160;
const validLine = (value: number | undefined): value is number => value !== undefined && Number.isSafeInteger(value) && value > 0;

export function fileQualifiedName(graph: GraphData | undefined, path: string): string | undefined {
    // File nodes may carry 0..0 and only return the first source fragment.
    // Module nodes own the whole-file range used by the source paging API.
    return graph?.nodes.find(node => node.file_path === path && node.label === 'Module' && node.qualified_name)?.qualified_name
        ?? graph?.nodes.find(node => node.file_path === path && node.label === 'File' && node.qualified_name)?.qualified_name;
}

/** Source stays next to its referring map/analysis. No external resources are loaded. */
export default function SourceEvidenceDrawer({ project, target, graph, client, onClose, onExplore }: {
    project: string; target: SourceEvidenceTarget; graph?: GraphData;
    client: Pick<RpcIntelligenceClient, 'getCodeSnippet' | 'queryRows'>; onClose: () => void;
    onExplore: (target: SourceEvidenceTarget) => void;
}) {
    const qualifiedName = fileQualifiedName(graph, target.filePath);
    const requestedLine = validLine(target.line) ? target.line : undefined;
    const initialStart = Math.max(1, (requestedLine ?? 1) - 8);
    const targetKey = JSON.stringify([project, target.filePath, requestedLine, qualifiedName]);
    const [pageWindow, setPageWindow] = useState<{ key: string; start: number }>();
    const start = pageWindow?.key === targetKey ? pageWindow.start : initialStart;
    const [refresh, setRefresh] = useState(0);
    const readKey = JSON.stringify([targetKey, start, refresh]);
    const activeKey = useRef(readKey); activeKey.current = readKey;
    const [reading, setReading] = useState<{ key: string; source?: CodeSnippetResult; error?: string }>();
    // A new target can render before its effect. Never relabel old file contents.
    const current = reading?.key === readKey ? reading : undefined;
    const [copiedKey, setCopiedKey] = useState('');
    const [copyError, setCopyError] = useState<{ key: string; message: string }>();
    const closeRef = useRef<HTMLButtonElement>(null);
    const selectedRef = useRef<HTMLSpanElement>(null);
    const sourceRef = useRef<HTMLPreElement>(null);
    const restoreFocus = useRef(true);
    useEffect(() => {
        const previous = document.activeElement;
        closeRef.current?.focus();
        return () => {
            if (restoreFocus.current && previous instanceof HTMLElement && previous.isConnected
                && !previous.closest('[hidden], [inert]')) previous.focus({ preventScroll: true });
        };
    }, []);
    useEffect(() => {
        let alive = true;
        const fail = (error: unknown) => {
            if (alive) setReading({ key: readKey, error: error instanceof Error ? error.message : String(error) });
        };
        if (!project || !target.filePath || /[\x00-\x1f\x7f]/.test(target.filePath)) {
            fail('Select a project and a valid repository file to inspect source evidence.');
            return;
        }
        void (async () => {
            let resolvedName = qualifiedName;
            if (!resolvedName) {
                // Evidence can originate in Explore or a bounded impact result,
                // before the user has ever loaded the architecture snapshot.
                const modules = await client.queryRows(project, moduleForFile(target.filePath));
                if (!alive) return;
                resolvedName = modules[0]?.[COLUMNS.moduleForFile[0]] || undefined;
                if (!resolvedName) {
                    const files = await client.queryRows(project, fileNodeForPath(target.filePath));
                    if (!alive) return;
                    resolvedName = files[0]?.[COLUMNS.fileNode[0]] || undefined;
                }
            }
            if (!resolvedName) throw new Error('No indexed Module or File declaration was found for this project and path. Check index diagnostics or choose another source location.');
            const source = await client.getCodeSnippet(project, resolvedName, { startLine: start, maxLines: PAGE_LINES });
            if (!alive) return;
            if (source.qualified_name && source.qualified_name !== resolvedName) throw new Error('The source reply identifies a different indexed file. Refresh the map before following this evidence.');
            const returnedPath = source.file_path?.replace(/\\/g, '/');
            if (returnedPath && returnedPath !== target.filePath && !returnedPath.endsWith(`/${target.filePath}`))
                throw new Error('The source reply identifies a different file path.');
            if (!validLine(source.start_line) || !validLine(source.end_line) || source.end_line < source.start_line)
                throw new Error('The source reply has no usable line range. No source location can be verified.');
            if (source.source_mode && source.source_mode !== 'full') throw new Error('The daemon returned an outline instead of source. Open Explore to inspect the available file information.');
            if (source.source.trim() === '(source not available)') throw new Error('The daemon could not read the current local source.');
            setReading({ key: readKey, source });
        })().catch(fail);
        return () => { alive = false; };
    }, [project, qualifiedName, start, client, readKey]);
    const source = current?.source;
    const lines = source ? source.source.split('\n') : [];
    // A complete Module range may include its empty EOF line. A clipped page
    // instead ends in a separator after its last reported line. The explicit
    // source range distinguishes these cases; never accept other count gaps.
    const reportedLines = source && validLine(source.start_line) && validLine(source.end_line)
        ? source.end_line - source.start_line + 1 : undefined;
    if (lines.at(-1) === '' && lines.length - 1 === reportedLines) lines.pop();
    const firstLine = source?.start_line;
    const lastLine = validLine(firstLine) ? firstLine + lines.length - 1 : undefined;
    const rangeMatches = source && lastLine === source.end_line;
    const more = source?.source_truncated === true || source?.source_clipped === true;
    const next = source?.next_start_line;
    const canNext = Boolean(more && validLine(next) && validLine(lastLine) && next === lastLine + 1 && rangeMatches);
    const locationVisible = requestedLine !== undefined && validLine(firstLine) && validLine(lastLine)
        && requestedLine >= firstLine && requestedLine <= lastLine && rangeMatches;
    useEffect(() => {
        const panel = sourceRef.current, selected = selectedRef.current;
        if (!panel) return;
        panel.scrollTop = 0;
        if (selected) {
            // Scroll only this code pane; preserve the referring map's viewport.
            panel.scrollTop = Math.max(0, selected.getBoundingClientRect().top - panel.getBoundingClientRect().top - panel.clientHeight / 2);
        }
    }, [source]);
    const moveTo = (line: number) => setPageWindow({ key: targetKey, start: line });
    const copy = async () => {
        try {
            await navigator.clipboard.writeText(`${target.filePath}:${requestedLine ?? firstLine ?? 1}`);
            if (activeKey.current === readKey) { setCopiedKey(readKey); setCopyError(undefined); }
        } catch {
            if (activeKey.current === readKey) setCopyError({ key: readKey, message: 'Copy unavailable. Select the location above to copy it manually.' });
        }
    };
    return <aside className="source-evidence-drawer" role="dialog" aria-modal="false" aria-label="Source evidence" onKeyDown={event => {
        if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); onClose(); }
    }}>
        <header><div><span>Source evidence</span><h2>{target.name ?? target.filePath.split('/').at(-1)}</h2></div>
            <button ref={closeRef} onClick={onClose} aria-label="Close source evidence">Close</button></header>
        <p className="source-evidence-location">{target.filePath}{requestedLine ? `:${requestedLine}` : ''}</p>
        <p className="source-evidence-note">{target.kind === 'callsite' ? 'Indexed call site' : target.kind === 'declaration' ? 'Indexed declaration' : 'Requested source location'}.
            {' '}Current local source may have changed since indexing. The map stays open.</p>
        <div className="source-evidence-actions"><button onClick={() => void copy()}>{copiedKey === readKey ? 'Copied location' : 'Copy location'}</button>
            <button onClick={() => setRefresh(value => value + 1)}>Refresh source</button>
            <button onClick={() => { restoreFocus.current = false; onExplore(target); }}>Open in Explore</button></div>
        {copyError?.key === readKey && <p role="status">{copyError.message}</p>}
        {current?.error ? <p role="alert">{current.error}</p> : !source ? <p role="status">Reading local source…</p> : <>
            {!rangeMatches && <p role="alert">Source text does not match the reported line range. Line numbers and highlighting are unavailable.</p>}
            {requestedLine && !locationVisible && rangeMatches && <p role="status" className="source-evidence-notice">Requested line {requestedLine} is outside this returned window.
                {start === initialStart ? ' The index or requested source window may be stale.' : <button onClick={() => moveTo(initialStart)}>Return to requested location</button>}</p>}
            <pre ref={sourceRef} tabIndex={0} aria-label="Source lines" className="source-evidence-lines"><code>{lines.map((line, index) => {
                const number = (firstLine ?? 1) + index;
                const selected = rangeMatches && number === requestedLine;
                return <span key={number} data-selected={selected} ref={selected ? selectedRef : undefined}>
                    <span className="source-evidence-line-number" aria-hidden="true">{rangeMatches ? number : ''}</span>{line || ' '}<br /></span>;
            })}</code></pre>
            {lines.length === 0 && <p role="status">No source text was returned.</p>}
            {more && !canNext && <p role="status">The source is truncated, but no valid next page was supplied. Refresh or open Explore to inspect the available source.</p>}
            <nav aria-label="Source pages"><button disabled={!rangeMatches || !firstLine || firstLine <= 1} onClick={() => moveTo(Math.max(1, firstLine! - PAGE_LINES))}>Previous lines</button>
                <span>{rangeMatches ? `Lines ${firstLine} to ${lastLine}` : 'Unverified line range'}{source.original_end_line ? ` · reported end ${source.original_end_line}` : ''}</span>
                <button disabled={!canNext} onClick={() => moveTo(next!)}>Next lines</button></nav>
        </>}
    </aside>;
}
