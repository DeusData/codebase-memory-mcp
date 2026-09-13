import { useEffect, useState } from 'react';
import { callToolJson } from '../provider/rpc-transport';
import { readCodeSnippet, type CodeSnippetResult } from '../provider/rpc-schemas';
import type { SystemPathEdge, SystemSymbol } from './system-architecture-source';

/** A small, current-source window. It never turns nearby syntax into an inferred guard. */
export default function BehaviorSourceEvidence({ project, generation, symbol, call, active, onNavigate }: {
    project: string; generation?: string; symbol: SystemSymbol; call?: SystemPathEdge; active: boolean;
    onNavigate: (path: string, line?: number, name?: string) => void;
}) {
    const line = call?.callsite?.line ?? symbol.start_line;
    const file = call?.callsite?.file_path ?? symbol.file_path;
    const key = JSON.stringify([project, generation, symbol.qualified_name, file, line]);
    const [reading, setReading] = useState<{ key: string; source?: CodeSnippetResult; error?: string }>();
    const current = reading?.key === key ? reading : undefined;
    useEffect(() => {
        if (!active || !file || !symbol.qualified_name) return;
        const controller = new AbortController();
        setReading({ key });
        void (async () => {
            try {
                const source = readCodeSnippet(await callToolJson('get_code_snippet', {
                    project, qualified_name: symbol.qualified_name, source_mode: 'full', format: 'json',
                    start_line: Math.max(1, (line ?? 1) - 8), max_lines: 32,
                }, { signal: controller.signal }));
                if (controller.signal.aborted) return;
                const path = source.file_path?.replaceAll('\\', '/');
                if (source.qualified_name && source.qualified_name !== symbol.qualified_name)
                    throw new Error('Source identity changed. Refresh the analysis before following this call.');
                if (!path || (path !== file && !path.endsWith(`/${file}`)))
                    throw new Error('The returned source does not match this call location.');
                if (source.source_mode && source.source_mode !== 'full') throw new Error('Full source is unavailable for this operation.');
                const lines = source.source.split('\n');
                const count = source.end_line !== undefined && source.start_line !== undefined ? source.end_line - source.start_line + 1 : 0;
                if (lines.at(-1) === '' && lines.length - 1 === count) lines.pop();
                if (!source.start_line || !source.end_line || count < 1 || lines.length !== count)
                    throw new Error('Source line range could not be verified. Open the definition to inspect it.');
                if (line && (line < source.start_line || line > source.end_line))
                    throw new Error('The indexed location is outside the current source window. The file may have changed.');
                setReading({ key, source: { ...source, source: lines.join('\n') } });
            } catch (error) {
                if (!controller.signal.aborted) setReading({ key, error: error instanceof Error ? error.message : 'Could not read this source.' });
            }
        })();
        return () => controller.abort();
    }, [project, generation, symbol.qualified_name, file, line, key, active]);
    return <section className="behavior-source" aria-label="Behavior source evidence">
        <div className="behavior-source-heading"><h4>{call?.callsite ? 'Code at this call' : 'Operation source'}</h4>
            {file && <button onClick={() => onNavigate(file, line, symbol.name)}>Open source ↗</button>}</div>
        {!file ? <p>Source location unavailable.</p> : <>
            <p className="behavior-source-location">{file}{line ? `:${line}` : ''}</p>
            {current?.error ? <p role="status">{current.error}</p> : !current?.source ? <p role="status">{active ? 'Reading source…' : 'Source reading paused.'}</p>
                : <pre aria-label="Code at selected operation"><code>{current.source.source.split('\n').map((text, index) => {
                    const number = current.source!.start_line! + index;
                    return <span key={number} data-selected={number === line}><i aria-hidden="true">{number}</i>{text || ' '}{'\n'}</span>;
                })}</code></pre>}
            <small>Current local source · may differ from the indexed snapshot.</small>
        </>}
    </section>;
}
