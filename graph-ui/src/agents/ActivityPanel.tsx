import { useMemo, useState } from 'react';
import type { JSX } from 'react';
import type { AgentEvent } from './agent-event';
import { eventKey, workKindOf } from './agent-event';
import type { AgentsState } from './agent-store';
import type { AgentSourceStatus } from './agent-source';
import type { GraphData, GraphNode } from '../galaxy/types';
import { buildPlacementIndex, placeEvent } from './agent-placement';
import { activityRows } from './activity-model';
import type { ActivityFilter } from './activity-model';
import { activityStrings as s } from './activity-strings';
import './activity.css';

interface Props {
    state: AgentsState; status: AgentSourceStatus; on: boolean; port: number;
    graph?: GraphData; onToggle: () => void; onOpenNode: (node: GraphNode) => void;
}
const emptyFilter: ActivityFilter = { agent: '', run: '', kind: '', query: '' };
const rowKey = (row: AgentEvent): string => `${row.agent}:${eventKey(row)}`;

export default function ActivityPanel(props: Props): JSX.Element {
    const [filter, setFilter] = useState<ActivityFilter>(emptyFilter);
    const [selected, setSelected] = useState<string>();
    const rows = useMemo(() => activityRows(props.state, filter), [props.state, filter]);
    const all = useMemo(() => activityRows(props.state, emptyFilter), [props.state]);
    const index = useMemo(() => buildPlacementIndex(props.graph?.nodes ?? []), [props.graph]);
    const entry = all.find((row) => rowKey(row) === selected);
    const placement = entry === undefined ? undefined : placeEvent(entry, workKindOf(entry.tool, entry.detail), index);
    const target = placement?.nodeId === undefined ? undefined : props.graph?.nodes.find((node) => node.id === placement.nodeId);
    const agents = [...new Set(all.map((row) => row.agent))].sort();
    const runs = [...new Set(all.filter((row) => !filter.agent || row.agent === filter.agent).map((row) => row.run))].sort();
    const set = (patch: Partial<ActivityFilter>): void => setFilter((value) => ({ ...value, ...patch }));
    return <section className="cbm-activity" aria-label={s.title}>
        <header className="cbm-page-head">
            <div><p className="cbm-eyebrow">{s.live}</p><h2>{s.title}</h2><p>{s.subtitle}</p></div>
            <div className="cbm-activity-connection"><span data-state={props.status.state}>{props.status.state}</span>
                <button type="button" onClick={props.onToggle}>{props.on ? s.disconnect : s.connect}</button></div>
        </header>
        <div className="cbm-activity-metrics">
            {[[s.events, props.state.actors.filter((actor) => !actor.you).reduce((sum, actor) => sum + actor.count, 0)], [s.actors, props.state.actors.filter((actor) => !actor.you).length],
                [s.gaps, props.state.missed], [s.dropped, props.status.drops]].map(([label, value]) =>
                <div key={label}><strong>{value}</strong><span>{label}</span></div>)}
        </div>
        {!props.on && <p className="cbm-activity-notice">{all.length > 0 ? s.disconnected : s.noSource} <code>{`127.0.0.1:${props.port}`}</code></p>}
        {props.status.error && <p role="status" className="cbm-activity-notice">{props.status.error}</p>}
        {props.state.unreadable > 0 && <p role="status" className="cbm-activity-notice">{s.unreadable}: {props.state.unreadable}</p>}
        <div className="cbm-activity-filters">
            <input aria-label={s.search} placeholder={s.placeholder} value={filter.query} onChange={(event) => set({ query: event.target.value })} />
            <select aria-label={s.agent} value={filter.agent} onChange={(event) => set({ agent: event.target.value, run: '' })}>
                <option value="">{s.allAgents}</option>{agents.map((value) => <option key={value}>{value}</option>)}
            </select>
            <select aria-label={s.run} value={filter.run} onChange={(event) => set({ run: event.target.value })}>
                <option value="">{s.allRuns}</option>{runs.map((value) => <option key={value}>{value}</option>)}
            </select>
            <select aria-label={s.kind} value={filter.kind} onChange={(event) => set({ kind: event.target.value as ActivityFilter['kind'] })}>
                <option value="">{s.allKinds}</option>{s.kinds.map((value) => <option key={value}>{value}</option>)}
            </select>
            <button type="button" onClick={() => setFilter(emptyFilter)}>{s.clear}</button>
        </div>
        <div className="cbm-activity-body">
            <div className="cbm-activity-list" role="list" aria-label={s.title}>
                {rows.length === 0 && <div className="cbm-activity-empty">{all.length === 0 ? s.noEvents : s.noMatch}</div>}
                {rows.map((row) => {
                    const kind = workKindOf(row.tool, row.detail);
                    const mapped = placeEvent(row, kind, index);
                    return <button type="button" key={rowKey(row)} className="cbm-activity-row"
                        aria-pressed={rowKey(row) === selected} onClick={() => setSelected(rowKey(row))}>
                        <span className="cbm-activity-dot" data-kind={kind} />
                        <span><strong>{row.agent}</strong><span className="cbm-activity-action">{row.tool}</span>
                            <code>{row.path || row.detail || s.unknown}</code>
                            <small>{row.run} · {row.phase} · {row.replay ? s.replay : s.live}{mapped.kind === 'none' ? ` · ${s.unmapped}` : ''}</small></span>
                        <time dateTime={Number.isFinite(new Date(row.ts).getTime()) ? new Date(row.ts).toISOString() : undefined}>{new Date(row.ts).toLocaleTimeString()}</time>
                    </button>;
                })}
            </div>
            <aside className="cbm-activity-inspector"><h3>{s.evidence}</h3>
                {entry === undefined ? <p>{s.select}</p> : <>
                    <h4>{entry.tool}</h4><p>{entry.agent} · {entry.run}</p>
                    <dl>{[[s.timestamp, new Date(entry.ts).toLocaleString()], [s.phase, entry.phase],
                        [s.source, entry.source || s.unknown], [s.mapping, placement?.kind === 'none' ? s.unmapped : placement?.kind ?? s.unknown]]
                        .map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{value}</dd></div>)}</dl>
                    {entry.path && <code>{entry.path}{entry.lines ? `:${entry.lines.join('-')}` : ''}</code>}
                    {placement?.why && <p>{placement.kind === 'none' && entry.path ? s.noGraphMatch : placement.why}</p>}
                    {target !== undefined && <button type="button" onClick={() => props.onOpenNode(target)}>{s.open}</button>}
                    <h4>{s.details}</h4><pre>{entry.detail || s.unknown}</pre>
                    <p className="cbm-activity-note">{s.outcome}</p>
                </>}
            </aside>
        </div>
        <footer><p>{s.retained}</p><p className="atlas-guidance-note">{s.limited}</p></footer>
    </section>;
}
