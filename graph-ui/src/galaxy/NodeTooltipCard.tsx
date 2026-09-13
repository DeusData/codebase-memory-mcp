/**
 * Die Hover-Karte der Galaxie, in den Tokens dieses Projekts.
 *
 * Sie ersetzt NodeTooltip.tsx aus der Uebernahme, und das ist der einzige
 * Grund, warum sie existiert: die Karte des Originals ist mit Tailwind-Klassen
 * gebaut, deren Farben aus einem `@theme`-Block kommen, den dieses Projekt
 * nicht hat. Sie mitzunehmen hiesse, Tailwind fuer eine einzige Karte
 * einzufuehren und dann eine zweite Farbtabelle neben src/styles/tokens.css
 * zu fuehren.
 *
 * Gezeigt wird, was der Layout-Datensatz ohnehin schon traegt. Es wird beim
 * Hovern nichts nachgeladen, so wie im Original auch nicht: eine Karte, die
 * beim Darueberfahren eine Anfrage stellt, ruckelt und erzaehlt beim Ruckeln
 * nichts. Was der Datensatz nicht traegt, steht nicht da; es gibt hier keine
 * Zeile, die mangels Zahl eine Null zeigt.
 */

import type { JSX } from 'react';
import { Html } from '@react-three/drei';
import type { GraphNode } from './types';

/** Der Zeilenbereich, wenn das Layout einen fuehrt. */
export function lineRangeOf(node: GraphNode): string {
    if (node.start_line === undefined || node.start_line <= 0) {
        return '';
    }
    if (node.end_line !== undefined && node.end_line !== node.start_line) {
        return `L${node.start_line}-${node.end_line}`;
    }
    return `L${node.start_line}`;
}

/** Die Kennzahlen, die der Datensatz traegt. Fehlende Zahlen fehlen. */
export function tooltipRows(node: GraphNode): [string, string][] {
    const rows: [string, string][] = [];
    if (node.in_calls !== undefined) {
        rows.push(['fan-in', String(node.in_calls)]);
    }
    if (node.out_calls !== undefined) {
        rows.push(['fan-out', String(node.out_calls)]);
    }
    if (
        node.start_line !== undefined &&
        node.end_line !== undefined &&
        node.end_line >= node.start_line
    ) {
        rows.push(['lines', String(node.end_line - node.start_line + 1)]);
    }
    return rows;
}

/** Was ein Klick auf diesen Knoten tun wird, als Satz. */
export function tooltipAction(node: GraphNode): string {
    return node.file_path === undefined || node.file_path.length === 0
        ? 'no file in the index: nothing to open'
        : 'click to open the file and follow the twin';
}

export function NodeTooltipCard({ node }: { node: GraphNode }): JSX.Element {
    const rows = tooltipRows(node);
    const lines = lineRangeOf(node);
    return (
        <Html
            position={[node.x, node.y + node.size * 0.7, node.z]}
            center
            style={{ pointerEvents: 'none' }}
        >
            <div className="atlas-galaxy-card" data-testid="atlas-galaxy-card">
                <div className="atlas-galaxy-card-head">
                    <span className="atlas-galaxy-card-dot" style={{ backgroundColor: node.color }} />
                    <span className="atlas-galaxy-card-name">{node.name}</span>
                    <span className="atlas-galaxy-card-label">{node.label}</span>
                </div>
                {node.file_path !== undefined && node.file_path.length > 0 && (
                    <p className="atlas-galaxy-card-path">
                        {node.file_path}
                        {lines.length > 0 && <span className="atlas-galaxy-card-lines"> {lines}</span>}
                    </p>
                )}
                {rows.length > 0 && (
                    <dl className="atlas-galaxy-card-rows">
                        {rows.map(([label, value]) => (
                            <div key={label} className="atlas-galaxy-card-row">
                                <dt>{label}</dt>
                                <dd>{value}</dd>
                            </div>
                        ))}
                    </dl>
                )}
                {node.status !== undefined && node.status !== 'structural' && (
                    <p className="atlas-galaxy-card-status">{node.status}</p>
                )}
                <p className="atlas-galaxy-card-action">{tooltipAction(node)}</p>
            </div>
        </Html>
    );
}

export default NodeTooltipCard;
