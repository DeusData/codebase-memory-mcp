/**
 * Das Suchfenster ueber der Kommandozeile.
 *
 * Es liegt ueber der Fusszeile und nicht in der Mitte des Bildschirms, weil es
 * zu der Zeile gehoert, in die getippt wird: ein Dialog in der Mitte nimmt dem
 * Leser die Datei aus dem Blick, waehrend er noch sucht, wozu sie gehoert.
 *
 * Rein darstellend. Was gefunden wurde, welche Zeile ausgewaehlt ist und was
 * ein Enter bedeutet, entscheidet die App; diese Datei zeichnet es. Der Grund
 * ist derselbe wie beim Chrome: so laesst sich das Fenster in jsdom pruefen,
 * ohne dass ein Server antworten muss.
 */

import type { JSX } from 'react';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import type { SearchRow } from './overlay-model';

/** Der Zustand, in dem das Fenster steht. */
export type SearchOverlayStatus = 'searching' | 'ready' | 'failed';

export interface SearchOverlayProps {
    headline: string;
    rows: SearchRow[];
    /** Index der ausgewaehlten Zeile. */
    selected: number;
    status: SearchOverlayStatus;
    /** Ehrliche Zusatzzeile: der Fehler, oder was gerade laeuft. */
    message: string;
    onChoose: (index: number) => void;
    onPoint: (index: number) => void;
}

export default function SearchOverlay(props: SearchOverlayProps): JSX.Element {
    return (
        <div
            className="atlas-search-results"
            data-testid="atlas-search-results"
            data-status={props.status}
            role="listbox"
            aria-label={messages.search.resultsLabel}
        >
            <div className="atlas-search-headline" data-testid="atlas-search-headline">
                {props.headline}
            </div>
            {/*
              * Die Zeilen stehen in einem Kasten fester Hoehe, und das ist die
              * Antwort auf die zweite Haelfte des Nutzerbefundes vom
              * 2026-08-29.
              *
              * Das Fenster haengt ueber der Kommandozeile und waechst nach
              * oben. Eine Liste, die dort waechst, schiebt die oberste Zeile
              * weiter nach oben, sooft eine Zeile dazukommt: erst die
              * vorlaeufigen Vorschlaege, dann die Antwort des Index, und beim
              * Weitertippen wieder. Der beste Treffer, also genau die Zeile, auf
              * die der Leser sieht, wandert dabei unter seinem Blick weg.
              *
              * Mit fester Hoehe steht die erste Zeile still: die vorlaeufigen
              * Zeilen werden an Ort und Stelle durch die Antwort ersetzt. Der
              * Preis ist ein Rest freier Flaeche, wenn wenige Treffer dastehen.
              * Das ist der billigere von beiden: ein leerer Streifen sagt
              * nichts Falsches, eine springende Liste macht das Zeigen zum
              * Glueckspiel.
              */}
            {props.rows.length > 0 && (
            <div className="atlas-search-list" data-testid="atlas-search-list">
                {props.rows.map((row, index) => (
                    <button
                        key={row.key}
                        type="button"
                        className="atlas-search-row"
                        data-testid="atlas-search-row"
                        data-selected={index === props.selected}
                        data-name={row.name}
                        data-source={row.source}
                        role="option"
                        aria-selected={index === props.selected}
                        onMouseDown={(event) => {
                            // Vor dem Blur der Kommandozeile: ein Klick, der erst
                            // nach dem Fokusverlust wirkt, trifft ein Fenster, das
                            // dann schon zu waere.
                            event.preventDefault();
                            props.onChoose(index);
                        }}
                        onMouseEnter={() => props.onPoint(index)}
                        onKeyDown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault();
                                props.onChoose(index);
                            }
                        }}
                    >
                        <span className="atlas-search-name">{row.name}</span>
                        <span className="atlas-search-path">{row.path}</span>
                        {row.line.length > 0 && <span className="atlas-search-line">{row.line}</span>}
                        {row.source === 'loaded' && (
                            <Hint name="search-provisional" text={messages.search.provisionalMarkTitle}>
                                <span
                                    className="atlas-search-provisional"
                                    data-testid="atlas-search-provisional"
                                >
                                    {messages.search.provisionalMark}
                                </span>
                            </Hint>
                        )}
                        <span className="atlas-search-matched">{row.matched}</span>
                    </button>
                ))}
            </div>
            )}
            {props.message.length > 0 && (
                <p className="atlas-search-message" data-testid="atlas-search-message" data-status={props.status}>
                    {props.message}
                </p>
            )}
        </div>
    );
}
