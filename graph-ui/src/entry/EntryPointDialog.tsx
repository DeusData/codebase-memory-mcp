/**
 * Pick the place to start from: the ways in the index flagged, or any symbol.
 *
 * Two lists in one overlay, and the split is the point. The upper list is what
 * the analysis flagged as a way into this program, which is the answer for
 * somebody who wants to be shown the front doors. The field under it is the same
 * meaning search the command line runs, which is the answer for somebody who
 * already knows the name of the function they care about and does not care
 * whether anybody flagged it.
 *
 * It reuses the search rather than growing one: `findByMeaning` is called by
 * App.tsx for both, and the rows are built by the same `searchRows` the command
 * line uses. A second search would rank differently the first time somebody
 * improved one of them.
 *
 * Purely presentational, like the other overlays here.
 */

import type { JSX, KeyboardEvent } from 'react';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import type { SearchRow } from '../search/overlay-model';
import type { EntryRow } from './entry-model';

/** What the dialog is doing while the reader types. */
export type EntrySearchStatus = 'idle' | 'searching' | 'ready' | 'failed';

export interface EntryPointDialogProps {
    /** What the list holds and what it is not showing. */
    headline: string;
    rows: readonly EntryRow[];
    /** The search field's value and what typing in it produced. */
    query: string;
    onQueryChange: (value: string) => void;
    hits: readonly SearchRow[];
    status: EntrySearchStatus;
    /** An honest extra line: the failure, or what is missing. */
    message: string;
    /** Why the list holds no route, when it holds none. Empty otherwise. */
    routeNote: string;
    onChooseFlagged: (key: string) => void;
    onChooseHit: (index: number) => void;
    onClose: () => void;
    onKeyDown?: (event: KeyboardEvent<HTMLDivElement>) => void;
}

/** What the field says it does, so nobody expects it to run a command. */
export const ENTRY_SEARCH_PLACEHOLDER = messages.entry.searchPlaceholder;

/** The dialog's own headline. Neutral, and it names what happens next. */
export const ENTRY_TITLE = messages.entry.title;

/** What the dialog says under its title. */
export const ENTRY_SUBLINE = messages.entry.subline;

export default function EntryPointDialog(props: EntryPointDialogProps): JSX.Element {
    return (
        <div
            className="atlas-entry"
            data-testid="atlas-entry"
            role="dialog"
            aria-label={ENTRY_TITLE}
            onKeyDown={props.onKeyDown}
        >
            <div className="atlas-entry-inner">
                <header className="atlas-entry-head">
                    <h2 className="atlas-entry-title">{ENTRY_TITLE}</h2>
                    <button
                        type="button"
                        className="atlas-entry-close"
                        data-testid="atlas-entry-close"
                        aria-label={messages.entry.closeLabel}
                        onClick={props.onClose}
                    >
                        {messages.entry.close}
                    </button>
                </header>
                <p className="atlas-entry-subline">{ENTRY_SUBLINE}</p>

                <div className="atlas-entry-search">
                    <span className="atlas-entry-prompt">{'>'}</span>
                    <input
                        className="atlas-entry-input"
                        data-testid="atlas-entry-input"
                        aria-label={messages.entry.searchLabel}
                        placeholder={ENTRY_SEARCH_PLACEHOLDER}
                        value={props.query}
                        autoComplete="off"
                        onChange={(event) => props.onQueryChange(event.target.value)}
                    />
                    <span className="atlas-entry-status" data-status={props.status}>
                        {props.status === 'searching' ? messages.entry.searching : ''}
                    </span>
                </div>

                {props.hits.length > 0 && (
                    <div className="atlas-entry-list" data-testid="atlas-entry-hits" role="listbox">
                        {props.hits.map((row, index) => (
                            <button
                                key={row.key}
                                type="button"
                                className="atlas-entry-row"
                                data-testid="atlas-entry-hit"
                                data-name={row.name}
                                role="option"
                                aria-selected="false"
                                onMouseDown={(event) => {
                                    event.preventDefault();
                                    props.onChooseHit(index);
                                }}
                                onKeyDown={(event) => {
                                    if (event.key === 'Enter' || event.key === ' ') {
                                        event.preventDefault();
                                        props.onChooseHit(index);
                                    }
                                }}
                            >
                                <span className="atlas-entry-name">{row.name}</span>
                                <span className="atlas-entry-where">{row.path}</span>
                                <span className="atlas-entry-origin">{row.matched}</span>
                            </button>
                        ))}
                    </div>
                )}

                <div className="atlas-entry-headline" data-testid="atlas-entry-headline">
                    {props.headline}
                </div>

                <div className="atlas-entry-list" data-testid="atlas-entry-flagged" role="listbox">
                    {props.rows.map((row) => (
                        <Hint
                            key={row.key}
                            name="entry-row"
                            text={
                                row.target === undefined
                                    ? messages.entry.notOpenable
                                    : messages.entry.startWalkAt(row.name)
                            }
                        >
                            <button
                                type="button"
                                className="atlas-entry-row"
                                data-testid="atlas-entry-row"
                                data-name={row.name}
                                data-openable={row.target !== undefined}
                                role="option"
                                aria-selected="false"
                                disabled={row.target === undefined}
                                onMouseDown={(event) => {
                                    event.preventDefault();
                                    props.onChooseFlagged(row.key);
                                }}
                                onKeyDown={(event) => {
                                    if (event.key === 'Enter' || event.key === ' ') {
                                        event.preventDefault();
                                        props.onChooseFlagged(row.key);
                                    }
                                }}
                            >
                                <span className="atlas-entry-name">{row.name}</span>
                                <span className="atlas-entry-where">{row.where}</span>
                                <span className="atlas-entry-origin">{row.origin}</span>
                            </button>
                        </Hint>
                    ))}
                </div>

                {props.routeNote.length > 0 && (
                    <p className="atlas-entry-note" data-testid="atlas-entry-route-note">
                        {props.routeNote}
                    </p>
                )}

                {props.message.length > 0 && (
                    <p className="atlas-entry-message" data-testid="atlas-entry-message" data-status={props.status}>
                        {props.message}
                    </p>
                )}
            </div>
        </div>
    );
}
