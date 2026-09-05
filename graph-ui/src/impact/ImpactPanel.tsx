/**
 * What a change would reach, and the word that says how much of it matters.
 *
 * The body is the reference project's impact view in the order it draws it
 * (CodeAtlasIDE, codeatlas-views/src/browser/impact-widget.tsx): a toolbar with
 * the comparison point, then the tiles, then the badge and its reasoning, then
 * the narrative with one evidence row per claim, then the four lists. What is
 * not here is the review mode and the staleness banner, both of which need a
 * reviewed baseline and a file watcher this surface does not have; a control
 * that led to a sentence about why it cannot work would be worse than no
 * control.
 *
 * Two decisions are this file's own.
 *
 * **A ref is refused here, before anything is asked.** The reference lets its
 * backend refuse, because that backend has a repository in front of it. This one
 * does not: `detect_changes` takes an unknown `since` without complaint and
 * answers about the working tree, so a typo would come back as a plausible
 * answer to a different question. So the format is checked in the browser
 * (`refRejection`), the sentence names the rule that was broken and says that
 * nothing was asked, and the request is simply not made. The proof run counts
 * the requests on the wire and reads that count as the claim.
 *
 * **The badge lists the rules that fired.** A word with a paragraph beside it is
 * still a word somebody has to take on trust. `badgeRules` turns the state that
 * produced it back into sentences, one per rule, and they sit under the badge.
 *
 * Purely presentational: what to read is App.tsx's business, what it means is
 * impact-model.ts's, and this file draws it.
 */

import type { JSX } from 'react';
import { messages } from '../i18n/messages';
import Hint from '../ui/tooltip/Hint';
import type { ImpactModel, ImpactTarget } from './impact-model';
import { badgeRules, endpointLabel } from './impact-model';
import {
    IMPACT_BADGE_TOOLTIP,
    IMPACT_BADGE_WHY_NONE,
    IMPACT_BADGE_WHY_TITLE,
    IMPACT_DIRECT_EMPTY,
    IMPACT_DIRECT_TITLE,
    IMPACT_DOWNSTREAM_EMPTY,
    IMPACT_DOWNSTREAM_TITLE,
    IMPACT_EMPTY_MESSAGE,
    IMPACT_ENDPOINTS_EMPTY,
    IMPACT_ENDPOINTS_TITLE,
    IMPACT_ENDPOINT_VIA_FILE,
    IMPACT_ENDPOINT_VIA_HANDLER,
    IMPACT_EVIDENCE_TITLE,
    IMPACT_LOADING,
    IMPACT_LOAD_FAILED,
    IMPACT_MODE_LABEL,
    IMPACT_MODE_SINCE_REF,
    IMPACT_MODE_SINCE_REF_TOOLTIP,
    IMPACT_MODE_WORKING_TREE,
    IMPACT_MODE_WORKING_TREE_TOOLTIP,
    IMPACT_REF_GO,
    IMPACT_REF_GO_TOOLTIP,
    IMPACT_REF_LABEL,
    IMPACT_REF_PLACEHOLDER,
    IMPACT_SUBLINE,
    IMPACT_TESTS_EMPTY,
    IMPACT_TESTS_TITLE,
    IMPACT_TITLE,
    RISK_LABELS,
    impactEvidenceValue,
    impactRiskTooltip,
} from './impact-strings';

/** Which comparison point the panel is showing. */
export type ImpactMode = 'worktree' | 'since-ref';

export type ImpactStatus = 'idle' | 'loading' | 'ready' | 'failed';

export interface ImpactPanelProps {
    project: string;
    mode: ImpactMode;
    onMode: (mode: ImpactMode) => void;
    /** The text in the ref field, held by the caller so a mode switch keeps it. */
    refDraft: string;
    onRefDraft: (value: string) => void;
    /** Asked for a reading against the ref in the field. Never called with a refused one. */
    onGo: () => void;
    /** Why the ref in the field was refused, in the words of the rule it broke. */
    refError: string;
    model?: ImpactModel;
    status: ImpactStatus;
    message: string;
    /** What the route scan opened, said out loud. Empty when nothing was scanned. */
    routeNote: string;
    onOpen: (target: ImpactTarget) => void;
    onClose: () => void;
}

function Tile(props: { label: string; value: number }): JSX.Element {
    return (
        <span className="atlas-impact-tile" data-testid="atlas-impact-tile" data-tile={props.label}>
            <b className="atlas-impact-tile-value">{props.value}</b>
            <span className="atlas-impact-tile-label">{props.label}</span>
        </span>
    );
}

function RowButton(props: {
    target: ImpactTarget;
    onOpen: (target: ImpactTarget) => void;
    children: JSX.Element | string;
}): JSX.Element {
    const openable = props.target.filePath !== undefined && props.target.filePath.length > 0;
    return (
        <Hint
            name="impact-open"
            text={
                openable
                    ? messages.impact.openRow(props.target.name)
                    : messages.impact.rowNotOpenable
            }
        >
            <button
                type="button"
                className="atlas-impact-link"
                data-testid="atlas-impact-open"
                data-name={props.target.name}
                data-openable={openable}
                disabled={!openable}
                onClick={() => props.onOpen(props.target)}
            >
                {props.children}
            </button>
        </Hint>
    );
}

function Section(props: {
    name: string;
    title: string;
    empty: string;
    count: number;
    children: JSX.Element | JSX.Element[];
}): JSX.Element {
    return (
        <section
            className="atlas-impact-section"
            data-testid={`atlas-impact-${props.name}`}
            data-count={props.count}
        >
            <span className="atlas-impact-eyebrow">{props.title}</span>
            {props.count === 0 ? (
                <p className="atlas-impact-empty" data-testid={`atlas-impact-${props.name}-empty`}>
                    {props.empty}
                </p>
            ) : (
                props.children
            )}
        </section>
    );
}

export default function ImpactPanel(props: ImpactPanelProps): JSX.Element {
    const model = props.model;
    const tiles = model?.summaryTiles;
    const rules = model === undefined ? [] : badgeRules(model);
    const testCount = (model?.tests.covering.length ?? 0) + (model?.tests.missing.length ?? 0);

    return (
        <div
            className="atlas-impact"
            data-testid="atlas-impact"
            role="dialog"
            aria-label={IMPACT_TITLE}
            data-mode={props.mode}
            data-status={props.status}
            data-badge={model?.risk ?? ''}
            data-tiles={tiles === undefined ? 0 : 5 + tiles.indirect.length}
            data-direct={model?.direct.length ?? 0}
            data-downstream={model?.downstream.reduce((sum, group) => sum + group.symbols.length, 0) ?? 0}
            data-endpoints={model?.endpoints.length ?? 0}
        >
            <div className="atlas-impact-inner">
                <header className="atlas-impact-head">
                    <h2 className="atlas-impact-title">{IMPACT_TITLE}</h2>
                    <button
                        type="button"
                        className="atlas-impact-close"
                        data-testid="atlas-impact-close"
                        aria-label={messages.impact.closeLabel}
                        onClick={props.onClose}
                    >
                        {messages.impact.close}
                    </button>
                </header>
                <p className="atlas-impact-subline">{IMPACT_SUBLINE}</p>

                <div className="atlas-impact-toolbar" data-testid="atlas-impact-toolbar">
                    <span className="atlas-impact-toolbar-label">{IMPACT_MODE_LABEL}</span>
                    <Hint name="impact-mode-worktree" text={IMPACT_MODE_WORKING_TREE_TOOLTIP}>
                        <button
                            type="button"
                            className="atlas-impact-mode"
                            data-testid="atlas-impact-mode-worktree"
                            data-active={props.mode === 'worktree'}
                            aria-pressed={props.mode === 'worktree'}
                            onClick={() => props.onMode('worktree')}
                        >
                            {IMPACT_MODE_WORKING_TREE}
                        </button>
                    </Hint>
                    <Hint name="impact-mode-since-ref" text={IMPACT_MODE_SINCE_REF_TOOLTIP}>
                        <button
                            type="button"
                            className="atlas-impact-mode"
                            data-testid="atlas-impact-mode-since-ref"
                            data-active={props.mode === 'since-ref'}
                            aria-pressed={props.mode === 'since-ref'}
                            onClick={() => props.onMode('since-ref')}
                        >
                            {IMPACT_MODE_SINCE_REF}
                        </button>
                    </Hint>
                    {props.mode === 'since-ref' && (
                        <form
                            className="atlas-impact-ref"
                            onSubmit={(event) => {
                                event.preventDefault();
                                props.onGo();
                            }}
                        >
                            <label className="atlas-impact-ref-label" htmlFor="atlas-impact-ref-input">
                                {IMPACT_REF_LABEL}
                            </label>
                            <input
                                id="atlas-impact-ref-input"
                                className="atlas-impact-ref-input"
                                data-testid="atlas-impact-ref-input"
                                type="text"
                                spellCheck={false}
                                autoComplete="off"
                                placeholder={IMPACT_REF_PLACEHOLDER}
                                value={props.refDraft}
                                onChange={(event) => props.onRefDraft(event.target.value)}
                            />
                            <Hint name="impact-ref-go" text={IMPACT_REF_GO_TOOLTIP}>
                                <button
                                    type="submit"
                                    className="atlas-impact-ref-go"
                                    data-testid="atlas-impact-ref-go"
                                >
                                    {IMPACT_REF_GO}
                                </button>
                            </Hint>
                        </form>
                    )}
                </div>

                {props.refError.length > 0 && (
                    <p className="atlas-impact-ref-error" data-testid="atlas-impact-ref-error" role="alert">
                        {props.refError}
                    </p>
                )}

                {props.project.length === 0 && (
                    <p className="atlas-impact-empty" data-testid="atlas-impact-no-project">
                        {IMPACT_EMPTY_MESSAGE}
                    </p>
                )}
                {props.status === 'loading' && (
                    <p className="atlas-impact-busy" data-testid="atlas-impact-loading">{IMPACT_LOADING}</p>
                )}
                {props.status === 'failed' && (
                    <p className="atlas-impact-alarm" data-testid="atlas-impact-failed">
                        {IMPACT_LOAD_FAILED}
                        {props.message.length > 0 ? `: ${props.message}` : ''}
                    </p>
                )}

                {model !== undefined && tiles !== undefined && (
                    <>
                        <div className="atlas-impact-tiles" data-testid="atlas-impact-tiles">
                            <Tile label={messages.impact.tileChangedFiles} value={tiles.changedFiles} />
                            <Tile label={messages.impact.tileDirectSymbols} value={tiles.directSymbols} />
                            {tiles.indirect.map((band) => (
                                <Tile
                                    key={`indirect-${band.distance}`}
                                    label={messages.impact.stepsOut(band.distance)}
                                    value={band.count}
                                />
                            ))}
                            <Tile label={messages.impact.tileEndpoints} value={tiles.endpoints} />
                            <Tile label={messages.impact.tileTestsAffected} value={tiles.testsAffected} />
                            <Tile label={messages.impact.tileUntestedAffected} value={tiles.untestedAffected} />
                        </div>

                        <div className="atlas-impact-badge-row">
                            <Hint name="impact-badge" text={IMPACT_BADGE_TOOLTIP}>
                                <span
                                    className="atlas-impact-badge"
                                    data-testid="atlas-impact-badge"
                                    data-level={model.risk}
                                >
                                    {RISK_LABELS[model.risk]}
                                </span>
                            </Hint>
                            <div className="atlas-impact-badge-why">
                                <span className="atlas-impact-eyebrow">{IMPACT_BADGE_WHY_TITLE}</span>
                                {rules.length === 0 ? (
                                    <p className="atlas-impact-rule" data-testid="atlas-impact-badge-rule">
                                        {IMPACT_BADGE_WHY_NONE}
                                    </p>
                                ) : (
                                    rules.map((rule, index) => (
                                        <p
                                            key={`rule-${index}`}
                                            className="atlas-impact-rule"
                                            data-testid="atlas-impact-badge-rule"
                                        >
                                            {rule}
                                        </p>
                                    ))
                                )}
                            </div>
                        </div>

                        <div className="atlas-impact-narrative" data-testid="atlas-impact-narrative">
                            <p className="atlas-impact-narrative-text">{model.narrative.text}</p>
                            <span className="atlas-impact-eyebrow">{IMPACT_EVIDENCE_TITLE}</span>
                            <ul className="atlas-impact-evidence">
                                {model.narrative.evidence.map((entry, index) => (
                                    <li
                                        key={`evidence-${index}`}
                                        className="atlas-impact-evidence-row"
                                        data-testid="atlas-impact-evidence"
                                        data-source={entry.source}
                                    >
                                        <span className="atlas-impact-evidence-claim">{entry.claim}</span>
                                        <span className="atlas-impact-evidence-value">
                                            {impactEvidenceValue(entry.source, entry.value)}
                                        </span>
                                    </li>
                                ))}
                            </ul>
                        </div>

                        {props.routeNote.length > 0 && (
                            <p className="atlas-impact-note" data-testid="atlas-impact-route-note">
                                {props.routeNote}
                            </p>
                        )}

                        <Section
                            name="direct"
                            title={IMPACT_DIRECT_TITLE}
                            empty={IMPACT_DIRECT_EMPTY}
                            count={model.direct.length}
                        >
                            <ul className="atlas-impact-list">
                                {model.direct.map((row, index) => (
                                    <li key={`direct-${index}`} className="atlas-impact-row" data-testid="atlas-impact-direct-row">
                                        <RowButton target={row} onOpen={props.onOpen}>
                                            {row.name}
                                        </RowButton>
                                        <span className="atlas-impact-where">{row.filePath ?? ''}</span>
                                        <Hint name="impact-risk" text={impactRiskTooltip(row.risk, row.reasons)}>
                                            <span className="atlas-impact-chip" data-level={row.risk}>
                                                {RISK_LABELS[row.risk]}
                                            </span>
                                        </Hint>
                                    </li>
                                ))}
                            </ul>
                        </Section>

                        <Section
                            name="downstream"
                            title={IMPACT_DOWNSTREAM_TITLE}
                            empty={IMPACT_DOWNSTREAM_EMPTY}
                            count={model.downstream.reduce((sum, group) => sum + group.symbols.length, 0)}
                        >
                            <>
                                {model.downstream.map((group) => (
                                    <div
                                        key={`band-${group.distance}`}
                                        className="atlas-impact-band"
                                        data-distance={group.distance}
                                    >
                                        <span className="atlas-impact-band-label">
                                            {messages.impact.stepsOut(group.distance)}
                                        </span>
                                        <ul className="atlas-impact-list">
                                            {group.symbols.map((row, index) => (
                                                <li
                                                    key={`down-${group.distance}-${index}`}
                                                    className="atlas-impact-row"
                                                    data-testid="atlas-impact-downstream-row"
                                                >
                                                    <RowButton target={row} onOpen={props.onOpen}>
                                                        {row.name}
                                                    </RowButton>
                                                    <span className="atlas-impact-where">{row.filePath ?? ''}</span>
                                                    <Hint
                                                        name="impact-risk"
                                                        text={impactRiskTooltip(row.risk, row.reasons)}
                                                    >
                                                        <span
                                                            className="atlas-impact-chip"
                                                            data-level={row.risk}
                                                        >
                                                            {RISK_LABELS[row.risk]}
                                                        </span>
                                                    </Hint>
                                                </li>
                                            ))}
                                        </ul>
                                    </div>
                                ))}
                            </>
                        </Section>

                        <Section
                            name="endpoints"
                            title={IMPACT_ENDPOINTS_TITLE}
                            empty={IMPACT_ENDPOINTS_EMPTY}
                            count={model.endpoints.length}
                        >
                            <ul className="atlas-impact-list">
                                {model.endpoints.map((endpoint, index) => (
                                    <li
                                        key={`endpoint-${index}`}
                                        className="atlas-impact-row"
                                        data-testid="atlas-impact-endpoint"
                                        data-endpoint={endpointLabel(endpoint)}
                                        data-via={endpoint.via}
                                    >
                                        <RowButton
                                            target={{ name: endpointLabel(endpoint), filePath: endpoint.filePath, line: endpoint.line }}
                                            onOpen={props.onOpen}
                                        >
                                            {endpointLabel(endpoint)}
                                        </RowButton>
                                        <span className="atlas-impact-where">{endpoint.filePath ?? ''}</span>
                                        <span className="atlas-impact-via">
                                            {endpoint.via === 'handler'
                                                ? IMPACT_ENDPOINT_VIA_HANDLER
                                                : IMPACT_ENDPOINT_VIA_FILE}
                                        </span>
                                    </li>
                                ))}
                            </ul>
                        </Section>

                        <Section
                            name="tests"
                            title={IMPACT_TESTS_TITLE}
                            empty={IMPACT_TESTS_EMPTY}
                            count={testCount}
                        >
                            <ul className="atlas-impact-list">
                                {model.tests.covering.map((test, index) => (
                                    <li key={`test-${index}`} className="atlas-impact-row" data-testid="atlas-impact-test">
                                        <RowButton target={{ name: test.name, filePath: test.file, line: test.line }} onOpen={props.onOpen}>
                                            {test.name}
                                        </RowButton>
                                        <span className="atlas-impact-where">{test.covers.join(', ')}</span>
                                    </li>
                                ))}
                                {model.tests.missing.map((row, index) => (
                                    <li
                                        key={`untested-${index}`}
                                        className="atlas-impact-row"
                                        data-testid="atlas-impact-untested"
                                    >
                                        <RowButton target={row} onOpen={props.onOpen}>
                                            {row.name}
                                        </RowButton>
                                        <span className="atlas-impact-where">{messages.impact.noTestCaller}</span>
                                    </li>
                                ))}
                            </ul>
                        </Section>
                    </>
                )}
            </div>
        </div>
    );
}
