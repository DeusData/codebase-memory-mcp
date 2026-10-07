import type { BrowserAiProgress, BrowserChatRuntime } from './browser-ai-runtime';

/** A project switch stays in the page (K24): the window of the old project unmounts and the
 * window of the new one mounts in the same commit (app/project-windows.tsx). The chat dock
 * of the old project hands its worker with the loaded model to the dock of the new one, so
 * nothing is created, read from the cache or downloaded again, and the lamp stays on.
 *
 * The hand-over is bounded by the switch: outside `switchKeepingAgent` an unmounting dock
 * unloads its model as before, and a model no dock takes is unloaded when the switch ends. */
export interface AgentHandover {
    runtime: BrowserChatRuntime;
    modelId: string;
    /** An answer of the old project that was stopped and has not settled yet; the new dock waits for it. */
    settled?: Promise<void>;
    /** A model that is still loading; the new dock finishes loading it. */
    ready?: Promise<void>;
    /** How far that load got, and its later progress, for the dock that shows it now. */
    progress?: LoadProgress;
}

/** The progress of a model load, shown by one dock at a time. The worker reports to the dock
 * that started the load; after a switch the dock of the new project follows it instead, from
 * the last value on, so a download still running neither stalls nor starts over in the view. */
export interface LoadProgress {
    readonly latest?: BrowserAiProgress;
    report(value: BrowserAiProgress): void;
    /** The dock that shows the progress from now on; the one before stops hearing of it. */
    follow(listener: (value: BrowserAiProgress) => void): void;
}

export function loadProgress(): LoadProgress {
    let latest: BrowserAiProgress | undefined;
    let listener: ((value: BrowserAiProgress) => void) | undefined;
    return {
        get latest() { return latest; },
        report(value) { latest = value; listener?.(value); },
        follow(next) { listener = next; if (latest) next(latest); },
    };
}

let offered: (() => AgentHandover | undefined) | undefined;
let switching = false;
let handed: AgentHandover | undefined;

/** The mounted dock offers to give up its model; the returned function withdraws the offer. */
export function offerAgent(release: () => AgentHandover | undefined): () => void {
    offered = release;
    return () => { if (offered === release) offered = undefined; };
}

/** Runs a switch that replaces the window synchronously (flushSync inside `commit`). The model
 * is taken from the old dock before the new window renders, so its first frame is loaded. */
export function switchKeepingAgent(commit: () => void): void {
    switching = true;
    handed = offered?.();
    try { commit(); } finally {
        switching = false;
        const left = handed; handed = undefined;
        left?.runtime.dispose();
    }
}

/** An unmounting dock during a switch keeps its model for the next one instead of unloading
 * it; this is also how React's development double effect gives the model back. */
export function keepAgent(handover: AgentHandover): boolean {
    if (!switching) return false;
    if (handed && handed.runtime !== handover.runtime) handed.runtime.dispose();
    handed = handover;
    return true;
}

/** What the next dock is about to take, for its first render. */
export function peekAgent(modelId: string): AgentHandover | undefined {
    return handed?.modelId === modelId ? handed : undefined;
}

/** The mounting dock takes the handed-over model. A model of another choice is unloaded. */
export function takeAgent(modelId: string): AgentHandover | undefined {
    const handover = handed;
    if (!handover) return undefined;
    handed = undefined;
    if (handover.modelId === modelId) return handover;
    handover.runtime.dispose();
    return undefined;
}
