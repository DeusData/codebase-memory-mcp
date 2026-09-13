/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/client/errors.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Uebernommen wurde die Unterscheidung
 * der beiden Fehlerarten, an der der portierte Provider seine Antworten
 * ausrichtet: "die Engine hat geantwortet und nein gesagt" fuehrt zu
 * notIndexed, "es hat gar niemand geantwortet" zu unknown. Wer die beiden
 * zusammenlegt, macht aus einem abgestuerzten Server eine Aussage ueber den
 * Code des Lesers. Aenderungen gegenueber dem Original: EngineTimeoutError ist
 * nicht mitportiert, weil es hier keinen Kindprozess mit Budget gibt; ein
 * abgebrochener fetch ist ein nicht erreichbarer Server und wird als solcher
 * gemeldet.
 */

/** The engine ran, refused the call and explained itself. */
export class EngineError extends Error {
    readonly tool: string;
    /** Remediation text the engine supplied, when it did. */
    readonly hint?: string;
    /** HTTP status of the refusal, when one arrived. */
    readonly status?: number;

    constructor(tool: string, message: string, details: { hint?: string; status?: number } = {}) {
        super(`${tool}: ${message}`);
        this.name = 'EngineError';
        this.tool = tool;
        this.hint = details.hint;
        this.status = details.status;
    }
}

/**
 * No engine to call: the server did not answer, or it does not offer this tool.
 *
 * The second half is what /rpc adds to the picture. The read-only allowlist
 * answers `index_repository`, `delete_project` and `ingest_traces` with 403 and
 * a `-32601`, and that is not a refusal about a project: it is the honest
 * statement that this surface has no such capability. Treating it as an engine
 * error would make a caller conclude something about the workspace.
 */
export class EngineUnavailableError extends Error {
    /** Machine-readable cause: `unreachable`, `not-allowed` or `server-error`. */
    readonly reason: string;
    /** Tool that was asked for, so a message can name it. */
    readonly tool?: string;

    constructor(reason: string, message?: string, tool?: string) {
        super(message ?? reason);
        this.name = 'EngineUnavailableError';
        this.reason = reason;
        this.tool = tool;
    }
}
