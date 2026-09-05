/*
 * Herkunft: portiert am 2026-08-28 aus CodeAtlasIDE,
 * /Users/bernhard/Desktop/CodeAtlasIDE/theia-extensions/codeatlas-intelligence/src/node/ir/file-uri.ts
 * Gleicher Urheber (Bernhard Jackiewicz). Semantisch unveraendert uebernommen,
 * weil IR-Builder und Checklisten-Generator dieselbe eine Umrechnung brauchen
 * und zwei Fassungen davon zwei verschiedene Orte fuer dieselbe Datei ergeben
 * wuerden. Aenderungen gegenueber dem Original: keine.
 */
/**
 * The one conversion from an engine file reference to something an editor can
 * open, and the one conversion back.
 *
 * It lives on its own because two modules need it and neither may import the
 * other: the IR builder normalises every fact family through it, and the
 * checklist generator builds an item's navigation target through it. Leaving it
 * on the builder and reaching back for it from the generator would make the two
 * files circular for the sake of one function.
 */

/**
 * Turn an engine file reference into an absolute URI.
 *
 * Accepts what the engine actually emits: a workspace-relative path, an
 * absolute path, an already-formed URI, or nothing at all.
 */
export function toFileUri(root: string, file: string | undefined): string | undefined {
    if (file === undefined || file.length === 0) {
        return undefined;
    }
    if (/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(file)) {
        return file;
    }
    const normalizedRoot = root.replace(/\/+$/, '');
    const absolute = file.startsWith('/') ? file : `${normalizedRoot}/${file.replace(/^\.\//, '')}`;
    return `file://${absolute.split('/').map(segment => encodeURIComponent(segment)).join('/')}`;
}

/**
 * Turn any of the spellings above back into a path relative to the workspace.
 *
 * The inverse of {@link toFileUri}, and it exists for one reason: a value that
 * has to stay the same when the workspace is moved to another directory. An
 * absolute path or a `file://` URI names the machine that produced it, so
 * anything keyed on one is silently discarded the day a repository is cloned
 * somewhere else. A workspace-relative path names the repository, which is what
 * such keys are actually about.
 *
 * A reference that does not live under the root is returned unchanged. That is
 * not a failure: a test in a sibling checkout genuinely has no name relative to
 * this workspace, and inventing one with enough `..` segments would produce a
 * key that changed with the depth of the directory the workspace sits in, which
 * is the exact fragility this function exists to remove.
 */
export function toWorkspaceRelative(root: string, file: string | undefined): string {
    if (file === undefined || file.length === 0) {
        return '';
    }
    const path = file.startsWith('file://')
        ? file.slice('file://'.length).split('/').map(decodeSegment).join('/')
        : file;
    const normalizedRoot = root.replace(/\/+$/, '');
    if (normalizedRoot.length > 0 && path.startsWith(`${normalizedRoot}/`)) {
        return path.slice(normalizedRoot.length + 1);
    }
    return path;
}

/** A segment that is not valid percent-encoding is taken literally, never dropped. */
function decodeSegment(segment: string): string {
    try {
        return decodeURIComponent(segment);
    } catch {
        return segment;
    }
}
