import { ATTRIBUTION_PATTERNS, NAME_PATTERN, WATCHED_NAMES } from './forbidden-names.mjs';

/** A real integration must identify its client and configuration protocol.
 * This exemption is limited to four operational source/test files and the
 * README's exact installation instructions. Two named local verification JSON
 * files permit only configuration-path/setup-command fields; the named download
 * artifact must equal the canonical hook. Authorship claims remain forbidden.
 * Names use the guard's existing codepoint source, avoiding self-matches here.
 */
const FILES = new Set([
    'src/agents/AgentSetup.tsx',
    'src/agents/AgentSetup.test.tsx',
    'agents/hooks/atlas-trace.py',
    'agents/hooks/test_atlas_trace.py',
]);
const client = WATCHED_NAMES[0];
const JSON_ARTIFACTS = new Set([
    'verification/pr-2068/final-agent-boundary-initial-unavailable.json',
    'verification/pr-2068/final-agent-diagnosis-restart.json',
]);
export const DOWNLOADED_HOOK_ARTIFACT = 'verification/pr-2068/final-downloaded-hook.txt';
const README_LINES = new Set([
    `python3 ~/Downloads/cbm-atlas-trace.py --install-${client} --root /absolute/repository --project INDEXED_NAME --daemon-url http://127.0.0.1:9749`,
    `Installation is explicit and local: it preserves unrelated ${client[0].toUpperCase() + client.slice(1)} settings,`,
    `adds a PostToolUse entry to \`.${client}/settings.local.json\`, and copies the hook`,
    `into \`.${client}/hooks/\`. It refuses conflicting hooks, symlinks and malformed`,
]);
const REFERENCES = [
    new RegExp(`\\b${client} Code\\b`, 'gi'),
    new RegExp(`--install-${client}\\b`, 'gi'),
    new RegExp(`\\binstall_${client}\\b`, 'g'),
    new RegExp(`\\.${client}(?=[/'"\\s)]|$)`, 'g'),
];

function setupEvidenceField(path, line) {
    if (!JSON_ARTIFACTS.has(path)) return false;
    const field = line.match(/^\s*"(path|command)"\s*:\s*("(?:[^"\\]|\\.)*")\s*,?\s*$/);
    if (!field) return false;
    let value;
    try { value = JSON.parse(field[2]); } catch { return false; }
    return field[1] === 'path'
        ? new RegExp(`(?:^|/)\\.${client}/settings(?:\\.local)?\\.json$`).test(value)
        : value.startsWith(`python3 ~/Downloads/cbm-atlas-trace.py --install-${client} --root `)
            && value.includes(' --project ') && value.includes(" --daemon-url 'http://127.0.0.1:");
}

export function operationalClientReference(path, line, evidence = {}) {
    const setupInstruction = path === 'README.md' && README_LINES.has(line.trim());
    const canonicalDownload = path === DOWNLOADED_HOOK_ARTIFACT && evidence.canonicalHook instanceof Uint8Array
        && evidence.artifact instanceof Uint8Array && evidence.canonicalHook.length > 0
        && evidence.artifact.length === evidence.canonicalHook.length
        && evidence.artifact.every((byte, index) => byte === evidence.canonicalHook[index]);
    if ((!FILES.has(path) && !setupInstruction && !canonicalDownload && !setupEvidenceField(path, line)) || !NAME_PATTERN.test(line)
        || ATTRIBUTION_PATTERNS.some(({ pattern }) => pattern.test(line))) return false;
    if (setupInstruction) return true;
    let remaining = line;
    for (const pattern of REFERENCES) remaining = remaining.replace(pattern, '');
    return !NAME_PATTERN.test(remaining);
}

export const OPERATIONAL_CLIENT_REASON =
    'explicit client setup protocol/configuration reference in scoped source/test, exact README instructions, '
    + 'named local evidence fields, or the byte-identical hook download; authorship claims remain forbidden';
