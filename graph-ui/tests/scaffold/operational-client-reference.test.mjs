import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { WATCHED_NAMES } from '../../tools/lib/forbidden-names.mjs';
import { DOWNLOADED_HOOK_ARTIFACT, operationalClientReference } from '../../tools/lib/operational-client-reference.mjs';

const client = WATCHED_NAMES[0];
test('permits only operational client terms in the exact setup source/test files', () => {
    const files = ['src/agents/AgentSetup.tsx', 'src/agents/AgentSetup.test.tsx',
        'agents/hooks/atlas-trace.py', 'agents/hooks/test_atlas_trace.py'];
    for (const file of files) for (const line of [
        `${client} Code on macOS/Linux`, `--install-${client}`, `install_${client}(root)`,
        `root / '.${client}'`, `.${client}/settings.local.json`,
    ]) assert.equal(operationalClientReference(file, line), true, `${file}: ${line}`);
    assert.equal(operationalClientReference('src/App.tsx', `${client} Code`), false);
    assert.equal(operationalClientReference('verification/proof.json', `.${client}/settings.local.json`), false);
    assert.equal(operationalClientReference('src/agents/AgentSetup.tsx', client), false);
    assert.equal(operationalClientReference('src/agents/AgentSetup.tsx', `${client} Code ${WATCHED_NAMES[1]}`), false);
    assert.equal(operationalClientReference('README.md', `${client} Code`), false);
    const instruction = `adds a PostToolUse entry to \`.${client}/settings.local.json\`, and copies the hook`;
    assert.equal(operationalClientReference('README.md', instruction), true);
    assert.equal(operationalClientReference('other/README.md', instruction), false);
    assert.equal(operationalClientReference('README.md', instruction + `; written by ${client}`), false);
});

test('authorship claims stay forbidden even beside an otherwise valid protocol reference', () => {
    for (const text of [`written by ${client}`, `Co-authored-by: ${client}`,
        `generated with ${client}`, `${client} authored this`, `@${client}`]) {
        assert.equal(operationalClientReference('src/agents/AgentSetup.tsx', `${text}; --install-${client}`), false);
    }
});

test('local verification exemptions are limited to named configuration and command fields', () => {
    for (const path of ['verification/pr-2068/final-agent-boundary-initial-unavailable.json',
        'verification/pr-2068/final-agent-diagnosis-restart.json']) {
        assert.equal(operationalClientReference(path, `"path": "/repo/.${client}/settings.local.json",`), true);
        assert.equal(operationalClientReference(path, `"path": "/user/.${client}/settings.json",`), true);
        const command = `python3 ~/Downloads/cbm-atlas-trace.py --install-${client} --root '/repo' --project 'p' --daemon-url 'http://127.0.0.1:9749'`;
        assert.equal(operationalClientReference(path, `"command": ${JSON.stringify(command)},`), true);
        assert.equal(operationalClientReference(path, `"message": "${client} Code"`), false);
        assert.equal(operationalClientReference(path, `"path": "/repo/.${client}/unrelated.txt"`), false);
        assert.equal(operationalClientReference(path, `"command": "written by ${client}; ${command}"`), false);
    }
    assert.equal(operationalClientReference('verification/unrelated.json', `"path": "/repo/.${client}/settings.json"`), false);
});

test('the named downloaded hook is exempt only while identical to its canonical source', () => {
    const source = `def install_${client}(root):\n    pass\n`;
    const bytes = text => new TextEncoder().encode(text);
    assert.equal(operationalClientReference(DOWNLOADED_HOOK_ARTIFACT, source.split('\n')[0]), false);
    assert.equal(operationalClientReference(DOWNLOADED_HOOK_ARTIFACT, source.split('\n')[0], { artifact: bytes(source), canonicalHook: bytes(source) }), true);
    assert.equal(operationalClientReference(DOWNLOADED_HOOK_ARTIFACT, source.split('\n')[0], { artifact: bytes(source + '# modified'), canonicalHook: bytes(source) }), false);
    assert.equal(operationalClientReference(DOWNLOADED_HOOK_ARTIFACT, source.split('\n')[0], { artifact: Uint8Array.of(0xff), canonicalHook: Uint8Array.of(0xfe) }), false);
    const claim = `# written by ${client} Code`;
    assert.equal(operationalClientReference(DOWNLOADED_HOOK_ARTIFACT, claim, { artifact: bytes(claim), canonicalHook: bytes(claim) }), false);
});

test('the recorded hook download matches the canonical source and the scanner path', async () => {
    const root = new URL('../../', import.meta.url);
    const [artifact, canonicalHook] = await Promise.all([
        readFile(new URL(DOWNLOADED_HOOK_ARTIFACT, root)),
        readFile(new URL('agents/hooks/atlas-trace.py', root)),
    ]);
    assert.deepEqual(artifact, canonicalHook);
    const line = artifact.toString('utf8').split('\n').find(row => row.includes(`def install_${client}(`));
    assert.ok(line);
    assert.equal(operationalClientReference(DOWNLOADED_HOOK_ARTIFACT, line, { artifact, canonicalHook }), true);
});
