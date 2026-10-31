import { describe, expect, it } from 'vitest';
import { readGalaxyEvidence, selectionSentence } from './galaxy-evidence';
import { githubFolderEvidence } from './galaxy-evidence.fixture';
import { relationshipWords } from './strings';

/*
 * Handtest 04.10. (17:09): Die Karte zu .github sagte "Selected: .github (Folder)
 * in .github." Bei einem Ordner oder einer Datei ist der Pfad das, was gewaehlt
 * ist, kein Ort, an dem es steht; "in .github" wiederholte nur den Namen.
 */
describe('the selection sentence of a folder or a file', () => {
    it('names a folder without repeating its own path as its place', () => {
        const evidence = readGalaxyEvidence(githubFolderEvidence('hierarchy').text)!;
        expect(selectionSentence(evidence, relationshipWords.en)[0]).toBe('Selected: .github (Folder).');
        expect(selectionSentence(evidence, relationshipWords.de)[0]).toBe('Ausgewählt: .github (Ordner).');
    });
});
