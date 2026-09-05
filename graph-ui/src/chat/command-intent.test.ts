import { describe, expect, it } from 'vitest';

import { commandIntent, looksLikeQuestion } from './command-intent';

describe('looksLikeQuestion', () => {
    it('reads a question mark in either language', () => {
        expect(looksLikeQuestion('Wer ruft createUser?')).toBe(true);
        expect(looksLikeQuestion('who calls createUser?')).toBe(true);
    });

    it('reads a mention as a request to fetch that symbol', () => {
        expect(looksLikeQuestion('@createUser')).toBe(true);
    });

    it('reads a plain symbol name as a search', () => {
        expect(looksLikeQuestion('createUser')).toBe(false);
    });
});

describe('the search keeps priority', () => {
    it('opens the selected hit for a line the search answered', () => {
        expect(commandIntent({ line: 'createUser', hitCount: 3, answered: true }))
            .toBe('search-hit');
    });

    it('asks even with hits when the line ends with a question mark', () => {
        expect(commandIntent({ line: 'Wer ruft createUser?', hitCount: 3, answered: true }))
            .toBe('ask');
    });

    it('asks when the search answered with nothing', () => {
        expect(commandIntent({ line: 'wie haengt das zusammen', hitCount: 0, answered: true }))
            .toBe('ask');
    });

    it('waits while the search has not answered yet', () => {
        expect(commandIntent({ line: 'wie haengt das zusammen', hitCount: 0, answered: false }))
            .toBe('nothing');
    });

    it('does nothing on an empty line', () => {
        expect(commandIntent({ line: '   ', hitCount: 0, answered: true })).toBe('nothing');
    });

    it('asks for a mention even before the search has answered', () => {
        expect(commandIntent({ line: '@createUser', hitCount: 0, answered: false })).toBe('ask');
    });
});
