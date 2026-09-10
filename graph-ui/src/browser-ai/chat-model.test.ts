import { describe, expect, it } from 'vitest';
import { buildChatMessages, selectionLocation, snapshotAttachment, userMessage, type BrowserChatAttachment, type BrowserChatTurn } from './chat-model';

const source: BrowserChatAttachment = { id: 'selection-1', text: '\t partial(\r\n  a + b\n', path: 'src/math.ts', project: 'sample', startLine: 3, startColumn: 8, endLine: 5, endColumn: 1, sourceVersion: 'sha256:test' };
const attached = (content: string) => JSON.parse(content.slice(content.indexOf('\n{') + 1).split('\n')[0]);

describe('browser chat context', () => {
    it('preserves partial selection characters, whitespace, line endings and range exactly', () => {
        const message = userMessage('What does this do?', source);
        expect(attached(message)).toEqual({ project: source.project, path: source.path, range: { startLine: 3, startColumn: 8, endLine: 5, endColumn: 1 }, sourceVersion: source.sourceVersion });
        expect(message.includes(source.text)).toBe(true);
        expect(selectionLocation(source)).toBe('src/math.ts:3:8-5:1');
    });

    it('copies selection metadata so navigation cannot mutate the sent snapshot', () => {
        const live = { ...source };
        const snapshot = snapshotAttachment(live);
        live.text = 'different code'; live.path = 'new.ts';
        expect(snapshot).toEqual(source);
    });

    it('retains prior context but attaches selected code only to the selected user message', () => {
        const turn: BrowserChatTurn = { id: '1', prompt: 'Explain', attachment: source, answer: 'A sum.', request: [], modelId: 'test', status: 'complete' };
        const messages = buildChatMessages([turn], 'And why?');
        expect(messages.map(message => message.role)).toEqual(['system', 'user', 'assistant', 'user']);
        expect(messages[1].content).toContain(source.text);
        expect(messages[3].content).toBe('And why?');
    });

    it('does not fabricate answers or reuse a failed request as conversation evidence', () => {
        const failed: BrowserChatTurn = { id: '1', prompt: 'Explain', answer: 'Unfinished', request: [], modelId: 'test', status: 'error' };
        expect(buildChatMessages([failed], 'New question')).toHaveLength(2);
    });

    it('does not silently shorten large source selections', () => {
        const text = 'a'.repeat(20000) + '\n\tend  ';
        expect(userMessage('Explain', { ...source, text })).toContain(text);
    });
});
