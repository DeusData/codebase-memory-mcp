// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import ChatMarkdown from './ChatMarkdown';

let container: HTMLDivElement;
let root: Root;
let clipboardDescriptor: PropertyDescriptor | undefined;
let writeText: ReturnType<typeof vi.fn>;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    container = document.createElement('div'); document.body.appendChild(container); root = createRoot(container);
    clipboardDescriptor = Object.getOwnPropertyDescriptor(navigator, 'clipboard');
    writeText = vi.fn(async () => {});
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
});
afterEach(async () => {
    await act(async () => root.unmount()); container.remove();
    if (clipboardDescriptor) Object.defineProperty(navigator, 'clipboard', clipboardDescriptor);
    else Reflect.deleteProperty(navigator, 'clipboard');
    vi.restoreAllMocks();
});
async function render(text: string): Promise<void> { await act(async () => root.render(<ChatMarkdown text={text} />)); }

describe('local chat Markdown presentation', () => {
    it('renders headings, paragraphs, emphasis, lists, quotes and inline code semantically', async () => {
        await render('## Explanation\n\nA **strong** point and *emphasis* with `sum(x)`.\n\n- First\n- Second\n\n1. Ordered\n2. Steps\n\n> A quoted note.');
        expect(container.querySelector('h2')?.textContent).toBe('Explanation');
        expect(container.querySelector('strong')?.textContent).toBe('strong');
        expect(container.querySelector('em')?.textContent).toBe('emphasis');
        expect(container.querySelector('p > code')?.textContent).toBe('sum(x)');
        expect(container.querySelectorAll('ul > li')).toHaveLength(2); expect(container.querySelectorAll('ol > li')).toHaveLength(2);
        expect(container.querySelector('blockquote')?.textContent).toContain('A quoted note.');
    });

    it('renders GFM tables, strikethrough, and inert task lists', async () => {
        await render('| Symbol | Role |\n| --- | --- |\n| `main` | Entry |\n\n~~Old description~~\n\n- [x] Read\n- [ ] Verify');
        expect(container.querySelectorAll('thead th')).toHaveLength(2);
        expect(container.querySelector('tbody td code')?.textContent).toBe('main');
        expect(container.querySelector('del')?.textContent).toBe('Old description');
        const boxes = [...container.querySelectorAll<HTMLInputElement>('input[type=checkbox]')];
        expect(boxes).toHaveLength(2); expect(boxes.every(box => box.disabled)).toBe(true);
    });

    it('shows fenced-code language and copies the rendered code without UI labels', async () => {
        await render('```typescript\nconst answer = 42;\n  next();\n```');
        expect(container.querySelector('.cbm-chat-code-language')?.textContent).toBe('typescript');
        expect(container.querySelector('pre > code')?.textContent).toBe('const answer = 42;\n  next();\n');
        const button = container.querySelector('button[aria-label="Copy code"]');
        expect(button).not.toBeNull(); await act(async () => (button as HTMLButtonElement).click());
        expect(writeText).toHaveBeenCalledExactlyOnceWith('const answer = 42;\n  next();\n');
        expect(container.textContent).toContain('Copied');
    });

    it('keeps incomplete streaming fences readable and copies their latest contents', async () => {
        await render('Before\n\n```js\nconst value =');
        const incompleteCode = container.querySelector('pre code');
        expect(incompleteCode, 'an open streaming fence is a readable code block').not.toBeNull();
        expect(incompleteCode?.textContent).toContain('const value =');
        await render('Before\n\n```js\nconst value = 42;\n```\n\nAfter.');
        expect(container.querySelector('pre code')?.textContent).toBe('const value = 42;\n');
        expect(container.querySelectorAll('pre')).toHaveLength(1);
        expect(container.textContent).toContain('After.');
        await act(async () => (container.querySelector('button[aria-label="Copy code"]') as HTMLButtonElement).click());
        expect(writeText).toHaveBeenCalledExactlyOnceWith('const value = 42;\n');
    });

    it('opens safe links separately without granting access to the opener', async () => {
        await render('[Documentation](https://example.com/docs?q=code) and [Email](mailto:team@example.com)');
        const links = [...container.querySelectorAll('a')]; expect(links).toHaveLength(2);
        expect(links[0].getAttribute('href')).toBe('https://example.com/docs?q=code');
        expect(links.every(link => link.target === '_blank' && link.rel.includes('noopener') && link.rel.includes('noreferrer'))).toBe(true);
    });

    it('does not execute raw HTML or expose dangerous URL schemes', async () => {
        await render('<script>window.__markdownInjected = true</script>\n\n<img src=https://example.com/pixel onerror=alert(1)>\n\n[Bad](javascript:alert%281%29) [Data](data:text/html;base64,PHNjcmlwdD4=) [File](file:///etc/passwd)');
        expect(container.querySelector('script, img, iframe')).toBeNull();
        expect([...container.querySelectorAll('a')].every(link => /^(https?:|mailto:)/.test(link.getAttribute('href') ?? ''))).toBe(true);
        expect(container.textContent).toContain('Bad'); expect(container.textContent).toContain('Data');
        expect((window as unknown as Record<string, unknown>).__markdownInjected).toBeUndefined();
    });

    it('turns image syntax into an explicit link without loading an external image', async () => {
        await render('![Architecture](https://example.com/tracking-pixel?code=secret)\n\n![Unsafe](data:image/png;base64,AA==)');
        expect(container.querySelector('img, picture, source')).toBeNull();
        const imageLink = container.querySelector('a');
        expect(imageLink?.getAttribute('href')).toBe('https://example.com/tracking-pixel?code=secret');
        expect(imageLink?.textContent).toContain('Architecture'); expect(container.textContent).toContain('Unsafe');
    });

    it('reports clipboard failure without claiming the code was copied', async () => {
        writeText.mockRejectedValueOnce(new Error('Clipboard unavailable'));
        await render('```\nplain code\n```');
        const button = container.querySelector('button[aria-label="Copy code"]'); expect(button).not.toBeNull();
        await act(async () => (button as HTMLButtonElement).click());
        expect(container.textContent).toContain('Copy unavailable'); expect(container.textContent).not.toContain('Copied');
    });

    it('keeps HTML inside code fences as literal code text', async () => {
        await render('```html\n<img src=x onerror=alert(1)>\n```');
        expect(container.querySelector('img')).toBeNull();
        expect(container.querySelector('pre code')?.textContent).toBe('<img src=x onerror=alert(1)>\n');
    });

    it('does not label newly streamed code as copied when an earlier clipboard write completes', async () => {
        let finish!: () => void;
        writeText.mockReturnValueOnce(new Promise<void>(resolve => { finish = resolve; }));
        await render('```js\nconst value =');
        await act(async () => (container.querySelector('button[aria-label="Copy code"]') as HTMLButtonElement).click());
        await render('```js\nconst value = 42;\n```');
        await act(async () => finish());
        expect(container.textContent).not.toContain('Copied');
        expect(container.querySelector('pre code')?.textContent).toBe('const value = 42;\n');
    });
});
