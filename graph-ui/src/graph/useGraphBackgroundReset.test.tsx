// @vitest-environment jsdom
import { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useGraphBackgroundReset } from './useGraphBackgroundReset';

let host: HTMLDivElement, root: Root;
beforeEach(() => {
    (globalThis as unknown as Record<string, unknown>).IS_REACT_ACT_ENVIRONMENT = true;
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });
function Fixture({ reset }: { reset: () => void }) {
    const { onPointerMissed, ...capture } = useGraphBackgroundReset(reset);
    return <div {...capture}><canvas onClick={event => onPointerMissed(event.nativeEvent)} />
        <button onClick={event => onPointerMissed(event.nativeEvent)}>Node label</button></div>;
}
async function setup() {
    const reset = vi.fn(); await act(async () => root.render(<Fixture reset={reset} />));
    return { reset, canvas: host.querySelector('canvas')!, label: host.querySelector('button')! };
}
function pointer(target: Element, type: string, x = 10, y = 10, changes: { button?: number; pointerId?: number; isPrimary?: boolean } = {}) {
    const event = new MouseEvent(type, { bubbles: true, clientX: x, clientY: y, button: changes.button ?? 0 });
    Object.defineProperties(event, { pointerId: { value: changes.pointerId ?? 1 }, isPrimary: { value: changes.isPrimary ?? true } });
    target.dispatchEvent(event);
}
it('resets only after a primary click on genuinely empty canvas', async () => {
    const { reset, canvas } = await setup();
    pointer(canvas, 'pointerdown'); pointer(canvas, 'pointerup'); pointer(canvas, 'click');
    expect(reset).toHaveBeenCalledOnce();
    pointer(canvas, 'click'); expect(reset).toHaveBeenCalledOnce();
});
it('does not reset orbit/pan drags, including drags that return to their starting point', async () => {
    const { reset, canvas } = await setup();
    pointer(canvas, 'pointerdown'); pointer(canvas, 'pointermove', 50); pointer(canvas, 'pointermove');
    pointer(canvas, 'pointerup'); pointer(canvas, 'click'); expect(reset).not.toHaveBeenCalled();
    pointer(canvas, 'pointerdown', 10, 10, { button: 2 }); pointer(canvas, 'click', 10, 10, { button: 2 });
    expect(reset).not.toHaveBeenCalled();
});
it('does not treat labels or controls as empty canvas', async () => {
    const { reset, canvas, label } = await setup();
    pointer(label, 'pointerdown'); pointer(label, 'click');
    pointer(canvas, 'pointerdown'); pointer(label, 'click');
    expect(reset).not.toHaveBeenCalled();
});
it('ignores canceled and multitouch gestures', async () => {
    const { reset, canvas } = await setup();
    pointer(canvas, 'pointerdown'); pointer(canvas, 'pointercancel'); pointer(canvas, 'click');
    pointer(canvas, 'pointerdown'); pointer(canvas, 'pointerdown', 20, 20, { pointerId: 2, isPrimary: false }); pointer(canvas, 'click');
    expect(reset).not.toHaveBeenCalled();
});
