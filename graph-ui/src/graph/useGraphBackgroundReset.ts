import { useRef, type PointerEvent as ReactPointerEvent } from 'react';

/** A missed raycast is a reset only after a primary, stationary canvas gesture.
 * Tracking the entire gesture also excludes drags that return to their start. */
export function useGraphBackgroundReset(onReset?: () => void) {
    const gesture = useRef<{ id: number; x: number; y: number; moved: boolean } | undefined>(undefined);
    const onPointerDownCapture = (event: ReactPointerEvent) => {
        if (event.button !== 0 || event.isPrimary === false || !(event.target instanceof HTMLCanvasElement)) {
            gesture.current = undefined;
            return;
        }
        gesture.current = { id: event.pointerId, x: event.clientX, y: event.clientY, moved: false };
    };
    const onPointerMoveCapture = (event: ReactPointerEvent) => {
        const start = gesture.current;
        if (start && (event.pointerId !== start.id || Math.hypot(event.clientX - start.x, event.clientY - start.y) > 4)) start.moved = true;
    };
    const onPointerCancelCapture = () => { gesture.current = undefined; };
    const onPointerMissed = (event: MouseEvent) => {
        const start = gesture.current;
        gesture.current = undefined;
        if (event.type !== 'click' || event.button !== 0 || !(event.target instanceof HTMLCanvasElement)
            || !start || start.moved || Math.hypot(event.clientX - start.x, event.clientY - start.y) > 4) return;
        onReset?.();
    };
    return { onPointerDownCapture, onPointerMoveCapture, onPointerCancelCapture, onPointerMissed };
}
