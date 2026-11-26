export function projectPickerPlacement(trigger: { top: number; bottom: number; right: number }, viewport: { width: number; height: number }): { top: number; left: number; width: number; maxHeight: number } {
    const width = Math.max(0, Math.min(370, viewport.width - 24));
    const limit = Math.max(0, Math.min(520, viewport.height - 24));
    const minimum = Math.min(240, limit);
    const below = viewport.height - trigger.bottom - 20;
    const above = trigger.top - 20;
    const maxHeight = Math.max(0, Math.min(limit, below >= minimum ? below : above >= minimum ? above : limit));
    return {
        top: below >= minimum ? Math.max(12, trigger.bottom + 8)
            : above >= minimum ? Math.max(12, trigger.top - maxHeight - 8) : 12,
        left: Math.max(12, Math.min(trigger.right - width, viewport.width - width - 12)),
        width,
        maxHeight,
    };
}
