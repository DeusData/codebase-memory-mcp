/** Build a pattern for literal text with optional start/end anchoring. */
export function literalPattern(value, { start = false, end = false } = {}) {
    const escaped = value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp(`${start ? '^' : ''}${escaped}${end ? '$' : ''}`);
}
