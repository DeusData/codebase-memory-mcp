/** A repository-relative path request, kept intact instead of split into words. */
export function fileQueryPath(query: string): string | undefined {
    const path = query.trim().replace(/\\/g, '/').replace(/^(?:\.\/)+/, '');
    if (!path || path.startsWith('/') || path.endsWith('/') || /^[A-Za-z]:/.test(path)
        || /[\x00-\x1f]/.test(path) || path.split('/').some(part => !part || part === '..' || part === '.')) return undefined;
    return path.includes('/') || /\.[^\s.]+$/.test(path) ? path : undefined;
}
