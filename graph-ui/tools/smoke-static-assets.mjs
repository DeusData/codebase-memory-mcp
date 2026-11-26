import { createReadStream, realpathSync, statSync } from 'node:fs';
import { resolve, extname, sep } from 'node:path';

/** Serve the smoke fixture and installed assets only, including after resolving symlinks. */
export function serveStaticAsset(root, req, res, mime = {}) {
    const notFound = () => {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('not found');
    };
    let candidate;
    try {
        const urlPath = decodeURIComponent((req.url ?? '/').split('?')[0]);
        candidate = resolve(root, `.${urlPath}`);
    } catch {
        notFound();
        return;
    }
    for (const directory of ['spike', 'node_modules']) {
        const allowedRoot = resolve(root, directory);
        if (!candidate.startsWith(allowedRoot + sep)) continue;
        try {
            const realRoot = realpathSync(allowedRoot);
            const realCandidate = realpathSync(candidate);
            if (!realCandidate.startsWith(realRoot + sep)) continue;
            if (!statSync(realCandidate).isFile()) break;
            const stream = createReadStream(realCandidate);
            stream.once('error', () => {
                if (!res.headersSent) notFound();
                else res.destroy();
            });
            res.writeHead(200, { 'Content-Type': mime[extname(realCandidate)] ?? 'application/octet-stream' });
            stream.pipe(res);
            return;
        } catch {
            break;
        }
    }
    notFound();
}
