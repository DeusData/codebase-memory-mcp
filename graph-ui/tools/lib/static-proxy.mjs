/**
 * Ein Ursprung fuer den Beweislauf: dist/ ausliefern und /rpc plus /api an den
 * C-Server weiterreichen.
 *
 * Warum das noetig ist, steht in INVENTAR.md Abschnitt 5 und ist keine
 * Bequemlichkeit: der Server nimmt eine Anfrage auf /api oder /rpc nur an, wenn
 * der Host-Kopf genau seine eigene Loopback-Adresse mit seinem eigenen Port
 * nennt, und weist jeden Origin ab, der nicht derselbe Server ist. Ein anderer
 * localhost-Port ist fuer ihn ein anderes Principal. Eine Seite auf Port B kann
 * also nicht per fetch an Port A: der Browser wuerde 403 "forbidden origin"
 * bekommen.
 *
 * Dieser Proxy loest das, indem er auf der Serverseite genau zwei Koepfe
 * geradezieht:
 *
 *  - **Host** wird auf `127.0.0.1:<Serverport>` gesetzt. Ohne das schlaegt der
 *    Rebinding-Schutz zu.
 *  - **Origin** wird ENTFERNT. Der Server laesst eine Anfrage ohne Origin
 *    durch (http_server.c: geprueft wird nur ein *gesetzter* Origin), und einen
 *    fremden Origin lehnt er ab. Ihn auf die Serveradresse umzuschreiben waere
 *    dasselbe Ergebnis mit einer Luege mehr im Kopf.
 *
 * Im Betrieb gibt es diesen Proxy nicht: dort liefert der C-Server die Assets
 * selbst aus und alles ist same-origin (INVENTAR.md Abschnitt 8). Der Proxy ist
 * der Ersatz dafuer im Beweislauf, und der Unterschied ist genau ein Ursprung.
 *
 * Nebenbei schreibt er mit, welche /api-Routen und welche /rpc-Werkzeuge
 * wirklich gerufen wurden. Das macht aus "der Baum kommt aus /api/tree" einen
 * Befund statt einer Behauptung.
 */

import { createReadStream } from 'node:fs';
import { stat } from 'node:fs/promises';
import http from 'node:http';
import { extname, join, normalize, resolve } from 'node:path';

const MIME = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.mjs': 'text/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.png': 'image/png',
    '.ttf': 'font/ttf',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
    '.map': 'application/json; charset=utf-8',
    '.ico': 'image/x-icon',
};

/** Ein Pfad, der dist/ nicht verlassen kann. */
function safeJoin(root, urlPath) {
    const decoded = decodeURIComponent(urlPath.split('?')[0]);
    const candidate = resolve(join(root, normalize(decoded)));
    return candidate.startsWith(resolve(root)) ? candidate : null;
}

function readBody(req) {
    return new Promise((resolveBody, reject) => {
        const chunks = [];
        req.on('data', (chunk) => chunks.push(chunk));
        req.on('end', () => resolveBody(Buffer.concat(chunks)));
        req.on('error', reject);
    });
}

/**
 * dist/ ausliefern, /rpc und /api weiterreichen.
 *
 * @param {{distDir: string, upstreamPort: number, port: number}} options
 * @returns {Promise<{port: number, log: {apiRoutes: Record<string, number>, rpcTools: Record<string, number>, errors: string[]}, close: () => Promise<void>}>}
 */
export async function startStaticProxy({ distDir, upstreamPort, port }) {
    const log = { apiRoutes: {}, rpcTools: {}, errors: [] };

    const forward = async (req, res, body) => {
        const route = req.url.split('?')[0];
        if (route === '/rpc' && body.length > 0) {
            try {
                const parsed = JSON.parse(body.toString('utf8'));
                const tool = parsed?.params?.name;
                if (typeof tool === 'string') {
                    log.rpcTools[tool] = (log.rpcTools[tool] ?? 0) + 1;
                }
            } catch {
                // Ein unlesbarer Rumpf ist die Sache des Servers, nicht des Proxys.
            }
        } else if (route.startsWith('/api')) {
            log.apiRoutes[route] = (log.apiRoutes[route] ?? 0) + 1;
        }

        const headers = { ...req.headers };
        // Die beiden Koepfe, um die es geht. Alles andere geht unveraendert durch.
        headers.host = `127.0.0.1:${upstreamPort}`;
        delete headers.origin;
        delete headers.referer;
        delete headers['accept-encoding'];

        const upstream = http.request(
            {
                host: '127.0.0.1',
                port: upstreamPort,
                method: req.method,
                path: req.url,
                headers,
            },
            (upstreamRes) => {
                res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
                upstreamRes.pipe(res);
            },
        );
        upstream.on('error', (error) => {
            log.errors.push(`${req.method} ${req.url}: ${error.message}`);
            res.writeHead(502, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ error: `upstream unreachable: ${error.message}` }));
        });
        if (body.length > 0) {
            upstream.write(body);
        }
        upstream.end();
    };

    const server = http.createServer((req, res) => {
        const route = (req.url ?? '/').split('?')[0];
        if (route === '/rpc' || route.startsWith('/api')) {
            readBody(req)
                .then((body) => forward(req, res, body))
                .catch((error) => {
                    log.errors.push(String(error));
                    res.writeHead(500);
                    res.end();
                });
            return;
        }

        const filePath = route === '/' ? join(distDir, 'index.html') : safeJoin(distDir, route);
        if (filePath === null) {
            res.writeHead(403);
            res.end('forbidden path');
            return;
        }
        stat(filePath)
            .then((info) => {
                if (!info.isFile()) {
                    throw new Error('not a file');
                }
                res.writeHead(200, {
                    'Content-Type': MIME[extname(filePath)] ?? 'application/octet-stream',
                    'Content-Length': info.size,
                    'Cache-Control': 'no-store',
                });
                createReadStream(filePath).pipe(res);
            })
            .catch(() => {
                res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
                res.end(`not found: ${route}`);
            });
    });

    await new Promise((ready, fail) => {
        server.once('error', fail);
        server.listen(port, '127.0.0.1', ready);
    });

    return {
        port,
        log,
        close: () =>
            new Promise((done) => {
                server.closeAllConnections?.();
                server.close(() => done());
            }),
    };
}
