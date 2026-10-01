// Entry point of the atlas-sample fixture. Builds the router, registers the
// route modules and prints the routing table. Nothing here opens a socket:
// the fixture is indexed, never executed.

import { loadConfig } from './config';
import { registerOrderRoutes } from './routes/orders';
import { registerUserRoutes } from './routes/users';
import type { RouteHandler, Router } from './types';

interface Route {
    method: string;
    path: string;
    handler: RouteHandler;
}

export class SimpleRouter implements Router {
    private readonly routes: Route[] = [];

    get(path: string, handler: RouteHandler): void {
        this.routes.push({ method: 'GET', path, handler });
    }

    post(path: string, handler: RouteHandler): void {
        this.routes.push({ method: 'POST', path, handler });
    }

    table(): string[] {
        return this.routes.map((route) => `${route.method} ${route.path}`);
    }
}

export function createApp(): SimpleRouter {
    const router = new SimpleRouter();
    registerUserRoutes(router);
    registerOrderRoutes(router);
    return router;
}

export function main(): void {
    const config = loadConfig();
    const app = createApp();
    for (const entry of app.table()) {
        console.log(`[atlas-sample] route ${entry}`);
    }
    console.log(`[atlas-sample] would listen on ${config.host}:${config.port}`);
}

main();
