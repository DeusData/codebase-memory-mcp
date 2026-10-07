// User routes. Handlers stay thin so the call edge from the route to the
// service is the only thing the graph has to resolve.

import { createUser, listUsers } from '../services/userService';
import type { HttpRequest, HttpResponse, Router } from '../types';

export function registerUserRoutes(router: Router): void {
    router.get('/users', (req: HttpRequest, res: HttpResponse) => {
        const limit = Number(req.query.limit ?? '50');
        const users = listUsers(limit);
        res.json({ users, count: users.length });
    });

    router.post('/users', (req: HttpRequest, res: HttpResponse) => {
        const user = createUser(req.body);
        res.status(201).json({ user });
    });
}
