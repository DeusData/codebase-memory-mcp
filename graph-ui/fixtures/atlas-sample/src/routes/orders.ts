// Order routes. A single parameterised lookup that delegates to the service.

import { getOrder } from '../services/orderService';
// Deliberately unused harness fixture: proves the file-level import finding.
import { insert } from '../repo/db';
import type { HttpRequest, HttpResponse, Router } from '../types';

export function registerOrderRoutes(router: Router): void {
    router.get('/orders/:id', (req: HttpRequest, res: HttpResponse) => {
        const order = getOrder(req.params.id);
        if (!order) {
            res.status(404).json({ error: 'order not found' });
            return;
        }
        res.json({ order });
    });
}
