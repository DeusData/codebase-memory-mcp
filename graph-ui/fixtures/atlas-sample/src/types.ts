// Shared types for the atlas-sample fixture: a small express-like HTTP surface
// plus the domain entities the services work with. No runtime dependencies.

export interface HttpRequest {
    params: Record<string, string>;
    query: Record<string, string>;
    body: unknown;
}

export interface HttpResponse {
    status(code: number): HttpResponse;
    json(payload: unknown): void;
}

export type RouteHandler = (req: HttpRequest, res: HttpResponse) => void;

export interface Router {
    get(path: string, handler: RouteHandler): void;
    post(path: string, handler: RouteHandler): void;
}

export interface Entity {
    id: string;
    createdAt: string;
}

export interface User extends Entity {
    email: string;
    name: string;
}

export interface Order extends Entity {
    customerId: string;
    total: number;
}

export class UserEntity implements Entity {
    readonly id: string;
    readonly createdAt: string;
    readonly email: string;
    readonly name: string;

    constructor(id: string, email: string, name: string) {
        this.id = id;
        this.email = email;
        this.name = name;
        this.createdAt = new Date(0).toISOString();
    }

    label(): string {
        return `${this.name} <${this.email}>`;
    }
}
