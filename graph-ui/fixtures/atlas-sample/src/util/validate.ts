// Input validation. ValidationError lives here so both services throw the
// same class and the graph has a single error type to point at.

export class ValidationError extends Error {
    readonly field: string;

    constructor(field: string, message: string) {
        super(message);
        this.name = 'ValidationError';
        this.field = field;
    }
}

export interface UserInput {
    email: string;
    name: string;
}

export function validateUser(input: unknown): UserInput {
    const candidate = input as Partial<UserInput> | null;
    if (!candidate || typeof candidate.email !== 'string') {
        throw new ValidationError('email', 'email must be a string');
    }
    if (!candidate.email.includes('@')) {
        throw new ValidationError('email', 'email must contain an at sign');
    }
    if (typeof candidate.name !== 'string' || candidate.name.length === 0) {
        throw new ValidationError('name', 'name must be a non-empty string');
    }
    return { email: candidate.email, name: candidate.name };
}

export function validateId(value: string): string {
    if (value.trim().length === 0) {
        throw new ValidationError('id', 'id must not be empty');
    }
    return value;
}
