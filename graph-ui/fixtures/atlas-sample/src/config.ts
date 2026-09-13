// Configuration loader. Every value comes from the environment, which gives
// the fixture a stable set of environment read sites for the graph.

export interface AppConfig {
    host: string;
    port: number;
    databaseUrl: string;
    logLevel: string;
}

export function loadConfig(): AppConfig {
    return {
        host: process.env.HOST ?? '127.0.0.1',
        port: Number(process.env.PORT ?? '8080'),
        databaseUrl: process.env.DB_URL ?? 'memory://atlas-sample',
        logLevel: process.env.LOG_LEVEL ?? 'info',
    };
}

export function isProduction(): boolean {
    return process.env.NODE_ENV === 'production';
}
