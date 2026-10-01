export interface BrowserAiSource { text: string; path: string; startLine: number; project: string }
export interface BrowserAiProgress { file?: string; loaded?: number; total?: number; progress?: number }
export interface BrowserAiRuntime {
    prepare(onProgress: (progress: BrowserAiProgress) => void): Promise<void>;
    explain(source: BrowserAiSource): Promise<string>;
    dispose(): void;
}
export interface BrowserChatMessage { role: 'system' | 'user' | 'assistant'; content: string }
export interface BrowserChatOptions {
    maxOutputTokens?: number;
    /** Short prose summaries only; ordinary chat retains the model's generation defaults. */
    generationProfile?: 'automatic-explanation';
}
export interface BrowserChatRuntime extends BrowserAiRuntime {
    countTokens(messages: readonly BrowserChatMessage[]): Promise<number>;
    chat(messages: readonly BrowserChatMessage[], onToken: (chunk: string) => void, options?: BrowserChatOptions): Promise<string>;
    /** Device loss can happen while idle. Registration reports an existing fatal fault immediately. */
    setFatalHandler?(handler: ((error: Error) => void) | undefined): void;
    /** Interrupts generation; the chat promise settles with the partial answer. Keeps the model loaded. */
    stop(): void;
}
export interface BrowserAiState {
    phase: 'off' | 'preparing' | 'ready' | 'generating' | 'error' | 'removing';
    progress?: BrowserAiProgress; error?: string; output?: string; outputSource?: BrowserAiSource;
}

export class BrowserAiController {
    state: BrowserAiState = { phase: 'off' };
    private runtime?: BrowserAiRuntime;
    private ticket = 0;
    constructor(readonly createRuntime: () => BrowserAiRuntime, readonly removeCache: () => Promise<void>, readonly onState: (state: BrowserAiState) => void) {}
    private update(state: BrowserAiState): void { this.state = state; this.onState(state); }
    private stop(): void { this.ticket += 1; this.runtime?.dispose(); this.runtime = undefined; }
    async prepare(): Promise<void> {
        if (this.state.phase === 'preparing' || this.state.phase === 'generating' || this.state.phase === 'removing') return;
        this.stop();
        const ticket = this.ticket;
        this.update({ phase: 'preparing' });
        try {
            const runtime = this.createRuntime();
            this.runtime = runtime;
            await runtime.prepare(progress => { if (this.ticket === ticket) this.update({ phase: 'preparing', progress }); });
            if (this.ticket === ticket) this.update({ phase: 'ready' });
        } catch (error) {
            if (this.ticket !== ticket) return;
            this.stop();
            this.update({ phase: 'error', error: error instanceof Error ? error.message : String(error) });
        }
    }
    async explain(source: BrowserAiSource): Promise<void> {
        if (this.state.phase !== 'ready' || !this.runtime || !source.text.trim()) return;
        const ticket = this.ticket;
        const snapshot = { ...source };
        this.update({ phase: 'generating', outputSource: snapshot });
        try {
            const output = await this.runtime.explain(snapshot);
            if (this.ticket === ticket) this.update({ phase: 'ready', output, outputSource: snapshot });
        } catch (error) {
            if (this.ticket !== ticket) return;
            this.stop();
            this.update({ phase: 'error', error: error instanceof Error ? error.message : String(error) });
        }
    }
    cancel(): void { this.stop(); this.update({ phase: 'off' }); }
    async remove(): Promise<void> {
        this.stop();
        const ticket = this.ticket;
        this.update({ phase: 'removing' });
        try {
            await this.removeCache();
            if (this.ticket === ticket) this.update({ phase: 'off' });
        } catch (error) {
            if (this.ticket === ticket) this.update({ phase: 'error', error: error instanceof Error ? error.message : String(error) });
        }
    }
    dispose(): void { this.stop(); }
}
