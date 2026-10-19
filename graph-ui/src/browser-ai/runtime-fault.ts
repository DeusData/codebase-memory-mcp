/** GPU failures invalidate the whole worker, including pending device resources. */
export class BrowserRuntimeFatalError extends Error {
    readonly code = 'BROWSER_RUNTIME_FATAL';
    readonly detail: string;
    constructor(failure: unknown) {
        super('The browser GPU runtime failed. Reload the model to continue.');
        this.name = 'BrowserRuntimeFatalError';
        this.detail = runtimeErrorDetail(failure);
    }
}

export function isFatalBrowserRuntimeError(error: unknown): error is BrowserRuntimeFatalError {
    return Boolean(error && typeof error === 'object' && 'code' in error
        && error.code === 'BROWSER_RUNTIME_FATAL' && 'detail' in error && typeof error.detail === 'string');
}

export function runtimeErrorDetail(error: unknown): string {
    if (isFatalBrowserRuntimeError(error)) return error.detail;
    if (error && typeof error === 'object' && 'message' in error && typeof error.message === 'string') return error.message;
    return String(error ?? 'Unknown browser runtime failure');
}

/** Compatibility for untyped ONNX/worker failures; normal input errors stay recoverable. */
export function isGpuRuntimeFailure(error: unknown): boolean {
    return isFatalBrowserRuntimeError(error)
        || /\b(?:OrtRun|GPUBuffer|GPUDevice|GPUValidationError|GPUOutOfMemoryError|mapAsync)\b|\bdevice\b.{0,48}\b(?:lost|destroyed|disconnected)\b/i.test(runtimeErrorDetail(error));
}
