/** What every Refresh button of the app says after it ran (hand test 2026-10-04, A3, and its review, K42). */
export const refreshText = {
    unchanged: (time: string) => `Up to date at ${time}: no changes since the last load`,
    /** Without a reason when the view already shows the error in its own words beside the button. */
    failed: (time: string, error: string) => (error ? `Refresh failed at ${time}: ${error}` : `Refresh failed at ${time}`),
};
