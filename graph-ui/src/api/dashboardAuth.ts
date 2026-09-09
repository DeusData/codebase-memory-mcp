const TOKEN_PATTERN = /^[0-9a-f]{64}$/;
const SESSION_KEY = "cbm.dashboard.capability";

let dashboardToken: string | null = null;
let environmentChecked = false;

function validToken(value: string | null | undefined): value is string {
  return typeof value === "string" && TOKEN_PATTERN.test(value);
}

function captureFromEnvironment(): void {
  if (environmentChecked) return;
  environmentChecked = true;

  if (typeof window === "undefined") return;

  const fragment = window.location.hash.startsWith("#")
    ? window.location.hash.slice(1)
    : window.location.hash;
  const fromFragment = new URLSearchParams(fragment).get("token");
  const fromSession = window.sessionStorage?.getItem(SESSION_KEY);
  const candidate = validToken(fromFragment)
    ? fromFragment
    : validToken(fromSession)
      ? fromSession
      : null;

  if (candidate) {
    dashboardToken = candidate;
    window.sessionStorage?.setItem(SESSION_KEY, candidate);
  }

  /* A fragment is not sent over HTTP, but removing it after capture also keeps
   * the capability out of screenshots and copy/pasted URLs. */
  if (fromFragment && window.history?.replaceState) {
    window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}`);
  }
}

export function setDashboardToken(token: string): boolean {
  if (!validToken(token)) return false;
  dashboardToken = token;
  environmentChecked = true;
  if (typeof window !== "undefined") {
    window.sessionStorage?.setItem(SESSION_KEY, token);
  }
  return true;
}

export function clearDashboardToken(): void {
  dashboardToken = null;
  environmentChecked = false;
  if (typeof window !== "undefined") {
    window.sessionStorage?.removeItem(SESSION_KEY);
  }
}

export function authenticatedPath(path: string): string {
  captureFromEnvironment();
  if (!dashboardToken) {
    throw new Error("Dashboard authentication token is missing");
  }
  const separator = path.includes("?") ? "&" : "?";
  return `${path}${separator}token=${encodeURIComponent(dashboardToken)}`;
}

export async function dashboardFetch(
  path: string,
  init?: RequestInit,
): Promise<Response> {
  return fetch(authenticatedPath(path), init);
}
