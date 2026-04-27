/**
 * hooks.server.js
 * Server-side hook that proxies /api/* requests to the backend service.
 *
 * In development, Vite's server.proxy handles this.
 * In production (Docker), there's no Vite dev server — SvelteKit's
 * Node adapter serves the app directly. This hook forwards API
 * requests to the backend container.
 */

const BACKEND_URL = process.env.BACKEND_URL || 'http://localhost:8000';

/** Headers that should NOT be forwarded to the backend */
const HOP_BY_HOP = new Set([
    'connection',
    'keep-alive',
    'transfer-encoding',
    'te',
    'trailer',
    'upgrade',
    'host',
]);

export async function handle({ event, resolve }) {
    // Only intercept /api/* requests
    if (!event.url.pathname.startsWith('/api')) {
        return resolve(event);
    }

    const targetUrl = `${BACKEND_URL}${event.url.pathname}${event.url.search}`;

    try {
        // Build headers — forward everything except hop-by-hop
        const forwardHeaders = new Headers();
        for (const [key, value] of event.request.headers.entries()) {
            if (!HOP_BY_HOP.has(key.toLowerCase())) {
                forwardHeaders.set(key, value);
            }
        }

        // Determine body handling
        const method = event.request.method;
        let body = null;
        if (method !== 'GET' && method !== 'HEAD') {
            body = await event.request.arrayBuffer();
        }

        // Enroll/sync can take 6+ minutes (LLM categorization batches).
        // Use a generous timeout for long-running endpoints, default for others.
        const isLongRunning = targetUrl.includes('/api/enroll') || targetUrl.includes('/api/sync');
        const timeoutMs = isLongRunning ? 10 * 60 * 1000 : 2 * 60 * 1000; // 10min / 2min

        const backendResponse = await fetch(targetUrl, {
            method,
            headers: forwardHeaders,
            body,
            signal: AbortSignal.timeout(timeoutMs),
        });

        // Build response — forward backend headers
        const responseHeaders = new Headers();
        for (const [key, value] of backendResponse.headers.entries()) {
            if (!HOP_BY_HOP.has(key.toLowerCase())) {
                responseHeaders.set(key, value);
            }
        }

        return new Response(backendResponse.body, {
            status: backendResponse.status,
            statusText: backendResponse.statusText,
            headers: responseHeaders,
        });
    } catch (err) {
        console.error(`[hooks.server.js] Proxy error for ${targetUrl}:`, err.message);
        return new Response(
            JSON.stringify({ detail: 'Backend unavailable' }),
            {
                status: 502,
                headers: { 'Content-Type': 'application/json' },
            }
        );
    }
}
