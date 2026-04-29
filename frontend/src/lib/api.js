// src/lib/api.js
import { get } from 'svelte/store';
import { profileParam } from '$lib/stores/profileStore.js';

const BASE = '/api';

/**
 * [FIX F2] API key for authenticated requests.
 * 
 * IMPORTANT: We read the key lazily (on every request) via getApiKey()
 * instead of caching it at module-load time. During a Vite HMR restart,
 * import.meta.env may not be populated during the very first module
 * evaluation pass — causing the first fetch to go out without the key (→ 401).
 * Reading it fresh on each request guarantees the key is available as soon
 * as Vite's env injection has completed.
 */
function getApiKey() {
    return (typeof import.meta !== 'undefined' && import.meta.env?.VITE_API_KEY)
        ? import.meta.env.VITE_API_KEY
        : '';
}

/* ── Client-side response cache (TTL: 2 minutes) ── */
const CACHE_TTL = 2 * 60 * 1000; // 2 minutes
const _cache = new Map();

function getCached(key) {
    const entry = _cache.get(key);
    if (!entry) return undefined;
    if (Date.now() - entry.ts > CACHE_TTL) {
        _cache.delete(key);
        return undefined;
    }
    return entry.data;
}

function setCache(key, data) {
    _cache.set(key, { data, ts: Date.now() });
}

/** Manually invalidate the entire cache (e.g. after sync or profile switch) */
export function invalidateCache() {
    _cache.clear();
}

/**
 * Invalidate cache entries whose keys contain the given substring.
 * Useful for targeted invalidation (e.g. after enrollment, clear only
 * dashboard-related entries without nuking unrelated cached data).
 */
export function invalidateCacheByPrefix(substring) {
    for (const key of _cache.keys()) {
        if (key.includes(substring)) {
            _cache.delete(key);
        }
    }
}

/**
 * Endpoints that should NEVER have a profile parameter appended.
 * These are either global operations or return profile-agnostic data.
 */
const PROFILE_EXEMPT_ENDPOINTS = new Set([
    '/sync',
    '/sync-status',
    '/profiles',
    '/copilot/explain-category',
    '/copilot/merchants-missing-category',
    '/simplefin/claim',
    '/simplefin/connections',
    '/simplefin/connections/deactivate',
    '/simplefin/sync',
    '/migration/status',
    '/migration/preview',
    '/migration/execute',
]);

const UNCACHED_ENDPOINTS = new Set([
    '/sync-status',
]);

/**
 * Check if an endpoint path should skip profile injection.
 * Matches against the path portion only (before any query string).
 */
function isProfileExempt(endpoint) {
    // Extract path before query string
    const path = endpoint.split('?')[0];
    return PROFILE_EXEMPT_ENDPOINTS.has(path);
}

function isUncached(endpoint) {
    const path = endpoint.split('?')[0];
    return UNCACHED_ENDPOINTS.has(path);
}

/**
 * Check if a request method is a mutation (non-GET).
 * PATCH/POST/PUT/DELETE requests for things like category updates
 * and copilot questions should still get profile context where relevant.
 */
function isMutation(method) {
    return method !== 'GET';
}

/**
 * Append the active profile query parameter to an endpoint string.
 * Returns the endpoint unchanged if:
 *   - The endpoint is profile-exempt (sync, profiles)
 *   - The active profile is 'household' (empty param = show all)
 *   - We're on the server (store not available)
 */
function appendProfileParam(endpoint, method) {
    // Never append to exempt endpoints
    if (isProfileExempt(endpoint)) return endpoint;

    // Don't append to mutation endpoints EXCEPT copilot/proactive insights (which need profile scope)
    // Note: subscription endpoints that need profile pass it manually in their method definitions
    if (isMutation(method) && !endpoint.startsWith('/copilot') && !endpoint.startsWith('/proactive-insights')) return endpoint;

    try {
        const profile = get(profileParam);
        if (!profile) return endpoint; // household or empty = no filter

        const sep = endpoint.includes('?') ? '&' : '?';
        return `${endpoint}${sep}profile=${encodeURIComponent(profile)}`;
    } catch (e) {
        // get() can throw if called during SSR before stores are initialized
        return endpoint;
    }
}


/**
 * Creates a request function bound to a specific fetch implementation.
 * In load() functions, pass SvelteKit's fetch. In components, pass window.fetch (or nothing).
 */
function createRequest(fetchFn = fetch) {
    return async function request(endpoint, options = {}) {
        const method = (options.method || 'GET').toUpperCase();

        // Inject profile parameter into the endpoint
        const profiledEndpoint = appendProfileParam(endpoint, method);

        // Build cache key from the profiled endpoint (includes ?profile=X)
        // so switching profiles naturally cache-misses
        const cacheKey = method === 'GET' && !isUncached(profiledEndpoint) ? profiledEndpoint : null;

        if (cacheKey) {
            const cached = getCached(cacheKey);
            if (cached !== undefined) return cached;
        }

        const headers = { 'Content-Type': 'application/json' };
        // [FIX F2] Read API key lazily — guarantees availability after Vite env injection
        const key = getApiKey();
        if (key) headers['X-API-Key'] = key;

        const res = await fetchFn(`${BASE}${profiledEndpoint}`, {
            headers,
            ...options
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: res.statusText }));
            const detail = err.detail;
            const message = typeof detail === 'object' && detail
                ? (detail.message || 'API error')
                : (detail || 'API error');
            const error = new Error(message);
            if (typeof detail === 'object' && detail?.code) error.code = detail.code;
            error.status = res.status;
            throw error;
        }

        const data = await res.json();

        if (cacheKey) setCache(cacheKey, data);

        return data;
    };
}

/**
 * Creates an API client bound to a specific fetch implementation.
 *
 * Usage in +page.js load():
 *   const api = createApi(fetch);   // SvelteKit's fetch
 *
 * Usage in components (client-side):
 *   import { api } from '$lib/api.js';  // uses window.fetch
 */
export function createApi(fetchFn = fetch) {
    const request = createRequest(fetchFn);

    return {
        getAccounts: () => request('/accounts'),

        getTransactions: (params = {}) => {
            const query = new URLSearchParams();
            if (params.month) query.set('month', params.month);
            if (params.category) query.set('category', params.category);
            if (params.account) query.set('account', params.account);
            if (params.search) query.set('search', params.search);
            if (params.reviewed != null) query.set('reviewed', params.reviewed);
            if (params.limit != null) query.set('limit', params.limit);
            if (params.offset != null) query.set('offset', params.offset);
            const qs = query.toString();
            return request(`/transactions${qs ? '?' + qs : ''}`);
        },

        getReviewQueue: () => request('/transactions/review-queue'),

        bulkReviewTransactions: (params = {}, targetReviewed = true) => {
            const query = new URLSearchParams();
            if (params.month) query.set('month', params.month);
            if (params.category) query.set('category', params.category);
            if (params.account) query.set('account', params.account);
            if (params.search) query.set('search', params.search);
            if (params.reviewed != null) query.set('reviewed', params.reviewed);
            query.set('target_reviewed', targetReviewed ? 'true' : 'false');
            return request(`/transactions/bulk-review?${query.toString()}`, { method: 'POST' });
        },

        exportTransactions: (params = {}) => {
            const query = new URLSearchParams();
            if (params.month) query.set('month', params.month);
            if (params.category) query.set('category', params.category);
            if (params.account) query.set('account', params.account);
            if (params.search) query.set('search', params.search);
            if (params.reviewed != null) query.set('reviewed', params.reviewed);
            const qs = query.toString();
            return request(`/transactions/export${qs ? '?' + qs : ''}`);
        },

        updateCategory: (txId, category, oneOff = false) =>
            request(`/transactions/${txId}/category`, {
                method: 'PATCH',
                body: JSON.stringify({ category, one_off: oneOff })
            }),

        updateTransactionExcluded: (txId, isExcluded) =>
            request(`/transactions/${txId}/exclude`, {
                method: 'PATCH',
                body: JSON.stringify({ is_excluded: isExcluded })
            }),

        updateTransactionMetadata: (txId, payload) =>
            request(`/transactions/${txId}/metadata`, {
                method: 'PATCH',
                body: JSON.stringify(payload)
            }),

        getTransactionSplits: (txId) => request(`/transactions/${txId}/splits`),

        updateTransactionSplits: (txId, splits) =>
            request(`/transactions/${txId}/splits`, {
                method: 'PATCH',
                body: JSON.stringify({ splits })
            }),

        getCategories: () => request('/categories'),

        getCategoriesMeta: () => request('/categories/meta'),

        createCategory: (name) =>
            request('/categories', {
                method: 'POST',
                body: JSON.stringify({ name })
            }),

        updateCategoryParent: (categoryName, parentCategory) =>
            request(`/categories/${encodeURIComponent(categoryName)}/parent`, {
                method: 'PATCH',
                body: JSON.stringify({ parent_category: parentCategory || null })
            }),

        getCategoryRules: (source) =>
            request(`/category-rules${source ? '?source=' + source : ''}`),

        updateCategoryRule: (ruleId, payload) =>
            request(`/category-rules/${ruleId}`, {
                method: 'PATCH',
                body: JSON.stringify(payload)
            }),

        getCategoryRuleImpact: (ruleId, limit = 20) => {
            const params = new URLSearchParams();
            params.set('limit', String(limit));
            return request(`/category-rules/${ruleId}/impact?${params.toString()}`);
        },

        getMonthlyAnalytics: () => request('/analytics/monthly'),
        getCategoryAnalytics: (month) =>
            request(`/analytics/categories${month ? '?month=' + month : ''}`),

        getSummary: () => request('/summary'),

        getProfiles: () => request('/profiles'),

        getAppConfig: () => request('/app-config'),
        getLocalLlmCatalog: () => request('/local-llm/catalog'),
        getLocalLlmStatus: () => request('/local-llm/status'),
        updateLocalLlmSettings: (payload) =>
            request('/local-llm/settings', {
                method: 'PATCH',
                body: JSON.stringify(payload),
            }),
        installLocalLlmModel: (model) =>
            request('/local-llm/install', {
                method: 'POST',
                body: JSON.stringify({ model }),
            }),

        sync: async () => {
            const result = await request('/sync', { method: 'POST' });
            invalidateCache();
            return result;
        },

        getSyncStatus: () => request('/sync-status'),
        getDataHealth: () => request('/data-health'),
        getScheduledTransactions: (days = 45) => request(`/scheduled-transactions?days=${days}`),
        getCashFlowForecast: (days = 90) => request(`/analytics/cash-flow-forecast?days=${days}`),
        explainMonth: (month, useLlm = true, profile = null) => {
            const params = new URLSearchParams();
            const activeProfile = profile || get(profileParam);
            if (activeProfile) params.set('profile', activeProfile);
            const qs = params.toString();
            return request(`/analytics/explain-month${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({ month, use_llm: useLlm })
            });
        },
        getInvestments: () => request('/investments'),
        createInvestmentHolding: (payload) =>
            request('/investments/holdings', {
                method: 'POST',
                body: JSON.stringify(payload)
            }),
        updateInvestmentHolding: (holdingId, payload) =>
            request(`/investments/holdings/${holdingId}`, {
                method: 'PATCH',
                body: JSON.stringify(payload)
            }),
        deleteInvestmentHolding: (holdingId) =>
            request(`/investments/holdings/${holdingId}`, { method: 'DELETE' }),
        getBackupStatus: () => request('/backup/status'),
        exportBackup: async (includeCredentials = false) => {
            const endpoint = appendProfileParam(`/backup/export?include_credentials=${includeCredentials ? 'true' : 'false'}`, 'GET');
            const headers = {};
            const key = getApiKey();
            if (key) headers['X-API-Key'] = key;
            const res = await fetchFn(`${BASE}${endpoint}`, { headers });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                throw new Error(err.detail || 'API error');
            }
            const blob = await res.blob();
            const disposition = res.headers.get('Content-Disposition') || '';
            const match = disposition.match(/filename="([^"]+)"/);
            return { blob, filename: match?.[1] || 'folio-backup.json' };
        },

        askCopilot: (question, profile, history = null) => {
            // Build endpoint with profile param for copilot context
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/ask${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({ question, history })
            });
        },

        /**
         * Streaming variant — returns a cancel() function and invokes `onEvent`
         * for each SSE event from the agent. Caller should handle:
         *   { type: 'reset_text' }
         *   { type: 'controller', controller_act }
         *   { type: 'action', domain_action }
         *   { type: 'progress', stage, label }
         *   { type: 'token', text }
         *   { type: 'tool_call', name, args }
         *   { type: 'tool_result', name, duration_ms }
         *   { type: 'done', answer, data, data_source, tool_trace, iterations }
         *   { type: 'error', message }
         */
        askCopilotStream: (question, profile, history, onEvent) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            const controller = new AbortController();

            (async () => {
                try {
                    const resp = await fetchFn(`${BASE}/copilot/ask/stream${qs ? '?' + qs : ''}`, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'X-API-Key': getApiKey(),
                        },
                        body: JSON.stringify({ question, history }),
                        signal: controller.signal,
                    });
                    if (!resp.ok || !resp.body) {
                        onEvent({ type: 'error', message: `HTTP ${resp.status}` });
                        return;
                    }
                    const reader = resp.body.getReader();
                    const decoder = new TextDecoder();
                    let buffer = '';
                    while (true) {
                        const { value, done } = await reader.read();
                        if (done) break;
                        buffer += decoder.decode(value, { stream: true });
                        let boundary;
                        while ((boundary = buffer.indexOf('\n\n')) !== -1) {
                            const chunk = buffer.slice(0, boundary);
                            buffer = buffer.slice(boundary + 2);
                            const line = chunk.split('\n').find(l => l.startsWith('data:'));
                            if (!line) continue;
                            const json = line.slice(5).trim();
                            if (!json) continue;
                            try {
                                onEvent(JSON.parse(json));
                            } catch {}
                        }
                    }
                } catch (err) {
                    if (err?.name !== 'AbortError') {
                        onEvent({ type: 'error', message: err?.message || 'stream failed' });
                    }
                }
            })();

            return () => controller.abort();
        },

        /**
         * Send confirmation_id only. The backend stores a structured operation
         * behind the nonce and executes it after confirmation.
         */
        confirmCopilotWrite: (question, confirmationId, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/confirm${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({ question, confirmation_id: confirmationId })
            });
        },

        saveInsight: (question, answer, kind = 'insight', sourceConversationId = null, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/insights${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({
                    question,
                    answer,
                    kind,
                    source_conversation_id: sourceConversationId
                })
            });
        },
        getSavedInsights: (limit = 20, profile = null) => {
            const params = new URLSearchParams();
            params.set('limit', String(limit));
            if (profile && profile !== 'household') params.set('profile', profile);
            return request(`/copilot/insights?${params.toString()}`);
        },

        // ── Persistent memory (about_user.md) ──

        getMemoryEntries: (profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/entries${qs ? '?' + qs : ''}`);
        },
        createMemoryEntry: (entry, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/entries${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify(entry),
            });
        },
        updateMemoryEntry: (entryId, body, evidence = null, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/entries/${entryId}${qs ? '?' + qs : ''}`, {
                method: 'PATCH',
                body: JSON.stringify({ body, evidence }),
            });
        },
        deleteMemoryEntry: (entryId, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/entries/${entryId}${qs ? '?' + qs : ''}`, { method: 'DELETE' });
        },
        getMemoryMarkdown: (profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/markdown${qs ? '?' + qs : ''}`);
        },
        getMemoryProposals: (profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/proposals${qs ? '?' + qs : ''}`);
        },
        acceptMemoryProposal: (proposalId, overrides = null, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/proposals/${proposalId}/accept${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify(overrides || {}),
            });
        },
        rejectMemoryProposal: (proposalId, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/proposals/${proposalId}/reject${qs ? '?' + qs : ''}`, { method: 'POST' });
        },
        consolidateMemory: (profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/memory/consolidate${qs ? '?' + qs : ''}`, { method: 'POST' });
        },

        getCopilotHistory: (limit = 40, profile = null) => {
            const params = new URLSearchParams();
            params.set('limit', String(limit));
            if (profile && profile !== 'household') params.set('profile', profile);
            return request(`/copilot/history?${params.toString()}`);
        },
        clearCopilotHistory: (profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/history${qs ? '?' + qs : ''}`, { method: 'DELETE' });
        },
        deleteCopilotHistoryItem: (id, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/history/${id}${qs ? '?' + qs : ''}`, { method: 'DELETE' });
        },

        getCopilotDataBrowser: (table, search = '', limit = 100) => {
            const params = new URLSearchParams();
            params.set('table', table);
            params.set('limit', String(limit));
            if (search) params.set('search', search);
            return request(`/copilot/data-browser?${params.toString()}`);
        },

        // ── Deterministic copilot tools (chip prompts) ──

        explainCategory: (merchant, profile) => {
            const params = new URLSearchParams();
            params.set('merchant', merchant);
            if (profile && profile !== 'household') params.set('profile', profile);
            return request(`/copilot/explain-category?${params.toString()}`);
        },

        getMerchantsMissingCategory: (profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            return request(`/copilot/merchants-missing-category?${params.toString()}`);
        },

        bulkRecategorizePreview: (merchantQuery, newCategory, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/bulk-recategorize-preview${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({ merchant_query: merchantQuery, new_category: newCategory })
            });
        },

        previewRuleCreation: (pattern, category, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/preview-rule${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({ pattern, category })
            });
        },

        renameMerchantPreview: (oldName, newName, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/copilot/rename-merchant-preview${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({ old_name: oldName, new_name: newName })
            });
        },

        getRecentTransactions: async (months = 4) => {
            const now = new Date();
            const promises = [];
            for (let i = 0; i < months; i++) {
                const d = new Date(now.getFullYear(), now.getMonth() - i);
                const monthStr = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
                promises.push(
                    request(`/transactions?month=${monthStr}&limit=1000`)
                        .then(res => (res && Array.isArray(res.data)) ? res.data : [])
                        .catch(() => [])
                );
            }
            const results = await Promise.all(promises);
            return results.flat();
        },

        getNetWorthSeries: (interval = 'weekly') =>
            request(`/analytics/net-worth-series?interval=${interval}`),

        getDashboardBundle: (nwInterval = 'biweekly', month = null) => {
            const params = new URLSearchParams();
            params.set('nw_interval', nwInterval);
            if (month) params.set('month', month);
            return request(`/dashboard-bundle?${params.toString()}`);
        },

        getProactiveInsights: (includeDismissed = false) =>
            request(`/proactive-insights${includeDismissed ? '?include_dismissed=true' : ''}`),

        dismissProactiveInsight: (id) =>
            request(`/proactive-insights/${encodeURIComponent(id)}/dismiss`, { method: 'POST' }),

        restoreProactiveInsight: (id) =>
            request(`/proactive-insights/${encodeURIComponent(id)}/restore`, { method: 'POST' }),

        getSankeyData: (month) =>
            request(`/analytics/sankey${month ? '?month=' + month : ''}`),

        getMerchants: (month) =>
            request(`/merchants${month ? '?month=' + month : ''}`),

        getRecurring: () =>
            request('/analytics/recurring'),

        updateExpenseType: (categoryName, expenseType) =>
            request(`/categories/${encodeURIComponent(categoryName)}/expense-type`, {
                method: 'PATCH',
                body: JSON.stringify({ expense_type: expenseType })
            }),

        confirmSubscription: (merchant, pattern, frequencyHint, category, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/subscriptions/confirm${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({
                    merchant,
                    pattern: pattern || null,
                    frequency_hint: frequencyHint || 'monthly',
                    category: category || 'Subscriptions'
                })
            });
        },

        dismissSubscription: (merchant, pattern, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/subscriptions/dismiss${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify({
                    merchant,
                    pattern: pattern || null
                })
            });
        },

        declareSubscription: (merchant, amount, frequency, profile, category = 'Subscriptions', expectedDay = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            const payload = { merchant, amount, frequency, category, profile: profile || null };
            if (expectedDay !== null && expectedDay !== undefined && expectedDay !== '') {
                payload.expected_day = Number(expectedDay);
            }
            return request(`/subscriptions/declare${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify(payload)
            });
        },

        dismissSubscriptionAmountReview: (merchant, suggestedAmount, latestDate, profile) => {
            return request('/subscriptions/amount-review/dismiss', {
                method: 'POST',
                body: JSON.stringify({
                    merchant,
                    suggested_amount: suggestedAmount,
                    latest_date: latestDate,
                    profile: profile || null
                })
            });
        },

        cancelSubscription: (merchant, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/subscriptions/${encodeURIComponent(merchant)}/cancel${qs ? '?' + qs : ''}`, {
                method: 'POST'
            });
        },

        restoreSubscription: (merchant, profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/subscriptions/${encodeURIComponent(merchant)}/restore${qs ? '?' + qs : ''}`, {
                method: 'POST'
            });
        },

        getDismissedSubscriptions: (profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/subscriptions/dismissed${qs ? '?' + qs : ''}`);
        },

        getSubscriptionEvents: (profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/subscriptions/events${qs ? '?' + qs : ''}`);
        },

        markEventsRead: (eventIds) =>
            request('/subscriptions/events/mark-read', {
                method: 'POST',
                body: JSON.stringify({ event_ids: eventIds })
            }),

        redetectSubscriptions: async (profile) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            const result = await request(`/subscriptions/redetect${qs ? '?' + qs : ''}`, {
                method: 'POST'
            });
            invalidateCacheByPrefix('analytics/recurring');
            invalidateCacheByPrefix('scheduled-transactions');
            invalidateCacheByPrefix('dashboard-bundle');
            invalidateCacheByPrefix('cash-flow-forecast');
            return result;
        },

        getBudgets: () => request('/budgets'),

        updateBudget: (categoryName, amount, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/budgets/${encodeURIComponent(categoryName)}${qs ? '?' + qs : ''}`, {
                method: 'PATCH',
                body: JSON.stringify(typeof amount === 'object' ? amount : { amount })
            });
        },

        getGoals: () => request('/goals'),
        createGoal: (payload, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/goals${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify(payload)
            });
        },
        updateGoal: (id, payload, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/goals/${id}${qs ? '?' + qs : ''}`, {
                method: 'PATCH',
                body: JSON.stringify(payload)
            });
        },
        deleteGoal: (id, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/goals/${id}${qs ? '?' + qs : ''}`, { method: 'DELETE' });
        },

        createManualAccount: (payload, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/manual-accounts${qs ? '?' + qs : ''}`, {
                method: 'POST',
                body: JSON.stringify(payload)
            });
        },
        updateManualAccount: (id, payload, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/manual-accounts/${encodeURIComponent(id)}${qs ? '?' + qs : ''}`, {
                method: 'PATCH',
                body: JSON.stringify(payload)
            });
        },
        deleteManualAccount: (id, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/manual-accounts/${encodeURIComponent(id)}${qs ? '?' + qs : ''}`, { method: 'DELETE' });
        },

        getMerchantDirectory: (search = '', limit = 50) => {
            const params = new URLSearchParams();
            params.set('limit', String(limit));
            if (search) params.set('search', search);
            return request(`/merchant-directory?${params.toString()}`);
        },

        getMerchantTransactions: (merchantKey, profileId = null, limit = 25) => {
            const params = new URLSearchParams();
            params.set('limit', String(limit));
            if (profileId) params.set('profile_id', profileId);
            return request(`/merchant-directory/${encodeURIComponent(merchantKey)}/transactions?${params.toString()}`);
        },

        updateMerchantDirectory: (merchantKey, payload) =>
            request(`/merchant-directory/${encodeURIComponent(merchantKey)}`, {
                method: 'PATCH',
                body: JSON.stringify(payload)
            }),

        getTellerConfig: () => request('/teller-config'),

        enrollAccount: (accessToken, institutionName, enrollmentId) =>
            request('/enroll', {
                method: 'POST',
                body: JSON.stringify({
                    accessToken,
                    institutionName: institutionName || 'Unknown',
                    enrollmentId: enrollmentId || null,
                }),
            }),

        getEnrollments: () => request('/enrollments'),

        deactivateEnrollment: (id) =>
            request('/enrollments/deactivate', {
                method: 'POST',
                body: JSON.stringify({ id })
            }),

        // ── SimpleFIN Bridge ──

        claimSimpleFIN: (setupToken, profile, displayName = '') =>
            request('/simplefin/claim', {
                method: 'POST',
                body: JSON.stringify({ setupToken, profile, displayName }),
            }),

        getSimpleFINConnections: () => request('/simplefin/connections'),

        deactivateSimpleFINConnection: (id) =>
            request('/simplefin/connections/deactivate', {
                method: 'POST',
                body: JSON.stringify({ id }),
            }),

        syncSimpleFIN: async () => {
            const result = await request('/simplefin/sync', { method: 'POST' });
            invalidateCache();
            return result;
        },

        // ── Provider Migration ──

        getMigrationStatus: () => request('/migration/status'),

        getMigrationPreview: () => request('/migration/preview'),

        executeMigration: (mappings, deactivateTeller = true) =>
            request('/migration/execute', {
                method: 'POST',
                body: JSON.stringify({ mappings, deactivate_teller: deactivateTeller }),
            }),

        // ── Mira Receipt Intelligence ──

        parseReceipt: async (file, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            const form = new FormData();
            form.append('file', file);
            const headers = {};
            const key = getApiKey();
            if (key) headers['X-API-Key'] = key;
            const res = await fetchFn(`${BASE}/receipts/parse${qs ? '?' + qs : ''}`, {
                method: 'POST',
                headers,
                body: form,
            });
            if (!res.ok) {
                const err = await res.json().catch(() => ({ detail: res.statusText }));
                throw new Error(err.detail || 'Receipt parse failed');
            }
            return res.json();
        },

        getReceipt: (receiptId, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/receipts/${receiptId}${qs ? '?' + qs : ''}`);
        },

        updateReceiptItems: (receiptId, items, profile = null, metadata = {}) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/receipts/${receiptId}/items${qs ? '?' + qs : ''}`, {
                method: 'PATCH',
                body: JSON.stringify({ items, ...metadata }),
            });
        },

        approveReceipt: (receiptId, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/receipts/${receiptId}/approve${qs ? '?' + qs : ''}`, { method: 'POST' });
        },

        discardReceipt: (receiptId, profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/receipts/${receiptId}/discard${qs ? '?' + qs : ''}`, { method: 'POST' });
        },

        getReceiptComparisons: (profile = null) => {
            const params = new URLSearchParams();
            if (profile && profile !== 'household') params.set('profile', profile);
            const qs = params.toString();
            return request(`/receipts/comparisons${qs ? '?' + qs : ''}`);
        },
    };
}

/** Default client-side API instance (uses window.fetch) */
export const api = createApi();
