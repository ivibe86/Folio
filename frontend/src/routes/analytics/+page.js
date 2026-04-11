// src/routes/analytics/+page.js
import { createApi } from '$lib/api.js';

export async function load({ fetch, depends }) {
    depends('app:analytics');

    const api = createApi(fetch);

    const [monthly, categories] = await Promise.all([
        api.getMonthlyAnalytics(),
        api.getCategoryAnalytics()
    ]);

    return {
        monthly,
        categories
    };
}