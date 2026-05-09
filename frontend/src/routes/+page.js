export const ssr = false;

function withTimeout(promise, ms) {
    return Promise.race([
        promise,
        new Promise((_, reject) => setTimeout(() => reject(new Error('prefetch_timeout')), ms))
    ]);
}

export async function load({ fetch }) {
    let bundle;

    // Use the pre-fired fetch from app.html if available, but don't block forever
    if (typeof window !== 'undefined' && window.__dashboardData) {
        try {
            bundle = await withTimeout(window.__dashboardData, 4000);
        } catch (_) {
            bundle = null;
        }
        // Clear it so it's not reused on client-side navigation
        window.__dashboardData = null;
    }

    // Fallback if prefetch failed or timed out
    if (!bundle) {
        const { createApi } = await import('$lib/api.js');
        const loadApi = createApi(fetch);
        bundle = await loadApi.getDashboardBundle('biweekly');
    }

    return {
        summary: bundle.summary || {},
        accounts: bundle.accounts || [],
        monthly: bundle.monthly || [],
        categories: bundle.categories || [],
        netWorthSeries: Array.isArray(bundle.netWorthSeries) ? bundle.netWorthSeries : [],
        netWorthMomDelta: bundle.netWorthMomDelta ?? null,
        netWorthYtdDelta: bundle.netWorthYtdDelta ?? null,
        savingsTransferTotal: bundle.savingsTransferTotal || 0,
        personalTransferTotal: bundle.personalTransferTotal || 0,
        monthlyCategoryBreakdown: Array.isArray(bundle.monthlyCategoryBreakdown) ? bundle.monthlyCategoryBreakdown : [],
        ccRepaid: bundle.ccRepaid || 0,
        externalTransfers: bundle.externalTransfers || 0,
        incomingTransfers: bundle.incomingTransfers || 0,
        cashDeposits: bundle.cashDeposits || 0,
        cashWithdrawals: bundle.cashWithdrawals || 0,
        investmentInflows: bundle.investmentInflows || 0,
        investmentOutflows: bundle.investmentOutflows || 0,
        creditsRefunds: bundle.creditsRefunds || bundle.summary?.credits_refunds || bundle.summary?.refunds || 0,
        planSnapshot: bundle.planSnapshot || null,
        reviewQueue: bundle.reviewQueue || null,
        dataHealth: bundle.dataHealth || null,
        scheduled: bundle.scheduled || null,
        cashFlowForecast: bundle.cashFlowForecast || null,
        investments: bundle.investments || null,
        config: bundle.config || null,
    };
}
