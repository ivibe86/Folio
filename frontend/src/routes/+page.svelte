<script>
    import '$lib/styles/dashboard.css';
    import { onMount, tick } from 'svelte';
    import { api } from '$lib/api.js';
    import {
        formatCurrency, formatCompact, formatPercent, formatMonth, formatMonthShort,
        formatDate, getCurrentMonth, computeDelta, getGreeting,
        groupTransactionsByDate, CATEGORY_COLORS, CATEGORY_ICONS, springCount,
        computeTrailingSavingsRate
    } from '$lib/utils.js';
    import { darkMode, syncing, selectedPeriodStore, selectedCustomMonthStore, privacyMode } from '$lib/stores.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import SankeyChart from '$lib/components/SankeyChart.svelte';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';
    import TellerConnect from '$lib/components/TellerConnect.svelte';    

    export let data;
    // Accept (and ignore) the 'params' prop that SvelteKit may pass to pages
    export let params = {};
    let summary = data.summary;
    let accounts = data.accounts;
    let monthly = data.monthly;
    let categories = data.categories;
    let loading = !data.summary;
    let profileSwitching = false;

    // Teller Connect configuration (fetched from backend)
    let tellerAppId = '';
    let tellerEnvironment = 'development';

    // Period — synced with global store for cross-page persistence
    let selectedPeriod = 'this_month';
    let selectedCustomMonth = '';

    // Hydrate from store on init
    selectedPeriodStore.subscribe(v => { selectedPeriod = v; });
    selectedCustomMonthStore.subscribe(v => { if (v) selectedCustomMonth = v; });
    let allMonths = [];
    let periodLoading = false;

    let periodSummary = null;
    let periodCategories = [];
    let sankeyCategoryList = [];

    // Sankey: separated flows
    let sankeySavingsTotal = 0;
    let sankeyPersonalTransferTotal = 0;

    // Sankey drill-down
    let selectedSankeyCategory = null;
    let sankeyDrillTxns = [];
    let drillDownSection;
    let monthDropdownOpen = false;

    // iOS-style period toggle
    const periodOptions = [
        { key: 'this_month', label: 'This Month' },
        { key: 'last_month', label: 'Last Month' },
        { key: 'ytd', label: 'YTD' },
        { key: 'all', label: 'All Time' }
    ];
    $: activePeriodIdx = Math.max(periodOptions.findIndex(p => p.key === selectedPeriod), 0);

    // Animation
    // Seed from load() data so the first paint shows real values — no flash of zeros.
    // The spring animation will run on mount to add polish, starting FROM these values
    // or from 0 if you still want the count-up effect.
    let animatedNetWorth = 0;
    let animatedIncome = 0;
    let animatedExpenses = 0;
    let animationDone = false;
    let animationStarted = false;
    let mounted = false;
    let animationFrameId = null;

    // Net worth trend data (full area chart)
    let netWorthTrendData = [];

    // Hover tooltip for net worth chart
    let hoverPoint = null;

    // Hover tooltip for monthly net bar chart
    let hoveredBarIdx = -1;
    let hoveredBar = null;

    // Trailing savings rate
    let trailingSavingsInfo = { rate: 0, delta: 0, months: 0 };

    // Account deltas (month-over-month per account)
    let accountDeltas = {};

    // Categories to exclude from expenses (flow directly from income)
    const DIRECT_FLOW_CATEGORIES = ['Savings Transfer', 'Personal Transfer'];

    // Sankey theater mouse-tracking glow
    let sankeyTheaterEl = null;
    let sankeyGlowOpacity = 0;

    function handleTheaterMouseMove(e) {
        if (!sankeyTheaterEl) return;
        const rect = sankeyTheaterEl.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;
        sankeyTheaterEl.style.setProperty('--theater-mx', `${mx}px`);
        sankeyTheaterEl.style.setProperty('--theater-my', `${my}px`);
        sankeyGlowOpacity = 1;
    }

    function handleTheaterMouseLeave() {
        sankeyGlowOpacity = 0;
    }

    // ── Scroll overflow detection for hero account lists ──
    function checkScrollOverflow(node) {
        function update() {
            const wrapper = node.closest('.hero-account-scroll-wrapper');
            if (wrapper) {
                if (node.scrollHeight > node.clientHeight + 2) {
                    wrapper.classList.add('has-overflow');
                } else {
                    wrapper.classList.remove('has-overflow');
                }
            }
        }
        // Check after render and on resize
        requestAnimationFrame(update);
        const observer = new ResizeObserver(update);
        observer.observe(node);
        return {
            update() { requestAnimationFrame(update); },
            destroy() { observer.disconnect(); }
        };
    }

    /* ——— Cyan trend line constants ——— */
    /* âââ Cyan trend line constants âââ */
    /* These are used as defaults; theme-aware overrides applied via reactive statement */
    let CYAN_LINE = '#38BDF8';
    let CYAN_GLOW = '#7DD3FC';
    let CYAN_DEEP = '#0369A1';
    let TEAL_AREA = '#4AEDC4';
    let TEAL_AREA_GLOW = '#2DD4A8';

    /* Theme-aware NW chart colors
       Both modes now use luminous cyan — the light-mode hero card
       is a dark island so the chart needs the same bright treatment */
    $: {
        if ($darkMode) {
            CYAN_LINE = '#38BDF8';
            CYAN_GLOW = '#7DD3FC';
            CYAN_DEEP = '#0369A1';
            TEAL_AREA = '#4AEDC4';
            TEAL_AREA_GLOW = '#2DD4A8';
        } else {
            /* Dark island in light mode — use luminous colors */
            CYAN_LINE = '#38BDF8';
            CYAN_GLOW = '#7DD3FC';
            CYAN_DEEP = '#0369A1';
            TEAL_AREA = '#4AEDC4';
            TEAL_AREA_GLOW = '#2DD4A8';
        }
    }

    // ── Seed derived state from load() data ──────────────────────
    {
        allMonths = [...monthly].sort((a, b) => b.month.localeCompare(a.month)).map(m => m.month);
        if (allMonths.length > 0) selectedCustomMonth = allMonths[0];

        // Net worth trend: use pre-fetched real balance history from load().
        // No more cumulative monthly-net fallback — that produced a visually
        // incorrect chart shape (the "ghost chart" on initial load).
        netWorthTrendData = (Array.isArray(data.netWorthSeries) && data.netWorthSeries.length >= 2)
            ? data.netWorthSeries.map(d => ({ month: d.month || d.date, value: d.value }))
            : [];

        // Trailing savings rate
        trailingSavingsInfo = computeTrailingSavingsRate(monthly, 3);
    }

    onMount(async () => {
        mounted = true;
        try {
            // ââ Guard: if the load() data is empty (401 race condition),
            //    retry the dashboard bundle before bootstrapping âââââââââ
            if (!summary || !accounts || accounts.length === 0) {
                const bundle = await fetchBundleWithRetry();
                if (bundle) {
                    summary = bundle.summary;
                    accounts = bundle.accounts;
                    monthly = bundle.monthly;
                    categories = bundle.categories;
                    netWorthTrendData = (Array.isArray(bundle.netWorthSeries) && bundle.netWorthSeries.length >= 2)
                        ? bundle.netWorthSeries.map(d => ({ month: d.month || d.date, value: d.value }))
                        : [];
                    trailingSavingsInfo = computeTrailingSavingsRate(monthly, 3);
                    allMonths = [...monthly].sort((a, b) => b.month.localeCompare(a.month)).map(m => m.month);
                    if (allMonths.length > 0) selectedCustomMonth = allMonths[0];
                }
            }

            await computeAccountDeltas();
            bootstrapInitialPeriod();

            requestAnimationFrame(() => animateNumbers());

            await tick();
            // Only mark complete if we actually have data to show
            initialLoadComplete = !!(summary && accounts && accounts.length > 0);

            // Fetch Teller Connect configuration (non-blocking)
            try {
                const cfg = await api.getTellerConfig();
                tellerAppId = cfg.applicationId || '';
                tellerEnvironment = cfg.environment || 'sandbox';
            } catch (_) {
                // Teller Connect will just be disabled if config isn't available
            }
        } catch (e) {
            console.error('Failed to initialize dashboard:', e);
        }
    });

    /**
     * Handle successful Teller Connect enrollment.
     * Reloads the entire dashboard to include the newly linked accounts.
     */
    async function handleTellerEnrolled(event) {
        const result = event.detail;
        console.info('Teller enrollment success:', result);

        // Full dashboard reload to pick up new accounts
        try {
            const bundle = await api.getDashboardBundle();
            summary = bundle.summary;
            accounts = bundle.accounts;
            monthly = bundle.monthly;
            categories = bundle.categories;
            netWorthTrendData = (Array.isArray(bundle.netWorthSeries) && bundle.netWorthSeries.length >= 2)
                ? bundle.netWorthSeries.map(d => ({ month: d.month || d.date, value: d.value }))
                : [];
            trailingSavingsInfo = computeTrailingSavingsRate(monthly, 3);
            allMonths = [...monthly].sort((a, b) => b.month.localeCompare(a.month)).map(m => m.month);
            if (allMonths.length > 0 && !allMonths.includes(selectedCustomMonth)) {
                selectedCustomMonth = allMonths[0];
            }
            await computeAccountDeltas();
            bootstrapInitialPeriod();
            netWorthAnimationDone = false;
            animationDone = false;
            animatedNetWorth = 0;
            animatedIncome = 0;
            animatedExpenses = 0;
            requestAnimationFrame(() => animateNumbers());
        } catch (e) {
            console.error('Failed to reload after enrollment:', e);
        }
    }

    /**
    * Retry wrapper for dashboard-bundle fetch.
    * Handles the race condition where the very first request fires
    * before Vite's env injection is complete (API key = undefined → 401).
    */
    async function fetchBundleWithRetry(retries = 3, delayMs = 300) {
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const bundle = await api.getDashboardBundle();
                // Validate we got real data back, not an empty/error response
                if (bundle && bundle.summary && bundle.accounts) {
                    return bundle;
                }
                throw new Error('Empty bundle response');
            } catch (err) {
                console.warn(`Dashboard bundle attempt ${attempt}/${retries} failed:`, err);
                if (attempt < retries) {
                    await new Promise(resolve => setTimeout(resolve, delayMs * attempt));
                }
            }
        }
        console.error('Could not load dashboard bundle after retries');
        return null;
    }

    /**
     * Seed the Sankey / metric ribbon from already-loaded bundle data.
     * This avoids 3 extra API round-trips on initial page load.
     * Only used for the FIRST render — subsequent period changes still
     * call updatePeriod() which fetches fresh data for the selected month.
     */
    function bootstrapInitialPeriod() {
        // The default period is 'this_month', so compute for current month
        const targetMonth = getMonthForPeriod(selectedPeriod);

        // Extract savings + personal transfer totals from bundle categories
        const savCat = categories.find(c => c.category === 'Savings Transfer');
        const ptCat = categories.find(c => c.category === 'Personal Transfer');

        // For 'this_month' or any single-month period, we need month-specific
        // data. The bundle categories are all-time. But we have monthly data
        // from the bundle too, so we can estimate.
        //
        // The most accurate fast path: use all-time categories (which match
        // "all" period) or the monthly income from the bundle for the target month.
        // For the initial load, we use the all-time categories as a close
        // approximation and let the user's period selection trigger a precise
        // refresh if they switch.

        let inc = 0;
        if (selectedPeriod === 'all') {
            inc = summary?.income || 0;
            periodCategories = categories;
            sankeySavingsTotal = savCat?.total || 0;
            sankeyPersonalTransferTotal = ptCat?.total || 0;
        } else if (selectedPeriod === 'ytd') {
            const year = new Date().getFullYear().toString();
            const ytdMonths = monthly.filter(m => m.month.startsWith(year));
            inc = ytdMonths.reduce((s, m) => s + m.income, 0);
            periodCategories = categories;
            sankeySavingsTotal = savCat?.total || 0;
            sankeyPersonalTransferTotal = ptCat?.total || 0;
        } else {
            // this_month, last_month, custom — use monthly data from bundle
            const m = monthly.find(m => m.month === targetMonth);
            inc = m ? m.income : 0;
            // For single-month, we don't have month-specific categories from
            // the bundle (it's all-time). Fire a lightweight background refresh.
            periodCategories = categories;
            sankeySavingsTotal = savCat?.total || 0;
            sankeyPersonalTransferTotal = ptCat?.total || 0;

            // Kick off the precise month-specific fetch in the background
            // so the Sankey updates within ~200ms without blocking first paint.
            updatePeriodBackground(targetMonth, inc);
        }

        sankeyCategoryList = buildSankeyCategoryList(periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
        periodSummary = buildPeriodSummary(inc, periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
        periodLoading = false;
    }

    /**
     * Non-blocking background refresh for month-specific Sankey data.
     * Runs after first paint so the user sees content immediately.
     */
    async function updatePeriodBackground(targetMonth, income) {
        try {
            const [cats, savTotal, ptTotal] = await Promise.all([
                api.getCategoryAnalytics(targetMonth).catch(() => periodCategories),
                fetchCategoryTransferTotal(targetMonth, 'Savings Transfer'),
                fetchCategoryTransferTotal(targetMonth, 'Personal Transfer')
            ]);
            periodCategories = cats;
            sankeySavingsTotal = savTotal;
            sankeyPersonalTransferTotal = ptTotal;
            sankeyCategoryList = buildSankeyCategoryList(periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
            periodSummary = buildPeriodSummary(income, periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
        } catch (_) {}
    }

    async function computeAccountDeltas() {
        try {
            const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
            if (sorted.length < 2) return;
            const prevNet = sorted[1]?.net || 0;
            const curNet = sorted[0]?.net || 0;
            accountDeltas = { _portfolioDelta: curNet - prevNet };
        } catch (_) {}
    }

    let netWorthAnimationDone = false;

    function animateNumbers() {
        // Cancel any in-flight animation before starting a new one
        if (animationFrameId != null) {
            cancelAnimationFrame(animationFrameId);
            animationFrameId = null;
        }

        animationStarted = true;
        const duration = 900;
        const staggerDelay = 150; // ms delay before metric ribbon starts
        const targetInc = periodSummary?.income || 0;
        const targetExp = periodSummary?.expenses || 0;
        const overshoot = 1.06;

        // Net Worth only animates on first page load
        const shouldAnimateNW = !netWorthAnimationDone;
        const targetNW = netWorth;

        // If NW already animated, just snap it
        if (!shouldAnimateNW) {
            animatedNetWorth = targetNW;
        }

        let startTime = -1;

        function springEase(t) {
            if (t < 0.7) {
                const p = t / 0.7;
                return p * p * (3 - 2 * p) * overshoot;
            } else {
                const p = (t - 0.7) / 0.3;
                const ease = p * p * (3 - 2 * p);
                return overshoot + (1.0 - overshoot) * ease;
            }
        }

        function step(rafTimestamp) {
            // Use the RAF-provided timestamp for frame-perfect timing
            if (startTime < 0) startTime = rafTimestamp;
            const elapsed = rafTimestamp - startTime;

            // ── Net Worth: starts immediately ──
            const nwProgress = Math.min(elapsed / duration, 1);
            const nwEased = springEase(nwProgress);

            if (shouldAnimateNW) {
                animatedNetWorth = targetNW * nwEased;
            }

            // ── Income & Expenses: staggered start ──
            const ribbonElapsed = Math.max(elapsed - staggerDelay, 0);
            const ribbonProgress = Math.min(ribbonElapsed / duration, 1);
            const ribbonEased = springEase(ribbonProgress);

            animatedIncome = targetInc * ribbonEased;
            animatedExpenses = targetExp * ribbonEased;

            // Continue until both tracks are done
            if (nwProgress < 1 || ribbonProgress < 1) {
                animationFrameId = requestAnimationFrame(step);
            } else {
                animationFrameId = null;
                if (shouldAnimateNW) {
                    animatedNetWorth = targetNW;
                    netWorthAnimationDone = true;
                }
                animatedIncome = targetInc;
                animatedExpenses = targetExp;
                animationDone = true;
            }
        }

        animationFrameId = requestAnimationFrame(step);
    }

    function getMonthForPeriod(period) {
        const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
        switch (period) {
            case 'this_month': return getCurrentMonth();
            case 'last_month': {
                // Always compute last month from calendar, not from data array
                const now = new Date();
                const lastMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1);
                const y = lastMonth.getFullYear();
                const m = String(lastMonth.getMonth() + 1).padStart(2, '0');
                return `${y}-${m}`;
            }
            case 'custom': return selectedCustomMonth;
            default: return null;
        }
    }

    async function fetchCategoryTransferTotal(monthOrMonths, categoryName) {
        try {
            let txns = [];
            if (Array.isArray(monthOrMonths)) {
                const results = await Promise.all(monthOrMonths.map(m => api.getTransactions({ month: m, category: categoryName, limit: 1000 }).then(r => r.data)));
                txns = results.flat();
            } else if (monthOrMonths) {
                txns = (await api.getTransactions({ month: monthOrMonths, category: categoryName, limit: 1000 })).data;
            } else {
                txns = (await api.getTransactions({ category: categoryName, limit: 1000 })).data;
            }
            const outflowTxns = txns.filter(t => parseFloat(t.amount) < 0);
            return Math.round(outflowTxns.reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0) * 100) / 100;
        } catch (e) { return 0; }
    }

    function buildSankeyCategoryList(cats, savingsTotal, personalTransferTotal) {
        const expenseCats = cats.filter(c => !DIRECT_FLOW_CATEGORIES.includes(c.category));
        const expenseTotal = expenseCats.reduce((s, c) => s + (c.total || 0), 0);

        const combined = expenseCats.map(c => ({
            ...c,
            percent: expenseTotal > 0 ? (c.total / expenseTotal) * 100 : 0
        }));

        if (savingsTotal > 0) {
            combined.push({ category: 'Savings Transfer', total: savingsTotal, percent: 0, isDirectFlow: true });
        }
        if (personalTransferTotal > 0) {
            combined.push({ category: 'Personal Transfer', total: personalTransferTotal, percent: 0, isDirectFlow: true });
        }

        combined.sort((a, b) => b.total - a.total);
        return combined;
    }

    function buildPeriodSummary(income, cats, savingsTotal, personalTransferTotal) {
        const expenseCats = (cats || []).filter(c => !DIRECT_FLOW_CATEGORIES.includes(c.category));
        const expenses = expenseCats.reduce((s, c) => s + (c.total || 0), 0);
        const netFlow = income - expenses - savingsTotal - personalTransferTotal;
        const savings = Math.max(income - expenses, 0);
        return {
            income, expenses, savings,
            savingsTransfer: savingsTotal,
            personalTransfer: personalTransferTotal,
            net_flow: netFlow,
            savings_rate: sanitizeSavingsRate(income, expenses, savings)
        };
    }

    async function updatePeriod() {
        periodLoading = true;
        selectedSankeyCategory = null;
        sankeyDrillTxns = [];
        
        // Keep stale data visible while fetching (no more null flash)
        // sankeyCategoryList, periodCategories, periodSummary retain
        // their previous values until fresh data arrives below.

        if (selectedPeriod === 'all') {
            const inc = summary?.income || 0;
            try {
                const [cats, savTotal, ptTotal] = await Promise.all([
                    api.getCategoryAnalytics().catch(() => categories),
                    fetchCategoryTransferTotal(null, 'Savings Transfer'),
                    fetchCategoryTransferTotal(null, 'Personal Transfer')
                ]);
                periodCategories = cats;
                sankeySavingsTotal = savTotal;
                sankeyPersonalTransferTotal = ptTotal;
            } catch (e) { periodCategories = categories; sankeySavingsTotal = 0; sankeyPersonalTransferTotal = 0; }
            sankeyCategoryList = buildSankeyCategoryList(periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
            periodSummary = buildPeriodSummary(inc, periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
        } else if (selectedPeriod === 'ytd') {
            const year = new Date().getFullYear().toString();
            const ytdMonths = monthly.filter(m => m.month.startsWith(year));
            const inc = ytdMonths.reduce((s, m) => s + m.income, 0);
            const ytdMonthKeys = ytdMonths.map(m => m.month);
            try {
                const [cats, savTotal, ptTotal] = await Promise.all([
                    fetchAggregatedCategories(ytdMonthKeys).catch(() => categories),
                    fetchCategoryTransferTotal(ytdMonthKeys, 'Savings Transfer'),
                    fetchCategoryTransferTotal(ytdMonthKeys, 'Personal Transfer')
                ]);
                periodCategories = cats;
                sankeySavingsTotal = savTotal;
                sankeyPersonalTransferTotal = ptTotal;
            } catch (e) { periodCategories = categories; sankeySavingsTotal = 0; sankeyPersonalTransferTotal = 0; }
            sankeyCategoryList = buildSankeyCategoryList(periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
            periodSummary = buildPeriodSummary(inc, periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
        } else {
            const targetMonth = getMonthForPeriod(selectedPeriod);
            if (targetMonth) {
                const m = monthly.find(m => m.month === targetMonth);
                    const inc = m ? m.income : 0;
                    try {
                        const [cats, savTotal, ptTotal] = await Promise.all([
                            api.getCategoryAnalytics(targetMonth).catch(() => []),
                            fetchCategoryTransferTotal(targetMonth, 'Savings Transfer'),
                            fetchCategoryTransferTotal(targetMonth, 'Personal Transfer')
                        ]);
                        periodCategories = cats;
                        sankeySavingsTotal = savTotal;
                        sankeyPersonalTransferTotal = ptTotal;
                    } catch (e) { periodCategories = []; sankeySavingsTotal = 0; sankeyPersonalTransferTotal = 0; }
                    sankeyCategoryList = buildSankeyCategoryList(periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
                    periodSummary = buildPeriodSummary(inc, periodCategories, sankeySavingsTotal, sankeyPersonalTransferTotal);
            } else {
                periodSummary = { income: 0, expenses: 0, savings: 0, savingsTransfer: 0, personalTransfer: 0, net_flow: 0, savings_rate: 0 };
                periodCategories = [];
                sankeyCategoryList = [];
                sankeySavingsTotal = 0;
                sankeyPersonalTransferTotal = 0;
            }
        }

        periodLoading = false;

        if (!loading && typeof window !== 'undefined') {
            animationDone = false;
            animatedIncome = 0;
            animatedExpenses = 0;
            requestAnimationFrame(() => animateNumbers());
        }
    }

    async function fetchAggregatedCategories(monthList) {
        if (!monthList || monthList.length === 0) return [];
        const results = await Promise.all(monthList.map(m => api.getCategoryAnalytics(m)));
        const merged = {};
        for (const monthCats of results) {
            for (const cat of (monthCats || [])) {
                merged[cat.category] = (merged[cat.category] || 0) + (cat.total || 0);
            }
        }
        const entries = Object.entries(merged).map(([category, total]) => ({ category, total }));
        const grandTotal = entries.reduce((s, e) => s + e.total, 0) || 1;
        return entries.map(e => ({ ...e, percent: (e.total / grandTotal) * 100 })).sort((a, b) => b.total - a.total);
    }

    function sanitizeSavingsRate(income, expenses, savings) {
        if (!income || income <= 0) return 0;
        if (expenses > income) return 0;
        const rate = (savings / income) * 100;
        if (rate < 0 || rate > 100 || !isFinite(rate)) return 0;
        return rate;
    }

    // Only react to period changes AFTER initial mount setup is complete.
    // onMount handles the first call — this reactive statement handles subsequent
    // period selector clicks only.
    let initialLoadComplete = false;

    $: if (initialLoadComplete && !profileSwitching && selectedPeriod && monthly.length > 0) {
        void privacyKey; // re-evaluate when privacy toggles
        selectedPeriodStore.set(selectedPeriod);
        if (selectedCustomMonth) selectedCustomMonthStore.set(selectedCustomMonth);
        updatePeriod();
    }


    // ââ Profile switch: full reload of dashboard data ââ
    let _prevProfile = null;
    $: if (initialLoadComplete && $activeProfile && $activeProfile !== _prevProfile) {
        if (_prevProfile !== null) {
            // Profile actually changed (not initial mount)
            reloadDashboardForProfile();
        }
        _prevProfile = $activeProfile;
    }

    async function reloadDashboardForProfile() {
        profileSwitching = true;
        try {
            const bundle = await api.getDashboardBundle();
            summary = bundle.summary;
            accounts = bundle.accounts;
            monthly = bundle.monthly;
            categories = bundle.categories;
            netWorthTrendData = (Array.isArray(bundle.netWorthSeries) && bundle.netWorthSeries.length >= 2)
                ? bundle.netWorthSeries.map(d => ({ month: d.month || d.date, value: d.value }))
                : [];
            trailingSavingsInfo = computeTrailingSavingsRate(monthly, 3);
            allMonths = [...monthly].sort((a, b) => b.month.localeCompare(a.month)).map(m => m.month);
            if (allMonths.length > 0 && !allMonths.includes(selectedCustomMonth)) {
                selectedCustomMonth = allMonths[0];
            }
            await computeAccountDeltas();
            bootstrapInitialPeriod();
            netWorthAnimationDone = false;
            animationDone = false;
            animatedNetWorth = 0;
            animatedIncome = 0;
            animatedExpenses = 0;
            requestAnimationFrame(() => animateNumbers());
        } catch (e) {
            console.error('Failed to reload dashboard for profile:', e);
        } finally {
            profileSwitching = false;
        }
    }

    function handleCustomMonthChange() {
        selectedPeriod = 'custom';
        updatePeriod();
    }

    // Deltas
    // ── Unified derived state: accounts + deltas (single reactive block) ──
    // ── Consolidated dashboard metrics: single reactive block ──
    // All derived values computed in one pass so Svelte batches into a
    // single synchronous update instead of cascading through 6+ reactive
    // statements sequentially.
    $: dashboardMetrics = (() => {
        // Privacy reactivity anchor â forces re-computation when privacy toggles
        void privacyKey;
        // ââ Account totals ââ
        const _cashAccounts = accounts.filter(a => !a.is_credit);
        const _creditAccounts = accounts.filter(a => a.is_credit);
        // Separate investment accounts into assets (already in _cashAccounts via is_credit=false)
        // Note: loan accounts have is_credit=true and will appear in _creditAccounts

        const _totalCash = _cashAccounts.reduce((s, a) => s + parseFloat(a.balance || 0), 0);
        const _totalOwed = _creditAccounts.reduce((s, a) => s + parseFloat(a.balance || 0), 0);
        const _netWorth = _totalCash - _totalOwed;

        // ── Month-over-month deltas ──
        const _currentMonthData = monthly.find(m => m.month === getCurrentMonth()) || null;
        const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
        const _prevMonthData = sorted.length > 1 ? sorted[1] : null;

        const _incomeDelta = _currentMonthData && _prevMonthData ? computeDelta(_currentMonthData.income, _prevMonthData.income) : null;
        const _expenseDelta = _currentMonthData && _prevMonthData ? computeDelta(_currentMonthData.expenses, _prevMonthData.expenses) : null;
        const _netWorthDelta = _currentMonthData && _prevMonthData ? _currentMonthData.net - _prevMonthData.net : null;

        // ── Savings rate (trailing) ──
        const _savingsRate = trailingSavingsInfo.rate;
        const _savingsRateDelta = trailingSavingsInfo.delta;

        // ── Daily spending pace ──
        let _dailyPace = null;
        if (periodSummary && selectedPeriod === 'this_month') {
            const now = new Date();
            const dayOfMonth = now.getDate();
            if (dayOfMonth > 0) {
                const dailyAvg = periodSummary.expenses / dayOfMonth;
                const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
                const projected = dailyAvg * daysInMonth;
                _dailyPace = { dailyAvg, projected, daysInMonth, dayOfMonth };
            }
        }

        // ── Top category insight ──
        let _topCatInsight = null;
        {
            const expenseCats = periodCategories.filter(c => !DIRECT_FLOW_CATEGORIES.includes(c.category));
            if (expenseCats.length > 0 && periodSummary) {
                const top = expenseCats[0];
                _topCatInsight = {
                    name: top.category,
                    total: top.total,
                    pctOfExpenses: periodSummary.expenses > 0 ? (top.total / periodSummary.expenses * 100) : 0
                };
            }
        }

        return {
            totalCash: _totalCash,
            totalOwed: _totalOwed,
            netWorth: _netWorth,
            cashAccounts: _cashAccounts,
            creditAccounts: _creditAccounts,
            currentMonthData: _currentMonthData,
            prevMonthData: _prevMonthData,
            incomeDelta: _incomeDelta,
            expenseDelta: _expenseDelta,
            netWorthDelta: _netWorthDelta,
            savingsRate: _savingsRate,
            savingsRateDelta: _savingsRateDelta,
            dailyPace: _dailyPace,
            topCatInsight: _topCatInsight
        };
    })();

    // ── Convenience aliases (template-compatible, no extra reactive passes) ──
    $: totalCash = dashboardMetrics.totalCash;
    $: totalOwed = dashboardMetrics.totalOwed;
    $: netWorth = dashboardMetrics.netWorth;
    $: cashAccounts = dashboardMetrics.cashAccounts;
    $: creditAccounts = dashboardMetrics.creditAccounts;
    $: currentMonthData = dashboardMetrics.currentMonthData;
    $: prevMonthData = dashboardMetrics.prevMonthData;
    $: incomeDelta = dashboardMetrics.incomeDelta;
    $: expenseDelta = dashboardMetrics.expenseDelta;
    $: netWorthDelta = dashboardMetrics.netWorthDelta;
    $: savingsRate = dashboardMetrics.savingsRate;
    $: savingsRateDelta = dashboardMetrics.savingsRateDelta;
    $: dailyPace = dashboardMetrics.dailyPace;
    $: topCatInsight = dashboardMetrics.topCatInsight;

    // ââ Privacy Mode reactivity key ââ
    // When privacyMode toggles, this forces Svelte to re-evaluate all
    // template expressions that depend on formatCurrency / formatCompact.
    // We use it as a hidden dependency in key reactive expressions.
    $: privacyKey = $privacyMode;

    // ââ Net Worth chart hover ââ
    function handleChartHover(event) {
        if (!netWorthTrendData.length || !nwChart.linePath) return;

        const svg = event.currentTarget.closest('svg') || event.currentTarget;
        const rect = svg.getBoundingClientRect();
        const mouseX = ((event.clientX - rect.left) / rect.width) * 600;

        const stepX = 600 / (netWorthTrendData.length - 1);
        const index = Math.round(mouseX / stepX);
        const clampedIndex = Math.max(0, Math.min(index, netWorthTrendData.length - 1));

        const d = netWorthTrendData[clampedIndex];
        const padY = 8;
        const padBottom = 28;
        const chartH = 160 - padY - padBottom;
        const values = netWorthTrendData.map(p => p.value);
        const rawMin = Math.min(...values);
        const rawMax = Math.max(...values);
        const visibleRange = rawMax - rawMin || 1;
        const min = rawMin - visibleRange * 0.15;
        const max = rawMax + visibleRange * 0.08;
        const range = max - min || 1;

        const x = clampedIndex * stepX;
        const y = padY + chartH - ((d.value - min) / range) * chartH;

        const raw = d.month || d.date;
        const dt = new Date(raw.length === 7 ? raw + '-01' : raw);
        const dateLabel = dt.toLocaleDateString('en-US', {
            month: 'short', day: 'numeric', year: 'numeric'
        });

        hoverPoint = { x, y, date: dateLabel, value: d.value };
    }

    function handleChartLeave() {
        hoverPoint = null;
    }

    // Net Worth Area Chart — smooth Catmull-Rom spline with quarterly labels
    function buildNWAreaChart(data, width = 600, height = 160) {
        if (!Array.isArray(data) || data.length < 2) return {
            linePath: '', areaPath: '', endPoint: null,
            labels: [], gridLines: [], monthLabels: []
        };

        const values = data.map(d => d.value);
        const rawMin = Math.min(...values);
        const rawMax = Math.max(...values);
        const visibleRange = rawMax - rawMin || 1;
        const min = rawMin - visibleRange * 0.15;
        const max = rawMax + visibleRange * 0.08;
        const range = max - min || 1;
        const padY = 8;
        const padBottom = 28;
        const chartH = height - padY - padBottom;
        const stepX = width / (values.length - 1);

        const rawAnchors = values.map((v, i) => ({
            x: i * stepX,
            y: padY + chartH - ((v - min) / range) * chartH
        }));

        function catmullRomChain(pts, tension, samplesPerSegment) {
            const alpha = tension;
            const result = [];
            for (let i = 0; i < pts.length - 1; i++) {
                const p0 = pts[Math.max(i - 1, 0)];
                const p1 = pts[i];
                const p2 = pts[i + 1];
                const p3 = pts[Math.min(i + 2, pts.length - 1)];
                const steps = (i === pts.length - 2) ? samplesPerSegment + 1 : samplesPerSegment;
                for (let s = 0; s < steps; s++) {
                    const tt = s / samplesPerSegment;
                    const t2 = tt * tt;
                    const t3 = t2 * tt;
                    const m1x = alpha * (p2.x - p0.x);
                    const m1y = alpha * (p2.y - p0.y);
                    const m2x = alpha * (p3.x - p1.x);
                    const m2y = alpha * (p3.y - p1.y);
                    const ax = (2*t3 - 3*t2 + 1)*p1.x + (t3 - 2*t2 + tt)*m1x + (-2*t3 + 3*t2)*p2.x + (t3 - t2)*m2x;
                    const ay = (2*t3 - 3*t2 + 1)*p1.y + (t3 - 2*t2 + tt)*m1y + (-2*t3 + 3*t2)*p2.y + (t3 - t2)*m2y;
                    result.push({ x: ax, y: ay });
                }
            }
            return result;
        }

        const tension = 0.5;
        const samplesPerSeg = 16;
        const curvePts = rawAnchors.length >= 2 ? catmullRomChain(rawAnchors, tension, samplesPerSeg) : rawAnchors;

        let linePath = '';
        if (curvePts.length > 0) {
            linePath = `M${curvePts[0].x.toFixed(1)},${curvePts[0].y.toFixed(1)}`;
            for (let i = 1; i < curvePts.length; i++) {
                linePath += ` L${curvePts[i].x.toFixed(1)},${curvePts[i].y.toFixed(1)}`;
            }
        }

        const lastPt = curvePts[curvePts.length - 1];
        const firstPt = curvePts[0];
        const areaPath = linePath + ` L${lastPt.x.toFixed(1)},${height - padBottom}` + ` L${firstPt.x.toFixed(1)},${height - padBottom} Z`;
        const endPoint = curvePts[curvePts.length - 1];

        const labels = data.map((d, i) => ({ x: i * stepX, text: formatMonthShort(d.month || d.date) }));

        const monthLabels = [];
        const labelledQuarters = new Set();
        for (let i = 0; i < data.length; i++) {
            const raw = data[i].month || data[i].date;
            if (!raw) continue;
            const dt = new Date(raw.length === 7 ? raw + '-01' : raw);
            if (isNaN(dt.getTime())) continue;
            const mo = dt.getMonth();
            const yr = dt.getFullYear();
            const quarter = `${yr}-Q${Math.floor(mo / 3)}`;
            if ([0, 3, 6, 9].includes(mo) && !labelledQuarters.has(quarter)) {
                labelledQuarters.add(quarter);
                const shortMonth = dt.toLocaleString('default', { month: 'short' });
                const shortYear = String(yr).slice(-2);
                monthLabels.push({ x: i * stepX, y: height - 6, label: `${shortMonth} '${shortYear}` });
            }
        }

        if (monthLabels.length === 0 && data.length >= 1) {
            const firstRaw = data[0].month || data[0].date;
            const firstDt = new Date(firstRaw.length === 7 ? firstRaw + '-01' : firstRaw);
            monthLabels.push({ x: 0, y: height - 6, label: `${firstDt.toLocaleString('default', { month: 'short' })} '${String(firstDt.getFullYear()).slice(-2)}` });
            if (data.length > 1) {
                const lastRaw = data[data.length - 1].month || data[data.length - 1].date;
                const lastDt = new Date(lastRaw.length === 7 ? lastRaw + '-01' : lastRaw);
                monthLabels.push({ x: (data.length - 1) * stepX, y: height - 6, label: `${lastDt.toLocaleString('default', { month: 'short' })} '${String(lastDt.getFullYear()).slice(-2)}` });
            }
        }

        const gridLines = [];
        const gridCount = 3;
        for (let i = 0; i <= gridCount; i++) {
            const y = padY + (chartH / gridCount) * i;
            gridLines.push({ y });
        }

        return { linePath, areaPath, endPoint, labels, gridLines, monthLabels };
    }
    // Memoize: deep content-aware cache — only recompute when the data
    // actually differs (length, first value, last value) or dimensions change.
    let nwChart = { linePath: '', areaPath: '', endPoint: null, labels: [], gridLines: [], monthLabels: [] };

    const _nwChartCache = { key: '', result: nwChart };

    function getNWChartCacheKey(data, width, height) {
        if (!Array.isArray(data) || data.length < 2) return 'empty';
        return `${data.length}|${data[0].month}:${data[0].value}|${data[data.length - 1].month}:${data[data.length - 1].value}|${width}x${height}`;
    }

    $: {
        if (Array.isArray(netWorthTrendData) && netWorthTrendData.length >= 2) {
            const cacheKey = getNWChartCacheKey(netWorthTrendData, 600, 160);
            if (cacheKey !== _nwChartCache.key) {
                _nwChartCache.key = cacheKey;
                _nwChartCache.result = buildNWAreaChart(netWorthTrendData);
            }
            nwChart = _nwChartCache.result;
        } else {
            nwChart = { linePath: '', areaPath: '', endPoint: null, labels: [], gridLines: [], monthLabels: [] };
        }
    }

    $: nwTrendDirection = (Array.isArray(netWorthTrendData) && netWorthTrendData.length >= 2)
        ? (netWorthTrendData[netWorthTrendData.length - 1].value >= netWorthTrendData[netWorthTrendData.length - 2].value ? 'up' : 'down')
        : 'flat';
    $: nwAreaFillColor = nwTrendDirection === 'up' ? '#34d399' : nwTrendDirection === 'down' ? '#f87171' : '#627d98';

    // Credit card utilization helper
    function getUtilization(card) {
        const limit = card.limit || card.credit_limit || 10000;
        const balance = Math.abs(parseFloat(card.balance || 0));
        return Math.min((balance / limit) * 100, 100);
    }

    // Sankey drill-down
    async function handleSankeySelect(event) {
        const cat = event.detail;
        if (cat === null || cat === selectedSankeyCategory) {
            selectedSankeyCategory = null;
            sankeyDrillTxns = [];
            await tick();
            return;
        }

        selectedSankeyCategory = cat;
        const targetMonth = ['this_month', 'last_month', 'custom'].includes(selectedPeriod)
            ? getMonthForPeriod(selectedPeriod) : null;

        try {
            let allTxns = [];
            if (selectedPeriod === 'ytd') {
                const year = new Date().getFullYear().toString();
                const ytdMonthKeys = monthly.filter(m => m.month.startsWith(year)).map(m => m.month);
                const results = await Promise.all(ytdMonthKeys.map(m => api.getTransactions({ month: m, category: cat, limit: 1000 }).then(r => r.data)));
                allTxns = results.flat();
            } else if (selectedPeriod === 'all') {
                allTxns = (await api.getTransactions({ category: cat, limit: 1000 })).data;
            } else {
                const params = { limit: 1000 };
                if (targetMonth) params.month = targetMonth;
                params.category = cat;
                allTxns = (await api.getTransactions(params)).data;
            }
            sankeyDrillTxns = allTxns.sort((a, b) => Math.abs(parseFloat(b.amount)) - Math.abs(parseFloat(a.amount)));
            await tick();
            if (drillDownSection) drillDownSection.scrollIntoView({ behavior: 'smooth', block: 'center' });
        } catch (e) { sankeyDrillTxns = []; }
    }

    function getDrillDownTotal(category) {
        const catEntry = sankeyCategoryList.find(c => c.category === category);
        if (catEntry) return catEntry.total;
        return sankeyDrillTxns.reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
    }

    // ══════════════════════════════════════════
    // S4: MONTHLY NET SURPLUS/DEFICIT (Plotly)
    // ══════════════════════════════════════════

    // Computed stats for the monthly net section
    $: monthlyNetStats = (() => {
        if (!monthly || monthly.length === 0) return null;
        const sorted = [...monthly].sort((a, b) => a.month.localeCompare(b.month));
        const nets = sorted.map(m => m.net);
        const surplusMonths = nets.filter(n => n >= 0);
        const deficitMonths = nets.filter(n => n < 0);
        const totalIncome = sorted.reduce((s, m) => s + m.income, 0);
        const totalExpenses = sorted.reduce((s, m) => s + m.expenses, 0);
        const totalNet = totalIncome - totalExpenses;
        const effectiveSaveRate = totalIncome > 0 ? Math.max((totalNet / totalIncome) * 100, 0) : 0;
        const avgNet = nets.length > 0 ? nets.reduce((s, n) => s + n, 0) / nets.length : 0;

        return {
            surplusCount: surplusMonths.length,
            deficitCount: deficitMonths.length,
            totalMonths: nets.length,
            avgSurplus: surplusMonths.length > 0 ? surplusMonths.reduce((s, n) => s + n, 0) / surplusMonths.length : 0,
            avgDeficit: deficitMonths.length > 0 ? deficitMonths.reduce((s, n) => s + n, 0) / deficitMonths.length : 0,
            effectiveSaveRate: Math.min(effectiveSaveRate, 100),
            avgNet
        };
    })();

    // ── Monthly Net SVG Bar Chart (replaces Plotly) ──────────────
    let prevMonthlyRef = null;
    let monthlyNetSVG = null;

    $: {
        if (monthly !== prevMonthlyRef) {
            prevMonthlyRef = monthly;
            monthlyNetSVG = (() => {
                if (!monthly || monthly.length === 0) return null;

                const sorted = [...monthly].sort((a, b) => a.month.localeCompare(b.month));
                const labels = sorted.map(m => {
                    const [y, mo] = m.month.split('-');
                    const d = new Date(parseInt(y), parseInt(mo) - 1);
                    return d.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
                });
                const vals = sorted.map(m => m.net);
                const avg = vals.reduce((a, b) => a + b, 0) / (vals.length || 1);
                const maxAbs = Math.max(...vals.map(Math.abs), Math.abs(avg)) * 1.15 || 1;

                const W = 720, H = 280;
                const pad = { top: 24, right: 16, bottom: 40, left: 60 };
                const plotW = W - pad.left - pad.right;
                const plotH = H - pad.top - pad.bottom;

                const barGap = 0.3;
                const step = plotW / vals.length;
                const barW = step * (1 - barGap);

                const yScale = (v) => pad.top + plotH / 2 - (v / maxAbs) * (plotH / 2);
                const zeroY = yScale(0);

                // Y-axis ticks
                const tickCount = 5;
                const yTicks = Array.from({ length: tickCount }, (_, i) => {
                    const v = maxAbs - (2 * maxAbs * i) / (tickCount - 1);
                    return { value: v, y: yScale(v) };
                });

                // Bars
                const bars = vals.map((v, i) => {
                    const isLast = i === vals.length - 1;
                    return {
                        x: pad.left + i * step + (step - barW) / 2,
                        y: v >= 0 ? yScale(v) : zeroY,
                        w: barW,
                        h: Math.max(Math.abs(yScale(v) - zeroY), 1),
                        fill: v >= 0 ? 'rgba(13, 150, 104,' : 'rgba(220, 53, 69,',
                        opacity: isLast ? 0.95 : 0.65,
                        isLast,
                        strokeColor: isLast ? (v >= 0 ? 'rgba(13, 150, 104, 1)' : 'rgba(220, 53, 69, 1)') : 'transparent',
                        value: v,
                        label: labels[i]
                    };
                });

                const avgY = yScale(avg);

                return { W, H, pad, bars, yTicks, zeroY, avgY, avg, labels, step, barW };
            })();
        }
    }

    // ══════════════════════════════════════════
    // S5a: YoY SAME-PERIOD COMPARISON
    // ══════════════════════════════════════════
    $: yoyData = (() => {
        if (!monthly || monthly.length === 0) return null;
        const now = new Date();
        const currentMonth = now.getMonth(); // 0-indexed (0=Jan, 2=Mar)
        const currentYear = now.getFullYear();

        // Group months by year
        const byYear = {};
        for (const m of monthly) {
            const [y, mo] = m.month.split('-').map(Number);
            if (!byYear[y]) byYear[y] = [];
            byYear[y].push({ ...m, monthNum: mo });
        }

        const years = Object.keys(byYear).map(Number).sort();
        if (years.length === 0) return null;

        const currentMonthName = new Date(currentYear, currentMonth).toLocaleString('default', { month: 'short' });

        const yearStats = years.map(yr => {
            let periodMonths;
            let periodLabel;

            if (yr === currentYear) {
                // Current year: only up to the current month (YTD)
                const maxMonth = currentMonth + 1; // monthNum is 1-indexed
                periodMonths = byYear[yr].filter(m => m.monthNum <= maxMonth);
                periodLabel = `YTD (Jan–${currentMonthName})`;
            } else {
                // Past years: use ALL months
                periodMonths = byYear[yr];
                periodLabel = 'Full Year';
            }

            const totalIn = periodMonths.reduce((s, m) => s + m.income, 0);
            const totalOut = periodMonths.reduce((s, m) => s + m.expenses, 0);
            const net = totalIn - totalOut;
            const monthCount = periodMonths.length;
            return { year: yr, totalIn, totalOut, net, monthCount, isPartial: yr === currentYear, periodLabel };
        });

        // Find max value for bar scaling
        const maxVal = Math.max(...yearStats.flatMap(y => [y.totalIn, y.totalOut]), 1);

        // Compute delta: current YTD vs previous year's SAME period for a fair comparison
        let delta = null;
        if (yearStats.length >= 2) {
            const current = yearStats[yearStats.length - 1];
            const previousYearData = byYear[years[years.length - 2]];
            // Compare against same period (Jan–currentMonth) of the previous year
            const prevSamePeriod = previousYearData
                ? previousYearData.filter(m => m.monthNum <= currentMonth + 1)
                : [];
            const prevSamePeriodNet = prevSamePeriod.reduce((s, m) => s + m.income, 0)
                                    - prevSamePeriod.reduce((s, m) => s + m.expenses, 0);
            delta = {
                amount: current.net - prevSamePeriodNet,
                prevYear: years[years.length - 2],
                note: `Jan–${currentMonthName}`
            };
        }

        return { yearStats, maxVal, delta };
    })();

    // ══════════════════════════════════════════
    // S5b: NET WORTH TRAJECTORY (Plotly)
    // ══════════════════════════════════════════

    $: trajectoryStats = (() => {
        if (!Array.isArray(netWorthTrendData) || netWorthTrendData.length < 2) return null;
        const values = netWorthTrendData.map(d => d.value);
        const first = values[0];
        const last = values[values.length - 1];
        const change = last - first;
        const peak = Math.max(...values);
        const low = Math.min(...values);
        const peakIdx = values.indexOf(peak);
        const lowIdx = values.indexOf(low);

        const peakDate = netWorthTrendData[peakIdx]?.month || netWorthTrendData[peakIdx]?.date || '';
        const lowDate = netWorthTrendData[lowIdx]?.month || netWorthTrendData[lowIdx]?.date || '';

        // Estimate monthly growth
        const months = netWorthTrendData.length > 1 ? netWorthTrendData.length / 2 : 1; // biweekly data
        const avgGrowth = change / Math.max(months, 1);

        function formatShortDate(dateStr) {
            const dt = new Date(dateStr.length === 7 ? dateStr + '-01' : dateStr);
            return dt.toLocaleString('default', { month: 'short', year: '2-digit' });
        }

        return { change, peak, low, peakDate: formatShortDate(peakDate), lowDate: formatShortDate(lowDate), avgGrowth, current: last };
    })();

    // ── Net Worth Trajectory SVG (replaces Plotly) ─────────────
    let prevTrajRef = null;
    let trajectorySVG = null;

    $: {
        if (netWorthTrendData !== prevTrajRef) {
            prevTrajRef = netWorthTrendData;
            trajectorySVG = (() => {
                if (!Array.isArray(netWorthTrendData) || netWorthTrendData.length < 2) return null;

                const dates = netWorthTrendData.map(d => {
                    const raw = d.month || d.date;
                    const dt = new Date(raw.length === 7 ? raw + '-01' : raw);
                    return dt.toLocaleDateString('en-US', { month: 'short', year: '2-digit' });
                });
                const values = netWorthTrendData.map(d => d.value);

                const W = 640, H = 200;
                const pad = { top: 12, right: 12, bottom: 32, left: 55 };
                const plotW = W - pad.left - pad.right;
                const plotH = H - pad.top - pad.bottom;

                const rawMin = Math.min(...values);
                const rawMax = Math.max(...values);
                const range = rawMax - rawMin || 1;
                const min = rawMin - range * 0.08;
                const max = rawMax + range * 0.08;
                const fullRange = max - min;

                const stepX = plotW / (values.length - 1 || 1);
                const yScale = (v) => pad.top + plotH - ((v - min) / fullRange) * plotH;

                const points = values.map((v, i) => ({
                    x: pad.left + i * stepX,
                    y: yScale(v)
                }));

                // Build smooth line path (reuse catmullRom from NW chart logic)
                let linePath = `M${points[0].x.toFixed(1)},${points[0].y.toFixed(1)}`;
                for (let i = 1; i < points.length; i++) {
                    const prev = points[i - 1];
                    const curr = points[i];
                    const cpx = (prev.x + curr.x) / 2;
                    linePath += ` C${cpx.toFixed(1)},${prev.y.toFixed(1)} ${cpx.toFixed(1)},${curr.y.toFixed(1)} ${curr.x.toFixed(1)},${curr.y.toFixed(1)}`;
                }

                const lastPt = points[points.length - 1];
                const firstPt = points[0];
                const areaPath = linePath + ` L${lastPt.x.toFixed(1)},${pad.top + plotH} L${firstPt.x.toFixed(1)},${pad.top + plotH} Z`;

                // Y-axis ticks
                const tickCount = 4;
                const yTicks = Array.from({ length: tickCount }, (_, i) => {
                    const v = min + (fullRange * (tickCount - 1 - i)) / (tickCount - 1);
                    return { value: v, y: yScale(v) };
                });

                // X-axis labels (every nth)
                const labelEvery = Math.max(1, Math.floor(values.length / 8));
                const xLabels = dates.map((label, i) => ({
                    x: pad.left + i * stepX,
                    label,
                    show: i % labelEvery === 0 || i === dates.length - 1
                })).filter(l => l.show);

                return { W, H, pad, plotW, plotH, linePath, areaPath, yTicks, xLabels, points };
            })();
        }
    }

    $: greeting = getGreeting();
</script>


{#if loading}
    <div class="space-y-6 fade-in">
        <!-- Header skeleton -->
        <div>
            <div class="skeleton h-3 w-24 mb-2"></div>
            <div class="skeleton h-8 w-48"></div>
        </div>
        <!-- Hero card: net worth number placeholder -->
        <div class="card-hero" style="min-height: 180px; display: flex; flex-direction: column; justify-content: center; padding: 1.5rem;">
            <div class="skeleton h-3 w-20 mb-3" style="border-radius: 6px;"></div>
            <div class="skeleton-hero-number"></div>
            <div class="skeleton h-2 w-32 mt-3" style="border-radius: 4px;"></div>
        </div>
        <!-- Metric ribbon skeleton -->
        <div class="skeleton h-16 rounded-2xl" style="opacity: 0.7"></div>
        <!-- Sankey chart skeleton -->
        <div>
            <div class="skeleton h-3 w-28 mb-3" style="border-radius: 6px;"></div>
            <div class="skeleton-chart-block skeleton-sankey rounded-2xl"></div>
        </div>
        <!-- Monthly net bar chart skeleton -->
        <div>
            <div class="skeleton h-3 w-44 mb-3" style="border-radius: 6px;"></div>
            <div class="skeleton-chart-block skeleton-bar-chart rounded-2xl"></div>
        </div>
        <!-- Two-panel row skeleton -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div class="skeleton h-64 rounded-2xl"></div>
            <div class="skeleton h-64 rounded-2xl"></div>
        </div>
    </div>
{:else if summary}

<div class="profile-transition" class:profile-loading={profileSwitching}>

    <!-- ═══ HEADER ═══ -->
    <div class="flex items-start justify-between mb-8 fade-in">
        <div>
            <p class="text-[10px] font-bold tracking-[0.2em] uppercase mb-1.5" style="color: var(--accent)">{greeting}</p>
            <h2 class="text-2xl md:text-[2rem] font-extrabold font-display tracking-tight" style="color: var(--text-primary)">
                Your finances at a glance
            </h2>
        </div>
        <div class="flex items-center gap-3">
            <button
                on:click={() => privacyMode.toggle()}
                class="privacy-toggle-btn"
                class:privacy-active={$privacyMode}
                aria-label={$privacyMode ? 'Show values' : 'Hide values'}
                title={$privacyMode ? 'Show values' : 'Hide values'}
            >
                <span class="material-symbols-outlined text-[18px]">
                    {$privacyMode ? 'visibility_off' : 'visibility'}
                </span>
            </button>
            <ProfileSwitcher />
            {#if tellerAppId}
                <TellerConnect
                    applicationId={tellerAppId}
                    environment={tellerEnvironment}
                    on:enrolled={handleTellerEnrolled}
                    on:error={(e) => console.warn('Teller Connect error:', e.detail)}
                />
            {/if}
        </div>
    </div>

    <!-- ═══════════════════════════════════════════════════════
         S1: UNIFIED HERO CARD (Net Worth + Accounts + Credit Cards)
         ═══════════════════════════════════════════════════════ -->
    <section class="mb-6 fade-in-up" style="animation-delay: 60ms">
        <div class="card-hero-unified">

            <!-- ——— LEFT ZONE: Net Worth + SVG Chart Background ——— -->
            <div class="hero-zone hero-zone-left" style="padding: 1.25rem 1.5rem 0; position: relative; display: flex; flex-direction: column; min-height: 160px; overflow: hidden;">

                {#if netWorthTrendData.length > 1}
                    <div class="hero-chart-bg" style="pointer-events: auto; width: 100%; margin-top: auto; z-index: 0;">
                        <svg width="100%" height="100%" viewBox="0 0 600 160" preserveAspectRatio="none" style="overflow: visible"
                            on:mousemove={handleChartHover}
                            on:mouseleave={handleChartLeave}>
                            <defs>
                                <linearGradient id="nwAreaGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%"   stop-color={CYAN_LINE}  stop-opacity="0.55"/>
                                    <stop offset="5%"   stop-color={CYAN_LINE}  stop-opacity="0.40"/>
                                    <stop offset="15%"  stop-color="#2DD4BF"     stop-opacity="0.35"/>
                                    <stop offset="30%"  stop-color={TEAL_AREA}   stop-opacity="0.30"/>
                                    <stop offset="50%"  stop-color={TEAL_AREA}   stop-opacity="0.22"/>
                                    <stop offset="70%"  stop-color={TEAL_AREA}   stop-opacity="0.14"/>
                                    <stop offset="85%"  stop-color={TEAL_AREA}   stop-opacity="0.07"/>
                                    <stop offset="100%" stop-color={TEAL_AREA}   stop-opacity="0.02"/>
                                </linearGradient>
                                <linearGradient id="nwLineGradCyan" x1="0" y1="0" x2="1" y2="0">
                                    <stop offset="0%"   stop-color={CYAN_LINE} stop-opacity="0.30"/>
                                    <stop offset="20%"  stop-color={CYAN_LINE} stop-opacity="0.7"/>
                                    <stop offset="45%"  stop-color={CYAN_GLOW} stop-opacity="1"/>
                                    <stop offset="55%"  stop-color="#BAE6FD"   stop-opacity="0.9"/>
                                    <stop offset="80%"  stop-color={CYAN_GLOW} stop-opacity="1"/>
                                    <stop offset="100%" stop-color={CYAN_LINE} stop-opacity="0.8"/>
                                </linearGradient>
                                <filter id="nwGlow" x="-20%" y="-20%" width="140%" height="140%">
                                    <feGaussianBlur in="SourceGraphic" stdDeviation="8"  result="outerBlur"/>
                                    <feGaussianBlur in="SourceGraphic" stdDeviation="3"  result="midBlur"/>
                                    <feGaussianBlur in="SourceGraphic" stdDeviation="1.5" result="innerBlur"/>
                                    <feMerge>
                                        <feMergeNode in="outerBlur"/>
                                        <feMergeNode in="midBlur"/>
                                        <feMergeNode in="innerBlur"/>
                                        <feMergeNode in="SourceGraphic"/>
                                    </feMerge>
                                </filter>
                                <filter id="nwAreaGlow" x="-10%" y="-10%" width="120%" height="120%">
                                    <feGaussianBlur stdDeviation="18" result="areaBlur"/>
                                    <feMerge>
                                        <feMergeNode in="areaBlur"/>
                                        <feMergeNode in="SourceGraphic"/>
                                    </feMerge>
                                </filter>
                                <filter id="nwDotGlow" x="-50%" y="-50%" width="200%" height="200%">
                                    <feGaussianBlur stdDeviation="5" result="dotBlur"/>
                                    <feMerge>
                                        <feMergeNode in="dotBlur"/>
                                        <feMergeNode in="SourceGraphic"/>
                                    </feMerge>
                                </filter>
                            </defs>
                            {#each nwChart.gridLines as gl}
                                <line x1="0" y1={gl.y} x2="600" y2={gl.y} stroke="var(--text-muted)" stroke-width="1" opacity="0.06" />
                            {/each}
                            <path d={nwChart.areaPath} fill="url(#nwAreaGrad)" filter="url(#nwAreaGlow)" opacity="0.7" />
                            <path d={nwChart.areaPath} fill="url(#nwAreaGrad)" />
                            <path d={nwChart.linePath} fill="none" stroke={CYAN_GLOW} stroke-width="18" stroke-linecap="round" stroke-linejoin="round" filter="url(#nwGlow)" opacity="0.18" />
                            <path d={nwChart.linePath} fill="none" stroke={CYAN_LINE} stroke-width="8" stroke-linecap="round" stroke-linejoin="round" opacity="0.30" />
                            <path d={nwChart.linePath} fill="none" stroke="url(#nwLineGradCyan)" stroke-width="2.8" stroke-linecap="round" stroke-linejoin="round" />
                            <path d={nwChart.linePath} fill="none" stroke="white" stroke-width="1.0" opacity="0.45" stroke-linecap="round" stroke-linejoin="round" />
                            {#if nwChart.endPoint}
                                <circle cx={nwChart.endPoint.x} cy={nwChart.endPoint.y} r="16" fill="none" stroke={CYAN_GLOW} stroke-width="1.5" opacity="0.12" filter="url(#nwDotGlow)" class="pulse-dot" />
                                <circle cx={nwChart.endPoint.x} cy={nwChart.endPoint.y} r="9" fill="none" stroke={CYAN_LINE} stroke-width="1.2" opacity="0.35" class="pulse-dot" />
                                <circle cx={nwChart.endPoint.x} cy={nwChart.endPoint.y} r="4" fill="#CCFBF1" stroke={CYAN_DEEP} stroke-width="2" filter="url(#nwDotGlow)" class="pulse-dot" />
                            {/if}
                            {#each nwChart.monthLabels as ml}
                                <line x1={ml.x} y1={ml.y - 14} x2={ml.x} y2={ml.y - 10} stroke="var(--text-secondary)" stroke-width="1" opacity="0.3" />
                                <text x={ml.x} y={ml.y} text-anchor="middle" fill="var(--text-secondary)" font-size="8" font-family="Inter, system-ui, sans-serif" font-weight="600" letter-spacing="0.5" opacity="0.85" style="pointer-events: none">{ml.label}</text>
                            {/each}
                            <rect x="0" y="0" width="600" height="160" fill="transparent" />
                            {#if hoverPoint}
                                <line x1={hoverPoint.x} y1="8" x2={hoverPoint.x} y2="132" stroke="var(--accent)" stroke-width="1" stroke-dasharray="3,3" opacity="0.3" />
                                <circle cx={hoverPoint.x} cy={hoverPoint.y} r="5" fill="#CCFBF1" stroke={CYAN_LINE} stroke-width="2" filter="url(#nwDotGlow)" />
                                <rect x={hoverPoint.x > 480 ? hoverPoint.x - 130 : hoverPoint.x + 10} y={Math.max(4, hoverPoint.y - 36)} width="120" height="32" rx="6" fill="var(--bg-card)" stroke="var(--accent)" stroke-width="1" opacity="0.95" />
                                <text x={hoverPoint.x > 480 ? hoverPoint.x - 70 : hoverPoint.x + 70} y={Math.max(4, hoverPoint.y - 36) + 13} text-anchor="middle" fill="var(--text-primary)" font-size="10" font-weight="700" font-family="JetBrains Mono, monospace" style="pointer-events: none">{(void privacyKey, formatCurrency(hoverPoint.value))}</text>
                                <text x={hoverPoint.x > 480 ? hoverPoint.x - 70 : hoverPoint.x + 70} y={Math.max(4, hoverPoint.y - 36) + 26} text-anchor="middle" fill="var(--text-muted)" font-size="8" font-weight="500" font-family="Inter, system-ui, sans-serif" style="pointer-events: none">{hoverPoint.date}</text>
                            {/if}
                        </svg>
                    </div>
                {/if}

                <div class="flex items-center justify-between mb-1">
                    <p class="text-[10px] font-bold tracking-[0.18em] uppercase" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">Net Worth</p>
                    {#if netWorthDelta !== null}
                        <span class="text-[10px] font-mono font-bold px-2.5 py-0.5 rounded-lg"
                            style="background: {netWorthDelta >= 0 ? 'rgba(52,211,153,0.18)' : 'rgba(248,113,113,0.18)'}; color: {netWorthDelta >= 0 ? '#34d399' : '#f87171'}">
                            {netWorthDelta >= 0 ? '▲' : '▼'} {(void privacyKey, formatCurrency(Math.abs(netWorthDelta)))}
                        </span>
                    {/if}
                </div>
                <p class="text-[2.5rem] md:text-[2.75rem] font-extrabold font-display mt-0 mb-2 tracking-tight leading-none"
                   style="color: {$darkMode ? 'var(--text-primary)' : 'var(--island-text-primary)'}; opacity: {animationStarted ? 1 : 0}; transition: opacity 0.2s ease-out;">
                    {formatCurrency(animationDone ? netWorth : animatedNetWorth)}
                </p>
            </div>

            <!-- ——— CENTER ZONE: Accounts ——— -->
            <div class="hero-zone hero-zone-center">
                <div class="flex items-center gap-2 mb-3">
                    <div class="w-7 h-7 rounded-lg flex items-center justify-center" style="background: {$darkMode ? 'rgba(74, 222, 128, 0.12)' : 'rgba(74, 222, 128, 0.15)'}">
                        <span class="material-symbols-outlined text-[15px]" style="color: #4ade80">account_balance</span>
                    </div>
                    <p class="text-[9px] font-bold tracking-[0.15em] uppercase" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">Accounts</p>
                    <p class="ml-auto text-base font-bold font-mono" style="color: {$darkMode ? 'var(--positive)' : 'var(--island-positive)'}">{(void privacyKey, formatCurrency(totalCash))}</p>
                </div>
                <div class="hero-account-scroll-wrapper">
                    <div class="hero-account-scroll" use:checkScrollOverflow>
                        {#each cashAccounts as acc}
                            {@const accPct = totalCash > 0 ? (parseFloat(acc.balance || 0) / totalCash) * 100 : 0}
                            <div class="hero-account-row">
                            <div class="flex items-center gap-2 flex-1 min-w-0">
                                <span class="material-symbols-outlined text-[14px]" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">account_balance</span>
                                <div class="flex-1 min-w-0">
                                    <p class="text-[11px] font-medium truncate" style="color: {$darkMode ? 'var(--text-primary)' : 'var(--island-text-primary)'}">{acc.name}</p>
                                    <p class="text-[9px]" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">{acc.type.replace('_', ' ')}</p>
                                    <!-- Proportion bar -->
                                    <div class="account-proportion-track">
                                        <div class="account-proportion-fill" style="width: {accPct}%"></div>
                                    </div>
                                </div>
                            </div>
                            <div class="text-right flex-shrink-0 ml-2">
                                <p class="text-[12px] font-bold font-mono" style="color: {$darkMode ? 'var(--text-primary)' : 'var(--island-text-primary)'}">{(void privacyKey, formatCurrency(acc.balance))}</p>
                                <p class="text-[9px] font-mono" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">
                                    {accPct.toFixed(0)}%
                                    {#if currentMonthData && prevMonthData && accountDeltas._portfolioDelta !== undefined}
                                        <span style="color: {accountDeltas._portfolioDelta >= 0 ? 'var(--positive)' : 'var(--negative)'}; font-size: 8px;">
                                            {accountDeltas._portfolioDelta >= 0 ? '▲' : '▼'}
                                        </span>
                                    {/if}
                                </p>
                            </div>
                        </div>
                    {/each}
                </div>
                </div>
                {#if currentMonthData && prevMonthData}
                    <div class="hero-zone-footer">
                        <span class="text-[9px] font-medium" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">vs last month</span>
                        <span class="delta-badge {(currentMonthData.net) >= 0 ? 'delta-up' : 'delta-down'}" style="font-size: 9px;">
                            {currentMonthData.net >= 0 ? '▲' : '▼'} {(void privacyKey, formatCurrency(Math.abs(currentMonthData.net)))} net
                        </span>
                    </div>
                {/if}
            </div>

            <!-- ——— RIGHT ZONE: Credit Cards ——— -->
            <div class="hero-zone hero-zone-right">
                <div class="flex items-center gap-2 mb-3">
                    <div class="w-7 h-7 rounded-lg flex items-center justify-center" style="background: {$darkMode ? 'rgba(245, 158, 11, 0.12)' : 'rgba(245, 158, 11, 0.15)'}">
                        <span class="material-symbols-outlined text-[15px]" style="color: #f59e0b">account_balance_wallet</span>
                    </div>
                    <p class="text-[9px] font-bold tracking-[0.15em] uppercase" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">Liabilities</p>
                    <p class="ml-auto text-base font-bold font-mono" style="color: {$darkMode ? 'var(--warning)' : 'var(--island-warning)'}">{(void privacyKey, formatCurrency(totalOwed))}</p>
                </div>
                <div class="hero-account-scroll-wrapper">
                    <div class="hero-account-scroll" use:checkScrollOverflow>
                        {#each creditAccounts as acc}
                        {@const util = getUtilization(acc)}
                        {@const liabilityIcon = acc.account_type === 'loan'
                            ? (acc.type === 'mortgage' ? 'home' : acc.type === 'auto' ? 'directions_car' : acc.type === 'student' ? 'school' : 'account_balance')
                            : 'credit_card'}
                        {@const isLoan = acc.account_type === 'loan'}
                        <div class="hero-account-row">
                            <div class="flex items-center gap-2 flex-1 min-w-0">
                                <span class="material-symbols-outlined text-[14px]" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">{liabilityIcon}</span>
                                <div class="flex-1 min-w-0">
                                    <p class="text-[11px] font-medium truncate" style="color: {$darkMode ? 'var(--text-primary)' : 'var(--island-text-primary)'}">{acc.name}</p>
                                    <p class="text-[9px]" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">{acc.type.replace('_', ' ')}</p>
                                    {#if !isLoan}
                                        <div class="utilization-bar-track">
                                            <div class="utilization-bar-fill {util > 70 ? 'high' : ''}" style="width: {util}%"></div>
                                        </div>
                                    {/if}
                                </div>
                            </div>
                            <p class="text-[12px] font-bold font-mono ml-3" style="color: {parseFloat(acc.balance) > 0 ? ($darkMode ? 'var(--warning)' : 'var(--island-warning)') : ($darkMode ? 'var(--positive)' : 'var(--island-positive)')}">
                                {(void privacyKey, formatCurrency(acc.balance))}
                            </p>
                        </div>
                    {/each}
                    {#if creditAccounts.length === 0}
                        <p class="text-xs py-2 text-center" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">No liabilities</p>
                    {/if}
                </div>
                </div>
                {#if totalOwed > 0}
                    <div class="hero-zone-footer">
                        <span class="text-[9px] font-medium" style="color: {$darkMode ? 'var(--text-muted)' : 'var(--island-text-muted)'}">Total owed</span>
                        <span class="text-[9px] font-mono font-semibold" style="color: {$darkMode ? 'var(--warning)' : 'var(--island-warning)'}; opacity: 0.8">
                            {(void privacyKey, formatCurrency(totalOwed))}
                        </span>
                    </div>
                {/if}
            </div>

        </div>
    </section>

    <!-- ═══════════════════════════════════════════════════════
         S2: COMPACT METRIC RIBBON
         ═══════════════════════════════════════════════════════ -->
    <section class="mb-6 fade-in-up" style="animation-delay: 100ms">
        <div class="flex items-center justify-between mb-3">
            <p class="section-header">Income & Spending</p>
            <div class="flex items-center gap-2">
                <div class="period-toggle-track" style="--seg-count: {periodOptions.length}; --active-idx: {activePeriodIdx};">
                    <div class="period-toggle-thumb"></div>
                    {#each periodOptions as p}
                        <button class="period-toggle-label" class:active={selectedPeriod === p.key}
                            on:click={() => { selectedPeriod = p.key; }}>
                            {p.label}
                        </button>
                    {/each}
                </div>
                <div class="month-dropdown-wrapper">
                    <button
                        class="month-dropdown-trigger"
                        class:ring-2={selectedPeriod === 'custom'}
                        class:ring-accent={selectedPeriod === 'custom'}
                        on:click={() => monthDropdownOpen = !monthDropdownOpen}
                    >
                        <span>{formatMonth(selectedCustomMonth)}</span>
                        <span class="material-symbols-outlined text-[13px]"
                              style="opacity: 0.5; transition: transform 0.2s;"
                              class:rotate-180={monthDropdownOpen}>
                            expand_more
                        </span>
                    </button>

                    {#if monthDropdownOpen}
                        <!-- svelte-ignore a11y-click-events-have-key-events -->
                        <div class="month-dropdown-backdrop" on:click={() => monthDropdownOpen = false}></div>
                        <div class="month-dropdown-menu" role="listbox">
                            {#each allMonths as m}
                                <button
                                    class="month-dropdown-item"
                                    class:month-dropdown-item-active={selectedCustomMonth === m}
                                    role="option"
                                    aria-selected={selectedCustomMonth === m}
                                    on:click={() => {
                                        selectedCustomMonth = m;
                                        monthDropdownOpen = false;
                                        handleCustomMonthChange();
                                    }}
                                >
                                    {formatMonth(m)}
                                </button>
                            {/each}
                        </div>
                    {/if}
                </div>
            </div>
        </div>

        {#if periodSummary}
            <div class="metric-ribbon fade-in" class:period-updating={periodLoading}>
                <div class="metric-ribbon-item">
                    <span class="metric-ribbon-label">Income</span>
                    <span class="metric-ribbon-value text-positive">
                        {formatCurrency(animationStarted ? (animationDone ? periodSummary.income : animatedIncome) : (periodSummary?.income ?? 0))}
                    </span>
                    {#if selectedPeriod === 'this_month' && incomeDelta !== null}
                        <span class="metric-ribbon-sub">
                            <span class="delta-badge {incomeDelta >= 0 ? 'delta-up' : 'delta-down'}">
                                {incomeDelta >= 0 ? '▲' : '▼'} {formatPercent(Math.abs(incomeDelta))}
                            </span>
                        </span>
                    {/if}
                </div>
                <div class="metric-ribbon-item">
                    <span class="metric-ribbon-label">Expenses</span>
                    <span class="metric-ribbon-value text-negative">
                        {formatCurrency(animationStarted ? (animationDone ? periodSummary.expenses : animatedExpenses) : (periodSummary?.expenses ?? 0))}
                    </span>
                    <span class="metric-ribbon-sub">
                        {#if selectedPeriod === 'this_month' && expenseDelta !== null}
                            <span class="delta-badge {expenseDelta <= 0 ? 'delta-up' : 'delta-down'}">
                                {expenseDelta >= 0 ? '▲' : '▼'} {formatPercent(Math.abs(expenseDelta))}
                            </span>
                        {/if}
                        {#if topCatInsight && selectedPeriod === 'this_month'}
                            <span class="ml-1" style="color: var(--text-muted)">
                                · <span style="color: {CATEGORY_COLORS[topCatInsight.name]}" class="font-semibold">{topCatInsight.name}</span>
                                {formatPercent(topCatInsight.pctOfExpenses)}
                            </span>
                        {/if}
                    </span>
                </div>
                <div class="metric-ribbon-item metric-derived metric-derived-first">
                    <span class="metric-ribbon-label">Net Flow</span>
                    <span class="metric-ribbon-value" style="color: {periodSummary.net_flow >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                        {periodSummary.net_flow >= 0 ? '+' : ''}{formatCurrency(periodSummary.net_flow)}
                    </span>
                </div>
                <div class="metric-ribbon-item metric-derived">
                    <span class="metric-ribbon-label">
                        Savings Rate
                        {#if trailingSavingsInfo.months > 1}
                            <span style="font-weight: 500; letter-spacing: 0.05em; text-transform: none; opacity: 0.7">
                                ({trailingSavingsInfo.months}mo avg)
                            </span>
                        {/if}
                    </span>
                    <span class="metric-ribbon-value" style="color: var(--accent)">
                        {savingsRate > 0 ? formatPercent(savingsRate) : '—'}
                    </span>
                    {#if savingsRate > 0}
                        <div class="flex items-center gap-2 mt-0.5">
                            <div class="flex-1 h-1 rounded-full" style="background: var(--surface-200)">
                                <div class="h-1 rounded-full transition-all duration-700" style="width: {Math.min(savingsRate, 100)}%; background: var(--accent)"></div>
                            </div>
                            {#if savingsRateDelta !== 0}
                                <span class="text-[9px] font-mono font-semibold" style="color: {savingsRateDelta >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                    {savingsRateDelta >= 0 ? '↑' : '↓'}{formatPercent(Math.abs(savingsRateDelta))}
                                </span>
                            {/if}
                        </div>
                    {/if}
                </div>
                {#if dailyPace}
                    <div class="metric-ribbon-item metric-derived" style="border-left: 1px solid var(--card-border)">
                        <span class="metric-ribbon-label">Pace</span>
                        <span class="metric-ribbon-value" style="color: {dailyPace.projected > (periodSummary.income || Infinity) ? 'var(--negative)' : 'var(--text-secondary)'}">
                            {formatCurrency(dailyPace.dailyAvg)}/day
                        </span>
                        <span class="metric-ribbon-sub">
                            → {formatCurrency(dailyPace.projected)} projected
                        </span>
                    </div>
                {/if}
            </div>
        {/if}
    </section>

    <!-- ═══════════════════════════════════════════════════════
         S3: MONEY FLOW (SANKEY)
         ═══════════════════════════════════════════════════════ -->
    <section class="mb-10 fade-in-up" style="animation-delay: 140ms">
        <div class="flex items-center justify-between mb-4">
            <div class="flex items-center gap-2">
                <div class="section-accent-bar"></div>
                <p class="section-header">Money Flow</p>
                {#if periodSummary && periodSummary.income < periodSummary.expenses + sankeySavingsTotal + sankeyPersonalTransferTotal}
                    <span class="text-[9px] font-semibold px-2 py-0.5 rounded-md"
                        style="background: rgba(251, 191, 36, 0.12); color: var(--warning); border: 1px solid rgba(251, 191, 36, 0.20);">
                        Drawing from balance
                    </span>
                {/if}
            </div>
            {#if selectedSankeyCategory}
                <button on:click={() => { selectedSankeyCategory = null; sankeyDrillTxns = []; }}
                    class="flex items-center gap-1 text-[11px] px-3 py-1.5 rounded-lg hover:opacity-80 transition-opacity"
                    style="background: var(--surface-100); color: var(--text-secondary)">
                    <span class="material-symbols-outlined text-[14px]">close</span> Clear
                </button>
            {/if}
        </div>

        <div class="sankey-theater"
             class:period-updating={periodLoading}
             bind:this={sankeyTheaterEl}
             on:mousemove={handleTheaterMouseMove}
             on:mouseleave={handleTheaterMouseLeave}>
            <div class="sankey-theater-glow" style="opacity: {sankeyGlowOpacity};"></div>
            {#if periodSummary}
                <SankeyChart
                    income={periodSummary.income}
                    expenses={periodSummary.expenses}
                    savingsTransfer={sankeySavingsTotal}
                    personalTransfer={sankeyPersonalTransferTotal}
                    categories={sankeyCategoryList.filter(c => !c.isDirectFlow)}
                    selectedCategory={selectedSankeyCategory}
                    height={340}
                    autoHeight={true}
                    on:select={handleSankeySelect}
                />
            {/if}
        </div>

        {#if sankeyCategoryList.length > 0}
            <div class="sankey-pill-card">
                <div class="sankey-pill-card-header">
                    <span class="material-symbols-outlined text-[13px]" style="color: var(--text-muted); opacity: 0.7">category</span>
                    <span class="sankey-pill-card-title">Categories</span>
                    <span class="sankey-pill-card-count">{sankeyCategoryList.slice(0, 12).length} flows</span>
                </div>
                <div bind:this={drillDownSection} class="sankey-pill-grid">
                    {#each sankeyCategoryList.slice(0, 12) as cat}
                        {@const catColor = CATEGORY_COLORS[cat.category] || '#627d98'}
                        {@const isActive = selectedSankeyCategory === cat.category}
                        {@const isDimmed = selectedSankeyCategory && !isActive}
                        <button
                            on:click={() => handleSankeySelect({ detail: isActive ? null : cat.category })}
                            class="sankey-cat-pill"
                            class:pill-active={isActive}
                            class:pill-dimmed={isDimmed}
                            style="color: {catColor};
                                   background: color-mix(in srgb, {catColor} {isActive ? 'var(--pill-bg-active)' : '6'}%, transparent);
                                   border-color: color-mix(in srgb, {catColor} {isActive ? 'var(--pill-border-active)' : '12'}%, transparent);">
                            <span class="material-symbols-outlined text-[13px]">{CATEGORY_ICONS[cat.category] || 'label'}</span>
                            {cat.category}
                            <span class="sankey-cat-pill-value">{formatCompact(cat.total)}</span>
                            {#if cat.isDirectFlow}
                                <span class="sankey-cat-pill-direct">→</span>
                            {/if}
                        </button>
                    {/each}
                </div>
            </div>
        {/if}

        <!-- Drill-down transactions -->
        <div>
            {#if selectedSankeyCategory && sankeyDrillTxns.length > 0}
                {@const drillTotal = getDrillDownTotal(selectedSankeyCategory)}
                <div class="card overflow-hidden mt-3 fade-in" style="padding: 0">
                    <div class="flex items-center gap-3 px-5 py-2.5" style="border-bottom: 1px solid var(--card-border); background: var(--surface-100)">
                        <div class="w-6 h-6 rounded-lg flex items-center justify-center"
                            style="background: color-mix(in srgb, {CATEGORY_COLORS[selectedSankeyCategory] || '#627d98'} 15%, transparent)">
                            <span class="material-symbols-outlined text-[14px]" style="color: {CATEGORY_COLORS[selectedSankeyCategory] || '#627d98'}">
                                {CATEGORY_ICONS[selectedSankeyCategory] || 'label'}
                            </span>
                        </div>
                        <div>
                            <p class="text-[13px] font-semibold" style="color: var(--text-primary)">{selectedSankeyCategory}</p>
                            <p class="text-[10px]" style="color: var(--text-muted)">{sankeyDrillTxns.length} transactions · by amount</p>
                        </div>
                        <p class="ml-auto text-[13px] font-bold font-mono" style="color: var(--negative)">{formatCurrency(drillTotal)}</p>
                    </div>
                    {#each sankeyDrillTxns.slice(0, 10) as tx}
                        {@const amount = parseFloat(tx.amount)}
                        <div class="flex items-center gap-4 px-5 py-2" style="border-bottom: 1px solid color-mix(in srgb, var(--card-border) 50%, transparent)">
                            <div class="flex-1 min-w-0">
                                <p class="text-[13px] font-medium truncate" style="color: var(--text-primary)">{tx.description}</p>
                                <p class="text-[10px]" style="color: var(--text-muted)">{formatDate(tx.date)} · {tx.account_name}</p>
                            </div>
                            <div class="flex items-center gap-2 flex-shrink-0">
                                <p class="text-[13px] font-bold font-mono" style="color: {amount >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                    {amount >= 0 ? '+' : ''}{formatCurrency(amount, 2)}
                                </p>
                                {#if amount > 0 && (selectedSankeyCategory === 'Savings Transfer' || selectedSankeyCategory === 'Personal Transfer')}
                                    <span class="text-[8px] font-semibold px-1.5 py-0.5 rounded-full" style="background: color-mix(in srgb, var(--text-muted) 12%, transparent); color: var(--text-muted)">MIRROR</span>
                                {:else if amount > 0}
                                    <span class="text-[8px] font-semibold px-1.5 py-0.5 rounded-full" style="background: var(--positive-light); color: var(--positive)">REFUND</span>
                                {:else if selectedSankeyCategory === 'Savings Transfer' || selectedSankeyCategory === 'Personal Transfer'}
                                    <span class="text-[8px] font-semibold px-1.5 py-0.5 rounded-full" style="background: color-mix(in srgb, #3b82f6 12%, transparent); color: #3b82f6">OUTFLOW</span>
                                {/if}
                            </div>
                        </div>
                    {/each}
                    {#if sankeyDrillTxns.length > 10}
                        <div class="px-5 py-2.5 text-center">
                            <a href="/transactions" class="text-[11px] font-medium" style="color: var(--accent)">
                                View all {sankeyDrillTxns.length} transactions →
                            </a>
                        </div>
                    {/if}
                </div>
            {:else if selectedSankeyCategory && sankeyDrillTxns.length === 0}
                <div class="card mt-3 fade-in">
                    <p class="text-sm text-center py-3" style="color: var(--text-muted)">No transactions found.</p>
                </div>
            {/if}
        </div>
    </section>

    <!-- ═══════════════════════════════════════════════════════
         TRENDS & TRAJECTORY — FULL HISTORY
         ═══════════════════════════════════════════════════════ -->

    <div class="flex items-center gap-3 mb-6 fade-in-up" style="animation-delay: 180ms">
        <div class="section-accent-bar"></div>
        <p class="section-header">Trends & Trajectory</p>
        <span class="section-zone-label">Full History</span>
    </div>

    <!-- ═══ S4: MONTHLY NET SURPLUS / DEFICIT ═══ -->
    <section class="mb-6 fade-in-up" style="animation-delay: 200ms">
        <div class="flex items-center justify-between mb-3">
            <p class="section-header">Monthly Net Surplus / Deficit</p>
            <div class="flex items-center gap-1.5">
                <span class="w-3 h-1 rounded-full" style="background: rgba(13, 150, 104, 0.75)"></span>
                <span class="text-[10px] font-medium" style="color: var(--text-muted)">Surplus</span>
                <span class="w-3 h-1 rounded-full ml-2" style="background: rgba(220, 53, 69, 0.75)"></span>
                <span class="text-[10px] font-medium" style="color: var(--text-muted)">Deficit</span>
            </div>
        </div>
        <div class="card" style="background-image: var(--surface-trend)">
            {#if monthlyNetSVG}
                {@const c = monthlyNetSVG}
                <div class="monthly-net-chart-wrap">
                    <svg viewBox="0 0 {c.W} {c.H}" preserveAspectRatio="xMidYMid meet" class="monthly-net-svg"
                        on:mousemove={(e) => {
                            const svg = e.currentTarget;
                            const pt = svg.createSVGPoint();
                            pt.x = e.clientX;
                            pt.y = e.clientY;
                            const svgPt = pt.matrixTransform(svg.getScreenCTM().inverse());
                            const mouseX = svgPt.x;
                            const slotIdx = Math.floor((mouseX - c.pad.left) / c.step);
                            const idx = (slotIdx >= 0 && slotIdx < c.bars.length) ? slotIdx : -1;
                            if (idx >= 0) {
                                const bar = c.bars[idx];
                                hoveredBarIdx = idx;
                                hoveredBar = {
                                    x: bar.x + bar.w / 2,
                                    y: bar.value >= 0 ? bar.y - 8 : bar.y + bar.h + 8,
                                    label: bar.label,
                                    value: bar.value,
                                    above: bar.value >= 0
                                };
                            } else {
                                hoveredBarIdx = -1;
                                hoveredBar = null;
                            }
                        }}
                        on:mouseleave={() => { hoveredBarIdx = -1; hoveredBar = null; }}>

                        <defs>
                            <!-- Glow filter for the zero baseline -->
                            <filter id="zeroLineGlow" x="-2%" y="-50%" width="104%" height="200%">
                                <feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/>
                                <feMerge>
                                    <feMergeNode in="blur"/>
                                    <feMergeNode in="SourceGraphic"/>
                                </feMerge>
                            </filter>
                            <!-- Glow filter for the average line -->
                            <filter id="avgLineGlow" x="-1%" y="-50%" width="102%" height="200%">
                                <feGaussianBlur in="SourceGraphic" stdDeviation="2" result="blur"/>
                                <feMerge>
                                    <feMergeNode in="blur"/>
                                    <feMergeNode in="SourceGraphic"/>
                                </feMerge>
                            </filter>
                        </defs>

                        <!-- Y-axis grid + labels -->
                        {#each c.yTicks as tick}
                            <line
                                x1={c.pad.left} x2={c.W - c.pad.right}
                                y1={tick.y} y2={tick.y}
                                stroke="var(--text-muted)" stroke-width="1" opacity="0.06"
                            />
                            <text
                                x={c.pad.left - 8} y={tick.y + 3.5}
                                text-anchor="end" fill="var(--text-muted)" font-size="10"
                                font-family="JetBrains Mono, monospace" opacity="0.7"
                            >{$privacyMode ? '$•••' : ((tick.value >= 0 ? '' : 'â') + '$' + (Math.abs(tick.value) >= 1000 ? (Math.abs(tick.value) / 1000).toFixed(1) + 'k' : Math.abs(tick.value).toFixed(0)))}</text>
                        {/each}

                        <!-- Zero line zone tints (subtle positive/negative regions) -->
                        <rect
                            x={c.pad.left} y={c.pad.top}
                            width={c.W - c.pad.left - c.pad.right}
                            height={c.zeroY - c.pad.top}
                            fill={$darkMode ? 'rgba(52, 211, 153, 0.02)' : 'rgba(5, 150, 105, 0.02)'}
                        />
                        <rect
                            x={c.pad.left} y={c.zeroY}
                            width={c.W - c.pad.left - c.pad.right}
                            height={c.pad.top + (c.H - c.pad.top - c.pad.bottom) - c.zeroY + c.pad.top}
                            fill={$darkMode ? 'rgba(248, 113, 113, 0.02)' : 'rgba(225, 29, 72, 0.02)'}
                        />

                        <!-- Zero line — premium accent glow baseline -->
                        <line
                            x1={c.pad.left} x2={c.W - c.pad.right}
                            y1={c.zeroY} y2={c.zeroY}
                            stroke="var(--accent)" stroke-width="1.5" opacity="0.40"
                            filter="url(#zeroLineGlow)"
                        />
                        <!-- Zero label -->
                        <text
                            x={c.pad.left - 8} y={c.zeroY + 3.5}
                            text-anchor="end"
                            fill={$darkMode ? 'rgba(255,255,255,0.50)' : 'rgba(0,0,0,0.45)'}
                            font-size="9"
                            font-family="JetBrains Mono, monospace"
                            font-weight="700"
                        >{$privacyMode ? '$•••' : '$0'}</text>

                        <!-- Bars with rounded corners + hover highlight -->
                        {#each c.bars as bar, i}
                            {@const isHovered = hoveredBarIdx === i}
                            {@const barRx = 4}
                            {#if bar.value >= 0}
                                <!-- Positive bar: rounded top corners via clipPath trick -->
                                <rect
                                    x={bar.x} y={bar.y} width={bar.w} height={bar.h + barRx}
                                    rx={barRx} ry={barRx}
                                    fill="{bar.fill}{isHovered ? Math.min(bar.opacity + 0.25, 1) : bar.opacity})"
                                    stroke={isHovered ? (bar.value >= 0 ? 'rgba(13, 150, 104, 0.9)' : 'rgba(220, 53, 69, 0.9)') : bar.strokeColor}
                                    stroke-width={isHovered ? 2 : (bar.isLast ? 2 : 0)}
                                    class="monthly-net-bar"
                                    style="animation-delay: {i * 30}ms; cursor: pointer; transition: fill 0.15s ease, stroke 0.15s ease;"
                                />
                                <!-- Clip the bottom rounded corners by overlaying a rect -->
                                <rect
                                    x={bar.x} y={bar.y + bar.h} width={bar.w} height={barRx}
                                    fill="{bar.fill}{isHovered ? Math.min(bar.opacity + 0.25, 1) : bar.opacity})"
                                />
                            {:else}
                                <!-- Negative bar: rounded bottom corners -->
                                <rect
                                    x={bar.x} y={bar.y - barRx} width={bar.w} height={bar.h + barRx}
                                    rx={barRx} ry={barRx}
                                    fill="{bar.fill}{isHovered ? Math.min(bar.opacity + 0.25, 1) : bar.opacity})"
                                    stroke={isHovered ? 'rgba(220, 53, 69, 0.9)' : bar.strokeColor}
                                    stroke-width={isHovered ? 2 : (bar.isLast ? 2 : 0)}
                                    class="monthly-net-bar"
                                    style="animation-delay: {i * 30}ms; cursor: pointer; transition: fill 0.15s ease, stroke 0.15s ease;"
                                />
                                <!-- Clip the top rounded corners -->
                                <rect
                                    x={bar.x} y={bar.y - barRx} width={bar.w} height={barRx}
                                    fill="{bar.fill}{isHovered ? Math.min(bar.opacity + 0.25, 1) : bar.opacity})"
                                />
                            {/if}
                        {/each}

                        <!-- Average dashed line — enhanced contrast with glow -->
                        <line
                            x1={c.pad.left} x2={c.W - c.pad.right}
                            y1={c.avgY} y2={c.avgY}
                            stroke={$darkMode ? '#7CB9E8' : '#2563EB'} stroke-width="2" stroke-dasharray="8,4" opacity="0.85"
                            filter="url(#avgLineGlow)"
                        />

                        <!-- Average label — pill-style badge at right end -->
                        <rect
                            x={c.W - c.pad.right - 120} y={c.avgY - 14}
                            width="118" height="18" rx="9"
                            fill={$darkMode ? 'rgba(30, 58, 95, 0.85)' : 'rgba(219, 234, 254, 0.90)'}
                            stroke={$darkMode ? 'rgba(124, 185, 232, 0.30)' : 'rgba(37, 99, 235, 0.25)'}
                            stroke-width="1"
                        />
                        <text
                            x={c.W - c.pad.right - 61} y={c.avgY - 2}
                            text-anchor="middle" fill={$darkMode ? '#7CB9E8' : '#2563EB'}
                            font-size="9" font-family="JetBrains Mono, monospace" font-weight="700"
                        >AVG {$privacyMode ? '$•••••' : formatCurrency(c.avg)}/mo</text>

                        <!-- X-axis labels -->
                        {#each c.bars as bar, i}
                            <text
                                x={bar.x + bar.w / 2}
                                y={c.H - c.pad.bottom + 18}
                                text-anchor="middle" fill="var(--text-muted)" font-size="10"
                                font-family="Inter, system-ui, sans-serif"
                                transform="rotate(-30, {bar.x + bar.w / 2}, {c.H - c.pad.bottom + 18})"
                            >{bar.label}</text>
                        {/each}

                        <!-- Hover tooltip -->
                        {#if hoveredBar}
                            {@const tt = hoveredBar}
                            {@const ttW = 130}
                            {@const ttH = 36}
                            {@const ttX = Math.max(c.pad.left, Math.min(tt.x - ttW / 2, c.W - c.pad.right - ttW))}
                            {@const ttY = tt.above ? tt.y - ttH - 4 : tt.y + 4}
                            <rect
                                x={ttX} y={ttY}
                                width={ttW} height={ttH} rx="8"
                                fill={$darkMode ? 'rgba(15, 23, 42, 0.92)' : 'rgba(255, 255, 255, 0.95)'}
                                stroke={$darkMode ? 'rgba(148, 163, 184, 0.20)' : 'rgba(100, 116, 139, 0.20)'}
                                stroke-width="1"
                                filter="url(#avgLineGlow)"
                            />
                            <text
                                x={ttX + ttW / 2} y={ttY + 14}
                                text-anchor="middle" fill="var(--text-primary)"
                                font-size="10" font-weight="700" font-family="JetBrains Mono, monospace"
                                style="pointer-events: none"
                            >{$privacyMode ? '$•••••' : ((tt.value >= 0 ? '+' : '') + formatCurrency(tt.value))}</text>
                            <text
                                x={ttX + ttW / 2} y={ttY + 28}
                                text-anchor="middle" fill="var(--text-muted)"
                                font-size="8" font-weight="500" font-family="Inter, system-ui, sans-serif"
                                style="pointer-events: none"
                            >{tt.label}</text>
                        {/if}
                    </svg>
                </div>
            {/if}

            {#if monthlyNetStats}
                <div class="monthly-net-stats">
                    <div class="monthly-net-stat">
                        <p class="monthly-net-stat-label">Surplus Months</p>
                        <p class="monthly-net-stat-value" style="color: var(--positive)">
                            {monthlyNetStats.surplusCount}/{monthlyNetStats.totalMonths}
                        </p>
                        <p class="monthly-net-stat-sub">Avg: {formatCurrency(monthlyNetStats.avgSurplus)}</p>
                    </div>
                    <div class="monthly-net-stat">
                        <p class="monthly-net-stat-label">Deficit Months</p>
                        <p class="monthly-net-stat-value" style="color: var(--negative)">
                            {monthlyNetStats.deficitCount}/{monthlyNetStats.totalMonths}
                        </p>
                        <p class="monthly-net-stat-sub">Avg: {formatCurrency(monthlyNetStats.avgDeficit)}</p>
                    </div>
                    <div class="monthly-net-stat">
                        <p class="monthly-net-stat-label">Eff. Save Rate</p>
                        <p class="monthly-net-stat-value" style="color: var(--accent)">
                            {formatPercent(monthlyNetStats.effectiveSaveRate)}
                        </p>
                        <p class="monthly-net-stat-sub">Avg net: {formatCurrency(monthlyNetStats.avgNet)}/mo</p>
                    </div>
                </div>
            {/if}
        </div>
    </section>

    <!-- ═══ S5: TWO-PANEL ROW (YoY + Balance Trajectory) ═══ -->
    <section class="mb-10 fade-in-up" style="animation-delay: 240ms">
        <div class="trends-two-panel">

            <!-- ——— S5a: YoY Same-Period Comparison ——— -->
            <div class="card" style="padding: 1.25rem">
                <p class="section-header mb-3">Year-over-Year</p>

                {#if yoyData}
                    <div class="space-y-0">
                        {#each yoyData.yearStats as ys, yi}
                            {@const isCurrent = yi === yoyData.yearStats.length - 1}
                            <div class="yoy-year-row">
                                <span class="yoy-year-label">
                                    {ys.year}
                                    <span class="yoy-period-tag">{ys.periodLabel}</span>
                                </span>
                                <div class="yoy-bars">
                                    <div class="yoy-bar-row">
                                        <span class="yoy-bar-label">In</span>
                                        <div class="yoy-bar-track">
                                            <div class="yoy-bar-fill {isCurrent ? '' : 'yoy-bar-ghost'}" style="width: {(ys.totalIn / yoyData.maxVal) * 100}%; {isCurrent ? 'background: var(--positive)' : 'background: transparent; border: 1.5px solid var(--positive); box-shadow: inset 0 0 0 100px rgba(5, 150, 105, 0.12); box-sizing: border-box;'}"></div>
                                        </div>
                                        <span class="yoy-bar-value" style="{isCurrent ? '' : 'opacity: 0.6'}">{formatCompact(ys.totalIn)}</span>
                                    </div>
                                    <div class="yoy-bar-row">
                                        <span class="yoy-bar-label">Out</span>
                                        <div class="yoy-bar-track">
                                            <div class="yoy-bar-fill {isCurrent ? '' : 'yoy-bar-ghost'}" style="width: {(ys.totalOut / yoyData.maxVal) * 100}%; {isCurrent ? 'background: var(--negative)' : 'background: transparent; border: 1.5px solid var(--negative); box-shadow: inset 0 0 0 100px rgba(225, 29, 72, 0.12); box-sizing: border-box;'}"></div>
                                        </div>
                                        <span class="yoy-bar-value" style="{isCurrent ? '' : 'opacity: 0.6'}">{formatCompact(ys.totalOut)}</span>
                                    </div>
                                </div>
                                <span class="yoy-net" style="background: {ys.net >= 0 ? 'var(--positive-light)' : 'var(--negative-light)'}; color: {ys.net >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                    {ys.net >= 0 ? '+' : ''}{formatCompact(ys.net)}
                                </span>
                            </div>
                        {/each}
                    </div>

                    {#if yoyData.delta}
                        <div class="yoy-delta-callout">
                            <span class="yoy-delta-icon">{yoyData.delta.amount >= 0 ? '📈' : '📉'}</span>
                            <p class="yoy-delta-text">
                                <span class="yoy-delta-value" style="color: {yoyData.delta.amount >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                    {yoyData.delta.amount >= 0 ? '+' : ''}{formatCurrency(yoyData.delta.amount)}
                                </span>
                                vs {yoyData.delta.note} in {yoyData.delta.prevYear}
                            </p>
                        </div>
                    {/if}
                {:else}
                    <p class="text-sm text-center py-6" style="color: var(--text-muted)">Not enough data for comparison</p>
                {/if}
            </div>

            <!-- ——— S5b: Net Worth Trajectory ——— -->
            <div class="card" style="padding: 1.25rem">
                <p class="section-header mb-3">Net Worth Trajectory</p>

                {#if netWorthTrendData.length > 1}
                    {#if trajectorySVG}
                        {@const t = trajectorySVG}
                        <div class="trajectory-chart-wrap">
                            <svg viewBox="0 0 {t.W} {t.H}" preserveAspectRatio="xMidYMid meet" class="trajectory-svg">
                                <defs>
                                    <linearGradient id="trajAreaGrad" x1="0" y1="0" x2="0" y2="1">
                                        <stop offset="0%" stop-color={$darkMode ? 'rgba(56,189,248,0.15)' : 'rgba(56,189,248,0.18)'} />
                                        <stop offset="100%" stop-color={$darkMode ? 'rgba(56,189,248,0.01)' : 'rgba(56,189,248,0.02)'} />
                                    </linearGradient>
                                </defs>

                                <!-- Y grid + labels -->
                                {#each t.yTicks as tick}
                                    <line
                                        x1={t.pad.left} x2={t.W - t.pad.right}
                                        y1={tick.y} y2={tick.y}
                                        stroke="var(--text-muted)" stroke-width="1" opacity="0.06"
                                    />
                                    <text
                                        x={t.pad.left - 8} y={tick.y + 3.5}
                                        text-anchor="end" fill="var(--text-muted)" font-size="10"
                                        font-family="JetBrains Mono, monospace" opacity="0.7"
                                    >{$privacyMode ? '$•••' : ('$' + (tick.value >= 1000 ? (tick.value / 1000).toFixed(0) + 'k' : tick.value.toFixed(0)))}</text>
                                {/each}

                                <!-- Area fill -->
                                <path d={t.areaPath} fill="url(#trajAreaGrad)" />

                                <!-- Line -->
                                <path d={t.linePath} fill="none" stroke="#38BDF8" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" />

                                <!-- End dot -->
                                {#if t.points.length > 0}
                                    {@const lastPt = t.points[t.points.length - 1]}
                                    <circle cx={lastPt.x} cy={lastPt.y} r="4" fill="#E0F2FE" stroke="#38BDF8" stroke-width="2" />
                                {/if}

                                <!-- X-axis labels -->
                                {#each t.xLabels as xl}
                                    <text
                                        x={xl.x} y={t.H - 8}
                                        text-anchor="middle" fill="var(--text-muted)" font-size="9"
                                        font-family="Inter, system-ui, sans-serif" opacity="0.6"
                                    >{xl.label}</text>
                                {/each}
                            </svg>
                        </div>
                    {/if}

                    {#if trajectoryStats}
                        <div class="trajectory-stats">
                            <div class="trajectory-stat">
                                <span class="trajectory-stat-label">Change</span>
                                <span class="trajectory-stat-value" style="color: {trajectoryStats.change >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                    {trajectoryStats.change >= 0 ? '▲' : '▼'} {formatCurrency(Math.abs(trajectoryStats.change))}
                                </span>
                            </div>
                            <div class="trajectory-stat">
                                <span class="trajectory-stat-label">Avg/mo</span>
                                <span class="trajectory-stat-value" style="color: {trajectoryStats.avgGrowth >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                    {trajectoryStats.avgGrowth >= 0 ? '+' : ''}{formatCurrency(trajectoryStats.avgGrowth)}
                                </span>
                            </div>
                            <div class="trajectory-stat">
                                <span class="trajectory-stat-label">Peak</span>
                                <span class="trajectory-stat-value" style="color: var(--text-secondary)">
                                    {formatCompact(trajectoryStats.peak)} <span class="text-[8px] opacity-60">({trajectoryStats.peakDate})</span>
                                </span>
                            </div>
                            <div class="trajectory-stat">
                                <span class="trajectory-stat-label">Low</span>
                                <span class="trajectory-stat-value" style="color: var(--text-secondary)">
                                    {formatCompact(trajectoryStats.low)} <span class="text-[8px] opacity-60">({trajectoryStats.lowDate})</span>
                                </span>
                            </div>
                        </div>
                    {/if}
                {:else}
                    <p class="text-sm text-center py-6" style="color: var(--text-muted)">Not enough data</p>
                {/if}
            </div>

        </div>
    </section>
</div>
{/if}