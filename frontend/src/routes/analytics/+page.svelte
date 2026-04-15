<script>
    import '$lib/styles/analytics.css';
    import { onMount, tick } from 'svelte';
    import { api, invalidateCache  } from '$lib/api.js';
    import { darkMode, selectedPeriodStore, selectedCustomMonthStore } from '$lib/stores.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import {
        formatCurrency, formatCompact, formatPercent, formatMonth, formatMonthShort,
        formatDate, formatDateShort, formatDateWithYear, getCurrentMonth, computeDelta,
        CATEGORY_COLORS, CATEGORY_ICONS
    } from '$lib/utils.js';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';

    /* ═══════════════════════════════════════
       STATE
       ═══════════════════════════════════════ */
    export let data;

    let monthly = data.monthly;
    let allCategories = Array.isArray(data.categories) ? data.categories : (data.categories?.categories || []);
    let loading = false;
    let profileSwitching = false;
    let selectedMonth = '';
    let monthPickerOpen = false;
    
    // iOS-style period toggle
    const analyticsPeriods = ['This Month', 'Last Month', 'Custom'];
    let selectedAnalyticsPeriod = 'This Month';
    $: activeAnalyticsPeriodIdx = Math.max(analyticsPeriods.indexOf(selectedAnalyticsPeriod), 0);

    let monthCategories = [];
    let monthTransactions = [];
    let prevMonthCategories = [];
    let prevMonthData = null;

    // Drill-down
    let selectedCategory = '';
    let categoryTransactions = [];
    // Top Merchants
    let topMerchants = [];    

    // Recurring / Subscriptions
    let recurringData = null;
    let recurringLoading = true;
    let activeRecurring = [];
    let inactiveRecurring = [];
    let cancelledRecurring = [];
    // Split recurring items into active/inactive/cancelled whenever recurringData changes
    $: {
        if (recurringData && recurringData.items) {
            activeRecurring = recurringData.items.filter(i => i.status === 'active' && !i.cancelled);
            cancelledRecurring = recurringData.items.filter(i => i.cancelled);
            inactiveRecurring = recurringData.items.filter(i => i.status === 'inactive' && !i.cancelled);
        } else {
            activeRecurring = [];
            inactiveRecurring = [];
            cancelledRecurring = [];
        }
    }

    // Subscription events (alerts)
    let subscriptionEvents = [];
    let unreadEventCount = 0;
    $: {
        if (recurringData && recurringData.events) {
            subscriptionEvents = recurringData.events.filter(e => !e.is_read);
            unreadEventCount = recurringData.unread_event_count || subscriptionEvents.length;
        } else {
            subscriptionEvents = [];
            unreadEventCount = 0;
        }
    }

    // Dismissed subscriptions
    let dismissedRecurring = [];
    let dismissedOpen = false;
    $: {
        if (recurringData && recurringData.dismissed) {
            dismissedRecurring = recurringData.dismissed;
        } else {
            dismissedRecurring = [];
        }
    }

    // Redetect loading state
    let redetectLoading = false;

    // Categories that are not real spending in the accrual model
    const NON_SPENDING_CATEGORIES_SET = new Set(['Savings Transfer', 'Personal Transfer', 'Credit Card Payment', 'Income']);

    // Inactive subscriptions dropdown state (closed by default)
    let inactiveOpen = false;

    // Cancelled subscriptions dropdown state (closed by default)
    let cancelledOpen = false;

    // Total historically spent on inactive subscriptions
    // Uses total_spent from backend, falls back to amount × charge_count
    $: inactiveTotalSpent = inactiveRecurring.reduce(
        (sum, item) => sum + (item.total_spent || (item.amount || 0) * (item.charge_count || 0)), 0
    );

    // Price change aggregation for recurring subscriptions
    let priceChangeCount = 0;
    let priceChangeTotalDelta = 0;
    $: {
        const increases = activeRecurring.filter(i => i.price_change && i.price_change.change > 0);
        priceChangeCount = increases.length;
        priceChangeTotalDelta = increases.reduce((sum, i) => sum + i.price_change.change, 0);
    }

    // Waterfall
    let waterfallEl;
    let waterfallTooltip = { show: false, x: 0, y: 0, label: '', amount: 0, runningFrom: 0, runningTo: 0, count: 0 };

    /* ═══════════════════════════════════════
       LIFECYCLE
       ═══════════════════════════════════════ */
    onMount(async () => {
        // Fetch recurring detection (profile-aware, not month-specific)
        api.getRecurring().then(data => { recurringData = data; recurringLoading = false; }).catch(() => { recurringLoading = false; });

        if (monthly.length > 0) {
            const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));

            // If the dashboard had a specific month selected, use it
            let initialMonth = sorted[0].month;
            let storedPeriod, storedCustom;
            const unsubP = selectedPeriodStore.subscribe(v => { storedPeriod = v; });
            const unsubC = selectedCustomMonthStore.subscribe(v => { storedCustom = v; });
            unsubP(); unsubC();

            if (storedPeriod === 'custom' && storedCustom && sorted.some(m => m.month === storedCustom)) {
                initialMonth = storedCustom;
                selectedAnalyticsPeriod = 'Custom';
            } else if (storedPeriod === 'last_month') {
                const now = new Date();
                const lm = new Date(now.getFullYear(), now.getMonth() - 1, 1);
                const lmStr = `${lm.getFullYear()}-${String(lm.getMonth() + 1).padStart(2, '0')}`;
                if (sorted.some(m => m.month === lmStr)) {
                    initialMonth = lmStr;
                    selectedAnalyticsPeriod = 'Last Month';
                }
            } else {
                // Default case: check if initialMonth matches current month
                const now = new Date();
                const currentMonthStr = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
                if (initialMonth === currentMonthStr) {
                    selectedAnalyticsPeriod = 'This Month';
                } else {
                    selectedAnalyticsPeriod = 'Custom';
                }
            }

            selectedMonth = initialMonth;
            await loadMonthData();
        }
    });

    /* ═══════════════════════════════════════
       DATA LOADING
       ═══════════════════════════════════════ */
    async function loadMonthData() {
        if (!selectedMonth) return;
        try {
            const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
            const currentIdx = sorted.findIndex(m => m.month === selectedMonth);
            const hasPrev = currentIdx >= 0 && currentIdx < sorted.length - 1;
            const prevMonth = hasPrev ? sorted[currentIdx + 1].month : null;

            const promises = [
                api.getCategoryAnalytics(selectedMonth),
                api.getTransactions({ month: selectedMonth, limit: 1000 }).then(res => res.data),
                api.getMerchants(selectedMonth).catch(() => []),
            ];
            if (prevMonth) {
                promises.push(api.getCategoryAnalytics(prevMonth).catch(() => []));
            }

            const results = await Promise.all(promises);
            const catResult0 = results[0];
            monthCategories = Array.isArray(catResult0) ? catResult0 : (catResult0?.categories || []);
            monthTransactions = results[1];
            topMerchants = results[2] || [];
            selectedCategory = '';
            categoryTransactions = [];

            if (hasPrev) {
                prevMonthData = sorted[currentIdx + 1];
                const catResult3 = results[3];
                prevMonthCategories = Array.isArray(catResult3) ? catResult3 : (catResult3?.categories || []);
            } else {
                prevMonthData = null;
                prevMonthCategories = [];
            }
        } catch (e) {
            console.error('Failed to load month data:', e);
        }
    }

    $: if (selectedMonth) loadMonthData();

    // ââ Profile switch: reload all analytics data ââ
    let _prevAnalyticsProfile = null;
    $: if ($activeProfile && $activeProfile !== _prevAnalyticsProfile) {
        if (_prevAnalyticsProfile !== null) {
            reloadAnalyticsForProfile();
        }
        _prevAnalyticsProfile = $activeProfile;
    }

    async function reloadAnalyticsForProfile() {
        profileSwitching = true;
        try {
            const [m, c, rec] = await Promise.all([
                api.getMonthlyAnalytics(),
                api.getCategoryAnalytics(),
                api.getRecurring().catch(() => null)
            ]);
            recurringData = rec;
            monthly = m;
            allCategories = Array.isArray(c) ? c : (c?.categories || []);
            if (monthly.length > 0) {
                const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
                if (!sorted.some(s => s.month === selectedMonth)) {
                    selectedMonth = sorted[0].month;
                }
                await loadMonthData();
            }
        } catch (e) {
            console.error('Failed to reload analytics for profile:', e);
        } finally {
            profileSwitching = false;
        }
    }

    // ── Unified pre-computed analytics context ──
    $: analyticsContext = (() => {
        const currentMonthSummary = monthly.find(m => m.month === selectedMonth) || null;
        const sortedMonthly = [...monthly].sort((a, b) => a.month.localeCompare(b.month));
        const totalMonths = monthly.length;
        return { currentMonthSummary, sortedMonthly, totalMonths };
    })();

    $: currentMonthSummary = analyticsContext.currentMonthSummary;

    /* ═══════════════════════════════════════
       S1: SPENDING PULSE — Anomaly Detection
       ═══════════════════════════════════════ */
    // ââ Cached history map: recomputes only when allCategories or monthly.length changes ââ
    // This is independent of the selected period / month.
    let _historyMapKey = '';
    let _historyMapCache = {};

    $: {
        const hKey = `${allCategories.length}|${monthly.length}|${(allCategories[0]?.category || '')}`;
        if (hKey !== _historyMapKey) {
            _historyMapKey = hKey;
            const totalMonths = monthly.length;
            const map = {};
            for (const allCat of allCategories) {
                const catName = allCat.category;
                const allTimeTotal = allCat.total || 0;
                const naiveAvg = totalMonths > 0 ? allTimeTotal / totalMonths : 0;
                map[catName] = { allTimeTotal, naiveAvg, totalMonths };
            }
            _historyMapCache = map;
        }
    }

    $: spendingPulseCards = (() => {
        if (!monthCategories.length || !monthly.length) return [];

        const totalMonths = analyticsContext.totalMonths;

        // Filter out transfer categories — they're not spending in the accrual model
        const spendingCategories = monthCategories.filter(c => !NON_SPENDING_CATEGORIES_SET.has(c.category));

        return spendingCategories.map(cat => {
            const catName = cat.category;
            const currentTotal = cat.total;

            // Read from cached history map instead of scanning allCategories each time
            const history = _historyMapCache[catName] || { allTimeTotal: 0, naiveAvg: 0, totalMonths };
            const allTimeTotal = history.allTimeTotal;
            const naiveAvg = history.naiveAvg;
            const naiveRatio = naiveAvg > 0 ? currentTotal / naiveAvg : 0;

            let avgTotal = naiveAvg;
            let isPeriodic = false;
            let comparisonLabel = `${totalMonths}-mo avg`;

            if (naiveRatio > 4 && currentTotal > 50 && allTimeTotal > 0) {
                const estimatedActiveMonths = Math.max(Math.round(allTimeTotal / currentTotal), 1);
                const frequency = estimatedActiveMonths / totalMonths;

                if (frequency <= 0.4) {
                    isPeriodic = true;
                    avgTotal = allTimeTotal / estimatedActiveMonths;
                    comparisonLabel = `avg of ~${estimatedActiveMonths} active mo`;
                }
            }

            const deviation = avgTotal > 0 ? ((currentTotal - avgTotal) / avgTotal) * 100 : 0;

            const threshold = isPeriodic ? 50 : 25;
            const isAnomaly = Math.abs(deviation) > threshold;
            const isOver = deviation > threshold;
            const isUnder = deviation < -threshold;

            const displayDeviation = Math.max(Math.min(deviation, 999), -999);

            const prevCat = prevMonthCategories.find(c => c.category === catName);
            const prevTotal = prevCat ? prevCat.total : 0;

            return {
                category: catName,
                total: currentTotal,
                percent: cat.percent,
                avgTotal,
                deviation: displayDeviation,
                rawDeviation: deviation,
                isAnomaly,
                isOver,
                isUnder,
                isPeriodic,
                comparisonLabel,
                prevTotal,
                color: CATEGORY_COLORS[catName] || '#627d98',
                icon: CATEGORY_ICONS[catName] || 'label'
            };
        }).sort((a, b) => {
            if (a.isAnomaly && !b.isAnomaly) return -1;
            if (!a.isAnomaly && b.isAnomaly) return 1;
            return Math.abs(b.deviation) - Math.abs(a.deviation);
        });
    })();

    /* ═══════════════════════════════════════
       S2: CASH FLOW WATERFALL — SVG Data
       ═══════════════════════════════════════ */
    $: waterfallData = (() => {
        if (!currentMonthSummary || !monthCategories.length) return null;

        const income = currentMonthSummary.income;
        const expenses = currentMonthSummary.expenses;
        // Accrual-basis: external transfers are a real outflow (Zelle/Venmo to others)
        const externalTransfers = currentMonthSummary.external_transfers || 0;

        // The waterfall shows: Income → Expense categories → External Transfers → Net
        // Internal/household transfers (Savings Transfer, Personal Transfer, CC Payment)
        // are excluded — they're just money moving between your own accounts.

        const items = [];
        let running = 0;

        // START bar (anchor)
        items.push({
            label: 'Opening',
            value: 0,
            runningBefore: 0,
            runningAfter: 0,
            type: 'anchor',
            color: 'var(--accent)'
        });

        // Income (single bar for now — could be split if we had source data)
        running += income;
        items.push({
            label: 'Income',
            value: income,
            runningBefore: 0,
            runningAfter: running,
            type: 'income',
            color: 'var(--flow-income)',
            icon: 'trending_up'
        });

        // Expense categories sorted by total descending
        // Exclude transfer categories — they're not real spending in the accrual model
        const expenseCats = [...monthCategories]
            .filter(c => c.category !== 'Savings Transfer' && c.category !== 'Personal Transfer')
            .sort((a, b) => b.total - a.total);

        // Get transaction counts per category
        const txnCounts = {};
        monthTransactions.forEach(t => {
            if (parseFloat(t.amount) < 0) {
                txnCounts[t.category] = (txnCounts[t.category] || 0) + 1;
            }
        });

        for (const cat of expenseCats) {
            const before = running;
            running -= cat.total;
            items.push({
                label: cat.category,
                value: -cat.total,
                runningBefore: before,
                runningAfter: running,
                type: 'expense',
                color: CATEGORY_COLORS[cat.category] || '#627d98',
                icon: CATEGORY_ICONS[cat.category] || 'label',
                count: txnCounts[cat.category] || 0
            });
        }

        // External Transfers (Zelle/Venmo to people outside your accounts)
        // These are real outflows in the accrual model
        if (externalTransfers > 0) {
            const before = running;
            running -= externalTransfers;
            items.push({
                label: 'Ext. Transfers',
                value: -externalTransfers,
                runningBefore: before,
                runningAfter: running,
                type: 'external_transfer',
                color: 'var(--warning)',
                icon: 'send_money',
                count: monthTransactions.filter(t => t.expense_type === 'transfer_external' && parseFloat(t.amount) < 0).length
            });
        }

        // END bar (anchor) — Net = Income - Spending - External Transfers
        items.push({
            label: 'Net',
            value: running,
            runningBefore: 0,
            runningAfter: running,
            type: 'result',
            color: running >= 0 ? 'var(--positive)' : 'var(--negative)'
        });

        return { items, maxValue: income, minValue: Math.min(running, 0), netResult: running };
    })();

    /* Waterfall SVG geometry */
    $: waterfallGeometry = (() => {
        if (!waterfallData) return null;
        const { items, maxValue, minValue } = waterfallData;

        const W = 900;
        const H = 320;
        const padTop = 32;
        const padBottom = 60;
        const padLeft = 10;
        const padRight = 10;
        const chartW = W - padLeft - padRight;
        const chartH = H - padTop - padBottom;

        const barCount = items.length;
        const barGap = Math.min(12, chartW / barCount * 0.2);
        const barWidth = Math.max(20, (chartW - barGap * (barCount - 1)) / barCount);

        // Y scale: 0 to maxValue with some padding
        const yMax = maxValue * 1.12;
        const yMin = Math.min(minValue * 1.1, -maxValue * 0.05);
        const yRange = yMax - yMin;

        function yScale(val) {
            return padTop + chartH - ((val - yMin) / yRange) * chartH;
        }

        const zeroY = yScale(0);

        const bars = items.map((item, i) => {
            const x = padLeft + i * (barWidth + barGap);

            let y, h;
            if (item.type === 'anchor') {
                // Zero-height marker at baseline
                y = zeroY;
                h = 2;
            } else if (item.type === 'result') {
                // Anchored from zero
                const top = Math.max(item.runningAfter, 0);
                const bottom = Math.min(item.runningAfter, 0);
                y = yScale(top);
                h = Math.max(yScale(bottom) - y, 3);
            } else if (item.type === 'income') {
                // Rises from runningBefore to runningAfter
                y = yScale(item.runningAfter);
                h = Math.max(yScale(item.runningBefore) - y, 3);
            } else {
                // Expense: drops from runningBefore to runningAfter
                y = yScale(item.runningBefore);
                h = Math.max(yScale(item.runningAfter) - y, 3);
            }

            return { ...item, x, y, h, barWidth, index: i };
        });

        // Bridge connectors (dashed lines between bar tops)
        const bridges = [];
        for (let i = 0; i < bars.length - 1; i++) {
            const curr = bars[i];
            const next = bars[i + 1];

            let bridgeY;
            if (curr.type === 'anchor') {
                bridgeY = zeroY;
            } else if (curr.type === 'income' || curr.type === 'result') {
                bridgeY = yScale(curr.runningAfter);
            } else {
                bridgeY = yScale(curr.runningAfter);
            }

            bridges.push({
                x1: curr.x + barWidth,
                x2: next.x,
                y: bridgeY
            });
        }

        // Grid lines
        const gridLines = [];
        const gridCount = 4;
        for (let i = 0; i <= gridCount; i++) {
            const val = yMin + (yRange / gridCount) * i;
            gridLines.push({ y: yScale(val), label: formatCompact(val) });
        }

        return { bars, bridges, gridLines, zeroY, W, H, padTop, padBottom, barWidth };
    })();

    function handleWaterfallHover(bar, event) {
        if (bar.type === 'anchor') return;
        const svg = event.currentTarget.closest('svg');
        const rect = svg.getBoundingClientRect();
        waterfallTooltip = {
            show: true,
            x: event.clientX - rect.left,
            y: event.clientY - rect.top - 16,
            label: bar.label,
            amount: bar.value,
            runningFrom: bar.runningBefore,
            runningTo: bar.runningAfter,
            count: bar.count || 0,
            type: bar.type
        };
    }

    function handleWaterfallLeave() {
        waterfallTooltip = { ...waterfallTooltip, show: false };
    }

    function handleWaterfallClick(bar) {
        if (bar.type === 'anchor' || bar.type === 'result' || bar.type === 'income') return;
        if (bar.type === 'external_transfer') {
            // Drill into external transfer transactions
            selectedCategory = 'External Transfers';
            categoryTransactions = monthTransactions
                .filter(t => t.expense_type === 'transfer_external' && parseFloat(t.amount) < 0)
                .sort((a, b) => Math.abs(parseFloat(b.amount)) - Math.abs(parseFloat(a.amount)));
            return;
        }
        let catName = bar.label;
        drillIntoCategory(catName);
    }

    /* ═══════════════════════════════════════
       S4: SAVINGS RATE TREND
       ═══════════════════════════════════════ */
    $: savingsRateTrend = (() => {
        if (monthly.length < 2) return null;
        const sorted = analyticsContext.sortedMonthly;

        const points = sorted.map(m => {
            // Accrual-basis: Net Flow = Income - Spending - External Transfers
            const extTransfers = m.external_transfers || 0;
            const rate = m.income > 0 ? Math.max(((m.income - m.expenses - extTransfers) / m.income) * 100, 0) : 0;
            return { month: m.month, rate: Math.min(rate, 100) };
        });

        // 3-month rolling average
        const rolling = points.map((p, i) => {
            const start = Math.max(0, i - 2);
            const window = points.slice(start, i + 1);
            const avg = window.reduce((s, w) => s + w.rate, 0) / window.length;
            return { ...p, rolling: avg };
        });

        const currentRate = rolling[rolling.length - 1]?.rate || 0;
        const avgRate = points.reduce((s, p) => s + p.rate, 0) / points.length;

        return { points: rolling, currentRate, avgRate, target: 25 };
    })();

    /* Savings Rate SVG geometry */
    $: savingsRateGeometry = (() => {
        if (!savingsRateTrend || savingsRateTrend.points.length < 2) return null;
        const { points, target } = savingsRateTrend;

        const W = 500;
        const H = 180;
        const padTop = 16;
        const padBottom = 28;
        const padLeft = 36;
        const padRight = 12;
        const chartW = W - padLeft - padRight;
        const chartH = H - padTop - padBottom;

        const rates = points.map(p => p.rate);
        const rollingRates = points.map(p => p.rolling);
        const allVals = [...rates, ...rollingRates, target];
        const yMax = Math.max(...allVals, 40) * 1.1;
        const yMin = 0;
        const yRange = yMax - yMin;

        function yScale(val) {
            return padTop + chartH - ((val - yMin) / yRange) * chartH;
        }

        const stepX = chartW / (points.length - 1);

        // Actual rate dots
        const dots = points.map((p, i) => ({
            x: padLeft + i * stepX,
            y: yScale(p.rate),
            rate: p.rate,
            month: p.month
        }));

        // Rolling average line
        let rollingPath = '';
        points.forEach((p, i) => {
            const x = padLeft + i * stepX;
            const y = yScale(p.rolling);
            rollingPath += i === 0 ? `M${x},${y}` : ` L${x},${y}`;
        });

        // Target line
        const targetY = yScale(target);

        // Grid lines
        const gridLines = [];
        const gridSteps = [0, 10, 20, 30, 40];
        for (const val of gridSteps) {
            if (val <= yMax) {
                gridLines.push({ y: yScale(val), label: `${val}%` });
            }
        }

        // Month labels (every 3 months)
        const monthLabels = [];
        points.forEach((p, i) => {
            if (i === 0 || i === points.length - 1 || i % 3 === 0) {
                monthLabels.push({ x: padLeft + i * stepX, label: formatMonthShort(p.month) });
            }
        });

        return { dots, rollingPath, targetY, gridLines, monthLabels, W, H, padTop, padBottom, padLeft };
    })();

    /* ═══════════════════════════════════════
       S5: PROJECTED YEAR-END
       ═══════════════════════════════════════ */
    $: projectedYearEnd = (() => {
        if (monthly.length < 3) return null;
        const sorted = analyticsContext.sortedMonthly;

        // Last 3 months rolling average (accrual-basis)
        const last3 = sorted.slice(-3);
        const avgIncome = last3.reduce((s, m) => s + m.income, 0) / last3.length;
        const avgExpenses = last3.reduce((s, m) => s + m.expenses, 0) / last3.length;
        const avgExtTransfers = last3.reduce((s, m) => s + (m.external_transfers || 0), 0) / last3.length;
        const avgNet = avgIncome - avgExpenses - avgExtTransfers;

        // Current year
        const currentYear = new Date().getFullYear();
        const currentMonth = new Date().getMonth(); // 0-indexed
        const remainingMonths = 12 - currentMonth - 1;

        // YTD totals (accrual-basis net)
        const ytdMonths = sorted.filter(m => m.month.startsWith(currentYear.toString()));
        const ytdNet = ytdMonths.reduce((s, m) => s + (m.income || 0) - (m.expenses || 0) + (m.refunds || 0) - (m.external_transfers || 0), 0);

        const projectedAdditional = avgNet * remainingMonths;
        const projectedTotal = ytdNet + projectedAdditional;

        // Optimistic (+20%) and pessimistic (-20%)
        const optimistic = ytdNet + projectedAdditional * 1.20;
        const pessimistic = ytdNet + projectedAdditional * 0.80;

        const projectedSavingsRate = avgIncome > 0 ? Math.max((avgNet / avgIncome) * 100, 0) : 0;

        return {
            avgNet,
            remainingMonths,
            projectedTotal,
            optimistic,
            pessimistic,
            projectedSavingsRate,
            ytdNet,
            currentYear
        };
    })();

    /* ═══════════════════════════════════════
       S6: INCOME STABILITY
       ═══════════════════════════════════════ */
    $: incomeStability = (() => {
        if (monthly.length < 3) return null;
        const sorted = analyticsContext.sortedMonthly;
        const incomes = sorted.map(m => m.income);
        const avgIncome = incomes.reduce((s, v) => s + v, 0) / incomes.length;
        const variance = incomes.reduce((s, v) => s + Math.pow(v - avgIncome, 2), 0) / incomes.length;
        const stdDev = Math.sqrt(variance);
        const cv = avgIncome > 0 ? (stdDev / avgIncome) * 100 : 0; // Coefficient of variation

        let level = 'Stable';
        let dots = 5;
        if (cv > 30) { level = 'Volatile'; dots = 1; }
        else if (cv > 20) { level = 'Moderate'; dots = 3; }
        else if (cv > 10) { level = 'Stable'; dots = 4; }
        else { level = 'Very Stable'; dots = 5; }

        // Consecutive months with income
        let streak = 0;
        for (let i = incomes.length - 1; i >= 0; i--) {
            if (incomes[i] > 0) streak++;
            else break;
        }

        return { avgIncome, stdDev, cv, level, dots, streak, totalMonths: incomes.length };
    })();

    /* ═══════════════════════════════════════
       S7: MONTH-OVER-MONTH DIFF TABLE
       ═══════════════════════════════════════ */
    $: momDiff = (() => {
        if (!monthCategories.length) return [];
        return monthCategories.map(cat => {
            const prev = prevMonthCategories.find(c => c.category === cat.category);
            const prevTotal = prev ? prev.total : 0;
            const delta = cat.total - prevTotal;
            const deltaPct = prevTotal > 0 ? ((cat.total - prevTotal) / prevTotal) * 100 : (cat.total > 0 ? 100 : 0);
            return {
                category: cat.category,
                currentTotal: cat.total,
                prevTotal,
                delta,
                deltaPct,
                color: CATEGORY_COLORS[cat.category] || '#627d98',
                icon: CATEGORY_ICONS[cat.category] || 'label'
            };
        }).sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta));
    })();

    // Best/Worst months
    $: bestWorstMonth = (() => {
        if (monthly.length < 2) return null;
        const sorted = [...monthly].sort((a, b) => a.expenses - b.expenses);
        const best = sorted[0];
        const worst = sorted[sorted.length - 1];
        return { best, worst };
    })();

    /* ═══════════════════════════════════════
       S8: ACTIONABLE NUDGE
       ═══════════════════════════════════════ */
    $: actionableNudge = (() => {
        if (!spendingPulseCards.length || !currentMonthSummary) return null;

        const overSpend = spendingPulseCards.filter(c => c.isOver);
        if (overSpend.length === 0) return null;

        // Sum potential savings: reduce each over-budget category to its average
        let totalPotential = 0;
        const suggestions = [];
        for (const cat of overSpend.slice(0, 3)) {
            const savings = cat.total - cat.avgTotal;
            if (savings > 0) {
                totalPotential += savings;
                suggestions.push({ name: cat.category, savings, color: cat.color });
            }
        }

        if (totalPotential <= 0) return null;

        const annualized = totalPotential * 12;
        const extTransfers = currentMonthSummary.external_transfers || 0;
        const currentSR = currentMonthSummary.income > 0
            ? ((currentMonthSummary.income - currentMonthSummary.expenses - extTransfers) / currentMonthSummary.income) * 100
            : 0;
        const newSR = currentMonthSummary.income > 0
            ? ((currentMonthSummary.income - currentMonthSummary.expenses - extTransfers + totalPotential) / currentMonthSummary.income) * 100
            : 0;

        return { totalPotential, annualized, suggestions, currentSR, newSR };
    })();


    /* ---------------------------------------
       DRILL-DOWN
       --------------------------------------- */
    function drillIntoCategory(cat) {
        selectedCategory = cat;
        categoryTransactions = monthTransactions
            .filter(t => t.category === cat && parseFloat(t.amount) < 0)
            .sort((a, b) => Math.abs(parseFloat(b.amount)) - Math.abs(parseFloat(a.amount)));
    }

    function closeDrillDown() {
        selectedCategory = '';
        categoryTransactions = [];
    }

    function selectAnalyticsPeriod(period) {
        selectedAnalyticsPeriod = period;
        const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
        if (period === 'This Month') {
            const now = new Date();
            const thisMonth = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, '0')}`;
            const match = sorted.find(m => m.month === thisMonth);
            if (match) selectedMonth = thisMonth;
            else if (sorted.length > 0) selectedMonth = sorted[0].month;
        } else if (period === 'Last Month') {
            const now = new Date();
            const lm = new Date(now.getFullYear(), now.getMonth() - 1, 1);
            const lmStr = `${lm.getFullYear()}-${String(lm.getMonth() + 1).padStart(2, '0')}`;
            const match = sorted.find(m => m.month === lmStr);
            if (match) selectedMonth = lmStr;
            else if (sorted.length > 1) selectedMonth = sorted[1].month;
        }
        // 'Custom' does nothing — user picks from dropdown
    }

    // ── Subscription feedback handlers ──────────────────────────────
    let subscriptionFeedback = '';

    async function handleConfirmSubscription(item) {
        try {
            await api.confirmSubscription(item.merchant, null, item.frequency, item.category);
            subscriptionFeedback = `✓ ${item.merchant} confirmed`;
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        } catch (e) {
            console.error('Failed to confirm subscription:', e);
            subscriptionFeedback = 'Failed to confirm';
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        }
    }

    async function handleDismissSubscription(item) {
        try {
            await api.dismissSubscription(item.merchant, null);
            // Remove from local list immediately
            if (recurringData && recurringData.items) {
                recurringData = {
                    ...recurringData,
                    items: recurringData.items.filter(i => i.merchant !== item.merchant),
                    dismissed_count: (recurringData.dismissed_count || 0) + 1,
                    dismissed: [...(recurringData.dismissed || []), { merchant: item.merchant, dismissed_at: new Date().toISOString() }],
                };
                // Recalculate totals
                let newMonthly = 0, newAnnual = 0;
                for (const r of recurringData.items) {
                    if (r.status === 'active' && !r.cancelled) {
                        newAnnual += r.annual_cost;
                        newMonthly += r.annual_cost / 12;
                    }
                }
                recurringData.total_monthly = Math.round(newMonthly * 100) / 100;
                recurringData.total_annual = Math.round(newAnnual * 100) / 100;
                recurringData.active_count = recurringData.items.filter(i => i.status === 'active' && !i.cancelled).length;
            }
            subscriptionFeedback = `✕ ${item.merchant} dismissed`;
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        } catch (e) {
            console.error('Failed to dismiss subscription:', e);
            subscriptionFeedback = 'Failed to dismiss';
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        }
    }

    async function handleCancelSubscription(item) {
        try {
            const profile = $activeProfile && $activeProfile !== 'household' ? $activeProfile : null;
            await api.cancelSubscription(item.merchant, profile);
            // Mark as cancelled locally
            if (recurringData && recurringData.items) {
                recurringData = {
                    ...recurringData,
                    items: recurringData.items.map(i =>
                        i.merchant === item.merchant ? { ...i, cancelled: true } : i
                    ),
                    cancelled_count: (recurringData.cancelled_count || 0) + 1,
                    inactive_count: Math.max((recurringData.inactive_count || 0) - 1, 0),
                };
            }
            subscriptionFeedback = `✓ ${item.merchant} marked as cancelled`;
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        } catch (e) {
            console.error('Failed to cancel subscription:', e);
            subscriptionFeedback = 'Failed to cancel';
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        }
    }

    async function handleRestoreSubscription(item) {
        try {
            const profile = $activeProfile && $activeProfile !== 'household' ? $activeProfile : null;
            await api.restoreSubscription(item.merchant, profile);
            // Refetch recurring data to get fresh state
            recurringLoading = true;
            try {
                recurringData = await api.getRecurring();
            } catch (_) {}
            recurringLoading = false;
            subscriptionFeedback = `✓ ${item.merchant} restored`;
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        } catch (e) {
            console.error('Failed to restore subscription:', e);
            subscriptionFeedback = 'Failed to restore';
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        }
    }

    async function handleMarkEventRead(event) {
        try {
            await api.markEventsRead([event.id]);
            // Remove from local events list
            if (recurringData && recurringData.events) {
                recurringData = {
                    ...recurringData,
                    events: recurringData.events.map(e =>
                        e.id === event.id ? { ...e, is_read: true } : e
                    ),
                    unread_event_count: Math.max((recurringData.unread_event_count || 0) - 1, 0),
                };
            }
        } catch (e) {
            console.error('Failed to mark event read:', e);
        }
    }

    async function handleMarkAllEventsRead() {
        const ids = subscriptionEvents.map(e => e.id);
        if (ids.length === 0) return;
        try {
            await api.markEventsRead(ids);
            if (recurringData && recurringData.events) {
                recurringData = {
                    ...recurringData,
                    events: recurringData.events.map(e => ({ ...e, is_read: true })),
                    unread_event_count: 0,
                };
            }
        } catch (e) {
            console.error('Failed to mark all events read:', e);
        }
    }

    async function handleRedetectSubscriptions() {
        redetectLoading = true;
        try {
            const profile = $activeProfile && $activeProfile !== 'household' ? $activeProfile : null;
            await api.redetectSubscriptions(profile);
            // Refetch recurring data
            recurringData = await api.getRecurring();
            subscriptionFeedback = '✓ Subscriptions re-scanned';
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        } catch (e) {
            console.error('Failed to redetect subscriptions:', e);
            subscriptionFeedback = 'Re-detection failed';
            setTimeout(() => { subscriptionFeedback = ''; }, 3000);
        } finally {
            redetectLoading = false;
        }
    }

    function getEventIcon(eventType) {
        switch (eventType) {
            case 'new_detected': return 'add_circle';
            case 'price_increase': return 'trending_up';
            case 'price_decrease': return 'trending_down';
            case 'gone_inactive': return 'pause_circle';
            case 'zombie_charge': return 'warning';
            default: return 'info';
        }
    }

    function getEventColor(eventType) {
        switch (eventType) {
            case 'new_detected': return 'var(--accent)';
            case 'price_increase': return 'var(--negative)';
            case 'price_decrease': return 'var(--positive)';
            case 'gone_inactive': return 'var(--warning)';
            case 'zombie_charge': return 'var(--negative)';
            default: return 'var(--text-muted)';
        }
    }

    function getEventBgColor(eventType) {
        switch (eventType) {
            case 'new_detected': return 'color-mix(in srgb, var(--accent) 8%, transparent)';
            case 'price_increase': return 'color-mix(in srgb, var(--negative) 8%, transparent)';
            case 'price_decrease': return 'color-mix(in srgb, var(--positive) 8%, transparent)';
            case 'gone_inactive': return 'color-mix(in srgb, var(--warning) 8%, transparent)';
            case 'zombie_charge': return 'color-mix(in srgb, var(--negative) 10%, transparent)';
            default: return 'var(--surface-100)';
        }
    }

    function getEventMessage(event) {
        const d = event.detail || {};
        switch (event.event_type) {
            case 'new_detected': return `New subscription detected: ${event.merchant_name}`;
            case 'price_increase': return `${event.merchant_name}: ${formatCurrency(d.old_amount)} → ${formatCurrency(d.new_amount)} (+${formatCurrency(d.change)})`;
            case 'price_decrease': return `${event.merchant_name}: ${formatCurrency(d.old_amount)} → ${formatCurrency(d.new_amount)} (${formatCurrency(d.change)})`;
            case 'gone_inactive': return `${event.merchant_name} appears inactive — no recent charges`;
            case 'zombie_charge': return `⚠️ ${event.merchant_name} charged after being marked cancelled`;
            default: return `${event.merchant_name}: ${event.event_type}`;
        }
    }

    function handleWindowClick() {
        if (monthPickerOpen) monthPickerOpen = false;
        if (selectedCategory) closeDrillDown();
    }    
</script>
<svelte:window on:click={handleWindowClick} />

{#if loading}
    <div class="space-y-6">
        <div class="skeleton h-8 w-40 rounded-xl"></div>
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            {#each Array(6) as _}
                <div class="skeleton h-28 rounded-xl"></div>
            {/each}
        </div>
        <div class="skeleton h-80 rounded-2xl"></div>
        <div class="skeleton h-40 rounded-2xl"></div>
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <div class="skeleton h-52 rounded-2xl"></div>
            <div class="skeleton h-52 rounded-2xl"></div>
        </div>
    </div>
{:else}
<div class="profile-transition" class:profile-loading={profileSwitching}>

    <!-- --- HEADER --- -->
    <div class="flex items-start justify-between mb-8 fade-in" style="position: relative; z-index: 100;">
        <div>
            <p class="text-[10px] font-bold tracking-[0.2em] uppercase mb-1.5" style="color: var(--accent)">Insights</p>
            <h2 class="text-2xl md:text-[2rem] font-extrabold font-display tracking-tight" style="color: var(--text-primary)">
                Analytics
            </h2>
            <p class="text-[12px] mt-1" style="color: var(--text-muted)">What your data means and what to do about it</p>
        </div>
        <ProfileSwitcher />
    </div>


<!-- ═══════════════════════════════════════
         S1: CASH FLOW WATERFALL (Hero)
         ═══════════════════════════════════════ -->
    <!-----------------------------------------
         S0: HERO SUMMARY HEADLINE
         ----------------------------------------->
    {#if currentMonthSummary}
        {@const allTimeAvgExpenses = monthly.length > 0 ? monthly.reduce((s, m) => s + m.expenses, 0) / monthly.length : 0}
        {@const expVsAvgPct = allTimeAvgExpenses > 0 ? ((currentMonthSummary.expenses - allTimeAvgExpenses) / allTimeAvgExpenses) * 100 : 0}
        {@const extTransfers = currentMonthSummary.external_transfers || 0}
        {@const currentSavingsRate = currentMonthSummary.income > 0 ? Math.max(((currentMonthSummary.income - currentMonthSummary.expenses - extTransfers) / currentMonthSummary.income) * 100, 0) : 0}
        <section class="mb-8 fade-in-up" style="animation-delay: 30ms">
            <div class="analytics-hero-strip">
                <div class="analytics-hero-headline">
                    <p>In <strong>{formatMonth(selectedMonth)}</strong>, you spent <strong class="font-mono">{formatCurrency(currentMonthSummary.expenses)}</strong>
                        — <span style="color: {expVsAvgPct <= 0 ? 'var(--positive)' : 'var(--negative)'}; font-weight: 700;">{formatPercent(Math.abs(expVsAvgPct))}</span>
                        {expVsAvgPct <= 0 ? 'below' : 'above'} your average.
                    </p>
                </div>
                <div class="analytics-hero-metrics">
                    <div class="analytics-hero-metric">
                        <span class="analytics-hero-metric-label">Total Expenses</span>
                        <span class="analytics-hero-metric-value text-negative">{formatCurrency(currentMonthSummary.expenses)}</span>
                    </div>
                    <div class="analytics-hero-metric">
                        <span class="analytics-hero-metric-label">vs Average</span>
                        <span class="analytics-hero-metric-value" style="color: {expVsAvgPct <= 0 ? 'var(--positive)' : 'var(--negative)'}">
                            {expVsAvgPct <= 0 ? '▼' : '▲'} {formatPercent(Math.abs(expVsAvgPct))}
                        </span>
                    </div>
                    <div class="analytics-hero-metric">
                        <span class="analytics-hero-metric-label">Savings Rate</span>
                        <span class="analytics-hero-metric-value" style="color: var(--accent)">{formatPercent(currentSavingsRate)}</span>
                    </div>
                </div>
            </div>
        </section>
    {/if}

    {#if waterfallData && waterfallGeometry}
        <section class="mb-10 fade-in-up" style="animation-delay: 60ms">
            <div class="flex items-center justify-between mb-1" style="position: relative; z-index: 90;">
                <div class="flex items-center gap-2">
                    <div class="section-accent-bar"></div>
                    <p class="section-header">Cash Flow Waterfall</p>
                </div>
                <div class="flex items-center gap-3">
                    <!-- iOS-style period toggle (relocated from header) -->
                    <div class="period-toggle-track" style="--seg-count: {analyticsPeriods.length}; --active-idx: {activeAnalyticsPeriodIdx};">
                        <div class="period-toggle-thumb"></div>
                        {#each analyticsPeriods as period}
                            <button class="period-toggle-label" class:active={selectedAnalyticsPeriod === period}
                                on:click={() => selectAnalyticsPeriod(period)}>
                                {period}
                            </button>
                        {/each}
                    </div>

                    <!-- Month dropdown -->
                    <div class="analytics-month-picker">
                        <button class="analytics-month-picker-btn"
                            class:ring-2={selectedAnalyticsPeriod === 'Custom'}
                            class:ring-accent={selectedAnalyticsPeriod === 'Custom'}
                            on:click|stopPropagation={() => { monthPickerOpen = !monthPickerOpen; selectedAnalyticsPeriod = 'Custom'; }}>
                            <span class="text-[12px] font-medium" style="color: var(--text-primary)">{formatMonth(selectedMonth)}</span>
                            <span class="material-symbols-outlined text-[16px]" style="color: var(--text-muted); transition: transform 0.2s;"
                                class:rotate-180={monthPickerOpen}>
                                expand_more
                            </span>
                        </button>
                        {#if monthPickerOpen}
                            <div class="analytics-month-picker-dropdown" on:click|stopPropagation>
                                {#each [...monthly].sort((a,b) => b.month.localeCompare(a.month)) as m}
                                    <button
                                        class="analytics-month-picker-option"
                                        class:active={m.month === selectedMonth}
                                        on:click={() => { selectedMonth = m.month; monthPickerOpen = false; selectedAnalyticsPeriod = 'Custom'; }}>
                                        {formatMonth(m.month)}
                                        {#if m.month === selectedMonth}
                                            <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">check</span>
                                        {/if}
                                    </button>
                                {/each}
                            </div>
                        {/if}
                    </div>
                </div>
            </div>
            <p class="text-[11px] mb-4 ml-6" style="color: var(--text-muted)">
                Your money, step by step - {formatMonth(selectedMonth)}
            </p>

            <div class="card analytics-waterfall-theater" style="padding: 1rem 0.5rem 0.5rem">
                <div bind:this={waterfallEl} class="analytics-waterfall-container" style="position: relative;">
                    <svg width="100%" viewBox="0 0 {waterfallGeometry.W} {waterfallGeometry.H}" preserveAspectRatio="xMidYMid meet">
                        <defs>
                            <filter id="wfBarGlow" x="-20%" y="-20%" width="140%" height="140%">
                                <feGaussianBlur in="SourceGraphic" stdDeviation="6" result="blur"/>
                                <feMerge>
                                    <feMergeNode in="blur"/>
                                    <feMergeNode in="SourceGraphic"/>
                                </feMerge>
                            </filter>
                            <filter id="wfBarSoft" x="-10%" y="-10%" width="120%" height="120%">
                                <feGaussianBlur in="SourceGraphic" stdDeviation={$darkMode ? 3 : 1.5}/>
                            </filter>
                        </defs>

                        <!-- Grid lines -->
                        {#each waterfallGeometry.gridLines as gl}
                            <line x1="36" y1={gl.y} x2={waterfallGeometry.W - 10} y2={gl.y}
                                stroke="var(--text-muted)" stroke-width="0.5" opacity="0.08" />
                            <text x="32" y={gl.y + 3} text-anchor="end"
                                fill="var(--text-muted)" font-size="8" font-family="DM Mono, monospace" opacity="0.4">
                                {gl.label}
                            </text>
                        {/each}

                        <!-- Zero line -->
                        <line x1="36" y1={waterfallGeometry.zeroY} x2={waterfallGeometry.W - 10} y2={waterfallGeometry.zeroY}
                            stroke="var(--text-muted)" stroke-width="1" opacity="0.15" stroke-dasharray="4,4" />

                        <!-- Bridge connectors -->
                        {#each waterfallGeometry.bridges as bridge}
                            <line x1={bridge.x1} y1={bridge.y} x2={bridge.x2} y2={bridge.y}
                                stroke="var(--text-muted)" stroke-width="1" opacity="0.12" stroke-dasharray="3,3" />
                        {/each}

                        <!-- Bar glow underlayer -->
                        {#each waterfallGeometry.bars as bar}
                            {#if bar.type !== 'anchor'}
                                <rect
                                    x={bar.x - 2} y={bar.y - 2}
                                    width={bar.barWidth + 4} height={bar.h + 4}
                                    rx="6" fill={bar.color}
                                    opacity={$darkMode ? 0.08 : 0.04}
                                    filter="url(#wfBarSoft)"
                                />
                            {/if}
                        {/each}

                        <!-- Bars -->
                        {#each waterfallGeometry.bars as bar, i}
                            {#if bar.type !== 'anchor'}
                                <rect
                                    x={bar.x} y={bar.y}
                                    width={bar.barWidth} height={bar.h}
                                    rx="4" fill={bar.color}
                                    opacity={bar.type === 'result' ? 0.90 : 0.75}
                                    class="analytics-wf-bar"
                                    style="cursor: {bar.type === 'expense' || bar.type === 'savings' || bar.type === 'transfer' ? 'pointer' : 'default'}"
                                    on:mouseenter={(e) => handleWaterfallHover(bar, e)}
                                    on:mouseleave={handleWaterfallLeave}
                                    on:click|stopPropagation={() => handleWaterfallClick(bar)}
                                />
                                <!-- Top edge highlight -->
                                <line
                                    x1={bar.x + 3} y1={bar.y + 0.5}
                                    x2={bar.x + bar.barWidth - 3} y2={bar.y + 0.5}
                                    stroke="white" stroke-width="0.5" opacity="0.2" stroke-linecap="round"
                                />
                            {/if}

                            <!-- Label below -->
                            <text x={bar.x + bar.barWidth / 2} y={waterfallGeometry.H - waterfallGeometry.padBottom + 16}
                                text-anchor="middle" fill="var(--text-muted)"
                                font-size="8.5" font-family="Inter, system-ui, sans-serif" font-weight="500">
                                {bar.label.length > 9 ? bar.label.slice(0, 8) + '…' : bar.label}
                            </text>

                            <!-- Value above/below bar -->
                            {#if bar.type !== 'anchor'}
                                <text x={bar.x + bar.barWidth / 2}
                                    y={bar.type === 'income' || bar.type === 'result' && bar.value >= 0 ? bar.y - 6 : bar.y + bar.h + 12}
                                    text-anchor="middle" fill={bar.color}
                                    font-size="8" font-family="DM Mono, monospace" font-weight="500" opacity="0.8">
                                    {bar.type === 'income' ? '+' : ''}{formatCompact(bar.value)}
                                </text>
                            {/if}
                        {/each}
                    </svg>

                    <!-- Tooltip -->
                    <!-- Tooltip (always mounted, visibility via class) -->
                    <div class="analytics-wf-tooltip"
                        class:wf-tooltip-visible={waterfallTooltip.show}
                        style="left: {waterfallTooltip.x}px; top: {waterfallTooltip.y}px;">
                        <p class="text-[11px] font-semibold" style="color: var(--text-primary)">{waterfallTooltip.label}</p>
                        <p class="text-[12px] font-mono font-bold" style="color: {waterfallTooltip.amount >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                            {waterfallTooltip.amount >= 0 ? '+' : ''}{formatCurrency(waterfallTooltip.amount)}
                        </p>
                        {#if waterfallTooltip.type !== 'income' && waterfallTooltip.type !== 'result'}
                            <p class="text-[9px]" style="color: var(--text-muted)">
                                {formatCompact(waterfallTooltip.runningFrom)} â {formatCompact(waterfallTooltip.runningTo)}
                            </p>
                        {/if}
                        {#if waterfallTooltip.count > 0}
                            <p class="text-[9px]" style="color: var(--text-muted)">{waterfallTooltip.count} transactions</p>
                        {/if}
                    </div>
                </div>

                <!-- Summary ribbon -->
                <div class="analytics-wf-summary">
                    <div class="analytics-wf-summary-item">
                        <span class="analytics-wf-summary-label">Income</span>
                        <span class="analytics-wf-summary-value text-positive">+{formatCurrency(currentMonthSummary.income)}</span>
                    </div>
                    <div class="analytics-wf-summary-item">
                        <span class="analytics-wf-summary-label">Spending</span>
                        <span class="analytics-wf-summary-value text-negative">-{formatCurrency(currentMonthSummary.expenses)}</span>
                    </div>
                    {#if (currentMonthSummary.external_transfers || 0) > 0}
                        <div class="analytics-wf-summary-item">
                            <span class="analytics-wf-summary-label">Ext. Transfers</span>
                            <span class="analytics-wf-summary-value" style="color: var(--warning)">-{formatCurrency(currentMonthSummary.external_transfers)}</span>
                        </div>
                    {/if}
                    <div class="analytics-wf-summary-item">
                        <span class="analytics-wf-summary-label">Net Flow</span>
                        <span class="analytics-wf-summary-value" style="color: {waterfallData.netResult >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                            {waterfallData.netResult >= 0 ? '+' : ''}{formatCurrency(waterfallData.netResult)}
                        </span>
                    </div>
                </div>
            </div>

            <!-- ── Contextual Drill-Down (inside waterfall section) ── -->
            {#if selectedCategory && categoryTransactions.length > 0}
                <div class="analytics-waterfall-drilldown" on:click|stopPropagation>
                    <div class="analytics-waterfall-drilldown-header">
                        <div class="flex items-center gap-2.5">
                            <div class="w-6 h-6 rounded-md flex items-center justify-center"
                                style="background: color-mix(in srgb, {CATEGORY_COLORS[selectedCategory] || '#627d98'} 12%, transparent)">
                                <span class="material-symbols-outlined text-[13px]"
                                    style="color: {CATEGORY_COLORS[selectedCategory] || '#627d98'}">
                                    {CATEGORY_ICONS[selectedCategory] || 'label'}
                                </span>
                            </div>
                            <div>
                                <h4 style="margin: 0; text-transform: none; letter-spacing: 0; font-size: 0.8125rem; color: var(--text-primary)">
                                    {selectedCategory}
                                </h4>
                                <p class="text-[10px]" style="color: var(--text-muted); margin: 0">
                                    {categoryTransactions.length} transactions · {formatMonth(selectedMonth)}
                                </p>
                            </div>
                        </div>
                        <button class="analytics-waterfall-drilldown-close" on:click={closeDrillDown}>
                            <span class="flex items-center gap-1">
                                <span class="material-symbols-outlined text-[12px]">close</span>
                                Close
                            </span>
                        </button>
                    </div>
                    <div class="analytics-waterfall-drilldown-body">
                        {#each categoryTransactions.slice(0, 12) as tx}
                            {@const amount = Math.abs(parseFloat(tx.amount))}
                            <div class="analytics-waterfall-drilldown-row">
                                <div class="flex-1 min-w-0">
                                    <p class="text-[12px] font-medium truncate" style="color: var(--text-primary)">{tx.description}</p>
                                    <p class="text-[9px]" style="color: var(--text-muted)">{formatDate(tx.date)} · {tx.account_name}</p>
                                </div>
                                <p class="text-[12px] font-bold font-mono text-negative flex-shrink-0">{formatCurrency(amount, 2)}</p>
                            </div>
                        {/each}
                    </div>
                    {#if categoryTransactions.length > 12}
                        <div class="px-4 py-2.5 text-center" style="border-top: 1px solid var(--border-subtle)">
                            <a href="/transactions" class="text-[11px] font-medium" style="color: var(--accent)">
                                View all {categoryTransactions.length} transactions →
                            </a>
                        </div>
                    {/if}
                </div>
            {/if}
        </section>
    {/if}

    <!-- ═══════════════════════════════════════
         TOP MERCHANTS
         ═══════════════════════════════════════ -->
    {#if topMerchants.length > 0}
        <section class="mb-10 fade-in-up" style="animation-delay: 120ms">
            <div class="flex items-center gap-2 mb-4">
                <div class="section-accent-bar"></div>
                <p class="section-header">Top Merchants</p>
            </div>

            <div class="card" style="padding: 0; overflow: hidden">
                {#each topMerchants.slice(0, 8) as merchant, i}
                    {@const maxSpend = topMerchants[0]?.total_spent || 1}
                    {@const barPct = (merchant.total_spent / maxSpend) * 100}
                    <div class="flex items-center gap-4 px-5 py-3" style="border-bottom: {i < Math.min(topMerchants.length, 8) - 1 ? '1px solid var(--card-border)' : 'none'}">
                        <div class="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0"
                            style="background: color-mix(in srgb, var(--accent) 8%, transparent)">
                            <span class="text-[11px] font-bold" style="color: var(--accent)">{i + 1}</span>
                        </div>
                        <div class="flex-1 min-w-0">
                            <div class="flex items-center gap-2">
                                <p class="text-[12px] font-semibold truncate" style="color: var(--text-primary)">{merchant.name}</p>
                                {#if merchant.industry}
                                    <span class="text-[9px] px-1.5 py-0.5 rounded-full flex-shrink-0" style="background: var(--surface-200); color: var(--text-muted)">{merchant.industry}</span>
                                {/if}
                            </div>
                            <div class="flex items-center gap-2 mt-1">
                                <div class="flex-1 h-1 rounded-full" style="background: var(--surface-200)">
                                    <div class="h-1 rounded-full transition-all duration-500" style="width: {barPct}%; background: var(--accent); opacity: 0.6"></div>
                                </div>
                                <span class="text-[9px] font-mono" style="color: var(--text-muted)">{merchant.transaction_count} txn{merchant.transaction_count !== 1 ? 's' : ''}</span>
                            </div>
                        </div>
                        <p class="text-[12px] font-bold font-mono flex-shrink-0" style="color: var(--text-primary)">{formatCurrency(merchant.total_spent)}</p>
                    </div>
                {/each}
            </div>
        </section>
    {/if}

    <!-- ═══════════════════════════════════════
         RECURRING SUBSCRIPTIONS
         ═══════════════════════════════════════ -->
    {#if recurringData && recurringData.items && recurringData.items.length > 0}
        <section class="mb-10 fade-in-up" style="animation-delay: 130ms">
            <div class="flex items-center gap-2 mb-1">
                <div class="section-accent-bar"></div>
                <p class="section-header">Recurring Subscriptions</p>
                {#if unreadEventCount > 0}
                    <span class="analytics-event-count-badge">{unreadEventCount}</span>
                {/if}
                <div class="flex items-center gap-2 ml-auto">
                    <button class="analytics-redetect-btn" on:click|stopPropagation={handleRedetectSubscriptions}
                        disabled={redetectLoading} title="Re-scan for subscriptions">
                        <span class="material-symbols-outlined text-[14px]"
                            class:animate-spin={redetectLoading}
                            style="color: var(--text-muted)">refresh</span>
                    </button>
                    {#if subscriptionFeedback}
                        <span class="text-[10px] font-medium px-2 py-1 rounded-lg fade-in"
                            style="background: var(--surface-100); color: var(--text-secondary)">
                            {subscriptionFeedback}
                        </span>
                    {/if}
                </div>
            </div>
            <div class="flex items-center gap-2 mb-2 ml-6 flex-wrap">
                <span class="analytics-sub-summary-badge" style="--badge-color: var(--positive)">
                    {recurringData.active_count || activeRecurring.length} active
                </span>
                {#if (recurringData.inactive_count || inactiveRecurring.length) > 0}
                    <span class="analytics-sub-summary-badge" style="--badge-color: var(--warning)">
                        {recurringData.inactive_count || inactiveRecurring.length} inactive
                    </span>
                {/if}
                {#if (recurringData.cancelled_count || cancelledRecurring.length) > 0}
                    <span class="analytics-sub-summary-badge" style="--badge-color: var(--negative)">
                        {recurringData.cancelled_count || cancelledRecurring.length} cancelled
                    </span>
                {/if}
                {#if (recurringData.dismissed_count || dismissedRecurring.length) > 0}
                    <span class="analytics-sub-summary-badge" style="--badge-color: var(--text-muted)">
                        {recurringData.dismissed_count || dismissedRecurring.length} dismissed
                    </span>
                {/if}
            </div>
            <p class="text-[11px] mb-4 ml-6" style="color: var(--text-muted)">
                <span class="font-bold font-mono" style="color: var(--text-primary)">{formatCurrency(recurringData.total_monthly)}/mo</span> ·
                <span class="font-mono" style="color: var(--text-muted)">{formatCurrency(recurringData.total_annual)}/yr</span>
                {#if priceChangeCount > 0}
                    · <span class="text-[10px]" style="color: var(--negative)">{priceChangeCount} price increase{priceChangeCount !== 1 ? 's' : ''}</span>
                {/if}
            </p>

            <div class="card" style="padding: 0; overflow: hidden">
                <!-- Subscription Events / Alerts Strip -->
                {#if subscriptionEvents.length > 0}
                    <div class="analytics-events-strip">
                        <div class="analytics-events-strip-header">
                            <span class="text-[9px] font-bold tracking-[0.1em] uppercase" style="color: var(--text-muted)">
                                Recent Alerts
                            </span>
                            {#if subscriptionEvents.length > 1}
                                <button class="analytics-events-dismiss-all" on:click|stopPropagation={handleMarkAllEventsRead}>
                                    Dismiss all
                                </button>
                            {/if}
                        </div>
                        {#each subscriptionEvents.slice(0, 5) as event}
                            <div class="analytics-event-card"
                                style="--event-color: {getEventColor(event.event_type)}; background: {getEventBgColor(event.event_type)}">
                                <div class="flex items-center gap-2.5 flex-1 min-w-0">
                                    <span class="material-symbols-outlined text-[16px]" style="color: var(--event-color)">
                                        {getEventIcon(event.event_type)}
                                    </span>
                                    <div class="min-w-0 flex-1">
                                        <p class="text-[11px] font-medium" style="color: var(--text-primary)">
                                            {getEventMessage(event)}
                                        </p>
                                        <p class="text-[9px]" style="color: var(--text-muted)">
                                            {event.created_at ? formatDateWithYear(event.created_at) : ''}
                                        </p>
                                    </div>
                                </div>
                                <button class="analytics-event-dismiss" on:click|stopPropagation={() => handleMarkEventRead(event)}
                                    title="Dismiss">
                                    <span class="material-symbols-outlined text-[12px]">close</span>
                                </button>
                            </div>
                        {/each}
                        {#if subscriptionEvents.length > 5}
                            <p class="text-[9px] px-4 py-1.5" style="color: var(--text-muted)">
                                + {subscriptionEvents.length - 5} more alerts
                            </p>
                        {/if}
                    </div>
                {/if}

                <!-- Header -->
                <div class="analytics-recurring-header">
                    <span class="analytics-recurring-hcell flex-1">Merchant</span>
                    <span class="analytics-recurring-hcell w-16 text-center">Freq</span>
                    <span class="analytics-recurring-hcell w-20 text-right">Amount</span>
                    <span class="analytics-recurring-hcell w-20 text-right">Annual</span>
                    <span class="analytics-recurring-hcell w-16 text-center">Status</span>
                    <span class="analytics-recurring-hcell w-16 text-center">Conf.</span>
                    <span class="analytics-recurring-hcell w-8"></span>
                </div>

                <!-- Active subscriptions -->
                {#each activeRecurring.slice(0, 10) as item, i}
                    <div class="analytics-recurring-row" style="border-bottom: {i < Math.min(activeRecurring.length, 10) - 1 ? '1px solid color-mix(in srgb, var(--card-border) 50%, transparent)' : 'none'}">
                        <div class="flex items-center gap-3 flex-1 min-w-0">
                            <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
                                style="background: color-mix(in srgb, {CATEGORY_COLORS[item.category] || '#627d98'} 10%, transparent)">
                                {#if item.logo_url}
                                    <img src={item.logo_url} alt="" class="w-5 h-5 rounded" style="object-fit: contain" />
                                {:else}
                                    <span class="material-symbols-outlined text-[13px]"
                                        style="color: {CATEGORY_COLORS[item.category] || '#627d98'}">
                                        {item.is_subscription ? 'subscriptions' : 'event_repeat'}
                                    </span>
                                {/if}
                            </div>
                            <div class="min-w-0">
                                <div class="flex items-center gap-1.5">
                                    <p class="text-[12px] font-medium truncate" style="color: var(--text-primary)">{item.clean_name || item.merchant}</p>
                                    {#if item.matched_by === 'user' || item.confidence === 'user'}
                                        <span class="analytics-recurring-badge-user">User</span>
                                    {/if}
                                </div>
                                <div class="flex items-center gap-1.5 flex-wrap">
                                    <span class="text-[9px]" style="color: {CATEGORY_COLORS[item.category] || 'var(--text-muted)'}">{item.category}</span>
                                    {#if item.charge_count}
                                        <span class="text-[9px]" style="color: var(--text-muted)">· {item.charge_count} charges</span>
                                    {/if}
                                    {#if item.total_spent}
                                        <span class="text-[9px]" style="color: var(--text-muted)">· {formatCurrency(item.total_spent)} total</span>
                                    {/if}
                                    {#if item.last_charge}
                                        <span class="text-[9px]" style="color: var(--text-muted)">· last {formatDateWithYear(item.last_charge)}</span>
                                    {/if}
                                    {#if item.next_expected}
                                        <span class="text-[9px] font-medium" style="color: var(--accent)">· next ~{formatDateWithYear(item.next_expected)}</span>
                                    {/if}
                                    {#if item.price_change}
                                        <span class="analytics-recurring-price-change" class:price-up={item.price_change.change > 0} class:price-down={item.price_change.change < 0}
                                            title="{item.price_change.change > 0 ? 'Price increased' : 'Price decreased'}: {formatCurrency(item.price_change.previous)} → {formatCurrency(item.price_change.current)}">
                                            <span class="material-symbols-outlined text-[9px]">{item.price_change.change > 0 ? 'trending_up' : 'trending_down'}</span>
                                            {item.price_change.change > 0 ? '+' : ''}{formatCurrency(item.price_change.change)}
                                        </span>
                                    {/if}
                                </div>
                            </div>
                        </div>
                        <div class="w-16 flex justify-center">
                            <span class="text-[9px] font-mono font-medium" style="color: var(--text-muted)">{item.frequency}</span>
                        </div>
                        <span class="text-[12px] font-bold font-mono w-20 text-right" style="color: var(--text-primary)">{formatCurrency(item.amount || item.avg_amount)}</span>
                        <span class="text-[11px] font-mono w-20 text-right" style="color: var(--text-muted)">{formatCurrency(item.annual_cost)}</span>
                        <div class="w-16 flex justify-center">
                            <span class="analytics-recurring-badge-active">Active</span>
                        </div>
                        <div class="w-16 flex justify-center">
                            {#if item.confidence === 'user'}
                                <span class="text-[8px] font-bold px-1.5 py-0.5 rounded-full"
                                    style="background: color-mix(in srgb, #8b5cf6 12%, transparent); color: #8b5cf6">
                                    User
                                </span>
                            {:else if item.confidence === 'high'}
                                <span class="text-[8px] font-bold px-1.5 py-0.5 rounded-full"
                                    style="background: color-mix(in srgb, var(--positive) 12%, transparent); color: var(--positive)">
                                    High
                                </span>
                            {:else}
                                <span class="text-[8px] font-bold px-1.5 py-0.5 rounded-full"
                                    style="background: color-mix(in srgb, var(--warning) 12%, transparent); color: var(--warning)">
                                    Low
                                </span>
                            {/if}
                        </div>
                        <div class="w-8 flex justify-center">
                            <button class="analytics-recurring-action-btn analytics-recurring-dismiss"
                                title="Not a subscription — dismiss"
                                on:click|stopPropagation={() => handleDismissSubscription(item)}>
                                <span class="material-symbols-outlined text-[13px]">close</span>
                            </button>
                        </div>
                    </div>
                {/each}

                {#if activeRecurring.length > 10}
                    <div class="px-5 py-2 text-center" style="border-top: 1px solid color-mix(in srgb, var(--card-border) 50%, transparent)">
                        <span class="text-[10px]" style="color: var(--text-muted)">+ {activeRecurring.length - 10} more active</span>
                    </div>
                {/if}

                <!-- Inactive subscriptions (collapsible dropdown, closed by default) -->
                {#if inactiveRecurring.length > 0}
                    <div class="analytics-recurring-inactive-toggle"
                         style="border-top: 1px solid var(--card-border); background: var(--surface-100)">
                        <button class="analytics-recurring-inactive-trigger"
                            on:click|stopPropagation={() => { inactiveOpen = !inactiveOpen; }}>
                            <div class="flex items-center gap-2">
                                <span class="material-symbols-outlined text-[14px]" style="color: var(--warning)">
                                    {inactiveOpen ? 'expand_less' : 'expand_more'}
                                </span>
                                <span class="text-[9px] font-bold tracking-[0.1em] uppercase" style="color: var(--text-muted)">
                                    Inactive / Possibly Cancelled
                                </span>
                                <span class="text-[9px] font-mono font-bold px-1.5 py-0.5 rounded-full"
                                    style="background: var(--surface-200); color: var(--warning)">
                                    {inactiveRecurring.length}
                                </span>
                            </div>
                            <span class="text-[9px] font-mono" style="color: var(--text-muted)">
                                {formatCurrency(inactiveTotalSpent)} total spent
                            </span>
                        </button>
                    </div>
                    {#if inactiveOpen}
                        <div class="analytics-recurring-inactive-body">
                            {#each inactiveRecurring as item, i}
                                <div class="analytics-recurring-row analytics-recurring-row-inactive" style="border-bottom: {i < inactiveRecurring.length - 1 ? '1px solid color-mix(in srgb, var(--card-border) 30%, transparent)' : 'none'}">
                                    <div class="flex items-center gap-3 flex-1 min-w-0">
                                        <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
                                            style="background: color-mix(in srgb, {CATEGORY_COLORS[item.category] || '#627d98'} 6%, transparent)">
                                            {#if item.logo_url}
                                                <img src={item.logo_url} alt="" class="w-5 h-5 rounded" style="object-fit: contain; opacity: 0.5" />
                                            {:else}
                                                <span class="material-symbols-outlined text-[13px]"
                                                    style="color: {CATEGORY_COLORS[item.category] || '#627d98'}; opacity: 0.5">
                                                    {item.is_subscription ? 'subscriptions' : 'event_repeat'}
                                                </span>
                                            {/if}
                                        </div>
                                        <div class="min-w-0">
                                            <p class="text-[12px] font-medium truncate" style="color: var(--text-muted)">{item.clean_name || item.merchant}</p>
                                            <div class="flex items-center gap-1.5 flex-wrap">
                                                <span class="text-[9px]" style="color: var(--text-muted)">{item.category}</span>
                                                {#if item.charge_count}
                                                    <span class="text-[9px]" style="color: var(--text-muted)">· {item.charge_count} charges</span>
                                                {/if}
                                                {#if item.last_charge}
                                                    <span class="text-[9px]" style="color: var(--text-muted)">· last {formatDateWithYear(item.last_charge)}</span>
                                                {/if}
                                                {#if item.price_change}
                                                    <span class="analytics-recurring-price-change" class:price-up={item.price_change.change > 0} class:price-down={item.price_change.change < 0}
                                                        title="{item.price_change.change > 0 ? 'Price increased' : 'Price decreased'}: {formatCurrency(item.price_change.previous)} → {formatCurrency(item.price_change.current)}">
                                                        <span class="material-symbols-outlined text-[9px]">{item.price_change.change > 0 ? 'trending_up' : 'trending_down'}</span>
                                                        {item.price_change.change > 0 ? '+' : ''}{formatCurrency(item.price_change.change)}
                                                    </span>
                                                {/if}
                                            </div>
                                        </div>
                                    </div>
                                    <div class="w-16 flex justify-center">
                                        <span class="text-[9px] font-mono" style="color: var(--text-muted)">{item.frequency}</span>
                                    </div>
                                    <span class="text-[12px] font-mono w-20 text-right" style="color: var(--text-muted)">{formatCurrency(item.amount || item.avg_amount)}</span>
                                    <span class="text-[11px] font-mono w-20 text-right" style="color: var(--text-muted)">{formatCurrency(item.annual_cost)}</span>
                                    <div class="w-16 flex justify-center">
                                        <span class="analytics-recurring-badge-inactive">Inactive</span>
                                    </div>
                                    <div class="w-16 flex justify-center">
                                        <button class="analytics-recurring-action-btn analytics-recurring-cancel-btn"
                                            title="Confirm cancelled"
                                            on:click|stopPropagation={() => handleCancelSubscription(item)}>
                                            <span class="text-[8px] font-bold whitespace-nowrap">Cancel</span>
                                        </button>
                                    </div>
                                    <div class="w-8 flex justify-center">
                                        <button class="analytics-recurring-action-btn analytics-recurring-dismiss"
                                            title="Not a subscription — dismiss"
                                            on:click|stopPropagation={() => handleDismissSubscription(item)}>
                                            <span class="material-symbols-outlined text-[13px]">close</span>
                                        </button>
                                    </div>
                                </div>
                            {/each}
                        </div>
                    {/if}
                {/if}

                <!-- Cancelled subscriptions (collapsible) -->
                {#if cancelledRecurring.length > 0}
                    <div class="analytics-recurring-inactive-toggle"
                         style="border-top: 1px solid var(--card-border); background: var(--surface-100)">
                        <button class="analytics-recurring-inactive-trigger"
                            on:click|stopPropagation={() => { cancelledOpen = !cancelledOpen; }}>
                            <div class="flex items-center gap-2">
                                <span class="material-symbols-outlined text-[14px]" style="color: var(--negative)">
                                    {cancelledOpen ? 'expand_less' : 'expand_more'}
                                </span>
                                <span class="text-[9px] font-bold tracking-[0.1em] uppercase" style="color: var(--text-muted)">
                                    Cancelled
                                </span>
                                <span class="text-[9px] font-mono font-bold px-1.5 py-0.5 rounded-full"
                                    style="background: var(--surface-200); color: var(--negative)">
                                    {cancelledRecurring.length}
                                </span>
                            </div>
                        </button>
                    </div>
                    {#if cancelledOpen}
                        <div class="analytics-recurring-inactive-body">
                            {#each cancelledRecurring as item, i}
                                <div class="analytics-recurring-row analytics-recurring-row-inactive" style="border-bottom: {i < cancelledRecurring.length - 1 ? '1px solid color-mix(in srgb, var(--card-border) 30%, transparent)' : 'none'}">
                                    <div class="flex items-center gap-3 flex-1 min-w-0">
                                        <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
                                            style="background: color-mix(in srgb, var(--negative) 6%, transparent)">
                                            <span class="material-symbols-outlined text-[13px]"
                                                style="color: var(--negative); opacity: 0.5">
                                                cancel
                                            </span>
                                        </div>
                                        <div class="min-w-0">
                                            <div class="flex items-center gap-1.5">
                                                <p class="text-[12px] font-medium truncate" style="color: var(--text-muted); text-decoration: line-through;">{item.clean_name || item.merchant}</p>
                                                <span class="analytics-recurring-badge-cancelled">Cancelled</span>
                                            </div>
                                            <div class="flex items-center gap-1.5 flex-wrap">
                                                <span class="text-[9px]" style="color: var(--text-muted)">{item.category}</span>
                                                {#if item.last_charge}
                                                    <span class="text-[9px]" style="color: var(--text-muted)">· last {formatDateWithYear(item.last_charge)}</span>
                                                {/if}
                                                {#if item.total_spent}
                                                    <span class="text-[9px]" style="color: var(--text-muted)">· {formatCurrency(item.total_spent)} total spent</span>
                                                {/if}
                                            </div>
                                        </div>
                                    </div>
                                    <div class="w-16 flex justify-center">
                                        <span class="text-[9px] font-mono" style="color: var(--text-muted)">{item.frequency}</span>
                                    </div>
                                    <span class="text-[12px] font-mono w-20 text-right" style="color: var(--text-muted)">{formatCurrency(item.amount || item.avg_amount)}</span>
                                    <span class="text-[11px] font-mono w-20 text-right" style="color: var(--text-muted)">{formatCurrency(item.annual_cost)}</span>
                                    <div class="w-16 flex justify-center">
                                        <span class="analytics-recurring-badge-cancelled">Cancelled</span>
                                    </div>
                                    <div class="w-16"></div>
                                    <div class="w-8"></div>
                                </div>
                            {/each}
                        </div>
                    {/if}
                {/if}

                <!-- Dismissed subscriptions (collapsible) -->
                {#if dismissedRecurring.length > 0}
                    <div class="analytics-recurring-inactive-toggle"
                         style="border-top: 1px solid var(--card-border); background: var(--surface-100)">
                        <button class="analytics-recurring-inactive-trigger"
                            on:click|stopPropagation={() => { dismissedOpen = !dismissedOpen; }}>
                            <div class="flex items-center gap-2">
                                <span class="material-symbols-outlined text-[14px]" style="color: var(--text-muted)">
                                    {dismissedOpen ? 'expand_less' : 'expand_more'}
                                </span>
                                <span class="text-[9px] font-bold tracking-[0.1em] uppercase" style="color: var(--text-muted)">
                                    Dismissed
                                </span>
                                <span class="text-[9px] font-mono font-bold px-1.5 py-0.5 rounded-full"
                                    style="background: var(--surface-200); color: var(--text-muted)">
                                    {dismissedRecurring.length}
                                </span>
                            </div>
                        </button>
                    </div>
                    {#if dismissedOpen}
                        <div class="analytics-recurring-inactive-body">
                            {#each dismissedRecurring as item, i}
                                <div class="analytics-recurring-row analytics-recurring-row-inactive"
                                    style="border-bottom: {i < dismissedRecurring.length - 1 ? '1px solid color-mix(in srgb, var(--card-border) 30%, transparent)' : 'none'}">
                                    <div class="flex items-center gap-3 flex-1 min-w-0">
                                        <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
                                            style="background: var(--surface-100)">
                                            <span class="material-symbols-outlined text-[13px]"
                                                style="color: var(--text-muted); opacity: 0.4">
                                                visibility_off
                                            </span>
                                        </div>
                                        <div class="min-w-0">
                                            <p class="text-[12px] font-medium truncate" style="color: var(--text-muted)">{item.merchant}</p>
                                            {#if item.dismissed_at}
                                                <span class="text-[9px]" style="color: var(--text-muted)">Dismissed {formatDateWithYear(item.dismissed_at)}</span>
                                            {/if}
                                        </div>
                                    </div>
                                    <div class="flex-shrink-0">
                                        <button class="analytics-recurring-restore-btn"
                                            on:click|stopPropagation={() => handleRestoreSubscription(item)}>
                                            <span class="material-symbols-outlined text-[12px]">undo</span>
                                            Restore
                                        </button>
                                    </div>
                                </div>
                            {/each}
                        </div>
                    {/if}
                {/if}

                <!-- Summary footer -->
                <div class="flex items-center justify-between px-5 py-3" style="border-top: 1px solid var(--card-border); background: var(--surface-100)">
                    <div class="flex items-center gap-2">
                        <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">lightbulb</span>
                        <span class="text-[11px]" style="color: var(--text-secondary)">
                            {activeRecurring.length} active subscription{activeRecurring.length !== 1 ? 's' : ''} totaling
                            <span class="font-bold" style="color: var(--text-primary)">{formatCurrency(recurringData.total_annual)}/yr</span>
                            {#if inactiveRecurring.length > 0}
                                · {inactiveRecurring.length} inactive — review for savings
                            {/if}
                            {#if cancelledRecurring.length > 0}
                                · {cancelledRecurring.length} cancelled
                            {/if}
                            {#if activeRecurring.filter(i => i.price_change && i.price_change.change > 0).length > 0}
                                {@const priceIncreases = activeRecurring.filter(i => i.price_change && i.price_change.change > 0)}
                                {@const totalIncrease = priceIncreases.reduce((sum, i) => sum + i.price_change.change, 0)}
                                · <span style="color: var(--negative)">{priceIncreases.length} recent price increase{priceIncreases.length !== 1 ? 's' : ''}</span>
                                <span class="font-mono font-bold" style="color: var(--negative)">(+{formatCurrency(totalIncrease)}/mo)</span>
                            {/if}
                        </span>
                    </div>
                </div>
            </div>
        </section>
    {/if}

    <!-- ═══════════════════════════════════════
         S3: SPENDING PULSE — Anomaly Cards
         ═══════════════════════════════════════ -->
    {#if currentMonthSummary && spendingPulseCards.length > 0}
        <section class="mb-10 fade-in-up" style="animation-delay: 140ms">
            <div class="flex items-center gap-2 mb-1">
                <div class="section-accent-bar"></div>
                <p class="section-header">Spending Pulse</p>
            </div>
            <p class="text-[11px] mb-4 ml-6" style="color: var(--text-muted)">
                You spent <span class="font-bold font-mono" style="color: var(--text-primary)">{formatCurrency(currentMonthSummary.expenses)}</span> in {formatMonth(selectedMonth)} — here's what stands out.
            </p>

            <div class="analytics-pulse-grid">
                {#each spendingPulseCards.slice(0, 6) as card, i}
                    {@const isClickable = true}
                    <button
                        on:click={() => drillIntoCategory(card.category)}
                        class="analytics-pulse-card card card-interactive"
                        class:analytics-pulse-anomaly={card.isOver}
                        class:analytics-pulse-under={card.isUnder}
                        class:ring-2={selectedCategory === card.category}
                        class:ring-accent={selectedCategory === card.category}
                        style="animation-delay: {i * 40}ms; --pulse-color: {card.color}">

                        <div class="flex items-center justify-between mb-2">
                            <div class="flex items-center gap-2">
                                {#if card.isOver && !card.isPeriodic}
                                    <span class="analytics-anomaly-badge">
                                        <span class="material-symbols-outlined text-[11px]">warning</span>
                                    </span>
                                {:else if card.isOver && card.isPeriodic}
                                    <span class="analytics-periodic-icon-badge">
                                        <span class="material-symbols-outlined text-[11px]">event_repeat</span>
                                    </span>
                                {:else}
                                    <span class="analytics-ok-badge">
                                        <span class="material-symbols-outlined text-[11px]">check_circle</span>
                                    </span>
                                {/if}
                                <span class="text-[11px] font-semibold truncate" style="color: var(--text-secondary)">{card.category}</span>
                            </div>
                        </div>

                        <p class="text-base font-bold font-mono" style="color: var(--text-primary)">{formatCurrency(card.total)}</p>

                        <div class="mt-1.5">
                            {#if card.isOver}
                                <span class="text-[10px] font-semibold" style="color: var(--negative)">
                                    {#if card.isPeriodic}<span class="analytics-periodic-badge" title="Periodic/seasonal expense">♻</span>{/if}
                                    ▲ {formatPercent(Math.abs(card.deviation))}{Math.abs(card.rawDeviation) > 999 ? '+' : ''} above avg
                                </span>
                            {:else if card.isUnder}
                                <span class="text-[10px] font-semibold" style="color: var(--positive)">
                                    {#if card.isPeriodic}<span class="analytics-periodic-badge" title="Periodic/seasonal expense">♻</span>{/if}
                                    ▼ {formatPercent(Math.abs(card.deviation))}{Math.abs(card.rawDeviation) > 999 ? '+' : ''} below avg
                                </span>
                            {:else}
                                <span class="text-[10px] font-medium" style="color: var(--text-muted)">
                                    {#if card.isPeriodic}<span class="analytics-periodic-badge" title="Periodic/seasonal expense">♻</span>{/if}
                                    On track
                                    {#if card.deviation !== 0}
                                        · {card.deviation > 0 ? '▲' : '▼'}{formatPercent(Math.abs(card.deviation))}
                                    {/if}
                                </span>
                            {/if}
                            {#if card.comparisonLabel && card.isPeriodic}
                                <span class="text-[8px] font-medium block mt-0.5" style="color: var(--text-muted); opacity: 0.7">
                                    vs {card.comparisonLabel}
                                </span>
                            {/if}
                        </div>

                        <!-- Mini comparison bar: current vs avg -->
                        {#if card.avgTotal > 0}
                            {@const maxBar = Math.max(card.total, card.avgTotal)}
                            <div class="mt-2 flex items-center gap-1.5">
                                <div class="flex-1 h-1 rounded-full" style="background: var(--surface-200)">
                                    <div class="h-1 rounded-full transition-all duration-500"
                                        style="width: {(card.total / maxBar) * 100}%; background: {card.isOver ? 'var(--negative)' : card.color}"></div>
                                </div>
                                <span class="text-[8px] font-mono" style="color: var(--text-muted)">avg {formatCompact(card.avgTotal)}</span>
                            </div>
                        {/if}
                    </button>
                {/each}
            </div>

            {#if spendingPulseCards.length > 6}
                <p class="text-[10px] mt-2 ml-6" style="color: var(--text-muted)">
                    + {spendingPulseCards.length - 6} more categories
                </p>
            {/if}
        </section>
    {/if}

    <!-- ═══════════════════════════════════════
         S4: TRENDS & TRAJECTORY (2-panel)
         ═══════════════════════════════════════ -->
    <section class="mb-10 fade-in-up" style="animation-delay: 180ms">
        <div class="flex items-center gap-2 mb-4">
            <div class="section-accent-bar"></div>
            <p class="section-header">Trends & Trajectory</p>
        </div>

        <div class="analytics-two-panel">
            <!-- Savings Rate Trend -->
            <div class="card" style="padding: 1.5rem">
                <p class="text-[10px] font-bold tracking-[0.12em] uppercase mb-3" style="color: var(--text-muted)">Savings Rate Over Time</p>

                {#if savingsRateGeometry}
                    {@const lastDot = savingsRateGeometry.dots[savingsRateGeometry.dots.length - 1]}
                    {@const currentRate = lastDot ? lastDot.rate : 0}
                    {@const sentimentColor = currentRate >= 25 ? 'var(--positive)' : currentRate >= 10 ? 'var(--warning)' : 'var(--negative)'}
                    <div style="position: relative;">
                        <svg width="100%" viewBox="0 0 {savingsRateGeometry.W} {savingsRateGeometry.H}" preserveAspectRatio="xMidYMid meet">
                            <defs>
                                <linearGradient id="savingsAreaGrad" x1="0" y1="0" x2="0" y2="1">
                                    <stop offset="0%" stop-color="var(--accent)" stop-opacity="0.20" />
                                    <stop offset="60%" stop-color="var(--accent)" stop-opacity="0.06" />
                                    <stop offset="100%" stop-color="var(--accent)" stop-opacity="0.01" />
                                </linearGradient>
                                <filter id="srDotGlow" x="-50%" y="-50%" width="200%" height="200%">
                                    <feGaussianBlur stdDeviation="4" result="glow"/>
                                    <feMerge>
                                        <feMergeNode in="glow"/>
                                        <feMergeNode in="SourceGraphic"/>
                                    </feMerge>
                                </filter>
                            </defs>

                            <!-- Grid -->
                            {#each savingsRateGeometry.gridLines as gl}
                                <line x1={savingsRateGeometry.padLeft} y1={gl.y} x2={savingsRateGeometry.W - 12} y2={gl.y}
                                    stroke="var(--text-muted)" stroke-width="0.5" opacity="0.08" />
                                <text x={savingsRateGeometry.padLeft - 4} y={gl.y + 3} text-anchor="end"
                                    fill="var(--text-muted)" font-size="8" font-family="DM Mono, monospace" opacity="0.5">
                                    {gl.label}
                                </text>
                            {/each}

                            <!-- Target line -->
                            <line x1={savingsRateGeometry.padLeft} y1={savingsRateGeometry.targetY}
                                x2={savingsRateGeometry.W - 12} y2={savingsRateGeometry.targetY}
                                stroke="rgba(255,255,255,0.15)" stroke-width="1" stroke-dasharray="6,4" />
                            <text x={savingsRateGeometry.W - 10} y={savingsRateGeometry.targetY - 4}
                                text-anchor="end" fill="var(--positive)" font-size="8" font-family="Inter, system-ui" font-weight="600" opacity="0.5">
                                Target {savingsRateTrend.target}%
                            </text>

                            <!-- Gradient area fill below the rolling average line -->
                            {#if savingsRateGeometry.rollingPath}
                                {@const firstDot = savingsRateGeometry.dots[0]}
                                <path d="{savingsRateGeometry.rollingPath} L{lastDot.x},{savingsRateGeometry.H - 28} L{firstDot.x},{savingsRateGeometry.H - 28} Z"
                                    fill="url(#savingsAreaGrad)" />
                            {/if}

                            <!-- Rolling average line -->
                            <path d={savingsRateGeometry.rollingPath} fill="none"
                                stroke="var(--accent)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" opacity="0.8" />

                            <!-- Actual rate dots -->
                            {#each savingsRateGeometry.dots as dot, i}
                                {#if i === savingsRateGeometry.dots.length - 1}
                                    <!-- Glowing end dot -->
                                    <circle cx={dot.x} cy={dot.y} r="8"
                                        fill={sentimentColor} opacity="0.12"
                                        filter="url(#srDotGlow)"
                                        class="savings-rate-end-dot" />
                                    <circle cx={dot.x} cy={dot.y} r="5"
                                        fill={sentimentColor}
                                        stroke="var(--card-bg)" stroke-width="2.5"
                                        class="savings-rate-end-dot"
                                        style="color: {sentimentColor}" />
                                {:else}
                                    <circle cx={dot.x} cy={dot.y} r="3"
                                        fill={dot.rate >= 25 ? 'var(--positive)' : 'var(--negative)'}
                                        opacity="0.45" />
                                {/if}
                            {/each}

                            <!-- Month labels -->
                            {#each savingsRateGeometry.monthLabels as ml}
                                <text x={ml.x} y={savingsRateGeometry.H - 4} text-anchor="middle"
                                    fill="var(--text-muted)" font-size="7.5" font-family="Inter, system-ui" font-weight="500" opacity="0.5">
                                    {ml.label}
                                </text>
                            {/each}
                        </svg>
                    </div>

                    <div class="flex items-center gap-4 mt-2 pt-2" style="border-top: 1px solid var(--card-border)">
                        <div class="flex items-center gap-1.5">
                            <span class="w-5 h-0.5 rounded-full" style="background: var(--accent)"></span>
                            <span class="text-[9px]" style="color: var(--text-muted)">3mo Rolling Avg</span>
                        </div>
                        <div class="flex items-center gap-1.5">
                            <span class="w-2 h-2 rounded-full" style="background: var(--accent); opacity: 0.5"></span>
                            <span class="text-[9px]" style="color: var(--text-muted)">Actual</span>
                        </div>
                        <div class="flex items-center gap-1.5">
                            <span class="w-5 h-0.5 rounded-full" style="background: var(--positive); opacity: 0.3; border-style: dashed;"></span>
                            <span class="text-[9px]" style="color: var(--text-muted)">Target</span>
                        </div>
                        <span class="ml-auto text-[11px] font-bold font-mono" style="color: {sentimentColor}">
                            Current: {formatPercent(savingsRateTrend.currentRate)}
                        </span>
                    </div>
                {:else}
                    <p class="text-sm text-center py-6" style="color: var(--text-muted)">Not enough data</p>
                {/if}
            </div>

            <!-- Financial Health Snapshot (merged Projected + Income Stability) -->
            <div class="card" style="padding: 1.5rem">
                <p class="text-[10px] font-bold tracking-[0.12em] uppercase mb-3" style="color: var(--text-muted)">Financial Health Snapshot</p>

                <div class="analytics-health-grid">
                    <!-- Projected Year-End -->
                    <div class="analytics-health-cell">
                        <span class="analytics-health-label">Projected Year-End</span>
                        {#if projectedYearEnd}
                            <span class="analytics-health-value" style="color: {projectedYearEnd.projectedTotal >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                {projectedYearEnd.projectedTotal >= 0 ? '+' : ''}{formatCompact(projectedYearEnd.projectedTotal)}
                            </span>
                            <span class="analytics-health-sub">
                                {formatCompact(projectedYearEnd.pessimistic)} — {formatCompact(projectedYearEnd.optimistic)} range
                            </span>
                        {:else}
                            <span class="analytics-health-value" style="color: var(--text-muted)">—</span>
                        {/if}
                    </div>

                    <!-- Avg Net/Mo -->
                    <div class="analytics-health-cell">
                        <span class="analytics-health-label">Avg Net / Month</span>
                        {#if projectedYearEnd}
                            <span class="analytics-health-value" style="color: {projectedYearEnd.avgNet >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                {projectedYearEnd.avgNet >= 0 ? '+' : ''}{formatCurrency(projectedYearEnd.avgNet)}
                            </span>
                            <span class="analytics-health-sub">
                                {projectedYearEnd.remainingMonths} months left in {projectedYearEnd.currentYear}
                            </span>
                        {:else}
                            <span class="analytics-health-value" style="color: var(--text-muted)">—</span>
                        {/if}
                    </div>

                    <!-- Income Stability -->
                    <div class="analytics-health-cell">
                        <span class="analytics-health-label">Income Stability</span>
                        {#if incomeStability}
                            <div class="flex items-center gap-2">
                                <span class="analytics-health-value" style="color: {incomeStability.dots >= 4 ? 'var(--positive)' : incomeStability.dots >= 3 ? 'var(--warning)' : 'var(--negative)'}">
                                    {incomeStability.level}
                                </span>
                                <div class="flex gap-0.5">
                                    {#each Array(5) as _, i}
                                        <span class="w-1.5 h-1.5 rounded-full"
                                            style="background: {i < incomeStability.dots ? (incomeStability.dots >= 4 ? 'var(--positive)' : incomeStability.dots >= 3 ? 'var(--warning)' : 'var(--negative)') : 'var(--surface-200)'}">
                                        </span>
                                    {/each}
                                </div>
                            </div>
                            <span class="analytics-health-sub">σ {formatCurrency(incomeStability.stdDev)}</span>
                        {:else}
                            <span class="analytics-health-value" style="color: var(--text-muted)">—</span>
                        {/if}
                    </div>

                    <!-- Income Consistency -->
                    <div class="analytics-health-cell">
                        <span class="analytics-health-label">Consistency</span>
                        {#if incomeStability}
                            <div class="flex items-center gap-1.5">
                                <span class="material-symbols-outlined text-[16px]" style="color: var(--positive)">local_fire_department</span>
                                <span class="analytics-health-value" style="color: var(--text-primary)">{incomeStability.streak} mo</span>
                            </div>
                            <span class="analytics-health-sub">consecutive income · avg {formatCompact(incomeStability.avgIncome)}/mo</span>
                        {:else}
                            <span class="analytics-health-value" style="color: var(--text-muted)">—</span>
                        {/if}
                    </div>
                </div>
            </div>
        </div>
    </section>

    <!-- ═══════════════════════════════════════
         S6: MONTH-OVER-MONTH DIFF
         ═══════════════════════════════════════ -->
    {#if momDiff.length > 0 && prevMonthData}
        <section class="mb-10 fade-in-up" style="animation-delay: 260ms">
            <div class="flex items-center gap-2 mb-4">
                <div class="section-accent-bar"></div>
                <p class="section-header">Month-over-Month Changes</p>
            </div>

            <div class="mom-glass-grid">
                <!-- Header row -->
                <div class="mom-glass-header">
                    <span class="mom-glass-cell mom-cell-category">Category</span>
                    <span class="mom-glass-cell mom-cell-right">This Month</span>
                    <span class="mom-glass-cell mom-cell-right">Last Month</span>
                    <span class="mom-glass-cell mom-cell-right">Change</span>
                </div>

                <!-- Data rows -->
                {#each momDiff.slice(0, 8) as row}
                    {@const isPositiveChange = row.delta <= 0}
                    <div class="mom-glass-row"
                         style="--row-tint: {isPositiveChange ? 'var(--positive)' : 'var(--negative)'};">
                        <div class="mom-glass-cell mom-cell-category">
                            <div class="flex items-center gap-2">
                                <div class="w-5 h-5 rounded-md flex items-center justify-center" style="background: color-mix(in srgb, {row.color} 10%, transparent)">
                                    <span class="material-symbols-outlined text-[11px]" style="color: {row.color}">{row.icon}</span>
                                </div>
                                <span class="text-[11px] font-medium" style="color: var(--text-primary)">{row.category}</span>
                            </div>
                        </div>
                        <span class="mom-glass-cell mom-cell-right text-[11px] font-mono font-medium" style="color: var(--text-primary)">{formatCurrency(row.currentTotal)}</span>
                        <span class="mom-glass-cell mom-cell-right text-[11px] font-mono" style="color: var(--text-muted)">{formatCurrency(row.prevTotal)}</span>
                        <span class="mom-glass-cell mom-cell-right">
                            <span class="delta-badge {row.delta <= 0 ? 'delta-up' : 'delta-down'}">
                                {row.delta > 0 ? '▲' : '▼'} {formatCurrency(Math.abs(row.delta))}
                            </span>
                        </span>
                    </div>
                {/each}

                {#if bestWorstMonth}
                    <div class="mom-glass-footer">
                        <span class="text-[10px]" style="color: var(--text-muted)">
                            <span class="font-bold" style="color: var(--positive)">Best month:</span> {formatMonth(bestWorstMonth.best.month)} ({formatCurrency(bestWorstMonth.best.expenses)} spend)
                        </span>
                        <span class="text-[10px]" style="color: var(--text-muted)">
                            <span class="font-bold" style="color: var(--negative)">Highest:</span> {formatMonth(bestWorstMonth.worst.month)} ({formatCurrency(bestWorstMonth.worst.expenses)} spend)
                        </span>
                    </div>
                {/if}
            </div>
        </section>
    {/if}

    <!-- ═══════════════════════════════════════
         S7: ACTIONABLE NUDGE
         ═══════════════════════════════════════ -->
    {#if actionableNudge}
        <section class="mb-10 fade-in-up" style="animation-delay: 300ms">
            <div class="analytics-nudge-card card">
                <div class="flex items-start gap-3">
                    <div class="w-9 h-9 rounded-xl flex items-center justify-center flex-shrink-0" style="background: color-mix(in srgb, var(--accent) 12%, transparent)">
                        <span class="material-symbols-outlined text-[18px]" style="color: var(--accent)">lightbulb</span>
                    </div>
                    <div class="flex-1">
                        <p class="text-[13px] font-semibold mb-1" style="color: var(--text-primary)">
                            Reduce
                            {#each actionableNudge.suggestions as sug, i}
                                <span class="font-bold" style="color: {sug.color}">{sug.name}</span>{#if i < actionableNudge.suggestions.length - 1}{i === actionableNudge.suggestions.length - 2 ? ' and ' : ', '}{/if}
                            {/each}
                            to their averages
                        </p>
                        <p class="text-[12px]" style="color: var(--text-secondary)">
                            Save an extra <span class="font-bold font-mono" style="color: var(--positive)">{formatCurrency(actionableNudge.totalPotential)}/month</span>
                            — that's <span class="font-bold font-mono" style="color: var(--positive)">{formatCurrency(actionableNudge.annualized)}/year</span>.
                        </p>
                        <p class="text-[11px] mt-1" style="color: var(--text-muted)">
                            Savings rate: {formatPercent(actionableNudge.currentSR)} → <span class="font-bold" style="color: var(--positive)">{formatPercent(actionableNudge.newSR)}</span>
                        </p>
                    </div>
                    <a href="/budget" class="analytics-nudge-cta">
                        Set a Budget
                        <span class="material-symbols-outlined text-[14px]">arrow_forward</span>
                    </a>
                </div>
            </div>
        </section>
    {/if}


    <!-- ═══════════════════════════════════════
         MONTHLY DATA TABLE (collapsed)
         ═══════════════════════════════════════ -->
    <div class="card mb-8 fade-in-up" style="padding: 1.25rem 1.5rem; animation-delay: 340ms;">
    <details style="margin-bottom: 0;">
        <summary class="flex items-center gap-3 cursor-pointer select-none mb-3 rounded-xl transition-colors duration-150 hover:bg-[var(--surface-100)]"
            style="list-style: none;">
            <div class="section-accent-bar"></div>
            <div class="flex-1">
                <p class="section-header" style="margin: 0;">Monthly Data Table</p>
                <p class="text-[10px]" style="color: var(--text-muted); margin: 0;">Click to view detailed monthly breakdown</p>
            </div>
            <span class="material-symbols-outlined text-[18px] transition-transform duration-200" style="color: var(--text-primary)">expand_more</span>
        </summary>
        <div class="overflow-hidden" style="padding: 0">
            <table class="w-full">
                <thead>
                    <tr style="border-bottom: 1px solid var(--card-border)">
                        {#each ['Month', 'Income', 'Spending', 'Ext. Transfers', 'Net Flow'] as h}
                            <th class="text-left px-5 py-2.5 text-[9px] font-bold uppercase tracking-wider"
                                style="color: var(--text-muted)">{h}</th>
                        {/each}
                    </tr>
                </thead>
                <tbody>
                    {#each [...monthly].sort((a,b) => b.month.localeCompare(a.month)) as m}
                        {@const accrualNet = (m.income || 0) - (m.expenses || 0) + (m.refunds || 0) - (m.external_transfers || 0)}
                        <tr class="transition-colors cursor-pointer" style="border-bottom: 1px solid var(--card-border)"
                            on:click={() => { selectedMonth = m.month; }}>
                            <td class="px-5 py-2.5 text-[12px] font-medium" style="color: var(--text-primary)">
                                {formatMonth(m.month)}
                                {#if m.month === selectedMonth}
                                    <span class="inline-block w-1.5 h-1.5 rounded-full ml-2" style="background: var(--accent)"></span>
                                {/if}
                            </td>
                            <td class="px-5 py-2.5 text-[12px] font-mono text-positive">{formatCurrency(m.income)}</td>
                            <td class="px-5 py-2.5 text-[12px] font-mono text-negative">{formatCurrency(m.expenses)}</td>
                            <td class="px-5 py-2.5 text-[12px] font-mono" style="color: {(m.external_transfers || 0) > 0 ? 'var(--warning)' : 'var(--text-muted)'}">{formatCurrency(m.external_transfers || 0)}</td>
                            <td class="px-5 py-2.5 text-[12px] font-bold font-mono"
                                style="color: {accrualNet >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                                {accrualNet >= 0 ? '+' : ''}{formatCurrency(accrualNet)}
                            </td>
                        </tr>
                    {/each}
                </tbody>
            </table>
        </div>
    </details>
    </div>
    </div>
{/if}