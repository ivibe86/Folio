<script>
import { onMount, createEventDispatcher } from 'svelte';
import { formatCurrency, formatCompact, formatMonthShort, CATEGORY_COLORS } from '$lib/utils.js';
import { darkMode, privacyMode } from '$lib/stores.js';

export let monthlyData = []; // Array of { month, income, expenses, refunds, external_transfers }
export let height = 370;
export let categoryData = []; // Optional: Array of { month, categories: [{ category, total }] } for tooltip breakdown

let containerEl;
let svgWidth = 800;
let mounted = false;
let hoveredIdx = -1;

// Reactive privacy key — forces Svelte to re-render all formatted values when toggled
$: privacyKey = $privacyMode ? 1 : 0;

const MARGIN = { top: 32, right: 24, bottom: 84, left: 56 };
const LABEL_HEIGHT = 22;
const MINI_BAR_ZONE_HEIGHT = 44;
const MINI_BAR_GAP = 6; // gap between main chart and mini bars

// Colors
const INCOME_COLOR = '#38BDF8';
const INCOME_GLOW = '#7DD3FC';
const SPENDING_COLOR = '#F472B6';
const SPENDING_GLOW = '#F9A8D4';
const AVG_LINE_COLOR = 'rgba(251, 191, 36, 0.6)';
const AVG_PILL_BG = 'rgba(251, 191, 36, 0.12)';
const AVG_PILL_BORDER = 'rgba(251, 191, 36, 0.30)';
const AVG_TEXT_COLOR = '#FBBF24';
const SURPLUS_BAR_COLOR = '#34d399';
const DEFICIT_BAR_COLOR = '#f87171';
const NET_POSITIVE_COLOR = '#34d399';
const NET_NEGATIVE_COLOR = '#f87171';

// Privacy-safe formatting helpers
function fmtCompact(v) {
    if ($privacyMode) return '$\u2022\u2022\u2022';
    return formatCompact(v);
}

function fmtCurrency(v) {
    if ($privacyMode) return '$\u2022\u2022\u2022\u2022\u2022';
    return formatCurrency(v);
}

function fmtAxis(v) {
    if ($privacyMode) return '$\u2022\u2022\u2022';
    return '$' + (v >= 1000 ? (v / 1000).toFixed(1) + 'k' : v.toFixed(0));
}

function fmtSignedCompact(v) {
    if ($privacyMode) return '\u2022\u2022\u2022';
    return (v >= 0 ? '+' : '') + formatCompact(v);
}

// Month formatting helper for full month name in tooltip
function formatMonthFull(monthStr) {
    if (!monthStr) return '';
    const [y, m] = monthStr.split('-');
    const dt = new Date(parseInt(y), parseInt(m) - 1);
    return dt.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });
}

// Derive the last 12 months of data, sorted chronologically
$: chartData = (() => {
    if (!monthlyData || monthlyData.length === 0) return [];
    const sorted = [...monthlyData].sort((a, b) => a.month.localeCompare(b.month));
    const last12 = sorted.slice(-12);
    return last12.map(m => {
        const netExpenses = (m.expenses || 0) - (m.refunds || 0);
        return {
            month: m.month,
            income: m.income || 0,
            spending: Math.max(netExpenses, 0),
            net: (m.income || 0) - netExpenses,
            external_transfers: m.external_transfers || 0,
        };
    });
})();

// Averages
$: avgSpending = chartData.length > 0
    ? chartData.reduce((s, d) => s + d.spending, 0) / chartData.length
    : 0;

$: avgIncome = chartData.length > 0
    ? chartData.reduce((s, d) => s + d.income, 0) / chartData.length
    : 0;

$: avgNet = avgIncome - avgSpending;

// Summary stats
$: summaryStats = (() => {
    if (chartData.length < 2) return null;
    const totalIncome = chartData.reduce((s, d) => s + d.income, 0);
    const totalSpending = chartData.reduce((s, d) => s + d.spending, 0);
    const nets = chartData.map(d => d.net);

    const bestMonth = chartData.reduce((best, d) =>
        d.net > (best?.net ?? -Infinity) ? d : best, chartData[0]);

    const highestSpendMonth = chartData.reduce((best, d) =>
        d.spending > (best?.spending ?? 0) ? d : best, chartData[0]);

    return {
        avgIncome: totalIncome / chartData.length,
        avgSpending: totalSpending / chartData.length,
        avgNet: (totalIncome - totalSpending) / chartData.length,
        bestMonth,
        highestSpendMonth,
        monthCount: chartData.length,
    };
})();

// Chart geometry
$: chart = (() => {
    if (chartData.length < 2) return null;

    const W = svgWidth;
    const H = height;
    const mainPlotBottom = H - MARGIN.bottom;
    const plotW = W - MARGIN.left - MARGIN.right;
    const plotH = mainPlotBottom - MARGIN.top - MINI_BAR_ZONE_HEIGHT - MINI_BAR_GAP;

    const allValues = chartData.flatMap(d => [d.income, d.spending]);
    const rawMax = Math.max(...allValues, 1);
    const padding = rawMax * 0.12;
    const max = rawMax + padding;
    const min = 0;
    const range = max - min || 1;

    const stepX = plotW / (chartData.length - 1);

    const yScale = (v) => MARGIN.top + plotH - ((v - min) / range) * plotH;
    const xScale = (i) => MARGIN.left + i * stepX;

    // Build points
    const incomePoints = chartData.map((d, i) => ({ x: xScale(i), y: yScale(d.income), value: d.income }));
    const spendingPoints = chartData.map((d, i) => ({ x: xScale(i), y: yScale(d.spending), value: d.spending }));

    // Smooth spline path builder (Catmull-Rom) — also returns per-segment control points
    function catmullRomSpline(points, tension = 0.5) {
        const segments = [];
        if (points.length < 2) return { path: '', segments };
        let path = `M${points[0].x.toFixed(1)},${points[0].y.toFixed(1)}`;
        for (let i = 0; i < points.length - 1; i++) {
            const p0 = points[Math.max(i - 1, 0)];
            const p1 = points[i];
            const p2 = points[i + 1];
            const p3 = points[Math.min(i + 2, points.length - 1)];

            const cp1x = p1.x + (p2.x - p0.x) * tension / 3;
            const cp1y = p1.y + (p2.y - p0.y) * tension / 3;
            const cp2x = p2.x - (p3.x - p1.x) * tension / 3;
            const cp2y = p2.y - (p3.y - p1.y) * tension / 3;

            segments.push({ p1, p2, cp1: { x: cp1x, y: cp1y }, cp2: { x: cp2x, y: cp2y } });
            path += ` C${cp1x.toFixed(1)},${cp1y.toFixed(1)} ${cp2x.toFixed(1)},${cp2y.toFixed(1)} ${p2.x.toFixed(1)},${p2.y.toFixed(1)}`;
        }
        return { path, segments };
    }

    const incomeSpline = catmullRomSpline(incomePoints);
    const spendingSpline = catmullRomSpline(spendingPoints);
    const incomeLine = incomeSpline.path;
    const spendingLine = spendingSpline.path;

    // Base Y for area fills
    const baseY = yScale(0);

    // Gap segments between curves (for green/red fill) — using curved edges
    // Helper: evaluate a cubic bezier at parameter t
    function bezierAt(p1, cp1, cp2, p2, t) {
        const u = 1 - t;
        return {
            x: u*u*u*p1.x + 3*u*u*t*cp1.x + 3*u*t*t*cp2.x + t*t*t*p2.x,
            y: u*u*u*p1.y + 3*u*u*t*cp1.y + 3*u*t*t*cp2.y + t*t*t*p2.y,
        };
    }
    // Helper: split a cubic bezier at parameter t, returns { left, right } each with { p1, cp1, cp2, p2 }
    function splitBezier(p1, cp1, cp2, p2, t) {
        const a = { x: p1.x + t*(cp1.x - p1.x), y: p1.y + t*(cp1.y - p1.y) };
        const b = { x: cp1.x + t*(cp2.x - cp1.x), y: cp1.y + t*(cp2.y - cp1.y) };
        const c = { x: cp2.x + t*(p2.x - cp2.x), y: cp2.y + t*(p2.y - cp2.y) };
        const d = { x: a.x + t*(b.x - a.x), y: a.y + t*(b.y - a.y) };
        const e = { x: b.x + t*(c.x - b.x), y: b.y + t*(c.y - b.y) };
        const f = { x: d.x + t*(e.x - d.x), y: d.y + t*(e.y - d.y) };
        return {
            left:  { p1, cp1: a, cp2: d, p2: f },
            right: { p1: f, cp1: e, cp2: c, p2 },
        };
    }
    // Helper: reverse a bezier segment
    function reverseSeg(s) {
        return { p1: s.p2, cp1: s.cp2, cp2: s.cp1, p2: s.p1 };
    }
    // Helper: format a cubic bezier segment as SVG C command (without the M)
    function segToC(s) {
        return `C${s.cp1.x.toFixed(1)},${s.cp1.y.toFixed(1)} ${s.cp2.x.toFixed(1)},${s.cp2.y.toFixed(1)} ${s.p2.x.toFixed(1)},${s.p2.y.toFixed(1)}`;
    }

    const gapSegments = [];
    for (let i = 0; i < chartData.length - 1; i++) {
        const iSeg = incomeSpline.segments[i];   // { p1, p2, cp1, cp2 }
        const sSeg = spendingSpline.segments[i];

        const d1 = chartData[i].income - chartData[i].spending;
        const d2 = chartData[i + 1].income - chartData[i + 1].spending;

        const hasCrossover = (d1 >= 0 && d2 < 0) || (d1 < 0 && d2 >= 0);

        if (hasCrossover) {
            // Approximate crossover t by sampling the bezier gap
            let tCross = 0.5;
            let lo = 0, hi = 1;
            for (let iter = 0; iter < 20; iter++) {
                const mid = (lo + hi) / 2;
                const iy = bezierAt(iSeg.p1, iSeg.cp1, iSeg.cp2, iSeg.p2, mid).y;
                const sy = bezierAt(sSeg.p1, sSeg.cp1, sSeg.cp2, sSeg.p2, mid).y;
                const gap = sy - iy; // positive = income above spending (surplus) in SVG coords (y inverted)
                if ((d1 >= 0 && gap > 0) || (d1 < 0 && gap < 0)) {
                    lo = mid;
                } else {
                    hi = mid;
                }
                tCross = mid;
            }

            const iSplit = splitBezier(iSeg.p1, iSeg.cp1, iSeg.cp2, iSeg.p2, tCross);
            const sSplit = splitBezier(sSeg.p1, sSeg.cp1, sSeg.cp2, sSeg.p2, tCross);

            // First half: income left + spending left reversed
            const sLeftRev = reverseSeg(sSplit.left);
            const path1 = `M${iSplit.left.p1.x.toFixed(1)},${iSplit.left.p1.y.toFixed(1)} ${segToC(iSplit.left)} L${sLeftRev.p1.x.toFixed(1)},${sLeftRev.p1.y.toFixed(1)} ${segToC(sLeftRev)} Z`;
            gapSegments.push({ path: path1, surplus: d1 >= 0 });

            // Second half: income right + spending right reversed
            const sRightRev = reverseSeg(sSplit.right);
            const path2 = `M${iSplit.right.p1.x.toFixed(1)},${iSplit.right.p1.y.toFixed(1)} ${segToC(iSplit.right)} L${sRightRev.p1.x.toFixed(1)},${sRightRev.p1.y.toFixed(1)} ${segToC(sRightRev)} Z`;
            gapSegments.push({ path: path2, surplus: d2 >= 0 });
        } else {
            // No crossover: trace income curve forward, then spending curve backward
            const sRev = reverseSeg(sSeg);
            const segPath = `M${iSeg.p1.x.toFixed(1)},${iSeg.p1.y.toFixed(1)} ${segToC(iSeg)} L${sRev.p1.x.toFixed(1)},${sRev.p1.y.toFixed(1)} ${segToC(sRev)} Z`;
            gapSegments.push({ path: segPath, surplus: d1 >= 0 });
        }
    }

    // Average spending line Y
    const avgSpendingY = yScale(avgSpending);

    // Mini net bars geometry
    const miniBarTop = MARGIN.top + plotH + MINI_BAR_GAP;
    const miniBarHeight = MINI_BAR_ZONE_HEIGHT;
    const miniBarMid = miniBarTop + miniBarHeight / 2;
    const nets = chartData.map(d => d.net);
    const maxAbsNet = Math.max(...nets.map(Math.abs), 1);
    const miniBarScale = (v) => (Math.abs(v) / maxAbsNet) * (miniBarHeight / 2 - 2);
    const miniBarWidth = Math.min(stepX * 0.55, 20);

    const miniBars = chartData.map((d, i) => {
        const barH = miniBarScale(d.net);
        const isPositive = d.net >= 0;
        return {
            x: xScale(i) - miniBarWidth / 2,
            y: isPositive ? miniBarMid - barH : miniBarMid,
            w: miniBarWidth,
            h: Math.max(barH, 1),
            positive: isPositive,
            value: d.net,
        };
    });

    // Y-axis ticks
    const tickCount = 5;
    const yTicks = Array.from({ length: tickCount }, (_, i) => {
        const v = min + (range * (tickCount - 1 - i)) / (tickCount - 1);
        return { value: v, y: yScale(v) };
    });

    // X-axis labels
    const xLabels = chartData.map((d, i) => ({
        x: xScale(i),
        label: formatMonthShort(d.month),
        month: d.month
    }));

    // Divider line between main chart and mini bars
    const dividerY = MARGIN.top + plotH + MINI_BAR_GAP / 2;

    return {
        W, H, plotW, plotH,
        incomePoints, spendingPoints,
        incomeLine, spendingLine,
        gapSegments,
        avgSpendingY,
        yTicks, xLabels, stepX,
        yScale, xScale, baseY,
        miniBarTop, miniBarMid, miniBars, miniBarWidth,
        dividerY,
    };
})();

// Hover tooltip data
$: tooltipData = (() => {
    // Force Svelte to track categoryData as a dependency
    const _catData = categoryData;

    if (hoveredIdx < 0 || !chartData[hoveredIdx] || !chart) return null;
    const d = chartData[hoveredIdx];
    const iP = chart.incomePoints[hoveredIdx];
    const sP = chart.spendingPoints[hoveredIdx];
    const net = d.income - d.spending;
    const saveRate = d.income > 0 ? Math.max((net / d.income) * 100, 0) : 0;
    const vsAvg = net - avgNet;

    // Find category breakdown for this month if available
    let topCategories = [];
    if (_catData && _catData.length > 0) {
        const monthCats = _catData.find(c => c.month === d.month);
        if (monthCats && monthCats.categories) {
            topCategories = [...monthCats.categories]
                .sort((a, b) => b.total - a.total)
                .slice(0, 5);
        }
    }

    return {
        x: iP.x,
        incomeY: iP.y,
        spendingY: sP.y,
        month: d.month,
        income: d.income,
        spending: d.spending,
        net,
        saveRate,
        isSurplus: net >= 0,
        vsAvg,
        topCategories,
    };
})();

function handleMouseMove(e) {
    if (!chart || !containerEl) return;
    const svg = e.currentTarget;
    const rect = svg.getBoundingClientRect();
    const mouseX = ((e.clientX - rect.left) / rect.width) * chart.W;

    const idx = Math.round((mouseX - MARGIN.left) / chart.stepX);
    hoveredIdx = Math.max(0, Math.min(idx, chartData.length - 1));
}

function handleMouseLeave() {
    hoveredIdx = -1;
}

onMount(() => {
    mounted = true;
    if (containerEl) {
        const rect = containerEl.getBoundingClientRect();
        svgWidth = rect.width || 800;
    }

    const observer = new ResizeObserver(() => {
        if (containerEl) {
            svgWidth = containerEl.getBoundingClientRect().width || 800;
        }
    });
    if (containerEl) observer.observe(containerEl);
    return () => observer.disconnect();
});
</script>

<div bind:this={containerEl} class="ivs-container">
    {#if chart && chartData.length >= 2}
        <svg
            width={chart.W}
            height={chart.H}
            viewBox="0 0 {chart.W} {chart.H}"
            preserveAspectRatio="xMidYMid meet"
            class="ivs-svg"
            role="img"
            aria-label="Income versus spending chart"
            on:mousemove={handleMouseMove}
            on:mouseleave={handleMouseLeave}
        >
            <defs>
                <!-- Income area gradient -->
                <linearGradient id="ivsIncomeAreaGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color={INCOME_COLOR} stop-opacity="0.25" />
                    <stop offset="40%" stop-color={INCOME_COLOR} stop-opacity="0.12" />
                    <stop offset="100%" stop-color={INCOME_COLOR} stop-opacity="0.02" />
                </linearGradient>

                <!-- Spending area gradient -->
                <linearGradient id="ivsSpendingAreaGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color={SPENDING_COLOR} stop-opacity="0.22" />
                    <stop offset="40%" stop-color={SPENDING_COLOR} stop-opacity="0.10" />
                    <stop offset="100%" stop-color={SPENDING_COLOR} stop-opacity="0.02" />
                </linearGradient>

                <!-- Mini bar gradients -->
                <linearGradient id="ivsMiniBarPos" x1="0" y1="1" x2="0" y2="0">
                    <stop offset="0%" stop-color={SURPLUS_BAR_COLOR} stop-opacity="0.45" />
                    <stop offset="100%" stop-color={SURPLUS_BAR_COLOR} stop-opacity="0.85" />
                </linearGradient>
                <linearGradient id="ivsMiniBarNeg" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stop-color={DEFICIT_BAR_COLOR} stop-opacity="0.45" />
                    <stop offset="100%" stop-color={DEFICIT_BAR_COLOR} stop-opacity="0.85" />
                </linearGradient>

                <!-- Line glow filters -->
                <filter id="ivsIncomeGlow" x="-10%" y="-30%" width="120%" height="160%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="4" result="blur" />
                    <feMerge>
                        <feMergeNode in="blur" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>

                <filter id="ivsSpendingGlow" x="-10%" y="-30%" width="120%" height="160%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="4" result="blur" />
                    <feMerge>
                        <feMergeNode in="blur" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>

                <filter id="ivsDotGlow" x="-50%" y="-50%" width="200%" height="200%">
                    <feGaussianBlur stdDeviation="5" result="blur" />
                    <feMerge>
                        <feMergeNode in="blur" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>

                <filter id="ivsTooltipShadow" x="-15%" y="-10%" width="130%" height="140%">
                    <feGaussianBlur in="SourceAlpha" stdDeviation="6" result="blur" />
                    <feOffset in="blur" dx="0" dy="4" result="offsetBlur" />
                    <feFlood flood-color="rgba(0,0,0,0.35)" result="color" />
                    <feComposite in="color" in2="offsetBlur" operator="in" result="shadow" />
                    <feMerge>
                        <feMergeNode in="shadow" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>

                <filter id="ivsTooltipGlow" x="-20%" y="-20%" width="140%" height="140%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="8" result="blur" />
                    <feMerge>
                        <feMergeNode in="blur" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>

                <!-- Surplus gap glow -->
                <filter id="ivsSurplusGlow" x="-5%" y="-10%" width="110%" height="120%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="6" result="blur" />
                    <feMerge>
                        <feMergeNode in="blur" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>

                <filter id="ivsAvgLineGlow" x="-1%" y="-50%" width="102%" height="200%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="2" result="blur"/>
                    <feMerge>
                        <feMergeNode in="blur"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>
            </defs>

            <!-- Y-axis gridlines -->
            {#each chart.yTicks as tick}
                <line
                    x1={MARGIN.left} x2={chart.W - MARGIN.right}
                    y1={tick.y} y2={tick.y}
                    stroke="var(--text-muted)" stroke-width="0.5" opacity="0.10"
                    stroke-dasharray="4,6"
                />
                <text
                    x={MARGIN.left - 10} y={tick.y + 3.5}
                    text-anchor="end" fill="var(--text-muted)" font-size="9"
                    font-family="DM Mono, monospace" opacity="0.55"
                >{(void privacyKey, fmtAxis(tick.value))}</text>
            {/each}

            <!-- Gap fill: colored segments between curves -->
            {#each chart.gapSegments as seg, i}
                <path
                    d={seg.path}
                    fill={seg.surplus ? 'rgba(52, 211, 153, 0.14)' : 'rgba(248, 113, 113, 0.14)'}
                    class="ivs-gap-segment"
                    style="animation-delay: {i * 50}ms"
                />
                <path
                    d={seg.path}
                    fill={seg.surplus ? 'rgba(52, 211, 153, 0.08)' : 'rgba(248, 113, 113, 0.08)'}
                    filter="url(#ivsSurplusGlow)"
                    style="pointer-events: none;"
                />
            {/each}

            <!-- ——— Average Spending Threshold Line ——— -->
            {#if avgSpending > 0}
                <!-- Glow underlayer -->
                <line
                    x1={MARGIN.left} x2={chart.W - MARGIN.right}
                    y1={chart.avgSpendingY} y2={chart.avgSpendingY}
                    stroke={AVG_LINE_COLOR}
                    stroke-width="3"
                    stroke-dasharray="6,4"
                    stroke-linecap="round"
                    filter="url(#ivsAvgLineGlow)"
                    opacity="0.5"
                />
                <!-- Crisp visible dashed line on top -->
                <line
                    x1={MARGIN.left} x2={chart.W - MARGIN.right}
                    y1={chart.avgSpendingY} y2={chart.avgSpendingY}
                    stroke={AVG_TEXT_COLOR}
                    stroke-width="1.5"
                    stroke-dasharray="6,4"
                    stroke-linecap="round"
                    opacity="0.85"
                />
                <!-- Pill label at right end -->
                {@const pillW = 78}
                {@const pillH = 18}
                {@const pillX = chart.W - MARGIN.right - pillW - 4}
                {@const pillY = chart.avgSpendingY - pillH / 2}
                <rect
                    x={pillX} y={pillY}
                    width={pillW} height={pillH}
                    rx="9" ry="9"
                    fill={AVG_PILL_BG}
                    stroke={AVG_PILL_BORDER}
                    stroke-width="1"
                />
                <text
                    x={pillX + pillW / 2} y={pillY + pillH / 2 + 3}
                    text-anchor="middle"
                    fill={AVG_TEXT_COLOR}
                    font-size="8" font-weight="700"
                    font-family="DM Mono, monospace"
                    style="pointer-events: none;"
                >Avg {(void privacyKey, fmtCompact(avgSpending))}/mo</text>
            {/if}

            <!-- Income line glow -->
            <path
                d={chart.incomeLine} fill="none"
                stroke={INCOME_GLOW} stroke-width="8"
                stroke-linecap="round" stroke-linejoin="round"
                filter="url(#ivsIncomeGlow)" opacity="0.15"
            />

            <!-- Income line -->
            <path
                d={chart.incomeLine} fill="none"
                stroke={INCOME_COLOR} stroke-width="2.5"
                stroke-linecap="round" stroke-linejoin="round"
            />

            <!-- Spending line glow -->
            <path
                d={chart.spendingLine} fill="none"
                stroke={SPENDING_GLOW} stroke-width="8"
                stroke-linecap="round" stroke-linejoin="round"
                filter="url(#ivsSpendingGlow)" opacity="0.15"
            />

            <!-- Spending line -->
            <path
                d={chart.spendingLine} fill="none"
                stroke={SPENDING_COLOR} stroke-width="2.5"
                stroke-linecap="round" stroke-linejoin="round"
            />

            <!-- Data point dots + value labels -->
            {#each chartData as d, i}
                {@const iP = chart.incomePoints[i]}
                {@const sP = chart.spendingPoints[i]}
                {@const isHovered = hoveredIdx === i}
                {@const dimmed = hoveredIdx >= 0 && !isHovered}

                <!-- Income dot — only shown on hover -->
                {#if isHovered}
                    <circle
                        cx={iP.x} cy={iP.y} r={5}
                        fill="#E0F7FA"
                        stroke={INCOME_COLOR} stroke-width={2}
                        filter="url(#ivsDotGlow)"
                        class="ivs-dot"
                    />

                    <!-- Income value label (above the line) — only on hover -->
                    <g class="ivs-value-label" opacity={1}>
                        <rect
                            x={iP.x - 26} y={iP.y - LABEL_HEIGHT - 6}
                            width="52" height={LABEL_HEIGHT - 2}
                            rx="8" ry="8"
                            fill={$darkMode ? 'rgba(15, 23, 42, 0.80)' : 'rgba(27, 31, 42, 0.80)'}
                            stroke={INCOME_COLOR}
                            stroke-width={1}
                            stroke-opacity={0.6}
                        />
                        <text
                            x={iP.x} y={iP.y - LABEL_HEIGHT + 8}
                            text-anchor="middle"
                            fill={INCOME_COLOR}
                            font-size="9" font-weight="700"
                            font-family="DM Mono, monospace"
                            style="pointer-events: none;"
                        >{(void privacyKey, fmtCompact(d.income))}</text>
                    </g>
                {/if}

                <!-- Spending dot — always visible -->
                <circle
                    cx={sP.x} cy={sP.y} r={isHovered ? 5 : 3.5}
                    fill={isHovered ? '#FFF1F2' : SPENDING_COLOR}
                    stroke={SPENDING_COLOR} stroke-width={isHovered ? 2 : 1.5}
                    opacity={dimmed ? 0.3 : 1}
                    filter={isHovered ? 'url(#ivsDotGlow)' : 'none'}
                    class="ivs-dot"
                />

                <!-- Spending value label (below the line) -->
                {#if !dimmed}
                    <g class="ivs-value-label" opacity={isHovered ? 1 : 0.85}>
                        <rect
                            x={sP.x - 26} y={sP.y + 6}
                            width="52" height={LABEL_HEIGHT - 2}
                            rx="8" ry="8"
                            fill={$darkMode ? 'rgba(15, 23, 42, 0.80)' : 'rgba(27, 31, 42, 0.80)'}
                            stroke={SPENDING_COLOR}
                            stroke-width={isHovered ? 1 : 0.5}
                            stroke-opacity={isHovered ? 0.6 : 0.25}
                        />
                        <text
                            x={sP.x} y={sP.y + LABEL_HEIGHT - 2}
                            text-anchor="middle"
                            fill={SPENDING_COLOR}
                            font-size="9" font-weight="700"
                            font-family="DM Mono, monospace"
                            style="pointer-events: none;"
                        >{(void privacyKey, fmtCompact(d.spending))}</text>
                    </g>
                {/if}
            {/each}

            <!-- Crossover points (where lines intersect) -->
            {#each chartData as d, i}
                {#if i < chartData.length - 1}
                    {@const curr = chartData[i]}
                    {@const next = chartData[i + 1]}
                    {@const currDiff = curr.income - curr.spending}
                    {@const nextDiff = next.income - next.spending}
                    {#if (currDiff >= 0 && nextDiff < 0) || (currDiff < 0 && nextDiff >= 0)}
                        {@const t = Math.abs(currDiff) / (Math.abs(currDiff) + Math.abs(nextDiff))}
                        {@const cx = chart.incomePoints[i].x + t * (chart.incomePoints[i + 1].x - chart.incomePoints[i].x)}
                        {@const cy = chart.incomePoints[i].y + t * (chart.incomePoints[i + 1].y - chart.incomePoints[i].y)}
                        <circle
                            cx={cx} cy={cy} r="5"
                            fill="none"
                            stroke="rgba(255, 255, 255, 0.6)"
                            stroke-width="1.5"
                            stroke-dasharray="2,2"
                            class="ivs-crossover"
                        />
                        <circle
                            cx={cx} cy={cy} r="2.5"
                            fill="white" opacity="0.7"
                        />
                    {/if}
                {/if}
            {/each}

            <!-- ——— Divider between main chart and mini bars ——— -->
            <line
                x1={MARGIN.left} x2={chart.W - MARGIN.right}
                y1={chart.dividerY} y2={chart.dividerY}
                stroke="var(--text-muted)" stroke-width="0.5" opacity="0.12"
            />

            <!-- ——— Mini Net Surplus/Deficit Bars ——— -->
            <!-- Zero line for mini bars -->
            <line
                x1={MARGIN.left} x2={chart.W - MARGIN.right}
                y1={chart.miniBarMid} y2={chart.miniBarMid}
                stroke="var(--text-muted)" stroke-width="0.5" opacity="0.15"
            />

            {#each chart.miniBars as bar, i}
                {@const isHovered = hoveredIdx === i}
                <rect
                    x={bar.x} y={bar.y}
                    width={bar.w} height={bar.h}
                    rx="2" ry="2"
                    fill={bar.positive ? 'url(#ivsMiniBarPos)' : 'url(#ivsMiniBarNeg)'}
                    opacity={hoveredIdx >= 0 ? (isHovered ? 1 : 0.35) : 0.75}
                    stroke={isHovered ? (bar.positive ? SURPLUS_BAR_COLOR : DEFICIT_BAR_COLOR) : 'transparent'}
                    stroke-width={isHovered ? 1.5 : 0}
                    class="ivs-mini-bar"
                    style="animation-delay: {i * 40}ms"
                />
                <!-- Hovered bar value label -->
                {#if isHovered}
                    {@const labelY = bar.positive ? bar.y - 5 : bar.y + bar.h + 10}
                    <text
                        x={bar.x + bar.w / 2} y={labelY}
                        text-anchor="middle"
                        fill={bar.positive ? SURPLUS_BAR_COLOR : DEFICIT_BAR_COLOR}
                        font-size="7.5" font-weight="700"
                        font-family="DM Mono, monospace"
                        style="pointer-events: none;"
                    >{(void privacyKey, fmtSignedCompact(bar.value))}</text>
                {/if}
            {/each}

            <!-- X-axis month labels (below mini bars) -->
            {#each chart.xLabels as xl, i}
                <text
                    x={xl.x}
                    y={chart.H - 8}
                    text-anchor="middle"
                    fill="var(--text-muted)"
                    font-size="9"
                    font-family="Inter, system-ui, sans-serif"
                    font-weight={hoveredIdx === i ? '700' : '500'}
                    opacity={hoveredIdx === i ? 1 : 0.6}
                    style="transition: opacity 0.15s ease, font-weight 0.15s ease;"
                >{xl.label}</text>
            {/each}

            <!-- ——— Layer 6: Hover vertical reference line + tooltip card ——— -->
            {#if tooltipData}
                {@const tt = tooltipData}
                {@const hasCats = tt.topCategories.length > 0}

                <!-- Vertical reference line — full height from top through mini bars -->
                <line
                    x1={tt.x} y1={MARGIN.top}
                    x2={tt.x} y2={chart.miniBarMid + MINI_BAR_ZONE_HEIGHT / 2}
                    stroke="rgba(255,255,255,0.30)"
                    stroke-width="1"
                    class="ivs-ref-line"
                />

                <!-- Tooltip card — pinned near top of chart -->
                {@const ttW = 210}
                {@const headerH = 28}
                {@const metricsH = 56}
                {@const separatorH = 10}
                {@const catHeaderH = hasCats ? 18 : 0}
                {@const catRowH = hasCats ? tt.topCategories.length * 18 : 0}
                {@const ttH = headerH + metricsH + separatorH + catHeaderH + catRowH + (hasCats ? 8 : 0)}
                {@const ttX = Math.max(MARGIN.left, Math.min(tt.x - ttW / 2, chart.W - MARGIN.right - ttW))}
                {@const ttY = Math.max(4, MARGIN.top - 8)}

                <!-- Outer glow -->
                <rect
                    x={ttX - 1} y={ttY - 1}
                    width={ttW + 2} height={ttH + 2}
                    rx="13" ry="13"
                    fill="none"
                    stroke={tt.isSurplus ? 'rgba(52, 211, 153, 0.15)' : 'rgba(248, 113, 113, 0.15)'}
                    stroke-width="2"
                    filter="url(#ivsTooltipGlow)"
                    class="ivs-tooltip-glow"
                />

                <!-- Card background -->
                <rect
                    x={ttX} y={ttY}
                    width={ttW} height={ttH}
                    rx="12" ry="12"
                    fill={$darkMode ? 'rgba(15, 23, 42, 0.96)' : 'rgba(20, 24, 33, 0.96)'}
                    stroke={$darkMode ? 'rgba(148, 163, 184, 0.15)' : 'rgba(100, 116, 139, 0.20)'}
                    stroke-width="1"
                    filter="url(#ivsTooltipShadow)"
                    class="ivs-tooltip-card"
                />

                <!-- Month header -->
                <text
                    x={ttX + 12} y={ttY + 18}
                    fill="rgba(241, 245, 249, 0.95)" font-size="10.5" font-weight="800"
                    font-family="Inter, system-ui, sans-serif"
                    style="pointer-events: none; letter-spacing: 0.01em;"
                >{formatMonthFull(tt.month)}</text>

                <!-- Separator under header -->
                <line
                    x1={ttX + 10} y1={ttY + headerH}
                    x2={ttX + ttW - 10} y2={ttY + headerH}
                    stroke="rgba(148, 163, 184, 0.15)" stroke-width="0.5"
                />

                <!-- Income row -->
                {@const rowStartY = ttY + headerH + 14}
                <circle cx={ttX + 14} cy={rowStartY - 3} r="3.5" fill={INCOME_COLOR} />
                <text
                    x={ttX + 24} y={rowStartY}
                    fill="rgba(241, 245, 249, 0.7)" font-size="9" font-weight="500"
                    font-family="Inter, system-ui, sans-serif"
                    style="pointer-events: none;"
                >Income</text>
                <text
                    x={ttX + ttW - 12} y={rowStartY}
                    text-anchor="end"
                    fill={INCOME_COLOR} font-size="9.5" font-weight="700"
                    font-family="DM Mono, monospace"
                    style="pointer-events: none;"
                >{(void privacyKey, fmtCurrency(tt.income))}</text>

                <!-- Spending row -->
                {@const spendRowY = rowStartY + 18}
                <circle cx={ttX + 14} cy={spendRowY - 3} r="3.5" fill={SPENDING_COLOR} />
                <text
                    x={ttX + 24} y={spendRowY}
                    fill="rgba(241, 245, 249, 0.7)" font-size="9" font-weight="500"
                    font-family="Inter, system-ui, sans-serif"
                    style="pointer-events: none;"
                >Spending</text>
                <text
                    x={ttX + ttW - 12} y={spendRowY}
                    text-anchor="end"
                    fill={SPENDING_COLOR} font-size="9.5" font-weight="700"
                    font-family="DM Mono, monospace"
                    style="pointer-events: none;"
                >{(void privacyKey, fmtCurrency(tt.spending))}</text>

                <!-- Net row with colored pill background -->
                {@const netRowY = spendRowY + 18}
                {@const netColor = tt.isSurplus ? NET_POSITIVE_COLOR : NET_NEGATIVE_COLOR}
                {@const netPillBg = tt.isSurplus ? 'rgba(52, 211, 153, 0.10)' : 'rgba(248, 113, 113, 0.10)'}
                {@const netPillBorder = tt.isSurplus ? 'rgba(52, 211, 153, 0.25)' : 'rgba(248, 113, 113, 0.25)'}
                <rect
                    x={ttX + 6} y={netRowY - 11}
                    width={ttW - 12} height={16}
                    rx="6" ry="6"
                    fill={netPillBg}
                    stroke={netPillBorder}
                    stroke-width="0.5"
                />
                <circle cx={ttX + 14} cy={netRowY - 3} r="3.5" fill={netColor} />
                <text
                    x={ttX + 24} y={netRowY}
                    fill="rgba(241, 245, 249, 0.85)" font-size="9" font-weight="600"
                    font-family="Inter, system-ui, sans-serif"
                    style="pointer-events: none;"
                >Net</text>
                <text
                    x={ttX + ttW - 12} y={netRowY}
                    text-anchor="end"
                    fill={netColor} font-size="9.5" font-weight="700"
                    font-family="DM Mono, monospace"
                    style="pointer-events: none;"
                >{(void privacyKey, $privacyMode ? '$\u2022\u2022\u2022\u2022\u2022' : ((tt.net >= 0 ? '+' : '') + formatCurrency(tt.net)))}</text>

                <!-- Separator before categories -->
                {@const catSepY = ttY + headerH + metricsH}
                <line
                    x1={ttX + 10} y1={catSepY}
                    x2={ttX + ttW - 10} y2={catSepY}
                    stroke="rgba(148, 163, 184, 0.18)" stroke-width="0.5"
                />

                <!-- Category breakdown -->
                {#if hasCats}
                    <text
                        x={ttX + 12} y={catSepY + 14}
                        fill="rgba(241, 245, 249, 0.5)" font-size="7.5" font-weight="700"
                        font-family="Inter, system-ui, sans-serif"
                        style="pointer-events: none; text-transform: uppercase; letter-spacing: 0.08em;"
                    >Top Categories</text>

                    {#each tt.topCategories as cat, ci}
                        {@const catY = catSepY + 14 + catHeaderH + ci * 18}
                        {@const catColor = CATEGORY_COLORS[cat.category] || '#94A3B8'}
                        <circle cx={ttX + 16} cy={catY - 3} r="3" fill={catColor} />
                        <text
                            x={ttX + 26} y={catY}
                            fill="rgba(241, 245, 249, 0.75)" font-size="8.5" font-weight="500"
                            font-family="Inter, system-ui, sans-serif"
                            style="pointer-events: none;"
                        >{cat.category}</text>
                        <text
                            x={ttX + ttW - 12} y={catY}
                            text-anchor="end"
                            fill="rgba(241, 245, 249, 0.55)" font-size="8" font-weight="600"
                            font-family="DM Mono, monospace"
                            style="pointer-events: none;"
                        >{(void privacyKey, $privacyMode ? '\u2022\u2022\u2022' : formatCurrency(cat.total))}</text>
                    {/each}
                {/if}
            {/if}

            <!-- Transparent hover zone -->
            <rect
                x={MARGIN.left} y={MARGIN.top}
                width={chart.plotW} height={chart.H - MARGIN.top - 20}
                fill="transparent"
                style="cursor: crosshair;"
            />
        </svg>

        <!-- Legend -->
        <div class="ivs-legend">
            <div class="ivs-legend-item">
                <span class="ivs-legend-dot" style="background: {INCOME_COLOR}; box-shadow: 0 0 6px {INCOME_COLOR};"></span>
                <span class="ivs-legend-label">Income</span>
            </div>
            <div class="ivs-legend-item">
                <span class="ivs-legend-dot" style="background: {SPENDING_COLOR}; box-shadow: 0 0 6px {SPENDING_COLOR};"></span>
                <span class="ivs-legend-label">Spending</span>
            </div>
            <div class="ivs-legend-item">
                <span class="ivs-legend-swatch" style="background: rgba(52, 211, 153, 0.25); border: 1px solid rgba(52, 211, 153, 0.4);"></span>
                <span class="ivs-legend-label">Surplus</span>
            </div>
            <div class="ivs-legend-item">
                <span class="ivs-legend-swatch" style="background: rgba(248, 113, 113, 0.25); border: 1px solid rgba(248, 113, 113, 0.4);"></span>
                <span class="ivs-legend-label">Deficit</span>
            </div>
            <div class="ivs-legend-item">
                <span class="ivs-legend-line" style="border-top: 2px dashed {AVG_TEXT_COLOR};"></span>
                <span class="ivs-legend-label">Avg Spending</span>
            </div>
        </div>
    {:else}
        <div class="ivs-empty">
            <p>Not enough data for Income vs Spending chart (need at least 2 months)</p>
        </div>
    {/if}
</div>

<style>
    .ivs-container {
        width: 100%;
        overflow: hidden;
        position: relative;
    }

    .ivs-svg {
        width: 100%;
        display: block;
        overflow: visible;
    }

    .ivs-dot {
        transition: r 0.15s ease, opacity 0.15s ease;
    }

    .ivs-value-label {
        transition: opacity 0.2s ease;
        pointer-events: none;
    }

    .ivs-gap-segment {
        animation: ivsGapFadeIn 0.6s ease-out both;
    }

    @keyframes ivsGapFadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }

    .ivs-crossover {
        animation: ivsCrossoverPulse 3s ease-in-out infinite;
    }

    @keyframes ivsCrossoverPulse {
        0%, 100% { opacity: 0.5; r: 5; }
        50% { opacity: 0.9; r: 7; }
    }

    /* Mini bars */
    .ivs-mini-bar {
        transition: opacity 0.15s ease, stroke 0.15s ease;
        animation: ivsMiniBarGrow 0.4s cubic-bezier(0.22, 1, 0.36, 1) both;
    }

    @keyframes ivsMiniBarGrow {
        from { opacity: 0; transform: scaleY(0); }
        to { opacity: 1; transform: scaleY(1); }
    }

    /* Layer 6: Vertical reference line — smooth fade */
    .ivs-ref-line {
        opacity: 0.30;
        transition: opacity 0.15s ease;
        animation: ivsRefLineFadeIn 0.15s ease both;
    }

    @keyframes ivsRefLineFadeIn {
        from { opacity: 0; }
        to { opacity: 0.30; }
    }

    /* Layer 6: Tooltip card — smooth fade-in */
    .ivs-tooltip-card {
        animation: ivsTooltipFadeIn 0.15s ease both;
    }

    .ivs-tooltip-glow {
        animation: ivsTooltipFadeIn 0.15s ease both;
    }

    @keyframes ivsTooltipFadeIn {
        from { opacity: 0; }
        to { opacity: 1; }
    }

    /* Legend */
    .ivs-legend {
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 1.25rem;
        margin-top: 0.5rem;
        padding: 0.375rem 0;
    }

    .ivs-legend-item {
        display: flex;
        align-items: center;
        gap: 0.375rem;
    }

    .ivs-legend-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        flex-shrink: 0;
    }

    .ivs-legend-swatch {
        width: 12px;
        height: 8px;
        border-radius: 2px;
        flex-shrink: 0;
    }

    .ivs-legend-line {
        width: 14px;
        height: 0;
        flex-shrink: 0;
    }

    .ivs-legend-label {
        font-size: 10px;
        font-weight: 600;
        color: var(--text-muted);
        font-family: 'Inter', system-ui, sans-serif;
    }

    .ivs-empty {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 200px;
        color: var(--text-muted);
        font-size: 0.8125rem;
    }
</style>
