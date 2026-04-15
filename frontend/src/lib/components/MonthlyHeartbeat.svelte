<script>
    import { onMount } from 'svelte';
    import { scaleLinear, scaleBand } from 'd3-scale';
    import { area, curveMonotoneX } from 'd3-shape';
    import { formatCurrency, formatCompact } from '$lib/utils.js';
    import { privacyMode, darkMode } from '$lib/stores.js';

    export let monthlyData = [];
    export let months = 12;

    let container;
    let width = 800;
    let height = 260;
    const margin = { top: 40, right: 60, bottom: 30, left: 20 };
    const ASPECT_RATIO = 3;

    let mounted = false;
    let animateIn = false;
    let hoveredIdx = null;
    let tooltip = { show: false, x: 0, y: 0, data: null };

    /**
     * Format a month string like "2025-06" into "Jun '25".
     * Falls back gracefully if the format is unexpected.
     */
    function formatMonthShort(monthStr) {
        if (!monthStr) return '';
        try {
            const [year, mon] = monthStr.split('-');
            const date = new Date(+year, (+mon) - 1, 1);
            const abbr = date.toLocaleString('en-US', { month: 'short' });
            return `${abbr} '${year.slice(2)}`;
        } catch {
            return monthStr;
        }
    }

    // Process data: take last N months
    $: data = monthlyData
        .slice(-months)
        .map((d, i) => ({
            ...d,
            index: i,
            isCurrent: i === Math.min(monthlyData.length, months) - 1,
            isDeficit: d.spending > d.income
        }));

    $: innerWidth = width - margin.left - margin.right;
    $: innerHeight = height - margin.top - margin.bottom;

    // Scales
    $: xScale = scaleBand()
        .domain(data.map(d => d.month))
        .range([0, innerWidth])
        .padding(0.4);

    $: maxY = Math.max(...data.map(d => Math.max(d.income, d.spending)), 1000) * 1.15;
    $: yScale = scaleLinear()
        .domain([0, maxY])
        .range([innerHeight, 0]);

    // Average spending
    $: avgSpending = data.reduce((acc, d) => acc + d.spending, 0) / (data.length || 1);

    // Income Area Path
    $: incomeAreaGenerator = area()
        .x(d => xScale(d.month) + xScale.bandwidth() / 2)
        .y0(innerHeight)
        .y1(d => yScale(d.income))
        .curve(curveMonotoneX);

    $: incomeLinePath = incomeAreaGenerator.lineY1()(data);
    $: incomeAreaPath = incomeAreaGenerator(data);

    // Per-bar computed values for the deficit split
    $: barMetrics = data.map(d => {
        const barTop = yScale(d.spending);
        const barBottom = innerHeight;
        const incomeY = yScale(d.income);
        const fullHeight = barBottom - barTop;

        if (d.isDeficit) {
            // Split: surplus portion (income line to bottom) + deficit portion (bar top to income line)
            const surplusHeight = barBottom - incomeY;
            const deficitHeight = incomeY - barTop;
            return {
                surplusY: incomeY,
                surplusHeight,
                deficitY: barTop,
                deficitHeight,
                fullHeight,
                fullY: barTop
            };
        }
        return {
            surplusY: barTop,
            surplusHeight: fullHeight,
            deficitY: 0,
            deficitHeight: 0,
            fullHeight,
            fullY: barTop
        };
    });

    // Privacy-aware formatting
    $: fmt = (val) => $privacyMode ? '•••' : formatCompact(val);
    $: fmtFull = (val) => $privacyMode ? '••••' : formatCurrency(val);

    function handleMouseMove(e, d, i) {
        hoveredIdx = i;
        const rect = container.getBoundingClientRect();
        tooltip = {
            show: true,
            x: xScale(d.month) + xScale.bandwidth() / 2 + margin.left,
            y: yScale(Math.max(d.income, d.spending)) + margin.top - 14,
            data: d,
            prevSpending: i > 0 ? data[i - 1].spending : null
        };
    }

    function handleMouseLeave() {
        hoveredIdx = null;
        tooltip = { ...tooltip, show: false };
    }

    onMount(() => {
        mounted = true;
        const observer = new ResizeObserver(entries => {
            if (entries[0]) {
                width = entries[0].contentRect.width;
                height = Math.max(200, Math.round(width / ASPECT_RATIO));
            }
        });
        observer.observe(container);
        setTimeout(() => (animateIn = true), 100);
        return () => observer.disconnect();
    });
</script>

<div class="heartbeat-wrapper" bind:this={container}>
    <svg {width} {height} class="heartbeat-svg" on:mouseleave={handleMouseLeave}
         role="img" aria-label="Monthly spending vs income chart">
        <defs>
            <!-- Surplus Gradient (Blue/Teal) -->
            <linearGradient id="hb-grad-surplus" x1="0" y1="1" x2="0" y2="0">
                <stop offset="0%" stop-color="var(--hb-surplus-base, #1e3a8a)" />
                <stop offset="100%" stop-color="var(--hb-surplus-top, #2dd4bf)" />
            </linearGradient>

            <!-- Deficit Gradient (Crimson/Coral) -->
            <linearGradient id="hb-grad-deficit" x1="0" y1="1" x2="0" y2="0">
                <stop offset="0%" stop-color="var(--hb-deficit-base, #991b1b)" />
                <stop offset="100%" stop-color="var(--hb-deficit-top, #fb7185)" />
            </linearGradient>

            <!-- Income Area Fill -->
            <linearGradient id="hb-income-grad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="var(--hb-income-color, #2dd4bf)" stop-opacity="0.12" />
                <stop offset="100%" stop-color="var(--hb-income-color, #2dd4bf)" stop-opacity="0.01" />
            </linearGradient>

            <!-- Glow Filters -->
            <filter id="hb-glow-surplus" x="-50%" y="-20%" width="200%" height="140%">
                <feGaussianBlur stdDeviation="4" result="blur" />
                <feComposite in="SourceGraphic" in2="blur" operator="over" />
            </filter>
            <filter id="hb-glow-deficit" x="-50%" y="-20%" width="200%" height="140%">
                <feGaussianBlur stdDeviation="6" result="blur" />
                <feComposite in="SourceGraphic" in2="blur" operator="over" />
            </filter>

            <!-- Clip paths for deficit bar splitting -->
            {#each data as d, i}
                {#if d.isDeficit && barMetrics[i]}
                    <clipPath id="hb-clip-deficit-{i}">
                        <rect
                            x={xScale(d.month) - 1}
                            y={barMetrics[i].deficitY}
                            width={xScale.bandwidth() + 2}
                            height={barMetrics[i].deficitHeight}
                        />
                    </clipPath>
                    <clipPath id="hb-clip-surplus-{i}">
                        <rect
                            x={xScale(d.month) - 1}
                            y={barMetrics[i].surplusY}
                            width={xScale.bandwidth() + 2}
                            height={barMetrics[i].surplusHeight}
                        />
                    </clipPath>
                {/if}
            {/each}
        </defs>

        <g transform="translate({margin.left}, {margin.top})">
            <!-- Income Area (Behind everything) -->
            {#if data.length > 1}
                <path
                    d={incomeAreaPath}
                    fill="url(#hb-income-grad)"
                    class="hb-income-area"
                    style="opacity: {animateIn ? 1 : 0}"
                />
                <path
                    d={incomeLinePath}
                    fill="none"
                    stroke="var(--hb-income-color, #2dd4bf)"
                    stroke-width="2"
                    stroke-opacity="0.6"
                    class="hb-income-line"
                    style="opacity: {animateIn ? 1 : 0}"
                />
            {/if}

            <!-- Average Line -->
            {#if animateIn}
                <line
                    x1="0" y1={yScale(avgSpending)}
                    x2={innerWidth} y2={yScale(avgSpending)}
                    class="hb-avg-line"
                />
                <text
                    x={innerWidth + 6} y={yScale(avgSpending)}
                    class="hb-avg-label"
                    dy="0.35em"
                >
                    Avg {fmt(avgSpending)}
                </text>
            {/if}

            <!-- Spending Bars -->
            {#each data as d, i}
                {@const metrics = barMetrics[i]}
                <g
                    class="hb-bar-group"
                    class:hb-dimmed={hoveredIdx !== null && hoveredIdx !== i}
                    class:hb-hovered={hoveredIdx === i}
                    on:mousemove={(e) => handleMouseMove(e, d, i)}
                    on:focus={() => {}}
                    role="group"
                    aria-label="{formatMonthShort(d.month)}: spending {formatCompact(d.spending)}, income {formatCompact(d.income)}"
                >
                    {#if d.isDeficit && metrics}
                        <!-- DEFICIT BAR: Two layers split at income line -->
                        <!-- Surplus portion (below income line) -->
                        <rect
                            x={xScale(d.month)}
                            y={animateIn ? metrics.fullY : innerHeight}
                            width={xScale.bandwidth()}
                            height={animateIn ? metrics.fullHeight : 0}
                            rx="4"
                            fill="url(#hb-grad-surplus)"
                            clip-path="url(#hb-clip-surplus-{i})"
                            class="hb-spending-bar"
                            style="transition-delay: {i * 50}ms"
                        />
                        <!-- Deficit portion (above income line) -->
                        <rect
                            x={xScale(d.month)}
                            y={animateIn ? metrics.fullY : innerHeight}
                            width={xScale.bandwidth()}
                            height={animateIn ? metrics.fullHeight : 0}
                            rx="4"
                            fill="url(#hb-grad-deficit)"
                            filter="url(#hb-glow-deficit)"
                            clip-path="url(#hb-clip-deficit-{i})"
                            class="hb-spending-bar"
                            style="transition-delay: {i * 50}ms"
                        />
                        <!-- Deficit label -->
                        {#if animateIn}
                            <text
                                x={xScale(d.month) + xScale.bandwidth() + 4}
                                y={metrics.deficitY + metrics.deficitHeight / 2}
                                class="hb-deficit-label"
                                dy="0.35em"
                                style="transition-delay: {(data.length * 50) + 400}ms"
                            >
                                −{fmt(d.spending - d.income)}
                            </text>
                        {/if}
                    {:else}
                        <!-- SURPLUS BAR: Single layer -->
                        <rect
                            x={xScale(d.month)}
                            y={animateIn ? yScale(d.spending) : innerHeight}
                            width={xScale.bandwidth()}
                            height={animateIn ? innerHeight - yScale(d.spending) : 0}
                            rx="4"
                            fill="url(#hb-grad-surplus)"
                            filter="url(#hb-glow-surplus)"
                            class="hb-spending-bar"
                            style="transition-delay: {i * 50}ms"
                        />
                    {/if}

                    <!-- Current month pulse overlay -->
                    {#if d.isCurrent}
                        <rect
                            x={xScale(d.month)}
                            y={animateIn ? yScale(d.spending) : innerHeight}
                            width={xScale.bandwidth()}
                            height={animateIn ? innerHeight - yScale(d.spending) : 0}
                            rx="4"
                            fill="transparent"
                            class="hb-pulse"
                            style="--pulse-color: {d.isDeficit ? 'var(--hb-deficit-top, #fb7185)' : 'var(--hb-surplus-top, #2dd4bf)'}; transition-delay: {i * 50}ms"
                        />
                    {/if}

                    <!-- Value Label Above Bar -->
                    <text
                        x={xScale(d.month) + xScale.bandwidth() / 2}
                        y={yScale(d.spending) - 8}
                        class="hb-bar-label"
                        class:hb-current={d.isCurrent}
                        class:hb-visible={animateIn}
                        style="transition-delay: {(data.length * 50) + 200}ms"
                    >
                        {fmt(d.spending)}
                    </text>

                    <!-- X-Axis Label -->
                    <text
                        x={xScale(d.month) + xScale.bandwidth() / 2}
                        y={innerHeight + 20}
                        class="hb-x-label"
                    >
                        {formatMonthShort(d.month)}
                    </text>

                    <!-- Hover Reference Line (bar top → income line) -->
                    {#if hoveredIdx === i}
                        <line
                            x1={xScale(d.month) + xScale.bandwidth() / 2}
                            y1={yScale(d.spending)}
                            x2={xScale(d.month) + xScale.bandwidth() / 2}
                            y2={yScale(d.income)}
                            class="hb-ref-line"
                        />
                    {/if}
                </g>
            {/each}
        </g>
    </svg>

    <!-- Tooltip -->
    {#if tooltip.show && tooltip.data}
        {@const net = tooltip.data.income - tooltip.data.spending}
        <div
            class="hb-tooltip glass"
            style="left: {tooltip.x}px; top: {tooltip.y}px"
        >
            <p class="hb-tt-month">{formatMonthShort(tooltip.data.month)}</p>
            <div class="hb-tt-row">
                <span>Income</span>
                <span class="hb-tt-val hb-tt-income">{fmtFull(tooltip.data.income)}</span>
            </div>
            <div class="hb-tt-row">
                <span>Spending</span>
                <span class="hb-tt-val hb-tt-spending">{fmtFull(tooltip.data.spending)}</span>
            </div>
            <div class="hb-tt-net" class:neg={net < 0}>
                Net: {net >= 0 ? '+' : ''}{fmtFull(net)}
            </div>
            {#if tooltip.prevSpending != null}
                {@const diff = tooltip.data.spending - tooltip.prevSpending}
                <p class="hb-tt-delta">
                    {diff >= 0 ? '▲' : '▼'} {fmtFull(Math.abs(diff))}
                    {diff >= 0 ? 'more' : 'less'} than prev month
                </p>
            {/if}
        </div>
    {/if}

    <!-- Legend -->
    <div class="hb-legend">
        <div class="hb-legend-item">
            <span class="hb-legend-dot hb-legend-spending"></span> Spending
        </div>
        <div class="hb-legend-item">
            <span class="hb-legend-line hb-legend-income"></span> Income
        </div>
        <div class="hb-legend-item">
            <span class="hb-legend-line hb-legend-avg"></span> Average
        </div>
    </div>
</div>

<style>
    /* ─── Container ─────────────────────────────────── */
    .heartbeat-wrapper {
        width: 100%;
        position: relative;
        margin: 2rem 0;
    }

    .heartbeat-svg {
        overflow: visible;
        display: block;
    }

    .heartbeat-svg text {
        user-select: none;
    }

    /* ─── Income Area ───────────────────────────────── */
    .hb-income-area {
        transition: opacity 0.5s ease-out;
    }

    .hb-income-line {
        transition: opacity 0.5s ease-out;
    }

    /* ─── Spending Bars ─────────────────────────────── */
    .hb-spending-bar {
        transition:
            y 0.6s cubic-bezier(0.22, 1, 0.36, 1),
            height 0.6s cubic-bezier(0.22, 1, 0.36, 1),
            opacity 0.2s ease;
    }

    .hb-bar-group {
        transition: opacity 0.15s ease;
    }

    .hb-bar-group.hb-dimmed {
        opacity: 0.35;
    }

    .hb-bar-group.hb-hovered .hb-spending-bar {
        filter: brightness(1.2);
    }

    /* ─── Pulse (current month) ─────────────────────── */
    .hb-pulse {
        pointer-events: none;
        animation: hb-pulse-glow 2s infinite ease-in-out;
        transition:
            y 0.6s cubic-bezier(0.22, 1, 0.36, 1),
            height 0.6s cubic-bezier(0.22, 1, 0.36, 1);
    }

    @keyframes hb-pulse-glow {
        0%, 100% {
            filter: drop-shadow(0 0 3px var(--pulse-color));
        }
        50% {
            filter: drop-shadow(0 0 10px var(--pulse-color)) drop-shadow(0 0 20px var(--pulse-color));
        }
    }

    /* ─── Labels ────────────────────────────────────── */
    .hb-bar-label {
        font-size: 10px;
        font-family: var(--font-mono, 'SF Mono', monospace);
        fill: var(--text-primary, #e2e8f0);
        text-anchor: middle;
        opacity: 0;
        transition: opacity 0.4s ease-out;
    }

    .hb-bar-label.hb-visible {
        opacity: 0.55;
    }

    .hb-bar-label.hb-current {
        opacity: 1 !important;
        font-weight: 700;
    }

    .hb-bar-group:hover .hb-bar-label,
    .hb-bar-group.hb-hovered .hb-bar-label {
        opacity: 1;
    }

    .hb-x-label {
        font-size: 9px;
        fill: var(--text-muted, #94a3b8);
        text-anchor: middle;
        font-weight: 500;
        font-family: var(--font-body, system-ui);
    }

    .hb-deficit-label {
        font-size: 9px;
        font-family: var(--font-mono, 'SF Mono', monospace);
        fill: var(--hb-deficit-top, #fb7185);
        opacity: 0;
        transition: opacity 0.4s ease-out 0.8s;
    }

    .hb-bar-label.hb-visible ~ .hb-deficit-label,
    .heartbeat-wrapper .hb-deficit-label {
        opacity: 0.8;
    }

    /* ─── Average Line ──────────────────────────────── */
    .hb-avg-line {
        stroke: var(--text-primary, white);
        stroke-width: 1;
        stroke-dasharray: 6, 4;
        opacity: 0;
        animation: hb-draw-line 0.8s forwards 0.9s;
    }

    @keyframes hb-draw-line {
        to { opacity: 0.25; }
    }

    .hb-avg-label {
        font-size: 9px;
        fill: var(--text-muted, #94a3b8);
        opacity: 0.5;
        font-family: var(--font-mono, 'SF Mono', monospace);
    }

    /* ─── Reference Line (hover) ────────────────────── */
    .hb-ref-line {
        stroke: var(--text-primary, white);
        stroke-width: 1;
        stroke-dasharray: 2, 2;
        opacity: 0.35;
    }

    /* ─── Tooltip ───────────────────────────────────── */
    .hb-tooltip {
        position: absolute;
        pointer-events: none;
        padding: 10px 12px;
        border-radius: 10px;
        min-width: 170px;
        transform: translate(-50%, -100%);
        z-index: 100;
        font-size: 11px;
        color: var(--text-primary, #e2e8f0);
        line-height: 1.4;
    }

    .hb-tt-month {
        font-weight: 700;
        margin-bottom: 5px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
        padding-bottom: 5px;
        font-size: 12px;
    }

    .hb-tt-row {
        display: flex;
        justify-content: space-between;
        margin-bottom: 2px;
    }

    .hb-tt-val {
        font-family: var(--font-mono, 'SF Mono', monospace);
        font-weight: 600;
    }

    .hb-tt-income { color: var(--hb-income-color, #2dd4bf); }
    .hb-tt-spending { color: var(--text-primary, #e2e8f0); }

    .hb-tt-net {
        margin-top: 5px;
        padding-top: 4px;
        border-top: 1px solid rgba(255, 255, 255, 0.08);
        font-weight: 700;
        font-family: var(--font-mono, 'SF Mono', monospace);
        color: var(--positive, #34d399);
    }

    .hb-tt-net.neg {
        color: var(--negative, #fb7185);
    }

    .hb-tt-delta {
        font-size: 9px;
        opacity: 0.6;
        margin-top: 4px;
    }

    /* ─── Legend ─────────────────────────────────────── */
    .hb-legend {
        display: flex;
        justify-content: center;
        gap: 20px;
        margin-top: 12px;
        font-size: 10px;
        color: var(--text-muted, #94a3b8);
    }

    .hb-legend-item {
        display: flex;
        align-items: center;
        gap: 6px;
    }

    .hb-legend-dot {
        width: 8px;
        height: 8px;
        border-radius: 2px;
    }

    .hb-legend-spending {
        background: linear-gradient(to top, var(--hb-surplus-base, #1e3a8a), var(--hb-surplus-top, #2dd4bf));
    }

    .hb-legend-line {
        width: 14px;
        height: 2px;
    }

    .hb-legend-income {
        background: var(--hb-income-color, #2dd4bf);
        opacity: 0.6;
    }

    .hb-legend-avg {
        border-top: 1.5px dashed var(--text-primary, white);
        opacity: 0.25;
        height: 0;
    }
</style>