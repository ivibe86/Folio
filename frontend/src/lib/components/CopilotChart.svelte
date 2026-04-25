<script>
    import { scaleLinear, scaleBand, scalePoint } from 'd3-scale';
    import { line, curveMonotoneX, arc, pie } from 'd3-shape';
    import { formatCurrency, formatCompact, CATEGORY_COLORS } from '$lib/utils.js';
    import { privacyMode, darkMode } from '$lib/stores.js';

    /** Chart spec: { type, title, series_name, labels, values, unit } */
    export let spec;

    $: labels = spec?.labels ?? [];
    $: values = spec?.values ?? [];
    $: type = spec?.type ?? 'line';
    $: title = spec?.title ?? '';
    $: seriesName = spec?.series_name ?? '';
    $: unit = spec?.unit ?? 'currency';

    const WIDTH = 520;
    const HEIGHT = 200;
    const MARGIN = { top: 14, right: 18, bottom: 26, left: 46 };
    $: innerW = WIDTH - MARGIN.left - MARGIN.right;
    $: innerH = HEIGHT - MARGIN.top - MARGIN.bottom;

    // Same accent colors used across Folio
    const ACCENT = '#6366F1';     // indigo primary accent
    const ACCENT_GLOW = '#818CF8';
    const POSITIVE = '#34D399';
    const NEGATIVE = '#F87171';
    const AXIS_COLOR = 'rgba(148, 163, 184, 0.55)';
    const GRID_COLOR = 'rgba(148, 163, 184, 0.18)';

    // Donut palette — cycles through category colors, falls back to indigos
    $: palette = Object.values(CATEGORY_COLORS || {});
    $: fallbackPalette = ['#6366F1', '#8B5CF6', '#EC4899', '#F59E0B', '#10B981', '#3B82F6', '#F472B6', '#34D399'];

    function fmt(v) {
        if ($privacyMode) return '$•••';
        if (unit === 'percent') return `${(v).toFixed(1)}%`;
        if (unit === 'number') return Number.isFinite(v) ? v.toLocaleString() : v;
        return formatCurrency(v);
    }

    function fmtAxis(v) {
        if ($privacyMode) return '$•••';
        if (unit === 'percent') return `${v.toFixed(0)}%`;
        if (unit === 'number') return formatCompact(v);
        return formatCompact(v);
    }

    // ─────────── LINE ───────────
    // scalePoint is the correct ordinal scale for line charts (no padding needed)
    $: lineX = scalePoint().domain(labels).range([0, innerW]).padding(0.5);
    $: maxV = Math.max(...values, 1);
    $: minV = Math.min(...values, 0);
    $: lineY = scaleLinear().domain([minV, maxV]).nice().range([innerH, 0]);
    $: linePath = (() => {
        try {
            const pts = values.map((d, i) => {
                const x = lineX(labels[i]);
                const y = lineY(d);
                return Number.isFinite(x) && Number.isFinite(y) ? [x, y] : null;
            }).filter(Boolean);
            if (pts.length < 2) return null;
            return 'M ' + pts.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' L ');
        } catch { return null; }
    })();
    $: ticks = lineY.ticks(4);

    // ─────────── BAR ───────────
    $: barX = scaleBand().domain(labels).range([0, innerW]).padding(0.25);
    $: barMax = Math.max(...values.map(v => Math.abs(v)), 1);
    $: barY = scaleLinear().domain([0, barMax]).nice().range([innerH, 0]);
    $: barTicks = barY.ticks(4);

    // ─────────── DONUT ───────────
    $: total = values.reduce((a, b) => a + b, 0);
    $: donutArcs = pie().sort(null).value(d => d)(values.map(v => Math.max(v, 0)));
    const DONUT_R = 78;
    const DONUT_R_INNER = 48;
    const donutArcGen = arc().innerRadius(DONUT_R_INNER).outerRadius(DONUT_R);
</script>

<div class="copilot-chart" class:dark={$darkMode}>
    {#if title}
        <div class="chart-title">{title}</div>
    {/if}

    {#if type === 'line'}
        <svg viewBox="0 0 {WIDTH} {HEIGHT}" preserveAspectRatio="xMidYMid meet">
            <defs>
                <linearGradient id="line-fill" x1="0" x2="0" y1="0" y2="1">
                    <stop offset="0%" stop-color={ACCENT} stop-opacity="0.28" />
                    <stop offset="100%" stop-color={ACCENT} stop-opacity="0" />
                </linearGradient>
            </defs>
            <g transform="translate({MARGIN.left}, {MARGIN.top})">
                <!-- y grid + labels -->
                {#each ticks as t}
                    <line x1="0" x2={innerW} y1={lineY(t)} y2={lineY(t)} stroke={GRID_COLOR} stroke-width="1" />
                    <text x="-8" y={lineY(t)} text-anchor="end" dominant-baseline="central" class="axis-label">{fmtAxis(t)}</text>
                {/each}
                {#if linePath}
                <!-- area fill -->
                <path d="{linePath} L {(lineX(labels[labels.length - 1]) ?? innerW)},{innerH} L {(lineX(labels[0]) ?? 0)},{innerH} Z" fill="url(#line-fill)" />
                <!-- line -->
                <path d={linePath} fill="none" stroke={ACCENT} stroke-width="2.25" stroke-linecap="round" stroke-linejoin="round" />
                <!-- points (only if few enough to not crowd) -->
                {#if labels.length <= 20}
                    {#each values as v, i}
                        {@const cx = lineX(labels[i])}
                        {#if Number.isFinite(cx)}
                            <circle cx={cx} cy={lineY(v)} r="3" fill={ACCENT} stroke="var(--card-bg, #fff)" stroke-width="1.5" />
                        {/if}
                    {/each}
                {/if}
                {/if}
                <!-- x labels (every Nth if many) -->
                {#each labels as l, i}
                    {#if labels.length <= 8 || i % Math.ceil(labels.length / 8) === 0}
                        {@const lx = lineX(l)}
                        {#if Number.isFinite(lx)}
                            <text x={lx} y={innerH + 16} text-anchor="middle" class="axis-label">
                                {l.length > 7 ? l.slice(2) : l}
                            </text>
                        {/if}
                    {/if}
                {/each}
            </g>
        </svg>
        {#if seriesName}
            <div class="chart-legend">
                <span class="legend-dot" style="background: {ACCENT}"></span>
                {seriesName}
            </div>
        {/if}

    {:else if type === 'bar'}
        <svg viewBox="0 0 {WIDTH} {HEIGHT}" preserveAspectRatio="xMidYMid meet">
            <g transform="translate({MARGIN.left}, {MARGIN.top})">
                {#each barTicks as t}
                    <line x1="0" x2={innerW} y1={barY(t)} y2={barY(t)} stroke={GRID_COLOR} stroke-width="1" />
                    <text x="-8" y={barY(t)} text-anchor="end" dominant-baseline="central" class="axis-label">{fmtAxis(t)}</text>
                {/each}
                {#each values as v, i}
                    {@const absv = Math.abs(v)}
                    {@const x = barX(labels[i]) ?? 0}
                    {@const h = innerH - barY(absv)}
                    <rect x={x} y={barY(absv)} width={barX.bandwidth()} height={h} rx="3" fill={v < 0 ? NEGATIVE : ACCENT} opacity="0.92" />
                    <text x={x + barX.bandwidth() / 2} y={barY(absv) - 4} text-anchor="middle" class="bar-value">{fmt(v)}</text>
                {/each}
                {#each labels as l, i}
                    {#if labels.length <= 10 || i % Math.ceil(labels.length / 10) === 0}
                        <text x={(barX(l) ?? 0) + barX.bandwidth() / 2} y={innerH + 16} text-anchor="middle" class="axis-label">
                            {l.length > 14 ? l.slice(0, 13) + '…' : l}
                        </text>
                    {/if}
                {/each}
            </g>
        </svg>

    {:else if type === 'donut'}
        <div class="donut-wrap">
            <svg viewBox="-100 -95 200 190" width="200" height="200">
                {#each donutArcs as a, i}
                    <path d={donutArcGen(a)} fill={palette[i % palette.length] || fallbackPalette[i % fallbackPalette.length]} opacity="0.92" />
                {/each}
                <text x="0" y="-2" text-anchor="middle" class="donut-center-label">{fmt(total)}</text>
                <text x="0" y="14" text-anchor="middle" class="donut-center-sub">{seriesName || 'total'}</text>
            </svg>
            <div class="donut-legend">
                {#each labels as l, i}
                    <div class="donut-item">
                        <span class="legend-dot" style="background: {palette[i % palette.length] || fallbackPalette[i % fallbackPalette.length]}"></span>
                        <span class="donut-label">{l}</span>
                        <span class="donut-value">{fmt(values[i])}</span>
                    </div>
                {/each}
            </div>
        </div>
    {/if}
</div>

<style>
    .copilot-chart {
        margin-top: 8px;
        padding: 12px 14px 10px;
        border-radius: 14px;
        background: var(--card-bg, #fff);
        border: 1px solid var(--card-border, rgba(148, 163, 184, 0.22));
    }
    .chart-title {
        font-size: 12px;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 8px;
        letter-spacing: 0.01em;
    }
    .axis-label {
        font-size: 10px;
        fill: var(--text-muted);
        font-weight: 500;
    }
    .bar-value {
        font-size: 9.5px;
        fill: var(--text-secondary);
        font-weight: 600;
    }
    .chart-legend {
        margin-top: 6px;
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 11px;
        color: var(--text-secondary);
    }
    .legend-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        display: inline-block;
        flex-shrink: 0;
    }
    .donut-wrap {
        display: flex;
        align-items: center;
        gap: 18px;
        padding: 4px 0;
    }
    .donut-center-label {
        font-size: 14px;
        font-weight: 700;
        fill: var(--text-primary);
    }
    .donut-center-sub {
        font-size: 9.5px;
        fill: var(--text-muted);
        font-weight: 500;
    }
    .donut-legend {
        display: flex;
        flex-direction: column;
        gap: 4px;
        font-size: 11px;
        flex: 1;
        min-width: 0;
    }
    .donut-item {
        display: flex;
        align-items: center;
        gap: 6px;
        color: var(--text-secondary);
    }
    .donut-label {
        flex: 1;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .donut-value {
        color: var(--text-primary);
        font-weight: 600;
        flex-shrink: 0;
    }
</style>
