<script>
import { onMount, createEventDispatcher } from 'svelte';
import { sankey as d3Sankey, sankeyJustify } from 'd3-sankey';
import { formatCurrency, formatCompact, CATEGORY_COLORS } from '$lib/utils.js';
import { privacyMode } from '$lib/stores.js';

export let income = 0;
export let expenses = 0;
export let savingsTransfer = 0;
export let personalTransfer = 0;
export let categories = [];
export let selectedCategory = null;
export let height = 400;
export let autoHeight = true;

const dispatch = createEventDispatcher();

/* ── Theme-aware filter intensities ── */
/* Detect dark mode reactively for SVG filter tuning */
let isDarkMode = false;

function checkDarkMode() {
    if (typeof document !== 'undefined') {
        isDarkMode = document.documentElement.classList.contains('dark');
    }
}

let containerEl;
let svgWidth = 800;
let computedHeight = height;
let nodes = [];
let links = [];
let hoveredLink = null;
let tooltip = { show: false, x: 0, y: 0, text: '', subtext: '' };
let mounted = false;
let animateIn = false;

let realValues = {};

const PAD = 24;
const NODE_WIDTH = 22;
const MARGIN = { top: 16, right: 20, bottom: 16, left: 20 };
const MIN_NODE_HEIGHT = 28;

/* ── Luminous Flow Color Map ── */
/* Use CSS variables for theme-aware colors, with fallbacks */
const FLOW_COLORS = {
    income:            { color: 'var(--flow-income)',       glow: 'var(--flow-income-glow)' },
    expenses:          { color: 'var(--flow-expenses)',     glow: 'var(--flow-expenses-glow)' },
    savings_transfer:  { color: 'var(--flow-savings)',      glow: 'var(--flow-savings-glow)' },
    personal_transfer: { color: 'var(--flow-transfer)',     glow: 'var(--flow-transfer-glow)' },
    from_balance:      { color: 'var(--flow-from-balance)', glow: 'var(--flow-from-balance-glow)' },
    to_balance:        { color: 'var(--flow-to-balance)',   glow: 'var(--flow-to-balance-glow)' },
    _default:          { color: 'var(--flow-default)',      glow: 'var(--flow-default-glow)' },
};


/* Resolve CSS custom property to computed color value */
function resolveColor(cssVar) {
    if (!cssVar.startsWith('var(')) return cssVar;
    const match = cssVar.match(/var\(([^,)]+)(?:,\s*([^)]+))?\)/);
    if (!match) return cssVar;
    if (typeof getComputedStyle === 'undefined') return match[2] || cssVar;
    const resolved = getComputedStyle(document.documentElement).getPropertyValue(match[1]).trim();
    return resolved || match[2] || cssVar;
}

function getFlowColor(nodeId) {
    return FLOW_COLORS[nodeId] || FLOW_COLORS._default;
}

    // ── Memoized graph data: only rebuild when actual data inputs change ──
    // We create a stable key from the data inputs. If the key hasn't changed,
    // we skip the expensive buildGraph + layoutSankey entirely.
    let prevGraphKey = '';
    let graphData = null;

    $: {
        // Build a lightweight key from the data that actually affects layout.
        // selectedCategory and theme changes do NOT affect layout â only visual props.
        // Privacy mode is included to force label re-render (layout stays the same).
        const key = `${income}|${expenses}|${savingsTransfer}|${personalTransfer}|${(categories || []).map(c => c.category + ':' + c.total).join(',')}|priv:${$privacyMode}`;

        if (key !== prevGraphKey) {
            prevGraphKey = key;
            graphData = buildGraph(income, expenses, savingsTransfer, personalTransfer, categories);
            if (containerEl && graphData && mounted) {
                layoutSankey(graphData);
            }
        }
    }

function buildGraph(inc, exp, savTotal, ptTotal, cats) {
    if ((!cats || cats.length === 0) && savTotal === 0 && ptTotal === 0) return null;

    const top = (cats || []).slice(0, 10);
    const categoryTotal = top.reduce((s, c) => s + (c.total || 0), 0);

    const totalOutflow = categoryTotal + savTotal + ptTotal;
    if (totalOutflow === 0) return null;

    const realIncome = inc || 0;

    // ── Balance-as-Reservoir model ──
    // When income doesn't cover outflow, the shortfall comes "from balance".
    // When income exceeds outflow, the surplus goes "to balance" (savings pool).
    const shortfall = Math.max(totalOutflow - realIncome, 0);
    const surplus = Math.max(realIncome - totalOutflow, 0);

    // Layout income = the total left-side value that feeds into outflows.
    // This must equal totalOutflow so the Sankey balances visually.
    const layoutIncome = totalOutflow;

    realValues = {
        'income': realIncome,
        'expenses': categoryTotal,
        'savings_transfer': savTotal,
        'personal_transfer': ptTotal,
        'from_balance': shortfall,
        'to_balance': surplus
    };
    top.forEach(c => { realValues[c.category] = c.total; });

    const nodeList = [];

    // ── Left-side source nodes ──
    // Always show income node if there IS income, even if it doesn't cover everything
    if (realIncome > 0) {
        nodeList.push({ id: 'income', label: 'Income', color: FLOW_COLORS.income.color });
    }

    // Show "From Balance" node when drawing down reserves
    if (shortfall > 0.01) {
        nodeList.push({ id: 'from_balance', label: 'From Balance', color: FLOW_COLORS.from_balance.color });
    }

    // If no income at all, we still need a source — "From Balance" handles it above.
    // If there IS income but no shortfall, income alone is the source.

    // ── Middle/right-side nodes ──
    if (top.length > 0) {
        nodeList.push({ id: 'expenses', label: 'Expenses', color: FLOW_COLORS.expenses.color });
    }

    if (savTotal > 0) {
        nodeList.push({ id: 'savings_transfer', label: 'Savings', color: FLOW_COLORS.savings_transfer.color });
    }

    if (ptTotal > 0) {
        nodeList.push({ id: 'personal_transfer', label: 'Transfers', color: FLOW_COLORS.personal_transfer.color });
    }

    // Show "To Balance" node when income exceeds all outflows (surplus)
    if (surplus > 0.01) {
        nodeList.push({ id: 'to_balance', label: 'To Balance', color: FLOW_COLORS.to_balance.color });
    }

    top.forEach(c => {
        const catId = c.category.trim();
        nodeList.push({
            id: catId,
            label: c.category,
            color: CATEGORY_COLORS[c.category] || '#64748B'
        });
    });

    const idToIndex = {};
    nodeList.forEach((n, i) => { idToIndex[n.id] = i; });

    const linkList = [];

    // ── Source → Expenses links ──
    // Income feeds into expenses (up to the lesser of income or categoryTotal)
    if (realIncome > 0 && top.length > 0 && categoryTotal > 0) {
        const incomeToExpenses = Math.min(realIncome, categoryTotal);
        linkList.push({
            source: idToIndex['income'],
            target: idToIndex['expenses'],
            value: incomeToExpenses,
            id: 'income-expenses'
        });
    }

    // From Balance feeds the remainder of expenses
    if (shortfall > 0.01 && top.length > 0 && categoryTotal > 0) {
        // How much of categoryTotal is NOT covered by income?
        const incomeToExpenses = Math.min(realIncome, categoryTotal);
        const balanceToExpenses = categoryTotal - incomeToExpenses;
        if (balanceToExpenses > 0.01) {
            linkList.push({
                source: idToIndex['from_balance'],
                target: idToIndex['expenses'],
                value: balanceToExpenses,
                id: 'from_balance-expenses'
            });
        }
    }

    // ── Source → Savings Transfer links ──
    if (savTotal > 0) {
        // Allocate: income covers expenses first, then savings, then personal
        const incomeAfterExpenses = Math.max(realIncome - categoryTotal, 0);
        const incomeToSavings = Math.min(incomeAfterExpenses, savTotal);

        if (incomeToSavings > 0.01 && idToIndex['income'] !== undefined) {
            linkList.push({
                source: idToIndex['income'],
                target: idToIndex['savings_transfer'],
                value: incomeToSavings,
                id: 'income-savings_transfer'
            });
        }

        const balanceToSavings = savTotal - incomeToSavings;
        if (balanceToSavings > 0.01 && idToIndex['from_balance'] !== undefined) {
            linkList.push({
                source: idToIndex['from_balance'],
                target: idToIndex['savings_transfer'],
                value: balanceToSavings,
                id: 'from_balance-savings_transfer'
            });
        }
    }

    // ── Source → Personal Transfer links ──
    if (ptTotal > 0) {
        const incomeAfterExpAndSav = Math.max(realIncome - categoryTotal - savTotal, 0);
        const incomeToPersonal = Math.min(incomeAfterExpAndSav, ptTotal);

        if (incomeToPersonal > 0.01 && idToIndex['income'] !== undefined) {
            linkList.push({
                source: idToIndex['income'],
                target: idToIndex['personal_transfer'],
                value: incomeToPersonal,
                id: 'income-personal_transfer'
            });
        }

        const balanceToPersonal = ptTotal - incomeToPersonal;
        if (balanceToPersonal > 0.01 && idToIndex['from_balance'] !== undefined) {
            linkList.push({
                source: idToIndex['from_balance'],
                target: idToIndex['personal_transfer'],
                value: balanceToPersonal,
                id: 'from_balance-personal_transfer'
            });
        }
    }

    // ── Income → To Balance (surplus) ──
    if (surplus > 0.01 && idToIndex['income'] !== undefined && idToIndex['to_balance'] !== undefined) {
        linkList.push({
            source: idToIndex['income'],
            target: idToIndex['to_balance'],
            value: surplus,
            id: 'income-to_balance'
        });
    }

    // ── Expenses → Category leaf nodes ──
    top.forEach(c => {
        const catId = c.category.trim();
        if (c.total > 0 && idToIndex['expenses'] !== undefined && idToIndex[catId] !== undefined) {
            linkList.push({
                source: idToIndex['expenses'],
                target: idToIndex[catId],
                value: c.total,
                id: `expenses-${catId}`
            });
        } else if (c.total > 0 && idToIndex[catId] === undefined) {
            console.warn(`[Sankey] Node missing for category "${c.category}" (id: "${catId}") — skipping link`);
        }
    });

    return { nodes: nodeList, links: linkList };
}

function layoutSankey(data) {
    if (!data || !containerEl) return;
    const rect = containerEl.getBoundingClientRect();
    svgWidth = rect.width || 800;

    if (autoHeight) {
        const visibleNodes = data.nodes.filter(n => !n.isHidden).length;
        const catNodes = visibleNodes - 1;
        const needed = MARGIN.top + MARGIN.bottom + Math.max(catNodes, 3) * (MIN_NODE_HEIGHT + PAD);
        computedHeight = Math.max(height, Math.min(needed, 650));
    } else {
        computedHeight = height;
    }

    const sankeyGen = d3Sankey()
        .nodeId(d => d.index)
        .nodeWidth(NODE_WIDTH)
        .nodePadding(PAD)
        .nodeAlign(sankeyJustify)
        .extent([[MARGIN.left, MARGIN.top], [svgWidth - MARGIN.right, computedHeight - MARGIN.bottom]]);

    const graph = sankeyGen({
        nodes: data.nodes.map(d => ({ ...d })),
        links: data.links.map(d => ({ ...d }))
    });

    nodes = graph.nodes;
    links = graph.links;
}

function getNodeDisplayValue(node) {
    if (node.isHidden) return 0;
    return realValues[node.id] ?? node.value ?? 0;
}

function getLinkDisplayValue(link) {
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];

    // For the reservoir model, each link's layout value IS the real value —
    // we no longer inflate income and hide the difference.
    // Category leaf links still use realValues for consistency.
    if (sourceNode?.id === 'expenses' && targetNode && realValues[targetNode.id] !== undefined) return realValues[targetNode.id];

    if (link.isHidden) return 0;
    return link.value;
}

function linkPath(link) {
    const source = typeof link.source === 'object' ? link.source : nodes[link.source];
    const target = typeof link.target === 'object' ? link.target : nodes[link.target];

    if (!source || !target) return '';

    const x0 = source.x1;
    const x1 = target.x0;
    const y0 = link.y0;
    const y1 = link.y1;

    const dx = x1 - x0;
    const mx = x0 + dx * 0.5;

    const sourceId = source.id;
    const targetId = target.id;

    const isExpenseBranch = sourceId === 'expenses' && targetId !== '_unallocated';
    const isIncomeBranch = sourceId === 'income' || sourceId === 'from_balance';
    const sameY = Math.abs(y1 - y0) < 0.5;

    let curveLift = 0;

    if (isExpenseBranch) {
        const targetIndex = links
            .filter(l => {
                const s = typeof l.source === 'object' ? l.source : nodes[l.source];
                return s?.id === 'expenses' && !l.isHidden;
            })
            .findIndex(l => l.id === link.id);

        curveLift = targetIndex === 0 ? 10 : 6;
    } else if (isIncomeBranch) {
        /* Curve lift must be proportional to ribbon thickness —
           a fixed pixel value is invisible on thick ribbons.
           Use 8% of ribbon width as the lift, minimum 4px. */
        const ribbonWidth = link.width || 0;
        curveLift = Math.max(ribbonWidth * 0.01, 4);
    } else if (sameY) {
        curveLift = 4;
    }

    const mx1 = x0 + dx * 0.45;
    const mx2 = x0 + dx * 0.55;

    return `M${x0},${y0} C${mx1},${y0} ${mx2},${y1} ${x1},${y1}`;
}
function isHiddenLink(link) { return link.isHidden === true; }
function isHiddenNode(node) { return node.isHidden === true; }

function getLinkTargetCategory(link) {
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    if (!targetNode || targetNode.isHidden) return null;
    if (targetNode.id === 'income' || targetNode.id === 'expenses') return null;
    if (targetNode.id === 'from_balance' || targetNode.id === 'to_balance') return null;
    if (targetNode.id === 'savings_transfer') return 'Savings Transfer';
    if (targetNode.id === 'personal_transfer') return 'Personal Transfer';
    return targetNode.id;
}

function getLinkOpacity(link) {
    if (isHiddenLink(link)) return 0;
    const resolvedCategory = getLinkTargetCategory(link);
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];

    if (selectedCategory) {
        if (resolvedCategory === selectedCategory) return 0.75;
        // Dim source→expenses trunk links (from income OR from_balance)
        if ((sourceNode?.id === 'income' || sourceNode?.id === 'from_balance') && targetNode?.id === 'expenses') {
            const isExpenseCat = nodes.some(n => n.id === selectedCategory && n.id !== 'savings_transfer' && n.id !== 'personal_transfer' && n.id !== 'from_balance' && n.id !== 'to_balance');
            if (isExpenseCat) return 0.15;
        }
        if (sourceNode?.id === 'expenses' && targetNode?.id === selectedCategory) return 0.75;
        return 0.06;
    }
    if (hoveredLink) return link === hoveredLink ? 0.75 : 0.18;
    return 0.50;
}

function getLinkStrokeWidth(link) {
    if (isHiddenLink(link)) return 0;
    const resolvedCategory = getLinkTargetCategory(link);
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];

    /* Ensure minimum visible width for thin flows — thicker on frost surfaces
       Light mode frost needs wider minimums for contrast */
    const minWidth = 4;
    const baseWidth = Math.max(link.width || 0, minWidth);

    if (selectedCategory) {
        if (resolvedCategory === selectedCategory) return baseWidth + 2;
        if (sourceNode?.id === 'expenses' && targetNode?.id === selectedCategory) return baseWidth + 2;
    }
    if (hoveredLink && link === hoveredLink) return baseWidth + 1;
    return baseWidth;
}

/* Gradient ID for each link */
function sanitizeId(str) {
    return str.replace(/[^a-zA-Z0-9_-]/g, '_');
}

function getLinkGradientId(link) {
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    return `flow-grad-${sanitizeId(sourceNode?.id || 's')}-${sanitizeId(targetNode?.id || 't')}`;
}

function getLinkSourceColor(link) {
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    return sourceNode?.color || '#94A3B8';
}

function getLinkTargetColor(link) {
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    return targetNode?.color || '#94A3B8';
}

function getNodeOpacity(node) {
    if (isHiddenNode(node)) return 0;
    if (!selectedCategory) return 1;
    if (node.id === selectedCategory) return 1;
    if (node.id === 'savings_transfer' && selectedCategory === 'Savings Transfer') return 1;
    if (node.id === 'personal_transfer' && selectedCategory === 'Personal Transfer') return 1;
    if (node.id === 'income' || node.id === 'from_balance' || node.id === 'to_balance') return 0.4;
    if (node.id === 'expenses') {
        const isExpenseCat = nodes.some(n => n.id === selectedCategory && n.id !== 'savings_transfer' && n.id !== 'personal_transfer' && n.id !== 'income' && n.id !== 'expenses' && n.id !== 'from_balance' && n.id !== 'to_balance');
        if (isExpenseCat) return 0.4;
        return 0.1;
    }
    return 0.1;
}

function getNodeLabelOpacity(node) {
    if (isHiddenNode(node)) return 0;
    if (!selectedCategory) return 1;
    if (node.id === selectedCategory) return 1;
    if (node.id === 'savings_transfer' && selectedCategory === 'Savings Transfer') return 1;
    if (node.id === 'personal_transfer' && selectedCategory === 'Personal Transfer') return 1;
    if (node.id === 'income' || node.id === 'expenses' || node.id === 'from_balance' || node.id === 'to_balance') return 0.4;
    return 0.1;
}

function isLeafNode(node) {
    if (isHiddenNode(node)) return false;
    return node.id !== 'income' && node.id !== 'expenses' && node.id !== '_unallocated' && node.id !== 'from_balance' && node.id !== 'to_balance';
}

function isClickable(node) {
    if (isHiddenNode(node)) return false;
    return node.id !== 'income' && node.id !== 'expenses' && node.id !== '_unallocated' && node.id !== 'from_balance' && node.id !== 'to_balance';
}

function getClickCategory(node) {
    if (node.id === 'savings_transfer') return 'Savings Transfer';
    if (node.id === 'personal_transfer') return 'Personal Transfer';
    return node.id;
}

function handleLinkHover(link, event) {
    if (selectedCategory || isHiddenLink(link)) return;
    hoveredLink = link;
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    tooltip = {
        show: true,
        x: event.offsetX,
        y: event.offsetY - 10,
        text: `${sourceNode?.label} → ${targetNode?.label}`,
        subtext: formatCurrency(getLinkDisplayValue(link))
    };
}

function handleLinkLeave() {
    hoveredLink = null;
    tooltip = { ...tooltip, show: false };
}

function handleLinkClick(event, link) {
    event.stopPropagation();
    if (isHiddenLink(link)) return;
    const category = getLinkTargetCategory(link);
    if (!category) return;
    if (selectedCategory === category) {
        selectedCategory = null;
        dispatch('select', null);
    } else {
        selectedCategory = category;
        dispatch('select', category);
    }
}

function handleBackgroundClick() {
    selectedCategory = null;
    dispatch('select', null);
}

function handleNodeClick(event, node) {
    event.stopPropagation();
    if (!isClickable(node)) return;
    const category = getClickCategory(node);
    if (selectedCategory === category) {
        selectedCategory = null;
        dispatch('select', null);
    } else {
        selectedCategory = category;
        dispatch('select', category);
    }
}

function handleNodeHover(node, event) {
    if (isHiddenNode(node)) return;
    tooltip = {
        show: true,
        x: event.offsetX,
        y: event.offsetY - 10,
        text: node.label,
        subtext: formatCurrency(getNodeDisplayValue(node))
    };
}

function handleNodeLeave() {
    tooltip = { ...tooltip, show: false };
}

function nodeLabelX(node) {
    return node.x0 < svgWidth / 3 ? node.x1 + 10 : node.x0 - 10;
}

function nodeLabelAnchor(node) {
    return node.x0 < svgWidth / 3 ? 'start' : 'end';
}

function flowDelay(index) {
    return `${0.1 + index * 0.12}s`;
}

onMount(() => {
    mounted = true;
    checkDarkMode();
    if (graphData) layoutSankey(graphData);

    requestAnimationFrame(() => {
        animateIn = true;
    });

    const observer = new ResizeObserver(() => {
        if (graphData) layoutSankey(graphData);
    });
    if (containerEl) observer.observe(containerEl);

    /* Watch for theme changes */
    const themeObserver = new MutationObserver(() => {
        checkDarkMode();
    });
    themeObserver.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ['class']
    });

    return () => {
        observer.disconnect();
        themeObserver.disconnect();
    };
});
</script>

<div bind:this={containerEl} class="sankey-container" class:animate-in={animateIn} style="height: {computedHeight}px; position: relative;">
    {#if nodes.length > 0}
        <svg width={svgWidth} height={computedHeight} class="sankey-svg">
            <defs>
                <!-- ── Node outer glow (luminous halo) ── -->
                <filter id="nodeGlow" x="-60%" y="-60%" width="220%" height="220%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="8" result="outerBlur"/>
                    <feGaussianBlur in="SourceGraphic" stdDeviation="3" result="innerBlur"/>
                    <feMerge>
                        <feMergeNode in="outerBlur"/>
                        <feMergeNode in="innerBlur"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>

                <!-- ── Flow glow for hovered/selected (intense) ── -->
                <filter id="flowGlow" x="-10%" y="-30%" width="120%" height="160%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="6" result="outerBlur"/>
                    <feGaussianBlur in="SourceGraphic" stdDeviation="2" result="innerBlur"/>
                    <feMerge>
                        <feMergeNode in="outerBlur"/>
                        <feMergeNode in="innerBlur"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>

                <!-- ── Ambient flow diffusion (default state) ── -->
                <filter id="flowSoft" x="-5%" y="-15%" width="110%" height="130%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="1.2"/>
                </filter>

                <!-- ── Subtle glow for flow edges ── -->
                <filter id="flowEdgeGlow" x="-8%" y="-20%" width="116%" height="140%">
                    <feGaussianBlur in="SourceGraphic" stdDeviation="3" result="blur"/>
                    <feMerge>
                        <feMergeNode in="blur"/>
                        <feMergeNode in="SourceGraphic"/>
                    </feMerge>
                </filter>

                <!-- ── Flow pulse overlay gradient ── -->
                <linearGradient id="flowPulse" x1="0%" y1="0%" x2="100%" y2="0%"
                    gradientUnits="userSpaceOnUse">
                    <stop offset="0%" stop-color="white" stop-opacity="0" />
                    <stop offset="40%" stop-color="white" stop-opacity="0.15" />
                    <stop offset="60%" stop-color="white" stop-opacity="0.15" />
                    <stop offset="100%" stop-color="white" stop-opacity="0" />
                </linearGradient>

                <!-- ── Per-link gradient definitions ── -->
                {#each links as link (link.id + '_grad')}
                    {#if !isHiddenLink(link)}
                        <linearGradient
                            id={getLinkGradientId(link)}
                            x1={typeof link.source === 'object' ? link.source.x1 : 0}
                            x2={typeof link.target === 'object' ? link.target.x0 : svgWidth}
                            y1="0" y2="0"
                            gradientUnits="userSpaceOnUse">
                            <stop offset="0%" stop-color={getLinkSourceColor(link)} />
                            <stop offset="100%" stop-color={getLinkTargetColor(link)} />
                        </linearGradient>
                    {/if}
                {/each}
            </defs>

            <!-- ── Dark theater background ── -->
            <rect x="0" y="0" width={svgWidth} height={computedHeight} fill="transparent"
                rx="0" ry="0"
                on:click={handleBackgroundClick} style="cursor: default;" />

            <!-- ── Flow links: glow underlayer (bloom effect) ── -->
            <!-- Single CSS filter on parent <g> instead of per-element SVG filter -->
            <g class="sankey-links-glow" style="pointer-events: none; filter: blur(1.2px);">
                {#each links as link, i (link.id + '_glow_' + selectedCategory)}
                    {#if !isHiddenLink(link)}
                        <path
                            d={linkPath(link)}
                            fill="none"
                            stroke="url(#{getLinkGradientId(link)})"
                            stroke-width={Math.max(getLinkStrokeWidth(link) + 4, 6)}
                            stroke-opacity={getLinkOpacity(link) * 0.08}
                            class="sankey-link-glow"
                        />
                    {/if}
                {/each}
            </g>

            <!-- ── Flow links: main ribbons ── -->
            <g class="sankey-links">
                {#each links as link, i (link.id + '_' + selectedCategory)}
                    {#if !isHiddenLink(link)}
                        <path
                            d={linkPath(link)}
                            fill="none"
                            stroke="url(#{getLinkGradientId(link)})"
                            stroke-width={getLinkStrokeWidth(link)}
                            stroke-opacity={getLinkOpacity(link)}
                            filter="none"
                            class="sankey-link"
                            class:sankey-link-clickable={getLinkTargetCategory(link) !== null}
                            on:mouseenter={(e) => handleLinkHover(link, e)}
                            on:mouseleave={handleLinkLeave}
                            on:click={(e) => handleLinkClick(e, link)}
                            aria-label="flow"
                        />
                    {/if}
                {/each}
            </g>

            <!-- ── Animated flow pulse overlay ── -->
            {#if animateIn}
                <g class="sankey-flow-overlay" style="pointer-events: none;">
                    {#each links as link, i (link.id + '_pulse')}
                        {#if !isHiddenLink(link)}
                            <path
                                d={linkPath(link)}
                                fill="none"
                                stroke="url(#flowPulse)"
                                stroke-width={Math.max(link.width, 2)}
                                class="sankey-flow-pulse"
                                style="animation-delay: {flowDelay(i)};"
                            />
                        {/if}
                    {/each}
                </g>
            {/if}

            <!-- ── Nodes with luminous glow ── -->
            <g class="sankey-nodes">
                {#each nodes as node (node.id + '_' + selectedCategory)}
                    {#if !isHiddenNode(node)}
                        <!-- Outer ambient glow – filter only on hover/selected for perf -->
                        <rect
                            x={node.x0 - 5} y={node.y0 - 5}
                            width={node.x1 - node.x0 + 10}
                            height={Math.max(node.y1 - node.y0, 2) + 10}
                            fill={node.color} rx="12"
                            opacity={getNodeOpacity(node) * 0.18}
                            filter={(selectedCategory === getClickCategory(node) || (hoveredLink && (typeof hoveredLink.target === 'object' ? hoveredLink.target.id : '') === node.id)) ? 'url(#nodeGlow)' : 'none'}
                            class="sankey-node-halo"
                        />

                        <!-- Inner glow ring -->
                        <rect
                            x={node.x0 - 2} y={node.y0 - 2}
                            width={node.x1 - node.x0 + 4}
                            height={Math.max(node.y1 - node.y0, 2) + 4}
                            fill="none" rx="8"
                            stroke={node.color}
                            stroke-width="1"
                            stroke-opacity={getNodeOpacity(node) * 0.35}
                            class="sankey-node-ring"
                        />

                        <!-- Node body -->
                        <rect
                            x={node.x0} y={node.y0}
                            width={node.x1 - node.x0}
                            height={Math.max(node.y1 - node.y0, 2)}
                            fill={node.color} rx="6"
                            opacity={getNodeOpacity(node) * 0.92}
                            stroke={node.color}
                            stroke-width="1"
                            stroke-opacity={getNodeOpacity(node) * 0.50}
                            class="sankey-node"
                            class:clickable={isClickable(node)}
                            class:selected={selectedCategory === getClickCategory(node)}
                            on:click={(e) => handleNodeClick(e, node)}
                            on:mouseenter={(e) => handleNodeHover(node, e)}
                            on:mouseleave={handleNodeLeave}
                            on:keydown={(e) => { if (e.key === 'Enter') handleNodeClick(e, node); }}
                            role="button" tabindex="0" aria-label={node.label}
                        />

                        <!-- Bright inner highlight line (top edge of node) -->
                        <line
                            x1={node.x0 + 3} y1={node.y0 + 0.5}
                            x2={node.x1 - 3} y2={node.y0 + 0.5}
                            stroke="white"
                            stroke-width="0.5"
                            stroke-opacity={getNodeOpacity(node) * 0.20}
                            stroke-linecap="round"
                        />

                        {#if isLeafNode(node)}
                            <circle
                                cx={node.x1}
                                cy={(node.y0 + node.y1) / 2}
                                r="3.5"
                                fill={node.color}
                                class="sankey-pulse-dot"
                                opacity={getNodeOpacity(node)}
                            />
                        {/if}

                        <text
                            x={nodeLabelX(node)} y={(node.y0 + node.y1) / 2}
                            dy="0.35em" text-anchor={nodeLabelAnchor(node)}
                            class="sankey-label"
                            opacity={getNodeLabelOpacity(node)}
                            class:clickable={isClickable(node)}
                            on:click={(e) => handleNodeClick(e, node)}
                            on:keydown={(e) => { if (e.key === 'Enter') handleNodeClick(e, node); }}
                            role="button" tabindex="-1"
                        >
                            <tspan class="sankey-label-name">{node.label}</tspan>
                            <tspan class="sankey-label-value" dx="6">{formatCompact(getNodeDisplayValue(node))}</tspan>
                        </text>
                    {/if}
                {/each}
            </g>
        </svg>

        {#if tooltip.show}
            <div class="sankey-tooltip" style="left: {tooltip.x}px; top: {tooltip.y}px;">
                <p class="sankey-tooltip-title">{tooltip.text}</p>
                <p class="sankey-tooltip-value">{tooltip.subtext}</p>
            </div>
        {/if}
    {:else}
        <div class="flex items-center justify-center h-full" style="color: var(--sankey-label-muted, #64748b)">
            <p class="text-sm">No data available for this period</p>
        </div>
    {/if}
</div>

<style>
    .sankey-container { width: 100%; overflow: hidden; transition: height 0.3s ease; }
    .sankey-svg { display: block; }
    .sankey-svg *:focus, .sankey-svg *:focus-visible { outline: none; }

    /* ── Flow entrance animation ── */
    @keyframes flowSweep {
        0%   { stroke-dashoffset: 1; opacity: 0; }
        10%  { opacity: 1; }
        90%  { opacity: 1; }
        100% { stroke-dashoffset: 0; opacity: 0; }
    }

    .sankey-flow-pulse {
        stroke-dasharray: 1;
        stroke-dashoffset: 1;
        opacity: 0;
        animation: flowSweep 2.5s ease-in-out forwards;
        pointer-events: none;
        mix-blend-mode: screen;
    }

    .sankey-flow-overlay path {
        stroke-dasharray: 2000;
        stroke-dashoffset: 2000;
        animation: flowDash 2.5s ease-in-out forwards;
    }

    @keyframes flowDash {
        0%   { stroke-dashoffset: 2000; opacity: 0; }
        5%   { opacity: 0.5; }
        80%  { opacity: 0.3; }
        100% { stroke-dashoffset: 0; opacity: 0; }
    }

    /* ── Pulsing endpoint dot ── */
    @keyframes dotPulse {
        0%, 100% { opacity: 0.9; r: 3.5; }
        50%      { opacity: 0.4; r: 5; }
    }

    .sankey-pulse-dot {
        animation: dotPulse 3s ease-in-out infinite;
        pointer-events: none;
    }

    /* ── Node halos ── */
    .sankey-node-halo {
        pointer-events: none;
        transition: opacity 0.35s ease;
    }

    .sankey-node-ring {
        pointer-events: none;
        transition: stroke-opacity 0.35s ease;
    }

    /* ── Link glow underlayer ── */
    .sankey-link-glow {
        pointer-events: none;
        transition: stroke-opacity 0.35s ease;
    }

    /* Light mode dark island: glow layer at full intensity */
    :global(:root:not(.dark)) .sankey-link-glow {
        opacity: 1;
    }

    /* ── Flow links ── */
    .sankey-link {
        transition: stroke-opacity 0.35s ease, stroke-width 0.25s ease, stroke 0.3s ease;
        pointer-events: stroke;
    }
    .sankey-link-clickable { cursor: pointer; }


    /* ── Node body ── */
    .sankey-node {
        transition: opacity 0.35s ease, filter 0.3s ease, stroke-opacity 0.3s ease;
    }
    .sankey-node.clickable { cursor: pointer; }
    .sankey-node.clickable:hover {
        filter: brightness(1.1) drop-shadow(0 0 8px currentColor);
        stroke-opacity: 0.7;
    }
    .sankey-node.selected {
        filter: brightness(1.05) drop-shadow(0 0 10px currentColor);
        stroke-opacity: 0.6;
    }

    /* Light mode dark island: use bright glow like dark mode */
    :global(:root:not(.dark)) .sankey-node.clickable:hover {
        filter: brightness(1.1) drop-shadow(0 0 10px currentColor);
        stroke-opacity: 0.7;
    }
    :global(:root:not(.dark)) .sankey-node.selected {
        filter: brightness(1.05) drop-shadow(0 0 14px currentColor);
        stroke-opacity: 0.6;
    }
    .sankey-node:focus, .sankey-node:focus-visible { outline: none; }

    /* ── Labels: Light text for dark theater ── */
    .sankey-label {
        font-family: 'Inter', system-ui, sans-serif;
        font-size: 11.5px;
        fill: var(--sankey-label-primary, #CBD5E1);
        transition: opacity 0.35s ease;
        pointer-events: none;
    }
    .sankey-label.clickable { pointer-events: auto; cursor: pointer; }
    .sankey-label-name { font-weight: 600; }

    .sankey-label-value {
        font-family: 'DM Mono', 'Cascadia Code', monospace;
        font-size: 10.5px;
        font-weight: 500;
        fill: var(--sankey-label-secondary, #94A3B8);
    }
    /* ── Tooltip: elevated glass on dark ── */
    .sankey-tooltip {
        position: absolute;
        pointer-events: none;
        padding: 8px 14px;
        border-radius: 12px;
        background: var(--glass-bg-strong, rgba(255, 255, 255, 0.95));
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--glass-border, rgba(200, 210, 225, 0.60));
        box-shadow:
            0 8px 32px rgba(0, 0, 0, 0.12),
            0 2px 8px rgba(0, 0, 0, 0.08);
        transform: translate(-50%, -100%);
        z-index: 10;
        white-space: nowrap;
    }
    .sankey-tooltip-title {
        font-size: 12px;
        font-weight: 600;
        font-family: 'Inter', system-ui, sans-serif;
        color: var(--text-primary, #1E293B);
    }
    .sankey-tooltip-value {
        font-size: 13px;
        font-weight: 500;
        font-family: 'DM Mono', 'Cascadia Code', monospace;
        color: var(--accent, #3B82F6);
        margin-top: 2px;
    }

    /* ── Dark mode tooltip overrides ── */
    :global(.dark) .sankey-tooltip {
        background: var(--glass-bg-strong, rgba(15, 23, 42, 0.92));
        border-color: var(--glass-border, rgba(56, 78, 108, 0.40));
    }
    :global(.dark) .sankey-tooltip-title {
        color: var(--text-primary, #E2E8F0);
    }
    :global(.dark) .sankey-tooltip-value {
        color: var(--accent, #38BDF8);
    }

    :global(.dark) .sankey-tooltip {
        background: var(--glass-bg-strong, rgba(15, 23, 42, 0.92));
        border-color: var(--glass-border, rgba(56, 78, 108, 0.40));
    }
    :global(.dark) .sankey-tooltip-title {
        color: var(--text-primary, #E2E8F0);
    }
    :global(.dark) .sankey-tooltip-value {
        color: var(--accent, #38BDF8);
    }

    /* Light mode dark island: tooltip needs dark treatment */
    :global(:root:not(.dark)) .sankey-tooltip {
        background: rgba(15, 23, 42, 0.92);
        border-color: rgba(56, 189, 248, 0.20);
        box-shadow:
            0 8px 32px rgba(0, 0, 0, 0.25),
            0 2px 8px rgba(0, 0, 0, 0.15),
            0 0 12px rgba(56, 189, 248, 0.06);
    }
    :global(:root:not(.dark)) .sankey-tooltip-title {
        color: #E2E8F0;
    }
    :global(:root:not(.dark)) .sankey-tooltip-value {
        color: #38BDF8;
    }

    /* ── Dark mode label fallback ── */
    :global(.dark) .sankey-label {
        fill: var(--sankey-label-primary, #CBD5E1);
    }
    :global(.dark) .sankey-label-value {
        fill: var(--sankey-label-secondary, #d2dae5);
    }

    /* ── Dark mode: flows use screen blend for luminance ── */
    :global(.dark) .sankey-flow-pulse { mix-blend-mode: screen; }

    /* Light mode dark island: also use screen blend for luminous flows */
    :global(:root:not(.dark)) .sankey-flow-pulse { mix-blend-mode: screen; }
</style>