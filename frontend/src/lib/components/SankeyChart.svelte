<script>
import { onMount, createEventDispatcher } from 'svelte';
import { sankey as d3Sankey, sankeyJustify } from 'd3-sankey';
import { formatCurrency, formatCompact, CATEGORY_COLORS } from '$lib/utils.js';
import { privacyMode } from '$lib/stores.js';

export let income = 0;
export let expenses = 0;
export let creditsRefunds = 0;
export let incomingTransfers = 0;
export let savingsTransfer = 0;
export let personalTransfer = 0;
export let ccRepaid = 0;
export let categories = [];
export let selectedCategory = null;
export let height = 400;
export let autoHeight = true;
export let syncOverlay = false;

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
    credits_refunds:   { color: 'var(--positive)',          glow: 'rgba(16, 185, 129, 0.18)' },
    incoming_transfer: { color: 'var(--flow-transfer)',     glow: 'var(--flow-transfer-glow)' },
    expenses:          { color: 'var(--flow-expenses)',     glow: 'var(--flow-expenses-glow)' },
    savings_transfer:  { color: 'var(--flow-savings)',      glow: 'var(--flow-savings-glow)' },
    personal_transfer: { color: 'var(--flow-transfer)',     glow: 'var(--flow-transfer-glow)' },
    from_balance:      { color: 'var(--flow-from-balance)', glow: 'var(--flow-from-balance-glow)' },
    to_balance:        { color: 'var(--flow-to-balance)',   glow: 'var(--flow-to-balance-glow)' },
    cc_repaid:         { color: '#D97706',                  glow: '#FBBF24' },
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

    // ── Ghost Flow state ──
    let ghostFlowPath = null;
    let ghostNode = null;
    let ghostComputedHeight = 0;

    $: {
        // Build a lightweight key from the data that actually affects layout.
        // selectedCategory and theme changes do NOT affect layout — only visual props.
        // Privacy mode is included to force label re-render (layout stays the same).
        const key = `${income}|${creditsRefunds}|${incomingTransfers}|${expenses}|${savingsTransfer}|${personalTransfer}|${ccRepaid}|${(categories || []).map(c => c.category + ':' + c.total).join(',')}|priv:${$privacyMode}`;

        if (key !== prevGraphKey) {
            prevGraphKey = key;
            graphData = buildGraph(income, expenses, savingsTransfer, personalTransfer, categories, ccRepaid, creditsRefunds, incomingTransfers);
            if (containerEl && graphData && mounted) {
                layoutSankey(graphData);
            }
        }
    }

    // ── Ghost Flow geometry (recomputes when nodes or ccRepaid change) ──
    $: {
        if (nodes.length > 0 && ccRepaid > 0.01) {
            const ccPaid = ccRepaid;
            const incomeNode = nodes.find(n => n.id === 'income');
            const balanceNode = nodes.find(n => n.id === 'from_balance');
            const creditNode = nodes.find(n => n.id === 'credits_refunds');
            const incomingNode = nodes.find(n => n.id === 'incoming_transfer');
            const fundingNode = balanceNode || incomeNode || creditNode || incomingNode;
            const maxDepth = nodes.reduce((mx, n) => Math.max(mx, n.depth || 0), 0);
            const destNodes = nodes.filter(n => n.depth === maxDepth && !n.isGhost);
            const maxX = destNodes.length > 0 ? Math.max(...destNodes.map(n => n.x0)) : (svgWidth - 120);
            const maxY = nodes.filter(n => !n.isGhost).reduce((mx, n) => Math.max(mx, n.y1 || 0), 0);

            if (fundingNode) {
                const ghostY0 = maxY + 36;
                const sourceHeight = fundingNode.y1 - fundingNode.y0;
                const ghostHeight = Math.max(18, Math.min(40, ccPaid / (fundingNode.value || 1) * sourceHeight));
                ghostNode = {
                    x0: maxX,
                    x1: maxX + NODE_WIDTH,
                    y0: ghostY0,
                    y1: ghostY0 + ghostHeight,
                    name: 'CC Repaid',
                    value: ccPaid,
                    color: '#f472b6'
                };

                const srcX = fundingNode.x1;
                const srcY0 = fundingNode.y1 - ghostHeight;
                const srcY1 = fundingNode.y1;
                const tgtX = ghostNode.x0;
                const tgtY0 = ghostNode.y0;
                const tgtY1 = ghostNode.y1;
                const midX = (srcX + tgtX) / 2;

                ghostFlowPath = {
                    d: `M${srcX},${srcY0} C${midX},${srcY0} ${midX},${tgtY0} ${tgtX},${tgtY0} L${tgtX},${tgtY1} C${midX},${tgtY1} ${midX},${srcY1} ${srcX},${srcY1} Z`,
                    value: ccPaid
                };
                ghostComputedHeight = ghostNode.y1 + 50;
            } else {
                ghostFlowPath = null;
                ghostNode = null;
                ghostComputedHeight = 0;
            }
        } else {
            ghostFlowPath = null;
            ghostNode = null;
            ghostComputedHeight = 0;
        }
    }

function buildGraph(inc, exp, savTotal, ptTotal, cats, ccRepaidAmt = 0, creditsRefundsAmt = 0, incomingTransfersAmt = 0) {
    if ((!cats || cats.length === 0) && savTotal === 0 && ptTotal === 0 && ccRepaidAmt === 0 && creditsRefundsAmt === 0 && incomingTransfersAmt === 0) return null;

    const top = (cats || []).slice(0, 10);
    const categoryTotal = top.reduce((s, c) => s + (c.total || 0), 0);

    const totalOutflow = categoryTotal + savTotal + ptTotal;
    if (totalOutflow === 0) return null;

    const realIncome = inc || 0;
    const realCreditsRefunds = creditsRefundsAmt || 0;
    const realIncomingTransfers = incomingTransfersAmt || 0;
    const realInflow = realIncome + realCreditsRefunds + realIncomingTransfers;

    // ── Balance-as-Reservoir model ──
    // When income doesn't cover outflow, the shortfall comes "from balance".
    // When income exceeds outflow, the surplus goes "to balance" (savings pool).
    const shortfall = Math.max(totalOutflow - realInflow, 0);
    const surplus = Math.max(realInflow - totalOutflow, 0);

    // Layout income = the total left-side value that feeds into outflows.
    // This must equal totalOutflow so the Sankey balances visually.
    const layoutIncome = totalOutflow;

    realValues = {
        'income': realIncome,
        'credits_refunds': realCreditsRefunds,
        'incoming_transfer': realIncomingTransfers,
        'expenses': categoryTotal,
        'savings_transfer': savTotal,
        'personal_transfer': ptTotal,
        'from_balance': shortfall,
        'to_balance': surplus
    };
    top.forEach(c => { realValues[c.category] = c.total; });

    // Store ccRepaid in realValues for display (ghost node uses this)
    if (ccRepaidAmt > 0.01) {
        realValues['cc_repaid'] = ccRepaidAmt;
    }

    const nodeList = [];

    // ── Left-side source nodes ──
    // Always show income node if there IS income, even if it doesn't cover everything
    if (realIncome > 0) {
        nodeList.push({ id: 'income', label: 'Income', color: FLOW_COLORS.income.color });
    }
    if (realCreditsRefunds > 0) {
        nodeList.push({ id: 'credits_refunds', label: 'Credits', color: FLOW_COLORS.credits_refunds.color });
    }
    if (realIncomingTransfers > 0) {
        nodeList.push({ id: 'incoming_transfer', label: 'Incoming', color: FLOW_COLORS.incoming_transfer.color });
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

    // NOTE: Ghost node (CC Repaid) is NO LONGER added to the D3 Sankey node list.
    // It is rendered entirely outside the layout as a manual SVG overlay (see ghostNode reactive block).

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

    const sources = [
        { id: 'income', remaining: realIncome },
        { id: 'credits_refunds', remaining: realCreditsRefunds },
        { id: 'incoming_transfer', remaining: realIncomingTransfers },
        { id: 'from_balance', remaining: shortfall }
    ].filter(s => s.remaining > 0.01 && idToIndex[s.id] !== undefined);

    function allocateTo(targetId, amount) {
        if (amount <= 0.01 || idToIndex[targetId] === undefined) return;
        let remaining = amount;
        for (const source of sources) {
            if (remaining <= 0.01) break;
            const take = Math.min(source.remaining, remaining);
            if (take <= 0.01) continue;
            linkList.push({
                source: idToIndex[source.id],
                target: idToIndex[targetId],
                value: take,
                id: `${source.id}-${targetId}`
            });
            source.remaining -= take;
            remaining -= take;
        }
    }

    allocateTo('expenses', top.length > 0 ? categoryTotal : 0);
    allocateTo('savings_transfer', savTotal);
    allocateTo('personal_transfer', ptTotal);
    allocateTo('to_balance', surplus);

    //    Ghost: CC Repaid (visual only, does not affect Sankey balance)   
    // This link is injected AFTER the Sankey layout as a visual overlay.
    // We store the data here but render it separately to avoid breaking d3-sankey's
    // flow conservation constraint.

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

function selectedNodeId() {
    if (selectedCategory === 'Credits & Refunds') return 'credits_refunds';
    if (selectedCategory === 'Savings Transfer') return 'savings_transfer';
    if (selectedCategory === 'Personal Transfer') return 'personal_transfer';
    return selectedCategory;
}

function getLinkTargetCategory(link) {
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    if (sourceNode?.id === 'credits_refunds') return 'Credits & Refunds';
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
    const activeNodeId = selectedNodeId();

    let opacity;

    if (selectedCategory) {
        if (resolvedCategory === selectedCategory) {
            opacity = 0.75;
        } else if (sourceNode?.id === activeNodeId || targetNode?.id === activeNodeId) {
            opacity = 0.75;
        } else if ((sourceNode?.id === 'income' || sourceNode?.id === 'from_balance') && targetNode?.id === 'expenses') {
        // Dim source→expenses trunk links (from income OR from_balance)
            const isExpenseCat = nodes.some(n => n.id === selectedCategory && n.id !== 'savings_transfer' && n.id !== 'personal_transfer' && n.id !== 'from_balance' && n.id !== 'to_balance');
            opacity = isExpenseCat ? 0.15 : 0.06;
        } else if (sourceNode?.id === 'expenses' && targetNode?.id === selectedCategory) {
            opacity = 0.75;
        } else {
            opacity = 0.06;
        }
    } else if (hoveredLink) {
        opacity = link === hoveredLink ? 0.75 : 0.18;
    } else {
        opacity = 0.50;
    }

    if (syncOverlay) {
        return opacity * 0.42;
    }

    return opacity;
}

function getLinkStrokeWidth(link) {
    if (isHiddenLink(link)) return 0;
    const resolvedCategory = getLinkTargetCategory(link);
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    const activeNodeId = selectedNodeId();

    /* Ensure minimum visible width for thin flows — thicker on frost surfaces
       Light mode frost needs wider minimums for contrast */
    const minWidth = 4;
    const baseWidth = Math.max(link.width || 0, minWidth);

    if (selectedCategory) {
        if (resolvedCategory === selectedCategory) return baseWidth + 2;
        if (sourceNode?.id === activeNodeId || targetNode?.id === activeNodeId) return baseWidth + 2;
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
    if (syncOverlay) return '#6f8098';
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    return sourceNode?.color || '#94A3B8';
}

function getLinkTargetColor(link) {
    if (syncOverlay) return '#c2cfdf';
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    return targetNode?.color || '#94A3B8';
}

function getNodeRenderColor(node) {
    if (!syncOverlay) return node.color;

    if (node.id === 'income' || node.id === 'credits_refunds' || node.id === 'incoming_transfer' || node.id === 'from_balance') return '#7f8ea2';
    if (node.id === 'expenses') return '#9fb1c8';
    if (node.id === 'to_balance') return '#95a8bb';
    return '#b7c4d5';
}

function getNodeOpacity(node) {
    if (isHiddenNode(node)) return 0;
    let opacity = 1;
    const activeNodeId = selectedNodeId();

    if (!selectedCategory) {
        opacity = 1;
    } else if (node.id === selectedCategory || node.id === activeNodeId) {
        opacity = 1;
    } else if (node.id === 'savings_transfer' && selectedCategory === 'Savings Transfer') {
        opacity = 1;
    } else if (node.id === 'personal_transfer' && selectedCategory === 'Personal Transfer') {
        opacity = 1;
    } else if (node.id === 'income' || node.id === 'credits_refunds' || node.id === 'incoming_transfer' || node.id === 'from_balance' || node.id === 'to_balance') {
        opacity = 0.4;
    } else if (node.id === 'expenses') {
        const isExpenseCat = nodes.some(n => n.id === selectedCategory && !['savings_transfer', 'personal_transfer', 'income', 'credits_refunds', 'incoming_transfer', 'expenses', 'from_balance', 'to_balance'].includes(n.id));
        opacity = isExpenseCat ? 0.4 : 0.1;
    } else {
        opacity = 0.1;
    }

    if (syncOverlay) {
        return opacity * 0.52;
    }

    return opacity;
}

function getNodeLabelOpacity(node) {
    if (isHiddenNode(node)) return 0;
    let opacity = 1;
    const activeNodeId = selectedNodeId();

    if (!selectedCategory) {
        opacity = 1;
    } else if (node.id === selectedCategory || node.id === activeNodeId) {
        opacity = 1;
    } else if (node.id === 'savings_transfer' && selectedCategory === 'Savings Transfer') {
        opacity = 1;
    } else if (node.id === 'personal_transfer' && selectedCategory === 'Personal Transfer') {
        opacity = 1;
    } else if (node.id === 'income' || node.id === 'credits_refunds' || node.id === 'incoming_transfer' || node.id === 'expenses' || node.id === 'from_balance' || node.id === 'to_balance') {
        opacity = 0.4;
    } else {
        opacity = 0.1;
    }

    if (syncOverlay) {
        return opacity * 0.66;
    }

    return opacity;
}

function getNodeById(id) {
    return nodes.find(node => node.id === id);
}

let maxLinkValue = 1;

$: maxLinkValue = links.length > 0
    ? Math.max(...links.map(link => link.value || 0), 1)
    : 1;

function getSyncMaskWidth(link) {
    return Math.max(getLinkStrokeWidth(link) + 5, 8);
}

function getSyncMaskOpacity(link) {
    const sourceNode = typeof link.source === 'object' ? link.source : nodes[link.source];
    const targetNode = typeof link.target === 'object' ? link.target : nodes[link.target];
    const ratio = Math.min((link.value || 0) / maxLinkValue, 1);
    let opacity = 0.24 + ratio * 0.28;

    if (targetNode?.id === 'expenses') opacity += 0.06;
    if (sourceNode?.id === 'expenses') opacity += 0.02;
    if (targetNode?.id === 'to_balance') opacity += 0.03;

    return Math.min(opacity, 1);
}

function getSyncNodeMaskOpacity(node) {
    if (node.id === 'expenses') return 0.18;
    if (node.id === 'income' || node.id === 'from_balance' || node.id === 'to_balance') return 0.62;
    if (node.id === 'savings_transfer' || node.id === 'personal_transfer') return 0.5;
    return 0.4;
}

function isLeafNode(node) {
    if (isHiddenNode(node)) return false;
    if (node.isGhost) return false;
    return !['income', 'incoming_transfer', 'expenses', '_unallocated', 'from_balance', 'to_balance'].includes(node.id);
}

function isClickable(node) {
    if (isHiddenNode(node)) return false;
    if (node.isGhost) return false;
    return !['income', 'incoming_transfer', 'expenses', '_unallocated', 'from_balance', 'to_balance'].includes(node.id);
}

function getClickCategory(node) {
    if (node.id === 'credits_refunds') return 'Credits & Refunds';
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

function handleBackgroundKeydown(event) {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    handleBackgroundClick();
}

function handleLinkKeydown(event, link) {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    handleLinkClick(event, link);
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

function handleNodeKeydown(event, node) {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    handleNodeClick(event, node);
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

<div bind:this={containerEl} class="sankey-container" class:animate-in={animateIn} style="height: {ghostComputedHeight > computedHeight ? ghostComputedHeight : computedHeight}px; position: relative;">
    {#if nodes.length > 0}
        <svg width={svgWidth} height={ghostComputedHeight > computedHeight ? ghostComputedHeight : computedHeight} class="sankey-svg" role="group" aria-label="Cash flow Sankey chart">
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
                <filter id="syncGasBlur" x="-40%" y="-60%" width="180%" height="220%">
                    <feGaussianBlur stdDeviation="12" />
                </filter>
                <linearGradient id="syncGasStreamA" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stop-color="#2dd4bf" stop-opacity="0" />
                    <stop offset="24%" stop-color="#2dd4bf" stop-opacity="1" />
                    <stop offset="48%" stop-color="#67e8f9" stop-opacity="1" />
                    <stop offset="68%" stop-color="#e0f2fe" stop-opacity="1" />
                    <stop offset="82%" stop-color="#818cf8" stop-opacity="1" />
                    <stop offset="100%" stop-color="#bae6fd" stop-opacity="0" />
                </linearGradient>
                <linearGradient id="syncGasStreamB" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stop-color="#bae6fd" stop-opacity="0" />
                    <stop offset="28%" stop-color="#bae6fd" stop-opacity="1" />
                    <stop offset="52%" stop-color="#38bdf8" stop-opacity="1" />
                    <stop offset="72%" stop-color="#818cf8" stop-opacity="1" />
                    <stop offset="86%" stop-color="#c4b5fd" stop-opacity="1" />
                    <stop offset="100%" stop-color="#818cf8" stop-opacity="0" />
                </linearGradient>
                <linearGradient id="syncGasStreamC" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stop-color="#99f6e4" stop-opacity="0" />
                    <stop offset="24%" stop-color="#2dd4bf" stop-opacity="1" />
                    <stop offset="46%" stop-color="#a5f3fc" stop-opacity="1" />
                    <stop offset="56%" stop-color="#ffffff" stop-opacity="1" />
                    <stop offset="74%" stop-color="#c4b5fd" stop-opacity="1" />
                    <stop offset="88%" stop-color="#818cf8" stop-opacity="1" />
                    <stop offset="100%" stop-color="#c4b5fd" stop-opacity="0" />
                </linearGradient>
                <linearGradient id="syncGasStreamD" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stop-color="#f0abfc" stop-opacity="0" />
                    <stop offset="22%" stop-color="#c084fc" stop-opacity="1" />
                    <stop offset="48%" stop-color="#93c5fd" stop-opacity="1" />
                    <stop offset="68%" stop-color="#67e8f9" stop-opacity="1" />
                    <stop offset="82%" stop-color="#e0e7ff" stop-opacity="1" />
                    <stop offset="100%" stop-color="#67e8f9" stop-opacity="0" />
                </linearGradient>
                <linearGradient id="syncGasStreamE" x1="0%" y1="0%" x2="100%" y2="0%">
                    <stop offset="0%" stop-color="#5eead4" stop-opacity="0" />
                    <stop offset="24%" stop-color="#22d3ee" stop-opacity="1" />
                    <stop offset="48%" stop-color="#e0f2fe" stop-opacity="1" />
                    <stop offset="66%" stop-color="#67e8f9" stop-opacity="1" />
                    <stop offset="80%" stop-color="#818cf8" stop-opacity="1" />
                    <stop offset="100%" stop-color="#818cf8" stop-opacity="0" />
                </linearGradient>
                {#if syncOverlay}
                    <mask id="syncTopologyMask" maskUnits="userSpaceOnUse" x="0" y="0" width={svgWidth} height={computedHeight}>
                        <rect x="0" y="0" width={svgWidth} height={computedHeight} fill="black" />
                        <g>
                            {#each links as link (link.id + '_sync-mask')}
                                {#if !isHiddenLink(link)}
                                    <path
                                        d={linkPath(link)}
                                        fill="none"
                                        stroke="white"
                                        stroke-linecap="butt"
                                        stroke-linejoin="round"
                                        stroke-width={getSyncMaskWidth(link)}
                                        stroke-opacity={getSyncMaskOpacity(link)}
                                    />
                                {/if}
                            {/each}
                            {#each nodes as node (node.id + '_sync-mask')}
                                {#if !isHiddenNode(node) && !node.isGhost}
                                    <rect
                                        x={node.x0 - 1}
                                        y={node.y0 - 1}
                                        width={node.x1 - node.x0 + 2}
                                        height={Math.max(node.y1 - node.y0, 2) + 2}
                                        rx="8"
                                        fill="white"
                                        fill-opacity={getSyncNodeMaskOpacity(node)}
                                    />
                                {/if}
                            {/each}
                            {#if ghostFlowPath && ghostNode}
                                <path
                                    d={ghostFlowPath.d}
                                    fill="none"
                                    stroke="white"
                                    stroke-linecap="round"
                                    stroke-linejoin="round"
                                    stroke-width="18"
                                    stroke-opacity="0.38"
                                />
                                <rect
                                    x={ghostNode.x0 - 1}
                                    y={ghostNode.y0 - 1}
                                    width={ghostNode.x1 - ghostNode.x0 + 2}
                                    height={ghostNode.y1 - ghostNode.y0 + 2}
                                    rx="8"
                                    fill="white"
                                    fill-opacity="0.44"
                                />
                            {/if}
                        </g>
                    </mask>
                {/if}
            </defs>

            <!-- ── Dark theater background ── -->
            <rect x="0" y="0" width={svgWidth} height={computedHeight} fill="transparent"
                rx="0" ry="0"
                role="button"
                tabindex="0"
                aria-label="Clear Sankey selection"
                on:click={handleBackgroundClick}
                on:keydown={handleBackgroundKeydown}
                style="cursor: default;" />

            <!-- ── Flow links: glow underlayer (bloom effect) ── -->
            <!-- Single CSS filter on parent <g> instead of per-element SVG filter -->
            <g class="sankey-links-glow" class:sankey-links-glow--sync={syncOverlay} style="pointer-events: none;">
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
            <g class="sankey-links" class:sankey-links--sync={syncOverlay}>
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
                            role="button"
                            tabindex="0"
                            aria-label={`Flow from ${typeof link.source === 'object' ? link.source.label : 'source'} to ${typeof link.target === 'object' ? link.target.label : 'target'}`}
                            on:mouseenter={(e) => handleLinkHover(link, e)}
                            on:mouseleave={handleLinkLeave}
                            on:click={(e) => handleLinkClick(e, link)}
                            on:keydown={(e) => handleLinkKeydown(e, link)}
                        />
                    {/if}
                {/each}
            </g>

            {#if syncOverlay}
                <g class="sankey-live-sync" aria-hidden="true">
                    <g class="sankey-live-sync__topology" mask="url(#syncTopologyMask)">
                        <rect
                            x="0"
                            y="0"
                            width={svgWidth}
                            height={computedHeight}
                            class="sankey-live-sync__topology-wash"
                        />
                            <rect
                                x={-svgWidth * 0.18}
                                y={computedHeight * 0.17}
                                width={svgWidth * 0.96}
                                height={computedHeight * 0.115}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--upper"
                                filter="url(#syncGasBlur)"
                            />
                        <rect
                            x={-svgWidth * 0.14}
                            y={computedHeight * 0.44}
                                width={svgWidth * 0.92}
                                height={computedHeight * 0.12}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--lower"
                                filter="url(#syncGasBlur)"
                            />
                        <rect
                            x={-svgWidth * 0.12}
                            y={computedHeight * 0.31}
                                width={svgWidth * 0.9}
                                height={computedHeight * 0.11}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--accent"
                                filter="url(#syncGasBlur)"
                            />
                        <rect
                            x={-svgWidth * 0.16}
                            y={computedHeight * 0.24}
                                width={svgWidth * 0.98}
                                height={computedHeight * 0.09}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--shear"
                                filter="url(#syncGasBlur)"
                            />
                        <rect
                            x={-svgWidth * 0.10}
                            y={computedHeight * 0.36}
                                width={svgWidth * 0.92}
                                height={computedHeight * 0.085}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--undertow"
                                filter="url(#syncGasBlur)"
                            />
                        <rect
                            x={-svgWidth * 0.20}
                            y={computedHeight * 0.22}
                                width={svgWidth * 1.08}
                                height={computedHeight * 0.11}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--violet-drift"
                                filter="url(#syncGasBlur)"
                            />
                        <rect
                            x={-svgWidth * 0.18}
                            y={computedHeight * 0.34}
                                width={svgWidth * 1.04}
                                height={computedHeight * 0.095}
                                rx={999}
                                class="sankey-live-sync__stream sankey-live-sync__stream--teal-drift"
                                filter="url(#syncGasBlur)"
                            />
                    </g>
                </g>
            {/if}

            <!-- ── Animated flow pulse overlay ── -->
            {#if animateIn}
                <g class="sankey-flow-overlay" class:sankey-flow-overlay--sync={syncOverlay} style="pointer-events: none;">
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
            <g class="sankey-nodes" class:sankey-nodes--sync={syncOverlay}>
                {#each nodes as node (node.id + '_' + selectedCategory)}
                    {#if !isHiddenNode(node) && !node.isGhost}
                        <!-- Outer ambient glow — filter only on hover/selected for perf -->
                        <rect
                            x={node.x0 - 5} y={node.y0 - 5}
                            width={node.x1 - node.x0 + 10}
                            height={Math.max(node.y1 - node.y0, 2) + 10}
                            fill={getNodeRenderColor(node)} rx="12"
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
                            stroke={getNodeRenderColor(node)}
                            stroke-width="1"
                            stroke-opacity={getNodeOpacity(node) * 0.35}
                            class="sankey-node-ring"
                        />

                        <!-- Node body -->
                        <rect
                            x={node.x0} y={node.y0}
                            width={node.x1 - node.x0}
                            height={Math.max(node.y1 - node.y0, 2)}
                            fill={getNodeRenderColor(node)} rx="6"
                            opacity={getNodeOpacity(node) * 0.92}
                            stroke={getNodeRenderColor(node)}
                            stroke-width="1"
                            stroke-opacity={getNodeOpacity(node) * 0.50}
                            class="sankey-node"
                            class:clickable={isClickable(node)}
                            class:selected={selectedCategory === getClickCategory(node)}
                            on:click={(e) => handleNodeClick(e, node)}
                            on:mouseenter={(e) => handleNodeHover(node, e)}
                            on:mouseleave={handleNodeLeave}
                            on:keydown={(e) => handleNodeKeydown(e, node)}
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
                                fill={getNodeRenderColor(node)}
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
                            on:keydown={(e) => handleNodeKeydown(e, node)}
                            role="button" tabindex="-1"
                        >
                            <tspan class="sankey-label-name">{node.label}</tspan>
                            <tspan class="sankey-label-value" dx="6">{formatCompact(getNodeDisplayValue(node))}</tspan>
                        </text>
                    {/if}
                {/each}
            </g>

            <!-- ── Ghost Flow Layer (CC Repaid) ── -->
            {#if ghostFlowPath && ghostNode}
                <g class="ghost-flow-layer" class:ghost-flow-layer--sync={syncOverlay} opacity="0.6">
                    <defs>
                        <linearGradient id="ghost-grad" x1="0%" y1="0%" x2="100%" y2="0%">
                            <stop offset="0%" stop-color={syncOverlay ? '#6f8098' : '#a78bfa'} stop-opacity={syncOverlay ? '0.2' : '0.45'} />
                            <stop offset="100%" stop-color={syncOverlay ? '#bfcbdb' : '#f472b6'} stop-opacity={syncOverlay ? '0.28' : '0.55'} />
                        </linearGradient>
                    </defs>

                    <!-- Ghost link (filled ribbon, dashed outline) -->
                    <path
                        d={ghostFlowPath.d}
                        fill="url(#ghost-grad)"
                        fill-opacity="0.16"
                        stroke={syncOverlay ? '#b4c2d3' : '#f472b6'}
                        stroke-width="1.5"
                        stroke-dasharray="8 4"
                        stroke-opacity={syncOverlay ? '0.34' : '0.6'}
                    >
                        <title>CC Repaid: {formatCurrency(ghostNode.value)}</title>
                    </path>

                    <!-- Ghost destination node -->
                    <rect
                        x={ghostNode.x0}
                        y={ghostNode.y0}
                        width={ghostNode.x1 - ghostNode.x0}
                        height={ghostNode.y1 - ghostNode.y0}
                        rx="6"
                        fill={syncOverlay ? '#bcc8d7' : '#f472b6'}
                        fill-opacity={syncOverlay ? '0.16' : '0.30'}
                        stroke={syncOverlay ? '#bcc8d7' : '#f472b6'}
                        stroke-width="1.5"
                        stroke-dasharray="6 3"
                        role="img"
                        aria-label={`CC Repaid ${formatCurrency(ghostNode.value)}`}
                        on:mouseenter={(e) => {
                            tooltip = {
                                show: true,
                                x: e.offsetX,
                                y: e.offsetY - 10,
                                text: 'CC Repaid',
                                subtext: formatCurrency(ccRepaid) + ' — Payment toward prior month\'s credit card charges. Not counted as new spending.'
                            };
                        }}
                        on:mouseleave={() => { tooltip = { ...tooltip, show: false }; }}
                        style="cursor: help;"
                    />

                    <!-- Ghost node label (single line, matching other leaf nodes) -->
                    <text
                        x={ghostNode.x0 - 10}
                        y={(ghostNode.y0 + ghostNode.y1) / 2}
                        dy="0.35em"
                        text-anchor="end"
                        fill={syncOverlay ? '#c5d0de' : '#f472b6'}
                        opacity={syncOverlay ? '0.54' : '0.85'}
                        style="font-style: italic;"
                    >
                        <tspan font-family="'Inter', system-ui, sans-serif" font-size="11.5" font-weight="600">CC Repaid</tspan>
                        <tspan dx="6" font-family="'DM Mono', 'Cascadia Code', monospace" font-size="10.5" font-weight="500" opacity="0.85" fill="#ffffff">{formatCompact(ghostNode.value)}</tspan>
                    </text>
                </g>
            {/if}
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
        animation: flowSweep 1.4s cubic-bezier(0.22, 0.61, 0.36, 1) forwards;
        pointer-events: none;
        mix-blend-mode: screen;
    }

    .sankey-flow-overlay path {
        stroke-dasharray: 2000;
        stroke-dashoffset: 2000;
        animation: flowDash 2.5s ease-in-out forwards;
    }

    .sankey-links-glow {
        filter: blur(1.2px);
    }

    .sankey-links-glow--sync {
        opacity: 0.18;
        filter: blur(1.6px) saturate(0.7);
    }

    .sankey-links--sync {
        filter: saturate(0.58) brightness(0.82) contrast(0.94);
    }

    .sankey-flow-overlay--sync {
        opacity: 0.12;
    }

    .sankey-nodes--sync {
        filter: saturate(0.72) brightness(0.86);
    }

    .ghost-flow-layer--sync {
        opacity: 0.32;
    }

    .sankey-live-sync {
        pointer-events: none;
        mix-blend-mode: screen;
        isolation: isolate;
    }

    .sankey-live-sync__topology {
        pointer-events: none;
    }

    .sankey-live-sync__topology-wash {
        fill: rgba(6, 12, 24, 0.006);
    }

    .sankey-live-sync__stream {
        transform-box: fill-box;
        transform-origin: center;
        will-change: transform, opacity;
    }

    .sankey-live-sync__stream--upper {
        fill: url(#syncGasStreamA);
        opacity: 1;
        animation: syncGasStreamA 18s ease-in-out infinite;
    }

    .sankey-live-sync__stream--lower {
        fill: url(#syncGasStreamB);
        opacity: 0.9;
        animation: syncGasStreamB 20s ease-in-out infinite;
    }

    .sankey-live-sync__stream--accent {
        fill: url(#syncGasStreamC);
        opacity: 0.96;
        animation: syncGasStreamC 16s ease-in-out infinite;
    }

    .sankey-live-sync__stream--shear {
        fill: url(#syncGasStreamD);
        opacity: 0.78;
        animation: syncGasStreamShear 22s ease-in-out infinite;
    }

    .sankey-live-sync__stream--undertow {
        fill: url(#syncGasStreamB);
        opacity: 0.62;
        animation: syncGasStreamUndertow 24s ease-in-out infinite;
    }

    .sankey-live-sync__stream--violet-drift {
        fill: url(#syncGasStreamD);
        opacity: 0.84;
        animation: syncGasStreamViolet 21s ease-in-out infinite;
    }

    .sankey-live-sync__stream--teal-drift {
        fill: url(#syncGasStreamE);
        opacity: 0.9;
        animation: syncGasStreamTeal 19s ease-in-out infinite;
    }

    @keyframes flowDash {
        0%   { stroke-dashoffset: 2000; opacity: 0; }
        5%   { opacity: 0.5; }
        80%  { opacity: 0.3; }
        100% { stroke-dashoffset: 0; opacity: 0; }
    }

    @keyframes syncGasStreamA {
        0% {
            transform: translate3d(-4%, -3%, 0) rotate(-6deg) scaleX(0.96) scaleY(0.9);
            opacity: 0.34;
        }
        50% {
            transform: translate3d(62%, 1%, 0) rotate(-2deg) scaleX(1.04) scaleY(1.08);
            opacity: 0.88;
        }
        100% {
            transform: translate3d(132%, 4%, 0) rotate(3deg) scaleX(1.12) scaleY(0.98);
            opacity: 0.4;
        }
    }

    @keyframes syncGasStreamC {
        0% {
            transform: translate3d(-6%, -2%, 0) rotate(-4deg) scaleX(0.92) scaleY(0.94);
            opacity: 0.18;
        }
        32% {
            opacity: 0.96;
        }
        100% {
            transform: translate3d(146%, 3%, 0) rotate(4deg) scaleX(1.12) scaleY(1.08);
            opacity: 0.22;
        }
    }

    @keyframes syncGasStreamB {
        0% {
            transform: translate3d(-5%, 4%, 0) rotate(6deg) scaleX(0.94) scaleY(0.96);
            opacity: 0.24;
        }
        48% {
            transform: translate3d(68%, -2%, 0) rotate(1deg) scaleX(1.04) scaleY(1.1);
            opacity: 0.82;
        }
        100% {
            transform: translate3d(136%, -4%, 0) rotate(-4deg) scaleX(1.1) scaleY(0.98);
            opacity: 0.34;
        }
    }

    @keyframes syncGasStreamShear {
        0% {
            transform: translate3d(-8%, -3%, 0) rotate(-8deg) scaleX(0.9) scaleY(0.92);
            opacity: 0.16;
        }
        38% {
            opacity: 0.74;
        }
        100% {
            transform: translate3d(138%, 5%, 0) rotate(6deg) scaleX(1.12) scaleY(1.08);
            opacity: 0.22;
        }
    }

    @keyframes syncGasStreamUndertow {
        0% {
            transform: translate3d(-7%, 3%, 0) rotate(7deg) scaleX(0.92) scaleY(0.9);
            opacity: 0.12;
        }
        46% {
            opacity: 0.58;
        }
        100% {
            transform: translate3d(126%, -5%, 0) rotate(-6deg) scaleX(1.08) scaleY(1.04);
            opacity: 0.18;
        }
    }

    @keyframes syncGasStreamViolet {
        0% {
            transform: translate3d(-6%, -2%, 0) rotate(-5deg) scaleX(0.92) scaleY(0.96);
            opacity: 0.14;
        }
        44% {
            opacity: 0.78;
        }
        100% {
            transform: translate3d(138%, 3%, 0) rotate(3deg) scaleX(1.1) scaleY(1.06);
            opacity: 0.18;
        }
    }

    @keyframes syncGasStreamTeal {
        0% {
            transform: translate3d(-5%, 2%, 0) rotate(4deg) scaleX(0.94) scaleY(0.94);
            opacity: 0.18;
        }
        52% {
            opacity: 0.84;
        }
        100% {
            transform: translate3d(132%, -3%, 0) rotate(-3deg) scaleX(1.08) scaleY(1.08);
            opacity: 0.22;
        }
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

    /* Ghost Flow Layer */
    .ghost-flow-layer {
        transition: opacity 0.4s ease;
    }
    .ghost-flow-layer path {
        animation: ghostPulse 3s ease-in-out infinite alternate;
    }
    @keyframes ghostPulse {
        0%   { stroke-opacity: 0.4; fill-opacity: 0.10; }
        100% { stroke-opacity: 0.7; fill-opacity: 0.22; }
    }

    @media (prefers-reduced-motion: reduce) {
        .sankey-live-sync__stream {
            animation: none;
        }
    }
</style>
