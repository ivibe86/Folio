<script>
    import '$lib/styles/budget.css';
    import { onMount } from 'svelte';
    import { api } from '$lib/api.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import { formatCurrency, formatPercent, formatMonth, CATEGORY_COLORS, CATEGORY_ICONS } from '$lib/utils.js';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';

    let categories = [];
    let monthly = [];
    let loading = true;
    let profileSwitching = false;    
    let selectedMonth = '';
    let budgets = {};

    onMount(async () => {
        try {
            const [m, c, budgetResult] = await Promise.all([
                api.getMonthlyAnalytics(),
                api.getCategoryAnalytics(),
                api.getBudgets()
            ]);
            monthly = m;
            categories = Array.isArray(c) ? c : (c?.categories || []);
            budgets = Object.fromEntries((budgetResult?.items || []).map(item => [item.category, item.amount]));

            if (monthly.length > 0) {
                selectedMonth = monthly[monthly.length - 1].month;
                await loadMonth();
            }
        } catch (e) {
            console.error('Failed to load budgets:', e);
        } finally {
            loading = false;
        }
    });

    let monthCategories = [];

    async function loadMonth() {
        try {
            const result = await api.getCategoryAnalytics(selectedMonth);
            monthCategories = Array.isArray(result) ? result : (result?.categories || []);
        } catch (e) { console.error(e); }
    }

    $: if (selectedMonth) loadMonth();

    async function saveBudget(category, value) {
        const num = parseFloat(value);
        const nextBudgets = { ...budgets };
        if (!isNaN(num) && num > 0) nextBudgets[category] = num;
        else delete nextBudgets[category];
        budgets = nextBudgets;

        const profile = $activeProfile && $activeProfile !== 'household' ? $activeProfile : null;
        try {
            await api.updateBudget(category, !isNaN(num) && num > 0 ? num : null, profile);
        } catch (e) {
            console.error('Failed to save budget:', e);
            reloadBudgetsForProfile();
        }
    }

    $: budgetItems = monthCategories.map(cat => {
        const budget = budgets[cat.category] || 0;
        const spent = cat.total;
        const remaining = budget - spent;
        const percent = budget > 0 ? (spent / budget) * 100 : 0;
        const status = budget === 0 ? 'unset' : percent > 100 ? 'over' : percent > 80 ? 'warning' : 'good';
        return { ...cat, budget, spent, remaining, budgetPercent: percent, status };
    });

    let editingCategory = null;
    let editValue = '';

    function startEdit(cat, currentBudget) {
        editingCategory = cat;
        editValue = currentBudget > 0 ? currentBudget.toString() : '';
    }

    async function commitEdit(cat) {
        await saveBudget(cat, editValue);
        editingCategory = null;
    }

    // ââ Profile switch: reload budget data ââ
    let _prevBudgetProfile = null;
    $: if ($activeProfile && $activeProfile !== _prevBudgetProfile) {
        if (_prevBudgetProfile !== null) {
            reloadBudgetsForProfile();
        }
        _prevBudgetProfile = $activeProfile;
    }

    async function reloadBudgetsForProfile() {
        profileSwitching = true;
        try {
            const [m, c, budgetResult] = await Promise.all([
                api.getMonthlyAnalytics(),
                api.getCategoryAnalytics(),
                api.getBudgets()
            ]);
            monthly = m;
            categories = Array.isArray(c) ? c : (c?.categories || []);
            budgets = Object.fromEntries((budgetResult?.items || []).map(item => [item.category, item.amount]));
            if (monthly.length > 0) {
                const sorted = [...monthly].sort((a, b) => b.month.localeCompare(a.month));
                if (!sorted.some(s => s.month === selectedMonth)) {
                    selectedMonth = sorted[0].month;
                }
                await loadMonth();
            }
        } catch (e) {
            console.error('Failed to reload budgets for profile:', e);
        } finally {
            profileSwitching = false;
        }
    }

    $: totalBudget = budgetItems.reduce((s, b) => s + b.budget, 0);
    $: totalSpent = budgetItems.reduce((s, b) => s + b.spent, 0);
    $: totalRemaining = totalBudget - totalSpent;
</script>

{#if loading}
    <div class="space-y-6">
        <div class="skeleton h-8 w-32 rounded-xl"></div>
        <div class="grid grid-cols-3 gap-4">
            {#each Array(3) as _}
                <div class="skeleton h-20 rounded-xl"></div>
            {/each}
        </div>
        <div class="skeleton h-14 rounded-xl"></div>
        {#each Array(3) as _}
            <div class="skeleton h-24 rounded-xl"></div>
        {/each}
    </div>
{:else}
<div class="profile-transition" class:profile-loading={profileSwitching}>
    <div class="flex items-start justify-between mb-8 fade-in">
        <div>
            <h2 class="text-2xl md:text-3xl font-extrabold font-display" style="color: var(--text-primary)">Budgets</h2>
            <p class="text-[12px] mt-1" style="color: var(--text-muted)">Set limits, track spending by category</p>
        </div>
        <ProfileSwitcher />
    </div>

    <!-- Summary row -->
    <div class="grid grid-cols-3 gap-3 mb-6 fade-in-up" style="animation-delay: 60ms">
        <div class="card" style="padding: 0.875rem 1.125rem">
            <p class="stat-label">Total Budget</p>
            <p class="text-lg font-bold font-mono mt-1" style="color: var(--accent)">{formatCurrency(totalBudget)}</p>
        </div>
        <div class="card" style="padding: 0.875rem 1.125rem">
            <p class="stat-label">Total Spent</p>
            <p class="text-lg font-bold font-mono mt-1 text-negative">{formatCurrency(totalSpent)}</p>
        </div>
        <div class="card" style="padding: 0.875rem 1.125rem">
            <p class="stat-label">Remaining</p>
            <p class="text-lg font-bold font-mono mt-1"
                style="color: {totalRemaining >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                {formatCurrency(totalRemaining)}
            </p>
        </div>
    </div>

    <!-- Overall progress -->
    {#if totalBudget > 0}
        <div class="card mb-6 fade-in-up" style="animation-delay: 100ms">
            <div class="flex items-center justify-between mb-2">
                <span class="text-[12px] font-medium" style="color: var(--text-secondary)">Overall Utilization</span>
                <span class="text-[12px] font-mono font-bold"
                    style="color: {totalSpent/totalBudget > 1 ? 'var(--negative)' : totalSpent/totalBudget > 0.8 ? 'var(--warning)' : 'var(--positive)'}">
                    {formatPercent(Math.min(totalSpent / totalBudget * 100, 100))}
                </span>
            </div>
            <div class="w-full h-2.5 rounded-full overflow-hidden" style="background: var(--surface-200)">
                <div class="h-2.5 rounded-full transition-all duration-700"
                    style="width: {Math.min(totalSpent / totalBudget * 100, 100)}%;
                           background: {totalSpent/totalBudget > 1 ? 'var(--negative)' : totalSpent/totalBudget > 0.8 ? 'var(--warning)' : 'var(--positive)'}">
                </div>
            </div>
        </div>
    {/if}

    <!-- Category budgets -->
    <div class="flex items-center justify-between mb-3 fade-in">
        <p class="text-[9px] font-bold tracking-[0.2em] uppercase" style="color: var(--text-muted)">By Category</p>
        <select bind:value={selectedMonth}
            class="px-3 py-2 rounded-xl text-[12px] border-none"
            style="background: var(--card-bg); color: var(--text-primary); border: 1px solid var(--card-border)">
            {#each [...monthly].reverse() as m}
                <option value={m.month}>{formatMonth(m.month)}</option>
            {/each}
        </select>
    </div>
    <div class="space-y-2.5">
        {#each budgetItems as item, i}
            <div class="card fade-in" style="animation-delay: {140 + i * 35}ms; padding: 1rem 1.25rem">
                <div class="flex items-center justify-between mb-2.5">
                    <div class="flex items-center gap-3">
                        <div class="w-8 h-8 rounded-xl flex items-center justify-center"
                            style="background: color-mix(in srgb, {CATEGORY_COLORS[item.category] || '#627d98'} 10%, transparent)">
                            <span class="material-symbols-outlined text-[16px]"
                                style="color: {CATEGORY_COLORS[item.category] || '#627d98'}">
                                {CATEGORY_ICONS[item.category] || 'label'}
                            </span>
                        </div>
                        <div>
                            <p class="text-[13px] font-semibold" style="color: var(--text-primary)">{item.category}</p>
                            <p class="text-[10px] font-mono" style="color: var(--text-muted)">
                                {formatCurrency(item.spent)} spent
                            </p>
                        </div>
                    </div>

                    <div class="flex items-center gap-2">
                        {#if editingCategory === item.category}
                            <div class="flex items-center gap-1">
                                <span class="text-[11px]" style="color: var(--text-muted)">$</span>
                                <input bind:value={editValue} type="number"
                                    class="w-24 px-2 py-1 rounded-lg text-[12px] font-mono text-right border-none"
                                    style="background: var(--surface-200); color: var(--text-primary)"
                                    on:keydown={(e) => { if (e.key === 'Enter') commitEdit(item.category); }}
                                    on:blur={() => commitEdit(item.category)} />
                            </div>
                        {:else}
                            <button on:click={() => startEdit(item.category, item.budget)}
                                class="text-[12px] font-mono px-3 py-1 rounded-lg transition-colors hover:opacity-80"
                                style="background: var(--surface-200); color: {item.budget > 0 ? 'var(--text-primary)' : 'var(--text-muted)'}">
                                {item.budget > 0 ? formatCurrency(item.budget) : 'Set budget'}
                            </button>
                        {/if}

                        {#if item.status === 'over'}
                            <span class="text-[9px] font-bold px-2 py-0.5 rounded-full" style="background: var(--negative-light); color: var(--negative)">OVER</span>
                        {:else if item.status === 'warning'}
                            <span class="text-[9px] font-bold px-2 py-0.5 rounded-full" style="background: var(--warning-light); color: var(--warning)">80%+</span>
                        {:else if item.status === 'good'}
                            <span class="text-[9px] font-bold px-2 py-0.5 rounded-full" style="background: var(--positive-light); color: var(--positive)">OK</span>
                        {/if}
                    </div>
                </div>

                {#if item.budget > 0}
                    <div class="w-full h-1.5 rounded-full overflow-hidden" style="background: var(--surface-200)">
                        <div class="h-1.5 rounded-full transition-all duration-500"
                            style="width: {Math.min(item.budgetPercent, 100)}%;
                                   background: {item.status === 'over' ? 'var(--negative)' : item.status === 'warning' ? 'var(--warning)' : CATEGORY_COLORS[item.category] || 'var(--accent)'}">
                        </div>
                    </div>
                    <div class="flex justify-between mt-1.5">
                        <span class="text-[9px] font-mono" style="color: var(--text-muted)">{formatPercent(item.budgetPercent)}</span>
                        <span class="text-[9px] font-mono"
                            style="color: {item.remaining >= 0 ? 'var(--positive)' : 'var(--negative)'}">
                            {item.remaining >= 0 ? formatCurrency(item.remaining) + ' left' : formatCurrency(Math.abs(item.remaining)) + ' over'}
                        </span>
                    </div>
                {/if}
            </div>
        {/each}
    </div>

    <!-- Tip -->
    <div class="mt-8 card" style="border-color: var(--accent-border); background: var(--insight-gradient)">
        <p class="text-[11px] font-semibold mb-1" style="color: var(--accent)">💡 Tip</p>
        <p class="text-[12px]" style="color: var(--text-secondary)">
            Click "Set budget" on any category to define a monthly limit. Budgets are saved locally and persist between sessions.
        </p>
    </div>
</div>
{/if}
