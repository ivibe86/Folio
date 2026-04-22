<script>
    import '$lib/styles/transactions.css';
    import { onMount } from 'svelte';
    import { api, invalidateCache } from '$lib/api.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import {
        formatCurrency, formatDate, formatDayHeader,
        getCurrentMonth, formatMonth,
        groupTransactionsByDate, CATEGORY_COLORS, CATEGORY_ICONS
    } from '$lib/utils.js';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';

    let transactions = [];
    let totalCount = 0;
    let pageLimit = 50;
    let pageOffset = 0;
    let allCategories = [];
    let loading = true;
    let profileSwitching = false;
    let search = '';
    let filterMonth = getCurrentMonth();
    let filterCategory = '';
    let filterAccount = '';
    let editingTxId = null;
    let months = [];
    let accountNames = [];

    // —— Period selector state (mirrored from Dashboard) ——
    let selectedPeriod = 'this_month';
    let selectedCustomMonth = getCurrentMonth();
    let monthDropdownOpen = false;

    const periodOptions = [
        { key: 'this_month', label: 'This Month' },
        { key: 'last_month', label: 'Last Month' },
        { key: 'ytd',        label: 'YTD' },
        { key: 'all',        label: 'All Time' }
    ];
    $: activePeriodIdx = Math.max(periodOptions.findIndex(p => p.key === selectedPeriod), 0);

    function getMonthForPeriod(period) {
        switch (period) {
            case 'this_month': return getCurrentMonth();
            case 'last_month': {
                const now = new Date();
                const lm = new Date(now.getFullYear(), now.getMonth() - 1, 1);
                return `${lm.getFullYear()}-${String(lm.getMonth() + 1).padStart(2, '0')}`;
            }
            case 'custom': return selectedCustomMonth;
            default: return null; // 'all' and 'ytd' handled in filter
        }
    }

    function handlePeriodChange(key) {
        selectedPeriod = key;
        // Sync filterMonth based on period selection
        if (key === 'this_month' || key === 'last_month') {
            filterMonth = getMonthForPeriod(key);
        } else if (key === 'custom') {
            filterMonth = selectedCustomMonth;
        } else if (key === 'ytd') {
            // filterMonth will be handled in the reactive filter below
            filterMonth = '__ytd__';
        } else {
            // 'all'
            filterMonth = '';
        }
    }

    function handleCustomMonthSelect(m) {
        selectedCustomMonth = m;
        monthDropdownOpen = false;
        selectedPeriod = 'custom';
        filterMonth = m;
    }

    // —— Custom filter dropdown state ——
    let monthPickerOpen = false;
    let categoryPickerOpen = false;
    let accountPickerOpen = false;


    function openFilter(which) {
        monthPickerOpen    = which === 'month';
        categoryPickerOpen = which === 'category';
        accountPickerOpen  = which === 'account';
    }

    function closeAllFilters() {
        monthPickerOpen = false;
        categoryPickerOpen = false;
        accountPickerOpen = false;
    }

    function handleWindowClick() {
        closeAllFilters();
        monthDropdownOpen = false;
        // Close category re-tag dropdown if open (but don't cancel full edit)
        if (catDropdownOpenForTx) {
            catDropdownOpenForTx = null;
            catDropdownSearch = '';
        }
    }

    // New category creation
    let creatingNewCategory = false;
    let newCategoryName = '';
    let newCategoryError = '';

    // ── Category re-tag dropdown state ──
    let catDropdownOpenForTx = null;   // original_id of tx whose dropdown is open
    let catDropdownSearch = '';         // search/filter within the dropdown
    let categoryApplyMode = 'always';

    // Recategorization feedback
    let recentlyUpdatedTxId = null;
    let updateFeedback = '';

    // Subscription declaration prompt state
    let subscriptionPromptTxId = null;
    let subscriptionPromptMerchant = '';
    let subscriptionPromptAmount = 0;
    let subscriptionPromptFrequency = '';
    let subscriptionDeclareLoading = false;

    const frequencyOptions = [
        { key: 'monthly', label: 'Monthly' },
        { key: 'quarterly', label: 'Quarterly' },
        { key: 'annual', label: 'Annual' },
    ];

    async function handleDeclareSubscription(frequency) {
        if (!subscriptionPromptMerchant || subscriptionDeclareLoading) return;
        subscriptionDeclareLoading = true;
        try {
            const profile = $activeProfile && $activeProfile !== 'household' ? $activeProfile : null;
            await api.declareSubscription(subscriptionPromptMerchant, subscriptionPromptAmount, frequency, profile);
            updateFeedback = `✓ Tracking ${subscriptionPromptMerchant} as ${frequency} subscription`;
            recentlyUpdatedTxId = subscriptionPromptTxId;
            setTimeout(() => {
                if (recentlyUpdatedTxId === subscriptionPromptTxId) {
                    recentlyUpdatedTxId = null;
                    updateFeedback = '';
                }
            }, 4000);
        } catch (e) {
            console.error('Failed to declare subscription:', e);
            updateFeedback = 'Failed to declare subscription';
            setTimeout(() => { updateFeedback = ''; }, 3000);
        } finally {
            subscriptionDeclareLoading = false;
            dismissSubscriptionPrompt();
        }
    }

    function dismissSubscriptionPrompt() {
        subscriptionPromptTxId = null;
        subscriptionPromptMerchant = '';
        subscriptionPromptAmount = 0;
        subscriptionPromptFrequency = '';
    }

    onMount(async () => {
        const handleSyncComplete = async (event) => {
            const detail = event?.detail || {};
            if (detail.status && detail.status !== 'completed') return;
            if (!['enrollment', 'manual-sync', 'simplefin'].includes(detail.source)) return;

            try {
                invalidateCache();
                await fetchTransactions();

                const allResult = await api.getTransactions({ limit: 1000 });
                const allTxns = allResult.data || [];
                const monthSet = new Set(allTxns.map(t => t.date?.substring(0, 7)).filter(Boolean));
                months = [...monthSet].sort().reverse();
                accountNames = [...new Set(allTxns.map(t => t.account_name).filter(Boolean))].sort();
            } catch (e) {
                console.error('Failed to refresh transactions after sync:', e);
            }
        };

        window.addEventListener('folio:sync-complete', handleSyncComplete);
        try {
            const [result, cats] = await Promise.all([
                api.getTransactions({ limit: pageLimit, offset: 0 }),
                api.getCategories()
            ]);
            transactions = result.data;
            totalCount = result.total_count;
            pageOffset = 0;
            allCategories = cats;

            // Fetch all months and accounts for filter dropdowns (lightweight metadata query)
            const allResult = await api.getTransactions({ limit: 1000 });
            const allTxns = allResult.data;
            const monthSet = new Set(allTxns.map(t => t.date?.substring(0, 7)).filter(Boolean));
            months = [...monthSet].sort().reverse();
            if (months.length > 0 && selectedCustomMonth === getCurrentMonth()) {
                selectedCustomMonth = months[0];
            }
            const accSet = new Set(allTxns.map(t => t.account_name).filter(Boolean));
            accountNames = [...accSet].sort();
        } catch (e) {
            console.error('Failed to load transactions:', e);
        } finally {
            loading = false;
        }

        return () => {
            window.removeEventListener('folio:sync-complete', handleSyncComplete);
        };
    });

    async function fetchTransactions() {
        try {
            const params = { limit: pageLimit, offset: pageOffset };
            if (filterMonth === '__ytd__') {
                // YTD: no month filter, handled client-side below
                // Actually, for pagination to work with YTD, we need server-side.
                // Approximate: fetch current year with high limit
                params.limit = 1000;
                params.offset = 0;
            } else if (filterMonth) {
                params.month = filterMonth;
            }
            if (filterCategory) params.category = filterCategory;
            if (filterAccount) params.account = filterAccount;
            if (search) params.search = search;

            const result = await api.getTransactions(params);
            transactions = result.data;
            totalCount = result.total_count;

            // For YTD, do client-side year filter on the result
            if (filterMonth === '__ytd__') {
                const year = new Date().getFullYear().toString();
                transactions = transactions.filter(t => t.date?.startsWith(year));
                totalCount = transactions.length;
            }
        } catch (e) {
            console.error('Failed to fetch transactions:', e);
        }
    }

    function goToPage(page) {
        pageOffset = page * pageLimit;
        fetchTransactions();
        // Scroll to top of transactions list
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    function nextPage() {
        if (pageOffset + pageLimit < totalCount) {
            pageOffset += pageLimit;
            fetchTransactions();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    function prevPage() {
        if (pageOffset > 0) {
            pageOffset = Math.max(0, pageOffset - pageLimit);
            fetchTransactions();
            window.scrollTo({ top: 0, behavior: 'smooth' });
        }
    }

    $: currentPage = Math.floor(pageOffset / pageLimit);
    $: totalPages = Math.ceil(totalCount / pageLimit);

    // Server-side filtering: re-fetch when filters change
    let _prevFilterKey = '';
    let _searchDebounce = null;

    $: {
        // Build a key from all filter values to detect changes
        const filterKey = `${filterMonth}|${filterCategory}|${filterAccount}`;

        if (!loading && filterKey !== _prevFilterKey) {
            _prevFilterKey = filterKey;
            pageOffset = 0;
            fetchTransactions();
        }
    }

    // Debounced search (separate from other filters since it changes per keystroke)
    $: if (!loading && search !== undefined) {
        if (_searchDebounce) clearTimeout(_searchDebounce);
        _searchDebounce = setTimeout(() => {
            pageOffset = 0;
            fetchTransactions();
        }, 300);
    }

    // Transactions are already server-filtered
    $: filteredTxns = transactions;

    // Transfer types excluded from accrual-basis totals (same model as dashboard)
    const EXCLUDED_EXPENSE_TYPES = new Set(['transfer_internal', 'transfer_cc_payment', 'transfer_household']);
    const NON_SPENDING_CATEGORIES = new Set(['Savings Transfer', 'Personal Transfer', 'Credit Card Payment', 'Income']);

    $: groupedTxns = groupTransactionsByDate(filteredTxns);
    $: totalSpending = filteredTxns
        .filter(t => {
            const amount = parseFloat(t.amount);
            if (amount >= 0) return false;
            // Exclude non-spending categories (same as dashboard)
            if (NON_SPENDING_CATEGORIES.has(t.category)) return false;
            // Exclude internal and CC payment transfers
            if (t.expense_type && EXCLUDED_EXPENSE_TYPES.has(t.expense_type)) return false;
            return true;
        })
        .reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
    $: totalIncome = filteredTxns
        .filter(t => {
            const amount = parseFloat(t.amount);
            if (amount <= 0) return false;
            // Only count actual income
            if (t.category !== 'Income') return false;
            // Exclude internal transfers showing as income
            if (t.expense_type && EXCLUDED_EXPENSE_TYPES.has(t.expense_type)) return false;
            return true;
        })
        .reduce((s, t) => s + parseFloat(t.amount), 0);
    $: txCcRepaid = filteredTxns
        .filter(t => t.category === 'Credit Card Payment' && parseFloat(t.amount) < 0)
        .reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
    $: txExternalTransfers = filteredTxns
        .filter(t => t.expense_type === 'transfer_external' && parseFloat(t.amount) < 0)
        .reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
    $: txNetFlow = totalIncome - totalSpending - txExternalTransfers;
    $: topCategory = (() => {
        const cats = {};
        filteredTxns.filter(t => {
            const amount = parseFloat(t.amount);
            if (amount >= 0) return false;
            if (NON_SPENDING_CATEGORIES.has(t.category)) return false;
            if (t.expense_type && EXCLUDED_EXPENSE_TYPES.has(t.expense_type)) return false;
            return true;
        }).forEach(t => {
            if (t.category) {
                cats[t.category] = (cats[t.category] || 0) + Math.abs(parseFloat(t.amount));
            }
        });
        const sorted = Object.entries(cats).sort((a, b) => b[1] - a[1]);
        return sorted[0] ? { name: sorted[0][0], total: sorted[0][1] } : null;
    })();

    async function updateCategory(txId, newCategory, oneOff = false) {
        try {
            const result = await api.updateCategory(txId, newCategory, oneOff);
            const tx = transactions.find(t => t.original_id === txId);
            if (tx) {
                tx.category = newCategory;
                tx.confidence = 'manual';
                tx.categorization_source = 'user';
                transactions = transactions;
            }

            // Check if backend suggests subscription tracking
            if (result && result.subscription_prompt) {
                subscriptionPromptTxId = txId;
                subscriptionPromptMerchant = result.merchant || tx?.description || '';
                subscriptionPromptAmount = result.amount || Math.abs(parseFloat(tx?.amount || 0));
            }

            // Show feedback — one-off gets a distinct message
            recentlyUpdatedTxId = txId;
            const retro = result?.retroactive_count ?? 0;
            updateFeedback = oneOff
                ? `Categorized as "${newCategory}" — this transaction only`
                : retro > 0
                    ? `Categorized as "${newCategory}" — updated ${retro} similar transaction${retro !== 1 ? 's' : ''}`
                    : `Categorized as "${newCategory}"`;

            // Invalidate cache since category rules may have changed
            invalidateCache();

            // Refresh transactions list so retroactively updated rows are visible immediately
            try {
                await fetchTransactions();
            } catch (_) {}

            // Refresh categories list in case a new one was created
            try {
                allCategories = await api.getCategories();
            } catch (_) {}

            // Clear feedback after a delay
            setTimeout(() => {
                if (recentlyUpdatedTxId === txId) {
                    recentlyUpdatedTxId = null;
                    updateFeedback = '';
                }
            }, 4000);
        } catch (e) {
            console.error('Failed to update category:', e);
            updateFeedback = 'Failed to update category';
            setTimeout(() => { updateFeedback = ''; }, 3000);
        }
        categoryApplyMode = 'always';
        editingTxId = null;
        creatingNewCategory = false;
        newCategoryName = '';
    }

    async function createAndApplyCategory(txId) {
        const name = newCategoryName.trim();
        if (!name) {
            newCategoryError = 'Category name cannot be empty';
            return;
        }

        // Check if it already exists (case-insensitive)
        if (allCategories.some(c => c.toLowerCase() === name.toLowerCase())) {
            const existing = allCategories.find(c => c.toLowerCase() === name.toLowerCase());
            creatingNewCategory = false;
            newCategoryName = '';
            newCategoryError = '';
            if (existing) {
                await updateCategory(txId, existing, categoryApplyMode === 'once');
            }
            return;
        }

        try {
            await api.createCategory(name);
            allCategories = [...allCategories, name].sort();
            creatingNewCategory = false;
            newCategoryName = '';
            await updateCategory(txId, name, categoryApplyMode === 'once');
        } catch (e) {
            newCategoryError = 'Failed to create category';
            console.error(e);
        }
    }

    function startEditing(txId) {
        editingTxId = txId;
        catDropdownOpenForTx = txId;
        catDropdownSearch = '';
        categoryApplyMode = 'always';
        creatingNewCategory = false;
        newCategoryName = '';
        newCategoryError = '';
    }

    function cancelEditing() {
        editingTxId = null;
        catDropdownOpenForTx = null;
        catDropdownSearch = '';
        creatingNewCategory = false;
        newCategoryName = '';
        newCategoryError = '';
        categoryApplyMode = 'always';
    }

    // Filtered category list for the re-tag dropdown search
    $: filteredEditCategories = catDropdownSearch
        ? allCategories.filter(c => c.toLowerCase().includes(catDropdownSearch.toLowerCase()))
        : allCategories;

    function clearFilters() {
        search = '';
        selectedPeriod = 'this_month';
        filterMonth = getCurrentMonth();
        selectedCustomMonth = getCurrentMonth();
        filterCategory = '';
        filterAccount = '';
        pageOffset = 0;
        // fetchTransactions() will be triggered by the reactive filter block
    }

    $: hasActiveFilters = search || filterCategory || filterAccount || selectedPeriod !== 'this_month';

    function formatDayHeaderFull(dateStr) {
        const base = formatDayHeader(dateStr);
        if ((selectedPeriod === 'all' || selectedPeriod === 'ytd') && base !== 'Today' && base !== 'Yesterday' && dateStr) {
            return `${base}, ${dateStr.substring(0, 4)}`;
        }
        return base;
    }

    function getMerchantMetaLine(tx) {
        const description = (tx.description || '').trim();
        const merchantDisplay = (tx.merchant_display_name || '').trim();
        const merchantName = (tx.merchant_name || '').trim();
        const accountName = (tx.account_name || '').trim();
        const parts = [];

        const merchantLabel = merchantDisplay || merchantName;
        if (merchantLabel && merchantLabel.toUpperCase() !== description.toUpperCase()) {
            parts.push(merchantLabel);
        }
        if (accountName && accountName !== merchantLabel) {
            parts.push(accountName);
        }

        return parts.join(' · ');
    }

    /**
     * Get a display label for the categorization source.
     */
    function getSourceLabel(tx) {
        const source = tx.categorization_source || '';
        const confidence = tx.confidence || '';

        if (source === 'user' || confidence === 'manual') return { label: 'Manual', type: 'manual' };
        if (source === 'user-rule') return { label: 'Auto-rule', type: 'auto-rule' };
        if (source === 'rule-high') return { label: 'Rule', type: 'rule' };
        if (source === 'llm') return { label: 'AI', type: 'ai' };
        if (source === 'fallback') return { label: 'Fallback', type: 'fallback' };
        return null;
    }

    // Profile switch: reload transactions
    let _prevTxProfile = null;
    $: if ($activeProfile && $activeProfile !== _prevTxProfile) {
        if (_prevTxProfile !== null) {
            reloadTransactionsForProfile();
        }
        _prevTxProfile = $activeProfile;
    }

    async function reloadTransactionsForProfile() {
        profileSwitching = true;
        try {
            const [result, cats, metaResult] = await Promise.all([
                api.getTransactions({ limit: pageLimit, offset: 0 }),
                api.getCategories(),
                api.getTransactions({ limit: 1000 })
            ]);
            transactions = result.data;
            totalCount = result.total_count;
            pageOffset = 0;
            allCategories = cats;

            const allTxns = metaResult.data;
            const monthSet = new Set(allTxns.map(t => t.date?.substring(0, 7)).filter(Boolean));
            months = [...monthSet].sort().reverse();
            const accSet = new Set(allTxns.map(t => t.account_name).filter(Boolean));
            accountNames = [...accSet].sort();
            if (months.length > 0 && !months.includes(selectedCustomMonth)) {
                selectedCustomMonth = months[0];
            }
            handlePeriodChange(selectedPeriod);
            filterCategory = '';
            filterAccount = '';
            search = '';
        } catch (e) {
            console.error('Failed to reload transactions for profile:', e);
        } finally {
            profileSwitching = false;
        }
    }
</script>

<svelte:window on:click={handleWindowClick} />
<div class="profile-transition" class:profile-loading={profileSwitching}>
<div class="flex items-start justify-between mb-6 fade-in">
    <div>
        <h2 class="text-2xl md:text-3xl font-extrabold font-display" style="color: var(--text-primary)">Transactions</h2>
        <p class="text-[12px] mt-1" style="color: var(--text-muted)">
            {#if selectedPeriod === 'all'}All time{:else if selectedPeriod === 'ytd'}YTD {new Date().getFullYear()}{:else if selectedPeriod === 'custom'}{formatMonth(selectedCustomMonth)}{:else}{formatMonth(getMonthForPeriod(selectedPeriod))}{/if} · {totalCount} transactions{#if totalCount > pageLimit} (showing {pageOffset + 1}–{Math.min(pageOffset + pageLimit, totalCount)}){/if}
        </p>
    </div>
    <ProfileSwitcher />
</div>

<!-- Update feedback toast -->
{#if updateFeedback}
    <div class="tx-feedback-toast fade-in">
        <span class="material-symbols-outlined text-[16px]" style="color: var(--positive)">check_circle</span>
        <span class="text-[12px] font-medium" style="color: var(--text-primary)">{updateFeedback}</span>
    </div>
{/if}

<!-- SUMMARY STRIP -->
<div class="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-5 fade-in-up" style="animation-delay: 60ms">
    <div class="card" style="padding: 0.75rem 1rem">
        <p class="stat-label">Income</p>
        <p class="text-base font-bold font-mono mt-0.5 text-positive">{formatCurrency(totalIncome)}</p>
    </div>
    <div class="card" style="padding: 0.75rem 1rem">
        <p class="stat-label">Spending</p>
        <p class="text-base font-bold font-mono mt-0.5 text-negative">{formatCurrency(totalSpending)}</p>
    </div>
    <div class="card" style="padding: 0.75rem 1rem; opacity: 0.7">
        <p class="stat-label" style="display: flex; align-items: center; gap: 4px;">
            Ext. Transfers
            <span class="material-symbols-outlined text-[12px]" style="color: var(--text-muted); opacity: 0.6" title="Transfers to people outside your accounts (Zelle, Venmo, etc). Counted in Net Flow.">info</span>
        </p>
        <p class="text-base font-bold font-mono mt-0.5" style="color: var(--warning)">{formatCurrency(txExternalTransfers)}</p>
    </div>
    <div class="card" style="padding: 0.75rem 1rem; opacity: 0.7">
        <p class="stat-label" style="display: flex; align-items: center; gap: 4px;">
            CC Repaid
            <span class="material-symbols-outlined text-[12px]" style="color: var(--text-muted); opacity: 0.6" title="Payments toward prior credit card charges. Not counted as new spending.">info</span>
        </p>
        <p class="text-base font-bold font-mono mt-0.5" style="color: var(--text-muted)">{formatCurrency(txCcRepaid)}</p>
    </div>
    <div class="card" style="padding: 0.75rem 1rem">
        <p class="stat-label">Net Flow</p>
        <p class="text-base font-bold font-mono mt-0.5" style="color: {txNetFlow >= 0 ? 'var(--positive)' : 'var(--negative)'}">
            {txNetFlow >= 0 ? '+' : ''}{formatCurrency(txNetFlow)}
        </p>
    </div>
</div>

<!-- PERIOD SELECTOR + FILTERS -->
<div class="flex flex-col gap-3 mb-5 fade-in-up" style="animation-delay: 100ms; position: relative; z-index: 10;">
    <!-- Row 1: Period toggle + Month dropdown + Category + Account filters -->
    <div class="flex flex-wrap items-center gap-3 txn-period-row">
        <div class="period-toggle-track" style="--seg-count: {periodOptions.length}; --active-idx: {activePeriodIdx};">
            <div class="period-toggle-thumb"></div>
            {#each periodOptions as p}
                <button class="period-toggle-label" class:active={selectedPeriod === p.key}
                    on:click={() => handlePeriodChange(p.key)}>
                    {p.label}
                </button>
            {/each}
        </div>
        <div class="month-dropdown-wrapper">
            <button
                class="month-dropdown-trigger"
                class:ring-2={selectedPeriod === 'custom'}
                class:ring-accent={selectedPeriod === 'custom'}
                on:click|stopPropagation={() => { monthDropdownOpen = !monthDropdownOpen; closeAllFilters(); }}
            >
                <span>{formatMonth(selectedCustomMonth)}</span>
                <span class="material-symbols-outlined text-[13px]"
                      style="opacity: 0.5; transition: transform 0.2s;"
                      class:rotate-180={monthDropdownOpen}>
                    expand_more
                </span>
            </button>

            {#if monthDropdownOpen}
                <div class="month-dropdown-backdrop" on:click={() => monthDropdownOpen = false}></div>
                <div class="month-dropdown-menu" role="listbox" style="bottom: auto; top: calc(100% + 6px);">
                    {#each months as m}
                        <button
                            class="month-dropdown-item"
                            class:month-dropdown-item-active={selectedCustomMonth === m && selectedPeriod === 'custom'}
                            role="option"
                            aria-selected={selectedCustomMonth === m && selectedPeriod === 'custom'}
                            on:click|stopPropagation={() => handleCustomMonthSelect(m)}
                        >
                            {formatMonth(m)}
                        </button>
                    {/each}
                </div>
            {/if}
        </div>

        <!-- Category Filter Pill -->
        <div class="relative" style="z-index: 51">
            <button class="txn-filter-pill"
                class:filter-active={filterCategory !== ''}
                on:click|stopPropagation={() => { openFilter(categoryPickerOpen ? '' : 'category'); monthDropdownOpen = false; }}>
                <span class="text-[12px] font-medium" style="color: var(--text-primary)">
                    {filterCategory || 'All Categories'}
                </span>
                <span class="material-symbols-outlined text-[16px]"
                    style="color: var(--text-muted); transition: transform 0.2s;"
                    class:txn-chevron-open={categoryPickerOpen}>
                    expand_more
                </span>
            </button>
            {#if categoryPickerOpen}
                <div class="txn-filter-dropdown" on:click|stopPropagation>
                    <button
                        class="txn-filter-option"
                        class:active={filterCategory === ''}
                        on:click={() => { filterCategory = ''; categoryPickerOpen = false; }}>
                        <span class="txn-filter-option-label">
                            <span class="material-symbols-outlined" style="color: var(--text-muted)">category</span>
                            <span>All Categories</span>
                        </span>
                        {#if filterCategory === ''}
                            <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">check</span>
                        {/if}
                    </button>
                    {#each allCategories as cat}
                        <button
                            class="txn-filter-option"
                            class:active={cat === filterCategory}
                            on:click={() => { filterCategory = cat; categoryPickerOpen = false; }}>
                            <span class="txn-filter-option-label">
                                <span class="material-symbols-outlined" style="color: {CATEGORY_COLORS[cat] || 'var(--text-muted)'}">
                                    {CATEGORY_ICONS[cat] || 'label'}
                                </span>
                                <span>{cat}</span>
                            </span>
                            {#if cat === filterCategory}
                                <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">check</span>
                            {/if}
                        </button>
                    {/each}
                </div>
            {/if}
        </div>

        <!-- Account Filter Pill -->
        <div class="relative" style="z-index: 50">
            <button class="txn-filter-pill"
                class:filter-active={filterAccount !== ''}
                on:click|stopPropagation={() => { openFilter(accountPickerOpen ? '' : 'account'); monthDropdownOpen = false; }}>
                <span class="text-[12px] font-medium" style="color: var(--text-primary)">
                    {filterAccount || 'All Accounts'}
                </span>
                <span class="material-symbols-outlined text-[16px]"
                    style="color: var(--text-muted); transition: transform 0.2s;"
                    class:txn-chevron-open={accountPickerOpen}>
                    expand_more
                </span>
            </button>
            {#if accountPickerOpen}
                <div class="txn-filter-dropdown" on:click|stopPropagation>
                    <button
                        class="txn-filter-option"
                        class:active={filterAccount === ''}
                        on:click={() => { filterAccount = ''; accountPickerOpen = false; }}>
                        All Accounts
                        {#if filterAccount === ''}
                            <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">check</span>
                        {/if}
                    </button>
                    {#each accountNames as acc}
                        <button
                            class="txn-filter-option"
                            class:active={acc === filterAccount}
                            on:click={() => { filterAccount = acc; accountPickerOpen = false; }}>
                            {acc}
                            {#if acc === filterAccount}
                                <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">check</span>
                            {/if}
                        </button>
                    {/each}
                </div>
            {/if}
        </div>

        {#if hasActiveFilters}
            <button on:click={() => { clearFilters(); closeAllFilters(); monthDropdownOpen = false; }}
                class="flex items-center gap-1 px-3 py-2.5 rounded-xl text-[11px] font-medium transition-all hover:opacity-80"
                style="background: var(--surface-100); color: var(--text-secondary)">
                <span class="material-symbols-outlined text-[14px]">close</span>
                Reset
            </button>
        {/if}
    </div>

    <!-- Row 2: Search bar only (standalone) -->
    <div class="flex items-center">
        <div class="relative w-full" style="max-width: 600px">
            <span class="material-symbols-outlined absolute left-3 top-1/2 -translate-y-1/2 text-[18px]" style="color: var(--text-muted)">search</span>
            <input bind:value={search} type="text" placeholder="Search transactions..."
                class="w-full pl-10 pr-4 py-2.5 rounded-xl text-sm focus:ring-2 focus:ring-accent/50 transition-all"
                style="background: var(--card-bg); color: var(--text-primary); border: 1px solid var(--card-border)" />
        </div>
    </div>
</div>

<!-- TRANSACTIONS (grouped by day) -->
{#if loading}
    <div class="space-y-3">
        {#each Array(6) as _}
            <div class="skeleton h-14 rounded-xl"></div>
        {/each}
    </div>
{:else}
    <div class="card tx-ledger-card fade-in-up" style="padding: 0; animation-delay: 140ms; overflow: visible;">
        {#if groupedTxns.length > 0}
            <div class="tx-column-headers">
                <span>Description</span>
                <span class="tx-col-header-category">Category</span>
                <span class="tx-col-header-amount">Amount</span>
            </div>
        {/if}
        {#if groupedTxns.length === 0}
            <div class="text-center py-16" style="color: var(--text-muted)">
                <span class="material-symbols-outlined text-5xl mb-3" style="opacity: 0.4">search_off</span>
                <p class="text-sm font-medium">No transactions match your filters</p>
                <p class="text-[11px] mt-1">Try adjusting the month, category, or search term</p>
            </div>
        {:else}
            {#each groupedTxns as [date, txns], gi}
                {@const daySpent = txns.filter(t => parseFloat(t.amount) < 0).reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0)}
                {@const dayIncome = txns.filter(t => parseFloat(t.amount) > 0).reduce((s, t) => s + parseFloat(t.amount), 0)}

                <div class="day-header tx-ledger-day-header" style="{gi > 0 ? 'border-top: 1px solid rgba(78, 88, 108, 0.16);' : ''}">
                    <span class="text-[11px] font-semibold" style="color: var(--text-primary)">{formatDayHeaderFull(date)}</span>
                    <div class="flex items-center gap-3">
                        {#if dayIncome > 0}
                            <span class="text-[10px] font-mono font-medium text-positive">+{formatCurrency(dayIncome, 2)}</span>
                        {/if}
                        {#if daySpent > 0}
                            <span class="text-[10px] font-mono font-medium text-negative">-{formatCurrency(daySpent, 2)}</span>
                        {/if}
                    </div>
                </div>

                {#each txns as tx (tx.original_id)}
                    {@const amount = parseFloat(tx.amount)}
                    {@const sourceInfo = getSourceLabel(tx)}
                    {@const isRecentlyUpdated = recentlyUpdatedTxId === tx.original_id}
                    <div class="tx-row-grid group transition-colors tx-row tx-ledger-row"
                        class:tx-row-updated={isRecentlyUpdated}>

                        <!-- Zone 1: Icon + Description + Account -->
                        <div class="tx-zone-desc">
                            <div class="w-8 h-8 rounded-xl flex items-center justify-center flex-shrink-0"
                                style="background: color-mix(in srgb, {CATEGORY_COLORS[tx.category] || '#627d98'} 8%, transparent)">
                                <span class="material-symbols-outlined text-[16px]"
                                    style="color: {CATEGORY_COLORS[tx.category] || '#627d98'}">
                                    {CATEGORY_ICONS[tx.category] || 'label'}
                                </span>
                            </div>
                            <div class="min-w-0 flex-1">
                                <p class="text-[13px] font-medium truncate" style="color: var(--text-primary)">
                                    {tx.description || '—'}
                                </p>
                                <span class="text-[10px] mt-0.5 block" style="color: var(--text-muted)">{getMerchantMetaLine(tx)}</span>
                            </div>
                        </div>

                        <!-- Zone 2: Category pill + source badge -->
                        <div class="tx-zone-category">
                            <div class="relative tx-cat-pill-wrapper">
                                <button
                                    class="tx-cat-pill"
                                    class:tx-cat-pill-editing={editingTxId === tx.original_id}
                                    on:click|stopPropagation={() => {
                                        if (editingTxId === tx.original_id) {
                                            catDropdownOpenForTx = catDropdownOpenForTx === tx.original_id ? null : tx.original_id;
                                        } else {
                                            startEditing(tx.original_id);
                                        }
                                    }}
                                    style="--pill-color: {CATEGORY_COLORS[tx.category] || '#627d98'}">
                                    <span class="material-symbols-outlined text-[13px]" style="color: var(--pill-color)">
                                        {CATEGORY_ICONS[tx.category] || 'label'}
                                    </span>
                                    <span class="tx-cat-pill-label">{tx.category || 'Uncategorized'}</span>
                                    <span class="material-symbols-outlined text-[12px] tx-cat-pill-chevron"
                                        class:txn-chevron-open={catDropdownOpenForTx === tx.original_id}
                                        style="color: var(--text-muted); opacity: 0.5;">
                                        expand_more
                                    </span>
                                </button>

                                {#if catDropdownOpenForTx === tx.original_id}
                                    <div class="txn-filter-dropdown tx-cat-dropdown" on:click|stopPropagation>
                                        <div class="tx-cat-apply-toggle">
                                            <div class="tx-cat-apply-toggle-copy">
                                                <span class="tx-cat-apply-toggle-label">Apply</span>
                                            </div>
                                            <div class="tx-cat-apply-toggle-actions">
                                                <button
                                                    class="tx-cat-apply-mode-pill"
                                                    class:tx-cat-apply-mode-pill--active={categoryApplyMode === 'always'}
                                                    on:click={() => { categoryApplyMode = 'always'; }}>
                                                    Always
                                                </button>
                                                <button
                                                    class="tx-cat-apply-mode-pill"
                                                    class:tx-cat-apply-mode-pill--active={categoryApplyMode === 'once'}
                                                    on:click={() => { categoryApplyMode = 'once'; }}>
                                                    Just once
                                                </button>
                                            </div>
                                        </div>
                                        <div class="tx-cat-dropdown-search-wrap">
                                            <span class="material-symbols-outlined text-[14px]" style="color: var(--text-muted)">search</span>
                                            <input
                                                bind:value={catDropdownSearch}
                                                placeholder="Search categories..."
                                                class="tx-cat-dropdown-search"
                                                on:keydown={(e) => {
                                                    if (e.key === 'Escape') cancelEditing();
                                                }}
                                            />
                                        </div>
                                        <div class="tx-cat-dropdown-list">
                                            {#each filteredEditCategories as cat}
                                                <button
                                                    class="txn-filter-option"
                                                    class:active={cat === tx.category}
                                                    on:click={() => {
                                                        if (cat !== tx.category) {
                                                            updateCategory(tx.original_id, cat, categoryApplyMode === 'once');
                                                        } else {
                                                            cancelEditing();
                                                        }
                                                    }}>
                                                    <span class="txn-filter-option-label">
                                                        <span class="material-symbols-outlined" style="color: {CATEGORY_COLORS[cat] || 'var(--text-muted)'}">
                                                            {CATEGORY_ICONS[cat] || 'label'}
                                                        </span>
                                                        <span>{cat}</span>
                                                    </span>
                                                    {#if cat === tx.category}
                                                        <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">check</span>
                                                    {/if}
                                                </button>
                                            {/each}
                                            {#if filteredEditCategories.length === 0 && catDropdownSearch}
                                                <div class="px-3 py-2 text-[11px]" style="color: var(--text-muted)">
                                                    No matching categories
                                                </div>
                                            {/if}
                                        </div>
                                        <div class="tx-cat-dropdown-footer">
                                            {#if creatingNewCategory}
                                                <div class="flex items-center gap-1.5 px-2 py-1.5">
                                                    <input
                                                        bind:value={newCategoryName}
                                                        placeholder="New category name..."
                                                        class="tx-cat-dropdown-new-input"
                                                        on:keydown={(e) => {
                                                            if (e.key === 'Enter') createAndApplyCategory(tx.original_id);
                                                            if (e.key === 'Escape') { creatingNewCategory = false; newCategoryName = ''; newCategoryError = ''; }
                                                        }}
                                                    />
                                                    <button
                                                        class="tx-edit-btn tx-edit-btn-confirm"
                                                        on:click={() => createAndApplyCategory(tx.original_id)}
                                                        disabled={!newCategoryName.trim()}>
                                                        <span class="material-symbols-outlined text-[13px]">check</span>
                                                    </button>
                                                </div>
                                                {#if newCategoryError}
                                                    <span class="text-[9px] px-3" style="color: var(--negative)">{newCategoryError}</span>
                                                {/if}
                                            {:else}
                                                <button
                                                    class="txn-filter-option tx-cat-create-btn"
                                                    on:click={() => { creatingNewCategory = true; }}>
                                                    <span class="txn-filter-option-label">
                                                        <span class="material-symbols-outlined" style="color: #8b5cf6">add_circle</span>
                                                        <span style="color: #8b5cf6; font-weight: 600;">Create new category</span>
                                                    </span>
                                                </button>
                                            {/if}
                                        </div>
                                    </div>
                                {/if}
                            </div>
                            {#if sourceInfo}
                                <span class="tx-source-badge tx-source-{sourceInfo.type}">{sourceInfo.label}</span>
                            {/if}
                        </div>

                        <!-- Zone 3: Amount (far right, terminal anchor) -->
                        <!-- Zone 3: Amount (far right, terminal anchor) -->
                        <div class="tx-zone-amount">
                            <p class="text-[13px] font-bold font-mono"
                                style="color: {amount >= 0 ? 'var(--positive)' : 'var(--text-primary)'}">
                                {amount >= 0 ? '+' : ''}{formatCurrency(amount, 2)}
                            </p>
                        </div>
                    </div>

                    <!-- Subscription Declaration Prompt -->
                    {#if subscriptionPromptTxId === tx.original_id}
                        <div class="tx-subscription-prompt fade-in" on:click|stopPropagation
                            style="border-bottom: 1px solid color-mix(in srgb, var(--card-border) 50%, transparent)">
                            <div class="tx-subscription-prompt-inner">
                                <div class="flex items-center gap-2.5 flex-1 min-w-0">
                                    <div class="w-7 h-7 rounded-lg flex items-center justify-center flex-shrink-0"
                                        style="background: color-mix(in srgb, var(--accent) 10%, transparent)">
                                        <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">event_repeat</span>
                                    </div>
                                    <div class="min-w-0">
                                        <p class="text-[12px] font-semibold" style="color: var(--text-primary)">
                                            Track <span style="color: var(--accent)">{subscriptionPromptMerchant}</span> as recurring?
                                        </p>
                                        <p class="text-[10px]" style="color: var(--text-muted)">
                                            {formatCurrency(subscriptionPromptAmount)} · Select frequency
                                        </p>
                                    </div>
                                </div>
                                <div class="flex items-center gap-1.5 flex-shrink-0">
                                    {#each frequencyOptions as freq}
                                        <button
                                            class="tx-subscription-freq-pill"
                                            class:tx-subscription-freq-active={subscriptionPromptFrequency === freq.key}
                                            disabled={subscriptionDeclareLoading}
                                            on:click|stopPropagation={() => handleDeclareSubscription(freq.key)}>
                                            {freq.label}
                                        </button>
                                    {/each}
                                    <button
                                        class="tx-subscription-dismiss-btn"
                                        on:click|stopPropagation={dismissSubscriptionPrompt}>
                                        <span class="material-symbols-outlined text-[14px]">close</span>
                                    </button>
                                </div>
                            </div>
                            {#if subscriptionDeclareLoading}
                                <div class="tx-subscription-loading">
                                    <div class="tx-subscription-loading-bar"></div>
                                </div>
                            {/if}
                        </div>
                    {/if}
                {/each}
            {/each}
        {/if}
    </div>
{/if}

    <!-- PAGINATION CONTROLS -->
    {#if totalPages > 1}
        <div class="flex items-center justify-between mt-4 px-1 fade-in">
            <p class="text-[11px] font-medium" style="color: var(--text-muted)">
                Showing {pageOffset + 1}–{Math.min(pageOffset + pageLimit, totalCount)} of {totalCount}
            </p>

            <div class="flex items-center gap-1.5">
                <button
                    on:click={prevPage}
                    disabled={pageOffset === 0}
                    class="pagination-btn"
                    class:pagination-btn-disabled={pageOffset === 0}>
                    <span class="material-symbols-outlined text-[16px]">chevron_left</span>
                </button>

                {#each Array(Math.min(totalPages, 7)) as _, i}
                    {@const pageNum = (() => {
                        // Show pages around current page
                        if (totalPages <= 7) return i;
                        if (currentPage < 4) return i;
                        if (currentPage > totalPages - 4) return totalPages - 7 + i;
                        return currentPage - 3 + i;
                    })()}
                    {#if pageNum >= 0 && pageNum < totalPages}
                        <button
                            on:click={() => goToPage(pageNum)}
                            class="pagination-btn"
                            class:pagination-btn-active={pageNum === currentPage}>
                            {pageNum + 1}
                        </button>
                    {/if}
                {/each}

                <button
                    on:click={nextPage}
                    disabled={pageOffset + pageLimit >= totalCount}
                    class="pagination-btn"
                    class:pagination-btn-disabled={pageOffset + pageLimit >= totalCount}>
                    <span class="material-symbols-outlined text-[16px]">chevron_right</span>
                </button>
            </div>

            <div class="flex items-center gap-2">
                <span class="text-[10px]" style="color: var(--text-muted)">Per page:</span>
                {#each [25, 50, 100] as size}
                    <button
                        on:click={() => { pageLimit = size; pageOffset = 0; fetchTransactions(); }}
                        class="text-[10px] px-2 py-1 rounded-lg transition-colors"
                        style="background: {pageLimit === size ? 'var(--accent)' : 'var(--surface-100)'}; color: {pageLimit === size ? 'white' : 'var(--text-muted)'}; font-weight: {pageLimit === size ? '700' : '500'}">
                        {size}
                    </button>
                {/each}
            </div>
        </div>
    {/if}
</div>
