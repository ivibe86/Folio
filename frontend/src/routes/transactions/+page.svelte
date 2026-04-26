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
    let summaryTransactions = [];
    let historyTransactions = [];
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
    let selectedTxId = null;
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
        { key: 'custom',     label: 'Custom' },
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

    async function fetchTransactionHistory() {
        let offset = 0;
        let all = [];
        let expectedTotal = null;

        do {
            const result = await api.getTransactions({ limit: 1000, offset });
            const page = result.data || [];
            all = all.concat(page);
            expectedTotal = result.total_count ?? all.length;
            offset += page.length;

            if (page.length === 0) break;
        } while (offset < expectedTotal);

        historyTransactions = all;
        return all;
    }

    onMount(async () => {
        const handleSyncComplete = async (event) => {
            const detail = event?.detail || {};
            if (detail.status && detail.status !== 'completed') return;
            if (!['enrollment', 'manual-sync', 'simplefin'].includes(detail.source)) return;

            try {
                invalidateCache();
                await fetchTransactions();
                await fetchSummaryTransactions();

                const allTxns = await fetchTransactionHistory();
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
            summaryTransactions = result.data || [];

            // Fetch all months and accounts for filter dropdowns (lightweight metadata query)
            const allTxns = await fetchTransactionHistory();
            const monthSet = new Set(allTxns.map(t => t.date?.substring(0, 7)).filter(Boolean));
            months = [...monthSet].sort().reverse();
            if (months.length > 0 && selectedCustomMonth === getCurrentMonth()) {
                selectedCustomMonth = months[0];
            }
            const accSet = new Set(allTxns.map(t => t.account_name).filter(Boolean));
            accountNames = [...accSet].sort();
            await fetchSummaryTransactions();
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

    function buildSummaryParams(offset = 0) {
        const params = { limit: 1000, offset };
        if (filterMonth && filterMonth !== '__ytd__') params.month = filterMonth;
        if (filterCategory) params.category = filterCategory;
        if (filterAccount) params.account = filterAccount;
        if (search) params.search = search;
        return params;
    }

    async function fetchSummaryTransactions() {
        try {
            let offset = 0;
            let all = [];
            let expectedTotal = null;

            do {
                const result = await api.getTransactions(buildSummaryParams(offset));
                const page = result.data || [];
                all = all.concat(page);
                expectedTotal = result.total_count ?? all.length;
                offset += page.length;

                if (page.length === 0) break;
            } while (offset < expectedTotal);

            if (filterMonth === '__ytd__') {
                const year = new Date().getFullYear().toString();
                all = all.filter(t => t.date?.startsWith(year));
            }

            summaryTransactions = all;
        } catch (e) {
            console.error('Failed to fetch transaction summary data:', e);
            summaryTransactions = filteredTxns;
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
            fetchSummaryTransactions();
        }
    }

    // Debounced search (separate from other filters since it changes per keystroke)
    $: if (!loading && search !== undefined) {
        if (_searchDebounce) clearTimeout(_searchDebounce);
        _searchDebounce = setTimeout(() => {
            pageOffset = 0;
            fetchTransactions();
            fetchSummaryTransactions();
        }, 300);
    }

    // Transactions are already server-filtered
    $: filteredTxns = transactions;
    $: summaryTxns = summaryTransactions.length > 0 ? summaryTransactions : filteredTxns;

    // Transfer types excluded from accrual-basis totals (same model as dashboard)
    const EXCLUDED_EXPENSE_TYPES = new Set(['transfer_internal', 'transfer_cc_payment', 'transfer_household']);
    const NON_SPENDING_CATEGORIES = new Set(['Savings Transfer', 'Personal Transfer', 'Credit Card Payment', 'Income']);
    const TITLE_CASE_SMALL_WORDS = new Set(['and', 'of', 'the', 'to', 'for', 'by', 'at', 'in']);
    const MERCHANT_LOCATION_PATTERNS = [
        { pattern: /\bSAN JOSE\b/i, label: 'San Jose' },
        { pattern: /\bSUNNYVALE\b/i, label: 'Sunnyvale' },
        { pattern: /\bMILPITAS\b/i, label: 'Milpitas' },
        { pattern: /\bPALO ALTO\b/i, label: 'Palo Alto' },
        { pattern: /\bLOS ALTOS\b/i, label: 'Los Altos' },
        { pattern: /\bCASTRO ST\b/i, label: 'Castro St.' }
    ];

    $: groupedTxns = groupTransactionsByDate(filteredTxns);
    $: totalSpending = summaryTxns
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
    $: totalIncome = summaryTxns
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
    $: txCcRepaid = summaryTxns
        .filter(t => t.category === 'Credit Card Payment' && parseFloat(t.amount) < 0)
        .reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
    $: txExternalTransfers = summaryTxns
        .filter(t => t.expense_type === 'transfer_external' && parseFloat(t.amount) < 0)
        .reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
    $: txNetFlow = totalIncome - totalSpending - txExternalTransfers;
    $: largestSpendTx = summaryTxns
        .filter(t => parseFloat(t.amount) < 0)
        .filter(t => !NON_SPENDING_CATEGORIES.has(t.category))
        .filter(t => !(t.expense_type && EXCLUDED_EXPENSE_TYPES.has(t.expense_type)))
        .sort((a, b) => Math.abs(parseFloat(b.amount)) - Math.abs(parseFloat(a.amount)))[0] || null;
    $: largestSpendAmount = largestSpendTx ? Math.abs(parseFloat(largestSpendTx.amount)) : 0;
    $: periodLabel = selectedPeriod === 'all'
        ? 'All time'
        : selectedPeriod === 'ytd'
            ? `YTD ${new Date().getFullYear()}`
            : selectedPeriod === 'custom'
                ? formatMonth(selectedCustomMonth)
                : formatMonth(getMonthForPeriod(selectedPeriod));
    $: storyKicker = selectedPeriod === 'this_month' || selectedPeriod === 'custom'
        ? `${periodLabel} · so far`
        : periodLabel;
    $: dailySpendBars = groupTransactionsByDate(summaryTxns).map(([date, txns]) => {
        const spent = txns
            .filter(t => parseFloat(t.amount) < 0)
            .filter(t => !NON_SPENDING_CATEGORIES.has(t.category))
            .filter(t => !(t.expense_type && EXCLUDED_EXPENSE_TYPES.has(t.expense_type)))
            .reduce((s, t) => s + Math.abs(parseFloat(t.amount)), 0);
        return { date, spent };
    }).reverse().slice(-24);
    $: maxDailySpend = Math.max(...dailySpendBars.map(d => d.spent), 1);


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

    function formatLedgerDay(dateStr) {
        if (!dateStr) return { day: '', meta: '', relative: '' };
        const d = new Date(dateStr + 'T00:00:00');
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const target = new Date(d);
        target.setHours(0, 0, 0, 0);
        const diffDays = Math.round((today - target) / 86400000);
        const relative = diffDays === 0
            ? 'Today'
            : diffDays === 1
                ? 'Yesterday'
                : diffDays > 1
                    ? `${diffDays}d ago`
                    : '';

        return {
            day: d.toLocaleDateString('en-US', { day: '2-digit' }),
            meta: d.toLocaleDateString('en-US', { month: 'short', weekday: 'long' }),
            relative
        };
    }

    function titleCaseMerchant(value) {
        const raw = (value || '').trim();
        if (!raw) return '';
        if (raw !== raw.toUpperCase()) return raw;
        return raw
            .toLowerCase()
            .split(/\s+/)
            .map((word, index) => {
                if (index > 0 && TITLE_CASE_SMALL_WORDS.has(word)) return word;
                if (/^#?\d+$/.test(word)) return word;
                return word.charAt(0).toUpperCase() + word.slice(1);
            })
            .join(' ');
    }

    function getMerchantDisplay(tx) {
        const preferred = tx.merchant_display_name || tx.merchant_name || tx.counterparty_name || '';
        const raw = tx.description || '';
        if (!preferred && raw) return splitRawMerchant(raw).name;
        let label = preferred || raw;
        label = label
            .replace(/\s+(INC|LLC|CORP|CO)\.?$/i, '')
            .replace(/\s{2,}/g, ' ')
            .trim();
        return titleCaseMerchant(label || 'Transaction');
    }

    function splitRawMerchant(rawValue) {
        const raw = (rawValue || '').replace(/[·]/g, ' ').replace(/\s{2,}/g, ' ').trim();
        for (const entry of MERCHANT_LOCATION_PATTERNS) {
            const match = raw.match(entry.pattern);
            if (match && match.index > 0) {
                const name = raw
                    .slice(0, match.index)
                    .replace(/\b(EL|CA)\b/gi, '')
                    .replace(/\s{2,}/g, ' ')
                    .trim();
                return {
                    name: titleCaseMerchant(name || raw),
                    location: entry.label
                };
            }
        }
        return { name: titleCaseMerchant(raw), location: '' };
    }

    function getRawDescriptor(tx) {
        return (tx.description || tx.raw_description || tx.merchant_name || '').trim();
    }

    function getMerchantLocation(tx) {
        if (!(tx.merchant_display_name || tx.merchant_name || tx.counterparty_name)) {
            return splitRawMerchant(tx.description || '').location;
        }
        const display = getMerchantDisplay(tx);
        const raw = getRawDescriptor(tx);
        const upperRaw = raw.toUpperCase();
        const upperDisplay = display.toUpperCase();
        const leftover = upperRaw.replace(upperDisplay, '').replace(/[^\w\s#]/g, ' ').trim();
        const locationWords = leftover
            .split(/\s+/)
            .filter(w => /^[A-Z]{3,}$/.test(w) && !['DES', 'WEB', 'ID', 'COM', 'BILL', 'PAYMENT'].includes(w));
        const location = locationWords.slice(-2).join(' ');
        return titleCaseMerchant(location);
    }

    function getMerchantTitle(tx) {
        const name = getMerchantDisplay(tx);
        const location = getMerchantLocation(tx);
        if (location && !name.toLowerCase().includes(location.toLowerCase())) return `${name} · ${location}`;
        return name;
    }

    function getMerchantKey(tx) {
        return (tx.merchant_display_key || tx.merchant_key || getMerchantDisplay(tx))
            .toLowerCase()
            .replace(/[^a-z0-9]+/g, ' ')
            .trim();
    }

    function getAccountSuffix(tx) {
        return tx.account_last4 || tx.last4 || tx.account_mask || '';
    }

    function hashString(value) {
        return [...(value || '')].reduce((hash, char) => ((hash << 5) - hash + char.charCodeAt(0)) | 0, 0);
    }

    function getAccountHue(tx) {
        const palette = ['#f0aa64', '#7dd3fc', '#a78bfa', '#34d399', '#f472b6', '#f87171'];
        return palette[Math.abs(hashString(tx.account_name || 'account')) % palette.length];
    }

    function getRowSignal(tx) {
        const amount = Math.abs(parseFloat(tx.amount || 0));
        if (tx.expense_type && EXCLUDED_EXPENSE_TYPES.has(tx.expense_type)) return null;
        if (amount >= 500) return 'Large';
        return null;
    }

    function getMerchantStats(tx) {
        const key = getMerchantKey(tx);
        const sourceTxns = historyTransactions.length > 0 ? historyTransactions : summaryTxns;
        const byId = new Map();
        sourceTxns
            .filter(item => getMerchantKey(item) === key)
            .forEach(item => byId.set(item.original_id || `${item.date}-${item.description}-${item.amount}`, item));
        byId.set(tx.original_id || `${tx.date}-${tx.description}-${tx.amount}`, tx);

        const matches = [...byId.values()].sort((a, b) => (b.date || '').localeCompare(a.date || ''));
        const spentMatches = matches.filter(item => parseFloat(item.amount) < 0);
        const average = spentMatches.length
            ? spentMatches.reduce((s, item) => s + Math.abs(parseFloat(item.amount || 0)), 0) / spentMatches.length
            : 0;
        const amount = Math.abs(parseFloat(tx.amount || 0));
        const delta = average ? ((amount - average) / average) * 100 : 0;
        const recent = matches.filter(item => item.original_id !== tx.original_id).slice(0, 3);
        const categoryCounts = matches.reduce((counts, item) => {
            const category = item.category || 'Uncategorized';
            counts[category] = (counts[category] || 0) + 1;
            return counts;
        }, {});
        const dominantCategory = Object.entries(categoryCounts)
            .sort((a, b) => b[1] - a[1])[0] || [tx.category || 'Uncategorized', 1];
        const monthsBack = [];
        const base = tx.date ? new Date(tx.date + 'T00:00:00') : new Date();
        for (let i = 5; i >= 0; i -= 1) {
            const d = new Date(base.getFullYear(), base.getMonth() - i, 1);
            const month = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
            const total = spentMatches
                .filter(item => item.date?.startsWith(month))
                .reduce((s, item) => s + Math.abs(parseFloat(item.amount || 0)), 0);
            monthsBack.push({
                month,
                label: d.toLocaleDateString('en-US', { month: 'short' }),
                total
            });
        }

        const activeMonths = monthsBack.filter(month => month.total > 0);
        const looksMonthly = spentMatches.length >= 3 && activeMonths.length >= 3 && Math.abs(delta) <= 15;
        const firstSeen = matches[matches.length - 1]?.date || '';
        const lastSeen = matches[0]?.date || '';

        return {
            visits: matches.length,
            average,
            delta,
            recent,
            months: monthsBack,
            dominantCategory: dominantCategory[0],
            dominantCategoryCount: dominantCategory[1],
            looksMonthly,
            firstSeen,
            lastSeen
        };
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
                fetchTransactionHistory()
            ]);
            transactions = result.data;
            totalCount = result.total_count;
            pageOffset = 0;
            allCategories = cats;

            const allTxns = metaResult;
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
            await fetchSummaryTransactions();
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
        <h2 class="folio-page-title">Transactions</h2>
        <p class="folio-page-subtitle">
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

<!-- MONTH STORY CARD -->
<section class="tx-story-card fade-in-up" style="animation-delay: 60ms">
    <div class="tx-story-kicker">{storyKicker}</div>
    <h3 class="tx-story-title">The month, <span>in full.</span></h3>

    <div class="tx-story-metrics">
        <div class="tx-story-metric tx-story-metric-main">
            <span class="tx-story-label">Spending</span>
            <strong>{formatCurrency(totalSpending, 2)}</strong>
            <small>{totalCount} transactions reviewed</small>
        </div>
        <div class="tx-story-metric">
            <span class="tx-story-label">Income</span>
            <strong class="tx-story-positive">{formatCurrency(totalIncome, 0)}</strong>
            <small>Actual income only</small>
        </div>
        <div class="tx-story-metric">
            <span class="tx-story-label">Ext. transfers</span>
            <strong class="tx-story-warning">{formatCurrency(txExternalTransfers, 0)}</strong>
            <small>External transfers counted in flow</small>
        </div>
        <div class="tx-story-metric">
            <span class="tx-story-label">CC repaid</span>
            <strong class="tx-story-muted">{formatCurrency(txCcRepaid, 0)}</strong>
            <small>Card payments excluded from spending</small>
        </div>
        <div class="tx-story-metric">
            <span class="tx-story-label">Net flow</span>
            <strong class:tx-story-positive={txNetFlow >= 0} class:tx-story-negative={txNetFlow < 0}>
                {txNetFlow >= 0 ? '+' : ''}{formatCurrency(txNetFlow, 0)}
            </strong>
            <small>Income minus spending and external transfers</small>
        </div>
        <div class="tx-story-metric">
            <span class="tx-story-label">Largest spend</span>
            <strong>-{formatCurrency(largestSpendAmount, 0)}</strong>
            <small>{largestSpendTx ? `${largestSpendTx.category || 'Uncategorized'} · ${largestSpendTx.description || 'Transaction'}` : 'No spending yet'}</small>
        </div>
    </div>

    <div class="tx-story-spark">
        <span>Daily spend</span>
        <div class="tx-story-bars" aria-hidden="true">
            {#each dailySpendBars as bar}
                <i class:tx-story-bar-hot={bar.spent === maxDailySpend && bar.spent > 0}
                   style="height: {bar.spent > 0 ? Math.max(8, (bar.spent / maxDailySpend) * 30) : 3}px"></i>
            {/each}
        </div>
        <span>large day</span>
    </div>
</section>

<!-- PERIOD SELECTOR + FILTERS -->
<div class="tx-command-card fade-in-up" style="animation-delay: 100ms; position: relative; z-index: 10;">
    <div class="tx-command-search">
        <span class="material-symbols-outlined">search</span>
        <input bind:value={search} type="text" placeholder="Search merchants, categories, accounts..." />
        <kbd>/</kbd>
    </div>

    <!-- Row 1: Period toggle + Month dropdown + Category + Account filters -->
    <div class="tx-command-controls txn-period-row">
        <div class="period-toggle-track" style="--seg-count: {periodOptions.length}; --active-idx: {activePeriodIdx};">
            <div class="period-toggle-thumb"></div>
            {#each periodOptions as p}
                <button class="period-toggle-label" class:active={selectedPeriod === p.key}
                    on:click={() => handlePeriodChange(p.key)}>
                    {p.label}
                </button>
            {/each}
        </div>
        {#if selectedPeriod === 'custom'}
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
        {/if}

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
                class="tx-command-reset">
                <span class="material-symbols-outlined text-[14px]">close</span>
                Reset
            </button>
        {/if}
    </div>

    <div class="tx-active-filters">
        <span class="tx-filter-chip">
            <span class="material-symbols-outlined">calendar_month</span>
            {periodLabel}
        </span>
        {#if filterCategory}
            <button class="tx-filter-chip tx-filter-chip-removable" on:click={() => filterCategory = ''}>
                <span class="material-symbols-outlined">category</span>
                {filterCategory}
                <span class="material-symbols-outlined">close</span>
            </button>
        {/if}
        {#if filterAccount}
            <button class="tx-filter-chip tx-filter-chip-removable" on:click={() => filterAccount = ''}>
                <span class="material-symbols-outlined">account_balance</span>
                {filterAccount}
                <span class="material-symbols-outlined">close</span>
            </button>
        {/if}
        {#if search}
            <button class="tx-filter-chip tx-filter-chip-removable" on:click={() => search = ''}>
                <span class="material-symbols-outlined">search</span>
                {search}
                <span class="material-symbols-outlined">close</span>
            </button>
        {/if}
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
    <div class="tx-ledger-card fade-in-up" style="animation-delay: 140ms;">
        {#if groupedTxns.length === 0}
            <div class="text-center py-16" style="color: var(--text-muted)">
                <span class="material-symbols-outlined text-5xl mb-3" style="opacity: 0.4">search_off</span>
                <p class="text-sm font-medium">No transactions match your filters</p>
                <p class="text-[11px] mt-1">Try adjusting the month, category, or search term</p>
            </div>
        {:else}
            {#each groupedTxns as [date, txns], gi}
                {@const dayNet = txns.reduce((s, t) => s + parseFloat(t.amount || 0), 0)}
                {@const dayInfo = formatLedgerDay(date)}

                <div class="tx-day-group" class:tx-day-group-separated={gi > 0}>
                    <aside class="tx-day-rail">
                        <div class="tx-day-rail-date">
                            <strong>{dayInfo.day}</strong>
                            <div>
                                <span>{dayInfo.meta}</span>
                                {#if dayInfo.relative}<small>{dayInfo.relative}</small>{/if}
                            </div>
                        </div>
                    </aside>
                    <div class="tx-day-body">
                        {#if txns.length > 1}
                        <div class="tx-day-summary-strip">
                            <div>
                                <em>{txns.length} tx</em>
                                <span>net</span>
                                <strong class={dayNet >= 0 ? 'tx-day-income' : 'tx-day-spend'}>
                                    {dayNet >= 0 ? '+' : ''}{formatCurrency(dayNet, 0)}
                                </strong>
                            </div>
                        </div>
                        {/if}

                {#each txns as tx (tx.original_id)}
                    {@const amount = parseFloat(tx.amount)}
                    {@const sourceInfo = getSourceLabel(tx)}
                    {@const isRecentlyUpdated = recentlyUpdatedTxId === tx.original_id}
                    {@const isSelected = selectedTxId === tx.original_id}
                    {@const merchantTitle = getMerchantTitle(tx)}
                    {@const rawDescriptor = getRawDescriptor(tx)}
                    {@const rowSignal = getRowSignal(tx)}
                    {@const merchantStats = getMerchantStats(tx)}
                    {@const hasMerchantTrend = merchantStats.recent.length > 0 && merchantStats.months.some(m => m.total > 0)}
                    {@const maxMerchantMonth = Math.max(...merchantStats.months.map(m => m.total), Math.abs(amount), 1)}
                    <div class="tx-row-grid group transition-colors tx-row tx-ledger-row"
                        class:tx-row-updated={isRecentlyUpdated}
                        class:tx-row-selected={isSelected}
                        role="button"
                        tabindex="0"
                        aria-expanded={isSelected}
                        on:click={() => selectedTxId = isSelected ? null : tx.original_id}
                        on:keydown={(event) => {
                            if (event.key === 'Enter' || event.key === ' ') {
                                event.preventDefault();
                                selectedTxId = isSelected ? null : tx.original_id;
                            }
                        }}>

                        <!-- Zone 1: Icon + Description + Account -->
                        <div class="tx-zone-desc">
                            <div class="tx-merchant-avatar"
                                style="--tx-cat-color: {CATEGORY_COLORS[tx.category] || '#627d98'}">
                                <i></i>
                                <span>{merchantTitle.charAt(0)}</span>
                            </div>
                            <div class="min-w-0 flex-1">
                                <p class="text-[13px] font-medium truncate" style="color: var(--text-primary)">
                                    {merchantTitle}
                                </p>
                                <span class="tx-row-subline">
                                    <span class="tx-account-dot" style="background: {getAccountHue(tx)}"></span>
                                    {tx.account_name || 'Account'}{#if getAccountSuffix(tx)} · ••{getAccountSuffix(tx)}{/if}
                                    {#if rawDescriptor && rawDescriptor.toUpperCase() !== merchantTitle.toUpperCase()}
                                        <span class="tx-row-dot"></span>
                                        <span class="tx-raw-preview">{rawDescriptor}</span>
                                    {/if}
                                </span>
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

                        <!-- Zone 3: Signal and amount -->
                        <div class="tx-zone-signal">
                            {#if rowSignal}
                                <span class="tx-signal-chip" class:tx-signal-chip-alert={rowSignal === 'Large'}>{rowSignal}</span>
                            {/if}
                        </div>

                        <div class="tx-zone-amount">
                            <div>
                                <p class="folio-amount-compact"
                                    style="color: {amount >= 0 ? 'var(--positive)' : txns.length === 1 ? 'var(--negative)' : 'var(--text-primary)'}">
                                    {amount >= 0 ? '+' : ''}{formatCurrency(amount, 2)}
                                </p>
                            </div>
                        </div>
                    </div>

                    {#if isSelected}
                        <div class="tx-detail-drawer fade-in" role="presentation" on:click|stopPropagation>
                            <div class="tx-detail-pane">
                                <h4>Merchant pattern</h4>
                                <div class="tx-merchant-intel">
                                    <div>
                                        <span>{merchantStats.looksMonthly ? 'Likely recurring' : 'Seen in history'}</span>
                                        <strong>
                                            {merchantStats.looksMonthly ? `Monthly · ${formatCurrency(merchantStats.average, 2)}` : `${merchantStats.visits} visit${merchantStats.visits === 1 ? '' : 's'}`}
                                        </strong>
                                    </div>
                                    <div>
                                        <span>Typical amount</span>
                                        <strong>{merchantStats.average > 0 ? formatCurrency(merchantStats.average, 2) : formatCurrency(Math.abs(amount), 2)}</strong>
                                    </div>
                                    <div>
                                        <span>Category pattern</span>
                                        <strong>{merchantStats.dominantCategory} · {merchantStats.dominantCategoryCount}/{merchantStats.visits}</strong>
                                    </div>
                                    {#if merchantStats.firstSeen}
                                        <div>
                                            <span>First seen</span>
                                            <strong>{formatDate(merchantStats.firstSeen)}</strong>
                                        </div>
                                    {/if}
                                </div>
                                {#if rawDescriptor && rawDescriptor.toUpperCase() !== merchantTitle.toUpperCase()}
                                    <details class="tx-raw-disclosure">
                                        <summary>Bank statement text</summary>
                                        <p>{rawDescriptor}</p>
                                    </details>
                                {/if}
                            </div>
                            <div class="tx-detail-pane">
                                <h4>Recent at this merchant</h4>
                                <div class="tx-merchant-history">
                                    {#if merchantStats.recent.length > 0}
                                        {#each merchantStats.recent as recent}
                                            <div>
                                                <span>{formatDate(recent.date)}</span>
                                                <strong>{formatCurrency(parseFloat(recent.amount), 2)}</strong>
                                            </div>
                                        {/each}
                                    {:else}
                                        <p>No earlier matching transactions in your loaded history.</p>
                                    {/if}
                                    {#if hasMerchantTrend}
                                        <div class="tx-history-bars" aria-label="Merchant spending trend">
                                            {#each merchantStats.months as month}
                                                <span style="height: {month.total > 0 ? Math.max(8, (month.total / maxMerchantMonth) * 44) : 4}px">
                                                    <em>{month.label}</em>
                                                </span>
                                            {/each}
                                        </div>
                                    {/if}
                                    <p>
                                        {merchantStats.visits} visit{merchantStats.visits === 1 ? '' : 's'} in history{#if merchantStats.average > 0}, average <strong>{formatCurrency(merchantStats.average, 2)}</strong>{/if}{#if merchantStats.visits > 1 && merchantStats.average > 0} · this ran <strong class={merchantStats.delta >= 0 ? 'tx-above-average' : 'tx-below-average'}>{merchantStats.delta >= 0 ? '+' : ''}{merchantStats.delta.toFixed(1)}%</strong> vs usual{/if}.
                                    </p>
                                </div>
                            </div>
                        </div>
                    {/if}

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
                    </div>
                </div>
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
