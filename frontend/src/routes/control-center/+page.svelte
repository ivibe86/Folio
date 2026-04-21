<script>
    import '$lib/styles/control-center.css';
    import { goto } from '$app/navigation';
    import { page } from '$app/stores';
    import { onMount } from 'svelte';
    import { api, invalidateCache } from '$lib/api.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import { formatCurrency, formatDate } from '$lib/utils.js';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';
    import SimpleFINConnect from '$lib/components/SimpleFINConnect.svelte';
    import MigrationWizard from '$lib/components/MigrationWizard.svelte';

    const allTabs = [
        { key: 'connections', label: 'Connections' },
        { key: 'merchants',   label: 'Merchants' },
        { key: 'rules',       label: 'Rules' },
        { key: 'categories',  label: 'Categories' },
        { key: 'history',     label: 'History' },
    ];
    const merchantFilterKeys = new Set(['all', 'subscriptions', 'non_subscriptions']);

    let activeTab = 'merchants';
    let notice = '';
    let noticeTimeout;
    let loading = true;
    let lastLoadedProfile = undefined;

    let merchantsLoading = false;
    let merchantPreviewLoading = false;
    let rulesLoading = false;
    let ruleImpactLoading = false;
    let categoriesLoading = false;
    let historyLoading = false;

    // ── Merchants ────────────────────────────────────────────────
    let merchantItems = [];
    let merchantSearch = '';
    let merchantSubFilter = 'all';        // all | subscriptions | non_subscriptions
    let selectedMerchantKey = null;
    let expandedMerchantKey = null;
    let merchantTransactions = [];
    let merchantSaving = false;
    let lastMerchantPreviewKey = null;
    let openMerchantCategoryMenuKey = null;
    let merchantFilterMenuOpen = false;
    let merchantAliasDrafts = {};

    // ── Rules ────────────────────────────────────────────────────
    let ruleItems = [];
    let ruleSearch = '';
    let ruleSourceFilter = 'all';
    let ruleStateFilter = 'all';
    let selectedRuleId = null;
    let ruleDraft = { id: null, category: '', priority: 1000, is_active: true };
    let ruleImpact = null;
    let lastRuleImpactId = null;
    let ruleSaving = false;

    // ── Categories ───────────────────────────────────────────────
    let categoriesMeta = [];
    let categorySearch = '';
    let categorySavingKey = null;   // name of the category currently being saved

    // ── History ──────────────────────────────────────────────────
    let historyItems = [];
    let historySearch = '';
    let selectedHistoryId = null;

    // ── Connections ──────────────────────────────────────────────
    let tellerEnrollments = [];
    let simplefinConnections = [];
    let connectionsLoading = false;
    let simplefinRef;
    let migrationRef;
    let tellerConfig = null;
    let appConfig = {
        demoMode: false,
        bankLinkingEnabled: true,
        manualSyncEnabled: true,
        demoPersistence: 'persistent'
    };

    // ── Profile reactivity ───────────────────────────────────────
    $: activeProfileId = $activeProfile || 'household';
    $: scopedProfile   = activeProfileId !== 'household' ? activeProfileId : null;
    $: allCategoryNames = categoriesMeta.map((item) => item.name).filter(Boolean);
    $: visibleTabs = allTabs.filter((tab) => appConfig.bankLinkingEnabled || tab.key !== 'connections');
    $: tabKeys = new Set(visibleTabs.map((tab) => tab.key));

    $: urlTab = $page.url.searchParams.get('tab') || 'merchants';
    $: if (tabKeys.has(urlTab) && urlTab !== activeTab) {
        activeTab = urlTab;
    }
    $: if (!tabKeys.has(urlTab) && activeTab !== 'merchants') {
        activeTab = 'merchants';
    }
    $: urlMerchantFilter = $page.url.searchParams.get('merchant_filter');
    $: if (urlMerchantFilter && merchantFilterKeys.has(urlMerchantFilter) && urlMerchantFilter !== merchantSubFilter) {
        merchantSubFilter = urlMerchantFilter;
    }

    // ── Merchants derived ────────────────────────────────────────
    function merchantRowKey(item) {
        return `${item.merchant_key}::${item.profile_id}`;
    }

    $: visibleMerchants = merchantItems.filter((item) => {
        if (merchantSubFilter === 'subscriptions')     return !!item.is_subscription;
        if (merchantSubFilter === 'non_subscriptions') return !item.is_subscription;
        return true;
    });
    $: merchantVisibleCount = visibleMerchants.length;
    $: merchantVisibleSpend = visibleMerchants.reduce((sum, item) => sum + Number(item.total_spent || 0), 0);
    $: merchantSubscriptionCount = visibleMerchants.filter((item) => !!item.is_subscription).length;
    $: merchantScopeLabel = activeProfileId === 'household' ? 'Household' : activeProfileId;
    $: if (visibleMerchants.length === 0) {
        selectedMerchantKey = null;
        expandedMerchantKey = null;
    } else if (!visibleMerchants.some((item) => merchantRowKey(item) === selectedMerchantKey)) {
        selectedMerchantKey = merchantRowKey(visibleMerchants[0]);
    }
    $: if (expandedMerchantKey && !visibleMerchants.some((item) => merchantRowKey(item) === expandedMerchantKey)) {
        expandedMerchantKey = null;
    }
    $: if (openMerchantCategoryMenuKey && !visibleMerchants.some((item) => merchantRowKey(item) === openMerchantCategoryMenuKey)) {
        openMerchantCategoryMenuKey = null;
    }
    $: selectedMerchant = visibleMerchants.find((item) => merchantRowKey(item) === selectedMerchantKey) || null;
    $: if (selectedMerchant) {
        const nextKey = merchantRowKey(selectedMerchant);
        if (nextKey !== lastMerchantPreviewKey) {
            lastMerchantPreviewKey = nextKey;
            loadSelectedMerchantTransactions(selectedMerchant);
        }
    } else if (lastMerchantPreviewKey !== null) {
        lastMerchantPreviewKey = null;
        merchantTransactions = [];
    }

    // ── Rules derived ────────────────────────────────────────────
    $: visibleRules = ruleItems.filter((item) => {
        const sourceOk = ruleSourceFilter === 'all' || item.source === ruleSourceFilter;
        const stateOk = ruleStateFilter === 'all'
            || (ruleStateFilter === 'active'   && !!item.is_active)
            || (ruleStateFilter === 'inactive' && !item.is_active);
        const searchOk = !ruleSearch.trim()
            || [item.pattern, item.category, item.source, item.match_type]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(ruleSearch.trim().toLowerCase()));
        return sourceOk && stateOk && searchOk;
    });
    $: if (visibleRules.length === 0) {
        selectedRuleId = null;
    } else if (!visibleRules.some((item) => item.id === selectedRuleId)) {
        selectedRuleId = visibleRules[0].id;
    }
    $: selectedRule = visibleRules.find((item) => item.id === selectedRuleId) || null;
    $: if (selectedRule && ruleDraft.id !== selectedRule.id) {
        ruleDraft = {
            id: selectedRule.id,
            category: selectedRule.category || '',
            priority: selectedRule.priority ?? 1000,
            is_active: !!selectedRule.is_active,
        };
    }
    $: if (selectedRule) {
        if (selectedRule.id !== lastRuleImpactId) {
            lastRuleImpactId = selectedRule.id;
            loadRuleImpact(selectedRule.id);
        }
    } else if (lastRuleImpactId !== null) {
        lastRuleImpactId = null;
        ruleImpact = null;
    }

    // ── Categories derived ───────────────────────────────────────
    $: visibleCategories = categoriesMeta.filter((item) => {
        if (!categorySearch.trim()) return true;
        const needle = categorySearch.trim().toLowerCase();
        return [item.name, item.parent_category, item.expense_type]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(needle));
    });

    // ── History derived ──────────────────────────────────────────
    $: visibleHistory = historyItems.filter((item) => {
        if (!historySearch.trim()) return true;
        const needle = historySearch.trim().toLowerCase();
        return [item.user_message, item.assistant_response, item.operation_type, item.generated_sql]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(needle));
    });
    $: if (visibleHistory.length === 0) {
        selectedHistoryId = null;
    } else if (!visibleHistory.some((item) => item.id === selectedHistoryId)) {
        selectedHistoryId = visibleHistory[0].id;
    }
    $: selectedHistory = visibleHistory.find((item) => item.id === selectedHistoryId) || null;

    // ── Lifecycle ────────────────────────────────────────────────
    onMount(async () => {
        try {
            appConfig = { ...appConfig, ...(await api.getAppConfig()) };
        } catch (_) {}
        lastLoadedProfile = activeProfileId;
        await refreshAll();
    });

    $: if (activeProfileId && lastLoadedProfile !== undefined && activeProfileId !== lastLoadedProfile) {
        lastLoadedProfile = activeProfileId;
        refreshAll();
    }

    // ── Utilities ────────────────────────────────────────────────
    function setNotice(message) {
        notice = message;
        clearTimeout(noticeTimeout);
        noticeTimeout = setTimeout(() => {
            if (notice === message) notice = '';
        }, 3200);
    }

    function handleWindowClick() {
        openMerchantCategoryMenuKey = null;
        merchantFilterMenuOpen = false;
    }

    function setTab(tab) {
        if (!tabKeys.has(tab)) return;
        activeTab = tab;
        const params = new URLSearchParams($page.url.searchParams);
        if (tab === 'merchants') {
            params.delete('tab');
        } else {
            params.set('tab', tab);
        }
        const query = params.toString();
        goto(query ? `/control-center?${query}` : '/control-center', {
            replaceState: true,
            noScroll: true,
            keepFocus: true,
        });
    }

    function formatDateTime(value) {
        if (!value) return '—';
        const normalized = String(value).includes('T') ? value : String(value).replace(' ', 'T');
        const dt = new Date(normalized);
        if (Number.isNaN(dt.getTime())) return String(value);
        return dt.toLocaleString([], {
            month: 'short',
            day: 'numeric',
            year: 'numeric',
            hour: 'numeric',
            minute: '2-digit',
        });
    }

    function ruleSourceBadgeClass(source) {
        return source === 'user' ? 'cc-badge cc-badge-info' : 'cc-badge cc-badge-muted';
    }

    function ruleSourceLabel(source) {
        return source === 'user' ? 'User' : 'System';
    }

    function ruleStatusBadge(rule) {
        return rule?.is_active ? 'cc-badge cc-badge-positive' : 'cc-badge cc-badge-muted';
    }

    // ── Data loaders ─────────────────────────────────────────────
    async function refreshAll() {
        loading = true;
        try {
            await Promise.all([
                loadConnections(),
                loadMerchants(),
                loadRules(),
                loadCategories(),
                loadHistory(),
            ]);
        } finally {
            loading = false;
        }
    }

    async function loadConnections() {
        connectionsLoading = true;
        try {
            const [enrollments, sfConns, config] = await Promise.all([
                api.getEnrollments().catch(() => []),
                api.getSimpleFINConnections().catch(() => []),
                api.getTellerConfig().catch(() => null),
            ]);
            tellerEnrollments = enrollments || [];
            simplefinConnections = sfConns || [];
            tellerConfig = config;
        } finally {
            connectionsLoading = false;
        }
    }

    async function handleDeactivateEnrollment(id) {
        try {
            await api.deactivateEnrollment(id);
            setNotice('Teller enrollment deactivated.');
            invalidateCache();
            await loadConnections();
        } catch (e) {
            setNotice('Failed to deactivate enrollment.');
        }
    }

    async function handleDeactivateSFConnection(id) {
        try {
            await api.deactivateSimpleFINConnection(id);
            setNotice('SimpleFIN connection deactivated.');
            invalidateCache();
            await loadConnections();
        } catch (e) {
            setNotice('Failed to deactivate connection.');
        }
    }

    function handleSimpleFINConnected() {
        invalidateCache();
        loadConnections();
    }

    async function loadMerchants(searchOverride = merchantSearch) {
        merchantsLoading = true;
        try {
            if (searchOverride !== merchantSearch) merchantSearch = searchOverride;
            const result = await api.getMerchantDirectory(searchOverride || '', 250);
            merchantItems = result?.items || [];
            merchantAliasDrafts = merchantItems.reduce((drafts, item) => {
                drafts[merchantRowKey(item)] = item.clean_name || item.merchant_key || '';
                return drafts;
            }, {});
            return merchantItems;
        } catch (error) {
            merchantItems = [];
            merchantAliasDrafts = {};
            setNotice('Failed to load merchants.');
            return [];
        } finally {
            merchantsLoading = false;
        }
    }

    async function loadSelectedMerchantTransactions(item) {
        if (!item) return;
        merchantPreviewLoading = true;
        try {
            const result = await api.getMerchantTransactions(item.merchant_key, item.profile_id, 25);
            merchantTransactions = result?.items || [];
        } catch (error) {
            merchantTransactions = [];
        } finally {
            merchantPreviewLoading = false;
        }
    }

    async function loadRules() {
        rulesLoading = true;
        try {
            const result = await api.getCategoryRules();
            ruleItems = Array.isArray(result) ? result : [];
            return ruleItems;
        } catch (error) {
            ruleItems = [];
            setNotice('Failed to load rules.');
            return [];
        } finally {
            rulesLoading = false;
        }
    }

    async function loadRuleImpact(ruleId) {
        if (!ruleId) return;
        ruleImpactLoading = true;
        try {
            ruleImpact = await api.getCategoryRuleImpact(ruleId, 20);
        } catch (error) {
            ruleImpact = null;
        } finally {
            ruleImpactLoading = false;
        }
    }

    async function loadCategories() {
        categoriesLoading = true;
        try {
            const result = await api.getCategoriesMeta();
            categoriesMeta = Array.isArray(result) ? result : [];
            return categoriesMeta;
        } catch (error) {
            categoriesMeta = [];
            setNotice('Failed to load categories.');
            return [];
        } finally {
            categoriesLoading = false;
        }
    }

    async function loadHistory() {
        historyLoading = true;
        try {
            const result = await api.getCopilotHistory(80);
            historyItems = result?.items || [];
            return historyItems;
        } catch (error) {
            historyItems = [];
            setNotice('Failed to load history.');
            return [];
        } finally {
            historyLoading = false;
        }
    }

    // ── Save actions ─────────────────────────────────────────────
    function merchantAliasDraftValue(item) {
        return merchantAliasDrafts[merchantRowKey(item)] ?? (item.clean_name || item.merchant_key || '');
    }

    function updateMerchantAliasDraft(item, value) {
        merchantAliasDrafts = {
            ...merchantAliasDrafts,
            [merchantRowKey(item)]: value,
        };
    }

    async function saveMerchantChanges(item, payload, successMessage) {
        if (!item || merchantSaving) return;
        merchantSaving = true;
        try {
            const result = await api.updateMerchantDirectory(item.merchant_key, {
                profile_id: item.profile_id,
                clean_name: null,
                category: null,
                domain: null,
                industry: null,
                ...payload,
            });
            invalidateCache();
            await loadMerchants();
            if (selectedMerchantKey === merchantRowKey(item)) {
                await loadSelectedMerchantTransactions(item);
            }
            setNotice(successMessage(result));
        } catch (error) {
            setNotice('Failed to update merchant.');
        } finally {
            merchantSaving = false;
        }
    }

    async function applyMerchantCategory(item, rowKey, categoryName) {
        openMerchantCategoryMenuKey = null;
        if (!item || merchantSaving) return;
        if ((item.category || '') === (categoryName || '')) return;
        selectedMerchantKey = rowKey;
        await saveMerchantChanges(
            item,
            { category: categoryName },
            (result) => {
                const touched = result?.merchant?.retroactive_count ?? 0;
                return touched > 0
                    ? `Category applied to ${touched} matching transactions.`
                    : 'Merchant category updated.';
            },
        );
    }

    async function saveMerchantAlias(item, rowKey) {
        if (!item || merchantSaving) return;
        const nextAlias = merchantAliasDraftValue(item).trim();
        const currentAlias = (item.clean_name || item.merchant_key || '').trim();
        if ((nextAlias || item.merchant_key) === currentAlias) return;
        selectedMerchantKey = rowKey;
        await saveMerchantChanges(
            item,
            { clean_name: nextAlias || item.merchant_key },
            () => 'Merchant name updated.',
        );
    }

    function handleMerchantRowClick(rowKey) {
        openMerchantCategoryMenuKey = null;
        if (selectedMerchantKey === rowKey) {
            expandedMerchantKey = expandedMerchantKey === rowKey ? null : rowKey;
            return;
        }
        selectedMerchantKey = rowKey;
        expandedMerchantKey = rowKey;
    }

    function handleMerchantRowKeydown(event, rowKey) {
        if (event.target !== event.currentTarget) return;
        if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            handleMerchantRowClick(rowKey);
        }
    }

    function toggleMerchantCategoryMenu(item, rowKey) {
        merchantFilterMenuOpen = false;
        selectedMerchantKey = rowKey;
        openMerchantCategoryMenuKey = openMerchantCategoryMenuKey === rowKey ? null : rowKey;
    }

    async function saveRule() {
        if (!selectedRule) return;
        ruleSaving = true;
        try {
            await api.updateCategoryRule(selectedRule.id, {
                category: ruleDraft.category,
                priority: Number(ruleDraft.priority),
                is_active: !!ruleDraft.is_active,
            });
            invalidateCache();
            await loadRules();
            await loadRuleImpact(selectedRule.id);
            setNotice('Rule updated.');
        } catch (error) {
            setNotice('Failed to update rule.');
        } finally {
            ruleSaving = false;
        }
    }

    async function saveExpenseTypeInline(item, newType) {
        if (!item || item.expense_type === 'non_expense') return;
        if (item.expense_type === newType) return;
        categorySavingKey = item.name;
        try {
            await api.updateExpenseType(item.name, newType);
            invalidateCache();
            // Update in place to avoid full reload flash
            categoriesMeta = categoriesMeta.map((c) =>
                c.name === item.name ? { ...c, expense_type: newType, expense_type_source: 'user' } : c
            );
        } catch (error) {
            setNotice('Failed to update expense type.');
            await loadCategories();
        } finally {
            if (categorySavingKey === item.name) categorySavingKey = null;
        }
    }

    async function reopenInCopilot() {
        if (!selectedHistory?.user_message) return;
        const params = new URLSearchParams();
        params.set('prompt', selectedHistory.user_message);
        await goto(`/copilot?${params.toString()}`);
    }
</script>

<svelte:window on:click={handleWindowClick} />

<div class="cc-page">
    <div class="cc-header fade-in">
        <div class="cc-title-wrap">
            <div class="cc-hero-icon">
                <span class="material-symbols-outlined">tune</span>
            </div>
            <div>
                <div class="cc-kicker">Calibration Surface</div>
                <h2 class="text-xl font-extrabold font-display" style="color: var(--text-primary)">Control Center</h2>
                <p class="text-[12px]" style="color: var(--text-muted)">Tune merchant metadata, category rules, and spending behavior with the same calmer visual rhythm as the rest of Folio.</p>
            </div>
        </div>
        <div class="cc-header-actions">
            <ProfileSwitcher />
        </div>
    </div>

    {#if appConfig.demoMode}
        <div class="cc-notice fade-in">Demo mode is active. Connections and sync are disabled, but merchant/category edits still work until the demo resets.</div>
    {/if}

    <div class="cc-tabbar fade-in-up">
        {#each visibleTabs as tab}
            <button type="button" class="cc-tab" class:cc-tab-active={activeTab === tab.key} on:click={() => setTab(tab.key)}>
                {tab.label}
            </button>
        {/each}
    </div>

    {#if notice}
        <div class="cc-notice fade-in">{notice}</div>
    {/if}

    {#if loading}
        <div class="cc-empty">Loading…</div>

    {:else if activeTab === 'connections'}
        <!-- ── CONNECTIONS ───────────────────────────────────────── -->
        <section class="cc-pane cc-pane-primary fade-in-up" style="max-width: 720px; margin: 0 auto;">
            <div class="cc-pane-header">
                <div class="cc-pane-title">
                    <h3>Bank Connections</h3>
                    <p>Manage your connected bank accounts. Add new connections via Teller or SimpleFIN Bridge.</p>
                </div>
                {#if tellerEnrollments.length > 0 && simplefinConnections.length > 0}
                    <button
                        type="button"
                        class="cc-secondary-btn"
                        on:click={() => migrationRef.show()}
                        title="Migrate from Teller to SimpleFIN"
                    >
                        <span class="material-symbols-outlined text-[14px]">swap_horiz</span>
                        Migrate to SimpleFIN
                    </button>
                {/if}
            </div>

            <!-- Teller Enrollments -->
            <div class="cc-conn-section">
                <div class="cc-conn-section-header">
                    <div class="cc-conn-section-title">
                        <span class="cc-provider-badge cc-provider-teller">Teller</span>
                        <span class="cc-conn-count">{tellerEnrollments.length} enrollment{tellerEnrollments.length !== 1 ? 's' : ''}</span>
                    </div>
                </div>

                {#if tellerEnrollments.length === 0}
                    <div class="cc-conn-empty">No Teller enrollments yet.</div>
                {:else}
                    {#each tellerEnrollments as enrollment}
                        <div class="cc-conn-row">
                            <div class="cc-conn-info">
                                <div class="cc-conn-name">{enrollment.institution || 'Unknown Institution'}</div>
                                <div class="cc-conn-meta">
                                    {enrollment.owner_name || ''}{enrollment.owner_name && enrollment.profile ? ' · ' : ''}{enrollment.profile || ''}
                                    {#if enrollment.created_at}
                                        · added {formatDateTime(enrollment.created_at)}
                                    {/if}
                                </div>
                            </div>
                            <button
                                type="button"
                                class="cc-conn-remove"
                                on:click={() => handleDeactivateEnrollment(enrollment.id)}
                                title="Remove this enrollment"
                            >
                                <span class="material-symbols-outlined text-[16px]">close</span>
                            </button>
                        </div>
                    {/each}
                {/if}
            </div>

            <!-- SimpleFIN Connections -->
            <div class="cc-conn-section">
                <div class="cc-conn-section-header">
                    <div class="cc-conn-section-title">
                        <span class="cc-provider-badge cc-provider-simplefin">SimpleFIN</span>
                        <span class="cc-conn-count">{simplefinConnections.length} connection{simplefinConnections.length !== 1 ? 's' : ''}</span>
                    </div>
                    <button
                        type="button"
                        class="cc-secondary-btn"
                        on:click={() => simplefinRef.show()}
                    >
                        <span class="material-symbols-outlined text-[14px]">add</span>
                        Connect Bank
                    </button>
                </div>

                {#if simplefinConnections.length === 0}
                    <div class="cc-conn-empty">No SimpleFIN connections yet. Click "Connect Bank" to add one.</div>
                {:else}
                    {#each simplefinConnections as conn}
                        <div class="cc-conn-row">
                            <div class="cc-conn-info">
                                <div class="cc-conn-name">{conn.display_name || 'SimpleFIN Connection'}</div>
                                <div class="cc-conn-meta">
                                    {conn.profile || ''}
                                    {#if conn.last_synced_at}
                                        · synced {formatDateTime(conn.last_synced_at)}
                                    {:else if conn.created_at}
                                        · added {formatDateTime(conn.created_at)}
                                    {/if}
                                </div>
                            </div>
                            <button
                                type="button"
                                class="cc-conn-remove"
                                on:click={() => handleDeactivateSFConnection(conn.id)}
                                title="Remove this connection"
                            >
                                <span class="material-symbols-outlined text-[16px]">close</span>
                            </button>
                        </div>
                    {/each}
                {/if}
            </div>
        </section>

        <SimpleFINConnect bind:this={simplefinRef} on:connected={handleSimpleFINConnected} />
        <MigrationWizard bind:this={migrationRef} on:done={loadConnections} />

    {:else if activeTab === 'merchants'}
        <!-- ── MERCHANTS ─────────────────────────────────────────── -->
        <section class="cc-pane cc-pane-primary cc-pane-merchants fade-in-up">
                <div class="cc-pane-header">
                    <div class="cc-pane-title">
                        <h3>Merchants</h3>
                        <p>Spend-only merchant directory. Savings transfers, personal transfers, and card payments are filtered out so the totals reflect real merchant spend.</p>
                    </div>
                    <div class="cc-toolbar">
                        <div class="cc-toolbar-pill-wrap">
                            <button
                                type="button"
                                class="cc-merchant-pill"
                                on:click|stopPropagation
                                on:click={() => {
                                    openMerchantCategoryMenuKey = null;
                                    merchantFilterMenuOpen = !merchantFilterMenuOpen;
                                }}>
                                <span>{merchantSubFilter === 'subscriptions' ? 'Recurring merchants' : merchantSubFilter === 'non_subscriptions' ? 'One-off merchants' : 'All spend'}</span>
                                <span class="material-symbols-outlined text-[16px]" class:cc-chevron-open={merchantFilterMenuOpen}>expand_more</span>
                            </button>
                            {#if merchantFilterMenuOpen}
                                <div class="cc-merchant-dropdown">
                                    <button type="button" class="cc-merchant-dropdown-option" class:active={merchantSubFilter === 'all'} on:click={() => { merchantSubFilter = 'all'; merchantFilterMenuOpen = false; }}>
                                        <span>All spend</span>
                                        {#if merchantSubFilter === 'all'}<span class="material-symbols-outlined text-[14px]">check</span>{/if}
                                    </button>
                                    <button type="button" class="cc-merchant-dropdown-option" class:active={merchantSubFilter === 'subscriptions'} on:click={() => { merchantSubFilter = 'subscriptions'; merchantFilterMenuOpen = false; }}>
                                        <span>Recurring merchants</span>
                                        {#if merchantSubFilter === 'subscriptions'}<span class="material-symbols-outlined text-[14px]">check</span>{/if}
                                    </button>
                                    <button type="button" class="cc-merchant-dropdown-option" class:active={merchantSubFilter === 'non_subscriptions'} on:click={() => { merchantSubFilter = 'non_subscriptions'; merchantFilterMenuOpen = false; }}>
                                        <span>One-off merchants</span>
                                        {#if merchantSubFilter === 'non_subscriptions'}<span class="material-symbols-outlined text-[14px]">check</span>{/if}
                                    </button>
                                </div>
                            {/if}
                        </div>
                        <input class="cc-search" bind:value={merchantSearch} placeholder="Search name, industry, or key…" on:keydown={(e) => e.key === 'Enter' && loadMerchants()} />
                        <button class="cc-secondary-btn" type="button" on:click={() => loadMerchants()} disabled={merchantsLoading}>Refresh</button>
                    </div>
                </div>
                <div class="cc-list-wrap">
                    <div class="cc-insights">
                        <div class="cc-insight-card">
                            <span class="cc-insight-label">Visible Merchants</span>
                            <strong>{merchantVisibleCount.toLocaleString()}</strong>
                            <small>{merchantScopeLabel} scope</small>
                        </div>
                        <div class="cc-insight-card">
                            <span class="cc-insight-label">Visible Spend</span>
                            <strong>{formatCurrency(merchantVisibleSpend, 2)}</strong>
                            <small>Transfer-like outflows excluded</small>
                        </div>
                        <div class="cc-insight-card">
                            <span class="cc-insight-label">Recurring Rows</span>
                            <strong>{merchantSubscriptionCount.toLocaleString()}</strong>
                            <small>Subscription-tagged merchants</small>
                        </div>
                    </div>
                    <div class="cc-list-meta">
                        One row per merchant key and profile. Search operates on the corrected spend totals, and merchant category changes apply to matching transactions immediately.
                    </div>
                    {#if merchantsLoading}
                        <div class="cc-empty">Loading merchants…</div>
                    {:else if visibleMerchants.length === 0}
                        <div class="cc-empty">No merchants matched the current filters.</div>
                    {:else}
                        <div class="cc-table">
                            <div class="cc-table-header" style="--cc-cols: 1.8fr 0.95fr 0.65fr 0.9fr 0.7fr;">
                                <div>Merchant</div>
                                <div>Category</div>
                                <div>Txns</div>
                                <div>Total Spent</div>
                                <div>Profile</div>
                            </div>
                            {#each visibleMerchants as item}
                                {@const rowKey = merchantRowKey(item)}
                                <div
                                    class="cc-table-row"
                                    class:cc-table-row-active={rowKey === selectedMerchantKey}
                                    style="--cc-cols: 1.8fr 0.95fr 0.65fr 0.9fr 0.7fr;"
                                    role="button"
                                    tabindex="0"
                                    on:click={() => handleMerchantRowClick(rowKey)}
                                    on:keydown={(event) => handleMerchantRowKeydown(event, rowKey)}>
                                    <div class="cc-cell-primary">
                                        <div class="cc-cell-title" style="display:flex;align-items:center;gap:0.4rem;">
                                            {item.clean_name || item.merchant_key}
                                            {#if item.is_subscription}
                                                <span class="material-symbols-outlined" style="font-size:13px;color:var(--accent);opacity:0.75" title="Subscription">event_repeat</span>
                                            {/if}
                                            <span class="material-symbols-outlined cc-row-chevron" class:cc-row-chevron-open={expandedMerchantKey === rowKey}>expand_more</span>
                                        </div>
                                        <div class="cc-cell-subtitle">{item.merchant_key}{item.industry ? ` · ${item.industry}` : ''}</div>
                                    </div>
                                    <div class="cc-row-category-wrap">
                                        <button
                                            type="button"
                                            class="cc-row-category-btn"
                                            class:cc-row-category-btn-active={openMerchantCategoryMenuKey === rowKey}
                                            on:click|stopPropagation={() => toggleMerchantCategoryMenu(item, rowKey)}
                                            disabled={merchantSaving}>
                                            <span>{item.category || 'Unassigned'}</span>
                                            <span class="material-symbols-outlined text-[14px]" class:cc-chevron-open={openMerchantCategoryMenuKey === rowKey}>expand_more</span>
                                        </button>
                                        {#if openMerchantCategoryMenuKey === rowKey}
                                            <div class="cc-row-category-dropdown">
                                                <button
                                                    type="button"
                                                    class="cc-merchant-dropdown-option"
                                                    class:active={!item.category}
                                                    on:click|stopPropagation={() => applyMerchantCategory(item, rowKey, '')}>
                                                    <span>Unassigned</span>
                                                    {#if !item.category}
                                                        <span class="material-symbols-outlined text-[14px]">check</span>
                                                    {/if}
                                                </button>
                                                {#each allCategoryNames as categoryName}
                                                    <button
                                                        type="button"
                                                        class="cc-merchant-dropdown-option"
                                                        class:active={categoryName === (item.category || '')}
                                                        on:click|stopPropagation={() => applyMerchantCategory(item, rowKey, categoryName)}>
                                                        <span>{categoryName}</span>
                                                        {#if categoryName === (item.category || '')}
                                                            <span class="material-symbols-outlined text-[14px]">check</span>
                                                        {/if}
                                                    </button>
                                                {/each}
                                            </div>
                                        {/if}
                                    </div>
                                    <div class="cc-cell-subtitle">{item.charge_count || 0}</div>
                                    <div class="cc-cell-subtitle">{formatCurrency(item.total_spent || 0, 2)}</div>
                                    <div class="cc-cell-subtitle" style="font-size:0.73rem;opacity:0.75;">{item.profile_id || '—'}</div>
                                </div>

                                {#if expandedMerchantKey === rowKey}
                                    <section class="cc-row-expansion">
                                        <div class="cc-inline-section-heading">
                                            <div>
                                                <div class="cc-inline-title-row">
                                                    <h3>Recent Transactions</h3>
                                                    <div class="cc-inline-merchant-rename">
                                                        <input
                                                            id={`merchant-alias-${rowKey}`}
                                                            class="cc-input cc-inline-merchant-input"
                                                            type="text"
                                                            value={merchantAliasDraftValue(item)}
                                                            placeholder={item.clean_name || item.merchant_key}
                                                            aria-label="Merchant display name"
                                                            on:input={(event) => updateMerchantAliasDraft(item, event.currentTarget.value)}
                                                            on:keydown|stopPropagation={(event) => {
                                                                if (event.key === 'Enter') {
                                                                    event.preventDefault();
                                                                    saveMerchantAlias(item, rowKey);
                                                                }
                                                            }} />
                                                        <button
                                                            type="button"
                                                            class="cc-ghost-btn cc-inline-merchant-btn"
                                                            on:click|stopPropagation={() => updateMerchantAliasDraft(item, item.clean_name || item.merchant_key || '')}
                                                            disabled={merchantSaving}>
                                                            Reset
                                                        </button>
                                                        <button
                                                            type="button"
                                                            class="cc-secondary-btn cc-inline-merchant-btn"
                                                            on:click|stopPropagation={() => saveMerchantAlias(item, rowKey)}
                                                            disabled={merchantSaving}>
                                                            Save
                                                        </button>
                                                    </div>
                                                </div>
                                                <p>
                                                    {item.clean_name || item.merchant_key} · {item.profile_id}
                                                    {item.industry ? ` · ${item.industry}` : ''}
                                                </p>
                                            </div>
                                            <div class="cc-inline-section-stats">
                                                <span>{item.charge_count || 0} txns</span>
                                                <span>{formatCurrency(item.total_spent || 0, 2)}</span>
                                            </div>
                                        </div>
                                        {#if merchantPreviewLoading && selectedMerchantKey === rowKey}
                                            <div class="cc-empty">Loading…</div>
                                        {:else if selectedMerchantKey === rowKey && merchantTransactions.length === 0}
                                            <div class="cc-empty">No transactions found for this merchant.</div>
                                        {:else if selectedMerchantKey === rowKey}
                                            <div class="cc-mini-table-wrap">
                                                <table class="cc-mini-table">
                                                    <thead>
                                                        <tr>
                                                            <th>Date</th>
                                                            <th>Description</th>
                                                            <th>Category</th>
                                                            <th>Amount</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {#each merchantTransactions as tx}
                                                            <tr>
                                                                <td>{formatDate(tx.date)}</td>
                                                                <td>{tx.description}</td>
                                                                <td>{tx.category || 'Uncategorized'}</td>
                                                                <td>{formatCurrency(tx.amount, 2)}</td>
                                                            </tr>
                                                        {/each}
                                                    </tbody>
                                                </table>
                                            </div>
                                        {/if}
                                    </section>
                                {/if}
                            {/each}
                        </div>
                    {/if}
                </div>
        </section>

    {:else if activeTab === 'rules'}
        <!-- ── RULES ─────────────────────────────────────────────── -->
        <div class="cc-shell fade-in-up">
            <section class="cc-pane cc-pane-primary">
                <div class="cc-pane-header">
                    <div class="cc-pane-title">
                        <h3>Rules</h3>
                        <p>Pattern rules that steer categorization before the model decides. User rules win first, then editable system defaults fill in the stable cases.</p>
                    </div>
                    <div class="cc-toolbar">
                        <select class="cc-select" bind:value={ruleSourceFilter}>
                            <option value="all">All sources</option>
                            <option value="user">User rules</option>
                            <option value="system">System rules</option>
                        </select>
                        <select class="cc-select" bind:value={ruleStateFilter}>
                            <option value="all">Active + paused</option>
                            <option value="active">Active only</option>
                            <option value="inactive">Paused only</option>
                        </select>
                        <input class="cc-search" bind:value={ruleSearch} placeholder="Search pattern, category, or type…" />
                        <button class="cc-secondary-btn" type="button" on:click={loadRules} disabled={rulesLoading}>Refresh</button>
                    </div>
                </div>
                <div class="cc-list-wrap">
                    <div class="cc-list-meta">{visibleRules.length} rules · User rules carry the highest priority.</div>
                    {#if rulesLoading}
                        <div class="cc-empty">Loading rules…</div>
                    {:else if visibleRules.length === 0}
                        <div class="cc-empty">No rules matched the current filters.</div>
                    {:else}
                        <div class="cc-table">
                            <div class="cc-table-header" style="--cc-cols: 1.8fr 1fr 0.8fr 0.6fr 0.7fr;">
                                <div>Pattern</div>
                                <div>Category</div>
                                <div>Priority</div>
                                <div>Status</div>
                                <div>Source</div>
                            </div>
                            {#each visibleRules as item}
                                <button
                                    type="button"
                                    class="cc-table-row"
                                    class:cc-table-row-active={item.id === selectedRuleId}
                                    style="--cc-cols: 1.8fr 1fr 0.8fr 0.6fr 0.7fr;"
                                    on:click={() => (selectedRuleId = item.id)}>
                                    <div class="cc-cell-primary">
                                        <div class="cc-cell-title">{item.pattern}</div>
                                        <div class="cc-cell-subtitle">{item.match_type} · #{item.id}</div>
                                    </div>
                                    <div class="cc-cell-subtitle">{item.category}</div>
                                    <div class="cc-cell-subtitle">{item.priority}</div>
                                    <div><span class={ruleStatusBadge(item)}>{item.is_active ? 'Active' : 'Paused'}</span></div>
                                    <div><span class={ruleSourceBadgeClass(item.source)}>{ruleSourceLabel(item.source)}</span></div>
                                </button>
                            {/each}
                        </div>
                    {/if}
                </div>
            </section>

            <aside class="cc-pane cc-pane-drawer">
                <div class="cc-inspector">
                    {#if !selectedRule}
                        <div class="cc-empty">Select a rule to edit its category, priority, and active state.</div>
                    {:else}
                        <section class="cc-inspector-section">
                            <div>
                                <h3>{selectedRule.pattern}</h3>
                                <p>#{selectedRule.id} · {selectedRule.match_type} · created {formatDateTime(selectedRule.created_at)}</p>
                            </div>
                            <div class="cc-form-grid">
                                <div class="cc-field cc-field-full">
                                    <span>Category</span>
                                    <select class="cc-select" bind:value={ruleDraft.category}>
                                        {#each allCategoryNames as categoryName}
                                            <option value={categoryName}>{categoryName}</option>
                                        {/each}
                                    </select>
                                </div>
                                <div class="cc-field">
                                    <span>Priority</span>
                                    <input class="cc-input" type="number" bind:value={ruleDraft.priority} />
                                </div>
                                <div class="cc-field">
                                    <span>State</span>
                                    <select class="cc-select" bind:value={ruleDraft.is_active}>
                                        <option value={true}>Active</option>
                                        <option value={false}>Paused</option>
                                    </select>
                                </div>
                            </div>
                            <div class="cc-actions">
                                <button class="cc-primary-btn" type="button" on:click={saveRule} disabled={ruleSaving}>
                                    {ruleSaving ? 'Saving…' : 'Save Rule'}
                                </button>
                            </div>
                        </section>

                        <section class="cc-inspector-section">
                            <div>
                                <h3>Current Impact</h3>
                                <p>Transactions currently matching this rule under the active profile scope.</p>
                            </div>
                            {#if ruleImpactLoading}
                                <div class="cc-empty">Calculating impact…</div>
                            {:else if !ruleImpact}
                                <div class="cc-empty">Impact preview unavailable.</div>
                            {:else}
                                <div class="cc-stats">
                                    <div class="cc-stat">
                                        <div class="cc-stat-label">Matching Rows</div>
                                        <div class="cc-stat-value">{ruleImpact.match_count}</div>
                                    </div>
                                    <div class="cc-stat">
                                        <div class="cc-stat-label">Rule Category</div>
                                        <div class="cc-stat-value">{ruleImpact.category}</div>
                                    </div>
                                </div>
                                {#if ruleImpact.sample?.length}
                                    <div class="cc-mini-table-wrap">
                                        <table class="cc-mini-table">
                                            <thead>
                                                <tr>
                                                    <th>Date</th>
                                                    <th>Description</th>
                                                    <th>Current Category</th>
                                                    <th>Amount</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                {#each ruleImpact.sample as item}
                                                    <tr>
                                                        <td>{formatDate(item.date)}</td>
                                                        <td>{item.description}</td>
                                                        <td>{item.category}</td>
                                                        <td>{formatCurrency(item.amount, 2)}</td>
                                                    </tr>
                                                {/each}
                                            </tbody>
                                        </table>
                                    </div>
                                {:else}
                                    <div class="cc-empty">No transactions currently match this rule.</div>
                                {/if}
                            {/if}
                        </section>
                    {/if}
                </div>
            </aside>
        </div>

    {:else if activeTab === 'categories'}
        <!-- ── CATEGORIES ─────────────────────────────────────────── -->
        <div class="cc-pane cc-pane-primary fade-in-up">
            <div class="cc-pane-header">
                <div class="cc-pane-title">
                    <h3>Categories</h3>
                    <p>Decide whether each category behaves like a fixed monthly commitment or flexible spend. Locked non-expense system categories stay read-only.</p>
                </div>
                <div class="cc-toolbar">
                    <input class="cc-search" bind:value={categorySearch} placeholder="Search category or spend type…" />
                    <button class="cc-secondary-btn" type="button" on:click={loadCategories} disabled={categoriesLoading}>Refresh</button>
                </div>
            </div>
            <div class="cc-list-wrap">
                <div class="cc-list-meta">{visibleCategories.length} active categories.</div>
                {#if categoriesLoading}
                    <div class="cc-empty">Loading categories…</div>
                {:else if visibleCategories.length === 0}
                    <div class="cc-empty">No categories matched the search.</div>
                {:else}
                    <div class="cc-cat-table" style="--cc-cat-cols: 1.5fr 10rem 1.6rem;">
                        <div class="cc-cat-header" style="--cc-cat-cols: 1.5fr 10rem 1.6rem;">
                            <div>Category</div>
                            <div>Expense Type</div>
                            <div></div>
                        </div>
                        {#each visibleCategories as item (item.name)}
                            {@const isLocked = item.expense_type === 'non_expense'}
                            {@const isSaving = categorySavingKey === item.name}
                            <div class="cc-cat-row" style="--cc-cat-cols: 1.5fr 10rem 1.6rem;" class:cc-cat-row-saving={isSaving}>
                                <div class="cc-cat-name">{item.name}</div>

                                <!-- Expense type pill toggle or lock -->
                                {#if isLocked}
                                    <div class="cc-cat-lock">
                                        <span class="material-symbols-outlined" style="font-size:14px;" title="System classification — cannot be changed">lock</span>
                                        <span style="font-size:0.73rem;">Non-expense</span>
                                    </div>
                                {:else}
                                    <div class="period-toggle-track"
                                         style="--seg-count: 2; --active-idx: {item.expense_type === 'fixed' ? 0 : 1};">
                                        <div class="period-toggle-thumb"></div>
                                        <button
                                            class="period-toggle-label"
                                            class:active={item.expense_type === 'fixed'}
                                            disabled={isSaving}
                                            on:click={() => saveExpenseTypeInline(item, 'fixed')}>
                                            Fixed
                                        </button>
                                        <button
                                            class="period-toggle-label"
                                            class:active={item.expense_type === 'variable'}
                                            disabled={isSaving}
                                            on:click={() => saveExpenseTypeInline(item, 'variable')}>
                                            Variable
                                        </button>
                                    </div>
                                {/if}

                                <!-- Saving indicator -->
                                <div style="display:flex;align-items:center;justify-content:center;">
                                    {#if isSaving}
                                        <span class="material-symbols-outlined" style="font-size:14px;color:var(--accent);animation:spin 0.8s linear infinite;">progress_activity</span>
                                    {/if}
                                </div>
                            </div>
                        {/each}
                    </div>
                {/if}
            </div>
        </div>

    {:else if activeTab === 'history'}
        <!-- ── HISTORY ────────────────────────────────────────────── -->
        <div class="cc-shell fade-in-up">
            <section class="cc-pane cc-pane-primary">
                <div class="cc-pane-header">
                    <div class="cc-pane-title">
                        <h3>History</h3>
                        <p>Recent Copilot reads and writes, with the stored response and generated SQL kept together for auditability.</p>
                    </div>
                    <div class="cc-toolbar">
                        <input class="cc-search" bind:value={historySearch} placeholder="Search prompt, response, or SQL…" />
                        <button class="cc-secondary-btn" type="button" on:click={loadHistory} disabled={historyLoading}>Refresh</button>
                    </div>
                </div>
                <div class="cc-list-wrap">
                    <div class="cc-list-meta">{visibleHistory.length} recent Copilot interactions for the current profile.</div>
                    {#if historyLoading}
                        <div class="cc-empty">Loading history…</div>
                    {:else if visibleHistory.length === 0}
                        <div class="cc-empty">No history matched the search.</div>
                    {:else}
                        <div class="cc-table">
                            <div class="cc-table-header" style="--cc-cols: 1.7fr 0.75fr 0.65fr 0.95fr;">
                                <div>Prompt</div>
                                <div>Operation</div>
                                <div>Rows</div>
                                <div>Created</div>
                            </div>
                            {#each visibleHistory as item}
                                <button
                                    type="button"
                                    class="cc-table-row"
                                    class:cc-table-row-active={item.id === selectedHistoryId}
                                    style="--cc-cols: 1.7fr 0.75fr 0.65fr 0.95fr;"
                                    on:click={() => (selectedHistoryId = item.id)}>
                                    <div class="cc-cell-primary">
                                        <div class="cc-cell-title">{item.user_message}</div>
                                        <div class="cc-cell-subtitle">{item.assistant_response || 'No response saved'}</div>
                                    </div>
                                    <div class="cc-cell-subtitle">{item.operation_type || 'read'}</div>
                                    <div class="cc-cell-subtitle">{item.rows_affected || 0}</div>
                                    <div class="cc-cell-subtitle">{formatDateTime(item.created_at)}</div>
                                </button>
                            {/each}
                        </div>
                    {/if}
                </div>
            </section>

            <aside class="cc-pane cc-pane-drawer">
                <div class="cc-inspector">
                    {#if !selectedHistory}
                        <div class="cc-empty">Select a history row to inspect its response and generated SQL.</div>
                    {:else}
                        <section class="cc-inspector-section">
                            <div>
                                <h3>Interaction #{selectedHistory.id}</h3>
                                <p>{selectedHistory.operation_type || 'read'} · {selectedHistory.rows_affected || 0} rows · {formatDateTime(selectedHistory.created_at)}</p>
                            </div>
                            <div class="cc-field cc-field-full">
                                <span>User Prompt</span>
                                <textarea class="cc-textarea" rows="3" disabled>{selectedHistory.user_message}</textarea>
                            </div>
                            <div class="cc-field cc-field-full">
                                <span>Assistant Response</span>
                                <textarea class="cc-textarea" rows="5" disabled>{selectedHistory.assistant_response || '—'}</textarea>
                            </div>
                            <div class="cc-field cc-field-full">
                                <span>Generated SQL</span>
                                <textarea class="cc-textarea cc-textarea-mono" rows="8" disabled>{selectedHistory.generated_sql || '—'}</textarea>
                            </div>
                            <div class="cc-actions">
                                <button class="cc-secondary-btn" type="button" on:click={reopenInCopilot}>Open in Copilot</button>
                            </div>
                        </section>
                    {/if}
                </div>
            </aside>
        </div>
    {/if}
</div>

<style>
    .cc-cat-row-saving {
        opacity: 0.6;
        pointer-events: none;
    }

    @keyframes spin {
        from { transform: rotate(0deg); }
        to   { transform: rotate(360deg); }
    }

    /* ── Connections tab ───────────────────────────────────────── */
    .cc-conn-section {
        padding: 16px 20px;
        border-bottom: 1px solid var(--card-border);
    }
    .cc-conn-section:last-of-type {
        border-bottom: none;
    }
    .cc-conn-section-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 12px;
    }
    .cc-conn-section-title {
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .cc-conn-count {
        font-size: 12px;
        color: var(--text-muted);
    }
    .cc-provider-badge {
        display: inline-flex;
        align-items: center;
        padding: 3px 8px;
        border-radius: 6px;
        font-size: 11px;
        font-weight: 600;
        letter-spacing: 0.02em;
    }
    .cc-provider-teller {
        background: color-mix(in srgb, #6366f1 12%, transparent);
        color: #6366f1;
    }
    .cc-provider-simplefin {
        background: color-mix(in srgb, #10b981 12%, transparent);
        color: #10b981;
    }
    .cc-conn-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 10px 12px;
        border-radius: 8px;
        background: var(--bg);
        margin-bottom: 6px;
    }
    .cc-conn-row:last-child {
        margin-bottom: 0;
    }
    .cc-conn-info {
        flex: 1;
        min-width: 0;
    }
    .cc-conn-name {
        font-size: 13px;
        font-weight: 500;
        color: var(--text-primary);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }
    .cc-conn-meta {
        font-size: 11px;
        color: var(--text-muted);
        margin-top: 2px;
    }
    .cc-conn-remove {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 28px;
        height: 28px;
        border: none;
        border-radius: 6px;
        background: transparent;
        color: var(--text-muted);
        cursor: pointer;
        flex-shrink: 0;
        transition: all 0.15s ease;
    }
    .cc-conn-remove:hover {
        background: var(--negative-light);
        color: var(--negative);
    }
    .cc-conn-empty {
        font-size: 12px;
        color: var(--text-muted);
        padding: 8px 0;
    }
</style>
