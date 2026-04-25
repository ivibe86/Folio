<script>
    import '$lib/styles/copilot.css';
    import { goto } from '$app/navigation';
    import { page } from '$app/stores';
    import { onMount, tick } from 'svelte';
    import { api, invalidateCache } from '$lib/api.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import { formatCurrency, formatDate } from '$lib/utils.js';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';
    import CopilotChart from '$lib/components/CopilotChart.svelte';

    let loading = false;
    let sidebarLoading = true;
    let actionNotice = '';
    let lastLoadedProfile = undefined;
    let input = '';
    let chatContainer;
    let showSqlForMsg = {};
    let showSqlForHistory = {};
    let historyOpen = false;
    let cancelStream = null;  // holds the in-flight stream's cancel fn

    let recurringData = null;
    let historyItems = [];
    let localLlmStatus = null;
    let localLlmCatalog = null;
    let copilotModel = '';
    let copilotModelSaving = false;
    let copilotModelInstalling = false;

    // Strip <observation>/<memory_proposal> tags from streamed text so they never
    // briefly appear in the UI before the server-side cleanup at done. Also reverses
    // a known model failure mode where it emits literal '/n' instead of a newline.
    function scrubMemoryTags(text) {
        if (!text) return text;
        let out = text.replace(/<observation\b[^>]*>[\s\S]*?<\/observation>/gi, '');
        out = out.replace(/<memory_proposal\b[^>]*>[\s\S]*?<\/memory_proposal>/gi, '');
        // Mid-stream: hide the open tag onward until close arrives, so we don't flicker XML
        const openIdx = out.search(/<(observation|memory_proposal)\b/i);
        if (openIdx >= 0) out = out.slice(0, openIdx);
        // Convert literal '/n' (model misfire) → real newline, then collapse runs
        out = out.replace(/\/n/g, '\n').replace(/\n{3,}/g, '\n\n');
        return out;
    }

    let messages = [
        {
            role: 'assistant',
            content: "Copilot is your language layer. Ask it to rename merchants, recategorize transactions, explain categorization, or preview a change before you confirm it.",
            operation: null,
            data: null,
            sql: null,
            preview_changes: [],
            needs_confirmation: false,
            rows_affected: 0
        }
    ];

    // ── Chip action descriptors ──
    // Each chip describes a structured operation with its required inputs.
    // Chips with no inputs execute immediately; others show an inline mini-form.
    const chipActions = [
        {
            id: 'explain_category',
            label: 'Explain why a merchant is categorized',
            inputs: [
                { key: 'merchant', label: 'Merchant name', type: 'text', placeholder: 'e.g. DoorDash', required: true },
            ],
        },
        {
            id: 'find_missing_categories',
            label: 'Find merchants missing categories',
            inputs: [],
        },
        {
            id: 'bulk_recategorize',
            label: 'Move a merchant\'s transactions to a category',
            inputs: [
                { key: 'merchant', label: 'Merchant', type: 'text', placeholder: 'e.g. Netflix', required: true },
                { key: 'category', label: 'New category', type: 'select', required: true },
            ],
        },
        {
            id: 'create_rule',
            label: 'Create a category rule',
            inputs: [
                { key: 'pattern', label: 'Merchant pattern', type: 'text', placeholder: 'e.g. CLAUDE PRO', required: true },
                { key: 'category', label: 'Category', type: 'select', required: true },
            ],
        },
        {
            id: 'rename_merchant',
            label: 'Rename a merchant',
            inputs: [
                { key: 'old_name', label: 'Current name', type: 'text', placeholder: 'e.g. AMZN MKTPLACE PMTS', required: true },
                { key: 'new_name', label: 'New display name', type: 'text', placeholder: 'e.g. Amazon Marketplace', required: true },
            ],
        },
    ];

    // Chip form state
    let activeChip = null;       // id of the currently expanded chip, or null
    let chipFormValues = {};     // { fieldKey: value } for the active form
    let categories = [];         // loaded on mount for dropdown inputs

    $: activeProfileId = $activeProfile || 'household';
    $: scopedProfile = activeProfileId !== 'household' ? activeProfileId : null;
    $: unreadEvents = recurringData?.events?.filter((event) => !event.is_read) || [];
    $: recentHistory = historyItems.slice(0, 6);
    $: copilotModelOptions = Array.isArray(localLlmCatalog?.tiers)
        ? localLlmCatalog.tiers.flatMap((tier) => tier.models.filter((model) => model.task_fit?.includes('copilot')))
        : [];
    $: selectedCopilotModelMeta = copilotModelOptions.find((model) => model.id === copilotModel) || null;

    onMount(async () => {
        const prompt = $page.url.searchParams.get('prompt');
        if (prompt) input = prompt;
        await Promise.all([refreshSidebar(), loadLocalLlm()]);
        lastLoadedProfile = activeProfileId;
        // Load categories for chip form dropdowns
        try {
            const catRes = await api.getCategories();
            categories = (catRes?.categories ?? catRes ?? []).filter(c => c.is_active !== 0);
        } catch (e) { categories = []; }
    });

    $: if (activeProfileId && lastLoadedProfile !== undefined && activeProfileId !== lastLoadedProfile) {
        lastLoadedProfile = activeProfileId;
        refreshSidebar();
    }

    function setNotice(message) {
        actionNotice = message;
        setTimeout(() => {
            if (actionNotice === message) actionNotice = '';
        }, 3000);
    }

    function openHistory() {
        historyOpen = true;
    }

    function closeHistory() {
        historyOpen = false;
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
            minute: '2-digit'
        });
    }

    function formatTableValue(key, value) {
        if (value === null || value === undefined || value === '') return '—';
        if (typeof value === 'boolean') return value ? 'Yes' : 'No';
        if (typeof value === 'number') {
            const currencyKeys = ['amount', 'total', 'balance', 'sum', 'avg', 'spent', 'income', 'expense', 'net', 'owed', 'assets', 'budget'];
            if (currencyKeys.some((token) => key.toLowerCase().includes(token))) return formatCurrency(value, 2);
            return Number.isInteger(value) ? value.toString() : value.toFixed(2);
        }
        if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)) {
            if (key.toLowerCase().includes('created') || key.toLowerCase().includes('updated') || key.toLowerCase().includes('synced')) {
                return formatDateTime(value);
            }
            return formatDate(value);
        }
        return String(value);
    }

    function getColumns(data) {
        if (!data || data.length === 0) return [];
        return Object.keys(data[0]);
    }

    function pushAssistantMessage(content, operation = null, extras = {}) {
        messages = [...messages, {
            role: 'assistant',
            content,
            operation,
            data: extras.data || null,
            sql: extras.sql || null,
            preview_changes: extras.preview_changes || [],
            confirmation_id: extras.confirmation_id || null,
            needs_confirmation: extras.needs_confirmation || false,
            rows_affected: extras.rows_affected || 0,
            original_question: extras.original_question || null
        }];
    }

    async function refreshSidebar() {
        sidebarLoading = true;
        try {
            const [recurringResult, historyResult] = await Promise.all([
                api.getRecurring().catch(() => null),
                api.getCopilotHistory(20).catch(() => ({ items: [] })),
            ]);
            recurringData = recurringResult;
            historyItems = historyResult?.items || [];
        } finally {
            sidebarLoading = false;
        }
    }

    async function loadLocalLlm() {
        try {
            const [status, catalog] = await Promise.all([
                api.getLocalLlmStatus(),
                api.getLocalLlmCatalog(),
            ]);
            localLlmStatus = status;
            localLlmCatalog = catalog;
            copilotModel = status?.selectedCopilotModel || '';
        } catch (error) {
            localLlmStatus = null;
            localLlmCatalog = null;
        }
    }

    async function updateCopilotModelSelection(nextModel) {
        if (!nextModel || nextModel === localLlmStatus?.selectedCopilotModel || copilotModelSaving) return;
        copilotModelSaving = true;
        try {
            const result = await api.updateLocalLlmSettings({ copilot_model: nextModel });
            if (result?.status) {
                localLlmStatus = result.status;
                copilotModel = result.status.selectedCopilotModel || nextModel;
            } else {
                copilotModel = nextModel;
            }
            setNotice(`Copilot model switched to ${nextModel}.`);
        } catch (error) {
            copilotModel = localLlmStatus?.selectedCopilotModel || '';
            setNotice(error?.message || 'Failed to switch Copilot model.');
        } finally {
            copilotModelSaving = false;
        }
    }

    async function installCopilotModel() {
        if (!copilotModel || copilotModelInstalling || localLlmStatus?.provider !== 'ollama' || !localLlmStatus?.ollamaReachable) return;
        copilotModelInstalling = true;
        try {
            const result = await api.installLocalLlmModel(copilotModel);
            if (result?.status) {
                localLlmStatus = result.status;
            }
            await loadLocalLlm();
            setNotice(`${copilotModel} installed in Ollama.`);
        } catch (error) {
            setNotice(error?.message || `Failed to install ${copilotModel}.`);
        } finally {
            copilotModelInstalling = false;
        }
    }

    async function openPage(path, params = new URLSearchParams()) {
        const query = params.toString();
        await goto(query ? `${path}?${query}` : path);
    }

    async function openControlCenter(tab, { prompt = '', merchantFilter = '' } = {}) {
        const params = new URLSearchParams();
        if (tab && tab !== 'merchants') params.set('tab', tab);
        if (prompt) params.set('prompt', prompt);
        if (merchantFilter) params.set('merchant_filter', merchantFilter);
        await openPage('/control-center', params);
    }

    async function handleShortcut(question) {
        const lower = question.toLowerCase();

        if (/\b(open|show)\b/.test(lower) && /\b(transaction|transactions)\b/.test(lower)) {
            await openPage('/transactions');
            return "Opened Transactions.";
        }
        if (/\b(open|show)\b/.test(lower) && /\b(merchant|merchants)\b/.test(lower)) {
            await openControlCenter('merchants');
            return "Opened Control Center on Merchants.";
        }
        if (/\b(open|show)\b/.test(lower) && /\brules?\b/.test(lower)) {
            await openControlCenter('rules');
            return "Opened Control Center on Rules.";
        }
        if (/\b(open|show)\b/.test(lower) && /\b(categories|category)\b/.test(lower)) {
            await openControlCenter('categories');
            return "Opened Control Center on Categories.";
        }
        if (/\b(open|show)\b/.test(lower) && /\b(subscription|subscriptions|recurring)\b/.test(lower)) {
            await openControlCenter('merchants', { merchantFilter: 'subscriptions' });
            return "Opened Control Center on recurring merchants.";
        }
        if (/\b(open|show)\b/.test(lower) && /\bhistory\b/.test(lower)) {
            openHistory();
            return "Opened recent Copilot activity.";
        }
        if (/\b(sync|refresh)\b/.test(lower) && !/\bhistory\b/.test(lower)) {
            const result = await runSync();
            return `Sync finished: ${result.accounts} accounts and ${result.transactions} transactions processed.`;
        }
        if (/\b(redetect|rescan)\b/.test(lower) && /\b(subscription|subscriptions|recurring)\b/.test(lower)) {
            await runRedetectSubscriptions();
            return "Subscription detection has been re-run.";
        }
        if (/\b(mark|clear)\b/.test(lower) && /\b(alert|alerts|event|events)\b/.test(lower) && /\bread\b/.test(lower)) {
            await markAllEventsRead();
            return "Marked all subscription alerts as read.";
        }

        return null;
    }

    function stopStream() {
        if (cancelStream) {
            try { cancelStream(); } catch {}
            cancelStream = null;
        }
        loading = false;
    }

    async function send() {
        const question = input.trim();
        if (!question) return;

        // Cancel any in-flight request (cancel-on-resubmit)
        if (loading) stopStream();

        messages = [...messages, {
            role: 'user',
            content: question,
            operation: null,
            data: null,
            sql: null,
            preview_changes: [],
            needs_confirmation: false,
            rows_affected: 0
        }];
        input = '';
        loading = true;

        await tick();
        scrollToBottom();

        try {
            const shortcut = await handleShortcut(question);
            if (shortcut) {
                pushAssistantMessage(shortcut, 'success');
                await refreshSidebar();
                loading = false;
                return;
            }
        } catch {}

        // Build chat history from prior turns
        const history = messages
            .slice(0, -1)
            .filter(m => m && m.content && (m.role === 'user' || (m.role === 'assistant' && m.operation && m.operation !== 'error')))
            .slice(-12)
            .map(m => ({ role: m.role, content: m.content }));

        // Add a live assistant placeholder that we'll mutate as events arrive
        messages = [...messages, {
            role: 'assistant',
            content: '',
            operation: 'streaming',
            data: null,
            sql: null,
            preview_changes: [],
            needs_confirmation: false,
            rows_affected: 0,
            original_question: question,
            tool_trace: [],
            active_tool: null,
        }];
        const streamIdx = messages.length - 1;
        await tick();
        scrollToBottom();

        let currentContent = '';
        cancelStream = api.askCopilotStream(question, activeProfileId, history, (ev) => {
            const msg = { ...messages[streamIdx] };
            switch (ev.type) {
                case 'reset_text':
                    currentContent = '';
                    msg.content = '';
                    break;
                case 'token':
                    currentContent += ev.text || '';
                    msg.content = scrubMemoryTags(currentContent);
                    break;
                case 'tool_call':
                    msg.active_tool = ev.name;
                    msg.tool_trace = [...(msg.tool_trace || []), { name: ev.name, args: ev.args, duration_ms: null }];
                    break;
                case 'chart':
                    msg.chart = ev.chart;
                    break;
                case 'tool_result': {
                    msg.active_tool = null;
                    const trace = [...(msg.tool_trace || [])];
                    for (let i = trace.length - 1; i >= 0; i--) {
                        if (trace[i].name === ev.name && trace[i].duration_ms == null) {
                            trace[i] = { ...trace[i], duration_ms: ev.duration_ms };
                            break;
                        }
                    }
                    msg.tool_trace = trace;
                    break;
                }
                case 'done':
                    msg.content = (ev.answer || currentContent || '').trim();
                    msg.data = ev.data || null;
                    msg.data_source = ev.data_source || null;
                    msg.tool_trace = ev.tool_trace || msg.tool_trace || [];
                    msg.active_tool = null;
                    msg.memory_proposals = ev.memory_proposals || [];
                    if (ev.chart) msg.chart = ev.chart;
                    if (ev.pending_write) {
                        msg.operation = 'write_preview';
                        msg.confirmation_id = ev.pending_write.confirmation_id;
                        msg.sql = ev.pending_write.sql;
                        msg.preview_changes = ev.pending_write.preview_changes || [];
                        msg.needs_confirmation = true;
                        msg.rows_affected = ev.pending_write.rows_affected || 0;
                    } else {
                        msg.operation = 'read';
                        msg.rows_affected = Array.isArray(ev.data) ? ev.data.length : 0;
                    }
                    loading = false;
                    cancelStream = null;
                    break;
                case 'memory_update':
                    // Late-arriving proposals from the post-turn detector. Append to
                    // any existing proposals, dedup by id so re-emits don't duplicate.
                    {
                        const incoming = ev.memory_proposals || [];
                        const existing = msg.memory_proposals || [];
                        const seen = new Set(existing.map((p) => p.id));
                        const merged = [...existing];
                        for (const p of incoming) {
                            if (!seen.has(p.id)) merged.push(p);
                        }
                        msg.memory_proposals = merged;
                    }
                    break;
                case 'error':
                    msg.content = ev.message || 'Something went wrong.';
                    msg.operation = 'error';
                    msg.active_tool = null;
                    loading = false;
                    cancelStream = null;
                    break;
            }
            messages[streamIdx] = msg;
            messages = messages;
            tick().then(scrollToBottom);
        });
    }

    async function saveInsight(msgIndex) {
        const msg = messages[msgIndex];
        if (!msg || msg.saved || msg.saving) return;

        let question = msg.original_question;
        if (!question) {
            for (let j = msgIndex - 1; j >= 0; j--) {
                if (messages[j].role === 'user') {
                    question = messages[j].content;
                    break;
                }
            }
        }
        if (!question || !msg.content) return;

        messages[msgIndex] = { ...msg, saving: true };
        messages = [...messages];

        try {
            const result = await api.saveInsight(question, msg.content, 'insight', null, activeProfileId);
            messages[msgIndex] = { ...messages[msgIndex], saving: false, saved: !!result?.saved };
            messages = [...messages];
            if (result?.saved && result.entry?.body) {
                setNotice(`Saved to memory: ${result.entry.body}`);
            } else if (result?.reason) {
                setNotice(result.reason);
            } else {
                setNotice('Saved to memory.');
            }
        } catch (error) {
            messages[msgIndex] = { ...messages[msgIndex], saving: false };
            messages = [...messages];
            setNotice(error?.message || 'Failed to save insight.');
        }
    }

    async function acceptMemoryProposal(msgIndex, proposalId) {
        try {
            await api.acceptMemoryProposal(proposalId, null, activeProfileId);
            const msg = { ...messages[msgIndex] };
            msg.memory_proposals = (msg.memory_proposals || []).filter((p) => p.id !== proposalId);
            messages[msgIndex] = msg;
            messages = [...messages];
            setNotice('Added to memory.');
        } catch (error) {
            setNotice(error?.message || 'Failed to accept proposal.');
        }
    }

    async function rejectMemoryProposal(msgIndex, proposalId) {
        try {
            await api.rejectMemoryProposal(proposalId, activeProfileId);
            const msg = { ...messages[msgIndex] };
            msg.memory_proposals = (msg.memory_proposals || []).filter((p) => p.id !== proposalId);
            messages[msgIndex] = msg;
            messages = [...messages];
        } catch (error) {
            setNotice(error?.message || 'Failed to reject proposal.');
        }
    }

    async function confirmWrite(msgIndex) {
        const msg = messages[msgIndex];
        if (!msg || !msg.confirmation_id) return;

        loading = true;
        await tick();

        try {
            const res = await api.confirmCopilotWrite(msg.original_question, msg.confirmation_id, activeProfileId);
            messages[msgIndex] = {
                ...messages[msgIndex],
                needs_confirmation: false,
                confirmed: true
            };
            messages = [...messages];

            pushAssistantMessage(res.answer || `Updated ${res.rows_affected} transaction(s).`, 'write_executed', {
                rows_affected: res.rows_affected || 0
            });

            invalidateCache();
            await refreshSidebar();
        } catch (error) {
            pushAssistantMessage("Failed to execute the operation. Please try again.", 'error');
        } finally {
            loading = false;
            await tick();
            scrollToBottom();
        }
    }

    function cancelWrite(msgIndex) {
        messages[msgIndex] = {
            ...messages[msgIndex],
            needs_confirmation: false,
            confirmed: false
        };
        messages = [...messages];
        pushAssistantMessage("Operation cancelled.");
    }

    function toggleSql(msgIndex) {
        showSqlForMsg = { ...showSqlForMsg, [msgIndex]: !showSqlForMsg[msgIndex] };
    }

    function toggleHistorySql(id) {
        showSqlForHistory = { ...showSqlForHistory, [id]: !showSqlForHistory[id] };
    }

    // ── Chip action functions ──

    function buildChipMessage(chip, values) {
        if (chip.id === 'explain_category') return `Why is ${values.merchant} categorized the way it is?`;
        if (chip.id === 'find_missing_categories') return 'Show me merchants with missing categories';
        if (chip.id === 'bulk_recategorize') return `Move all ${values.merchant} transactions to ${values.category}`;
        if (chip.id === 'create_rule') return `Create a rule: ${values.pattern} → ${values.category}`;
        if (chip.id === 'rename_merchant') return `Rename ${values.old_name} to ${values.new_name}`;
        return chip.label;
    }

    async function executeChipAction(id, values) {
        if (id === 'explain_category') {
            const res = await api.explainCategory(values.merchant, scopedProfile);
            pushAssistantMessage(res.answer, res.operation || 'read', {
                data: res.samples?.length ? res.samples : null,
            });
        } else if (id === 'find_missing_categories') {
            const res = await api.getMerchantsMissingCategory(scopedProfile);
            pushAssistantMessage(res.answer, res.operation || 'read', {
                data: res.items?.length ? res.items : null,
            });
        } else if (id === 'bulk_recategorize') {
            const res = await api.bulkRecategorizePreview(values.merchant, values.category, scopedProfile);
            if (!res.needs_confirmation) {
                pushAssistantMessage(res.answer, 'read');
            } else {
                pushAssistantMessage(res.answer, 'write_preview', {
                    data: res.samples?.length ? res.samples : null,
                    preview_changes: res.preview_changes || [],
                    confirmation_id: res.confirmation_id,
                    needs_confirmation: true,
                    rows_affected: res.count || 0,
                    original_question: `Move all ${values.merchant} transactions to ${values.category}`,
                });
            }
        } else if (id === 'create_rule') {
            const res = await api.previewRuleCreation(values.pattern, values.category, scopedProfile);
            pushAssistantMessage(res.answer, 'write_preview', {
                data: res.samples?.length ? res.samples : null,
                preview_changes: res.preview_changes || [],
                confirmation_id: res.confirmation_id,
                needs_confirmation: true,
                rows_affected: res.count || 0,
                original_question: `Create rule: ${values.pattern} → ${values.category}`,
            });
        } else if (id === 'rename_merchant') {
            const res = await api.renameMerchantPreview(values.old_name, values.new_name, scopedProfile);
            if (!res.needs_confirmation) {
                pushAssistantMessage(res.answer, 'read');
            } else {
                pushAssistantMessage(res.answer, 'write_preview', {
                    data: res.samples?.length ? res.samples : null,
                    preview_changes: res.preview_changes || [],
                    confirmation_id: res.confirmation_id,
                    needs_confirmation: true,
                    rows_affected: res.count || 0,
                    original_question: `Rename ${values.old_name} to ${values.new_name}`,
                });
            }
        }
    }

    async function activateChip(chip) {
        if (chip.inputs.length === 0) {
            // No inputs needed — execute immediately
            const userMsg = buildChipMessage(chip, {});
            messages = [...messages, {
                role: 'user', content: userMsg, operation: null,
                data: null, sql: null, preview_changes: [], needs_confirmation: false, rows_affected: 0,
            }];
            loading = true;
            await tick();
            scrollToBottom();
            try {
                await executeChipAction(chip.id, {});
            } catch (err) {
                pushAssistantMessage("Sorry, I couldn't process that. Please try again.", 'error');
            } finally {
                loading = false;
                await tick();
                scrollToBottom();
            }
        } else {
            activeChip = chip.id;
            chipFormValues = {};
        }
    }

    async function submitChipForm() {
        const chip = chipActions.find(c => c.id === activeChip);
        if (!chip) return;
        const userMsg = buildChipMessage(chip, chipFormValues);
        messages = [...messages, {
            role: 'user', content: userMsg, operation: null,
            data: null, sql: null, preview_changes: [], needs_confirmation: false, rows_affected: 0,
        }];
        loading = true;
        activeChip = null;
        await tick();
        scrollToBottom();
        try {
            await executeChipAction(chip.id, chipFormValues);
        } catch (err) {
            pushAssistantMessage("Sorry, I couldn't process that. Please try again.", 'error');
        } finally {
            loading = false;
            await tick();
            scrollToBottom();
        }
    }

    function reuseHistoryPrompt(item) {
        input = item.user_message || '';
        closeHistory();
    }

    function scrollToBottom() {
        if (chatContainer) chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    function handleKeydown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            send();
        } else if (e.key === 'Escape' && loading) {
            e.preventDefault();
            stopStream();
        }
    }

    async function runSync() {
        try {
            const result = await api.sync();
            invalidateCache();
            setNotice('Data sync completed.');
            await refreshSidebar();
            return result;
        } catch (error) {
            setNotice('Sync failed.');
            throw error;
        }
    }

    async function runRedetectSubscriptions() {
        try {
            await api.redetectSubscriptions(scopedProfile);
            invalidateCache();
            setNotice('Subscriptions re-scanned.');
            await refreshSidebar();
        } catch (error) {
            setNotice('Subscription re-scan failed.');
            throw error;
        }
    }

    async function markAllEventsRead() {
        if (!unreadEvents.length) {
            setNotice('No unread alerts to clear.');
            return;
        }
        try {
            await api.markEventsRead(unreadEvents.map((event) => event.id));
            await refreshSidebar();
            setNotice('Alerts marked as read.');
        } catch (error) {
            setNotice('Failed to mark alerts read.');
            throw error;
        }
    }
</script>

<div class="flex flex-col gap-4">
    <div class="flex items-start justify-between gap-4 flex-wrap fade-in">
        <div class="flex items-center gap-3">
            <div class="w-10 h-10 rounded-xl flex items-center justify-center copilot-hero-icon">
                <span class="material-symbols-outlined text-white text-[18px]">auto_awesome</span>
            </div>
            <div>
                <h2 class="folio-page-title" style="font-size: clamp(1.25rem, 0.8vw + 1rem, 1.7rem)">Copilot</h2>
                <p class="folio-page-subtitle">Chat-first assistant for explanations, proposed edits, and confirmation-backed mutations.</p>
            </div>
        </div>
        <div class="copilot-header-actions">
            <a href="/copilot/memory" class="copilot-side-pill" data-sveltekit-preload-data="hover">
                Memory
            </a>
            <button
                type="button"
                class="copilot-side-pill"
                class:copilot-side-pill-active={historyOpen}
                on:click={openHistory}>
                History
            </button>
            <ProfileSwitcher />
        </div>
    </div>

    {#if actionNotice}
        <div class="copilot-notice fade-in">{actionNotice}</div>
    {/if}

    <div class="copilot-chat-layout fade-in-up">
        <section class="copilot-chat-shell">
            <div bind:this={chatContainer} class="flex-1 overflow-y-auto space-y-3.5 pr-2 mb-4" style="scrollbar-width: thin">
                {#each messages as msg, i}
                    <div class="flex {msg.role === 'user' ? 'justify-end' : 'justify-start'} fade-in" style="animation-delay: {Math.min(i * 40, 240)}ms">
                        <div class="max-w-[90%] {msg.role === 'user' ? 'order-2' : ''}" class:w-full={msg.role === 'assistant' && (msg.chart || (msg.data && msg.data.length > 0))}>
                            {#if msg.role === 'assistant'}
                                <div class="flex items-start gap-2.5" class:copilot-wide-row={msg.chart || (msg.data && msg.data.length > 0)}>
                                    <div class="w-6 h-6 rounded-lg flex items-center justify-center flex-shrink-0 mt-0.5" style="background: var(--accent-soft)">
                                        <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">auto_awesome</span>
                                    </div>
                                    <div class="copilot-msg-container" class:copilot-wide-content={msg.chart || (msg.data && msg.data.length > 0)}>
                                        {#if msg.operation === 'write_preview'}
                                            <div class="copilot-op-badge copilot-op-write">
                                                <span class="material-symbols-outlined text-[12px]">edit</span>
                                                Write Preview · {msg.rows_affected} row{msg.rows_affected !== 1 ? 's' : ''}
                                            </div>
                                        {:else if msg.operation === 'write_executed' || msg.operation === 'success'}
                                            <div class="copilot-op-badge copilot-op-success">
                                                <span class="material-symbols-outlined text-[12px]">check_circle</span>
                                                Completed
                                            </div>
                                        {:else if msg.operation === 'read' && msg.rows_affected > 0}
                                            <div class="copilot-op-badge copilot-op-read">
                                                <span class="material-symbols-outlined text-[12px]">search</span>
                                                Query · {msg.rows_affected} result{msg.rows_affected !== 1 ? 's' : ''}
                                            </div>
                                        {:else if msg.operation === 'error'}
                                            <div class="copilot-op-badge copilot-op-error">
                                                <span class="material-symbols-outlined text-[12px]">error</span>
                                                Error
                                            </div>
                                        {/if}

                                        {#if msg.active_tool}
                                            <div class="copilot-op-badge copilot-op-read" style="margin-bottom: 6px; opacity: 0.9;">
                                                <span class="material-symbols-outlined text-[12px] animate-spin">progress_activity</span>
                                                Calling {msg.active_tool.replaceAll('_', ' ')}…
                                            </div>
                                        {/if}

                                        {#if msg.content?.trim() || msg.operation === 'streaming'}
                                            <div class="card" style="padding: 0.75rem 1rem">
                                                <p class="text-[13px] leading-relaxed whitespace-pre-wrap" style="color: var(--text-primary)">{msg.content}{#if msg.operation === 'streaming' && loading}<span class="copilot-cursor">▌</span>{/if}</p>
                                            </div>
                                        {/if}

                                        {#if msg.chart && msg.chart.labels && msg.chart.labels.length > 0}
                                            <CopilotChart spec={msg.chart} />
                                        {/if}

                                        {#if msg.preview_changes && msg.preview_changes.length > 0}
                                            <div class="copilot-change-list">
                                                {#each msg.preview_changes as change}
                                                    <span class="copilot-change-chip">
                                                        {change.column}: {formatTableValue(change.column, change.new_value)}
                                                    </span>
                                                {/each}
                                            </div>
                                        {/if}

                                        {#if msg.data && msg.data.length > 0}
                                            {@const columns = getColumns(msg.data)}
                                            <div class="copilot-data-table-wrap">
                                                <table class="copilot-data-table">
                                                    <thead>
                                                        <tr>
                                                            {#each columns as col}
                                                                <th>{col.replace(/_/g, ' ')}</th>
                                                            {/each}
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {#each msg.data.slice(0, 20) as row}
                                                            <tr>
                                                                {#each columns as col}
                                                                    <td>{formatTableValue(col, row[col])}</td>
                                                                {/each}
                                                            </tr>
                                                        {/each}
                                                    </tbody>
                                                </table>
                                            </div>
                                        {/if}

                                        {#if msg.needs_confirmation && !msg.confirmed}
                                            <div class="copilot-confirm-card">
                                                <p class="text-[11px] font-medium mb-3" style="color: var(--text-secondary)">
                                                    This will modify your data. Review the proposed changes and confirm.
                                                </p>
                                                <div class="flex items-center gap-2">
                                                    <button class="copilot-confirm-btn copilot-confirm-yes" on:click={() => confirmWrite(i)} disabled={loading}>
                                                        <span class="material-symbols-outlined text-[14px]">check</span>
                                                        Confirm
                                                    </button>
                                                    <button class="copilot-confirm-btn copilot-confirm-no" on:click={() => cancelWrite(i)} disabled={loading}>
                                                        <span class="material-symbols-outlined text-[14px]">close</span>
                                                        Cancel
                                                    </button>
                                                </div>
                                            </div>
                                        {:else if msg.confirmed}
                                            <div class="copilot-confirmed-badge">
                                                <span class="material-symbols-outlined text-[12px]">check_circle</span>
                                                Confirmed & executed
                                            </div>
                                        {/if}

                                        {#if msg.sql}
                                            <button class="copilot-sql-toggle" on:click={() => toggleSql(i)}>
                                                <span class="material-symbols-outlined text-[12px]">code</span>
                                                {showSqlForMsg[i] ? 'Hide SQL' : 'Show SQL'}
                                            </button>
                                            {#if showSqlForMsg[i]}
                                                <pre class="copilot-sql-block">{msg.sql}</pre>
                                            {/if}
                                        {/if}

                                        {#if msg.tool_trace && msg.tool_trace.length > 0 && msg.operation !== 'streaming'}
                                            <button class="copilot-sql-toggle" on:click={() => showSqlForMsg = { ...showSqlForMsg, ['trace_' + i]: !showSqlForMsg['trace_' + i] }}>
                                                <span class="material-symbols-outlined text-[12px]">manage_search</span>
                                                {showSqlForMsg['trace_' + i] ? 'Hide' : 'How I answered'} ({msg.tool_trace.length} tool{msg.tool_trace.length !== 1 ? 's' : ''})
                                            </button>
                                            {#if showSqlForMsg['trace_' + i]}
                                                <div class="copilot-sql-block" style="font-size: 11px; line-height: 1.6;">
                                                    {#each msg.tool_trace as t}
                                                        <div>→ <strong>{t.name}</strong>({Object.entries(t.args || {}).map(([k,v]) => `${k}=${JSON.stringify(v)}`).join(', ')}){t.duration_ms != null ? ` · ${t.duration_ms}ms` : ''}</div>
                                                    {/each}
                                                </div>
                                            {/if}
                                        {/if}

                                        {#if msg.content && msg.operation && msg.operation !== 'error' && msg.operation !== 'streaming' && !msg.needs_confirmation}
                                            <button
                                                class="copilot-sql-toggle"
                                                on:click={() => saveInsight(i)}
                                                disabled={msg.saving || msg.saved}
                                                title="Extract a takeaway from this turn and add to your persistent memory"
                                            >
                                                <span class="material-symbols-outlined text-[12px]">
                                                    {msg.saved ? 'check' : 'bookmark_add'}
                                                </span>
                                                {msg.saved ? 'Saved' : msg.saving ? 'Saving…' : 'Save to memory'}
                                            </button>
                                        {/if}

                                        {#if msg.memory_proposals && msg.memory_proposals.length > 0}
                                            <div class="copilot-memory-proposals">
                                                {#each msg.memory_proposals as prop (prop.id)}
                                                    <div class="copilot-memory-proposal">
                                                        <div class="copilot-memory-proposal-head">
                                                            <span class="material-symbols-outlined text-[14px]">lightbulb</span>
                                                            <span>I'd like to remember this in <strong>{prop.section.replace('_', ' ')}</strong>:</span>
                                                        </div>
                                                        <div class="copilot-memory-proposal-body">{prop.body}</div>
                                                        {#if prop.evidence}
                                                            <div class="copilot-memory-proposal-evidence">↳ {prop.evidence}</div>
                                                        {/if}
                                                        <div class="copilot-memory-proposal-actions">
                                                            <button class="copilot-sql-toggle" on:click={() => acceptMemoryProposal(i, prop.id)}>
                                                                <span class="material-symbols-outlined text-[12px]">check</span>Add
                                                            </button>
                                                            <button class="copilot-sql-toggle" on:click={() => rejectMemoryProposal(i, prop.id)}>
                                                                <span class="material-symbols-outlined text-[12px]">close</span>Skip
                                                            </button>
                                                        </div>
                                                    </div>
                                                {/each}
                                            </div>
                                        {/if}
                                    </div>
                                </div>
                            {:else}
                                <div class="px-4 py-2.5 rounded-2xl rounded-br-md text-[13px] copilot-user-bubble">
                                    {msg.content}
                                </div>
                            {/if}
                        </div>
                    </div>
                {/each}

                {#if loading}
                    <div class="flex items-start gap-2.5 fade-in">
                        <div class="w-6 h-6 rounded-lg flex items-center justify-center flex-shrink-0" style="background: var(--accent-soft)">
                            <span class="material-symbols-outlined text-[14px] animate-spin" style="color: var(--accent)">progress_activity</span>
                        </div>
                        <div class="card" style="padding: 0.75rem 1rem">
                            <div class="flex items-center gap-1.5">
                                <div class="w-1.5 h-1.5 rounded-full animate-bounce" style="background: var(--accent); opacity: 0.4; animation-delay: 0ms"></div>
                                <div class="w-1.5 h-1.5 rounded-full animate-bounce" style="background: var(--accent); opacity: 0.4; animation-delay: 150ms"></div>
                                <div class="w-1.5 h-1.5 rounded-full animate-bounce" style="background: var(--accent); opacity: 0.4; animation-delay: 300ms"></div>
                            </div>
                        </div>
                    </div>
                {/if}
            </div>

            {#if messages.length <= 1}
                <div class="flex-shrink-0 mb-4 fade-in-up" style="animation-delay: 100ms">
                    {#if activeChip}
                        {@const chip = chipActions.find(c => c.id === activeChip)}
                        <div class="card p-4 fade-in-up">
                            <div class="flex items-center justify-between mb-3">
                                <p class="text-[11px] font-semibold" style="color: var(--text-primary)">{chip.label}</p>
                                <button on:click={() => { activeChip = null; chipFormValues = {}; }}
                                    class="text-[11px] hover:underline" style="color: var(--text-muted)">Cancel</button>
                            </div>
                            <div class="flex flex-col gap-2.5">
                                {#each chip.inputs as field}
                                    <div>
                                        <label class="text-[10px] font-medium mb-1 block" style="color: var(--text-muted)">{field.label}</label>
                                        {#if field.type === 'select'}
                                            <select bind:value={chipFormValues[field.key]}
                                                class="w-full px-3 py-2 rounded-lg text-[12px] focus:ring-2 focus:ring-accent/40 outline-none"
                                                style="background: var(--card-bg); color: var(--text-primary); border: 1px solid var(--card-border)">
                                                <option value="">Select category…</option>
                                                {#each categories as cat}
                                                    <option value={cat.name ?? cat}>{cat.name ?? cat}</option>
                                                {/each}
                                            </select>
                                        {:else}
                                            <input type="text" bind:value={chipFormValues[field.key]}
                                                placeholder={field.placeholder}
                                                on:keydown={(e) => { if (e.key === 'Enter' && chip.inputs.filter(f => f.required).every(f => chipFormValues[f.key]?.trim())) submitChipForm(); }}
                                                class="w-full px-3 py-2 rounded-lg text-[12px] focus:ring-2 focus:ring-accent/40 outline-none"
                                                style="background: var(--card-bg); color: var(--text-primary); border: 1px solid var(--card-border)" />
                                        {/if}
                                    </div>
                                {/each}
                                <button on:click={submitChipForm}
                                    disabled={!chip.inputs.filter(f => f.required).every(f => chipFormValues[f.key]?.trim())}
                                    class="mt-1 px-4 py-2 rounded-lg text-[12px] font-semibold transition-opacity disabled:opacity-30 disabled:cursor-not-allowed"
                                    style="background: var(--accent); color: white">
                                    Run
                                </button>
                            </div>
                        </div>
                    {:else}
                        <p class="text-[9px] font-bold tracking-[0.2em] uppercase mb-2.5" style="color: var(--text-muted)">Try asking</p>
                        <div class="flex flex-wrap gap-2">
                            {#each chipActions as chip}
                                <button on:click={() => activateChip(chip)} class="copilot-suggestion-btn">
                                    {chip.label}
                                </button>
                            {/each}
                        </div>
                    {/if}
                </div>
            {/if}

            <div class="flex-shrink-0">
                <div class="flex items-end gap-2.5">
                    <div class="flex-1 relative">
                        <textarea bind:value={input} on:keydown={handleKeydown}
                            placeholder="Ask Copilot to explain or change something in your app data…"
                            rows="1"
                            class="w-full px-4 py-3 rounded-2xl text-[13px] resize-none focus:ring-2 focus:ring-accent/50 transition-all copilot-composer-textarea"
                            style="background: var(--card-bg); color: var(--text-primary); border: 1px solid var(--card-border); min-height: 48px; max-height: 120px; box-shadow: var(--card-shadow)"></textarea>
                        {#if localLlmStatus?.provider === 'ollama'}
                            <div class="copilot-model-inline">
                                <span class="copilot-mini-badge">Model</span>
                                <select
                                    class="copilot-model-select"
                                    bind:value={copilotModel}
                                    on:change={(event) => updateCopilotModelSelection(event.currentTarget.value)}
                                    disabled={copilotModelSaving}
                                >
                                    {#each localLlmCatalog?.tiers || [] as tier}
                                        <optgroup label={tier.label}>
                                            {#each tier.models.filter((model) => model.task_fit?.includes('copilot')) as model}
                                                <option value={model.id} disabled={model.expert_only && !localLlmStatus?.expertMode}>
                                                    {model.label} · {model.approx_size_gb} GB{model.installed ? ' · installed' : ''}
                                                </option>
                                            {/each}
                                        </optgroup>
                                    {/each}
                                </select>
                                <span class="copilot-model-meta">
                                    {selectedCopilotModelMeta?.installed ? 'Installed' : 'Not installed'}
                                </span>
                                {#if selectedCopilotModelMeta && !selectedCopilotModelMeta.installed && localLlmStatus?.ollamaReachable}
                                    <button
                                        type="button"
                                        class="copilot-model-install"
                                        on:click={installCopilotModel}
                                        disabled={copilotModelInstalling}
                                    >
                                        {copilotModelInstalling ? 'Installing…' : 'Install'}
                                    </button>
                                {/if}
                            </div>
                        {/if}
                    </div>
                    {#if loading}
                        <button on:click={stopStream} class="w-11 h-11 rounded-2xl flex items-center justify-center transition-all hover:scale-105 active:scale-95 copilot-send-btn" title="Stop (Esc)">
                            <span class="material-symbols-outlined text-white text-[18px]">stop</span>
                        </button>
                    {:else}
                        <button on:click={send} disabled={!input.trim()} class="w-11 h-11 rounded-2xl flex items-center justify-center transition-all hover:scale-105 active:scale-95 disabled:opacity-30 disabled:cursor-not-allowed copilot-send-btn">
                            <span class="material-symbols-outlined text-white text-[18px]">arrow_upward</span>
                        </button>
                    {/if}
                </div>
                <p class="text-[9px] text-center mt-2" style="color: var(--text-muted)">
                    Copilot can explain decisions, prepare edits, and ask for confirmation before changes are executed.
                </p>
            </div>
        </section>
    </div>

    {#if historyOpen}
        <div class="copilot-history-overlay fade-in" role="presentation">
            <button
                type="button"
                class="copilot-history-backdrop"
                aria-label="Close history"
                on:click={closeHistory}></button>

            <aside class="copilot-history-drawer" role="dialog" aria-modal="true" aria-label="Recent Copilot Activity">
                <div class="copilot-history-drawer-header">
                    <div class="copilot-panel-header">
                        <div>
                            <h3>Recent Copilot Activity</h3>
                            <p>Reuse a recent prompt or inspect the generated SQL.</p>
                        </div>
                    </div>
                    <button type="button" class="copilot-history-close" on:click={closeHistory} aria-label="Close history">
                        <span class="material-symbols-outlined text-[18px]">close</span>
                    </button>
                </div>

                <div class="copilot-history-drawer-body">
                    {#if sidebarLoading}
                        <div class="copilot-empty-state">Loading recent activity…</div>
                    {:else if recentHistory.length === 0}
                        <div class="copilot-empty-state">No recent Copilot history yet.</div>
                    {:else}
                        <div class="copilot-history-list">
                            {#each recentHistory as item}
                                <div class="copilot-history-card">
                                    <div class="flex items-start justify-between gap-3">
                                        <div>
                                            <p class="copilot-row-title">{item.user_message}</p>
                                            <p class="copilot-row-subtitle">{item.operation_type} · {formatDateTime(item.created_at)}</p>
                                        </div>
                                        <button class="copilot-inline-btn" type="button" on:click={() => reuseHistoryPrompt(item)}>Reuse</button>
                                    </div>
                                    {#if item.generated_sql}
                                        <button class="copilot-sql-toggle" on:click={() => toggleHistorySql(item.id)}>
                                            <span class="material-symbols-outlined text-[12px]">code</span>
                                            {showSqlForHistory[item.id] ? 'Hide SQL' : 'Show SQL'}
                                        </button>
                                        {#if showSqlForHistory[item.id]}
                                            <pre class="copilot-sql-block">{item.generated_sql}</pre>
                                        {/if}
                                    {/if}
                                </div>
                            {/each}
                        </div>
                    {/if}
                </div>
            </aside>
        </div>
    {/if}
</div>
