<script>
    import '$lib/styles/copilot.css';
    import { api, invalidateCache } from '$lib/api.js';
    import { onMount, tick } from 'svelte';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import { formatCurrency, formatDate } from '$lib/utils.js';

    /**
     * Message types:
     * - role: 'user' | 'assistant'
     * - content: string (main text)
     * - operation: 'read' | 'write_preview' | 'write_executed' | 'error' | null
     * - data: array | null (table data for read results)
     * - sql: string | null (generated SQL, hidden by default)
     * - needs_confirmation: boolean
     * - rows_affected: number
     * - confirmed: boolean (after user confirms)
     * - original_question: string (for write confirmation)
     */
    let messages = [
        {
            role: 'assistant',
            content: "Hi! I'm your Folio Copilot. I can query your financial data and even make changes for you.\n\nTry asking questions like \"How much did I spend on groceries last month?\" or give commands like \"Recategorize all Uber transactions as Transportation\".",
            operation: null,
            data: null,
            sql: null,
            needs_confirmation: false,
            rows_affected: 0
        }
    ];

    let input = '';
    let loading = false;
    let chatContainer;

    // SQL visibility toggle per message
    let showSqlForMsg = {};

    const quickPrompts = [
        "What's my biggest expense category this month?",
        "How much did I spend on dining last quarter?",
        "Show me all transactions over $200 in March",
        "Recategorize all Uber transactions as Transportation",
        "Compare my grocery spending this month vs last month",
        "Which subscription costs the most?"
    ];

    async function send() {
        const question = input.trim();
        if (!question || loading) return;

        messages = [...messages, {
            role: 'user',
            content: question,
            operation: null,
            data: null,
            sql: null,
            needs_confirmation: false,
            rows_affected: 0
        }];
        input = '';
        loading = true;

        await tick();
        scrollToBottom();

        try {
            const profile = $activeProfile || 'household';
            const res = await api.askCopilot(question, profile);

            messages = [...messages, {
                role: 'assistant',
                content: res.answer || 'No response received.',
                operation: res.operation || null,
                data: res.data || null,
                sql: res.sql || null,
                // [FIX] Capture server-side confirmation_id for write previews
                confirmation_id: res.confirmation_id || null,
                needs_confirmation: res.needs_confirmation || false,
                rows_affected: res.rows_affected || 0,
                original_question: question
            }];
        } catch (e) {
            messages = [...messages, {
                role: 'assistant',
                content: "Sorry, I couldn't process that. Please try again.",
                operation: 'error',
                data: null,
                sql: null,
                needs_confirmation: false,
                rows_affected: 0
            }];
        } finally {
            loading = false;
            await tick();
            scrollToBottom();
        }
    }

    async function confirmWrite(msgIndex) {
        const msg = messages[msgIndex];
        // [FIX] Require confirmation_id (server-side stored SQL) instead of raw SQL
        if (!msg || !msg.confirmation_id) return;

        loading = true;
        await tick();

        try {
            const profile = $activeProfile || 'household';
            const res = await api.confirmCopilotWrite(msg.original_question, msg.confirmation_id, profile);

            // Update the message to show it's been executed
            messages[msgIndex] = {
                ...messages[msgIndex],
                needs_confirmation: false,
                confirmed: true
            };

            // Add the result message
            messages = [...messages, {
                role: 'assistant',
                content: res.answer || `Updated ${res.rows_affected} transaction(s).`,
                operation: 'write_executed',
                data: null,
                sql: null,
                needs_confirmation: false,
                rows_affected: res.rows_affected || 0
            }];

            // Invalidate cache since data changed
            invalidateCache();
        } catch (e) {
            messages = [...messages, {
                role: 'assistant',
                content: "Failed to execute the operation. Please try again.",
                operation: 'error',
                data: null,
                sql: null,
                needs_confirmation: false,
                rows_affected: 0
            }];
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
        messages = [...messages, {
            role: 'assistant',
            content: "Operation cancelled.",
            operation: null,
            data: null,
            sql: null,
            needs_confirmation: false,
            rows_affected: 0
        }];
    }

    function toggleSql(msgIndex) {
        showSqlForMsg[msgIndex] = !showSqlForMsg[msgIndex];
        showSqlForMsg = showSqlForMsg;
    }

    function usePrompt(prompt) {
        input = prompt;
        send();
    }

    function scrollToBottom() {
        if (chatContainer) chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    function handleKeydown(e) {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            send();
        }
    }

    /**
     * Format a value for display in the data table.
     * Detects currency amounts, dates, and other types.
     */
    function formatTableValue(key, value) {
        if (value === null || value === undefined) return '—';
        if (typeof value === 'number') {
            // Heuristic: if the key contains 'amount', 'total', 'balance', 'sum', 'avg', 'spent', format as currency
            const currencyKeys = ['amount', 'total', 'balance', 'sum', 'avg', 'spent', 'income', 'expense', 'net', 'owed', 'assets'];
            if (currencyKeys.some(k => key.toLowerCase().includes(k))) {
                return formatCurrency(value, 2);
            }
            // Round other numbers
            return Number.isInteger(value) ? value.toString() : value.toFixed(2);
        }
        // Date detection
        if (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}/.test(value)) {
            return formatDate(value);
        }
        return String(value);
    }

    /**
     * Get column headers from data array.
     */
    function getColumns(data) {
        if (!data || data.length === 0) return [];
        return Object.keys(data[0]);
    }
</script>

<div class="flex flex-col h-[calc(100vh-7rem)]">
    <!-- Header -->
    <div class="mb-5 flex-shrink-0 fade-in">
        <div class="flex items-center gap-3">
            <div class="w-9 h-9 rounded-xl flex items-center justify-center"
                style="background: linear-gradient(135deg, var(--accent), #7c5cbf);
                       box-shadow: 0 4px 16px rgba(74, 144, 217, 0.3)">
                <span class="material-symbols-outlined text-white text-[18px]">auto_awesome</span>
            </div>
            <div>
                <h2 class="text-xl font-extrabold font-display" style="color: var(--text-primary)">Copilot</h2>
                <p class="text-[11px]" style="color: var(--text-muted)">AI-powered financial insights · Ask questions or give commands</p>
            </div>
        </div>
    </div>

    <!-- Chat area -->
    <div bind:this={chatContainer}
        class="flex-1 overflow-y-auto space-y-3.5 pr-2 mb-4" style="scrollbar-width: thin">
        {#each messages as msg, i}
            <div class="flex {msg.role === 'user' ? 'justify-end' : 'justify-start'} fade-in" style="animation-delay: {Math.min(i * 60, 300)}ms">
                <div class="max-w-[85%] {msg.role === 'user' ? 'order-2' : ''}">
                    {#if msg.role === 'assistant'}
                        <div class="flex items-start gap-2.5">
                            <div class="w-6 h-6 rounded-lg flex items-center justify-center flex-shrink-0 mt-0.5"
                                style="background: var(--accent-soft)">
                                <span class="material-symbols-outlined text-[14px]" style="color: var(--accent)">auto_awesome</span>
                            </div>
                            <div class="copilot-msg-container">
                                <!-- Operation badge -->
                                {#if msg.operation === 'write_preview'}
                                    <div class="copilot-op-badge copilot-op-write">
                                        <span class="material-symbols-outlined text-[12px]">edit</span>
                                        Write Operation · {msg.rows_affected} row{msg.rows_affected !== 1 ? 's' : ''}
                                    </div>
                                {:else if msg.operation === 'write_executed'}
                                    <div class="copilot-op-badge copilot-op-success">
                                        <span class="material-symbols-outlined text-[12px]">check_circle</span>
                                        Executed · {msg.rows_affected} row{msg.rows_affected !== 1 ? 's' : ''} affected
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

                                <!-- Main content -->
                                <div class="card" style="padding: 0.75rem 1rem">
                                    <p class="text-[13px] leading-relaxed whitespace-pre-wrap" style="color: var(--text-primary)">{msg.content}</p>
                                </div>

                                <!-- Data table for read results -->
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
                                        {#if msg.data.length > 20}
                                            <p class="text-[10px] text-center py-2" style="color: var(--text-muted)">
                                                Showing 20 of {msg.data.length} results
                                            </p>
                                        {/if}
                                    </div>
                                {/if}

                                <!-- Write confirmation card -->
                                {#if msg.needs_confirmation && !msg.confirmed}
                                    <div class="copilot-confirm-card">
                                        <p class="text-[11px] font-medium mb-3" style="color: var(--text-secondary)">
                                            This will modify your data. Please review and confirm.
                                        </p>

                                        <!-- Preview data if available -->
                                        {#if msg.data && msg.data.length > 0}
                                            {@const previewCols = getColumns(msg.data)}
                                            <div class="copilot-data-table-wrap" style="margin-bottom: 0.75rem">
                                                <table class="copilot-data-table copilot-data-table-preview">
                                                    <thead>
                                                        <tr>
                                                            {#each previewCols as col}
                                                                <th>{col.replace(/_/g, ' ')}</th>
                                                            {/each}
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        {#each msg.data.slice(0, 5) as row}
                                                            <tr>
                                                                {#each previewCols as col}
                                                                    <td>{formatTableValue(col, row[col])}</td>
                                                                {/each}
                                                            </tr>
                                                        {/each}
                                                    </tbody>
                                                </table>
                                                {#if msg.data.length > 5}
                                                    <p class="text-[10px] text-center py-1" style="color: var(--text-muted)">
                                                        +{msg.data.length - 5} more...
                                                    </p>
                                                {/if}
                                            </div>
                                        {/if}

                                        <div class="flex items-center gap-2">
                                            <button
                                                class="copilot-confirm-btn copilot-confirm-yes"
                                                on:click={() => confirmWrite(i)}
                                                disabled={loading}>
                                                <span class="material-symbols-outlined text-[14px]">check</span>
                                                Confirm
                                            </button>
                                            <button
                                                class="copilot-confirm-btn copilot-confirm-no"
                                                on:click={() => cancelWrite(i)}
                                                disabled={loading}>
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

                                <!-- SQL toggle -->
                                {#if msg.sql}
                                    <button class="copilot-sql-toggle" on:click={() => toggleSql(i)}>
                                        <span class="material-symbols-outlined text-[12px]">code</span>
                                        {showSqlForMsg[i] ? 'Hide SQL' : 'Show SQL'}
                                    </button>
                                    {#if showSqlForMsg[i]}
                                        <pre class="copilot-sql-block">{msg.sql}</pre>
                                    {/if}
                                {/if}
                            </div>
                        </div>
                    {:else}
                        <div class="px-4 py-2.5 rounded-2xl rounded-br-md text-[13px]"
                            style="background: linear-gradient(135deg, var(--accent), var(--accent-hover)); color: white;
                                   box-shadow: 0 2px 12px rgba(74, 144, 217, 0.25)">
                            {msg.content}
                        </div>
                    {/if}
                </div>
            </div>
        {/each}

        {#if loading}
            <div class="flex items-start gap-2.5 fade-in">
                <div class="w-6 h-6 rounded-lg flex items-center justify-center flex-shrink-0"
                    style="background: var(--accent-soft)">
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

    <!-- Quick prompts -->
    {#if messages.length <= 1}
        <div class="flex-shrink-0 mb-4 fade-in-up" style="animation-delay: 100ms">
            <p class="text-[9px] font-bold tracking-[0.2em] uppercase mb-2.5" style="color: var(--text-muted)">Try asking</p>
            <div class="flex flex-wrap gap-2">
                {#each quickPrompts as prompt}
                    <button on:click={() => usePrompt(prompt)}
                        class="px-3 py-2 rounded-xl text-[11px] font-medium transition-all hover:scale-[1.02] active:scale-[0.98]"
                        style="background: var(--card-bg); color: var(--text-secondary); border: 1px solid var(--card-border);
                               box-shadow: var(--card-shadow)">
                        {prompt}
                    </button>
                {/each}
            </div>
        </div>
    {/if}

    <!-- Input -->
    <div class="flex-shrink-0">
        <div class="flex items-end gap-2.5">
            <div class="flex-1 relative">
                <textarea bind:value={input} on:keydown={handleKeydown}
                    placeholder="Ask about your finances or give a command..."
                    rows="1"
                    class="w-full px-4 py-3 rounded-2xl text-[13px] resize-none focus:ring-2 focus:ring-accent/50 transition-all"
                    style="background: var(--card-bg); color: var(--text-primary); border: 1px solid var(--card-border);
                           min-height: 48px; max-height: 120px; box-shadow: var(--card-shadow)"
                ></textarea>
            </div>
            <button on:click={send} disabled={!input.trim() || loading}
                class="w-11 h-11 rounded-2xl flex items-center justify-center transition-all
                       hover:scale-105 active:scale-95 disabled:opacity-30 disabled:cursor-not-allowed"
                style="background: linear-gradient(135deg, var(--accent), var(--accent-hover));
                       box-shadow: 0 2px 12px rgba(74, 144, 217, 0.25)">
                <span class="material-symbols-outlined text-white text-[18px]">
                    {loading ? 'progress_activity' : 'arrow_upward'}
                </span>
            </button>
        </div>
        <p class="text-[9px] text-center mt-2" style="color: var(--text-muted)">
            Copilot can read your data and make changes with your confirmation. Responses are AI-generated.
        </p>
    </div>
</div>

<style>
    .animate-spin { animation: spin 1s linear infinite; }
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    .animate-bounce { animation: bounce 1.4s infinite; }
    @keyframes bounce {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-6px); }
    }
</style>