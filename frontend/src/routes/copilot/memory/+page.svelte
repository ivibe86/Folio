<script>
    import '$lib/styles/copilot.css';
    import { onMount } from 'svelte';
    import { api } from '$lib/api.js';
    import { activeProfile } from '$lib/stores/profileStore.js';
    import ProfileSwitcher from '$lib/components/ProfileSwitcher.svelte';

    let entries = [];
    let sections = [];
    let proposals = [];
    let markdown = '';
    let tokenEstimate = 0;
    let charCount = 0;
    let budget = 4000;
    let loading = true;
    let actionNotice = '';
    let activeProfileId;
    activeProfile.subscribe((p) => { activeProfileId = p; });

    let editingId = null;
    let editBuffer = '';
    let consolidating = false;

    let newSection = 'preferences';
    let newBody = '';
    let newEvidence = '';

    async function refresh() {
        loading = true;
        try {
            const [entriesResp, mdResp, propsResp] = await Promise.all([
                api.getMemoryEntries(activeProfileId),
                api.getMemoryMarkdown(activeProfileId),
                api.getMemoryProposals(activeProfileId),
            ]);
            entries = entriesResp.items || [];
            sections = entriesResp.sections || [];
            markdown = mdResp.markdown || '';
            tokenEstimate = mdResp.token_estimate || 0;
            charCount = mdResp.char_count || 0;
            budget = mdResp.budget || 4000;
            proposals = propsResp.items || [];
        } catch (e) {
            actionNotice = e?.message || 'Failed to load memory.';
        } finally {
            loading = false;
        }
    }

    function setNotice(msg) {
        actionNotice = msg;
        setTimeout(() => { if (actionNotice === msg) actionNotice = ''; }, 3500);
    }

    async function startEdit(entry) {
        editingId = entry.id;
        editBuffer = entry.body;
    }

    async function commitEdit() {
        if (!editingId || !editBuffer.trim()) return;
        try {
            await api.updateMemoryEntry(editingId, editBuffer.trim(), null, activeProfileId);
            editingId = null;
            editBuffer = '';
            await refresh();
        } catch (e) {
            setNotice(e?.message || 'Failed to update entry.');
        }
    }

    function cancelEdit() {
        editingId = null;
        editBuffer = '';
    }

    async function deleteEntry(id) {
        if (!confirm('Remove this entry from memory?')) return;
        try {
            await api.deleteMemoryEntry(id, activeProfileId);
            await refresh();
        } catch (e) {
            setNotice(e?.message || 'Failed to delete entry.');
        }
    }

    async function addEntry() {
        if (!newBody.trim()) return;
        try {
            await api.createMemoryEntry({
                section: newSection,
                body: newBody.trim(),
                confidence: 'stated',
                evidence: newEvidence.trim(),
            }, activeProfileId);
            newBody = '';
            newEvidence = '';
            await refresh();
        } catch (e) {
            setNotice(e?.message || 'Failed to add entry.');
        }
    }

    async function acceptProposal(id) {
        try {
            await api.acceptMemoryProposal(id, null, activeProfileId);
            await refresh();
            setNotice('Added to memory.');
        } catch (e) {
            setNotice(e?.message || 'Failed to accept proposal.');
        }
    }

    async function rejectProposal(id) {
        try {
            await api.rejectMemoryProposal(id, activeProfileId);
            await refresh();
        } catch (e) {
            setNotice(e?.message || 'Failed to reject proposal.');
        }
    }

    async function reviewMyMemory() {
        consolidating = true;
        try {
            const result = await api.consolidateMemory(activeProfileId);
            await refresh();
            const created = result?.proposals_created || 0;
            setNotice(created
                ? `Found ${created} consolidation${created !== 1 ? 's' : ''} to review.`
                : 'Memory looks clean — nothing to consolidate.');
        } catch (e) {
            setNotice(e?.message || 'Consolidation failed.');
        } finally {
            consolidating = false;
        }
    }

    function entriesForSection(key) {
        return entries.filter((e) => e.section === key);
    }

    function sectionLabel(key) {
        const found = sections.find((s) => s.key === key);
        return found ? found.label : key;
    }

    onMount(refresh);

    $: budgetPct = Math.min(100, Math.round((tokenEstimate / budget) * 100));
    $: budgetClass = budgetPct > 90 ? 'memory-budget-bar--over' : (budgetPct > 70 ? 'memory-budget-bar--warn' : '');
</script>

<div class="folio-page-shell">
    <div class="folio-page-header" style="display:flex;align-items:center;justify-content:space-between;gap:1rem">
        <div>
            <h2 class="folio-page-title" style="font-size: clamp(1.25rem, 0.8vw + 1rem, 1.7rem)">Memory</h2>
            <p class="folio-page-subtitle">What Copilot has come to understand about you. You can read and correct any of it.</p>
        </div>
        <div class="copilot-header-actions">
            <a href="/copilot" class="copilot-side-pill">Back to Copilot</a>
            <button class="copilot-side-pill" disabled={consolidating} on:click={reviewMyMemory}>
                {consolidating ? 'Reviewing…' : 'Review my memory'}
            </button>
            <ProfileSwitcher />
        </div>
    </div>

    {#if actionNotice}
        <div class="copilot-notice fade-in">{actionNotice}</div>
    {/if}

    <div class="memory-budget-row">
        <div class="memory-budget-label">
            File size: {charCount.toLocaleString()} chars · ~{tokenEstimate.toLocaleString()} tokens / {budget.toLocaleString()} budget
        </div>
        <div class="memory-budget-track">
            <div class="memory-budget-bar {budgetClass}" style="width: {budgetPct}%"></div>
        </div>
    </div>

    {#if proposals.length > 0}
        <section class="memory-card">
            <h3 class="memory-section-title">Pending proposals ({proposals.length})</h3>
            {#each proposals as prop (prop.id)}
                <div class="memory-proposal">
                    <div class="memory-proposal-meta">
                        <span class="memory-tag">{prop.section.replace('_', ' ')}</span>
                        <span class="memory-tag memory-tag--{prop.confidence}">{prop.confidence}</span>
                        <span class="memory-source">via {prop.source.replace('_', ' ')}</span>
                    </div>
                    <div class="memory-proposal-body">{prop.body}</div>
                    {#if prop.evidence}
                        <div class="memory-proposal-evidence">↳ {prop.evidence}</div>
                    {/if}
                    <div class="memory-proposal-actions">
                        <button class="copilot-sql-toggle" on:click={() => acceptProposal(prop.id)}>
                            <span class="material-symbols-outlined text-[12px]">check</span>Add
                        </button>
                        <button class="copilot-sql-toggle" on:click={() => rejectProposal(prop.id)}>
                            <span class="material-symbols-outlined text-[12px]">close</span>Skip
                        </button>
                    </div>
                </div>
            {/each}
        </section>
    {/if}

    <section class="memory-card">
        <h3 class="memory-section-title">Add an entry</h3>
        <div class="memory-add-row">
            <select bind:value={newSection} class="memory-select">
                {#each sections as s}
                    <option value={s.key}>{s.label}</option>
                {/each}
            </select>
            <input bind:value={newBody} placeholder="Short statement about you (e.g. Trying to cut alcohol to under $200/mo)" class="memory-input" />
            <input bind:value={newEvidence} placeholder="Optional evidence/quote" class="memory-input" />
            <button class="copilot-side-pill" on:click={addEntry} disabled={!newBody.trim()}>Add</button>
        </div>
    </section>

    {#if loading}
        <div style="opacity:0.6;padding:1rem">Loading memory…</div>
    {:else if entries.length === 0}
        <section class="memory-card">
            <p style="color: var(--text-secondary)">Your memory is empty. Use Copilot for a few turns and stated preferences / commitments will start to accumulate, or add an entry above.</p>
        </section>
    {:else}
        {#each sections as s (s.key)}
            {@const sectionEntries = entriesForSection(s.key)}
            {#if sectionEntries.length > 0}
                <section class="memory-card">
                    <h3 class="memory-section-title">{s.label}</h3>
                    {#each sectionEntries as entry (entry.id)}
                        <div class="memory-entry">
                            {#if editingId === entry.id}
                                <textarea bind:value={editBuffer} class="memory-input" rows="2" />
                                <div class="memory-entry-actions">
                                    <button class="copilot-sql-toggle" on:click={commitEdit}>Save</button>
                                    <button class="copilot-sql-toggle" on:click={cancelEdit}>Cancel</button>
                                </div>
                            {:else}
                                <div class="memory-entry-body">{entry.body}</div>
                                {#if entry.evidence || entry.confidence !== 'stated'}
                                    <div class="memory-entry-meta">
                                        {#if entry.confidence !== 'stated'}<span class="memory-tag memory-tag--{entry.confidence}">{entry.confidence}</span>{/if}
                                        {#if entry.evidence}<span class="memory-evidence">{entry.evidence}</span>{/if}
                                    </div>
                                {/if}
                                <div class="memory-entry-actions">
                                    <button class="copilot-sql-toggle" on:click={() => startEdit(entry)}>
                                        <span class="material-symbols-outlined text-[12px]">edit</span>Edit
                                    </button>
                                    <button class="copilot-sql-toggle" on:click={() => deleteEntry(entry.id)}>
                                        <span class="material-symbols-outlined text-[12px]">delete</span>Delete
                                    </button>
                                </div>
                            {/if}
                        </div>
                    {/each}
                </section>
            {/if}
        {/each}
    {/if}

    {#if markdown}
        <section class="memory-card">
            <details>
                <summary class="memory-section-title" style="cursor:pointer">Raw markdown view (about_user.md)</summary>
                <pre class="memory-markdown">{markdown}</pre>
            </details>
        </section>
    {/if}
</div>

<style>
    .memory-card {
        border: 1px solid var(--card-border);
        border-radius: 14px;
        padding: 1rem 1.25rem;
        margin-top: 1rem;
        background: var(--card-bg, transparent);
    }
    .memory-section-title {
        font-size: 13px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--text-secondary);
        margin: 0 0 0.75rem 0;
    }
    .memory-budget-row { margin-top: 1rem; }
    .memory-budget-label { font-size: 12px; color: var(--text-secondary); margin-bottom: 4px; }
    .memory-budget-track {
        height: 6px;
        background: rgba(0,0,0,0.06);
        border-radius: 3px;
        overflow: hidden;
    }
    .memory-budget-bar {
        height: 100%;
        background: var(--accent, #5b8def);
        transition: width 240ms ease;
    }
    .memory-budget-bar--warn { background: #d29849; }
    .memory-budget-bar--over { background: #c1554b; }

    .memory-entry {
        padding: 8px 0;
        border-top: 1px solid var(--card-border);
    }
    .memory-entry:first-of-type { border-top: none; padding-top: 0; }
    .memory-entry-body { font-size: 13px; color: var(--text-primary); line-height: 1.4; }
    .memory-entry-meta { display: flex; gap: 8px; align-items: center; margin-top: 4px; flex-wrap: wrap; }
    .memory-entry-actions { display: flex; gap: 6px; margin-top: 6px; }

    .memory-tag {
        display: inline-block;
        font-size: 10px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 2px 6px;
        border-radius: 4px;
        background: rgba(0,0,0,0.06);
        color: var(--text-secondary);
    }
    .memory-tag--inferred { background: rgba(210, 152, 73, 0.15); color: #b07a2c; }
    .memory-tag--saved { background: rgba(91, 141, 239, 0.15); color: #3b6bc4; }
    .memory-evidence { font-size: 11px; color: var(--text-secondary); font-style: italic; }

    .memory-add-row { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
    .memory-select, .memory-input {
        padding: 6px 8px;
        border: 1px solid var(--card-border);
        border-radius: 6px;
        background: transparent;
        color: var(--text-primary);
        font-size: 13px;
    }
    .memory-input { flex: 1; min-width: 200px; }

    .memory-proposal {
        padding: 10px 0;
        border-top: 1px solid var(--card-border);
    }
    .memory-proposal:first-of-type { border-top: none; padding-top: 0; }
    .memory-proposal-meta { display: flex; gap: 8px; align-items: center; margin-bottom: 4px; }
    .memory-proposal-body { font-size: 13px; color: var(--text-primary); line-height: 1.4; }
    .memory-proposal-evidence { font-size: 11px; color: var(--text-secondary); font-style: italic; margin-top: 2px; }
    .memory-proposal-actions { display: flex; gap: 6px; margin-top: 6px; }
    .memory-source { font-size: 11px; color: var(--text-secondary); }

    .memory-markdown {
        margin-top: 0.75rem;
        padding: 12px;
        background: rgba(0,0,0,0.03);
        border-radius: 8px;
        font-family: ui-monospace, monospace;
        font-size: 12px;
        white-space: pre-wrap;
        word-break: break-word;
    }
</style>
