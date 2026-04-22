<!--
  MigrationWizard.svelte
  4-step wizard for migrating from Teller to SimpleFIN.
  Call show() to open. Dispatches 'done' on successful migration.
-->
<script>
    import { createEventDispatcher } from 'svelte';
    import { api } from '$lib/api.js';

    const dispatch = createEventDispatcher();

    let open = false;
    let step = 1;           // 1=overview 2=mapping 3=preview 4=result
    let loading = false;
    let executing = false;
    let error = '';

    let preview = null;     // analyze_migration response
    let userMappings = [];  // [{teller_account_id, sf_account_id (null=skip)}]
    let deactivateTeller = false;
    let result = null;      // execute_migration response

    export function show() {
        open = true;
        step = 1;
        error = '';
        preview = null;
        userMappings = [];
        result = null;
        deactivateTeller = false;
        loadPreview();
    }

    function close() {
        open = false;
    }

    function handleBackdropClick(e) {
        if (e.target === e.currentTarget && !executing) close();
    }

    function handleKeydown(e) {
        if (e.key === 'Escape' && !executing) close();
    }

    async function loadPreview() {
        loading = true;
        error = '';
        try {
            preview = await api.getMigrationPreview();
            // Seed userMappings from suggestions
            userMappings = (preview.teller_accounts || []).map(tel => {
                const suggested = (preview.suggested_mappings || []).find(
                    m => m.teller_account_id === tel.id
                );
                return {
                    teller_account_id: tel.id,
                    sf_account_id: suggested ? suggested.sf_account_id : null,
                };
            });
        } catch (e) {
            error = 'Failed to load migration preview: ' + (e.message || 'Unknown error');
        } finally {
            loading = false;
        }
    }

    function getConfidence(telId) {
        if (!preview) return 'none';
        const m = (preview.suggested_mappings || []).find(s => s.teller_account_id === telId);
        return m ? m.confidence : 'none';
    }

    function getEstimate(telId) {
        if (!preview) return null;
        return (preview.estimates?.per_mapping || []).find(e => e.teller_account_id === telId) || null;
    }

    function getMappedSfName(sfId) {
        if (!sfId || !preview) return '(skip)';
        const sf = (preview.simplefin_accounts || []).find(a => a.id === sfId);
        return sf ? sf.account_name : sfId;
    }

    function updateMapping(telId, sfId) {
        userMappings = userMappings.map(m =>
            m.teller_account_id === telId
                ? { ...m, sf_account_id: sfId === '__skip__' ? null : sfId }
                : m
        );
    }

    // For step 3, recompute estimates based on user mappings
    $: confirmedMappings = userMappings.filter(m => m.sf_account_id !== null);
    $: totalHistorical = confirmedMappings.reduce((s, m) => {
        const est = getEstimate(m.teller_account_id);
        return s + (est ? est.historical_keep : 0);
    }, 0);
    $: totalDedup = confirmedMappings.reduce((s, m) => {
        const est = getEstimate(m.teller_account_id);
        return s + (est ? est.overlap_dedup : 0);
    }, 0);
    $: totalTellerOnly = confirmedMappings.reduce((s, m) => {
        const est = getEstimate(m.teller_account_id);
        return s + (est ? est.overlap_teller_only : 0);
    }, 0);

    $: hasTeller   = preview && (preview.teller_accounts || []).length > 0;
    $: hasSF       = preview && (preview.simplefin_accounts || []).length > 0;
    $: canMigrate  = hasTeller && hasSF;

    async function handleExecute() {
        executing = true;
        error = '';
        try {
            result = await api.executeMigration(userMappings, deactivateTeller);
            step = 4;
            dispatch('done', result);
        } catch (e) {
            error = e.message || 'Migration failed. No changes were made.';
        } finally {
            executing = false;
        }
    }

    const CONFIDENCE_LABEL = { high: 'High', medium: 'Medium', low: 'Low', none: 'No match' };
    const CONFIDENCE_CLASS = { high: 'conf-high', medium: 'conf-medium', low: 'conf-low', none: 'conf-none' };
</script>

<svelte:window on:keydown={handleKeydown} />

{#if open}
    <!-- svelte-ignore a11y-click-events-have-key-events a11y-no-static-element-interactions -->
    <div class="mw-backdrop" on:click={handleBackdropClick}>
        <div class="mw-modal" role="dialog" aria-modal="true" aria-label="Migrate to SimpleFIN">

            <!-- Header -->
            <div class="mw-header">
                <div class="mw-header-left">
                    <h3>Migrate to SimpleFIN</h3>
                    <div class="mw-steps">
                        {#each [1,2,3,4] as s}
                            <span class="mw-step-dot" class:active={step === s} class:done={step > s}></span>
                        {/each}
                    </div>
                </div>
                <button class="mw-close" on:click={close} disabled={executing} aria-label="Close">
                    <span class="material-symbols-outlined">close</span>
                </button>
            </div>

            <!-- Body -->
            <div class="mw-body">

                <!-- ── Step 1: Overview ── -->
                {#if step === 1}
                    {#if loading}
                        <div class="mw-loading">
                            <span class="material-symbols-outlined mw-spinner">progress_activity</span>
                            Loading account data…
                        </div>
                    {:else if error}
                        <div class="mw-notice mw-error">{error}</div>
                    {:else if preview}
                        <p class="mw-intro">
                            This wizard will deduplicate transactions in the overlap period,
                            preserve your full Teller history, and optionally disable all
                            Teller connections so SimpleFIN takes over going forward.
                        </p>

                        <div class="mw-stat-grid">
                            <div class="mw-stat">
                                <span class="mw-stat-value">{preview.teller_accounts?.length ?? 0}</span>
                                <span class="mw-stat-label">Teller accounts</span>
                            </div>
                            <div class="mw-stat">
                                <span class="mw-stat-value">{preview.simplefin_accounts?.length ?? 0}</span>
                                <span class="mw-stat-label">SimpleFIN accounts</span>
                            </div>
                            <div class="mw-stat">
                                <span class="mw-stat-value">{preview.simplefin_window_start ?? '—'}</span>
                                <span class="mw-stat-label">SimpleFIN data starts</span>
                            </div>
                        </div>

                        {#if !hasTeller}
                            <div class="mw-notice mw-warn">No active Teller accounts found. Nothing to migrate.</div>
                        {:else if !hasSF}
                            <div class="mw-notice mw-warn">No SimpleFIN accounts found. Connect SimpleFIN first.</div>
                        {/if}
                    {/if}
                {/if}

                <!-- ── Step 2: Account Mapping ── -->
                {#if step === 2}
                    <p class="mw-intro">Map each Teller account to its SimpleFIN counterpart. Choose "(skip)" to leave a Teller account unchanged.</p>

                    <div class="mw-map-table">
                        <div class="mw-map-header">
                            <span>Teller account</span>
                            <span>→</span>
                            <span>SimpleFIN account</span>
                            <span>Match</span>
                        </div>
                        {#each preview.teller_accounts as tel}
                            {@const conf = getConfidence(tel.id)}
                            {@const mapping = userMappings.find(m => m.teller_account_id === tel.id)}
                            <div class="mw-map-row">
                                <div class="mw-acct-info">
                                    <span class="mw-acct-name">{tel.account_name}</span>
                                    <span class="mw-acct-meta">{tel.institution_name || ''} · {tel.account_type}</span>
                                </div>
                                <span class="mw-arrow">→</span>
                                <select
                                    class="mw-select"
                                    value={mapping?.sf_account_id ?? '__skip__'}
                                    on:change={e => updateMapping(tel.id, e.target.value)}
                                >
                                    <option value="__skip__">(skip — do not migrate)</option>
                                    {#each (preview.simplefin_accounts || []).filter(sf => sf.profile === tel.profile) as sf}
                                        <option value={sf.id}>{sf.account_name}</option>
                                    {/each}
                                </select>
                                <span class="mw-conf {CONFIDENCE_CLASS[conf]}">{CONFIDENCE_LABEL[conf]}</span>
                            </div>
                        {/each}
                    </div>
                {/if}

                <!-- ── Step 3: Preview & Confirm ── -->
                {#if step === 3}
                    <p class="mw-intro">Review what will happen, then confirm.</p>

                    {#if confirmedMappings.length === 0}
                        <div class="mw-notice mw-warn">All accounts are set to skip. Nothing will be migrated.</div>
                    {:else}
                        <div class="mw-preview-rows">
                            {#each confirmedMappings as m}
                                {@const tel = (preview.teller_accounts || []).find(a => a.id === m.teller_account_id)}
                                {@const est = getEstimate(m.teller_account_id)}
                                <div class="mw-preview-row">
                                    <div class="mw-preview-acct">
                                        <span class="mw-preview-tel">{tel?.account_name}</span>
                                        <span class="mw-preview-arrow">→</span>
                                        <span class="mw-preview-sf">{getMappedSfName(m.sf_account_id)}</span>
                                    </div>
                                    {#if est}
                                        <ul class="mw-preview-stats">
                                            <li><strong>{est.historical_keep}</strong> pre-SimpleFIN transactions kept</li>
                                            <li><strong>{est.overlap_dedup}</strong> overlap transactions deduplicated</li>
                                            {#if est.overlap_teller_only > 0}
                                                <li><strong>{est.overlap_teller_only}</strong> Teller-only transactions preserved</li>
                                            {/if}
                                        </ul>
                                    {/if}
                                </div>
                            {/each}
                        </div>

                        <div class="mw-total-bar">
                            <span>{totalHistorical} historical kept · {totalDedup} deduped · {totalTellerOnly} Teller-only preserved</span>
                        </div>
                    {/if}

                    <label class="mw-checkbox-row">
                        <input type="checkbox" bind:checked={deactivateTeller} />
                        <span>Disable all Teller connections after migration ({preview.estimates?.total_teller_enrollments ?? 0} enrollment{preview.estimates?.total_teller_enrollments !== 1 ? 's' : ''})</span>
                    </label>
                    <div class="mw-notice mw-warn">
                        This turns off Teller for all active Teller profiles in Folio. Only use it after all household Teller accounts have been added and any desired migrations are complete.
                    </div>
                    {#if !deactivateTeller}
                        <div class="mw-notice mw-warn">
                            Teller will keep syncing. On the next Teller sync, previously deduplicated transactions may reappear. Disable Teller as soon as you finish migrating all accounts.
                        </div>
                    {/if}

                    {#if error}
                        <div class="mw-notice mw-error">{error}</div>
                    {/if}
                {/if}

                <!-- ── Step 4: Result ── -->
                {#if step === 4 && result}
                    <div class="mw-result">
                        <span class="material-symbols-outlined mw-result-icon">check_circle</span>
                        <p class="mw-result-title">Migration complete</p>
                        <ul class="mw-result-stats">
                            <li>{result.historical_kept} historical Teller transactions kept</li>
                            <li>{result.overlap_deduped} duplicate transactions removed</li>
                            {#if result.overlap_teller_only > 0}
                                <li>{result.overlap_teller_only} unique Teller transactions preserved</li>
                            {/if}
                            {#if result.teller_tokens_deactivated > 0}
                                <li>{result.teller_tokens_deactivated} Teller enrollment{result.teller_tokens_deactivated !== 1 ? 's' : ''} disabled</li>
                            {/if}
                        </ul>
                    </div>
                {/if}

            </div>

            <!-- Footer -->
            <div class="mw-footer">
                {#if step === 1}
                    <button class="mw-btn mw-btn-secondary" on:click={close}>Cancel</button>
                    <button class="mw-btn mw-btn-primary" disabled={!canMigrate || loading} on:click={() => step = 2}>
                        Next
                    </button>
                {:else if step === 2}
                    <button class="mw-btn mw-btn-secondary" on:click={() => step = 1}>Back</button>
                    <button class="mw-btn mw-btn-primary" on:click={() => step = 3}>
                        Review
                    </button>
                {:else if step === 3}
                    <button class="mw-btn mw-btn-secondary" on:click={() => { step = 2; error = ''; }} disabled={executing}>Back</button>
                    <button
                        class="mw-btn mw-btn-danger"
                        disabled={executing || confirmedMappings.length === 0}
                        on:click={handleExecute}
                    >
                        {#if executing}
                            <span class="material-symbols-outlined mw-spinner-sm">progress_activity</span>
                            Migrating…
                        {:else}
                            Confirm &amp; Migrate
                        {/if}
                    </button>
                {:else if step === 4}
                    <button class="mw-btn mw-btn-primary" on:click={close}>Done</button>
                {/if}
            </div>

        </div>
    </div>
{/if}

<style>
    .mw-backdrop {
        position: fixed;
        inset: 0;
        z-index: 1000;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(0,0,0,0.5);
        backdrop-filter: blur(4px);
    }
    .mw-modal {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 16px;
        width: 92%;
        max-width: 580px;
        max-height: 85vh;
        display: flex;
        flex-direction: column;
        box-shadow: 0 24px 64px rgba(0,0,0,0.35);
        overflow: hidden;
    }
    .mw-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 16px 20px;
        border-bottom: 1px solid var(--card-border);
        flex-shrink: 0;
    }
    .mw-header-left { display: flex; flex-direction: column; gap: 6px; }
    .mw-header h3 { margin: 0; font-size: 15px; font-weight: 600; color: var(--text-primary); }
    .mw-steps { display: flex; gap: 6px; align-items: center; }
    .mw-step-dot {
        width: 7px; height: 7px; border-radius: 50%;
        background: var(--card-border); transition: background 0.2s;
    }
    .mw-step-dot.active { background: var(--accent); }
    .mw-step-dot.done   { background: var(--positive, #34d399); }
    .mw-close {
        display: flex; align-items: center; justify-content: center;
        width: 28px; height: 28px; border: none; border-radius: 8px;
        background: transparent; color: var(--text-secondary); cursor: pointer;
    }
    .mw-close:hover { background: var(--hover-bg); color: var(--text-primary); }
    .mw-close:disabled { opacity: 0.4; cursor: not-allowed; }

    .mw-body {
        padding: 20px;
        overflow-y: auto;
        flex: 1;
        display: flex;
        flex-direction: column;
        gap: 14px;
    }
    .mw-intro { margin: 0; font-size: 13px; color: var(--text-secondary); line-height: 1.5; }

    .mw-loading {
        display: flex; align-items: center; gap: 8px;
        font-size: 13px; color: var(--text-secondary);
        padding: 20px 0;
    }
    .mw-spinner { animation: spin 1s linear infinite; font-size: 20px; }
    .mw-spinner-sm { animation: spin 1s linear infinite; font-size: 16px; }
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }

    .mw-notice {
        padding: 8px 12px; border-radius: 8px;
        font-size: 12px; line-height: 1.5;
    }
    .mw-error  { background: var(--negative-light, rgba(239,68,68,0.12)); color: var(--negative, #f87171); }
    .mw-warn   { background: color-mix(in srgb, var(--accent) 10%, transparent); color: var(--text-secondary); }

    /* Step 1 stats */
    .mw-stat-grid {
        display: grid; grid-template-columns: repeat(3, 1fr);
        gap: 10px;
    }
    .mw-stat {
        background: var(--bg-level-1, var(--hover-bg));
        border: 1px solid var(--card-border);
        border-radius: 10px; padding: 12px;
        display: flex; flex-direction: column; gap: 4px;
    }
    .mw-stat-value { font-size: 18px; font-weight: 700; color: var(--text-primary); }
    .mw-stat-label { font-size: 11px; color: var(--text-secondary); }

    /* Step 2 mapping table */
    .mw-map-table { display: flex; flex-direction: column; gap: 4px; }
    .mw-map-header {
        display: grid; grid-template-columns: 1fr 20px 1fr 80px;
        gap: 8px; padding: 0 4px 6px;
        font-size: 11px; font-weight: 600;
        color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em;
    }
    .mw-map-row {
        display: grid; grid-template-columns: 1fr 20px 1fr 80px;
        gap: 8px; align-items: center;
        padding: 8px;
        background: var(--bg-level-1, var(--hover-bg));
        border: 1px solid var(--card-border);
        border-radius: 8px;
    }
    .mw-acct-info { display: flex; flex-direction: column; gap: 2px; min-width: 0; }
    .mw-acct-name { font-size: 12px; font-weight: 500; color: var(--text-primary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .mw-acct-meta { font-size: 10px; color: var(--text-muted); }
    .mw-arrow { font-size: 14px; color: var(--text-muted); text-align: center; }
    .mw-select {
        width: 100%; padding: 5px 7px; border: 1px solid var(--card-border);
        border-radius: 6px; background: var(--bg); color: var(--text-primary);
        font-size: 12px; font-family: inherit;
    }
    .mw-conf {
        font-size: 10px; font-weight: 600; padding: 3px 7px;
        border-radius: 20px; text-align: center; white-space: nowrap;
    }
    .conf-high   { background: rgba(52,211,153,0.15); color: #34d399; }
    .conf-medium { background: rgba(251,191,36,0.15);  color: #fbbf24; }
    .conf-low    { background: rgba(248,113,113,0.15); color: #f87171; }
    .conf-none   { background: var(--hover-bg); color: var(--text-muted); }

    /* Step 3 preview */
    .mw-preview-rows { display: flex; flex-direction: column; gap: 8px; }
    .mw-preview-row {
        padding: 10px 12px;
        background: var(--bg-level-1, var(--hover-bg));
        border: 1px solid var(--card-border);
        border-radius: 8px;
    }
    .mw-preview-acct {
        display: flex; align-items: center; gap: 8px;
        font-size: 12px; margin-bottom: 6px;
    }
    .mw-preview-tel  { font-weight: 600; color: var(--text-primary); }
    .mw-preview-arrow { color: var(--text-muted); }
    .mw-preview-sf   { color: var(--accent); font-weight: 500; }
    .mw-preview-stats {
        margin: 0; padding: 0 0 0 12px;
        font-size: 11px; color: var(--text-secondary);
        display: flex; flex-direction: column; gap: 2px;
    }
    .mw-preview-stats li { list-style: disc; }
    .mw-total-bar {
        padding: 8px 12px; border-radius: 8px;
        background: color-mix(in srgb, var(--accent) 8%, transparent);
        font-size: 12px; color: var(--text-secondary);
    }
    .mw-checkbox-row {
        display: flex; align-items: center; gap: 8px;
        font-size: 13px; color: var(--text-primary); cursor: pointer;
    }
    .mw-checkbox-row input[type="checkbox"] { width: 15px; height: 15px; cursor: pointer; }

    /* Step 4 result */
    .mw-result {
        display: flex; flex-direction: column; align-items: center;
        gap: 12px; padding: 24px 0; text-align: center;
    }
    .mw-result-icon { font-size: 48px; color: var(--positive, #34d399); }
    .mw-result-title { margin: 0; font-size: 16px; font-weight: 600; color: var(--text-primary); }
    .mw-result-stats {
        margin: 0; padding: 0;
        font-size: 13px; color: var(--text-secondary);
        list-style: none; display: flex; flex-direction: column; gap: 4px;
    }

    /* Footer */
    .mw-footer {
        display: flex; justify-content: flex-end; gap: 8px;
        padding: 12px 20px;
        border-top: 1px solid var(--card-border);
        flex-shrink: 0;
    }
    .mw-btn {
        display: flex; align-items: center; gap: 6px;
        padding: 8px 18px; border: none; border-radius: 8px;
        font-size: 13px; font-weight: 500; cursor: pointer;
        transition: all 0.15s ease;
    }
    .mw-btn:disabled { opacity: 0.45; cursor: not-allowed; }
    .mw-btn-secondary { background: var(--hover-bg); color: var(--text-secondary); }
    .mw-btn-secondary:hover:not(:disabled) { background: var(--card-border); }
    .mw-btn-primary  { background: var(--accent); color: white; }
    .mw-btn-primary:hover:not(:disabled)  { filter: brightness(1.1); }
    .mw-btn-danger   { background: var(--negative, #ef4444); color: white; }
    .mw-btn-danger:hover:not(:disabled)   { filter: brightness(1.1); }
</style>
