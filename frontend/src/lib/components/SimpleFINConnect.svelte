<!--
  SimpleFINConnect.svelte
  Modal dialog for connecting a bank via SimpleFIN Bridge.
  User pastes a Setup Token, selects a profile, and clicks Connect.
-->
<script>
    import { createEventDispatcher, onMount } from 'svelte';
    import { api } from '$lib/api.js';
    import { syncing } from '$lib/stores.js';

    const dispatch = createEventDispatcher();

    let open = false;
    let setupToken = '';
    let displayName = '';
    let selectedProfile = '';
    let createProfile = false;
    let newProfileName = '';
    let profiles = [];
    let connecting = false;
    let error = '';
    let success = '';

    async function refreshProfiles() {
        try {
            profiles = await api.getProfiles();
        } catch (e) {
            profiles = [];
        }
    }

    onMount(refreshProfiles);

    $: existingProfiles = profiles.filter(p => p.id !== 'household');
    $: profileOptions = existingProfiles.some((profile) => profile.id === 'primary')
        ? existingProfiles
        : [...existingProfiles, { id: 'primary', name: 'Primary' }];

    export async function show() {
        await refreshProfiles();
        open = true;
        setupToken = '';
        displayName = '';
        error = '';
        success = '';
        connecting = false;
        createProfile = false;
        newProfileName = '';
        selectedProfile = existingProfiles.length > 0 ? existingProfiles[0].id : 'primary';
    }

    function close() {
        open = false;
    }

    function handleBackdropClick(e) {
        if (e.target === e.currentTarget) close();
    }

    function handleKeydown(e) {
        if (e.key === 'Escape') close();
    }

    async function handleConnect() {
        const token = setupToken.trim();
        if (!token) {
            error = 'Please paste your SimpleFIN Setup Token.';
            return;
        }
        const profileValue = createProfile
            ? newProfileName.trim()
            : (selectedProfile || 'primary');
        if (!profileValue) {
            error = 'Please choose a profile or enter a new profile name.';
            return;
        }

        error = '';
        success = '';
        connecting = true;
        syncing.start('simplefin');

        try {
            const result = await api.claimSimpleFIN(
                token,
                profileValue,
                displayName.trim(),
            );
            success = `Connected! Syncing accounts in the background…`;
            dispatch('connected', result);
            // Auto-close after a short delay
            setTimeout(() => {
                close();
            }, 1500);
        } catch (e) {
            error = e.message || 'Failed to connect. The token may be invalid or already claimed.';
            syncing.stop();
        } finally {
            connecting = false;
        }
    }

    function handleProfileSelection(value) {
        if (value === '__create__') {
            createProfile = true;
            newProfileName = '';
            return;
        }
        createProfile = false;
        selectedProfile = value;
    }
</script>

<svelte:window on:keydown={handleKeydown} />

{#if open}
    <!-- svelte-ignore a11y-click-events-have-key-events a11y-no-static-element-interactions -->
    <div class="sf-backdrop" on:click={handleBackdropClick}>
        <div class="sf-modal" role="dialog" aria-modal="true" aria-label="Connect via SimpleFIN">
            <div class="sf-header">
                <h3>Connect via SimpleFIN Bridge</h3>
                <button class="sf-close-btn" on:click={close} aria-label="Close">
                    <span class="material-symbols-outlined">close</span>
                </button>
            </div>

            <div class="sf-body">
                <div class="sf-step">
                    <span class="sf-step-num">1</span>
                    <p>
                        Visit <strong>SimpleFIN Bridge</strong> to connect your bank and get a
                        Setup Token. Copy the token they provide.
                    </p>
                </div>

                <div class="sf-step">
                    <span class="sf-step-num">2</span>
                    <p>Paste the Setup Token below and click Connect.</p>
                </div>

                <label class="sf-label" for="sf-token">Setup Token</label>
                <textarea
                    id="sf-token"
                    class="sf-textarea"
                    bind:value={setupToken}
                    placeholder="Paste your base64-encoded Setup Token here..."
                    rows="3"
                    disabled={connecting}
                ></textarea>

                <div class="sf-row">
                    <div class="sf-field">
                        <label class="sf-label" for="sf-profile">Profile</label>
                        <select
                            id="sf-profile"
                            class="sf-select"
                            value={createProfile ? '__create__' : selectedProfile}
                            on:change={(e) => handleProfileSelection(e.target.value)}
                            disabled={connecting}
                        >
                            {#each profileOptions as p}
                                <option value={p.id}>{p.name || p.display_name || p.id}</option>
                            {/each}
                            <option value="__create__">Create new profile…</option>
                        </select>
                    </div>
                    <div class="sf-field">
                        <label class="sf-label" for="sf-name">Label (optional)</label>
                        <input
                            id="sf-name"
                            class="sf-input"
                            type="text"
                            bind:value={displayName}
                            placeholder="e.g. My Credit Union"
                            disabled={connecting}
                        />
                    </div>
                </div>

                {#if createProfile}
                    <div class="sf-field">
                        <label class="sf-label" for="sf-profile-new">New Profile Name</label>
                        <input
                            id="sf-profile-new"
                            class="sf-input"
                            type="text"
                            bind:value={newProfileName}
                            placeholder="e.g. Karthik or Wife"
                            disabled={connecting}
                        />
                    </div>
                {/if}

                {#if error}
                    <div class="sf-notice sf-error">
                        <span class="material-symbols-outlined text-[14px]">error</span>
                        {error}
                    </div>
                {/if}
                {#if success}
                    <div class="sf-notice sf-success">
                        <span class="material-symbols-outlined text-[14px]">check_circle</span>
                        {success}
                    </div>
                {/if}
            </div>

            <div class="sf-footer">
                <button class="sf-btn sf-btn-secondary" on:click={close} disabled={connecting}>
                    Cancel
                </button>
                <button class="sf-btn sf-btn-primary" on:click={handleConnect} disabled={connecting || !setupToken.trim()}>
                    {#if connecting}
                        <span class="material-symbols-outlined text-[16px] animate-spin">progress_activity</span>
                        Connecting...
                    {:else}
                        Connect
                    {/if}
                </button>
            </div>
        </div>
    </div>
{/if}

<style>
    .sf-backdrop {
        position: fixed;
        inset: 0;
        z-index: 6000;
        display: flex;
        align-items: flex-start;
        justify-content: center;
        padding: clamp(72px, 11vh, 128px) 16px 24px;
        background: rgba(0, 0, 0, 0.45);
        backdrop-filter: blur(4px);
        overflow-y: auto;
    }
    .sf-modal {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 16px;
        width: 90%;
        max-width: 480px;
        box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
        overflow: hidden;
    }
    .sf-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 16px 20px;
        border-bottom: 1px solid var(--card-border);
    }
    .sf-header h3 {
        margin: 0;
        font-size: 15px;
        font-weight: 600;
        color: var(--text-primary);
    }
    .sf-close-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 28px;
        height: 28px;
        border: none;
        border-radius: 8px;
        background: transparent;
        color: var(--text-secondary);
        cursor: pointer;
        transition: all 0.15s ease;
    }
    .sf-close-btn:hover {
        background: var(--hover-bg);
        color: var(--text-primary);
    }
    .sf-body {
        padding: 20px;
        display: flex;
        flex-direction: column;
        gap: 12px;
    }
    .sf-step {
        display: flex;
        align-items: flex-start;
        gap: 10px;
        font-size: 13px;
        color: var(--text-secondary);
        line-height: 1.5;
    }
    .sf-step p {
        margin: 0;
    }
    .sf-step-num {
        flex-shrink: 0;
        display: flex;
        align-items: center;
        justify-content: center;
        width: 22px;
        height: 22px;
        border-radius: 50%;
        background: color-mix(in srgb, var(--accent) 12%, transparent);
        color: var(--accent);
        font-size: 11px;
        font-weight: 700;
    }
    .sf-label {
        font-size: 12px;
        font-weight: 600;
        color: var(--text-secondary);
        margin-bottom: -6px;
    }
    .sf-textarea,
    .sf-input,
    .sf-select {
        width: 100%;
        padding: 8px 10px;
        border: 1px solid var(--card-border);
        border-radius: 8px;
        background: var(--bg);
        color: var(--text-primary);
        font-size: 13px;
        font-family: inherit;
        resize: vertical;
        transition: border-color 0.15s ease;
        box-sizing: border-box;
    }
    .sf-textarea:focus,
    .sf-input:focus,
    .sf-select:focus {
        outline: none;
        border-color: var(--accent);
    }
    .sf-row {
        display: flex;
        gap: 12px;
    }
    .sf-field {
        flex: 1;
        display: flex;
        flex-direction: column;
        gap: 6px;
    }
    .sf-notice {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 8px 10px;
        border-radius: 8px;
        font-size: 12px;
    }
    .sf-error {
        color: var(--negative);
        background: var(--negative-light);
    }
    .sf-success {
        color: var(--positive);
        background: var(--positive-light);
    }
    .sf-footer {
        display: flex;
        justify-content: flex-end;
        gap: 8px;
        padding: 12px 20px;
        border-top: 1px solid var(--card-border);
    }
    .sf-btn {
        display: flex;
        align-items: center;
        gap: 6px;
        padding: 8px 16px;
        border: none;
        border-radius: 8px;
        font-size: 13px;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.15s ease;
    }
    .sf-btn:disabled {
        opacity: 0.5;
        cursor: not-allowed;
    }
    .sf-btn-secondary {
        background: var(--hover-bg);
        color: var(--text-secondary);
    }
    .sf-btn-secondary:hover:not(:disabled) {
        background: var(--card-border);
    }
    .sf-btn-primary {
        background: var(--accent);
        color: white;
    }
    .sf-btn-primary:hover:not(:disabled) {
        filter: brightness(1.1);
    }
    .animate-spin {
        animation: spin 1s linear infinite;
    }
    @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }
</style>
