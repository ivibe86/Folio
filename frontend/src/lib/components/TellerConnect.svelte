<!--
  TellerConnect.svelte
  "+" button that opens Teller Connect for dynamic bank enrollment.
  On success, sends the access token to the backend's /api/enroll endpoint.
-->
<script>
    import { createEventDispatcher, onMount } from 'svelte';
    import { api } from '$lib/api.js';

    export let applicationId = '';
    export let environment = 'sandbox';

    const dispatch = createEventDispatcher();

    let sdkReady = false;
    let enrolling = false;
    let error = '';

    onMount(() => {
        if (typeof window !== 'undefined' && !window.TellerConnect) {
            const script = document.createElement('script');
            script.src = 'https://cdn.teller.io/connect/connect.js';
            script.async = true;
            script.onload = () => { sdkReady = true; };
            script.onerror = () => { error = 'Failed to load Teller Connect SDK'; };
            document.head.appendChild(script);
        } else if (typeof window !== 'undefined' && window.TellerConnect) {
            sdkReady = true;
        }
    });

    async function openTellerConnect() {
        if (!sdkReady || !window.TellerConnect) {
            error = 'Teller Connect SDK not loaded yet. Please try again.';
            return;
        }
        if (!applicationId) {
            error = 'Teller Application ID not configured.';
            return;
        }

        error = '';
        enrolling = false;

        const handler = window.TellerConnect.setup({
            applicationId,
            environment,
            products: ['transactions', 'balance', 'identity'],
            onSuccess: async (enrollment) => {
                enrolling = true;
                error = '';
                try {
                    const result = await api.enrollAccount(
                        enrollment.accessToken,
                        enrollment.enrollment?.institution?.name,
                        enrollment.enrollment?.id
                    );
                    dispatch('enrolled', result);
                } catch (e) {
                    error = e.message || 'Enrollment failed';
                    dispatch('error', { message: error });
                } finally {
                    enrolling = false;
                }
            },
            onExit: () => {
                dispatch('exit');
            },
            onFailure: (failure) => {
                error = failure?.message || 'Teller Connect encountered an error';
                dispatch('error', { message: error });
            },
        });

        handler.open();
    }
</script>

<button
    class="teller-connect-btn"
    on:click={openTellerConnect}
    disabled={enrolling || !sdkReady}
    title={enrolling ? 'Connecting...' : 'Add bank account'}
    aria-label="Add bank account"
>
    {#if enrolling}
        <span class="material-symbols-outlined text-[16px] animate-spin">progress_activity</span>
    {:else}
        <span class="material-symbols-outlined text-[16px]">add</span>
    {/if}
</button>

{#if error}
    <div class="teller-connect-error">
        <span class="material-symbols-outlined text-[12px]">error</span>
        {error}
    </div>
{/if}

<style>
    .teller-connect-btn {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 32px;
        height: 32px;
        border-radius: 10px;
        border: 1.5px dashed var(--card-border);
        background: color-mix(in srgb, var(--accent) 6%, transparent);
        color: var(--accent);
        cursor: pointer;
        transition: all 0.2s ease;
        flex-shrink: 0;
    }
    .teller-connect-btn:hover:not(:disabled) {
        background: color-mix(in srgb, var(--accent) 14%, transparent);
        border-color: var(--accent);
        transform: scale(1.05);
    }
    .teller-connect-btn:disabled {
        opacity: 0.5;
        cursor: not-allowed;
    }
    .teller-connect-error {
        display: flex;
        align-items: center;
        gap: 4px;
        margin-top: 4px;
        padding: 4px 8px;
        border-radius: 6px;
        font-size: 10px;
        color: var(--negative);
        background: var(--negative-light);
    }
    .animate-spin {
        animation: spin 1s linear infinite;
    }
    @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }
</style>