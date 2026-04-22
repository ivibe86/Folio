<script>
    import { createEventDispatcher } from 'svelte';

    const dispatch = createEventDispatcher();

    function openChooser() {
        dispatch('open');
    }
</script>

<button
    class="teller-connect-btn"
    on:click={openChooser}
    title="Add bank account"
    aria-label="Add bank account"
>
    <span class="material-symbols-outlined text-[16px]">add</span>
</button>

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
    .teller-connect-btn:hover {
        background: color-mix(in srgb, var(--accent) 14%, transparent);
        border-color: var(--accent);
        transform: scale(1.05);
    }
</style>
