<script>
    import { profiles, activeProfile } from '$lib/stores/profileStore';

    function select(id) {
        $activeProfile = id;
    }
</script>

{#if $profiles.length > 0}
    <div class="profile-switcher-pills">
        <!-- "Household" (combined) button always first -->
        <button
            class="profile-pill"
            class:profile-pill--active={'household' === $activeProfile}
            on:click={() => select('household')}
        >
            <span class="material-symbols-outlined profile-pill-icon">groups</span>
            Household
        </button>

        {#each $profiles as p (p.id)}
            <button
                class="profile-pill"
                class:profile-pill--active={p.id === $activeProfile}
                on:click={() => select(p.id)}
            >
                <span class="material-symbols-outlined profile-pill-icon">person</span>
                {p.name}
            </button>
        {/each}
    </div>
{/if}

<style>
    .profile-switcher-pills {
        display: flex;
        align-items: center;
        gap: 6px;
        flex-shrink: 0;
    }

    .profile-pill {
        display: flex;
        align-items: center;
        gap: 5px;
        padding: 6px 14px;
        border: 1px solid var(--card-border);
        border-radius: 9999px;
        background: var(--card-bg);
        color: var(--text-muted);
        font-size: 12px;
        font-weight: 500;
        font-family: inherit;
        cursor: pointer;
        white-space: nowrap;
        transition: all 0.18s ease;
        line-height: 1.2;
    }

    .profile-pill:hover {
        background: var(--surface-100);
        color: var(--text-secondary);
        border-color: var(--text-muted);
    }

    .profile-pill--active {
        background: color-mix(in srgb, var(--accent) 12%, transparent);
        color: var(--accent);
        border-color: color-mix(in srgb, var(--accent) 40%, transparent);
        font-weight: 600;
        box-shadow: 0 0 0 1px color-mix(in srgb, var(--accent) 8%, transparent);
    }

    .profile-pill--active:hover {
        background: color-mix(in srgb, var(--accent) 18%, transparent);
        color: var(--accent);
        border-color: color-mix(in srgb, var(--accent) 50%, transparent);
    }

    .profile-pill-icon {
        font-size: 14px;
        line-height: 1;
    }
</style>