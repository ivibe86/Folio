import { writable } from 'svelte/store';
import { browser } from '$app/environment';

// ── Dark Mode ──
function createThemeStore() {
    // On the client the inline script in app.html has ALREADY applied the
    // correct class before any JS bundle executes.  Read the DOM truth so
    // the Svelte store is guaranteed to agree with what's painted.
    const initial = browser
        ? document.documentElement.classList.contains('dark')
        : false;

    const { subscribe, set, update } = writable(initial);

    // No need to touch classList here — the inline script already did it.
    // This avoids any momentary class toggle that could cause a flash.

    /**
     * Performs theme switch with blur-suppression to prevent
     * backdrop-filter recomposite jank.
     *
     * Sequence:
     *   1. Add .theme-switching  (kills all backdrop-filter instantly)
     *   2. Force style recalc    (browser batches the variable changes)
     *   3. Toggle .dark           (variables swap, but no blur to recomposite)
     *   4. Wait 1 rAF            (let paint happen without blur cost)
     *   5. Remove .theme-switching (blur re-enables on already-painted surfaces)
     */
    function performSwitch(next) {
        const root = document.documentElement;

        // Step 1: Kill all blur BEFORE changing theme
        root.classList.add('theme-switching');

        // Step 2: Force synchronous style recalc so the browser
        // processes the blur removal before we change variables
        void root.offsetHeight;

        // Step 3: Swap theme
        root.classList.toggle('dark', next);
        localStorage.setItem('theme', next ? 'dark' : 'light');

        // Step 4: Wait for two animation frames — one for the style
        // recalc, one for the paint — then re-enable blur
        requestAnimationFrame(() => {
            requestAnimationFrame(() => {
                root.classList.remove('theme-switching');
            });
        });
    }

    return {
        subscribe,
        toggle: () => {
            update((dark) => {
                const next = !dark;
                if (browser) performSwitch(next);
                return next;
            });
        },
        set: (value) => {
            if (browser) performSwitch(value);
            set(value);
        }
    };
}

export const darkMode = createThemeStore();

// ── Filters ──
export const filters = writable({
    month: null,
    category: null,
    account: null,
    search: ''
});

// ── Sync state (persistent across navigation + page refresh) ──
function createSyncStore() {
    // Rehydrate from sessionStorage so state survives navigation & refresh
    let initial = { active: false, startedAt: null, context: null };
    if (browser) {
        try {
            const stored = sessionStorage.getItem('folioSyncState');
            if (stored) {
                const parsed = JSON.parse(stored);
                // If sync was "active" but started more than 15 min ago, consider it stale
                if (parsed.active && parsed.startedAt) {
                    const elapsed = Date.now() - parsed.startedAt;
                    if (elapsed > 15 * 60 * 1000) {
                        parsed.active = false; // stale — clear it
                    }
                }
                initial = parsed;
            }
        } catch (_) {}
    }

    const { subscribe, set, update } = writable(initial);

    // Persist every change to sessionStorage
    if (browser) {
        subscribe(value => {
            try {
                sessionStorage.setItem('folioSyncState', JSON.stringify(value));
            } catch (_) {}
        });
    }

    return {
        subscribe,
        /** Start sync — call when enrollment begins */
        start: (context = 'enrollment') => {
            set({ active: true, startedAt: Date.now(), context });
        },
        /** End sync — call when polling detects completion */
        stop: () => {
            set({ active: false, startedAt: null, context: null });
        },
        /** Raw set for backward compatibility with $syncing = true/false pattern */
        set: (val) => {
            if (typeof val === 'boolean') {
                if (val) {
                    set({ active: true, startedAt: Date.now(), context: 'manual-sync' });
                } else {
                    set({ active: false, startedAt: null, context: null });
                }
            } else {
                set(val);
            }
        },
        update,
    };
}

export const syncing = createSyncStore();

// ── Global data cache ──
export const summaryData = writable(null);
export const accountsData = writable(null);

// ── Dashboard expanded sections state ──
export const dashboardPrefs = writable({
    showForecast: true,
    showUpcoming: true
});

// ── Shared period selection (persists across page navigation) ──
export const selectedPeriodStore = writable('this_month');
export const selectedCustomMonthStore = writable('');

// ââ Privacy Mode ââ
function createPrivacyStore() {
    const initial = browser
        ? localStorage.getItem('privacyMode') === 'true'
        : false;

    const { subscribe, set, update } = writable(initial);

    return {
        subscribe,
        toggle: () => {
            update((current) => {
                const next = !current;
                if (browser) localStorage.setItem('privacyMode', String(next));
                return next;
            });
        },
        set: (value) => {
            if (browser) localStorage.setItem('privacyMode', String(value));
            set(value);
        }
    };
}

export const privacyMode = createPrivacyStore();