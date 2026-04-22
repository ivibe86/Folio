import { writable, derived, get } from 'svelte/store';
import { browser } from '$app/environment';

// Stores the full profile objects: [{ id: "karthik", name: "Karthik" }, ...]
export const profiles = writable([]);

// Stores the selected profile ID: "karthik", "sarah", or "household"
export const activeProfile = writable('household');

// Derived store: converts activeProfile to the query-param value
// "household" → "" (empty, so backend returns all data)
// "karthik"  → "karthik"
export const profileParam = derived(activeProfile, ($ap) =>
    $ap === 'household' ? '' : $ap
);

/**
 * Load profiles using an API client instance (which includes auth headers).
 * Falls back to raw fetch with manual header injection if no API client provided.
 */
export async function loadProfiles(fetchFn = fetch) {
    if (!browser) return;

    // Build headers with API key to match the authenticated apiFetch wrapper
    const API_KEY = typeof import.meta !== 'undefined' && import.meta.env?.VITE_API_KEY
        ? import.meta.env.VITE_API_KEY
        : '';

    try {
        const res = await fetchFn('/api/profiles', {
            headers: {
                'Content-Type': 'application/json',
                ...(API_KEY ? { 'X-API-Key': API_KEY } : {}),
            },
        });

        if (!res.ok) {
            console.error('[profileStore] failed to fetch profiles:', res.status, res.statusText);
            profiles.set([]);
            return;
        }

        const data = await res.json();

        if (!Array.isArray(data)) {
            console.error('[profileStore] unexpected response:', data);
            profiles.set([]);
            return;
        }

        // Normalize: ensure every entry has { id, name }
        const normalized = data.map((item) => {
            if (typeof item === 'string') {
                return { id: item.toLowerCase(), name: item.charAt(0).toUpperCase() + item.slice(1).toLowerCase() };
            }
            if (item && typeof item === 'object' && item.id) {
                return {
                    id:   item.id,
                    name: item.name || item.id.charAt(0).toUpperCase() + item.id.slice(1).toLowerCase()
                };
            }
            return { id: String(item), name: String(item) };
        });

        // Filter out 'household' from the individual profile list
        // (household is a virtual aggregate, not a real Teller profile)
        const individualProfiles = normalized.filter(p => p.id !== 'household');

        profiles.set(individualProfiles);

        const currentActive = get(activeProfile);
        const validIds = new Set(individualProfiles.map((profile) => profile.id));
        if (currentActive === 'household') {
            return;
        }
        if (currentActive && currentActive !== 'household' && validIds.has(currentActive)) {
            return;
        }

        activeProfile.set('household');
    } catch (err) {
        console.error('[profileStore] failed to load profiles:', err);
        profiles.set([]);
    }
}
