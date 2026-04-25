/** @type {import('tailwindcss').Config} */
export default {
    content: ['./src/**/*.{html,js,svelte,ts}'],
    darkMode: 'class',
    theme: {
        extend: {
            fontFamily: {
                sans: ['var(--font-ui)'],
                display: ['var(--font-display)'],
                mono: ['var(--font-tabular)']
            },
            colors: {
                surface: {
                    50: 'var(--steel-50)',
                    100: 'var(--surface-100)',
                    200: 'var(--surface-200)',
                    300: 'var(--surface-300)',
                },
                accent: {
                    DEFAULT: 'var(--accent)',
                    hover: 'var(--accent-hover)',
                    soft: 'var(--accent-soft)',
                },
                positive: 'var(--positive)',
                negative: 'var(--negative)',
                warning: 'var(--warning)',
                sidebar: {
                    bg: 'var(--sidebar-bg)',
                    text: 'var(--sidebar-text)',
                    active: 'var(--sidebar-text-active)',
                },
                /* Cool Slate semantic tokens */
                pearl: {
                    50:  '#F4F6F9',
                    100: '#EEF1F6',
                    200: '#E2E6EE',
                    300: '#CDD3DF',
                },
                ink: {
                    900: '#1A1F2B',
                    700: '#3A4150',
                    500: '#5A6478',
                    300: '#8C95A6',
                },
                flow: {
                    blue:    '#3B82F6',
                    rose:    '#F43F5E',
                    emerald: '#10B981',
                    violet:  '#8B5CF6',
                    amber:   '#F59E0B',
                    cyan:    '#38BDF8',
                    slate:   '#829AB1',
                },
                theater: {
                    900: '#1B2432',
                    800: '#253040',
                    700: '#2E3A48',
                    600: '#384656',
                    border: 'rgba(56, 78, 108, 0.35)',
                },
            },
            boxShadow: {
                'card-rest': '0 1px 3px rgba(0,0,0,0.04), 0 4px 16px rgba(0,0,0,0.03), 0 8px 32px rgba(0,0,0,0.02)',
                'card-hover': '0 4px 12px rgba(0,0,0,0.06), 0 16px 48px rgba(0,0,0,0.05), 0 2px 4px rgba(0,0,0,0.02)',
                'recessed': 'inset 0 2px 4px rgba(0,0,0,0.03)',
                'node-glow-blue': '0 0 16px rgba(59,130,246,0.25), 0 2px 8px rgba(0,0,0,0.06)',
                'node-glow-rose': '0 0 16px rgba(244,63,94,0.25), 0 2px 8px rgba(0,0,0,0.06)',
                'node-glow-emerald': '0 0 16px rgba(16,185,129,0.25), 0 2px 8px rgba(0,0,0,0.06)',
                'node-glow-violet': '0 0 16px rgba(139,92,246,0.25), 0 2px 8px rgba(0,0,0,0.06)',
                'node-glow-amber': '0 0 16px rgba(245,158,11,0.25), 0 2px 8px rgba(0,0,0,0.06)',
            },
            animation: {
                'glow-pulse': 'glow-pulse 3s ease-in-out infinite',
                'flow-sweep': 'flowSweep 2.5s ease-in-out forwards',
            },
        }
    },
    plugins: []
};
