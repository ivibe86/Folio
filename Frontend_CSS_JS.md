Folio Frontend — Comprehensive Codebase Documentation


frontend_package_json.txt

Purpose

This is the NPM package manifest for the Folio frontend application (internally named finflow-frontend). It declares the project's metadata (name, version, type), all development and runtime dependencies, and the scripts used to develop, build, and preview the SvelteKit application. It is the single source of truth for which libraries are available at build time and runtime.


Key Dependencies

Dev Dependencies (build toolchain):


@sveltejs/adapter-auto / @sveltejs/adapter-node — SvelteKit deployment adapters; adapter-node is used for Docker builds, adapter-auto for general/dev use.
@sveltejs/kit (^2.5.0) — The SvelteKit framework core.
@sveltejs/vite-plugin-svelte (^3.1.0) — Integrates Svelte compilation into Vite's build pipeline.
autoprefixer / postcss — PostCSS toolchain for adding vendor prefixes to CSS.
svelte (^4.2.0) — The Svelte compiler itself.
tailwindcss (^3.4.3) — Utility-first CSS framework used for layout and spacing.
vite (^5.2.0) — The underlying dev server and bundler.

Runtime Dependencies:


d3-array, d3-interpolate, d3-sankey, d3-scale, d3-shape — Selective D3 modules used for the Sankey flow diagram, waterfall charts, net worth sparklines, and other custom SVG visualizations. Notably, the full d3 bundle is not imported — only the specific submodules needed, keeping the bundle size minimal.

Core Functions / Classes / Exports

scripts.dev

What it does: Starts the Vite development server with SvelteKit's dev middleware.
Notable logic: Proxies /api requests to the FastAPI backend via vite.config.js.

scripts.build

What it does: Runs vite build to produce the production SvelteKit output.
Notable logic: The adapter selection (auto vs. node) is governed by svelte.config.js based on the DOCKER env var.

scripts.preview

What it does: Serves the built output locally for pre-deployment testing.

Data Flow

This file doesn't process data directly. It defines the dependency graph that Vite resolves at build time, and the scripts that invoke the build/dev toolchain.


Integration Points

Referenced by npm install, npm run dev, npm run build.
The dependencies listed here are imported by Svelte components (e.g., Sankey chart components import d3-sankey).
The devDependencies are consumed by vite.config.js, svelte.config.js, postcss.config.js, and tailwind.config.js.

Known Quirks / Design Notes

The version is 3.0.0, suggesting significant iteration.
"type": "module" enables ESM imports throughout the project.
The "private": true flag prevents accidental npm publishing.


frontend_vite_config_js.txt

Purpose

Configures the Vite build tool for the Folio frontend. It loads environment variables, registers the custom injectEnvMeta plugin (which injects the API key into the HTML template), registers the SvelteKit Vite plugin, and sets up the development proxy so that all /api requests are forwarded to the FastAPI backend.


Key Dependencies

@sveltejs/kit/vite — Provides the sveltekit() Vite plugin.
vite — defineConfig and loadEnv are used for typed configuration and env loading.
./vite-env-plugin.js — Custom plugin for injecting the API key into app.html.

Core Functions / Classes / Exports

export default defineConfig(({ mode }) => { ... })

What it does: Returns a Vite configuration object tailored to the current build mode (development or production).
Inputs: mode — Vite build mode string.
Outputs: A Vite config object with plugins and server.proxy settings.
Notable logic:
loadEnv(mode, process.cwd(), '') loads all .env files (not just VITE_-prefixed ones) and merges them into process.env. The empty third argument ('') means all env vars are loaded, not just those with the VITE_ prefix — this is required because BACKEND_URL (used for the proxy target) doesn't have a VITE_ prefix.
Object.assign(process.env, env) makes loaded env vars available to the injectEnvMeta plugin, which reads process.env.VITE_API_KEY.
The proxy config forwards /api to env.BACKEND_URL || 'http://localhost:8000', enabling seamless local development without CORS issues.

Data Flow

Vite starts → loads .env files → populates process.env.
The injectEnvMeta plugin transforms app.html, replacing %FINFLOW_API_KEY% with the actual API key.
The sveltekit() plugin handles Svelte compilation, routing, SSR, etc.
Dev server proxies /api/* requests to the FastAPI backend.

Integration Points

Consumed by vite dev, vite build, and vite preview.
Depends on vite-env-plugin.js for HTML transformation.
The proxy configuration means the frontend dev server seamlessly talks to the backend during development.

Known Quirks / Design Notes

The plugin order matters: injectEnvMeta() runs before sveltekit() to ensure the HTML template is transformed before SvelteKit processes it.
The BACKEND_URL fallback to localhost:8000 means zero-config local development works out of the box.


frontend_vite-env-plugin_js.txt

Purpose

A custom Vite plugin that injects the VITE_API_KEY environment variable into the app.html template at build/dev time. This is necessary because the inline <script> in app.html that prefetches the dashboard bundle runs before any ES modules are evaluated, so it cannot use import.meta.env. Instead, it reads the key from a placeholder that this plugin replaces.


Key Dependencies

None (pure JavaScript, uses only the Vite plugin API).


Core Functions / Classes / Exports

injectEnvMeta() → VitePlugin

What it does: Returns a Vite plugin object with a transformIndexHtml hook.
Inputs: None (reads process.env.VITE_API_KEY from the environment).
Outputs: A plugin that replaces %FINFLOW_API_KEY% in app.html with the actual API key value.
Notable logic:
Falls back to an empty string if VITE_API_KEY is not set, which means the prefetch request will go out without authentication — but the load function fallback in +page.js will retry with the proper headers.
The transformIndexHtml hook is specifically designed for this pattern — it runs on every HTML file Vite processes.

Data Flow

Vite processes app.html.
This plugin's transformIndexHtml is called with the raw HTML string.
The %FINFLOW_API_KEY% placeholder is replaced with the actual key.
The transformed HTML is served to the browser.

Integration Points

Imported and registered by vite.config.js.
The placeholder it replaces exists in app.html's inline prefetch script.

Known Quirks / Design Notes

This is a clean workaround for the fundamental timing issue: inline <script> tags execute before Vite's module system is available, so import.meta.env cannot be used. The plugin bridges this gap by performing a build-time string replacement.
The approach is secure for server-rendered scenarios because the API key never appears in the source template — only in the built output.


frontend_svelte_config_js.txt

Purpose

The SvelteKit configuration file that controls the build adapter and other framework-level settings. Its key role is to conditionally select between adapter-auto (for general deployment platforms like Vercel/Netlify) and adapter-node (for Docker deployments) based on the DOCKER environment variable.


Key Dependencies

@sveltejs/adapter-auto — Auto-detects deployment platform.
@sveltejs/adapter-node — Produces a Node.js server suitable for Docker containers.

Core Functions / Classes / Exports

config (default export)

What it does: Exports the SvelteKit configuration object.
Notable logic:
const isDocker = process.env.DOCKER === 'true' — Checks if running in Docker.
If Docker: uses adapterNode({ out: 'build' }), which outputs to a build/ directory.
If not Docker: uses adapterAuto(), which auto-detects the target platform.

Data Flow

This file is read by SvelteKit at build time to determine how the application should be packaged.


Integration Points

Consumed by @sveltejs/kit during vite build.
The DOCKER env var is expected to be set in the Docker build context (e.g., Dockerfile).

Known Quirks / Design Notes

The dual-adapter approach allows the same codebase to deploy to both Docker and serverless platforms without code changes.
adapter-node with out: 'build' is a standard convention for Docker-based Node.js deployments.


frontend_postcss_config_js.txt

Purpose

Configures the PostCSS processing pipeline. It registers TailwindCSS as a PostCSS plugin (which processes @tailwind directives and utility classes) and Autoprefixer (which adds vendor prefixes for cross-browser CSS compatibility).


Key Dependencies

tailwindcss — Processes utility classes and @apply directives.
autoprefixer — Adds -webkit-, -moz-, etc. prefixes where needed.

Core Functions / Classes / Exports

Default export (config object)

What it does: Tells PostCSS to run TailwindCSS first, then Autoprefixer on all CSS files.

Data Flow

Vite processes CSS imports.
PostCSS runs TailwindCSS (generates utility classes, processes @apply).
PostCSS runs Autoprefixer (adds vendor prefixes).
The processed CSS is included in the bundle.

Integration Points

Consumed by Vite's built-in PostCSS integration.
Works in tandem with tailwind.config.js for Tailwind configuration.

Known Quirks / Design Notes

This is a minimal, standard PostCSS configuration. No custom plugins or advanced processing.


frontend_tailwind_config_js.txt

Purpose

Configures TailwindCSS for the Folio frontend. It defines the content paths for purging unused classes, enables class-based dark mode (toggled by the .dark class on <html>), and extends the default theme with custom font families, a comprehensive semantic color system (mapped to CSS custom properties), custom box shadows, and custom animations. This file is the bridge between Tailwind's utility classes and the design system defined in the CSS theme files.


Key Dependencies

TailwindCSS core (implicit — this file configures it).

Core Functions / Classes / Exports

content

What it does: Tells Tailwind to scan all .html, .js, .svelte, and .ts files in src/ for class usage. This enables tree-shaking of unused utility classes in production builds.

darkMode: 'class'

What it does: Enables dark mode via a .dark class on the root element, rather than relying on prefers-color-scheme. This gives the app programmatic control over theme switching.

theme.extend.fontFamily

sans: Inter → system-ui fallback chain.
display: Same as sans (used for headlines).
mono: DM Mono → Cascadia Code fallback (used for financial numbers).

theme.extend.colors

What it does: Maps Tailwind color names to CSS custom properties, enabling theme-aware colors in utility classes.
Notable entries:
surface.50/100/200/300 — Background levels mapped to --steel-50, --surface-100, etc.
accent.DEFAULT/hover/soft — Primary action color mapped to --accent variants.
positive/negative/warning — Semantic colors for financial indicators.
sidebar.* — Sidebar-specific colors.
pearl.* / ink.* — Named palette scales for the "Cool Slate" design system.
flow.* — Colors for Sankey diagram flows (blue, rose, emerald, violet, amber, cyan, slate).
theater.* — Deep dark colors for the Sankey "theater" background element.

theme.extend.boxShadow

Custom shadows like card-rest, card-hover, recessed, and category-specific node-glow-* shadows for the Sankey node elements.

theme.extend.animation

glow-pulse — A 3-second infinite glow animation.
flow-sweep — A 2.5-second one-shot animation for Sankey flow paths.

Data Flow

Tailwind reads this config at build time, generates the CSS utility classes that match the content files, and applies the extended theme values.


Integration Points

Consumed by PostCSS via postcss.config.js.
The CSS custom properties referenced here (--accent, --surface-100, etc.) are defined in theme-light.css and theme-dark.css.
Svelte components use these extended Tailwind classes in their templates.

Known Quirks / Design Notes

The color system is a hybrid: some colors use Tailwind's standard hex values (e.g., pearl, ink, theater), while others use CSS custom properties (e.g., accent, surface). The custom-property-based ones are theme-aware (they change between light and dark mode), while the hex-based ones are static.
No Tailwind plugins are used (plugins: []).


frontend__npmrc.txt

Purpose

An npm configuration file that sets a minimum release age policy. The min-release-age=7 directive tells npm to only install package versions that have been published for at least 7 days. This is a supply-chain security measure that protects against compromised packages that might be published and quickly removed, or against accidental publishing of broken versions.


Key Dependencies

None.


Core Functions / Classes / Exports

N/A — this is a configuration file.


Data Flow

Read by npm during npm install to filter package versions.


Integration Points

Affects all npm install operations for this project.


Known Quirks / Design Notes

This is a thoughtful security measure that's rare in frontend projects. It adds a 7-day buffer against supply-chain attacks targeting fresh npm publishes.


frontend_src_app_html.txt

Purpose

The root HTML template for the SvelteKit application. It serves as the shell into which SvelteKit renders the app. It contains three critical inline scripts and the font/icon loading strategy, all carefully ordered for performance and UX:


Dashboard data prefetch — Fires a fetch() to /api/dashboard-bundle immediately, before any JavaScript bundle loads. The response promise is stored on window.__dashboardData for the dashboard's +page.js to consume.
FOUC prevention — Reads the theme from localStorage and applies the .dark class before first paint, preventing a flash of the wrong theme.
Font loading strategy — Uses preload for the Material Symbols icon font CSS (preventing icon-name FOUT), and display=swap for text fonts (Inter, Manrope, JetBrains Mono, DM Mono).

Key Dependencies

Google Fonts (external CDN) — For Inter, Manrope, JetBrains Mono, DM Mono.
Material Symbols Outlined — Self-hosted from /fonts/ directory (referenced in app.css).
%sveltekit.head% and %sveltekit.body% — SvelteKit template placeholders.
%FINFLOW_API_KEY% — Replaced at build time by the injectEnvMeta Vite plugin.

Core Functions / Classes / Exports

Inline Script 1: Dashboard Prefetch

javascript
Copy Code
window.__dashboardData = fetch('/api/dashboard-bundle?nw_interval=biweekly', {
    headers: { 'X-API-Key': '%FINFLOW_API_KEY%' }
}).then(r => r.ok ? r.json() : null).catch(() => null);

What it does: Kicks off the most expensive API call immediately, in parallel with JS bundle download. The +page.js loader checks this promise first before making its own request.
Notable logic: Uses the %FINFLOW_API_KEY% placeholder which is replaced by the Vite plugin. If it fails (bad key, network error), it resolves to null, and the +page.js loader falls back to a standard fetch.

Inline Script 2: FOUC Prevention

What it does: Reads localStorage.getItem('theme') and applies/removes .dark on <html> synchronously, before CSS is parsed.
Notable logic: Wrapped in try/catch for environments where localStorage is unavailable (e.g., incognito mode in some browsers).

Font Loading

<link rel="preload" ... as="style"> for Material Symbols CSS — highest priority, prevents icon FOUT.
<link href="..." rel="stylesheet"> for text fonts with display=swap — shows fallback text immediately, swaps when fonts load.
Material Symbols Outlined is self-hosted (/fonts/material-symbols-outlined.woff2) as declared in app.css — zero network latency for icons.

Data Flow

Browser receives this HTML.
Inline scripts execute immediately (prefetch + theme).
Font preloads begin.
SvelteKit's JS bundle loads and hydrates into %sveltekit.body%.
The +page.js loader picks up window.__dashboardData.

Integration Points

vite-env-plugin.js replaces %FINFLOW_API_KEY%.
+page.js (dashboard route) consumes window.__dashboardData.
The theme store (stores.js) reads the DOM state set by the FOUC prevention script.
app.css defines the @font-face for the self-hosted Material Symbols font.

Known Quirks / Design Notes

The data-sveltekit-preload-data="hover" attribute on <body> tells SvelteKit to prefetch page data when the user hovers over internal links, improving perceived navigation speed.
The <div style="display: contents"> wrapper around %sveltekit.body% is a SvelteKit convention that avoids an extra DOM node while maintaining the template structure.
The comment notes that Material Symbols is self-hosted for "zero network latency, no FOUT" — but the <link rel="preload"> in the head still points to Google Fonts CDN. This is likely a redundancy: the CDN link is for the CSS that references the self-hosted font file, or it may be a legacy artifact. The @font-face in app.css uses /fonts/material-symbols-outlined.woff2 which is the actual self-hosted path.


frontend_src_app_css.txt

Purpose

The global stylesheet for the Folio application. This is the master CSS file that imports Tailwind's base/components/utilities layers, the light and dark theme variable files, and defines the entire shared design system. It covers: self-hosted font declarations, the glassmorphism card system, the Sankey theater and category pill components, typography, animations, skeleton loading states, scrollbar styling, utility classes, and theme switching mechanics. This file is imported by SvelteKit's root layout and applies to every page.


Key Dependencies

tailwindcss/base, tailwindcss/components, tailwindcss/utilities — Tailwind's CSS layers.
./theme-light.css and ./theme-dark.css — CSS custom property definitions for each theme.
Self-hosted font: /fonts/material-symbols-outlined.woff2.

Core Functions / Classes / Exports

Material Symbols Font Declaration

css
Copy Code
@font-face {
  font-family: 'Material Symbols Outlined';
  font-weight: 100 700;
  font-display: block;
  src: url('/fonts/material-symbols-outlined.woff2') format('woff2');
}

What it does: Declares the self-hosted Material Symbols icon font with variable weight support and font-display: block (hides text until glyphs load, which is <10ms for local files).

.material-symbols-outlined

Standard Material Symbols CSS class enabling ligatures, anti-aliasing, and proper rendering.

Base Reset & Body Styles

Universal box-sizing: border-box.
html — Sets Inter as the base font with advanced OpenType features (cv02, cv03, cv04, cv11).
body — Uses var(--body-bg) for background, var(--text-primary) for text, smooth transitions on theme change.
.dark body — Adds subtle radial gradient washes on top of the dark background.

.page-glow

What it does: A fixed-position overlay that creates a mouse-following radial gradient glow effect.
Notable logic: Uses CSS custom properties --mx and --my (set by JavaScript mouse tracking) for the gradient position. Only active in dark mode (:root:not(.dark) .page-glow { display: none; }).
The ::before pseudo-element adds ambient mesh drift animation (meshDrift keyframe, 30s cycle).

Card System (.card, .card-hero, .card-accounts, .card-credit, .card-insight, .card-forecast, .card-upcoming)

What it does: A comprehensive glassmorphism card system with multiple semantic variants:
.card — Base card with glass background, border, shadow, hover effects, and mouse-tracking glow via --card-mx/--card-my custom properties.
.card-hero — Steel blue themed card for net worth display with stronger glow effects.
.card-accounts — Green-themed card for account listings.
.card-credit — Amber-themed card for credit card information.
.card-insight — Compact card for AI insights with left-border accent indicator.
.card-forecast — Blue-accented card for cash flow forecasts.
.card-upcoming — Amber-accented card for upcoming bills.
Notable logic:
Each card uses ::before for mouse-tracking radial glow (opacity transitions on hover).
Each card uses ::after for a glass shine overlay.
will-change: transform, box-shadow and transform: translateZ(0) are used to promote cards to their own compositing layer for smooth animations.
-webkit-backface-visibility: hidden prevents rendering artifacts during transforms.
Light mode cards use white radial glows; dark mode uses blue-tinted glows.

.card-interactive

Adds cursor pointer and press-down scale effect (.card-interactive:active { transform: scale(0.995) }).

Sankey Theater (.sankey-theater)

What it does: The dark recessed container for the Sankey flow diagram.
Notable logic: In light mode, this is a "dark island" — a dark-background element sitting on the light page. Uses var(--island-bg-gradient) in light mode and the standard var(--sankey-theater-bg) in dark mode. Has a luminous top-edge highlight (::after) and ambient wash overlays (::before using var(--sankey-ambient)). Includes a mouse-tracking glow overlay (.sankey-theater-glow).

Sankey Category Pills (.sankey-cat-pill, .sankey-pill-card)

What it does: Interactive filter pills for Sankey categories with luminous glow effects.
Notable logic: Uses color-mix(in srgb, currentColor X%, transparent) for dynamic color mixing based on the pill's text color. Active pills get stronger glow; dimmed pills get reduced opacity and grayscale.

Metric Ribbon (.metric-ribbon)

What it does: A horizontal strip of financial metrics (income, expenses, net, savings rate) with glass background and dividers.
Notable logic: The .metric-derived-first element gets a special accent-colored divider via ::after.

Period Selector (.period-selector, .period-toggle-track)

What it does: Two variants of period selection UI:
Simple pill buttons (.period-btn).
iOS-style toggle with sliding thumb (.period-toggle-track + .period-toggle-thumb).
Notable logic: The iOS toggle uses CSS Grid for layout and a transform: translateX(calc(var(--active-idx, 0) * 100%)) translation driven by a CSS custom property for the thumb position.

Month Dropdown (.month-dropdown-*)

What it does: A custom dropdown for month selection that opens upward.
Notable logic: Uses opaque background (not backdrop-filter) to prevent nested-blur compositing bugs when the dropdown overlaps other glassmorphism cards. Has custom scrollbar styling and a slide-in animation.

Delta Badges (.delta-up, .delta-down)

Financial change indicators with colored backgrounds and borders.
Special brighter variants inside .card-hero-unified for visibility on dark island surfaces.

Upcoming Bills (.upcoming-row, .due-badge-*)

Row layout for upcoming bills with urgency-based badge colors (urgent/soon/normal/overdue).
Overdue badge has a pulsing animation.

Budget Progress (.budget-progress-bar, .budget-progress-fill)

Horizontal progress bar for budget tracking with animated width transitions and a pulsing animation for over-budget state.

Animations

fadeIn, fadeInUp — Entry animations.
pulse-dot — Breathing dot animation.
sankey-flow — Stroke dash animation for Sankey paths.
shimmer-surface / shimmer — Skeleton loading shimmer.
glow-pulse — Glow filter animation.
Skeleton loading components (.skeleton-hero-number, .skeleton-chart-block, .skeleton-sankey, .skeleton-bar-chart) with dark-mode-aware shimmer bands.

Scrollbar Styling

6px thin scrollbars with theme-aware colors.

Theme Switching (.theme-switching)

What it does: Suppresses all backdrop-filter, transitions, and animations during theme switching to prevent jank.
Notable logic: When .theme-switching is added to the root element, all elements have their backdrop-filter, transition-duration, transition-delay, animation-duration, and animation-delay forced to 0 via !important. This prevents expensive recompositing of glassmorphism effects during the theme variable swap.

Profile Transition (.profile-transition, .profile-loading)

Smooth opacity + blur transition when switching between user profiles.

Privacy Toggle (.privacy-toggle-btn)

Button to toggle privacy mode (masks financial values).

Teller Connect Dark Mode Fix

Forces dark styling on the Teller bank connection iframe in dark mode using filter: invert(0.9) hue-rotate(180deg).

Glass Rail Variables

CSS custom properties for the navigation rail sidebar (--rail-width: 260px, --rail-transition).

Data Flow

This CSS file doesn't process data in the traditional sense. It receives theme state from:


The .dark class on <html> (set by JS).
CSS custom properties from theme-light.css / theme-dark.css.
Mouse position custom properties (--mx, --my, --card-mx, --card-my, --theater-mx, --theater-my) set by JavaScript event handlers.

Integration Points

Imported at the app root by SvelteKit's layout.
All Svelte components use these classes.
Theme variables come from theme-light.css and theme-dark.css.
JavaScript sets mouse-tracking custom properties for glow effects.
The theme store (stores.js) controls the .dark and .theme-switching classes.

Known Quirks / Design Notes

The "dark stage islands" concept (light mode pages with dark cards) creates significant CSS complexity, with many :root:not(.dark) overrides.
color-mix(in srgb, ...) is used extensively — requires modern browser support (Chrome 111+, Firefox 113+, Safari 16.4+).
The font-display: block for Material Symbols (hiding text until font loads) is only acceptable because the font is self-hosted and loads in <10ms. For a CDN-hosted font, this would cause unacceptable layout shifts.
Multiple mojibake characters appear in comment headers (e.g., Ã¢Ã¢), suggesting the source file was saved with UTF-8 encoding but some decorative Unicode characters (box-drawing or arrow characters) were corrupted during transfer.
The .sankey-theater alone has ~100 lines of CSS, reflecting the visual complexity of the dark-island glassmorphism approach.


frontend_src_theme-light_css.txt

Purpose

Defines all CSS custom properties (design tokens) for the light theme of the Folio application. This is the "Cool Slate Design System v11 — Dark Stage Islands Edition." It establishes the complete visual vocabulary: color palettes, surface colors, glass/frost effects, borders, text colors, semantic colors, flow diagram colors, card styling, sidebar/topbar/rail theming, Sankey theater theming, dark island styling, typography, timing functions, and chart-specific tokens. Everything is scoped to :root (the default state, since .dark is only added explicitly).


Key Dependencies

None (pure CSS custom properties).


Core Functions / Classes / Exports

Steel Blue Palette (--steel-50 through --steel-950)

A 10-stop grayscale-blue palette ranging from #f0f4f8 (nearly white) to #0a1929 (near black).

Foundation Colors

--bg-base: #F5F5F5 — Main page background (neutral gray, not blue-tinted).
--bg-card: #FFFFFF — Card backgrounds.
--bg-recessed: #EFEFEF — Recessed/inset surfaces.
--bg-hero: #1E293B — Hero card background (dark slate blue — used for the dark island).
--mesh-canvas: none — Mesh gradient canvas is disabled for a clean white look.

Accent System

--accent: #2563EB — Primary blue (Tailwind blue-600).
--accent-hover: #1D4ED8 — Darker hover state.
--accent-soft / --accent-glow / --accent-border — Transparent tints for backgrounds, glows, and borders.

Surface Tokens

--body-bg: #FAFAFA — Slightly off-white body background.
--surface through --surface-elevated — Various opacity levels of white/gray for layered surfaces.
Design note: Comments indicate these were "CHANGED: removed blue tints" — the v11 design moved from blue-tinted whites to neutral whites.

Glass Tokens

--glass-blur: 16px / --glass-blur-heavy: 40px — Backdrop blur levels.
--glass-bg through --glass-bg-subtle — Semi-transparent white backgrounds.
--glass-shine — A diagonal gradient for glass highlight effects.

Frost Tokens (Hero Cards)

Higher opacity backgrounds and stronger shadows for premium frost-glass effect on hero-level cards.
--frost-blur: 40px — Heavier blur than standard glass.
--frost-shadow / --frost-shadow-hover — Multi-layer shadows with inset highlights.

Border System

--card-border: rgba(0, 0, 0, 0.08) — Subtle neutral borders.
--card-border-hover: rgba(37, 99, 235, 0.25) — Blue accent on hover.

Text Colors

--text-primary: #1A1F2B — Near-black for headlines.
--text-secondary: #475569 — Medium gray for body text.
--text-muted: #8C95A6 — Light gray for labels and metadata.

Semantic Colors

--positive: #059669 / --negative: #E11D48 / --warning: #D97706 — Green/red/amber with matching *-light background tints.

Flow Colors (Sankey Diagram)

Luminous colors designed for visibility on the dark island surface:
--flow-income: #2EE8A0 (bright green)
--flow-expenses: #38BDF8 (sky blue)
--flow-savings: #22D3EE (cyan)
--flow-transfer: #A78BFA (lavender)
--flow-default: #94A3B8 (slate)
--flow-from-balance: #FBBF24 (amber)
--flow-tax: #FBBF24 (amber)
--flow-rose: #FB7185 (pink)
Each has a matching *-glow variant for shadow effects.

Card System

--card-bg: #FFFFFF — Clean white.
--card-shadow — Triple-layer shadow with inset white highlight.
--card-radius: 20px — Generous rounded corners.

Sidebar, Rail, Topbar

Comprehensive theming for navigation elements with neutral white glass backgrounds.
Rail has a dedicated copilot section with violet tints.

Dark Stage Islands

What it does: Defines the styling for dark-background elements (hero card, Sankey theater) that sit on the light page.
--island-bg-gradient — Deep slate gradient (rgba(27, 31, 42, 0.96) to rgba(18, 21, 30, 0.98)).
--island-text-primary: #F1F5F9 — Light text for dark islands.
--island-border: rgba(56, 189, 248, 0.12) — Subtle cyan luminous border.
--island-glow: none — No glow in resting state (only on hover).
--island-glow-hover — Multi-layer glow with cyan accents on hover.
Island semantic colors are brighter versions for dark surfaces (e.g., --island-positive: #34D399 vs. --positive: #059669).

Sankey Theater Tokens

Maps to island variables for consistent dark-island styling.
--sankey-ambient — Multi-radial-gradient ambient wash inside the theater.
--sankey-label-primary: #F1F5F9 — Light labels for dark theater.

Category Pill Card

--pill-card-bg: rgba(255, 255, 255, 0.92) — Light surface (not a dark island).
--pill-glow: none / --pill-glow-active: none — No glow effects in light mode.

Typography & Transitions

--font-sans: 'Inter' / --font-display: 'Inter' / --font-mono: 'DM Mono'.
--ease-out-expo: cubic-bezier(0.16, 1, 0.3, 1) — Aggressive ease-out for snappy animations.
Duration tokens: --duration-fast: 150ms, --duration-normal: 280ms, --duration-slow: 500ms.

Data Flow

These variables are consumed by app.css and all component stylesheets. They change instantly when the .dark class is added/removed from <html>.


Integration Points

Imported by app.css.
Overridden by theme-dark.css when .dark class is present.
Referenced throughout all CSS files via var().

Known Quirks / Design Notes

Comments like "CHANGED: neutralized" and "CHANGED: removed blue tints" indicate this is an iteration of the design system that moved toward cleaner whites.
The "Dark Stage Islands" concept is architecturally unusual — it requires maintaining two sets of semantic colors (normal for the light page, island-specific for the dark cards).
color-scheme: light is set, telling the browser to use light-mode form controls and scrollbars.
--mesh-canvas: none explicitly disables what was likely a gradient mesh background in a previous version.


frontend_src_theme-dark_css.txt

Purpose

Defines all CSS custom properties for the dark theme, scoped to the .dark selector. This overrides the light theme defaults with a deep, cool-toned dark palette. The dark theme uses a consistent dark slate base (#141618) with blue-tinted glass effects and luminous accent glows.


Key Dependencies

None (pure CSS custom properties).


Core Functions / Classes / Exports

Dark Foundation

--body-bg: #141618 — Very dark, nearly black with slight warmth.
Three background levels: --bg-level-1: #1A1C20, --bg-level-2: #202328, --bg-level-3: #272A30 — for layered surface elevation.

Frost Token Remapping

Notable logic: Instead of setting frost tokens to unset (which causes inheritance issues), they're remapped to the dark-mode glass equivalents. For example, --frost-bg: var(--glass-bg) maps the frost background to the dark glass background.
--frost-saturate: 1.0 — No saturation boost in dark mode (light mode uses 1.2).

Accent

--accent: #5a9fd4 — Softer blue than light mode's #2563EB, better for dark backgrounds.
--accent-hover: #6db3e8 — Lighter on hover (reversed from light mode which goes darker).

Surfaces

Semi-transparent dark surfaces with blue tinting: rgba(26, 34, 48, 0.85) etc.

Glass Tokens

--glass-blur: 20px / --glass-blur-heavy: 30px — Slightly less blur than light mode (40px).
--glass-bg: rgba(26, 28, 32, 0.70) — Dark semi-transparent glass.
--glass-shine — Subtle blue-tinted shine gradient.

Borders

--card-border: rgba(82, 100, 124, 0.40) — More visible than light mode's 0.08 opacity.
--card-border-hover: rgba(90, 159, 212, 0.32) — Blue glow on hover.

Text

--text-primary: #F1F5F9 — Near-white for headlines.
--text-secondary: #8B9BB5 — Muted blue-gray for body.
--text-muted: #546378 — Darker still for labels.

Semantic Colors

Brighter, more saturated versions for visibility on dark backgrounds:
--positive: #34d399 (vs. light's #059669).
--negative: #f87171 (vs. light's #E11D48).
--warning: #fbbf24 (vs. light's #D97706).

Flow Colors (Sankey)

Similar to light mode but with slightly adjusted glow intensities.

Card System

--card-bg: rgba(26, 28, 32, 0.70) — Semi-transparent dark cards.
--card-shadow — Stronger, darker shadows than light mode.

Sidebar & Rail

--sidebar-bg: rgba(16, 17, 20, 0.96) — Nearly opaque dark sidebar.
Rail has a more complex shadow system with inset cyan glows.
Rail separator glow uses cyan (rgba(56, 189, 248, 0.12)).
Copilot section uses violet tints (rgba(139, 92, 246, ...)).

Sankey Theater

--sankey-theater-bg — Triple-stop dark gradient.
--sankey-ambient — Multi-radial-gradient ambient washes with category-coded colors.
--theater-glow-border — Luminous cyan border glow (0 0 0 1px rgba(56, 189, 248, 0.18), 0 0 20px rgba(56, 189, 248, 0.12), 0 0 45px rgba(90, 159, 212, 0.07)).

Category Pill Card

--pill-bg: rgba(255, 255, 255, 0.05) — Very subtle white tint.
--pill-glow: 0 0 8px rgba(56, 189, 248, 0.06) — Subtle ambient glow.
--pill-glow-active: 0 0 14px currentColor — Active pills glow in their own color.

Waterfall

--wf-bar-glow: 0 0 12px rgba(56, 189, 248, 0.08) — Cyan glow on waterfall bars.

Data Flow

Same as light theme — consumed by all CSS files via var(). Active when .dark is on <html>.


Integration Points

Imported by app.css.
All variables defined here override the :root (light) defaults when .dark is present.

Known Quirks / Design Notes

color-scheme: dark tells the browser to use dark-mode form controls and scrollbars.
The comment mentions "v10" while the light theme says "v11", suggesting the dark theme may be slightly behind in design iterations.
The frost token remapping strategy (mapping to glass equivalents instead of unset) is a deliberate workaround for CSS inheritance issues — documented in comments.


frontend_src_lib_stores_js.txt

Purpose

The central Svelte store module for the Folio frontend. It defines reactive stores for dark mode, filters, sync state, global data caches, dashboard preferences, shared period selection, and privacy mode. The dark mode store includes a sophisticated theme-switching mechanism with blur suppression to prevent rendering jank.


Key Dependencies

svelte/store — writable for creating reactive stores.
$app/environment — browser flag for SSR-safe code.

Core Functions / Classes / Exports

darkMode (custom store)

What it does: Manages the application's light/dark theme state with DOM synchronization.
Notable logic (the performSwitch function):
Adds .theme-switching class to <html> — this CSS class (app.css) forces backdrop-filter: none !important and transition-duration: 0s !important on all elements.
Forces a synchronous style recalc via void root.offsetHeight.
Toggles the .dark class and saves to localStorage.
Waits two requestAnimationFrame callbacks (one for recalc, one for paint) before removing .theme-switching.
Why: Glassmorphism uses heavy backdrop-filter: blur() which is GPU-expensive to recomposite. If the browser tried to recomposite blur effects while simultaneously changing all CSS custom properties, it would cause visible jank. The .theme-switching class eliminates blur during the transition, then re-enables it once the new theme is painted.
Initialization: Reads the DOM state (document.documentElement.classList.contains('dark')) rather than localStorage, because the inline script in app.html has already applied the correct class. This prevents any mismatch between the store and the DOM.

filters (writable store)

What it does: Stores filter state for the transactions page: { month, category, account, search }.
Default: All null/empty — no filters applied.

syncing (writable store)

What it does: Boolean flag indicating whether a bank data sync is in progress.

summaryData / accountsData (writable stores)

What it does: Global caches for summary and accounts data, preventing re-fetches across page navigations.

dashboardPrefs (writable store)

What it does: Stores user preferences for which dashboard sections are expanded: { showForecast, showUpcoming }.

selectedPeriodStore / selectedCustomMonthStore (writable stores)

What it does: Persists the selected time period and custom month across page navigations. Default period is 'this_month'.

privacyMode (custom store)

What it does: Toggles privacy mode which masks all financial values with $••••••.
Notable logic: Persists to localStorage and reads initial state from there. The toggle() method flips the boolean and saves.

Data Flow

Theme state: app.html inline script → DOM → darkMode store (reads DOM) → Components (subscribe) → performSwitch (writes DOM + localStorage).
Privacy mode: localStorage → privacyMode store → Components (subscribe) → utils.js formatCurrency() checks the store.
Filters: Components (write) → filters store → Transactions page (subscribes and refetches).
Period selection: Dashboard components (write) → selectedPeriodStore → Dashboard/Analytics pages (subscribe).

Integration Points

darkMode is used by the theme toggle button in the navigation rail.
filters is used by the transactions page for search/filter state.
syncing is set by the sync button and read by loading indicators.
summaryData / accountsData are populated by page loaders and read by components.
privacyMode is read by utils.js's formatCurrency() function.
selectedPeriodStore / selectedCustomMonthStore are read/written by dashboard and analytics components.

Known Quirks / Design Notes

The double-requestAnimationFrame trick in performSwitch is a common pattern for ensuring a style change is fully painted before triggering another change.
The theme store's initialization strategy (reading DOM instead of localStorage) is explicitly designed to prevent flash-of-wrong-theme and is documented in comments.
The void root.offsetHeight idiom forces a synchronous layout calculation, which is normally a performance anti-pattern but is intentionally used here to batch the style changes.


frontend_src_lib_stores_profileStore_js.txt

Purpose

Manages the multi-profile system for the Folio application. Supports switching between individual user profiles (e.g., "Karthik", "Sarah") and a "Household" aggregate view. The active profile determines which bank accounts and transactions are shown, and the profileParam derived store provides the query parameter value used by the API client.


Key Dependencies

svelte/store — writable, derived.
$app/environment — browser flag.

Core Functions / Classes / Exports

profiles (writable store)

What it does: Stores the list of individual profile objects: [{ id: "karthik", name: "Karthik" }, ...].
Default: Empty array.

activeProfile (writable store)

What it does: Stores the currently selected profile ID string.
Default: 'household'.

profileParam (derived store)

What it does: Converts activeProfile to a query parameter value. 'household' maps to empty string (no filter = show all data), any other value passes through as-is.
Notable logic: $ap === 'household' ? '' : $ap.

loadProfiles(fetchFn?) → void

What it does: Fetches the list of profiles from /api/profiles and populates the profiles store.
Inputs: Optional fetchFn (defaults to global fetch).
Notable logic:
Only runs on the client (if (!browser) return).
Manually injects X-API-Key header (reads from import.meta.env.VITE_API_KEY).
Normalizes the response: handles both string arrays and object arrays.
Filters out 'household' from the individual profiles list (household is a virtual aggregate, not a real Teller profile).
Sets activeProfile to 'household' after loading.
Comprehensive error handling with console warnings.

Data Flow

App initialization calls loadProfiles().
/api/profiles returns profile data.
Profiles are normalized and stored.
activeProfile defaults to 'household'.
When user clicks a profile pill, activeProfile is updated.
profileParam automatically derives the new query parameter.
API client (api.js) reads profileParam and appends it to requests.

Integration Points

loadProfiles() is called during app initialization (likely in root +layout.svelte).
profileParam is read by api.js's appendProfileParam() function.
activeProfile is read/written by ProfileSwitcher.svelte.
Various components subscribe to activeProfile for profile-aware data fetching.

Known Quirks / Design Notes

The household concept is a frontend abstraction — the backend returns all data when no profile parameter is sent.
The normalization logic handles both string[] and {id, name}[] responses, suggesting the API format may have evolved.
Profile IDs are lowercased; names are title-cased during normalization.


frontend_src_lib_api_js.txt

Purpose

The API client module for the Folio frontend. It provides a factory function (createApi) that returns a fully-typed API client object with methods for every backend endpoint. Features include: lazy API key loading (to survive Vite HMR), an in-memory response cache with 2-minute TTL, automatic profile parameter injection, and separate handling for read vs. write operations.


Key Dependencies

svelte/store — get function to read store values synchronously.
$lib/stores/profileStore.js — profileParam derived store.

Core Functions / Classes / Exports

getApiKey() → string

What it does: Lazily reads the API key from import.meta.env.VITE_API_KEY.
Notable logic: Reads fresh on every request (not cached at module load time) because during Vite HMR, import.meta.env may not be populated during the first module evaluation. This is documented as [FIX F2].

Cache System (CACHE_TTL, _cache, getCached, setCache, invalidateCache)

What it does: A Map-based cache with 2-minute TTL for GET requests.
getCached(key) — Returns cached data if fresh, deletes and returns undefined if stale.
setCache(key, data) — Stores data with timestamp.
invalidateCache() — Clears the entire cache (called after sync, profile switch, or copilot writes).

PROFILE_EXEMPT_ENDPOINTS (Set)

Endpoints that never get a profile parameter: /sync and /profiles.

isProfileExempt(endpoint) → boolean

Checks if an endpoint path (before query string) is in the exempt set.

appendProfileParam(endpoint, method) → string

What it does: Appends ?profile=<id> (or &profile=<id>) to the endpoint URL based on the active profile.
Notable logic:
Skips exempt endpoints.
Skips mutation methods (PATCH/POST/PUT/DELETE) except for /copilot and /subscriptions.
Returns endpoint unchanged if profile is 'household' (empty string = no filter).
Wraps get(profileParam) in try/catch for SSR safety.

createRequest(fetchFn?) → request(endpoint, options) → Promise<data>

What it does: Creates a bound request function that handles: profile injection, caching (GET only), API key header injection, error handling, and JSON parsing.
Notable logic:
Cache key is based on the profiled endpoint (includes ?profile=X), so profile switching naturally cache-misses.
Only GET requests are cached.
On non-OK responses, attempts to parse error JSON and throws with err.detail message.

createApi(fetchFn?) → ApiClient

What it does: Returns an object with methods for every API endpoint:
getAccounts() — GET /accounts
getTransactions(params) — GET /transactions with query params (month, category, account, search, limit, offset).
updateCategory(txId, category) — PATCH /transactions/:id/category
getCategories() — GET /categories
createCategory(name) — POST /categories
getCategoryRules(source) — GET /category-rules
getMonthlyAnalytics() — GET /analytics/monthly
getCategoryAnalytics(month) — GET /analytics/categories
getSummary() — GET /summary
getProfiles() — GET /profiles
sync() — POST /sync
askCopilot(question, profile) — POST /copilot/ask
confirmCopilotWrite(question, confirmationId, profile) — POST /copilot/confirm (sends confirmation_id instead of raw SQL — security fix [FIX F1]).
getRecentTransactions(months) — Fetches transactions for the last N months in parallel.
getNetWorthSeries(interval) — GET /analytics/net-worth-series
getDashboardBundle(nwInterval) — GET /dashboard-bundle
getMerchants(month) — GET /merchants
getRecurring() — GET /analytics/recurring
updateExpenseType(categoryName, expenseType) — PATCH /categories/:name/expense-type
confirmSubscription(merchant, pattern, frequencyHint, category) — POST /subscriptions/confirm
dismissSubscription(merchant, pattern) — POST /subscriptions/dismiss
getTellerConfig() — GET /teller-config
enrollAccount(accessToken, institutionName, enrollmentId) — POST /enroll

api (default instance)

A pre-created API client using window.fetch for client-side component usage.

Data Flow

Component calls api.getSummary().
createRequest builds the profiled endpoint (e.g., /summary?profile=karthik).
Checks cache — returns cached data if fresh.
If not cached, makes fetch() call with API key header.
Parses JSON response, caches it, returns to component.
For mutations, cache is not used; invalidateCache() is called manually where needed.

Integration Points

createApi(fetch) is used in +page.js load functions with SvelteKit's fetch (for SSR/prerendering compatibility).
api (default instance) is used in Svelte components for client-side requests.
invalidateCache() is called after sync operations, copilot writes, and profile switches.
profileParam store drives the profile query parameter injection.

Known Quirks / Design Notes

The lazy getApiKey() pattern is explicitly documented as a fix for Vite HMR timing issues ([FIX F2]).
The copilot confirmation flow uses a confirmation_id rather than sending SQL back to the server — this is a security fix ([FIX F1]) that prevents SQL injection via the client. The server stores the validated SQL and returns a nonce; the client sends only the nonce to confirm execution.
The cache TTL of 2 minutes is a pragmatic choice for a personal finance app where data doesn't change frequently.
getRecentTransactions fires parallel requests for N months and flattens the results — a clever approach to work around the API's per-month pagination.
Profile-exempt endpoints are a small, hardcoded set. Adding new global endpoints requires updating this set.


frontend_src_lib_utils_js.txt

Purpose

A comprehensive utility module providing formatting functions, date helpers, financial calculations, and data transformation utilities used throughout the Folio frontend. Includes currency/date formatting (with privacy mode support), greeting generation, transaction grouping, recurring transaction detection, cash flow forecasting, category color/icon mappings, and a custom Svelte action for spring-animated number counting.


Key Dependencies

$lib/stores.js — privacyMode store.
svelte/store — get for synchronous store reads.

Core Functions / Classes / Exports

formatCurrency(value, decimals?) → string

What it does: Formats a number as US currency (e.g., $1,234.56).
Notable logic: Checks privacyMode store first — if active, returns '$••••••' instead of the actual value.

formatCompact(value) → string

What it does: Formats large numbers compactly (e.g., $1.2K, $45.6K).
Notable logic: Uses Intl.NumberFormat with notation: 'compact' for values ≥ $1,000. Also checks privacy mode.

formatDate(dateStr) → string

Formats "2024-03-15" → "Mar 15, 2024".
Notable logic: Appends 'T00:00:00' to prevent timezone-related date shifting.

formatDateShort(dateStr) → string

Formats "2024-03-15" → "Mar 15" (no year).

formatDayHeader(dateStr) → string

Returns "Today", "Yesterday", or "Monday, Mar 15" for transaction grouping headers.

formatMonth(monthStr) → string

Formats "2024-03" → "March 2024".

formatMonthShort(monthStr) → string

Formats "2024-03" → "Mar '24".

formatPercent(value) → string

Returns "12.3%" with one decimal place.

relativeTime(isoString) → string

Returns "just now", "5m ago", "2h ago", "3d ago".

getCurrentMonth() → string

Returns current month as "2024-03".

getPreviousMonth(monthStr) → string

Returns the month before the given month string.

groupTransactionsByDate(transactions) → [date, txns][]

Groups transactions by date and sorts groups newest-first.

computeDelta(current, previous) → number | null

Computes percentage change. Returns null if previous is 0.

getGreeting() → string

Returns time-appropriate greeting: "Good morning" (5-12), "Good afternoon" (12-17), "Good evening" (17-21), "Good night" (21-5).

computeTrailingSavingsRate(monthlyData, windowSize?) → { rate, delta, months }

What it does: Computes average savings rate over the last N months (default 3).
Notable logic: savings = max(income - expenses, 0), capped at 0-100%. Also computes delta by comparing current window to the previous window of the same size.

detectRecurring(transactions, limit?) → RecurringItem[]

What it does: Detects recurring transactions from a raw transaction list.
Notable logic:
Groups outflow transactions by normalized description.
Computes intervals between occurrences.
Considers a transaction "recurring" if: std dev < 35% of avg interval, avg interval between 14-62 days.
Estimates next date by adding avg interval to last occurrence.
Only includes items where next date is within -7 to +45 days of today.
Returns sorted by next date, limited to limit items (default 5).

buildCashFlowForecast(currentBalance, dailyAvgSpend, dailyAvgIncome, days?) → ForecastItem[]

What it does: Projects future balance for N days (default 14) using daily averages.
Returns: Array of { date, projected, day }.

CATEGORY_COLORS (constant)

Maps 18 category names to hex colors (e.g., 'Food & Dining': '#DC2626').

CATEGORY_ICONS (constant)

Maps 18 category names to Material Symbols icon names (e.g., 'Food & Dining': 'restaurant').

springCount(node, params) → SvelteAction

What it does: A Svelte action that animates a number from 0 to a target value with spring-like overshoot.
Notable logic: Custom easing function that overshoots to 106% at 70% progress, then settles back to 100%. Uses requestAnimationFrame for smooth animation over 1 second.
Usage: <span use:springCount={{ value: 1234, format: formatCurrency }}>$0</span>.

Data Flow

These are pure utility functions — they receive data as parameters and return formatted/computed results. The only side effect is formatCurrency/formatCompact reading the privacyMode store.


Integration Points

formatCurrency — Used everywhere financial values are displayed.
formatDate / formatDayHeader — Used by transaction lists.
computeTrailingSavingsRate — Used by the dashboard for the savings rate metric.
detectRecurring — Used by the upcoming bills section.
buildCashFlowForecast — Used by the forecast card.
CATEGORY_COLORS / CATEGORY_ICONS — Used by Sankey charts, category lists, and analytics components.
springCount — Used by the hero card's net worth display.

Known Quirks / Design Notes

The 'T00:00:00' suffix in date parsing functions is a critical fix — without it, new Date('2024-03-15') is parsed as UTC midnight, which in US timezones would display as the previous day.
The privacy mask '$••••••' uses Unicode bullet characters, not asterisks.
The recurring transaction detection algorithm (35% std dev threshold, 14-62 day range) is heuristic and may miss some patterns (e.g., quarterly bills at ~90 days, weekly charges at ~7 days).
The springCount action's overshoot factor of 1.06 is subtle enough to feel premium without being distracting.


frontend_src_routes_+page_js.txt

Purpose

The SvelteKit load function for the dashboard route (/). It implements a two-stage data loading strategy: first, it tries to consume the pre-fired fetch from app.html (which started loading before any JS bundles), and if that fails or times out, it falls back to a fresh API call. This ensures the dashboard data is available as fast as possible while being resilient to prefetch failures.


Key Dependencies

$lib/api.js — createApi for the fallback fetch.

Core Functions / Classes / Exports

export const ssr = false

What it does: Disables server-side rendering for this route. All rendering happens client-side. This is appropriate because the dashboard requires authenticated API calls that can only happen from the browser (with the API key).

withTimeout(promise, ms) → Promise

What it does: Wraps a promise with a timeout. If the promise doesn't resolve within ms milliseconds, it rejects with 'prefetch_timeout'.
Inputs: Any promise and a timeout in milliseconds.

load({ fetch }) → DashboardData

What it does: Loads the dashboard bundle data.
Outputs: { summary, accounts, monthly, categories, netWorthSeries }.
Notable logic:
Checks if window.__dashboardData exists (set by app.html's inline script).
If it exists, waits for it with a 4-second timeout.
If the prefetch fails or times out, sets bundle = null.
Clears window.__dashboardData to prevent reuse on client-side navigation.
If bundle is still null, dynamically imports createApi and makes a fresh getDashboardBundle('biweekly') call.
Returns the destructured bundle with safe fallbacks (empty objects/arrays).

Data Flow

app.html fires fetch('/api/dashboard-bundle') → stores promise on window.__dashboardData.
JS bundles load → SvelteKit calls load().
load() tries to consume the prefetch promise (4s timeout).
If successful, returns the data immediately.
If not, makes a fresh API call and returns that data.
SvelteKit passes the returned data as props to the page component.

Integration Points

Depends on app.html's inline prefetch script.
Falls back to $lib/api.js createApi(fetch).
The returned data is consumed by the dashboard page component (+page.svelte).

Known Quirks / Design Notes

The ssr = false is necessary because the app requires browser-side API keys.
The 4-second timeout for the prefetch is generous — in practice, the prefetch should resolve much faster since it starts loading before the JS bundle.
window.__dashboardData = null cleanup prevents stale data from being reused when navigating back to the dashboard.
The dynamic import of createApi in the fallback path avoids loading the API module if the prefetch succeeds — a micro-optimization.


frontend_src_routes_analytics_+page_js.txt

Purpose

The SvelteKit load function for the analytics route (/analytics). It fetches monthly analytics and category analytics data in parallel using Promise.all.


Key Dependencies

$lib/api.js — createApi.

Core Functions / Classes / Exports

load({ fetch, depends }) → AnalyticsData

What it does: Loads analytics data for the page.
Outputs: { monthly, categories }.
Notable logic:
depends('app:analytics') — Registers a dependency key that allows programmatic invalidation (e.g., invalidate('app:analytics') would re-run this load function).
Uses Promise.all to fetch both endpoints concurrently.

Data Flow

SvelteKit calls load() when navigating to /analytics.
Two parallel API calls fire: /analytics/monthly and /analytics/categories.
Both responses are returned as page data.

Integration Points

createApi(fetch) uses SvelteKit's fetch for proper cookie/header handling.
Returns data consumed by the analytics page component.
The depends('app:analytics') key can be invalidated from anywhere using SvelteKit's invalidate().

Known Quirks / Design Notes

Unlike the dashboard, there's no prefetch optimization. This page loads data on navigation.
No ssr = false export — though it likely still runs client-side due to the API key requirement.


frontend_src_lib_styles_dashboard_css.txt

Purpose

Route-specific CSS for the dashboard page (/). Defines the unified hero card (a three-zone merged layout), net worth chart styling, the trends/trajectory section, account rows within the hero card, year-over-year comparison elements, monthly net SVG chart, and trajectory chart styles. This file handles the most visually complex component in the application — the hero card with its dark-island glassmorphism.


Key Dependencies

CSS custom properties from theme-light.css / theme-dark.css (via app.css).

Core Functions / Classes / Exports

.card-hero-unified

What it does: The main hero card — a three-column grid layout (1.4fr 1fr 1fr) showing net worth, checking accounts, and credit accounts.
Notable logic:
In light mode: uses var(--island-bg-gradient) for the dark island effect with backdrop-filter: none.
In dark mode: uses a deep dark gradient with heavy backdrop blur and a cyan glow ring (0 0 0 1px rgba(56, 189, 248, 0.16), 0 0 25px rgba(56, 189, 248, 0.10), 0 0 60px rgba(56, 189, 248, 0.05), 0 0 100px rgba(90, 159, 212, 0.03)).
Mouse-tracking glow via ::before pseudo-element.
Glass shine via ::after pseudo-element.
Min height: 280px.
Responsive: stacks to single column on screens < 1024px.

Hero Zones (.hero-zone, .hero-zone-left, .hero-zone-center, .hero-zone-right)

What it does: The three columns of the hero card.
Notable logic: Glowing vertical separators between zones using ::before (1px luminous line) and ::after (blurred glow wash). In light mode, these use cyan tints (rgba(120, 220, 255, ...)). On mobile, separators become horizontal.

.hero-chart-bg

The sparkline chart background in the left zone. Uses filter: drop-shadow(0 0 24px rgba(56, 189, 248, 0.22)) for a luminous glow effect.

.hero-account-scroll / .hero-account-scroll-wrapper

What it does: Scrollable account list within hero zones, max height 168px (~3.5 rows).
Notable logic: Custom thin scrollbar with luminous cyan gradient thumb. Bottom fade gradient (::after) that only appears when content overflows (toggled by .has-overflow class, added by JavaScript).

.utilization-bar-track / .utilization-bar-fill

Mini credit utilization bar with gradient fill. .high variant adds a red gradient for high utilization.

.account-proportion-track / .account-proportion-fill

Mini bar showing each account's proportion of total balance. Min width 4px to ensure visibility.

Net Worth Chart (.nw-chart-container)

Notable logic: In light mode (dark island), the chart SVG gets a strong glow: filter: drop-shadow(0 0 16px rgba(56, 189, 248, 0.30)). In dark mode, even stronger: 0 0 20px rgba(56, 189, 248, 0.35).

Monthly Net Stats (.monthly-net-stats)

Three-column grid for income/expenses/net totals.

YoY Comparison (.yoy-year-row, .yoy-bars, .yoy-bar-*)

Year-over-year comparison bars with ghost (previous year) and fill (current year) overlays.

Monthly Net SVG Chart (.monthly-net-svg, .monthly-net-bar)

Full-width SVG bar chart, 380px height. Bars use a grow animation.

Period Loading (.period-updating)

Overlay shimmer effect when switching time periods.

Trajectory Chart (.trajectory-svg)

200px height SVG chart for balance trajectory.

Data Flow

This is pure CSS — it styles elements rendered by the dashboard Svelte components.


Integration Points

Used by the dashboard +page.svelte (not provided).
Relies on CSS custom properties from theme files.
JavaScript adds .has-overflow class to scroll wrappers.
Mouse coordinates are set as CSS custom properties by JavaScript event handlers.

Known Quirks / Design Notes

The responsive breakpoint at 1024px is the only breakpoint in this file, suggesting the app is primarily designed for desktop/laptop screens.
The scrollbar styling is extensive (40+ lines) for a component that's a few pixels wide — reflecting the premium visual standards.
The animation-delay staggering pattern is used for skeleton loading states.


frontend_src_lib_styles_transactions_css.txt

Purpose

Route-specific CSS for the transactions page (/transactions). Defines styles for categorization source badges, inline category editing controls, filter pill dropdowns, pagination buttons, and transaction row states (hover, update flash).


Key Dependencies

CSS custom properties from theme files.

Core Functions / Classes / Exports

Source Badges (.tx-source-badge, .tx-source-manual, .tx-source-auto-rule, .tx-source-rule, .tx-source-ai, .tx-source-fallback)

Color-coded badges showing how a transaction was categorized: manual (blue), auto-rule (violet), rule (teal), AI (amber), fallback (gray).

Edit Controls (.tx-edit-controls, .tx-cat-select, .tx-new-cat-input, .tx-edit-btn-*)

Inline editing UI for changing transaction categories. Includes a select dropdown, new category text input, and confirm/cancel/new buttons (22×22px rounded squares).

Transaction Row States (.tx-row-updated)

Flash animation when a transaction is updated: green background that fades over 2 seconds.

Feedback Toast (.tx-feedback-toast)

Slide-in toast notification for category update confirmation.

Filter Pills (.txn-filter-pill, .txn-filter-dropdown, .txn-filter-option)

Custom filter dropdowns for month, category, and account filtering. Matches the analytics month-picker design.
Notable logic: .txn-filter-dropdown has theme-aware backgrounds. The dark mode variant uses var(--bg-level-2) for an opaque background to avoid blur compositing issues.

Pagination (.pagination-btn, .pagination-btn-active, .pagination-btn-disabled)

Styled pagination buttons with monospace font for page numbers.

Month Dropdown Override

.txn-period-row .month-dropdown-menu overrides the shared month dropdown to open downward (instead of upward) and uses z-index: 99999 plus background: var(--bg-card) !important with backdrop-filter: none !important to prevent compositing issues.

Data Flow

Pure CSS — styles elements rendered by the transactions page components.


Integration Points

Used by the transactions +page.svelte.
Uses shared components from app.css (.card, .period-selector, etc.).

Known Quirks / Design Notes

The z-index: 99999 on the month dropdown is a brute-force fix for stacking context issues. The comment notes this was "Increased z-index" — suggesting an earlier, lower value wasn't sufficient.
The !important on the dropdown background and backdrop-filter is a specificity override needed because the shared .month-dropdown-menu in app.css may have conflicting styles.


frontend_src_lib_styles_analytics_css.txt

Purpose

Route-specific CSS for the analytics page (/analytics). Defines the spending pulse grid, waterfall theater (dark island), waterfall tooltips, fixed-vs-variable expense split, month-over-month data table, recurring subscriptions table, narrative section headers, financial health snapshot, savings rate animation, and the analytics hero summary strip.


Key Dependencies

CSS custom properties from theme files.
Shared styles from app.css (card system, glassmorphism).

Core Functions / Classes / Exports

Spending Pulse Grid (.analytics-pulse-grid, .analytics-pulse-card)

6-column grid of compact spending category cards. Responsive: 3 columns at 1280px, 2 at 640px.
Anomaly detection: .analytics-pulse-anomaly (red left border), .analytics-pulse-under (green left border).

Waterfall Theater (.analytics-waterfall-theater)

What it does: The dark island container for the waterfall chart — identical architecture to the Sankey theater.
Notable logic: Same light-mode dark island + dark-mode deep recessed stage treatment. Top-edge luminous highlight. Hover glow effects differ between light and dark modes.
Waterfall bars (.analytics-wf-bar) have brightness/glow hover effects, but with reduced intensity in light-mode dark islands to avoid over-brightness.

Waterfall Tooltip (.analytics-wf-tooltip)

What it does: A floating tooltip that appears above waterfall bars.
Notable logic:
Positioned via CSS transform: translate(-50%, calc(-100% - 8px)) (centered above target).
Visibility toggled via .wf-tooltip-visible class (opacity + translateY animation).
Has a downward caret (rotated pseudo-element).
Three theme variants: light mode (on dark island — uses dark tooltip), dark mode (uses dark tooltip), and a standard light tooltip (commented).

Fixed vs Variable (.analytics-fv-split, .analytics-fv-*)

Three-column grid (left panel | divider | right panel) for displaying fixed vs. variable expense categorization.
Includes toggle buttons for switching categories between fixed/variable and a feedback toast for confirmation.

Month-over-Month Glass Grid (.mom-glass-grid, .mom-glass-row)

What it does: A glass-effect data table for comparing spending across months.
Notable logic: Rows have a left-edge color tint (--row-tint custom property) that appears on hover. Header and footer separated by subtle borders.

Analytics Hero Strip (.analytics-hero-strip)

A horizontal summary bar with accent left border, showing key metrics alongside narrative text.

Financial Health Snapshot (.analytics-health-grid)

2-column grid of health metric cells (savings rate, debt ratio, etc.).

Recurring Subscriptions (.analytics-recurring-*)

Table rows with confirm/dismiss action buttons (opacity 0 by default, revealed on row hover).
Price change badges (.analytics-recurring-price-change.price-up/price-down).

Month Picker (.analytics-month-picker-*)

Custom dropdown identical in structure to the transactions filter pills.

Narrative Section Headers (.analytics-narrative-section)

Section dividers with title, icon, and subtitle. Consistent spacing via --analytics-section-gap: 2.5rem.

Savings Rate Animation

savingsRatePulse keyframe for the end-dot on savings rate charts.

Data Flow

Pure CSS — styles elements rendered by the analytics page components.


Integration Points

Used by the analytics +page.svelte.
Shares the dark-island architecture with app.css's Sankey theater styles.
Uses :global(.dark) selector for dark-mode overrides within Svelte component <style> scoping.

Known Quirks / Design Notes

The waterfall tooltip has three distinct theme variants (light-island, dark, and standard-light) which significantly increases CSS complexity.
The details > summary::-webkit-details-marker / ::marker reset removes the default triangle from <details> elements.
The :root:not(.dark) .analytics-waterfall-theater.card::before selector specifically fixes a white radial mouse-glow that would otherwise appear on the dark island surface — this is a very targeted override.


frontend_src_lib_styles_copilot_css.txt

Purpose

Route-specific CSS for the Copilot chat interface (/copilot). Defines styles for chat messages, operation badges, data tables, write confirmation cards, SQL code blocks, and interactive buttons.


Key Dependencies

CSS custom properties from theme files.

Core Functions / Classes / Exports

Message Container (.copilot-msg-container)

Flex column layout for assistant messages with 6px gaps between elements.

Operation Badges (.copilot-op-badge, .copilot-op-read/write/success/error)

Compact badges indicating the type of copilot operation: read (blue), write (amber), success (green), error (red).

Data Table (.copilot-data-table-wrap, .copilot-data-table)

Scrollable data table for query results. Max height 320px with sticky headers.
Monospace font (JetBrains Mono) for data. 250px max width per cell with text-overflow ellipsis.
Preview variant (.copilot-data-table-preview) has smaller font and padding for use in confirmation cards.

Write Confirmation (.copilot-confirm-card, .copilot-confirm-btn-*)

Amber-tinted card for reviewing write operations before confirmation.
Green confirm button with gradient and shadow. Gray cancel button.
Confirmed badge (.copilot-confirmed-badge) in green.

SQL Toggle & Block (.copilot-sql-toggle, .copilot-sql-block)

Toggle button to show/hide generated SQL.
Code block with monospace font, border, max height 200px.

Data Flow

Pure CSS — styles elements rendered by the copilot page component.


Integration Points

Used by frontend_src_routes_copilot_+page_svelte.txt.

Known Quirks / Design Notes

The data table uses white-space: nowrap on cells, which can cause horizontal overflow. The wrapper provides horizontal scrolling.
The SQL block uses word-break: break-word with white-space: pre-wrap for better readability of long SQL statements.


frontend_src_lib_styles_budget_css.txt

Purpose

A placeholder CSS file for the budget route (/budget). Currently contains only comments noting that the budget route uses shared components from app.css and that route-specific styles can be added here as the budget UI evolves.


Key Dependencies

None — the file is empty of actual styles.


Core Functions / Classes / Exports

None.


Data Flow

N/A.


Integration Points

Would be imported by the budget +page.svelte.

Known Quirks / Design Notes

The file's existence as a placeholder suggests the budget feature is either new or uses only shared styles. The shared .budget-progress-bar and .budget-progress-fill styles in app.css handle the current budget UI.


frontend_src_routes_copilot_+page_svelte.txt

Purpose

The Copilot page component — an AI-powered chat interface for querying and modifying financial data. It provides a conversational UI where users can ask questions about their finances (read operations) or give commands to modify data (write operations with confirmation). The component handles message management, chat scrolling, quick prompts, data table rendering, write confirmation flow, SQL visibility toggling, and loading states.


Key Dependencies

$lib/styles/copilot.css — Route-specific styles.
$lib/api.js — api (default client) and invalidateCache.
svelte — onMount, tick.
$lib/stores/profileStore.js — activeProfile.
$lib/utils.js — formatCurrency, formatDate.

Core Functions / Classes / Exports

State Variables

messages — Array of message objects with properties: role, content, operation, data, sql, confirmation_id, needs_confirmation, rows_affected, confirmed, original_question.
input — Current text input value.
loading — Boolean loading state.
chatContainer — DOM reference for scroll management.
showSqlForMsg — Object mapping message index to SQL visibility boolean.
quickPrompts — Array of 6 suggested questions/commands.

send() → void

What it does: Sends the user's message to the copilot API and displays the response.
Notable logic:
Adds user message to messages array.
Clears input and sets loading.
Calls api.askCopilot(question, profile).
Adds assistant response with all metadata (operation type, data, SQL, confirmation_id, etc.).
On error, adds an error message.
Auto-scrolls to bottom after each update.

confirmWrite(msgIndex) → void

What it does: Confirms and executes a write operation.
Notable logic:
Reads confirmation_id from the message (server-side stored SQL nonce — security fix [FIX F1]).
Calls api.confirmCopilotWrite(question, confirmationId, profile).
Updates the original message to show confirmed state.
Adds execution result message.
Calls invalidateCache() since data has changed.

cancelWrite(msgIndex) → void

Marks the message as not needing confirmation and adds a "cancelled" message.

toggleSql(msgIndex) → void

Toggles SQL visibility for a specific message.

usePrompt(prompt) → void

Sets input to a quick prompt and calls send().

formatTableValue(key, value) → string

What it does: Smart formatting for data table cells.
Notable logic: Heuristically detects currency values by checking if the column name contains words like amount, total, balance, sum, avg, spent, income, expense, net, owed, assets. Detects dates by regex pattern ^\d{4}-\d{2}-\d{2}.

getColumns(data) → string[]

Extracts column headers from the first row of a data array.

Data Flow

User types a question or clicks a quick prompt.
send() adds the user message to the UI.
api.askCopilot() sends the question to the backend.
Backend returns: { answer, operation, data, sql, confirmation_id, needs_confirmation, rows_affected }.
For read operations: response displayed with optional data table.
For write previews: confirmation card shown with preview data.
User confirms → confirmCopilotWrite() → backend executes → result displayed.

Integration Points

API: api.askCopilot() and api.confirmCopilotWrite().
Stores: $activeProfile for profile context.
Utils: formatCurrency, formatDate for table cell formatting.
Cache: invalidateCache() after write operations.

Known Quirks / Design Notes

The component initializes with a welcome message that explains its capabilities.
Quick prompts are only shown when the conversation has ≤1 message (the welcome message).
The Enter key sends the message; Shift+Enter allows multiline input.
Data tables are limited to 20 rows with a "showing X of Y" indicator.
Preview tables in confirmation cards are limited to 5 rows.
The tick() + scrollToBottom() pattern ensures the DOM is updated before scrolling.
The confirmation_id flow is a security best practice — the client never sees or transmits SQL, only a server-generated nonce.


frontend_src_lib_components_ProfileSwitcher_svelte.txt

Purpose

A UI component that renders profile selection pills. Shows a "Household" button (combined view of all accounts) and individual profile buttons. The active profile is visually highlighted with an accent color.


Key Dependencies

$lib/stores/profileStore — profiles, activeProfile.

Core Functions / Classes / Exports

select(id) → void

Sets $activeProfile to the clicked profile ID.

Template

Conditionally rendered (only if $profiles.length > 0).
"Household" button with a groups icon.
Individual profile buttons with person icons, generated from $profiles array.
Active state styled with accent color background/border.

Scoped Styles

.profile-switcher-pills — Flex row with 6px gaps.
.profile-pill — Rounded pill (border-radius: 9999px) with 12px text.
.profile-pill--active — Accent-colored background with glow ring using color-mix().

Data Flow

profiles store provides the list of profiles.
User clicks a pill → select(id) updates activeProfile.
All components subscribing to activeProfile or profileParam react to the change.

Integration Points

Placed in the app's navigation area (likely in the topbar or sidebar).
Triggers data refetches across the app via the profileParam store → API client's appendProfileParam().

Known Quirks / Design Notes

The color-mix(in srgb, var(--accent) X%, transparent) usage requires modern browser support.
The component is self-contained with scoped styles — no external CSS import needed.
The .profile-pill--active:hover state is slightly different from the resting active state, providing visual feedback even on already-selected items.


System-Level Summary

The Folio frontend is a meticulously crafted SvelteKit application that functions as a premium personal finance dashboard. The architecture follows a clear separation of concerns: SvelteKit handles routing and server-side coordination, Svelte components manage UI state and rendering, a centralized API client handles all backend communication, reactive stores manage global state, utility functions provide data formatting and computation, and an extensive CSS design system delivers a glassmorphism visual language with both light and dark themes.


Data Flow End-to-End: When a user loads the dashboard, the HTML template immediately fires a prefetch request for the dashboard bundle while SvelteKit's JavaScript loads in parallel. The +page.js load function consumes this prefetched data (with a 4-second timeout fallback), providing the fastest possible initial render. All subsequent API calls flow through the centralized api.js client, which automatically injects the active profile as a query parameter, caches GET responses for 2 minutes, and includes the API key in every request. Profile switching (via the ProfileSwitcher component) updates a Svelte store, which the API client reads synchronously on each request, causing natural cache misses and fresh data loads. The Copilot chat interface uses a confirmation-based write flow where the server stores validated SQL and returns a nonce, which the client sends back to confirm execution — a security-conscious pattern that prevents SQL injection.


Visual Architecture: The design system is built around the concept of "Dark Stage Islands" — in light mode, premium cards (the hero net worth card, the Sankey flow diagram theater, the waterfall chart) render as dark-background elements floating on a clean white page. This creates visual hierarchy and a sense of depth. The glassmorphism card system uses CSS custom properties for every visual attribute (backgrounds, borders, shadows, glows), enabling seamless theme switching between light and dark modes. A sophisticated blur-suppression mechanism temporarily disables all backdrop-filter effects during theme transitions to prevent GPU recompositing jank. Mouse-tracking glow effects on cards and the page background use CSS custom properties set by JavaScript event handlers, creating an ambient, responsive visual experience.


Key Architectural Decisions: (1) The hybrid prefetch strategy (inline HTML script + SvelteKit load function with timeout) optimizes for perceived performance while maintaining resilience. (2) The 2-minute client-side cache in the API layer reduces redundant requests during normal browsing without risk of stale data in a personal finance context. (3) The profile system is a frontend abstraction — the backend simply filters by profile ID, and "household" is implemented as "no filter." (4) The CSS architecture uses a three-layer approach: Tailwind utilities for layout, CSS custom properties for theming, and hand-crafted CSS classes for the glassmorphism design system. (5) Security is prioritized in the Copilot flow through the confirmation_id nonce pattern, the lazy API key loading for HMR compatibility, and the npm min-release-age policy.


Potential Areas for Improvement: The dark-island concept, while visually striking, creates significant CSS maintenance burden with many :root:not(.dark) overrides. The recurring transaction detection algorithm in utils.js uses hardcoded thresholds (35% std dev, 14-62 day range) that may miss edge cases. The z-index: 99999 on dropdown menus is a symptom of stacking context complexity that could be addressed with a more systematic z-index scale. The client-side cache could benefit from per-endpoint TTL configuration rather than a global 2-minute window. Finally, the absence of TypeScript means the API response shapes are not statically verified, relying on runtime handling and fallback defaults.
