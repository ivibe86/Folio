Folio – Frontend Documentation


frontend_src_routes_+layout_svelte.txt

Purpose

This is the root SvelteKit layout component that wraps every page in the Folio application. It provides the persistent navigation sidebar ("Glass Rail"), mobile navigation drawer, theme toggling (dark/light mode), sync functionality, mouse-tracking glow effects, and the main content slot. Every route rendered in the app is a child of this layout, making it the single source of truth for global UI chrome, navigation state, and cross-cutting visual effects.


Key Dependencies

Import	Why
'../app.css'	Global CSS custom properties and base styles
$app/stores → page	Reactive access to the current route URL for active-link highlighting
$app/navigation → beforeNavigate, afterNavigate	Suppress backdrop-filter glitches during SvelteKit client-side navigation
$lib/stores.js → darkMode, syncing	Global theme toggle store and sync-in-progress flag
$lib/api.js → api	Backend API client for triggering data sync and fetching last-synced timestamp
$lib/utils.js → relativeTime	Converts an ISO timestamp into "2 min ago"-style label for the footer status bar
svelte → onMount	Lifecycle hook for initial profile load, summary fetch, and mouse-move listener registration
$lib/stores/profileStore.js → loadProfiles	Fetches the list of financial profiles (household, individual) on app boot

Core Functions / Classes / Exports

handleSync() → void

What it does: Triggers a full data sync with the backend (pulls latest transactions from Teller, re-categorizes, recomputes aggregates).
Inputs: None; reads $syncing store.
Outputs / Side effects: Sets $syncing = true, calls api.sync(), updates lastSynced timestamp, reloads profiles, then sets $syncing = false.
Notable logic: Uses a try/catch/finally pattern so the spinner always stops even on error.

handleMouseMove(e) → void

What it does: Updates CSS custom properties --mx and --my on <html> for the page-level glow effect.
Inputs: Native MouseEvent.
Outputs / Side effects: Sets CSS variables via document.documentElement.style.setProperty.
Notable logic: Uses requestAnimationFrame with a guard (glowRafId) to throttle updates to one per frame — prevents layout thrashing.

handleCardMouseMove(e) → void

What it does: Updates per-card --card-mx / --card-my CSS variables for individual card glow effects, plus --rail-mx / --rail-my for the sidebar's mouse-tracking glow.
Inputs: Native MouseEvent.
Outputs / Side effects: Queries all .card, .card-hero, .metric-ribbon, etc. elements and sets per-element CSS variables based on mouse position relative to each card's bounding rect. Also tracks the .glass-rail sidebar element.
Notable logic: Also throttled via requestAnimationFrame. Uses querySelectorAll with a broad CSS selector string — this is O(n) over all cards on every mouse move frame, which could become a performance concern with many cards.

handleKeyboard(e) → void

What it does: Global keyboard shortcut handler. Pressing t (when not in an input) toggles dark mode.
Inputs: KeyboardEvent.
Outputs / Side effects: Calls darkMode.toggle().
Notable logic: Ignores events when focus is on INPUT, TEXTAREA, or SELECT elements.

isActive(path, current) → boolean

What it does: Route matching for navigation link highlighting.
Inputs: path — the nav item's href; current — the current URL pathname.
Outputs: true if the nav item should show as active.
Notable logic: Root / requires an exact match; all other paths use startsWith for nested route matching.

Data Flow

On mount, the layout loads profiles via loadProfiles() and fetches the summary timestamp with a 100ms delay (setTimeout). The sidebar renders navigation links for Dashboard, Transactions, Analytics, Budgets, and Copilot. Theme state ($darkMode) and sync state ($syncing) flow from Svelte stores into the template. Mouse position is captured at the window level and distributed to individual card elements and the rail via CSS custom properties. Navigation events temporarily disable backdrop-filter to prevent visual glitches during page transitions.


Integration Points

Calls: api.sync(), api.getSummary(), loadProfiles()
Called by: SvelteKit runtime (automatic layout wrapping for all routes)
Stores consumed: $page, $darkMode, $syncing
Stores written: $syncing
Child routes: Rendered into the <slot /> inside <main>

Known Quirks / Design Notes

handleCardMouseMove querySelectorAll performance: The function queries a long CSS selector on every animation frame. With many cards this could be expensive. A future optimization could cache the NodeList or use a MutationObserver.
Navigation backdrop-filter suppression: The beforeNavigate / afterNavigate hooks add/remove a theme-switching class with a double-requestAnimationFrame delay. This is a workaround for a browser rendering bug where backdrop-filter causes flickers during route changes.
Mobile nav is conditionally rendered ({#if mobileMenuOpen}) — it's fully destroyed/recreated on toggle rather than hidden with CSS. This is fine for mobile perf but means the mobile nav's internal state resets on every open.
Brand images use two separate image files (folio-mark-dark-mode.png / folio-mark-light-mode.png) toggled via {#if $darkMode}.
Rail width is set via CSS variable --rail-w: 260px inline and --rail-width in the main content margin. These must be kept in sync.
Copilot nav item has special purple-tinted styling with its own active bar color, AI badge, and hover treatment — it's visually differentiated from standard nav links.
Glowing separators (.rail-glow-separator) use cyan glow in dark mode (rgba(56, 189, 248, ...)) matching the app's accent language.
params export is declared but unused — it exists to satisfy SvelteKit's prop contract for layout components.


frontend_src_routes_+page_svelte.txt

Purpose

This is the Dashboard — the app's primary landing page. It renders the unified hero card (net worth + accounts + credit cards), a compact metric ribbon (income, expenses, net flow, savings rate, daily spending pace), an interactive Sankey money flow chart, monthly net surplus/deficit bar chart, year-over-year comparison, and net worth trajectory. It is the most complex page in the application, orchestrating animated number count-ups, SVG chart generation, period selection, drill-down into Sankey categories, privacy mode, profile switching, and Teller Connect enrollment.


Key Dependencies

Import	Why
'$lib/styles/dashboard.css'	Dashboard-specific CSS (cards, hero, metric ribbon, chart styling)
svelte → onMount, tick	Lifecycle and DOM update flushing
$lib/api.js → api	All backend data fetching
$lib/utils.js	Formatting (currency, dates, percentages), category color/icon maps, springCount, computeTrailingSavingsRate, getGreeting, groupTransactionsByDate, getCurrentMonth, computeDelta
$lib/stores.js → darkMode, syncing, selectedPeriodStore, selectedCustomMonthStore, privacyMode	Global reactive state
$lib/stores/profileStore.js → activeProfile	Current financial profile
SankeyChart component	The D3-powered Sankey diagram
ProfileSwitcher component	Profile pill selector
TellerConnect component	Bank enrollment button

Core Functions / Classes / Exports

bootstrapInitialPeriod() → void

What it does: Seeds the Sankey chart and metric ribbon from data already loaded by +page.js (the load() function), avoiding extra API calls on first paint.
Inputs: Reads summary, categories, monthly from data.
Outputs / Side effects: Sets periodSummary, sankeyCategoryList, sankeySavingsTotal, sankeyPersonalTransferTotal.
Notable logic: For single-month periods, kicks off a background refresh (updatePeriodBackground) so the first paint is immediate with approximate data, then precise data replaces it ~200ms later.

updatePeriod() → void

What it does: Fetches fresh Sankey/metric data when the user changes the period selector.
Inputs: Reads selectedPeriod, monthly, summary.
Outputs / Side effects: Fetches category analytics and transfer totals, rebuilds sankeyCategoryList and periodSummary, then re-triggers animation.
Notable logic: Handles all five period types: this_month, last_month, ytd, all, custom. For YTD, it fetches and aggregates multiple months. Keeps stale data visible during fetch (no null flash).

animateNumbers() → void

What it does: Performs a spring-eased count-up animation for net worth, income, and expenses.
Inputs: Reads periodSummary, netWorth.
Outputs / Side effects: Updates animatedNetWorth, animatedIncome, animatedExpenses on each frame.
Notable logic: Uses a custom springEase function with 6% overshoot and a staggered 150ms delay for the metric ribbon vs. net worth. Net worth only animates on first page load (netWorthAnimationDone flag). Cancels any in-flight animation before starting a new one.

buildNWAreaChart(data, width, height) → Object

What it does: Generates SVG path strings for the net worth area chart using Catmull-Rom spline interpolation.
Inputs: data — array of { month, value } points; width/height — SVG viewBox dimensions.
Outputs: { linePath, areaPath, endPoint, labels, gridLines, monthLabels }.
Notable logic: Uses a custom catmullRomChain function with tension=0.5 and 16 samples per segment for smooth curves. Quarterly x-axis labels are auto-detected. Y-axis range includes 15% padding below and 8% above the data range.

buildSankeyCategoryList(cats, savingsTotal, personalTransferTotal) → Array

What it does: Merges expense categories with savings/personal transfer totals into a single sorted list for the Sankey chart and category pills.
Inputs: Category analytics array, transfer totals.
Outputs: Array of { category, total, percent, isDirectFlow } objects sorted by total descending.

buildPeriodSummary(income, cats, savingsTotal, personalTransferTotal) → Object

What it does: Computes the metric ribbon values for a given period.
Outputs: { income, expenses, savings, savingsTransfer, personalTransfer, net_flow, savings_rate }.
Notable logic: Expenses exclude "Savings Transfer" and "Personal Transfer" categories (these are direct flows). Savings rate is sanitized to [0, 100].

handleSankeySelect(event) → void

What it does: Handles drill-down when a user clicks a Sankey category node.
Inputs: Custom event with detail = category name or null.
Outputs / Side effects: Fetches transactions for the selected category and period, populates sankeyDrillTxns, scrolls to the drill-down section.
Notable logic: For YTD, fetches transactions across all year-to-date months in parallel.

fetchBundleWithRetry(retries, delayMs) → Object|null

What it does: Retry wrapper for the initial dashboard bundle fetch, handling a race condition where Vite's env injection isn't complete yet.
Notable logic: Exponential backoff (delay × attempt number), validates the response has real data before accepting.

handleTellerEnrolled(event) → void

What it does: Reloads the entire dashboard after a successful Teller Connect bank enrollment.
Notable logic: Full bundle re-fetch, animation reset, account delta recomputation.

dashboardMetrics (reactive block)

What it does: A single consolidated reactive computation that derives all dashboard metrics in one pass: totalCash, totalOwed, netWorth, account lists, month-over-month deltas, savings rate, daily spending pace, top category insight.
Notable logic: Uses void privacyKey as a reactive dependency to force recomputation when privacy mode toggles. This is a performance optimization — computing everything in one $: block ensures Svelte batches the update instead of cascading through 6+ individual reactive statements.

Data Flow

+page.js load() provides data.summary, data.accounts, data.monthly, data.categories, data.netWorthSeries.
onMount validates the data (retrying if empty due to 401 race), computes account deltas, bootstraps the initial period, and starts animations.
Period changes trigger updatePeriod() which fetches fresh data from the API.
Profile changes trigger reloadDashboardForProfile() which re-fetches the entire bundle.
The Sankey chart receives computed props; drill-down clicks fetch transaction details.
All monetary display values pass through formatCurrency() which respects $privacyMode.

Integration Points

Calls: api.getDashboardBundle(), api.getSummary(), api.getCategoryAnalytics(), api.getTransactions(), api.getTellerConfig()
Components used: SankeyChart, ProfileSwitcher, TellerConnect
Stores consumed: $darkMode, $syncing, $privacyMode, $activeProfile, selectedPeriodStore, selectedCustomMonthStore
Stores written: selectedPeriodStore, selectedCustomMonthStore

Known Quirks / Design Notes

DIRECT_FLOW_CATEGORIES is hardcoded as ['Savings Transfer', 'Personal Transfer']. These are excluded from expense totals because they represent money moving between accounts, not spending.
Net worth chart colors are the same in both light and dark mode because the hero card uses a "dark island" design — a dark background even in light mode.
initialLoadComplete guard: The $: reactive block for period changes only fires after initialLoadComplete is true, preventing premature fetches during initial hydration.
profileSwitching flag: Prevents the period-change reactive block from firing during a profile switch, which would cause duplicate fetches.
Privacy mode ($privacyMode) replaces monetary values with $••• via formatCurrency. The void privacyKey pattern is used in reactive blocks and even SVG text elements to force re-rendering.
Scroll overflow detection (checkScrollOverflow action): A Svelte action that uses ResizeObserver to detect when account lists overflow their container and adds a CSS class for fade-out gradients.
Monthly net SVG bar chart is computed reactively but uses a prevMonthlyRef guard to avoid unnecessary recomputation — only rebuilds when the monthly array reference changes.
YoY comparison uses "same period" comparison (Jan–current month) for fairness, not full year vs. partial year.
NW chart memoization uses a custom cache key based on array length, first/last values, and dimensions to avoid expensive Catmull-Rom computation on every reactive update.


frontend_src_lib_components_TellerConnect_svelte.txt

Purpose

A standalone UI component that renders a "+" button to initiate bank account enrollment via the Teller Connect drop-in widget. It dynamically loads the Teller Connect SDK script, opens the enrollment flow on click, and sends the resulting access token to the backend's /api/enroll endpoint. This is how users link new bank accounts to Folio.


Key Dependencies

Import	Why
svelte → createEventDispatcher, onMount	Component events and lifecycle
$lib/api.js → api	Calls api.enrollAccount() to send the access token to the backend

Core Functions / Classes / Exports

Props

applicationId: string — Teller application ID (fetched from backend config)
environment: string — 'sandbox', 'development', or 'production'

openTellerConnect() → void

What it does: Initializes and opens the Teller Connect modal.
Inputs: Reads applicationId, environment from props; sdkReady from local state.
Outputs / Side effects: On success, calls api.enrollAccount() with the access token, institution name, and enrollment ID. Dispatches enrolled event with the result. On failure/exit, dispatches error or exit events.
Notable logic: Requests three products: transactions, balance, identity. Error handling covers SDK not loaded, missing app ID, and enrollment failure.

Data Flow

Parent (Dashboard) passes applicationId and environment as props.
On mount, the component dynamically injects the Teller Connect SDK script tag.
User clicks the "+" button → openTellerConnect() → Teller modal opens.
User completes bank auth → onSuccess callback → api.enrollAccount() → backend stores credentials.
Component dispatches enrolled event → Dashboard reloads all data.

Integration Points

Called by: Dashboard (+page.svelte) conditionally renders this component if tellerAppId is available.
Calls: api.enrollAccount(accessToken, institutionName, enrollmentId)
Events dispatched: enrolled, error, exit

Known Quirks / Design Notes

Dynamic script loading: The SDK script is loaded via DOM manipulation in onMount, not via a static import. This avoids bundling the third-party SDK.
Button disabled states: Disabled when enrolling (POST in progress) or !sdkReady (SDK not yet loaded).
Error display: A small error banner appears below the button on failure.
No cleanup: The SDK script tag is never removed from the DOM, even if the component is destroyed.


frontend_src_routes_copilot_+page_svelte.txt

Purpose

The Copilot page provides an AI-powered chat interface for querying and modifying financial data. Users can ask natural language questions ("How much did I spend on groceries last month?") or issue commands ("Recategorize all Uber transactions as Transportation"). The backend generates SQL from natural language, and write operations require explicit user confirmation before execution.


Key Dependencies

Import	Why
'$lib/styles/copilot.css'	Copilot-specific chat UI styles
$lib/api.js → api, invalidateCache	api.askCopilot() for queries, api.confirmCopilotWrite() for confirmed writes, invalidateCache() after data mutations
svelte → onMount, tick	Lifecycle and DOM flushing for scroll-to-bottom
$lib/stores/profileStore.js → activeProfile	Sends current profile to backend for profile-scoped queries
$lib/utils.js → formatCurrency, formatDate	Table cell formatting

Core Functions / Classes / Exports

Message Schema

Each message in the messages array has:


role: 'user' | 'assistant'
content: Main text
operation: 'read' | 'write_preview' | 'write_executed' | 'error' | null
data: Array of row objects for tabular results
sql: Generated SQL (hidden by default, togglable)
needs_confirmation: Boolean for write previews
confirmation_id: Server-side ID for confirmed writes (security measure — raw SQL never sent back)
rows_affected: Number of modified rows

send() → void

What it does: Sends the user's input to the backend Copilot endpoint and appends the response to the chat.
Inputs: Reads input (text), $activeProfile.
Outputs / Side effects: Appends user message and assistant response to messages, scrolls to bottom.
Notable logic: Catches errors gracefully with a user-friendly fallback message.

confirmWrite(msgIndex) → void

What it does: Confirms and executes a pending write operation.
Inputs: Index of the message containing the write preview.
Outputs / Side effects: Calls api.confirmCopilotWrite() with the confirmation_id (NOT raw SQL), updates the message as confirmed, appends a result message, invalidates cache.
Notable logic: The confirmation_id pattern is a security fix — the server stores the validated SQL and only executes it when the client sends back the ID, preventing SQL injection via client manipulation.

cancelWrite(msgIndex) → void

What it does: Cancels a pending write operation.
Outputs / Side effects: Clears the confirmation state and appends a "cancelled" message.

formatTableValue(key, value) → string

What it does: Intelligent formatting for data table cells based on column name heuristics.
Notable logic: Currency-like columns (amount, total, balance, sum, avg, spent, income, expense, net, owed, assets) are formatted as currency. ISO date strings are formatted as human-readable dates.

Quick Prompts

Six predefined prompts displayed when the chat is empty: spending queries, comparisons, and recategorization commands.


Data Flow

User types a question or clicks a quick prompt → send() → api.askCopilot(question, profile).
Backend processes the question: classifies as read/write, generates SQL, executes (reads) or returns preview (writes).
Response rendered as a chat bubble with optional data table, SQL toggle, and confirmation buttons.
For writes: user clicks Confirm → confirmWrite() → api.confirmCopilotWrite(question, confirmationId, profile) → backend executes stored SQL → cache invalidated.

Integration Points

Calls: api.askCopilot(), api.confirmCopilotWrite(), invalidateCache()
Stores consumed: $activeProfile
Layout: Rendered inside the main layout's <slot />

Known Quirks / Design Notes

confirmation_id security pattern: Write operations never send raw SQL back to the server. The server stores the SQL keyed by a unique ID, and the client only sends the ID to confirm. This prevents prompt-injection attacks where a user could modify the SQL in transit.
Data table limit: Only first 20 rows shown (with a count indicator); write previews show first 5.
SQL visibility: Hidden by default behind a "Show SQL" toggle per message — useful for debugging but not for most users.
No message persistence: Chat history is lost on page navigation. There's no backend storage of conversation history.
Profile-scoped: All queries include the active profile, so individual users see only their data.


frontend_src_routes_analytics_+page_svelte.txt

Purpose

The Analytics page provides deep spending insights including: a hero summary headline, cash flow waterfall chart, fixed vs. variable expense split (with user-editable classification), top merchants, recurring subscription detection, spending pulse anomaly cards, savings rate trend, financial health snapshot, month-over-month diff table, and actionable spending nudges. It is the most analytically rich page in the app.


Key Dependencies

Import	Why
'$lib/styles/analytics.css'	Analytics-specific CSS
$lib/api.js → api, invalidateCache	Data fetching and cache invalidation on expense type changes
$lib/stores.js → darkMode, selectedPeriodStore, selectedCustomMonthStore	Theme and cross-page period sync
$lib/stores/profileStore.js → activeProfile	Profile-scoped data
$lib/utils.js	All formatting utilities, CATEGORY_COLORS, CATEGORY_ICONS
ProfileSwitcher component	Profile pill selector

Core Functions / Classes / Exports

loadMonthData() → void

What it does: Fetches category analytics, transactions, merchants, and previous month data for the selected month.
Inputs: selectedMonth.
Outputs / Side effects: Populates monthCategories, monthTransactions, topMerchants, prevMonthData, prevMonthCategories.
Notable logic: Fetches previous month data in parallel for month-over-month comparisons.

spendingPulseCards (reactive)

What it does: Computes anomaly detection cards for each spending category.
Notable logic: Compares current month spending to historical average. Uses a periodic spending detector: if the naive ratio (current/average) exceeds 4× and the category appears in fewer than 40% of months, it's classified as periodic/seasonal (e.g., annual insurance). For periodic categories, comparison uses only "active months" instead of the full history. Anomaly threshold is 50% for periodic, 25% for regular categories. Results sorted with anomalies first.

waterfallData / waterfallGeometry (reactive)

What it does: Generates the data model and SVG geometry for the cash flow waterfall chart.
Notable logic: Starts at "Opening" (zero), adds income, then subtracts each expense category, savings transfers, and personal transfers. Each bar shows its running total. Bridge connectors (dashed lines) link consecutive bars. Y-scale auto-adjusts to data range with 12% headroom.

fixedVsVariable (reactive)

What it does: Splits expenses into fixed (recurring) and variable (discretionary) based on the expense_type field from the backend database.
Notable logic: Classification is now DB-driven (not frontend heuristic). Users can toggle a category's classification via toggleExpenseType(), which calls api.updateExpenseType() and updates local state immediately. Historical averages computed from allCategories for delta comparison.

savingsRateTrend / savingsRateGeometry (reactive)

What it does: Computes monthly savings rates and a 3-month rolling average for the SVG trend chart.
Notable logic: Savings rate = max((income - expenses) / income * 100, 0), capped at 100%. Target line drawn at 25%.

projectedYearEnd (reactive)

What it does: Projects year-end financial outcome based on 3-month rolling average of net income.
Outputs: Projected total, optimistic (+20%), pessimistic (-20%), projected savings rate.

incomeStability (reactive)

What it does: Computes income consistency metrics using coefficient of variation (CV).
Notable logic: CV < 10% = "Very Stable", 10-20% = "Stable", 20-30% = "Moderate", >30% = "Volatile". Also tracks consecutive months with income as a "streak".

actionableNudge (reactive)

What it does: Generates a "what if" scenario: if the user reduced over-budget categories to their averages, how much would they save?
Outputs: Potential monthly savings, annualized savings, impact on savings rate.

toggleExpenseType(categoryName, newType) → void

What it does: Updates a category's fixed/variable classification in the backend database.
Inputs: Category name, new type ('fixed' or 'variable').
Outputs / Side effects: API call to api.updateExpenseType(), local state update, cache invalidation.
Notable logic: Updates both monthCategories and allCategories immediately for instant UI feedback.

handleConfirmSubscription(item) / handleDismissSubscription(item)

What it does: Confirms or dismisses a detected recurring subscription.
Notable logic: Dismiss removes the item from local state immediately and recalculates totals without a full refetch.

Data Flow

+page.js load() provides data.monthly and data.categories.
onMount fetches recurring data, determines initial month (honoring cross-page period sync), loads month-specific data.
Month/period changes trigger loadMonthData().
All charts are computed reactively from the fetched data.
Profile changes trigger reloadAnalyticsForProfile().

Integration Points

Calls: api.getCategoryAnalytics(), api.getTransactions(), api.getMerchants(), api.getRecurring(), api.updateExpenseType(), api.confirmSubscription(), api.dismissSubscription(), api.getMonthlyAnalytics()
Stores consumed: $darkMode, $activeProfile, selectedPeriodStore, selectedCustomMonthStore
Components used: ProfileSwitcher

Known Quirks / Design Notes

Periodic spending detection is a clever heuristic but imperfect — it estimates "active months" as round(allTimeTotal / currentTotal), which can be wrong if amounts vary significantly.
Cached history map (_historyMapCache) is an explicit memoization to avoid re-scanning allCategories on every selected-month change.
Waterfall tooltip uses SVG coordinate transformation (createSVGPoint / getScreenCTM) for accurate mouse position mapping.
Fixed vs. Variable toggle uses a two-button inline editor with immediate API call — no save/cancel workflow needed.
Recurring price changes are detected by the backend and displayed as small badges with trending_up/trending_down icons.
handleWindowClick closes all open dropdowns and drill-downs — a global click-away handler.


frontend_src_routes_transactions_+page_svelte.txt

Purpose

The Transactions page is a paginated, filterable, searchable list of all financial transactions. Users can filter by period, month, category, and account. Each transaction's category can be re-assigned (with a dropdown or by creating a new category), and the categorization source (Manual, Auto-rule, AI, Fallback) is displayed as a badge. Changes propagate to the backend and invalidate the API cache.


Key Dependencies

Import	Why
'$lib/styles/transactions.css'	Transaction page-specific CSS
$lib/api.js → api, invalidateCache	Transaction CRUD, category management
$lib/stores/profileStore.js → activeProfile	Profile-scoped data
$lib/utils.js	Formatting, groupTransactionsByDate, CATEGORY_COLORS, CATEGORY_ICONS
ProfileSwitcher component	Profile pill selector

Core Functions / Classes / Exports

fetchTransactions() → void

What it does: Fetches a page of transactions from the backend with current filter parameters.
Inputs: Reads filterMonth, filterCategory, filterAccount, search, pageLimit, pageOffset.
Outputs / Side effects: Populates transactions, totalCount.
Notable logic: For YTD, fetches with a high limit (1000) and filters client-side by year. This is an approximation — true server-side YTD filtering would be more scalable.

handlePeriodChange(key) → void

What it does: Maps period selector buttons to filterMonth values.
Notable logic: Maps 'ytd' to the special sentinel value '__ytd__' which triggers client-side year filtering.

updateCategory(txId, newCategory) → void

What it does: Re-categorizes a transaction.
Inputs: Transaction ID, new category name.
Outputs / Side effects: API call, local state update (sets confidence: 'manual', categorization_source: 'user'), cache invalidation, categories list refresh, feedback toast.
Notable logic: Shows feedback: "Categorized as X — future similar transactions will auto-categorize" — indicating the backend creates categorization rules from manual corrections.

createAndApplyCategory(txId) → void

What it does: Creates a new category (if it doesn't already exist) and applies it.
Notable logic: Case-insensitive duplicate check before creation.

getSourceLabel(tx) → Object|null

What it does: Maps the backend's categorization_source field to a human-readable label and CSS class.
Outputs: { label: 'Manual'|'Auto-rule'|'Rule'|'AI'|'Fallback', type: string } or null.

Data Flow

onMount fetches first page of transactions and category list. Also fetches up to 1000 transactions for month/account filter dropdown metadata.
Filter changes trigger reactive fetchTransactions() via a filter key change detector.
Search input is debounced at 300ms.
Category edits call api.updateCategory() → backend updates DB and creates rules → cache invalidated.
Profile changes trigger reloadTransactionsForProfile().

Integration Points

Calls: api.getTransactions(), api.getCategories(), api.updateCategory(), api.createCategory(), invalidateCache()
Stores consumed: $activeProfile
Components used: ProfileSwitcher

Known Quirks / Design Notes

Metadata fetch hack: On mount, fetches up to 1000 transactions just to extract unique months and account names for filter dropdowns. This is wasteful — a dedicated metadata endpoint would be better.
YTD filtering uses a client-side approach with filterMonth = '__ytd__' sentinel. Pagination doesn't work correctly for YTD since it fetches all with limit 1000.
Debounced search uses setTimeout — clearing the previous timeout on each keystroke. The $: reactive block triggers this.
_prevFilterKey pattern: Prevents re-fetching when the reactive block runs but filters haven't actually changed.
Transaction grouping by date uses groupTransactionsByDate() from utils, creating day headers with aggregated daily spend/income.
New category creation flow: inline text input → create category API → apply to transaction. Error handling for duplicate names.


frontend_src_lib_components_SankeyChart_svelte.txt

Purpose

A custom Sankey diagram component built on d3-sankey that visualizes money flow from income sources through expense categories. It features a "Balance-as-Reservoir" model (income shortfalls draw from balance, surpluses go to balance), luminous glow effects, animated flow pulses, interactive node/link selection with drill-down, and theme-aware styling for both light and dark modes.


Key Dependencies

Import	Why
d3-sankey → sankey, sankeyJustify	D3's Sankey layout algorithm
$lib/utils.js → formatCurrency, formatCompact, CATEGORY_COLORS	Value formatting and category colors
$lib/stores.js → privacyMode	Privacy-aware value display

Core Functions / Classes / Exports

Props

Prop	Type	Description
income	number	Total income for the period
expenses	number	Total expenses (excluding direct flows)
savingsTransfer	number	Total savings transfer outflow
personalTransfer	number	Total personal transfer outflow
categories	array	Expense category breakdown
selectedCategory	string	Currently selected category for highlighting
height	number	Minimum SVG height
autoHeight	boolean	Whether to auto-size based on node count

buildGraph(inc, exp, savTotal, ptTotal, cats) → Object|null

What it does: Constructs the node and link arrays for the Sankey layout.
Notable logic (Balance-as-Reservoir model):
Shortfall = max(totalOutflow - realIncome, 0) → creates a "From Balance" source node.
Surplus = max(realIncome - totalOutflow, 0) → creates a "To Balance" sink node.
Income is allocated to expenses first, then savings, then personal transfers, then balance surplus.
The layout is always balanced: left-side total equals right-side total.
realValues map stores actual (non-layout) values for display — e.g., the income node shows real income even if it's less than total outflow.

layoutSankey(data) → void

What it does: Runs the d3-sankey layout algorithm on the graph data.
Notable logic: Auto-height calculation based on visible node count with minimum 28px per node + 24px padding. Maximum height capped at 650px.

linkPath(link) → string

What it does: Generates cubic Bézier SVG path for each flow link.
Notable logic: Custom curve lift calculation — expense branch links get more curve lift based on their index; income/balance links get lift proportional to ribbon width (8% of width, minimum 4px) to prevent straight-line flows that look visually flat.

getLinkOpacity(link) / getLinkStrokeWidth(link) / getNodeOpacity(node)

What it does: Compute visual properties based on selection and hover state.
Notable logic: Selected category flows get opacity 0.75; the trunk link (income→expenses) dims to 0.15 when an expense category is selected; unrelated flows dim to 0.06. Minimum stroke width of 4px ensures thin flows are visible.

SVG Rendering Layers (in order)

Link glow underlayer — blurred copies of links for bloom effect
Main link ribbons — gradient-filled paths with per-link gradients
Flow pulse overlay — animated sweep effect on initial render
Node halos — soft outer glow rectangles
Node rings — thin stroke borders
Node bodies — solid rectangles with rounded corners
Node highlight lines — subtle white top-edge highlights
Pulse dots — animated dots at leaf node endpoints
Labels — node name + compact value

Data Flow

Parent passes income, expenses, savingsTransfer, personalTransfer, categories as props.
Reactive block detects data changes via a memoization key → calls buildGraph() → layoutSankey().
SVG renders nodes and links with computed visual properties.
User interactions (click, hover) modify selectedCategory and hoveredLink → dispatch select event to parent.

Integration Points

Used by: Dashboard (+page.svelte) inside the "Money Flow" section
Events dispatched: select (with category name or null)
Stores consumed: $privacyMode

Known Quirks / Design Notes

Memoized graph data: The prevGraphKey pattern prevents expensive rebuilds when only visual props change (theme, selection).
sanitizeId() replaces non-alphanumeric characters for SVG gradient IDs — necessary because category names may contain spaces or special characters.
Theme detection: Uses a MutationObserver on document.documentElement to detect class changes (dark mode toggle) and update isDarkMode. This is separate from the Svelte $darkMode store because SVG filters need imperative updates.
Performance: Per-link gradient definitions are created in SVG <defs> — with 10+ categories, this creates many gradients. The glow underlayer uses a CSS filter: blur() on the parent <g> instead of per-element SVG filters to reduce filter passes.
Light mode "dark island": The Sankey chart uses dark-theater styling even in light mode (luminous flows on dark background). The tooltip styling has explicit overrides for this case.
realValues vs layout values: The Sankey layout requires balanced flows (input = output), but display values show the actual amounts. getNodeDisplayValue() and getLinkDisplayValue() return real values for labels/tooltips.


frontend_src_routes_budget_+page_svelte.txt

Purpose

The Budgets page allows users to set monthly spending limits per category and track progress. Budget values are stored in localStorage (keyed by profile), not in the backend database. The page displays overall utilization, per-category progress bars with status badges (OK, 80%+, OVER), and a tip card.


Key Dependencies

Import	Why
'$lib/styles/budget.css'	Budget page-specific CSS
$lib/api.js → api	Fetches monthly and category analytics
$lib/stores/profileStore.js → activeProfile	Profile-scoped localStorage key
$lib/utils.js → formatCurrency, formatPercent, formatMonth, CATEGORY_COLORS, CATEGORY_ICONS	Formatting and visual mappings
ProfileSwitcher component	Profile pill selector

Core Functions / Classes / Exports

saveBudget(category, value) → void

What it does: Saves or removes a budget limit for a category.
Inputs: Category name, budget value string.
Outputs / Side effects: Updates budgets object, persists to localStorage.
Notable logic: Removes the entry if value is NaN, zero, or negative.

budgetItems (reactive)

What it does: Computes per-category budget status by joining monthCategories with budgets.
Outputs: Array of { ...cat, budget, spent, remaining, budgetPercent, status } where status is 'unset', 'good', 'warning', or 'over'.

startEdit(cat, currentBudget) / commitEdit(cat)

What it does: Inline editing workflow for budget values.

Data Flow

onMount loads budgets from localStorage, fetches monthly + category analytics.
Month selection triggers loadMonth() → fetches month-specific category analytics.
Budget edits are saved to localStorage immediately.
Profile changes trigger reloadBudgetsForProfile() which loads the new profile's budgets from localStorage and re-fetches analytics.

Integration Points

Calls: api.getMonthlyAnalytics(), api.getCategoryAnalytics()
Stores consumed: $activeProfile
Components used: ProfileSwitcher

Known Quirks / Design Notes

LocalStorage-only budgets: Budget data is not persisted to the backend. This means budgets don't sync across devices. The storage key is finflow_budgets_{profileId}.
No budget history: There's no tracking of budget changes over time or comparison of budget vs. actual across months.
Status thresholds: >100% = "OVER" (red), >80% = "WARNING" (yellow), otherwise "OK" (green). These are hardcoded.
Month selector uses a simple <select> element rather than the iOS-style period toggle used on other pages.


frontend_src_lib_components_ProfileSwitcher_svelte.txt

Purpose

A compact pill-button component that allows switching between financial profiles (e.g., "Household" combined view, individual family member views). The "Household" button is always first and acts as the default combined view.


Key Dependencies

Import	Why
$lib/stores/profileStore → profiles, activeProfile	Profile list and active selection store

Core Functions / Classes / Exports

select(id) → void

What it does: Sets the active profile store to the selected ID.
Inputs: Profile ID string.
Outputs / Side effects: Writes to $activeProfile store, which triggers reactive reloads across all pages.

Data Flow

Reads $profiles (array of { id, name }) and $activeProfile (string) from stores.
Renders a "Household" button (always present, hardcoded with groups icon) plus one button per profile.
Click → select(id) → store update → all pages reactively reload their data.

Integration Points

Used by: Dashboard, Transactions, Analytics, Budgets (in the header area)
Stores consumed/written: $profiles, $activeProfile

Known Quirks / Design Notes

"Household" is hardcoded as a special combined view with ID 'household'. The backend must understand this ID as meaning "all profiles combined".
Conditional rendering: The entire component is hidden if $profiles is empty (e.g., single-user setup with no profiles configured).
Pill styling uses color-mix() CSS function for the active state — blends accent color at 12% with transparent.


System-Level Summary

Architecture Overview

Folio's frontend is a SvelteKit single-page application with five main routes: Dashboard (/), Transactions (/transactions), Analytics (/analytics), Budgets (/budget), and Copilot (/copilot). The root layout (+layout.svelte) provides the persistent glass-morphism sidebar navigation, theme management, sync controls, and mouse-tracking visual effects. Each page consumes data from a shared API client ($lib/api.js) and communicates state through Svelte stores for theme, privacy mode, active profile, and cross-page period selection.


Data Flow End-to-End

Data flows from the Teller bank API → FastAPI backend → SQLite database → API endpoints → SvelteKit load() functions → Svelte reactive state → SVG/DOM rendering. The Dashboard's +page.js load function pre-fetches a "bundle" (summary, accounts, monthly, categories, net worth series) so the first paint is data-rich. Subsequent interactions (period changes, category drill-downs, profile switches) trigger targeted API calls. The Copilot page sends natural language to the backend, which uses Claude Haiku for SQL generation, with a confirmation-ID pattern for safe write operations.


Key Architectural Decisions

Custom SVG charts over charting libraries: The app renders all charts (net worth area, monthly net bars, waterfall, savings rate trend, trajectory, Sankey) using raw SVG with reactive Svelte computations. This gives full visual control (glow effects, gradients, animations) at the cost of more code. The Sankey chart is the exception — it uses d3-sankey for layout but renders everything else manually.

Balance-as-Reservoir Sankey model: Rather than showing a simple income→expenses flow, the Sankey handles the common real-world case where expenses exceed income (drawing from savings) or income exceeds expenses (building savings). "From Balance" and "To Balance" nodes make the flow diagram always balanced while showing real values.

Privacy mode as a reactive key: The $privacyMode store triggers re-rendering of all monetary values via a void privacyKey dependency pattern in reactive blocks. This is elegant but non-obvious — it works because Svelte's reactivity system tracks the read of privacyKey even though the value is discarded.

Profile-aware data isolation: Every page watches $activeProfile and performs a full data reload on change. The backend filters all queries by profile. Budgets are localStorage-scoped to profile IDs. This multi-profile architecture supports household financial management where different family members may want to see their individual or combined views.

Performance optimizations: Several patterns are used to avoid unnecessary work: memoized chart computations with cache keys, consolidated reactive blocks (the dashboardMetrics mega-reactive), requestAnimationFrame throttling for mouse tracking, prevRef guards against reactive re-execution, and debounced search. The Sankey chart's prevGraphKey pattern is particularly important — it prevents expensive d3-sankey layout recomputation when only visual properties (theme, selection) change.
