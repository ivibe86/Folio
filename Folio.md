1. Project Overview

Folio is a self-hosted personal finance dashboard that aggregates bank accounts and transactions from multiple financial institutions into a unified, privacy-first interface. It targets technically literate individuals and households who want full control over their financial data without surrendering it to a third-party SaaS product.


Core problem: Commercial finance apps (Mint, Copilot Money, Monarch) require users to trust a remote service with bank credentials and complete transaction history. Folio inverts this: the user runs the entire stack on their own hardware, brings their own Teller API credentials and (optionally) their own Anthropic API key, and all data stays in a local SQLite database.


Design philosophy:


Privacy-first. Dollar amounts are masked before any LLM call. Counterparty names are anonymized. Profile names are aliased. Enrichment requests send amount=1.00 instead of real values. A client-side privacy mode masks all values in the UI.
Bring-your-own-credentials. Teller certificates, Anthropic API keys, and Trove API keys are user-supplied. The app functions with graceful degradation when external services are absent.
Local-first where possible. SQLite is the sole persistent store. Deterministic categorization rules handle clear-cut transactions without LLM calls. A three-tier enrichment cache (in-memory → SQLite → API) minimizes external traffic. Budget data lives in localStorage.
Single-command deployment. docker compose up --build spins up the entire stack.

Multi-profile support: Folio supports household use — multiple family members each link their own bank accounts, and a "Household" aggregate view combines all data. Profile identity is derived from Teller account owner names.



2. Tech Stack

Layer	Technology	Version / Notes
Frontend framework	SvelteKit	v2.5.0+, adapter-node for Docker, adapter-auto for dev
Frontend build tool	Vite	v5.2.0
UI framework	Svelte	v4.2.0
CSS framework	TailwindCSS	v3.4.3, class-based dark mode
Charts	d3-sankey, d3-array, d3-scale, d3-shape, d3-interpolate	Selective imports; all other charts are hand-rolled SVG
Backend framework	FastAPI	v0.115.0
ASGI server	Uvicorn	v0.30.0, single-worker
Database	SQLite	WAL mode, via Python stdlib sqlite3
LLM	Anthropic Claude Haiku	claude-3-haiku-20240307, via raw httpx calls
Bank connectivity	Teller API	mTLS certificate auth, cursor-based pagination
Merchant enrichment	Trove API	Optional; single + bulk endpoints
HTTP client	httpx	v0.27.0 (backend), for Teller/Anthropic/Trove
Auth	Custom API key (X-API-Key header)	Timing-safe comparison, per-IP rate limiting
Token encryption	Fernet (cryptography)	v42.0.0+; optional, falls back to plaintext
Containerization	Docker Compose v3+	Two services: backend, frontend
Node.js (frontend runtime)	Node 20 LTS	node:20-slim Docker image
Python (backend runtime)	Python 3.12	python:3.12-slim Docker image
Font icons	Material Symbols Outlined	Self-hosted .woff2
Text fonts	Inter, Manrope, JetBrains Mono, DM Mono	Google Fonts CDN + self-hosted icons


3. Repository Structure

Copy Code
folio/
├── docker-compose.yml          # [CRITICAL] Top-level orchestration; defines both services
├── .env                        # [CRITICAL] All secrets and config (not committed)
├── .env.example                # Template for .env with documentation
├── .dockerignore               # Build context exclusions (secrets, node_modules, .venv)
├── data/                       # SQLite database volume mount (host-persistent)
│   └── finflow.db              # The SQLite database (created at runtime)
├── certs/                      # Teller mTLS certificates (mounted read-only into backend)
│   ├── teller-cert.pem
│   └── teller-key.pem
│
├── backend/
│   ├── Dockerfile              # [CRITICAL] Python 3.12-slim, CA certs, pip install, uvicorn
│   ├── requirements.txt        # Python dependencies
│   ├── main.py                 # [CRITICAL] FastAPI app, all route definitions, startup/shutdown
│   ├── data_manager.py         # [CRITICAL] Data access layer: SQL queries, sync, writes
│   ├── categorizer.py          # [CRITICAL] Two-phase categorization engine (rules + LLM)
│   ├── database.py             # [CRITICAL] Schema, migrations, seeding, connection management
│   ├── bank.py                 # [CRITICAL] Teller API client, mTLS, token/profile management
│   ├── copilot.py              # NLP-to-SQL engine (Claude), safety validation, confirmation flow
│   ├── enricher.py             # Trove merchant enrichment, three-tier cache
│   ├── recurring.py            # Subscription/recurring charge detection (3-layer)
│   ├── sanitizer.py            # Transaction sign normalization, PII cleanup
│   ├── privacy.py              # Amount/name masking for LLM calls
│   ├── token_store.py          # Encrypted Teller token CRUD (Fernet)
│   ├── auth.py                 # API key verification, rate limiting middleware
│   ├── log_config.py           # Centralized logging setup
│   ├── subscription_seeds.json # ~220 known subscription patterns
│   └── package.json            # Empty {}; workspace tooling placeholder
│
├── frontend/
│   ├── Dockerfile              # [CRITICAL] Multi-stage: builder (Vite build) → production (Node)
│   ├── package.json            # NPM manifest: SvelteKit, Vite, D3, Tailwind
│   ├── package-lock.json       # Lockfile for deterministic installs
│   ├── .npmrc                  # min-release-age=7 (supply-chain security)
│   ├── vite.config.js          # Vite config: env loading, dev proxy, custom plugin
│   ├── vite-env-plugin.js      # Custom plugin: injects API key into app.html
│   ├── svelte.config.js        # Adapter selection (node for Docker, auto for dev)
│   ├── postcss.config.js       # TailwindCSS + Autoprefixer
│   ├── tailwind.config.js      # Theme extension: colors, fonts, shadows, animations
│   │
│   └── src/
│       ├── app.html            # [CRITICAL] HTML shell: prefetch script, FOUC prevention, fonts
│       ├── app.css             # [CRITICAL] Global styles: glassmorphism, cards, Sankey theater
│       ├── theme-light.css     # Light theme CSS custom properties (Cool Slate v11)
│       ├── theme-dark.css      # Dark theme CSS custom properties
│       │
│       ├── lib/
│       │   ├── api.js          # [CRITICAL] API client factory: caching, profile injection, auth
│       │   ├── stores.js       # [CRITICAL] Svelte stores: darkMode, filters, syncing, privacy
│       │   ├── utils.js        # Formatting, date helpers, recurring detection, spring animation
│       │   │
│       │   ├── stores/
│       │   │   └── profileStore.js  # Multi-profile state: profiles, activeProfile, profileParam
│       │   │
│       │   ├── components/
│       │   │   ├── SankeyChart.svelte       # D3-powered Sankey flow diagram
│       │   │   ├── TellerConnect.svelte     # Bank enrollment via Teller Connect SDK
│       │   │   └── ProfileSwitcher.svelte   # Profile pill selector
│       │   │
│       │   └── styles/
│       │       ├── dashboard.css    # Hero card, NW chart, monthly bars, YoY
│       │       ├── transactions.css # Source badges, filters, pagination
│       │       ├── analytics.css    # Waterfall, pulse grid, recurring table
│       │       ├── copilot.css      # Chat UI, data tables, SQL blocks
│       │       └── budget.css       # Placeholder (uses shared app.css styles)
│       │
│       └── routes/
│           ├── +layout.svelte       # [CRITICAL] Root layout: sidebar, theme, sync, glow effects
│           ├── +page.svelte         # Dashboard (/) — hero card, Sankey, metrics
│           ├── +page.js             # Dashboard loader: prefetch consumption + fallback
│           ├── transactions/
│           │   └── +page.svelte     # Transaction list: filters, search, category editing
│           ├── analytics/
│           │   ├── +page.svelte     # Analytics: waterfall, pulse, recurring, health
│           │   └── +page.js         # Analytics loader: monthly + category data
│           ├── budget/
│           │   └── +page.svelte     # Budget tracking (localStorage-based)
│           └── copilot/
│               └── +page.svelte     # AI chat interface: read/write queries


4. System Architecture — End to End

4.1 Architecture Diagram

Copy Code
┌─────────────────────────────────────────────────────────────────────┐
│  Browser (localhost:3000)                                           │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  SvelteKit App                                               │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────┐ ┌──────┐ │   │
│  │  │Dashboard │ │Transact. │ │Analytics │ │Budget │ │Copilot│ │   │
│  │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └───┬───┘ └──┬───┘ │   │
│  │       │             │            │            │        │      │   │
│  │  ┌────┴─────────────┴────────────┴────────────┴────────┴──┐  │   │
│  │  │  api.js  (cache · profile injection · auth headers)    │  │   │
│  │  └────────────────────────┬───────────────────────────────┘  │   │
│  │                           │ fetch()                          │   │
│  └───────────────────────────┼──────────────────────────────────┘   │
│                              │ HTTP :3000 → proxy to :8000          │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
        Docker Compose Network │ http://backend:8000
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│  FastAPI Backend (port 8000)                                         │
│  ┌──────────┐  ┌──────────────┐  ┌────────────┐  ┌───────────────┐  │
│  │ main.py  │→ │data_manager  │→ │categorizer │→ │  Claude LLM   │  │
│  │ (routes) │  │(SQL queries) │  │(rules+LLM) │  │  (Anthropic)  │  │
│  └────┬─────┘  └──────┬───────┘  └─────┬──────┘  └───────────────┘  │
│       │               │                │                             │
│  ┌────┴────┐   ┌──────┴──────┐  ┌──────┴──────┐  ┌──────────────┐   │
│  │  auth   │   │  database   │  │  enricher   │→ │  Trove API   │   │
│  │(API key │   │  (SQLite)   │  │  (3-tier $) │  └──────────────┘   │
│  │ + rate) │   └──────┬──────┘  └─────────────┘                     │
│  └─────────┘          │                                              │
│                ┌──────┴──────┐  ┌─────────────┐  ┌──────────────┐   │
│                │  bank.py    │→ │  Teller API  │  │ token_store  │   │
│                │  (mTLS)     │  │  (mTLS)      │  │ (Fernet enc) │   │
│                └─────────────┘  └──────────────┘  └──────────────┘   │
│                                                                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────┐   │
│  │ copilot    │  │ recurring  │  │ sanitizer  │  │  privacy     │   │
│  │ (NLP→SQL)  │  │ (3-layer)  │  │ (normalize)│  │  (PII mask)  │   │
│  └────────────┘  └────────────┘  └────────────┘  └──────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
         │                    │
    ┌────┴────┐         ┌────┴────┐
    │ /data/  │         │ /certs/ │
    │finflow. │         │teller-* │
    │  db     │         │  .pem   │
    └─────────┘         └─────────┘
    (host volume)       (host volume, ro)

4.2 App Load / Auth Flow

Browser requests http://localhost:3000 → SvelteKit Node server.
app.html is served. Two inline scripts execute immediately:
Prefetch: fetch('/api/dashboard-bundle') fires with X-API-Key header (injected at build time by vite-env-plugin.js). Response promise stored on window.__dashboardData.
FOUC prevention: Reads localStorage.getItem('theme'), applies .dark class synchronously.
SvelteKit JS bundle loads. +layout.svelte mounts:
darkMode store reads DOM state (not localStorage — the inline script already applied the class).
loadProfiles() fetches /api/profiles.
api.getSummary() fetches last-synced timestamp.
+page.js (dashboard route) runs:
Checks window.__dashboardData — if present, awaits it with a 4-second timeout.
If prefetch fails/times out, dynamically imports createApi and makes a fresh getDashboardBundle('biweekly') call.
Returns { summary, accounts, monthly, categories, netWorthSeries } as page data.
All API calls include X-API-Key header. Backend verify_api_key dependency validates via hmac.compare_digest(). rate_limit_middleware checks per-IP limits.

4.3 Teller Bank Connection Flow

User clicks "+" button → TellerConnect.svelte dynamically loads the Teller Connect SDK script.
SDK opens modal → user authenticates with their bank.
onSuccess callback receives { accessToken, institution, enrollment }.
Frontend calls api.enrollAccount(accessToken, institutionName, enrollmentId) → POST /api/enroll.
Backend validates the token via get_identity(), derives profile name from owner's first name.
Token is Fernet-encrypted and persisted via token_store.save_token().
bank.reload_tokens_and_profiles() hot-reloads in-memory token/profile registries.
_invalidate_profile_cache() clears the validated profiles set.
Immediate sync is triggered: fetch_fresh_data() pulls transactions for the new account.
Frontend receives success → handleTellerEnrolled() reloads the entire dashboard.

4.4 Transaction Fetch → Sanitize → Categorize → Store Flow

Copy Code
Teller API (per account, per profile)
  │
  ▼
bank.get_transactions()         cursor-based pagination, max 5000 txns/account
  │
  ▼
data_manager.fetch_fresh_data() thread-locked, deduplicates by transaction ID
  │
  ▼
categorizer.categorize_transactions()
  │
  ├─ Phase 1:   sanitizer.sanitize_transactions()
  │              - CC sign normalization (Teller positive → negative expense)
  │              - PII strip (IDs, confirmation numbers, INDN: names)
  │              - Payment-type filtering (card-side duplicates → None)
  │              - raw_description preserved for enrichment
  │
  ├─ Phase 1.5: enricher.enrich_transactions()
  │              - Filter enrichable (skip transfers, income, fees, short descriptions)
  │              - Memory LRU cache → DB cache → Trove API (single or bulk)
  │              - Deduplication: normalize descriptions, one API call per unique merchant
  │              - Fanout: copy enrichment to all duplicates
  │              - Adds: merchant_name, domain, industry, city, state
  │
  ├─ Phase 1.6: User rule matching (DB query, source='user', highest priority)
  │              - Matched → skip all further categorization
  │
  ├─ Phase 1b:  _rule_based_categorize()
  │              - Deterministic rules: interest, CC payments, savings transfers, bank fees
  │              - Medium confidence: deposits, tax, P2P, ATM, Teller category hints
  │              - Outputs: (category, confidence) or (None, "")
  │
  ├─ Phase 2:   _categorize_batch_llm() [Claude Haiku]
  │              - rule-high → skip LLM entirely
  │              - rule-medium → send with suggestion, LLM may override
  │              - unmatched → full LLM categorization
  │              - Batches of 50, 1-second delay between batches
  │              - PII masked: amounts → -$XXX/+$XXX, names → [person]/[organization]
  │              - Fallback: rule suggestion or "Other" if LLM fails
  │
  └─ Final:     Validate every tx has category + profile
  │
  ▼
data_manager._insert_transaction()
  - INSERT OR IGNORE (dedup by tx ID)
  - Resolves account_id from account_name + profile_id
  - Derives categorization_source from confidence if not set
  │
  ▼
data_manager._snapshot_net_worth()
  - INSERT OR REPLACE per (date, profile_id)
  - Profiles + household aggregate

4.5 Dashboard / Sankey Render Flow

+page.svelte receives data from loader. onMount:
Validates data (retries if empty due to auth race).
Computes account deltas (month-over-month balance changes).
Calls bootstrapInitialPeriod() → seeds Sankey and metric ribbon from preloaded data.
Starts animateNumbers() → spring-eased count-up for net worth, income, expenses.
dashboardMetrics reactive block consolidates: totalCash, totalOwed, netWorth, account lists, savings rate, daily spending pace, top category.
Sankey chart (SankeyChart.svelte) receives props → buildGraph():
Balance-as-Reservoir model: If expenses > income, a "From Balance" source node appears. If income > expenses, a "To Balance" sink node appears.
d3-sankey computes layout → custom cubic Bézier link paths → SVG renders nodes, links, glows, animated pulses.
Period changes → updatePeriod() → targeted API calls → re-render with new data (stale data stays visible during fetch).
Sankey category click → handleSankeySelect() → fetches transactions for that category → drill-down table renders below.

4.6 Copilot Query Flow

User types question → send() → api.askCopilot(question, profile) → POST /api/copilot/ask.
Backend copilot.ask_copilot():
Builds anonymized profile map (real names → profile_1, profile_2).
Builds schema context prompt: table schemas, category list, today's date, profile rules.
Sends to Claude → expects SQL response.
If CANNOT: returns explanation, no SQL.
Read (SELECT): validates via _validate_read_sql() → executes with PRAGMA query_only = ON → anonymizes results → sends to Claude for natural language summary → returns { answer, sql, data, operation: 'read' }.
Write (UPDATE/INSERT): validates via _validate_write_sql() (table/column allowlist) → generates preview (_preview_write()) → stores validated SQL server-side via store_pending_sql() → returns { sql, data: preview, confirmation_id, needs_confirmation: true, operation: 'write_preview' }.
Frontend displays response. For write previews, shows confirmation card.
User clicks Confirm → api.confirmCopilotWrite(question, confirmationId, profile) → POST /api/copilot/confirm.
Backend retrieve_pending_sql(nonce) → pops stored SQL → _execute_write() → row count pre-check → execute → auto-create category rules if applicable → return result.
Frontend invalidateCache() after successful write.

Security: The client never sends SQL. It sends only a confirmation_id (nonce). The server stores the validated SQL keyed by this nonce with a 5-minute TTL.



5. Backend Deep Dive

5.1 main.py — Application Entry Point

Purpose: FastAPI app definition, all HTTP route handlers, middleware registration, startup/shutdown lifecycle.


Key Functions:


Function	Route	Method	Description
health_check()	/healthz/health	GET	Unauthenticated (sub-app). Docker health target.
accounts(profile, db)	/api/accounts	GET	Filtered account list.
transactions(...)	/api/transactions	GET	Paginated, filtered transactions. Limit capped at 1000.
update_category(tx_id, body)	/api/transactions/{tx_id}/category	PATCH	User category override → auto-creates rules → retroactive recategorization.
dashboard_bundle(nw_interval, profile, db)	/api/dashboard-bundle	GET	Aggregates summary + accounts + monthly + categories + NW series.
get_recurring_transactions(profile)	/api/analytics/recurring	GET	Subscription detection. Only endpoint using get_data() (full load).
confirm_subscription(body, profile, db)	/api/subscriptions/confirm	POST	User confirms detected recurring.
dismiss_subscription(body, profile, db)	/api/subscriptions/dismiss	POST	User dismisses false positive.
copilot_ask(body, profile)	/api/copilot/ask	POST	NLP question → SQL → result.
copilot_confirm(body, profile)	/api/copilot/confirm	POST	Execute previewed write (by confirmation_id).
enroll_account(req)	/api/enroll	POST	Teller Connect enrollment → token persist → sync.
deactivate_enrollment(body)	/api/enrollments/deactivate	POST	Soft-delete enrollment.
get_summary(profile, db)	/api/summary	GET	Dashboard summary stats.
get_monthly(profile, db)	/api/analytics/monthly	GET	Monthly income/expense aggregation.
get_categories_analytics(month, profile, db)	/api/analytics/categories	GET	Per-category breakdown.
get_categories(db)	/api/categories	GET	Active category list.
create_category(body, db)	/api/categories	POST	Create new category.
update_expense_type(name, body, db)	/api/categories/{name}/expense-type	PATCH	Set fixed/variable/non_expense.
get_merchants(month, profile, db)	/api/merchants	GET	Merchant spending breakdown.
get_nw_series(interval, profile, db)	/api/analytics/net-worth-series	GET	Net worth time series (weekly/biweekly).
sync_data(profile)	/api/sync	POST	Trigger Teller fetch → categorize → store.
get_profiles()	/api/profiles	GET	List configured profiles.
get_teller_config()	/api/teller-config	GET	Returns Teller app ID + environment for frontend.

Startup (startup()): validate_teller_config() → init_db() → sync_subscription_seeds() → sync_enrichment_cache_from_seeds().


Shutdown (shutdown()): close_thread_local_connection() → bank.close_all_clients().


Middleware: CORS (configurable origins), TrustedHostMiddleware, rate_limit_middleware.


Global dependency: Depends(verify_api_key) on the main app. /healthz/health is a separate sub-app to bypass this.


⚠️ Quirk: /health on the main app appears unauthenticated (dependencies=[]) but requires auth due to FastAPI's global dependency behavior. Docker must use /healthz/health.


⚠️ Quirk: NON_SPENDING_CATEGORIES is a set here and a tuple in data_manager.py. Must be kept in sync manually.


5.2 data_manager.py — Data Access Layer

Purpose: All SQLite reads and writes. Sync orchestration. SQL-driven analytics.


Key Functions:


Function	Inputs	Outputs	Notes
get_accounts_filtered(profile, conn)	Optional profile	list[dict]	is_credit derived via SQL CASE.
get_transactions_paginated(month, cat, acct, search, profile, limit, offset, conn)	All optional filters	{data, total_count, limit, offset}	Search uses UPPER(description) LIKE ? with _escape_like().
get_summary_data(profile, conn)	Optional profile	dict (income, expenses, refunds, net, savings_rate, NW, etc.)	Entirely SQL aggregation. NON_SPENDING_CATEGORIES excluded from expenses.
get_monthly_analytics_data(profile, conn)	Optional profile	list[dict]	GROUP BY SUBSTR(date, 1, 7) with conditional aggregation.
get_category_analytics_data(month, profile, conn)	Optional month + profile	list[dict]	Includes expense_type from categories table. Net ≤ 0 categories excluded.
get_merchant_insights_data(month, profile, conn)	Month + profile	list[dict]	Only enriched transactions.
get_net_worth_series_data(interval, profile, conn)	'weekly'/'biweekly' + profile	list[dict]	Current balance − cumulative transaction changes. Sampled at intervals.
get_dashboard_bundle_data(nw_interval, profile, conn)	Interval + profile	dict (summary, accounts, monthly, categories, NW series)	Single DB connection for all sub-queries.
get_data(force_refresh)	Boolean	dict (all transactions + accounts)	⚠️ DEPRECATED. Full memory load. Used only by /api/analytics/recurring.
fetch_fresh_data(incremental)	Boolean	dict	Core sync function. Thread-locked. Iterates profiles → accounts → Teller fetch → categorize → insert → snapshot NW.
update_transaction_category(tx_id, new_category)	tx ID + category string	bool	Creates user rule from merchant pattern. Retroactively recategorizes matching transactions.
_insert_transaction(conn, tx)	Connection + categorized tx dict	Side effect	INSERT OR IGNORE. Resolves account_id by name + profile.
_snapshot_net_worth(conn, timestamp)	Connection + datetime	Side effect	Per-profile + household. INSERT OR REPLACE keyed on (date, profile_id).

Integration: Called by main.py (all endpoints). Calls bank (Teller API), categorizer (pipeline), database (connections).


5.3 categorizer.py — Two-Phase Categorization Engine

Purpose: Deterministic rule matching (Phase 1) followed by LLM categorization/validation (Phase 2).


Key Functions:


Function	Description
categorize_transactions(transactions, batch_size=50)	Main entry point. Full pipeline: sanitize → enrich → user rules → deterministic rules → LLM → validate.
get_active_categories()	Fetches from DB with fallback to _DEFAULT_CATEGORIES.
_rule_based_categorize(tx)	Returns (category, confidence). rule-high = skip LLM. rule-medium = send with suggestion. (None, "") = full LLM.
_get_teller_category_map()	DB-backed Teller→Folio mapping. Cached in _teller_map_cache. User overrides win (ORDER BY source ASC).
_build_llm_line(idx, tx, rule_suggestion)	Builds a PII-masked line for Claude prompt. Amounts as -$XXX/+$XXX. Names as [person]/[organization].
_categorize_batch_llm(batch, start_index)	Sends batch to Claude Haiku. Expects JSON array response. Strips markdown fences.

Confidence levels:


rule-high: Interest income, CC payments, savings transfers, bank fees → never sent to LLM.
rule-medium: Deposits (probably income), tax, P2P to persons, ATM, Teller category hints → sent to LLM with suggestion.
(None, ""): No match → full LLM categorization.

LLM model: claude-3-haiku-20240307. Prompt includes all active categories with detailed semantic descriptions.


Feature toggle: ENABLE_LLM_CATEGORIZATION=false disables Phase 2 entirely (falls back to rule suggestions or "Other").


5.4 database.py — Schema, Connections, Seeding

Purpose: SQLite schema definition, migration, connection lifecycle, default data seeding.


Connection Strategies (two coexist by design):


get_connection() / get_db(): Thread-local connection. Used by background sync and module-level code. Deprecated for API use.
get_db_session(): Request-scoped FastAPI dependency. New connection per request, closed in finally. Preferred for endpoints.

Both set: WAL journal mode, foreign keys ON, 5-second busy timeout.


Key Functions:


Function	Description
init_db()	Creates all tables (idempotent) → runs migrations → seeds defaults.
_migrate_expense_type(conn)	Backfill expense_type/expense_type_source columns. Only updates source='system' rows.
_extract_merchant_pattern(description)	Strips store numbers, dates, phone numbers, state codes, HQ cities, suffixes. Returns reusable pattern for rules.
sync_subscription_seeds()	Loads subscription_seeds.json → clears system seeds → re-inserts. User seeds untouched.
sync_enrichment_cache_from_seeds()	Populates enrichment_cache from subscription seeds. INSERT OR IGNORE (never overwrites Trove data).
dicts_from_rows(cursor)	Converts sqlite3.Row objects to dicts.

Schema: See Section 7 (Data Model).


5.5 bank.py — Teller API Client

Purpose: All Teller API communication. mTLS certificate auth. Token/profile management. Retry logic.


Key Functions:


Function	Description
validate_teller_config()	Checks cert files exist and are readable. Called at startup. Raises RuntimeError on failure.
_load_tokens()	Merges tokens from env vars + token_store.load_all_tokens().
_load_profiles()	Groups tokens by profile name (prefix before _TOKEN). Fallback: {"primary": all_tokens}.
_get_client(token)	Returns cached httpx.Client with mTLS certs. Keyed by token string.
_request_with_retry(client, method, url, max_retries=3)	Exponential backoff on 429. Wait: min(2^attempt * 2, 30) seconds.
get_all_accounts_by_profile()	Iterates PROFILES → fetches accounts → tags with access_token + profile. 1s delay between calls.
get_transactions(account_id, token)	Cursor-based pagination. Max 100/page. Stops at TELLER_MAX_PAGES (50) or TELLER_MAX_TRANSACTIONS (5000).
get_balances(account_id, token)	Account balance fetch.
get_identity(token, account_id)	Owner name resolution via Teller Identity API.
reload_tokens_and_profiles()	Hot-reloads TOKENS/PROFILES globals. Closes clients for removed tokens. Called after enrollments.
close_all_clients()	Closes all cached httpx clients. Called during shutdown.

Rate limiting: RATE_LIMIT_DELAY = 1.0 second between all Teller API calls.


5.6 copilot.py — NLP-to-SQL Engine

Purpose: Translates natural language → validated SQL → execution → natural language answer.


Key Functions:


Function	Description
ask_copilot(question, profile, confirm_write, pending_sql)	Main entry. Profile anonymization → schema context → Claude → validate → execute/preview.
_validate_read_sql(sql)	Single statement, must start with SELECT, no forbidden keywords outside string literals.
_validate_write_sql(sql)	Allowlist: UPDATE transactions (6 columns), UPDATE category_rules (3 columns), INSERT category_rules, INSERT categories. No DELETE ever. No subqueries in SET/VALUES.
store_pending_sql(sql, profile) / retrieve_pending_sql(nonce)	Server-side store with 5-minute TTL. One-time retrieval (pop semantics).
_build_profile_map()	Bidirectional mapping: real names ↔ profile_1, profile_2, all_profiles (household).
_build_schema_context(real_to_alias)	Schema prompt with table definitions, categories, anonymized profiles, today's date, profile filtering rules.
_preview_write(sql, profile)	Extracts WHERE clause → runs SELECT with PRAGMA query_only = ON. Returns {count, sample}.
_execute_write(sql, profile, original_question)	Row count pre-check (COPILOT_MAX_WRITE_ROWS default 5000) → execute → auto-create rules if applicable.
_generate_natural_answer(question, rows)	Sends privacy-sanitized results to Claude for summarization.

Security fixes documented in code: FIX C1 (column parsing), C2 (subquery blocking), C3 (server-side SQL store), C4 (preview safety), C5 (write execution safety).


5.7 enricher.py — Trove Merchant Enrichment

Purpose: Identifies merchants from raw bank descriptions. Three-tier cache. Deduplication.


Cache Strategy:


In-memory LRU (_EnrichmentCache): Thread-safe OrderedDict, max 1000 entries.
Persistent DB (enrichment_cache table): Updates hit_count/last_seen on hits. Trove results upgrade seed entries.
Trove API: Single-enrich (default, higher match rate) or bulk (configurable via BULK_THRESHOLD).

Privacy: Amount sent as 1.00 (Trove matching is description-based). Raw descriptions are used for best Trove matching.


Deduplication: _dedup_key() normalizes descriptions (masks long numbers). One representative per group sent to API, then fanned out to siblings.


Feature toggle: ENABLE_TROVE=false disables all enrichment.


5.8 recurring.py — Subscription Detection

Purpose: Three-layer recurring charge detection: seed matching → algorithmic → category-based.


Class: RecurringDetector


Constant	Value	Meaning
ALGO_MIN_CHARGES	3	Minimum transactions for algorithmic detection
ALGO_CV_THRESHOLD	0.10	Max coefficient of variation (10%)
ALGO_CV_THRESHOLD_LOOSE	0.40	For variable-amount categories (utilities, insurance)
ALGO_EXCLUDED_CATEGORIES	Groceries, Food & Dining, Transportation, Shopping, Travel, + transfers	Never detected algorithmically

detect(transactions, profile) pipeline:


Filter to expenses, skip disqualified descriptions (_DISQUALIFY_TOKENS: ATM, CASH, CHECK, REFUND, etc.).
Group by normalized merchant key.
Merge groups sharing a subscription seed.
Layer 1 — Seed matching (known patterns from subscription_seeds.json).
Layer 2 — Algorithmic (frequency + amount consistency analysis via _detect_frequency() + _amount_confidence()).
Layer 3 — Category-based (remaining "Subscriptions" category items).
Sort: active first, then by annual cost descending.

Frequency definitions (FREQUENCY_DEFS): monthly (30d), quarterly (91d), semi_annual (182d), annual (365d) — each with grace periods and valid ranges.


Seed cache: 60-second TTL in _load_seeds_cached().


5.9 sanitizer.py — Transaction Normalization

Purpose: First pipeline step. CC sign normalization (Teller convention → Folio convention), PII stripping, payment duplicate filtering.


sanitize_transaction(tx) returns None for: payment type transactions (card-side payments, already counted on bank side).


CC sign logic:


Teller CC purchase (positive amount) → negative (expense).
Teller CC transaction with negative amount → positive (refund).

Strips: ID:XXXXX, confirmation numbers, INDN: (individual names in ACH), CO ID: references.


Preserves: raw_description (original, for enrichment) alongside cleaned description.


5.10 privacy.py — PII Masking

Purpose: Shared utilities for sanitizing data before LLM/Trove calls.


Function	Input	Output
mask_amount(amount, placeholder)	Number	-$XXX, +$XXX, or $XXX
mask_counterparty(name, counterparty_type)	Name + type	[person], [organization], [counterparty], or ""
sanitize_row_for_llm(row)	Dict	Dict with amounts/balances/NW masked, counterparty masked, raw_description stripped
sanitize_rows_for_llm(rows)	List[dict]	List of sanitized dicts

5.11 token_store.py — Encrypted Token Storage

Purpose: CRUD for Teller access tokens stored in enrolled_tokens table. Fernet encryption.


Function	Description
save_token(profile, token, institution, owner_name, enrollment_id)	Encrypts + inserts. Returns False on duplicate.
load_all_tokens()	Returns {profile: [decrypted_tokens]} for all active enrollments.
load_all_enrollments()	Returns metadata (no tokens) for UI display.
deactivate_token(token_id)	Soft-delete: is_active = 0.

⚠️ Note: If TOKEN_ENCRYPTION_KEY is not set or cryptography not installed, tokens are stored in plaintext with a warning.


5.12 auth.py — Authentication & Rate Limiting

Purpose: API key verification + per-IP rate limiting.


verify_api_key(api_key): FastAPI dependency. hmac.compare_digest() for timing-safe comparison. Auto-generates session key if none configured (printed to stdout).


Rate limits:


Route prefix	Limit	Window
/api/copilot	20 requests	60 seconds
/api/sync	5 requests	300 seconds
default	120 requests	60 seconds

Cleanup: Global log cleanup runs at most every 5 minutes when _MAX_LOG_KEYS (500) exceeded.


5.13 log_config.py — Logging

setup_logging() configures root logger with stdout handler. get_logger(name) is the factory. Default level: INFO. Format: %(asctime)s %(levelname)s [%(module)s]: %(message)s.



6. Frontend Deep Dive

6.1 Svelte Stores & State Management

$lib/stores.js:


Store	Type	Purpose
darkMode	Custom	Theme toggle with DOM sync. performSwitch() adds .theme-switching class → disables all backdrop-filter + transitions → toggles .dark → waits 2 rAF callbacks → removes class. Reads DOM (not localStorage) on init.
filters	Writable	Transaction page filter state: {month, category, account, search}.
syncing	Writable	Boolean; sync-in-progress indicator.
summaryData / accountsData	Writable	Global caches to avoid re-fetch across navigations.
dashboardPrefs	Writable	{showForecast, showUpcoming} section visibility.
selectedPeriodStore	Writable	Cross-page period selection. Default: 'this_month'.
selectedCustomMonthStore	Writable	Custom month for period selector.
privacyMode	Custom	Persists to localStorage. toggle() method. Read by formatCurrency().

$lib/stores/profileStore.js:


Store / Function	Description
profiles	Writable. Array of {id, name}.
activeProfile	Writable. String. Default: 'household'.
profileParam	Derived. 'household' → '' (empty = no filter), otherwise pass-through.
loadProfiles(fetchFn?)	Fetches /api/profiles. Normalizes string/object arrays. Filters out 'household'.

Reactivity patterns:


void privacyKey in reactive blocks forces recomputation when privacy mode toggles (Svelte tracks the read even though the value is discarded).
_prevFilterKey / prevGraphKey / prevMonthlyRef guards prevent reactive blocks from re-executing when data hasn't actually changed.
initialLoadComplete flag prevents premature fetches during hydration.

6.2 API Layer ($lib/api.js)

createApi(fetchFn?) returns an object with a method for every backend endpoint.


Key design points:


Lazy API key: getApiKey() reads import.meta.env.VITE_API_KEY fresh on every request (not cached at module load) — documented fix for Vite HMR timing issues.
Cache: Map-based, 2-minute TTL, GET-only. Key includes profiled endpoint. invalidateCache() clears all.
Profile injection: appendProfileParam(endpoint, method) appends ?profile=<id> unless endpoint is in PROFILE_EXEMPT_ENDPOINTS (/sync, /profiles) or method is mutation (except /copilot, /subscriptions).
Default instance: api (module export) uses window.fetch. createApi(fetch) with SvelteKit's fetch is used in +page.js load functions.
Copilot confirmation: confirmCopilotWrite() sends confirmation_id, not raw SQL.

Route mapping (api.js method → backend route):


API Method	Backend Route	HTTP
getAccounts()	/api/accounts	GET
getTransactions(params)	/api/transactions	GET
updateCategory(txId, category)	/api/transactions/:id/category	PATCH
getCategories()	/api/categories	GET
createCategory(name)	/api/categories	POST
getMonthlyAnalytics()	/api/analytics/monthly	GET
getCategoryAnalytics(month)	/api/analytics/categories	GET
getSummary()	/api/summary	GET
getProfiles()	/api/profiles	GET
sync()	/api/sync	POST
askCopilot(question, profile)	/api/copilot/ask	POST
confirmCopilotWrite(question, confirmationId, profile)	/api/copilot/confirm	POST
getNetWorthSeries(interval)	/api/analytics/net-worth-series	GET
getDashboardBundle(nwInterval)	/api/dashboard-bundle	GET
getMerchants(month)	/api/merchants	GET
getRecurring()	/api/analytics/recurring	GET
updateExpenseType(categoryName, type)	/api/categories/:name/expense-type	PATCH
confirmSubscription(...)	/api/subscriptions/confirm	POST
dismissSubscription(...)	/api/subscriptions/dismiss	POST
getTellerConfig()	/api/teller-config	GET
enrollAccount(...)	/api/enroll	POST

6.3 Key Components

SankeyChart.svelte

D3-powered Sankey flow diagram. Uses d3-sankey for layout, everything else is custom SVG.
Balance-as-Reservoir model: buildGraph() creates "From Balance" (shortfall) or "To Balance" (surplus) nodes to keep flows balanced.
realValues map: Stores actual amounts for display labels; layout uses balanced (adjusted) values.
Visual layers (render order): Link glow underlayer → main link ribbons → flow pulse overlay → node halos → node rings → node bodies → highlight lines → pulse dots → labels.
Props: income, expenses, savingsTransfer, personalTransfer, categories, selectedCategory, height, autoHeight.
Events dispatched: select (category name or null).
Theme detection: MutationObserver on document.documentElement class changes (separate from $darkMode store because SVG filters need imperative updates).

TellerConnect.svelte

Dynamically loads Teller Connect SDK script via DOM manipulation in onMount.
Props: applicationId, environment.
Events dispatched: enrolled (success), error, exit.
openTellerConnect(): Requests products: transactions, balance, identity. On success → api.enrollAccount().

ProfileSwitcher.svelte

Pill-button component. "Household" always first (hardcoded, groups icon). Individual profiles from $profiles store.
select(id): Sets $activeProfile, triggering reactive reloads across all pages.
Hidden if $profiles.length === 0.

6.4 Route Pages

Dashboard (/) — +page.svelte + +page.js

Data loading: +page.js consumes window.__dashboardData prefetch (4s timeout) → fallback to createApi(fetch).getDashboardBundle('biweekly'). ssr = false.


Key functions:


Function	Description
bootstrapInitialPeriod()	Seeds Sankey + metrics from preloaded data. Background refresh for precise data.
updatePeriod()	Fetches fresh data for period changes (this_month, last_month, ytd, all, custom).
animateNumbers()	Spring-eased count-up (6% overshoot) for NW, income, expenses. Staggered 150ms.
buildNWAreaChart(data, w, h)	Catmull-Rom spline SVG paths. Tension=0.5, 16 samples/segment. Quarterly x-labels.
buildSankeyCategoryList(cats, savings, transfers)	Merges expenses + savings/transfers for Sankey.
handleSankeySelect(event)	Drill-down: fetches transactions for selected category.
fetchBundleWithRetry(retries, delayMs)	Retry wrapper for initial load race condition. Exponential backoff.
dashboardMetrics (reactive)	Single consolidated block: totalCash, totalOwed, netWorth, deltas, savings rate, daily pace, top category.

DIRECT_FLOW_CATEGORIES: ['Savings Transfer', 'Personal Transfer']. Excluded from expense totals (money moving between accounts).


Transactions (/transactions) — +page.svelte

Paginated, filterable list. Filters: period, month, category, account, search (debounced 300ms).


Key functions:


Function	Description
fetchTransactions()	Fetches page with current filters. YTD uses __ytd__ sentinel + client-side filtering (limit 1000).
updateCategory(txId, newCategory)	api.updateCategory() → local state update (sets confidence: 'manual', categorization_source: 'user') → cache invalidation.
createAndApplyCategory(txId)	Creates new category (case-insensitive dedup) → applies.
getSourceLabel(tx)	Maps categorization_source to display: Manual, Auto-rule, Rule, AI, Fallback.

⚠️ Quirk: On mount, fetches up to 1000 transactions just for filter dropdown metadata (unique months/accounts). A dedicated metadata endpoint would be better.


Analytics (/analytics) — +page.svelte + +page.js

Data loading: +page.js fetches monthly + category analytics in parallel. depends('app:analytics') for invalidation.


Key reactive computations:


Computation	Description
spendingPulseCards	Anomaly detection per category. Periodic spending detector: if ratio > 4× and category appears < 40% of months → classified as periodic/seasonal.
waterfallData / waterfallGeometry	Cash flow waterfall SVG. Opening → income → expense categories → savings → personal transfers. Bridge connectors.
fixedVsVariable	DB-driven expense classification. toggleExpenseType() calls api.updateExpenseType() + immediate local update.
savingsRateTrend / savingsRateGeometry	Monthly savings rates + 3-month rolling average. Target line at 25%.
projectedYearEnd	Year-end projection from 3-month rolling average. Optimistic (+20%), pessimistic (-20%).
incomeStability	Coefficient of variation: <10% "Very Stable", 10-20% "Stable", 20-30% "Moderate", >30% "Volatile".
actionableNudge	"What if" scenario: reduce over-budget categories to averages → potential savings.

Budget (/budget) — +page.svelte

Budget values stored in localStorage (keyed by finflow_budgets_{profileId}). Not synced to backend.


Key functions:


Function	Description
saveBudget(category, value)	Saves/removes budget limit. Removes on NaN/zero/negative.
budgetItems (reactive)	Joins monthCategories with budgets. Status: unset/good/warning (>80%)/over (>100%).

Copilot (/copilot) — +page.svelte

AI chat interface. See Section 4.6 for full flow.


Key functions:


Function	Description
send()	api.askCopilot(question, $activeProfile) → append response.
confirmWrite(msgIndex)	api.confirmCopilotWrite() with confirmation_id (not SQL) → invalidateCache().
cancelWrite(msgIndex)	Marks message as not needing confirmation.
formatTableValue(key, value)	Heuristic: currency-like columns → formatCurrency(), ISO dates → formatDate().

Quick prompts: 6 predefined questions/commands shown on empty chat.


Data table limits: 20 rows for reads, 5 rows for write previews.


6.5 Theming System

Architecture: Three-layer CSS approach.


TailwindCSS utilities — Layout, spacing, typography (tailwind.config.js extends with custom colors, fonts, shadows, animations).
CSS custom properties — All visual tokens defined in theme-light.css (:root) and theme-dark.css (.dark).
Hand-crafted CSS classes — Glassmorphism card system, Sankey theater, glow effects (app.css + route-specific CSS files).

Dark mode toggle: darkMode store's performSwitch():


Adds .theme-switching to <html> — CSS forces backdrop-filter: none !important, transition-duration: 0s !important on all elements.
void root.offsetHeight (forces synchronous layout).
Toggles .dark class, saves to localStorage.
Waits 2 requestAnimationFrame callbacks.
Removes .theme-switching.

Why: Glassmorphism uses heavy backdrop-filter: blur() which is GPU-expensive to recomposite during CSS variable swaps.


"Dark Stage Islands": In light mode, premium cards (hero NW card, Sankey theater, waterfall theater) render as dark-background elements on a clean white page. Uses --island-* CSS variables. This requires maintaining two sets of semantic colors — normal + island-specific.


Mouse-tracking glows: JavaScript sets --mx/--my (page level), --card-mx/--card-my (per card), --rail-mx/--rail-my (sidebar), --theater-mx/--theater-my (Sankey) as CSS custom properties. Card ::before pseudo-elements use radial gradients positioned at these coordinates.


Tailwind color mapping (hybrid):


Theme-aware (CSS vars): accent, surface, positive, negative, warning — change between light/dark.
Static (hex): pearl, ink, theater, flow — fixed values.

Key CSS constructs:


.card — Base glassmorphism card with backdrop-filter, ::before mouse glow, ::after glass shine.
.card-hero / .card-accounts / .card-credit / .card-insight — Semantic variants with themed borders and glows.
.sankey-theater — Dark recessed container with ambient wash overlays and luminous top-edge highlight.
.period-toggle-track — iOS-style sliding thumb via transform: translateX(calc(var(--active-idx) * 100%)).
.theme-switching — Nuclear option: forces all backdrop-filter, transition-*, animation-* to 0/none via !important.

6.6 Utility Functions ($lib/utils.js)

Function	Description
formatCurrency(value, decimals?)	US currency. Checks privacyMode → returns $••••••• if active.
formatCompact(value)	$1.2K, $45.6K via Intl.NumberFormat compact notation. Privacy-aware.
formatDate(dateStr)	"2024-03-15" → "Mar 15, 2024". Appends T00:00:00 to prevent timezone shift.
formatDayHeader(dateStr)	"Today", "Yesterday", or "Monday, Mar 15".
formatMonth(monthStr)	"2024-03" → "March 2024".
formatPercent(value)	"12.3%".
relativeTime(isoString)	"just now", "5m ago", "2h ago", "3d ago".
groupTransactionsByDate(txns)	Groups by date, sorts newest-first.
computeDelta(current, previous)	Percentage change. null if previous is 0.
getGreeting()	Time-of-day greeting (morning/afternoon/evening/night).
computeTrailingSavingsRate(monthly, window?)	Trailing N-month savings rate + delta vs previous window. Default window: 3.
detectRecurring(txns, limit?)	Heuristic recurring detection (separate from backend's recurring.py). 35% std dev threshold, 14-62 day interval range.
buildCashFlowForecast(balance, dailySpend, dailyIncome, days?)	Projects balance for N days (default 14).
CATEGORY_COLORS	18 categories → hex colors.
CATEGORY_ICONS	18 categories → Material Symbols icon names.
springCount(node, params)	Svelte action: animated number count-up. 1s duration, 6% overshoot at 70% progress.


7. Data Model

7.1 SQLite Tables

profiles

Column	Type	Constraints	Notes
id	TEXT	PRIMARY KEY	Lowercase first name (e.g., "karthik")
name	TEXT	NOT NULL	Display name
created_at	TEXT	DEFAULT CURRENT_TIMESTAMP	

accounts

Column	Type	Constraints	Notes
id	TEXT	PRIMARY KEY	Teller account ID
name	TEXT		Institution + account name
institution	TEXT		Bank name
account_type	TEXT		depository, credit, loan, investment
subtype	TEXT		Teller subtype
currency	TEXT		
current_balance	REAL		Available or ledger (depends on type)
available_balance	REAL		
ledger_balance	REAL		
last_synced	TEXT		ISO timestamp
is_active	INTEGER	DEFAULT 1	
profile_id	TEXT	REFERENCES profiles(id)	
enrollment_id	TEXT		

categories

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
name	TEXT	UNIQUE NOT NULL	
is_active	INTEGER	DEFAULT 1	
expense_type	TEXT	DEFAULT 'variable'	fixed, variable, non_expense
expense_type_source	TEXT	DEFAULT 'system'	system or user

transactions

Column	Type	Constraints	Notes
id	TEXT	PRIMARY KEY	Teller transaction ID
account_id	TEXT	REFERENCES accounts(id)	
date	TEXT		YYYY-MM-DD
description	TEXT		Sanitized
raw_description	TEXT		Original from Teller
amount	REAL		Negative = expense, positive = income/refund
category	TEXT		Assigned category
original_category	TEXT		Pre-user-override category
confidence	TEXT		rule-high, rule-medium, llm, user, fallback
categorization_source	TEXT		system, user, ai, rule, fallback
type	TEXT		Teller type (card_payment, ach, etc.)
status	TEXT		pending, posted
counterparty_name	TEXT		
counterparty_type	TEXT		person, organization
merchant_name	TEXT		From enrichment
merchant_domain	TEXT		
merchant_industry	TEXT		
merchant_city	TEXT		
merchant_state	TEXT		
enriched	INTEGER	DEFAULT 0	Boolean
is_excluded	INTEGER	DEFAULT 0	Boolean
profile_id	TEXT	REFERENCES profiles(id)	
created_at	TEXT		
updated_at	TEXT		
rule_override	TEXT		Set when LLM overrides a rule suggestion

category_rules

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
pattern	TEXT	NOT NULL	LIKE pattern for description matching
category	TEXT	NOT NULL	Target category
source	TEXT	DEFAULT 'system'	system or user
priority	INTEGER	DEFAULT 0	Higher = checked first
is_active	INTEGER	DEFAULT 1	
created_at	TEXT		

net_worth_history

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
date	TEXT	NOT NULL	YYYY-MM-DD
total_assets	REAL		
total_owed	REAL		
net_worth	REAL		
profile_id	TEXT	DEFAULT 'household'	Per-profile + aggregate
UNIQUE		(date, profile_id)	INSERT OR REPLACE

copilot_conversations

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
question	TEXT		User's natural language query
sql_generated	TEXT		Generated SQL
result	TEXT		Truncated to 5000 chars
operation	TEXT		read, write_preview, write_executed, error
profile_id	TEXT		
created_at	TEXT		

subscription_seeds

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
merchant_name	TEXT	NOT NULL	
pattern	TEXT	NOT NULL	LIKE pattern for bank descriptions
category	TEXT		
frequency	TEXT		monthly, quarterly, annual, etc.
source	TEXT	DEFAULT 'system'	system (from JSON) or user (confirmed/dismissed)
is_active	INTEGER	DEFAULT 1	0 = dismissed
created_at	TEXT		
updated_at	TEXT		

enrichment_cache

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
pattern_key	TEXT	UNIQUE NOT NULL	Normalized description key
merchant_name	TEXT		
domain	TEXT		
industry	TEXT		
city	TEXT		
state	TEXT		
source	TEXT	DEFAULT 'trove'	trove or seed
hit_count	INTEGER	DEFAULT 1	
last_seen	TEXT		
created_at	TEXT		

teller_category_map

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
teller_category	TEXT	NOT NULL	Teller's category string
folio_category	TEXT		Mapped Folio category. NULL = no useful signal.
source	TEXT	DEFAULT 'system'	system or user. User overrides win (ORDER BY source ASC).
UNIQUE		(teller_category, source)	

enrolled_tokens

Column	Type	Constraints	Notes
id	INTEGER	PRIMARY KEY AUTOINCREMENT	
profile	TEXT	NOT NULL	
token_encrypted	TEXT	NOT NULL	Fernet-encrypted (or plaintext if no key)
institution	TEXT		
owner_name	TEXT		
enrollment_id	TEXT		
is_active	INTEGER	DEFAULT 1	
created_at	TEXT		
UNIQUE		(profile, token_encrypted)	Duplicate prevention

7.2 Key In-Memory / Derived Data Structures

Sankey graph (built by SankeyChart.svelte buildGraph()):


Copy Code
{
  nodes: [
    { name: "Income", type: "income" },
    { name: "Expenses", type: "trunk" },
    { name: "Food & Dining", type: "expense" },
    { name: "From Balance", type: "from_balance" },  // if shortfall
    { name: "To Balance", type: "to_balance" },      // if surplus
    { name: "Savings", type: "savings" },
    ...
  ],
  links: [
    { source: 0, target: 1, value: 5000 },  // income → expenses
    ...
  ]
}

realValues map stores actual display amounts separately from layout-balanced values.


Recurring detection result:


Copy Code
{
  items: [
    { merchant, display_name, amount, frequency, next_date,
      confidence, source, category, price_change?, is_active },
    ...
  ],
  count: int,
  total_monthly: float,
  total_annual: float
}

Dashboard bundle (API response):


Copy Code
{
  summary: { income, expenses, refunds, net_spending, savings, net_flow,
             savings_rate, total_assets, total_owed, net_worth, ... },
  accounts: [ { id, name, institution, account_type, current_balance, ... } ],
  monthly: [ { month, income, expenses, refunds, net, savings } ],
  categories: [ { category, gross, refunds, net, percent, expense_type } ],
  net_worth_series: [ { date, net_worth } ]
}

Copilot pending SQL store (in-memory):


Copy Code
_pending: {
  "nonce_string": {
    "sql": "UPDATE transactions SET ...",
    "profile": "karthik",
    "ts": 1712345678.0
  }
}

5-minute TTL. Pop-on-read (one-time retrieval).



8. LLM Integration

8.1 Claude Haiku for Categorization (categorizer.py)

Model: claude-3-haiku-20240307


When called: Phase 2 of categorization pipeline — for transactions where deterministic rules produced rule-medium (has suggestion) or no match.


Prompt structure:


System message: "You are a financial transaction categorizer..."
Available categories with semantic descriptions (e.g., "Food & Dining — restaurants, bars, cafes, fast food" vs. "Groceries — supermarkets, grocery stores").
Transaction lines, one per item: {index}. {description} | {amount_masked} | {merchant_name?} | {domain?} | {industry?} | [suggestion: {category}]
Expected response: JSON array of [{index, category}].

PII masking (before sending):


Amounts → -$XXX / +$XXX
Counterparty names → [person] / [organization]
Profile names → not sent

Batching: 50 transactions per batch. 1-second delay between batches.


Fallback: If LLM fails → use rule suggestion (if any) → "Other".


Feature toggle: ENABLE_LLM_CATEGORIZATION=false disables entirely.


8.2 Claude for Copilot SQL Generation (copilot.py)

Model: claude-3-haiku-20240307


Prompt structure:


System message: "You are a SQL assistant for a personal finance database..."
Schema context: all table definitions, available categories, anonymized profile list, today's date.
Rules: profile filtering semantics ("all_profiles = no WHERE clause"), allowed write operations, response format.
User question.

Expected responses:


SELECT ... for reads
UPDATE ... or INSERT ... for writes
CANNOT: <explanation> for unanswerable questions

Profile anonymization: Real names → profile_1, profile_2, all_profiles (household). De-anonymized before execution.


8.3 Claude for Natural Language Answers (copilot.py)

When called: After a read query executes, results are sent to Claude for summarization.


PII masking: sanitize_rows_for_llm() — amounts → $XXX, balances masked, raw_description stripped, counterparty names masked.


Prompt instructions: "Do not invent dollar amounts (they're masked). Do not reveal personal names."


Fallback: Simple formatting if Claude call fails.


8.4 API Calls

All Claude calls use raw httpx (not the anthropic SDK). SSL verification via certifi. Retry logic not explicit for Anthropic (only Teller has _request_with_retry).



9. Teller API Integration

9.1 Authentication

mTLS (mutual TLS): Teller requires client certificates for API access.


Certificate: TELLER_CERT_PATH → mounted into Docker container at /certs/teller-cert.pem.
Private key: TELLER_KEY_PATH → /certs/teller-key.pem.
Mounted read-only (:ro) in docker-compose.yml.
httpx.Client initialized with cert=(cert_path, key_path).

9.2 Token Management

Sources:


Environment variables: Any var ending in _TOKEN (excluding ANTHROPIC_API_KEY, TELLER_TOKEN_PREFIX, TOKEN_ENCRYPTION_KEY). Profile name derived from prefix before _TOKEN (e.g., JOHN_BOFA_TOKEN → profile "john").
Token store: enrolled_tokens table (Teller Connect enrollments, Fernet-encrypted).

Hot-reload: bank.reload_tokens_and_profiles() merges both sources. Called after new enrollments.


9.3 Teller Connect (Dynamic Enrollment)

Requires TELLER_APPLICATION_ID and TELLER_ENVIRONMENT env vars. Frontend loads the Teller Connect JS SDK dynamically. On success, POST /api/enroll receives the access token.


9.4 Data Fetching

Accounts: GET https://api.teller.io/accounts per token. Tagged with profile + access_token.


Transactions: GET https://api.teller.io/accounts/{id}/transactions with cursor-based pagination.


Page size: 100 (assumed Teller default).
Cursor: from_id = last transaction's ID.
Stop conditions: page < 100 results, TELLER_MAX_PAGES (50), TELLER_MAX_TRANSACTIONS (5000).
1-second delay between pages.

Balances: GET https://api.teller.io/accounts/{id}/balances.


Credit/loan accounts use ledger balance; others use available.

Identity: GET https://api.teller.io/identity. Used to resolve owner name during enrollment.


9.5 Teller → Folio Data Transformation

Teller Field	Folio Transformation
CC purchase (positive amount)	Flipped to negative (expense)
CC refund (negative amount)	Flipped to positive
payment type transactions	Filtered out (card-side duplicates)
details.counterparty	Extracted to counterparty_name, counterparty_type
description	Sanitized (IDs, confirmation numbers, INDN: names stripped). Original preserved as raw_description.
details.category	Mapped via teller_category_map table (medium confidence hint for categorizer)
Account type depository/credit/loan/investment	Mapped to internal types. is_credit derived.

9.6 Webhook Handling

❓ No webhook handling is documented or visible in the codebase. All data is pull-based (triggered by /api/sync).



10. Configuration & Environment

10.1 Backend .env

Variable	Required	Purpose	Example
TELLER_CERT_PATH	Yes	Path to Teller mTLS certificate	/certs/teller-cert.pem
TELLER_KEY_PATH	Yes	Path to Teller mTLS private key	/certs/teller-key.pem
TELLER_APPLICATION_ID	No	Teller Connect app ID (for dynamic enrollment)	app_abc123
TELLER_ENVIRONMENT	No	sandbox, development, or production	sandbox
TOKEN_ENCRYPTION_KEY	Recommended	Fernet key for encrypting stored tokens	(generate via python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
FINFLOW_API_KEY	Recommended	Backend API key. Auto-generated if absent.	your-secret-key
VITE_API_KEY	Yes	Must match FINFLOW_API_KEY. Passed to frontend build.	your-secret-key
ANTHROPIC_API_KEY	No	Powers AI categorization + copilot.	sk-ant-...
ENABLE_LLM_CATEGORIZATION	No	true (default) or false.	true
TROVE_API_KEY	No	Merchant enrichment.	trove_...
TROVE_USER_SEED	No	Stable seed for anonymous Trove user ID.	my-seed-string
ENABLE_TROVE	No	true (default) or false.	true
{FIRSTNAME}_{BANKNAME}_TOKEN	Conditional	Manual Teller token. Prefix determines profile.	JOHN_BOFA_TOKEN=test_tok_abc
CORS_ORIGINS	No	Comma-separated allowed origins.	http://localhost:3000
BACKEND_PORT	No	Host port for backend. Default 8000.	8000
FRONTEND_PORT	No	Host port for frontend. Default 3000.	3000
DB_FILE	No	SQLite path. Default ./data/finflow.db.	/data/finflow.db
LOG_LEVEL	No	Logging level. Default INFO.	DEBUG
TELLER_MAX_PAGES	No	Max pagination pages per account. Default 50.	50
TELLER_MAX_TRANSACTIONS	No	Max transactions per account. Default 5000.	5000
TROVE_CACHE_MAX_SIZE	No	In-memory enrichment cache size. Default 1000.	1000
BULK_THRESHOLD	No	Trove bulk vs single threshold. Default 0 (always single).	0
COPILOT_MAX_WRITE_ROWS	No	Max rows affected by copilot write. Default 5000.	5000

10.2 Frontend .env (Build-Time)

Variable	Required	Purpose	Notes
VITE_API_KEY	Yes	Baked into JS bundle at build time	Must match FINFLOW_API_KEY. Visible in browser dev tools.
VITE_TELLER_APP_ID	No	Teller Connect application ID	Build-time only.
VITE_TELLER_ENVIRONMENT	No	Teller environment	Build-time only.

⚠️ Critical: VITE_* variables are build-time only. Changing them requires docker compose build, not just restart.


10.3 Frontend Runtime (Set by Docker Compose)

Variable	Purpose	Default
ORIGIN	SvelteKit CSRF protection. Must match browser URL.	http://localhost:${FRONTEND_PORT:-3000}
BACKEND_URL	Server-side proxy target. Docker DNS.	http://backend:8000
PORT	Node server listen port.	3000
HOST	Node server bind address.	0.0.0.0
DOCKER	Triggers adapter-node in svelte.config.js.	true


11. Docker & Deployment

11.1 Service Topology

Copy Code
docker-compose.yml
├── backend
│   ├── Build: ./backend/Dockerfile
│   ├── Port: ${BACKEND_PORT:-8000}:8000
│   ├── Volumes: ./data:/data, ./certs:/certs:ro
│   ├── env_file: .env
│   ├── Health: /healthz/health (every 30s, 10s start period)
│   └── Restart: unless-stopped
│
└── frontend
    ├── Build: ./frontend/Dockerfile (multi-stage)
    ├── Port: ${FRONTEND_PORT:-3000}:3000
    ├── Build args: VITE_API_KEY, VITE_TELLER_APP_ID, VITE_TELLER_ENVIRONMENT
    ├── Runtime env: ORIGIN, BACKEND_URL
    ├── depends_on: backend (condition: service_healthy)
    ├── Health: http://localhost:3000/ (every 30s, 5s start period)
    └── Restart: unless-stopped

11.2 Backend Dockerfile

dockerfile
Copy Code
FROM python:3.12-slim
# 1. Install CA certificates (for Teller/Anthropic HTTPS)
RUN apt-get update && apt-get install -y ca-certificates && update-ca-certificates && rm -rf /var/lib/apt/lists/*
# 2. Install Python deps (layer caching: requirements.txt before source)
COPY requirements.txt .
RUN pip install -r requirements.txt
# 3. Copy source
COPY . .
# 4. Belt-and-suspenders: remove .env (should be .dockerignored anyway)
RUN rm -f .env .env_bkp
# 5. Create /data fallback directory
RUN mkdir -p /data
# 6. Health check (uses urllib, no curl needed)
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz/health')" || exit 1
# 7. Run
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

11.3 Frontend Dockerfile (Multi-Stage)

Builder stage:


dockerfile
Copy Code
FROM node:20-slim AS builder
ARG VITE_API_KEY VITE_TELLER_APP_ID VITE_TELLER_ENVIRONMENT
ENV VITE_API_KEY=$VITE_API_KEY VITE_TELLER_APP_ID=$VITE_TELLER_APP_ID VITE_TELLER_ENVIRONMENT=$VITE_TELLER_ENVIRONMENT
ENV DOCKER=true
COPY package.json package-lock.json ./
RUN npm ci || npm install
COPY . .
RUN npm run build

Production stage:


dockerfile
Copy Code
FROM node:20-slim
COPY --from=builder build/ build/
COPY --from=builder package.json .
COPY --from=builder node_modules/ node_modules/
ENV PORT=3000 HOST=0.0.0.0
CMD ["node", "build"]

11.4 Spinning Up From Scratch

bash
Copy Code
# 1. Clone repo
git clone <repo-url> && cd folio

# 2. Copy and configure environment
cp .env.example .env
# Edit .env: add Teller certs, API keys, tokens

# 3. Place Teller certificates
mkdir -p certs
cp /path/to/teller-cert.pem certs/
cp /path/to/teller-key.pem certs/

# 4. Create data directory
mkdir -p data

# 5. Build and run
docker compose up --build

# 6. Access
# Frontend: http://localhost:3000
# Backend:  http://localhost:8000 (API only)

11.5 .dockerignore

Excludes: **/.env*, data/, **/node_modules, **/.venv, **/__pycache__, **/*.pyc, **/build, **/.svelte-kit, .git, **/README.md, .DS_Store, Thumbs.db.



12. Key Architectural Decisions & Tradeoffs

SQLite over PostgreSQL

Chosen: SQLite with WAL mode, single-file persistence.
Alternative: PostgreSQL in a third Docker container.
Why: Target use case is a single household (1-5 users, low concurrent writes). SQLite requires zero operational overhead — no connection pooling, no credentials, no backup scripts beyond copying a single file. WAL mode allows concurrent reads during writes. The trade-off is no multi-process write concurrency, which constrains the backend to single-worker Uvicorn. If the app needed to scale to multi-user SaaS, PostgreSQL would be the natural migration path.


Raw httpx over Anthropic SDK / LangChain

Chosen: Direct HTTP calls to api.anthropic.com via httpx.
Alternative: anthropic Python SDK, or LangChain/LlamaIndex framework.
Why: Minimizes dependencies and abstraction layers. The app makes exactly two types of LLM calls (categorization batches and copilot queries) with carefully crafted prompts. A framework would add overhead without value. The anthropic SDK is not even listed in requirements.txt — the raw HTTP approach gives full control over request construction, retry logic, and error handling.


SvelteKit over Next.js / Remix

Chosen: SvelteKit 2 with Svelte 4.
Alternative: Next.js (React), Remix, Nuxt.
Why: Svelte's reactive model is well-suited to a data-heavy dashboard where many values need to update in concert (net worth, income, expenses, savings rate, Sankey diagram all react to profile/period changes). The compiled-away framework means smaller bundle sizes. SvelteKit's load() functions and depends()/invalidate() pattern enable the prefetch-with-fallback strategy used on the dashboard.


Custom SVG Charts over Charting Library

Chosen: Hand-rolled SVG with reactive Svelte computations. d3-sankey for Sankey layout only.
Alternative: Chart.js, Recharts, Highcharts.
Why: Full visual control over glow effects, gradients, animations, the glassmorphism aesthetic, and the Balance-as-Reservoir Sankey model. No charting library supports the "dark island" theming or mouse-tracking glow effects. The cost is significantly more code — the Sankey component alone has ~500+ lines.


Local Categorization Caching (Three-Phase Pipeline)

Chosen: Deterministic rules first, LLM only for ambiguous transactions.
Alternative: Send all transactions to LLM.
Why: Cost and latency. Claude Haiku charges per token. Interest payments, CC payments, savings transfers, bank fees are categorized with 100% accuracy by simple rules at zero cost. The three-phase approach (rules → user rules → LLM) means the LLM handles only ~30-50% of transactions on average, with the percentage decreasing over time as user rules accumulate.


Multi-Profile as Frontend Abstraction

Chosen: household profile = no profile filter (backend returns all data). Individual profiles filter by profile_id.
Alternative: Dedicated multi-tenant database isolation.
Why: The household aggregate view is the most common use case (combined family finances). Implementing it as "no filter" is trivially simple and performant. Individual profile views are secondary and achieved by adding WHERE profile_id = ?. No data duplication or complex multi-tenant logic needed.


Privacy Sanitization Before LLM

Chosen: Amounts masked (-$XXX), counterparty names replaced ([person]), profile names aliased (profile_1), raw_description stripped entirely from copilot results.
Alternative: Send real data to the LLM.
Why: Core design philosophy — even though the user controls the LLM API key, the principle of data minimization applies. The LLM doesn't need exact dollar amounts to categorize "NETFLIX.COM $15.99" as a subscription. It doesn't need real names to understand P2P transfers. The copilot's natural language answers explicitly state amounts as "$XXX" rather than fabricating numbers.


Budget Data in localStorage

Chosen: Budget values stored client-side only, keyed by finflow_budgets_{profileId}.
Alternative: Backend budgets table in SQLite.
Why: ❓ This appears to be an intentional simplification — budgets are a lightweight, ephemeral preference rather than critical financial data. The trade-off is no cross-device sync. If the user accesses Folio from a different browser, budget data is lost.


Confirmation-ID Pattern for Copilot Writes

Chosen: Server stores validated SQL keyed by a nonce. Client sends nonce to confirm. SQL never leaves the server.
Alternative: Client sends back the SQL for execution.
Why: Prevents SQL injection via client manipulation. Even if a user intercepts the response and modifies the SQL before sending it back, the server only executes the original validated SQL associated with the nonce. This is an explicit security fix documented as FIX C3/M2.


npm min-release-age=7

Chosen: .npmrc policy that rejects npm packages published less than 7 days ago.
Alternative: No restriction.
Why: Supply-chain security. Protects against compromised packages that are published and quickly removed, or typosquatting packages that get brief traction before detection. Rare in frontend projects — indicates security-conscious development.



13. Known Limitations, TODOs & Improvement Vectors

Security

No non-root user in either Dockerfile. Both containers run as root. Adding USER node (frontend, already exists in node:20-slim) and RUN useradd -m appuser && USER appuser (backend) would limit blast radius.
VITE_API_KEY is visible in browser bundle. Ensure it has minimal permissions. It should never be a server-side secret.
No HTTPS termination. ORIGIN is http://. Production deployment behind a TLS-terminating reverse proxy (Caddy, Nginx) requires updating ORIGIN to the public HTTPS URL.
Rate limiting is in-memory and per-process. Doesn't persist across restarts. All users behind same NAT share a limit.
Copilot pending SQL store is in-memory. Lost on server restart. 5-minute TTL means users must confirm quickly.
Token encryption is optional. If TOKEN_ENCRYPTION_KEY is not set, Teller tokens are stored in plaintext.

Performance

Single-process Uvicorn. No --workers flag. CPU-bound work blocks the event loop. For production, gunicorn with UvicornWorker would enable multi-process concurrency — but conflicts with SQLite's write-locking model.
handleCardMouseMove in +layout.svelte runs querySelectorAll with a broad CSS selector on every animation frame. Could become expensive with many cards. Should cache the NodeList or use MutationObserver.
Transaction metadata fetch hack. Transactions page fetches up to 1000 transactions on mount just to extract unique months and account names for filter dropdowns. A dedicated /api/transactions/metadata endpoint would be far more efficient.
get_data() (full dataset load) is deprecated but still used by /api/analytics/recurring. The recurring detection algorithm requires cross-transaction analysis that is difficult to express in SQL, but this could be improved with window functions or pre-computed aggregates.
No resource limits in Docker Compose. No mem_limit/cpus defined for production safety.
node_modules copied from builder to production stage includes devDependencies. Adding RUN npm prune --production before the production COPY would reduce image size.

Data Integrity

NON_SPENDING_CATEGORIES is defined as a set in main.py and a tuple in data_manager.py. Must be kept in sync manually.
Net worth series derives historical values by subtracting cumulative transaction changes from current balances. Assumes current balances are accurate and all transactions are present.
SQLite volume path ./data is relative to docker-compose.yml. Fragile in CI environments where the working directory may differ.
_VALID_PROFILES is a global mutable set requiring manual invalidation after enrollments.
⚠️ The frontend utils.js detectRecurring() is separate from and different than the backend recurring.py RecurringDetector. The frontend version uses simpler heuristics (35% std dev, 14-62 day range). It is unclear where this frontend version is used vs. the backend endpoint. ❓

Features

No budget persistence to backend. Budgets don't sync across devices.
No webhook-based data refresh. All data is pull-based (manual sync).
No chat history persistence. Copilot conversations are logged server-side but not reloaded in the UI.
No test file exclusion in .dockerignore. Test directories could be added to reduce image size.
No TypeScript. API response shapes are not statically verified; relies on runtime handling and fallback defaults.
YTD transaction filtering on the transactions page uses client-side approach with a __ytd__ sentinel and limit=1000. Pagination doesn't work correctly for YTD.
Recurring detection thresholds are hardcoded. Quarterly bills (90 days) and weekly charges (7 days) fall outside the 14-62 day detection range.

CSS / Visual

Dark stage islands create significant CSS maintenance burden with many :root:not(.dark) overrides.
color-mix(in srgb, ...) requires modern browser support (Chrome 111+, Firefox 113+, Safari 16.4+).
z-index: 99999 on transaction filter dropdowns is a brute-force stacking context fix.
Multiple mojibake characters in CSS comments suggest encoding issues during file transfer.
No explicit Docker network. Compose creates a default bridge. Declaring a named network (e.g., folio-net) would improve clarity and multi-service extensibility.

Build / Dev

npm ci || npm install fallback in frontend Dockerfile could install non-deterministic dependency versions if lockfile is missing. In CI, failing hard on missing lockfile would be safer.
No Makefile or convenience scripts. A Makefile target that enforces --build when .env changes would prevent the common confusion of stale VITE_* variables.
TellerConnect SDK script tag is never cleaned up on component destruction.


14. Glossary

Term	Definition
Profile	A financial identity within Folio, typically one person in a household (e.g., "karthik", "sarah"). Derived from Teller account owner's first name. "Household" is a virtual aggregate of all profiles.
Enrollment	The process of linking a new bank account via the Teller Connect widget. Results in an encrypted access token stored in enrolled_tokens.
Sanitization	The first step in the categorization pipeline (sanitizer.py). Normalizes credit card amount signs, strips PII/IDs from descriptions, filters duplicate payment-type transactions.
Enrichment	Adding structured merchant metadata (name, domain, industry, location) to a transaction via the Trove API (enricher.py). Uses a three-tier cache (memory → SQLite → API).
Categorization	Assigning a spending category to a transaction. Two-phase: deterministic rules (Phase 1) then LLM (Phase 2). Confidence levels: rule-high, rule-medium, llm, user, fallback.
User Rule	A categorization rule created automatically when a user manually re-categorizes a transaction. Stored in category_rules with source='user'. Checked first (highest priority) in the pipeline.
Subscription Seed	A known subscription service pattern (from subscription_seeds.json). Used for Layer 1 of recurring detection. Can be source='system' (from JSON) or source='user' (confirmed/dismissed by user).
Dark Stage Island	A light-mode design concept where premium cards (hero, Sankey theater, waterfall) render with dark backgrounds on the otherwise light page, creating visual hierarchy. Requires dual CSS variable sets.
Glass Rail	The persistent navigation sidebar using glassmorphism (backdrop-filter blur + semi-transparent background).
Balance-as-Reservoir	The Sankey chart's model for handling income/expense imbalances. If expenses exceed income, a "From Balance" source node appears. If income exceeds expenses, a "To Balance" sink node appears.
Confirmation ID (Nonce)	A server-generated unique identifier for a pending copilot write operation. The client sends this ID (not raw SQL) to confirm execution. Security measure against SQL injection.
FOUC	Flash of Unstyled Content. Prevented by an inline script in app.html that applies the correct theme class before CSS loads.
Direct Flow Categories	['Savings Transfer', 'Personal Transfer']. Money moving between accounts, not actual spending. Excluded from expense totals in dashboard metrics.
Non-Spending Categories	Superset including Income, Savings Transfer, CC Payment, Personal Transfer. Excluded from expense aggregation in SQL queries.
Privacy Mode	Client-side toggle that replaces all monetary values with $••••••• via the formatCurrency() utility. Persisted in localStorage.
Expense Type	Category classification: fixed (recurring bills), variable (discretionary spending), non_expense (transfers, income). Editable by users, stored in categories table.
mTLS	Mutual TLS — both client and server present certificates. Required by Teller API for authentication. Folio's certs are mounted read-only into the backend container.
Dashboard Bundle	A single API response (GET /api/dashboard-bundle) aggregating summary, accounts, monthly analytics, category analytics, and net worth series. Replaces 5 separate API calls.

