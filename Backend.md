Folio Backend — Detailed Module Documentation


backend_main.py

Purpose

This is the FastAPI application entry point — the central routing and orchestration layer for the entire Folio backend. It defines all HTTP API endpoints, configures middleware (CORS, rate limiting, trusted hosts), manages application startup/shutdown lifecycle, and wires together the data layer, categorization engine, authentication, and copilot services. Every client request flows through this file.


Key Dependencies

Dependency	Why
fastapi, starlette	Web framework, middleware (CORS, TrustedHost)
pydantic	Request body validation models
auth (internal)	API key verification, rate limiting middleware
bank (internal)	Teller config validation, profile registry, client lifecycle
data_manager (internal)	All data read/write operations (SQL-backed)
categorizer (internal)	Active category list retrieval
database (internal)	DB initialization, session management, connection cleanup
copilot (internal)	NLP-to-SQL query engine
recurring (internal)	Subscription/recurring charge detection
token_store (internal)	Teller Connect enrollment persistence

Core Functions / Classes / Exports

health_check() → {"status": "ok"}

What it does: Unauthenticated health endpoint mounted as a sub-application at /healthz/health.
Notable logic: Mounted as a separate FastAPI sub-app to bypass all global dependencies=[Depends(verify_api_key)]. Docker healthcheck should target /healthz/health. There's also a convenience /health on the main app, but it does require auth due to FastAPI's global dependency behavior (decorator-level dependencies=[] doesn't override app-level).

startup()

What it does: Runs on FastAPI startup event. Validates Teller certificate config, initializes the database schema, syncs subscription seeds from JSON, and populates the enrichment cache from seeds.
Side effects: Creates/migrates all SQLite tables, seeds default categories, system rules, and Teller category mappings.

shutdown()

What it does: Closes thread-local DB connections and all cached Teller httpx clients.

validate_profile(profile) → str | None

What it does: FastAPI dependency that validates the profile query parameter against known profiles.
Notable logic: Uses a lazily-initialized _VALID_PROFILES set built from bank.PROFILES.keys(). Returns 400 for unknown profiles. The cache is invalidated via _invalidate_profile_cache() after new enrollments.

accounts(profile, db) → list[dict]

What it does: GET /api/accounts — returns filtered accounts via get_accounts_filtered().

transactions(month, category, account, search, profile, limit, offset, db) → dict

What it does: GET /api/transactions — paginated, filtered transaction listing via get_transactions_paginated().
Inputs: All filters are optional query params. limit capped at 1000, offset >= 0.
Outputs: {"data": [...], "total_count": int, "limit": int, "offset": int}

update_category(tx_id, body: CategoryUpdate) → dict

What it does: PATCH /api/transactions/{tx_id}/category — user category override. Delegates to update_transaction_category() which also auto-creates rules and retroactively recategorizes.
Notable logic: Allows any non-empty category string (new categories are auto-created).

dashboard_bundle(nw_interval, profile, db) → dict

What it does: GET /api/dashboard-bundle — single-request dashboard data aggregation. Returns summary, accounts, monthly analytics, category analytics, and net-worth time series.
Notable logic: Replaces 5 separate API calls. All computation is SQL-level aggregation.

get_recurring_transactions(profile) → dict

What it does: GET /api/analytics/recurring — detects recurring/subscription charges.
Notable logic: This is the only endpoint that uses get_data() (full dataset load) because the recurring detection algorithm requires cross-transaction analysis (interval detection, amount consistency, merchant grouping) that can't be efficiently expressed as SQL.

confirm_subscription(body, profile, db) / dismiss_subscription(body, profile, db)

What it does: POST /api/subscriptions/confirm and /dismiss — user feedback on detected recurring charges. Creates/updates user-sourced seeds in subscription_seeds table.

copilot_ask(body, profile) → dict

What it does: POST /api/copilot/ask — sends natural language question to the NLP-to-SQL copilot.

copilot_confirm(body, profile) → dict

What it does: POST /api/copilot/confirm — executes a previously previewed write operation. Client sends a confirmation_id (not raw SQL) which is resolved from a server-side store. This is a security fix (FIX M2/C3).

enroll_account(req: EnrollRequest) → dict

What it does: POST /api/enroll — handles new Teller Connect enrollments. Validates the token, resolves owner identity, persists encrypted token, hot-reloads in-memory registries, triggers sync.
Notable logic: Profile name is derived from identity first name (lowercased). Falls back to "primary" if name is too long, contains spaces, or identity lookup fails. After enrollment, calls reload_tokens_and_profiles() and _invalidate_profile_cache().

deactivate_enrollment(body) → dict

What it does: POST /api/enrollments/deactivate — soft-deletes an enrollment, reloads tokens.

Data Flow

Client sends HTTP request with X-API-Key header
verify_api_key dependency validates the key (timing-safe comparison)
rate_limit_middleware checks per-IP rate limits
Route handler receives validated params (profile validated, DB session injected)
Handler delegates to data_manager functions (SQL queries) or copilot/recurring for complex operations
Response returned as JSON

Integration Points

Called by: Frontend SvelteKit app via REST API
Calls: data_manager (all data ops), bank (config, token reload), database (init, sessions), categorizer (category list), copilot (NLP), recurring (subscription detection), token_store (enrollment CRUD), auth (key verification, rate limiting)

Known Quirks / Design Notes

_VALID_PROFILES is a global mutable set that needs manual invalidation after enrollments — could be a source of bugs if another code path adds profiles without calling _invalidate_profile_cache().
The /health endpoint on the main app misleadingly appears unauthenticated (has dependencies=[]) but actually requires auth due to FastAPI's global dependency behavior. Docker should use /healthz/health.
NON_SPENDING_CATEGORIES is defined as a set here and as a tuple in data_manager.py — they must be kept in sync manually (noted in comments).
_filter_by_profile() is a Python-level filter used only by the /api/analytics/recurring endpoint; all other endpoints push profile filtering into SQL.
CORS origins, trusted hosts, and rate limits are all configurable via environment variables.


backend_data_manager.py

Purpose

This is the primary data access layer — it handles all reads and writes to the SQLite database, as well as syncing fresh data from the Teller API. It replaces an older JSON-cache approach with SQL-backed operations. The module is organized into three sections: targeted SQL queries (preferred), full-dataset reads (legacy, for recurring detection only), and sync/write operations. All analytics computations (summaries, monthly/category breakdowns, merchant insights, net-worth series) are performed via SQL-level aggregation.


Key Dependencies

Dependency	Why
bank (internal)	get_all_accounts_by_profile, get_transactions, get_balances — Teller API calls
categorizer (internal)	categorize_transactions — runs the categorization pipeline on new transactions
database (internal)	get_db, dicts_from_rows, _extract_merchant_pattern — DB connection and helpers
threading	_lock for thread-safe sync operations
dotenv	Environment variable loading

Core Functions / Classes / Exports

_escape_like(pattern) → str

What it does: Escapes SQL LIKE wildcards (%, _, \) in user-provided search patterns.
Notable logic: Uses \\ as the ESCAPE character, which is declared in the SQL query via ESCAPE '\\'.

get_accounts_filtered(profile, conn) → list[dict]

What it does: Fetches active accounts with optional profile filter pushed into SQL.
Outputs: List of account dicts with is_credit converted to Python bool. Includes derived is_credit field computed as CASE WHEN account_type IN ('credit', 'loan') THEN 1 ELSE 0 END.

get_transactions_paginated(month, category, account, search, profile, limit, offset, conn) → dict

What it does: Primary transaction listing endpoint. All filters are SQL WHERE clauses — no Python-level filtering.
Outputs: {"data": [...], "total_count": int, "limit": int, "offset": int}
Notable logic: Search uses UPPER(description) LIKE ? with escaped wildcards. Boolean fields (enriched, is_excluded) are converted from SQLite integers.

get_summary_data(profile, conn) → dict

What it does: Computes dashboard summary statistics (income, expenses, refunds, net spending, savings, net flow, savings rate, total assets, total owed, net worth, transaction/enriched counts) entirely via SQL aggregation.
Notable logic: Uses separate queries for each metric with NON_SPENDING_CATEGORIES exclusions. Income is category='Income' AND amount > 0. Expenses are amount < 0 AND category NOT IN (...). Refunds are amount > 0 AND category NOT IN (...) AND category != 'Income'. The net_spending = expenses - refunds formula means refunds reduce apparent spending.

get_monthly_analytics_data(profile, conn) → list[dict]

What it does: Monthly income/expense/refund/savings aggregation using SQL GROUP BY SUBSTR(date, 1, 7) with conditional aggregation (CASE WHEN).
Notable logic: NON_SPENDING_CATEGORIES params appear twice in the query (for expenses and refunds conditions), so they're concatenated twice in the params list. Net = income - expenses + refunds.

get_category_analytics_data(month, profile, conn) → list[dict]

What it does: Per-category spending breakdown. Computes gross expenses and refunds separately by category, then calculates net per category in Python.
Notable logic: Categories with net ≤ 0 (fully refunded) are excluded from results. Includes expense_type from the categories table (fixed/variable/non_expense).

get_merchant_insights_data(month, profile, conn) → list[dict]

What it does: Merchant-level spending breakdown using SQL aggregation on enriched transactions.
Notable logic: Only includes enriched = 1 transactions with non-empty merchant_name. Groups by merchant_name, ordered by total_spent DESC.

get_net_worth_series_data(interval, profile, conn) → list[dict]

What it does: Computes a running net-worth time series from transaction history.
Notable logic: This is the most complex query function. It:
Gets current account balances (assets vs. owed)
Fetches daily net transaction changes via SQL GROUP BY
Builds cumulative sum in Python (SQLite lacks window functions efficient enough)
Derives starting_nw = current_net_worth - total_cumulative
Samples at weekly (7-day) or biweekly (14-day) intervals
Always includes the last date as the final data point

get_dashboard_bundle_data(nw_interval, profile, conn) → dict

What it does: Aggregates all dashboard data in a single DB connection — summary, accounts, monthly, categories, net-worth series.
Notable logic: Reuses a single conn across all sub-queries for connection efficiency.

get_data(force_refresh) → dict

What it does: Loads ALL transactions and accounts into memory. Deprecated for general use.
Notable logic: Marked with ⚠️ deprecation warning. Exists only for /api/analytics/recurring which needs full cross-transaction analysis. Returns EMPTY_DATA if no transactions exist.

fetch_fresh_data(incremental) → dict

What it does: Core sync function — fetches from Teller API and writes to SQLite. Called only by /api/sync.
Inputs: incremental=True skips transactions already in DB (by ID).
Notable logic:
Thread-locked via _lock to prevent concurrent syncs
Iterates all profiles → all accounts → fetches transactions and balances
Filters out cached transaction IDs
Runs categorize_transactions() on new transactions only
Upserts accounts with internal type mapping (depository/credit/loan/investment)
Balance selection: credit/loan use ledger balance; others use available
After inserting transactions, takes a net worth snapshot via _snapshot_net_worth()

_insert_transaction(conn, tx)

What it does: Inserts a single categorized transaction into the database.
Notable logic: Derives categorization_source from confidence if not explicitly set. Resolves account_id by matching account_name + profile_id. Uses INSERT OR IGNORE to skip duplicates.

update_transaction_category(tx_id, new_category) → bool

What it does: User category override with automatic rule creation and retroactive recategorization.
Notable logic:
Updates the transaction, preserving original_category
Auto-creates a user rule from the merchant pattern (_extract_merchant_pattern)
If a user rule already exists for the pattern, updates it
Retroactively recategorizes past transactions matching the pattern (except those manually set by user)
Uses UPPER(description) LIKE ? with escaped patterns for retroactive updates

_snapshot_net_worth(conn, timestamp)

What it does: Records current net worth for each profile and household aggregate.
Notable logic: Uses INSERT OR REPLACE keyed on (date, profile_id) so multiple syncs on the same day just update the snapshot.

Data Flow

Sync path: fetch_fresh_data() → Teller API → categorize_transactions() → _insert_transaction() → SQLite
Read path: API endpoint → targeted query function → SQL → dicts → JSON response
Write path: update_transaction_category() → UPDATE transaction → INSERT/UPDATE rule → UPDATE matching transactions retroactively

Integration Points

Called by: main.py (all API endpoints), copilot.py (indirectly via imports)
Calls: bank (Teller API), categorizer (categorization pipeline), database (DB operations)

Known Quirks / Design Notes

NON_SPENDING_CATEGORIES is a tuple here but a set in main.py — must be kept in sync (comment notes this).
get_data() loads everything into memory and is explicitly deprecated with warnings.
The _lock is a module-level threading lock — only one sync can run at a time across all threads.
Net worth series calculation derives historical values by subtracting cumulative transaction changes from current balances — this assumes current balances are accurate and all transactions are present.
_check_user_rules() function exists but is never called (user rules are checked in categorizer.py Phase 1.6 instead). Dead code.


backend_categorizer.py

Purpose

This is the two-phase transaction categorization engine. Phase 1 applies deterministic rules (transaction type, description patterns, Teller category hints, user-defined rules) to categorize transactions with varying confidence levels. Phase 2 sends uncertain transactions to Claude Haiku (Anthropic's LLM) for AI-powered categorization and validation. It sits in the pipeline between sanitization/enrichment and database insertion.


Key Dependencies

Dependency	Why
httpx	HTTP client for Anthropic API calls
anthropic API	Claude Haiku for LLM categorization
sanitizer (internal)	sanitize_transactions — Phase 1 preprocessing
enricher (internal)	enrich_transactions — Phase 1.5 merchant enrichment
database (internal)	Active categories, user rules from DB
privacy (internal)	mask_amount, mask_counterparty — PII anonymization for LLM
dotenv	API key loading

Core Functions / Classes / Exports

get_active_categories() → list[str]

What it does: Fetches active category names from the database, with fallback to _DEFAULT_CATEGORIES.
Notable logic: Uses try/except to handle cases where the DB isn't available yet (e.g., during testing).

_rule_based_categorize(tx) → tuple[str | None, str]

What it does: Applies deterministic rules to a single transaction.
Outputs: (category, confidence) where confidence is:
"rule-high": certain, skip LLM entirely
"rule-medium": likely correct, send to LLM for validation
(None, ""): no rule matched, needs full LLM categorization
Notable logic:
High confidence: interest income, CC payments (from bank side), internal savings transfers, bank fees
Medium confidence: deposits (probably income), tax payments, P2P to persons, ATM withdrawals, Teller category hints
P2P to organizations gets (None, "") — could be payment for a service
Teller category mapping is DB-backed and cached in _teller_map_cache

_get_teller_category_map() → dict

What it does: Loads Teller category → Folio category mapping from DB, cached in memory.
Notable logic: ORDER BY source ASC ensures user overrides (source='user') win over system defaults (dict keeps last written value per key).

_build_llm_line(idx, tx, rule_suggestion) → str

What it does: Builds a single line for the LLM prompt with PII masking.
Notable logic: Amounts are masked (-$XXX / +$XXX), counterparty names are masked ([person] / [organization]). Merchant name, domain, and industry are included when available. Pre-categorized suggestions are passed through.

_categorize_batch_llm(batch, start_index) → list[dict]

What it does: Sends a batch of transactions to Claude for categorization/validation.
Notable logic: Uses Claude Haiku (claude-3-haiku-20240307). Prompt includes all active categories with detailed descriptions (Food & Dining vs. Groceries, Subscriptions vs. Entertainment, etc.). Response is expected as a JSON array. Markdown fences are stripped from responses.

categorize_transactions(transactions, batch_size=50) → list[dict]

What it does: Main entry point — full two-phase categorization pipeline.
Pipeline:
Phase 1: Sanitize (normalize signs, light cleanup)
Phase 1.5: Enrich via Trove (merchant name, industry, domain)
Phase 1.6: Check user-defined rules from database (highest priority, skip everything if matched)
Phase 1b: Apply rule-based categorization on remaining transactions
Phase 2: Send rule-medium and unmatched transactions to LLM in batches
Final validation: Ensure every transaction has a category and profile
Notable logic:
User rules (source='user') are checked first and take absolute priority
rule-high transactions never go to LLM (saves API costs)
rule-medium transactions go to LLM with a suggestion — LLM can override
LLM batches of 50 with 1-second delays between batches
Failed LLM batches fall back to rule suggestion or "Other"
If LLM is disabled (ENABLE_LLM_CATEGORIZATION=false) or no API key, falls back gracefully
Tracks rule_override when LLM corrects a rule suggestion
Defensive profile backfill at the end ensures profile survives the pipeline

Data Flow


Raw transactions from Teller
  → sanitize_transactions() [sign normalization, PII cleanup]
  → enrich_transactions() [Trove merchant enrichment]
  → User rule matching [DB query]
  → Rule-based categorization [regex patterns, transaction types, Teller hints]
  → LLM categorization [Claude Haiku, batched]
  → Final validation [ensure category + profile present]
  → Categorized transactions returned to data_manager

Integration Points

Called by: data_manager.fetch_fresh_data() (during sync)
Calls: sanitizer.sanitize_transactions(), enricher.enrich_transactions(), database.get_db() (user rules, categories), privacy.mask_amount/mask_counterparty (LLM prompts), Anthropic API (LLM)

Known Quirks / Design Notes

CATEGORIES module-level variable is set to _DEFAULT_CATEGORIES list but the @property decorator above it has no effect (it's on a plain variable, not a class attribute). Functions use get_active_categories() dynamically.
The LLM prompt is very detailed with category descriptions — changes to category semantics require updating this prompt.
Batch size of 50 is hardcoded as default but configurable via parameter.
The 1-second delay between LLM batches is to avoid Anthropic rate limits.
User rules from DB are loaded once at the start of each categorize_transactions() call — not per-transaction.


backend_database.py

Purpose

This is the database infrastructure layer — it defines the complete SQLite schema, manages connection lifecycle (thread-local and request-scoped), handles database initialization and migration, and seeds default data (categories, system rules, Teller category mappings, subscription seeds, enrichment cache). It's the foundation that all other modules build upon.


Key Dependencies

Dependency	Why
sqlite3	Database driver
threading	Thread-local connection storage
pathlib	Database file path resolution
dotenv	DB_FILE environment variable

Core Functions / Classes / Exports

get_connection() → sqlite3.Connection

What it does: Returns a thread-local SQLite connection (reused within the same thread).
Notable logic: Enables WAL journal mode, foreign keys, and 5-second busy timeout. Marked as DEPRECATED — use get_db_session() for request-scoped connections.

get_db() (context manager)

What it does: Context manager wrapping get_connection() with auto-commit/rollback.
Notable logic: Used by background tasks (sync) and module-level code that can't use FastAPI dependency injection.

get_db_session() (generator)

What it does: FastAPI dependency that provides a request-scoped DB connection. Creates a new connection per request and closes it in the finally block.
Notable logic: This is the preferred approach for API endpoints — prevents connection leaks in long-running servers. Each connection gets its own WAL mode, foreign keys, and busy timeout settings.

close_thread_local_connection()

What it does: Closes the thread-local connection during shutdown.

init_db()

What it does: Creates all tables (idempotent), runs migrations, seeds default data.
Calls: _migrate_expense_type(), _seed_default_categories(), _seed_system_rules(), _seed_teller_category_map()

_migrate_expense_type(conn)

What it does: Backfill migration for expense_type and expense_type_source columns on the categories table.
Notable logic: Checks PRAGMA table_info to see if columns exist. Only updates rows that haven't been user-modified (expense_type_source = 'system'). Known fixed categories: Utilities, Housing, Subscriptions, Taxes, Insurance. Known non-expense: Savings Transfer, CC Payment, Income, Personal Transfer.

_extract_merchant_pattern(description) → str

What it does: Extracts a reusable merchant pattern from a transaction description for rule creation.
Notable logic: Strips store numbers (#0742), long numbers, dates, phone numbers, US state codes, common HQ cities, sanitized IDs, .COM/BILL suffixes. Takes the first chunk before - or * delimiters. Returns empty string for patterns shorter than 3 chars or generic words (ACH, DEBIT, etc.).

sync_subscription_seeds()

What it does: Loads subscription_seeds.json into the subscription_seeds table on startup.
Notable logic: Clears all system seeds and re-inserts (ensures deletions in JSON are reflected). User seeds (source='user') are never touched. Accepts both [...] and {"subscriptions": [...]} JSON formats.

sync_enrichment_cache_from_seeds()

What it does: Populates enrichment_cache with merchant identities from subscription seeds.
Notable logic: Uses INSERT OR IGNORE — never overwrites existing entries (Trove data is richer). Normalizes pattern keys using the same _dedup_key logic as enricher.py.

Schema (Tables)

Table	Purpose
profiles	User profiles (e.g., "john", "sarah", "household")
accounts	Bank accounts with balances, types, sync timestamps
categories	Transaction categories with expense_type (fixed/variable/non_expense)
transactions	All transactions with full metadata (28+ columns)
category_rules	Pattern-matching rules for auto-categorization
net_worth_history	Daily net worth snapshots per profile
copilot_conversations	Copilot query/response audit log
subscription_seeds	Known subscription patterns (system + user)
enrichment_cache	Cached merchant enrichment results (Trove + seed)
teller_category_map	Teller category → Folio category mapping
enrolled_tokens	Encrypted Teller access tokens from Connect enrollments

Key Indexes

idx_transactions_date, idx_transactions_profile, idx_transactions_category, idx_transactions_profile_date, idx_transactions_account
idx_rules_source_priority, idx_rules_active
idx_nw_profile_date
idx_sub_seeds_pattern, idx_sub_seeds_source, idx_sub_seeds_active
idx_enrichment_cache_source
idx_enrolled_tokens_profile, idx_enrolled_tokens_active

Data Flow

Init path: main.py startup → init_db() → schema creation → migrations → seeding
Runtime read: Any module → get_db() or get_db_session() → SQL query → results
Runtime write: Sync or user actions → get_db() → INSERT/UPDATE → commit

Integration Points

Called by: Every module in the system (directly or indirectly)
Calls: Only standard library (sqlite3, json, pathlib)

Known Quirks / Design Notes

DB_PATH resolution handles both absolute paths (Docker: /data/finflow.db) and relative paths (local dev).
The comment at the bottom explicitly warns against auto-initializing at import time — init_db() must be called from main.py's startup event.
Two connection strategies coexist: thread-local (get_connection() / get_db()) for background sync, and request-scoped (get_db_session()) for API endpoints. This is by design but adds complexity.
WAL mode is set on every new connection — SQLite persists this per-database, so this is redundant after the first connection but harmless.
DEFAULT_CATEGORIES includes 18 categories with expense_type classifications.
SYSTEM_RULES contains 28 regex-based rules derived from the categorizer's hardcoded patterns — these are now queryable and editable via the DB.
TELLER_CATEGORY_DEFAULTS maps 28 Teller categories to Folio categories. NULL folio_category means "no useful signal, skip".


backend_bank.py

Purpose

This is the Teller API client layer — it handles all communication with the Teller banking API, including account fetching, transaction retrieval (with cursor-based pagination), balance queries, and identity resolution. It manages mTLS certificate authentication, rate limiting, retry logic, and httpx client caching. It also dynamically loads and manages Teller access tokens from both environment variables and the persistent token store.


Key Dependencies

Dependency	Why
httpx	HTTP client with mTLS certificate support
dotenv	Token and certificate path loading
token_store (internal)	Persistent encrypted token storage for Teller Connect enrollments

Core Functions / Classes / Exports

_load_tokens() → list[str]

What it does: Loads all Teller access tokens from environment variables (suffix matching) and the persistent token store.
Notable logic: Skips ANTHROPIC_API_KEY, TELLER_TOKEN_PREFIX, TOKEN_ENCRYPTION_KEY. Merges DB tokens via token_store.load_all_tokens(). Handles first-run gracefully (token_store may not be initialized yet).

_load_profiles() → dict[str, list[str]]

What it does: Groups tokens by profile name. Profile name is derived from the env var prefix before _TOKEN (e.g., JOHN_BOFA_TOKEN → profile "john").
Notable logic: Falls back to {"primary": all_tokens} if no structured naming is found.

validate_teller_config()

What it does: Validates that Teller certificate files exist and are readable. Called at startup.
Raises: RuntimeError if certificates are missing, unreadable, or paths are incomplete.

_get_client(token) → httpx.Client

What it does: Returns a cached httpx.Client for a given token. Reuses clients to avoid TLS handshake overhead.
Notable logic: Clients are cached in module-level _client_cache dict keyed by token string.

close_all_clients()

What it does: Closes all cached httpx clients. Called during FastAPI shutdown.

_request_with_retry(client, method, url, max_retries=3, params) → dict

What it does: Makes HTTP request with exponential backoff retry on 429 (rate limit).
Notable logic: Wait time: min(2^attempt * 2, 30) seconds. Max 3 retries.

get_all_accounts_by_profile() → list[dict]

What it does: Fetches accounts from all configured tokens, tagged with profile name.
Notable logic: Iterates PROFILES dict. Each account gets access_token and profile fields injected. 1-second delay between API calls.

get_transactions(account_id, token) → list[dict]

What it does: Fetches all transactions for an account using Teller's cursor-based pagination.
Notable logic:
Teller returns up to 100 transactions per page
Uses from_id cursor (last transaction's ID) for next page
Stops when: page returns < 100 results, TELLER_MAX_PAGES (50) reached, or TELLER_MAX_TRANSACTIONS (5000) reached
1-second delay between pages
Configurable via TELLER_MAX_PAGES and TELLER_MAX_TRANSACTIONS env vars

get_balances(account_id, token) → dict

What it does: Fetches account balance information.

get_identity(token, account_id) → dict

What it does: Resolves beneficial owner name via Teller's Identity API.
Notable logic: Teller's identity endpoint is GET /identity (top-level, not per-account). Response is a list of {account: {id: ...}, owners: [...]}. Falls back to first entry if account_id not found. Gracefully returns empty name fields on failure.

reload_tokens_and_profiles()

What it does: Hot-reloads TOKENS and PROFILES globals from env + token store. Closes httpx clients for removed tokens.
Notable logic: Called after new Teller Connect enrollments to pick up tokens without server restart.

Data Flow


Environment variables + token_store DB
  → _load_tokens() / _load_profiles()
  → TOKENS / PROFILES globals

API request → _get_client(token) → _request_with_retry() → Teller API → JSON response

Integration Points

Called by: data_manager.fetch_fresh_data() (sync), main.py (enrollment, startup/shutdown, profile registry)
Calls: token_store.load_all_tokens() (token loading), Teller API (HTTP)

Known Quirks / Design Notes

TOKENS and PROFILES are module-level globals modified by reload_tokens_and_profiles() — not thread-safe for the reload itself (protected by caller's _lock in practice).
Certificate path resolution handles both absolute (Docker) and relative paths, with a fallback to ../ relative to the file.
_TELLER_PAGE_SIZE = 100 is a constant assumption about Teller's behavior, not from their API docs.
RATE_LIMIT_DELAY = 1.0 second between all API calls is conservative but safe.
The client cache never expires clients — they persist until server shutdown or token removal.


backend_auth.py

Purpose

Implements API key authentication and per-IP rate limiting for all Folio endpoints. If no API key is configured in .env, it auto-generates a session key at startup and prints it to stdout. Authentication is always enforced — there's no "open" mode.


Key Dependencies

Dependency	Why
fastapi	HTTPException, Request, Security, APIKeyHeader
secrets	Session key auto-generation
hmac	Timing-safe key comparison
collections.defaultdict	Rate limit tracking storage

Core Functions / Classes / Exports

verify_api_key(api_key) → str

What it does: FastAPI dependency that validates the X-API-Key header.
Notable logic: Uses hmac.compare_digest() for timing-safe comparison (FIX A2 — prevents character-by-character guessing attacks). Returns 401 for missing key, 403 for invalid key.

rate_limit_middleware(request, call_next) → Response

What it does: FastAPI middleware for per-IP rate limiting.
Notable logic:
Route-specific limits: /api/copilot (20/min), /api/sync (5/5min), default (120/min)
Client key = {IP}:{route_prefix}
Entries older than the window are cleaned per-request
Global cleanup runs at most every 5 minutes when _MAX_LOG_KEYS (500) is exceeded (FIX A3)
Skips rate limiting for /health endpoint

Rate Limit Configuration

python

RATE_LIMITS = {
    "/api/copilot": (20, 60),     # 20 requests per minute
    "/api/sync": (5, 300),        # 5 syncs per 5 minutes
    "default": (120, 60),         # 120 requests per minute
}

Data Flow

Every request hits rate_limit_middleware first
Then verify_api_key dependency validates the key
If both pass, request proceeds to route handler

Integration Points

Called by: main.py (middleware registration, global dependency)
Calls: Nothing external

Known Quirks / Design Notes

Auto-generated session key is printed to stdout at startup — appropriate for dev/self-hosted but not for production logging.
_request_log is an in-memory dict — doesn't persist across restarts and is per-process (no shared state in multi-worker setups).
Rate limiting is IP-based — all users behind the same NAT/proxy share a limit.
_KEY_SOURCE variable tracks whether the key is from env or auto-generated but isn't used anywhere after initialization.


backend_enricher.py

Purpose

Handles merchant enrichment via the Trove API. It sits between the sanitizer and categorizer in the pipeline, identifying merchants from raw bank descriptions and adding structured metadata (name, domain, industry, location). Features a three-tier caching strategy (in-memory LRU, persistent DB cache, API calls), smart deduplication to minimize API calls, bulk/single API strategy selection, and privacy-preserving request construction.


Key Dependencies

Dependency	Why
httpx	HTTP client for Trove API
hashlib	Anonymous user ID generation
threading	Thread-safe LRU cache
collections.OrderedDict	LRU cache implementation
database (internal)	Persistent enrichment cache (enrichment_cache table)
privacy (internal)	mask_amount (not used directly here but imported)

Core Functions / Classes / Exports

_EnrichmentCache (class)

What it does: Thread-safe in-memory LRU cache keyed on description.upper().strip().
Notable logic: Max size configurable via TROVE_CACHE_MAX_SIZE (default 1000). Uses OrderedDict with move_to_end() for LRU behavior.

_lookup_persistent_cache(pattern_key) → dict | None

What it does: Checks the SQLite enrichment_cache table for a cached enrichment result.
Notable logic: Updates hit_count and last_seen on cache hits. Returns dict compatible with _apply_enrichment().

_persist_enrichment(pattern_key, enrichment, source)

What it does: Stores enrichment result in the DB cache.
Notable logic: Uses ON CONFLICT DO UPDATE with conditional logic: Trove-sourced results upgrade seed entries (Trove data is richer with domain, city, industry). Seed-sourced results only fill empty fields. Only persists if there's meaningful data (name or domain non-empty).

_should_enrich(tx) → bool

What it does: Determines if a transaction should be sent to Trove.
Skips: Already enriched, categories in SKIP_ENRICHMENT_CATEGORIES (transfers, income, fees), description too short, non-merchant transaction types (transfer, payment, fee, deposit, ach), payroll/tax refund/mobile deposit descriptions.

_build_trove_payload(tx, anonymous_id) → dict | None

What it does: Builds a Trove API request payload.
Notable logic: Uses raw_description (pre-sanitization) for best Trove matching. Amount is always sent as 1.00 (privacy: Trove doesn't need real amounts, matching is description-based). Validates date format, non-empty description (≥2 chars), and non-zero original amount. Returns None for invalid transactions.

_dedup_key(tx) → str

What it does: Builds a normalized deduplication key. Masks long numeric sequences (\d{6,} → XXXXX) so "CHECK #001234" and "CHECK #005678" share a key.

_deduplicate_for_trove(transactions, indices) → tuple

What it does: Groups transactions by dedup key, picks one representative per group.
Outputs: (representative_indices, fanout_map) where fanout_map = {key: [all indices]}

_fanout_enrichment(transactions, fanout_map, enriched_index) → int

What it does: Copies enrichment data from a representative transaction to all siblings sharing the same dedup key.

enrich_transactions(transactions) → list[dict]

What it does: Main entry point. Multi-step enrichment pipeline.
Pipeline:
Filter to enrichable transactions
Resolve in-memory cache hits
Resolve persistent DB cache hits
Deduplicate remaining by description
Choose strategy: single-enrich (≤ BULK_THRESHOLD) or bulk API (> BULK_THRESHOLD)
Fan out results to duplicates
Log summary
Notable logic: BULK_THRESHOLD defaults to 0 (always uses single-enrich, which has higher match rate). Configurable via env var.

_enrich_via_single(transactions, indices, fanout_map) → tuple[int, int]

What it does: Enriches one-by-one with 0.3-second delays between calls.
Notable logic: On 429 rate limit: pauses 30 seconds, retries once, stops if still limited. Results are cached in both memory and DB.

_enrich_via_bulk(transactions, indices, fanout_map) → tuple[int, int]

What it does: Sends batches to Trove's bulk API, polls for results.
Notable logic: Batches of BULK_BATCH_SIZE (default 100). Polling intervals scale with batch size: first wait = max(10, batch_size // 2) seconds. Falls back to single-enrich on any failure (per-batch, not globally).

Data Flow


Sanitized transactions
  → Filter enrichable
  → Memory cache check
  → DB cache check
  → Deduplicate
  → Trove API (single or bulk)
  → Apply enrichment (domain, name, industry, location)
  → Fanout to duplicates
  → Cache results (memory + DB)
  → Return enriched transactions

Integration Points

Called by: categorizer.categorize_transactions() (Phase 1.5)
Calls: Trove API (HTTP), database.get_db() (persistent cache)

Known Quirks / Design Notes

Amount is always sent as 1.00 to Trove — a deliberate privacy decision since Trove's matching is description-based.
_scrub_for_trove() is intentionally minimal — Trove wants the messy raw merchant string with store numbers, asterisks, etc.
The _get_anonymous_user_id() generates a stable hash from TROVE_USER_SEED env var — same user always gets the same anonymous ID.
Three-tier cache strategy: memory (fast, per-process) → DB (persistent, cross-restart) → API (network, authoritative).
ENABLE_TROVE feature toggle allows disabling all enrichment via env var.
The bulk API polling has a maximum of 7 poll attempts with escalating wait times (up to 120s). Total max wait for a single batch: ~280 seconds.


backend_copilot.py

Purpose

Implements the NLP-to-SQL copilot feature — translates natural language questions about financial data into SQL queries, executes them safely, and generates natural language answers. Supports both read (SELECT) and write (UPDATE/INSERT) operations with comprehensive safety validation, PII anonymization, and a server-side confirmation flow for writes.


Key Dependencies

Dependency	Why
httpx, certifi	HTTP client for Anthropic API with SSL verification
database (internal)	DB operations, schema info, merchant pattern extraction
privacy (internal)	sanitize_rows_for_llm — masks amounts/PII before sending results to LLM
secrets, time, threading	Server-side pending SQL store with TTL

Core Functions / Classes / Exports

_validate_read_sql(sql) → tuple[bool, str]

What it does: Validates that a SQL string is a safe SELECT query.
Checks: Single statement only, must start with SELECT, no forbidden keywords (DROP, ALTER, TRUNCATE, PRAGMA, etc.), no write operations (INSERT, UPDATE, DELETE, REPLACE) outside string literals.

_validate_write_sql(sql) → tuple[bool, str]

What it does: Validates write SQL against an allowlist of tables and columns.
Allowed operations:
UPDATE transactions: Only category, categorization_source, is_excluded, updated_at, original_category, confidence
UPDATE category_rules: Only category, priority, is_active
INSERT category_rules: Allowed (any columns)
INSERT categories: Allowed
DELETE: Never allowed
Notable logic (FIX C1): Parses SET clause columns with quote-aware comma splitting. Rejects subqueries (SELECT) in SET values and INSERT VALUES (FIX C2) to prevent data exfiltration.

_keyword_outside_strings(upper_sql, keyword) → bool

What it does: Checks if a SQL keyword appears outside of string literals (single and double quotes).
Notable logic: Tracks quote state character-by-character, checks word boundaries.

store_pending_sql(sql, profile) → str / retrieve_pending_sql(nonce) → dict | None

What it does: Server-side store for validated SQL awaiting user confirmation (FIX C3).
Notable logic: 5-minute TTL. Expired entries cleaned on each store. One-time retrieval (pop semantics).

_build_profile_map() → tuple[dict, dict]

What it does: Builds bidirectional mapping between real profile names and anonymized aliases (profile_1, profile_2, etc.).
Notable logic: household maps to all_profiles. Used to anonymize profile names in LLM interactions.

_build_schema_context(real_to_alias) → str

What it does: Generates the schema description prompt for Claude.
Notable logic: Includes table schemas, available categories, anonymized profile list, today's date, and critical rules about profile filtering (especially the all_profiles virtual profile that should never appear in WHERE clauses).

ask_copilot(question, profile, confirm_write, pending_sql) → dict

What it does: Main copilot entry point.
Flow:
If confirm_write=True, executes the pending SQL
Otherwise, builds anonymized schema context
Calls Claude to generate SQL
Handles CANNOT responses (question can't be answered with SQL)
For WRITE operations: validates, previews, stores server-side, returns confirmation request
For READ operations: validates, executes with PRAGMA query_only = ON, anonymizes results, sends back to Claude for natural language answer
Notable logic: SQL is de-anonymized (alias → real profile ID) before execution. household/all_profiles profile filters are sanitized (removed) since no rows have those as profile_id.

_preview_write(sql, profile) → dict

What it does: Previews a write operation by extracting the WHERE clause and running a SELECT with it (FIX C4).
Notable logic: Uses PRAGMA query_only = ON to prevent any accidental writes. Validates the constructed preview SQL as a read query. Returns {count, sample}.

_execute_write(sql, profile, original_question) → dict

What it does: Executes a validated write operation with safety checks (FIX C5).
Notable logic: Row count pre-check against COPILOT_MAX_WRITE_ROWS (default 5000). Auto-creates category rules from UPDATE operations that change categories. Ensures referenced categories exist before executing.

_generate_natural_answer(question, rows) → str

What it does: Sends query results to Claude for natural language summarization.
Notable logic: Results are privacy-sanitized (sanitize_rows_for_llm). Prompt explicitly tells Claude not to invent dollar amounts (they're masked as $XXX) and not to reveal personal names. Falls back to simple formatting if Claude call fails.

Data Flow


User question
  → Profile anonymization
  → Schema context building
  → Claude generates SQL
  → SQL validation (read or write safety checks)
  → De-anonymize profile references
  → Execute against SQLite (with query_only for reads)
  → Anonymize result rows
  → Privacy-sanitize for LLM
  → Claude generates natural language answer
  → Return {answer, sql, data, operation}

Integration Points

Called by: main.py (/api/copilot/ask, /api/copilot/confirm)
Calls: database.get_db(), privacy.sanitize_rows_for_llm(), Anthropic API

Known Quirks / Design Notes

Five explicit security fixes (C1-C5) are documented in comments throughout the file.
The pending SQL store is in-memory — lost on server restart. 5-minute TTL means users must confirm quickly.
Profile anonymization adds complexity but prevents the LLM from learning real names.
_sanitize_profile_filter() uses regex to strip household/all_profiles WHERE clauses — fragile if SQL formatting changes.
Conversation logging (copilot_conversations table) stores the query result (truncated to 5000 chars).
The PRAGMA query_only = ON/OFF pattern is used in a try/finally block for safety.
COPILOT_MAX_WRITE_ROWS default of 5000 prevents accidental mass updates.


backend_privacy.py

Purpose

Shared privacy utilities for sanitizing data before sending to external APIs (Anthropic LLM, Trove). Provides functions to mask dollar amounts, counterparty names, and other PII while preserving the sign (expense vs income) and type information that has categorization value.


Key Dependencies

None (pure utility module).


Core Functions / Classes / Exports

mask_amount(amount, placeholder="$XXX") → str

What it does: Replaces exact dollar amounts with a placeholder while preserving sign.
Outputs: -$XXX (expense), +$XXX (income/refund), $XXX (zero/invalid).

mask_counterparty(name, counterparty_type) → str

What it does: Replaces real counterparty names with type-based placeholders.
Outputs: [person], [organization], [counterparty] (unknown type), or "" (no name).
Notable logic: For P2P transfers (Zelle, Venmo), the counterparty is a person's real name — pure PII with zero categorization value.

sanitize_row_for_llm(row) → dict

What it does: Sanitizes a single database result row for LLM consumption.
Masks: amount, balance fields (current_balance, total_assets, net_worth, etc.), counterparty_name.
Strips: raw_description entirely (contains unsanitized merchant + location data).

sanitize_rows_for_llm(rows) → list[dict]

What it does: Convenience wrapper applying sanitize_row_for_llm to a list.

Integration Points

Called by: categorizer.py (mask_amount, mask_counterparty in LLM prompts), copilot.py (sanitize_rows_for_llm for query results)
Calls: Nothing

Known Quirks / Design Notes

The sign preservation in mask_amount is a deliberate design choice — the LLM prompt tells Claude that -$XXX = expense and +$XXX = income/refund.
Balance fields are masked with a generic $XXX without sign — they could be negative (credit cards) but the context is less important for copilot answer generation.
raw_description is stripped entirely rather than masked because it contains location data that could be sensitive.


backend_recurring.py

Purpose

Subscription and recurring charge detection service. Uses a three-layer approach: (1) seed-based matching against a known database of subscription patterns, (2) algorithmic detection based on transaction frequency and amount consistency, and (3) category-based detection for transactions already categorized as "Subscriptions". Extracted from main.py for testability.


Key Dependencies

Dependency	Why
statistics	mean, stdev, median — statistical analysis of amounts and intervals
collections.defaultdict	Merchant grouping
database (internal)	_extract_merchant_pattern — merchant name extraction

Core Functions / Classes / Exports

FREQUENCY_DEFS (dict)

What it does: Single source of truth for frequency definitions.
Format: {name: (nominal_days, grace_days, range_low, range_high)}
Values: monthly (30, 15, 25, 38), quarterly (91, 30, 80, 105), semi_annual (182, 45, 160, 210), annual (365, 45, 340, 400)

_detect_frequency(dates, seed_freq_hint) → str | None

What it does: Determines the frequency of a set of dates using median interval analysis.
Notable logic: If a seed hint is provided, checks that frequency first with looser tolerances (35% deviation OK, 50% of intervals must match). For unhinted detection, requires 30% deviation tolerance and 60% interval match rate.

_amount_confidence(amounts) → float

What it does: Computes a 0-1 confidence score for amount consistency.
Notable logic: 1 - (std_dev / mean). Perfect consistency = 1.0. Single transaction = 1.0.

_detect_price_change(group_txns) → dict | None

What it does: Detects if the most recent charge differs from the average of the 3 previous charges.
Notable logic: Ignores changes smaller than $0.50. Returns {previous, current, change, change_pct}.

_load_seeds_cached(get_db_conn, profile) → tuple[list, set]

What it does: Loads subscription seeds from DB with 60-second TTL cache.
Notable logic: Short patterns (< 6 chars) get word-boundary regex compilation (\bPATTERN\b). Returns both active seeds and suppressed patterns (user-dismissed).

RecurringDetector (class)

Constants:
ALGO_MIN_CHARGES = 3 — minimum transactions to consider algorithmic detection
ALGO_CV_THRESHOLD = 0.10 — max coefficient of variation for amount consistency (10%)
ALGO_CV_THRESHOLD_LOOSE = 0.40 — looser threshold for variable-amount categories (utilities, insurance) (40%)
ALGO_EXCLUDED_CATEGORIES — categories never detected algorithmically (Groceries, Food & Dining, Transportation, Shopping, Travel, plus all transfer/income categories)
VARIABLE_AMOUNT_CATEGORIES — categories that get the loose CV threshold (Utilities, Electric, Gas, Water, Internet, Insurance variants)
_DISQUALIFY_TOKENS — description tokens that disqualify transactions (ATM, CASH, CHECK, REFUND, etc.)

RecurringDetector.detect(transactions, profile) → dict

What it does: Main detection entry point. Returns {items, count, total_monthly, total_annual}.
Pipeline:
Filter to expenses, skip disqualified descriptions
Group by merchant (normalized key)
Merge groups that share a subscription seed
Layer 1 — Seed matching: Match against known subscription patterns
Layer 2 — Algorithmic detection: Frequency + amount consistency analysis
Layer 3 — Category-based: Catch remaining "Subscriptions" category items
Sort: active items first, then by annual cost descending

RecurringDetector._group_by_merchant(expense_txns) → tuple[dict, dict]

What it does: Groups transactions by normalized merchant key.
Notable logic: Strips .COM/.NET/.ORG suffixes, corporate suffixes (INC, LLC, LTD, CORP), leading/trailing punctuation. Prefers enriched merchant_name over extracted description patterns.

Data Flow


All expense transactions
  → Filter + disqualify
  → Group by merchant
  → Merge seed-matched groups
  → Seed matching (Layer 1)
  → Algorithmic detection (Layer 2)
  → Category-based detection (Layer 3)
  → Sort + summarize
  → {items, count, total_monthly, total_annual}

Integration Points

Called by: main.py (/api/analytics/recurring)
Calls: database._extract_merchant_pattern(), database.get_db() (seed loading)

Known Quirks / Design Notes

This is the reason get_data() (full dataset load) exists — the cross-transaction analysis can't be efficiently done in SQL.
Seed cache has a 60-second TTL — user confirmations/dismissals take effect within a minute.
_merge_seed_groups() prevents the same subscription from appearing multiple times under different merchant key variants.
The MAX_TXN_PER_PERIOD_RATIO = 1.35 prevents false positives from merchants with irregular but frequent charges (like weekly grocery runs).
Price change detection only compares against the 3 most recent previous charges — not the full history.


backend_sanitizer.py

Purpose

Light transaction sanitization — the first step in the categorization pipeline. Normalizes credit card amount signs (Teller's convention → Folio's convention), strips sensitive IDs and confirmation numbers from descriptions, extracts counterparty information, and filters out card-side payment transactions (which are duplicates of bank-side payments).


Key Dependencies

Dependency	Why
re	Regex for description cleanup

Core Functions / Classes / Exports

sanitize_transaction(tx) → dict | None

What it does: Sanitizes a single Teller transaction.
Returns None for: payment type transactions (card-side payments already counted on bank side).
Notable logic:
Credit card sign normalization: Teller returns CC purchases as positive → flipped to negative (expense). CC transaction type with negative amount = refund → flipped to positive.
Strips: ID:XXXXX, confirmation numbers, INDN: (individual names in ACH), CO ID: references.
Preserves raw_description (pre-sanitization) for enrichment.
Extracts counterparty_name, counterparty_type from Teller's details.counterparty object.
Preserves profile tag if present.

sanitize_transactions(transactions) → list[dict]

What it does: Applies sanitize_transaction to a list, filtering out None results (skipped transactions).

Data Flow


Raw Teller transactions
  → sanitize_transaction() per item
  → Filter out None (payment transactions)
  → Sanitized transactions with normalized signs and cleaned descriptions

Integration Points

Called by: categorizer.categorize_transactions() (Phase 1)
Calls: Nothing external

Known Quirks / Design Notes

The sign normalization logic is Teller-specific. Different banking APIs may have different conventions.
INDN: stripping removes individual names from ACH transactions — important for privacy but could lose useful info in rare cases.
The sanitized output includes both description (cleaned) and raw_description (original) — downstream modules choose which to use.
amount is converted to string in the output dict (str(amount)) — this is unusual and requires downstream consumers to convert back to float.


backend_token_store.py

Purpose

CRUD operations for dynamically enrolled Teller tokens from the Teller Connect flow. Tokens are stored encrypted (Fernet) in the enrolled_tokens table of the main SQLite database. Supports saving, loading (grouped by profile), listing enrollments (metadata only), and soft-deleting enrollments.


Key Dependencies

Dependency	Why
cryptography.fernet	Token encryption (optional but recommended)
database (internal)	get_db — database access

Core Functions / Classes / Exports

_encrypt(plaintext) → str / _decrypt(stored) → str

What it does: Encrypts/decrypts tokens using Fernet symmetric encryption.
Notable logic: If TOKEN_ENCRYPTION_KEY is not set or cryptography package is not installed, tokens are stored in plaintext with a warning.

save_token(profile, token, institution, owner_name, enrollment_id) → bool

What it does: Persists a new encrypted token. Returns True if inserted, False if duplicate.
Notable logic: Duplicate detection via UNIQUE constraint on (profile, token_encrypted).

load_all_tokens() → dict[str, list[str]]

What it does: Loads all active tokens grouped by profile (decrypted).
Outputs: {"profile_name": ["tok_abc", "tok_def"], ...}

load_all_enrollments() → list[dict]

What it does: Returns enrollment metadata (no tokens) for the /api/enrollments endpoint.

deactivate_token(token_id) → bool

What it does: Soft-deletes an enrollment by setting is_active = 0.

Integration Points

Called by: bank.py (token loading on startup/reload), main.py (enrollment, deactivation, listing)
Calls: database.get_db(), cryptography.fernet.Fernet

Known Quirks / Design Notes

Encryption is optional — the module works without cryptography installed, but tokens are stored in plaintext.
The UNIQUE constraint is on (profile, token_encrypted) — the same token encrypted with the same key produces the same ciphertext, so duplicates are caught. However, if the encryption key changes, the same token would produce different ciphertext and could be inserted again.
deactivate_token() returns the cursor rowcount result, but due to the with get_db() context manager, the check on cur.rowcount happens after the context exits — this works because rowcount is set before commit.


backend_log_config.py

Purpose

Centralized logging configuration. Provides a setup_logging() function that configures the root logger with a stdout handler, and a get_logger() factory that ensures logging is configured before returning a named logger. Prevents duplicate handler attachment across multiple calls.


Key Dependencies

Dependency	Why
logging, os, sys	Standard library logging

Core Functions / Classes / Exports

setup_logging() → None

What it does: Configures root logger with stdout handler, format string, and log level from LOG_LEVEL env var.
Notable logic: Idempotent — uses _initialized flag. Only adds handler if root.handlers is empty.

get_logger(name) → logging.Logger

What it does: Returns a named logger, calling setup_logging() first.
Usage: logger = get_logger(__name__) at module level.

Integration Points

Called by: Every module in the backend.

Known Quirks / Design Notes

Log format: %(asctime)s %(levelname)s [%(module)s]: %(message)s with %Y-%m-%d %H:%M:%S date format.
Default log level is INFO. Set LOG_LEVEL=DEBUG in .env for verbose output.


backend__env_example_txt.txt

Purpose

Example environment configuration file documenting all available settings, their purposes, and how to generate required keys. Serves as both documentation and a setup template.


Key Configuration Groups

Group	Variables	Notes
Teller Certificates	TELLER_CERT_PATH, TELLER_KEY_PATH	Required. mTLS for Teller API.
Teller Connect	TELLER_APPLICATION_ID, TELLER_ENVIRONMENT	For dynamic enrollment via UI.
Token Encryption	TOKEN_ENCRYPTION_KEY	Fernet key for encrypting stored tokens.
Security	FINFLOW_API_KEY, VITE_API_KEY	API key for backend auth. Must match frontend.
Feature Toggles	ENABLE_TROVE, ENABLE_LLM_CATEGORIZATION	Opt-out of Trove enrichment or LLM categorization.
Anthropic	ANTHROPIC_API_KEY	Powers AI categorization + copilot. Paid service.
Trove	TROVE_API_KEY, TROVE_USER_SEED	Merchant enrichment. Free tier available.
Teller Tokens	FIRSTNAME_BANKNAME_TOKEN	Manual token setup. Prefix determines profile name.
Infrastructure	CORS_ORIGINS, BACKEND_PORT, FRONTEND_PORT, DB_FILE	Docker/deployment settings.


backend_requirements_txt.txt

Purpose

Python dependency manifest for the backend.


Dependencies

Package	Version	Purpose
fastapi	0.115.0	Web framework
uvicorn[standard]	0.30.0	ASGI server
httpx	0.27.0	HTTP client (Teller, Anthropic, Trove)
python-dotenv	1.0.1	Environment variable loading
pydantic	2.9.0	Request/response validation
certifi	≥2024.2.2	SSL certificate bundle
cryptography	≥42.0.0	Token encryption (Fernet)
pip-system-certs	—	System certificate integration

Known Quirks

cryptography is listed as required but the code handles its absence gracefully (falls back to plaintext token storage).
anthropic SDK is not listed — the code uses raw httpx calls to the Anthropic API.
sqlite3 is part of the Python standard library and doesn't need to be listed.


backend_subscription_seeds_json.txt

Purpose

A comprehensive database of ~220 known subscription services organized by category. Each entry contains the service name, multiple description patterns (how it appears on bank statements), frequency hint, and category. Loaded into the subscription_seeds table on startup and used by the recurring detection engine for seed-based matching (Layer 1).


Categories Covered

Streaming (25), Music (9), Gaming (7), Software (40+), Cloud/Hosting (18), AI/Productivity (10), News/Media (12), Fitness (20), Meal Kit (12), Pet (5), Beauty (8), Education (12), VPN/Security (8), Internet Providers (12), Wireless Carriers (10), Electric Utilities (16), Gas Utilities (2), Water Utilities (3), Insurance — Auto (14), Home (2), Renters (1), Health (8), Life (5), Telehealth (7), Pharmacy (3), Rent Payment (10), Dating (4)


Notable Design Decisions

Patterns are UPPERCASE strings matching raw bank descriptions (e.g., "NETFLIX", "NETFLIX.COM", "NETFLIX INC", "NETFLIX DIGITAL").
frequency_hint: "monthly_or_annual" is normalized to "monthly" by the detector unless actual charge intervals suggest otherwise.
Some categories (Streaming, Music) are specific to subscription types, not matching the default Folio categories — the detector preserves the seed's category.
The file includes both well-known services and niche ones (Criterion Channel, Mubi, Nebula, Obsidian Sync).


backend_package_json.txt

Purpose

Empty JSON object ({}). Placeholder file — the backend is Python-based and doesn't use Node.js packages. Likely exists for workspace tooling compatibility.



System-Level Summary

The Folio backend is a self-hosted personal finance tracking system built as a FastAPI application backed by SQLite. Its primary data source is the Teller banking API, which provides real-time access to bank accounts and transactions across multiple financial institutions via mTLS certificate authentication. The system supports multiple user profiles (e.g., household members), each with their own set of bank accounts, and can aggregate data into a "household" view.


Data ingestion follows a well-defined pipeline: the sync endpoint (/api/sync) triggers data_manager.fetch_fresh_data(), which iterates through all configured Teller tokens, fetches accounts and transactions, and passes new transactions through the categorization pipeline. This pipeline consists of four phases: (1) sanitization (sign normalization, PII cleanup via sanitizer.py), (2) merchant enrichment via the Trove API (enricher.py, with three-tier caching), (3) user rule matching from the database, and (4) deterministic rule-based categorization followed by LLM categorization/validation via Claude Haiku (categorizer.py). The system is designed to minimize expensive API calls — user rules and high-confidence deterministic rules skip the LLM entirely, while medium-confidence rules are sent with suggestions for the LLM to validate or override. Enrichment uses aggressive deduplication and caching (in-memory LRU + persistent SQLite cache) to minimize Trove API calls.


Data access is primarily SQL-driven, with targeted query functions in data_manager.py that push all filtering, pagination, and aggregation into SQL WHERE clauses and GROUP BY expressions. The dashboard bundle endpoint (/api/dashboard-bundle) aggregates five different analytics views into a single request. The only endpoint that loads the full dataset into memory is the recurring transaction detector (/api/analytics/recurring), which requires cross-transaction analysis (interval detection, amount consistency, merchant grouping) that can't be efficiently expressed in SQL. The NLP copilot (copilot.py) translates natural language questions into validated SQL queries, with comprehensive safety checks including table/column allowlists, forbidden keyword detection, subquery blocking, and row count limits for write operations.


Security and privacy are woven throughout: API key authentication with timing-safe comparison, per-IP rate limiting, mTLS for Teller, Fernet encryption for stored tokens, PII anonymization before LLM calls (amounts masked to $XXX, names replaced with [person]/[organization], profile names aliased), CORS restrictions, trusted host validation, and server-side SQL storage for copilot write confirmations (preventing client-supplied SQL injection). The architecture makes explicit trade-offs — for example, sending amount=1.00 to Trove instead of real amounts, and stripping raw_description from LLM-bound data.


The key architectural decisions are: (1) SQLite with WAL mode as the sole data store, with dual connection strategies (thread-local for background sync, request-scoped for API endpoints); (2) a layered categorization approach where cheap deterministic rules handle clear-cut cases and expensive LLM calls are reserved for ambiguous transactions; (3) subscription seeds maintained as a JSON file synced to the database on startup, with user feedback (confirm/dismiss) creating persistent overrides; (4) a three-tier enrichment cache that ensures Trove API costs grow sublinearly with transaction volume; and (5) a feature toggle system (ENABLE_TROVE, ENABLE_LLM_CATEGORIZATION) that allows the app to function with graceful degradation when external services are unavailable or undesired.
