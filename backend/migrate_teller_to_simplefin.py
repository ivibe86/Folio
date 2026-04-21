"""
migrate_teller_to_simplefin.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
One-shot CLI script for migrating from Teller to SimpleFIN Bridge.

What it does:
  1. Pulls your FULL Teller history (years, not just recent 90 days)
  2. Pulls SimpleFIN data (up to 90 days)
  3. Interactively maps accounts across providers
  4. Deduplicates the overlap period
  5. Optionally deactivates Teller so SimpleFIN takes over going forward

PREREQUISITES:
  1. Teller configured — tokens set in .env (e.g. MYBANK_TOKEN=...) OR
     enrolled via Control Center → Connections → Teller
  2. SimpleFIN connected — via Control Center → Connections → Connect Bank
  3. Run from inside Docker:
       docker exec -it folio-backend-1 python3 migrate_teller_to_simplefin.py
     Or locally (from backend/ with venv active):
       python3 migrate_teller_to_simplefin.py

SAFETY:
  - All deduplication writes happen inside a single SQLite transaction.
    Any failure rolls back completely — no partial state.
  - Safe to run more than once (idempotent).
  - Does not delete any transactions. Duplicates are marked excluded only.
"""

import os
import sys


# ── Sanity-check working directory ────────────────────────────────────────────
# Ensure we can import backend modules regardless of where the script is run.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)


# ── Remove the default Teller fetch caps before importing bank ────────────────
# bank.py reads these from env at module import time; override before import.
os.environ.setdefault("TELLER_MAX_PAGES", "999")
os.environ.setdefault("TELLER_MAX_TRANSACTIONS", "999999")


# ── Imports (must come after path/env setup) ──────────────────────────────────
import bank
bank.TELLER_MAX_PAGES = 999
bank.TELLER_MAX_TRANSACTIONS = 999_999

from database import init_db, get_db
from data_manager import fetch_fresh_data, fetch_simplefin_data
from migration import analyze_migration, execute_migration


# ── Pretty printing helpers ───────────────────────────────────────────────────

BOLD  = "\033[1m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED   = "\033[31m"
CYAN  = "\033[36m"
RESET = "\033[0m"

def header(text):
    print(f"\n{BOLD}{CYAN}{'━' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  {text}{RESET}")
    print(f"{BOLD}{CYAN}{'━' * 60}{RESET}")

def step(n, text):
    print(f"\n{BOLD}Step {n}:{RESET} {text}")

def ok(text):
    print(f"  {GREEN}✓{RESET} {text}")

def warn(text):
    print(f"  {YELLOW}⚠{RESET}  {text}")

def err(text):
    print(f"  {RED}✗{RESET} {text}")

def info(text):
    print(f"  {text}")

def ask(prompt, default="y"):
    marker = f"[{'Y' if default == 'y' else 'y'}/{'N' if default == 'n' else 'n'}]"
    answer = input(f"\n  {prompt} {marker}: ").strip().lower()
    if not answer:
        return default == "y"
    return answer in ("y", "yes")


# ── Prerequisites check ───────────────────────────────────────────────────────

def check_prerequisites():
    """Validate Teller and SimpleFIN are configured before doing any work."""

    # DB path
    db_file = os.getenv("DB_FILE", "/data/Folio.db")
    info(f"Database: {db_file}")

    # Init schema (idempotent)
    init_db()

    errors = []

    # Check Teller: tokens in env or enrolled_tokens table
    has_env_tokens = any(
        k.endswith("_TOKEN") and not k.startswith("TELLER")
        for k in os.environ
    )
    with get_db() as conn:
        enrolled_count = conn.execute(
            "SELECT COUNT(*) FROM enrolled_tokens WHERE is_active = 1"
        ).fetchone()[0]
        sf_count = conn.execute(
            "SELECT COUNT(*) FROM simplefin_connections WHERE is_active = 1"
        ).fetchone()[0]

    has_teller = has_env_tokens or enrolled_count > 0
    has_sf = sf_count > 0

    if has_teller:
        source = "env vars" if has_env_tokens else f"{enrolled_count} enrollment(s) in DB"
        ok(f"Teller configured — {source}")
    else:
        err("No Teller tokens found in .env and no active enrollments in DB.")
        errors.append("teller")

    if has_sf:
        ok(f"SimpleFIN configured — {sf_count} active connection(s)")
    else:
        err("No active SimpleFIN connections found.")
        errors.append("simplefin")

    if errors:
        print()
        if "teller" in errors:
            warn("Add Teller tokens to .env (e.g. MYBANK_TOKEN=...) or enroll via Control Center.")
        if "simplefin" in errors:
            warn("Connect SimpleFIN via Control Center → Connections → Connect Bank.")
        sys.exit(1)


# ── Main migration flow ───────────────────────────────────────────────────────

def main():
    header("Folio — Teller → SimpleFIN Migration")

    print("""
  This script will:
    1. Pull your complete Teller history (all available years)
    2. Pull SimpleFIN data (up to 90 days)
    3. Map accounts across providers interactively
    4. Deduplicate the overlap period
    5. Optionally disable Teller going forward

  No data is deleted. Duplicates are only marked excluded.
  All writes are atomic — any failure rolls back completely.
""")

    if not ask("Ready to begin?", default="y"):
        print("\n  Aborted.\n")
        sys.exit(0)

    # ── Step 0: Prerequisites ─────────────────────────────────────────────────
    step(0, "Checking prerequisites…")
    check_prerequisites()

    # ── Step 1: Full Teller sync ──────────────────────────────────────────────
    step(1, "Pulling full Teller history (this may take a minute)…")
    print(f"  {YELLOW}Teller fetch caps removed — pulling all available history{RESET}")
    try:
        result = fetch_fresh_data(incremental=False)
        with get_db() as conn:
            total = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE id NOT LIKE 'sf_%'"
            ).fetchone()[0]
        ok(f"Teller sync complete — {total} total Teller transactions in DB")
    except Exception as exc:
        warn(f"Teller sync failed: {exc}")
        warn("Continuing — SimpleFIN data will still be processed.")

    # ── Step 2: SimpleFIN sync ────────────────────────────────────────────────
    step(2, "Pulling SimpleFIN data (up to 90 days)…")
    try:
        fetch_simplefin_data()
        with get_db() as conn:
            sf_total = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE id LIKE 'sf_%'"
            ).fetchone()[0]
        ok(f"SimpleFIN sync complete — {sf_total} total SimpleFIN transactions in DB")
    except Exception as exc:
        warn(f"SimpleFIN sync failed: {exc}")
        warn("Continuing — Teller history is still intact.")

    # ── Step 3: Analyze overlap ───────────────────────────────────────────────
    step(3, "Analyzing overlap between providers…")
    with get_db() as conn:
        analysis = analyze_migration(conn)

    window = analysis.get("simplefin_window_start")
    teller_accounts = analysis.get("teller_accounts", [])
    sf_accounts = analysis.get("simplefin_accounts", [])
    suggested = {m["teller_account_id"]: m for m in analysis.get("suggested_mappings", [])}
    estimates = {e["teller_account_id"]: e for e in analysis.get("estimates", {}).get("per_mapping", [])}

    info(f"SimpleFIN window start: {window or 'no SimpleFIN transactions yet'}")
    info(f"Teller accounts: {len(teller_accounts)}")
    info(f"SimpleFIN accounts: {len(sf_accounts)}")

    if not teller_accounts:
        warn("No active Teller accounts found — nothing to migrate.")
        sys.exit(0)

    if not sf_accounts:
        warn("No active SimpleFIN accounts found — run step 2 first.")
        sys.exit(0)

    # ── Step 4: Interactive account mapping ───────────────────────────────────
    step(4, "Map Teller accounts to SimpleFIN counterparts")
    print()

    # Build a display list of SF accounts
    sf_by_profile = {}
    for sf in sf_accounts:
        sf_by_profile.setdefault(sf["profile"], []).append(sf)

    user_mappings = []
    CONF_COLOR = {"high": GREEN, "medium": YELLOW, "low": RED, "none": RED}

    for i, tel in enumerate(teller_accounts, 1):
        tel_id = tel["id"]
        tel_name = tel["account_name"]
        tel_type = tel["account_type"]
        tel_profile = tel.get("profile", "")

        sugg = suggested.get(tel_id)
        sugg_sf_id = sugg["sf_account_id"] if sugg else None
        sugg_sf = next((s for s in sf_accounts if s["id"] == sugg_sf_id), None)
        conf = sugg["confidence"] if sugg else "none"
        conf_col = CONF_COLOR.get(conf, RESET)

        print(f"  [{i}] {BOLD}Teller:{RESET} {tel_name} ({tel_type}) — profile: {tel_profile}")
        if sugg_sf:
            print(f"       {conf_col}Suggested → {sugg_sf['account_name']} [SimpleFIN] — {conf.upper()} confidence{RESET}")
        else:
            print(f"       {RED}No suggestion found{RESET}")

        # For high-confidence, default to accept; for others always prompt
        if conf == "high":
            prompt = "Accept? [Y/n/list/skip]"
        else:
            prompt = "Choose [Y=accept suggestion / list / skip]"

        while True:
            choice = input(f"       {prompt}: ").strip().lower()

            if choice in ("", "y", "yes"):
                if sugg_sf:
                    user_mappings.append({
                        "teller_account_id": tel_id,
                        "sf_account_id": sugg_sf_id,
                    })
                    ok(f"Mapped: {tel_name} → {sugg_sf['account_name']}")
                    break
                else:
                    print("       No suggestion to accept. Type 'list' to choose or 'skip'.")

            elif choice in ("list", "l"):
                profile_options = sf_by_profile.get(tel_profile, sf_accounts)
                print(f"\n       SimpleFIN accounts (profile: {tel_profile or 'all'}):")
                for j, sf in enumerate(profile_options, 1):
                    print(f"         {j}. {sf['account_name']} ({sf['account_type']})")
                print(f"         0. Skip this account")
                num = input("       Enter number: ").strip()
                try:
                    idx = int(num)
                    if idx == 0:
                        user_mappings.append({"teller_account_id": tel_id, "sf_account_id": None})
                        warn(f"Skipped: {tel_name}")
                    else:
                        chosen = profile_options[idx - 1]
                        user_mappings.append({
                            "teller_account_id": tel_id,
                            "sf_account_id": chosen["id"],
                        })
                        ok(f"Mapped: {tel_name} → {chosen['account_name']}")
                    break
                except (ValueError, IndexError):
                    print("       Invalid choice, try again.")

            elif choice in ("skip", "s"):
                user_mappings.append({"teller_account_id": tel_id, "sf_account_id": None})
                warn(f"Skipped: {tel_name}")
                break

            else:
                print("       Enter Y, list, or skip.")

        print()

    # ── Step 5: Deactivate Teller? ────────────────────────────────────────────
    print(f"\n  {BOLD}Disable Teller after migration?{RESET}")
    info("This stops future Teller syncs. Account records and history are kept.")
    deactivate = ask("Deactivate Teller enrollments?", default="y")
    if not deactivate:
        warn("Teller will keep syncing. Run the migration again if duplicates reappear.")

    # ── Step 6: Preview ───────────────────────────────────────────────────────
    step(5, "Migration preview")
    confirmed = [m for m in user_mappings if m["sf_account_id"]]
    skipped   = [m for m in user_mappings if not m["sf_account_id"]]

    total_hist = total_dedup = total_teller_only = 0
    print()
    for m in confirmed:
        tel = next((a for a in teller_accounts if a["id"] == m["teller_account_id"]), {})
        sf  = next((a for a in sf_accounts      if a["id"] == m["sf_account_id"]),      {})
        est = estimates.get(m["teller_account_id"], {})
        hist = est.get("historical_keep", "?")
        dedup = est.get("overlap_dedup", "?")
        only  = est.get("overlap_teller_only", "?")
        if isinstance(hist, int): total_hist += hist
        if isinstance(dedup, int): total_dedup += dedup
        if isinstance(only, int): total_teller_only += only
        print(f"  {BOLD}{tel.get('account_name', '?')}{RESET} → {CYAN}{sf.get('account_name', '?')}{RESET}")
        print(f"    {hist} pre-SimpleFIN transactions kept")
        print(f"    {dedup} overlap transactions deduplicated")
        if only:
            print(f"    {only} Teller-only transactions preserved (SimpleFIN missed these)")
        print()

    if skipped:
        warn(f"{len(skipped)} account(s) skipped — no deduplication applied to them.")

    print(f"  {BOLD}Total:{RESET} {total_hist} historical kept · {total_dedup} deduped · {total_teller_only} Teller-only kept")
    if deactivate:
        info(f"  Teller: {analysis['estimates']['total_teller_enrollments']} enrollment(s) will be deactivated")

    if not confirmed and not deactivate:
        warn("All accounts skipped and Teller not deactivated — nothing to do.")
        sys.exit(0)

    if not ask(f"\n  {BOLD}Proceed with migration?{RESET} This cannot be undone", default="n"):
        print("\n  Aborted. No changes made.\n")
        sys.exit(0)

    # ── Step 7: Execute ───────────────────────────────────────────────────────
    step(6, "Running migration…")
    try:
        with get_db() as conn:
            result = execute_migration(user_mappings, deactivate, conn)

        print()
        ok(f"{result['historical_kept']} historical Teller transactions kept")
        ok(f"{result['overlap_deduped']} duplicate transactions removed")
        if result['overlap_teller_only']:
            ok(f"{result['overlap_teller_only']} Teller-only transactions preserved")
        if result['teller_tokens_deactivated']:
            ok(f"{result['teller_tokens_deactivated']} Teller enrollment(s) deactivated")
        if result['teller_accounts_deactivated']:
            ok(f"{result['teller_accounts_deactivated']} Teller account(s) marked inactive")

        print(f"\n  {GREEN}{BOLD}Migration complete.{RESET}")
        print("  Open Folio — your full history is now available under SimpleFIN.\n")

        if deactivate:
            print(f"  {YELLOW}Next step:{RESET} Remove your *_TOKEN vars from .env to prevent")
            print("  accidental Teller re-activation on the next docker compose up.\n")

    except Exception as exc:
        print()
        err(f"Migration failed: {exc}")
        err("All changes have been rolled back. Your data is unchanged.")
        sys.exit(1)


if __name__ == "__main__":
    main()
