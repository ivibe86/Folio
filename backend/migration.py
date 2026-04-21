"""
migration.py
Teller → SimpleFIN migration logic.

Two public functions:
  analyze_migration(conn)  → preview payload for GET /api/migration/preview
  execute_migration(...)   → atomic migration for POST /api/migration/execute
"""

import re
from datetime import datetime, timedelta, timezone

from log_config import get_logger

logger = get_logger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_last_four(name: str, acct_id: str = "") -> str | None:
    """Return the last trailing 4-digit group found in name or id, else None."""
    for source in (name, acct_id):
        sequences = re.findall(r'\d{4,}', source or "")
        if sequences:
            return sequences[-1][-4:]
    return None


def _match_confidence(tel: dict, sf: dict) -> str:
    """
    Rate how likely two accounts represent the same real-world account.
    Returns: 'high' | 'medium' | 'low' | 'none'
    """
    # Types must agree at the top level (credit vs depository)
    tel_type = (tel.get("account_type") or "").lower()
    sf_type  = (sf.get("account_type")  or "").lower()
    if tel_type != sf_type:
        return "none"

    # Last-4 overlap → high confidence
    tel_l4 = _extract_last_four(tel.get("account_name", ""), tel.get("id", ""))
    sf_l4  = _extract_last_four(sf.get("account_name",  ""), sf.get("id",  ""))
    if tel_l4 and sf_l4 and tel_l4 == sf_l4:
        return "high"

    # Name keyword overlap (≥4-char words, ignoring digits-only tokens)
    tel_words = {w for w in re.split(r'\W+', (tel.get("account_name") or "").upper()) if len(w) >= 4 and not w.isdigit()}
    sf_words  = {w for w in re.split(r'\W+', (sf.get("account_name")  or "").upper()) if len(w) >= 4 and not w.isdigit()}
    common = tel_words & sf_words

    tel_inst = (tel.get("institution_name") or "").upper()
    sf_inst  = (sf.get("institution_name")  or "").upper()
    inst_overlap = bool(tel_inst and sf_inst and (
        tel_inst in sf_inst or sf_inst in tel_inst or
        # partial: first word of institution matches
        (tel_inst.split()[0] in sf_inst if tel_inst.split() else False)
    ))

    if common and inst_overlap:
        return "high"
    if common or inst_overlap:
        return "medium"

    return "low"


def _sf_window_start(conn) -> str | None:
    """Return the earliest date of any SimpleFIN transaction, or None."""
    row = conn.execute(
        "SELECT MIN(date) FROM transactions WHERE id LIKE 'sf_%' AND is_excluded = 0"
    ).fetchone()
    return row[0] if row and row[0] else None


def _parse_date(d: str) -> datetime:
    return datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _fuzzy_match(teller_tx: dict, candidates: list[dict], used_ids: set) -> dict | None:
    """
    Find the best SimpleFIN transaction that matches a Teller transaction.
    Criteria: exact amount (±0.01) + date within ±3 days.
    One-to-one: each SF tx can only be matched once (used_ids tracks consumed matches).
    """
    tel_amount = float(teller_tx.get("amount", 0))
    tel_date   = _parse_date(teller_tx["date"])

    best = None
    best_days = 999

    for sf_tx in candidates:
        if sf_tx["id"] in used_ids:
            continue
        if abs(float(sf_tx.get("amount", 0)) - tel_amount) > 0.01:
            continue
        days_diff = abs((_parse_date(sf_tx["date"]) - tel_date).days)
        if days_diff <= 3 and days_diff < best_days:
            best = sf_tx
            best_days = days_diff

    return best


# ── Public API ────────────────────────────────────────────────────────────────

def analyze_migration(conn) -> dict:
    """
    Read-only analysis. Returns the preview payload including suggested account
    mappings, confidence ratings, and estimated transaction counts per mapping.
    """
    # Active Teller accounts
    teller_accounts = [
        dict(r) for r in conn.execute(
            """SELECT id, account_name, institution_name, account_type,
                      account_subtype, currency, profile_id as profile
               FROM accounts WHERE provider = 'teller' AND is_active = 1
               ORDER BY profile_id, account_name"""
        ).fetchall()
    ]

    # Active SimpleFIN accounts
    sf_accounts = [
        dict(r) for r in conn.execute(
            """SELECT id, account_name, institution_name, account_type,
                      account_subtype, currency, profile_id as profile
               FROM accounts WHERE provider = 'simplefin' AND is_active = 1
               ORDER BY profile_id, account_name"""
        ).fetchall()
    ]

    sf_window_start = _sf_window_start(conn)

    # Active Teller enrollments (for deactivation count)
    teller_enrollments = [
        dict(r) for r in conn.execute(
            "SELECT id, profile, institution FROM enrolled_tokens WHERE is_active = 1"
        ).fetchall()
    ]

    # Build suggested mappings (only same-profile pairs)
    suggested_mappings = []
    for tel in teller_accounts:
        best_sf   = None
        best_conf = "none"
        conf_rank = {"high": 3, "medium": 2, "low": 1, "none": 0}

        for sf in sf_accounts:
            if sf["profile"] != tel["profile"]:
                continue
            conf = _match_confidence(tel, sf)
            if conf_rank[conf] > conf_rank[best_conf]:
                best_sf   = sf
                best_conf = conf

        if best_sf:
            suggested_mappings.append({
                "teller_account_id": tel["id"],
                "sf_account_id":     best_sf["id"],
                "confidence":        best_conf,
            })

    # Compute per-mapping estimates (dry-run)
    per_mapping_estimates = []
    for mapping in suggested_mappings:
        tel_id = mapping["teller_account_id"]
        sf_id  = mapping["sf_account_id"]

        historical_keep = 0
        overlap_dedup   = 0
        overlap_teller_only = 0

        if sf_window_start:
            historical_keep = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE account_id = ? AND date < ? AND is_excluded = 0",
                (tel_id, sf_window_start),
            ).fetchone()[0]

            # Load overlap transactions for both accounts (in-memory fuzzy match)
            tel_overlap = [dict(r) for r in conn.execute(
                "SELECT id, date, amount FROM transactions WHERE account_id = ? AND date >= ? AND is_excluded = 0",
                (tel_id, sf_window_start),
            ).fetchall()]

            sf_overlap = [dict(r) for r in conn.execute(
                "SELECT id, date, amount FROM transactions WHERE account_id = ? AND date >= ? AND is_excluded = 0",
                (sf_id, sf_window_start),
            ).fetchall()]

            used_ids: set = set()
            for t_tx in tel_overlap:
                match = _fuzzy_match(t_tx, sf_overlap, used_ids)
                if match:
                    used_ids.add(match["id"])
                    overlap_dedup += 1
                else:
                    overlap_teller_only += 1
        else:
            # No SF transactions at all — everything is historical
            historical_keep = conn.execute(
                "SELECT COUNT(*) FROM transactions WHERE account_id = ? AND is_excluded = 0",
                (tel_id,),
            ).fetchone()[0]

        per_mapping_estimates.append({
            "teller_account_id":    tel_id,
            "sf_account_id":        sf_id,
            "historical_keep":      historical_keep,
            "overlap_dedup":        overlap_dedup,
            "overlap_teller_only":  overlap_teller_only,
        })

    return {
        "teller_accounts":      teller_accounts,
        "simplefin_accounts":   sf_accounts,
        "suggested_mappings":   suggested_mappings,
        "simplefin_window_start": sf_window_start,
        "teller_enrollments":   teller_enrollments,
        "estimates": {
            "per_mapping":                per_mapping_estimates,
            "total_teller_enrollments":   len(teller_enrollments),
            "total_teller_accounts":      len(teller_accounts),
        },
    }


def execute_migration(mappings: list[dict], deactivate_teller: bool, conn) -> dict:
    """
    Atomically run the migration. All writes happen inside a single SQLite
    transaction — any failure rolls back everything.

    mappings: [{"teller_account_id": "...", "sf_account_id": "..." | None}]
              sf_account_id = None means "skip this Teller account"
    """
    import token_store

    sf_window_start = _sf_window_start(conn)

    totals = {
        "historical_kept":            0,
        "overlap_deduped":            0,
        "overlap_teller_only":        0,
        "teller_tokens_deactivated":  0,
        "teller_accounts_deactivated": 0,
    }

    try:
        conn.execute("BEGIN IMMEDIATE")

        for mapping in mappings:
            tel_id = mapping.get("teller_account_id")
            sf_id  = mapping.get("sf_account_id")

            if not tel_id or not sf_id:
                continue  # user chose to skip this account

            # Validate both accounts exist and share a profile
            tel_row = conn.execute(
                "SELECT profile_id, provider FROM accounts WHERE id = ? AND is_active = 1",
                (tel_id,),
            ).fetchone()
            sf_row = conn.execute(
                "SELECT profile_id, provider FROM accounts WHERE id = ? AND is_active = 1",
                (sf_id,),
            ).fetchone()

            if not tel_row or not sf_row:
                raise ValueError(f"Account not found or inactive: teller={tel_id}, sf={sf_id}")
            if tel_row[0] != sf_row[0]:
                raise ValueError(f"Profile mismatch for accounts {tel_id} / {sf_id}")
            if tel_row[1] != "teller":
                raise ValueError(f"{tel_id} is not a Teller account")
            if sf_row[1] != "simplefin":
                raise ValueError(f"{sf_id} is not a SimpleFIN account")

            # Count historical transactions (pre-SF window) — never touched
            if sf_window_start:
                hist_count = conn.execute(
                    "SELECT COUNT(*) FROM transactions WHERE account_id = ? AND date < ? AND is_excluded = 0",
                    (tel_id, sf_window_start),
                ).fetchone()[0]
                totals["historical_kept"] += hist_count

                # Load overlap transactions for fuzzy matching
                tel_overlap = [dict(r) for r in conn.execute(
                    "SELECT id, date, amount FROM transactions WHERE account_id = ? AND date >= ? AND is_excluded = 0",
                    (tel_id, sf_window_start),
                ).fetchall()]

                sf_overlap = [dict(r) for r in conn.execute(
                    "SELECT id, date, amount FROM transactions WHERE account_id = ? AND date >= ? AND is_excluded = 0",
                    (sf_id, sf_window_start),
                ).fetchall()]

                used_ids: set = set()
                for t_tx in tel_overlap:
                    match = _fuzzy_match(t_tx, sf_overlap, used_ids)
                    if match:
                        used_ids.add(match["id"])
                        # Mark Teller row as superseded (excluded)
                        conn.execute(
                            """UPDATE transactions
                               SET is_excluded = 1,
                                   categorization_source = 'superseded-by-simplefin',
                                   updated_at = datetime('now')
                               WHERE id = ?""",
                            (t_tx["id"],),
                        )
                        totals["overlap_deduped"] += 1
                    else:
                        totals["overlap_teller_only"] += 1
            else:
                # No SF transactions yet — all Teller data is kept
                hist_count = conn.execute(
                    "SELECT COUNT(*) FROM transactions WHERE account_id = ? AND is_excluded = 0",
                    (tel_id,),
                ).fetchone()[0]
                totals["historical_kept"] += hist_count

        if deactivate_teller:
            # Deactivate all active Teller enrollments
            enrollment_ids = [
                r[0] for r in conn.execute(
                    "SELECT id FROM enrolled_tokens WHERE is_active = 1"
                ).fetchall()
            ]
            for eid in enrollment_ids:
                conn.execute(
                    "UPDATE enrolled_tokens SET is_active = 0 WHERE id = ?", (eid,)
                )
                totals["teller_tokens_deactivated"] += 1
                logger.info("Deactivated Teller enrollment ID %d via migration.", eid)

            # Deactivate Teller accounts so they drop out of balance totals
            result = conn.execute(
                "UPDATE accounts SET is_active = 0 WHERE provider = 'teller' AND is_active = 1"
            )
            totals["teller_accounts_deactivated"] = result.rowcount

        conn.commit()

        logger.info(
            "Migration complete: historical_kept=%d, overlap_deduped=%d, "
            "overlap_teller_only=%d, tokens_deactivated=%d, accounts_deactivated=%d",
            totals["historical_kept"], totals["overlap_deduped"],
            totals["overlap_teller_only"], totals["teller_tokens_deactivated"],
            totals["teller_accounts_deactivated"],
        )

        return {"status": "success", **totals}

    except Exception as exc:
        conn.rollback()
        logger.error("Migration failed, rolled back: %s", exc)
        raise
