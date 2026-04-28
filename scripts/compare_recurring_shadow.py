#!/usr/bin/env python3
"""Compare legacy recurring state with the v2 recurring obligation model."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / "backend"
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import database  # noqa: E402
from recurring_obligations import backfill_from_legacy, shadow_comparison  # noqa: E402


def connect(db_file: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    return conn


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-file", default=str(ROOT / "data" / "Folio.db"))
    parser.add_argument("--profile", default=None)
    parser.add_argument("--days", type=int, default=45)
    parser.add_argument("--json", action="store_true", help="Print raw JSON.")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="Run the idempotent v2 legacy backfill before comparing.",
    )
    args = parser.parse_args()

    db_file = Path(args.db_file).expanduser().resolve()
    with connect(db_file) as conn:
        database._migrate_recurring_obligations(conn)
        backfill_counts = None
        if args.backfill:
            backfill_counts = backfill_from_legacy(conn, profile=args.profile)
        comparison = shadow_comparison(conn, profile=args.profile, days=args.days)
        if backfill_counts is not None:
            comparison["backfill_run"] = backfill_counts

    if args.json:
        print(json.dumps(comparison, indent=2, sort_keys=True))
        return 0

    legacy = comparison["legacy"]
    v2 = comparison["v2"]
    backfill = comparison["backfill"]
    print(f"Recurring shadow comparison: profile={comparison['profile']} days={comparison['days']}")
    print("")
    print("Legacy")
    print(f"  active:     {legacy['active_count']} (${legacy['active_annual_total']}/yr)")
    print(f"  inactive:   {legacy['inactive_count']}")
    print(f"  cancelled:  {legacy['cancelled_count']}")
    print(f"  dismissed:  {legacy['dismissed_count']}")
    print(f"  declared:   {legacy['user_declared_count']}")
    print("")
    print("V2 obligations")
    print(f"  active:     {v2['active_count']}")
    print(f"  inactive:   {v2['inactive_count']}")
    print(f"  candidates: {v2['candidate_count']}")
    print(f"  dismissed:  {v2['dismissed_count']}")
    print(f"  cancelled:  {v2['cancelled_count']}")
    print(f"  confirmed active annual: ${v2['confirmed_active_annual_total']}")
    print(f"  inferred active annual:  ${v2['inferred_active_annual_total']}")
    print(f"  upcoming confirmed:      ${v2['upcoming_confirmed_total']}")
    print(f"  upcoming inferred:       ${v2['upcoming_inferred_total']}")
    print(f"  needs review:            ${v2['needs_review_total']}")
    print("")
    print(f"Backfill integrity: {'ok' if backfill['ok'] else 'needs attention'}")
    if not backfill["ok"]:
        print(json.dumps(backfill, indent=2, sort_keys=True))
    return 0 if backfill["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
