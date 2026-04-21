#!/usr/bin/env python3
"""
Create a deterministic Folio demo database with synthetic accounts and transactions.

This is intended for public demo deployments where:
1. the UI should feel realistic,
2. no real banking data should be shipped, and
3. changes can safely reset on redeploy.
"""

from __future__ import annotations

import argparse
import os
import random
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path


@dataclass(frozen=True)
class AccountSeed:
    id: str
    profile_id: str
    institution_name: str
    account_name: str
    account_type: str
    account_subtype: str
    balance: float


@dataclass(frozen=True)
class MerchantSeed:
    key: str
    display: str
    category: str
    domain: str
    industry: str
    city: str
    state: str
    amount_min: float
    amount_max: float


TODAY = date.today()
NOW = datetime.now().replace(microsecond=0).isoformat(sep=" ")

PROFILES = [
    ("primary", "Primary", 1),
    ("shared", "Shared", 0),
]

ACCOUNTS = [
    AccountSeed("demo_chk_primary", "primary", "Northstar Bank", "Everyday Checking", "depository", "checking", 5482.31),
    AccountSeed("demo_cc_primary", "primary", "Northstar Bank", "Travel Card", "credit", "credit_card", -842.77),
    AccountSeed("demo_sav_primary", "primary", "Northstar Bank", "Rainy Day Savings", "depository", "savings", 16840.12),
    AccountSeed("demo_chk_shared", "shared", "Summit Credit Union", "House Checking", "depository", "checking", 7124.54),
]

FIXED_SPEND = [
    MerchantSeed("HARBOR VIEW APARTMENTS", "Harbor View Apartments", "Housing", "harborview.example", "Housing", "Seattle", "WA", 1825, 1825),
    MerchantSeed("GRID ELECTRIC", "Grid Electric", "Utilities", "gridelectric.example", "Utilities", "Seattle", "WA", 78, 146),
    MerchantSeed("WAVE MOBILE", "Wave Mobile", "Utilities", "wavemobile.example", "Telecom", "Seattle", "WA", 55, 88),
    MerchantSeed("PINE INTERNET", "Pine Internet", "Utilities", "pineinternet.example", "Telecom", "Seattle", "WA", 62, 84),
]

SUBSCRIPTIONS = [
    MerchantSeed("STREAMSPACE", "StreamSpace", "Entertainment", "streamspace.example", "Streaming", "Los Angeles", "CA", 14, 19),
    MerchantSeed("NOTEFLOW", "NoteFlow", "Subscriptions", "noteflow.example", "Software", "San Francisco", "CA", 8, 14),
    MerchantSeed("MOVEFIT", "MoveFit", "Healthcare", "movefit.example", "Fitness", "Seattle", "WA", 49, 79),
]

VARIABLE_SPEND = [
    MerchantSeed("SUNBEAM MARKET", "Sunbeam Market", "Groceries", "sunbeam.example", "Groceries", "Seattle", "WA", 34, 128),
    MerchantSeed("LANTERN CAFE", "Lantern Cafe", "Food & Dining", "lanterncafe.example", "Cafe", "Seattle", "WA", 9, 28),
    MerchantSeed("CINDER BISTRO", "Cinder Bistro", "Food & Dining", "cinderbistro.example", "Restaurant", "Seattle", "WA", 24, 76),
    MerchantSeed("CITYRIDE", "CityRide", "Transportation", "cityride.example", "Transportation", "Seattle", "WA", 11, 36),
    MerchantSeed("TRAILHEAD GOODS", "Trailhead Goods", "Shopping", "trailheadgoods.example", "Retail", "Portland", "OR", 22, 165),
    MerchantSeed("CINEMA NORTH", "Cinema North", "Entertainment", "cinemanorth.example", "Entertainment", "Seattle", "WA", 14, 42),
    MerchantSeed("RIVER PHARMACY", "River Pharmacy", "Healthcare", "riverpharmacy.example", "Pharmacy", "Seattle", "WA", 12, 58),
]

SPECIAL_PURCHASES = [
    MerchantSeed("SKYLINE AIR", "Skyline Air", "Transportation", "skylineair.example", "Travel", "San Diego", "CA", 220, 540),
    MerchantSeed("HEARTH HOME", "Hearth Home", "Shopping", "hearthhome.example", "Home Goods", "Seattle", "WA", 140, 420),
    MerchantSeed("PEAK OUTDOOR", "Peak Outdoor", "Shopping", "peakoutdoor.example", "Outdoor Retail", "Bend", "OR", 120, 360),
    MerchantSeed("NOVA MEDICAL", "Nova Medical", "Healthcare", "novamedical.example", "Healthcare", "Seattle", "WA", 90, 260),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a synthetic Folio demo database.")
    parser.add_argument("--output", default=str(Path(__file__).with_name("Folio-demo.db")))
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--months", type=int, default=8)
    parser.add_argument("--force", action="store_true", help="Overwrite the output file if it already exists.")
    return parser.parse_args()


def init_demo_db(output_path: Path) -> None:
    os.environ["DB_FILE"] = str(output_path)
    from database import init_db  # Imported after DB_FILE is set.

    init_db()


def reset_demo_tables(conn: sqlite3.Connection) -> None:
    for table in (
        "transactions",
        "accounts",
        "profiles",
        "net_worth_history",
        "merchants",
        "merchant_aliases",
        "enrolled_tokens",
        "simplefin_connections",
        "copilot_conversations",
    ):
        conn.execute(f"DELETE FROM {table}")


def add_profiles(conn: sqlite3.Connection) -> None:
    conn.executemany(
        "INSERT INTO profiles (id, display_name, is_default, created_at) VALUES (?, ?, ?, ?)",
        [(pid, name, is_default, NOW) for pid, name, is_default in PROFILES],
    )


def add_accounts(conn: sqlite3.Connection) -> None:
    conn.executemany(
        """
        INSERT INTO accounts (
            id, profile_id, institution_name, account_name, account_type, account_subtype,
            current_balance, available_balance, currency, last_synced_at, is_active, provider
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'USD', ?, 1, 'demo')
        """,
        [
            (
                item.id,
                item.profile_id,
                item.institution_name,
                item.account_name,
                item.account_type,
                item.account_subtype,
                item.balance,
                item.balance,
                NOW,
            )
            for item in ACCOUNTS
        ],
    )


def month_anchor(months_ago: int) -> date:
    month_index = TODAY.month - 1 - months_ago
    year = TODAY.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def clamp_day(year: int, month: int, day: int) -> date:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = (next_month - timedelta(days=1)).day
    return date(year, month, min(day, last_day))


def insert_tx(
    rows: list[tuple],
    tx_id: str,
    account: AccountSeed,
    posted_on: date,
    amount: float,
    category: str,
    merchant: MerchantSeed | None,
    description: str,
) -> None:
    merchant_name = merchant.display if merchant else ""
    rows.append(
        (
            tx_id,
            account.id,
            account.profile_id,
            posted_on.isoformat(),
            description,
            description,
            round(amount, 2),
            category,
            "demo_seed",
            category,
            "card_payment" if amount < 0 else "credit",
            merchant_name,
            "organization" if merchant else "",
            category.lower().replace(" ", "_"),
            account.account_name,
            account.account_type,
            merchant_name,
            merchant.domain if merchant else "",
            merchant.industry if merchant else "",
            merchant.city if merchant else "",
            merchant.state if merchant else "",
            1,
            "high",
            0,
            None,
            merchant_name.lower() if merchant_name else description.lower(),
            NOW,
            0,
        )
    )


def build_transactions(rng: random.Random, months: int) -> list[tuple]:
    rows: list[tuple] = []
    tx_counter = 1
    account_by_id = {item.id: item for item in ACCOUNTS}
    primary_checking = account_by_id["demo_chk_primary"]
    primary_credit = account_by_id["demo_cc_primary"]
    primary_savings = account_by_id["demo_sav_primary"]
    shared_checking = account_by_id["demo_chk_shared"]

    for months_ago in range(months - 1, -1, -1):
        anchor = month_anchor(months_ago)
        year, month = anchor.year, anchor.month
        progress = months - months_ago - 1

        # Give each month a different personality so charts feel less synthetic.
        month_profile = [0.84, 0.92, 1.05, 0.97, 1.18, 0.89, 1.27, 1.01][progress % 8]
        grocery_profile = [0.88, 0.95, 1.02, 1.08, 1.16, 0.93, 1.24, 0.98][progress % 8]
        dining_profile = [0.82, 0.9, 1.12, 0.96, 1.22, 0.92, 1.3, 1.04][progress % 8]
        transit_profile = [0.9, 1.0, 1.05, 0.93, 1.15, 0.94, 1.18, 0.97][progress % 8]

        # Income
        for payday in (1, 15):
            insert_tx(rows, f"demo_tx_{tx_counter:04d}", primary_checking, clamp_day(year, month, payday), 3250 + rng.uniform(-40, 60), "Income", None, "Northstar Payroll")
            tx_counter += 1
            insert_tx(rows, f"demo_tx_{tx_counter:04d}", shared_checking, clamp_day(year, month, payday + 1), 2100 + rng.uniform(-25, 45), "Income", None, "Studio Payroll")
            tx_counter += 1

        # Fixed spend
        for merchant, day in zip(FIXED_SPEND, (2, 6, 9, 12), strict=True):
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_checking if merchant.category != "Housing" else shared_checking,
                clamp_day(year, month, day),
                -rng.uniform(merchant.amount_min, merchant.amount_max),
                merchant.category,
                merchant,
                merchant.display,
            )
            tx_counter += 1

        # Savings sweep and card payment
        insert_tx(rows, f"demo_tx_{tx_counter:04d}", primary_checking, clamp_day(year, month, 4), -450, "Savings Transfer", None, "Transfer to Rainy Day Savings")
        tx_counter += 1
        insert_tx(rows, f"demo_tx_{tx_counter:04d}", primary_savings, clamp_day(year, month, 4), 450, "Savings Transfer", None, "Transfer from Everyday Checking")
        tx_counter += 1
        insert_tx(rows, f"demo_tx_{tx_counter:04d}", primary_checking, clamp_day(year, month, 27), -rng.uniform(480, 920), "Credit Card Payment", None, "Travel Card Payment")
        tx_counter += 1

        # Subscriptions
        for merchant, day in zip(SUBSCRIPTIONS, (3, 11, 19), strict=True):
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_credit if merchant.category != "Healthcare" else primary_checking,
                clamp_day(year, month, day),
                -rng.uniform(merchant.amount_min, merchant.amount_max),
                merchant.category,
                merchant,
                merchant.display,
            )
            tx_counter += 1

        # Variable weekly spend
        for week_start in (5, 12, 19, 26):
            grocery = VARIABLE_SPEND[0]
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                shared_checking,
                clamp_day(year, month, week_start),
                -rng.uniform(grocery.amount_min, grocery.amount_max) * grocery_profile,
                grocery.category,
                grocery,
                grocery.display,
            )
            tx_counter += 1

            cafe = VARIABLE_SPEND[1]
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_credit,
                clamp_day(year, month, week_start + 1),
                -rng.uniform(cafe.amount_min, cafe.amount_max) * dining_profile,
                cafe.category,
                cafe,
                cafe.display,
            )
            tx_counter += 1

            ride = VARIABLE_SPEND[3]
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_credit,
                clamp_day(year, month, week_start + 2),
                -rng.uniform(ride.amount_min, ride.amount_max) * transit_profile,
                ride.category,
                ride,
                ride.display,
            )
            tx_counter += 1

        # Rotating fun/retail/health spend
        for merchant, day in zip(VARIABLE_SPEND[2:], (8, 14, 17, 22, 24), strict=True):
            account = primary_credit if merchant.category in {"Food & Dining", "Shopping", "Entertainment"} else primary_checking
            amount = -rng.uniform(merchant.amount_min, merchant.amount_max) * month_profile
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                account,
                clamp_day(year, month, day),
                amount,
                merchant.category,
                merchant,
                merchant.display,
            )
            tx_counter += 1

        # A couple of months should look meaningfully heavier or lighter.
        if progress % 4 == 0:
            purchase = SPECIAL_PURCHASES[(progress // 4) % len(SPECIAL_PURCHASES)]
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_credit if purchase.category in {"Shopping", "Transportation"} else primary_checking,
                clamp_day(year, month, 21),
                -rng.uniform(purchase.amount_min, purchase.amount_max),
                purchase.category,
                purchase,
                purchase.display,
            )
            tx_counter += 1

        if progress % 5 == 2:
            merchant = VARIABLE_SPEND[2]
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_credit,
                clamp_day(year, month, 23),
                -rng.uniform(merchant.amount_min + 40, merchant.amount_max + 120),
                merchant.category,
                merchant,
                f"{merchant.display} Group Dinner",
            )
            tx_counter += 1

        # Occasional refund to show positive non-income behavior
        if months_ago % 3 == 0:
            merchant = VARIABLE_SPEND[4]
            insert_tx(
                rows,
                f"demo_tx_{tx_counter:04d}",
                primary_credit,
                clamp_day(year, month, 25),
                rng.uniform(18, 42),
                "Shopping",
                merchant,
                f"{merchant.display} Refund",
            )
            tx_counter += 1

    return rows


def add_transactions(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    conn.executemany(
        """
        INSERT INTO transactions (
            id, account_id, profile_id, date, description, raw_description, amount, category,
            categorization_source, original_category, transaction_type, counterparty_name,
            counterparty_type, teller_category, account_name, account_type, merchant_name,
            merchant_domain, merchant_industry, merchant_city, merchant_state, enriched,
            confidence, is_excluded, expense_type, description_normalized, updated_at, category_pinned
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def add_net_worth_history(conn: sqlite3.Connection, rng: random.Random, months: int) -> None:
    rows = []
    base_primary = 16400
    base_shared = 9800
    for months_ago in range(months - 1, -1, -1):
        anchor = month_anchor(months_ago)
        progress = months - months_ago
        primary_assets = base_primary + progress * 920 + rng.uniform(-180, 220)
        primary_owed = 1800 - progress * 75 + rng.uniform(-60, 60)
        shared_assets = base_shared + progress * 540 + rng.uniform(-140, 180)
        shared_owed = 600 + rng.uniform(-30, 45)
        rows.extend(
            [
                (anchor.isoformat(), "primary", round(primary_assets, 2), round(primary_owed, 2), round(primary_assets - primary_owed, 2)),
                (anchor.isoformat(), "shared", round(shared_assets, 2), round(shared_owed, 2), round(shared_assets - shared_owed, 2)),
            ]
        )
    conn.executemany(
        "INSERT INTO net_worth_history (date, profile_id, total_assets, total_owed, net_worth) VALUES (?, ?, ?, ?, ?)",
        rows,
    )


def main() -> None:
    args = parse_args()
    output_path = Path(args.output).expanduser().resolve()
    if output_path.exists() and not args.force:
        raise SystemExit(f"{output_path} already exists. Re-run with --force to overwrite it.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    init_demo_db(output_path)
    rng = random.Random(args.seed)

    conn = sqlite3.connect(str(output_path))
    try:
        reset_demo_tables(conn)
        add_profiles(conn)
        add_accounts(conn)
        add_transactions(conn, build_transactions(rng, args.months))
        add_net_worth_history(conn, rng, args.months)
        conn.commit()
    finally:
        conn.close()

    print(f"Created demo database at {output_path}")


if __name__ == "__main__":
    main()
