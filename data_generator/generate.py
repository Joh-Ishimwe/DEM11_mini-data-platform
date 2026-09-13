"""Synthetic sales data generator.

DETERMINISTIC by design: the same --seed always produces the same file.
Non-deterministic fixtures create flaky tests, and a flaky test is worse than
no test because it trains the team to ignore red.

Usage:
    python -m data_generator.generate --rows 5000 --profile clean --out data/samples/
    python -m data_generator.generate --rows 5000 --profile messy --upload
"""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from data_generator.profiles import (
    CATEGORIES,
    COUNTRIES,
    CURRENCIES,
    MESSY_INJECTIONS,
    PROFILES,
)


def generate_clean(rows: int, seed: int = 42) -> pd.DataFrame:
    """Produce `rows` valid sales records. Deterministic given `seed`."""
    rng = random.Random(seed)
    today = date(2026, 1, 1)  # fixed, NOT date.today(): keeps output reproducible
    records = []
    for i in range(rows):
        records.append(
            {
                "order_id": f"ORD-{seed}-{i:07d}",
                "customer_id": f"CUST-{rng.randint(1, max(rows // 10, 1)):06d}",
                "order_date": (today - timedelta(days=rng.randint(0, 365))).isoformat(),
                "product_category": rng.choice(CATEGORIES),
                "quantity": rng.randint(1, 10),
                "unit_price": round(rng.uniform(1.0, 500.0), 2),
                "currency": rng.choice(CURRENCIES),
                "country": rng.choice(COUNTRIES),
            }
        )
    return pd.DataFrame(records)


def inject_mess(df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """Apply every injection in profiles.MESSY_INJECTIONS.

    Deterministic: one `random.Random(seed)` instance drives every choice, and
    injections are applied in MESSY_INJECTIONS' fixed iteration order, so the
    same seed always touches the same rows in the same way.

    Independent: each injection samples its own row indices from the full
    frame, so a single row can legitimately be hit by more than one mess
    (e.g. a whitespace-padded country on a row that also gets a bad price).

    dtype=object columns are used wherever a string value must sit next to
    numbers (e.g. '$12.50' in unit_price), so pandas cannot silently coerce
    the mess back into something clean before the pipeline ever sees it.
    """
    out = df.copy()
    # Keep the original numeric values around: two injections can land on the
    # same row (they're independent), and formatting an already-messed-up
    # value (e.g. re-parsing '$12.50' as a float) would blow up.
    original_quantity = df["quantity"].copy()
    original_price = df["unit_price"].copy()

    for col in (
        "order_id",
        "customer_id",
        "order_date",
        "product_category",
        "quantity",
        "unit_price",
        "country",
    ):
        out[col] = out[col].astype(object)

    rng = random.Random(seed)
    n = len(out)
    if n == 0:
        return out

    def pick(fraction: float) -> list[int]:
        k = max(1, round(n * fraction))
        return rng.sample(range(n), min(k, n))

    # null_order_id: cannot identify the order -> quarantine.
    for i in pick(MESSY_INJECTIONS["null_order_id"]):
        out.at[i, "order_id"] = None

    # null_customer_id: quarantine.
    for i in pick(MESSY_INJECTIONS["null_customer_id"]):
        out.at[i, "customer_id"] = None

    # duplicate_order_id: copy another row's id onto this one. cleaning must dedupe.
    for i in pick(MESSY_INJECTIONS["duplicate_order_id"]):
        donor = rng.randrange(n)
        out.at[i, "order_id"] = out.at[donor, "order_id"]

    # negative_quantity: impossible -> quarantine.
    for i in pick(MESSY_INJECTIONS["negative_quantity"]):
        out.at[i, "quantity"] = -abs(int(original_quantity.iloc[i]))

    # price_with_currency_symbol: cleaning must coerce '$12.50' -> 12.50.
    for i in pick(MESSY_INJECTIONS["price_with_currency_symbol"]):
        out.at[i, "unit_price"] = f"${float(original_price.iloc[i]):.2f}"

    # price_with_comma_decimal: cleaning must coerce '12,50' -> 12.50.
    for i in pick(MESSY_INJECTIONS["price_with_comma_decimal"]):
        out.at[i, "unit_price"] = f"{float(original_price.iloc[i]):.2f}".replace(
            ".", ","
        )

    # future_order_date: data entry error -> quarantine.
    for i in pick(MESSY_INJECTIONS["future_order_date"]):
        future = date(2026, 1, 1) + timedelta(days=rng.randint(30, 3650))
        out.at[i, "order_date"] = future.isoformat()

    # unparseable_date: coerce to NaT, then quarantine.
    for i in pick(MESSY_INJECTIONS["unparseable_date"]):
        out.at[i, "order_date"] = "not-a-date"

    # unknown_category: DECIDED (docs/decisions.md ADR-011) to quarantine rather
    # than silently bucket into 'other', so a new category is never invisible.
    for i in pick(MESSY_INJECTIONS["unknown_category"]):
        out.at[i, "product_category"] = "unobtainium"

    # inconsistent_country_case: cleaning must normalise, NOT quarantine.
    for i in pick(MESSY_INJECTIONS["inconsistent_country_case"]):
        value = str(out.at[i, "country"])
        out.at[i, "country"] = rng.choice([value.upper(), value.lower()])

    # whitespace_padding: cleaning must strip.
    for i in pick(MESSY_INJECTIONS["whitespace_padding"]):
        column = rng.choice(["country", "customer_id", "product_category"])
        out.at[i, column] = f"  {out.at[i, column]}  "

    # empty_string_country: nullable, should survive.
    for i in pick(MESSY_INJECTIONS["empty_string_country"]):
        out.at[i, "country"] = ""

    return out


def apply_schema_drift(df: pd.DataFrame) -> pd.DataFrame:
    """Rename `unit_price` to `price` and turn `quantity` into strings.

    This profile exists for one purpose: to prove to yourself that CI going
    green says nothing about tomorrow's data. Feed this through your pipeline
    and watch where it fails. Then decide where the check belongs.
    """
    out = df.copy()
    out = out.rename(columns={"unit_price": "price"})
    out["quantity"] = out["quantity"].astype(str)
    return out


def build(rows: int, profile: str, seed: int) -> pd.DataFrame:
    if profile == "empty":
        return generate_clean(0, seed)
    df = generate_clean(rows, seed)
    if profile == "messy":
        return inject_mess(df, seed)
    if profile == "schema_drift":
        return apply_schema_drift(df)
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic sales data")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--profile", choices=PROFILES, default="clean")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path("data/samples"))
    parser.add_argument(
        "--upload",
        action="store_true",
        help="write straight into the MinIO raw bucket instead of local disk",
    )
    args = parser.parse_args()

    df = build(args.rows, args.profile, args.seed)
    filename = f"sales_{args.profile}_{args.seed}_{len(df)}.csv"

    if args.upload:
        from src.ingestion.minio_client import MinioClient
        from src.utils.constants import MinioConfig

        config = MinioConfig.from_env()
        client = MinioClient(config)
        payload = df.to_csv(index=False).encode("utf-8")
        key = f"sales/{filename}"
        client.upload_bytes(config.raw_bucket, key, payload)
        print(f"uploaded {len(df)} rows to s3://{config.raw_bucket}/{key}")
        return

    args.out.mkdir(parents=True, exist_ok=True)
    target = args.out / filename
    df.to_csv(target, index=False)
    print(f"wrote {len(df)} rows to {target}")


if __name__ == "__main__":
    main()
