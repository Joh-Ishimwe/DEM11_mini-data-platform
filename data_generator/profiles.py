"""Data profiles.

You need broken data on purpose. A test suite built only on perfect rows
proves nothing: it goes green against data that never occurs in reality.

Four profiles:
  clean        - everything valid. The happy path.
  messy        - every quarantine case your validation must catch.
  empty        - zero rows. Does your pipeline crash or handle it gracefully?
  schema_drift - a renamed column and a retyped column. Simulates the vendor
                 change that breaks pipelines in production while CI stays green.
"""

from __future__ import annotations

CATEGORIES = ["electronics", "apparel", "home", "grocery", "toys", "sports"]
CURRENCIES = ["USD", "EUR", "GBP", "RWF", "KES"]
COUNTRIES = ["Rwanda", "Kenya", "Ghana", "Germany", "United Kingdom", "United States"]

PROFILES = ("clean", "messy", "empty", "schema_drift")

# Each entry: (fraction of rows affected, what to do). Implement in generate.py.
MESSY_INJECTIONS: dict[str, float] = {
    "null_order_id": 0.02,  # quarantine: cannot identify the order
    "null_customer_id": 0.02,  # quarantine
    "duplicate_order_id": 0.03,  # cleaning should dedupe before validation
    "negative_quantity": 0.02,  # quarantine: impossible
    "price_with_currency_symbol": 0.05,  # cleaning must coerce '$12.50'
    "price_with_comma_decimal": 0.03,  # cleaning must coerce '12,50'
    "future_order_date": 0.01,  # quarantine: data entry error
    "unparseable_date": 0.02,  # coerce to NaT, then quarantine
    "unknown_category": 0.02,  # quarantine or map to 'other'? DECIDE and document
    "inconsistent_country_case": 0.05,  # cleaning must normalise, NOT quarantine
    "whitespace_padding": 0.05,  # cleaning must strip
    "empty_string_country": 0.02,  # nullable, should survive
}
