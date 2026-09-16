"""Set up Metabase entirely from the API: connection, questions, dashboard.

Why a script instead of clicking through the UI: a dashboard built by hand
only exists on the machine where you built it. This script makes the
dashboard reproducible from a fresh `make up`, the same way the buckets and
schemas already are.

Usage (stack must already be up and Metabase healthy):

    python scripts/setup_metabase.py

Idempotent-ish: re-running it won't duplicate the database connection or the
dashboard if they already exist under the same name, but it will add a new
copy of each question. If you want to rebuild from scratch, delete the
"Sales Overview" dashboard and the "analytics" database connection in the
Metabase admin UI first.

Metabase's API has shifted shape across releases; this was written against
v0.50.x (the version pinned in docker-compose.yml). If a request below
returns something unexpected, the fallback is the "Manual setup" section in
docs/runbook.md, which lists the same four SQL queries to paste into
New Question -> Native Query by hand.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request
import json as _json

MB_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ.get("METABASE_ADMIN_EMAIL", "admin@example.com")
# Metabase's own setup form rejects "change_me_locally" as too common - it
# runs new passwords through a strength/common-password check that none of
# this project's other services apply, so the fallback here can't reuse the
# same placeholder those use.
ADMIN_PASSWORD = os.environ.get("METABASE_ADMIN_PASSWORD", "ChangeMe-2026!Local")
ADMIN_FIRST_NAME = os.environ.get("METABASE_ADMIN_FIRST_NAME", "Data")
ADMIN_LAST_NAME = os.environ.get("METABASE_ADMIN_LAST_NAME", "Engineer")

DB_NAME = "analytics"
DASHBOARD_NAME = "Sales Overview"

QUESTIONS = [
    {
        "name": "Revenue trend over time",
        "display": "line",
        "sql": """
            SELECT date_trunc('day', order_date)::date AS day, SUM(revenue) AS revenue
            FROM marts.sales_orders
            GROUP BY 1
            ORDER BY 1
        """,
    },
    {
        "name": "Revenue by product category",
        "display": "bar",
        "sql": """
            SELECT product_category, SUM(revenue) AS revenue
            FROM marts.sales_orders
            GROUP BY 1
            ORDER BY revenue DESC
        """,
    },
    {
        "name": "Top 10 customers by revenue",
        "display": "bar",
        "sql": """
            SELECT customer_id, SUM(revenue) AS revenue
            FROM marts.sales_orders
            GROUP BY 1
            ORDER BY revenue DESC
            LIMIT 10
        """,
    },
    {
        "name": "Rejected rows per day",
        "display": "bar",
        "sql": """
            SELECT day, rejected_rows
            FROM marts.v_daily_rejections
            ORDER BY day
        """,
    },
    {
        "name": "Recent pipeline runs",
        "display": "table",
        "sql": """
            SELECT started_at, source_file, status, rows_read, rows_loaded, rows_rejected
            FROM marts.v_pipeline_runs
            ORDER BY started_at DESC
            LIMIT 20
        """,
    },
]


def _request(
    method: str, path: str, token: str | None = None, body: dict | None = None
) -> dict:
    url = f"{MB_URL}{path}"
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Metabase-Session"] = token
    data = _json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(
        request, timeout=30
    ) as response:  # noqa: S310 - local trusted host
        raw = response.read()
        return _json.loads(raw) if raw else {}


def wait_for_metabase(timeout: int = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            health = _request("GET", "/api/health")
            if health.get("status") == "ok":
                return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(5)
    raise TimeoutError("Metabase never became healthy")


def ensure_admin_and_login() -> str:
    """Run first-time setup if needed, then return a session token."""
    properties = _request("GET", "/api/session/properties")
    setup_token = properties.get("setup-token")

    if setup_token:
        print("Running first-time Metabase setup...")
        result = _request(
            "POST",
            "/api/setup",
            body={
                "token": setup_token,
                "user": {
                    "first_name": ADMIN_FIRST_NAME,
                    "last_name": ADMIN_LAST_NAME,
                    "email": ADMIN_EMAIL,
                    "password": ADMIN_PASSWORD,
                },
                "prefs": {"site_name": "Mini Data Platform", "allow_tracking": False},
            },
        )
        return result["id"]

    print("Metabase already set up, logging in...")
    result = _request(
        "POST",
        "/api/session",
        body={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
    )
    return result["id"]


def ensure_database(token: str) -> int:
    databases = _request("GET", "/api/database", token=token).get("data", [])
    existing = next((d for d in databases if d["name"] == DB_NAME), None)
    if existing:
        print(f"Database '{DB_NAME}' already connected (id={existing['id']}).")
        return existing["id"]

    print(f"Connecting Metabase to '{DB_NAME}'...")
    created = _request(
        "POST",
        "/api/database",
        token=token,
        body={
            "engine": "postgres",
            "name": DB_NAME,
            "details": {
                "host": "postgres",
                "port": 5432,
                "dbname": DB_NAME,
                "user": os.environ.get("METABASE_READONLY_USER", "metabase_ro"),
                "password": os.environ.get(
                    "METABASE_READONLY_PASSWORD", "change_me_locally"
                ),
                "ssl": False,
            },
        },
    )
    return created["id"]


def create_question(
    token: str, database_id: int, name: str, sql: str, display: str
) -> int:
    card = _request(
        "POST",
        "/api/card",
        token=token,
        body={
            "name": name,
            "display": display,
            "visualization_settings": {},
            "dataset_query": {
                "type": "native",
                "native": {"query": sql.strip()},
                "database": database_id,
            },
        },
    )
    print(f"  created question: {name} (id={card['id']})")
    return card["id"]


def build_dashboard(token: str, card_ids: list[int]) -> int:
    dashboards = _request("GET", "/api/dashboard", token=token)
    existing = next((d for d in dashboards if d["name"] == DASHBOARD_NAME), None)
    if existing:
        print(
            f"Dashboard '{DASHBOARD_NAME}' already exists (id={existing['id']}); leaving it as-is."
        )
        return existing["id"]

    dashboard = _request(
        "POST", "/api/dashboard", token=token, body={"name": DASHBOARD_NAME}
    )
    dashboard_id = dashboard["id"]

    # There is no "add one card" endpoint in this Metabase version - adding
    # cards to a dashboard means PUT-ing the dashboard's ENTIRE dashcards
    # list. Each new dashcard needs a unique NEGATIVE id (Metabase's
    # convention for "not created yet, please assign a real one") alongside
    # the real, positive card_id of the question it displays.
    dashcards = []
    for i, card_id in enumerate(card_ids):
        row, col = divmod(i, 2)
        dashcards.append(
            {
                "id": -(i + 1),
                "card_id": card_id,
                "row": row * 4,
                "col": col * 6,
                "size_x": 6,
                "size_y": 4,
            }
        )
    _request(
        "PUT",
        f"/api/dashboard/{dashboard_id}",
        token=token,
        body={"dashcards": dashcards},
    )
    print(
        f"Dashboard '{DASHBOARD_NAME}' built (id={dashboard_id}) with {len(card_ids)} cards."
    )
    return dashboard_id


def main() -> int:
    print(f"Waiting for Metabase at {MB_URL}...")
    wait_for_metabase()

    token = ensure_admin_and_login()
    database_id = ensure_database(token)

    print(f"Creating the {len(QUESTIONS)} dashboard cards...")
    card_ids = [
        create_question(token, database_id, q["name"], q["sql"], q["display"])
        for q in QUESTIONS
    ]

    build_dashboard(token, card_ids)
    print("Done. Open Metabase and look for 'Sales Overview'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
