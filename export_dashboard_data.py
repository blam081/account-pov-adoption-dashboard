#!/usr/bin/env python3
"""
Refresh dashboard-data.json from Snowflake using queries documented in
../account-pov-success-measures-dashboard-sql.md

Usage:
  export SNOWFLAKE_SECRET=<snowflake-auth>   # required; see ../../.cursor/snowflake_conn.py
  pip install snowflake-connector-python
  python export_dashboard_data.py

  python export_dashboard_data.py --sample   # write JSON only (no DB), for layout checks

Commit and push dashboard-data.json; static hosts (GitHub Pages, etc.) serve the updated file.

See ../account-pov-success-measures-dashboard-sql.md for object names and fiscal-quarter logic.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
CURSOR = ROOT.parent / ".cursor"
OUT_JSON = ROOT / "dashboard-data.json"

# Funnel copy matches the static prototype; replace via JSON or extend this script when APM mapping is defined.
FUNNEL_DEFAULT: list[dict[str, Any]] = [
    {
        "smb": {"text": "+27% uplift (sig)", "sub": "Total connects & mtgs Actual/Benchmark", "tone": "good"},
        "all": {"text": "+21% uplift (sig)", "sub": "Total connects & mtgs Actual/Benchmark", "tone": "good"},
    },
    {
        "smb": {"text": "+18.8% uplift (Sig)", "sub": "New pipeline created", "tone": "good"},
        "all": {"text": "+14.5% uplift (Sig)", "sub": "New pipeline created", "tone": "good"},
    },
    {
        "smb": {"text": "+9% uplift (Sig)", "sub": "# of Opportunities", "tone": "good"},
        "all": {"text": "+6% uplift (Sig)", "sub": "# of Opportunities", "tone": "good"},
    },
    {
        "smb": {"text": "+9% uplift (Sig)", "sub": "$ of Upsell/Cross-sell", "tone": "good"},
        "all": {"text": "+5% uplift (Sig)", "sub": "$ of Upsell/Cross-sell", "tone": "good"},
    },
    {
        "smb": {"text": "—", "sub": "", "tone": "empty"},
        "all": {"text": "—", "sub": "", "tone": "empty"},
    },
    {
        "smb": {"text": "49% Progression (Sig)", "sub": "Stg 2-4 movement Share of opty's", "tone": "good"},
        "all": {"text": "41% Progression (Sig)", "sub": "Stg 2-4 movement Share of opty's", "tone": "good"},
    },
    {
        "smb": {"text": "25% Progression (No Sig)", "sub": "Stg 2-4 Days between stages", "tone": "good"},
        "all": {"text": "20% Progression (No Sig)", "sub": "Stg 2-4 Days between stages", "tone": "good"},
    },
    {
        "smb": {"text": "2.9% uplift (Not sig)", "sub": "Stg 4+ momentum", "tone": "neutral"},
        "all": {"text": "1.5% uplift (Not sig)", "sub": "Stg 4+ momentum", "tone": "neutral"},
    },
    {
        "smb": {"text": "—", "sub": "", "tone": "empty"},
        "all": {"text": "—", "sub": "", "tone": "empty"},
    },
    {
        "smb": {"text": "+46% uplift (Sig)", "sub": "Closed Won results $1.4K ($1.1K)", "tone": "good"},
        "all": {"text": "+34% uplift (Sig)", "sub": "Closed Won results $1.2K ($1.0K)", "tone": "good"},
    },
    {
        "smb": {"text": "+11% uplift (No sig)", "sub": "Larger Deal sizes", "tone": "good"},
        "all": {"text": "+8% uplift (No sig)", "sub": "Larger Deal sizes", "tone": "good"},
    },
    {
        "smb": {"text": "+8% uplift (No sig)", "sub": "Higher Deal cycles", "tone": "bad"},
        "all": {"text": "+6% uplift (No sig)", "sub": "Higher Deal cycles", "tone": "bad"},
    },
    {
        "smb": {"text": "-13% uplift", "sub": "Lower conversion", "tone": "bad"},
        "all": {"text": "-9% uplift", "sub": "Lower conversion", "tone": "bad"},
    },
]

SQL_TOTAL_POVS = """
SELECT COUNT(DISTINCT acct_id)
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_POV_USAGE_AGG_VW
"""

SQL_TOTAL_POVS_GROWTH = """
SELECT
(
    COUNT(DISTINCT CASE
        WHEN DATE_TRUNC('QUARTER', DATEADD(MONTH,11, requested_date)) =
             DATE_TRUNC('QUARTER', DATEADD(MONTH,11, CURRENT_DATE))
        THEN acct_id
    END)
    -
    COUNT(DISTINCT CASE
        WHEN DATE_TRUNC('QUARTER', DATEADD(MONTH,11, requested_date)) =
             DATEADD(QUARTER,-1, DATE_TRUNC('QUARTER', DATEADD(MONTH,11, CURRENT_DATE)))
        THEN acct_id
    END)
)
* 100.0 /
NULLIF(
    COUNT(DISTINCT CASE
        WHEN DATE_TRUNC('QUARTER', DATEADD(MONTH,11, requested_date)) =
             DATEADD(QUARTER,-1, DATE_TRUNC('QUARTER', DATEADD(MONTH,11, CURRENT_DATE)))
        THEN acct_id
    END)
,0) AS growth_percentage
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_POV_USAGE_AGG_VW
"""

SQL_ACTIVE_AES = """
SELECT
    COUNT(DISTINCT CASE
        WHEN usage_flg = 'Y' THEN emp_id
    END) AS active_aes
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
"""

SQL_ACTIVE_AES_GROWTH = """
WITH quarter_counts AS (
    SELECT
        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('QUARTER', DATEADD(MONTH,11,requested_date)) =
                 DATE_TRUNC('QUARTER', DATEADD(MONTH,11,CURRENT_DATE))
            THEN emp_id
        END) AS curr_qtr_count,

        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('QUARTER', DATEADD(MONTH,11,requested_date)) =
                 DATEADD(QUARTER,-1, DATE_TRUNC('QUARTER', DATEADD(MONTH,11,CURRENT_DATE)))
            THEN emp_id
        END) AS prev_qtr_count
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
)
SELECT
    ROUND(
        100.0 * (curr_qtr_count - prev_qtr_count) / NULLIF(prev_qtr_count,0),
        2
    ) AS pct_change_active_aes
FROM quarter_counts
"""

SQL_ADOPTION_PCT = """
SELECT
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN usage_flg = 'Y' THEN emp_id END)
        / NULLIF(COUNT(DISTINCT emp_id),0),
        2
    ) AS adoption_pct
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
"""

SQL_ADOPTION_GROWTH = """
WITH global_pool AS (
    SELECT COUNT(DISTINCT EMP_ID) AS fixed_total_pool
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
),
quarterly_metrics AS (
    SELECT
        'FY' || RIGHT(CAST(YEAR(DATEADD('month', 11, REQUESTED_DATE)) AS STRING), 2) ||
        ' Q' || CAST(DATE_PART('quarter', DATEADD('month', 11, REQUESTED_DATE)) AS STRING) AS fiscal_qtr,
        COUNT(DISTINCT CASE WHEN USAGE_FLG = 'Y' THEN EMP_ID END) AS active_aes
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
    WHERE REQUESTED_DATE IS NOT NULL
    GROUP BY 1
),
adoption_calc AS (
    SELECT
        fiscal_qtr,
        (active_aes::FLOAT / g.fixed_total_pool::FLOAT) * 100 AS adoption_rate
    FROM quarterly_metrics
    CROSS JOIN global_pool g
)
SELECT
    (adoption_rate - LAG(adoption_rate) OVER (ORDER BY fiscal_qtr)) AS pt_difference
FROM adoption_calc
QUALIFY ROW_NUMBER() OVER (ORDER BY fiscal_qtr DESC) = 1
"""

SQL_SEG_ADOPTION = {
    "smb": """
WITH smb_counts AS (
    SELECT
        COUNT(DISTINCT CASE WHEN USER_FLG = 'SMB' AND USAGE_FLG = 'Y' THEN EMP_ID END) AS active_smb,
        COUNT(DISTINCT CASE WHEN USER_FLG = 'SMB' THEN EMP_ID END) AS total_smb
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
)
SELECT ROUND((active_smb::FLOAT / NULLIF(total_smb, 0)::FLOAT) * 100, 1) AS pct FROM smb_counts
""",
    "entr": """
WITH entr_counts AS (
    SELECT
        COUNT(DISTINCT CASE WHEN USER_FLG = 'ENTR' AND USAGE_FLG = 'Y' THEN EMP_ID END) AS active_entr,
        COUNT(DISTINCT CASE WHEN USER_FLG = 'ENTR' THEN EMP_ID END) AS total_entr
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
)
SELECT ROUND((active_entr::FLOAT / NULLIF(total_entr, 0)::FLOAT) * 100, 1) AS pct FROM entr_counts
""",
    "cmrcl": """
WITH cmrcl_counts AS (
    SELECT
        COUNT(DISTINCT CASE WHEN USER_FLG = 'CMRCL' AND USAGE_FLG = 'Y' THEN EMP_ID END) AS active_cmrcl,
        COUNT(DISTINCT CASE WHEN USER_FLG = 'CMRCL' THEN EMP_ID END) AS total_cmrcl
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_USERS_NON_USERS_VW
)
SELECT ROUND((active_cmrcl::FLOAT / NULLIF(total_cmrcl, 0)::FLOAT) * 100, 1) AS pct FROM cmrcl_counts
""",
}

SQL_CANVAS_PCT = """
SELECT
    (SUM(CASE WHEN NUDGE_SENT_DATE < USER_CANVAS_OPENS_MAX_TS THEN 1 ELSE 0 END) /
     NULLIF(COUNT(NUDGE_SENT_DATE), 0)) * 100 AS canvas_open_pct
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_NUDGES_CANVAS_OPENS_VW
"""

SQL_CANVAS_GROWTH = """
WITH metrics AS (
    SELECT
        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('QUARTER', DATEADD('month', 11, NUDGE_SENT_DATE)) =
                 DATE_TRUNC('QUARTER', DATEADD('month', 11, CURRENT_DATE()))
            AND NUDGE_SENT_DATE < USER_CANVAS_OPENS_MAX_TS
            THEN CONCAT(ACCT_ID, NUDGE_SENT_DATE) END) AS curr_opens,

        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('QUARTER', DATEADD('month', 11, NUDGE_SENT_DATE)) =
                 DATE_TRUNC('QUARTER', DATEADD('month', 11, CURRENT_DATE()))
            THEN CONCAT(ACCT_ID, NUDGE_SENT_DATE) END) AS curr_nudges,

        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('QUARTER', DATEADD('month', 11, NUDGE_SENT_DATE)) =
                 DATEADD('quarter', -1, DATE_TRUNC('QUARTER', DATEADD('month', 11, CURRENT_DATE())))
            AND NUDGE_SENT_DATE < USER_CANVAS_OPENS_MAX_TS
            THEN CONCAT(ACCT_ID, NUDGE_SENT_DATE) END) AS last_opens,

        COUNT(DISTINCT CASE
            WHEN DATE_TRUNC('QUARTER', DATEADD('month', 11, NUDGE_SENT_DATE)) =
                 DATEADD('quarter', -1, DATE_TRUNC('QUARTER', DATEADD('month', 11, CURRENT_DATE())))
            THEN CONCAT(ACCT_ID, NUDGE_SENT_DATE) END) AS last_nudges
    FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_NUDGES_CANVAS_OPENS_VW
),
rates AS (
    SELECT
        (curr_opens::FLOAT / NULLIF(curr_nudges, 0)::FLOAT) * 100 AS curr_rate,
        (last_opens::FLOAT / NULLIF(last_nudges, 0)::FLOAT) * 100 AS last_rate
    FROM metrics
)
SELECT
    curr_rate - last_rate AS point_difference
FROM rates
"""

SQL_USAGE_SEGMENT = """
SELECT
    USER_FLG,
    COUNT(DISTINCT ACCT_ID) AS unique_account_count
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_POV_USAGE_AGG_VW
GROUP BY 1
ORDER BY 2 DESC
"""

SQL_USAGE_ROLE = """
SELECT
    CASE
        WHEN SELLER_GROUP IN ('SE', 'Solutions Other') THEN 'SE'
        WHEN SELLER_GROUP IN ('2nd Line and Above Manager', 'BVS', 'Unmapped')
             OR SELLER_GROUP IS NULL THEN 'Other'
        WHEN SELLER_GROUP = 'BDR' THEN 'BDR'
        WHEN SELLER_GROUP = 'ECS' THEN 'ECS'
        WHEN SELLER_GROUP = 'FLM' THEN 'FLM'
        WHEN SELLER_GROUP = 'Prime AE' THEN 'Prime AE'
        WHEN SELLER_GROUP = 'SDR' THEN 'SDR'
        WHEN SELLER_GROUP = 'Specialist AE' THEN 'Specialist AE'
        ELSE 'Other'
    END AS seller_group_new,
    COUNT(DISTINCT ACCT_ID) AS unique_account_count
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_POV_USAGE_AGG_VW
GROUP BY 1
ORDER BY unique_account_count DESC
"""

SQL_USAGE_REGION = """
SELECT
    COALESCE(WORK_LOCATION_REGION, 'Unknown') AS region,
    COUNT(DISTINCT ACCT_ID) AS unique_account_count
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_POV_USAGE_AGG_VW
GROUP BY 1
ORDER BY unique_account_count DESC
"""

SQL_USAGE_TIER = """
SELECT
    COALESCE(ACCT_TIER, 'Unmapped') AS account_tier,
    COUNT(DISTINCT ACCT_ID) AS unique_account_count
FROM SSE_DM_GDSO_PRD.ACCOUNT.APOV_SUCCESS_METRICS_POV_USAGE_AGG_VW
GROUP BY 1
ORDER BY 1 ASC
"""


def _import_get_conn():
    if str(CURSOR) not in sys.path:
        sys.path.insert(0, str(CURSOR))
    from snowflake_conn import get_conn

    return get_conn


def _fmt_qtr_trend(val: float | None) -> str:
    if val is None:
        return "— vs Last Qtr"
    v = round(val)
    arrow = "▲" if v >= 0 else "▼"
    return f"{arrow} {abs(v)}% vs Last Qtr"


def _rows_to_usage_bars(rows: list[tuple[Any, ...]], label_key: int = 0, count_key: int = 1) -> list[dict[str, Any]]:
    out = []
    total = sum(int(r[count_key] or 0) for r in rows)
    max_c = max((int(r[count_key] or 0) for r in rows), default=0)
    for r in rows:
        label = str(r[label_key] or "Other")
        c = int(r[count_key] or 0)
        pct = round(100.0 * c / total, 0) if total else 0
        w = round(100.0 * c / max_c, 1) if max_c else 0
        out.append({"label": label, "count": c, "pct": int(pct), "bar_width_pct": w})
    return out


def _donut_slices(rows: list[tuple[Any, ...]]) -> list[dict[str, Any]]:
    total = sum(int(r[1] or 0) for r in rows)
    out = []
    for r in rows:
        c = int(r[1] or 0)
        p = round(100.0 * c / total, 0) if total else 0
        out.append({"label": str(r[0]), "count": c, "pct": int(p)})
    return out


def build_payload_from_snowflake() -> dict[str, Any]:
    get_conn = _import_get_conn()
    conn = get_conn()
    cs = conn.cursor()
    try:
        cs.execute("ALTER SESSION SET WEEK_START = 7")

        cs.execute(SQL_TOTAL_POVS)
        total_povs = int(cs.fetchone()[0] or 0)

        cs.execute(SQL_TOTAL_POVS_GROWTH)
        row = cs.fetchone()
        g1 = float(row[0]) if row and row[0] is not None else None

        cs.execute(SQL_ACTIVE_AES)
        active_aes = int(cs.fetchone()[0] or 0)

        cs.execute(SQL_ACTIVE_AES_GROWTH)
        row = cs.fetchone()
        g2 = float(row[0]) if row and row[0] is not None else None

        cs.execute(SQL_ADOPTION_PCT)
        adoption = float(cs.fetchone()[0] or 0)

        cs.execute(SQL_ADOPTION_GROWTH)
        row = cs.fetchone()
        g3 = float(row[0]) if row and row[0] is not None else None

        seg = {}
        for k, sql in SQL_SEG_ADOPTION.items():
            cs.execute(sql)
            seg[k] = float(cs.fetchone()[0] or 0)

        cs.execute(SQL_CANVAS_PCT)
        canvas = float(cs.fetchone()[0] or 0)

        cs.execute(SQL_CANVAS_GROWTH)
        row = cs.fetchone()
        g5 = float(row[0]) if row and row[0] is not None else None

        cs.execute(SQL_USAGE_SEGMENT)
        seg_rows = cs.fetchall()
        cs.execute(SQL_USAGE_ROLE)
        role_rows = cs.fetchall()
        cs.execute(SQL_USAGE_REGION)
        region_rows = cs.fetchall()
        cs.execute(SQL_USAGE_TIER)
        tier_rows = cs.fetchall()

        asof = datetime.now(timezone.utc).strftime("Data as of %b %Y")
        region_slices = _donut_slices(region_rows)
        tier_slices = _donut_slices(tier_rows)
        region_total = sum(s["count"] for s in region_slices)
        tier_total = sum(s["count"] for s in tier_slices)

        return {
            "schemaVersion": 1,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "kpi": {
                "totalPovs": {"value": total_povs, "trend": _fmt_qtr_trend(g1)},
                "activeAes": {"value": active_aes, "trend": _fmt_qtr_trend(g2)},
                "adoptionPct": {"value": round(adoption, 1), "trend": _fmt_qtr_trend(g3)},
                "segmentAdoption": {"smb": seg["smb"], "entr": seg["entr"], "cmrcl": seg["cmrcl"], "asOf": asof},
                "canvasOpenPct": {"value": round(canvas, 1), "trend": _fmt_qtr_trend(g5)},
            },
            "usage": {
                "bySegment": _rows_to_usage_bars(seg_rows),
                "byRole": _rows_to_usage_bars(role_rows),
            },
            "donuts": {
                "region": {"slices": region_slices, "total": region_total},
                "tier": {"slices": tier_slices, "total": tier_total},
            },
            "funnel": FUNNEL_DEFAULT,
        }
    finally:
        cs.close()
        conn.close()


def build_sample_payload() -> dict[str, Any]:
    """Snapshot aligned with the current static HTML (no DB)."""
    asof = datetime.now(timezone.utc).strftime("Data as of %b %Y")
    return {
        "schemaVersion": 1,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "kpi": {
            "totalPovs": {"value": 32427, "trend": "▲ 196% vs Last Qtr"},
            "activeAes": {"value": 8534, "trend": "▲ 93% vs Last Qtr"},
            "adoptionPct": {"value": 56.0, "trend": "▲ 19% vs Last Qtr"},
            "segmentAdoption": {"smb": 63, "entr": 48, "cmrcl": 50, "asOf": asof},
            "canvasOpenPct": {"value": 13.0, "trend": "▲ 1% vs Last Qtr"},
        },
        "usage": {
            "bySegment": [
                {"label": "SMB", "count": 15338, "pct": 47, "bar_width_pct": 100},
                {"label": "CMRCL", "count": 6584, "pct": 20, "bar_width_pct": 43},
                {"label": "Other", "count": 5510, "pct": 17, "bar_width_pct": 36},
                {"label": "ENTR", "count": 5036, "pct": 16, "bar_width_pct": 33},
                {"label": "Non Sales", "count": 1951, "pct": 6, "bar_width_pct": 13},
            ],
            "byRole": [
                {"label": "Prime AE", "count": 24231, "pct": 75, "bar_width_pct": 100},
                {"label": "Specialist AE", "count": 4785, "pct": 15, "bar_width_pct": 20},
                {"label": "Other", "count": 2969, "pct": 9, "bar_width_pct": 12},
                {"label": "FLM", "count": 1293, "pct": 4, "bar_width_pct": 5},
                {"label": "BDR", "count": 992, "pct": 3, "bar_width_pct": 4},
                {"label": "ECS", "count": 866, "pct": 3, "bar_width_pct": 4},
                {"label": "SE", "count": 520, "pct": 2, "bar_width_pct": 2},
                {"label": "SDR", "count": 37, "pct": 0, "bar_width_pct": 1},
            ],
        },
        "donuts": {
            "region": {
                "total": 32427,
                "slices": [
                    {"label": "AMER", "count": 18268, "pct": 56},
                    {"label": "UKI", "count": 8036, "pct": 25},
                    {"label": "EMEA", "count": 2157, "pct": 7},
                    {"label": "APAC", "count": 1609, "pct": 5},
                    {"label": "LATAM", "count": 531, "pct": 2},
                ],
            },
            "tier": {
                "total": 32427,
                "slices": [
                    {"label": "Tier 2", "count": 9872, "pct": 30},
                    {"label": "Tier 1", "count": 9181, "pct": 28},
                    {"label": "Tier 3", "count": 5285, "pct": 16},
                    {"label": "Other", "count": 3318, "pct": 10},
                    {"label": "Tier 4", "count": 1644, "pct": 5},
                ],
            },
        },
        "funnel": FUNNEL_DEFAULT,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", action="store_true", help="Write demo JSON without Snowflake")
    ap.add_argument("-o", "--output", type=Path, default=OUT_JSON)
    args = ap.parse_args()

    if args.sample:
        payload = build_sample_payload()
    else:
        payload = build_payload_from_snowflake()

    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {args.output} ({'sample' if args.sample else 'snowflake'})")


if __name__ == "__main__":
    main()
