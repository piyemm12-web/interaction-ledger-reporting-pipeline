"""
pipeline/report.py
Generate the five report files from the database.
"""
from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import Optional

from .db import get_conn, init_db, DB_PATH
from .crypto import decrypt, mask_phone
from .ingest_events import parse_timestamp, utc_to_ist_date, utc_to_ist_iso


# ── Disposition helpers ───────────────────────────────────────────────────────

def get_effective_disposition(conn: sqlite3.Connection, call_id: str) -> Optional[str]:
    """
    Effective disposition = latest disposition.override by occurred_at.
    If no override, latest disposition.set.
    Otherwise None.
    """
    override = conn.execute(
        """SELECT payload FROM events
           WHERE call_id=? AND event_type='disposition.override'
           ORDER BY occurred_at DESC LIMIT 1""",
        (call_id,)
    ).fetchone()
    if override:
        p = json.loads(override["payload"])
        return p.get("disposition_code")

    disp_set = conn.execute(
        """SELECT payload FROM events
           WHERE call_id=? AND event_type='disposition.set'
           ORDER BY occurred_at DESC LIMIT 1""",
        (call_id,)
    ).fetchone()
    if disp_set:
        p = json.loads(disp_set["payload"])
        return p.get("disposition_code")

    return None


def get_disposition_info(conn: sqlite3.Connection, workflow_id: str, code: str) -> dict:
    row = conn.execute(
        "SELECT is_terminal, is_success FROM dispositions_catalogue WHERE workflow_id=? AND code=?",
        (workflow_id, code)
    ).fetchone()
    if row:
        return {"is_terminal": bool(row["is_terminal"]), "is_success": bool(row["is_success"])}
    return {"is_terminal": False, "is_success": False}


def compute_bucket(conn: sqlite3.Connection, lead_id: int, workflow_id: str) -> str:
    """
    Bucket logic (first matching):
    1. yet_to_reach_out: no calls
    2. completed: any call's effective disposition is terminal
    3. in_progress_telephony_failed: every call has telephony_failed
    4. in_progress_dialed: everything else
    """
    calls = conn.execute(
        "SELECT call_id FROM calls WHERE lead_id=?", (lead_id,)
    ).fetchall()

    if not calls:
        return "yet_to_reach_out"

    for call in calls:
        code = get_effective_disposition(conn, call["call_id"])
        if code:
            info = get_disposition_info(conn, workflow_id, code)
            if info["is_terminal"]:
                return "completed"

    # Check if every call has telephony_failed
    all_failed = True
    for call in calls:
        has_failed = conn.execute(
            """SELECT 1 FROM events
               WHERE call_id=? AND event_type='call.telephony_failed'""",
            (call["call_id"],)
        ).fetchone()
        if not has_failed:
            all_failed = False
            break

    if all_failed:
        return "in_progress_telephony_failed"

    return "in_progress_dialed"


# ── Report writers ────────────────────────────────────────────────────────────

def write_csv_file(path: str, fieldnames: list[str], rows: list[dict]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="\n", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def generate_reports(out_dir: str, db_path: str = DB_PATH):
    """Generate all 5 report files into out_dir."""
    conn = init_db(db_path)
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    _report_batch_summary(conn, out_dir)
    _report_daily_overview(conn, out_dir)
    _report_lead_report(conn, out_dir)
    _report_rejections(conn, out_dir)
    _report_data_quality(conn, out_dir)

    print(f"Reports written to {out_dir}/")


# ── 1. batch_summary.csv ─────────────────────────────────────────────────────

def _report_batch_summary(conn: sqlite3.Connection, out_dir: str):
    leads = conn.execute(
        "SELECT id, workflow_id, batch_id FROM leads ORDER BY workflow_id, batch_id"
    ).fetchall()

    # Aggregate by (workflow_id, batch_id)
    summary: dict[tuple, dict] = {}
    for lead in leads:
        key = (lead["workflow_id"], lead["batch_id"])
        if key not in summary:
            summary[key] = {
                "workflow_id": lead["workflow_id"],
                "batch_id": lead["batch_id"],
                "total_leads": 0,
                "yet_to_reach_out": 0,
                "in_progress_telephony_failed": 0,
                "in_progress_dialed": 0,
                "completed": 0,
            }
        summary[key]["total_leads"] += 1
        bucket = compute_bucket(conn, lead["id"], lead["workflow_id"])
        summary[key][bucket] += 1

    rows = sorted(summary.values(), key=lambda r: (r["workflow_id"], r["batch_id"]))
    write_csv_file(
        os.path.join(out_dir, "batch_summary.csv"),
        ["workflow_id", "batch_id", "total_leads", "yet_to_reach_out",
         "in_progress_telephony_failed", "in_progress_dialed", "completed"],
        rows
    )


# ── 2. daily_overview.csv ────────────────────────────────────────────────────

def _report_daily_overview(conn: sqlite3.Connection, out_dir: str):
    # Get all calls with their IST date
    calls = conn.execute(
        """SELECT c.call_id, c.workflow_id, c.lead_id, c.ist_date
           FROM calls c
           ORDER BY c.workflow_id, c.ist_date"""
    ).fetchall()

    # Build daily aggregations
    daily: dict[tuple, dict] = {}

    for call in calls:
        key = (call["workflow_id"], call["ist_date"])
        if key not in daily:
            daily[key] = {
                "workflow_id": call["workflow_id"],
                "date": call["ist_date"],
                "total_calls": 0,
                "connected_calls": 0,
                "_call_ids": set(),
                "_success_leads": set(),
            }
        daily[key]["total_calls"] += 1
        daily[key]["_call_ids"].add(call["call_id"])

        # Connected?
        connected = conn.execute(
            "SELECT 1 FROM events WHERE call_id=? AND event_type='call.connected'",
            (call["call_id"],)
        ).fetchone()
        if connected:
            daily[key]["connected_calls"] += 1

        # Success lead: effective disposition is_success on this date
        eff = get_effective_disposition(conn, call["call_id"])
        if eff:
            info = get_disposition_info(conn, call["workflow_id"], eff)
            if info["is_success"]:
                daily[key]["_success_leads"].add(call["lead_id"])

    rows = []
    for key in sorted(daily.keys()):
        d = daily[key]
        total = d["total_calls"]
        conn_calls = d["connected_calls"]
        rate = conn_calls / total if total > 0 else 0.0
        rows.append({
            "workflow_id": d["workflow_id"],
            "date": d["date"],
            "total_calls": total,
            "connected_calls": conn_calls,
            "connectivity_rate": f"{rate:.4f}",
            "success_leads": len(d["_success_leads"]),
        })

    write_csv_file(
        os.path.join(out_dir, "daily_overview.csv"),
        ["workflow_id", "date", "total_calls", "connected_calls", "connectivity_rate", "success_leads"],
        rows
    )


# ── 3. lead_report.csv ───────────────────────────────────────────────────────

def _report_lead_report(conn: sqlite3.Connection, out_dir: str):
    leads = conn.execute(
        """SELECT id, workflow_id, batch_id, phone_enc, external_ref,
                  preferred_language
           FROM leads
           ORDER BY workflow_id, external_ref"""
    ).fetchall()

    rows = []
    for lead in leads:
        lead_id = lead["id"]
        workflow_id = lead["workflow_id"]

        # Decrypt phone for masking
        try:
            canon_phone = decrypt(lead["phone_enc"])
            phone_masked = mask_phone(canon_phone)
        except Exception:
            phone_masked = "+91******????"

        # Bucket
        bucket = compute_bucket(conn, lead_id, workflow_id)

        # Calls for this lead
        lead_calls = conn.execute(
            "SELECT call_id, initiated_at FROM calls WHERE lead_id=? ORDER BY initiated_at",
            (lead_id,)
        ).fetchall()

        total_calls = len(lead_calls)
        connected_calls = 0
        last_call_at = ""
        ever_success = False
        current_disposition = ""

        if lead_calls:
            # Last call by initiated_at
            last_call = lead_calls[-1]
            try:
                dt = parse_timestamp(last_call["initiated_at"])
                last_call_at = utc_to_ist_iso(dt)
            except Exception:
                last_call_at = ""

            # Current disposition: effective disposition of most recent call that has one
            for call in reversed(lead_calls):
                eff = get_effective_disposition(conn, call["call_id"])
                if eff:
                    current_disposition = eff
                    break

            for call in lead_calls:
                # Connected?
                conn_ev = conn.execute(
                    "SELECT 1 FROM events WHERE call_id=? AND event_type='call.connected'",
                    (call["call_id"],)
                ).fetchone()
                if conn_ev:
                    connected_calls += 1

                # Ever success?
                eff = get_effective_disposition(conn, call["call_id"])
                if eff:
                    info = get_disposition_info(conn, workflow_id, eff)
                    if info["is_success"]:
                        ever_success = True

        rows.append({
            "workflow_id": workflow_id,
            "external_ref": lead["external_ref"],
            "batch_id": lead["batch_id"],
            "phone_masked": phone_masked,
            "bucket": bucket,
            "total_calls": total_calls,
            "connected_calls": connected_calls,
            "last_call_at": last_call_at,
            "current_disposition": current_disposition,
            "ever_success": str(ever_success).lower(),
        })

    write_csv_file(
        os.path.join(out_dir, "lead_report.csv"),
        ["workflow_id", "external_ref", "batch_id", "phone_masked", "bucket",
         "total_calls", "connected_calls", "last_call_at", "current_disposition", "ever_success"],
        rows
    )


# ── 4. rejections.csv ────────────────────────────────────────────────────────

def _report_rejections(conn: sqlite3.Connection, out_dir: str):
    rows_db = conn.execute(
        """SELECT workflow_id, batch_id, row_number, reason
           FROM rejections
           ORDER BY workflow_id, batch_id, row_number"""
    ).fetchall()

    rows = [
        {
            "workflow_id": r["workflow_id"],
            "batch_id": r["batch_id"],
            "row_number": r["row_number"],
            "reason": r["reason"],
        }
        for r in rows_db
    ]

    write_csv_file(
        os.path.join(out_dir, "rejections.csv"),
        ["workflow_id", "batch_id", "row_number", "reason"],
        rows
    )


# ── 5. data_quality.json ─────────────────────────────────────────────────────

def _report_data_quality(conn: sqlite3.Connection, out_dir: str):
    counters = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM quality_counters").fetchall()
    }

    # Build rejections_by_reason
    rejections_by_reason: dict[str, int] = {}
    for key, val in counters.items():
        if key.startswith("rejection:"):
            reason = key[len("rejection:"):]
            rejections_by_reason[reason] = val

    report = {
        "lead_rows_total": counters.get("lead_rows_total", 0),
        "lead_rows_rejected": counters.get("lead_rows_rejected", 0),
        "rejections_by_reason": rejections_by_reason,
        "duplicate_lead_rows_merged": counters.get("duplicate_lead_rows_merged", 0),
        "field_warnings": counters.get("field_warnings", 0),
        "events_total": counters.get("events_total", 0),
        "malformed_events": counters.get("malformed_events", 0),
        "duplicate_events_dropped": counters.get("duplicate_events_dropped", 0),
        "orphan_events": counters.get("orphan_events", 0),
        "unknown_disposition_events": counters.get("unknown_disposition_events", 0),
    }

    out_path = os.path.join(out_dir, "data_quality.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

