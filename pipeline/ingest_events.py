"""
pipeline/ingest_events.py
Event ingestion: read JSONL files, dedupe, link to leads, quarantine orphans.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .db import get_conn, init_db, DB_PATH
from .crypto import phone_token
from .normalise import normalise_phone

IST = timezone(timedelta(hours=5, minutes=30))


def parse_timestamp(ts: str) -> datetime:
    """Parse any event timestamp to UTC-aware datetime."""
    ts = ts.strip()
    if ts.endswith("Z"):
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if "T" in ts:
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IST)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    # Naive datetime => IST
    try:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST).astimezone(timezone.utc)
    except ValueError:
        raise ValueError(f"Cannot parse timestamp: {ts!r}")


def utc_to_ist_date(dt_utc: datetime) -> str:
    ist_dt = dt_utc.astimezone(IST)
    return ist_dt.strftime("%Y-%m-%d")


def utc_to_ist_iso(dt_utc: datetime) -> str:
    ist_dt = dt_utc.astimezone(IST)
    return ist_dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")


def ingest_drop_events(drop_path: str, db_path: str = DB_PATH):
    """
    Ingest all JSONL event files under <drop_path>/events/*.jsonl
    Files are processed in filename order.
    Idempotent: already-ingested files are skipped.
    """
    conn = init_db(db_path)

    drop_p = Path(drop_path)
    events_dir = drop_p / "events"
    if not events_dir.exists():
        return

    now_utc = datetime.now(timezone.utc).isoformat()

    for jsonl_file in sorted(events_dir.glob("*.jsonl")):
        file_path = str(jsonl_file.resolve())

        # Idempotency: skip if already ingested
        existing = conn.execute(
            "SELECT id FROM ingest_runs WHERE file_path=?", (file_path,)
        ).fetchone()
        if existing:
            continue

        _ingest_jsonl_file(conn, jsonl_file, drop_path, now_utc)

        conn.execute(
            """INSERT OR IGNORE INTO ingest_runs(drop_path, file_path, file_type, ingested_at)
               VALUES (?, ?, 'events', ?)""",
            (drop_path, file_path, now_utc)
        )
        conn.commit()

    # After all events in this drop, re-resolve orphans
    _resolve_orphans_events(conn)
    conn.commit()


def _ingest_jsonl_file(conn: sqlite3.Connection, jsonl_file: Path, drop_path: str, now_utc: str):
    """Process one JSONL file line by line."""
    with open(jsonl_file, encoding="utf-8") as f:
        for raw_line in f:
            raw_line = raw_line.rstrip("\n")
            if not raw_line.strip():
                continue

            conn.execute(
                "UPDATE quality_counters SET value=value+1 WHERE key='events_total'"
            )

            # Step 1: Malformed check
            try:
                event = json.loads(raw_line)
                if not isinstance(event, dict) or "event_id" not in event:
                    raise ValueError("no event_id")
            except (json.JSONDecodeError, ValueError):
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key='malformed_events'"
                )
                continue

            event_id = event.get("event_id", "")

            # Step 2: Duplicate check
            already = conn.execute(
                "SELECT 1 FROM events WHERE event_id=? UNION SELECT 1 FROM orphan_events WHERE event_id=?",
                (event_id, event_id)
            ).fetchone()
            dup_seen = conn.execute(
                "SELECT 1 FROM duplicate_event_ids WHERE event_id=?", (event_id,)
            ).fetchone()
            if already or dup_seen:
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key='duplicate_events_dropped'"
                )
                conn.execute(
                    "INSERT OR IGNORE INTO duplicate_event_ids(event_id) VALUES (?)", (event_id,)
                )
                continue

            event_type = event.get("event_type", "")
            workflow_id = event.get("workflow_id", "")
            call_id = event.get("call_id", "")
            occurred_at_raw = event.get("occurred_at", "")
            phone_raw = event.get("phone", "")
            payload = event.get("payload", {})
            payload_json = json.dumps(payload)

            # Parse timestamp
            try:
                occurred_dt = parse_timestamp(occurred_at_raw)
                occurred_at_utc = occurred_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, TypeError):
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key='malformed_events'"
                )
                continue

            # Step 3: Unknown disposition check
            if event_type.startswith("disposition."):
                disp_code = payload.get("disposition_code", "")
                if disp_code:
                    known = conn.execute(
                        "SELECT 1 FROM dispositions_catalogue WHERE workflow_id=? AND code=?",
                        (workflow_id, disp_code)
                    ).fetchone()
                    if not known:
                        conn.execute(
                            "UPDATE quality_counters SET value=value+1 WHERE key='unknown_disposition_events'"
                        )
                        continue

            # Step 4: Link to lead
            lead_id = None
            phone_tok = None

            if event_type == "disposition.override":
                # Link via call_id only
                call_row = conn.execute(
                    "SELECT lead_id FROM calls WHERE call_id=?", (call_id,)
                ).fetchone()
                if call_row:
                    lead_id = call_row["lead_id"]
            else:
                # Normalise phone and find lead
                canon = normalise_phone(phone_raw)
                if canon:
                    phone_tok = phone_token(canon)
                    lead_row = conn.execute(
                        "SELECT id FROM leads WHERE workflow_id=? AND phone_token=?",
                        (workflow_id, phone_tok)
                    ).fetchone()
                    if lead_row:
                        lead_id = lead_row["id"]

            if lead_id is None:
                # Orphan
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key='orphan_events'"
                )
                conn.execute(
                    """INSERT OR IGNORE INTO orphan_events(
                           event_id, event_type, workflow_id, call_id,
                           phone_token, occurred_at, payload, drop_path
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (event_id, event_type, workflow_id, call_id,
                     phone_tok, occurred_at_utc, payload_json, drop_path)
                )
                continue

            # Insert into events ledger
            conn.execute(
                """INSERT OR IGNORE INTO events(event_id, event_type, workflow_id, call_id,
                   lead_id, occurred_at, payload)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (event_id, event_type, workflow_id, call_id,
                 lead_id, occurred_at_utc, payload_json)
            )

            # Create call record for call.initiated
            if event_type == "call.initiated":
                ist_date = utc_to_ist_date(occurred_dt)
                conn.execute(
                    """INSERT OR IGNORE INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
                       VALUES (?, ?, ?, ?, ?)""",
                    (call_id, workflow_id, lead_id, occurred_at_utc, ist_date)
                )

    conn.commit()


def _resolve_orphans_events(conn: sqlite3.Connection):
    """
    Re-check all orphan events to see if leads have since arrived.
    Called after both lead and event ingestion.
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    # Phone-linked orphans
    orphans = conn.execute(
        "SELECT * FROM orphan_events WHERE phone_token IS NOT NULL"
    ).fetchall()

    resolved_ids = []
    for o in orphans:
        lead = conn.execute(
            "SELECT id FROM leads WHERE workflow_id=? AND phone_token=?",
            (o["workflow_id"], o["phone_token"])
        ).fetchone()
        if lead is None:
            continue
        lead_id = lead["id"]

        conn.execute(
            """INSERT OR IGNORE INTO events(event_id, event_type, workflow_id, call_id,
               lead_id, occurred_at, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (o["event_id"], o["event_type"], o["workflow_id"], o["call_id"],
             lead_id, o["occurred_at"], o["payload"])
        )
        if o["event_type"] == "call.initiated":
            try:
                dt = parse_timestamp(o["occurred_at"])
                ist_date = utc_to_ist_date(dt)
            except ValueError:
                ist_date = ""
            conn.execute(
                """INSERT OR IGNORE INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
                   VALUES (?, ?, ?, ?, ?)""",
                (o["call_id"], o["workflow_id"], lead_id, o["occurred_at"], ist_date)
            )
        resolved_ids.append(o["id"])

    # Override orphans — now resolvable if call exists
    override_orphans = conn.execute(
        "SELECT * FROM orphan_events WHERE phone_token IS NULL AND event_type='disposition.override'"
    ).fetchall()

    for o in override_orphans:
        call = conn.execute(
            "SELECT lead_id FROM calls WHERE call_id=?", (o["call_id"],)
        ).fetchone()
        if call is None:
            continue
        lead_id = call["lead_id"]
        conn.execute(
            """INSERT OR IGNORE INTO events(event_id, event_type, workflow_id, call_id,
               lead_id, occurred_at, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (o["event_id"], o["event_type"], o["workflow_id"], o["call_id"],
             lead_id, o["occurred_at"], o["payload"])
        )
        resolved_ids.append(o["id"])

    if resolved_ids:
        placeholders = ",".join("?" for _ in resolved_ids)
        conn.execute(
            f"DELETE FROM orphan_events WHERE id IN ({placeholders})", resolved_ids
        )

    # Update the orphan counter to current state
    remaining = conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0]
    conn.execute(
        "UPDATE quality_counters SET value=? WHERE key='orphan_events'", (remaining,)
    )
    conn.commit()

