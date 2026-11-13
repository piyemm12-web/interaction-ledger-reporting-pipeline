"""
pipeline/ingest_leads.py
Lead ingestion: read CSV files, validate, normalise, reject bad rows, merge duplicates.
"""
from __future__ import annotations

import csv
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .db import get_conn, init_db, DB_PATH
from .crypto import phone_token, encrypt, mask_phone
from .normalise import (
    build_header_map, validate_row, normalise_phone
)


def _load_schema(workflow_id: str) -> dict:
    """Load schema JSON from assignment-a-data/schemas/<workflow_id>.json"""
    schema_path = Path(__file__).parent.parent / "assignment-a-data" / "schemas" / f"{workflow_id}.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")
    with open(schema_path, encoding="utf-8") as f:
        return json.load(f)


def _load_dispositions(conn: sqlite3.Connection, schema_dir: Optional[Path] = None):
    """Load dispositions catalogue into DB if not already loaded."""
    count = conn.execute("SELECT COUNT(*) FROM dispositions_catalogue").fetchone()[0]
    if count > 0:
        return
    if schema_dir is None:
        schema_dir = Path(__file__).parent.parent / "assignment-a-data" / "schemas"
    disp_path = schema_dir / "dispositions.csv"
    if not disp_path.exists():
        return
    import csv as _csv
    with open(disp_path, encoding="utf-8") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            conn.execute(
                """INSERT OR IGNORE INTO dispositions_catalogue
                   (workflow_id, code, label, is_terminal, is_success)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    row["workflow_id"].strip(),
                    row["code"].strip(),
                    row["label"].strip(),
                    1 if row["is_terminal"].strip().lower() == "true" else 0,
                    1 if row["is_success"].strip().lower() == "true" else 0,
                )
            )
    conn.commit()


def ingest_drop_leads(drop_path: str, db_path: str = DB_PATH):
    """
    Ingest all lead CSV files under <drop_path>/leads/<workflow_id>/*.csv
    Files are processed in filename order (batch_001 before batch_002, etc.).
    Idempotent: already-ingested files are skipped.
    """
    conn = init_db(db_path)
    _load_dispositions(conn)

    drop_p = Path(drop_path)
    leads_dir = drop_p / "leads"
    if not leads_dir.exists():
        return

    now_utc = datetime.now(timezone.utc).isoformat()

    # Iterate workflows sorted, then files sorted
    for wf_dir in sorted(leads_dir.iterdir()):
        if not wf_dir.is_dir():
            continue
        workflow_id = wf_dir.name
        schema = _load_schema(workflow_id)
        schema_fields = schema["fields"]

        # Reference field (external_ref)
        ref_field = next((f["name"] for f in schema_fields if f.get("reference")), None)

        for csv_file in sorted(wf_dir.glob("*.csv")):
            file_path = str(csv_file.resolve())

            # Idempotency: skip if already ingested
            existing = conn.execute(
                "SELECT id FROM ingest_runs WHERE file_path=?", (file_path,)
            ).fetchone()
            if existing:
                continue

            batch_id = csv_file.stem  # e.g. "batch_001"
            _ingest_csv_file(
                conn, csv_file, workflow_id, batch_id, schema_fields,
                ref_field, drop_path, now_utc
            )

            # Record ingest run
            conn.execute(
                """INSERT OR IGNORE INTO ingest_runs(drop_path, file_path, file_type, ingested_at)
                   VALUES (?, ?, 'leads', ?)""",
                (drop_path, file_path, now_utc)
            )
            conn.commit()

    # After all lead files in this drop, try to link any orphan events
    _resolve_orphans(conn)
    conn.commit()


def _ingest_csv_file(
    conn, csv_file: Path, workflow_id: str, batch_id: str,
    schema_fields: list, ref_field: Optional[str],
    drop_path: str, now_utc: str
):
    """Process one CSV file."""
    with open(csv_file, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return
        header_map = build_header_map(list(reader.fieldnames), schema_fields)

        row_number = 0
        for raw_row in reader:
            # Check blank row (every cell empty or whitespace)
            if all((v or "").strip() == "" for v in raw_row.values()):
                continue

            row_number += 1
            conn.execute(
                "UPDATE quality_counters SET value=value+1 WHERE key='lead_rows_total'"
            )

            normalised, reason, warnings = validate_row(raw_row, header_map, schema_fields)

            if reason is not None:
                # Rejected row
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key='lead_rows_rejected'"
                )
                conn.execute(
                    """INSERT OR IGNORE INTO rejections(workflow_id, batch_id, file_name, row_number, reason)
                       VALUES (?, ?, ?, ?, ?)""",
                    (workflow_id, batch_id, csv_file.stem, row_number, reason)
                )
                # Also track rejection reason in rejections_by_reason (use quality_counters with prefix)
                counter_key = f"rejection:{reason}"
                conn.execute(
                    "INSERT OR IGNORE INTO quality_counters(key, value) VALUES (?, 0)",
                    (counter_key,)
                )
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key=?",
                    (counter_key,)
                )
                continue

            # Valid row — extract identity
            canon_phone: str = normalised["phone"]   # +91XXXXXXXXXX
            tok = phone_token(canon_phone)

            existing_lead = conn.execute(
                "SELECT id, batch_id FROM leads WHERE workflow_id=? AND phone_token=?",
                (workflow_id, tok)
            ).fetchone()

            if existing_lead is None:
                # New lead — insert
                phone_ciphertext = encrypt(canon_phone)
                name_plaintext = normalised.get("customer_name") or ""
                name_ciphertext = encrypt(name_plaintext)
                ext_ref = normalised.get(ref_field, "") if ref_field else ""

                conn.execute(
                    """INSERT OR IGNORE INTO leads(
                           workflow_id, batch_id, phone_token, phone_enc, name_enc,
                           external_ref, policy_expiry, order_date, premium_amount,
                           product, preferred_language, created_at
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        workflow_id, batch_id, tok, phone_ciphertext, name_ciphertext,
                        ext_ref or "",
                        normalised.get("policy_expiry"),
                        normalised.get("order_date"),
                        normalised.get("premium_amount"),
                        normalised.get("product"),
                        normalised.get("preferred_language"),
                        now_utc,
                    )
                )
            else:
                # Duplicate: merge (last-write-wins for non-null fields)
                lead_id = existing_lead["id"]
                conn.execute(
                    "UPDATE quality_counters SET value=value+1 WHERE key='duplicate_lead_rows_merged'"
                )
                # Build update: overwrite only non-null fields
                updates = {}
                if ref_field and normalised.get(ref_field):
                    updates["external_ref"] = normalised[ref_field]
                if normalised.get("policy_expiry") is not None:
                    updates["policy_expiry"] = normalised["policy_expiry"]
                if normalised.get("order_date") is not None:
                    updates["order_date"] = normalised["order_date"]
                if normalised.get("premium_amount") is not None:
                    updates["premium_amount"] = normalised["premium_amount"]
                if normalised.get("product") is not None:
                    updates["product"] = normalised["product"]
                if normalised.get("preferred_language") is not None:
                    updates["preferred_language"] = normalised["preferred_language"]
                # Update name if non-empty
                name_plaintext = normalised.get("customer_name") or ""
                if name_plaintext:
                    updates["name_enc"] = encrypt(name_plaintext)

                if updates:
                    set_clause = ", ".join(f"{k}=?" for k in updates)
                    conn.execute(
                        f"UPDATE leads SET {set_clause} WHERE id=?",
                        list(updates.values()) + [lead_id]
                    )

                # Log the merge
                conn.execute(
                    """INSERT INTO lead_merges(lead_id, batch_id, file_name, row_number, merged_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (lead_id, batch_id, csv_file.stem, row_number, now_utc)
                )

            # Field warnings (only for non-rejected rows)
            if warnings:
                # Get lead_id for newly inserted or existing
                lead_row = conn.execute(
                    "SELECT id FROM leads WHERE workflow_id=? AND phone_token=?",
                    (workflow_id, tok)
                ).fetchone()
                lead_id = lead_row["id"] if lead_row else None
                for field_name, raw_val in warnings:
                    conn.execute(
                        "UPDATE quality_counters SET value=value+1 WHERE key='field_warnings'"
                    )
                    conn.execute(
                        """INSERT INTO field_warnings(lead_id, field_name, raw_value, warned_at)
                           VALUES (?, ?, ?, ?)""",
                        (lead_id, field_name, raw_val, now_utc)
                    )

    conn.commit()


def _resolve_orphans(conn: sqlite3.Connection):
    """
    Re-check orphan events: if their (workflow_id, phone_token) now has a lead,
    move them to the events table and create call records as needed.
    Also handle disposition.override orphans via call_id linkage.
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
        # Insert into events
        conn.execute(
            """INSERT OR IGNORE INTO events(event_id, event_type, workflow_id, call_id,
               lead_id, occurred_at, payload)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (o["event_id"], o["event_type"], o["workflow_id"], o["call_id"],
             lead_id, o["occurred_at"], o["payload"])
        )
        # Create call record if call.initiated
        if o["event_type"] == "call.initiated":
            ist_date = _utc_to_ist_date(o["occurred_at"])
            conn.execute(
                """INSERT OR IGNORE INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
                   VALUES (?, ?, ?, ?, ?)""",
                (o["call_id"], o["workflow_id"], lead_id, o["occurred_at"], ist_date)
            )
        resolved_ids.append(o["id"])

    # Disposition.override orphans (linked by call_id)
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
        # Update orphan counter to reflect final state
        remaining = conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0]
        conn.execute(
            "UPDATE quality_counters SET value=? WHERE key='orphan_events'", (remaining,)
        )


def _utc_to_ist_date(ts: str) -> str:
    """Convert UTC ISO timestamp to IST date string YYYY-MM-DD."""
    from datetime import timedelta
    IST_OFFSET = timedelta(hours=5, minutes=30)
    dt = _parse_timestamp(ts)
    ist = dt + IST_OFFSET
    return ist.strftime("%Y-%m-%d")


def _parse_timestamp(ts: str) -> datetime:
    """Parse any event timestamp to UTC datetime."""
    from datetime import timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    ts = ts.strip()
    if ts.endswith("Z"):
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if "T" in ts and ("+" in ts[10:] or ts.count("-") > 2):
        try:
            return datetime.fromisoformat(ts).astimezone(timezone.utc)
        except ValueError:
            pass
    if "T" in ts:
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=IST).astimezone(timezone.utc)
    # Naive => IST
    return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=IST).astimezone(timezone.utc)

