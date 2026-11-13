"""
tests/test_pipeline.py
Integration tests for the full pipeline:
- Effective disposition logic
- Bucket computation
- Idempotency
- Orphan linking
"""
import json
import os
import sqlite3
import tempfile
import pytest
from pathlib import Path

from pipeline.db import init_db
from pipeline.ingest_leads import ingest_drop_leads
from pipeline.ingest_events import ingest_drop_events
from pipeline.report import (
    compute_bucket, get_effective_disposition, generate_reports
)


@pytest.fixture
def tmp_db(tmp_path):
    """Create a temporary database for each test."""
    db_path = str(tmp_path / "test.db")
    return db_path


@pytest.fixture
def schema_dir():
    """Point to actual schema files."""
    return Path(__file__).parent.parent / "assignment-a-data" / "schemas"


def make_lead_csv(path: Path, rows: list[dict], headers: list[str] = None):
    """Write a simple lead CSV file."""
    import csv
    path.parent.mkdir(parents=True, exist_ok=True)
    if headers is None and rows:
        headers = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def make_event_jsonl(path: Path, events: list[dict]):
    """Write a JSONL events file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


# ── Effective disposition tests ───────────────────────────────────────────────

class TestEffectiveDisposition:
    def test_no_disposition(self, tmp_db, tmp_path):
        conn = init_db(tmp_db)
        # Set up a lead and call with no disposition
        conn.execute(
            """INSERT INTO leads(workflow_id, batch_id, phone_token, phone_enc, name_enc,
               external_ref, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("motor_renewal", "batch_001", "tok1", "enc1", "enc_name", "POL001", "2026-01-01T00:00:00Z")
        )
        lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES (?, ?, ?, ?, ?)""",
            ("call_001", "motor_renewal", lead_id, "2026-10-01T10:00:00Z", "2026-10-01")
        )
        conn.commit()
        assert get_effective_disposition(conn, "call_001") is None

    def test_disposition_set(self, tmp_db, tmp_path):
        conn = init_db(tmp_db)
        conn.execute(
            """INSERT INTO leads(workflow_id, batch_id, phone_token, phone_enc, name_enc,
               external_ref, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("motor_renewal", "batch_001", "tok1", "enc1", "enc_name", "POL001", "2026-01-01T00:00:00Z")
        )
        lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES (?, ?, ?, ?, ?)""",
            ("call_001", "motor_renewal", lead_id, "2026-10-01T10:00:00Z", "2026-10-01")
        )
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("evt_001", "disposition.set", "motor_renewal", "call_001", lead_id,
             "2026-10-01T11:00:00Z", json.dumps({"disposition_code": "renewed"}))
        )
        conn.commit()
        assert get_effective_disposition(conn, "call_001") == "renewed"

    def test_override_wins_over_set(self, tmp_db):
        conn = init_db(tmp_db)
        conn.execute(
            """INSERT INTO leads(workflow_id, batch_id, phone_token, phone_enc, name_enc,
               external_ref, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("motor_renewal", "batch_001", "tok1", "enc1", "enc_name", "POL001", "2026-01-01T00:00:00Z")
        )
        lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES (?, ?, ?, ?, ?)""",
            ("call_001", "motor_renewal", lead_id, "2026-10-01T10:00:00Z", "2026-10-01")
        )
        # disposition.set first
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("evt_001", "disposition.set", "motor_renewal", "call_001", lead_id,
             "2026-10-01T11:00:00Z", json.dumps({"disposition_code": "renewed"}))
        )
        # disposition.override later
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("evt_002", "disposition.override", "motor_renewal", "call_001", lead_id,
             "2026-10-02T09:00:00Z",
             json.dumps({"disposition_code": "not_interested",
                         "overridden_by": "supervisor_1", "reason": "qa_review"}))
        )
        conn.commit()
        # Override should win
        assert get_effective_disposition(conn, "call_001") == "not_interested"

    def test_latest_override_wins(self, tmp_db):
        conn = init_db(tmp_db)
        conn.execute(
            """INSERT INTO leads(workflow_id, batch_id, phone_token, phone_enc, name_enc,
               external_ref, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("motor_renewal", "batch_001", "tok1", "enc1", "enc_name", "POL001", "2026-01-01T00:00:00Z")
        )
        lead_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES (?, ?, ?, ?, ?)""",
            ("call_001", "motor_renewal", lead_id, "2026-10-01T10:00:00Z", "2026-10-01")
        )
        # Two overrides
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("evt_001", "disposition.override", "motor_renewal", "call_001", lead_id,
             "2026-10-01T12:00:00Z", json.dumps({"disposition_code": "not_interested"}))
        )
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("evt_002", "disposition.override", "motor_renewal", "call_001", lead_id,
             "2026-10-02T12:00:00Z", json.dumps({"disposition_code": "renewed"}))
        )
        conn.commit()
        # Latest by occurred_at should win
        assert get_effective_disposition(conn, "call_001") == "renewed"


# ── Bucket logic tests ────────────────────────────────────────────────────────

class TestBucketLogic:
    def _setup_lead(self, conn, workflow_id="motor_renewal"):
        conn.execute(
            """INSERT INTO dispositions_catalogue(workflow_id, code, label, is_terminal, is_success)
               VALUES (?, 'renewed', 'Renewed', 1, 1)""", (workflow_id,)
        )
        conn.execute(
            """INSERT INTO dispositions_catalogue(workflow_id, code, label, is_terminal, is_success)
               VALUES (?, 'callback_requested', 'Callback', 0, 0)""", (workflow_id,)
        )
        conn.execute(
            """INSERT INTO leads(workflow_id, batch_id, phone_token, phone_enc, name_enc,
               external_ref, created_at) VALUES (?, 'batch_001', 'tok1', 'enc1', 'enc_name',
               'REF001', '2026-01-01T00:00:00Z')""", (workflow_id,)
        )
        return conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_no_calls_yet_to_reach(self, tmp_db):
        conn = init_db(tmp_db)
        lead_id = self._setup_lead(conn)
        conn.commit()
        assert compute_bucket(conn, lead_id, "motor_renewal") == "yet_to_reach_out"

    def test_terminal_disposition_completed(self, tmp_db):
        conn = init_db(tmp_db)
        lead_id = self._setup_lead(conn)
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES ('c1', 'motor_renewal', ?, '2026-10-01T10:00:00Z', '2026-10-01')""",
            (lead_id,)
        )
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES ('e1', 'disposition.set', 'motor_renewal', 'c1', ?,
               '2026-10-01T11:00:00Z', ?)""",
            (lead_id, json.dumps({"disposition_code": "renewed"}))
        )
        conn.commit()
        assert compute_bucket(conn, lead_id, "motor_renewal") == "completed"

    def test_all_telephony_failed(self, tmp_db):
        conn = init_db(tmp_db)
        lead_id = self._setup_lead(conn)
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES ('c1', 'motor_renewal', ?, '2026-10-01T10:00:00Z', '2026-10-01')""",
            (lead_id,)
        )
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES ('e1', 'call.telephony_failed', 'motor_renewal', 'c1', ?,
               '2026-10-01T10:01:00Z', '{}')""",
            (lead_id,)
        )
        conn.commit()
        assert compute_bucket(conn, lead_id, "motor_renewal") == "in_progress_telephony_failed"

    def test_mixed_calls_in_progress_dialed(self, tmp_db):
        conn = init_db(tmp_db)
        lead_id = self._setup_lead(conn)
        # Call 1: telephony failed
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES ('c1', 'motor_renewal', ?, '2026-10-01T10:00:00Z', '2026-10-01')""",
            (lead_id,)
        )
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES ('e1', 'call.telephony_failed', 'motor_renewal', 'c1', ?,
               '2026-10-01T10:01:00Z', '{}')""",
            (lead_id,)
        )
        # Call 2: connected, non-terminal disposition
        conn.execute(
            """INSERT INTO calls(call_id, workflow_id, lead_id, initiated_at, ist_date)
               VALUES ('c2', 'motor_renewal', ?, '2026-10-02T10:00:00Z', '2026-10-02')""",
            (lead_id,)
        )
        conn.execute(
            """INSERT INTO events(event_id, event_type, workflow_id, call_id, lead_id,
               occurred_at, payload) VALUES ('e2', 'disposition.set', 'motor_renewal', 'c2', ?,
               '2026-10-02T11:00:00Z', ?)""",
            (lead_id, json.dumps({"disposition_code": "callback_requested"}))
        )
        conn.commit()
        assert compute_bucket(conn, lead_id, "motor_renewal") == "in_progress_dialed"


# ── Idempotency test ──────────────────────────────────────────────────────────

class TestIdempotency:
    def test_reingest_same_drop(self, tmp_db, tmp_path):
        """Re-ingesting the same drop must produce identical results."""
        # Create a simple drop
        drop_dir = tmp_path / "drop_test"
        lead_rows = [
            {"customer_name": "Priya Sharma", "phone": "9876543210",
             "policy_number": "POL001", "policy_expiry": "2026-10-15",
             "premium_amount": "12000", "preferred_language": "en"}
        ]
        make_lead_csv(drop_dir / "leads" / "motor_renewal" / "batch_001.csv", lead_rows)

        events = [
            {"event_id": "evt_001", "event_type": "call.initiated",
             "workflow_id": "motor_renewal", "call_id": "call_001",
             "occurred_at": "2026-10-01T10:00:00Z", "phone": "9876543210", "payload": {}}
        ]
        make_event_jsonl(drop_dir / "events" / "events_0001.jsonl", events)

        # First ingest
        ingest_drop_leads(str(drop_dir), tmp_db)
        ingest_drop_events(str(drop_dir), tmp_db)

        conn = init_db(tmp_db)
        lead_count_1 = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        event_count_1 = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        total_1 = conn.execute(
            "SELECT value FROM quality_counters WHERE key='lead_rows_total'"
        ).fetchone()[0]

        # Second ingest (should be no-op)
        ingest_drop_leads(str(drop_dir), tmp_db)
        ingest_drop_events(str(drop_dir), tmp_db)

        lead_count_2 = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        event_count_2 = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        total_2 = conn.execute(
            "SELECT value FROM quality_counters WHERE key='lead_rows_total'"
        ).fetchone()[0]

        assert lead_count_1 == lead_count_2
        assert event_count_1 == event_count_2
        assert total_1 == total_2  # counters unchanged


# ── Orphan linking test ───────────────────────────────────────────────────────

class TestOrphanLinking:
    def test_event_before_lead_resolves(self, tmp_db, tmp_path):
        """Events in drop_1 that are orphans must link when lead arrives in drop_2."""
        drop1 = tmp_path / "drop1"
        drop2 = tmp_path / "drop2"

        # Drop 1: only events, no lead yet
        events_d1 = [
            {"event_id": "evt_001", "event_type": "call.initiated",
             "workflow_id": "motor_renewal", "call_id": "call_001",
             "occurred_at": "2026-10-01T10:00:00Z", "phone": "9876543210", "payload": {}}
        ]
        make_event_jsonl(drop1 / "events" / "events_0001.jsonl", events_d1)
        ingest_drop_events(str(drop1), tmp_db)

        conn = init_db(tmp_db)
        orphan_count = conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0]
        assert orphan_count == 1  # event is orphan

        # Drop 2: lead arrives
        lead_rows = [
            {"customer_name": "Test User", "phone": "9876543210",
             "policy_number": "POL001", "policy_expiry": "2026-10-15",
             "premium_amount": "12000", "preferred_language": "en"}
        ]
        make_lead_csv(drop2 / "leads" / "motor_renewal" / "batch_001.csv", lead_rows)
        ingest_drop_leads(str(drop2), tmp_db)
        ingest_drop_events(str(drop2), tmp_db)  # triggers orphan resolution

        orphan_count_after = conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0]
        event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert orphan_count_after == 0  # resolved
        assert event_count == 1

    def test_override_links_via_call_id(self, tmp_db, tmp_path):
        """disposition.override (no phone) should link via call_id."""
        drop1 = tmp_path / "drop1"

        lead_rows = [
            {"customer_name": "Test User", "phone": "9876543210",
             "policy_number": "POL001", "policy_expiry": "2026-10-15",
             "premium_amount": "12000", "preferred_language": "en"}
        ]
        make_lead_csv(drop1 / "leads" / "motor_renewal" / "batch_001.csv", lead_rows)

        events_d1 = [
            {"event_id": "evt_001", "event_type": "call.initiated",
             "workflow_id": "motor_renewal", "call_id": "call_001",
             "occurred_at": "2026-10-01T10:00:00Z", "phone": "9876543210", "payload": {}},
            # Override has no phone
            {"event_id": "evt_002", "event_type": "disposition.override",
             "workflow_id": "motor_renewal", "call_id": "call_001",
             "occurred_at": "2026-10-01T12:00:00Z",
             "payload": {"disposition_code": "renewed",
                         "overridden_by": "supervisor_1", "reason": "qa_review"}},
        ]
        make_event_jsonl(drop1 / "events" / "events_0001.jsonl", events_d1)

        ingest_drop_leads(str(drop1), tmp_db)
        ingest_drop_events(str(drop1), tmp_db)

        conn = init_db(tmp_db)
        # Both events should be in the events table
        event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        assert event_count == 2
        orphan_count = conn.execute("SELECT COUNT(*) FROM orphan_events").fetchone()[0]
        assert orphan_count == 0

