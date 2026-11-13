"""
pipeline/db.py
Database schema setup and connection management.
Uses SQLite. Schema designed for append-only ledger, PII protection, and idempotency.
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path

DB_PATH = os.environ.get("PIPELINE_DB", "pipeline.db")


def get_conn(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


SCHEMA_SQL = """
-- ─────────────────────────────────────────────────────────────────────────────
-- PII approach:
--   Fields marked pii:true are stored as HMAC-SHA256 tokens (hex) for
--   matching and as AES-GCM ciphertext (base64) for display/export.
--   The HMAC key and encryption key are stored in the environment or a key
--   file (pipeline/keys.env). phone is stored as its HMAC token for fast
--   lookup; customer_name is stored only as ciphertext (never needed for joins).
--   The canonical phone (+91XXXXXXXXXX) is never written plaintext to the DB.
--
-- Idempotency:
--   Every ingest run is tracked in ingest_runs. Lead rows and events have
--   unique constraints so re-ingesting a file is a no-op (INSERT OR IGNORE).
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ingest_runs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    drop_path   TEXT NOT NULL,
    file_path   TEXT NOT NULL,
    file_type   TEXT NOT NULL,   -- 'leads' or 'events'
    ingested_at TEXT NOT NULL,   -- ISO-8601 UTC
    UNIQUE(file_path)
);

-- Disposition catalogue (loaded from dispositions.csv)
CREATE TABLE IF NOT EXISTS dispositions_catalogue (
    workflow_id TEXT NOT NULL,
    code        TEXT NOT NULL,
    label       TEXT NOT NULL,
    is_terminal INTEGER NOT NULL,  -- 0/1
    is_success  INTEGER NOT NULL,  -- 0/1
    PRIMARY KEY (workflow_id, code)
);

-- Leads table
-- phone_token: HMAC-SHA256(canonical_phone, HMAC_KEY) — used for joins
-- phone_enc  : AES-GCM encrypted canonical phone — for display (phone_masked)
-- name_enc   : AES-GCM encrypted customer_name
CREATE TABLE IF NOT EXISTS leads (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id         TEXT NOT NULL,
    batch_id            TEXT NOT NULL,
    phone_token         TEXT NOT NULL,  -- HMAC; identity key
    phone_enc           TEXT NOT NULL,  -- ciphertext
    name_enc            TEXT NOT NULL,  -- ciphertext
    external_ref        TEXT NOT NULL,
    policy_expiry       TEXT,           -- ISO date string or null
    order_date          TEXT,           -- ISO date string or null
    premium_amount      REAL,
    product             TEXT,
    preferred_language  TEXT,
    created_at          TEXT NOT NULL,
    UNIQUE(workflow_id, phone_token)
);

-- Lead merge log (each duplicate that overwrites fields)
CREATE TABLE IF NOT EXISTS lead_merges (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER NOT NULL REFERENCES leads(id),
    batch_id    TEXT NOT NULL,
    file_name   TEXT NOT NULL,
    row_number  INTEGER NOT NULL,
    merged_at   TEXT NOT NULL
);

-- Rejections
CREATE TABLE IF NOT EXISTS rejections (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    file_name   TEXT NOT NULL,
    row_number  INTEGER NOT NULL,
    reason      TEXT NOT NULL
);

-- Field warnings (optional field present but invalid value → stored null)
CREATE TABLE IF NOT EXISTS field_warnings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER REFERENCES leads(id),
    field_name  TEXT NOT NULL,
    raw_value   TEXT,
    warned_at   TEXT NOT NULL
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Interaction ledger (append-only)
-- ─────────────────────────────────────────────────────────────────────────────

-- Calls: one row per call.initiated event that links to a lead
CREATE TABLE IF NOT EXISTS calls (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id         TEXT NOT NULL UNIQUE,
    workflow_id     TEXT NOT NULL,
    lead_id         INTEGER NOT NULL REFERENCES leads(id),
    initiated_at    TEXT NOT NULL,   -- ISO-8601 UTC
    ist_date        TEXT NOT NULL    -- YYYY-MM-DD IST date of initiation
);

-- Events ledger (append-only; every accepted, non-duplicate event)
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL UNIQUE,
    event_type  TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id     TEXT NOT NULL,
    lead_id     INTEGER REFERENCES leads(id),  -- null for disposition.override resolved later
    occurred_at TEXT NOT NULL,  -- ISO-8601 UTC (normalised)
    payload     TEXT NOT NULL   -- JSON text
);

-- Orphan events: linked to no lead at time of ingestion
-- Re-evaluated after each drop
CREATE TABLE IF NOT EXISTS orphan_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    TEXT NOT NULL UNIQUE,
    event_type  TEXT NOT NULL,
    workflow_id TEXT NOT NULL,
    call_id     TEXT NOT NULL,
    phone_token TEXT,           -- null for disposition.override
    occurred_at TEXT NOT NULL,
    payload     TEXT NOT NULL,
    drop_path   TEXT NOT NULL
);

-- Duplicate event tracking (just the IDs)
CREATE TABLE IF NOT EXISTS duplicate_event_ids (
    event_id TEXT NOT NULL PRIMARY KEY
);

-- Data quality counters (running totals — updated per ingest run)
CREATE TABLE IF NOT EXISTS quality_counters (
    key   TEXT NOT NULL PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_leads_phone_token ON leads(workflow_id, phone_token);
CREATE INDEX IF NOT EXISTS idx_calls_call_id ON calls(call_id);
CREATE INDEX IF NOT EXISTS idx_calls_lead_id ON calls(lead_id, ist_date);
CREATE INDEX IF NOT EXISTS idx_events_call_id ON events(call_id);
CREATE INDEX IF NOT EXISTS idx_events_lead_id ON events(lead_id, event_type);
CREATE INDEX IF NOT EXISTS idx_orphan_phone_token ON orphan_events(workflow_id, phone_token);
"""


def init_db(db_path: str = DB_PATH):
    conn = get_conn(db_path)
    conn.executescript(SCHEMA_SQL)
    # Seed quality counters
    keys = [
        "lead_rows_total", "lead_rows_rejected", "duplicate_lead_rows_merged",
        "field_warnings", "events_total", "malformed_events",
        "duplicate_events_dropped", "orphan_events", "unknown_disposition_events"
    ]
    for k in keys:
        conn.execute(
            "INSERT OR IGNORE INTO quality_counters(key, value) VALUES (?, 0)", (k,)
        )
    conn.commit()
    return conn

