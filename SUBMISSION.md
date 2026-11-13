# SUBMISSION.md

## How to Run

### Prerequisites
```bash
pip install click cryptography pytz
```

### Generate Data
```bash
cd c:\path\to\project
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
```

### Ingest Data
```bash
python pipeline_cli.py ingest data/drop_1
python pipeline_cli.py ingest data/drop_2
```

### Generate Reports
```bash
python pipeline_cli.py report --out reports/
```

### Run Tests
```bash
python -m pytest tests/ -v
```

### Full Workflow (single command)
```bash
python pipeline_cli.py ingest data/drop_1 && python pipeline_cli.py ingest data/drop_2 && python pipeline_cli.py report --out reports/
```

### CLI Options
```
python pipeline_cli.py --db path/to/custom.db ingest data/drop_1   # custom DB path
python pipeline_cli.py reset                                         # wipe and reinitialise DB
```

Environment variables:
- `PIPELINE_DB`: SQLite database file path (default: `pipeline.db`)
- `PIPELINE_HMAC_KEY`: 64-hex-char key for phone HMAC tokens (default: all-zero — test only)
- `PIPELINE_ENC_KEY`: 64-hex-char key for AES-GCM encryption (default: test keys)

---

## Design Decisions & Ambiguities Resolved

### 1. PII at Rest

**Decision**: Phone numbers are stored as HMAC-SHA256 tokens for joins, and as AES-256-GCM ciphertext for retrieval. Customer names are stored only as ciphertext (never needed for joins). The plaintext canonical phone (`+91XXXXXXXXXX`) never appears in the database.

**How phone matching works**: When an event arrives with a phone number, we normalise it to canonical form, compute `HMAC-SHA256(canonical_phone, HMAC_KEY)`, and look up that token in the `leads` table. This allows exact matching without ever storing the plaintext.

**Key management**: Keys are read from environment variables (`PIPELINE_HMAC_KEY`, `PIPELINE_ENC_KEY`) or a `pipeline/keys.env` file. If neither is set, deterministic test keys are used — clearly not for production. In production, these would come from a secrets manager (AWS KMS, HashiCorp Vault, etc.).

**Trade-off**: HMAC tokens are deterministic (same phone → same token), which enables joining but means the token is a stable pseudonymifier. If HMAC key is ever rotated, all tokens would need recomputation. AES-GCM ciphertext is randomised per encrypt call (12-byte nonce), so the same plaintext produces different ciphertexts each time — this is correct.

### 2. Idempotency

**Decision**: Each ingested file is recorded in `ingest_runs` by its absolute path. On re-run, files already in this table are skipped entirely. Lead rows use `INSERT OR IGNORE` on `(workflow_id, phone_token)` and event rows use `INSERT OR IGNORE` on `event_id`. Quality counters are only incremented for genuinely new data.

**Trade-off**: This is "file-level" idempotency, not truly incremental (a partially-failed file would need manual intervention). The spec says "re-running ingest on a drop you've already ingested must change nothing" — this satisfies that exactly.

### 3. Orphan Events

**Ambiguity**: The spec says "An event that was an orphan after drop_1 must count once its lead arrives in drop_2." The `orphan_events` counter in `data_quality.json` should reflect the **final state** (remaining orphans), not the running total.

**Decision**: The `orphan_events` quality counter is updated to the current count of unresolved orphans after each ingest run (not a monotonically incrementing counter). This matches "Orphan counts in data_quality.json reflect the final state."

**Re-resolution**: After both lead and event ingestion in a drop, we run `_resolve_orphans()` which scans all orphan events and tries to link them to now-existing leads.

### 4. Duplicate Event Counting

**Ambiguity**: Does `duplicate_events_dropped` count events seen a second time across drops, or only within the same file?

**Decision**: It counts globally — any event_id seen more than once across all files and drops is a duplicate. We maintain a `duplicate_event_ids` table for fast lookup. `events_total` counts every non-empty line read including malformed and duplicates.

### 5. Blank Row Handling

**Decision**: A blank row is defined as a row where every cell is empty or whitespace. Such rows are skipped and not counted toward `lead_rows_total`. Row numbers count only non-blank data rows within a file, starting at 1.

### 6. Lead Merge Semantics

**Decision**: "Last write wins for non-null fields." If a duplicate row has a null/empty value for a field, the existing value is preserved. The `batch_id` of a lead is permanently set by the first valid row — subsequent duplicates do not change it.

**Edge case**: If a duplicate row appears in the same batch as the original (same batch_id), the merge still records a `lead_merges` entry. This is correct — the problem says "the first valid row creates the lead."

### 7. `bucket` for a Lead with Multiple Calls

**Bucket priority** (first match):
1. `yet_to_reach_out`: no calls at all
2. `completed`: **any** call's effective disposition is terminal
3. `in_progress_telephony_failed`: **every** call has a `call.telephony_failed` event
4. `in_progress_dialed`: everything else

**Ambiguity**: For `in_progress_telephony_failed`, the spec says "every call has a call.telephony_failed event." A call could have both `call.telephony_failed` AND other events (e.g., `call.initiated`). We check for the presence of a `call.telephony_failed` event on each call, not whether it's the only event.

### 8. `success_leads` in daily_overview

**Decision**: `success_leads` = distinct leads with at least one call **on that date** whose **effective** disposition `is_success=true`. A lead is counted for the date of `call.initiated`, not the date the disposition was set. This matches "calls on that date."

### 9. Timestamp Handling

**Decision**: All timestamps are normalised to UTC in the database. All report dates are computed in IST (UTC+05:30). Naive timestamps in events are treated as IST per the spec.

### 10. Unknown Disposition Events

**Decision**: A `disposition.*` event with a code not in `dispositions_catalogue` for that workflow is counted as `unknown_disposition_events` and **not** inserted into the events ledger (not even as an orphan). This matches the spec: "count it and ignore it."

### 11. Schema Field Ordering for Validation

**Decision**: Fields are validated in schema-definition order. The first failure becomes the rejection reason. This matters when multiple required fields are missing — the spec is explicit about this.

### 12. `events_total` Counter

**Decision**: Counts every non-empty line read from every JSONL file, including malformed JSON lines and duplicate events. Empty lines (pure whitespace) are not counted. This matches "every non-empty line read, including malformed and duplicate lines."

---

## Database Schema

See `pipeline/db.py` for full DDL with comments. Key design choices:

| Table | Purpose |
|---|---|
| `leads` | One row per unique (workflow_id, phone_token). PII stored encrypted. |
| `lead_merges` | Log of duplicate rows merged into existing leads. |
| `rejections` | Invalid rows with reason code. No PII. |
| `field_warnings` | Optional fields with unparseable values (stored null). |
| `calls` | One row per `call.initiated` linked to a lead. |
| `events` | Append-only ledger. All valid, deduplicated, linked events. |
| `orphan_events` | Events pending lead linkage. Drained as leads arrive. |
| `duplicate_event_ids` | Fast lookup set for deduplication. |
| `quality_counters` | Running totals for `data_quality.json`. |
| `ingest_runs` | File-level idempotency registry. |
| `dispositions_catalogue` | Reference data from `dispositions.csv`. |

---

## What I'd Do Next / In Production

### Production Architecture (100k leads/day)

1. **Scheduling**: Use Airflow or Prefect. Two DAGs: `lead_ingest` (triggered on file upload to S3) and `event_ingest` (triggered by time window close). Report generation runs at 06:00 IST.

2. **Storage**: Replace SQLite with PostgreSQL. The schema is designed to work with either. Add connection pooling (PgBouncer), read replicas for report queries.

3. **PII**: Move keys to AWS KMS or HashiCorp Vault. Consider field-level encryption with envelope encryption (data key per row, encrypted with master key).

4. **Failure handling**: Each file ingestion is atomic (transaction). Failed files are retried with exponential backoff. `ingest_runs` prevents double-processing on retry.

5. **Backfills**: The `drop_path` in `ingest_runs` allows re-running a specific historical drop. Reset by deleting the `ingest_runs` entry for that file.

6. **Truly incremental (stretch)**: Currently, re-ingesting drop_1 is a no-op because we track at file level. Truly incremental would mean tracking at row level (hash of each row), skipping only unchanged rows. Trade-off: much higher storage and lookup cost per row vs. the marginal benefit of partial-file recovery.

7. **Performance (large scale)**: For the `--scale large` dataset (~140k leads, ~600k events), the main bottleneck is the per-row HMAC computation and DB lookups. Improvements: batch HMAC computation, bulk INSERT with conflict handling (INSERT OR IGNORE in batches of 1000), indexed phone_token lookups (already indexed), and async event processing.

8. **Report SQL aggregation (stretch)**: The five reports are currently computed in Python loops. For 100k+ leads, moving bucket computation and daily aggregation to SQL window functions would be ~10x faster. Left as future work.

---

## Testing

Tests cover:
- Phone normalisation (all 6 formats + invalid cases)
- Date parsing (all 4 formats + impossible dates)
- Decimal parsing (all currency prefixes + invalid)
- Enum validation (case-insensitive + default)
- Header alias matching
- Row validation (required field order, warning vs rejection)
- Effective disposition (set, override, latest override)
- Bucket logic (all 4 buckets)
- Idempotency (re-ingest produces identical state)
- Orphan linking (event before lead, disposition.override via call_id)

Run: `python -m pytest tests/ -v`

