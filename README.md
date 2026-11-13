# Interaction Ledger & Reporting Pipeline

A high-performance, idempotent data ingestion and analytics pipeline for outbound voice campaigns. It processes messy customer lead uploads (CSV) and real-time telephony event streams (JSON Lines), secures Personally Identifiable Information (PII) at rest using cryptographic tokens and envelope encryption, and materialises daily operational reports and quality metrics against a strict output contract.

---

## Architecture Overview

```
                                      ┌──────────────────────────────────────────────────────────┐
                                      │                      Drop Directory                      │
                                      │  ┌───────────────────────┐   ┌────────────────────────┐  │
                                      │  │ Leads (CSV batches)   │   │ Call Events (JSONL)    │  │
                                      │  └───────────┬───────────┘   └───────────┬────────────┘  │
                                      └──────────────┼───────────────────────────┼───────────────┘
                                                     │                           │
                                                     ▼                           ▼
                                      ┌──────────────────────────┐   ┌──────────────────────────┐
                                      │     ingest_leads.py      │   │     ingest_events.py     │
                                      │ • Header alias matching  │   │ • Malformed & JSON check │
                                      │ • Validation & warnings  │   │ • Global deduplication   │
                                      │ • Phone canonicalisation │   │ • Disposition catalogue  │
                                      │ • PII Tokenisation (HMAC)│   │ • Lead linking & orphan  │
                                      │ • AES-256-GCM encryption │   │   quarantine             │
                                      │ • Last-write-wins merge  │   │ • Append-only ledger     │
                                      └──────────────┬───────────┘   └───────────┬──────────────┘
                                                     │                           │
                                                     ▼                           ▼
                                      ┌──────────────────────────────────────────────────────────┐
                                      │                  SQLite Database Layer                   │
                                      │      (Append-only ledger, indexed by phone HMAC)         │
                                      └─────────────────────────────┬────────────────────────────┘
                                                                    │
                                                                    ▼
                                      ┌──────────────────────────────────────────────────────────┐
                                      │                        report.py                         │
                                      │  • Effective disposition resolution (overrides > set)    │
                                      │  • Lead lifecycle bucketing                              │
                                      │  • Daily campaign connectivity & conversion rollups      │
                                      │  • Data quality accounting                               │
                                      └─────────────────────────────┬────────────────────────────┘
                                                                    │
                                      ┌─────────────────────────────┴────────────────────────────┐
                                      ▼               ▼                ▼           ▼             ▼
                               batch_summary.csv  daily_overview.csv  lead_report.csv  rejections.csv  data_quality.json
```

---

## Key Features & Engineering Design

### 1. Cryptographic PII Protection at Rest
- **Deterministic Blind Indexing for Joins**: Phone numbers are converted to canonical format (`+91XXXXXXXXXX`) and hashed via `HMAC-SHA256` with a secret salt key to create `phone_token`. All event-to-lead joins occur strictly on these tokens.
- **Envelope Encryption for Stored PII**: Customer names and raw canonical phone numbers are encrypted with `AES-256-GCM` using 12-byte random nonces prior to persistence. Plaintext PII is never stored in the database.
- **Display Masking**: When generating client reports, phone numbers are decrypted in memory and masked canonically (e.g., `+91******1819`).

### 2. Append-Only Interaction Ledger
- The events table operates as an immutable, append-only log.
- Human supervisor overrides (`disposition.override`) do not overwrite or mutate original automated dispositions (`disposition.set`); instead, overrides append as discrete events.
- An effective disposition resolution engine evaluates events chronologically, ensuring auditability and compliance.

### 3. Out-of-Order Delivery & Orphan Quarantine
- Events can arrive before the corresponding lead upload batch.
- Unlinked events are quarantined in `orphan_events`. Whenever a new lead batch is ingested, a reconciliation process re-evaluates all quarantined orphans and links them automatically.

### 4. Idempotent & Incremental Execution
- All ingested files are registered with absolute path metadata in `ingest_runs`. Re-running ingestion over existing drops incurs zero duplicate operations and does not corrupt quality counters.
- Duplicate leads within or across batches update existing records using a field-level "last write wins" merge strategy for non-null values while preserving the original `batch_id`.

---

## Directory Structure

```
.
├── assignment-a-data/
│   ├── schemas/
│   │   ├── motor_renewal.json        # Lead schema & validation rules for motor insurance
│   │   ├── policy_feedback.json      # Lead schema for feedback campaign
│   │   └── dispositions.csv          # Catalog of terminal and success disposition codes
│   └── tools/
│       └── generate_data.py          # Synthetic data generator simulating messy drops
├── pipeline/
│   ├── __init__.py
│   ├── db.py                         # SQLite schema, indexing, and connection management
│   ├── crypto.py                     # PII HMAC tokenisation and AES-256-GCM encryption
│   ├── normalise.py                  # Parser for 6 phone formats, 4 date formats, decimals, enums
│   ├── ingest_leads.py               # Lead CSV processing, validation, deduplication & merging
│   ├── ingest_events.py              # Event JSONL stream ingest, dedupe, and orphan handling
│   └── report.py                     # Report aggregations and output file generation
├── tests/
│   ├── conftest.py
│   ├── test_normalise.py             # Unit tests for data normalisation and schema validation
│   └── test_pipeline.py              # Integration tests for effective disposition, orphans & buckets
├── pipeline_cli.py                   # Unified CLI entrypoint
├── requirements.txt                  # Python package dependencies
├── SUBMISSION.md                     # Engineering trade-offs, decisions, and scaling analysis
└── README.md                         # Project documentation
```

---

## Getting Started

### 1. Prerequisites
- Python 3.10 or higher
- `pip`

Install dependencies:
```bash
pip install -r requirements.txt
```

### 2. Generate Synthetic Test Data
Generate mock drop data (seeds are deterministic):
```bash
python assignment-a-data/tools/generate_data.py --seed 42 --scale small --out data
```
This produces two simulated daily drops in `data/drop_1` and `data/drop_2`.

### 3. Ingest Data
Run ingestion for each drop:
```bash
# Ingest first drop
python pipeline_cli.py ingest data/drop_1

# Ingest second drop (resolves any previous orphans whose leads arrived in drop 2)
python pipeline_cli.py ingest data/drop_2
```

### 4. Generate Reports
Export the 5 contract report files into the `reports/` folder:
```bash
python pipeline_cli.py report --out reports/
```

### 5. Run the Test Suite
Execute the comprehensive test suite (69 tests covering all edge cases):
```bash
pytest tests/ -v
```

---

## Output Contract & Report Descriptions

When `python pipeline_cli.py report --out reports/` is executed, the following 5 files are created:

1. **`batch_summary.csv`**
   - Summarises lead progression per workflow and batch.
   - Buckets leads into: `yet_to_reach_out`, `in_progress_telephony_failed`, `in_progress_dialed`, or `completed`.
   - Columns: `workflow_id,batch_id,total_leads,yet_to_reach_out,in_progress_telephony_failed,in_progress_dialed,completed`

2. **`daily_overview.csv`**
   - Daily campaign performance aggregated by IST date.
   - Computes `connectivity_rate` (`connected_calls / total_calls` to 4 decimal places) and distinct `success_leads`.
   - Columns: `workflow_id,date,total_calls,connected_calls,connectivity_rate,success_leads`

3. **`lead_report.csv`**
   - Per-lead interaction breakdown.
   - Displays masked phone (`+91******XXXX`), call counts, latest call timestamp (ISO-8601 in IST), effective current disposition, and overall success indicator.
   - Columns: `workflow_id,external_ref,batch_id,phone_masked,bucket,total_calls,connected_calls,last_call_at,current_disposition,ever_success`

4. **`rejections.csv`**
   - Detailed rejection ledger listing malformed or invalid lead rows along with exact reason codes (e.g., `invalid_phone`, `missing_required:customer_name`). Free of PII.
   - Columns: `workflow_id,batch_id,row_number,reason`

5. **`data_quality.json`**
   - Quality balance sheet accounting for total rows processed, rejected rows, duplicates merged, field warnings, malformed events, deduplicated events, and unresolved orphans.

---

## Database Configuration

The pipeline defaults to using an embedded SQLite database (`pipeline.db`) configured with Write-Ahead Logging (`WAL`) for concurrent access and sub-second ingestion.

You can override the database path or cryptographic keys via environment variables:

| Variable | Description | Default |
|---|---|---|
| `PIPELINE_DB` | Path to the SQLite database file | `pipeline.db` |
| `PIPELINE_HMAC_KEY` | 32-byte hex key for phone tokenisation | Test key |
| `PIPELINE_ENC_KEY` | 32-byte hex key for AES-256-GCM encryption | Test key |

To reset the database at any time:
```bash
python pipeline_cli.py reset --yes
```
