#!/usr/bin/env python3
"""
pipeline_cli.py – Main command-line interface for the Agentic Universe Interaction Ledger pipeline.

Usage:
    python pipeline_cli.py ingest <drop_path>
    python pipeline_cli.py report --out <reports_dir>
    python pipeline_cli.py reset          # Drop and recreate the database

Environment variables:
    PIPELINE_DB        Path to the SQLite database file (default: pipeline.db)
    PIPELINE_HMAC_KEY  32-byte hex key for phone HMAC tokens
    PIPELINE_ENC_KEY   32-byte hex key for AES-GCM encryption
"""
from __future__ import annotations

import sys
import os
import time
from pathlib import Path

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent))

import click

from pipeline.db import DB_PATH, init_db
from pipeline.ingest_leads import ingest_drop_leads
from pipeline.ingest_events import ingest_drop_events
from pipeline.report import generate_reports


@click.group()
@click.option("--db", default=None, help="SQLite database path (overrides PIPELINE_DB env var)")
@click.pass_context
def cli(ctx, db):
    """Agentic Universe Interaction Ledger Pipeline."""
    ctx.ensure_object(dict)
    if db:
        os.environ["PIPELINE_DB"] = db
        import pipeline.db as _db
        _db.DB_PATH = db
    ctx.obj["db"] = os.environ.get("PIPELINE_DB", DB_PATH)


@cli.command()
@click.argument("drop_path", type=click.Path(exists=True))
@click.pass_context
def ingest(ctx, drop_path):
    """
    Ingest a data drop (leads + events).

    DROP_PATH: path to the drop directory (e.g. data/drop_1)

    Processes lead CSVs first, then event JSONLs.
    Idempotent: re-running on the same drop is safe.
    """
    db_path = ctx.obj["db"]
    drop_path = str(Path(drop_path).resolve())

    click.echo(f"[ingest] Starting ingestion of: {drop_path}")
    click.echo(f"[ingest] Database: {db_path}")

    t0 = time.time()

    click.echo("[ingest] Processing lead files...")
    ingest_drop_leads(drop_path, db_path)

    click.echo("[ingest] Processing event files...")
    ingest_drop_events(drop_path, db_path)

    elapsed = time.time() - t0
    click.echo(f"[ingest] Done in {elapsed:.2f}s")


@cli.command()
@click.option("--out", default="reports", show_default=True, help="Output directory for report files")
@click.pass_context
def report(ctx, out):
    """
    Generate the five report files.

    Writes to: batch_summary.csv, daily_overview.csv, lead_report.csv,
               rejections.csv, data_quality.json
    """
    db_path = ctx.obj["db"]
    click.echo(f"[report] Generating reports into: {out}/")
    click.echo(f"[report] Database: {db_path}")

    t0 = time.time()
    generate_reports(out, db_path)
    elapsed = time.time() - t0
    click.echo(f"[report] Done in {elapsed:.2f}s")


@cli.command()
@click.pass_context
@click.confirmation_option(prompt="This will DELETE all data. Are you sure?")
def reset(ctx):
    """Drop and recreate the database (destructive!)."""
    db_path = ctx.obj["db"]
    if Path(db_path).exists():
        Path(db_path).unlink()
        click.echo(f"[reset] Deleted {db_path}")
    init_db(db_path)
    click.echo(f"[reset] Database re-initialised at {db_path}")


if __name__ == "__main__":
    cli()

