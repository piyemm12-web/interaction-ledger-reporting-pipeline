"""
pipeline/normalise.py
Field normalisation and validation logic per the spec.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional


# ── Header normalisation ──────────────────────────────────────────────────────

def normalise_header(h: str) -> str:
    """Lower-case, trim, replace runs of _ or whitespace with a single space."""
    h = h.strip().lower()
    h = re.sub(r"[_\s]+", " ", h)
    return h


def build_header_map(csv_headers: list[str], schema_fields: list[dict]) -> dict[str, str]:
    """
    Map CSV column indices → schema field names.
    Returns {csv_header: field_name} for matched columns.
    Columns that match nothing are silently ignored.
    """
    result = {}
    for col in csv_headers:
        norm = normalise_header(col)
        for field in schema_fields:
            field_norm = normalise_header(field["name"])
            aliases_norm = [normalise_header(a) for a in field.get("aliases", [])]
            if norm == field_norm or norm in aliases_norm:
                result[col] = field["name"]
                break
    return result


# ── Phone normalisation ───────────────────────────────────────────────────────

_VALID_PREFIXES = set("6789")


def normalise_phone(raw: str) -> Optional[str]:
    """
    Strip non-digits. Handle 91/0 prefixes.
    Returns canonical +91XXXXXXXXXX or None if invalid.
    """
    if not raw or not raw.strip():
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
    if len(digits) == 10 and digits[0] in _VALID_PREFIXES:
        return f"+91{digits}"
    return None


# ── Date parsing ──────────────────────────────────────────────────────────────

_MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_date(raw: str) -> Optional[str]:
    """
    Accept YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, DD-Mon-YYYY.
    Returns ISO date string YYYY-MM-DD or None if invalid.
    """
    raw = raw.strip()
    if not raw:
        return None

    # YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _make_date(y, mo, d)

    # DD/MM/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _make_date(y, mo, d)

    # DD-MM-YYYY
    m = re.match(r"^(\d{1,2})-(\d{1,2})-(\d{4})$", raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _make_date(y, mo, d)

    # DD-Mon-YYYY  (e.g. 01-Dec-2026)
    m = re.match(r"^(\d{1,2})-([A-Za-z]{3})-(\d{4})$", raw)
    if m:
        d = int(m.group(1))
        mo_str = m.group(2).lower()
        y = int(m.group(3))
        mo = _MONTH_MAP.get(mo_str)
        if mo is None:
            return None
        return _make_date(y, mo, d)

    return None


def _make_date(y: int, mo: int, d: int) -> Optional[str]:
    try:
        dt = date(y, mo, d)
        return dt.isoformat()
    except ValueError:
        return None


# ── Decimal parsing ───────────────────────────────────────────────────────────

_CURRENCY_PREFIX = re.compile(r"^(₹|rs\.?|inr)\s*", re.IGNORECASE)


def parse_decimal(raw: str) -> Optional[Decimal]:
    """
    Strip currency prefixes (₹, Rs, Rs., INR), remove commas, parse.
    Must be ≥ 0. Returns Decimal rounded to 2 places or None.
    """
    raw = raw.strip()
    if not raw:
        return None
    raw = _CURRENCY_PREFIX.sub("", raw).strip()
    raw = raw.replace(",", "")
    try:
        val = Decimal(raw)
        if val < 0:
            return None
        return val.quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


# ── Enum parsing ──────────────────────────────────────────────────────────────

def parse_enum(raw: str, values: list[str]) -> Optional[str]:
    """Case-insensitive match; store lower-case."""
    v = raw.strip().lower()
    if v in [x.lower() for x in values]:
        return v
    return None


# ── Row validation ────────────────────────────────────────────────────────────

def validate_row(
    row: dict[str, str],
    header_map: dict[str, str],
    schema_fields: list[dict],
) -> tuple[Optional[dict], Optional[str], list[tuple[str, str]]]:
    """
    Validate and normalise a CSV row against schema fields.

    Returns:
        (normalised_dict, rejection_reason, field_warnings)
        If rejection_reason is not None, the row is rejected.
        field_warnings is a list of (field_name, raw_value) for optional invalid fields.
    """
    # Build a {field_name: raw_value} map from the row using header_map
    mapped: dict[str, str] = {}
    for col, raw in row.items():
        fname = header_map.get(col)
        if fname:
            mapped[fname] = (raw or "").strip()

    result: dict[str, Any] = {}
    warnings: list[tuple[str, str]] = []

    for field in schema_fields:
        fname = field["name"]
        ftype = field["type"]
        required = field.get("required", False)
        raw = mapped.get(fname, "")

        # Missing check
        if not raw:
            if required:
                return None, f"missing_required:{fname}", []
            else:
                # Apply default if available
                default = field.get("default")
                result[fname] = default
                continue

        # Type parsing
        if ftype == "string":
            result[fname] = raw

        elif ftype == "phone":
            canon = normalise_phone(raw)
            if canon is None:
                return None, "invalid_phone", []
            result[fname] = canon  # canonical +91XXXXXXXXXX

        elif ftype == "date":
            parsed = parse_date(raw)
            if parsed is None:
                if required:
                    return None, f"missing_required:{fname}", []
                warnings.append((fname, raw))
                result[fname] = None
            else:
                result[fname] = parsed

        elif ftype == "decimal":
            parsed_d = parse_decimal(raw)
            if parsed_d is None:
                if required:
                    return None, f"missing_required:{fname}", []
                warnings.append((fname, raw))
                result[fname] = None
            else:
                result[fname] = float(parsed_d)

        elif ftype == "enum":
            vals = field.get("values", [])
            parsed_e = parse_enum(raw, vals)
            if parsed_e is None:
                if required:
                    return None, f"missing_required:{fname}", []
                warnings.append((fname, raw))
                # Apply default
                result[fname] = field.get("default")
            else:
                result[fname] = parsed_e

        else:
            result[fname] = raw

    return result, None, warnings

