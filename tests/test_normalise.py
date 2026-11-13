"""
tests/test_normalise.py
Tests for field normalisation rules (phone, date, decimal, enum, header matching).
These are the rules most likely to break per the spec.
"""
import pytest
from pipeline.normalise import (
    normalise_phone, parse_date, parse_decimal, parse_enum,
    normalise_header, build_header_map, validate_row
)
from decimal import Decimal


# ── Phone normalisation ───────────────────────────────────────────────────────

class TestPhone:
    def test_bare_10_digit(self):
        assert normalise_phone("9876543210") == "+919876543210"

    def test_plus91_space(self):
        assert normalise_phone("+91 98765 43210") == "+919876543210"

    def test_zero_prefix(self):
        assert normalise_phone("09876543210") == "+919876543210"

    def test_91_hyphen(self):
        assert normalise_phone("91-98765-43210") == "+919876543210"

    def test_plus91_no_space(self):
        assert normalise_phone("+919876543210") == "+919876543210"

    def test_parens_plus91(self):
        assert normalise_phone("(+91) 9876543210") == "+919876543210"

    def test_91_prefix_12_digits(self):
        assert normalise_phone("919876543210") == "+919876543210"

    def test_starts_with_6(self):
        assert normalise_phone("6123456789") == "+916123456789"

    def test_starts_with_7(self):
        assert normalise_phone("7123456789") == "+917123456789"

    def test_starts_with_8(self):
        assert normalise_phone("8123456789") == "+918123456789"

    # Invalid phones
    def test_9_digits_invalid(self):
        assert normalise_phone("987654321") is None  # 9 digits

    def test_bad_prefix_5(self):
        assert normalise_phone("5123456789") is None  # starts with 5

    def test_landline(self):
        assert normalise_phone("022-2345-6789") is None

    def test_na(self):
        assert normalise_phone("N/A") is None

    def test_us_number(self):
        assert normalise_phone("+1 415 555 0134") is None

    def test_empty(self):
        assert normalise_phone("") is None

    def test_whitespace(self):
        assert normalise_phone("   ") is None


# ── Date parsing ──────────────────────────────────────────────────────────────

class TestDate:
    def test_iso_format(self):
        assert parse_date("2026-10-15") == "2026-10-15"

    def test_dd_mm_yyyy_slash(self):
        assert parse_date("15/10/2026") == "2026-10-15"

    def test_dd_mm_yyyy_hyphen(self):
        assert parse_date("15-10-2026") == "2026-10-15"

    def test_dd_mon_yyyy(self):
        assert parse_date("15-Oct-2026") == "2026-10-15"

    def test_dd_mon_yyyy_lower(self):
        assert parse_date("01-dec-2026") == "2026-12-01"

    def test_dd_mon_yyyy_upper(self):
        assert parse_date("01-DEC-2026") == "2026-12-01"

    def test_impossible_date_feb31(self):
        assert parse_date("31/02/2026") is None

    def test_impossible_month_13(self):
        assert parse_date("2026-13-01") is None

    def test_text_invalid(self):
        assert parse_date("soon") is None

    def test_tbd_invalid(self):
        assert parse_date("TBD") is None

    def test_empty(self):
        assert parse_date("") is None


# ── Decimal parsing ───────────────────────────────────────────────────────────

class TestDecimal:
    def test_plain_float(self):
        assert parse_decimal("12345.67") == Decimal("12345.67")

    def test_plain_int(self):
        assert parse_decimal("12345") == Decimal("12345.00")

    def test_rupee_symbol(self):
        assert parse_decimal("₹12,345") == Decimal("12345.00")

    def test_rs_dot(self):
        assert parse_decimal("Rs. 12345") == Decimal("12345.00")

    def test_rs_no_dot(self):
        assert parse_decimal("Rs 12345") == Decimal("12345.00")

    def test_inr(self):
        assert parse_decimal("INR 12345.5") == Decimal("12345.50")

    def test_case_insensitive(self):
        assert parse_decimal("inr 500") == Decimal("500.00")

    def test_negative_invalid(self):
        assert parse_decimal("-100") is None

    def test_text_invalid(self):
        assert parse_decimal("N/A") is None

    def test_text_invalid2(self):
        assert parse_decimal("twelve thousand") is None

    def test_dash_invalid(self):
        assert parse_decimal("-") is None

    def test_empty(self):
        assert parse_decimal("") is None

    def test_round_2dp(self):
        assert parse_decimal("1234.567") == Decimal("1234.57")

    def test_comma_thousands(self):
        assert parse_decimal("1,23,456.78") == Decimal("123456.78")


# ── Header normalisation ──────────────────────────────────────────────────────

class TestHeaderNorm:
    def test_lowercase_trim(self):
        assert normalise_header("  Customer_Name  ") == "customer name"

    def test_underscore_to_space(self):
        assert normalise_header("policy_number") == "policy number"

    def test_mixed_whitespace(self):
        assert normalise_header("Mobile  No") == "mobile no"

    def test_alias_match(self):
        schema_fields = [
            {"name": "phone", "type": "phone", "aliases": ["mobile", "mobile no", "contact"]},
            {"name": "customer_name", "type": "string", "aliases": ["name", "insured name"]},
        ]
        mapping = build_header_map(["Mobile No", "Insured Name", "extra_col"], schema_fields)
        assert mapping["Mobile No"] == "phone"
        assert mapping["Insured Name"] == "customer_name"
        assert "extra_col" not in mapping

    def test_exact_name_match(self):
        schema_fields = [{"name": "policy_number", "type": "string", "aliases": ["policy no"]}]
        mapping = build_header_map(["policy_number"], schema_fields)
        assert mapping["policy_number"] == "policy_number"


# ── Row validation ────────────────────────────────────────────────────────────

MOTOR_SCHEMA = [
    {"name": "customer_name", "type": "string", "required": True, "pii": True,
     "aliases": ["name", "insured name"]},
    {"name": "phone", "type": "phone", "required": True, "pii": True,
     "aliases": ["mobile", "mobile no", "contact"]},
    {"name": "policy_number", "type": "string", "required": True, "reference": True,
     "aliases": ["policy no", "policy"]},
    {"name": "policy_expiry", "type": "date", "required": False,
     "aliases": ["expiry date", "expiry"]},
    {"name": "premium_amount", "type": "decimal", "required": False,
     "aliases": ["premium"]},
    {"name": "preferred_language", "type": "enum", "required": False,
     "values": ["en", "hi", "ta", "kn", "te", "mr"], "default": "en",
     "aliases": ["language"]},
]


class TestRowValidation:
    def _make_row(self, **kwargs):
        defaults = {
            "customer_name": "Test User",
            "phone": "9876543210",
            "policy_number": "MOT/2026/100001",
            "policy_expiry": "2026-10-15",
            "premium_amount": "12000",
            "preferred_language": "en",
        }
        defaults.update(kwargs)
        return defaults

    def _header_map(self):
        return build_header_map(
            ["customer_name", "phone", "policy_number", "policy_expiry", "premium_amount", "preferred_language"],
            MOTOR_SCHEMA
        )

    def test_valid_row(self):
        norm, reason, warnings = validate_row(self._make_row(), self._header_map(), MOTOR_SCHEMA)
        assert reason is None
        assert norm["phone"] == "+919876543210"
        assert norm["policy_expiry"] == "2026-10-15"
        assert norm["premium_amount"] == 12000.0

    def test_missing_required_name(self):
        _, reason, _ = validate_row(self._make_row(customer_name=""), self._header_map(), MOTOR_SCHEMA)
        assert reason == "missing_required:customer_name"

    def test_missing_required_phone(self):
        _, reason, _ = validate_row(self._make_row(phone=""), self._header_map(), MOTOR_SCHEMA)
        assert reason == "missing_required:phone"

    def test_invalid_phone(self):
        _, reason, _ = validate_row(self._make_row(phone="N/A"), self._header_map(), MOTOR_SCHEMA)
        assert reason == "invalid_phone"

    def test_missing_required_policy(self):
        _, reason, _ = validate_row(self._make_row(policy_number=""), self._header_map(), MOTOR_SCHEMA)
        assert reason == "missing_required:policy_number"

    def test_rejection_order_name_before_phone(self):
        # Walk schema order: customer_name is first, so missing name wins
        _, reason, _ = validate_row(
            self._make_row(customer_name="", phone="N/A"),
            self._header_map(), MOTOR_SCHEMA
        )
        assert reason == "missing_required:customer_name"

    def test_invalid_optional_date_becomes_warning(self):
        norm, reason, warnings = validate_row(
            self._make_row(policy_expiry="31/02/2026"), self._header_map(), MOTOR_SCHEMA
        )
        assert reason is None  # not rejected
        assert norm["policy_expiry"] is None  # stored null
        assert any(w[0] == "policy_expiry" for w in warnings)

    def test_invalid_optional_decimal_becomes_warning(self):
        norm, reason, warnings = validate_row(
            self._make_row(premium_amount="N/A"), self._header_map(), MOTOR_SCHEMA
        )
        assert reason is None
        assert norm["premium_amount"] is None
        assert any(w[0] == "premium_amount" for w in warnings)

    def test_invalid_enum_becomes_warning_with_default(self):
        norm, reason, warnings = validate_row(
            self._make_row(preferred_language="Hindi"), self._header_map(), MOTOR_SCHEMA
        )
        assert reason is None
        assert norm["preferred_language"] == "en"  # default applied
        assert any(w[0] == "preferred_language" for w in warnings)

    def test_enum_case_insensitive(self):
        norm, reason, _ = validate_row(
            self._make_row(preferred_language="HI"), self._header_map(), MOTOR_SCHEMA
        )
        assert reason is None
        assert norm["preferred_language"] == "hi"

    def test_whitespace_trimmed(self):
        norm, reason, _ = validate_row(
            self._make_row(customer_name="  Priya Sharma  ", phone="  9876543210  "),
            self._header_map(), MOTOR_SCHEMA
        )
        assert reason is None
        assert norm["phone"] == "+919876543210"

