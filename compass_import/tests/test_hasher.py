"""
tests/test_hasher.py
Tests unitaires pour core/hasher.py
"""

import hashlib
from datetime import date
from unittest.mock import MagicMock

import pytest

from core.hasher import file_hash, is_file_already_imported, verbatim_hash


# ─── verbatim_hash ────────────────────────────────────────────────────────────

class TestVerbatimHash:
    def test_returns_64_char_hexdigest(self):
        h = verbatim_hash("brand", "2024-01-15", "product", "content")
        assert len(h) == 64

    def test_only_hex_chars(self):
        h = verbatim_hash("brand", "2024-01-15", "product", "content")
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        h1 = verbatim_hash("brand", "2024-01-15", "product", "content")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content")
        assert h1 == h2

    def test_case_insensitive(self):
        h1 = verbatim_hash("Brand", "2024-01-15", "PRODUCT", "Content")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content")
        assert h1 == h2

    def test_strips_whitespace(self):
        h1 = verbatim_hash("  brand  ", " 2024-01-15 ", " product ", " content ")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content")
        assert h1 == h2

    def test_different_brand_different_hash(self):
        h1 = verbatim_hash("brand_a", "2024-01-15", "product", "content")
        h2 = verbatim_hash("brand_b", "2024-01-15", "product", "content")
        assert h1 != h2

    def test_different_date_different_hash(self):
        h1 = verbatim_hash("brand", "2024-01-15", "product", "content")
        h2 = verbatim_hash("brand", "2024-01-16", "product", "content")
        assert h1 != h2

    def test_different_product_different_hash(self):
        h1 = verbatim_hash("brand", "2024-01-15", "prod_a", "content")
        h2 = verbatim_hash("brand", "2024-01-15", "prod_b", "content")
        assert h1 != h2

    def test_different_content_different_hash(self):
        h1 = verbatim_hash("brand", "2024-01-15", "product", "content a")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content b")
        assert h1 != h2

    def test_matches_manual_sha256(self):
        raw = "brand2024-01-15productcontent"
        expected = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        assert verbatim_hash("brand", "2024-01-15", "product", "content") == expected

    def test_date_object_accepted(self):
        d = date(2024, 1, 15)
        h = verbatim_hash("brand", d, "product", "content")
        assert len(h) == 64

    def test_date_object_matches_iso_string(self):
        d = date(2024, 1, 15)
        h_obj = verbatim_hash("brand", d, "product", "content")
        h_str = verbatim_hash("brand", "2024-01-15", "product", "content")
        assert h_obj == h_str

    def test_empty_content_still_produces_hash(self):
        h = verbatim_hash("brand", "2024-01-15", "product", "")
        assert len(h) == 64


# ─── file_hash ────────────────────────────────────────────────────────────────

class TestFileHash:
    def test_returns_64_char_hexdigest(self):
        h = file_hash(b"some file content")
        assert len(h) == 64

    def test_only_hex_chars(self):
        h = file_hash(b"data")
        assert all(c in "0123456789abcdef" for c in h)

    def test_matches_hashlib_directly(self):
        data = b"brand;date;product;content\n"
        expected = hashlib.sha256(data).hexdigest()
        assert file_hash(data) == expected

    def test_different_bytes_different_hash(self):
        assert file_hash(b"file_a") != file_hash(b"file_b")

    def test_same_bytes_same_hash(self):
        data = b"identical content"
        assert file_hash(data) == file_hash(data)

    def test_empty_bytes(self):
        h = file_hash(b"")
        assert len(h) == 64
        assert h == hashlib.sha256(b"").hexdigest()

    def test_large_file(self):
        data = b"x" * 10_000_000  # 10 MB
        h = file_hash(data)
        assert len(h) == 64


# ─── is_file_already_imported ─────────────────────────────────────────────────

def _make_cursor(fetchone_result, col_names=None):
    """Crée un mock cursor psycopg2."""
    cols = col_names or ["id", "filename", "started_at", "status", "import_type"]
    cur = MagicMock()
    cur.description = [(c,) for c in cols]
    cur.fetchone.return_value = fetchone_result
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    return cur


def _make_conn(fetchone_result, col_names=None):
    """Crée un mock connexion psycopg2."""
    cur = _make_cursor(fetchone_result, col_names)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


class TestIsFileAlreadyImported:
    def test_returns_none_when_not_found(self):
        conn, _ = _make_conn(None)
        assert is_file_already_imported(conn, "abc123") is None

    def test_returns_dict_when_found(self):
        from datetime import datetime
        row = ("uuid-1", "file.csv", datetime(2024, 6, 1), "success", "mensuel")
        conn, _ = _make_conn(row)
        result = is_file_already_imported(conn, "abc123")
        assert result is not None
        assert result["filename"] == "file.csv"
        assert result["status"] == "success"
        assert result["import_type"] == "mensuel"
        assert result["id"] == "uuid-1"

    def test_keys_match_column_names(self):
        row = ("uuid-1", "file.csv", None, "success", "mensuel")
        conn, _ = _make_conn(row)
        result = is_file_already_imported(conn, "abc123")
        assert set(result.keys()) == {"id", "filename", "started_at", "status", "import_type"}

    def test_hash_passed_as_query_param(self):
        conn, cur = _make_conn(None)
        is_file_already_imported(conn, "deadbeef00")
        args = cur.execute.call_args[0][1]
        assert "deadbeef00" in args

    def test_query_excludes_error_and_duplicate_statuses(self):
        conn, cur = _make_conn(None)
        is_file_already_imported(conn, "abc")
        sql = cur.execute.call_args[0][0]
        assert "error" in sql
        assert "duplicate" in sql
        assert "NOT IN" in sql
