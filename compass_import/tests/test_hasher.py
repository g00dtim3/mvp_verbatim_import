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


# ─── verbatim_hash — scénario B (spec §1.3, §1.5) ─────────────────────────────

class TestVerbatimHashScenarioB:
    """Scénario B : hash composite élargi (brand, date, product, content,
    country, source, rating) — cf. core/hasher.py. Ces tests figent le
    comportement qui corrige les faux doublons multi-pays / multi-sources
    identifiés en §1.1 de la spec, sans casser la détection des vrais
    doublons (lignes strictement identiques)."""

    def test_different_country_different_hash(self):
        """Même avis syndiqué FR/BE : deux id distincts (avant le fix, un seul)."""
        h_fr = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Amazon", "4")
        h_be = verbatim_hash("brand", "2024-01-15", "product", "content", "BE", "Amazon", "4")
        assert h_fr != h_be

    def test_different_source_different_hash(self):
        """Même avis collecté Amazon + site marque : deux id distincts."""
        h_amazon = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Amazon", "4")
        h_site   = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Site marque", "4")
        assert h_amazon != h_site

    def test_different_rating_different_hash(self):
        h1 = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Amazon", "4")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Amazon", "5")
        assert h1 != h2

    def test_strictly_identical_rows_same_hash(self):
        """Le vrai doublon (ligne strictement identique) doit rester détecté."""
        h1 = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Amazon", "4")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content", "FR", "Amazon", "4")
        assert h1 == h2

    def test_missing_optional_fields_default_to_empty(self):
        """Sans country/source/rating (ex. anciens appelants), le hash reste
        calculable et déterministe — les nouveaux champs défaultent à ''."""
        h1 = verbatim_hash("brand", "2024-01-15", "product", "content")
        h2 = verbatim_hash("brand", "2024-01-15", "product", "content", "", "", "")
        assert h1 == h2

    def test_frozen_reference_values(self):
        """Valeurs de référence figées — si ce test casse, c'est que le hash
        a changé : la base doit être repurgée et l'import initial rejoué
        (cf. scripts/reset_verbatims.py, spec §1.4)."""
        reference = {
            ("L'Oreal", "2024-06-15", "Hydra Pro", "Super produit", "FR", "Amazon", "4"):
                "d6ca3b48c0c78cef680a723dcd02699f709f00bc62306f7a13b71b7a0e3846ed",
            ("L'Oreal", "2024-06-15", "Hydra Pro", "Super produit", "BE", "Amazon", "4"):
                "ae7fbc9cf4cdec3a1aeec53427c9ce34be082dfb457232d506f36459e9dacad3",
            ("L'Oreal", "2024-06-15", "Hydra Pro", "", "FR", "Amazon", "5"):
                "2965599682b7ac5cf7470aa927f5f635a90391457ee28ed39c9f411adf078fc0",
        }
        for args, expected in reference.items():
            actual = verbatim_hash(*args)
            assert actual == expected, (
                f"verbatim_hash{args} = {actual!r}, attendu {expected!r} — "
                "LE HASH A CHANGÉ : la base `verbatims` doit être repurgée "
                "et l'import initial rejoué (scripts/reset_verbatims.py), "
                "sans quoi les id déjà stockés ne correspondront plus aux "
                "lignes qu'ils sont censés identifier."
            )


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
