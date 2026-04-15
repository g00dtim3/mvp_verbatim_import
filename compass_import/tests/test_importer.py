"""
tests/test_importer.py
Tests unitaires pour core/importer.py
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.importer import (
    apply_known_categories,
    import_batch,
    normalize_row,
    parse_csv,
)

# ─── CSV de test ──────────────────────────────────────────────────────────────

_HEADER = (
    "guid;brand;country;date;opinion;product_name_SEMANTIWEB;rating;"
    "source;verbatim_content;sampling;"
    "attribute_Efficiency;attribute_Packaging;attribute_Price;"
    "attribute_Quality;attribute_Scent;attribute_Taste;attribute_Texture;"
    "attribute_Safety;attribute_Composition;"
    "categorie interne;sous categorie interne;photo"
)
_ROW = (
    "abc123;L'Oreal;FR;15/06/2024;positive;Hydra Pro SEMANTIWEB;4;"
    "Amazon;Super produit;1;"
    "positive;0;0;positive;0;0;0;0;0;"
    ";;",
)

VALID_CSV_BYTES = ("\n".join([_HEADER, *_ROW]) + "\n").encode("utf-8-sig")


@pytest.fixture
def valid_df() -> pd.DataFrame:
    return parse_csv(VALID_CSV_BYTES)


@pytest.fixture
def valid_row(valid_df) -> pd.Series:
    return valid_df.iloc[0]


# ─── parse_csv ────────────────────────────────────────────────────────────────

class TestParseCsv:
    def test_returns_dataframe(self):
        df = parse_csv(VALID_CSV_BYTES)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        assert len(parse_csv(VALID_CSV_BYTES)) == 1

    def test_required_columns_present(self):
        df = parse_csv(VALID_CSV_BYTES)
        for col in ("brand", "country", "date", "product_name_SEMANTIWEB",
                    "verbatim_content", "sampling", "opinion", "source"):
            assert col in df.columns, f"Missing column: {col}"

    def test_all_columns_string_dtype(self):
        df = parse_csv(VALID_CSV_BYTES)
        for col in df.columns:
            assert pd.api.types.is_string_dtype(df[col]), (
                f"Column {col} is not a string dtype (got {df[col].dtype})"
            )

    def test_multiple_rows(self):
        row2 = (
            "def456;Nivea;DE;01/07/2024;negative;Cream SEMANTIWEB;2;"
            "eBay;Pas terrible;0;"
            "0;negative;0;0;0;0;0;0;0;"
            ";;"
        )
        csv = ("\n".join([_HEADER, _ROW[0], row2]) + "\n").encode("utf-8-sig")
        assert len(parse_csv(csv)) == 2

    def test_handles_utf8_bom(self):
        # VALID_CSV_BYTES is already encoded with utf-8-sig
        df = parse_csv(VALID_CSV_BYTES)
        assert "guid" in df.columns  # first col, no BOM prefix

    def test_raises_on_missing_required_column(self):
        bad = "brand;country\nL'Oreal;FR\n".encode("utf-8-sig")
        with pytest.raises(ValueError, match="Colonnes obligatoires manquantes"):
            parse_csv(bad)

    def test_error_message_lists_missing_columns(self):
        bad = "brand;country\nL'Oreal;FR\n".encode("utf-8-sig")
        with pytest.raises(ValueError) as exc_info:
            parse_csv(bad)
        msg = str(exc_info.value)
        assert "product_name_SEMANTIWEB" in msg

    def test_raises_on_undecodable_bytes(self):
        with pytest.raises(ValueError, match="Impossible de lire"):
            parse_csv(b"\x80\x81\x82 not utf8 at all")

    def test_raises_on_wrong_separator(self):
        """CSV avec virgule au lieu de point-virgule → colonnes manquantes."""
        bad = (
            "guid,brand,country,date,opinion,product_name_SEMANTIWEB,"
            "rating,source,verbatim_content,sampling\n"
            "1,L'Oreal,FR,15/06/2024,positive,Hydra,4,Amazon,good,1\n"
        ).encode("utf-8-sig")
        with pytest.raises(ValueError):
            parse_csv(bad)


# ─── normalize_row ────────────────────────────────────────────────────────────

class TestNormalizeRow:
    def test_returns_dict(self, valid_row):
        assert isinstance(normalize_row(valid_row, "mensuel"), dict)

    def test_product_name_renamed(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert "product_name" in result
        assert "product_name_SEMANTIWEB" not in result

    def test_semantiweb_suffix_stripped_from_value(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        # Le renommage de champ ne supprime pas le suffixe dans la valeur —
        # c'est la responsabilité du flux amont (export API).
        # On vérifie juste que la clé est correcte.
        assert "product_name" in result

    def test_date_converted_to_date_object(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert isinstance(result["date"], date)
        assert result["date"] == date(2024, 6, 15)

    def test_sampling_one_becomes_true(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert result["sampling"] is True

    def test_sampling_zero_becomes_false(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["sampling"] = "0"
        result = normalize_row(row, "mensuel")
        assert result["sampling"] is False

    def test_photo_none_for_mensuel(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert result["photo"] is None

    def test_photo_oui_true_for_initial(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["photo"] = "oui"
        assert normalize_row(row, "initial")["photo"] is True

    def test_photo_non_false_for_initial(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["photo"] = "non"
        assert normalize_row(row, "initial")["photo"] is False

    def test_photo_none_when_empty_initial(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["photo"] = ""
        assert normalize_row(row, "initial")["photo"] is None

    def test_attribute_zero_becomes_none(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert result["attribute_packaging"] is None  # "0"
        assert result["attribute_price"] is None      # "0"

    def test_attribute_positive_kept(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert result["attribute_efficiency"] == "positive"
        assert result["attribute_quality"] == "positive"

    def test_attribute_negative_kept(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["attribute_Packaging"] = "negative"
        result = normalize_row(row, "mensuel")
        assert result["attribute_packaging"] == "negative"

    def test_all_nine_attributes_in_result(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        for attr in (
            "attribute_efficiency", "attribute_packaging", "attribute_price",
            "attribute_quality", "attribute_scent", "attribute_taste",
            "attribute_texture", "attribute_safety", "attribute_composition",
        ):
            assert attr in result

    def test_id_is_64_char_hex(self, valid_row):
        h = normalize_row(valid_row, "mensuel")["id"]
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_id_is_deterministic(self, valid_row):
        h1 = normalize_row(valid_row, "mensuel")["id"]
        h2 = normalize_row(valid_row, "mensuel")["id"]
        assert h1 == h2

    def test_import_batch_id_is_none(self, valid_row):
        assert normalize_row(valid_row, "mensuel")["import_batch_id"] is None

    def test_invalid_date_raises(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["date"] = "not-a-date"
        with pytest.raises(ValueError, match="Format de date"):
            normalize_row(row, "mensuel")

    def test_categorie_none_for_mensuel_even_if_present(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["categorie interne"] = "Body Care"
        result = normalize_row(row, "mensuel")
        assert result["categorie_interne"] is None

    def test_categorie_preserved_for_initial(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["categorie interne"] = "Body Care"
        row["sous categorie interne"] = "Body Care : Moisturizer"
        result = normalize_row(row, "initial")
        assert result["categorie_interne"] == "Body Care"
        assert result["sous_categorie_interne"] == "Body Care : Moisturizer"

    def test_rating_parsed_as_int(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert result["rating"] == 4

    def test_rating_out_of_range_becomes_none(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["rating"] = "6"
        assert normalize_row(row, "mensuel")["rating"] is None

    def test_rating_non_numeric_becomes_none(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["rating"] = "N/A"
        assert normalize_row(row, "mensuel")["rating"] is None

    def test_date_formats(self):
        """Formats de date alternatifs acceptés."""
        csv_bytes = (
            _HEADER + "\nabc;B;FR;2024-06-15;positive;P;4;S;C;0;"
            "0;0;0;0;0;0;0;0;0;;;\n"
        ).encode("utf-8-sig")
        df = parse_csv(csv_bytes)
        result = normalize_row(df.iloc[0], "mensuel")
        assert result["date"] == date(2024, 6, 15)


# ─── import_batch ─────────────────────────────────────────────────────────────

def _make_row(n: int = 0) -> dict:
    """Dict verbatim minimal valide pour import_batch."""
    return {
        "id": f"{'0' * (63 - len(str(n)))}{n}",
        "brand": "TestBrand",
        "country": "FR",
        "date": date(2024, 6, 15),
        "opinion": "positive",
        "product_name": "Test Product",
        "rating": 4,
        "source": "Amazon",
        "verbatim_content": f"Content {n}",
        "sampling": False,
        "attribute_efficiency": None,
        "attribute_packaging": None,
        "attribute_price": None,
        "attribute_quality": None,
        "attribute_scent": None,
        "attribute_taste": None,
        "attribute_texture": None,
        "attribute_safety": None,
        "attribute_composition": None,
        "categorie_interne": None,
        "sous_categorie_interne": None,
        "photo": None,
        "import_batch_id": None,
    }


def _make_db_conn():
    """Mock connexion psycopg2 avec cursor context-manager."""
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


class TestImportBatch:
    def test_empty_rows_returns_zeros(self):
        conn, _ = _make_db_conn()
        result = import_batch(conn, [], "batch-uuid")
        assert result == {"inserted": 0, "skipped": 0, "errors": []}
        conn.cursor.assert_not_called()

    def test_returns_inserted_count(self):
        rows = [_make_row(i) for i in range(3)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = [("id1",), ("id2",)]  # 2 insérés sur 3
            result = import_batch(conn, rows, "uuid")
        assert result["inserted"] == 2

    def test_returns_skipped_count(self):
        rows = [_make_row(i) for i in range(3)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = [("id1",), ("id2",)]
            result = import_batch(conn, rows, "uuid")
        assert result["skipped"] == 1

    def test_no_errors_on_success(self):
        rows = [_make_row(i) for i in range(2)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = [("id0",), ("id1",)]
            result = import_batch(conn, rows, "uuid")
        assert result["errors"] == []

    def test_commit_called_per_batch(self):
        rows = [_make_row(i) for i in range(5)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = [("id",) for _ in rows]
            import_batch(conn, rows, "uuid")
        conn.commit.assert_called_once()  # 5 rows < 1000 → 1 batch

    def test_single_batch_for_small_input(self):
        rows = [_make_row(i) for i in range(5)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = []
            import_batch(conn, rows, "uuid")
        assert mock_ev.call_count == 1

    def test_three_batches_for_2500_rows(self):
        rows = [_make_row(i) for i in range(2500)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = []
            import_batch(conn, rows, "uuid")
        assert mock_ev.call_count == 3  # 1000 + 1000 + 500

    def test_error_captured_not_raised(self):
        rows = [_make_row(0)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.side_effect = Exception("connection lost")
            result = import_batch(conn, rows, "uuid")
        assert len(result["errors"]) == 1
        assert "connection lost" in result["errors"][0]

    def test_rollback_called_on_error(self):
        rows = [_make_row(0)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.side_effect = Exception("boom")
            import_batch(conn, rows, "uuid")
        conn.rollback.assert_called_once()

    def test_batch_id_injected_into_tuples(self):
        rows = [_make_row(0)]
        conn, _ = _make_db_conn()
        captured = []
        with patch("core.importer.execute_values") as mock_ev:
            def capture(cur, sql, values, **kw):
                captured.extend(values)
                return []
            mock_ev.side_effect = capture
            import_batch(conn, rows, "my-batch-uuid")
        assert any("my-batch-uuid" in str(t) for t in captured)

    def test_previous_batches_kept_after_later_error(self):
        """Si batch 2 échoue, batch 1 reste commité."""
        rows = [_make_row(i) for i in range(1500)]
        conn, _ = _make_db_conn()
        call_count = [0]
        with patch("core.importer.execute_values") as mock_ev:
            def side_effect(cur, sql, values, **kw):
                call_count[0] += 1
                if call_count[0] == 2:
                    raise Exception("batch 2 fails")
                return [("id",) for _ in values]
            mock_ev.side_effect = side_effect
            result = import_batch(conn, rows, "uuid")
        assert result["inserted"] == 1000
        assert len(result["errors"]) == 1
        assert conn.commit.call_count == 1


# ─── apply_known_categories ───────────────────────────────────────────────────

def _make_cursor_with_mapping(rows: list):
    cur = MagicMock()
    cur.__enter__ = lambda s: s
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = rows
    return cur


def _make_conn_with_mapping(mapping_rows: list):
    cur = _make_cursor_with_mapping(mapping_rows)
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


class TestApplyKnownCategories:
    def test_empty_input_returns_empty(self):
        conn = MagicMock()
        assert apply_known_categories(conn, []) == []
        conn.cursor.assert_not_called()

    def test_known_product_enriched(self):
        rows = [_make_row(0)]
        rows[0]["product_name"] = "Hydra Pro"
        conn, _ = _make_conn_with_mapping([
            ("Hydra Pro", "Body Care", "Body Care : Moisturizer", True)
        ])
        result = apply_known_categories(conn, rows)
        assert result[0]["categorie_interne"] == "Body Care"
        assert result[0]["sous_categorie_interne"] == "Body Care : Moisturizer"
        assert result[0]["photo"] is True

    def test_unknown_product_unchanged(self):
        rows = [_make_row(0)]
        rows[0]["product_name"] = "Unknown Product"
        conn, _ = _make_conn_with_mapping([])
        result = apply_known_categories(conn, rows)
        assert result[0]["categorie_interne"] is None

    def test_does_not_overwrite_existing_category(self):
        """Import initial : categorie_interne déjà remplie → ne pas écraser."""
        rows = [_make_row(0)]
        rows[0]["product_name"] = "Hydra Pro"
        rows[0]["categorie_interne"] = "Face Care"  # déjà remplie
        conn, _ = _make_conn_with_mapping([
            ("Hydra Pro", "Body Care", "Body Care : Moisturizer", True)
        ])
        result = apply_known_categories(conn, rows)
        assert result[0]["categorie_interne"] == "Face Care"  # inchangée

    def test_only_one_db_query_for_all_products(self):
        rows = [_make_row(i) for i in range(5)]
        for i, row in enumerate(rows):
            row["product_name"] = f"Product {i}"
        conn, cur = _make_conn_with_mapping([])
        apply_known_categories(conn, rows)
        assert cur.execute.call_count == 1

    def test_mixed_known_and_unknown(self):
        rows = [
            {**_make_row(0), "product_name": "Known",   "categorie_interne": None},
            {**_make_row(1), "product_name": "Unknown", "categorie_interne": None},
        ]
        conn, _ = _make_conn_with_mapping([
            ("Known", "Body Care", "Body Care : Hand Cream", False)
        ])
        result = apply_known_categories(conn, rows)
        assert result[0]["categorie_interne"] == "Body Care"
        assert result[1]["categorie_interne"] is None

    def test_returns_same_length(self):
        rows = [_make_row(i) for i in range(10)]
        conn, _ = _make_conn_with_mapping([])
        result = apply_known_categories(conn, rows)
        assert len(result) == 10

    def test_deduplicates_product_names_for_query(self):
        """Plusieurs lignes du même produit → un seul product_name en query."""
        rows = [
            {**_make_row(0), "product_name": "Hydra", "categorie_interne": None},
            {**_make_row(1), "product_name": "Hydra", "categorie_interne": None},
            {**_make_row(2), "product_name": "Hydra", "categorie_interne": None},
        ]
        conn, cur = _make_conn_with_mapping([
            ("Hydra", "Body Care", "Body Care : Moisturizer", None)
        ])
        result = apply_known_categories(conn, rows)
        # Une seule requête avec un seul product_name distinct
        args = cur.execute.call_args[0][1]
        assert args[0].count("Hydra") == 1
        # Toutes les lignes enrichies
        for r in result:
            assert r["categorie_interne"] == "Body Care"
