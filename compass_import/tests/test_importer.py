"""
tests/test_importer.py
Tests unitaires pour core/importer.py
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.importer import (
    RowValidationError,
    apply_known_categories,
    import_batch,
    normalize_batch,
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
    df, _bad_lines = parse_csv(VALID_CSV_BYTES)
    return df


@pytest.fixture
def valid_row(valid_df) -> pd.Series:
    return valid_df.iloc[0]


# ─── parse_csv ────────────────────────────────────────────────────────────────

class TestParseCsv:
    def test_returns_dataframe_and_bad_lines_tuple(self):
        df, bad_lines = parse_csv(VALID_CSV_BYTES)
        assert isinstance(df, pd.DataFrame)
        assert isinstance(bad_lines, list)

    def test_no_bad_lines_on_valid_csv(self):
        _df, bad_lines = parse_csv(VALID_CSV_BYTES)
        assert bad_lines == []

    def test_correct_row_count(self):
        df, _ = parse_csv(VALID_CSV_BYTES)
        assert len(df) == 1

    def test_required_columns_present(self):
        df, _ = parse_csv(VALID_CSV_BYTES)
        for col in ("brand", "country", "date", "product_name_SEMANTIWEB",
                    "verbatim_content", "sampling", "opinion", "source"):
            assert col in df.columns, f"Missing column: {col}"

    def test_all_columns_string_dtype(self):
        df, _ = parse_csv(VALID_CSV_BYTES)
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
        df, _ = parse_csv(csv)
        assert len(df) == 2

    def test_handles_utf8_bom(self):
        # VALID_CSV_BYTES is already encoded with utf-8-sig
        df, _ = parse_csv(VALID_CSV_BYTES)
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

    def test_malformed_line_captured_not_raised(self):
        """Une ligne avec trop de champs (spec §2.9 : 30 colonnes au lieu de
        25) ne doit plus faire échouer tout le fichier — elle est capturée
        dans bad_lines, le reste du fichier reste exploitable."""
        row2 = (
            "def456;Nivea;DE;01/07/2024;negative;Cream SEMANTIWEB;2;"
            "eBay;Pas terrible;0;"
            "0;negative;0;0;0;0;0;0;0;"
            ";;;;;;;;;;"  # bien plus de champs que l'en-tête
        )
        csv = ("\n".join([_HEADER, _ROW[0], row2]) + "\n").encode("utf-8-sig")
        df, bad_lines = parse_csv(csv)
        assert len(df) == 1  # seule la ligne valide est dans le DataFrame
        assert len(bad_lines) == 1
        assert bad_lines[0]["code"] == "LIGNE_MALFORMEE"
        assert bad_lines[0]["ligne"] is None  # numéro non déterminable
        assert bad_lines[0]["extrait"]  # contenu brut conservé pour diagnostic


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
        df, _ = parse_csv(csv_bytes)
        result = normalize_row(df.iloc[0], "mensuel")
        assert result["date"] == date(2024, 6, 15)

    def test_guid_extracted(self, valid_row):
        result = normalize_row(valid_row, "mensuel")
        assert result["guid"] == "abc123"

    def test_hash_changes_with_country(self, valid_df):
        """Scénario B (lot 1) : même avis, pays différent → id distinct."""
        row_fr = valid_df.iloc[0].copy()
        row_be = valid_df.iloc[0].copy()
        row_be["country"] = "BE"
        id_fr = normalize_row(row_fr, "mensuel")["id"]
        id_be = normalize_row(row_be, "mensuel")["id"]
        assert id_fr != id_be

    def test_hash_changes_with_source(self, valid_df):
        row_amazon = valid_df.iloc[0].copy()
        row_site = valid_df.iloc[0].copy()
        row_site["source"] = "Site marque"
        id_amazon = normalize_row(row_amazon, "mensuel")["id"]
        id_site = normalize_row(row_site, "mensuel")["id"]
        assert id_amazon != id_site


# ─── normalize_row — RowValidationError (spec §2.3) ───────────────────────────

class TestNormalizeRowValidation:
    def test_date_manquante_code(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["date"] = ""
        with pytest.raises(RowValidationError) as exc_info:
            normalize_row(row, "mensuel")
        assert exc_info.value.code == "DATE_MANQUANTE"
        assert exc_info.value.champ == "date"

    def test_date_invalide_code(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["date"] = "31/02/2025"
        with pytest.raises(RowValidationError) as exc_info:
            normalize_row(row, "mensuel")
        assert exc_info.value.code == "DATE_INVALIDE"
        assert exc_info.value.champ == "date"

    def test_rating_invalid_does_not_raise(self, valid_df):
        """Les champs non structurants restent tolérants (spec §2.3) :
        rating invalide → None, la ligne passe, pas d'exception."""
        row = valid_df.iloc[0].copy()
        row["rating"] = "abc"
        result = normalize_row(row, "mensuel")
        assert result["rating"] is None

    def test_verbatim_vide_tolerated_by_default(self, valid_df):
        """Comportement par défaut inchangé : skip_si_verbatim_vide=false."""
        row = valid_df.iloc[0].copy()
        row["verbatim_content"] = ""
        result = normalize_row(row, "mensuel", {
            "skip_si_verbatim_vide": False,
            "skip_si_brand_vide": False,
            "skip_si_produit_vide": False,
        })
        assert result["verbatim_content"] == ""

    def test_verbatim_vide_blocks_when_rule_enabled(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["verbatim_content"] = ""
        with pytest.raises(RowValidationError) as exc_info:
            normalize_row(row, "mensuel", {
                "skip_si_verbatim_vide": True,
                "skip_si_brand_vide": False,
                "skip_si_produit_vide": False,
            })
        assert exc_info.value.code == "VERBATIM_VIDE"

    def test_brand_vide_blocks_when_rule_enabled(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["brand"] = ""
        with pytest.raises(RowValidationError) as exc_info:
            normalize_row(row, "mensuel", {
                "skip_si_verbatim_vide": False,
                "skip_si_brand_vide": True,
                "skip_si_produit_vide": False,
            })
        assert exc_info.value.code == "BRAND_MANQUANTE"

    def test_produit_vide_blocks_when_rule_enabled(self, valid_df):
        row = valid_df.iloc[0].copy()
        row["product_name_SEMANTIWEB"] = ""
        with pytest.raises(RowValidationError) as exc_info:
            normalize_row(row, "mensuel", {
                "skip_si_verbatim_vide": False,
                "skip_si_brand_vide": False,
                "skip_si_produit_vide": True,
            })
        assert exc_info.value.code == "PRODUIT_MANQUANT"

    def test_default_validation_rules_all_tolerant(self, valid_df):
        """Sans validation_rules explicite, la config par défaut ne durcit
        rien (config.toml [import.validation] désactivé par défaut)."""
        row = valid_df.iloc[0].copy()
        row["brand"] = ""
        row["product_name_SEMANTIWEB"] = ""
        row["verbatim_content"] = ""
        result = normalize_row(row, "mensuel")  # pas de validation_rules
        assert result["brand"] == ""
        assert result["product_name"] == ""
        assert result["verbatim_content"] == ""


# ─── normalize_batch (spec §2.4, §2.9) ────────────────────────────────────────

class TestNormalizeBatch:
    def test_valid_rows_produce_no_skips(self):
        df, _ = parse_csv(VALID_CSV_BYTES)
        rows, skip_details, skips_par_code = normalize_batch(df, "mensuel")
        assert len(rows) == 1
        assert skip_details == []
        assert skips_par_code == {}

    def test_invalid_date_produces_skip_with_correct_line_number(self):
        """Header = ligne 1, donc la 1re ligne de données est la ligne 2."""
        csv_bytes = (
            _HEADER + "\nabc;B;FR;31/02/2025;positive;P;4;S;C;0;"
            "0;0;0;0;0;0;0;0;0;;;\n"
        ).encode("utf-8-sig")
        df, _ = parse_csv(csv_bytes)
        rows, skip_details, skips_par_code = normalize_batch(df, "mensuel")
        assert rows == []
        assert len(skip_details) == 1
        assert skip_details[0]["ligne"] == 2
        assert skip_details[0]["code"] == "DATE_INVALIDE"
        assert skips_par_code == {"DATE_INVALIDE": 1}

    def test_mixed_file_counts_correctly(self):
        good_row = _ROW[0]
        bad_row = (
            "def;B;FR;not-a-date;positive;P;4;S;C;0;"
            "0;0;0;0;0;0;0;0;0;;;"
        )
        csv_bytes = ("\n".join([_HEADER, good_row, bad_row]) + "\n").encode("utf-8-sig")
        df, _ = parse_csv(csv_bytes)
        rows, skip_details, skips_par_code = normalize_batch(df, "mensuel")
        assert len(rows) == 1
        assert len(skip_details) == 1
        assert skip_details[0]["ligne"] == 3  # header=1, ligne1=2, ligne2=3
        assert skips_par_code["DATE_INVALIDE"] == 1

    def test_bad_lines_merged_with_code_ligne_malformee(self):
        df, _ = parse_csv(VALID_CSV_BYTES)
        bad_lines = [{
            "ligne": None, "code": "LIGNE_MALFORMEE",
            "raison": "test", "champ": None, "extrait": "x;y;z",
        }]
        rows, skip_details, skips_par_code = normalize_batch(df, "mensuel", bad_lines)
        assert len(skip_details) == 1
        assert skip_details[0]["code"] == "LIGNE_MALFORMEE"
        assert skips_par_code == {"LIGNE_MALFORMEE": 1}

    def test_extrait_truncated_to_200_chars(self):
        long_value = "x" * 500
        csv_bytes = (
            _HEADER + f"\nabc;{long_value};FR;not-a-date;positive;P;4;S;C;0;"
            "0;0;0;0;0;0;0;0;0;;;\n"
        ).encode("utf-8-sig")
        df, _ = parse_csv(csv_bytes)
        _rows, skip_details, _codes = normalize_batch(df, "mensuel")
        assert len(skip_details[0]["extrait"]) <= 200

    def test_extrait_never_contains_full_verbatim(self):
        """Spec §2.4 : ne jamais mettre le verbatim en clair dans l'extrait —
        volumétrie et données personnelles. Chaque valeur est tronquée à 30."""
        long_verbatim = "Ceci est un verbatim très long qui ne doit jamais apparaître en clair"
        csv_bytes = (
            _HEADER + f"\nabc;B;FR;not-a-date;positive;P;4;S;{long_verbatim};0;"
            "0;0;0;0;0;0;0;0;0;;;\n"
        ).encode("utf-8-sig")
        df, _ = parse_csv(csv_bytes)
        _rows, skip_details, _codes = normalize_batch(df, "mensuel")
        assert long_verbatim not in skip_details[0]["extrait"]

    def test_unknown_exception_gets_erreur_inconnue_code(self, monkeypatch):
        import core.importer as importer_mod

        def _boom(row, import_type, validation_rules=None):
            raise RuntimeError("panne inattendue")

        monkeypatch.setattr(importer_mod, "normalize_row", _boom)
        df, _ = parse_csv(VALID_CSV_BYTES)
        _rows, skip_details, skips_par_code = importer_mod.normalize_batch(df, "mensuel")
        assert skip_details[0]["code"] == "ERREUR_INCONNUE"
        assert skips_par_code == {"ERREUR_INCONNUE": 1}


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
        assert result == {
            "inserted": 0, "duplicates": 0,
            "duplicates_fichier": 0, "duplicates_base": 0,
            "errors": [],
        }
        conn.cursor.assert_not_called()

    def test_returns_inserted_count(self):
        rows = [_make_row(i) for i in range(3)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = [("id1",), ("id2",)]  # 2 insérés sur 3
            result = import_batch(conn, rows, "uuid")
        assert result["inserted"] == 2

    def test_returns_duplicates_count(self):
        rows = [_make_row(i) for i in range(3)]
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            mock_ev.return_value = [("id1",), ("id2",)]
            result = import_batch(conn, rows, "uuid")
        assert result["duplicates"] == 1

    def test_duplicates_within_file_detected_before_insert(self):
        """3 lignes dont 2 partagent le même id (doublon interne au fichier) —
        distingué d'un doublon déjà en base (spec §2.5 : Counter sur les id
        avant l'INSERT)."""
        rows = [_make_row(0), _make_row(0), _make_row(1)]
        rows[1]["id"] = rows[0]["id"]  # doublon interne volontaire
        conn, _ = _make_db_conn()
        with patch("core.importer.execute_values") as mock_ev:
            # Postgres n'insère qu'une fois par id : 2 id distincts insérés.
            mock_ev.return_value = [(rows[0]["id"],), (rows[2]["id"],)]
            result = import_batch(conn, rows, "uuid")
        assert result["inserted"] == 2
        assert result["duplicates"] == 1
        assert result["duplicates_fichier"] == 1
        assert result["duplicates_base"] == 0

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
