"""
tests/test_referentiel.py
Tests unitaires pour core/referentiel.py
"""

import pytest
from pathlib import Path

from core.referentiel import (
    get_all_categories,
    get_sous_categories,
    is_valid_combination,
    load_referentiel,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────

SAMPLE_CSV = (
    "categorie,sous_categorie\n"
    "Body Care,Body Care : Body Moisturizer\n"
    "Body Care,Body Care : Hand Cream\n"
    "Face Care,Face Care : Moisturizer\n"
    "Face Care,Face Care : Cleanser\n"
    "Hair Care,Hair Care : Shampoo\n"
)


@pytest.fixture
def ref_csv(tmp_path) -> Path:
    """CSV référentiel minimal écrit dans un répertoire temporaire."""
    path = tmp_path / "referentiel.csv"
    path.write_text(SAMPLE_CSV, encoding="utf-8")
    return path


@pytest.fixture
def ref_csv_bom(tmp_path) -> Path:
    """CSV avec BOM UTF-8 (comme Excel l'exporte)."""
    path = tmp_path / "ref_bom.csv"
    path.write_bytes(b"\xef\xbb\xbf" + SAMPLE_CSV.encode("utf-8"))
    return path


# ─── load_referentiel ─────────────────────────────────────────────────────────

class TestLoadReferentiel:
    def test_returns_dict(self, ref_csv):
        ref = load_referentiel(ref_csv)
        assert isinstance(ref, dict)

    def test_correct_categories(self, ref_csv):
        ref = load_referentiel(ref_csv)
        assert set(ref.keys()) == {"Body Care", "Face Care", "Hair Care"}

    def test_correct_subcategories_body(self, ref_csv):
        ref = load_referentiel(ref_csv)
        assert "Body Care : Body Moisturizer" in ref["Body Care"]
        assert "Body Care : Hand Cream" in ref["Body Care"]
        assert len(ref["Body Care"]) == 2

    def test_correct_subcategories_face(self, ref_csv):
        ref = load_referentiel(ref_csv)
        assert "Face Care : Moisturizer" in ref["Face Care"]
        assert "Face Care : Cleanser" in ref["Face Care"]

    def test_no_duplicates(self, tmp_path):
        csv = "categorie,sous_categorie\nA,A1\nA,A1\nA,A2\n"
        path = tmp_path / "dup.csv"
        path.write_text(csv, encoding="utf-8")
        ref = load_referentiel(path)
        assert ref["A"].count("A1") == 1
        assert len(ref["A"]) == 2

    def test_handles_utf8_bom(self, ref_csv_bom):
        ref = load_referentiel(ref_csv_bom)
        assert "Body Care" in ref

    def test_skips_empty_rows(self, tmp_path):
        csv = "categorie,sous_categorie\nA,A1\n,\nB,B1\n"
        path = tmp_path / "empty.csv"
        path.write_text(csv, encoding="utf-8")
        ref = load_referentiel(path)
        assert set(ref.keys()) == {"A", "B"}

    def test_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError, match="introuvable"):
            load_referentiel("/nonexistent/path/ref.csv")

    def test_raises_on_missing_columns(self, tmp_path):
        bad = "wrong,columns\nval1,val2\n"
        path = tmp_path / "bad.csv"
        path.write_text(bad, encoding="utf-8")
        with pytest.raises(ValueError, match="colonnes"):
            load_referentiel(path)

    def test_raises_on_missing_one_column(self, tmp_path):
        bad = "categorie,other_col\nA,X\n"
        path = tmp_path / "one_col.csv"
        path.write_text(bad, encoding="utf-8")
        with pytest.raises(ValueError):
            load_referentiel(path)

    def test_order_preserved(self, tmp_path):
        """Les catégories apparaissent dans l'ordre du CSV."""
        csv = "categorie,sous_categorie\nZebra,Z1\nAlpha,A1\nMango,M1\n"
        path = tmp_path / "order.csv"
        path.write_text(csv, encoding="utf-8")
        ref = load_referentiel(path)
        assert list(ref.keys()) == ["Zebra", "Alpha", "Mango"]

    def test_each_call_reloads_file(self, tmp_path):
        """Pas de cache global — chaque appel relit le fichier."""
        path = tmp_path / "dynamic.csv"
        path.write_text("categorie,sous_categorie\nA,A1\n", encoding="utf-8")
        ref1 = load_referentiel(path)
        assert "B" not in ref1

        path.write_text("categorie,sous_categorie\nA,A1\nB,B1\n", encoding="utf-8")
        ref2 = load_referentiel(path)
        assert "B" in ref2


# ─── get_all_categories ───────────────────────────────────────────────────────

class TestGetAllCategories:
    def test_returns_list(self, ref_csv):
        cats = get_all_categories(ref_csv)
        assert isinstance(cats, list)

    def test_contains_all_categories(self, ref_csv):
        cats = get_all_categories(ref_csv)
        assert "Body Care" in cats
        assert "Face Care" in cats
        assert "Hair Care" in cats

    def test_is_sorted(self, ref_csv):
        cats = get_all_categories(ref_csv)
        assert cats == sorted(cats)

    def test_correct_count(self, ref_csv):
        cats = get_all_categories(ref_csv)
        assert len(cats) == 3


# ─── get_sous_categories ──────────────────────────────────────────────────────

class TestGetSousCategories:
    def test_returns_list_for_known_category(self, ref_csv):
        sous = get_sous_categories("Body Care", ref_csv)
        assert isinstance(sous, list)
        assert len(sous) == 2

    def test_correct_values(self, ref_csv):
        sous = get_sous_categories("Body Care", ref_csv)
        assert "Body Care : Body Moisturizer" in sous
        assert "Body Care : Hand Cream" in sous

    def test_returns_empty_for_unknown_category(self, ref_csv):
        sous = get_sous_categories("Unknown Category", ref_csv)
        assert sous == []

    def test_returns_empty_for_empty_string(self, ref_csv):
        sous = get_sous_categories("", ref_csv)
        assert sous == []

    def test_case_sensitive(self, ref_csv):
        """La clé est sensible à la casse."""
        sous = get_sous_categories("body care", ref_csv)
        assert sous == []


# ─── is_valid_combination ─────────────────────────────────────────────────────

class TestIsValidCombination:
    def test_valid_pair_returns_true(self, ref_csv):
        assert is_valid_combination(
            "Body Care", "Body Care : Body Moisturizer", ref_csv
        )

    def test_valid_face_pair_returns_true(self, ref_csv):
        assert is_valid_combination("Face Care", "Face Care : Cleanser", ref_csv)

    def test_invalid_subcategory_returns_false(self, ref_csv):
        assert not is_valid_combination(
            "Body Care", "Body Care : Nonexistent Sub", ref_csv
        )

    def test_unknown_category_returns_false(self, ref_csv):
        assert not is_valid_combination(
            "Unknown", "Body Care : Body Moisturizer", ref_csv
        )

    def test_cross_category_returns_false(self, ref_csv):
        """Sous-catégorie valide mais sous une autre catégorie."""
        assert not is_valid_combination(
            "Face Care", "Body Care : Body Moisturizer", ref_csv
        )

    def test_empty_strings_return_false(self, ref_csv):
        assert not is_valid_combination("", "", ref_csv)

    def test_both_valid_but_wrong_pairing(self, ref_csv):
        assert not is_valid_combination(
            "Hair Care", "Face Care : Moisturizer", ref_csv
        )
