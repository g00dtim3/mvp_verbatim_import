"""
tests/test_skip_report.py
Tests unitaires pour core/skip_report.py (spec §2.6, §2.9).
"""

import json

from core.skip_report import build_error_detail, parse_error_detail


def _skip(i: int, code: str = "DATE_INVALIDE") -> dict:
    return {"ligne": i, "code": code, "raison": f"raison {i}", "champ": "date", "extrait": f"ligne={i}"}


# ─── build_error_detail ────────────────────────────────────────────────────────

class TestBuildErrorDetail:
    def test_none_when_nothing_to_report(self):
        assert build_error_detail([], []) is None
        assert build_error_detail([], None) is None

    def test_version_1(self):
        payload = build_error_detail([_skip(2)])
        assert payload["version"] == 1

    def test_skips_total_matches_full_count(self):
        skips = [_skip(i) for i in range(10)]
        payload = build_error_detail(skips, max_skip_details=5)
        assert payload["skips_total"] == 10
        assert len(payload["skips"]) == 5

    def test_truncation_1000_to_500(self):
        """Spec §2.9 : 1000 skips → 500 entrées détaillées, skips_total=1000,
        skips_tronque=True."""
        skips = [_skip(i) for i in range(1000)]
        payload = build_error_detail(skips, max_skip_details=500)
        assert payload["skips_total"] == 1000
        assert len(payload["skips"]) == 500
        assert payload["skips_tronque"] is True

    def test_no_truncation_when_under_limit(self):
        skips = [_skip(i) for i in range(3)]
        payload = build_error_detail(skips, max_skip_details=500)
        assert payload["skips_tronque"] is False
        assert len(payload["skips"]) == 3

    def test_skips_par_code_counts_full_list_not_truncated(self):
        """Le compteur par code porte sur la totalité, même si le détail
        est tronqué — sinon la répartition serait faussée."""
        skips = [_skip(i, code="DATE_INVALIDE") for i in range(3)] + \
                [_skip(i, code="VERBATIM_VIDE") for i in range(2)]
        payload = build_error_detail(skips, max_skip_details=1)
        assert payload["skips_par_code"] == {"DATE_INVALIDE": 3, "VERBATIM_VIDE": 2}

    def test_streaming_overrides_used_when_provided(self):
        """Cas d'usage pages/1_Import.py : l'appelant a déjà plafonné
        skip_details en amont (accumulateur borné en mémoire pendant un
        import streaming) et fournit le vrai total/la vraie répartition
        séparément — build_error_detail ne doit pas recalculer depuis la
        liste déjà tronquée (ce qui sous-compterait)."""
        capped_details = [_skip(i) for i in range(3)]  # déjà plafonné à 3
        payload = build_error_detail(
            capped_details,
            skips_total=10_000,
            skips_par_code={"DATE_INVALIDE": 9_000, "VERBATIM_VIDE": 1_000},
        )
        assert payload["skips_total"] == 10_000
        assert len(payload["skips"]) == 3  # le détail reste celui fourni
        assert payload["skips_par_code"] == {"DATE_INVALIDE": 9_000, "VERBATIM_VIDE": 1_000}
        assert payload["skips_tronque"] is True

    def test_streaming_overrides_absent_falls_back_to_computed(self):
        """Sans override, comportement identique à l'ancien (liste complète)."""
        skips = [_skip(i) for i in range(3)]
        payload = build_error_detail(skips)
        assert payload["skips_total"] == 3
        assert payload["skips_tronque"] is False

    def test_skips_total_override_without_details_still_reports(self):
        """Le detail peut être vide (rien conservé) tant que skips_total
        indique qu'il y a bien eu des skips — ne doit pas retourner None."""
        payload = build_error_detail([], skips_total=5, skips_par_code={"X": 5})
        assert payload is not None
        assert payload["skips_total"] == 5

    def test_batch_errors_included(self):
        payload = build_error_detail([], ["Erreur batch 1 : boom"])
        assert payload["batch_errors"] == ["Erreur batch 1 : boom"]
        assert payload["skips"] == []

    def test_accents_and_apostrophes_preserved_through_json_roundtrip(self):
        """Spec §2.9 : caractères accentués et apostrophes préservés, JSON
        relisible."""
        skips = [{
            "ligne": 5, "code": "BRAND_MANQUANTE",
            "raison": "La marque « L'Oréal » n'est pas reconnue, à vérifier",
            "champ": "brand", "extrait": "brand=L'Oréal; ville=Non-déterminée",
        }]
        payload = build_error_detail(skips)
        serialized = json.dumps(payload, ensure_ascii=False)
        assert "L'Oréal" in serialized
        assert "\\u" not in serialized  # ensure_ascii=False : pas d'échappement unicode
        reloaded = json.loads(serialized)
        assert reloaded["skips"][0]["raison"] == skips[0]["raison"]


# ─── parse_error_detail ─────────────────────────────────────────────────────────

class TestParseErrorDetail:
    def test_none_does_not_crash(self):
        parsed = parse_error_detail(None)
        assert parsed["skips"] == []
        assert parsed["is_legacy"] is False

    def test_empty_string_does_not_crash(self):
        parsed = parse_error_detail("")
        assert parsed["skips"] == []

    def test_roundtrip_new_format(self):
        payload = build_error_detail([_skip(2)], ["err"])
        parsed = parse_error_detail(payload)  # dict déjà décodé (colonne JSONB)
        assert parsed["version"] == 1
        assert parsed["is_legacy"] is False
        assert parsed["skips"][0]["ligne"] == 2
        assert parsed["batch_errors"] == ["err"]

    def test_roundtrip_new_format_as_json_string(self):
        payload = build_error_detail([_skip(2)])
        parsed = parse_error_detail(json.dumps(payload, ensure_ascii=False))
        assert parsed["version"] == 1
        assert parsed["skips"][0]["ligne"] == 2

    def test_invalid_json_string_does_not_crash(self):
        parsed = parse_error_detail("{not valid json")
        assert parsed["is_legacy"] is True
        assert parsed["legacy_text"] == "{not valid json"

    def test_legacy_free_text_does_not_crash(self):
        """Ancien format : str(exc) brut, stocké tel quel avant cette refonte."""
        parsed = parse_error_detail("connection refused: timeout after 30s")
        assert parsed["is_legacy"] is True
        assert parsed["legacy_text"] == "connection refused: timeout after 30s"

    def test_legacy_dict_without_version_does_not_crash(self):
        """Format pré-lot-2 : {"skipped": [...], "batch_errors": [...]}."""
        legacy = {"skipped": [_skip(3)], "batch_errors": ["boom"]}
        parsed = parse_error_detail(legacy)
        assert parsed["is_legacy"] is True
        assert parsed["skips"] == [_skip(3)]
        assert parsed["batch_errors"] == ["boom"]

    def test_legacy_bare_list_does_not_crash(self):
        parsed = parse_error_detail([_skip(1), _skip(2)])
        assert parsed["is_legacy"] is True
        assert len(parsed["skips"]) == 2

    def test_legacy_text_wrapper_does_not_crash(self):
        """Format utilisé par pages/1_Import.py en cas d'exception globale."""
        parsed = parse_error_detail({"legacy_text": "Import échoué : timeout"})
        assert parsed["is_legacy"] is True
        assert parsed["legacy_text"] == "Import échoué : timeout"

    def test_unknown_type_does_not_crash(self):
        parsed = parse_error_detail(42)
        assert parsed["is_legacy"] is True
