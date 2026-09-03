"""
tests/test_skip_table.py
Tests pour compass_ui/skip_table.py via streamlit.testing.v1.AppTest.

Couvre en particulier la régression identifiée lors du débogage du crash
mémoire de l'import : un import n'ayant QUE des erreurs de lot (aucune
ligne rejetée par la validation) ne remontait rien du tout dans l'onglet
Outils / Logs — ni le tableau de skips (vide), ni les erreurs de lot
(branche jamais atteinte). C'est l'un des deux problèmes signalés
("l'export complet du log d'import ... n'est toujours pas visible").
"""

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _run(error_detail):
    def _script(detail):
        from compass_ui.skip_table import render_skip_details
        render_skip_details(detail, key_prefix="test")

    at = AppTest.from_function(_script, args=(error_detail,))
    at.run()
    assert not at.exception, f"render_skip_details a levé : {at.exception}"
    return at


class TestBatchErrorsOnlyRegression:
    """Le bug : payload valide (version=1) avec des batch_errors mais
    skips=[] ne rendait RIEN. Corrigé pour toujours afficher les erreurs
    de lot quand elles sont présentes, indépendamment des skips."""

    def test_batch_errors_only_are_rendered(self):
        payload = {
            "version": 1,
            "skips": [],
            "skips_par_code": {},
            "skips_total": 0,
            "skips_tronque": False,
            "batch_errors": ["Erreur batch 3 (lignes 2000-2999) : connexion perdue"],
        }
        at = _run(payload)
        all_code_text = " ".join(c.value for c in at.code)
        assert "connexion perdue" in all_code_text

    def test_batch_errors_only_shows_no_misleading_skip_caption(self):
        """Ne doit pas dire 'aucune ligne ignorée' sans mentionner les
        erreurs de lot qui, elles, existent bien."""
        payload = {
            "version": 1, "skips": [], "skips_par_code": {}, "skips_total": 0,
            "skips_tronque": False, "batch_errors": ["boom"],
        }
        at = _run(payload)
        captions = [c.value for c in at.caption]
        assert not any("Aucune ligne ignorée" in c for c in captions)


class TestSkipsAndBatchErrorsTogether:
    def test_both_present_both_rendered(self):
        payload = {
            "version": 1,
            "skips": [{"ligne": 5, "code": "DATE_INVALIDE", "raison": "bad date",
                       "champ": "date", "extrait": "date=31/02/2025"}],
            "skips_par_code": {"DATE_INVALIDE": 1},
            "skips_total": 1,
            "skips_tronque": False,
            "batch_errors": ["Erreur batch 1 : timeout"],
        }
        at = _run(payload)
        assert len(at.dataframe) == 1
        all_code_text = " ".join(c.value for c in at.code)
        assert "timeout" in all_code_text


class TestSkipsOnly:
    def test_skips_rendered_with_download_button(self):
        payload = {
            "version": 1,
            "skips": [{"ligne": 2, "code": "VERBATIM_VIDE", "raison": "vide",
                       "champ": "verbatim_content", "extrait": "brand=X"}],
            "skips_par_code": {"VERBATIM_VIDE": 1},
            "skips_total": 1,
            "skips_tronque": False,
            "batch_errors": [],
        }
        at = _run(payload)
        assert len(at.dataframe) == 1
        assert len(at.download_button) == 1


class TestEmptyOrLegacy:
    def test_none_renders_without_crash(self):
        at = _run(None)
        assert len(at.dataframe) == 0

    def test_legacy_free_text_rendered(self):
        at = _run("connection refused: timeout after 30s")
        all_code_text = " ".join(c.value for c in at.code)
        assert "connection refused" in all_code_text
