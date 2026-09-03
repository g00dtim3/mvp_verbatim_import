"""
tests/test_compass_ui.py
Tests pour compass_ui/compass_ui.py — en particulier previous_import_alert
et hash_check, qui distinguent maintenant un fichier déjà importé AVEC
SUCCÈS (vert, bloquant — réimporter serait inutile) d'une tentative
précédente en échec/incomplète (ambre, PAS bloquant — l'utilisateur doit
pouvoir réessayer sans avoir l'impression d'un blocage arbitraire).
"""

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _run_previous_import_alert(**kwargs):
    def _script(kwargs):
        from compass_ui.compass_ui import previous_import_alert
        blocking = previous_import_alert(**kwargs)
        import streamlit as st
        st.session_state["_blocking_result"] = blocking

    at = AppTest.from_function(_script, args=(kwargs,))
    at.run()
    assert not at.exception, f"previous_import_alert a levé : {at.exception}"
    return at


class TestPreviousImportAlertSuccess:
    def test_is_blocking(self):
        at = _run_previous_import_alert(
            filename="f.csv", status="success", started_at="01/01/2026 10:00",
            rows_inserted=45213,
        )
        assert at.session_state["_blocking_result"] is True

    def test_wording_is_positive_not_alarming(self):
        at = _run_previous_import_alert(
            filename="f.csv", status="success", started_at="01/01/2026 10:00",
            rows_inserted=45213,
        )
        markdown_text = " ".join(m.value for m in at.markdown)
        assert "45,213" in markdown_text or "45213" in markdown_text
        assert "succès" in markdown_text.lower()
        # Pas de vocabulaire d'échec — le seul "bloqué" présent explique
        # calmement pourquoi (fichier déjà en base), ce n'est pas une erreur.
        for word in ("échec", "erreur", "échoué"):
            assert word not in markdown_text.lower()


class TestPreviousImportAlertError:
    def test_is_not_blocking(self):
        at = _run_previous_import_alert(
            filename="f.csv", status="error", started_at="01/01/2026 10:00",
        )
        assert at.session_state["_blocking_result"] is False

    def test_invites_retry(self):
        at = _run_previous_import_alert(
            filename="f.csv", status="error", started_at="01/01/2026 10:00",
        )
        markdown_text = " ".join(m.value for m in at.markdown).lower()
        assert "relancez" in markdown_text or "relance" in markdown_text


class TestPreviousImportAlertPartial:
    def test_is_not_blocking(self):
        at = _run_previous_import_alert(
            filename="f.csv", status="partial", started_at="01/01/2026 10:00",
            rows_inserted=500, rows_total=1000,
        )
        assert at.session_state["_blocking_result"] is False


class TestPreviousImportAlertRunning:
    def test_is_not_blocking(self):
        """L'appelant (pages/1_Import.py) ne passe status='running' à cette
        fonction que pour le cas 'probablement interrompu' — le cas
        'réellement en cours' est géré séparément en amont et reste
        bloquant, mais via un autre appel (alert() direct, pas cette
        fonction)."""
        at = _run_previous_import_alert(
            filename="f.csv", status="running", started_at="01/01/2026 10:00",
        )
        assert at.session_state["_blocking_result"] is False


def _run_hash_check(status):
    def _script(status):
        from compass_ui.compass_ui import hash_check
        hash_check(status)

    at = AppTest.from_function(_script, args=(status,))
    at.run()
    assert not at.exception, f"hash_check a levé : {at.exception}"
    return at


class TestHashCheckStates:
    def test_all_states_render_without_crash(self):
        for status in ("ok", "dupe-success", "dupe-issue", "dupe-running", "idle"):
            _run_hash_check(status)
