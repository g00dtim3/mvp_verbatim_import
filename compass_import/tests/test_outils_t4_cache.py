"""
tests/test_outils_t4_cache.py
Régression pour le crash de production :
    KeyError: 'error_summary' at pages/3_Outils.py:671

Root cause : Streamlit Cloud ne réinitialise pas forcément les sessions de
navigateur déjà ouvertes lors d'un déploiement. Une session dont
`st.session_state.t4_data` avait été mis en cache par le code précédent
(sans la clé "error_summary", ajoutée dans un PR précédent) plantait dès
que le code mis à jour essayait de lire cette clé — le seul garde-fou
était `if st.session_state.t4_data is None`, qui ne détecte pas un cache
présent mais de forme obsolète.

Correctif : `_is_cache_stale()`, une fonction PURE (aucun appel Streamlit)
extraite exprès pour rester testable directement — contrairement à
`st.session_state`/`st.stop()`, qui ne se comportent pas comme en
production quand le script de la page est chargé hors d'un run Streamlit
réel (cf. le warning "Session state does not function when running a
script without `streamlit run`"). Un premier essai de ce test pré-remplissait
session_state puis important la page en espérant observer l'absence de
KeyError : ce test passait même contre le code fautif (avant ce correctif),
parce que session_state ne fonctionne pas hors run réel — d'où
l'extraction en fonction pure ci-dessous, seule façon fiable de couvrir
cette régression sans faux positif.
"""

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
_PAGE_PATH = _ROOT / "pages" / "3_Outils.py"
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _load_outils_module():
    """Charge pages/3_Outils.py pour en extraire les constantes/fonctions
    pures (_T4_EXPECTED_KEYS, _is_cache_stale) — n'exerce PAS le flux
    contrôlé par Streamlit (session_state, st.stop()), qui ne se comporte
    pas fidèlement hors d'un run réel."""
    spec = importlib.util.spec_from_file_location("outils_page_under_test", _PAGE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _stale_row() -> dict:
    """Forme d'un t4_data mis en cache par le code AVANT l'ajout du champ
    "error_summary" — c'est ce cache-là qui provoquait le KeyError."""
    return {
        "id": "batch-1", "started_at": "01/01/2026 10:00", "filename": "old.csv",
        "import_type": "mensuel", "rows_total": 10, "rows_inserted": 10,
        "rows_skipped": 0, "rows_duplicates": 0, "rows_matched": 10,
        "rows_unmatched": 0, "status": "success", "error_detail": None,
        "duration_s": 5, "data_purged": False,
        # PAS de "error_summary" — c'est le point du test.
    }


class TestIsCacheStale:
    def test_missing_new_key_is_stale(self):
        """Le scénario exact de l'incident : cache pré-PR, sans
        "error_summary" — doit être détecté comme obsolète."""
        module = _load_outils_module()
        assert module._is_cache_stale([_stale_row()], module._T4_EXPECTED_KEYS) is True

    def test_fresh_row_is_not_stale(self):
        module = _load_outils_module()
        fresh_row = {**_stale_row(), "error_summary": "DATE_INVALIDE:2"}
        assert module._is_cache_stale([fresh_row], module._T4_EXPECTED_KEYS) is False

    def test_none_is_not_considered_stale(self):
        """None est déjà géré par le `is None` du garde-fou — _is_cache_stale
        n'a pas besoin de le traiter comme obsolète, juste de ne pas planter."""
        module = _load_outils_module()
        assert module._is_cache_stale(None, module._T4_EXPECTED_KEYS) is False

    def test_empty_list_is_not_considered_stale(self):
        module = _load_outils_module()
        assert module._is_cache_stale([], module._T4_EXPECTED_KEYS) is False

    def test_expected_keys_matches_freshly_built_rows(self):
        """_T4_EXPECTED_KEYS doit rester synchronisé avec les clés
        réellement produites lors d'un chargement frais, sinon le cache ne
        serait jamais considéré comme à jour (rechargement à chaque run)."""
        module = _load_outils_module()
        fresh_row_keys = set(_stale_row().keys()) | {"error_summary"}
        assert module._T4_EXPECTED_KEYS == fresh_row_keys

    def test_would_have_failed_before_the_fix(self):
        """Preuve que ce test couvre bien la régression : reproduit la
        vérification pré-correctif (`cached is None` seul) et montre
        qu'elle laissait passer un cache obsolète — contrairement à
        `_is_cache_stale`, qui le détecte."""
        module = _load_outils_module()
        stale_cache = [_stale_row()]
        old_guard_would_reload = stale_cache is None  # comportement d'avant
        assert old_guard_would_reload is False  # confirme le bug : ne rechargeait pas
        assert module._is_cache_stale(stale_cache, module._T4_EXPECTED_KEYS) is True
