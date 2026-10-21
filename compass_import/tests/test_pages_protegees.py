"""
Vérifie que chaque page Streamlit de Compass est protégée par le mot de passe (lot 0).

Streamlit exécute chaque page séparément : chacune doit appeler ``exiger_mot_de_passe()``
dans l'instruction qui suit immédiatement ``st.set_page_config(...)``. Le test lit le texte
des fichiers (sans lancer Streamlit) ; une page ajoutée plus tard sans protection le fait échouer.
"""

import ast
from pathlib import Path

import pytest

RACINE = Path(__file__).resolve().parent.parent
PAGES = [RACINE / "Home.py"] + sorted((RACINE / "pages").glob("*.py"))


def _est_appel(instruction: ast.stmt, test) -> bool:
    """Vrai si l'instruction est un simple appel de fonction qui satisfait ``test``."""
    return (
        isinstance(instruction, ast.Expr)
        and isinstance(instruction.value, ast.Call)
        and test(instruction.value.func)
    )


def _est_set_page_config(func: ast.expr) -> bool:
    return isinstance(func, ast.Attribute) and func.attr == "set_page_config"


def _est_exiger_mot_de_passe(func: ast.expr) -> bool:
    return isinstance(func, ast.Name) and func.id == "exiger_mot_de_passe"


def page_protegee(source: str) -> bool:
    """Vrai si ``st.set_page_config(...)`` est suivi immédiatement de ``exiger_mot_de_passe()``."""
    corps = ast.parse(source).body
    for i, instruction in enumerate(corps):
        if _est_appel(instruction, _est_set_page_config):
            return i + 1 < len(corps) and _est_appel(corps[i + 1], _est_exiger_mot_de_passe)
    return False


def test_les_pages_attendues_sont_trouvees():
    noms = {p.name for p in PAGES}
    assert {"Home.py", "1_Import.py", "2_Matching.py", "3_Outils.py", "99_Admin.py"} <= noms


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_page_appelle_exiger_mot_de_passe_apres_set_page_config(page):
    assert page_protegee(page.read_text(encoding="utf-8")), (
        f"{page.name} doit appeler exiger_mot_de_passe() juste après st.set_page_config(...)"
    )


def test_detecte_une_page_sans_protection():
    """Garde-fou : retirer l'appel d'une vraie page doit être détecté."""
    source = (RACINE / "Home.py").read_text(encoding="utf-8")
    sans_appel = "\n".join(
        ligne for ligne in source.splitlines() if not ligne.startswith("exiger_mot_de_passe()")
    )
    assert page_protegee(source)
    assert not page_protegee(sans_appel)


def test_detecte_un_appel_trop_tardif():
    fake_page = (
        "import streamlit as st\n"
        "from auth import exiger_mot_de_passe\n"
        "st.set_page_config(page_title='x')\n"
        "inject_css()\n"
        "exiger_mot_de_passe()\n"
    )
    assert not page_protegee(fake_page)
