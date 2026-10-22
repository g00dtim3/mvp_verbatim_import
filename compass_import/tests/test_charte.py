"""
Tests du kit de charte Consumer Voice (à placer dans tests/ de chaque application).

- Le thème .streamlit/config.toml reprend exactement les jetons de charte/jetons.py.
- Les contrastes texte / fond tiennent (WCAG AA : 4,5:1 pour le texte, 3:1 pour les liserés).
- Les composants échappent le texte (aucun HTML injecté depuis les données).
- Chaque page appelle la charte (test à activer dans chaque appli, voir PAGES).
"""

import re
import sys
from pathlib import Path

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover
    import tomli as tomllib

RACINE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RACINE))

from charte import jetons  # noqa: E402



def _trouver_config() -> Path:
    """.streamlit/config.toml est à la racine du dépôt (exigence de Streamlit Community Cloud),
    qui peut être un dossier au-dessus du code (cas de Compass : compass_import/)."""
    for dossier in (RACINE, *RACINE.parents):
        chemin = dossier / ".streamlit" / "config.toml"
        if chemin.exists():
            return chemin
    raise FileNotFoundError(".streamlit/config.toml introuvable à la racine du dépôt")


CONFIG = tomllib.loads(_trouver_config().read_text(encoding="utf-8"))

# option du thème Streamlit → jeton de la charte
CORRESPONDANCE = {
    "primaryColor": "accent-ink", "backgroundColor": "page", "secondaryBackgroundColor": "panel",
    "textColor": "ink", "linkColor": "focus", "borderColor": "line", "dataframeBorderColor": "line",
    "dataframeHeaderBackgroundColor": "brand", "codeBackgroundColor": "panel",
    "blueBackgroundColor": "sel", "blueTextColor": "info-ink", "greenColor": "above",
    "greenBackgroundColor": "ok-bg", "greenTextColor": "ok-ink", "yellowColor": "warn",
    "yellowBackgroundColor": "warn-bg", "yellowTextColor": "warn-ink", "redColor": "neg",
    "redBackgroundColor": "err-bg", "redTextColor": "err-ink", "orangeColor": "below",
    "grayBackgroundColor": "panel-2", "grayTextColor": "ink-2",
}


@pytest.mark.parametrize("theme,valeurs", [("light", jetons.CLAIR), ("dark", jetons.SOMBRE)])
def test_config_identique_aux_jetons(theme, valeurs):
    section = CONFIG["theme"][theme]
    for option, jeton in CORRESPONDANCE.items():
        assert section[option].lower() == valeurs[jeton].lower(), f"[theme.{theme}] {option} ≠ jeton {jeton}"
    assert section["sidebar"]["backgroundColor"].lower() == valeurs["sidebar"].lower()


def test_police():
    assert CONFIG["theme"]["font"].startswith(jetons.POLICE + ":")
    assert CONFIG["theme"]["font"].endswith(jetons.POLICE_URL)


def _luminance(hexa: str) -> float:
    r, g, b = (int(hexa.lstrip("#")[i:i + 2], 16) / 255 for i in (0, 2, 4))
    lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in (r, g, b)]
    return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]


def contraste(a: str, b: str) -> float:
    la, lb = sorted((_luminance(a), _luminance(b)), reverse=True)
    return (la + 0.05) / (lb + 0.05)


PAIRES_TEXTE = [  # (texte, fond)
    ("ink", "page"), ("ink-2", "page"), ("ink-3", "page"), ("ink", "panel"), ("ink-2", "panel"),
    ("info-ink", "sel"), ("ok-ink", "ok-bg"), ("warn-ink", "warn-bg"), ("err-ink", "err-bg"),
    ("ink", "accent-soft"), ("ink", "panel-2"), ("focus", "page"),
]
PAIRES_BLANC = ["brand-deep", "brand", "brand-mid", "accent-ink", "bench"]  # fonds qui portent du texte blanc


@pytest.mark.parametrize("valeurs", [jetons.CLAIR, jetons.SOMBRE], ids=["clair", "sombre"])
def test_contrastes(valeurs):
    for texte, fond in PAIRES_TEXTE:
        assert contraste(valeurs[texte], valeurs[fond]) >= 4.5, f"{texte} sur {fond}"
    for fond in PAIRES_BLANC:
        assert contraste("#ffffff", valeurs[fond]) >= 4.5, f"blanc sur {fond}"
    assert contraste(valeurs["accent"], valeurs["page"]) >= 2.2  # liseré décoratif, jamais seul porteur de sens


def test_composants_echappent_le_texte(monkeypatch):
    from charte import composants
    sortie = []
    monkeypatch.setattr(composants, "_html", sortie.append)
    piege = '<script>alert(1)</script> & "guillemets"'
    composants.bandeau(piege, piege, sous_titre=piege, etiquette=piege)
    composants.fil_ariane([piege, piege])
    composants.encadre_cle(piege, titre=piege)
    composants.etapes([piege, piege], courante=1)
    composants.carte_verbatim(piege, meta=piege, sentiment="negative", detail=piege)
    composants.vide(piege)
    for html in sortie:
        assert "<script>" not in html
        assert "&lt;script&gt;" in html


def test_badge_statut():
    from charte import badge_statut, libelle_statut
    assert badge_statut("COMPLETED") == ":green-badge[:material/check_circle: Terminé]"
    assert badge_statut("FAILED").startswith(":red-badge[")
    assert badge_statut("statut [inconnu]") == ":gray-badge[:material/help: statut (inconnu)]"
    assert libelle_statut("MISMATCH") == "Écart"


# À compléter dans chaque application (chemins relatifs à RACINE) :
# - PAGES : fichiers lancés par Streamlit (point d'entrée, pages/*.py) ; ils appellent appliquer_charte().
# - MODULES : fichiers affichés DANS une page (ex. les modules d'api-explorer) ; ils ne contiennent plus st.title().
PAGES: list[str] = []
MODULES: list[str] = []


@pytest.mark.parametrize("page", PAGES)
def test_chaque_page_applique_la_charte(page):
    texte = (RACINE / page).read_text(encoding="utf-8")
    assert re.search(r"^\s*appliquer_charte\(\)", texte, re.M), f"{page} n'appelle pas appliquer_charte()"
    assert "st.title(" not in texte, f"{page} : remplacer st.title() par bandeau()"


@pytest.mark.parametrize("module", MODULES)
def test_modules_sans_titre_propre(module):
    texte = (RACINE / module).read_text(encoding="utf-8")
    assert "st.title(" not in texte, f"{module} : le bandeau de la page remplace st.title()"
