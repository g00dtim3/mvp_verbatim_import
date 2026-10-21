"""
Composants Streamlit de la charte Consumer Voice.

Utilisation, en haut de chaque page :

    import streamlit as st
    from auth import exiger_mot_de_passe
    from charte import appliquer_charte, bandeau

    st.set_page_config(page_title="Import — Compass", page_icon=":material/upload:", layout="wide")
    exiger_mot_de_passe()
    appliquer_charte()
    bandeau("Consumer Voice · Import Compass", "Importer des verbatims", sous_titre="Export validé ou fichier CSV")

Règles :
- Tout texte passé à ces fonctions est échappé (aucun HTML injecté depuis les données).
- Ces fonctions ne remplacent pas les éléments natifs : boutons, alertes, métriques, tableaux,
  onglets et formulaires restent ceux de Streamlit, stylés par .streamlit/config.toml.
"""

from __future__ import annotations

from contextlib import contextmanager
from html import escape
from pathlib import Path
from typing import Iterator, Sequence

import streamlit as st

from charte.jetons import variables_css

_CSS = Path(__file__).with_name("charte.css")

# Pictogramme commun (bulle de dialogue) : même trait que celui du gabarit R&R, sans l'étoile propre aux avis.
_LOGO = ('<svg width="30" height="30" viewBox="0 0 32 32" aria-hidden="true" focusable="false">'
         '<path d="M5 6h22a3 3 0 0 1 3 3v12a3 3 0 0 1-3 3H14l-6 5v-5H5a3 3 0 0 1-3-3V9a3 3 0 0 1 3-3z" '
         'fill="none" stroke="#fff" stroke-width="2"/>'
         '<path d="M9 12h14M9 16h10" stroke="#fff" stroke-width="2" stroke-linecap="round"/></svg>')


def _theme_actif() -> str:
    """'dark' ou 'light', selon le thème choisi dans le menu de Streamlit (Paramètres → Thème)."""
    try:
        return "dark" if st.context.theme.type == "dark" else "light"
    except Exception:
        return "light"


def _html(markup: str) -> None:
    st.markdown(markup, unsafe_allow_html=True)


def appliquer_charte() -> None:
    """À appeler sur chaque page, juste après st.set_page_config() et exiger_mot_de_passe()."""
    css = _CSS.read_text(encoding="utf-8")
    bloc = f"{variables_css(_theme_actif())}\n{css}"
    # Les lignes vides couperaient le bloc <style> en paragraphes Markdown : on les retire.
    bloc = "\n".join(ligne for ligne in bloc.splitlines() if ligne.strip())
    _html(f"<style>\n{bloc}\n</style>")


def bandeau(appli: str, page: str, *, sous_titre: str | None = None, etiquette: str | None = None) -> None:
    """En-tête de page : nom de l'application, titre de la page, sous-titre et étiquette facultatifs.
    Remplace st.title(). Un seul bandeau par page."""
    sous = f'<p class="cv-sous-titre">{escape(sous_titre)}</p>' if sous_titre else ""
    etiq = f'<span class="cv-etiquette">{escape(etiquette)}</span>' if etiquette else ""
    _html(
        f'<header class="cv-bandeau">{_LOGO}'
        f'<div class="cv-titres"><span class="cv-appli">{escape(appli)}</span>'
        f'<h1 class="cv-page">{escape(page)}</h1>{sous}</div>{etiq}</header>'
    )


def fil_ariane(elements: Sequence[str]) -> None:
    """Chemin de navigation, le dernier élément étant la page courante."""
    items = [f"<li>{escape(e)}</li>" for e in elements[:-1]]
    if elements:
        items.append(f'<li aria-current="page">{escape(elements[-1])}</li>')
    _html(f'<nav class="cv-ariane" aria-label="Fil d’Ariane"><ol>{"".join(items)}</ol></nav>')


@contextmanager
def section(titre: str, intro: str | None = None) -> Iterator[None]:
    """Carte de section (conteneur bordé natif) avec un titre et une phrase d'introduction.

        with section("Exports validés", "Choisissez l'export à importer."):
            st.dataframe(...)
    """
    with st.container(border=True):
        st.header(titre, anchor=False)
        if intro:
            _html(f'<p class="cv-lead">{escape(intro)}</p>')
        yield


def encadre_cle(texte: str, titre: str | None = None) -> None:
    """Message à retenir (résultat, décision, consigne). Pas pour les erreurs : utiliser st.error / st.warning."""
    t = f'<p class="cv-cle-titre">{escape(titre)}</p>' if titre else ""
    _html(f'<div class="cv-cle">{t}<p>{escape(texte)}</p></div>')


def etapes(libelles: Sequence[str], courante: int) -> None:
    """Étapes d'un parcours. `courante` commence à 0 ; les étapes précédentes sont cochées."""
    items = []
    for i, libelle in enumerate(libelles):
        classe = "cv-fait" if i < courante else ("cv-courante" if i == courante else "")
        attr = ' aria-current="step"' if i == courante else ""
        items.append(f'<li class="{classe}"{attr}>{escape(libelle)}</li>')
    _html(f'<ol class="cv-etapes" aria-label="Étapes">{"".join(items)}</ol>')


def carte_verbatim(texte: str, *, meta: str = "", sentiment: str | None = None,
                   detail: str | None = None, detail_titre: str = "Texte d'origine") -> None:
    """Un verbatim cité tel quel. sentiment : 'positive' | 'negative' | 'neutral' | None.
    `detail` (facultatif) s'affiche replié, par exemple le texte en langue d'origine."""
    classe = {"positive": "cv-pos", "negative": "cv-neg"}.get((sentiment or "").lower(), "")
    m = f'<p class="cv-meta">{escape(meta)}</p>' if meta else ""
    d = (f"<details><summary>{escape(detail_titre)}</summary><p>{escape(detail)}</p></details>"
         if detail else "")
    _html(f'<article class="cv-verbatim {classe}"><p>{escape(texte)}</p>{m}{d}</article>')


def vide(message: str) -> None:
    """État vide : rien à afficher, et quoi faire."""
    _html(f'<div class="cv-vide">{escape(message)}</div>')


# ─── Statuts : un seul vocabulaire et un seul code couleur pour les trois applis ──────────────────

# statut technique → (couleur de badge Streamlit, icône Material, libellé affiché)
STATUTS: dict[str, tuple[str, str, str]] = {
    # Réussite
    "COMPLETED": ("green", "check_circle", "Terminé"),
    "success": ("green", "check_circle", "Réussi"),
    "COMPLETE": ("green", "check_circle", "Complet"),
    "MATCH": ("green", "check_circle", "Conforme"),
    "READY": ("green", "check_circle", "Prêt"),
    # En cours
    "RUNNING": ("blue", "sync", "En cours"),
    "running": ("blue", "sync", "En cours"),
    "VALIDATING": ("blue", "sync", "Validation"),
    "STOPPING": ("blue", "sync", "Arrêt en cours"),
    # En attente / neutre
    "PENDING": ("gray", "schedule", "En attente"),
    "CREATED": ("gray", "schedule", "Créé"),
    "LOCAL": ("gray", "schedule", "Local"),
    "CANCELLED": ("gray", "block", "Annulé"),
    # À vérifier
    "PAUSED": ("yellow", "pause_circle", "En pause"),
    "INCOMPLETE": ("yellow", "warning", "Incomplet"),
    "partial": ("yellow", "warning", "Partiel"),
    "PARTIAL": ("yellow", "warning", "Partiel"),
    "STALE": ("yellow", "warning", "À rafraîchir"),
    "WARNING": ("yellow", "warning", "À vérifier"),
    "duplicate": ("yellow", "content_copy", "Déjà importé"),
    # Échec
    "FAILED": ("red", "error", "Échec"),
    "error": ("red", "error", "Erreur"),
    "MISMATCH": ("red", "error", "Écart"),
}


def badge_statut(statut: str) -> str:
    """Badge Markdown natif (à passer à st.markdown ou à placer dans un texte) pour un statut connu.
    Statut inconnu : badge gris avec le texte tel quel."""
    couleur, icone, libelle = STATUTS.get(statut, ("gray", "help", statut))
    libelle = libelle.replace("[", "(").replace("]", ")")
    return f":{couleur}-badge[:material/{icone}: {libelle}]"


def libelle_statut(statut: str) -> str:
    """Libellé français d'un statut (pour un tableau, où les badges ne s'affichent pas)."""
    return STATUTS.get(statut, ("gray", "help", statut))[2]
