"""Charte Consumer Voice — kit commun aux applications Streamlit du pipeline verbatims.

Version : voir VERSION. Le dossier `charte/` et `.streamlit/config.toml` sont recopiés à l'identique
dans chaque application ; ne pas les modifier dans une seule appli.
"""

VERSION = "1.0.0"

from charte.composants import (  # noqa: E402
    STATUTS,
    appliquer_charte,
    badge_statut,
    bandeau,
    carte_verbatim,
    encadre_cle,
    etapes,
    fil_ariane,
    libelle_statut,
    section,
    vide,
)

__all__ = [
    "VERSION", "STATUTS", "appliquer_charte", "badge_statut", "bandeau", "carte_verbatim",
    "encadre_cle", "etapes", "fil_ariane", "libelle_statut", "section", "vide",
]
