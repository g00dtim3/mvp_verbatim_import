"""
compass_ui.py
─────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Composants UI réutilisables pour Streamlit.

Depuis la charte Consumer Voice (docs/charte/DESIGN_GUIDE.md), ces fonctions
gardent leur nom et leur signature, mais s'affichent avec les éléments natifs
de Streamlit et le kit commun (charte/) : plus de HTML ni de CSS propres à
Compass. Le thème clair / sombre se choisit dans le menu de Streamlit.

Usage :
    from charte import appliquer_charte
    from compass_ui.compass_ui import page_header, metric_row
    appliquer_charte()
    page_header("Import", "Charger le fichier CSV de l'API")
    metric_row([...])

Les messages passés à ``alert`` sont en Markdown (``**gras**``, ``` `code` ```),
pas en HTML.
"""

from typing import Literal, Optional

import pandas as pd
import streamlit as st

from charte import (
    appliquer_charte,
    badge_statut,
    bandeau,
    encadre_cle,
    etapes,
    libelle_statut,
    vide,
)

APPLI = "Consumer Voice · Import Compass"


# ─── Format français ───────────────────────────────────────────────────────────

def nombre(n) -> str:
    """Entier au format français : 189 824 (espace insécable comme séparateur)."""
    return f"{n:,}".replace(",", " ")


# ─── Charte ────────────────────────────────────────────────────────────────────

def inject_css():
    """Ancien point d'entrée du design system : applique désormais la charte commune.
    Les pages appellent directement ``charte.appliquer_charte()``."""
    appliquer_charte()


def theme_toggle():
    """Ne fait plus rien : le thème se choisit dans le menu de Streamlit
    (⋮ → Settings → Theme)."""


def sidebar_header():
    """Ne fait plus rien : le nom de l'application est dans le bandeau de chaque page."""


def sidebar_section(label: str):
    """Label de section dans la sidebar."""
    st.sidebar.caption(label)


# ─── Page header ────────────────────────────────────────────────────────────────

def page_header(
    title: str,
    subtitle: str = "",
    badge: Optional[str] = None,
    badge_type: str = "cyan",
):
    """
    Bandeau de page (un seul par page, en premier).

    Args:
        title: Titre de la page
        subtitle: Sous-titre descriptif
        badge: Étiquette optionnelle (ex: "Import initial")
        badge_type: conservé pour compatibilité, sans effet (le bandeau a une
            seule couleur d'étiquette)
    """
    bandeau(APPLI, title, sous_titre=subtitle or None, etiquette=badge)


# ─── Metrics ────────────────────────────────────────────────────────────────────

def metric_row(metrics: list[dict]):
    """
    Ligne de chiffres clés (``st.metric`` bordés).

    Args:
        metrics: Liste de dicts avec clés :
            - label (str)
            - value (str | int)
            - delta (optionnel) : affiché seulement si toutes les métriques
              en ont un (règle de la charte : tous ou aucun)
            - color, delta_dir : conservés pour compatibilité, sans effet

    Exemple:
        ui.metric_row([
            {"label": "Verbatims insérés", "value": "42 156"},
            {"label": "Lignes ignorées", "value": "14"},
        ])
    """
    if not metrics:
        return
    avec_delta = all(m.get("delta") for m in metrics)
    for col, m in zip(st.columns(len(metrics)), metrics):
        valeur = m["value"]
        if isinstance(valeur, int) and not isinstance(valeur, bool):
            valeur = nombre(valeur)
        with col:
            st.metric(
                m["label"],
                valeur,
                delta=m["delta"] if avec_delta else None,
                border=True,
            )


# ─── Badges ─────────────────────────────────────────────────────────────────────

def badge(text: str, type: str = "gray") -> str:
    """
    Badge natif en Markdown (à afficher avec st.markdown).
    type: "success" | "error" | "warning" | "info" | "cyan" | "gray" | "duplicate"
    """
    couleur = {
        "success": "green", "error": "red", "warning": "yellow",
        "info": "blue", "cyan": "blue", "duplicate": "yellow",
    }.get(type, "gray")
    texte = text.replace("[", "(").replace("]", ")")
    return f":{couleur}-badge[{texte}]"


def status_badge(status: str) -> str:
    """
    Badge de statut pour les logs d'import (Markdown natif).
    status: "running" | "success" | "partial" | "error" | "duplicate"
    """
    return badge_statut(status)


_TYPES_IMPORT = {
    "initial": "Import initial",
    "mensuel": "Import mensuel",
    "reset":   "Reprise à zéro",
}


def import_type_badge(type_: str) -> str:
    """Badge pour le type d'import (Markdown natif)."""
    return badge(_TYPES_IMPORT.get(type_, type_), "warning" if type_ == "reset" else "info")


# ─── Alert blocks ───────────────────────────────────────────────────────────────

_ALERTES = {
    "success":   (st.success, ":material/check_circle:"),
    "error":     (st.error,   ":material/error:"),
    "warning":   (st.warning, ":material/warning:"),
    "info":      (st.info,    ":material/info:"),
    "duplicate": (st.info,    ":material/content_copy:"),
}


def alert(
    message: str,
    type: Literal["success", "error", "warning", "info", "duplicate"] = "info",
    title: Optional[str] = None,
):
    """
    Alerte native (st.info / st.success / st.warning / st.error).

    Args:
        message: Corps du message, en Markdown
        type: Type d'alerte
        title: Titre en gras optionnel
    """
    fonction, icone = _ALERTES.get(type, _ALERTES["info"])
    corps = f"**{title}**\n\n{message}" if title else message
    fonction(corps, icon=icone)


def previous_import_alert(
    filename: str,
    status: str,
    started_at: str,
    finished_at: str = "",
    rows_inserted: int = 0,
    rows_total: int = 0,
    batch_id: str = "",
) -> bool:
    """
    Alerte affichée quand ce fichier (même hash) a déjà été soumis
    auparavant. Le ton, la couleur et le texte dépendent du statut de
    cette tentative précédente — un import réussi n'est pas un problème
    à signaler en rouge, et un import en échec ne doit pas se contenter
    de bloquer silencieusement sans expliquer quoi faire.

    Args:
        filename: Nom du fichier de la tentative précédente.
        status: ``running`` | ``success`` | ``partial`` | ``error`` (valeur
            de ``import_logs.status``).
        started_at: Date/heure de démarrage, déjà formatée.
        finished_at: Date/heure de fin, déjà formatée (``""`` si absente —
            import jamais terminé).
        rows_inserted: Lignes effectivement insérées lors de la tentative.
        rows_total: Total de lignes du fichier (pour un import partiel).
        batch_id: UUID du log, pour affichage court.

    Returns:
        ``True`` si l'import doit rester bloqué (nouvelle tentative
        inutile — fichier déjà intégralement importé), ``False`` si
        l'utilisateur doit pouvoir relancer l'import malgré cette alerte
        (tentative précédente en échec, incomplète, ou probablement
        interrompue).
    """
    batch_str = f" (lot `{batch_id[:8]}…`)" if batch_id else ""
    when = finished_at or started_at

    if status == "success":
        alert(
            message=(
                f"**{nombre(rows_inserted)}** verbatim(s) déjà "
                f"inséré(s) le **{when}**{batch_str}. Relancer "
                "l'import réinsérerait les mêmes lignes pour rien — c'est "
                "pour ça qu'il est bloqué, pas parce qu'il y a un problème."
            ),
            type="success",
            title=f"Ce fichier a déjà été importé avec succès — {filename}",
        )
        return True

    if status == "partial":
        alert(
            message=(
                f"**{nombre(rows_inserted)}** / {nombre(rows_total)} "
                f"verbatim(s) insérés le **{started_at}**"
                f"{batch_str} — l'import précédent ne s'est pas terminé "
                "normalement (voir Outils → Logs d'import pour le détail). "
                "Vous pouvez relancer l'import ci-dessous : les lignes déjà "
                "en base ne seront pas dupliquées."
            ),
            type="warning",
            title=f"Import précédent incomplet — {filename}",
        )
        return False

    if status == "error":
        alert(
            message=(
                f"La tentative du **{started_at}**{batch_str} "
                "a échoué avant d'aboutir. Consultez "
                "**Outils → Logs d'import** pour le détail, "
                "puis relancez l'import ci-dessous — rien ne vous en empêche."
            ),
            type="error",
            title=f"L'import précédent de ce fichier a échoué — {filename}",
        )
        return False

    # status == "running" : soit un import est réellement en cours, soit un
    # précédent essai a été interrompu (crash) sans jamais se finaliser —
    # l'appelant tranche selon l'ancienneté de started_at (cf. pages/1_Import.py)
    # et n'appelle cette fonction que pour le cas "probablement interrompu".
    alert(
        message=(
            f"Un import de ce fichier a démarré le **{started_at}**"
            f"{batch_str} et n'est jamais arrivé à son terme (crash probable — "
            "voir Outils → Logs d'import). Vous pouvez relancer l'import "
            "ci-dessous."
        ),
        type="warning",
        title=f"Import précédent inachevé — {filename}",
    )
    return False


# ─── Hash check ─────────────────────────────────────────────────────────────────

_HASH_CHECK = {
    # statut : (couleur du badge, icône Material, libellé)
    "ok":           ("green",  "check_circle", "Fichier nouveau — import autorisé"),
    "dupe-success": ("green",  "check_circle", "Déjà importé avec succès"),
    "dupe-issue":   ("yellow", "warning",      "Tentative précédente incomplète — nouvel essai possible"),
    "dupe-running": ("blue",   "sync",         "Import déjà en cours pour ce fichier"),
    "idle":         ("gray",   "schedule",     "En attente d'un fichier…"),
}


def hash_check(
    status: Literal["ok", "dupe-success", "dupe-issue", "dupe-running", "idle"],
    detail: str = "",
):
    """
    Indicateur de contrôle hash fichier (badge natif : icône + mot).

    Args:
        status: "ok" = nouveau fichier | "dupe-success" = déjà importé avec
            succès (bloquant) | "dupe-issue" = tentative précédente en échec
            ou incomplète (non bloquant, nouvel essai possible) |
            "dupe-running" = import déjà en cours pour ce fichier (bloquant,
            temporaire) | "idle" = en attente
        detail: Texte complémentaire (ex: date du premier import)
    """
    couleur, icone, libelle = _HASH_CHECK[status]
    texte = (detail or libelle).replace("[", "(").replace("]", ")")
    st.markdown(f":{couleur}-badge[:material/{icone}: {texte}]")


# ─── Step indicator ──────────────────────────────────────────────────────────────

def steps(items: list[str], current: int):
    """
    Indicateur d'étapes horizontal.

    Args:
        items: Liste des labels d'étapes
        current: Index de l'étape active (0-based)

    Exemple:
        ui.steps(["Fichier", "Vérification", "Import", "Résumé"], current=1)
    """
    etapes(items, courante=current)


# ─── Progress block ──────────────────────────────────────────────────────────────

def progress_block(title: str, subtitle: str, percent: int):
    """
    Barre de progression pendant un import.

    Args:
        title: Ex: "Import en cours…"
        subtitle: Ex: "Traitement de 42 000 / 500 000 lignes"
        percent: 0–100
    """
    percent = max(0, min(100, int(percent)))
    st.progress(percent / 100, text=f"{title} — {subtitle} ({percent} %)")


# ─── Import mode toggle ──────────────────────────────────────────────────────────

def import_mode_toggle() -> str:
    """
    Choix Import mensuel / Import initial.
    Retourne "initial" ou "mensuel".
    """
    mode = st.segmented_control(
        "Mode d'import",
        options=["Import mensuel", "Import initial (one-shot)"],
        default="Import mensuel",
        required=True,
        help=(
            "**Import mensuel** : fichier courant de l'API — "
            "catégorie, sous-catégorie et photo seront vides.\n\n"
            "**Import initial** : fichier historique complet — "
            "tous les champs sont déjà remplis."
        ),
    )
    return "initial" if mode and "initial" in mode else "mensuel"


# ─── Log table ───────────────────────────────────────────────────────────────────

def log_table(logs: list[dict]):
    """
    Tableau des logs d'import.

    Args:
        logs: Liste de dicts avec clés :
            started_at, filename, import_type, rows_inserted,
            rows_skipped, rows_matched, rows_unmatched, status, duration_s
    """
    if not logs:
        vide("Aucun import réalisé. Les logs apparaîtront ici après le premier import.")
        return

    df = pd.DataFrame([
        {
            "Date":            log.get("started_at", "—"),
            "Fichier":         log.get("filename", "—"),
            "Type":            _TYPES_IMPORT.get(log.get("import_type", ""), log.get("import_type", "—")),
            "Insérés":         log.get("rows_inserted", 0),
            "Ignorées":        log.get("rows_skipped", 0),
            "Avec catégorie":  log.get("rows_matched", 0),
            "Sans catégorie":  log.get("rows_unmatched", 0),
            "Statut":          libelle_statut(log.get("status", "")),
            "Durée (s)":       log.get("duration_s", 0),
        }
        for log in logs
    ])
    nombre_col = st.column_config.NumberColumn
    st.dataframe(
        df,
        hide_index=True,
        column_config={
            "Insérés":        nombre_col(format="localized"),
            "Ignorées":       nombre_col(format="localized"),
            "Avec catégorie": nombre_col(format="localized"),
            "Sans catégorie": nombre_col(format="localized"),
            "Durée (s)":      nombre_col(format="localized"),
        },
    )


# ─── Import summary ──────────────────────────────────────────────────────────────

def import_summary(
    rows_inserted: int,
    rows_skipped: int,
    rows_matched: int,
    rows_unmatched: int,
    duration_s: int = 0,
    rows_duplicates: int = 0,
):
    """
    Résumé post-import : chiffres clés et phrase de conclusion.

    ``rows_skipped`` (validation, en amont de l'INSERT) et
    ``rows_duplicates`` (``ON CONFLICT DO NOTHING``, en base ou interne au
    fichier) sont deux réalités distinctes — cf. spec §2.5/§2.7 — affichées
    comme deux tuiles séparées plutôt que fusionnées, pour ne pas reproduire
    la confusion qui masquait l'origine des "doublons" avant cette refonte.
    """
    metric_row([
        {"label": "Verbatims insérés", "value": rows_inserted},
        {"label": "Lignes ignorées",   "value": rows_skipped},
        {"label": "Doublons",          "value": rows_duplicates},
        {"label": "Avec catégorie",    "value": rows_matched},
        {"label": "Sans catégorie",    "value": rows_unmatched},
    ])
    conclusion = f"{nombre(rows_inserted)} verbatim(s) inséré(s)"
    if duration_s:
        conclusion += f" en {nombre(duration_s)} s"
    encadre_cle(conclusion + ".", titre="Import terminé")
    if rows_unmatched > 0:
        alert(
            message=(
                f"**{nombre(rows_unmatched)} verbatims** n'ont pas de catégorie. "
                "Utilisez le module **Matching catégories** pour les compléter."
            ),
            type="warning",
        )


# ─── Matching summary ────────────────────────────────────────────────────────────

def matching_summary(products_matched: int, verbatims_updated: int):
    """Résumé post-matching."""
    metric_row([
        {"label": "Produits matchés",     "value": products_matched},
        {"label": "Verbatims mis à jour", "value": verbatims_updated},
    ])


# ─── Product table (matching) ────────────────────────────────────────────────────

def product_status_table(products: list[dict]):
    """
    Tableau des produits avec leur statut de matching.

    Args:
        products: List de dicts :
            product_name, nb_verbatims, categorie_interne, sous_categorie_interne, photo
    """
    if not products:
        return

    photo_libelle = {True: "Oui", False: "Non"}
    df = pd.DataFrame([
        {
            "Produit":        p.get("product_name", ""),
            "Verbatims":      p.get("nb_verbatims", 0),
            "Catégorie":      p.get("categorie_interne") or "—",
            "Sous-catégorie": p.get("sous_categorie_interne") or "—",
            "Photo":          photo_libelle.get(p.get("photo"), "—"),
            "Statut":         "Matchée" if p.get("categorie_interne") else "À compléter",
        }
        for p in products
    ])
    st.dataframe(
        df,
        hide_index=True,
        column_config={
            "Produit":   st.column_config.TextColumn(width="large"),
            "Verbatims": st.column_config.NumberColumn(format="localized"),
        },
    )


# ─── Empty state ─────────────────────────────────────────────────────────────────

def empty_state(icon: str, title: str, subtitle: str = ""):
    """État vide générique. ``icon`` est conservé pour compatibilité, sans effet."""
    titre = title.rstrip(".")
    vide(f"{titre}. {subtitle}" if subtitle else f"{titre}.")


# ─── Divider with label ───────────────────────────────────────────────────────────

def section_divider(label: str = ""):
    """Séparateur de section avec label optionnel."""
    if label:
        st.header(label, anchor=False)
    else:
        st.divider()
