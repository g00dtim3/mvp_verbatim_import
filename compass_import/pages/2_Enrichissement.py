"""
pages/2_Enrichissement.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Module 2 — Enrichissement manuel des verbatims.

Flux :
  1. Filtres sidebar → "Appliquer les filtres" (pas de chargement en temps réel)
  2. Tableau st.data_editor (50 lignes / page) — colonnes éditables :
       categorie_interne, sous_categorie_interne, photo
  3. "Sauvegarder les modifications" → confirmation → UPDATE verbatim par verbatim
     (ne touche jamais categories_mapping)
"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from compass_ui.compass_ui import (
    alert,
    empty_state,
    inject_css,
    metric_row,
    page_header,
    sidebar_header,
    sidebar_section,
    theme_toggle,
)
from core.db import get_active_env, get_connection
from core.referentiel import load_referentiel

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Enrichissement — Compass Consumer Voice",
    page_icon="✏",
    layout="wide",
)

inject_css()
theme_toggle()
sidebar_header()

# ── Constantes ────────────────────────────────────────────────────────────────
PAGE_SIZE = 50

# ── Session state ─────────────────────────────────────────────────────────────
_DEFAULTS: dict = {
    "enrich_filters_applied":  False,
    "enrich_brand":            "Tous",
    "enrich_source":           "Tous",
    "enrich_date_from":        None,
    "enrich_date_to":          None,
    "enrich_opinion":          "Tous",
    "enrich_statut":           "Tous",
    "enrich_page":             0,
    "enrich_total":            0,
    "enrich_df_original":      None,   # DataFrame chargé (page courante)
    "enrich_editor_version":   0,      # incrémenté après sauvegarde pour forcer le refresh
    "enrich_save_pending":     False,
    "enrich_save_count":       0,
    "enrich_saved":            False,
    "_pending_changes":        None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── En-tête ───────────────────────────────────────────────────────────────────
page_header(
    title="Enrichissement manuel",
    subtitle="Mettre à jour catégorie, sous-catégorie et photo sur des verbatims",
)

with st.sidebar:
    st.markdown(
        f'<div style="font-size:11px;color:var(--c-sidebar-text);padding:8px 16px">'
        f'Environnement : <strong style="color:var(--c-cyan)">'
        f'{get_active_env().upper()}</strong></div>',
        unsafe_allow_html=True,
    )

# ── Chargement des options de filtre ──────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def _load_filter_options() -> dict:
    """Charge brand et source distincts depuis la base (cache 5 min)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT brand FROM verbatims "
                    "WHERE brand IS NOT NULL ORDER BY brand"
                )
                brands = [r[0] for r in cur.fetchall()]
                cur.execute(
                    "SELECT DISTINCT source FROM verbatims "
                    "WHERE source IS NOT NULL ORDER BY source"
                )
                sources = [r[0] for r in cur.fetchall()]
        return {"brands": brands, "sources": sources}
    except Exception:
        return {"brands": [], "sources": []}


filter_opts = _load_filter_options()

# ── Sidebar — filtres ─────────────────────────────────────────────────────────
with st.sidebar:
    sidebar_section("Filtres")

    brand_options = ["Tous"] + filter_opts["brands"]
    sel_brand = st.selectbox(
        "Marque",
        options=brand_options,
        index=brand_options.index(st.session_state.enrich_brand)
        if st.session_state.enrich_brand in brand_options else 0,
        key="sb_brand",
    )

    source_options = ["Tous"] + filter_opts["sources"]
    sel_source = st.selectbox(
        "Source",
        options=source_options,
        index=source_options.index(st.session_state.enrich_source)
        if st.session_state.enrich_source in source_options else 0,
        key="sb_source",
    )

    sel_date_from = st.date_input(
        "Date de",
        value=st.session_state.enrich_date_from,
        key="sb_date_from",
    )
    sel_date_to = st.date_input(
        "Date à",
        value=st.session_state.enrich_date_to,
        key="sb_date_to",
    )

    opinion_options = ["Tous", "positive", "negative", "neutral"]
    sel_opinion = st.selectbox(
        "Opinion",
        options=opinion_options,
        index=opinion_options.index(st.session_state.enrich_opinion)
        if st.session_state.enrich_opinion in opinion_options else 0,
        key="sb_opinion",
    )

    statut_options = ["Tous", "Avec catégorie", "Sans catégorie"]
    sel_statut = st.selectbox(
        "Statut catégorie",
        options=statut_options,
        index=statut_options.index(st.session_state.enrich_statut)
        if st.session_state.enrich_statut in statut_options else 0,
        key="sb_statut",
    )

    st.markdown("")
    apply_clicked = st.button(
        "Appliquer les filtres",
        type="primary",
        use_container_width=True,
        key="btn_apply_filters",
    )

# Appliquer les filtres → reset page et données
if apply_clicked:
    st.session_state.enrich_brand          = sel_brand
    st.session_state.enrich_source         = sel_source
    st.session_state.enrich_date_from      = sel_date_from if sel_date_from else None
    st.session_state.enrich_date_to        = sel_date_to if sel_date_to else None
    st.session_state.enrich_opinion        = sel_opinion
    st.session_state.enrich_statut         = sel_statut
    st.session_state.enrich_page           = 0
    st.session_state.enrich_df_original    = None
    st.session_state.enrich_filters_applied = True
    st.session_state.enrich_save_pending   = False
    st.session_state.enrich_saved          = False
    st.session_state._pending_changes      = None
    st.rerun()

# Avant le premier "Appliquer les filtres"
if not st.session_state.enrich_filters_applied:
    empty_state(
        "🔍",
        "Appliquez les filtres pour charger les verbatims",
        "Utilisez le panneau latéral et cliquez sur « Appliquer les filtres ».",
    )
    st.stop()

# ── Helpers SQL ───────────────────────────────────────────────────────────────

def _build_where(filters: dict) -> tuple[str, list]:
    """Construit la clause WHERE et les paramètres SQL."""
    clauses: list[str] = []
    params:  list      = []

    if filters["brand"] != "Tous":
        clauses.append("brand = %s")
        params.append(filters["brand"])

    if filters["source"] != "Tous":
        clauses.append("source = %s")
        params.append(filters["source"])

    if filters["date_from"]:
        clauses.append("date >= %s")
        params.append(filters["date_from"])

    if filters["date_to"]:
        clauses.append("date <= %s")
        params.append(filters["date_to"])

    if filters["opinion"] != "Tous":
        clauses.append("opinion = %s")
        params.append(filters["opinion"])

    if filters["statut"] == "Avec catégorie":
        clauses.append("categorie_interne IS NOT NULL")
    elif filters["statut"] == "Sans catégorie":
        clauses.append("categorie_interne IS NULL")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _get_total(conn, filters: dict) -> int:
    where, params = _build_where(filters)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM verbatims {where}", params)
        return cur.fetchone()[0]


def _load_page(conn, filters: dict, page: int) -> pd.DataFrame:
    where, params = _build_where(filters)
    offset        = page * PAGE_SIZE
    sql = f"""
        SELECT id, verbatim_content, product_name,
               categorie_interne, sous_categorie_interne, photo
        FROM verbatims
        {where}
        ORDER BY date DESC, id
        LIMIT %s OFFSET %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params + [PAGE_SIZE, offset])
        rows = cur.fetchall()
    df = pd.DataFrame(
        rows,
        columns=["id", "verbatim_content", "product_name",
                 "categorie_interne", "sous_categorie_interne", "photo"],
    )
    # Colonne d'affichage tronquée (lecture seule dans data_editor)
    df["_verbatim_display"] = df["verbatim_content"].fillna("").str[:120]
    return df


# ── Chargement des données ────────────────────────────────────────────────────
current_filters = {
    "brand":     st.session_state.enrich_brand,
    "source":    st.session_state.enrich_source,
    "date_from": st.session_state.enrich_date_from,
    "date_to":   st.session_state.enrich_date_to,
    "opinion":   st.session_state.enrich_opinion,
    "statut":    st.session_state.enrich_statut,
}

if st.session_state.enrich_df_original is None:
    try:
        with get_connection() as conn:
            total    = _get_total(conn, current_filters)
            df_page  = _load_page(conn, current_filters, st.session_state.enrich_page)
        st.session_state.enrich_total       = total
        st.session_state.enrich_df_original = df_page
    except Exception as exc:
        alert(f"Impossible de charger les verbatims : {exc}", type="error")
        st.stop()

total   = st.session_state.enrich_total
df_orig = st.session_state.enrich_df_original
page    = st.session_state.enrich_page
n_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

# ── État vide ─────────────────────────────────────────────────────────────────
if total == 0:
    empty_state(
        "○",
        "Aucun verbatim ne correspond aux filtres",
        "Essayez d'élargir les critères de recherche.",
    )
    st.stop()

# ── Métriques ─────────────────────────────────────────────────────────────────
metric_row([
    {"label": "Verbatims trouvés", "value": f"{total:,}",             "color": "blue"},
    {"label": "Page",              "value": f"{page + 1} / {n_pages}", "color": "gray"},
    {"label": "Page size",         "value": f"{len(df_orig)}",         "color": "gray"},
])

# ── Référentiel ───────────────────────────────────────────────────────────────
try:
    referentiel = load_referentiel()
    all_cats    = [""] + sorted(referentiel.keys())
    all_sous    = [""] + sorted({sc for scs in referentiel.values() for sc in scs})
except Exception as exc:
    alert(f"Impossible de charger le référentiel : {exc}", type="error")
    st.stop()

# ── Tableau éditable ──────────────────────────────────────────────────────────
st.markdown(f"#### Verbatims — page {page + 1} / {n_pages}")
st.caption(
    "Modifiez les colonnes **Catégorie**, **Sous-catégorie** et **Photo**, "
    "puis cliquez sur **Sauvegarder les modifications**. "
    "Les modifications non sauvegardées sont perdues lors du changement de page."
)

# DataFrame normalisé pour data_editor (pas de None, types corrects)
df_display = df_orig[
    ["_verbatim_display", "product_name",
     "categorie_interne", "sous_categorie_interne", "photo"]
].copy()
df_display["photo"]                  = df_display["photo"].fillna(False).astype(bool)
df_display["categorie_interne"]      = df_display["categorie_interne"].fillna("").astype(str)
df_display["sous_categorie_interne"] = df_display["sous_categorie_interne"].fillna("").astype(str)

_editor_key = (
    f"editor_p{page}_v{st.session_state.enrich_editor_version}"
)

edited_df = st.data_editor(
    df_display,
    key=_editor_key,
    use_container_width=True,
    hide_index=True,
    column_config={
        "_verbatim_display": st.column_config.TextColumn(
            "Verbatim (extrait)",
            disabled=True,
            width="large",
        ),
        "product_name": st.column_config.TextColumn(
            "Produit",
            disabled=True,
            width="medium",
        ),
        "categorie_interne": st.column_config.SelectboxColumn(
            "Catégorie",
            options=all_cats,
            required=False,
            width="medium",
        ),
        "sous_categorie_interne": st.column_config.SelectboxColumn(
            "Sous-catégorie",
            options=all_sous,
            required=False,
            width="medium",
        ),
        "photo": st.column_config.CheckboxColumn(
            "Photo",
            width="small",
        ),
    },
    num_rows="fixed",
)

# ── Pagination ────────────────────────────────────────────────────────────────
if n_pages > 1:
    st.markdown("")
    col_prev, col_lbl, col_next = st.columns([1, 4, 1])

    with col_prev:
        if page > 0 and st.button("← Précédent", use_container_width=True, key="btn_prev"):
            st.session_state.enrich_page           = page - 1
            st.session_state.enrich_df_original    = None
            st.session_state.enrich_save_pending   = False
            st.session_state.enrich_saved          = False
            st.session_state._pending_changes      = None
            st.rerun()

    with col_lbl:
        st.markdown(
            f'<p style="text-align:center;color:var(--c-text-2);margin-top:6px">'
            f'Page {page + 1} sur {n_pages}</p>',
            unsafe_allow_html=True,
        )

    with col_next:
        if page < n_pages - 1 and st.button(
            "Suivant →", use_container_width=True, key="btn_next"
        ):
            st.session_state.enrich_page           = page + 1
            st.session_state.enrich_df_original    = None
            st.session_state.enrich_save_pending   = False
            st.session_state.enrich_saved          = False
            st.session_state._pending_changes      = None
            st.rerun()

st.divider()

# ── Détection des modifications ───────────────────────────────────────────────

def _find_changes(
    original: pd.DataFrame,
    edited:   pd.DataFrame,
    ids:      pd.Series,
) -> list[dict]:
    """Retourne la liste des verbatims effectivement modifiés."""
    changes: list[dict] = []
    for i in range(len(original)):
        orig = original.iloc[i]
        edit = edited.iloc[i]
        if (
            orig["categorie_interne"]      != edit["categorie_interne"]
            or orig["sous_categorie_interne"] != edit["sous_categorie_interne"]
            or orig["photo"]               != edit["photo"]
        ):
            changes.append({
                "id":                     ids.iloc[i],
                "categorie_interne":      edit["categorie_interne"] or None,
                "sous_categorie_interne": edit["sous_categorie_interne"] or None,
                "photo":                  bool(edit["photo"]),
            })
    return changes


changes   = _find_changes(df_display, edited_df, df_orig["id"])
n_changes = len(changes)

# ── Bouton de sauvegarde ──────────────────────────────────────────────────────
col_btn, col_hint = st.columns([2, 5])
with col_btn:
    save_clicked = st.button(
        "Sauvegarder les modifications"
        + (f" ({n_changes})" if n_changes else ""),
        type="primary",
        use_container_width=True,
        disabled=n_changes == 0 or st.session_state.enrich_saved,
        key="btn_save",
    )
with col_hint:
    if n_changes > 0:
        st.caption(
            f"**{n_changes}** ligne(s) modifiée(s) sur cette page. "
            "Seuls les verbatims modifiés seront mis à jour. "
            "La table `categories_mapping` n'est jamais touchée ici."
        )
    else:
        st.caption("Aucune modification détectée sur cette page.")

# Capture des changements au clic
if save_clicked and n_changes > 0:
    st.session_state.enrich_save_pending = True
    st.session_state.enrich_save_count   = n_changes
    st.session_state._pending_changes    = changes

# ── Confirmation ──────────────────────────────────────────────────────────────
if st.session_state.enrich_save_pending and not st.session_state.enrich_saved:
    n_pending = st.session_state.enrich_save_count
    st.markdown("")
    alert(
        f"<strong>{n_pending} verbatim(s)</strong> vont être mis à jour. "
        "Cette opération modifie uniquement la table <code>verbatims</code> — "
        "<code>categories_mapping</code> n'est <strong>pas</strong> modifiée.",
        type="warning",
        title="Confirmer la sauvegarde ?",
    )

    col_confirm, col_cancel, _ = st.columns([1, 1, 4])
    with col_confirm:
        confirmed = st.button(
            "Confirmer",
            type="primary",
            use_container_width=True,
            key="btn_confirm",
        )
    with col_cancel:
        cancelled = st.button(
            "Annuler",
            use_container_width=True,
            key="btn_cancel",
        )

    if confirmed:
        pending = st.session_state._pending_changes or []
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    for chg in pending:
                        cur.execute(
                            """
                            UPDATE verbatims
                               SET categorie_interne      = %s,
                                   sous_categorie_interne = %s,
                                   photo                  = %s
                             WHERE id = %s
                            """,
                            (
                                chg["categorie_interne"],
                                chg["sous_categorie_interne"],
                                chg["photo"],
                                chg["id"],
                            ),
                        )
                conn.commit()

            st.session_state.enrich_save_pending  = False
            st.session_state.enrich_saved         = True
            st.session_state._pending_changes     = None
            # Forcer le rechargement et le refresh du data_editor
            st.session_state.enrich_df_original   = None
            st.session_state.enrich_editor_version += 1
            st.rerun()

        except Exception as exc:
            alert(f"Erreur lors de la sauvegarde : {exc}", type="error")

    if cancelled:
        st.session_state.enrich_save_pending = False
        st.session_state._pending_changes    = None
        st.rerun()

# ── Succès ────────────────────────────────────────────────────────────────────
if st.session_state.enrich_saved:
    n_saved = st.session_state.enrich_save_count
    alert(
        f"<strong>{n_saved} verbatim(s)</strong> mis à jour avec succès. "
        "Vous pouvez continuer à éditer d'autres verbatims.",
        type="success",
        title="Sauvegarde réussie",
    )
    if st.button("Continuer les modifications", key="btn_continue"):
        st.session_state.enrich_saved      = False
        st.session_state.enrich_save_count = 0
        st.rerun()
