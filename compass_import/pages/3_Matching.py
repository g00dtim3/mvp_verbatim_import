"""
pages/3_Matching.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Module 3 — Matching catégories.

Flux :
  1. Affichage de l'état actuel (produits sans catégorie)
  2. Export XLS à compléter dans Excel / LibreOffice
  3. Réimport du XLS complété → validation → application
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
    matching_summary,
    metric_row,
    page_header,
    product_status_table,
    sidebar_header,
    theme_toggle,
)
from core.db import get_active_env, get_connection
from core.hasher import file_hash as compute_file_hash
from core.matcher import (
    apply_matching,
    export_matching_xls,
    get_unmatched_products,
    validate_matching_xls,
)
from core.referentiel import load_referentiel

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Matching — Compass Consumer Voice",
    page_icon="🏷",
    layout="wide",
)

inject_css()
theme_toggle()
sidebar_header()

# ── Session state ─────────────────────────────────────────────────────────────
_DEFAULTS = {
    "match_xls_bytes":       None,   # XLS généré (cache)
    "match_upload_hash":     None,   # hash du dernier XLS uploadé
    "match_validation":      None,   # dict {valid, errors}
    "match_applied":         False,
    "match_result":          None,   # dict {products_matched, verbatims_updated}
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── En-tête ───────────────────────────────────────────────────────────────────
page_header(
    title="Matching catégories",
    subtitle="Assigner catégorie, sous-catégorie et photo aux produits non traités",
)

with st.sidebar:
    st.markdown(
        f'<div style="font-size:11px;color:var(--c-sidebar-text);padding:8px 16px">'
        f'Environnement : <strong style="color:var(--c-cyan)">'
        f'{get_active_env().upper()}</strong></div>',
        unsafe_allow_html=True,
    )

# ── Chargement des données ────────────────────────────────────────────────────
try:
    with get_connection() as conn:
        unmatched_products = get_unmatched_products(conn)
except Exception as exc:
    alert(f"Impossible de charger les produits depuis la base : {exc}", type="error")
    st.stop()

try:
    referentiel = load_referentiel()
except Exception as exc:
    alert(f"Impossible de charger le référentiel des catégories : {exc}", type="error")
    st.stop()

# ═══════════════════════════════════════════════════════════════
# SECTION 1 — État actuel
# ═══════════════════════════════════════════════════════════════

st.markdown("### État actuel")

if not unmatched_products:
    empty_state(
        "✓",
        "Tous les produits sont matchés",
        "Aucun verbatim ne manque de catégorie.",
    )
else:
    total_verbatims = sum(p["nb_verbatims"] for p in unmatched_products)
    metric_row([
        {
            "label": "Produits sans catégorie",
            "value": len(unmatched_products),
            "color": "warning",
        },
        {
            "label": "Verbatims impactés",
            "value": f"{total_verbatims:,}",
            "color": "error",
        },
    ])
    product_status_table(unmatched_products)

st.divider()

# ═══════════════════════════════════════════════════════════════
# SECTION 2 — Export XLS
# ═══════════════════════════════════════════════════════════════

st.markdown("### 1 — Télécharger le fichier de matching")

if not unmatched_products:
    st.caption(
        "Aucun produit à matcher. Le fichier de matching ne sera généré "
        "qu'après un import mensuel comportant des produits inconnus."
    )
else:
    # Générer le XLS et le mettre en cache (éviter de le régénérer à chaque rerun)
    if st.session_state.match_xls_bytes is None:
        with st.spinner("Génération du fichier XLS…"):
            try:
                xls_bytes = export_matching_xls(unmatched_products, referentiel)
                st.session_state.match_xls_bytes = xls_bytes
            except Exception as exc:
                alert(f"Erreur lors de la génération du XLS : {exc}", type="error")
                xls_bytes = None
    else:
        xls_bytes = st.session_state.match_xls_bytes

    if xls_bytes:
        st.markdown(
            "Téléchargez le fichier, complétez les colonnes "
            "**catégorie**, **sous-catégorie** et **photo** dans Excel "
            "ou LibreOffice, puis réimportez-le ci-dessous."
        )
        st.download_button(
            label="📥 Télécharger le fichier de matching",
            data=xls_bytes,
            file_name="matching_categories.xlsx",
            mime=(
                "application/vnd.openxmlformats-officedocument"
                ".spreadsheetml.sheet"
            ),
            use_container_width=False,
        )

st.divider()

# ═══════════════════════════════════════════════════════════════
# SECTION 3 — Réimport XLS complété
# ═══════════════════════════════════════════════════════════════

st.markdown("### 2 — Importer le fichier complété")

uploaded_xls = st.file_uploader(
    "Déposer le fichier XLS complété",
    type=["xlsx"],
    help="Importez uniquement le fichier exporté par cette application, "
         "après l'avoir complété dans Excel ou LibreOffice.",
    key="xls_uploader",
)

if uploaded_xls is not None:
    xls_bytes_uploaded = uploaded_xls.read()
    xls_hash = compute_file_hash(xls_bytes_uploaded)

    # Nouveau fichier → reset validation
    if xls_hash != st.session_state.match_upload_hash:
        st.session_state.match_upload_hash = xls_hash
        st.session_state.match_validation  = None
        st.session_state.match_applied     = False
        st.session_state.match_result      = None

    # ── Validation ────────────────────────────────────────────────────────────
    if st.session_state.match_validation is None:
        original_keys = {p["brand"] + p["product_name"] for p in unmatched_products}
        try:
            validation = validate_matching_xls(
                xls_bytes_uploaded, referentiel, original_keys
            )
            st.session_state.match_validation = validation
        except ValueError as exc:
            alert(str(exc), type="error", title="Fichier invalide")
            st.stop()
        except Exception as exc:
            alert(
                f"Erreur inattendue lors de la validation : {exc}", type="error"
            )
            st.stop()

    validation = st.session_state.match_validation
    if validation is None:
        st.stop()

    n_valid  = len(validation["valid"])
    n_errors = len(validation["errors"])

    # ── Résumé validation ──────────────────────────────────────────────────────
    metric_row([
        {
            "label": "Lignes valides",
            "value": n_valid,
            "color": "success" if n_valid > 0 else "gray",
        },
        {
            "label": "Lignes rejetées",
            "value": n_errors,
            "color": "error" if n_errors > 0 else "gray",
        },
    ])

    # ── Détail des erreurs ─────────────────────────────────────────────────────
    if n_errors > 0:
        with st.expander(
            f"⚠ {n_errors} ligne(s) rejetée(s) — voir le détail", expanded=True
        ):
            df_errors = pd.DataFrame(validation["errors"])[
                ["ligne", "key_brandxpdt", "colonne", "valeur", "raison"]
            ]
            st.dataframe(df_errors, use_container_width=True, hide_index=True)

        if n_valid == 0:
            alert(
                "Aucune ligne valide à appliquer. "
                "Corrigez les erreurs dans le fichier XLS et réimportez-le.",
                type="warning",
            )
            st.stop()
        else:
            alert(
                f"<strong>{n_valid}</strong> ligne(s) valide(s) seront appliquées. "
                f"<strong>{n_errors}</strong> ligne(s) rejetée(s) ne seront pas traitées.",
                type="warning",
            )

    # ── Aperçu des lignes valides ──────────────────────────────────────────────
    if n_valid > 0 and not st.session_state.match_applied:
        with st.expander(f"Aperçu des {n_valid} ligne(s) valide(s)"):
            df_valid = pd.DataFrame(validation["valid"])
            st.dataframe(df_valid, use_container_width=True, hide_index=True)

    # ── Bouton d'application ───────────────────────────────────────────────────
    if n_valid > 0 and not st.session_state.match_applied:
        st.markdown("")
        col_btn, col_info = st.columns([2, 5])
        with col_btn:
            apply_clicked = st.button(
                f"Appliquer le matching ({n_valid} produit(s))",
                type="primary",
                use_container_width=True,
                key="apply_matching_btn",
            )
        with col_info:
            st.caption(
                "Cette action mettra à jour `categories_mapping` et propagera "
                "les catégories sur **tous** les verbatims des produits matchés."
            )

        if apply_clicked:
            with st.spinner("Application du matching en cours…"):
                try:
                    with get_connection() as conn:
                        result = apply_matching(conn, validation["valid"])
                    st.session_state.match_applied = True
                    st.session_state.match_result  = result
                    # Invalider le cache XLS (les produits matchés ne doivent
                    # plus apparaître dans le prochain export)
                    st.session_state.match_xls_bytes = None
                    st.rerun()
                except Exception as exc:
                    alert(
                        f"Erreur lors de l'application du matching : {exc}",
                        type="error",
                    )

    # ── Résultat post-application ──────────────────────────────────────────────
    if st.session_state.match_applied and st.session_state.match_result:
        result = st.session_state.match_result
        matching_summary(
            products_matched=result["products_matched"],
            verbatims_updated=result["verbatims_updated"],
        )
        alert(
            f"<strong>{result['products_matched']}</strong> produit(s) matchés — "
            f"<strong>{result['verbatims_updated']:,}</strong> verbatim(s) mis à jour.",
            type="success",
            title="Matching appliqué avec succès",
        )

        st.markdown("")
        if st.button("Nouveau matching", key="reset_matching"):
            for k, v in _DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()
