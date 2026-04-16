"""
pages/4_Outils.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Module 4 — Maintenance et consultation.

4 onglets :
  Tab 1 — Table de correspondance (categories_mapping paginée + export CSV)
  Tab 2 — Renommer un produit (UPDATE en cascade verbatims + categories_mapping)
  Tab 3 — Vérification des produits (badges matchés / à compléter)
  Tab 4 — Logs d'import (tableau + expandeurs + export CSV)
"""

import io
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
    log_table,
    metric_row,
    page_header,
    product_status_table,
    sidebar_header,
    theme_toggle,
)
from core.db import get_active_env, get_connection

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Outils — Compass Consumer Voice",
    page_icon="🔧",
    layout="wide",
)

inject_css()
theme_toggle()
sidebar_header()

# ── En-tête ───────────────────────────────────────────────────────────────────
page_header(
    title="Outils de maintenance",
    subtitle="Référentiel, renommage, vérification et logs d'import",
)

with st.sidebar:
    st.markdown(
        f'<div style="font-size:11px;color:var(--c-sidebar-text);padding:8px 16px">'
        f'Environnement : <strong style="color:var(--c-cyan)">'
        f'{get_active_env().upper()}</strong></div>',
        unsafe_allow_html=True,
    )

# ── Session state ─────────────────────────────────────────────────────────────
_DEFAULTS: dict = {
    # Tab 1
    "t1_data":         None,   # DataFrame categories_mapping
    "t1_filter_sig":   None,   # (search, cat) — pour détecter un changement de filtre
    "t1_page":         0,
    # Tab 2
    "t2_products":     None,   # dict product_name → {nb_verbatims, categorie, sous_cat}
    "t2_confirm":      False,
    "t2_success":      None,   # str message de succès
    # Tab 3
    "t3_data":         None,
    # Tab 4
    "t4_data":         None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📋 Table de correspondance",
    "✏ Renommer un produit",
    "✓ Vérification produits",
    "📊 Logs d'import",
])

# ═══════════════════════════════════════════════════════════════
# TAB 1 — Table de correspondance
# ═══════════════════════════════════════════════════════════════

with tab1:
    st.markdown("#### Table de correspondance `categories_mapping`")

    # ── Chargement (cache session) ────────────────────────────────────────────
    if st.session_state.t1_data is None:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT key_brandxpdt, brand, product_name,
                               categorie_interne, sous_categorie_interne,
                               photo, matched_at
                          FROM categories_mapping
                         ORDER BY matched_at DESC NULLS LAST
                    """)
                    rows = cur.fetchall()
            st.session_state.t1_data = pd.DataFrame(rows, columns=[
                "key_brandxpdt", "brand", "product_name",
                "categorie_interne", "sous_categorie_interne",
                "photo", "matched_at",
            ])
        except Exception as exc:
            alert(f"Impossible de charger la table de correspondance : {exc}", type="error")
            st.stop()

    df_all = st.session_state.t1_data

    if df_all is None or df_all.empty:
        empty_state(
            "📋",
            "Table de correspondance vide",
            "Utilisez le module Matching catégories pour ajouter des correspondances.",
        )
    else:
        # ── Filtres ───────────────────────────────────────────────────────────
        col_s, col_b, col_c = st.columns([3, 2, 2])
        with col_s:
            search = st.text_input(
                "Recherche produit",
                placeholder="Filtrer par nom de produit…",
                key="t1_search",
            )
        with col_b:
            brand_options = ["Toutes"] + sorted(
                df_all["brand"].dropna().unique().tolist()
            )
            brand_filter = st.selectbox("Marque", options=brand_options, key="t1_brand")
        with col_c:
            cat_options = ["Toutes"] + sorted(
                df_all["categorie_interne"].dropna().unique().tolist()
            )
            cat_filter = st.selectbox(
                "Catégorie",
                options=cat_options,
                key="t1_cat",
            )

        # Détecter changement de filtre → reset page
        filter_sig = (search, brand_filter, cat_filter)
        if filter_sig != st.session_state.t1_filter_sig:
            st.session_state.t1_filter_sig = filter_sig
            st.session_state.t1_page       = 0

        # ── Filtrage en mémoire ───────────────────────────────────────────────
        df_f = df_all.copy()
        if search:
            df_f = df_f[df_f["product_name"].str.contains(search, case=False, na=False)]
        if brand_filter != "Toutes":
            df_f = df_f[df_f["brand"] == brand_filter]
        if cat_filter != "Toutes":
            df_f = df_f[df_f["categorie_interne"] == cat_filter]

        PAGE_T1  = 25
        total_t1 = len(df_f)
        n_p_t1   = max(1, (total_t1 + PAGE_T1 - 1) // PAGE_T1)
        pg       = min(st.session_state.t1_page, n_p_t1 - 1)

        metric_row([
            {"label": "Correspondances totales", "value": f"{len(df_all):,}", "color": "blue"},
            {"label": "Résultats filtrés",        "value": f"{total_t1:,}",   "color": "cyan"},
            {"label": "Page",                     "value": f"{pg + 1} / {n_p_t1}", "color": "gray"},
        ])

        # ── Tableau ───────────────────────────────────────────────────────────
        df_page = df_f.iloc[pg * PAGE_T1 : (pg + 1) * PAGE_T1].copy()
        df_page["matched_at"] = df_page["matched_at"].astype(str).str[:16].replace("NaT", "—")
        df_page["photo"]      = df_page["photo"].map(
            {True: "oui", False: "non"}
        ).fillna("—")

        st.dataframe(
            df_page,
            use_container_width=True,
            hide_index=True,
            column_config={
                "key_brandxpdt":          st.column_config.TextColumn("Clé", width="medium"),
                "brand":                  st.column_config.TextColumn("Marque", width="small"),
                "product_name":           st.column_config.TextColumn("Produit"),
                "categorie_interne":      st.column_config.TextColumn("Catégorie"),
                "sous_categorie_interne": st.column_config.TextColumn("Sous-catégorie"),
                "photo":                  st.column_config.TextColumn("Photo", width="small"),
                "matched_at":             st.column_config.TextColumn("Matchée le"),
            },
        )

        # ── Pagination ────────────────────────────────────────────────────────
        if n_p_t1 > 1:
            col_prev, col_lbl, col_nxt = st.columns([1, 4, 1])
            with col_prev:
                if pg > 0 and st.button("← Précédent", key="t1_prev"):
                    st.session_state.t1_page = pg - 1
                    st.rerun()
            with col_lbl:
                st.markdown(
                    f'<p style="text-align:center;color:var(--c-text-2);margin-top:6px">'
                    f'Page {pg + 1} / {n_p_t1}</p>',
                    unsafe_allow_html=True,
                )
            with col_nxt:
                if pg < n_p_t1 - 1 and st.button("Suivant →", key="t1_next"):
                    st.session_state.t1_page = pg + 1
                    st.rerun()

        st.markdown("")

        # ── Actions ───────────────────────────────────────────────────────────
        col_dl, col_rf = st.columns([2, 1])
        with col_dl:
            csv_buf = io.StringIO()
            df_f.assign(
                photo=df_f["photo"].map({True: "oui", False: "non"}).fillna("—"),
                matched_at=df_f["matched_at"].astype(str).str[:16],
            ).to_csv(csv_buf, index=False, encoding="utf-8-sig")
            st.download_button(
                label="📥 Exporter CSV",
                data=csv_buf.getvalue().encode("utf-8-sig"),
                file_name="categories_mapping_export.csv",
                mime="text/csv",
                key="t1_export",
            )
        with col_rf:
            if st.button("↺ Rafraîchir", key="t1_refresh", use_container_width=True):
                st.session_state.t1_data = None
                st.rerun()


# ═══════════════════════════════════════════════════════════════
# TAB 2 — Renommer un produit
# ═══════════════════════════════════════════════════════════════

with tab2:
    st.markdown("#### Renommer un produit")
    st.caption(
        "Renomme un produit en cascade : tous ses verbatims et son entrée dans "
        "`categories_mapping` sont mis à jour atomiquement."
    )

    # ── Chargement liste produits ─────────────────────────────────────────────
    if st.session_state.t2_products is None:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT v.brand,
                               v.product_name,
                               COUNT(*) AS nb_verbatims,
                               cm.categorie_interne,
                               cm.sous_categorie_interne
                          FROM verbatims v
                          LEFT JOIN categories_mapping cm
                                 ON v.brand = cm.brand AND v.product_name = cm.product_name
                         GROUP BY v.brand, v.product_name,
                                  cm.categorie_interne, cm.sous_categorie_interne
                         ORDER BY v.brand, v.product_name
                    """)
                    rows = cur.fetchall()
            # key = brand + "||" + product_name  (affichage et lookup sans ambiguïté)
            st.session_state.t2_products = {
                r[0] + "||" + r[1]: {
                    "brand":        r[0],
                    "product_name": r[1],
                    "nb_verbatims": r[2],
                    "categorie":    r[3] or "—",
                    "sous_cat":     r[4] or "—",
                }
                for r in rows
            }
        except Exception as exc:
            alert(f"Impossible de charger les produits : {exc}", type="error")
            st.stop()

    products_map = st.session_state.t2_products or {}

    if not products_map:
        empty_state("○", "Aucun produit en base", "Importez d'abord un fichier CSV.")
    else:
        # ── Sélection du produit ──────────────────────────────────────────────
        selected_key = st.selectbox(
            "Produit à renommer",
            options=list(products_map.keys()),
            index=None,
            placeholder="Rechercher une marque || produit…",
            key="t2_select",
        )

        if selected_key:
            info = products_map[selected_key]
            old_brand   = info["brand"]
            old_pname   = info["product_name"]
            old_cm_key  = old_brand + old_pname

            metric_row([
                {"label": "Marque",              "value": old_brand,                   "color": "cyan"},
                {"label": "Verbatims concernés", "value": f"{info['nb_verbatims']:,}", "color": "warning"},
                {"label": "Catégorie actuelle",  "value": info["categorie"],            "color": "blue"},
                {"label": "Sous-catégorie",       "value": info["sous_cat"],             "color": "gray"},
            ])

            new_name = st.text_input(
                "Nouveau nom du produit",
                placeholder="Saisir le nouveau nom exact…",
                key="t2_new_name",
            )

            new_name_clean = new_name.strip()
            rename_ready   = bool(new_name_clean and new_name_clean != old_pname)

            if new_name_clean and new_name_clean == old_pname:
                alert("Le nouveau nom est identique à l'ancien.", type="warning")

            if rename_ready:
                alert(
                    f"Cette action mettra à jour "
                    f"<strong>{info['nb_verbatims']:,} verbatim(s)</strong> "
                    f"et la table de correspondance (<code>categories_mapping</code>). "
                    "L'opération est irréversible.",
                    type="warning",
                    title=f"Renommer « {old_pname} » → « {new_name_clean} » (marque : {old_brand})",
                )

                if not st.session_state.t2_confirm:
                    if st.button(
                        "Confirmer le renommage",
                        type="primary",
                        key="t2_confirm_btn",
                    ):
                        st.session_state.t2_confirm = True
                        st.rerun()

                if st.session_state.t2_confirm:
                    col_yes, col_no, _ = st.columns([1, 1, 4])
                    with col_yes:
                        if st.button(
                            "Oui, renommer",
                            type="primary",
                            use_container_width=True,
                            key="t2_yes",
                        ):
                            try:
                                new_cm_key = old_brand + new_name_clean
                                with get_connection() as conn:
                                    with conn.cursor() as cur:
                                        cur.execute(
                                            "UPDATE verbatims "
                                            "SET product_name = %s "
                                            "WHERE brand = %s AND product_name = %s",
                                            (new_name_clean, old_brand, old_pname),
                                        )
                                        n_updated = cur.rowcount
                                        cur.execute(
                                            "UPDATE categories_mapping "
                                            "SET product_name = %s, key_brandxpdt = %s "
                                            "WHERE key_brandxpdt = %s",
                                            (new_name_clean, new_cm_key, old_cm_key),
                                        )
                                    conn.commit()

                                # Invalider les caches affectés
                                st.session_state.t2_products = None
                                st.session_state.t1_data     = None
                                st.session_state.t3_data     = None
                                st.session_state.t2_confirm  = False
                                st.session_state.t2_success  = (
                                    f"Produit <strong>{old_pname}</strong> renommé en "
                                    f"<strong>{new_name_clean}</strong> "
                                    f"(marque : {old_brand}) — "
                                    f"{n_updated:,} verbatim(s) mis à jour."
                                )
                                st.rerun()

                            except Exception as exc:
                                st.session_state.t2_confirm = False
                                alert(f"Erreur lors du renommage : {exc}", type="error")

                    with col_no:
                        if st.button("Annuler", use_container_width=True, key="t2_no"):
                            st.session_state.t2_confirm = False
                            st.rerun()

        # ── Message de succès ─────────────────────────────────────────────────
        if st.session_state.t2_success:
            alert(
                st.session_state.t2_success,
                type="success",
                title="Renommage réussi",
            )
            if st.button("Nouveau renommage", key="t2_reset"):
                st.session_state.t2_success = None
                st.rerun()


# ═══════════════════════════════════════════════════════════════
# TAB 3 — Vérification produits
# ═══════════════════════════════════════════════════════════════

with tab3:
    st.markdown("#### Vérification des noms produits")
    st.caption(
        "Tous les produits en base, triés par volume de verbatims. "
        "Un badge indique si la catégorie est connue."
    )

    # ── Chargement ────────────────────────────────────────────────────────────
    if st.session_state.t3_data is None:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT v.product_name,
                               COUNT(*) AS nb_verbatims,
                               cm.categorie_interne,
                               cm.sous_categorie_interne,
                               cm.photo
                          FROM verbatims v
                          LEFT JOIN categories_mapping cm
                                 ON v.brand = cm.brand AND v.product_name = cm.product_name
                         GROUP BY v.product_name, cm.categorie_interne,
                                  cm.sous_categorie_interne, cm.photo
                         ORDER BY nb_verbatims DESC
                    """)
                    rows = cur.fetchall()
            st.session_state.t3_data = [
                {
                    "product_name":           r[0],
                    "nb_verbatims":           r[1],
                    "categorie_interne":      r[2],
                    "sous_categorie_interne": r[3],
                    "photo":                  r[4],
                }
                for r in rows
            ]
        except Exception as exc:
            alert(f"Impossible de charger les produits : {exc}", type="error")
            st.stop()

    products = st.session_state.t3_data or []

    if not products:
        empty_state("○", "Aucun produit en base", "Importez d'abord un fichier CSV.")
    else:
        n_matched   = sum(1 for p in products if p["categorie_interne"])
        n_unmatched = len(products) - n_matched

        metric_row([
            {"label": "Produits matchés",     "value": n_matched,   "color": "success"},
            {"label": "À compléter",          "value": n_unmatched, "color": "warning" if n_unmatched else "gray"},
            {"label": "Total produits",        "value": len(products), "color": "blue"},
        ])

        if n_unmatched > 0:
            st.markdown("")
            if st.button(
                f"Aller au Matching catégories ({n_unmatched} produit(s) à traiter) →",
                type="primary",
                key="t3_goto_matching",
            ):
                st.switch_page("pages/3_Matching.py")

        st.markdown("")
        product_status_table(products)

        st.markdown("")
        if st.button("↺ Rafraîchir", key="t3_refresh"):
            st.session_state.t3_data = None
            st.rerun()


# ═══════════════════════════════════════════════════════════════
# TAB 4 — Logs d'import
# ═══════════════════════════════════════════════════════════════

with tab4:
    st.markdown("#### Logs d'import")

    # ── Chargement ────────────────────────────────────────────────────────────
    if st.session_state.t4_data is None:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT id,
                               started_at,
                               finished_at,
                               filename,
                               import_type,
                               rows_total,
                               rows_inserted,
                               rows_skipped,
                               rows_matched,
                               rows_unmatched,
                               status,
                               error_detail,
                               EXTRACT(EPOCH FROM (finished_at - started_at))::int
                                   AS duration_s
                          FROM import_logs
                         ORDER BY started_at DESC
                    """)
                    rows = cur.fetchall()
            st.session_state.t4_data = [
                {
                    "id":             str(r[0]),
                    "started_at":     r[1].strftime("%d/%m/%Y %H:%M") if r[1] else "—",
                    "filename":       r[3] or "—",
                    "import_type":    r[4] or "—",
                    "rows_total":     r[5] or 0,
                    "rows_inserted":  r[6] or 0,
                    "rows_skipped":   r[7] or 0,
                    "rows_matched":   r[8] or 0,
                    "rows_unmatched": r[9] or 0,
                    "status":         r[10] or "—",
                    "error_detail":   r[11],
                    "duration_s":     r[12] or 0,
                }
                for r in rows
            ]
        except Exception as exc:
            alert(f"Impossible de charger les logs : {exc}", type="error")
            st.stop()

    all_logs = st.session_state.t4_data or []

    # ── Filtre statut ─────────────────────────────────────────────────────────
    col_filt, col_rf = st.columns([3, 1])
    with col_filt:
        status_opts = ["Tous", "success", "partial", "error", "duplicate", "running"]
        sel_status  = st.selectbox(
            "Filtre statut",
            options=status_opts,
            key="t4_status",
        )
    with col_rf:
        st.markdown("<div style='margin-top:28px'></div>", unsafe_allow_html=True)
        if st.button("↺ Rafraîchir", key="t4_refresh", use_container_width=True):
            st.session_state.t4_data = None
            st.rerun()

    # ── Filtrage ──────────────────────────────────────────────────────────────
    filtered = (
        all_logs if sel_status == "Tous"
        else [lg for lg in all_logs if lg["status"] == sel_status]
    )

    if not filtered:
        if sel_status != "Tous":
            st.info(f"Aucun log avec le statut « {sel_status} ».")
        else:
            empty_state(
                "📋",
                "Aucun log d'import",
                "Les logs apparaîtront ici après le premier import.",
            )
    else:
        metric_row([
            {"label": "Logs affichés", "value": len(filtered),  "color": "blue"},
            {"label": "Total logs",    "value": len(all_logs),   "color": "gray"},
        ])

        # ── Tableau principal ─────────────────────────────────────────────────
        log_table(filtered)

        # ── Expandeurs détail ─────────────────────────────────────────────────
        st.markdown("##### Détail par import")
        for lg in filtered:
            has_err = bool(lg.get("error_detail"))
            icon    = {"success": "✓", "partial": "⚠", "error": "✕",
                       "duplicate": "⊘", "running": "↻"}.get(lg["status"], "○")
            label   = (
                f"{icon} [{lg['started_at']}] {lg['filename']}"
                + (" — voir erreurs" if has_err else "")
            )
            with st.expander(label, expanded=False):
                ca, cb, cc = st.columns(3)
                with ca:
                    st.markdown(f"**Batch ID :** `{lg['id'][:8]}…`")
                    st.markdown(f"**Type :** {lg['import_type']}")
                    st.markdown(f"**Durée :** {lg['duration_s']}s")
                with cb:
                    st.markdown(f"**Total lignes :** {lg['rows_total']:,}")
                    st.markdown(f"**Insérés :** {lg['rows_inserted']:,}")
                    st.markdown(f"**Skipped :** {lg['rows_skipped']:,}")
                with cc:
                    st.markdown(f"**Matchés :** {lg['rows_matched']:,}")
                    st.markdown(f"**Sans catégorie :** {lg['rows_unmatched']:,}")
                if has_err:
                    st.markdown("**Détail erreurs :**")
                    st.code(lg["error_detail"], language=None)

        st.markdown("")

        # ── Export CSV ────────────────────────────────────────────────────────
        df_logs = pd.DataFrame([
            {
                "date":           lg["started_at"],
                "fichier":        lg["filename"],
                "type":           lg["import_type"],
                "inseres":        lg["rows_inserted"],
                "skippes":        lg["rows_skipped"],
                "matches":        lg["rows_matched"],
                "sans_categorie": lg["rows_unmatched"],
                "statut":         lg["status"],
                "duree_s":        lg["duration_s"],
                "batch_id":       lg["id"],
            }
            for lg in filtered
        ])
        csv_logs = io.StringIO()
        df_logs.to_csv(csv_logs, index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 Exporter logs CSV",
            data=csv_logs.getvalue().encode("utf-8-sig"),
            file_name="import_logs_export.csv",
            mime="text/csv",
            key="t4_export",
        )
