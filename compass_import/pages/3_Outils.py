"""
pages/3_Outils.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Module 3 — Maintenance et consultation.

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

from charte import appliquer_charte, libelle_statut
from compass_ui.compass_ui import (
    alert,
    empty_state,
    log_table,
    metric_row,
    nombre,
    page_header,
    product_status_table,
)
from compass_ui.skip_table import render_skip_details
from core.db import get_active_env, get_connection
from auth import exiger_mot_de_passe
from core.skip_report import parse_error_detail

# ── Résumé compact des erreurs (export CSV) ───────────────────────────────────

def _error_summary(error_detail) -> str:
    """
    Résumé texte de ``error_detail``, une ligne par log — ex.
    ``"DATE_INVALIDE:12;VERBATIM_VIDE:3;lots_en_erreur:1"``.

    Sans ça, l'export CSV principal (une ligne par import) ne dit rien de
    LA NATURE des erreurs : ``ignorees``/``doublons`` donnent des comptes,
    mais pour savoir QUOI a été rejeté il fallait ouvrir chaque log un par
    un dans l'interface. Ce résumé rend l'export auto-suffisant pour un
    premier tri, sans remplacer le détail complet (téléchargeable ligne
    par ligne depuis chaque expander, cf. render_skip_details).
    """
    parsed = parse_error_detail(error_detail)
    parts = [f"{code}:{count}" for code, count in sorted(
        parsed["skips_par_code"].items(), key=lambda kv: -kv[1]
    )]
    if parsed["batch_errors"]:
        parts.append(f"lots_en_erreur:{len(parsed['batch_errors'])}")
    if not parts and parsed["is_legacy"] and parsed["legacy_text"]:
        return "format_legacy"
    return ";".join(parts)


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Outils — Compass Consumer Voice",
    page_icon=":material/build:",
    layout="wide",
)
exiger_mot_de_passe()  # la page s'arrête ici tant que le mot de passe n'est pas saisi

appliquer_charte()

# ── En-tête ───────────────────────────────────────────────────────────────────
page_header(
    title="Outils de maintenance",
    subtitle="Référentiel, renommage, vérification et logs d'import",
)

st.sidebar.caption(f"Environnement : **{get_active_env().upper()}**")

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
    ":material/table: Table de correspondance",
    ":material/edit: Renommer un produit",
    ":material/fact_check: Vérification des produits",
    ":material/history: Logs d'import",
])

# ═══════════════════════════════════════════════════════════════
# TAB 1 — Table de correspondance
# ═══════════════════════════════════════════════════════════════

with tab1:
    st.header("Table de correspondance", anchor=False)
    st.caption("Contenu de la table `categories_mapping`.")

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
            "",
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
            {"label": "Correspondances totales", "value": len(df_all)},
            {"label": "Résultats filtrés",       "value": total_t1},
            {"label": "Page",                    "value": f"{pg + 1} / {n_p_t1}"},
        ])

        # ── Tableau ───────────────────────────────────────────────────────────
        df_page = df_f.iloc[pg * PAGE_T1 : (pg + 1) * PAGE_T1].copy()
        df_page["matched_at"] = pd.to_datetime(df_page["matched_at"], errors="coerce")
        df_page["photo"]      = df_page["photo"].map(
            {True: "oui", False: "non"}
        ).fillna("—")

        st.dataframe(
            df_page,
            hide_index=True,
            column_config={
                "key_brandxpdt":          st.column_config.TextColumn("Clé", width="medium"),
                "brand":                  st.column_config.TextColumn("Marque", width="small"),
                "product_name":           st.column_config.TextColumn("Produit"),
                "categorie_interne":      st.column_config.TextColumn("Catégorie"),
                "sous_categorie_interne": st.column_config.TextColumn("Sous-catégorie"),
                "photo":                  st.column_config.TextColumn("Photo", width="small"),
                "matched_at":             st.column_config.DatetimeColumn(
                    "Matchée le", format="DD/MM/YYYY HH:mm"
                ),
            },
        )

        # ── Pagination ────────────────────────────────────────────────────────
        if n_p_t1 > 1:
            with st.container(horizontal=True, vertical_alignment="center"):
                if pg > 0 and st.button(
                    "Page précédente", icon=":material/arrow_back:", key="t1_prev"
                ):
                    st.session_state.t1_page = pg - 1
                    st.rerun()
                st.caption(f"Page {pg + 1} / {n_p_t1}")
                if pg < n_p_t1 - 1 and st.button(
                    "Page suivante", icon=":material/arrow_forward:", key="t1_next"
                ):
                    st.session_state.t1_page = pg + 1
                    st.rerun()

        # ── Actions ───────────────────────────────────────────────────────────
        with st.container(horizontal=True):
            csv_buf = io.StringIO()
            df_f.assign(
                photo=df_f["photo"].map({True: "oui", False: "non"}).fillna("—"),
                matched_at=df_f["matched_at"].astype(str).str[:16],
            ).to_csv(csv_buf, index=False, encoding="utf-8-sig")
            st.download_button(
                label="Exporter en CSV",
                icon=":material/download:",
                data=csv_buf.getvalue().encode("utf-8-sig"),
                file_name="categories_mapping_export.csv",
                mime="text/csv",
                key="t1_export",
            )
            if st.button("Rafraîchir la table", icon=":material/refresh:", key="t1_refresh"):
                st.session_state.t1_data = None
                st.rerun()


# ═══════════════════════════════════════════════════════════════
# TAB 2 — Renommer un produit
# ═══════════════════════════════════════════════════════════════

with tab2:
    st.header("Renommer un produit", anchor=False)
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
        empty_state("", "Aucun produit en base", "Importez d'abord un fichier CSV.")
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
                {"label": "Marque",              "value": old_brand},
                {"label": "Verbatims concernés", "value": info["nb_verbatims"]},
                {"label": "Catégorie actuelle",  "value": info["categorie"]},
                {"label": "Sous-catégorie",      "value": info["sous_cat"]},
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
                    f"**{nombre(info['nb_verbatims'])} verbatim(s)** "
                    f"et la table de correspondance (`categories_mapping`). "
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
                    with st.container(horizontal=True):
                        if st.button(
                            "Renommer le produit",
                            type="primary",
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
                                    f"Produit **{old_pname}** renommé en "
                                    f"**{new_name_clean}** "
                                    f"(marque : {old_brand}) — "
                                    f"{nombre(n_updated)} verbatim(s) mis à jour."
                                )
                                st.rerun()

                            except Exception as exc:
                                st.session_state.t2_confirm = False
                                alert(f"Erreur lors du renommage : {exc}", type="error")

                        if st.button("Annuler le renommage", key="t2_no"):
                            st.session_state.t2_confirm = False
                            st.rerun()

        # ── Message de succès ─────────────────────────────────────────────────
        if st.session_state.t2_success:
            alert(
                st.session_state.t2_success,
                type="success",
                title="Renommage réussi",
            )
            if st.button("Renommer un autre produit", key="t2_reset"):
                st.session_state.t2_success = None
                st.rerun()


# ═══════════════════════════════════════════════════════════════
# TAB 3 — Vérification produits
# ═══════════════════════════════════════════════════════════════

with tab3:
    st.header("Vérification des noms de produits", anchor=False)
    st.caption(
        "Tous les produits en base, triés par volume de verbatims. "
        "La colonne Statut indique si la catégorie est connue."
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
        empty_state("", "Aucun produit en base", "Importez d'abord un fichier CSV.")
    else:
        n_matched   = sum(1 for p in products if p["categorie_interne"])
        n_unmatched = len(products) - n_matched

        metric_row([
            {"label": "Produits matchés", "value": n_matched},
            {"label": "À compléter",      "value": n_unmatched},
            {"label": "Total produits",   "value": len(products)},
        ])

        if n_unmatched > 0:
            if st.button(
                f"Ouvrir le matching des catégories ({nombre(n_unmatched)} produit(s) à traiter)",
                type="primary",
                icon=":material/arrow_forward:",
                key="t3_goto_matching",
            ):
                st.switch_page("pages/2_Matching.py")

        product_status_table(products)

        if st.button("Rafraîchir la liste", icon=":material/refresh:", key="t3_refresh"):
            st.session_state.t3_data = None
            st.rerun()


# ═══════════════════════════════════════════════════════════════
# TAB 4 — Logs d'import
# ═══════════════════════════════════════════════════════════════

_T4_EXPECTED_KEYS = {
    "id", "started_at", "filename", "import_type", "rows_total",
    "rows_inserted", "rows_skipped", "rows_duplicates", "rows_matched",
    "rows_unmatched", "status", "error_detail", "error_summary",
    "duration_s", "data_purged",
}


def _is_cache_stale(cached, expected_keys: set) -> bool:
    """
    True si ``cached`` (la liste de dicts de ``t4_data``) existe mais ne
    contient pas toutes les clés que le code actuel attend.

    Un déploiement Streamlit Cloud n'invalide pas forcément les sessions de
    navigateur déjà ouvertes : si ce PR ajoute un champ au dict mis en
    cache (ex. "error_summary"), une session restée ouverte pendant le
    déploiement garde un ``t4_data`` à l'ancienne forme, et le code mis à
    jour plante avec un ``KeyError`` en le relisant — c'est exactement
    l'incident constaté en production. Un simple ``is None`` ne détecte
    pas ce cas : un cache présent mais obsolète passe au travers. Fonction
    pure (aucun appel Streamlit) pour rester testable sans dépendre du
    comportement de ``st.session_state`` hors d'un run réel.
    """
    return bool(cached) and not expected_keys.issubset(cached[0].keys())


with tab4:
    st.header("Logs d'import", anchor=False)

    # ── Chargement ────────────────────────────────────────────────────────────
    if st.session_state.t4_data is None or _is_cache_stale(st.session_state.t4_data, _T4_EXPECTED_KEYS):
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
                               rows_duplicates,
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
                    # Batches ayant encore des verbatims en base — permet de
                    # repérer les logs antérieurs à une reprise à zéro dont
                    # les données ont été purgées (cf. spec §2.8).
                    cur.execute("""
                        SELECT DISTINCT import_batch_id
                          FROM verbatims
                         WHERE import_batch_id IS NOT NULL
                    """)
                    live_batch_ids = {str(r[0]) for r in cur.fetchall()}
            st.session_state.t4_data = [
                {
                    "id":             str(r[0]),
                    "started_at":     r[1].strftime("%d/%m/%Y %H:%M") if r[1] else "—",
                    "filename":       r[3] or "—",
                    "import_type":    r[4] or "—",
                    "rows_total":     r[5] or 0,
                    "rows_inserted":  r[6] or 0,
                    "rows_skipped":   r[7] or 0,
                    "rows_duplicates": r[8] or 0,
                    "rows_matched":   r[9] or 0,
                    "rows_unmatched": r[10] or 0,
                    "status":         r[11] or "—",
                    "error_detail":   r[12],
                    "error_summary":  _error_summary(r[12]),
                    "duration_s":     r[13] or 0,
                    "data_purged": (
                        (r[6] or 0) > 0 and str(r[0]) not in live_batch_ids
                    ),
                }
                for r in rows
            ]
        except Exception as exc:
            alert(f"Impossible de charger les logs : {exc}", type="error")
            st.stop()

    all_logs = st.session_state.t4_data or []

    # ── Filtre statut ─────────────────────────────────────────────────────────
    with st.container(horizontal=True, vertical_alignment="bottom"):
        status_opts = ["Tous", "success", "partial", "error", "duplicate", "running"]
        sel_status  = st.selectbox(
            "Filtrer par statut",
            options=status_opts,
            format_func=lambda s: s if s == "Tous" else libelle_statut(s),
            key="t4_status",
        )
        if st.button("Rafraîchir les logs", icon=":material/refresh:", key="t4_refresh"):
            st.session_state.t4_data = None
            st.rerun()

    # ── Filtrage ──────────────────────────────────────────────────────────────
    filtered = (
        all_logs if sel_status == "Tous"
        else [lg for lg in all_logs if lg["status"] == sel_status]
    )

    if not filtered:
        if sel_status != "Tous":
            st.info(
                f"Aucun log avec le statut « {libelle_statut(sel_status)} ».",
                icon=":material/info:",
            )
        else:
            empty_state(
                "",
                "Aucun log d'import",
                "Les logs apparaîtront ici après le premier import.",
            )
    else:
        metric_row([
            {"label": "Logs affichés", "value": len(filtered)},
            {"label": "Total logs",    "value": len(all_logs)},
        ])

        # ── Tableau principal ─────────────────────────────────────────────────
        log_table(filtered)

        # ── Expandeurs détail ─────────────────────────────────────────────────
        st.subheader("Détail par import", anchor=False)
        for lg in filtered:
            raw_detail = lg.get("error_detail")
            has_detail = bool(raw_detail)
            icon    = {"success": ":material/check_circle:", "partial": ":material/warning:",
                       "error": ":material/error:", "duplicate": ":material/content_copy:",
                       "running": ":material/sync:"}.get(lg["status"], ":material/help:")
            reset_prefix = "Reprise à zéro — " if lg["import_type"] == "reset" else ""
            label   = (
                f"{reset_prefix}{lg['started_at']} · {lg['filename']} · "
                f"{libelle_statut(lg['status'])}"
                + (" — voir le détail" if has_detail else "")
            )
            with st.expander(label, expanded=False, icon=icon):
                if lg.get("data_purged"):
                    alert(
                        f"Données purgées lors de la reprise du "
                        f"{lg['started_at']}. Les compteurs et le détail "
                        "ci-dessous restent affichés à titre d'audit — "
                        "les verbatims correspondants n'existent plus en base.",
                        type="warning",
                    )

                ca, cb, cc = st.columns(3)
                with ca:
                    st.markdown(f"**Lot :** `{lg['id'][:8]}…`")
                    st.markdown(f"**Type :** {lg['import_type']}")
                    st.markdown(f"**Durée :** {nombre(lg['duration_s'])} s")
                with cb:
                    st.markdown(f"**Total lignes :** {nombre(lg['rows_total'])}")
                    st.markdown(f"**Insérés :** {nombre(lg['rows_inserted'])}")
                    st.markdown(f"**Ignorées :** {nombre(lg['rows_skipped'])}")
                with cc:
                    st.markdown(f"**Doublons :** {nombre(lg['rows_duplicates'])}")
                    st.markdown(f"**Avec catégorie :** {nombre(lg['rows_matched'])}")
                    st.markdown(f"**Sans catégorie :** {nombre(lg['rows_unmatched'])}")

                if has_detail:
                    render_skip_details(raw_detail, key_prefix=f"log_{lg['id']}")

        # ── Export CSV ────────────────────────────────────────────────────────
        # "erreurs" résume LA NATURE des lignes ignorées (ex.
        # "DATE_INVALIDE:12;VERBATIM_VIDE:3"), pas seulement leur nombre —
        # sans cette colonne, l'export ne permettait pas de savoir quoi
        # avait été rejeté sans rouvrir chaque log un par un dans l'app.
        # Le détail ligne par ligne (numéro, extrait…) reste dans le
        # téléchargement dédié de chaque expander (render_skip_details).
        df_logs = pd.DataFrame([
            {
                "date":           lg.get("started_at", "—"),
                "fichier":        lg.get("filename", "—"),
                "type":           lg.get("import_type", "—"),
                "inseres":        lg.get("rows_inserted", 0),
                "ignorees":       lg.get("rows_skipped", 0),
                "erreurs":        lg.get("error_summary", ""),
                "doublons":       lg.get("rows_duplicates", 0),
                "matches":        lg.get("rows_matched", 0),
                "sans_categorie": lg.get("rows_unmatched", 0),
                "statut":         lg.get("status", "—"),
                "duree_s":        lg.get("duration_s", 0),
                "batch_id":       lg.get("id", "—"),
            }
            for lg in filtered
        ])
        csv_logs = io.StringIO()
        df_logs.to_csv(csv_logs, index=False, encoding="utf-8-sig")
        st.download_button(
            label="Exporter les logs en CSV",
            icon=":material/download:",
            data=csv_logs.getvalue().encode("utf-8-sig"),
            file_name="import_logs_export.csv",
            mime="text/csv",
            key="t4_export",
        )
