"""
compass_ui/skip_table.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Composant partagé d'affichage des lignes ignorées à l'import.

Utilisé par pages/1_Import.py (résumé juste après un import) et
pages/3_Outils.py (détail d'un log historique, onglet Logs) — même rendu,
une seule implémentation (cf. spec §2.8).

Usage :
    from compass_ui.skip_table import render_skip_details, render_duplicates_note

    render_skip_details(log["error_detail"], key_prefix=f"log_{batch_id}")
    render_duplicates_note(rows_duplicates)
"""

import pandas as pd
import streamlit as st

from core.skip_report import parse_error_detail

_CODE_LABELS: dict[str, str] = {
    "DATE_INVALIDE":    "date(s) invalide(s)",
    "DATE_MANQUANTE":   "date(s) manquante(s)",
    "BRAND_MANQUANTE":  "marque(s) manquante(s)",
    "PRODUIT_MANQUANT": "produit(s) manquant(s)",
    "VERBATIM_VIDE":    "verbatim(s) vide(s)",
    "GUID_MANQUANT":    "guid manquant(s)",
    "LIGNE_MALFORMEE":  "ligne(s) malformée(s)",
    "ERREUR_INCONNUE":  "erreur(s) inconnue(s)",
}


def _summary_line(skips_par_code: dict) -> str:
    parts = []
    for code, count in sorted(skips_par_code.items(), key=lambda kv: -kv[1]):
        label = _CODE_LABELS.get(code, code.lower())
        parts.append(f"{count} {label}")
    return " · ".join(parts) if parts else "détail indisponible"


def render_skip_details(error_detail: dict | str | None, key_prefix: str) -> None:
    """
    Affiche le détail des lignes ignorées à l'import.

    Args:
        error_detail: Valeur brute de ``import_logs.error_detail`` (dict
            déjà décodé par psycopg2 pour une colonne JSONB, chaîne JSON,
            format legacy, ou ``None``) — jamais parsée par l'appelant,
            c'est le rôle de ``core.skip_report.parse_error_detail``.
        key_prefix: Préfixe unique des clés de widgets Streamlit, pour
            éviter les collisions entre plusieurs appels sur la même page
            (ex. un par ligne de log dans l'onglet Logs). Sert aussi au nom
            du fichier exporté (``lignes_ignorees_{key_prefix}.csv``).
    """
    parsed = parse_error_detail(error_detail)

    if parsed["is_legacy"] and not parsed["skips"]:
        if parsed["legacy_text"]:
            st.caption("Détail antérieur à la traçabilité par ligne (format libre) :")
            st.code(parsed["legacy_text"], language=None)
        if parsed["batch_errors"]:
            st.markdown("**Erreurs de lot :**")
            for err in parsed["batch_errors"]:
                st.code(err)
        if not parsed["legacy_text"] and not parsed["batch_errors"]:
            st.caption("Aucun détail exploitable pour cet import.")
        return

    skips = parsed["skips"]

    if skips:
        st.markdown(f"**Résumé :** {_summary_line(parsed['skips_par_code'])}")

        if parsed["skips_tronque"]:
            st.warning(
                f"Seules les {len(skips):,} premières lignes sur "
                f"{parsed['skips_total']:,} sont détaillées ci-dessous."
            )

        df_skips = pd.DataFrame(skips)
        for col in ("ligne", "code", "raison", "champ", "extrait"):
            if col not in df_skips.columns:
                df_skips[col] = None

        st.dataframe(
            df_skips[["ligne", "raison", "champ", "extrait"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "ligne":   st.column_config.NumberColumn("Ligne", width="small"),
                "raison":  st.column_config.TextColumn("Raison", width="large"),
                "champ":   st.column_config.TextColumn("Champ", width="small"),
                "extrait": st.column_config.TextColumn("Extrait", width="large"),
            },
        )

        csv_export = df_skips[["ligne", "code", "raison", "champ", "extrait"]].to_csv(
            index=False, encoding="utf-8-sig"
        )
        st.download_button(
            label="📥 Télécharger le détail (CSV)",
            data=csv_export.encode("utf-8-sig"),
            file_name=f"lignes_ignorees_{key_prefix}.csv",
            mime="text/csv",
            key=f"{key_prefix}_skip_download",
        )
    elif parsed["is_legacy"]:
        st.caption("Aucune ligne ignorée détaillée pour cet import.")


def render_duplicates_note(rows_duplicates: int) -> None:
    """
    Affiche, séparément des skips, le nombre de lignes déjà présentes en
    base (non réimportées) — libellé volontairement distinct de "ignorées"
    pour ne pas laisser croire à un problème de qualité du fichier
    (cf. spec §2.7 : ne pas mélanger skips et doublons dans la même UI).
    """
    if rows_duplicates > 0:
        st.info(
            f"ℹ {rows_duplicates:,} ligne(s) déjà présente(s) en base "
            "(non réimportées)."
        )
