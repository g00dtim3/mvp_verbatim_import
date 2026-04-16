"""
pages/99_Admin.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Page d'administration — initialisation de la base de données.

Accessible uniquement en connaissant l'URL directe.
Non listée dans la navigation principale.
"""

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(page_title="Admin — Compass", layout="centered")

from compass_ui.compass_ui import inject_css, sidebar_header
inject_css()
sidebar_header()

# ── En-tête ───────────────────────────────────────────────────────────────────
st.title("Administration")
st.caption("Page réservée — initialisation de la base de données.")

st.divider()

# ═══════════════════════════════════════════════════════════════
# SECTION — Initialiser categories_mapping depuis Table_CO.csv
# ═══════════════════════════════════════════════════════════════

st.subheader("Initialiser categories_mapping")
st.write(
    "Charge `Table_CO.csv` dans la table `categories_mapping` "
    "via UPSERT sur `key_brandxpdt`."
)

with st.expander("Colonnes attendues dans Table_CO.csv", expanded=False):
    st.markdown(
        "| Colonne | Description |\n"
        "|---|---|\n"
        "| `Key brandxpdt` | brand \\|\\| product\\_name (clé primaire) |\n"
        "| `brand` | Marque |\n"
        "| `product_name_SEMANTIWEB` | Nom produit API |\n"
        "| `categorie interne` | Catégorie |\n"
        "| `sous categorie interne` | Sous-catégorie |\n"
        "| `photo` | oui / non / true / false |"
    )

st.markdown("")

col1, col2, col3 = st.columns(3)
dry_run   = col1.checkbox("Dry-run (simulation)", value=True,
                          help="Lit et valide le CSV sans rien écrire en base.")
propagate = col2.checkbox("Propager vers verbatims", value=False,
                          help="Met à jour categorie_interne / sous_categorie_interne / photo "
                               "sur tous les verbatims dont (brand, product_name) est connu.")
reset     = col3.checkbox("Vider la table d'abord", value=False,
                          help="DELETE FROM categories_mapping avant l'UPSERT. "
                               "Irréversible — utiliser avec précaution.")

if reset and not dry_run:
    st.warning(
        "⚠ **Vider la table** supprimera toutes les correspondances existantes "
        "avant l'import. Cochez Dry-run pour prévisualiser d'abord.",
        icon=None,
    )

st.markdown("")

if st.button("Lancer", type="primary", key="admin_run"):
    from scripts.load_table_co import run

    log_area = st.empty()
    logs: list[str] = []

    # Rediriger st.write() vers un accumulateur affiché en temps réel
    def _log(msg):
        logs.append(str(msg))
        log_area.code("\n".join(logs), language=None)

    # Monkey-patch temporaire de st.write pour capturer les logs de run()
    _orig_write = st.write
    st.write = _log

    with st.spinner("En cours…"):
        try:
            result = run(dry_run=dry_run, propagate=propagate, reset=reset)
            st.write = _orig_write
            st.success(
                f"Terminé — {result['rows_read']:,} lignes lues, "
                f"{result['upserted']:,} upsertées"
                + (f", {result['propagated']:,} verbatims mis à jour" if propagate else "")
                + (" (dry-run)" if dry_run else "")
                + "."
            )
        except Exception as exc:
            st.write = _orig_write
            st.error(f"Erreur : {exc}")
