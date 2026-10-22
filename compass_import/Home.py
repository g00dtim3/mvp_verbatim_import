"""
Home.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Page d'accueil et point d'entrée Streamlit.

Ce fichier est exécuté en premier par Streamlit.
Les pages additionnelles sont chargées automatiquement depuis pages/.
"""

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from charte import appliquer_charte, section
from compass_ui.compass_ui import alert, metric_row, nombre, page_header
from auth import exiger_mot_de_passe
from core.db import get_active_env, get_connection, test_connection

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Compass · Consumer Voice Import",
    page_icon=":material/dashboard:",
    layout="wide",
)
exiger_mot_de_passe()  # la page s'arrête ici tant que le mot de passe n'est pas saisi

appliquer_charte()

# ── Test de connexion DB ──────────────────────────────────────────────────────
try:
    db_ok = test_connection()
except Exception:
    db_ok = False

if not db_ok:
    st.sidebar.markdown(":red-badge[:material/error: Base de données inaccessible]")
    page_header(
        title="Tableau de bord",
        subtitle="Pipeline d'import et d'enrichissement des verbatims Consumer Voice",
    )
    alert(
        "Impossible de se connecter à la base de données. "
        "Vérifiez les variables d'environnement et la disponibilité du serveur "
        "(modèle de configuration : `.env.example`).",
        type="error",
        title="Connexion à la base de données échouée",
    )
    st.stop()

# ── Sidebar : env + stats ─────────────────────────────────────────────────────
env = get_active_env()


@st.cache_data(ttl=60, show_spinner=False)
def _load_global_stats() -> dict:
    """Charge les métriques globales (cache 60 s)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM verbatims")
                total_verbatims = cur.fetchone()[0]

                cur.execute("""
                    SELECT COUNT(DISTINCT v.brand || v.product_name)
                      FROM verbatims v
                      LEFT JOIN categories_mapping cm
                             ON v.brand = cm.brand AND v.product_name = cm.product_name
                     WHERE cm.key_brandxpdt IS NULL
                """)
                n_unmatched = cur.fetchone()[0]

        return {"total_verbatims": total_verbatims, "n_unmatched": n_unmatched}
    except Exception:
        return {"total_verbatims": None, "n_unmatched": None}


stats = _load_global_stats()

with st.sidebar:
    st.markdown(":green-badge[:material/check_circle: Base de données connectée]")
    st.caption(f"Environnement : **{env.upper()}**")

    if stats["total_verbatims"] is not None:
        st.caption(f"Verbatims : **{nombre(stats['total_verbatims'])}**")

    if stats["n_unmatched"] is not None:
        if stats["n_unmatched"] > 0:
            st.markdown(
                f":yellow-badge[:material/warning: {nombre(stats['n_unmatched'])} "
                "produit(s) sans catégorie]"
            )
        else:
            st.markdown(":green-badge[:material/check_circle: Tous les produits ont une catégorie]")

# ── En-tête page ──────────────────────────────────────────────────────────────
page_header(
    title="Tableau de bord",
    subtitle="Pipeline d'import et d'enrichissement des verbatims Consumer Voice",
    badge=f"Environnement : {env.upper()}",
)

# ── Métriques globales ────────────────────────────────────────────────────────
if stats["total_verbatims"] is not None:
    metric_row([
        {"label": "Verbatims en base",       "value": stats["total_verbatims"]},
        {"label": "Produits sans catégorie", "value": stats["n_unmatched"] or 0},
    ])

# ── Modules ───────────────────────────────────────────────────────────────────
_MODULES = [
    (
        ":material/upload: Import",
        "Import initial (one-shot) ou mensuel des fichiers CSV Semantiweb. "
        "Contrôle des doublons par empreinte SHA-256.",
    ),
    (
        ":material/sell: Matching",
        "Assigner les catégories via un fichier Excel à menus déroulants dépendants. "
        "Propagation automatique sur tous les verbatims du produit.",
    ),
    (
        ":material/build: Outils",
        "Table de correspondance, renommage de produit, vérification "
        "et logs d'import.",
    ),
]

with section(
    "Modules disponibles",
    "Utilisez la navigation dans la barre latérale pour accéder aux modules.",
):
    for col, (titre, description) in zip(st.columns(len(_MODULES)), _MODULES):
        with col, st.container(border=True, height="stretch"):
            st.subheader(titre, anchor=False)
            st.caption(description)

# ── Action rapide ─────────────────────────────────────────────────────────────
if (stats.get("n_unmatched") or 0) > 0:
    alert(
        f"**{nombre(stats['n_unmatched'])} produit(s)** sans catégorie dans la base. "
        "Utilisez le module **Matching catégories** pour les compléter.",
        type="warning",
        title="Action requise",
    )
    if st.button(
        "Ouvrir le matching des catégories",
        type="primary",
        icon=":material/arrow_forward:",
        key="home_goto_matching",
    ):
        st.switch_page("pages/2_Matching.py")
