"""
app.py
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

from compass_ui.compass_ui import (
    alert,
    inject_css,
    metric_row,
    page_header,
    sidebar_header,
    theme_toggle,
)
from core.db import get_active_env, get_connection, test_connection

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Compass · Consumer Voice Import",
    page_icon="📊",
    layout="wide",
)

inject_css()
theme_toggle()
sidebar_header()

# ── Test de connexion DB ──────────────────────────────────────────────────────
try:
    db_ok = test_connection()
except Exception:
    db_ok = False

if not db_ok:
    with st.sidebar:
        st.markdown(
            '<div style="font-size:11px;color:var(--c-error);padding:8px 16px">'
            '✕ Base de données inaccessible</div>',
            unsafe_allow_html=True,
        )
    page_header(
        title="Tableau de bord",
        subtitle="Pipeline d'import et d'enrichissement des verbatims Consumer Voice",
    )
    alert(
        "Impossible de se connecter à la base de données. "
        "Vérifiez les variables d'environnement et la disponibilité du serveur. "
        "Consultez <code>.env.example</code> pour la configuration.",
        type="error",
        title="Connexion base de données échouée",
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
                    SELECT COUNT(DISTINCT v.product_name)
                      FROM verbatims v
                      LEFT JOIN categories_mapping cm USING (product_name)
                     WHERE cm.product_name IS NULL
                """)
                n_unmatched = cur.fetchone()[0]

        return {"total_verbatims": total_verbatims, "n_unmatched": n_unmatched}
    except Exception:
        return {"total_verbatims": None, "n_unmatched": None}


stats = _load_global_stats()

with st.sidebar:
    st.markdown(
        '<div style="font-size:11px;color:var(--c-success);padding:4px 16px">'
        '✓ Base de données connectée</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div style="font-size:11px;color:var(--c-sidebar-text);padding:4px 16px">'
        f'Environnement : <strong style="color:var(--c-cyan)">'
        f'{env.upper()}</strong></div>',
        unsafe_allow_html=True,
    )

    if stats["total_verbatims"] is not None:
        st.markdown(
            f'<div style="font-size:11px;color:var(--c-sidebar-text);padding:4px 16px">'
            f'Verbatims : <strong>{stats["total_verbatims"]:,}</strong></div>',
            unsafe_allow_html=True,
        )

    if stats["n_unmatched"] is not None:
        color = "var(--c-warning)" if stats["n_unmatched"] > 0 else "var(--c-success)"
        icon  = "⚠" if stats["n_unmatched"] > 0 else "✓"
        st.markdown(
            f'<div style="font-size:11px;color:{color};padding:4px 16px">'
            f'{icon} {stats["n_unmatched"]:,} produit(s) sans catégorie</div>',
            unsafe_allow_html=True,
        )

# ── En-tête page ──────────────────────────────────────────────────────────────
page_header(
    title="Tableau de bord",
    subtitle="Pipeline d'import et d'enrichissement des verbatims Consumer Voice",
    badge=f"Env : {env.upper()}",
    badge_type="cyan" if env == "dev" else "info",
)

# ── Métriques globales ────────────────────────────────────────────────────────
if stats["total_verbatims"] is not None:
    metrics = [
        {
            "label": "Verbatims en base",
            "value": f"{stats['total_verbatims']:,}",
            "color": "blue",
        },
        {
            "label": "Produits sans catégorie",
            "value": str(stats["n_unmatched"] or 0),
            "color": "warning" if (stats["n_unmatched"] or 0) > 0 else "success",
        },
    ]
    metric_row(metrics)

st.divider()

# ── Modules ───────────────────────────────────────────────────────────────────
st.markdown("### Modules disponibles")
st.caption(
    "Utilisez la navigation dans la barre latérale gauche pour accéder aux modules."
)

col1, col2, col3, col4 = st.columns(4)

_card_style = (
    "padding:1.4rem;background:var(--c-card-bg);"
    "border:1px solid var(--c-border);border-radius:10px;height:180px"
)

with col1:
    st.markdown(f"""
    <div style="{_card_style}">
        <div style="font-size:28px;margin-bottom:8px">📥</div>
        <div style="font-weight:700;color:var(--c-deep);margin-bottom:6px">
            1 — Import
        </div>
        <div style="font-size:12px;color:var(--c-text-2)">
            Import initial (one-shot) ou mensuel des fichiers CSV Semantiweb.
            Contrôle doublon par hash SHA-256.
        </div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="{_card_style}">
        <div style="font-size:28px;margin-bottom:8px">✏</div>
        <div style="font-weight:700;color:var(--c-deep);margin-bottom:6px">
            2 — Enrichissement
        </div>
        <div style="font-size:12px;color:var(--c-text-2)">
            Édition manuelle de catégorie, sous-catégorie et photo sur des
            verbatims individuels ou en sélection filtrée.
        </div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div style="{_card_style}">
        <div style="font-size:28px;margin-bottom:8px">🏷</div>
        <div style="font-weight:700;color:var(--c-deep);margin-bottom:6px">
            3 — Matching
        </div>
        <div style="font-size:12px;color:var(--c-text-2)">
            Assigner catégories via XLS avec menus déroulants dépendants.
            Propagation automatique sur tous les verbatims du produit.
        </div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div style="{_card_style}">
        <div style="font-size:28px;margin-bottom:8px">🔧</div>
        <div style="font-weight:700;color:var(--c-deep);margin-bottom:6px">
            4 — Outils
        </div>
        <div style="font-size:12px;color:var(--c-text-2)">
            Table de correspondance, renommage de produit en cascade,
            vérification et logs d'import.
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── Action rapide ─────────────────────────────────────────────────────────────
if (stats.get("n_unmatched") or 0) > 0:
    st.markdown("")
    alert(
        f"<strong>{stats['n_unmatched']:,} produit(s)</strong> sans catégorie dans la base. "
        "Utilisez le module <strong>Matching catégories</strong> pour les compléter.",
        type="warning",
        title="Action requise",
    )
    if st.button(
        "Aller au Matching catégories →",
        type="primary",
        key="home_goto_matching",
    ):
        st.switch_page("pages/3_Matching.py")
