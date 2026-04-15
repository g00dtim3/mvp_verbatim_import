"""
pages/1_Import.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Module 1 — Import mensuel et initial de fichiers CSV Semantiweb.

Flux :
  Étape 0 — Sélection du mode (initial / mensuel)
  Étape 1 — Upload + contrôle anti-doublon hash fichier
  Étape 2 — Validation CSV + aperçu
  Étape 3 — Import par batches avec barre de progression
  Étape 4 — Résumé des métriques
"""

import sys
import time
import uuid
from pathlib import Path

import streamlit as st

# ── Résolution du chemin racine du projet ──────────────────────────────────────
# pages/ est un sous-répertoire : on remonte d'un cran pour trouver core/, etc.
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from compass_ui.compass_ui import (
    alert,
    duplicate_alert,
    hash_check,
    import_mode_toggle,
    import_summary,
    inject_css,
    page_header,
    progress_block,
    sidebar_header,
    steps,
    theme_toggle,
)
from core.db import get_active_env, get_connection
from core.hasher import file_hash as compute_file_hash
from core.hasher import is_file_already_imported
from core.importer import apply_known_categories, import_batch, normalize_row, parse_csv

# ── Configuration Streamlit ────────────────────────────────────────────────────
st.set_page_config(
    page_title="Import — Compass Consumer Voice",
    page_icon="📥",
    layout="wide",
)

# ── Design system ──────────────────────────────────────────────────────────────
inject_css()
theme_toggle()
sidebar_header()

# ── Initialisation session_state ──────────────────────────────────────────────
_DEFAULTS = {
    "import_mode":        None,   # "initial" | "mensuel"
    "file_hash":          None,
    "file_bytes":         None,
    "file_name":          None,
    "df_parsed":          None,
    "step":               0,      # étape courante : 0-3
    "batch_id":           None,
    "import_done":        False,
    "import_stats":       None,   # dict résultats
    "import_duration_s":  0,
    "import_error":       None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _reset_from_step(step: int) -> None:
    """Réinitialise l'état à partir d'une étape donnée."""
    if step <= 1:
        st.session_state.file_hash   = None
        st.session_state.file_bytes  = None
        st.session_state.file_name   = None
        st.session_state.df_parsed   = None
    if step <= 2:
        st.session_state.batch_id    = None
        st.session_state.import_done = False
        st.session_state.import_stats = None
        st.session_state.import_error = None
    st.session_state.step = step


def _log_insert(conn, batch_id: str, file_hash_val: str, filename: str,
                import_type: str, rows_total: int) -> None:
    """Crée l'enregistrement import_logs au début de l'import."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_logs
                (id, file_hash, filename, import_type, rows_total, status)
            VALUES (%s, %s, %s, %s, %s, 'running')
            """,
            (batch_id, file_hash_val, filename, import_type, rows_total),
        )
    conn.commit()


def _log_update(conn, batch_id: str, stats: dict, status: str,
                error_detail: str | None = None) -> None:
    """Met à jour import_logs en fin d'import."""
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE import_logs SET
                finished_at    = NOW(),
                rows_inserted  = %s,
                rows_skipped   = %s,
                rows_matched   = %s,
                rows_unmatched = %s,
                status         = %s,
                error_detail   = %s
            WHERE id = %s
            """,
            (
                stats.get("inserted", 0),
                stats.get("skipped", 0),
                stats.get("matched", 0),
                stats.get("unmatched", 0),
                status,
                error_detail,
                batch_id,
            ),
        )
    conn.commit()


# ─── En-tête de page ──────────────────────────────────────────────────────────

_mode = st.session_state.import_mode
_badge = (
    "Import initial" if _mode == "initial"
    else "Import mensuel" if _mode == "mensuel"
    else None
)
_badge_type = "cyan" if _mode == "initial" else "info"

page_header(
    title="Import",
    subtitle="Charger le fichier CSV mensuel de l'API",
    badge=_badge,
    badge_type=_badge_type,
)

# ─── Sidebar info ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        f'<div style="font-size:11px;color:var(--c-sidebar-text);padding:8px 16px">'
        f'Environnement : <strong style="color:var(--c-cyan)">'
        f'{get_active_env().upper()}</strong></div>',
        unsafe_allow_html=True,
    )

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 0 — Sélection du mode
# ═══════════════════════════════════════════════════════════════

st.markdown("### Mode d'import")

prev_mode = st.session_state.import_mode
selected_mode = import_mode_toggle()

# Si le mode change, on repart de zéro
if selected_mode != prev_mode:
    _reset_from_step(0)
    st.session_state.import_mode = selected_mode
    st.rerun()

st.session_state.import_mode = selected_mode

# Note explicative selon le mode
if selected_mode == "initial":
    alert(
        message=(
            "Le fichier historique complet contient déjà les champs "
            "<strong>catégorie interne</strong>, <strong>sous-catégorie</strong> "
            "et <strong>photo</strong>. Ils seront importés tels quels."
        ),
        type="info",
        title="Import initial (one-shot)",
    )
else:
    alert(
        message=(
            "Les champs <strong>catégorie</strong>, <strong>sous-catégorie</strong> "
            "et <strong>photo</strong> seront <strong>NULL</strong> à l'import. "
            "Utilisez le module <strong>Matching catégories</strong> pour les compléter."
        ),
        type="info",
        title="Import mensuel courant",
    )

st.divider()

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 1 — Upload et contrôle anti-doublon
# ═══════════════════════════════════════════════════════════════

st.markdown("### 1 — Fichier CSV")

uploaded = st.file_uploader(
    "Déposer le fichier CSV Semantiweb",
    type=["csv"],
    help="Encodage UTF-8 BOM, séparateur point-virgule (;)",
    key="csv_uploader",
)

if uploaded is not None:
    file_bytes = uploaded.read()
    fhash = compute_file_hash(file_bytes)

    # Nouveau fichier (ou changement de fichier)
    if fhash != st.session_state.file_hash:
        _reset_from_step(1)
        st.session_state.file_hash  = fhash
        st.session_state.file_bytes = file_bytes
        st.session_state.file_name  = uploaded.name

    # Contrôle anti-doublon
    try:
        with get_connection() as conn:
            existing_log = is_file_already_imported(conn, fhash)
    except Exception as exc:
        alert(f"Erreur de connexion à la base : {exc}", type="error")
        st.stop()

    if existing_log:
        started = existing_log.get("started_at")
        date_str = (
            started.strftime("%d/%m/%Y à %H:%M")
            if hasattr(started, "strftime")
            else str(started)
        )
        hash_check("dupe")
        duplicate_alert(
            filename=existing_log.get("filename", uploaded.name),
            date=date_str,
            batch_id=str(existing_log.get("id", "")),
        )
        st.stop()
    else:
        hash_check("ok")
        st.session_state.step = max(st.session_state.step, 1)

else:
    hash_check("idle")
    # Pas de fichier → on s'arrête ici
    st.stop()

st.divider()

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 2 — Validation et aperçu
# ═══════════════════════════════════════════════════════════════

steps(["Upload", "Validation", "Import", "Résumé"], current=1)

# Parse le CSV (ou utilise le résultat mis en cache dans session_state)
if st.session_state.df_parsed is None:
    try:
        df = parse_csv(st.session_state.file_bytes)
        st.session_state.df_parsed = df
    except ValueError as exc:
        alert(str(exc), type="error", title="Fichier invalide")
        st.stop()
    except Exception as exc:
        alert(f"Erreur inattendue lors de la lecture : {exc}", type="error")
        st.stop()

df = st.session_state.df_parsed

col_info, col_preview = st.columns([1, 3])
with col_info:
    st.metric("Lignes détectées", f"{len(df):,}")
    st.metric("Colonnes", len(df.columns))
    st.caption(f"Fichier : `{st.session_state.file_name}`")

with col_preview:
    st.markdown("**Aperçu — 5 premières lignes**")
    st.dataframe(df.head(5), use_container_width=True, height=200)

st.markdown("")

# Bouton de lancement — désactivé si l'import est déjà terminé
if not st.session_state.import_done:
    if st.button("Lancer l'import →", type="primary", use_container_width=False):
        st.session_state.step = 2
        st.rerun()

if st.session_state.step < 2:
    st.stop()

st.divider()

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 3 — Import par batches
# ═══════════════════════════════════════════════════════════════

steps(["Upload", "Validation", "Import", "Résumé"], current=2)

if not st.session_state.import_done:

    batch_id   = str(uuid.uuid4())
    df         = st.session_state.df_parsed
    mode       = st.session_state.import_mode
    fhash      = st.session_state.file_hash
    fname      = st.session_state.file_name
    total_rows = len(df)

    st.session_state.batch_id = batch_id

    # Placeholder pour la barre de progression
    progress_placeholder = st.empty()

    try:
        # ── Créer le log d'import ──────────────────────────────────────────────
        with get_connection() as conn:
            _log_insert(conn, batch_id, fhash, fname, mode, total_rows)

        # ── Normaliser toutes les lignes ───────────────────────────────────────
        progress_placeholder.markdown(
            "<div style='text-align:center;padding:1rem;"
            "color:var(--c-text-2)'>Normalisation des données…</div>",
            unsafe_allow_html=True,
        )

        skipped_parse = 0
        rows: list[dict] = []
        for _, row in df.iterrows():
            try:
                rows.append(normalize_row(row, mode))
            except Exception:
                skipped_parse += 1

        # ── Enrichissement avec les catégories connues ─────────────────────────
        try:
            with get_connection() as conn:
                rows = apply_known_categories(conn, rows)
        except Exception as exc:
            alert(
                f"Avertissement : enrichissement catégories impossible ({exc}). "
                "L'import continue sans pré-remplissage.",
                type="warning",
            )

        # Compter les lignes déjà enrichies (pour la métrique "matchées")
        pre_matched = sum(
            1 for r in rows if r.get("categorie_interne") is not None
        )

        # ── INSERT par batches ─────────────────────────────────────────────────
        BATCH_SIZE = 1000
        total_inserted = 0
        total_skipped  = skipped_parse
        all_errors: list[str] = []

        t_start = time.time()

        with get_connection() as conn:
            for batch_start in range(0, len(rows), BATCH_SIZE):
                chunk = rows[batch_start : batch_start + BATCH_SIZE]
                done  = min(batch_start + BATCH_SIZE, len(rows))
                pct   = int(done / len(rows) * 100) if rows else 100

                with progress_placeholder.container():
                    progress_block(
                        title="Import en cours…",
                        subtitle=(
                            f"Traitement de {done:,} / {len(rows):,} lignes"
                            f" — lot {batch_start // BATCH_SIZE + 1}"
                        ),
                        percent=pct,
                    )

                result = import_batch(conn, chunk, batch_id)
                total_inserted += result["inserted"]
                total_skipped  += result["skipped"]
                all_errors.extend(result["errors"])

        t_elapsed = int(time.time() - t_start)

        # ── Métriques finales ──────────────────────────────────────────────────
        rows_unmatched = max(0, total_inserted - pre_matched)
        rows_matched   = total_inserted - rows_unmatched

        final_stats = {
            "inserted":  total_inserted,
            "skipped":   total_skipped,
            "matched":   rows_matched,
            "unmatched": rows_unmatched,
            "errors":    all_errors,
        }

        final_status = (
            "success" if not all_errors
            else "partial" if total_inserted > 0
            else "error"
        )

        # ── Mettre à jour le log ───────────────────────────────────────────────
        try:
            with get_connection() as conn:
                _log_update(
                    conn,
                    batch_id,
                    final_stats,
                    final_status,
                    error_detail="\n".join(all_errors) if all_errors else None,
                )
        except Exception as exc:
            alert(f"Avertissement : impossible de finaliser le log : {exc}", type="warning")

        st.session_state.import_stats      = final_stats
        st.session_state.import_duration_s = t_elapsed
        st.session_state.import_done       = True
        st.session_state.step              = 3

        progress_placeholder.empty()
        st.rerun()

    except Exception as exc:
        # Marquer le log en erreur si possible
        try:
            with get_connection() as conn:
                _log_update(
                    conn,
                    batch_id,
                    {"inserted": 0, "skipped": 0, "matched": 0, "unmatched": 0},
                    "error",
                    error_detail=str(exc),
                )
        except Exception:
            pass

        progress_placeholder.empty()
        st.session_state.import_error = str(exc)
        st.session_state.step = 1
        alert(f"Import échoué : {exc}", type="error", title="Erreur d'import")
        st.stop()

if st.session_state.step < 3:
    st.stop()

st.divider()

# ═══════════════════════════════════════════════════════════════
# ÉTAPE 4 — Résumé
# ═══════════════════════════════════════════════════════════════

steps(["Upload", "Validation", "Import", "Résumé"], current=3)

stats    = st.session_state.import_stats or {}
duration = st.session_state.import_duration_s

import_summary(
    rows_inserted=stats.get("inserted", 0),
    rows_skipped=stats.get("skipped", 0),
    rows_matched=stats.get("matched", 0),
    rows_unmatched=stats.get("unmatched", 0),
    duration_s=duration,
)

# Erreurs par lot
if stats.get("errors"):
    with st.expander(f"⚠ {len(stats['errors'])} lot(s) en erreur — détail"):
        for err in stats["errors"]:
            st.code(err)

# Navigation vers Matching si des verbatims sont sans catégorie
if stats.get("unmatched", 0) > 0:
    st.markdown("")
    if st.button(
        "Aller au Matching catégories →",
        type="primary",
        key="goto_matching",
    ):
        st.switch_page("pages/3_Matching.py")

# Bouton pour recommencer un nouvel import
st.markdown("")
if st.button("Importer un autre fichier", key="reset_import"):
    for k, v in _DEFAULTS.items():
        st.session_state[k] = v
    st.rerun()
