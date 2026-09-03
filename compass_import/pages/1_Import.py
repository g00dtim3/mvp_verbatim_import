"""
pages/1_Import.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Module 1 — Import périodique et initial de fichiers CSV Semantiweb.

Flux :
  Étape 0 — Sélection du mode (initial / périodique)
  Étape 1 — Upload + contrôle anti-doublon hash fichier
  Étape 2 — Validation CSV + aperçu (léger — jamais le fichier entier)
  Étape 3 — Import EN STREAMING (chunk par chunk) avec barre de progression
  Étape 4 — Résumé des métriques

Étape 3 — note mémoire :
  Un import de fichier volumineux (189 824 lignes constatées en production)
  faisait planter le process sur Streamlit Cloud (OOM, tué par l'OS sans
  message applicatif, après ~17 000 lignes) parce que l'ancienne version
  chargeait tout le CSV en DataFrame puis construisait une liste Python de
  TOUTES les lignes normalisées avant de commencer le moindre INSERT. La
  boucle ci-dessous ne garde jamais plus d'un chunk (normalisé, enrichi,
  inséré) en mémoire à la fois, et persiste sa progression après chaque
  chunk — pour qu'un crash, quelle qu'en soit la cause, laisse un
  `import_logs` exploitable au lieu d'un enregistrement bloqué à zéro.
"""

import json
import logging
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import streamlit as st

# ── Résolution du chemin racine du projet ──────────────────────────────────────
# pages/ est un sous-répertoire : on remonte d'un cran pour trouver core/, etc.
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from psycopg2.extras import Json

from compass_ui.compass_ui import (
    alert,
    hash_check,
    import_mode_toggle,
    import_summary,
    inject_css,
    page_header,
    previous_import_alert,
    progress_block,
    sidebar_header,
    steps,
    theme_toggle,
)
from compass_ui.skip_table import render_duplicates_note, render_skip_details
from core.db import get_active_env, get_connection
from core.hasher import file_hash as compute_file_hash
from core.hasher import is_file_already_imported
from core.importer import (
    apply_known_categories,
    import_batch,
    iter_csv_chunks,
    load_import_settings,
    normalize_batch,
    preview_csv,
)
from core.skip_report import build_error_detail

# ── Logging serveur ────────────────────────────────────────────────────────────
# Sans configuration explicite, le logger de niveau INFO n'émet rien de
# visible dans les logs Streamlit Cloud (le root logger par défaut ne
# traite que WARNING+). basicConfig() est un no-op si déjà configuré ailleurs
# dans ce process — sûr à appeler depuis chaque page, y compris si celle-ci
# est la toute première exécutée (navigation directe vers /Import).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Nombre de lignes traitées entre deux logs de progression serveur.
_PROGRESS_LOG_EVERY = 5000

# Au-delà de ce délai depuis started_at, un import resté au statut
# 'running' (jamais finalisé) est traité comme probablement interrompu
# (crash) plutôt que réellement en cours — cf. contrôle anti-doublon de
# l'étape 1. Heuristique approximative faute d'un timestamp de dernière
# activité en base (started_at ne bouge pas pendant tout l'import) : à
# ajuster si des imports légitimes dépassent régulièrement ce délai.
_RUNNING_STALE_MINUTES = 15

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
    "preview_df":         None,   # DataFrame léger (n premières lignes seulement)
    "preview_total_rows": 0,      # estimation — jamais un chargement complet
    "step":               0,      # étape courante : 0-3
    "batch_id":           None,
    "import_done":        False,
    "import_stats":       None,   # dict résultats
    "import_duration_s":  0,
    "import_error":       None,
    "skip_details":       None,   # list[{ligne, code, raison, champ, extrait}] — plafonnée
    "error_detail_payload": None, # enveloppe JSON envoyée à import_logs.error_detail
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
        st.session_state.preview_df         = None
        st.session_state.preview_total_rows = 0
    if step <= 2:
        st.session_state.batch_id     = None
        st.session_state.import_done  = False
        st.session_state.import_stats = None
        st.session_state.import_error = None
        st.session_state.skip_details = None
        st.session_state.error_detail_payload = None
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


def _log_progress(conn, batch_id: str, stats: dict, status: str,
                   error_detail: dict | None, *, final: bool) -> None:
    """Met à jour import_logs — appelée après CHAQUE chunk (``final=False``,
    ``status`` reste ``'running'``) ET une dernière fois à la fin
    (``final=True``).

    C'est ce qui rend un import consultable dans Outils / Logs même après
    un crash partiel (OOM, coupure réseau…) : sans mise à jour
    intermédiaire, seule la ligne 'running' initiale (tout à zéro) créée
    par ``_log_insert`` existait tant que l'import n'était pas allé à son
    terme — un crash à la ligne 17 000 sur 189 824 ne laissait alors aucune
    trace exploitable des lignes déjà traitées ni des erreurs déjà
    rencontrées.

    ``final`` ne contrôle QUE l'écriture de ``finished_at`` — la valeur est
    un des deux littéraux ci-dessous, jamais dérivée d'une entrée
    utilisateur, donc l'interpolation du fragment SQL est sûre (pas
    d'injection possible).

    ``error_detail`` est l'enveloppe dict construite par
    ``core.skip_report.build_error_detail`` (ou ``None``) — enveloppée dans
    ``psycopg2.extras.Json`` avec ``ensure_ascii=False`` pour la colonne
    JSONB, ce qui préserve les caractères accentués sans double sérialisation.
    """
    finished_clause = "finished_at = NOW()," if final else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE import_logs SET
                {finished_clause}
                rows_total      = %s,
                rows_inserted   = %s,
                rows_skipped    = %s,
                rows_duplicates = %s,
                rows_matched    = %s,
                rows_unmatched  = %s,
                status          = %s,
                error_detail    = %s
            WHERE id = %s
            """,
            (
                stats.get("rows_total", 0),
                stats.get("inserted", 0),
                stats.get("skipped", 0),
                stats.get("duplicates", 0),
                stats.get("matched", 0),
                stats.get("unmatched", 0),
                status,
                Json(error_detail, dumps=lambda o: json.dumps(o, ensure_ascii=False))
                    if error_detail is not None else None,
                batch_id,
            ),
        )
    conn.commit()


# ─── En-tête de page ──────────────────────────────────────────────────────────

_mode = st.session_state.import_mode
_badge = (
    "Import initial" if _mode == "initial"
    else "Import périodique" if _mode == "mensuel"
    else None
)
_badge_type = "cyan" if _mode == "initial" else "info"

page_header(
    title="Import",
    subtitle="Charger le fichier CSV de l'API",
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
        title="Import périodique courant",
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
        def _fmt(value) -> str:
            return value.strftime("%d/%m/%Y à %H:%M") if hasattr(value, "strftime") else str(value or "")

        status = existing_log.get("status")

        # 'running' est ambigu : import réellement en cours, ou tentative
        # interrompue (crash) qui n'a jamais atteint son statut final. Sans
        # timestamp de dernière activité, on tranche sur l'ancienneté de
        # started_at (cf. _RUNNING_STALE_MINUTES) plutôt que de bloquer
        # indéfiniment un fichier sur la base d'un import mort.
        if status == "running":
            started_at_raw = existing_log.get("started_at")
            is_stale = True
            if getattr(started_at_raw, "tzinfo", None) is not None:
                age = datetime.now(timezone.utc) - started_at_raw
                is_stale = age.total_seconds() > _RUNNING_STALE_MINUTES * 60
            if not is_stale:
                hash_check("dupe-running")
                alert(
                    f"Un import de ce fichier a démarré le "
                    f"<strong>{_fmt(started_at_raw)}</strong> et semble "
                    f"encore actif (moins de {_RUNNING_STALE_MINUTES} min). "
                    "Patientez qu'il se termine avant de relancer, pour "
                    "éviter un double import concurrent.",
                    type="warning",
                    title=f"Import déjà en cours — {existing_log.get('filename', uploaded.name)}",
                )
                st.stop()
            status = "running"  # traité comme "probablement interrompu" ci-dessous

        blocking = previous_import_alert(
            filename=existing_log.get("filename", uploaded.name),
            status=status,
            started_at=_fmt(existing_log.get("started_at")),
            finished_at=_fmt(existing_log.get("finished_at")),
            rows_inserted=existing_log.get("rows_inserted") or 0,
            rows_total=existing_log.get("rows_total") or 0,
            batch_id=str(existing_log.get("id", "")),
        )

        if blocking:
            hash_check("dupe-success")
            st.stop()
        else:
            hash_check("dupe-issue")
            st.session_state.step = max(st.session_state.step, 1)
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

# Aperçu LÉGER : seulement les 5 premières lignes + une estimation du total
# (comptage de sauts de ligne), jamais un chargement complet du fichier —
# un fichier de plusieurs centaines de milliers de lignes ne doit pas être
# entièrement parsé juste pour afficher un aperçu avant même de cliquer
# "Lancer l'import".
if st.session_state.preview_df is None:
    try:
        preview_df, total_rows_est = preview_csv(st.session_state.file_bytes, n=5)
        st.session_state.preview_df         = preview_df
        st.session_state.preview_total_rows = total_rows_est
    except ValueError as exc:
        alert(str(exc), type="error", title="Fichier invalide")
        st.stop()
    except Exception as exc:
        alert(f"Erreur inattendue lors de la lecture : {exc}", type="error")
        st.stop()

preview_df = st.session_state.preview_df
total_rows_est = st.session_state.preview_total_rows

col_info, col_preview = st.columns([1, 3])
with col_info:
    st.metric("Lignes détectées (estimation)", f"{total_rows_est:,}")
    st.metric("Colonnes", len(preview_df.columns))
    st.caption(f"Fichier : `{st.session_state.file_name}`")

with col_preview:
    st.markdown("**Aperçu — 5 premières lignes**")
    st.dataframe(preview_df.head(5), use_container_width=True, height=200)

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


class _SkipAccumulator:
    """Accumule les lignes ignorées SANS jamais garder plus de
    ``max_details`` entrées complètes en mémoire — même si la totalité
    d'un fichier de 190 000 lignes s'avérait invalide. Les totaux et la
    ventilation par code restent exacts (ce sont de simples compteurs,
    coût mémoire négligeable) ; seul le détail conservé pour l'affichage
    et l'export CSV est plafonné, exactement comme le fait déjà
    ``core.skip_report.build_error_detail`` au moment de la sérialisation
    — la différence ici est que le plafond est appliqué DÈS
    l'accumulation, pas seulement à la fin.
    """

    def __init__(self, max_details: int):
        self._max_details = max_details
        self.details: list[dict] = []
        self.codes: Counter = Counter()
        self.total = 0

    def extend(self, entries: list[dict]) -> None:
        for entry in entries:
            self.total += 1
            self.codes[entry.get("code") or "INCONNU"] += 1
            if len(self.details) < self._max_details:
                self.details.append(entry)


if not st.session_state.import_done:

    batch_id       = str(uuid.uuid4())
    mode           = st.session_state.import_mode
    fhash          = st.session_state.file_hash
    fname          = st.session_state.file_name
    file_bytes     = st.session_state.file_bytes
    total_rows_est = st.session_state.preview_total_rows or 1  # évite /0 dans le %

    st.session_state.batch_id = batch_id

    _import_cfg      = load_import_settings()
    CHUNK_SIZE        = _import_cfg.get("batch_size", 1000)
    MAX_SKIP_DETAILS  = _import_cfg.get("max_skip_details", 500)

    skip_acc          = _SkipAccumulator(MAX_SKIP_DETAILS)
    total_inserted     = 0
    total_duplicates   = 0
    total_seen          = 0
    pre_matched_count   = 0
    all_errors: list[str] = []
    last_logged_at       = 0

    # Placeholder pour la barre de progression
    progress_placeholder = st.empty()

    def _running_stats() -> dict:
        """Totaux courants — utilisée pour les mises à jour incrémentales
        ET pour le bilan final, afin qu'un crash en cours de route et une
        fin normale produisent des chiffres calculés de la même façon."""
        rows_unmatched = max(0, total_inserted - pre_matched_count)
        return {
            "rows_total":  total_seen,
            "inserted":    total_inserted,
            "skipped":     skip_acc.total,
            "duplicates":  total_duplicates,
            "matched":     total_inserted - rows_unmatched,
            "unmatched":   rows_unmatched,
        }

    def _running_error_payload() -> dict | None:
        return build_error_detail(
            skip_acc.details,
            all_errors,
            max_skip_details=MAX_SKIP_DETAILS,
            skips_total=skip_acc.total,
            skips_par_code=dict(skip_acc.codes),
        )

    t_start = time.time()

    try:
        # ── Créer le log d'import (rows_total = estimation, corrigée au fil de l'eau) ──
        with get_connection() as conn:
            _log_insert(conn, batch_id, fhash, fname, mode, total_rows_est)

        logger.info(
            "Import %s démarré — fichier=%s mode=%s ~%d ligne(s) (estimation)",
            batch_id, fname, mode, total_rows_est,
        )

        # ── Boucle streaming : un chunk à la fois, jamais le fichier entier ──
        # C'est le cœur du correctif OOM : chunk_df et rows ne contiennent
        # jamais plus de CHUNK_SIZE lignes, quelle que soit la taille totale
        # du fichier (189 824 lignes en production). apply_known_categories
        # et import_batch opèrent aussi chunk par chunk — comme avant pour
        # import_batch, mais désormais aussi pour la normalisation et
        # l'enrichissement catégories, qui construisaient auparavant une
        # liste Python de TOUTES les lignes avant le premier INSERT.
        chunks, bad_lines = iter_csv_chunks(file_bytes, chunksize=CHUNK_SIZE)

        with get_connection() as conn:
            for chunk_num, chunk_df in enumerate(chunks, start=1):
                chunk_len = len(chunk_df)

                rows, chunk_skips, _chunk_codes = normalize_batch(chunk_df, mode)
                skip_acc.extend(chunk_skips)

                try:
                    rows = apply_known_categories(conn, rows)
                except Exception as exc:
                    logger.warning(
                        "Enrichissement catégories impossible (chunk %d, batch %s) : %s",
                        chunk_num, batch_id, exc,
                    )

                pre_matched_count += sum(
                    1 for r in rows if r.get("categorie_interne") is not None
                )

                result = import_batch(conn, rows, batch_id)
                total_inserted   += result["inserted"]
                total_duplicates += result["duplicates"]
                all_errors.extend(result["errors"])

                total_seen += chunk_len
                del rows, chunk_df  # libère explicitement la mémoire du chunk traité

                # ── Barre de progression (UI) ────────────────────────────────
                pct = min(100, int(total_seen / total_rows_est * 100))
                with progress_placeholder.container():
                    progress_block(
                        title="Import en cours…",
                        subtitle=(
                            f"Traitement de {total_seen:,} ligne(s) "
                            f"(~{total_rows_est:,} au total) — chunk {chunk_num}"
                        ),
                        percent=pct,
                    )

                # ── Log de progression serveur (visible dans Streamlit Cloud) ──
                if total_seen - last_logged_at >= _PROGRESS_LOG_EVERY:
                    logger.info(
                        "Import %s : %d ligne(s) traitées — %d inséré(s), "
                        "%d doublon(s), %d ignorée(s)",
                        batch_id, total_seen, total_inserted,
                        total_duplicates, skip_acc.total,
                    )
                    last_logged_at = total_seen

                # ── Persistance incrémentale ──────────────────────────────────
                # Après CHAQUE chunk, pas seulement à la fin : si le process
                # est tué (OOM) avant le prochain chunk, import_logs reflète
                # déjà la progression et les erreurs réelles au lieu de
                # rester bloqué à zéro — consultable dans Outils / Logs même
                # après un crash partiel.
                try:
                    _log_progress(
                        conn, batch_id, _running_stats(), "running",
                        _running_error_payload(), final=False,
                    )
                except Exception as exc:
                    logger.error(
                        "Échec de la mise à jour incrémentale du log "
                        "(chunk %d, batch %s) : %s", chunk_num, batch_id, exc,
                        exc_info=True,
                    )

        # ── Bad lines détectées pendant le streaming (complètes seulement
        # maintenant que l'itérateur est épuisé) ────────────────────────────
        skip_acc.extend(bad_lines)

        t_elapsed = int(time.time() - t_start)

        final_stats  = _running_stats()
        final_stats["errors"] = all_errors
        final_status = (
            "success" if not all_errors
            else "partial" if total_inserted > 0
            else "error"
        )
        error_payload = _running_error_payload()
        st.session_state.error_detail_payload = error_payload

        # ── Mise à jour finale du log ─────────────────────────────────────────
        try:
            with get_connection() as conn:
                _log_progress(
                    conn, batch_id, final_stats, final_status,
                    error_payload, final=True,
                )
        except Exception as exc:
            logger.error(
                "Échec de la finalisation du log d'import %s : %s",
                batch_id, exc, exc_info=True,
            )
            alert(
                f"L'import a traité {total_seen:,} ligne(s) mais la mise à "
                f"jour finale du log a échoué : {exc}\n"
                "Vérifiez que la migration sql/migrations/"
                "0001_hash_et_tracabilite.sql a bien été appliquée sur "
                "cette base (colonne rows_duplicates, error_detail en "
                "JSONB) — c'est la cause la plus probable si le détail "
                "n'apparaît jamais dans Outils / Logs.",
                type="error",
                title="Log d'import non finalisé",
            )

        st.session_state.import_stats      = final_stats
        st.session_state.import_duration_s = t_elapsed
        st.session_state.import_done       = True
        st.session_state.skip_details      = skip_acc.details
        st.session_state.step              = 3

        logger.info(
            "Import %s terminé — %d ligne(s), %d inséré(s), %d doublon(s), "
            "%d ignorée(s), statut=%s, durée=%ds",
            batch_id, total_seen, total_inserted, total_duplicates,
            skip_acc.total, final_status, t_elapsed,
        )

        progress_placeholder.empty()
        st.rerun()

    except Exception as exc:
        # Préserve la progression déjà accumulée au lieu de l'écraser à
        # zéro — les chunks déjà insérés avant l'exception restent en base
        # de toute façon (chaque chunk est sa propre transaction commitée
        # dans import_batch), donc le log doit le refléter.
        logger.error("Import %s interrompu par une exception : %s", batch_id, exc, exc_info=True)
        all_errors.append(f"Import interrompu : {exc}")
        try:
            with get_connection() as conn:
                _log_progress(
                    conn, batch_id, _running_stats(), "error",
                    _running_error_payload(), final=True,
                )
        except Exception as log_exc:
            logger.error(
                "Impossible de marquer le log %s en erreur : %s",
                batch_id, log_exc, exc_info=True,
            )

        progress_placeholder.empty()
        st.session_state.import_error = str(exc)
        st.session_state.step = 1
        alert(
            f"Import échoué après {total_seen:,} ligne(s) traitée(s) : {exc}\n"
            "Les lignes déjà insérées avant l'échec restent en base "
            "(consultables dans Outils / Logs).",
            type="error",
            title="Erreur d'import",
        )
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
    rows_duplicates=stats.get("duplicates", 0),
)

render_duplicates_note(stats.get("duplicates", 0))

# Lignes ignorées et/ou erreurs de lot — un seul expander, replié par
# défaut (spec §2.7 : un import réussi ne doit pas ressembler à un écran
# d'erreur), qui délègue entièrement à render_skip_details (même
# composant que l'onglet Outils / Logs — pas de logique dupliquée, et pas
# de cas où des erreurs de lot sans skip associé restaient invisibles).
n_skipped     = stats.get("skipped", 0)
n_batch_errors = len(stats.get("errors") or [])
if n_skipped or n_batch_errors:
    label = (
        f"Voir les lignes ignorées ({n_skipped:,})" if n_skipped
        else f"⚠ {n_batch_errors} erreur(s) de lot"
    )
    with st.expander(label, expanded=False):
        render_skip_details(
            st.session_state.get("error_detail_payload"),
            key_prefix=f"import_{st.session_state.batch_id}",
        )

# Navigation vers Matching si des verbatims sont sans catégorie
if stats.get("unmatched", 0) > 0:
    st.markdown("")
    if st.button(
        "Aller au Matching catégories →",
        type="primary",
        key="goto_matching",
    ):
        st.switch_page("pages/2_Matching.py")

# Bouton pour recommencer un nouvel import
st.markdown("")
if st.button("Importer un autre fichier", key="reset_import"):
    for k, v in _DEFAULTS.items():
        st.session_state[k] = v
    st.rerun()
