"""
compass_ui.py
─────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Composants UI réutilisables pour Streamlit.

Usage :
    from compass_ui import ui
    ui.inject_css()
    ui.page_header("Import mensuel", "Charger un fichier CSV")
    ui.metric_row([...])
"""

import re
import streamlit as st
from pathlib import Path
from datetime import datetime
from typing import Literal, Optional


# ─── Helper HTML ───────────────────────────────────────────────────────────────

def _html(markup: str) -> None:
    """Injecte du HTML via st.markdown en supprimant les lignes vides.

    Le parser CommonMark de Streamlit termine un bloc HTML de type 6
    (<div>, <table>, <span>…) à la première ligne vide ou ne contenant
    que des espaces. Ce helper élimine ces lignes avant l'injection pour
    éviter les rendus partiels où du HTML brut apparaît en clair.
    """
    cleaned = re.sub(r"\n[ \t]*\n", "\n", markup.strip())
    st.markdown(cleaned, unsafe_allow_html=True)


# ─── CSS injection ─────────────────────────────────────────────────────────────

def inject_css():
    """Injecte le design system Compass dans la page Streamlit.

    Note : Streamlit bloque les balises <script> dans st.markdown().
    Le JavaScript de restauration du thème est injecté via st.iframe()
    (iframe height=0) pour contourner cette limite.
    """
    css_path = Path(__file__).parent / "style.css"
    with open(css_path) as f:
        css = f.read()

    # CSS uniquement (les <script> sont stripés par Streamlit)
    st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

    # Restauration du thème depuis localStorage via une iframe height=0.
    # window.parent.document cible le document Streamlit parent.
    # height="content" → hauteur déterminée par le contenu rendu ;
    # un <script> seul ne produit aucun contenu visuel, donc l'iframe
    # n'occupe pas d'espace tout en exécutant le script.
    st.iframe(
        """
        <script>
        (function () {
            var theme = localStorage.getItem('compass_theme') || 'light';
            window.parent.document.documentElement.setAttribute('data-theme', theme);
        })();
        </script>
        """,
        height="content",
    )


def theme_toggle():
    """
    Bouton switch Light / Dark mode.
    À placer en haut de chaque page, avant le contenu.
    """
    st.markdown(
        '<button class="compass-theme-toggle" onclick="'
        "const html=document.documentElement;"
        "const cur=html.getAttribute('data-theme')||'light';"
        "const nxt=cur==='light'?'dark':'light';"
        "html.setAttribute('data-theme',nxt);"
        "localStorage.setItem('compass_theme',nxt);"
        "this.textContent=nxt==='dark'?'\u2600 Light':'\u25d1 Dark';"
        '">\u25d1 Dark</button>',
        unsafe_allow_html=True,
    )


# ─── Sidebar ────────────────────────────────────────────────────────────────────

def sidebar_header():
    """En-tête de la sidebar avec identité Compass."""
    st.sidebar.markdown(
        '<div class="compass-sidebar-logo">'
        '<div class="compass-sidebar-brand">Compass</div>'
        '<div class="compass-sidebar-title">Consumer Voice<br>Import Pipeline</div>'
        '<div class="compass-sidebar-version">v1.0</div>'
        "</div>",
        unsafe_allow_html=True,
    )


def sidebar_section(label: str):
    """Label de section dans la sidebar."""
    st.sidebar.markdown(
        f'<div class="compass-sidebar-section">{label}</div>',
        unsafe_allow_html=True,
    )


# ─── Page header ────────────────────────────────────────────────────────────────

def page_header(
    title: str,
    subtitle: str = "",
    badge: Optional[str] = None,
    badge_type: str = "cyan",
):
    """
    En-tête de page avec identité Compass.

    Args:
        title: Titre principal de la page
        subtitle: Sous-titre descriptif
        badge: Texte du badge optionnel (ex: "Import initial")
        badge_type: "success" | "error" | "warning" | "info" | "cyan" | "gray"
    """
    badge_html    = f'<span class="badge badge-{badge_type} badge-no-dot">{badge}</span>' if badge else ""
    subtitle_html = f'<p class="compass-subtitle">{subtitle}</p>' if subtitle else ""
    _html(
        f'<div class="compass-header">'
        f'<div class="compass-header-left">'
        f'<div class="compass-brand">Compass \u00b7 Consumer Voice</div>'
        f'<h1 class="compass-title">{title}</h1>'
        f"{subtitle_html}"
        f"</div>"
        f'<div class="compass-header-right">{badge_html}</div>'
        f"</div>"
    )


# ─── Metrics ────────────────────────────────────────────────────────────────────

def metric_row(metrics: list[dict]):
    """
    Ligne de cartes métriques.

    Args:
        metrics: Liste de dicts avec clés :
            - label (str)
            - value (str | int)
            - color (optionnel): "blue" | "success" | "warning" | "error" | "cyan"
            - delta (optionnel): str affiché en dessous
            - delta_dir (optionnel): "up" | "down"

    Exemple:
        ui.metric_row([
            {"label": "Verbatims insérés", "value": "42 156", "color": "success"},
            {"label": "Skipped", "value": "14", "color": "warning"},
            {"label": "Sans catégorie", "value": "87", "color": "error"},
        ])
    """
    color_map = {
        "blue":    "var(--c-blue)",
        "success": "var(--c-success)",
        "warning": "var(--c-warning)",
        "error":   "var(--c-error)",
        "cyan":    "var(--c-cyan)",
        "gray":    "var(--c-text-2)",
    }

    cards_html = ""
    for m in metrics:
        color = color_map.get(m.get("color", "blue"), color_map["blue"])
        delta_html = ""
        if m.get("delta"):
            dir_class  = m.get("delta_dir", "")
            symbol     = "↑" if dir_class == "up" else ("↓" if dir_class == "down" else "")
            delta_html = f'<div class="compass-metric-delta {dir_class}">{symbol} {m["delta"]}</div>'
        # Compact — pas de lignes vides même si delta_html est vide
        cards_html += (
            f'<div class="compass-metric">'
            f'<div class="compass-metric-value" style="color:{color}">{m["value"]}</div>'
            f'<div class="compass-metric-label">{m["label"]}</div>'
            f"{delta_html}"
            f"</div>"
        )

    _html(f'<div class="compass-metrics">{cards_html}</div>')


# ─── Badges ─────────────────────────────────────────────────────────────────────

def badge(text: str, type: str = "gray") -> str:
    """
    Retourne le HTML d'un badge inline.
    type: "success" | "error" | "warning" | "info" | "cyan" | "gray" | "duplicate"
    """
    return f'<span class="badge badge-{type}">{text}</span>'


def status_badge(status: str) -> str:
    """
    Badge de statut pour les logs d'import.
    status: "success" | "partial" | "error" | "duplicate"
    """
    config = {
        "success":   ("Succès",  "success"),
        "partial":   ("Partiel", "warning"),
        "error":     ("Erreur",  "error"),
        "duplicate": ("Doublon", "duplicate"),
    }
    label, type_ = config.get(status, (status, "gray"))
    return badge(label, type_)


def import_type_badge(type_: str) -> str:
    """Badge pour le type d'import."""
    config = {
        "initial": ("Import initial", "cyan"),
        "mensuel": ("Import mensuel", "info"),
    }
    label, btype = config.get(type_, (type_, "gray"))
    return badge(label, btype)


# ─── Alert blocks ───────────────────────────────────────────────────────────────

def alert(
    message: str,
    type: Literal["success", "error", "warning", "info", "duplicate"] = "info",
    title: Optional[str] = None,
):
    """
    Bloc d'alerte Compass avec bordure colorée.

    Args:
        message: Corps du message
        type: Type d'alerte
        title: Titre en gras optionnel
    """
    icons = {
        "success":   "✓",
        "error":     "✕",
        "warning":   "⚠",
        "info":      "ℹ",
        "duplicate": "⊘",
    }
    icon       = icons.get(type, "ℹ")
    title_html = f"<strong>{title}</strong>" if title else ""
    # Compact — title_html peut être vide, on évite une ligne blanche
    _html(
        f'<div class="compass-alert {type}">'
        f'<span class="compass-alert-icon">{icon}</span>'
        f'<div class="compass-alert-content">{title_html}{message}</div>'
        f"</div>"
    )


def duplicate_alert(filename: str, date: str, batch_id: str = ""):
    """Alerte spécifique doublon d'import."""
    batch_str = f" (batch <code>{batch_id[:8]}…</code>)" if batch_id else ""
    alert(
        message=(
            f"Ce fichier a déjà été importé le <strong>{date}</strong>"
            f"{batch_str}. L'import est bloqué."
        ),
        type="duplicate",
        title=f"Fichier déjà importé — {filename}",
    )


# ─── Hash check ─────────────────────────────────────────────────────────────────

def hash_check(
    status: Literal["ok", "dupe", "idle"],
    detail: str = "",
):
    """
    Indicateur de contrôle hash fichier.

    Args:
        status: "ok" = nouveau fichier | "dupe" = doublon | "idle" = en attente
        detail: Texte complémentaire (ex: date du premier import)
    """
    icons  = {"ok": "✓", "dupe": "⊘", "idle": "○"}
    labels = {
        "ok":   "Fichier nouveau — import autorisé",
        "dupe": "Fichier déjà importé — import bloqué",
        "idle": "En attente d'un fichier…",
    }
    icon  = icons[status]
    label = detail or labels[status]
    _html(
        f'<div class="compass-hash-check {status}">'
        f"<span>{icon}</span><span>{label}</span>"
        f"</div>"
    )


# ─── Step indicator ──────────────────────────────────────────────────────────────

def steps(items: list[str], current: int):
    """
    Indicateur d'étapes horizontal.

    Args:
        items: Liste des labels d'étapes
        current: Index de l'étape active (0-based)

    Exemple:
        ui.steps(["Upload", "Validation", "Import", "Résumé"], current=1)
    """
    steps_html = ""
    for i, label in enumerate(items):
        if i < current:
            cls, num = "done", "✓"
        elif i == current:
            cls, num = "active", str(i + 1)
        else:
            cls, num = "", str(i + 1)
        steps_html += (
            f'<div class="compass-step {cls}">'
            f'<div class="compass-step-num">{num}</div>'
            f"{label}</div>"
        )
    _html(f'<div class="compass-steps">{steps_html}</div>')


# ─── Progress block ──────────────────────────────────────────────────────────────

def progress_block(title: str, subtitle: str, percent: int):
    """
    Bloc de progression pendant un import.

    Args:
        title: Ex: "Import en cours…"
        subtitle: Ex: "Traitement de 42 000 / 500 000 lignes"
        percent: 0–100
    """
    _html(
        f'<div class="compass-progress-block">'
        f'<div class="compass-progress-title">{title}</div>'
        f'<div class="compass-progress-sub">{subtitle}</div>'
        f'<div class="compass-progress-bar-wrap">'
        f'<div class="compass-progress-bar" style="width:{percent}%"></div>'
        f"</div>"
        f'<div style="font-size:12px;color:var(--c-text-2)">{percent}%</div>'
        f"</div>"
    )


# ─── Section card ────────────────────────────────────────────────────────────────

def card_start(title: str, icon: str = ""):
    """Ouvre une carte section. Fermer avec card_end()."""
    icon_html = f'<span style="font-size:16px">{icon}</span>' if icon else ""
    _html(
        f'<div class="compass-card">'
        f'<div class="compass-card-title">{icon_html}{title}</div>'
    )


def card_end():
    """Ferme une carte section."""
    st.markdown("</div>", unsafe_allow_html=True)


# ─── Import mode toggle ──────────────────────────────────────────────────────────

def import_mode_toggle() -> str:
    """
    Toggle Import initial / Import mensuel.
    Retourne "initial" ou "mensuel".
    Utilise st.radio stylisé.
    """
    mode = st.radio(
        "Mode d'import",
        options=["Import mensuel", "Import initial (one-shot)"],
        horizontal=True,
        help=(
            "**Import mensuel** : fichier courant de l'API — "
            "catégorie, sous-catégorie et photo seront NULL.\n\n"
            "**Import initial** : fichier historique complet — "
            "tous les champs sont déjà remplis."
        ),
    )
    return "initial" if "initial" in mode else "mensuel"


# ─── Log table ───────────────────────────────────────────────────────────────────

def log_table(logs: list[dict]):
    """
    Tableau de logs d'import stylisé.

    Args:
        logs: Liste de dicts avec clés :
            started_at, filename, import_type, rows_inserted,
            rows_skipped, rows_matched, rows_unmatched, status, duration_s

    Exemple:
        ui.log_table([
            {
                "started_at": "2024-06-01 08:32",
                "filename": "aderma_june_2024.csv",
                "import_type": "mensuel",
                "rows_inserted": 42156,
                "rows_skipped": 14,
                "rows_matched": 41980,
                "rows_unmatched": 176,
                "status": "success",
                "duration_s": 127,
            }
        ])
    """
    if not logs:
        _html(
            '<div class="compass-card" style="text-align:center;padding:2.5rem">'
            '<div style="font-size:32px;margin-bottom:12px">📋</div>'
            '<div style="font-size:15px;font-weight:600;color:var(--c-deep)">Aucun import réalisé</div>'
            '<div style="font-size:13px;color:var(--c-text-2);margin-top:4px">Les logs apparaîtront ici après le premier import.</div>'
            "</div>"
        )
        return

    rows_html = ""
    for i, log in enumerate(logs):
        alt      = "alt" if i % 2 == 1 else ""
        duration = f"{log.get('duration_s', 0)}s"
        # Compact — une ligne par <tr> pour éviter les lignes vides
        rows_html += (
            f'<tr class="{alt}">'
            f'<td style="color:var(--c-text-2);white-space:nowrap">{log.get("started_at", "—")}</td>'
            f'<td style="font-weight:500">{log.get("filename", "—")}</td>'
            f"<td>{import_type_badge(log.get('import_type', ''))}</td>"
            f'<td style="text-align:right;color:var(--c-success)">{log.get("rows_inserted", 0):,}</td>'
            f'<td style="text-align:right;color:var(--c-warning)">{log.get("rows_skipped", 0):,}</td>'
            f'<td style="text-align:right">{log.get("rows_matched", 0):,}</td>'
            f'<td style="text-align:right;color:var(--c-error)">{log.get("rows_unmatched", 0):,}</td>'
            f"<td>{status_badge(log.get('status', ''))}</td>"
            f'<td style="color:var(--c-text-2)">{duration}</td>'
            f"</tr>"
        )

    _html(
        '<table class="compass-log-table">'
        "<thead><tr>"
        "<th>Date</th>"
        "<th>Fichier</th>"
        "<th>Type</th>"
        '<th style="text-align:right">Insérés</th>'
        '<th style="text-align:right">Skipped</th>'
        '<th style="text-align:right">Matchés</th>'
        '<th style="text-align:right">Sans catég.</th>'
        "<th>Statut</th>"
        "<th>Durée</th>"
        f"</tr></thead><tbody>{rows_html}</tbody></table>"
    )


# ─── Import summary ──────────────────────────────────────────────────────────────

def import_summary(
    rows_inserted: int,
    rows_skipped: int,
    rows_matched: int,
    rows_unmatched: int,
    duration_s: int = 0,
):
    """
    Résumé post-import avec métriques colorées.
    """
    metric_row([
        {"label": "Verbatims insérés", "value": f"{rows_inserted:,}",  "color": "success"},
        {"label": "Lignes skippées",   "value": f"{rows_skipped:,}",   "color": "warning" if rows_skipped else "gray"},
        {"label": "Avec catégorie",    "value": f"{rows_matched:,}",   "color": "blue"},
        {"label": "Sans catégorie",    "value": f"{rows_unmatched:,}", "color": "error" if rows_unmatched else "gray"},
    ])
    if duration_s:
        _html(f'<p style="font-size:12px;color:var(--c-text-2);margin-top:4px">⏱ Import terminé en {duration_s}s</p>')
    if rows_unmatched > 0:
        alert(
            message=(
                f"<strong>{rows_unmatched:,} verbatims</strong> n'ont pas de catégorie. "
                "Utilisez le module <strong>Matching catégories</strong> pour les compléter."
            ),
            type="warning",
        )


# ─── Matching summary ────────────────────────────────────────────────────────────

def matching_summary(products_matched: int, verbatims_updated: int):
    """Résumé post-matching."""
    metric_row([
        {"label": "Produits matchés",     "value": str(products_matched),    "color": "success"},
        {"label": "Verbatims mis à jour", "value": f"{verbatims_updated:,}", "color": "blue"},
    ])


# ─── Product table (matching) ────────────────────────────────────────────────────

def product_status_table(products: list[dict]):
    """
    Tableau des produits avec leur statut de matching.

    Args:
        products: List de dicts :
            product_name, nb_verbatims, categorie_interne, sous_categorie_interne, photo
    """
    if not products:
        return

    rows_html = ""
    for i, p in enumerate(products):
        alt    = "alt" if i % 2 == 1 else ""
        cat    = p.get("categorie_interne") or ""
        sous   = p.get("sous_categorie_interne") or ""
        photo  = p.get("photo")

        status_html = badge("Matchée", "success") if cat else badge("À compléter", "warning")
        photo_html  = (
            badge("Oui", "cyan") if photo is True
            else badge("Non", "gray") if photo is False
            else badge("—", "gray")
        )
        cat_html  = cat  or '<span style="color:var(--c-text-3)">—</span>'
        sous_html = sous or '<span style="color:var(--c-text-3)">—</span>'
        name      = p.get("product_name", "")

        # Compact — pas de lignes vides même avec de longues valeurs
        rows_html += (
            f'<tr class="{alt}">'
            f'<td style="font-weight:500;max-width:260px;overflow:hidden;'
            f'text-overflow:ellipsis;white-space:nowrap" title="{name}">{name}</td>'
            f'<td style="text-align:right">{p.get("nb_verbatims", 0):,}</td>'
            f"<td>{cat_html}</td>"
            f"<td>{sous_html}</td>"
            f"<td>{photo_html}</td>"
            f"<td>{status_html}</td>"
            f"</tr>"
        )

    _html(
        '<table class="compass-log-table">'
        "<thead><tr>"
        "<th>Produit</th>"
        '<th style="text-align:right">Verbatims</th>'
        "<th>Catégorie</th>"
        "<th>Sous-catégorie</th>"
        "<th>Photo</th>"
        "<th>Statut</th>"
        f"</tr></thead><tbody>{rows_html}</tbody></table>"
    )


# ─── Empty state ─────────────────────────────────────────────────────────────────

def empty_state(icon: str, title: str, subtitle: str = ""):
    """État vide générique."""
    subtitle_html = f'<div style="font-size:13px;color:var(--c-text-2)">{subtitle}</div>' if subtitle else ""
    _html(
        '<div class="compass-card" style="text-align:center;padding:3rem 2rem">'
        f'<div style="font-size:40px;margin-bottom:16px">{icon}</div>'
        f'<div style="font-size:16px;font-weight:600;color:var(--c-deep);margin-bottom:6px">{title}</div>'
        f"{subtitle_html}"
        "</div>"
    )


# ─── Divider with label ───────────────────────────────────────────────────────────

def section_divider(label: str = ""):
    """Séparateur de section avec label optionnel."""
    if label:
        _html(
            '<div style="display:flex;align-items:center;gap:12px;margin:1.5rem 0">'
            '<div style="flex:1;height:1px;background:var(--c-border)"></div>'
            f'<span style="font-size:11px;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;color:var(--c-text-2)">{label}</span>'
            '<div style="flex:1;height:1px;background:var(--c-border)"></div>'
            "</div>"
        )
    else:
        st.markdown("<hr>", unsafe_allow_html=True)
