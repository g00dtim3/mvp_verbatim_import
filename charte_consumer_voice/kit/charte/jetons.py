"""
Jetons de la charte Consumer Voice — source unique des couleurs, tailles et polices.

Repris de `verbatims_run/outils/rr_gabarit.html` (charte « Medical Experts Mapping », bleus et cyan),
qui fait référence. Les mêmes valeurs sont écrites dans `.streamlit/config.toml` ; le test
`tests/test_charte.py` vérifie que les deux restent identiques.

Règle de contraste reprise du gabarit : le cyan de la charte (#1ca9c9) n'atteint pas 3:1 avec du texte
blanc. Il sert de liseré et d'accent ; toute surface qui porte du texte blanc utilise ACCENT_INK (5,4:1).
"""

POLICE = "Atkinson Hyperlegible Next"
POLICE_URL = ("https://fonts.googleapis.com/css2?family=Atkinson+Hyperlegible+Next:"
              "wght@400;500;700;800&display=swap")
POLICE_PILE = f'"{POLICE}", "Atkinson Hyperlegible", system-ui, -apple-system, "Segoe UI", Arial, sans-serif'

CLAIR = {
    # Surfaces et texte
    "page": "#ffffff",          # fond de l'application (Streamlit backgroundColor)
    "panel": "#f8f9fa",         # champs (secondaryBackgroundColor)
    "sidebar": "#f8f9fa",       # barre latérale
    "panel-2": "#e9ecef",       # badge neutre, séparateurs appuyés
    "ink": "#212529",           # texte principal
    "ink-2": "#495057",         # texte secondaire, libellés
    "ink-3": "#6c757d",         # texte discret (méta, aide)
    "line": "#dee2e6",          # bordures
    "field": "#868e96",         # bordure des champs
    # Marque
    "brand-deep": "#024fad",    # début du dégradé du bandeau
    "brand": "#065cb1",         # en-têtes de tableaux, bleu principal
    "brand-mid": "#0a74b9",     # fin du dégradé du bandeau
    "brand-avg": "#0a3d7a",     # colonne de moyenne / référence
    "brand-sub": "#d6e4f5",     # texte secondaire sur fond bleu
    "bench": "#5c636a",         # références, bouton secondaire
    "accent": "#1ca9c9",        # liseré, accent (jamais sous du texte blanc)
    "accent-ink": "#0e7490",    # action principale, onglet actif (texte blanc OK)
    "accent-ink-hover": "#0b5f77",
    "accent-soft": "#e3f5f9",   # fond de l'encadré « à retenir », survol
    "focus": "#0d6efd",         # contour de focus, liens
    # Données (palette dataviz validée du gabarit)
    "pos": "#2a78d6",           # positif
    "mix": "#c4c2ba",           # neutre / mitigé
    "neg": "#e34948",           # négatif
    "above": "#198754",         # au-dessus de la référence
    "below": "#a8421c",         # en dessous de la référence
    # États (alertes)
    "warn-bg": "#fff4d6",
    "warn-ink": "#6b4a00",
    "sel": "#e7f1ff",
    # Compléments (absents du gabarit, choisis dans la même famille Bootstrap 5)
    "warn": "#b58105",
    "ok-bg": "#d1e7dd",
    "ok-ink": "#0a3622",
    "err-bg": "#f8d7da",
    "err-ink": "#58151c",
    "info-ink": "#0a3d7a",
}

SOMBRE = {
    **CLAIR,
    "page": "#12151d",
    "panel": "#1d2230",
    "sidebar": "#0f1218",
    "panel-2": "#262c3b",
    "ink": "#e9ecef",
    "ink-2": "#adb5bd",
    "ink-3": "#959da6",
    "line": "#2c3343",
    "field": "#7a828b",
    "brand": "#0b3f80",
    "brand-avg": "#082d5c",
    "brand-sub": "#c9dbf3",
    "bench": "#454b52",
    "accent-soft": "#11323b",
    "focus": "#7cc7ff",
    "pos": "#3987e5",
    "mix": "#5d5c57",
    "neg": "#e66767",
    "above": "#6fcf8a",
    "below": "#f2a07d",
    "warn-bg": "#3a2f10",
    "warn-ink": "#f6d58a",
    "sel": "#1b2b47",
    "warn": "#e0b341",
    "ok-bg": "#12301f",
    "ok-ink": "#a3e0b8",
    "err-bg": "#3b1619",
    "err-ink": "#f5b5b9",
    "info-ink": "#c9dbf3",
}

RAYON_S = "4px"   # champs, boutons, badges carrés
RAYON_M = "6px"   # cartes, tableaux


def variables_css(theme: str = "light") -> str:
    """Bloc `:root { --cv-… }` pour le thème demandé ("light" ou "dark")."""
    jetons = SOMBRE if theme == "dark" else CLAIR
    lignes = [f"  --cv-{nom}: {valeur};" for nom, valeur in jetons.items()]
    lignes += [f"  --cv-radius-s: {RAYON_S};", f"  --cv-radius-m: {RAYON_M};",
               f"  --cv-font: {POLICE_PILE};"]
    return ":root {\n" + "\n".join(lignes) + "\n}"
