# Sprint charte — Compass (compass_import)

**But :** Compass adopte la charte Consumer Voice (référence : `verbatims_run/outils/rr_gabarit.html`).
Lire d'abord `docs/charte/DESIGN_GUIDE.md`.
**Dépend de :** lot 0 (mot de passe sur toutes les pages), pour que l'ordre `set_page_config` →
`exiger_mot_de_passe` → `appliquer_charte` soit en place.
**Décision :** validée par l'utilisateur le 21/10/2026. La charte Compass actuelle (Inter, `#1F6ED4`, `#2EC4C7`,
barre latérale bleu nuit, bouton clair / sombre maison) est **remplacée**.

## Où vont les fichiers du kit
| Fichier du kit | Emplacement dans ce dépôt |
|---|---|
| `.streamlit/config.toml` | **racine du dépôt** `mvp_verbatim_import/.streamlit/config.toml` (Streamlit Cloud ne lit que celui-là) |
| `charte/` | `compass_import/charte/` (à côté de `compass_ui/`, déjà dans `sys.path` via `_ROOT`) |
| `charte_demo.py`, `tests/test_charte.py` | `compass_import/` et `compass_import/tests/` |
| `DESIGN_GUIDE.md`, `apercus/` | `compass_import/docs/charte/` |

## Stratégie : garder les noms, changer l'intérieur
Les pages appellent `compass_ui` à de nombreux endroits (`alert` ×28, `metric_row` ×7, `page_header`,
`hash_check`, `empty_state` ×5, `steps` ×3…). Pour limiter le risque, **on garde les signatures de
`compass_ui/compass_ui.py` et on réécrit leur contenu** avec les éléments natifs et le kit. Les pages changent peu.

| Fonction actuelle | Nouveau contenu |
|---|---|
| `inject_css()` | `charte.appliquer_charte()` |
| `theme_toggle()` | ne fait plus rien (le thème se choisit dans le menu Streamlit) ; retirer ses 4 appels |
| `sidebar_header()` | supprimer l'en-tête bleu nuit ; le nom de l'appli est dans le bandeau. Retirer les 5 appels |
| `sidebar_section(label)` | `st.sidebar.caption(label)` |
| `page_header(title, subtitle, badge, badge_type)` | `bandeau("Consumer Voice · Import Compass", title, sous_titre=subtitle, etiquette=badge)` |
| `metric_row(metrics)` | `st.columns` + `st.metric(label, value, border=True)` |
| `badge`, `status_badge`, `import_type_badge` | `badge_statut()` / `st.badge` natif (statuts d'import `running`, `success`, `partial`, `error`, `duplicate` déjà dans `charte.STATUTS`) |
| `alert(message, type, title)` | `st.info` / `st.success` / `st.warning` / `st.error` avec `icon=":material/…:"` ; titre en gras dans le message |
| `previous_import_alert`, `hash_check` | même logique (les 7 tests de `test_compass_ui.py` doivent rester verts) ; affichage par alertes natives |
| `steps(items, current)` | `charte.etapes(items, courante=current)` (vérifier si `current` commence à 0 ou à 1) |
| `progress_block(title, subtitle, percent)` | `st.progress(percent / 100, text=f"{title} — {subtitle}")` |
| `card_start` / `card_end` | `with charte.section(titre):` dans les pages (ces deux fonctions ne peuvent pas devenir un contexte) |
| `import_mode_toggle()` | `st.segmented_control` (deux options) |
| `log_table`, `product_status_table` | `st.dataframe(..., hide_index=True)` + `column_config` ; statuts via `libelle_statut()` |
| `import_summary`, `matching_summary` | `st.metric` + `encadre_cle()` pour la phrase de conclusion |
| `empty_state(icon, title, subtitle)` | `charte.vide(f"{title}. {subtitle}")` |
| `section_divider(label)` | `st.header(label)` ou `st.divider()` |
| `skip_table.py` | inchangé sur le fond ; vérifier qu'il n'utilise que des éléments natifs |

## Tâches
1. Copier le kit aux emplacements ci-dessus ; `requirements.txt` : `streamlit>=1.66`.
2. Réécrire `compass_ui/compass_ui.py` selon le tableau ; supprimer `style.css` et `style_v3.css` une fois plus
   aucune classe `compass-*` utilisée (`grep -r "compass-" compass_import/`).
3. Pages : `page_icon` en icône Material ; titres de sections en minuscules après la première lettre ; libellés
   de boutons « verbe + objet » ; plus d'emoji (`☀ Light` / `◑ Dark` disparaissent avec `theme_toggle`).
4. `tests/test_charte.py` : `PAGES = ["Home.py", "pages/1_Import.py", "pages/2_Matching.py", "pages/3_Outils.py", "pages/99_Admin.py"]`.
5. Contrôle visuel, clair et sombre, de chaque page avec un jeu de test (`scripts/seed_dev.py`) ; captures dans la pull request.

## Définition de terminé
- [ ] Toutes les pages : bandeau, sections, alertes natives, aucune classe `compass-*`, aucun emoji.
- [ ] `pytest` vert (tests existants + `test_charte.py`).
- [ ] Aucune différence de comportement (seule l'apparence change) ; captures avant / après dans la pull request.
