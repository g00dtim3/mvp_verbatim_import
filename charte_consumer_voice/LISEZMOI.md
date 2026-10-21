# Charte Consumer Voice — kit et design guide (v1.0, 21/10/2026)

Une seule apparence pour `api-explorer`, `mvp_verbatim_import` (Compass) et `rddt_connector`, calquée sur le
tableau de bord Ratings & Reviews (`verbatims_run/outils/rr_gabarit.html`).

## Contenu
| Élément | Rôle |
|---|---|
| `DESIGN_GUIDE.md` | Le guide que Claude Code lit avant de toucher à une interface : principes, jetons, quel composant pour quel besoin, rédaction, accessibilité, à éviter |
| `apercus/` | Captures de la page de démonstration : clair, sombre, écran étroit |
| `kit/` | Les fichiers à recopier **à l'identique** dans les trois dépôts : thème Streamlit, `charte/` (jetons, CSS, composants), page de démonstration, tests |
| `sprints/<dépôt>/SPRINT_charte.md` | La migration propre à chaque application |

Déjà testé ici : thème = jetons, contrastes WCAG AA en clair et en sombre (toutes les paires utilisées),
échappement du texte dans chaque composant, rendu de la page de démonstration (Streamlit 1.66, clair, sombre, 420 px).

## Installer dans un dépôt
1. Copier `kit/.streamlit/config.toml` à la **racine du dépôt** (`.streamlit/config.toml`).
2. Copier `kit/charte/`, `kit/charte_demo.py` et `kit/tests/test_charte.py` à côté du code de l'application
   (pour Compass : dans `compass_import/`).
3. Copier `DESIGN_GUIDE.md`, `apercus/` et le `SPRINT_charte.md` du dépôt dans `docs/charte/`.
4. Ouvrir une session Claude Code sur le dépôt : « Lis `docs/charte/DESIGN_GUIDE.md` puis
   `docs/charte/SPRINT_charte.md`, propose un plan, attends ma validation, puis code. »

## Ordre conseillé
1. **Reddit** en premier : une seule page, aucune ancienne charte branchée. C'est le pilote.
2. **api-explorer** ensuite, après la fusion dans `main` (lot 0) ; le module 3 garde son titre tant que vous
   n'autorisez pas sa modification.
3. **Compass** en dernier : c'est le plus de code d'interface (`compass_ui.py`, 681 lignes). On garde les noms des
   fonctions et on change leur contenu.

Chaque sprint suppose le mot de passe du lot 0 déjà en place (ordre : `set_page_config` → `exiger_mot_de_passe` →
`appliquer_charte`).

## Faire évoluer la charte
Modifier `kit/`, incrémenter `charte.VERSION`, relancer `pytest kit/tests` et `streamlit run kit/charte_demo.py`,
puis recopier les mêmes fichiers dans les trois dépôts. Jamais de modification dans une seule application.
