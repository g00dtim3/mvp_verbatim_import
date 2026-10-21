# Sprint charte — Collecte Semantiweb (api-explorer)

**But :** api-explorer adopte la charte Consumer Voice (référence : `verbatims_run/outils/rr_gabarit.html`).
Lire d'abord `docs/charte/DESIGN_GUIDE.md`.
**Dépend de :** lot 0 (fusion de `feature/bulk-export-v2` dans `main`). Partir d'une nouvelle branche depuis `main`.
**Décision :** validée par l'utilisateur le 21/10/2026.

## Contraintes propres à ce dépôt
- **Règle n°1 du `CLAUDE.md` : ne jamais modifier `module3_bulk_export.py`.** Le thème (`config.toml`) s'y applique
  tout seul (police, couleurs, boutons, tableaux). Son `st.title` et ses emojis restent tant que l'utilisateur
  n'a pas explicitement autorisé une modification de ce fichier. Le signaler dans la pull request.
- `main_app.py` affiche tous les modules dans une seule page (navigation par `st.sidebar.radio`).
- Aucune modification de logique : seule l'apparence change. Les 284 tests (au 19/10/2026) restent verts.

## Où vont les fichiers du kit
`.streamlit/config.toml` à la racine du dépôt ; `charte/`, `charte_demo.py` à la racine ; `tests/test_charte.py` ;
`DESIGN_GUIDE.md` et `apercus/` dans `docs/charte/`. `requirements.txt` : `streamlit>=1.66`
(aujourd'hui `>=1.28.0`) ; garder `pandas<3`.

## Tâches
1. `main_app.py` :
   - `set_page_config(page_title="Collecte Semantiweb", page_icon=":material/travel_explore:", layout="wide", …)` ;
     `appliquer_charte()` juste après `exiger_mot_de_passe()`.
   - Remplacer `st.title("🔍 Explorateur API Ratings & Reviews")` et la ligne en gras qui suit par
     `bandeau("Consumer Voice · Collecte Semantiweb", <nom du module choisi>, sous_titre=<une phrase>)`.
     Pour le module 3 (non modifiable), ne pas afficher de bandeau, pour éviter un double titre.
   - Navigation : libellés sans emoji (« Explorateur API », « Export manuel », « Export bulk (V1) »,
     « Bulk Export V2 », « Journal des exports ») ; `st.sidebar.title("🧭 Navigation")` → `st.sidebar.header("Navigation")`.
   - Pages « Guide des modules » / « Workflow recommandé » : `section()` + texte, sans emoji.
2. `module1_explorer.py`, `module2_manual_export.py`, `module4_bulk_export_v2.py` : retirer leur `st.title`
   (le bandeau de `main_app.py` le remplace), titres en `st.header` dans des `section()`, emojis retirés.
3. Bulk Export V2 (`module4`) — usage des composants :
   - statuts des exports (`PENDING` … `COMPLETED`) : `badge_statut()` dans les fiches, `libelle_statut()` dans les tableaux ;
   - onglet Export Monitor : `st.metric(border=True)` pour attendu / reçus / uniques / pages ; `st.progress(text=…)` ;
   - « Stop after current batch » et « Reprendre » : un seul bouton `type="primary"` visible à la fois ;
   - erreurs bloquantes : `st.error` avec ce qui s'est passé + quoi faire ;
   - parcours New Export : `etapes(["Configurer", "Estimer", "Lancer"], courante=…)`.
4. `tests/test_charte.py` : `PAGES = ["main_app.py"]` et
   `MODULES = ["module1_explorer.py", "module2_manual_export.py", "module4_bulk_export_v2.py"]`
   (le module 3 n'y figure pas : il n'est pas modifiable).
5. Contrôle visuel, clair et sombre, des 5 entrées du menu (module 3 compris) ; captures dans la pull request.

## Définition de terminé
- [ ] Bandeau sur chaque module sauf le module 3 ; aucun emoji hors du module 3.
- [ ] `pytest` vert, anciens tests compris ; `module3_bulk_export.py` inchangé (`git diff --stat` le prouve).
- [ ] `JOURNAL.md` mis à jour ; captures avant / après dans la pull request.
