# Lot 0 — Mot de passe sur toutes les pages de Compass

**But :** plus aucune page de Compass ne s'affiche, ne lit ni n'écrit dans la base sans le mot de passe.
**Pourquoi maintenant :** Compass partage désormais la base Supabase de `bulk_export` (schémas `public` et
`bulk_export`). La spec v1.2 avait retenu « application interne, pas d'authentification ». Si l'URL Streamlit
est publique, n'importe qui peut aujourd'hui importer, modifier le matching ou utiliser la page Admin.
**Décision :** validée par l'utilisateur le 21/10/2026 (chantier « gardening », lot 0). Elle remplace la ligne
« Utilisateurs — pas de gestion d'authentification » de `claude_code_package/SPECS_v1.2.md`.

## Ce qui est fourni
- `auth.py` (racine de `compass_import/`) : module **déjà écrit et testé**, repris d'`api-explorer/auth.py`
  (même comportement : 5 essais puis blocage 5 min, mot de passe d'au moins 16 caractères, comparaison
  `hmac.compare_digest`, saisie effacée de la session). Ne pas le réécrire.
- `tests/test_auth.py` : 6 tests (page factice, sans base).

## Tâches
1. Appeler `exiger_mot_de_passe()` **juste après `st.set_page_config(...)`**, avant `inject_css()` et avant
   toute connexion à la base, dans **chacune** de ces pages (Streamlit exécute chaque page séparément) :
   - `Home.py` (ligne ~31)
   - `pages/1_Import.py` (ligne ~96)
   - `pages/2_Matching.py` (ligne ~46)
   - `pages/3_Outils.py` (ligne ~66)
   - `pages/99_Admin.py` (ligne ~21)
   ```python
   from auth import exiger_mot_de_passe
   st.set_page_config(...)
   exiger_mot_de_passe()      # rien ne s'exécute après tant que le mot de passe n'est pas saisi
   inject_css()
   ```
   La session est partagée entre les pages : on ne saisit le mot de passe qu'une fois.
2. Ajouter un test qui vérifie que `Home.py` et **chaque** fichier `pages/*.py` appelle
   `exiger_mot_de_passe()` dans l'instruction qui suit `st.set_page_config(...)` (lecture du texte des fichiers,
   sans lancer Streamlit). Une page ajoutée plus tard sans protection fera échouer ce test.
   Vérifié le 21/10/2026 : aucune page n'ouvre de connexion avant `st.set_page_config`.
3. `.env.example` : ajouter `APP_PASSWORD=` avec un commentaire (« au moins 16 caractères, jamais dans Git »).
4. `README.md` : une section « Accès » de trois lignes (où se règle le mot de passe, comment le changer).

## Hors périmètre
- Comptes individuels, rôles, connexion Google : plus tard (lot 7, portail unifié).
- Toute autre modification des pages.

## Tests
- `pytest tests/test_auth.py` : 6 tests verts.
- Le nouveau test « toutes les pages sont protégées » est vert, et devient rouge si l'on retire l'appel d'une page.
- Tous les tests existants restent verts.

## À faire par l'utilisateur (après fusion)
- Streamlit Cloud (ou Posit) → Settings → Secrets, **en haut du fichier** :
  `APP_PASSWORD = "…"` (au moins 16 caractères, aléatoire, depuis un gestionnaire de mots de passe).
- Ouvrir l'application dans une fenêtre privée : seul l'écran « Accès protégé » doit apparaître, sur chaque page.

## Définition de terminé
- [ ] Les 5 pages appellent `exiger_mot_de_passe()` juste après `st.set_page_config`.
- [ ] Tous les tests passent, anciens compris.
- [ ] Journal mis à jour (ce qui est fait, décision du 21/10/2026), un commit sur une branche, une pull request.
