# Journal du chantier « gardening » — Compass (mvp_verbatim_import)

## 21/10/2026 — Lot 0 : mot de passe sur toutes les pages

**Décision (validée par l'utilisateur le 21/10/2026)** : Compass partage désormais la base Supabase de
`bulk_export`. Toutes les pages exigent un mot de passe. Cette décision remplace la ligne
« Utilisateurs — pas de gestion d'authentification » de `claude_code_package/SPECS_v1.2.md`.

**Fait**
- `Home.py`, `pages/1_Import.py`, `pages/2_Matching.py`, `pages/3_Outils.py`, `pages/99_Admin.py` :
  appel de `exiger_mot_de_passe()` juste après `st.set_page_config(...)`, avant `inject_css()` et toute
  connexion à la base. `auth.py` n'a pas été modifié.
- `tests/test_pages_protegees.py` : lit le texte de `Home.py` et de chaque `pages/*.py` (analyse `ast`,
  sans lancer Streamlit) et vérifie que l'instruction suivant `st.set_page_config(...)` est
  `exiger_mot_de_passe()`. Contrôlé : le test devient rouge si l'on retire l'appel d'une page.
- `tests/test_outils_t4_cache.py` : ce test charge `pages/3_Outils.py` hors Streamlit pour tester une
  fonction pure. L'écran de mot de passe y est neutralisé pendant le chargement ; sinon son `st.form`
  restait ouvert et faisait échouer `tests/test_skip_table.py` (2 tests) lancé ensuite.
- `.env.example` : `APP_PASSWORD=` (au moins 16 caractères, jamais dans Git).
- `README.md` (racine du dépôt, il n'y en a pas dans `compass_import/`) : section « Accès ».

**Tests** : `test_auth.py` 6/6 verts, nouveau test vert, 203 tests verts au total.

**Points ouverts**
- 4 tests `tests/test_importer.py::TestApplyKnownCategories` échouaient **déjà avant ce lot** (sur `main`) :
  `apply_known_categories` lit une 5e colonne `photo` que les fausses lignes de ces tests ne fournissent
  pas (`IndexError`). Hors périmètre du lot 0 ; à corriger dans un lot dédié.
- À faire par l'utilisateur après fusion : renseigner `APP_PASSWORD` dans les Secrets Streamlit, puis
  vérifier en fenêtre privée que seul l'écran « Accès protégé » apparaît, sur chaque page.
