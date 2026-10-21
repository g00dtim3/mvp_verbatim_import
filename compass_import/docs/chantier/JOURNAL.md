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

## 21/10/2026 — Lot 1B : importer un export validé d'api-explorer, sans téléchargement

**Décision (validée par l'utilisateur le 21/10/2026)** : option A — lire le fichier plat déjà validé dans
Supabase Storage (`bulk_export.files`, `kind = 'csv_plat'`), sans reconvertir le brut.
**Contrainte `guid`** : la requête lancée par l'utilisateur renvoie `verbatims_guid_key` → migration `0003`.

**Fait**
- `config.toml` : `[import] alias_colonnes = { Sampling = "sampling" }` (liste fermée), appliqué juste après chaque
  lecture (`parse_csv`, `preview_csv`, chaque morceau d'`iter_csv_chunks`), pour les deux parcours.
- `core/importer.py` : `preview_csv`, `iter_csv_chunks` et `estimate_row_count` acceptent un **chemin** en plus des
  octets (lecture par morceaux) ; le lecteur pandas est fermé en fin de lecture (fichier temporaire supprimable).
- `core/exports_source.py` (nouveau, lecture seule du schéma `bulk_export`) : `lister_exports()` (anti-doublon via
  `is_file_already_imported()`), `telecharger()` (flux par blocs de 1 Mo, SHA-256 vérifié, fichier supprimé en cas
  d'écart ou d'erreur, clé masquée), `nettoyer_dossier_temp()` (fichiers abandonnés de plus de 12 h).
- `pages/1_Import.py` : étape 1 à deux choix ; liste des exports, déjà importés grisés avec l'explication habituelle ;
  contrôle anti-doublon **avant** téléchargement ; `filename = "<nom> (<export_id>)"`, `file_hash = files.sha256`,
  `source_export_id` ; nombre de lignes exact (`files.rows_count`) ; fichier temporaire supprimé après l'import.
  Le dépôt manuel est inchangé (même code, déplacé dans une fonction commune `_controle_anti_doublon`).
- **Correctif au passage** : après un import réussi, le rechargement de la page retrouvait cet import en base et
  affichait « déjà importé » au lieu du résumé. Le contrôle ignore désormais l'import lancé par la session elle-même.
- Migrations rejouables : `0002_source_export.sql` (`import_logs.source_export_id`) et `0003_guid_non_unique.sql`
  (contrainte `verbatims_guid_key` remplacée par l'index `idx_verbatims_guid`) ; `schema.sql` aligné.
- `.env.example` : `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `BULK_EXPORT_BUCKET` ; `requirements.txt` : `requests`.

**Tests** : `tests/test_import_depuis_export.py` (22 tests : Sampling, `id` = `compass_id` d'api-explorer, chemin =
octets, téléchargement avec faux Storage, configuration, `lister_exports()` sur une vraie base PostgreSQL de test via
`COMPASS_TEST_DB_URL` — ignoré si absente). 225 tests verts ; 4 rouges déjà présents avant (voir lot 0).

**Vérifié hors tests (base PostgreSQL locale avec les vraies migrations d'api-explorer, faux Storage HTTP)**
- Vraie page Import : export importé de bout en bout (`success`, `inserted + duplicates + skipped = rows_total`),
  second clic refusé (bouton grisé), fichier temporaire supprimé, résumé toujours affiché après rechargement.
- Avant `0002` : message clair, rien en base. Avec `verbatims_guid_key` : **même la fixture seule échoue** (elle
  contient deux fois `guid = 1`, comme les vrais exports) ; après `0003`, import réussi.
- Mémoire : pic stable (~114 Mo) de 5 000 à 500 000 lignes synthétiques.

**Points ouverts**
- Place disque de Streamlit Cloud pour un fichier temporaire d'≈ 0,5 Go : non vérifiée, à observer au premier gros import.
- Un export plat sans `sha256` dans `bulk_export.files` n'est pas listé (impossible à vérifier).
