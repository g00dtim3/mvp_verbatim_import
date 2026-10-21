# Lot 1B — Importer un export validé d'api-explorer, sans téléchargement

**But :** dans la page Import, choisir un export Semantiweb validé par api-explorer et l'importer directement,
au lieu de télécharger `export_plat.csv` puis de le redéposer.
**Dépend de :** `SPRINT_L0_mot_de_passe.md` (même dépôt) et, côté api-explorer, d'exports produits avec le format
« plat » (case cochée par défaut depuis le 19/10/2026).
**Décision :** validée par l'utilisateur le 21/10/2026 (chantier « gardening », lot 1). Option retenue :
**lire le fichier plat déjà validé dans Supabase Storage** (option A), plutôt que reconvertir le brut
(option B). C'est exactement le parcours manuel actuel, sans l'étape « télécharger / redéposer » ; aucune
logique de conversion n'est recopiée.

## Contexte vérifié le 21/10/2026
- api-explorer et Compass sont dans **le même projet Supabase** : schéma `bulk_export` (api-explorer) et
  `public` (Compass). Compass peut donc lire `bulk_export.exports` et `bulk_export.files` en SQL.
- Pour un export COMPLETED, la ligne `bulk_export.files` de `kind = 'csv_plat'` donne le chemin dans Storage
  (`<export_id>/output/export_plat.csv`), son nombre de lignes et son **SHA-256 calculé sur les octets du
  fichier** : c'est exactement ce que calcule `core.hasher.file_hash()`. Le contrôle anti-doublon de Compass
  (`import_logs.file_hash`) fonctionne donc tel quel.
- **Bloquant actuel :** le format plat écrit la colonne `Sampling` (majuscule), alors que `config.toml`
  exige `sampling`. Constaté en faisant passer un faux fichier plat dans `core.importer.parse_csv()` :
  « Colonnes obligatoires manquantes dans le fichier CSV : sampling ». Une fois la colonne renommée, le fichier
  est accepté et l'`id` calculé par Compass est **identique** au `compass_id` d'api-explorer
  (`d051dfcd…c112` des deux côtés sur la mention de test).
- Le format plat contient aussi deux colonnes nommées `safety` (vide, héritée de `postprocess_reviews()`).
  pandas les lit comme `safety` et `safety.1` ; Compass ne s'en sert pas. Ne rien faire, mais ne pas s'en étonner.

## Tâches
1. **Accepter `Sampling`** — à la lecture (les deux parcours, upload et export), renommer les en-têtes
   connus dont seule la casse diffère : `Sampling` → `sampling`. Liste fermée dans `config.toml`
   (`[import] alias_colonnes = { Sampling = "sampling" }`), pas de mise en minuscules générale
   (`product_name_SEMANTIWEB` et `attribute_*` ont des majuscules voulues).
2. **Lire depuis Storage** — `core/exports_source.py` (nouveau) :
   - `lister_exports(conn)` : exports `status = 'COMPLETED'` avec un fichier `csv_plat` et
     `files_expired_at IS NULL`, du plus récent au plus ancien : `export_id`, `name`, `finished_at`,
     `rows_count`, `size_bytes`, `sha256`, `path`, et **déjà importé ?** (jointure sur
     `import_logs.file_hash = files.sha256`, en réutilisant la règle de `is_file_already_imported()`).
   - `telecharger(path, sha256_attendu, dossier_temp)` : lecture **en flux** (blocs de 1 Mo) vers un fichier
     temporaire, SHA-256 calculé au passage ; empreinte différente → erreur explicite, fichier supprimé.
     Même requête qu'api-explorer (`bulk_export/storage.py`, `SupabaseStorage.iter_bytes`) :
     `GET {supabase_url}/storage/v1/object/authenticated/{bucket}/{path}`, en-têtes `apikey: <clé>` et, si la clé
     commence par `eyJ`, `Authorization: Bearer <clé>`. Clé `service_role` « Legacy » : la nouvelle clé
     `sb_secret_…` est refusée par Storage (constaté le 15/10/2026 côté api-explorer).
   - Paramètres, lus comme le reste de la configuration Compass (`core/db.py`, variables d'environnement /
     `.env`) : `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `BULK_EXPORT_BUCKET` (défaut `bulk-exports`).
     Ajouter les trois lignes, vides, à `.env.example`. Ne jamais afficher la clé (messages d'erreur compris).
3. **Lecture en flux pour les gros fichiers** — `iter_csv_chunks()` accepte aujourd'hui des `bytes`
   (`BytesIO(file_bytes)`). Lui faire accepter aussi un **chemin de fichier** (lu par pandas par morceaux,
   sans tout charger). Un export de 500 000 mentions fait ≈ 0,5 Go : il ne doit jamais être entièrement en mémoire
   (cf. le plantage à 189 824 lignes déjà corrigé côté traitement). La limite `max_file_size_mb = 200` ne
   s'applique qu'au dépôt manuel.
4. **Page Import** — à l'étape 1, deux choix :
   « Depuis un export validé (recommandé) » | « Déposer un fichier CSV » (parcours actuel, inchangé).
   - Le premier affiche la liste de `lister_exports()` (nom, date de fin, lignes, « déjà importé le … »).
     Un export déjà importé est grisé, avec la même explication que pour un fichier déjà importé.
   - Ensuite, mêmes étapes qu'aujourd'hui : aperçu, contrôle, import par lots, matching.
   - `import_logs.filename` = `"<nom de l'export> (<export_id>)"` ; `file_hash` = `files.sha256`.
5. **Traçabilité** — migration `sql/migrations/0002_source_export.sql` (rejouable) : colonne
   `import_logs.source_export_id TEXT NULL`, remplie pour les imports depuis un export.
   Un verbatim retrouve ainsi son export d'origine : `verbatims.import_batch_id` → `import_logs.source_export_id`.
6. **Vérification de la contrainte sur `guid`** — `sql/schema.sql` déclare `guid VARCHAR(255) UNIQUE`, alors
   qu'api-explorer a établi que l'`id` Semantiweb est **régénéré à chaque export et se répète d'un export à
   l'autre** (`api-explorer/docs/bulk_v2/decision_identifiant.md`). Si la base porte cette contrainte, le
   deuxième import mensuel peut échouer sur un `guid` déjà vu, alors que l'`id` (empreinte) est différent.
   Vérifier dans la base :
   `SELECT conname FROM pg_constraint WHERE conrelid = 'public.verbatims'::regclass AND contype = 'u';`
   Si une contrainte porte sur `guid` : la remplacer par un index simple (migration `0003`, avec l'accord de
   l'utilisateur), et corriger `schema.sql`.

## Hors périmètre
- Toute conversion du brut (`raw_mentions`) : option B écartée.
- Import automatique sans clic (lot 8).
- La page Matching (inchangée).

## Tests
- **Fixtures fournies** (synthétiques, déjà produites le 21/10/2026) :
  `tests/fixtures/fake_export_plat.csv` = 4 fausses mentions passées par le vrai `derived.write_plat()`
  d'api-explorer (accents, `;`, guillemets, retour à la ligne, emoji, note et source vides, verbatim « NA »),
  en-tête `Sampling` compris ; `tests/fixtures/fake_export_plat_compass_ids.json` = les 4 `compass_id`
  calculés par api-explorer. Vérifié : une fois `Sampling` renommé, Compass calcule les 4 mêmes `id`, sans
  ligne écartée.
- `Sampling` accepté et lu comme `sampling` ; un fichier sans aucune des deux colonnes reste refusé.
- Les `id` calculés sur la fixture sont égaux aux `compass_id` du fichier JSON (test permanent : il garde
  les deux dépôts alignés).
- `telecharger()` avec un faux Storage : empreinte correcte → fichier prêt ; un octet modifié → refus,
  fichier temporaire supprimé, aucune ligne en base.
- `lister_exports()` sur une base de test : export COMPLETED avec `csv_plat` listé ; export INCOMPLETE, sans
  `csv_plat` ou aux fichiers expirés non listé ; export déjà importé marqué comme tel.
- `iter_csv_chunks(chemin)` : mêmes morceaux que `iter_csv_chunks(octets)` sur la fixture.
- Tous les tests existants restent verts.

## À faire par l'utilisateur
- Exécuter `0002_source_export.sql` dans le SQL Editor de Supabase.
- Secrets / variables de Compass : `SUPABASE_URL` (`https://<projet>.supabase.co`), `SUPABASE_SERVICE_KEY`
  (même clé `service_role` « Legacy » que dans les secrets d'api-explorer), `BULK_EXPORT_BUCKET = "bulk-exports"`.
- Lancer la requête de la tâche 6 et transmettre le résultat.
- Premier essai : importer le plus petit export validé, puis vérifier dans `import_logs` que
  `rows_inserted + rows_duplicates + rows_skipped = rows_total` = lignes du fichier.

## Définition de terminé
- [ ] Un export validé s'importe de bout en bout sans téléchargement ; un second clic est refusé (déjà importé).
- [ ] La mémoire de l'application reste stable pendant l'import d'un gros export (pas de pic proportionnel au fichier).
- [ ] Tous les tests passent ; journal mis à jour ; un commit sur une branche, une pull request.
