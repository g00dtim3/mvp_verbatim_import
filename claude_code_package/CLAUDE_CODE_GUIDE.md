# Guide Claude Code — Compass · Consumer Voice Import Pipeline

## Contexte général (à inclure dans chaque session)

**Projet :** Module Streamlit d'import et d'enrichissement de verbatims consommateurs.
**Stack :** Python · Streamlit · PostgreSQL (prod) · Supabase (dev) · openpyxl

**Fichiers de référence à toujours avoir en contexte :**
- `SPECS.md` — spécifications fonctionnelles complètes v1.1
- `compass_ui/style.css` — design system CSS Compass
- `compass_ui/compass_ui.py` — composants UI Python
- `sql/schema.sql` — DDL des 3 tables PostgreSQL

**Structure cible du projet :**
```
compass_import/
├── app.py                        # Point d'entrée Streamlit
├── pages/
│   ├── 1_Import.py               # Module 1 — Import mensuel + initial
│   ├── 2_Enrichissement.py       # Module 2 — Enrichissement manuel
│   ├── 3_Matching.py             # Module 3 — Matching catégories
│   └── 4_Outils.py               # Module 4 — Maintenance
├── core/
│   ├── db.py                     # Connexion PostgreSQL / Supabase
│   ├── hasher.py                 # SHA-256 verbatim + fichier
│   ├── importer.py               # Logique d'import CSV
│   ├── matcher.py                # Export XLS + réimport + propagation
│   └── referentiel.py            # Chargement CSV référentiel catégories
├── compass_ui/
│   ├── style.css                 # Design system CSS
│   └── compass_ui.py             # Composants UI
├── data/
│   └── referentiel_categories.csv  # Référentiel fermé catégories/sous-catégories
├── sql/
│   ├── schema.sql                # DDL complet
│   └── migrations/               # Migrations numérotées
├── config.toml                   # Config dev/prod
├── .env.example                  # Variables d'environnement
└── requirements.txt
```

---

## Sessions recommandées (dans cet ordre)

### Session 1 — Fondations (SQL + config + core/db)

**Fichiers à fournir :** `SPECS.md`, `CLAUDE_CODE_GUIDE.md`

**Prompt :**
```
Implémente les fondations du projet Compass Import Pipeline.

1. Crée sql/schema.sql avec les 3 tables selon SPECS.md section 1 :
   - verbatims (avec id SHA-256, product_name sans suffixe Semantiweb,
     categorie_interne/sous_categorie_interne/photo nullable)
   - categories_mapping (clef product_name)
   - import_logs (avec file_hash, import_type, statut duplicate)
   Ajoute les index sur : product_name, categorie_interne, date, brand, source.

2. Crée config.toml avec double profil dev (Supabase) et prod (PostgreSQL) :
   [database.dev]  — connexion Supabase via postgres://
   [database.prod] — connexion PostgreSQL standard
   Sélection via variable d'environnement COMPASS_ENV (défaut : dev)

3. Crée .env.example documentant toutes les variables requises.

4. Crée core/db.py :
   - Lit COMPASS_ENV pour choisir le profil
   - Pool de connexions psycopg2 (connection pool size 5)
   - Fonction get_connection() retourne un context manager
   - Fonction test_connection() → bool
   - Gestion des erreurs avec messages explicites

5. Crée requirements.txt avec toutes les dépendances.

Ne crée pas encore les pages Streamlit.
```

---

### Session 2 — core/hasher + core/referentiel + core/importer

**Fichiers à fournir :** `SPECS.md`, `CLAUDE_CODE_GUIDE.md`, `core/db.py`

**Prompt :**
```
Implémente les 3 modules core suivants.

1. core/hasher.py :
   - verbatim_hash(brand, date, product_name, verbatim_content) → str
     SHA-256 hexdigest sur la concaténation normalisée (strip + lower)
   - file_hash(file_bytes: bytes) → str
     SHA-256 du contenu binaire brut du fichier
   - is_file_already_imported(conn, file_hash: str) → dict | None
     Retourne le log existant si le hash est déjà dans import_logs, sinon None

2. core/referentiel.py :
   - load_referentiel(path="data/referentiel_categories.csv") → dict
     Retourne { categorie: [sous_categories] }
   - get_all_categories() → list[str]
   - get_sous_categories(categorie: str) → list[str]
   - is_valid_combination(categorie: str, sous_categorie: str) → bool
   - Le CSV a 2 colonnes : categorie, sous_categorie
   - Rechargeable à chaud (pas de cache global)

3. core/importer.py :
   - parse_csv(file_bytes: bytes) → pd.DataFrame
     Gère UTF-8 BOM, séparateur ;, colonnes obligatoires
     Lève ValueError avec message explicite si colonne manquante
   - normalize_row(row: pd.Series, import_type: str) → dict
     Renomme product_name_SEMANTIWEB → product_name
     Convertit date DD/MM/YYYY → date Python
     Convertit sampling 0/1 → bool
     Convertit photo oui/non → bool (import_type="initial" seulement, sinon None)
     Convertit attributs "0" → None
   - import_batch(conn, rows: list[dict], batch_id: str) → dict
     INSERT en batch de 1000 avec ON CONFLICT DO NOTHING sur id
     Retourne {inserted, skipped, errors: list}
   - apply_known_categories(conn, rows: list[dict]) → list[dict]
     Pour chaque row, si product_name existe dans categories_mapping,
     peuple categorie_interne, sous_categorie_interne, photo

Tous les modules doivent avoir des docstrings et des tests unitaires
dans tests/test_*.py (pytest).
```

---

### Session 3 — Page Import (Module 1)

**Fichiers à fournir :** `SPECS.md`, `CLAUDE_CODE_GUIDE.md`, `compass_ui/compass_ui.py`, tous les fichiers core/

**Prompt :**
```
Implémente pages/1_Import.py — Module 1 Import mensuel + initial.

UI avec compass_ui :
  ui.inject_css() en tête de page
  ui.theme_toggle()
  ui.sidebar_header()
  ui.page_header("Import", "Charger le fichier CSV mensuel de l'API")

Flux complet :

ÉTAPE 0 — Sélection du mode
  ui.import_mode_toggle() → "initial" ou "mensuel"
  Afficher une note explicative selon le mode choisi

ÉTAPE 1 — Upload fichier
  st.file_uploader accepte uniquement .csv
  Dès qu'un fichier est uploadé :
    - Calculer file_hash via core/hasher.file_hash()
    - Vérifier dans import_logs via is_file_already_imported()
    - ui.hash_check("ok") si nouveau, ui.hash_check("dupe") si doublon
    - Si doublon : ui.duplicate_alert() + st.stop()

ÉTAPE 2 — Validation
  ui.steps(["Upload","Validation","Import","Résumé"], current=1)
  Appeler parse_csv() → afficher aperçu 5 premières lignes (st.dataframe)
  Afficher nombre de lignes détectées
  Bouton "Lancer l'import →"

ÉTAPE 3 — Import
  ui.steps(..., current=2)
  Créer un batch_id (uuid4)
  Insérer dans import_logs avec status="running"
  Boucle batch de 1000 lignes :
    - ui.progress_block() mis à jour à chaque batch
    - apply_known_categories() avant INSERT
    - import_batch()
  Mettre à jour import_logs avec métriques finales et status

ÉTAPE 4 — Résumé
  ui.steps(..., current=3)
  ui.import_summary(inserted, skipped, matched, unmatched, duration_s)
  Si unmatched > 0 : bouton "Aller au Matching catégories →"
    (st.switch_page("pages/3_Matching.py"))

Règles :
- Utiliser st.session_state pour conserver l'état entre les étapes
- Chaque étape n'est visible que si la précédente est complète
- Toujours afficher le mode choisi dans le header (badge)
- Gérer toutes les erreurs avec ui.alert(type="error")
```

---

### Session 4 — core/matcher + Page Matching (Module 3)

**Fichiers à fournir :** `SPECS.md`, `CLAUDE_CODE_GUIDE.md`, `compass_ui/compass_ui.py`, `core/db.py`, `core/referentiel.py`

**Prompt :**
```
Implémente core/matcher.py puis pages/3_Matching.py.

core/matcher.py :

1. get_unmatched_products(conn) → list[dict]
   SELECT DISTINCT product_name, COUNT(*) as nb_verbatims
   FROM verbatims WHERE categorie_interne IS NULL
   GROUP BY product_name ORDER BY nb_verbatims DESC

2. export_matching_xls(products: list[dict], referentiel: dict) → bytes
   Génère un fichier XLS avec openpyxl :
   Onglet 1 "Matching" :
     Colonnes : product_name (grisé, verrouillé), nb_verbatims (grisé),
                categorie_interne (menu déroulant), sous_categorie_interne
                (menu déroulant dépendant), photo (menu déroulant true/false)
     Header en bleu Compass #1F6ED4, texte blanc
     Colonnes éditables sur fond #FFFBEB
     Protection feuille : seules les colonnes éditables sont déverrouillées
   Onglet 2 "Référentiel" :
     Toutes les combinaisons catégorie / sous-catégorie valides
     En lecture seule

3. validate_matching_xls(file_bytes: bytes, referentiel: dict) → dict
   Retourne {valid: list[dict], errors: list[dict]}
   Vérifie : categorie vide, sous_categorie vide, photo invalide,
              combinaison non dans le référentiel, product_name modifié

4. apply_matching(conn, valid_rows: list[dict]) → dict
   Pour chaque row valide :
     UPSERT dans categories_mapping (ON CONFLICT product_name DO UPDATE)
     UPDATE verbatims SET categorie_interne=..., sous_categorie_interne=..., photo=...
       WHERE product_name = row.product_name
   Retourne {products_matched, verbatims_updated}

pages/3_Matching.py :

Affichage état actuel :
  Appeler get_unmatched_products()
  Si 0 produits : ui.empty_state("✓","Tous les produits sont matchés")
  Sinon : ui.metric_row([{produits sans catégorie}, {verbatims impactés}])
  ui.product_status_table(products)  — tableau avec statut matchée/à compléter

Export XLS :
  Bouton "Télécharger le fichier de matching"
  st.download_button avec le bytes retourné par export_matching_xls()

Réimport XLS :
  st.file_uploader accepte .xlsx uniquement
  validate_matching_xls() → afficher résumé : X valides / Y erreurs
  Si erreurs : tableau détaillé des lignes rejetées (ligne, colonne, raison)
  Si tout valide (ou partiellement) : bouton "Appliquer le matching"
  apply_matching() → ui.matching_summary(products_matched, verbatims_updated)
  ui.alert(type="success")
```

---

### Session 5 — Page Enrichissement (Module 2)

**Fichiers à fournir :** `SPECS.md`, `CLAUDE_CODE_GUIDE.md`, `compass_ui/compass_ui.py`, `core/db.py`, `core/referentiel.py`

**Prompt :**
```
Implémente pages/2_Enrichissement.py — Module 2 enrichissement manuel.

Objectif : permettre de mettre à jour manuellement categorie_interne,
sous_categorie_interne et photo sur des verbatims individuels ou en masse.

Filtres (sidebar) :
  - brand (selectbox, valeurs distinctes depuis DB)
  - source (selectbox)
  - date_from / date_to (date_input)
  - opinion (selectbox : tous / positive / negative / neutral)
  - statut catégorie (selectbox : tous / avec catégorie / sans catégorie)

Résultats :
  Afficher le nombre de verbatims trouvés
  Tableau st.data_editor avec colonnes :
    - verbatim_content (lecture seule, truncated 120 chars)
    - product_name (lecture seule)
    - categorie_interne (selectbox depuis référentiel)
    - sous_categorie_interne (selectbox depuis référentiel)
    - photo (checkbox)
  Pagination 50 lignes par page

Actions :
  Bouton "Sauvegarder les modifications"
  Affiche le nombre de lignes modifiées avant confirmation (st.warning)
  UPDATE en base uniquement les lignes effectivement modifiées
  ui.alert(type="success") post-sauvegarde

Règles :
  - Charger les données au changement de filtre (pas en temps réel)
  - Bouton "Appliquer les filtres" explicite
  - Ne jamais mettre à jour categories_mapping depuis ce module
    (enrichissement verbatim par verbatim uniquement)
```

---

### Session 6 — Page Outils (Module 4)

**Fichiers à fournir :** `SPECS.md`, `CLAUDE_CODE_GUIDE.md`, `compass_ui/compass_ui.py`, `core/db.py`

**Prompt :**
```
Implémente pages/4_Outils.py — Module 4 maintenance et consultation.

Organiser en 4 onglets via st.tabs :

Onglet 1 — "Table de correspondance" :
  Affichage paginé de categories_mapping
  Filtre texte sur product_name
  Filtre selectbox sur categorie_interne
  Tableau : product_name / categorie / sous_categorie / photo / matched_at
  Bouton "Exporter CSV" → st.download_button

Onglet 2 — "Renommer un produit" :
  Searchbox : saisir un product_name existant (st.selectbox avec search)
  Afficher : nb verbatims concernés, catégorie actuelle
  Champ "Nouveau nom"
  ui.alert(type="warning") : "Cette action mettra à jour X verbatims
    et la table de correspondance."
  Bouton "Confirmer le renommage"
  UPDATE verbatims SET product_name=nouveau WHERE product_name=ancien
  UPDATE categories_mapping SET product_name=nouveau WHERE product_name=ancien
  ui.alert(type="success") post-opération

Onglet 3 — "Vérification produits" :
  Liste tous les product_name distincts triés par nb_verbatims DESC
  Pour chaque produit :
    - badge vert "Matchée" si dans categories_mapping
    - badge orange "À compléter" si absent
  Compteur en haut : X matchés / Y à compléter
  Bouton "Aller au Matching" si Y > 0

Onglet 4 — "Logs d'import" :
  ui.log_table() avec tous les imports
  Filtre statut (tous / success / partial / error / duplicate)
  Expandeur par log : détail des lignes skippées (error_detail)
  Bouton "Exporter logs CSV"

Règles :
  - Toutes les opérations destructives demandent une confirmation explicite
  - Afficher le nombre d'éléments impactés avant chaque opération
  - Chaque onglet gère son propre état via st.session_state
```

---

### Session 7 — app.py + intégration finale

**Fichiers à fournir :** tous les fichiers du projet

**Prompt :**
```
Implémente app.py et finalise l'intégration.

app.py :
  - ui.inject_css() — une seule fois ici pour toutes les pages
  - ui.theme_toggle()
  - ui.sidebar_header()
  - Test de connexion DB au démarrage :
      Si test_connection() échoue → ui.alert(type="error") + st.stop()
      Si succès → badge vert discret en sidebar
  - Afficher dans la sidebar :
      COMPASS_ENV actif (dev/prod)
      Nombre total de verbatims en base
      Nombre de produits sans catégorie (badge orange si > 0)
  - Navigation Streamlit native via pages/

Vérifications finales :
  1. Tester le flux complet import initial → matching → enrichissement
  2. Tester le contrôle doublon (importer deux fois le même fichier)
  3. Tester le renommage produit en cascade
  4. Vérifier le switch light/dark sur toutes les pages
  5. Vérifier que Supabase (dev) et PostgreSQL (prod) fonctionnent tous les deux

Créer un script scripts/seed_dev.py :
  Insère 50 verbatims de test depuis data/sample_verbatims.csv
  Ajoute 3 entrées dans categories_mapping
  Ajoute 2 entrées dans import_logs (dont 1 doublon)
  → Permet de tester l'app sans données réelles
```

---

## Règles transverses à rappeler dans chaque session

```
1. Toujours importer compass_ui : from compass_ui.compass_ui import *
   Appeler inject_css() en tête de chaque page
   Appeler theme_toggle() juste après

2. Double config DB :
   COMPASS_ENV=dev  → Supabase (config.toml [database.dev])
   COMPASS_ENV=prod → PostgreSQL (config.toml [database.prod])
   Ne jamais hardcoder de credentials — toujours via config.toml + .env

3. Contrôle doublon fichier :
   Le hash SHA-256 du contenu binaire est calculé AVANT tout traitement
   Si présent dans import_logs.file_hash → bloquer immédiatement

4. product_name :
   La colonne CSV s'appelle product_name_SEMANTIWEB
   En base elle s'appelle product_name (renommage à l'import)
   C'est la clef de matching dans categories_mapping
   Ne jamais la modifier sauf via l'outil "Renommer un produit"

5. Champs NULL à l'import mensuel :
   categorie_interne, sous_categorie_interne, photo → NULL
   (sauf si product_name connu dans categories_mapping)
   À l'import initial : tous les champs sont déjà remplis dans le CSV

6. Référentiel catégories :
   Chargé depuis data/referentiel_categories.csv
   CSV avec colonnes : categorie, sous_categorie
   Validation stricte — aucune valeur hors référentiel acceptée
   Rechargeable sans redémarrer l'app

7. XLS matching :
   Colonnes éditables : categorie_interne, sous_categorie_interne, photo
   Colonnes grisées/verrouillées : product_name, nb_verbatims
   Menus déroulants via openpyxl DataValidation
   Photo : menu déroulant true / false

8. Propagation matching :
   UPSERT categories_mapping + UPDATE verbatims en cascade
   Une modification de catégorie se propage sur TOUT l'historique

9. Design Compass :
   Couleur principale : #1F6ED4 (Compass Blue)
   Accent module : #2EC4C7 (Data Cyan)
   Fond : #F4F7FA light / #0A1628 dark
   Jamais de couleurs hardcodées dans les pages — toujours via les variables CSS

10. Gestion erreurs :
    Toute erreur DB → ui.alert(type="error") avec message lisible
    Toute validation échouée → ui.alert(type="warning") avec détail
    Ne jamais laisser remonter une exception non gérée à l'utilisateur
```
