# Pipeline Verbatims — Spécifications fonctionnelles
**Compass · Consumer Voice — Import Pipeline**  
_v1.2 — Validé_

---

## Contexte

| | |
|---|---|
| **Environnement** | Streamlit (Posit) · Python · PostgreSQL (prod) · Supabase (dev) |
| **Objectif** | Importer ~500 000 verbatims/mois depuis l'API, enrichir les catégories internes, rendre les données consultables via Tableau ou outil BI |

### Décisions validées

| Décision | Choix retenu |
|---|---|
| GUID | Hash SHA-256 de `brand + date + product_name + verbatim_content` |
| Photo | `BOOLEAN` — `NULL` à l'import mensuel / rempli dans l'import initial |
| Catégorie | 1 produit = 1 catégorie définitive — écrasement si correction |
| Doublon import | Détection par hash SHA-256 du contenu binaire du fichier — import bloqué si déjà traité |
| Doublon matching | Écrasement silencieux (`INSERT OR UPDATE`) |
| Nommage base | `product_name` (sans suffixe Semantiweb) |
| Référentiel | Fichier CSV existant, liste fermée, rechargeable sans redémarrage |
| Utilisateurs | Application interne — pas de gestion d'authentification |
| Base dev | Supabase — base prod PostgreSQL, sélection via `COMPASS_ENV` |
| Module 5 | Hors scope v1 — placeholder prévu dans l'architecture |

---

## 1. Structure des données

Le fichier source est un CSV délimité par des points-virgules. Voici le mapping complet vers PostgreSQL.

### 1.1 Table principale — `verbatims`

| Colonne CSV | Champ PostgreSQL | Type | Notes |
|---|---|---|---|
| `guid` (original) | — | — | Ignoré — remplacé par hash |
| — | `id` | TEXT | SHA-256 de `brand+date+product_name+verbatim_content` |
| `brand` | `brand` | VARCHAR | |
| `country` | `country` | VARCHAR | |
| `date` | `date` | DATE | Format `DD/MM/YYYY` → conversion à l'import |
| `opinion` | `opinion` | VARCHAR | `positive` / `negative` / `neutral` |
| `product_name_SEMANTIWEB` | `product_name` | VARCHAR | **Renommé en base** — clé de matching, jamais modifiée directement |
| `rating` | `rating` | INTEGER | 1–5 |
| `source` | `source` | VARCHAR | Nom de la source (pharmacie, site…) |
| `verbatim_content` | `verbatim_content` | TEXT | Texte brut de l'avis client |
| `sampling` | `sampling` | BOOLEAN | `0/1` → `false/true` |
| `attribute_Efficiency` | `attribute_efficiency` | VARCHAR | `positive` / `negative` / `0` → `NULL` |
| `attribute_Packaging` | `attribute_packaging` | VARCHAR | idem |
| `attribute_Price` | `attribute_price` | VARCHAR | idem |
| `attribute_Quality` | `attribute_quality` | VARCHAR | idem |
| `attribute_Scent` | `attribute_scent` | VARCHAR | idem |
| `attribute_Taste` | `attribute_taste` | VARCHAR | idem |
| `attribute_Texture` | `attribute_texture` | VARCHAR | idem |
| `attribute_Safety` | `attribute_safety` | VARCHAR | idem |
| `attribute_Composition` | `attribute_composition` | VARCHAR | idem |
| `categorie interne` | `categorie_interne` | VARCHAR | ⚠ `NULL` à l'import mensuel — rempli par matching |
| `sous categorie interne` | `sous_categorie_interne` | VARCHAR | ⚠ `NULL` à l'import mensuel — rempli par matching |
| `photo` | `photo` | BOOLEAN | ⚠ `NULL` à l'import mensuel / rempli dans l'import initial |
| — | `imported_at` | TIMESTAMP | Horodatage ajouté automatiquement |
| — | `import_batch_id` | UUID | Lien vers `import_logs.id` |

> ⚠ Les trois champs `categorie_interne`, `sous_categorie_interne` et `photo` sont `NULL` à l'import mensuel. Dans l'import initial one-shot, ils sont déjà remplis dans le fichier source.

---

### 1.2 Table de correspondance — `categories_mapping`

Source de vérité unique pour les catégories. Un enregistrement par `product_name`.

| Champ | Type | Contrainte | Description |
|---|---|---|---|
| `product_name` | VARCHAR | PRIMARY KEY | Clé — valeur exacte du CSV, sans suffixe Semantiweb |
| `categorie_interne` | VARCHAR | NOT NULL | Valeur du référentiel fermé |
| `sous_categorie_interne` | VARCHAR | NOT NULL | Valeur du référentiel fermé |
| `photo` | BOOLEAN | | Présence de photo pour ce produit |
| `matched_at` | TIMESTAMP | | Date du dernier matching |
| `matched_by` | VARCHAR | | Identifiant de l'opérateur |

---

### 1.3 Table de logs — `import_logs`

| Champ | Type | Description |
|---|---|---|
| `id` | UUID | Identifiant unique du batch d'import |
| `file_hash` | VARCHAR | SHA-256 du contenu binaire du fichier CSV — clé anti-doublon |
| `started_at` | TIMESTAMP | Début du traitement |
| `finished_at` | TIMESTAMP | Fin du traitement |
| `filename` | VARCHAR | Nom du fichier CSV importé |
| `import_type` | VARCHAR | `initial` (one-shot) / `mensuel` (courant) |
| `rows_total` | INTEGER | Nombre total de lignes dans le fichier |
| `rows_inserted` | INTEGER | Lignes insérées avec succès |
| `rows_skipped` | INTEGER | Lignes ignorées (format invalide, champs manquants) |
| `rows_matched` | INTEGER | Verbatims avec catégorie déjà connue à l'import |
| `rows_unmatched` | INTEGER | Verbatims sans catégorie — à traiter module 3 |
| `status` | VARCHAR | `success` / `partial` / `error` / `duplicate` |
| `error_detail` | TEXT | Message d'erreur si `status != success` |

---

## 2. Modules fonctionnels

L'application est structurée en 4 modules indépendants, chacun correspondant à une page Streamlit. La connexion PostgreSQL et la logique métier sont centralisées dans `core/`, réutilisable par les futurs modules (API, LangGraph).

---

### Module 1 — Import mensuel et initial

> **Objectif :** Charger un fichier CSV dans PostgreSQL, générer un identifiant unique par verbatim, contrôler les doublons au niveau fichier, et journaliser chaque opération.

#### Deux modes d'import

- **Import initial (one-shot)** : fichier historique complet avec `categorie_interne`, `sous_categorie_interne` et `photo` déjà remplis — tous les champs sont importés tels quels.
- **Import mensuel courant** : fichier brut API — `categorie_interne`, `sous_categorie_interne` et `photo` sont `NULL` à l'import, remplis ensuite par le Module 3.

L'interface propose une sélection du mode avant l'upload. Le comportement des champs enrichis diffère selon le mode choisi.

#### Contrôle anti-doublon fichier

- Avant tout traitement, le système calcule le SHA-256 du contenu binaire du fichier uploadé.
- Ce hash est comparé à la colonne `file_hash` de `import_logs`.
- Si le hash est déjà présent : import bloqué immédiatement, message affiché avec la date et le nom du fichier précédemment importé.
- Si absent : hash stocké dans `import_logs` et traitement démarré.

> ℹ️ La détection est basée sur le contenu, pas le nom du fichier. Un fichier renommé mais identique est correctement détecté comme doublon.

#### Flux de traitement

1. L'utilisateur choisit le mode (`initial` / `mensuel`) et charge le fichier CSV.
2. Contrôle anti-doublon sur hash fichier — bloqué si déjà importé.
3. Validation du format : encodage UTF-8 BOM, séparateur `;`, présence des colonnes obligatoires.
4. Pour chaque ligne, génération d'un hash SHA-256 : `sha256(brand + date + product_name + verbatim_content)`.
   - Ce hash devient l'`id` unique du verbatim en base.
   - Il garantit l'idempotence au niveau verbatim — complément du contrôle fichier.
5. Conversions de types :
   - `date` `DD/MM/YYYY` → `DATE`
   - `sampling` `0/1` → `BOOLEAN`
   - `photo` `oui/non` → `BOOLEAN` (mode initial uniquement, sinon `NULL`)
   - Attributs `"0"` → `NULL`
6. Renommage à la volée : `product_name_SEMANTIWEB` → `product_name`.
7. Vérification dans `categories_mapping` : si le `product_name` est connu, `categorie_interne`, `sous_categorie_interne` et `photo` sont peuplés immédiatement.
8. INSERT en batch de 1 000 lignes par transaction avec `ON CONFLICT DO NOTHING` sur l'`id`.
9. Création d'un enregistrement `import_logs` avec `file_hash`, `import_type` et toutes les métriques.

#### Comportement en cas d'erreur

- Hash fichier déjà connu → import bloqué, lien vers le log du premier import.
- Colonne obligatoire manquante → import bloqué, message explicite.
- Ligne malformée (mauvais nombre de colonnes) → ligne skippée, comptée dans `rows_skipped`.
- Erreur PostgreSQL → rollback de la transaction courante, les transactions précédentes sont conservées.

#### Interface Streamlit

- Sélecteur de mode : `Import initial` / `Import mensuel`.
- Uploader fichier CSV.
- Contrôle hash affiché avant lancement : `Fichier nouveau — OK` ou `Fichier déjà importé le JJ/MM/AAAA — import bloqué`.
- Barre de progression pendant l'import.
- Résumé post-import : X insérés / Y skippés / Z avec catégorie connue / N sans catégorie (lien direct vers Module 3).

> ℹ️ Pour 500 000 verbatims, l'import en batch de 1 000 génère ~500 transactions. Temps estimé : 2–5 min selon la connexion PostgreSQL.

---

### Module 2 — Enrichissement

> **Objectif :** Permettre de mettre à jour manuellement les champs `categorie_interne`, `sous_categorie_interne` et `photo` sur des verbatims individuels ou en sélection multiple.

Ce module couvre l'édition directe en base. Le cas le plus fréquent (enrichissement par produit en masse) est géré par le Module 3.

#### Fonctionnalités

- Filtres : par `brand`, `source`, `date`, `opinion`, statut catégorie (vide / remplie).
- Tableau de résultats avec colonnes éditables inline (`st.data_editor`).
- Sélection multiple → assignation en masse d'une catégorie.
- Sauvegarde avec confirmation (affichage du nombre de lignes impactées) et log de l'opération.

---

### Module 3 — Matching catégories

> **Objectif :** Assigner catégorie, sous-catégorie et photo à tous les produits nouveaux (50–100/mois) via un fichier XLS, puis propager automatiquement sur l'ensemble des verbatims du produit.

#### Étape 1 — Détection

Après chaque import mensuel, le système identifie automatiquement les `product_name` présents dans `verbatims` mais absents de `categories_mapping`. Résultat affiché : N produits non catégorisés, représentant X verbatims.

#### Étape 2 — Export XLS

L'opérateur télécharge un fichier XLS avec les colonnes suivantes :

| Colonne | Éditable ? | Description |
|---|---|---|
| `product_name` | Non (grisé) | Clé de matching — ne pas modifier |
| `nb_verbatims` | Non (grisé) | Nombre de verbatims impactés — aide à prioriser |
| `categorie_interne` | **Oui** | Valeur choisie dans le référentiel (onglet 2) |
| `sous_categorie_interne` | **Oui** | Valeur choisie dans le référentiel (onglet 2) |
| `photo` | **Oui** | `true` / `false` — présence de photo pour ce produit |

Le XLS contient un second onglet **Référentiel** listant toutes les combinaisons valides catégorie / sous-catégorie, avec menus déroulants dans les cellules éditables. La colonne `photo` accepte uniquement `true` ou `false` (menu déroulant).

#### Étape 3 — Saisie humaine (hors application)

L'opérateur complète le XLS dans Excel ou LibreOffice. Les menus déroulants contraignent la saisie aux valeurs du référentiel et éliminent les fautes de frappe.

#### Étape 4 — Réimport et validation

L'opérateur importe le XLS complété. Le système valide :

- Lignes incomplètes : `categorie`, `sous_categorie` ou `photo` vide → rejetées avec ligne signalée.
- Valeurs hors référentiel : catégorie non reconnue → rejetée avec message explicite.
- Valeur `photo` invalide (ni `true` ni `false`) → rejetée.
- `product_name` modifié par erreur → ligne rejetée.

Résultat affiché : X lignes acceptées / Y lignes rejetées (tableau détaillé). L'opérateur peut corriger et réimporter uniquement les lignes en erreur.

#### Étape 5 — Propagation

Pour chaque mapping valide :

- `INSERT OR UPDATE` dans `categories_mapping` (clé = `product_name`) — écrasement si déjà présent.
- `UPDATE` en masse sur `verbatims` : toutes les lignes avec ce `product_name` reçoivent `categorie_interne`, `sous_categorie_interne` et `photo`.
- Log de l'opération : N produits matchés, M verbatims mis à jour.

> ℹ️ La table `categories_mapping` est la source de vérité. Si une catégorie ou la valeur `photo` est corrigée pour un produit, l'UPDATE se propage sur l'intégralité de l'historique des verbatims de ce produit.

---

### Module 4 — Outils de maintenance

> **Objectif :** Consulter et maintenir les référentiels : table de correspondance, noms de produits, logs d'import.

#### 4.1 — Table de correspondance

- Affichage paginé de `categories_mapping` (`product_name` → catégorie → sous-catégorie → photo).
- Filtres par catégorie, recherche texte sur le nom produit.
- Export CSV du référentiel complet.

#### 4.2 — Renommer un produit

- Recherche d'un `product_name` existant en base.
- Saisie du nouveau nom.
- Confirmation : affichage du nombre de verbatims impactés avant validation.
- `UPDATE` en cascade : `verbatims` + `categories_mapping`.
- Log de l'opération.

> ℹ️ Cas d'usage : l'API renomme un produit entre deux exports. Il faut renommer l'ancien pour maintenir la continuité de l'historique.

#### 4.3 — Vérification des noms produits

- Liste de tous les `product_name` distincts en base.
- Tri par nombre de verbatims (les plus fréquents en premier).
- Indicateur visuel : catégorie connue (vert) / inconnue (orange).
- Action rapide : lancer le matching pour les produits sans catégorie.

#### 4.4 — Logs d'import

- Tableau chronologique de tous les imports.
- Colonnes : date, mode (initial/mensuel), fichier, insérés, skippés, matchés, non-matchés, statut, durée.
- Statut `duplicate` visible : import bloqué par contrôle hash.
- Détail d'un import : liste des lignes skippées avec raison.

---

### Module 5 — Connecteur API et backup _(futur — hors scope v1)_

> **Objectif :** Remplacer l'upload manuel du CSV par un appel direct à l'API, avec sauvegarde systématique du fichier brut reçu dans un stockage objet.

#### Backup fichier brut

- Chaque fichier CSV reçu de l'API est sauvegardé dans un stockage objet **avant** tout traitement.
- Nommage : `{brand}_{YYYY-MM}_{file_hash[:8]}.csv`.
- Si l'import échoue, le fichier est quand même conservé.
- Fournisseur de stockage objet : à déterminer (S3, Azure Blob, GCS, Scaleway…).
- Durée de rétention : à déterminer.

#### Architecture

- Le module utilise `core/importer.py` en aval — aucune modification du code d'import existant.
- Un nouveau module `core/storage.py` gère le backup objet de façon indépendante.
- Les credentials de stockage sont configurés dans `config.toml`.

> ℹ️ Ce module est conçu pour être branché sur le pipeline existant sans le modifier. La page `pages/5_api.py` est déjà prévue comme placeholder dans la structure.

---

## 3. Architecture technique

### 3.1 Structure des fichiers

```
compass_import/
├── app.py                        # Point d'entrée Streamlit — navigation, config globale
├── pages/
│   ├── 1_Import.py               # Module 1 — Import mensuel et initial
│   ├── 2_Enrichissement.py       # Module 2 — Enrichissement manuel
│   ├── 3_Matching.py             # Module 3 — Matching catégories
│   ├── 4_Outils.py               # Module 4 — Maintenance et consultation
│   └── 5_API.py                  # Module 5 — Connecteur API (placeholder)
├── core/
│   ├── db.py                     # Pool de connexions PostgreSQL / Supabase
│   ├── hasher.py                 # SHA-256 verbatim + hash fichier anti-doublon
│   ├── importer.py               # Parsing CSV, validation, batch INSERT, deux modes
│   ├── matcher.py                # Export XLS (avec photo), validation réimport, propagation UPDATE
│   ├── referentiel.py            # Chargement et validation du référentiel catégories
│   └── storage.py                # Backup fichier brut vers stockage objet (Module 5 — placeholder)
├── compass_ui/
│   ├── style.css                 # Design system CSS light/dark
│   └── compass_ui.py             # Composants UI Python
├── data/
│   └── referentiel_categories.csv  # Référentiel fermé catégories / sous-catégories
├── sql/
│   ├── schema.sql                # DDL des 3 tables PostgreSQL
│   └── migrations/               # Migrations numérotées
├── tests/
├── scripts/
│   └── seed_dev.py               # Données de test
├── config.toml                   # Configuration dev/prod
├── .env.example                  # Template variables d'environnement
└── requirements.txt
```

### 3.2 Double configuration base de données

```toml
[database.dev]   # Supabase — sélectionné si COMPASS_ENV=dev
url = "$SUPABASE_DB_URL"

[database.prod]  # PostgreSQL — sélectionné si COMPASS_ENV=prod
host     = "$PG_HOST"
port     = 5432
database = "$PG_DB"
user     = "$PG_USER"
password = "$PG_PASSWORD"
```

La sélection se fait via la variable d'environnement `COMPASS_ENV` (défaut : `dev`). Le code applicatif ne change pas entre les deux environnements.

### 3.3 Dépendances Python

| Package | Usage |
|---|---|
| `streamlit` | Interface utilisateur |
| `psycopg2-binary` | Connexion PostgreSQL / Supabase |
| `pandas` | Parsing CSV, manipulation XLS |
| `openpyxl` | Génération XLS avec menus déroulants (matching) |
| `hashlib` | Génération SHA-256 verbatim et fichier (stdlib Python) |
| `python-dotenv` | Variables d'environnement locales |
| `boto3` / sdk stockage | Backup fichier brut (Module 5 — fournisseur à déterminer) |

### 3.4 Extensibilité — modules futurs

L'architecture `core/` est conçue pour accueillir deux modules supplémentaires sans modification du code existant :

- **Module 5 — Connecteur API** : remplace l'upload manuel du CSV par un appel API automatique. Utilise `core/importer.py` tel quel en aval et `core/storage.py` pour le backup.
- **Module 6 — Traitement LangGraph** : enrichissement IA des verbatims (déjà développé séparément), branché sur `core/db.py` pour lire et écrire en base.

---

## 4. Décisions — points résolus

| # | Question | Décision |
|---|---|---|
| 1 | Référentiel catégories | Fichier CSV existant, rechargeable à chaud. Validation stricte — aucune valeur hors référentiel acceptée. |
| 2 | Gestion des droits | Application interne — pas de gestion d'utilisateurs ni de rôles. |
| 3 | Déploiement Posit | Double configuration `dev` (Supabase) / `prod` (PostgreSQL) via `COMPASS_ENV`. |
| 4 | Module 5 stockage objet | Hors scope v1. Architecture placeholder prévue. Fournisseur à déterminer ultérieurement. |
| 5 | Durée de rétention backups | Hors scope v1. À traiter lors de l'implémentation du Module 5. |

---

## 5. Livrables

| Livrable | Format | Contenu |
|---|---|---|
| Specs validées | Ce document (`.md`) | Référence fonctionnelle pour le développement |
| Package Claude Code | Archive `.zip` | `CLAUDE_CODE_GUIDE.md`, SQL schema, config, design system, référentiel CSV, README |
| Design system | `compass_ui/` | `style.css` + `compass_ui.py` + preview HTML |
| Template XLS matching | Généré à l'exécution | Colonnes pré-formatées + onglet Référentiel avec menus déroulants |

---

_— Fin des spécifications v1.2 —_
