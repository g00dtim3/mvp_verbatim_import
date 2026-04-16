# Guide Claude Code — Compass · Consumer Voice Import Pipeline

> **À lire en premier.** Ce document est la référence unique pour le développement.
> Il contient le contexte métier, l'architecture, les règles, et les prompts de chaque session.

---

## 1. Contexte métier

Le projet importe des verbatims consommateurs (avis clients) depuis une API externe
(Semantiweb) dans une base PostgreSQL. Les verbatims sont ensuite enrichis avec des
catégories internes et consultés via Tableau.

**Volume :** ~500 000 verbatims / mois  
**Stack :** Python · Streamlit (Posit) · PostgreSQL (prod) · Supabase (dev)  
**Utilisateurs :** usage interne — pas d'authentification

---

## 2. Structure du projet

```
compass_import/
├── app.py                        # Point d'entrée Streamlit
├── pages/
│   ├── 1_Import.py               # Module 1 — Import CSV (mensuel + initial)
│   ├── 2_Enrichissement.py       # Module 2 — Enrichissement manuel verbatims
│   ├── 3_Matching.py             # Module 3 — Matching catégories via XLS
│   └── 4_Outils.py               # Module 4 — Maintenance (renommage, logs, référentiel)
├── core/
│   ├── __init__.py
│   ├── db.py                     # Connexion PostgreSQL / Supabase
│   ├── hasher.py                 # SHA-256 verbatim + contrôle doublon fichier
│   ├── importer.py               # Parsing CSV, validation, INSERT batch
│   ├── matcher.py                # Export XLS matching, validation réimport, propagation
│   └── referentiel.py            # Chargement Table_CO.csv + validation catégories
├── compass_ui/
│   ├── __init__.py
│   ├── style.css                 # Design system CSS Compass light/dark
│   └── compass_ui.py             # Composants UI Python (inject_css, alert, metric_row…)
├── data/
│   ├── Table_CO.csv              # SOURCE DE VÉRITÉ catégories — 5 913 produits
│   └── referentiel_categories.csv # Référentiel catégories/sous-catégories (91 combinaisons)
├── sql/
│   └── schema.sql                # DDL complet PostgreSQL
├── scripts/
│   └── load_table_co.py          # Importe Table_CO.csv → categories_mapping en base
├── tests/
│   └── __init__.py
├── config.toml                   # Configuration dev/prod
├── .env.example                  # Template variables d'environnement
├── .streamlit/
│   └── secrets.toml.example      # Template secrets Streamlit
├── .gitignore
└── requirements.txt
```

---

## 3. Modèle de données

### 3.1 Table `verbatims`

Colonne CSV source → champ PostgreSQL :

| CSV | PostgreSQL | Type | Notes |
|---|---|---|---|
| `guid` | — | — | Ignoré — remplacé par hash |
| — | `id` | TEXT PK | SHA-256(brand+date+product_name+verbatim_content) |
| `brand` | `brand` | VARCHAR | |
| `country` | `country` | VARCHAR | |
| `date` | `date` | DATE | DD/MM/YYYY → DATE |
| `opinion` | `opinion` | VARCHAR | positive/negative/neutral |
| `product_name_SEMANTIWEB` | `product_name` | VARCHAR | **Renommé** à l'import |
| `rating` | `rating` | SMALLINT | 1–5 |
| `source` | `source` | VARCHAR | |
| `verbatim_content` | `verbatim_content` | TEXT | |
| `sampling` | `sampling` | BOOLEAN | 0/1 → bool |
| `attribute_*` (×9) | `attribute_*` | VARCHAR | "0" → NULL |
| `categorie interne` | `categorie_interne` | VARCHAR | ⚠ NULL import mensuel |
| `sous categorie interne` | `sous_categorie_interne` | VARCHAR | ⚠ NULL import mensuel |
| `photo` | `photo` | BOOLEAN | ⚠ NULL import mensuel |
| — | `imported_at` | TIMESTAMPTZ | Auto |
| — | `import_batch_id` | UUID | FK → import_logs |

### 3.2 Table `categories_mapping`

**Source de vérité** pour les catégories. Initialisée depuis `Table_CO.csv`.

| Champ | Type | Notes |
|---|---|---|
| `key_brandxpdt` | VARCHAR PK | Concaténation `brand \|\| product_name` — correspond à "Key brandxpdt" du CSV |
| `brand` | VARCHAR NOT NULL | |
| `product_name` | VARCHAR NOT NULL | |
| `categorie_interne` | VARCHAR NOT NULL | |
| `sous_categorie_interne` | VARCHAR NOT NULL | |
| `photo` | BOOLEAN | |
| `matched_at` | TIMESTAMPTZ | |
| `matched_by` | VARCHAR | 'system', 'Table_CO_import', ou nom opérateur |

**Contrainte UNIQUE (brand, product_name)** en plus de la PK.

> ⚠️ La clé de matching est **toujours `brand + product_name`** (pas `product_name` seul).
> Deux marques peuvent avoir un produit au même nom.

### 3.3 Table `import_logs`

| Champ | Type | Notes |
|---|---|---|
| `id` | UUID PK | |
| `file_hash` | VARCHAR | SHA-256 contenu binaire — clé anti-doublon |
| `filename` | VARCHAR | |
| `import_type` | VARCHAR | `initial` ou `mensuel` |
| `started_at` | TIMESTAMPTZ | |
| `finished_at` | TIMESTAMPTZ | |
| `rows_total/inserted/skipped/matched/unmatched` | INTEGER | |
| `status` | VARCHAR | `running/success/partial/error/duplicate` |
| `error_detail` | TEXT | |

---

## 4. Règles métier critiques

```
1. CLÉ DE MATCHING : toujours brand + product_name (jamais product_name seul)
   JOIN : ON cm.brand = v.brand AND cm.product_name = v.product_name

2. RENOMMAGE CSV : product_name_SEMANTIWEB → product_name à l'import (dans le code)

3. DOUBLON FICHIER : SHA-256 du contenu binaire calculé AVANT tout traitement
   Si présent dans import_logs.file_hash → bloquer immédiatement (status=duplicate)

4. DEUX MODES D'IMPORT :
   - initial  : tous les champs remplis (catégorie, sous-catégorie, photo inclus)
   - mensuel  : categorie_interne, sous_categorie_interne, photo → NULL à l'import

5. IDEMPOTENCE VERBATIM : ON CONFLICT DO NOTHING sur id (SHA-256)
   Un même verbatim importé deux fois ne crée pas de doublon

6. PROPAGATION MATCHING :
   UPSERT categories_mapping + UPDATE verbatims WHERE brand=x AND product_name=y
   Toute correction se propage sur l'intégralité de l'historique

7. RÉFÉRENTIEL FERMÉ : toute valeur catégorie/sous-catégorie
   doit exister dans referentiel_categories.csv (91 combinaisons)
   Table_CO.csv (5 913 lignes) est la source de vérité initiale

8. DESIGN : toujours importer compass_ui et appeler inject_css() en tête de page
   Ne jamais hardcoder de couleurs — utiliser les variables CSS (--c-blue, etc.)

9. DOUBLE CONFIG DB :
   COMPASS_ENV=dev  → st.secrets["database"]["url"] (Supabase, port 6543)
   COMPASS_ENV=prod → st.secrets["database"]["url"] (PostgreSQL)
   La sélection se fait dans core/db.py selon os.environ.get("COMPASS_ENV", "dev")

10. ERREURS : toujours attraper les exceptions DB et afficher via ui.alert(type="error")
    Ne jamais laisser remonter une exception brute à l'utilisateur Streamlit
```

---

## 5. Initialisation base de données

**Ordre obligatoire avant toute chose :**

```bash
# 1. Créer les tables
#    Sur Supabase : coller sql/schema.sql dans l'éditeur SQL
#    Sur PostgreSQL : psql -d compass_verbatims -f sql/schema.sql

# 2. Charger Table_CO.csv → categories_mapping
COMPASS_ENV=dev python scripts/load_table_co.py --dry-run   # vérifier d'abord
COMPASS_ENV=dev python scripts/load_table_co.py             # importer

# 3. Si des verbatims sont déjà en base, propager les catégories
COMPASS_ENV=dev python scripts/load_table_co.py --propagate
```

---

## 6. Sessions de développement

### Session 1 — core/db.py

**Fichiers contexte :** ce guide, `config.toml`, `.env.example`, `sql/schema.sql`

```
Implémente core/db.py pour le projet Compass Import Pipeline.

Comportement attendu :
- Lire COMPASS_ENV depuis os.environ (défaut : "dev")
- En mode "dev" : lire st.secrets["database"]["url"] (Streamlit secrets)
- En mode "prod" : lire st.secrets["database"]["url"]
- Les deux modes lisent la même clé secrets — seule la valeur dans secrets.toml change
- Créer un pool de connexions psycopg2 (SimpleConnectionPool)
  pool_size = 3 en dev, 5 en prod (lu depuis config.toml)
- Exposer get_connection() : context manager qui yield une connexion du pool
  et la remet dans le pool après usage (même en cas d'exception)
- Exposer test_connection() → bool : exécute SELECT 1, retourne True/False
- Gestion des erreurs explicite : OperationalError → message lisible (host, port, credentials)
- Ne jamais laisser une connexion ouverte après une exception

Pas d'import de compass_ui dans ce module (pas de dépendance Streamlit).
```

---

### Session 2 — core/hasher.py + core/referentiel.py

**Fichiers contexte :** ce guide, `data/Table_CO.csv`, `data/referentiel_categories.csv`

```
Implémente core/hasher.py et core/referentiel.py.

core/hasher.py :
  verbatim_hash(brand, date, product_name, verbatim_content) → str
    SHA-256 hexdigest de la concaténation normalisée (strip + lower de chaque champ)
    Format concaténation : f"{brand}|{date}|{product_name}|{verbatim_content}"

  file_hash(file_bytes: bytes) → str
    SHA-256 du contenu binaire brut

  is_file_already_imported(conn, hash_str: str) → dict | None
    SELECT id, filename, started_at, status FROM import_logs WHERE file_hash = %s
    Retourne le dict du premier résultat ou None

core/referentiel.py :
  Charge referentiel_categories.csv (colonnes : categorie, sous_categorie)
  et Table_CO.csv (colonnes : Key brandxpdt, brand, product_name_SEMANTIWEB,
  categorie interne, sous categorie interne, photo)

  load_referentiel() → dict { categorie: [sous_categories] }
    Depuis referentiel_categories.csv

  load_table_co() → pd.DataFrame
    Charge Table_CO.csv avec les bonnes colonnes
    Calcule key_brandxpdt = brand + product_name si absent ou pour validation

  is_valid_combination(categorie: str, sous_categorie: str) → bool
    Valide contre referentiel_categories.csv

  get_all_categories() → list[str]
  get_sous_categories(categorie: str) → list[str]

  lookup_product(brand: str, product_name: str) → dict | None
    Cherche dans Table_CO.csv (en mémoire) par brand + product_name
    Retourne {categorie_interne, sous_categorie_interne, photo} ou None

Écrire tests/test_hasher.py et tests/test_referentiel.py (pytest).
```

---

### Session 3 — core/importer.py

**Fichiers contexte :** ce guide, `core/db.py`, `core/hasher.py`, `core/referentiel.py`

```
Implémente core/importer.py.

parse_csv(file_bytes: bytes) → pd.DataFrame
  Lire avec sep=";", encoding="utf-8-sig"
  Vérifier la présence de toutes les required_columns (depuis config.toml)
  Lever ValueError avec message explicite si colonne manquante
  Retourner le DataFrame brut

normalize_row(row: pd.Series, import_type: str) → dict
  Renommer product_name_SEMANTIWEB → product_name (strip + conserver la casse)
  Convertir date "DD/MM/YYYY" → objet date Python
  Convertir sampling "0"/"1" → False/True
  Convertir photo "oui"/"non" → True/False si import_type="initial", sinon None
  Convertir attributs : valeur "0" ou "" → None, "positive"/"negative" → garder
  Générer id = hasher.verbatim_hash(brand, date, product_name, verbatim_content)
  Lever ValueError avec numéro de ligne si date invalide

apply_known_categories(conn, rows: list[dict]) → list[dict]
  Pour chaque row, chercher dans categories_mapping WHERE brand=%s AND product_name=%s
  Si trouvé et que categorie_interne est None : peupler categorie_interne,
  sous_categorie_interne, photo depuis le résultat
  Retourner la liste mise à jour avec un champ "was_matched": bool

import_batch(conn, rows: list[dict], batch_id: str) → dict
  INSERT en batch de 1000 lignes via execute_values (psycopg2.extras)
  ON CONFLICT (id) DO NOTHING
  Compter inserted (rowcount) vs skipped (len-rowcount)
  Retourner {inserted: int, skipped: int, errors: list[str]}
  En cas d'erreur sur un batch : rollback ce batch, logger l'erreur, continuer

Écrire tests/test_importer.py.
```

---

### Session 4 — pages/1_Import.py

**Fichiers contexte :** ce guide, tous les modules core/, `compass_ui/compass_ui.py`

```
Implémente pages/1_Import.py — Module 1 Import.

Structure de la page :

INITIALISATION
  from compass_ui.compass_ui import *
  inject_css()
  theme_toggle()  (via st.components.v1.html, window.parent.document)
  sidebar_header()

  Test connexion DB au chargement :
    Si test_connection() échoue → alert(type="error") + st.stop()

ÉTAPE 0 — Mode
  import_mode_toggle() → "initial" ou "mensuel"
  Note explicative selon le mode

ÉTAPE 1 — Upload
  steps(["Mode","Upload","Validation","Import","Résumé"], current=1)
  st.file_uploader(".csv uniquement")
  
  Dès upload :
    hash = file_hash(bytes)
    existing = is_file_already_imported(conn, hash)
    Si existing :
      hash_check("dupe", f"Importé le {existing['started_at']} — {existing['filename']}")
      duplicate_alert(filename, date, batch_id)
      st.stop()
    Sinon :
      hash_check("ok")

ÉTAPE 2 — Validation
  steps(..., current=2)
  df = parse_csv(bytes)
  st.dataframe(df.head(5), use_container_width=True)
  st.caption(f"{len(df):,} lignes détectées")
  Bouton "Lancer l'import →"

ÉTAPE 3 — Import
  steps(..., current=3)
  Créer batch_id = str(uuid4())
  
  Insérer import_logs avec status="running", file_hash, import_type
  
  rows_normalized = [normalize_row(row, import_type) for _, row in df.iterrows()]
  rows_with_cats  = apply_known_categories(conn, rows_normalized)
  
  Boucle par batch de 1000 :
    progress_block(f"Import en cours…", f"{done:,}/{total:,} lignes", pct)
    result = import_batch(conn, batch, batch_id)
  
  Mettre à jour import_logs (status, métriques, finished_at)

ÉTAPE 4 — Résumé
  steps(..., current=4)
  import_summary(inserted, skipped, matched, unmatched, duration_s)
  
  Si unmatched > 0 :
    Bouton "Aller au Matching →" → st.switch_page("pages/3_Matching.py")

Stocker l'état entre étapes dans st.session_state.
```

---

### Session 5 — core/matcher.py

**Fichiers contexte :** ce guide, `core/db.py`, `core/referentiel.py`, `data/referentiel_categories.csv`

```
Implémente core/matcher.py.

get_unmatched_products(conn) → list[dict]
  Utiliser la vue v_unmatched_products :
    SELECT key_brandxpdt, brand, product_name, nb_verbatims, first_seen, last_seen
    FROM v_unmatched_products
  Retourner liste de dicts triée par nb_verbatims DESC

export_matching_xls(products: list[dict], referentiel: dict) → bytes
  Générer un XLS avec openpyxl.

  Onglet 1 "Matching à compléter" :
    Colonnes :
      A - key_brandxpdt    : grisée, verrouillée (fond #F3F4F6)
      B - brand            : grisée, verrouillée
      C - product_name     : grisée, verrouillée
      D - nb_verbatims     : grisée, verrouillée
      E - categorie_interne      : éditable (fond #FFFBEB), menu déroulant = toutes les catégories
      F - sous_categorie_interne : éditable (fond #FFFBEB), menu déroulant = toutes les sous-catégories
      G - photo            : éditable (fond #FFFBEB), menu déroulant ["true","false"]

    Header ligne 1 : fond #1F6ED4, texte blanc, bold
    Protection feuille : seules colonnes E, F, G déverrouillées
    Largeurs colonnes adaptées au contenu

  Onglet 2 "Référentiel" :
    Colonnes A (categorie) et B (sous_categorie) depuis referentiel_categories.csv
    Lecture seule, header gris, titre "Valeurs valides"

  Retourner io.BytesIO().getvalue()

validate_matching_xls(file_bytes: bytes, referentiel: dict) → dict
  Lire l'onglet "Matching à compléter"
  Pour chaque ligne retourner dans valid ou errors :
    - categorie vide → erreur "Catégorie manquante"
    - sous_categorie vide → erreur "Sous-catégorie manquante"
    - photo non dans [True, False, "true", "false"] → erreur "Valeur photo invalide"
    - combinaison catégorie+sous_categorie absente du référentiel → erreur "Combinaison invalide"
    - key_brandxpdt modifié → erreur "Clé modifiée"
  Retourner {"valid": [...], "errors": [...], "nb_valid": int, "nb_errors": int}

apply_matching(conn, valid_rows: list[dict], operator: str = "manual") → dict
  Pour chaque row valide :
    Convertir photo str → bool
    Calculer key_brandxpdt = row["brand"] + row["product_name"]
    
    UPSERT categories_mapping :
      INSERT ... ON CONFLICT (key_brandxpdt) DO UPDATE SET
        categorie_interne=..., sous_categorie_interne=..., photo=...,
        matched_at=NOW(), matched_by=operator
    
    UPDATE verbatims SET categorie_interne=..., sous_categorie_interne=..., photo=...
      WHERE brand=%s AND product_name=%s
  
  Retourner {products_matched: int, verbatims_updated: int}

Écrire tests/test_matcher.py.
```

---

### Session 6 — pages/3_Matching.py

**Fichiers contexte :** ce guide, `core/matcher.py`, `core/referentiel.py`, `compass_ui/compass_ui.py`

```
Implémente pages/3_Matching.py — Module 3 Matching catégories.

ÉTAT INITIAL
  inject_css(), theme_toggle(), sidebar_header()
  page_header("Matching catégories", "Assigner catégorie et photo aux nouveaux produits")
  
  products = get_unmatched_products(conn)
  
  Si len(products) == 0 :
    alert(type="success", title="Tous les produits sont matchés")
    empty_state("✓", "Aucun produit en attente", "Tous les produits ont une catégorie.")
    st.stop()
  
  metric_row([
    {"label": "Produits sans catégorie", "value": len(products), "color": "warning"},
    {"label": "Verbatims impactés", "value": sum(p["nb_verbatims"] for p in products), "color": "error"},
  ])
  
  product_status_table(products)

SECTION EXPORT
  section_divider("Export XLS")
  p("Télécharger le fichier, le compléter dans Excel, puis le réimporter ci-dessous.")
  
  xls_bytes = export_matching_xls(products, load_referentiel())
  st.download_button(
    "⬇ Télécharger le fichier de matching",
    data=xls_bytes,
    file_name=f"matching_{date.today()}.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
  )

SECTION RÉIMPORT
  section_divider("Réimport XLS complété")
  
  uploaded = st.file_uploader("Fichier XLS complété", type=["xlsx"])
  
  Si uploaded :
    result = validate_matching_xls(uploaded.read(), load_referentiel())
    
    col1, col2 = st.columns(2)
    col1.metric("Lignes valides", result["nb_valid"])
    col2.metric("Lignes rejetées", result["nb_errors"])
    
    Si result["errors"] :
      alert(type="warning", title=f"{result['nb_errors']} lignes rejetées")
      Afficher tableau des erreurs : key_brandxpdt / colonne / raison
    
    Si result["valid"] :
      Bouton "Appliquer le matching (N produits)"
      
      Si clic :
        matched = apply_matching(conn, result["valid"])
        matching_summary(matched["products_matched"], matched["verbatims_updated"])
        alert(type="success", title="Matching appliqué")
        st.rerun()
```

---

### Session 7 — pages/2_Enrichissement.py + pages/4_Outils.py

**Fichiers contexte :** ce guide, `core/db.py`, `core/referentiel.py`, `compass_ui/compass_ui.py`

```
Implémente pages/2_Enrichissement.py et pages/4_Outils.py.

=== pages/2_Enrichissement.py ===

Filtres dans la sidebar :
  brand    : st.selectbox (valeurs distinctes depuis verbatims)
  source   : st.selectbox
  date_from / date_to : st.date_input
  opinion  : st.selectbox [Tous, positive, negative, neutral]
  catégorie : st.selectbox [Tous, Avec catégorie, Sans catégorie]
  Bouton "Appliquer les filtres"

Résultats :
  SELECT id, verbatim_content, product_name, brand, source, date,
         categorie_interne, sous_categorie_interne, photo
  FROM verbatims WHERE [filtres actifs]
  ORDER BY date DESC
  LIMIT 50 OFFSET (page * 50)

  Afficher nb total de verbatims filtrés
  
  st.data_editor avec colonnes :
    verbatim_content : disabled, max 120 chars affichés
    product_name     : disabled
    brand            : disabled
    categorie_interne      : selectbox depuis referentiel (ou None)
    sous_categorie_interne : selectbox depuis referentiel (ou None)
    photo            : checkbox

  Bouton "Sauvegarder les modifications"
  
  Si clic :
    Identifier les lignes modifiées (comparer avec état initial)
    alert(type="warning", f"Cette action va mettre à jour {n} verbatims")
    UPDATE verbatims SET ... WHERE id = %s pour chaque ligne modifiée
    ⚠ NE PAS mettre à jour categories_mapping depuis ce module
    alert(type="success")

=== pages/4_Outils.py ===

tabs = st.tabs(["Référentiel", "Renommer produit", "Vérification produits", "Logs import"])

Tab 1 — Référentiel :
  Afficher categories_mapping (paginé 50 lignes)
  Filtre texte product_name, filtre selectbox categorie
  Colonnes : brand / product_name / catégorie / sous-catégorie / photo / matched_at
  st.download_button CSV

Tab 2 — Renommer produit :
  st.selectbox "Produit à renommer" (tous les product_name distincts en base)
  Afficher : brand, catégorie actuelle, nb verbatims
  st.text_input "Nouveau nom"
  alert(type="warning") "Cette action va renommer X verbatims et mettre à jour categories_mapping"
  Bouton "Confirmer"
  UPDATE verbatims SET product_name=%s WHERE brand=%s AND product_name=%s
  UPDATE categories_mapping SET product_name=%s, key_brandxpdt=(brand||nouveau_nom)
    WHERE brand=%s AND product_name=%s
  alert(type="success")

Tab 3 — Vérification produits :
  SELECT brand, product_name, COUNT(*) nb_verbatims,
         cm.categorie_interne IS NOT NULL AS is_matched
  FROM verbatims v LEFT JOIN categories_mapping cm ON cm.brand=v.brand AND cm.product_name=v.product_name
  GROUP BY v.brand, v.product_name, is_matched ORDER BY nb_verbatims DESC
  
  Métriques : X matchés / Y à compléter
  log_table ou tableau stylisé avec badge vert/orange par ligne
  Bouton "Aller au Matching →" si Y > 0

Tab 4 — Logs import :
  SELECT * FROM import_logs ORDER BY started_at DESC
  Filtre statut
  log_table(logs)
  Pour chaque log : st.expander avec error_detail si status != success
  st.download_button CSV logs
```

---

### Session 8 — app.py + intégration finale

**Fichiers contexte :** tous les fichiers du projet

```
Implémente app.py et finalise l'intégration.

app.py :
  Configurer st.set_page_config(
    page_title="Compass · Consumer Voice Import",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
  )
  
  from compass_ui.compass_ui import *
  inject_css()
  theme_toggle()
  sidebar_header()
  
  Test connexion DB :
    Si test_connection() échoue :
      alert(type="error", title="Base de données inaccessible",
            message=f"Environnement : {COMPASS_ENV}. Vérifier les credentials.")
      st.stop()
  
  Dans la sidebar (après sidebar_header) :
    COMPASS_ENV actif avec badge cyan/gray
    Nb total verbatims en base
    Nb produits sans catégorie (badge orange si > 0, vert si 0)

Vérifications finales à faire :
  1. Flux complet : import initial → import mensuel → matching → enrichissement
  2. Contrôle doublon : importer deux fois le même fichier → bloqué
  3. Renommage produit en cascade
  4. load_table_co.py --dry-run puis --propagate
  5. Switch light/dark sur toutes les pages
  6. Test Supabase (dev) et PostgreSQL (prod)
```

---

## 7. Règles transverses — à vérifier dans chaque session

```
✓ Clé matching : brand + product_name (JAMAIS product_name seul)
✓ Renommage CSV : product_name_SEMANTIWEB → product_name dans normalize_row()
✓ key_brandxpdt = brand || product_name (concaténation sans séparateur)
✓ Doublon fichier : SHA-256 binaire vérifié avant tout traitement
✓ Modes import : initial (photo remplie) vs mensuel (photo NULL)
✓ inject_css() + theme_toggle() en tête de chaque page
✓ theme_toggle() via st.components.v1.html avec window.parent.document
✓ Secrets Streamlit : st.secrets["database"]["url"] et ["env"]
✓ Port Supabase : 6543 (Transaction pooler), pas 5432
✓ openpyxl : version >=3.1.2,<4.0.0 (import DefinedName changé en v4)
✓ Toutes les erreurs DB → ui.alert(type="error"), jamais de traceback brut
✓ Propagation matching : UPDATE verbatims WHERE brand=x AND product_name=y
```
