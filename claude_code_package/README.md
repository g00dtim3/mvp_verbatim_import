# Compass · Consumer Voice — Import Pipeline

Module Streamlit d'import, matching et enrichissement de verbatims consommateurs.

## Stack

| Couche | Technologie |
|--------|-------------|
| Interface | Streamlit (Posit) |
| Langage | Python 3.11+ |
| Base de données (prod) | PostgreSQL |
| Base de données (dev) | Supabase |
| Génération XLS | openpyxl |
| Design system | Compass UI (custom) |

## Installation

```bash
# 1. Cloner et installer les dépendances
pip install -r requirements.txt

# 2. Configurer l'environnement
cp .env.example .env
# Renseigner les valeurs dans .env

# 3. Créer les tables
# Sur Supabase : coller sql/schema.sql dans l'éditeur SQL
# Sur PostgreSQL : psql -d votre_base -f sql/schema.sql

# 4. Préparer les données de développement (optionnel)
python scripts/seed_dev.py

# 5. Lancer l'application
streamlit run app.py
```

## Structure

```
compass_import/
├── app.py                        # Point d'entrée — injection CSS, test DB, navigation
├── pages/
│   ├── 1_Import.py               # Module 1 — Import mensuel + initial
│   ├── 2_Enrichissement.py       # Module 2 — Enrichissement manuel verbatims
│   ├── 3_Matching.py             # Module 3 — Matching catégories via XLS
│   └── 4_Outils.py               # Module 4 — Maintenance et consultation
├── core/
│   ├── db.py                     # Connexion PostgreSQL / Supabase
│   ├── hasher.py                 # SHA-256 verbatim + contrôle doublon fichier
│   ├── importer.py               # Parsing CSV, validation, INSERT batch
│   ├── matcher.py                # Export XLS, validation réimport, propagation
│   └── referentiel.py            # Chargement et validation référentiel catégories
├── compass_ui/
│   ├── style.css                 # Design system CSS light/dark
│   └── compass_ui.py             # Composants UI Python
├── data/
│   └── referentiel_categories.csv  # Référentiel fermé catégories
├── sql/
│   ├── schema.sql                # DDL tables + index + vues
│   └── migrations/               # Migrations numérotées futures
├── tests/
│   ├── test_hasher.py
│   ├── test_importer.py
│   ├── test_matcher.py
│   └── test_referentiel.py
├── scripts/
│   └── seed_dev.py               # Données de test pour développement
├── config.toml                   # Configuration app + DB dev/prod
├── .env.example                  # Template variables d'environnement
└── requirements.txt
```

## Environnements

```bash
# Développement (Supabase)
export COMPASS_ENV=dev
streamlit run app.py

# Production (PostgreSQL)
export COMPASS_ENV=prod
streamlit run app.py
```

## Modules

### Module 1 — Import
- Deux modes : **mensuel** (catégorie/photo NULL) et **initial** (tous champs remplis)
- Contrôle doublon par hash SHA-256 du contenu fichier avant tout traitement
- Import en batch de 1 000 lignes avec `ON CONFLICT DO NOTHING`
- Mapping automatique des catégories connues à l'import

### Module 2 — Enrichissement
- Mise à jour manuelle des champs catégorie et photo par verbatim
- Filtres : brand, source, date, opinion, statut catégorie
- Édition inline via `st.data_editor`

### Module 3 — Matching catégories
- Détection automatique des produits sans catégorie
- Export XLS avec menus déroulants (catégorie, sous-catégorie, photo)
- Validation stricte contre le référentiel CSV
- Propagation en cascade sur tous les verbatims du produit

### Module 4 — Outils
- Table de correspondance produits ↔ catégories
- Renommage produit en cascade (verbatims + categories_mapping)
- Vérification noms produits avec statut matching
- Historique imports avec détail des erreurs

## Design system

Le design system Compass est injecté via `compass_ui/style.css`.
Tokens CSS avec switch light/dark intégré.
Voir `compass_ui/compass_ui.py` pour l'API des composants.

## Sessions Claude Code

Voir `CLAUDE_CODE_GUIDE.md` pour les prompts de chaque session de développement.
Ordre recommandé : Session 1 → 2 → 3 → 4 → 5 → 6 → 7
