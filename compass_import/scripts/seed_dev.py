#!/usr/bin/env python3
"""
scripts/seed_dev.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Script de peuplement de la base de développement.

Effets :
  1. Génère data/sample_verbatims.csv si le fichier n'existe pas
  2. Importe 50 verbatims de test (mode "mensuel") — idempotent
  3. Insère 3 entrées dans categories_mapping (ON CONFLICT DO NOTHING)
  4. Insère 2 entrées dans import_logs dont 1 doublon (ON CONFLICT DO NOTHING)

Utilisation :
    cd compass_import
    python scripts/seed_dev.py

Prérequis :
    - Variables d'environnement configurées (.env)
    - Schema SQL appliqué sur la base cible
"""

import csv
import sys
import uuid
from pathlib import Path

# ── Résolution du chemin racine ───────────────────────────────────────────────
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.db import get_active_env, get_connection, test_connection
from core.hasher import file_hash, verbatim_hash
from core.importer import import_batch, normalize_row
from core.referentiel import load_referentiel

# ── Données de test ───────────────────────────────────────────────────────────
_BRANDS   = ["Bioderma", "La Roche-Posay", "Uriage", "Avène", "CeraVe"]
_SOURCES  = ["Amazon", "Sephora", "Nocibé", "Marionnaud", "Douglas"]
_OPINIONS = ["positive", "negative", "neutral"]
_COUNTRIES = ["FR", "BE", "CH", "DE", "ES"]

_PRODUCTS = [
    ("Bioderma Sensibio H2O",          "Face Care"),
    ("La Roche-Posay Toleriane",        "Face Care"),
    ("Uriage Eau Thermale Crème",       "Body Care"),
    ("Avène Cicalfate+",                "Healing Care"),
    ("CeraVe Hydrating Cleanser",       "Face Care"),
    ("Bioderma Atoderm Huile",          "Body Care"),
    ("La Roche-Posay Lipikar",          "Body Care"),
    ("Uriage Baby 1ère Crème",          "Baby Care"),
    ("Avène Solaire SPF50",             "Sun Care"),
    ("CeraVe Moisturizing Cream",       "Body Care"),
]

_VERBATIMS = [
    "Excellent produit, ma peau est enfin hydratée après des années de recherche.",
    "Très bonne formule, sans parfum, idéale pour les peaux sensibles.",
    "Résultats visibles dès la première semaine d'utilisation.",
    "Texture légère et non grasse, s'absorbe rapidement.",
    "Un peu cher mais ça vaut vraiment le coup.",
    "Convient parfaitement à ma peau sèche et atopique.",
    "Le flacon est pratique et hygiénique.",
    "Mon dermatologue me l'a recommandé, je ne suis pas déçue.",
    "Parfum discret et agréable, pas d'irritation.",
    "Produit de qualité, packaging soigné.",
    "Résultat mitigé, amélioration légère mais pas miraculeuse.",
    "Peau plus douce après quelques jours, continue à tester.",
    "Bon rapport qualité-prix pour une marque dermatologique.",
    "Ma fille l'utilise aussi, on est toutes les deux satisfaites.",
    "Commande rapide, produit conforme à la description.",
    "Utilisé depuis 3 mois, ma peau tolère très bien.",
    "Idéal pour les peaux réactives et sensibles aux cosmétiques classiques.",
    "Se rince facilement, ne laisse pas de résidu.",
    "J'aurais préféré un conditionnement plus grand.",
    "Efficace contre les rougeurs, recommande vraiment.",
    "Produit décevant, pas d'amélioration notable après un mois.",
    "Sensation de film sur la peau, pas très agréable.",
    "Odeur légèrement chimique mais tolerable.",
    "Emballage abîmé à la livraison, produit OK.",
    "Aurait pu être plus hydratant pour le prix.",
]

_ATTR_FIELDS = [
    "attribute_Efficiency", "attribute_Packaging", "attribute_Price",
    "attribute_Quality", "attribute_Scent", "attribute_Taste",
    "attribute_Texture", "attribute_Safety", "attribute_Composition",
]


def _make_row(i: int) -> dict:
    """Génère une ligne CSV de test (index 0-49)."""
    brand   = _BRANDS[i % len(_BRANDS)]
    source  = _SOURCES[i % len(_SOURCES)]
    country = _COUNTRIES[i % len(_COUNTRIES)]
    opinion = _OPINIONS[i % len(_OPINIONS)]
    prod    = _PRODUCTS[i % len(_PRODUCTS)][0]
    verbatim = _VERBATIMS[i % len(_VERBATIMS)]
    date_str = f"{2023 + (i % 2):04d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"

    row = {
        "guid":                    str(uuid.uuid4()),
        "brand":                   brand,
        "country":                 country,
        "date":                    date_str,
        "opinion":                 opinion,
        "product_name_SEMANTIWEB": prod,
        "rating":                  str((i % 5) + 1),
        "source":                  source,
        "verbatim_content":        f"[Test {i + 1}] {verbatim}",
        "sampling":                "0" if i % 3 else "1",
    }
    for attr in _ATTR_FIELDS:
        row[attr] = "positive" if i % 7 == 0 else "0"
    return row


# ── Génération du CSV ─────────────────────────────────────────────────────────

def generate_sample_csv(path: Path) -> None:
    """Écrit data/sample_verbatims.csv avec 50 lignes de test."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = (
        ["guid", "brand", "country", "date", "opinion",
         "product_name_SEMANTIWEB", "rating", "source",
         "verbatim_content", "sampling"]
        + _ATTR_FIELDS
    )
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        for i in range(50):
            writer.writerow(_make_row(i))
    print(f"  ✓ CSV généré : {path} (50 lignes)")


# ── Import verbatims ──────────────────────────────────────────────────────────

def seed_verbatims(conn, csv_path: Path) -> None:
    """Importe les verbatims depuis le CSV de test."""
    import pandas as pd
    from core.importer import parse_csv

    with open(csv_path, "rb") as f:
        raw = f.read()

    df = parse_csv(raw)
    batch_id = str(uuid.uuid4())

    # Créer le log d'import
    fhash = file_hash(raw)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_logs
                (id, file_hash, filename, import_type, rows_total, status)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (batch_id, fhash, csv_path.name, "mensuel", len(df), "running"),
        )
    conn.commit()

    # Normaliser et insérer
    rows = []
    for _, row in df.iterrows():
        try:
            rows.append(normalize_row(row, "mensuel"))
        except Exception as exc:
            print(f"    ⚠ Ligne ignorée : {exc}")

    result = import_batch(conn, rows, batch_id)

    # Finaliser le log
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE import_logs
               SET finished_at   = NOW(),
                   rows_inserted = %s,
                   rows_skipped  = %s,
                   rows_matched  = 0,
                   rows_unmatched = %s,
                   status        = %s
             WHERE id = %s
            """,
            (
                result["inserted"],
                result["skipped"],
                result["inserted"],
                "success" if not result["errors"] else "partial",
                batch_id,
            ),
        )
    conn.commit()

    print(f"  ✓ Verbatims importés : {result['inserted']} insérés, "
          f"{result['skipped']} skippés")


# ── Seed categories_mapping ───────────────────────────────────────────────────

def seed_categories(conn) -> None:
    """Insère 3 entrées de test dans categories_mapping."""
    entries = [
        ("Bioderma",      "Bioderma Sensibio H2O",     "Face Care", "Face Care : Cleanser",        False),
        ("La Roche-Posay","La Roche-Posay Toleriane",  "Face Care", "Face Care : Moisturizer",      True),
        ("Uriage",        "Uriage Eau Thermale Crème", "Body Care", "Body Care : Body Moisturizer", False),
    ]
    with conn.cursor() as cur:
        for brand, product_name, cat, sous_cat, photo in entries:
            key = brand + product_name
            cur.execute(
                """
                INSERT INTO categories_mapping
                    (key_brandxpdt, brand, product_name,
                     categorie_interne, sous_categorie_interne,
                     photo, matched_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (key_brandxpdt) DO NOTHING
                """,
                (key, brand, product_name, cat, sous_cat, photo, "seed_dev"),
            )
    conn.commit()
    print("  ✓ categories_mapping : 3 entrées insérées (ON CONFLICT DO NOTHING)")


# ── Seed import_logs doublon ──────────────────────────────────────────────────

def seed_import_logs(conn) -> None:
    """Insère 1 entrée import_logs avec statut 'duplicate' pour test."""
    dummy_hash = "a" * 64  # Hash fictif (ne doit pas exister en vrai)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_logs
                (id, file_hash, filename, import_type, rows_total,
                 rows_inserted, rows_skipped, status, finished_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT DO NOTHING
            """,
            (
                str(uuid.uuid4()),
                dummy_hash,
                "export_semantiweb_jan2024.csv",
                "mensuel",
                45000, 0, 0,
                "duplicate",
            ),
        )
    conn.commit()
    print("  ✓ import_logs : 1 entrée 'duplicate' insérée (ON CONFLICT DO NOTHING)")


# ── Propagation des catégories connues ───────────────────────────────────────

def propagate_categories(conn) -> None:
    """Propage les catégories de categories_mapping vers verbatims."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE verbatims AS v
               SET categorie_interne      = cm.categorie_interne,
                   sous_categorie_interne = cm.sous_categorie_interne,
                   photo                  = cm.photo
              FROM categories_mapping cm
             WHERE v.brand = cm.brand
               AND v.product_name = cm.product_name
               AND v.categorie_interne IS NULL
        """)
        updated = cur.rowcount
    conn.commit()
    print(f"  ✓ Catégories propagées : {updated} verbatim(s) mis à jour")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    csv_path = _ROOT / "data" / "sample_verbatims.csv"

    print(f"\n{'=' * 60}")
    print("  Compass · Consumer Voice — Seed développement")
    print(f"  Environnement : {get_active_env().upper()}")
    print(f"{'=' * 60}\n")

    # 1. Test connexion
    print("[ 1/5 ] Test de connexion…")
    if not test_connection():
        print("  ✕ Connexion échouée. Vérifiez .env et la base de données.")
        sys.exit(1)
    print("  ✓ Connexion OK")

    # 2. Générer le CSV si nécessaire
    print(f"\n[ 2/5 ] Génération du CSV de test ({csv_path.name})…")
    if csv_path.exists():
        print(f"  ✓ Fichier déjà existant ({csv_path}) — conservé")
    else:
        generate_sample_csv(csv_path)

    # 3. Import verbatims
    print("\n[ 3/5 ] Import des verbatims de test…")
    try:
        with get_connection() as conn:
            seed_verbatims(conn, csv_path)
    except Exception as exc:
        print(f"  ✕ Erreur import verbatims : {exc}")

    # 4. categories_mapping
    print("\n[ 4/5 ] Seed categories_mapping…")
    try:
        with get_connection() as conn:
            seed_categories(conn)
            propagate_categories(conn)
    except Exception as exc:
        print(f"  ✕ Erreur categories_mapping : {exc}")

    # 5. import_logs doublon
    print("\n[ 5/5 ] Seed import_logs (entrée doublon)…")
    try:
        with get_connection() as conn:
            seed_import_logs(conn)
    except Exception as exc:
        print(f"  ✕ Erreur import_logs : {exc}")

    print(f"\n{'=' * 60}")
    print("  Seed terminé. Lancez l'application :")
    print(f"    cd {_ROOT}")
    print("    streamlit run app.py")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
