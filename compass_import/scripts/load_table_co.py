#!/usr/bin/env python3
"""
scripts/load_table_co.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Import de la table de référence Table_CO.csv dans categories_mapping.

Effets :
  1. Lit data/Table_CO.csv (encodage UTF-8 BOM, séparateur ; ou ,)
  2. UPSERT de toutes les lignes valides dans categories_mapping
  3. Propage les catégories sur TOUS les verbatims existants (brand + product_name)

Colonnes attendues dans Table_CO.csv :
  Key brandxpdt          = brand || product_name (concaténé, sans séparateur)
  brand                  = marque
  product_name_SEMANTIWEB = nom produit API
  categorie interne
  sous categorie interne
  photo                  = oui / non / true / false

Utilisation :
    cd compass_import
    python scripts/load_table_co.py [--dry-run]

Options :
    --dry-run    Lit et valide le CSV sans écrire en base.

Prérequis :
    - Variables d'environnement configurées (.env)
    - Schema SQL v1.3 appliqué (categories_mapping avec key_brandxpdt)
    - data/Table_CO.csv présent dans compass_import/data/
"""

import csv
import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.db import get_active_env, get_connection, test_connection
from psycopg2.extras import execute_values

_CSV_PATH = _ROOT / "data" / "Table_CO.csv"

_REQUIRED_COLS = {
    "Key brandxpdt",
    "brand",
    "product_name_SEMANTIWEB",
    "categorie interne",
    "sous categorie interne",
    "photo",
}

_BATCH_SIZE = 500


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_sep(path: Path) -> str:
    """Détecte le séparateur CSV (';' ou ',') depuis les 8 Ko initiaux."""
    with open(path, "r", encoding="utf-8-sig") as f:
        sample = f.read(8192)
    return ";" if sample.count(";") >= sample.count(",") else ","


def _parse_photo(val: str) -> bool | None:
    """Convertit oui/non/true/false/1/0 → bool. Retourne None si non reconnu."""
    v = (val or "").strip().lower()
    if v in ("oui", "true", "1", "yes"):
        return True
    if v in ("non", "false", "0", "no"):
        return False
    return None


# ── Lecture CSV ───────────────────────────────────────────────────────────────

def read_table_co(csv_path: Path) -> tuple[list[tuple], list[str]]:
    """
    Lit Table_CO.csv et retourne (rows, warnings).

    rows : liste de tuples (key_brandxpdt, brand, product_name,
                            categorie_interne, sous_categorie_interne, photo)
    warnings : lignes ignorées avec raison.
    """
    sep = _detect_sep(csv_path)
    rows: list[tuple] = []
    warnings: list[str] = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=sep)

        fieldnames = reader.fieldnames or []
        missing = _REQUIRED_COLS - set(fieldnames)
        if missing:
            raise ValueError(
                f"Colonnes manquantes dans {csv_path.name} : "
                f"{', '.join(sorted(missing))}\n"
                f"Colonnes trouvées : {', '.join(fieldnames)}"
            )

        for lineno, row in enumerate(reader, start=2):
            key          = (row.get("Key brandxpdt") or "").strip()
            brand        = (row.get("brand") or "").strip()
            product_name = (row.get("product_name_SEMANTIWEB") or "").strip()
            cat          = (row.get("categorie interne") or "").strip()
            sous_cat     = (row.get("sous categorie interne") or "").strip()
            photo        = _parse_photo(row.get("photo", ""))

            # Vérification cohérence de la clé
            expected_key = brand + product_name
            if key and key != expected_key:
                # Utiliser la clé du fichier comme référence (source de vérité)
                pass  # on conserve la clé du fichier

            if not key:
                # Reconstituer la clé si absente
                if brand and product_name:
                    key = expected_key
                else:
                    warnings.append(
                        f"Ligne {lineno} ignorée — key_brandxpdt, brand ou "
                        f"product_name vide : {dict(row)}"
                    )
                    continue

            if not cat or not sous_cat:
                warnings.append(
                    f"Ligne {lineno} ignorée — categorie ou sous_categorie vide "
                    f"pour '{key}'"
                )
                continue

            rows.append((key, brand, product_name, cat, sous_cat, photo))

    return rows, warnings


# ── Import en base ────────────────────────────────────────────────────────────

def load_table_co(conn, rows: list[tuple]) -> dict:
    """
    UPSERT les rows dans categories_mapping puis propage sur verbatims.

    Args:
        conn: Connexion psycopg2 (autocommit=False).
        rows: Liste de tuples (key_brandxpdt, brand, product_name,
                               categorie_interne, sous_categorie_interne, photo).

    Returns:
        dict { "upserted": int, "propagated": int }
    """
    if not rows:
        return {"upserted": 0, "propagated": 0}

    # ── UPSERT categories_mapping ─────────────────────────────────────────────
    upsert_sql = """
        INSERT INTO categories_mapping
            (key_brandxpdt, brand, product_name,
             categorie_interne, sous_categorie_interne, photo, matched_by)
        VALUES %s
        ON CONFLICT (key_brandxpdt) DO UPDATE SET
            brand                  = EXCLUDED.brand,
            product_name           = EXCLUDED.product_name,
            categorie_interne      = EXCLUDED.categorie_interne,
            sous_categorie_interne = EXCLUDED.sous_categorie_interne,
            photo                  = EXCLUDED.photo,
            matched_at             = NOW(),
            matched_by             = 'load_table_co'
    """

    upserted = 0
    for start in range(0, len(rows), _BATCH_SIZE):
        chunk = rows[start : start + _BATCH_SIZE]
        values = [r + ("load_table_co",) for r in chunk]
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, values)
        conn.commit()
        upserted += len(chunk)
        print(f"    {upserted:,} / {len(rows):,} lignes upsertées…")

    # ── Propagation sur verbatims (toutes lignes, même déjà catégorisées) ────
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE verbatims AS v
               SET categorie_interne      = cm.categorie_interne,
                   sous_categorie_interne = cm.sous_categorie_interne,
                   photo                  = cm.photo
              FROM categories_mapping cm
             WHERE v.brand = cm.brand
               AND v.product_name = cm.product_name
        """)
        propagated = cur.rowcount
    conn.commit()

    return {"upserted": upserted, "propagated": propagated}


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    dry_run = "--dry-run" in sys.argv

    print(f"\n{'=' * 60}")
    print("  Compass · Consumer Voice — Import Table_CO")
    print(f"  Environnement : {get_active_env().upper()}")
    if dry_run:
        print("  MODE : DRY-RUN (aucune écriture en base)")
    print(f"{'=' * 60}\n")

    # 1. Vérification du fichier CSV
    print(f"[ 1/3 ] Vérification du fichier {_CSV_PATH.name}…")
    if not _CSV_PATH.exists():
        print(f"  ✕ Fichier introuvable : {_CSV_PATH}")
        print("  Placez Table_CO.csv dans compass_import/data/ et relancez.")
        sys.exit(1)
    print(f"  ✓ Fichier trouvé ({_CSV_PATH})")

    # 2. Lecture + validation CSV
    print("\n[ 2/3 ] Lecture et validation du CSV…")
    try:
        rows, warnings = read_table_co(_CSV_PATH)
    except ValueError as exc:
        print(f"  ✕ Erreur CSV : {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"  ✕ Erreur inattendue : {exc}")
        sys.exit(1)

    print(f"  ✓ {len(rows):,} lignes valides lues")
    if warnings:
        print(f"  ⚠ {len(warnings)} ligne(s) ignorée(s) :")
        for w in warnings[:10]:
            print(f"      {w}")
        if len(warnings) > 10:
            print(f"      … et {len(warnings) - 10} autre(s)")

    if dry_run:
        print("\n  [DRY-RUN] Aucune écriture effectuée.")
        print(f"\n{'=' * 60}")
        print("  Dry-run terminé — relancez sans --dry-run pour importer.")
        print(f"{'=' * 60}\n")
        return

    # 3. Test connexion + import
    print("\n[ 3/3 ] Test de connexion et import…")
    if not test_connection():
        print("  ✕ Connexion échouée. Vérifiez .env et la base de données.")
        sys.exit(1)
    print("  ✓ Connexion OK")

    try:
        with get_connection() as conn:
            result = load_table_co(conn, rows)
    except Exception as exc:
        print(f"  ✕ Erreur lors de l'import : {exc}")
        sys.exit(1)

    print(f"\n  ✓ {result['upserted']:,} entrées upsertées dans categories_mapping")
    print(f"  ✓ {result['propagated']:,} verbatim(s) mis à jour")

    print(f"\n{'=' * 60}")
    print("  Import Table_CO terminé.")
    print(f"  Lancez l'application pour vérifier :")
    print(f"    cd {_ROOT}")
    print("    streamlit run app.py")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
