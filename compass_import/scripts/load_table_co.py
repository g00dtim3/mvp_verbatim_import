#!/usr/bin/env python3
"""
scripts/load_table_co.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Import de la table de référence Table_CO.csv dans categories_mapping.

API publique :
    run(dry_run, propagate, reset)   ← appelé depuis pages/99_Admin.py
    main()                           ← CLI (python scripts/load_table_co.py)

Colonnes attendues dans Table_CO.csv :
  Key brandxpdt          = brand || product_name (concaténé, sans séparateur)
  brand                  = marque
  product_name_SEMANTIWEB = nom produit API
  categorie interne
  sous categorie interne
  photo                  = oui / non / true / false

Utilisation CLI :
    cd compass_import
    python scripts/load_table_co.py [--dry-run]

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


# ── Logger partagé ────────────────────────────────────────────────────────────

def _make_log(use_streamlit: bool):
    """
    Retourne une fonction log(msg).
    Si use_streamlit=True et qu'un contexte Streamlit est actif, utilise
    st.write() ; sinon print().
    """
    if use_streamlit:
        try:
            import streamlit as st
            # Tester qu'un contexte de script est bien actif
            _ = st.session_state  # lève une exception hors contexte
            return st.write
        except Exception:
            pass
    return print


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
    warnings : messages pour les lignes ignorées.
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

            if not key:
                if brand and product_name:
                    key = brand + product_name
                else:
                    warnings.append(
                        f"Ligne {lineno} ignorée — key_brandxpdt, brand ou "
                        f"product_name vide"
                    )
                    continue

            if not cat or not sous_cat:
                warnings.append(
                    f"Ligne {lineno} ignorée — categorie ou sous_categorie vide "
                    f"pour « {key} »"
                )
                continue

            rows.append((key, brand, product_name, cat, sous_cat, photo))

    return rows, warnings


# ── Opérations base ───────────────────────────────────────────────────────────

def _upsert(conn, rows: list[tuple], log) -> int:
    """UPSERT par batches dans categories_mapping. Retourne le nombre upserted."""
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
        log(f"  {upserted:,} / {len(rows):,} lignes upsertées…")
    return upserted


def _propagate(conn, log) -> int:
    """Propage categories_mapping → verbatims sur brand + product_name."""
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
        n = cur.rowcount
    conn.commit()
    log(f"  ✓ {n:,} verbatim(s) mis à jour")
    return n


# ── API publique ──────────────────────────────────────────────────────────────

def run(
    dry_run:   bool = True,
    propagate: bool = False,
    reset:     bool = False,
    csv_path:  Path | None = None,
) -> dict:
    """
    Point d'entrée appelable depuis Streamlit (pages/99_Admin.py) ou la CLI.

    Args:
        dry_run:   Si True, lit et valide le CSV mais n'écrit rien en base.
        propagate: Si True, propage les catégories vers verbatims après l'UPSERT.
        reset:     Si True, vide categories_mapping avant l'UPSERT (TRUNCATE).
        csv_path:  Chemin vers Table_CO.csv. Défaut : data/Table_CO.csv.

    Returns:
        dict { "rows_read": int, "warnings": int,
               "upserted": int, "propagated": int }

    Écrit les logs via st.write() si un contexte Streamlit est actif,
    sinon via print().
    """
    log = _make_log(use_streamlit=True)
    path = Path(csv_path) if csv_path else _CSV_PATH

    # ── 1. Vérification fichier ───────────────────────────────────────────────
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {path}\n"
            "Placez Table_CO.csv dans compass_import/data/ et relancez."
        )
    log(f"✓ Fichier : {path.name}")

    # ── 2. Lecture CSV ────────────────────────────────────────────────────────
    rows, warnings = read_table_co(path)
    log(f"✓ {len(rows):,} lignes valides lues ({len(warnings)} ignorée(s))")
    for w in warnings[:5]:
        log(f"  ⚠ {w}")
    if len(warnings) > 5:
        log(f"  ⚠ … et {len(warnings) - 5} avertissement(s) supplémentaire(s)")

    if dry_run:
        log("— Dry-run : aucune écriture en base.")
        return {"rows_read": len(rows), "warnings": len(warnings),
                "upserted": 0, "propagated": 0}

    if not rows:
        log("⚠ Aucune ligne valide à importer.")
        return {"rows_read": 0, "warnings": len(warnings),
                "upserted": 0, "propagated": 0}

    # ── 3. Écriture en base ───────────────────────────────────────────────────
    with get_connection() as conn:
        if reset:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM categories_mapping")
                deleted = cur.rowcount
            conn.commit()
            log(f"✓ Table vidée ({deleted:,} entrée(s) supprimée(s))")

        upserted = _upsert(conn, rows, log)
        log(f"✓ {upserted:,} entrées upsertées dans categories_mapping")

        n_propagated = 0
        if propagate:
            n_propagated = _propagate(conn, log)

    return {
        "rows_read":  len(rows),
        "warnings":   len(warnings),
        "upserted":   upserted,
        "propagated": n_propagated,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    dry_run = "--dry-run" in sys.argv

    print(f"\n{'=' * 60}")
    print("  Compass · Consumer Voice — Import Table_CO")
    print(f"  Environnement : {get_active_env().upper()}")
    if dry_run:
        print("  MODE : DRY-RUN (aucune écriture en base)")
    print(f"{'=' * 60}\n")

    if not dry_run:
        print("[ 0/1 ] Test de connexion…")
        if not test_connection():
            print("  ✕ Connexion échouée. Vérifiez .env et la base de données.")
            sys.exit(1)
        print("  ✓ Connexion OK\n")

    try:
        result = run(dry_run=dry_run, propagate=True, reset=False)
    except FileNotFoundError as exc:
        print(f"  ✕ {exc}")
        sys.exit(1)
    except Exception as exc:
        print(f"  ✕ Erreur : {exc}")
        sys.exit(1)

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"  Dry-run terminé : {result['rows_read']:,} lignes valides.")
        print("  Relancez sans --dry-run pour importer.")
    else:
        print(f"  Import terminé.")
        print(f"  Upserted : {result['upserted']:,}")
        print(f"  Verbatims mis à jour : {result['propagated']:,}")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
