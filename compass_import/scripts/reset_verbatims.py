#!/usr/bin/env python3
"""
scripts/reset_verbatims.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Procédure de reprise à zéro de `verbatims` (spec §1.4).

La base `verbatims` est vidée pour rejouer l'import initial avec le
nouveau hash (core/hasher.py, scénario B). Ce script rend l'opération
reproductible et documentée plutôt que faite à la main en console.

Étapes, dans cet ordre, TOUTES nécessaires avant de relancer l'import :
  1. Appliquer la migration de schéma (sql/migrations/0001_...) — ce
     script ne le fait PAS, c'est un préalable manuel (psql/outil de
     migration).
  2. Dump complet (verbatims, import_logs, categories_mapping), horodaté,
     hors dépôt.
  3. TRUNCATE verbatims SEULE — jamais import_logs ni categories_mapping.
  4. Écriture d'une ligne de log import_type='reset' marquant la reprise.
  5. Rejouer l'import initial (pages/1_Import.py) — hors de ce script.

Usage :
    # Dry-run (défaut) : affiche le plan, n'exécute AUCUN TRUNCATE.
    python scripts/reset_verbatims.py

    # Exécution réelle — nécessite --apply ET le nom exact de la base :
    python scripts/reset_verbatims.py --apply --confirm-db compass_verbatims

    # Purge en cascade de tables dépendantes identifiées par
    # scripts/diag_hash.py (Q4) — aucune par défaut :
    python scripts/reset_verbatims.py --apply --confirm-db compass_verbatims \\
        --cascade-table annotations --cascade-table favoris

Sécurité :
    - Sans --apply : aucune écriture, uniquement l'affichage du plan.
    - Avec --apply mais sans --confirm-db : saisie interactive du nom de
      la base demandée (pas de purge accidentelle en scriptant --apply
      seul par erreur).
    - --confirm-db doit correspondre EXACTEMENT au nom de la base ciblée
      par la connexion active (COMPASS_ENV) — sinon le script s'arrête.
    - categories_mapping n'est jamais touchée : ni lue en écriture, ni
      passée en argument possible de --cascade-table (refus explicite).
"""

import argparse
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.db import _build_dsn, get_active_env, get_connection  # noqa: E402

_PROTECTED_TABLES = {"categories_mapping"}


def _target_db_name(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        return cur.fetchone()[0]


def _count_verbatims(conn) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM verbatims")
        return cur.fetchone()[0]


def _run_pg_dump(dump_path: Path) -> None:
    """Dump complet de verbatims, import_logs, categories_mapping.

    Utilise la même DSN que l'application (core.db._build_dsn) — un seul
    endroit qui sait résoudre les credentials selon COMPASS_ENV, pas de
    logique dupliquée ici. pg_dump accepte une chaîne de connexion libpq
    (postgresql://... ou host=... dbname=...) directement en argument.
    """
    dsn = _build_dsn()
    cmd = [
        "pg_dump", dsn,
        "-t", "verbatims",
        "-t", "import_logs",
        "-t", "categories_mapping",
        "-f", str(dump_path),
    ]
    print(f"→ pg_dump vers {dump_path} …")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"pg_dump a échoué (code {result.returncode}) :\n{result.stderr}\n"
            "Purge annulée — aucun TRUNCATE n'a été exécuté."
        )
    print(f"✓ Dump écrit : {dump_path} ({dump_path.stat().st_size:,} octets)")


def _log_reset(conn, dump_path: Path, n_deleted: int) -> str:
    """Insère la ligne de log marquant la reprise (spec §1.4).

    ``file_hash`` n'a pas de sens pour un reset (pas de fichier CSV importé)
    mais la colonne est ``NOT NULL`` et porte un index unique partiel sur
    les statuts hors erreur/doublon (cf. sql/schema.sql). On y met un
    SHA-256 dérivé de ``batch_id`` — garanti unique à chaque reset, pour
    qu'un second reset ultérieur n'entre jamais en collision avec le premier.
    """
    batch_id = str(uuid.uuid4())
    reset_hash = sha256(batch_id.encode("utf-8")).hexdigest()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO import_logs
                (id, file_hash, filename, import_type, started_at,
                 finished_at, rows_total, rows_inserted, status)
            VALUES (%s, %s, %s, 'reset', NOW(), NOW(), %s, 0, 'success')
            """,
            (
                batch_id,
                reset_hash,
                str(dump_path),
                n_deleted,
            ),
        )
    conn.commit()
    return batch_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reprise à zéro de verbatims (spec §1.4) — dry-run par défaut."
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="Exécute réellement la purge. Sans ce flag : plan affiché, rien n'est modifié.",
    )
    parser.add_argument(
        "--confirm-db", default=None,
        help="Nom exact de la base à purger — doit correspondre à la base ciblée par COMPASS_ENV.",
    )
    parser.add_argument(
        "--dump-dir", default=str(Path.home() / "compass_dumps"),
        help="Répertoire du dump, HORS DÉPÔT (défaut : ~/compass_dumps).",
    )
    parser.add_argument(
        "--cascade-table", action="append", default=[], metavar="TABLE",
        help=(
            "Table dépendante à TRUNCATE CASCADE avec verbatims (répétable). "
            "À renseigner UNIQUEMENT si scripts/diag_hash.py (Q4) a identifié "
            "cette table comme dépendante de verbatims.id. categories_mapping "
            "est refusée quelle que soit la valeur passée ici."
        ),
    )
    args = parser.parse_args()

    for t in args.cascade_table:
        if t in _PROTECTED_TABLES:
            print(f"❌ '{t}' est protégée — categories_mapping ne doit jamais être purgée (spec §1.4).")
            sys.exit(1)

    print("=" * 70)
    print("  Compass · Consumer Voice — Reprise à zéro de verbatims")
    print(f"  Environnement : {get_active_env().upper()}")
    print("=" * 70)

    with get_connection() as conn:
        db_name = _target_db_name(conn)
        n_verbatims = _count_verbatims(conn)

        print(f"\nBase ciblée         : {db_name}")
        print(f"Verbatims en base   : {n_verbatims:,}")
        print(f"Tables TRUNCATE      : verbatims"
              + (f", {', '.join(args.cascade_table)}" if args.cascade_table else ""))
        print("Tables PRÉSERVÉES    : import_logs (conservée pour l'audit), "
              "categories_mapping (jamais purgée)")
        print(f"Répertoire de dump   : {args.dump_dir}")

        if not args.apply:
            print(
                "\n— DRY-RUN — aucun --apply fourni : aucun TRUNCATE ne sera "
                "exécuté. Ceci est uniquement le plan.\n"
                "Pour exécuter réellement :\n"
                f"    python scripts/reset_verbatims.py --apply --confirm-db {db_name}"
            )
            return

        confirm_db = args.confirm_db
        if confirm_db is None:
            confirm_db = input(
                f"\n⚠ Purge IRRÉVERSIBLE de '{db_name}' ({n_verbatims:,} verbatims). "
                f"Tapez le nom exact de la base pour confirmer : "
            ).strip()

        if confirm_db != db_name:
            print(
                f"❌ Confirmation refusée : '{confirm_db}' ne correspond pas à "
                f"la base ciblée '{db_name}'. Purge annulée."
            )
            sys.exit(1)

        # ── 1. Dump ──────────────────────────────────────────────────────────
        dump_dir = Path(args.dump_dir).expanduser()
        dump_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        dump_path = dump_dir / f"compass_reset_{db_name}_{timestamp}.sql"

        try:
            _run_pg_dump(dump_path)
        except FileNotFoundError:
            print(
                "❌ pg_dump introuvable dans le PATH. Installez les outils "
                "client PostgreSQL avant de relancer. Purge annulée."
            )
            sys.exit(1)
        except RuntimeError as exc:
            print(f"❌ {exc}")
            sys.exit(1)

        # ── 2. TRUNCATE ──────────────────────────────────────────────────────
        cascade_sql = ", " + ", ".join(args.cascade_table) if args.cascade_table else ""
        truncate_sql = f"TRUNCATE verbatims{cascade_sql}"
        print(f"\n→ {truncate_sql} …")
        with conn.cursor() as cur:
            cur.execute(truncate_sql)
        conn.commit()
        print(f"✓ {n_verbatims:,} verbatim(s) supprimé(s).")

        # ── 3. Ligne de log 'reset' ──────────────────────────────────────────
        batch_id = _log_reset(conn, dump_path, n_verbatims)
        print(f"✓ Ligne de log 'reset' écrite (id={batch_id}).")

    print("\n" + "=" * 70)
    print("  Reprise terminée. Étapes suivantes (spec §1.4) :")
    print("  6. Rejouer l'import initial et vérifier que rows_duplicates ≈ 0.")
    print("  7. Vérifier qu'apply_known_categories retrouve les catégories.")
    print("  8. Ouvrir l'onglet Logs et vérifier l'affichage des imports antérieurs.")
    print("=" * 70)


if __name__ == "__main__":
    main()
