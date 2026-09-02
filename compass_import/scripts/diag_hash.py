"""
scripts/diag_hash.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Investigation préalable au Lot 1 (gestion des hash d'unicité) — spec
SPEC_import_hash_et_tracabilite_2.md §1.2.

Script de LECTURE SEULE : aucune écriture, ni sur le CSV, ni en base.
Répond aux 4 questions bloquantes avant d'implémenter le scénario A ou B
du nouveau hash :

  Q1 — Le guid Semantiweb est-il unique DANS un fichier ?
  Q2 — Le guid est-il STABLE entre deux exports mensuels ? (bloquant —
       conditionne le choix scénario A vs B)
  Q3 — Quelle est l'ampleur des collisions du hash actuel (4 champs),
       ventilée par cause ?
  Q4 — Qu'est-ce qui dépend de verbatims.id (FK formelles + candidats
       sans contrainte) ? La base sera vidée : tout ce qui référence ces
       id devra être purgé ou reconstruit.

Usage :
    python scripts/diag_hash.py fichier_mois_A.csv [fichier_mois_B.csv]

    fichier_mois_A.csv : export CSV Semantiweb réel, utilisé pour Q1 et Q3.
    fichier_mois_B.csv : optionnel, export d'un AUTRE mois — nécessaire
                          pour Q2 (stabilité du guid). Sans ce second
                          fichier, Q2 est signalée comme non testable.

Q4 nécessite une connexion base (core.db.get_connection) ; si elle échoue
(pas de .env / pas de réseau), Q4 est signalée comme non testable plutôt
que de faire échouer tout le script — Q1-Q3 restent utiles seules.
"""

import sys
from collections import Counter
from datetime import datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _read_csv(path: str) -> pd.DataFrame:
    """Lecture brute, indépendante de core.importer.parse_csv : ce script
    doit fonctionner même si le pipeline d'import est en cours de refonte."""
    with open(path, "rb") as f:
        file_bytes = f.read()
    from io import BytesIO
    return pd.read_csv(
        BytesIO(file_bytes), sep=";", encoding="utf-8-sig",
        dtype=str, keep_default_na=True, engine="python",
        on_bad_lines="skip",
    )


def _clean(val) -> str:
    """Chaîne nettoyée, "" si NaN/None — même règle que core.importer._str,
    appliquée AVANT hachage en production (normalize_row). Sans ce
    traitement, une valeur manquante deviendrait la chaîne littérale "nan"
    dans le hash au lieu de "", ce qui fausserait la mesure des collisions
    (deux verbatims vides ne "collisionneraient" pas alors qu'ils le font
    réellement en production)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val).strip()


def _legacy_hash_4field(brand, date_iso, product_name, verbatim_content) -> str:
    """Reproduction figée du hash de production AVANT cette refonte
    (core/hasher.py historique : SHA-256(brand+date+product+content),
    strip+lower, valeurs manquantes traitées comme ""). Volontairement
    indépendante de l'implémentation actuelle de core.hasher.verbatim_hash,
    pour que ce diagnostic continue de mesurer "le problème tel qu'il
    existe en production" même après que le hash a été corrigé dans le code."""
    parts = [
        _clean(brand).lower(),
        _clean(date_iso).lower(),
        _clean(product_name).lower(),
        _clean(verbatim_content).lower(),
    ]
    return sha256("".join(parts).encode("utf-8")).hexdigest()


def _parse_date_iso(val) -> str:
    s = str(val).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return s  # non parsable : utilisé tel quel, cohérent des deux côtés


def _section(title: str) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


# ─── Q1 — unicité du guid dans un fichier ──────────────────────────────────────

def check_guid_unique(df: pd.DataFrame, label: str) -> None:
    _section(f"Q1 — Unicité du guid dans {label}")
    if "guid" not in df.columns:
        print("❌ Colonne 'guid' absente du fichier — impossible de répondre.")
        return

    total = len(df)
    empty = df["guid"].isna().sum() + (df["guid"].astype(str).str.strip() == "").sum()
    dup_count = df["guid"].duplicated(keep=False).sum()

    print(f"Lignes totales      : {total:,}")
    print(f"guid vide/manquant  : {empty:,}")
    print(f"guid dupliqué       : {dup_count:,} ligne(s) impliquée(s) "
          f"({dup_count / total:.2%})" if total else "fichier vide")

    if dup_count == 0 and empty == 0:
        print("✓ guid unique et toujours renseigné dans ce fichier.")
    elif dup_count > 0:
        top = (
            df["guid"].value_counts().head(5)
        )
        print("Exemples de guid les plus répétés dans ce fichier :")
        for guid_val, count in top.items():
            if count > 1:
                print(f"  {guid_val!r} : {count} occurrence(s)")


# ─── Q2 — stabilité du guid entre deux exports ────────────────────────────────

def check_guid_stability(df1: pd.DataFrame, label1: str,
                          df2: pd.DataFrame, label2: str) -> None:
    _section(f"Q2 — Stabilité du guid entre {label1} et {label2} (BLOQUANT)")

    required = {"brand", "date", "product_name_SEMANTIWEB", "verbatim_content", "guid"}
    missing1 = required - set(df1.columns)
    missing2 = required - set(df2.columns)
    if missing1 or missing2:
        print(f"❌ Colonnes manquantes — fichier 1: {missing1 or 'aucune'}, "
              f"fichier 2: {missing2 or 'aucune'}")
        return

    def _key(df: pd.DataFrame) -> pd.Series:
        return (
            df["brand"].map(_clean).str.lower() + "|"
            + df["date"].map(_parse_date_iso) + "|"
            + df["product_name_SEMANTIWEB"].map(_clean).str.lower() + "|"
            + df["verbatim_content"].map(_clean).str.lower()
        )

    a = pd.DataFrame({"_key": _key(df1), "guid": df1["guid"]})
    b = pd.DataFrame({"_key": _key(df2), "guid": df2["guid"]})
    merged = a.merge(b, on="_key", suffixes=("_1", "_2"))

    n_common = len(merged)
    print(f"Avis identiques (brand+date+produit+verbatim) présents dans les "
          f"deux fichiers : {n_common:,}")

    if n_common == 0:
        print(
            "⚠ Aucun avis commun entre ces deux fichiers — impossible de "
            "conclure sur la stabilité du guid avec ce couple de fichiers. "
            "Il faut deux exports dont les périodes se recouvrent (un même "
            "avis doit apparaître dans les deux) pour que ce test soit "
            "concluant. Réessayer avec deux mois consécutifs."
        )
        return

    n_stable = int((merged["guid_1"] == merged["guid_2"]).sum())
    pct = n_stable / n_common
    print(f"guid identique sur les deux exports : {n_stable:,} / {n_common:,} "
          f"({pct:.1%})")

    if pct == 1.0:
        print("✓ guid STABLE sur cet échantillon — scénario A (guid) envisageable.")
    elif pct == 0.0:
        print("❌ guid JAMAIS stable — le guid est régénéré à chaque export. "
              "Scénario A ÉCARTÉ, partir sur le scénario B (hash composite).")
    else:
        print("⚠ guid PARTIELLEMENT stable — résultat ambigu, à examiner "
              "manuellement avant de trancher (échantillon d'avis instables "
              "ci-dessous).")
        unstable = merged[merged["guid_1"] != merged["guid_2"]].head(5)
        for _, row in unstable.iterrows():
            print(f"  guid {label1}={row['guid_1']!r} → {label2}={row['guid_2']!r}")


# ─── Q3 — ampleur des collisions du hash actuel ───────────────────────────────

def check_current_collisions(df: pd.DataFrame, label: str) -> None:
    _section(f"Q3 — Collisions internes du hash actuel (4 champs) dans {label}")

    required = {"brand", "date", "product_name_SEMANTIWEB", "verbatim_content"}
    missing = required - set(df.columns)
    if missing:
        print(f"❌ Colonnes manquantes : {missing}")
        return

    work = df.copy()
    work["_date_iso"] = work["date"].map(_parse_date_iso)
    work["_id"] = [
        _legacy_hash_4field(b, d, p, c)
        for b, d, p, c in zip(
            work["brand"], work["_date_iso"],
            work["product_name_SEMANTIWEB"], work["verbatim_content"],
        )
    ]

    total = len(work)
    id_counts = Counter(work["_id"])
    colliding_ids = {i: c for i, c in id_counts.items() if c > 1}
    n_rows_in_collision = sum(colliding_ids.values())
    n_extra_rows = sum(c - 1 for c in colliding_ids.values())  # lignes "perdues"

    print(f"Lignes totales                    : {total:,}")
    print(f"id distincts (hash 4 champs)       : {len(id_counts):,}")
    print(f"Lignes impliquées dans une collision : {n_rows_in_collision:,} "
          f"({n_rows_in_collision / total:.2%})" if total else "")
    print(f"Lignes qui seraient perdues (ON CONFLICT DO NOTHING) : "
          f"{n_extra_rows:,} ({n_extra_rows / total:.2%})" if total else "")

    if not colliding_ids:
        print("✓ Aucune collision interne sur ce fichier avec le hash actuel.")
        return

    has_country = "country" in work.columns
    has_source  = "source" in work.columns

    cause_counts = Counter()
    for dup_id in colliding_ids:
        group = work[work["_id"] == dup_id]
        verbatim_empty = group["verbatim_content"].map(_clean).eq("").all()
        if verbatim_empty:
            cause_counts["verbatim vide"] += 1
        elif has_country and group["country"].astype(str).str.strip().nunique() > 1:
            cause_counts["pays différent"] += 1
        elif has_source and group["source"].astype(str).str.strip().nunique() > 1:
            cause_counts["source différente"] += 1
        else:
            cause_counts["verbatim identique non vide (même pays/source)"] += 1

    print("\nVentilation des groupes en collision par cause "
          "(1 groupe = 1 valeur d'id partagée par ≥2 lignes) :")
    for cause, count in cause_counts.most_common():
        print(f"  {cause:<55} {count:>6} groupe(s)")

    if not has_country or not has_source:
        print(
            "\n⚠ Colonne 'country' et/ou 'source' absente de ce fichier — la "
            "ventilation ci-dessus est partielle (ces causes n'ont pas pu "
            "être testées)."
        )


# ─── Q4 — dépendances sur verbatims.id ────────────────────────────────────────

def check_dependents() -> None:
    _section("Q4 — Dépendances sur verbatims.id (base vidée lors de la reprise)")
    try:
        from core.db import get_connection
    except Exception as exc:
        print(f"❌ Import core.db impossible : {exc}")
        return

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                # FK formelles pointant vers verbatims.id
                cur.execute("""
                    SELECT tc.table_name, kcu.column_name, tc.constraint_name
                      FROM information_schema.table_constraints tc
                      JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                       AND tc.table_schema = kcu.table_schema
                      JOIN information_schema.constraint_column_usage ccu
                        ON tc.constraint_name = ccu.constraint_name
                       AND tc.table_schema = ccu.table_schema
                     WHERE tc.constraint_type = 'FOREIGN KEY'
                       AND ccu.table_name = 'verbatims'
                       AND ccu.column_name = 'id'
                """)
                fks = cur.fetchall()

                # Candidats : colonnes dont le nom évoque un id de verbatim,
                # dans une table autre que verbatims, SANS contrainte FK
                # formelle détectée ci-dessus. Heuristique par nom de colonne
                # uniquement — à vérifier manuellement, ce script ne peut pas
                # savoir avec certitude si la colonne référence verbatims.id.
                cur.execute("""
                    SELECT table_name, column_name, data_type
                      FROM information_schema.columns
                     WHERE table_schema = 'public'
                       AND table_name <> 'verbatims'
                       AND (column_name ILIKE '%verbatim%id%'
                            OR column_name ILIKE '%verbatim_id%')
                     ORDER BY table_name, column_name
                """)
                candidates = cur.fetchall()
    except Exception as exc:
        print(
            f"❌ Connexion base impossible ({exc}). Q4 non testable dans cet "
            "environnement — à relancer avec un accès DB avant la reprise "
            "de l'import initial (cf. spec §1.4)."
        )
        return

    if fks:
        print("Clés étrangères formelles → verbatims.id :")
        for table_name, column_name, constraint_name in fks:
            print(f"  {table_name}.{column_name}  (contrainte {constraint_name})")
    else:
        print("Aucune clé étrangère formelle ne pointe vers verbatims.id.")

    fk_cols = {(t, c) for t, c, _ in fks}
    remaining_candidates = [
        (t, c, dt) for t, c, dt in candidates if (t, c) not in fk_cols
    ]
    print("\nCandidats sans contrainte formelle (nom de colonne évocateur, "
          "à vérifier manuellement) :")
    if remaining_candidates:
        for table_name, column_name, data_type in remaining_candidates:
            print(f"  {table_name}.{column_name} ({data_type})")
        print(
            "\n⚠ Ces tables devront être purgées ou reconstruites en même "
            "temps que verbatims lors de la reprise (spec §1.2 Q4). Ce "
            "script ne supprime rien — c'est une liste, pas une action."
        )
    else:
        print("  (aucun candidat détecté par ce nom de colonne)")


# ─── main ───────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    path1 = sys.argv[1]
    path2 = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"Fichier 1 : {path1}")
    df1 = _read_csv(path1)
    label1 = Path(path1).name

    check_guid_unique(df1, label1)
    check_current_collisions(df1, label1)

    if path2:
        print(f"\nFichier 2 : {path2}")
        df2 = _read_csv(path2)
        label2 = Path(path2).name
        check_guid_unique(df2, label2)
        check_guid_stability(df1, label1, df2, label2)
    else:
        _section("Q2 — Stabilité du guid entre deux exports mensuels (BLOQUANT)")
        print(
            "⚠ Un seul fichier fourni — Q2 non testable. Relancer avec un "
            "second export CSV d'un AUTRE mois pour trancher entre le "
            "scénario A (guid stable) et le scénario B (hash composite) :\n"
            "    python scripts/diag_hash.py mois_A.csv mois_B.csv"
        )

    check_dependents()

    _section("Fin du diagnostic — aucune écriture effectuée (lecture seule).")


if __name__ == "__main__":
    main()
