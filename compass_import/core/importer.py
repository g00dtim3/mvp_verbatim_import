"""
core/importer.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Parsing CSV, normalisation des lignes, INSERT batch, enrichissement catégories.

Flux d'utilisation typique :
    df   = parse_csv(file_bytes)
    rows = [normalize_row(row, import_type) for _, row in df.iterrows()]
    rows = apply_known_categories(conn, rows)
    stats = import_batch(conn, rows, batch_id)
"""

import logging
import sys
from datetime import date, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg2.extras import execute_values

from core.hasher import verbatim_hash

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"

# Colonnes requises par défaut (repli si config.toml absent)
_DEFAULT_REQUIRED_COLUMNS = [
    "guid",
    "brand",
    "country",
    "date",
    "opinion",
    "product_name_SEMANTIWEB",
    "rating",
    "source",
    "verbatim_content",
    "sampling",
]

# Correspondance colonnes CSV → colonnes PostgreSQL (attributs sentiment)
_ATTRIBUTE_MAP: dict[str, str] = {
    "attribute_Efficiency":  "attribute_efficiency",
    "attribute_Packaging":   "attribute_packaging",
    "attribute_Price":       "attribute_price",
    "attribute_Quality":     "attribute_quality",
    "attribute_Scent":       "attribute_scent",
    "attribute_Taste":       "attribute_taste",
    "attribute_Texture":     "attribute_texture",
    "attribute_Safety":      "attribute_safety",
    "attribute_Composition": "attribute_composition",
}

# Ordre des colonnes dans le tuple INSERT (doit correspondre à _INSERT_SQL)
_INSERT_COLS = [
    "id", "brand", "country", "date", "opinion", "product_name", "rating",
    "source", "verbatim_content", "sampling",
    "attribute_efficiency", "attribute_packaging", "attribute_price",
    "attribute_quality", "attribute_scent", "attribute_taste",
    "attribute_texture", "attribute_safety", "attribute_composition",
    "categorie_interne", "sous_categorie_interne", "photo", "import_batch_id",
]

_INSERT_SQL = """
    INSERT INTO verbatims (
        id, brand, country, date, opinion, product_name, rating,
        source, verbatim_content, sampling,
        attribute_efficiency, attribute_packaging, attribute_price,
        attribute_quality, attribute_scent, attribute_taste,
        attribute_texture, attribute_safety, attribute_composition,
        categorie_interne, sous_categorie_interne, photo, import_batch_id
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING
    RETURNING id
"""


# ─── Config helper ────────────────────────────────────────────────────────────

def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}


# ─── parse_csv ────────────────────────────────────────────────────────────────

def parse_csv(file_bytes: bytes) -> pd.DataFrame:
    """
    Parse un fichier CSV Semantiweb depuis ses octets bruts.

    Gère l'encodage UTF-8 BOM (``utf-8-sig``) et le séparateur ``;``.
    Toutes les colonnes sont lues comme ``str`` pour éviter les conversions
    automatiques de pandas — la normalisation se fait dans ``normalize_row``.

    Args:
        file_bytes: Contenu binaire du fichier CSV (sortie d'un
                    ``st.file_uploader`` ou lecture de fichier).

    Returns:
        DataFrame pandas avec l'ensemble des colonnes du CSV.

    Raises:
        ValueError: Si des colonnes obligatoires sont manquantes, ou si
                    le fichier ne peut pas être lu (encodage, format).
    """
    config = _load_config()
    required_columns: list[str] = config.get("import", {}).get(
        "required_columns", _DEFAULT_REQUIRED_COLUMNS
    )

    try:
        df = pd.read_csv(
            BytesIO(file_bytes),
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
            keep_default_na=True,
        )
    except Exception as exc:
        raise ValueError(
            f"Impossible de lire le fichier CSV.\n"
            f"Vérifiez l'encodage (UTF-8 BOM) et le séparateur (;).\n"
            f"Détail : {exc}"
        ) from exc

    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(
            f"Colonnes obligatoires manquantes dans le fichier CSV : "
            f"{', '.join(missing)}\n"
            f"Colonnes trouvées : {', '.join(df.columns.tolist())}"
        )

    return df


# ─── normalize_row ────────────────────────────────────────────────────────────

def normalize_row(row: pd.Series, import_type: str) -> dict[str, Any]:
    """
    Normalise une ligne du DataFrame CSV vers un dict prêt pour l'INSERT.

    Transformations appliquées :

    - ``product_name_SEMANTIWEB`` → ``product_name``
    - ``date`` "JJ/MM/AAAA" → :class:`datetime.date`
    - ``sampling`` "0"/"1" → ``bool``
    - ``photo`` "oui"/"non" → ``bool`` (``import_type="initial"`` seulement,
      sinon ``None``)
    - Attributs valeur ``"0"`` → ``None``
    - Génération du SHA-256 ``id``
    - ``import_batch_id`` à ``None`` (sera rempli par ``import_batch``)

    Args:
        row: Ligne ``pd.Series`` issue de ``parse_csv()``.
        import_type: ``"initial"`` ou ``"mensuel"``.

    Returns:
        Dict avec toutes les clés correspondant aux colonnes de ``verbatims``.

    Raises:
        ValueError: Si le champ ``date`` est dans un format non reconnu.
    """

    # ── Helpers internes ──────────────────────────────────────────────────────

    def _is_null(val) -> bool:
        """True si val est None, NaN ou chaîne vide."""
        if val is None:
            return True
        try:
            return bool(pd.isna(val))
        except (TypeError, ValueError):
            return False

    def _str(val) -> str:
        """Chaîne nettoyée, "" si null."""
        return "" if _is_null(val) else str(val).strip()

    def _str_or_none(val) -> str | None:
        s = _str(val)
        return s if s else None

    def _first_valid(row: pd.Series, *keys: str) -> str | None:
        """Retourne la première valeur non-nulle trouvée parmi les clés."""
        for k in keys:
            v = row.get(k)
            if not _is_null(v):
                s = str(v).strip()
                if s:
                    return s
        return None

    def _parse_date(val) -> date:
        if isinstance(val, date):
            return val
        s = _str(val)
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except ValueError:
                continue
        raise ValueError(
            f"Format de date non reconnu : '{val}'. "
            "Format attendu : JJ/MM/AAAA"
        )

    def _parse_sampling(val) -> bool:
        if isinstance(val, bool):
            return val
        return _str(val) in ("1", "true", "True", "TRUE", "yes", "oui")

    def _parse_photo(val) -> bool | None:
        if import_type != "initial":
            return None
        if _is_null(val):
            return None
        s = _str(val).lower()
        if s in ("oui", "true", "1", "yes"):
            return True
        if s in ("non", "false", "0", "no"):
            return False
        return None

    def _parse_attr(val) -> str | None:
        if _is_null(val):
            return None
        s = _str(val)
        if s in ("0", ""):
            return None
        return s if s in ("positive", "negative") else None

    def _parse_rating(val) -> int | None:
        try:
            r = int(float(_str(val)))
            return r if 1 <= r <= 5 else None
        except (ValueError, TypeError):
            return None

    # ── Extraction des champs ─────────────────────────────────────────────────

    brand           = _str(row.get("brand"))
    country         = _str(row.get("country"))
    opinion         = _str_or_none(row.get("opinion"))
    product_name    = _str(row.get("product_name_SEMANTIWEB"))
    source          = _str_or_none(row.get("source"))
    verbatim_content = _str(row.get("verbatim_content"))
    sampling        = _parse_sampling(row.get("sampling", "0"))
    rating          = _parse_rating(row.get("rating"))
    parsed_date     = _parse_date(row.get("date"))
    photo           = _parse_photo(row.get("photo"))

    # id SHA-256 — toujours calculé avec la date en ISO format
    row_id = verbatim_hash(
        brand, parsed_date.isoformat(), product_name, verbatim_content
    )

    # Attributs sentiment : "0" → None
    attrs: dict[str, str | None] = {
        db_col: _parse_attr(row.get(csv_col))
        for csv_col, db_col in _ATTRIBUTE_MAP.items()
    }

    # Catégories : NULL à l'import mensuel (enrichies par apply_known_categories)
    if import_type == "initial":
        categorie_interne = _first_valid(
            row, "categorie interne", "categorie_interne"
        )
        sous_categorie_interne = _first_valid(
            row, "sous categorie interne", "sous_categorie_interne"
        )
    else:
        categorie_interne      = None
        sous_categorie_interne = None

    return {
        "id":                    row_id,
        "brand":                 brand,
        "country":               country,
        "date":                  parsed_date,
        "opinion":               opinion,
        "product_name":          product_name,
        "rating":                rating,
        "source":                source,
        "verbatim_content":      verbatim_content,
        "sampling":              sampling,
        **attrs,
        "categorie_interne":     categorie_interne,
        "sous_categorie_interne": sous_categorie_interne,
        "photo":                 photo,
        "import_batch_id":       None,  # rempli par import_batch
    }


# ─── import_batch ─────────────────────────────────────────────────────────────

def import_batch(conn, rows: list[dict], batch_id: str) -> dict:
    """
    Insère les verbatims en base par lots de 1 000 lignes.

    Chaque lot est une transaction indépendante avec ``ON CONFLICT (id) DO
    NOTHING`` : en cas d'erreur sur un lot, les lots précédents sont conservés.
    La taille de lot est lue depuis ``config.toml [import].batch_size``
    (défaut : 1000).

    Args:
        conn: Connexion psycopg2 (``autocommit=False``), obtenue via
              ``core.db.get_connection``.
        rows: Liste de dicts normalisés (sortie de ``normalize_row``).
        batch_id: UUID de l'``import_logs`` associé — stocké dans
                  ``verbatims.import_batch_id``.

    Returns:
        Dict ``{"inserted": int, "skipped": int, "errors": list[str]}``.
        ``inserted`` = lignes réellement insérées (après déduplication).
        ``skipped``  = conflits ``ON CONFLICT DO NOTHING``.
        ``errors``   = messages d'erreur par lot échoué.
    """
    if not rows:
        return {"inserted": 0, "skipped": 0, "errors": []}

    config = _load_config()
    batch_size: int = config.get("import", {}).get("batch_size", 1000)

    total_inserted = 0
    total_skipped  = 0
    errors: list[str] = []

    def _to_tuple(r: dict) -> tuple:
        return tuple(r.get(col) if col != "import_batch_id" else batch_id
                     for col in _INSERT_COLS)

    for batch_num, start in enumerate(range(0, len(rows), batch_size), start=1):
        chunk = rows[start : start + batch_size]
        try:
            values = [_to_tuple(r) for r in chunk]
            with conn.cursor() as cur:
                returned = execute_values(cur, _INSERT_SQL, values, fetch=True)
            conn.commit()

            n_inserted = len(returned)
            n_skipped  = len(chunk) - n_inserted
            total_inserted += n_inserted
            total_skipped  += n_skipped

            logger.debug(
                "Batch %d : %d insérés / %d skippés",
                batch_num, n_inserted, n_skipped,
            )

        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            msg = (
                f"Erreur batch {batch_num} "
                f"(lignes {start}–{start + len(chunk) - 1}) : {exc}"
            )
            logger.error(msg)
            errors.append(msg)

    return {"inserted": total_inserted, "skipped": total_skipped, "errors": errors}


# ─── apply_known_categories ───────────────────────────────────────────────────

def apply_known_categories(conn, rows: list[dict]) -> list[dict]:
    """
    Enrichit les verbatims avec les catégories déjà présentes dans
    ``categories_mapping``.

    Lookup par ``key_brandxpdt`` (= brand || product_name) — une seule requête
    pour tous les couples distincts du batch.  Pour chaque produit connu,
    ``categorie_interne``, ``sous_categorie_interne`` et ``photo`` sont peuplés
    **uniquement si ``categorie_interne`` est encore** ``None`` (ne pas écraser
    un import initial qui aurait déjà ses catégories).

    Args:
        conn: Connexion psycopg2 active.
        rows: Liste de dicts normalisés (sortie de ``normalize_row``).

    Returns:
        Même liste (potentiellement modifiée) avec les catégories enrichies.
    """
    if not rows:
        return rows

    keys = list({
        r["brand"] + r["product_name"]
        for r in rows
        if r.get("brand") and r.get("product_name")
    })
    if not keys:
        return rows

    query = """
        SELECT brand, product_name, categorie_interne, sous_categorie_interne, photo
        FROM categories_mapping
        WHERE key_brandxpdt = ANY(%s)
    """
    with conn.cursor() as cur:
        cur.execute(query, (keys,))
        mapping: dict[tuple, dict] = {
            (db_row[0], db_row[1]): {
                "categorie_interne":      db_row[2],
                "sous_categorie_interne": db_row[3],
                "photo":                  db_row[4],
            }
            for db_row in cur.fetchall()
        }

    if not mapping:
        return rows

    enriched = []
    for row in rows:
        known = mapping.get((row.get("brand", ""), row.get("product_name", "")))
        if known is not None and row.get("categorie_interne") is None:
            row = {**row, **known}
        enriched.append(row)

    return enriched
