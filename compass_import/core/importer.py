"""
core/importer.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Parsing CSV, normalisation des lignes, INSERT batch, enrichissement catégories.

Flux d'utilisation typique :
    df, bad_lines = parse_csv(file_bytes)
    rows, skip_details, skips_par_code = normalize_batch(df, import_type, bad_lines)
    rows = apply_known_categories(conn, rows)
    stats = import_batch(conn, rows, batch_id)
    # stats = {"inserted", "duplicates", "duplicates_fichier",
    #          "duplicates_base", "errors"} — cf. import_batch.
"""

import logging
import sys
from collections import Counter
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
    "id", "guid", "brand", "country", "date", "opinion", "product_name", "rating",
    "source", "verbatim_content", "sampling",
    "attribute_efficiency", "attribute_packaging", "attribute_price",
    "attribute_quality", "attribute_scent", "attribute_taste",
    "attribute_texture", "attribute_safety", "attribute_composition",
    "categorie_interne", "sous_categorie_interne", "photo", "import_batch_id",
]

_INSERT_SQL = """
    INSERT INTO verbatims (
        id, guid, brand, country, date, opinion, product_name, rating,
        source, verbatim_content, sampling,
        attribute_efficiency, attribute_packaging, attribute_price,
        attribute_quality, attribute_scent, attribute_taste,
        attribute_texture, attribute_safety, attribute_composition,
        categorie_interne, sous_categorie_interne, photo, import_batch_id
    ) VALUES %s
    ON CONFLICT (id) DO NOTHING
    RETURNING id
"""

# Extrait de skip_details : longueur max globale et par valeur (spec §2.4)
_EXTRAIT_MAX_LEN = 200
_EXTRAIT_VALUE_MAX_LEN = 30


# ─── Config helper ────────────────────────────────────────────────────────────

def _load_config() -> dict:
    try:
        with open(_CONFIG_PATH, "rb") as f:
            return tomllib.load(f)
    except FileNotFoundError:
        return {}


def _load_validation_rules(config: dict | None = None) -> dict[str, bool]:
    """Règles de validation activables (``config.toml [import.validation]``).

    Toutes désactivées par défaut : on ne durcit que ce qui est
    explicitement demandé (cf. spec §2.3) — le comportement tolérant actuel
    (lignes sans marque/produit/verbatim importées avec des champs vides)
    reste inchangé tant que ces clés ne sont pas activées en configuration.
    """
    cfg = config if config is not None else _load_config()
    validation_cfg = cfg.get("import", {}).get("validation", {})
    return {
        "skip_si_verbatim_vide": bool(validation_cfg.get("skip_si_verbatim_vide", False)),
        "skip_si_brand_vide":    bool(validation_cfg.get("skip_si_brand_vide", False)),
        "skip_si_produit_vide":  bool(validation_cfg.get("skip_si_produit_vide", False)),
    }


# ─── Exceptions ───────────────────────────────────────────────────────────────

class RowValidationError(ValueError):
    """Ligne CSV invalide, avec code de raison exploitable par l'UI.

    Args:
        code: Code fermé, voir la table §2.3 de la spec (ex. ``DATE_INVALIDE``).
        message: Message lisible en français, affiché tel quel dans l'UI.
        champ: Nom du champ CSV fautif, ``None`` si non applicable.
    """

    def __init__(self, code: str, message: str, champ: str | None = None):
        self.code = code
        self.champ = champ
        super().__init__(message)


# ─── parse_csv ────────────────────────────────────────────────────────────────

def parse_csv(file_bytes: bytes) -> tuple[pd.DataFrame, list[dict]]:
    """
    Parse un fichier CSV Semantiweb depuis ses octets bruts.

    Gère l'encodage UTF-8 BOM (``utf-8-sig``) et le séparateur ``;``.
    Toutes les colonnes sont lues comme ``str`` pour éviter les conversions
    automatiques de pandas — la normalisation se fait dans ``normalize_row``.

    Les lignes dont le nombre de champs ne correspond pas à l'en-tête
    (ex. un ``;`` non échappé dans un verbatim) ne font plus échouer tout
    le fichier : une première lecture rapide (moteur C) est tentée ; si
    elle échoue sur des lignes malformées, une seconde lecture (moteur
    Python, seul capable d'accepter un callback ``on_bad_lines``) les
    capture une à une dans la liste retournée au lieu du DataFrame.
    Le moteur Python n'est donc utilisé — avec son coût en performance —
    que pour les fichiers qui en ont réellement besoin.

    Args:
        file_bytes: Contenu binaire du fichier CSV (sortie d'un
                    ``st.file_uploader`` ou lecture de fichier).

    Returns:
        Tuple ``(df, bad_lines)`` :
          - ``df`` : DataFrame pandas avec l'ensemble des colonnes du CSV.
          - ``bad_lines`` : lignes rejetées par le parseur, au format
            ``skip_details`` (``{ligne, code, raison, champ, extrait}``,
            ``ligne`` toujours ``None`` — le callback pandas ne fournit pas
            le numéro de ligne d'origine).

    Raises:
        ValueError: Si des colonnes obligatoires sont manquantes, ou si
                    le fichier ne peut pas être lu du tout (encodage,
                    séparateur incorrect sur l'ensemble du fichier).
    """
    config = _load_config()
    required_columns: list[str] = config.get("import", {}).get(
        "required_columns", _DEFAULT_REQUIRED_COLUMNS
    )

    read_kwargs = dict(
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=True,
    )

    bad_lines: list[dict] = []

    def _collect_bad_line(bad_line: list[str]) -> None:
        contenu = ";".join(str(v) for v in bad_line)
        bad_lines.append({
            "ligne":   None,
            "code":    "LIGNE_MALFORMEE",
            "raison": (
                f"Ligne malformée : {len(bad_line)} champ(s) trouvé(s), "
                "nombre incohérent avec l'en-tête du fichier."
            ),
            "champ":   None,
            "extrait": contenu[:_EXTRAIT_MAX_LEN],
        })

    try:
        df = pd.read_csv(BytesIO(file_bytes), **read_kwargs)
    except pd.errors.ParserError:
        try:
            df = pd.read_csv(
                BytesIO(file_bytes),
                engine="python",
                on_bad_lines=_collect_bad_line,
                **read_kwargs,
            )
        except Exception as exc:
            raise ValueError(
                f"Impossible de lire le fichier CSV.\n"
                f"Vérifiez l'encodage (UTF-8 BOM) et le séparateur (;).\n"
                f"Détail : {exc}"
            ) from exc
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

    return df, bad_lines


# ─── normalize_row ────────────────────────────────────────────────────────────

def normalize_row(
    row: pd.Series,
    import_type: str,
    validation_rules: dict[str, bool] | None = None,
) -> dict[str, Any]:
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
        validation_rules: Dict ``{skip_si_verbatim_vide, skip_si_brand_vide,
            skip_si_produit_vide}`` (cf. ``config.toml [import.validation]``).
            ``None`` recharge la config depuis le disque — pratique pour un
            appel isolé (tests, script), mais ``normalize_batch`` la charge
            une seule fois par fichier et la transmet pour éviter une
            lecture de ``config.toml`` par ligne.

    Returns:
        Dict avec toutes les clés correspondant aux colonnes de ``verbatims``.

    Raises:
        RowValidationError: Date manquante ou dans un format non reconnu,
            ou champ obligatoire vide alors que la règle correspondante est
            activée dans ``config.toml``.
    """
    rules = validation_rules if validation_rules is not None else _load_validation_rules()

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
        raise RowValidationError(
            "DATE_INVALIDE",
            f"Format de date non reconnu : '{val}'. "
            "Format attendu : JJ/MM/AAAA",
            champ="date",
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
    guid            = _str_or_none(row.get("guid"))
    sampling        = _parse_sampling(row.get("sampling", "0"))
    rating          = _parse_rating(row.get("rating"))
    photo           = _parse_photo(row.get("photo"))

    if not _str(row.get("date")):
        raise RowValidationError(
            "DATE_MANQUANTE", "La date est manquante.", champ="date"
        )
    parsed_date = _parse_date(row.get("date"))

    if rules["skip_si_brand_vide"] and not brand:
        raise RowValidationError(
            "BRAND_MANQUANTE", "La marque (brand) est vide.", champ="brand"
        )
    if rules["skip_si_produit_vide"] and not product_name:
        raise RowValidationError(
            "PRODUIT_MANQUANT",
            "Le nom du produit (product_name_SEMANTIWEB) est vide.",
            champ="product_name",
        )
    if rules["skip_si_verbatim_vide"] and not verbatim_content:
        raise RowValidationError(
            "VERBATIM_VIDE", "Le contenu du verbatim est vide.", champ="verbatim_content"
        )

    # id SHA-256 — scénario B (hash composite élargi), cf. core/hasher.py :
    # brand/date/product_name/verbatim_content ne suffisent pas à distinguer
    # un même avis syndiqué sur plusieurs pays ou plusieurs sources.
    row_id = verbatim_hash(
        brand,
        parsed_date.isoformat(),
        product_name,
        verbatim_content,
        country,
        source or "",
        str(rating or ""),
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
        "guid":                  guid,
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


# ─── normalize_batch ──────────────────────────────────────────────────────────

def _build_extrait(row: pd.Series) -> str:
    """Extrait diagnostique ``clé=valeur`` des champs non vides d'une ligne.

    Chaque valeur est tronquée à ``_EXTRAIT_VALUE_MAX_LEN`` caractères, le
    résultat concaténé à ``_EXTRAIT_MAX_LEN``. Ne jamais y inclure le
    verbatim en clair : volumétrie du log et données personnelles.
    """
    parts = [
        f"{k}={str(v).strip()[:_EXTRAIT_VALUE_MAX_LEN]}"
        for k, v in row.items()
        if v is not None and str(v).strip() not in ("", "nan")
    ]
    return "; ".join(parts)[:_EXTRAIT_MAX_LEN]


def normalize_batch(
    df: "pd.DataFrame",
    import_type: str,
    bad_lines: list[dict] | None = None,
) -> tuple[list[dict], list[dict], dict[str, int]]:
    """
    Normalise toutes les lignes d'un DataFrame CSV.

    Wraps ``normalize_row`` en capturant les exceptions ligne par ligne.
    Charge la config de validation une seule fois pour tout le fichier
    (plutôt qu'à chaque ligne dans ``normalize_row``).

    Args:
        df: DataFrame issu de ``parse_csv``.
        import_type: ``"initial"`` ou ``"mensuel"``.
        bad_lines: Lignes malformées renvoyées par ``parse_csv`` (2e élément
            du tuple), fusionnées dans ``skip_details`` avec
            ``code="LIGNE_MALFORMEE"``. ``None`` si non applicable.

    Returns:
        (rows, skip_details, skips_par_code)
        rows           — liste de dicts normalisés prêts pour import_batch.
        skip_details   — liste de dicts ``{ligne, code, raison, champ, extrait}``
                         pour chaque ligne ignorée (``ligne`` = numéro CSV,
                         1-indexé header — la 1re ligne de données est la
                         ligne 2 ; ``None`` pour les ``bad_lines``).
        skips_par_code — compteur agrégé, ex. ``{"DATE_INVALIDE": 12}``.
    """
    config = _load_config()
    rules = _load_validation_rules(config)

    rows: list[dict] = []
    skip_details: list[dict] = list(bad_lines or [])

    for csv_line, (_, row) in enumerate(df.iterrows(), start=2):
        try:
            rows.append(normalize_row(row, import_type, rules))
        except RowValidationError as exc:
            skip_details.append({
                "ligne":   csv_line,
                "code":    exc.code,
                "raison":  str(exc),
                "champ":   exc.champ,
                "extrait": _build_extrait(row),
            })
        except Exception as exc:
            skip_details.append({
                "ligne":   csv_line,
                "code":    "ERREUR_INCONNUE",
                "raison":  str(exc),
                "champ":   None,
                "extrait": _build_extrait(row),
            })

    skips_par_code = dict(Counter(d["code"] for d in skip_details))
    return rows, skip_details, skips_par_code


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
        Dict ``{"inserted", "duplicates", "duplicates_fichier",
        "duplicates_base", "errors"}``.

        - ``inserted``           : lignes réellement insérées.
        - ``duplicates``         : total des conflits ``ON CONFLICT DO
          NOTHING`` (déjà en base OU doublon interne au fichier). Ne
          contient PAS les lignes rejetées par la validation en amont
          (``normalize_batch`` / ``RowValidationError``) — celles-ci sont
          des ``skipped``, comptées séparément, jamais transmises ici.
        - ``duplicates_fichier``, ``duplicates_base`` : répartition
          approchée du total ci-dessus, via un ``Counter`` sur les ``id``
          de ``rows`` calculé avant l'INSERT. ``duplicates_fichier``
          = nombre d'occurrences en trop d'un même ``id`` répété dans le
          fichier ; le reste (``duplicates_base``) est attribué à une
          collision avec une ligne déjà présente en base. Approximation :
          si un ``id`` répété dans le fichier existait déjà en base, une
          des occurrences en trop est en réalité une collision base — le
          détail exact par ``id`` n'est pas conservé, seul le total
          l'est (clampé pour ne jamais dépasser ``duplicates``).
        - ``errors`` : messages d'erreur par lot échoué.
    """
    if not rows:
        return {
            "inserted": 0, "duplicates": 0,
            "duplicates_fichier": 0, "duplicates_base": 0,
            "errors": [],
        }

    config = _load_config()
    batch_size: int = config.get("import", {}).get("batch_size", 1000)

    # Doublons internes au fichier, détectés avant l'INSERT — permet de
    # répondre à « pourquoi des doublons sur un fichier neuf ? » (spec §2.5,
    # §1.1) plutôt que de tout attribuer, à tort, à des lignes déjà en base.
    id_counts = Counter(r["id"] for r in rows)
    duplicates_fichier_max = sum(c - 1 for c in id_counts.values() if c > 1)

    total_inserted   = 0
    total_duplicates = 0
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

            n_inserted   = len(returned)
            n_duplicates = len(chunk) - n_inserted
            total_inserted   += n_inserted
            total_duplicates += n_duplicates

            logger.debug(
                "Batch %d : %d insérés / %d doublons",
                batch_num, n_inserted, n_duplicates,
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

    duplicates_fichier = min(duplicates_fichier_max, total_duplicates)
    duplicates_base     = total_duplicates - duplicates_fichier

    return {
        "inserted":           total_inserted,
        "duplicates":         total_duplicates,
        "duplicates_fichier": duplicates_fichier,
        "duplicates_base":    duplicates_base,
        "errors":             errors,
    }


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
