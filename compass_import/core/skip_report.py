"""
core/skip_report.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Construction et lecture de l'enveloppe JSON versionnée stockée dans
``import_logs.error_detail`` (cf. SPEC_import_hash_et_tracabilite_2.md §2.6).

Usage :
    from core.skip_report import build_error_detail, parse_error_detail

    payload = build_error_detail(skip_details, batch_errors)   # dict | None
    parsed  = parse_error_detail(row["error_detail"])          # toujours un dict
"""

import json
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_DEFAULT_MAX_SKIP_DETAILS = 500

ENVELOPE_VERSION = 1


def _load_max_skip_details() -> int:
    try:
        with open(_CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
    except FileNotFoundError:
        return _DEFAULT_MAX_SKIP_DETAILS
    return int(
        config.get("import", {}).get("max_skip_details", _DEFAULT_MAX_SKIP_DETAILS)
    )


def _count_by_code(skip_details: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in skip_details:
        code = entry.get("code") or "INCONNU"
        counts[code] = counts.get(code, 0) + 1
    return counts


# ─── Écriture ─────────────────────────────────────────────────────────────────

def build_error_detail(
    skip_details: list[dict],
    batch_errors: list[str] | None = None,
    max_skip_details: int | None = None,
) -> dict | None:
    """
    Construit l'enveloppe versionnée destinée à ``import_logs.error_detail``.

    Args:
        skip_details: Liste complète des lignes ignorées (format
            ``{ligne, code, raison, champ, extrait}``, sortie de
            ``normalize_batch`` fusionnée aux ``bad_lines`` de ``parse_csv``).
        batch_errors: Messages d'erreur par lot échoué (sortie de
            ``import_batch``).
        max_skip_details: Plafond du nombre d'entrées ``skips`` conservées
            en détail. ``None`` → lu depuis ``config.toml
            [import].max_skip_details`` (défaut 500). Un fichier très
            volumineux et majoritairement invalide ne doit pas faire
            exploser la ligne de log — ``skips_total`` reste le compte
            réel, seul le détail est tronqué.

    Returns:
        Dict de l'enveloppe, ou ``None`` si rien à consigner (import sans
        skip ni erreur de lot) — ``error_detail`` reste alors ``NULL``.
    """
    if not skip_details and not batch_errors:
        return None

    limit = max_skip_details if max_skip_details is not None else _load_max_skip_details()
    skips_total = len(skip_details)
    truncated = skip_details[:limit]

    return {
        "version":        ENVELOPE_VERSION,
        "skips":          truncated,
        "skips_par_code": _count_by_code(skip_details),
        "skips_total":    skips_total,
        "skips_tronque":  skips_total > len(truncated),
        "batch_errors":   list(batch_errors or []),
    }


# ─── Lecture ──────────────────────────────────────────────────────────────────

def _empty_parsed() -> dict:
    return {
        "version":        0,
        "skips":          [],
        "skips_par_code": {},
        "skips_total":    0,
        "skips_tronque":  False,
        "batch_errors":   [],
        "is_legacy":      False,
        "legacy_text":    None,
    }


def parse_error_detail(raw) -> dict:
    """
    Parse ``import_logs.error_detail`` vers un format toujours exploitable.

    Ne lève jamais, quelle que soit l'entrée : ``NULL``, chaîne vide, dict
    déjà décodé (psycopg2 renvoie les colonnes ``JSONB`` comme ``dict``/
    ``list`` directement), JSON invalide, ou format antérieur à cette
    refonte (liste nue, dict ``{"skipped": [...], "batch_errors": [...]}``
    sans ``version``, ou texte libre issu d'un ``str(exc)``).

    Returns:
        Dict avec les clés ``version`` (0 si legacy/inconnu), ``skips``,
        ``skips_par_code``, ``skips_total``, ``skips_tronque``,
        ``batch_errors``, ``is_legacy`` (bool), ``legacy_text``
        (``str | None`` — texte brut si aucune structure exploitable n'a
        été trouvée).
    """
    empty = _empty_parsed()
    if raw is None or raw == "":
        return empty

    data = raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {**empty, "is_legacy": True, "legacy_text": raw}

    if isinstance(data, list):
        # Ancien format le plus rudimentaire : liste nue de skip_details.
        return {
            "version":        0,
            "skips":          data,
            "skips_par_code": _count_by_code(data) if data else {},
            "skips_total":    len(data),
            "skips_tronque":  False,
            "batch_errors":   [],
            "is_legacy":      True,
            "legacy_text":    None,
        }

    if not isinstance(data, dict):
        return {**empty, "is_legacy": True, "legacy_text": str(data)}

    if "legacy_text" in data and "version" not in data:
        return {**empty, "is_legacy": True, "legacy_text": data.get("legacy_text")}

    if data.get("version") == ENVELOPE_VERSION:
        return {
            "version":        ENVELOPE_VERSION,
            "skips":          data.get("skips", []),
            "skips_par_code": data.get("skips_par_code", {}),
            "skips_total":    data.get("skips_total", len(data.get("skips", []))),
            "skips_tronque":  bool(data.get("skips_tronque", False)),
            "batch_errors":   data.get("batch_errors", []),
            "is_legacy":      False,
            "legacy_text":    None,
        }

    # Dict sans "version" reconnue : ancien format {"skipped": [...],
    # "batch_errors": [...]} pré-lot-2, ou enveloppe d'une version future
    # inconnue. On récupère ce qui est structurellement exploitable.
    skips = data.get("skips") or data.get("skipped") or []
    return {
        "version":        0,
        "skips":          skips,
        "skips_par_code": _count_by_code(skips) if skips else {},
        "skips_total":    len(skips),
        "skips_tronque":  False,
        "batch_errors":   data.get("batch_errors") or [],
        "is_legacy":      True,
        "legacy_text":    None,
    }
