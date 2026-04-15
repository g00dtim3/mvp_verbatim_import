"""
core/referentiel.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Gestion du référentiel fermé des catégories / sous-catégories.

Le CSV source a deux colonnes : categorie, sous_categorie.
Rechargeable à chaud sans redémarrer l'application — aucun cache global.

Usage :
    from core.referentiel import (
        load_referentiel,
        get_all_categories,
        get_sous_categories,
        is_valid_combination,
    )

    ref  = load_referentiel()                        # { cat: [sous, ...] }
    cats = get_all_categories()                      # ["Baby Care", "Body Care", …]
    sous = get_sous_categories("Body Care")          # ["Body Care : Hand Cream", …]
    ok   = is_valid_combination("Body Care", "Body Care : Hand Cream")  # True
"""

import csv
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib


_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_DEFAULT_CSV_PATH = Path(__file__).parent.parent / "data" / "referentiel_categories.csv"


# ─── Path resolution ──────────────────────────────────────────────────────────

def _resolve_path(path: str | Path | None) -> Path:
    """
    Résout le chemin vers le CSV référentiel.

    Priorité :
    1. Argument ``path`` explicite.
    2. Valeur ``[referentiel].path`` dans config.toml (relative à la racine projet).
    3. Chemin par défaut ``data/referentiel_categories.csv``.
    """
    if path is not None:
        return Path(path)

    try:
        with open(_CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
        rel = config.get("referentiel", {}).get("path", "")
        if rel:
            return Path(__file__).parent.parent / rel
    except (FileNotFoundError, KeyError):
        pass

    return _DEFAULT_CSV_PATH


# ─── Public API ───────────────────────────────────────────────────────────────

def load_referentiel(path: str | Path | None = None) -> dict[str, list[str]]:
    """
    Charge le référentiel des catégories depuis le CSV.

    Aucun cache global — rechargeable à chaud à chaque appel. Chaque appel
    relit le fichier, ce qui permet de mettre à jour le référentiel sans
    redémarrer l'application Streamlit.

    Args:
        path: Chemin vers le CSV. Si ``None``, utilise config.toml
              ``[referentiel].path``, avec repli sur
              ``data/referentiel_categories.csv``.

    Returns:
        Dict ``{ categorie: [sous_categorie_1, sous_categorie_2, …] }``,
        préservant l'ordre de première apparition de chaque catégorie.

    Raises:
        FileNotFoundError: Si le fichier CSV est introuvable.
        ValueError: Si les colonnes ``categorie`` / ``sous_categorie``
                    sont absentes du CSV.
    """
    csv_path = _resolve_path(path)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Référentiel des catégories introuvable : {csv_path}\n"
            "Vérifiez que data/referentiel_categories.csv est présent à la "
            "racine du projet."
        )

    referentiel: dict[str, list[str]] = {}

    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        required = {"categorie", "sous_categorie"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"Le CSV référentiel doit contenir les colonnes "
                f"'categorie' et 'sous_categorie'.\n"
                f"Colonnes trouvées : {reader.fieldnames}"
            )

        for row in reader:
            cat = row["categorie"].strip()
            sous = row["sous_categorie"].strip()
            if not cat or not sous:
                continue
            if cat not in referentiel:
                referentiel[cat] = []
            if sous not in referentiel[cat]:
                referentiel[cat].append(sous)

    return referentiel


def get_all_categories(path: str | Path | None = None) -> list[str]:
    """
    Retourne la liste triée de toutes les catégories du référentiel.

    Args:
        path: Chemin vers le CSV (optionnel, voir ``load_referentiel``).

    Returns:
        Liste de chaînes triée alphabétiquement.
    """
    return sorted(load_referentiel(path).keys())


def get_sous_categories(
    categorie: str,
    path: str | Path | None = None,
) -> list[str]:
    """
    Retourne les sous-catégories valides pour une catégorie donnée.

    Args:
        categorie: Nom exact de la catégorie (sensible à la casse).
        path: Chemin vers le CSV (optionnel).

    Returns:
        Liste des sous-catégories dans l'ordre du CSV.
        Liste vide si la catégorie est inconnue.
    """
    return load_referentiel(path).get(categorie, [])


def is_valid_combination(
    categorie: str,
    sous_categorie: str,
    path: str | Path | None = None,
) -> bool:
    """
    Vérifie qu'une combinaison catégorie / sous-catégorie existe dans
    le référentiel fermé.

    Args:
        categorie: Catégorie à vérifier (sensible à la casse).
        sous_categorie: Sous-catégorie à vérifier (sensible à la casse).
        path: Chemin vers le CSV (optionnel).

    Returns:
        ``True`` si la combinaison est dans le référentiel, ``False`` sinon.
    """
    return sous_categorie in load_referentiel(path).get(categorie, [])
