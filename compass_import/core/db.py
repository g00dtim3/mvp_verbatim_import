"""
core/db.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Gestion de la connexion PostgreSQL / Supabase.

Sélection de l'environnement via COMPASS_ENV :
  - dev  → Supabase (SUPABASE_DB_URL)
  - prod → PostgreSQL (PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD)

Usage :
    from core.db import get_connection, test_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM verbatims")
            count = cur.fetchone()[0]
"""

import os
import sys
import logging
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2 import pool, OperationalError, DatabaseError
from dotenv import load_dotenv

# Support Python < 3.11 pour tomllib
if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

logger = logging.getLogger(__name__)

# Chemin vers config.toml — résolu depuis ce fichier (core/ → racine du projet)
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"

# Pool de connexions — initialisé à la première demande (lazy)
_pool: pool.SimpleConnectionPool | None = None


# ─── Chargement de la configuration ──────────────────────────────────────────

def _load_config() -> dict:
    """Charge config.toml et retourne le dict complet."""
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Fichier de configuration introuvable : {_CONFIG_PATH}\n"
            "Vérifiez que config.toml est présent à la racine du projet."
        )
    with open(_CONFIG_PATH, "rb") as f:
        return tomllib.load(f)


def _get_env() -> str:
    """Retourne l'environnement actif ('dev' ou 'prod'). Défaut : 'dev'."""
    env = os.getenv("COMPASS_ENV", "dev").strip().lower()
    if env not in ("dev", "prod"):
        raise ValueError(
            f"COMPASS_ENV='{env}' invalide. Valeurs acceptées : 'dev' ou 'prod'."
        )
    return env


# ─── Construction des paramètres de connexion ─────────────────────────────────

def _build_dsn() -> str:
    """
    Construit la DSN psycopg2 selon l'environnement actif.
    Les credentials sont lus depuis les variables d'environnement (.env).
    """
    load_dotenv()  # charge .env s'il existe (sans écraser les variables déjà définies)

    config = _load_config()
    env = _get_env()
    db_cfg = config.get("database", {}).get(env, {})

    if env == "dev":
        dsn = os.getenv("SUPABASE_DB_URL")
        if not dsn:
            raise EnvironmentError(
                "Variable d'environnement SUPABASE_DB_URL non définie.\n"
                "Consultez .env.example pour la configuration dev (Supabase)."
            )
        return dsn

    # prod — paramètres individuels
    host = os.getenv("PG_HOST")
    port = os.getenv("PG_PORT", str(db_cfg.get("port", 5432)))
    database = os.getenv("PG_DB")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")

    missing = [name for name, val in [
        ("PG_HOST", host),
        ("PG_DB", database),
        ("PG_USER", user),
        ("PG_PASSWORD", password),
    ] if not val]

    if missing:
        raise EnvironmentError(
            f"Variables d'environnement manquantes pour l'env prod : {', '.join(missing)}\n"
            "Consultez .env.example pour la configuration prod (PostgreSQL)."
        )

    return (
        f"host={host} port={port} dbname={database} "
        f"user={user} password={password} "
        f"options='-c search_path={db_cfg.get('schema', 'public')}'"
    )


# ─── Gestion du pool de connexions ────────────────────────────────────────────

def _get_pool() -> pool.SimpleConnectionPool:
    """
    Retourne le pool de connexions, l'initialise si nécessaire (lazy init).
    Thread-safe pour Streamlit (session isolée par thread).
    """
    global _pool

    if _pool is not None and not _pool.closed:
        return _pool

    config = _load_config()
    env = _get_env()
    db_cfg = config.get("database", {}).get(env, {})

    min_conn = 1
    max_conn = db_cfg.get("pool_size", 5)
    connect_timeout = db_cfg.get("pool_timeout", 30)

    dsn = _build_dsn()

    try:
        _pool = pool.SimpleConnectionPool(
            minconn=min_conn,
            maxconn=max_conn,
            dsn=dsn,
            connect_timeout=connect_timeout,
        )
        logger.info(
            "Pool de connexions initialisé — env=%s pool_size=%d",
            env,
            max_conn,
        )
    except OperationalError as exc:
        raise ConnectionError(
            f"Impossible de se connecter à la base de données ({env}).\n"
            f"Vérifiez les credentials et la disponibilité du serveur.\n"
            f"Détail : {exc}"
        ) from exc

    return _pool


def close_pool() -> None:
    """Ferme toutes les connexions du pool. À appeler à l'arrêt de l'application."""
    global _pool
    if _pool and not _pool.closed:
        _pool.closeall()
        _pool = None
        logger.info("Pool de connexions fermé.")


# ─── Context manager principal ────────────────────────────────────────────────

@contextmanager
def get_connection():
    """
    Context manager retournant une connexion psycopg2 depuis le pool.

    La connexion est automatiquement remise dans le pool à la sortie du bloc,
    qu'il y ait eu une exception ou non. En cas d'exception non gérée,
    la transaction est rollbackée avant le retour au pool.

    Usage :
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

    Raises:
        ConnectionError: si la connexion au serveur échoue.
        DatabaseError: pour toute erreur PostgreSQL non récupérée.
    """
    connection = None
    p = _get_pool()

    try:
        connection = p.getconn()
        connection.autocommit = False
        yield connection
    except (OperationalError, DatabaseError) as exc:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        raise DatabaseError(
            f"Erreur base de données : {exc}"
        ) from exc
    except Exception:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        raise
    finally:
        if connection:
            try:
                p.putconn(connection)
            except Exception as exc:
                logger.warning("Erreur lors du retour de la connexion au pool : %s", exc)


# ─── Utilitaires ──────────────────────────────────────────────────────────────

def test_connection() -> bool:
    """
    Vérifie que la connexion à la base de données fonctionne.

    Returns:
        True si la connexion et le ping réussissent, False sinon.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        logger.info("test_connection : OK (env=%s)", _get_env())
        return True
    except Exception as exc:
        logger.error("test_connection : ÉCHEC — %s", exc)
        return False


def get_active_env() -> str:
    """Retourne l'environnement actif ('dev' ou 'prod')."""
    return _get_env()
