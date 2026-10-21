"""
core/exports_source.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Source « export validé d'api-explorer » (lot 1B).

api-explorer et Compass partagent le même projet Supabase. Pour un export
COMPLETED, la ligne ``bulk_export.files`` de ``kind = 'csv_plat'`` donne le
chemin du fichier plat dans Supabase Storage, son nombre de lignes et son
SHA-256 calculé sur les octets du fichier — exactement ce que calcule
``core.hasher.file_hash()``. Compass lit donc ce fichier tel quel, sans
reconvertir le brut : c'est le parcours manuel « télécharger / redéposer »,
sans l'étape manuelle.

Ce module ne fait que LIRE le schéma ``bulk_export`` (jamais d'écriture).

Usage :
    exports = lister_exports(conn)
    chemin = telecharger(e.path, e.sha256, dossier_temp)
    chunks, bad_lines = iter_csv_chunks(chemin)

Paramètres (variables d'environnement / .env, comme core/db.py) :
    SUPABASE_URL          https://<projet>.supabase.co
    SUPABASE_SERVICE_KEY  clé service_role « Legacy » (commence par eyJ…)
    BULK_EXPORT_BUCKET    défaut "bulk-exports"
La clé n'est jamais affichée, y compris dans les messages d'erreur.
"""

import hashlib
import os
import tempfile
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import psycopg2
import requests
from dotenv import load_dotenv

from core.hasher import is_file_already_imported

BUCKET_PAR_DEFAUT = "bulk-exports"
BLOC_LECTURE = 1024 * 1024        # 1 Mo
TIMEOUT = (10.0, 300.0)           # (connexion, lecture) en secondes
_FILENAME_MAX = 500               # import_logs.filename VARCHAR(500)
AGE_MAX_TEMP_S = 12 * 3600        # fichiers temporaires abandonnés : supprimés après 12 h


class ExportSourceError(Exception):
    """Erreur explicite, affichable telle quelle (ne contient jamais la clé)."""


# ─── Configuration ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class StorageConfig:
    supabase_url: str
    service_key: str = field(repr=False)  # jamais dans un repr / log
    bucket: str = BUCKET_PAR_DEFAUT

    @classmethod
    def from_env(cls) -> "StorageConfig":
        load_dotenv()  # charge .env s'il existe (sans écraser les variables déjà définies)
        url = (os.getenv("SUPABASE_URL") or "").strip()
        key = (os.getenv("SUPABASE_SERVICE_KEY") or "").strip()
        bucket = (os.getenv("BULK_EXPORT_BUCKET") or "").strip() or BUCKET_PAR_DEFAUT
        if not url or not key:
            raise ExportSourceError(
                "Lecture des exports non configurée : renseignez SUPABASE_URL et "
                "SUPABASE_SERVICE_KEY dans les Secrets de l'application (ou le fichier .env)."
            )
        if not url.startswith(("https://", "http://127.0.0.1:", "http://localhost:")):
            raise ExportSourceError("SUPABASE_URL doit commencer par https://")
        return cls(url.rstrip("/"), key, bucket)


def _masquer(texte: str, cle: str) -> str:
    return texte.replace(cle, "***") if cle else texte


def _en_tetes(cle: str) -> dict:
    """Mêmes en-têtes qu'api-explorer (bulk_export/storage.py) : ``apikey``
    toujours, et ``Authorization`` si la clé est un jeton JWT (eyJ…)."""
    headers = {"apikey": cle}
    if cle.startswith("eyJ"):
        headers["Authorization"] = f"Bearer {cle}"
    return headers


def _quote_path(path: str) -> str:
    return "/".join(urllib.parse.quote(part, safe="") for part in path.split("/"))


# ─── Liste des exports validés ───────────────────────────────────────────────

@dataclass(frozen=True)
class ExportValide:
    export_id: str
    name: str
    finished_at: datetime | None
    rows_count: int | None
    size_bytes: int | None
    sha256: str
    path: str
    dernier_import: dict | None  # tentative la plus récente (is_file_already_imported), None si jamais importé

    @property
    def filename_compass(self) -> str:
        """Valeur de ``import_logs.filename`` : ``"<nom> (<export_id>)"``."""
        suffixe = f" ({self.export_id})"
        return self.name[: _FILENAME_MAX - len(suffixe)] + suffixe


# Le fichier plat sans empreinte n'est pas listé : sans SHA-256 attendu, on ne
# peut ni vérifier le téléchargement ni appliquer le contrôle anti-doublon.
_SQL_EXPORTS = """
    SELECT e.export_id, e.name, e.finished_at,
           f.rows_count, f.size_bytes, f.sha256, f.path
    FROM bulk_export.exports e
    JOIN bulk_export.files f
      ON f.export_id = e.export_id AND f.kind = 'csv_plat'
    WHERE e.status = 'COMPLETED'
      AND e.files_expired_at IS NULL
      AND f.sha256 IS NOT NULL
    ORDER BY e.finished_at DESC NULLS LAST, e.export_id DESC
"""


def lister_exports(conn) -> list[ExportValide]:
    """
    Exports COMPLETED disposant d'un fichier ``csv_plat`` non expiré, du plus
    récent au plus ancien, chacun avec sa dernière tentative d'import.

    « Déjà importé ? » réutilise ``is_file_already_imported()`` (même règle
    que pour un fichier déposé : ``import_logs.file_hash = files.sha256``,
    tentative la plus récente, statut ``duplicate`` exclu) — une requête par
    export, le nombre d'exports restant de l'ordre de quelques dizaines.

    Raises:
        ExportSourceError: schéma ``bulk_export`` absent de cette base
            (ex. environnement prod hors Supabase).
    """
    try:
        with conn.cursor() as cur:
            cur.execute(_SQL_EXPORTS)
            lignes = cur.fetchall()
    except (psycopg2.errors.UndefinedTable, psycopg2.errors.InvalidSchemaName,
            psycopg2.errors.UndefinedColumn) as exc:
        conn.rollback()
        raise ExportSourceError(
            "Les exports d'api-explorer (schéma bulk_export) sont introuvables dans "
            f"cette base. Détail : {exc.pgerror or exc}".strip()
        ) from None

    return [
        ExportValide(
            export_id=export_id, name=name, finished_at=finished_at,
            rows_count=rows_count, size_bytes=size_bytes, sha256=sha256, path=path,
            dernier_import=is_file_already_imported(conn, sha256),
        )
        for export_id, name, finished_at, rows_count, size_bytes, sha256, path in lignes
    ]


# ─── Téléchargement vérifié ──────────────────────────────────────────────────

def _erreur_http(resp, cle: str) -> ExportSourceError:
    corps = _masquer((resp.content or b"")[:300].decode("utf-8", errors="replace"), cle)
    conseil = ""
    if "Invalid Compact JWS" in corps or "required property 'authorization'" in corps:
        # Constaté côté api-explorer (15/10 et 21/10/2026) : Storage refuse les clés « sb_secret_… ».
        conseil = (" → Storage n'accepte pas cette clé. Utilisez la clé « service_role » de l'onglet "
                   "« Legacy API Keys » (elle commence par eyJ…) dans SUPABASE_SERVICE_KEY.")
    return ExportSourceError(
        f"Lecture du fichier d'export : HTTP {resp.status_code} — {corps}{conseil}"
    )


def telecharger(
    path: str,
    sha256_attendu: str,
    dossier_temp: str | os.PathLike,
    *,
    config: StorageConfig | None = None,
    session: requests.Session | None = None,
) -> Path:
    """
    Copie le fichier ``path`` du bucket vers un fichier temporaire de
    ``dossier_temp``, en flux (blocs de 1 Mo, jamais le fichier entier en
    mémoire), en calculant son SHA-256 au passage.

    Empreinte différente de ``sha256_attendu`` (ou toute erreur en cours de
    route) → fichier temporaire supprimé et ``ExportSourceError`` : rien ne
    peut être importé à partir d'un fichier non vérifié.

    Returns:
        Chemin du fichier temporaire vérifié. À supprimer par l'appelant
        une fois l'import terminé.
    """
    config = config or StorageConfig.from_env()
    session = session or requests.Session()
    url = (f"{config.supabase_url}/storage/v1/object/authenticated/"
           f"{config.bucket}/{_quote_path(path)}")

    fd, nom_temp = tempfile.mkstemp(prefix="export_", suffix=".csv", dir=dossier_temp)
    chemin = Path(nom_temp)
    try:
        empreinte = hashlib.sha256()
        try:
            resp = session.get(url, headers=_en_tetes(config.service_key),
                               stream=True, timeout=TIMEOUT)
        except requests.RequestException as exc:
            raise ExportSourceError(
                f"Storage injoignable ({type(exc).__name__})."
            ) from None
        try:
            if resp.status_code != 200:
                raise _erreur_http(resp, config.service_key)
            with os.fdopen(fd, "wb") as f:
                fd = None
                for bloc in resp.iter_content(BLOC_LECTURE):
                    if bloc:
                        empreinte.update(bloc)
                        f.write(bloc)
        except requests.RequestException as exc:
            raise ExportSourceError(
                f"Lecture du fichier d'export interrompue ({type(exc).__name__})."
            ) from None
        finally:
            resp.close()

        obtenu = empreinte.hexdigest()
        if obtenu != sha256_attendu.lower():
            raise ExportSourceError(
                "Le fichier reçu ne correspond pas à l'export validé (empreinte SHA-256 "
                f"différente : attendu {sha256_attendu[:12]}…, reçu {obtenu[:12]}…). "
                "Rien n'a été importé."
            )
        return chemin
    except BaseException:
        if fd is not None:
            os.close(fd)
        chemin.unlink(missing_ok=True)
        raise


def nettoyer_dossier_temp(dossier: str | os.PathLike, age_max_s: float = AGE_MAX_TEMP_S) -> int:
    """
    Supprime de ``dossier`` les fichiers temporaires d'export (``export_*.csv``)
    plus vieux que ``age_max_s`` — laissés par une session de navigateur
    fermée en plein parcours, qui n'a donc jamais supprimé son fichier.
    Un fichier encore utilisé par une autre session a moins de 12 h.

    Returns:
        Nombre de fichiers supprimés.
    """
    limite = time.time() - age_max_s
    supprimes = 0
    for chemin in Path(dossier).glob("export_*.csv"):
        try:
            if chemin.stat().st_mtime < limite:
                chemin.unlink()
                supprimes += 1
        except OSError:
            pass  # supprimé entre-temps par une autre session
    return supprimes
