"""
core/hasher.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Fonctions de hachage SHA-256 pour les verbatims et les fichiers.

Usage :
    from core.hasher import verbatim_hash, file_hash, is_file_already_imported

    row_id  = verbatim_hash(brand, date_iso, product_name, content)
    f_hash  = file_hash(file_bytes)
    log     = is_file_already_imported(conn, f_hash)   # None si nouveau
"""

import hashlib


def verbatim_hash(
    brand: str,
    date,
    product_name: str,
    verbatim_content: str,
) -> str:
    """
    Génère l'identifiant unique d'un verbatim par SHA-256.

    La concaténation des 4 champs est normalisée (strip + lower) avant hachage,
    garantissant l'idempotence : un même verbatim produit toujours le même hash
    quel que soit son contexte d'import.

    Args:
        brand: Marque du produit (ex : "L'Oreal").
        date: Date de l'avis. Accepte un objet ``datetime.date`` ou une chaîne
              en format ISO (YYYY-MM-DD). Passer le résultat de
              ``date_obj.isoformat()`` pour garantir la cohérence.
        product_name: Nom du produit tel que stocké en base (sans suffixe
                      Semantiweb).
        verbatim_content: Texte brut de l'avis client.

    Returns:
        SHA-256 hexdigest de 64 caractères (minuscules).
    """
    parts = [
        str(brand).strip().lower(),
        str(date).strip().lower(),
        str(product_name).strip().lower(),
        str(verbatim_content).strip().lower(),
    ]
    raw = "".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_hash(file_bytes: bytes) -> str:
    """
    Calcule le SHA-256 du contenu binaire brut d'un fichier.

    Utilisé pour le contrôle anti-doublon d'import : deux fichiers dont
    le contenu est identique produiront le même hash, quel que soit leur nom.
    La détection est insensible au renommage de fichier.

    Args:
        file_bytes: Contenu binaire du fichier (tel que reçu du file uploader).

    Returns:
        SHA-256 hexdigest de 64 caractères (minuscules).
    """
    return hashlib.sha256(file_bytes).hexdigest()


def is_file_already_imported(conn, file_hash_value: str) -> dict | None:
    """
    Vérifie si un fichier a déjà été importé avec succès.

    Interroge ``import_logs`` à la recherche d'un enregistrement ayant le même
    ``file_hash`` et dont le statut n'est ni ``'error'`` ni ``'duplicate'``
    (c'est-à-dire un import réussi, partiel ou en cours).

    Args:
        conn: Connexion psycopg2 active (obtenue via ``core.db.get_connection``).
        file_hash_value: SHA-256 hexdigest du fichier à vérifier.

    Returns:
        Dict avec les colonnes ``id``, ``filename``, ``started_at``, ``status``,
        ``import_type`` si un doublon est détecté ; ``None`` sinon.
    """
    query = """
        SELECT id, filename, started_at, status, import_type
        FROM import_logs
        WHERE file_hash = %s
          AND status NOT IN ('error', 'duplicate')
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (file_hash_value,))
        row = cur.fetchone()
        if row is None:
            return None
        cols = [desc[0] for desc in cur.description]
        return dict(zip(cols, row))
