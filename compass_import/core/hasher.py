"""
core/hasher.py
──────────────────────────────────────────────────────────────
Compass · Consumer Voice — Import Pipeline
Fonctions de hachage SHA-256 pour les verbatims et les fichiers.

Usage :
    from core.hasher import verbatim_hash, file_hash, is_file_already_imported

    row_id  = verbatim_hash(brand, date_iso, product_name, content, country, source, rating)
    f_hash  = file_hash(file_bytes)
    log     = is_file_already_imported(conn, f_hash)   # None si nouveau
"""

import hashlib


def verbatim_hash(
    brand: str,
    date,
    product_name: str,
    verbatim_content: str,
    country: str = "",
    source: str = "",
    rating: str = "",
) -> str:
    """
    Génère l'identifiant unique d'un verbatim par SHA-256.

    Scénario B de la refonte du hash (cf. SPEC_import_hash_et_tracabilite_2.md
    §1.3) : composite élargi à 7 champs, dans un ordre figé. Retenu par
    défaut faute de confirmation que le ``guid`` Semantiweb est stable
    entre deux exports mensuels (scénario A, plus simple : ``verbatim_hash
    (guid)``) — voir ``scripts/diag_hash.py``. Si cette stabilité est
    confirmée, cette fonction peut être simplifiée sans repurger la base,
    car ``guid`` est déjà stocké sur chaque ligne (``verbatims.guid``).

    Champs entrant dans le hash, et pourquoi :
      - ``brand``, ``date``, ``product_name``, ``verbatim_content`` :
        champs d'origine (version à 4 champs, insuffisants seuls).
      - ``country`` : un même avis peut être syndiqué sur plusieurs pays
        (FR/BE/CH) sous des lignes distinctes du CSV — sans ce champ elles
        s'écrasaient (même hash, ``ON CONFLICT DO NOTHING``).
      - ``source`` : un même avis peut être collecté depuis plusieurs
        canaux (Amazon + site marque) — même problème que ``country``.
      - ``rating`` : renforce la distinction entre avis courts/génériques
        partageant les 6 autres champs (ex. deux "Parfait" 5 étoiles le
        même jour, même produit — mais pays/source différents).

    Limite assumée, non corrigée par ce hash : deux verbatims vides ou
    identiques et génériques (ex. deux notes 5 étoiles sans texte), même
    jour, même produit, même pays, même source et même note, restent
    indistinguables et ne produiront qu'une seule ligne en base. Ce n'est
    pas un bug de cette fonction — aucun champ disponible dans le CSV ne
    permet de les différencier — mais une limite structurelle à documenter
    plutôt qu'à contourner par un compteur artificiel.

    Quiconque modifie l'ensemble ou l'ordre de ces champs invalide tout hash
    déjà stocké en base : la base devra être repurgée et l'import initial
    rejoué (cf. ``scripts/reset_verbatims.py``). C'est la seule protection
    contre une régression silencieuse.

    La concaténation des champs est normalisée (strip + lower) avant
    hachage, garantissant l'idempotence : un même verbatim produit
    toujours le même hash quel que soit son contexte d'import. L'ordre des
    arguments est significatif (des valeurs permutées entre deux champs de
    même contenu produisent un hash différent — pas de risque de collision
    par transposition).

    Args:
        brand: Marque du produit (ex : "L'Oreal").
        date: Date de l'avis. Accepte un objet ``datetime.date`` ou une chaîne
              en format ISO (YYYY-MM-DD). Passer le résultat de
              ``date_obj.isoformat()`` pour garantir la cohérence.
        product_name: Nom du produit tel que stocké en base (sans suffixe
                      Semantiweb).
        verbatim_content: Texte brut de l'avis client.
        country: Code pays de l'avis (ex : "FR"). "" si absent.
        source: Canal de collecte (ex : "Amazon"). "" si absent.
        rating: Note, convertie en chaîne (ex : "4"). "" si absente.

    Returns:
        SHA-256 hexdigest de 64 caractères (minuscules).
    """
    parts = [
        str(brand).strip().lower(),
        str(date).strip().lower(),
        str(product_name).strip().lower(),
        str(verbatim_content).strip().lower(),
        str(country).strip().lower(),
        str(source).strip().lower(),
        str(rating).strip().lower(),
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
