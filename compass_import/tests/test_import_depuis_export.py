"""
Tests du lot 1B — importer un export validé d'api-explorer, sans téléchargement manuel.

Fixtures synthétiques fournies (21/10/2026) :
  - tests/fixtures/fake_export_plat.csv : 4 fausses mentions passées par le vrai
    derived.write_plat() d'api-explorer (en-tête « Sampling » compris) ;
  - tests/fixtures/fake_export_plat_compass_ids.json : les 4 compass_id calculés
    par api-explorer.

Le test de lister_exports() sur une vraie base ne tourne que si la variable
COMPASS_TEST_DB_URL désigne une base PostgreSQL de TEST (son nom doit contenir
« test ») ; tout y est fait dans une transaction annulée à la fin.
"""

import hashlib
import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
import pytest
import requests

from core.exports_source import (
    ExportSourceError,
    StorageConfig,
    lister_exports,
    nettoyer_dossier_temp,
    telecharger,
)
from core.importer import (
    estimate_row_count,
    iter_csv_chunks,
    normalize_batch,
    parse_csv,
    preview_csv,
)

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_PLAT = FIXTURES / "fake_export_plat.csv"
FAKE_IDS = json.loads((FIXTURES / "fake_export_plat_compass_ids.json").read_text())["ids"]
FAKE_CLE = "eyJfake-cle-de-test-jamais-reelle"


# ─── Sampling → sampling ──────────────────────────────────────────────────────

class TestAliasSampling:
    def test_fixture_contient_bien_Sampling_majuscule(self):
        en_tete = FAKE_PLAT.read_bytes().splitlines()[0].decode("utf-8-sig").split(";")
        assert "Sampling" in en_tete and "sampling" not in en_tete

    def test_parse_csv_lit_Sampling_comme_sampling(self):
        df, bad = parse_csv(FAKE_PLAT.read_bytes())
        assert "sampling" in df.columns and "Sampling" not in df.columns
        assert len(df) == 4 and bad == []

    def test_preview_csv_octets_et_chemin(self):
        for source in (FAKE_PLAT.read_bytes(), FAKE_PLAT, str(FAKE_PLAT)):
            head, _total = preview_csv(source, n=2)
            assert "sampling" in head.columns

    def test_chaque_morceau_a_sampling(self):
        chunks, _ = iter_csv_chunks(FAKE_PLAT, chunksize=3)
        for chunk in chunks:
            assert "sampling" in chunk.columns and "Sampling" not in chunk.columns

    def test_autres_majuscules_conservees(self):
        df, _ = parse_csv(FAKE_PLAT.read_bytes())
        assert "product_name_SEMANTIWEB" in df.columns
        assert "attribute_Efficiency" in df.columns

    def test_fichier_sans_aucune_des_deux_colonnes_refuse(self):
        sans = FAKE_PLAT.read_bytes().replace(b";Sampling;", b";autre_colonne;", 1)
        for lire in (parse_csv, preview_csv, iter_csv_chunks):
            with pytest.raises(ValueError, match="sampling"):
                lire(sans)

    def test_pas_de_renommage_si_sampling_existe_deja(self):
        double = FAKE_PLAT.read_bytes().replace(b";safety;", b";sampling;", 1)
        df, _ = parse_csv(double)
        assert list(df.columns).count("sampling") == 1
        assert "Sampling" in df.columns  # laissé tel quel, jamais deux colonnes du même nom


# ─── Alignement api-explorer ↔ Compass ───────────────────────────────────────

@pytest.mark.parametrize("mode", ["mensuel", "initial"])
def test_ids_compass_egaux_aux_compass_id_api_explorer(mode):
    """Test permanent : garde core/hasher.py aligné sur
    api-explorer bulk_export.fingerprints.compass_id."""
    chunks, bad = iter_csv_chunks(FAKE_PLAT, chunksize=1000)
    ids, ecartees = [], []
    for chunk in chunks:
        rows, skips, _ = normalize_batch(chunk, mode)
        ids += [r["id"] for r in rows]
        ecartees += skips
    assert ecartees == [] and bad == []
    assert ids == FAKE_IDS


# ─── Lecture depuis un chemin ────────────────────────────────────────────────

def test_iter_csv_chunks_chemin_identique_aux_octets():
    par_octets, bad_o = iter_csv_chunks(FAKE_PLAT.read_bytes(), chunksize=3)
    par_chemin, bad_c = iter_csv_chunks(FAKE_PLAT, chunksize=3)
    a, b = list(par_octets), list(par_chemin)
    assert len(a) == len(b) == 2
    for x, y in zip(a, b):
        pd.testing.assert_frame_equal(x, y)
    assert bad_o == bad_c == []


def test_estimate_row_count_chemin_identique_aux_octets(tmp_path):
    assert estimate_row_count(FAKE_PLAT) == estimate_row_count(FAKE_PLAT.read_bytes())
    vide = tmp_path / "vide.csv"
    vide.write_bytes(b"")
    assert estimate_row_count(vide) == 0


# ─── Téléchargement vérifié (faux Storage) ───────────────────────────────────

class FakeResponse:
    def __init__(self, status_code=200, blocs=(), content=b"", casse_apres=None):
        self.status_code = status_code
        self._blocs = list(blocs)
        self.content = content
        self._casse_apres = casse_apres
        self.closed = False

    def iter_content(self, taille):
        for i, bloc in enumerate(self._blocs):
            if self._casse_apres is not None and i >= self._casse_apres:
                raise requests.ConnectionError("coupure")
            yield bloc

    def close(self):
        self.closed = True


class FakeStorage:
    """Imite requests.Session.get pour l'URL Storage d'api-explorer."""

    def __init__(self, reponse):
        self.reponse = reponse
        self.appels = []

    def get(self, url, headers=None, stream=None, timeout=None):
        self.appels.append({"url": url, "headers": headers, "stream": stream})
        return self.reponse


def _config():
    return StorageConfig("https://fake-projet.supabase.co", FAKE_CLE, "bulk-exports")


def _blocs(data: bytes, taille=50):
    return [data[i:i + taille] for i in range(0, len(data), taille)]


class TestTelecharger:
    def test_empreinte_correcte_fichier_pret(self, tmp_path):
        data = FAKE_PLAT.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        storage = FakeStorage(FakeResponse(blocs=_blocs(data)))
        chemin = telecharger("exp_1/output/export_plat.csv", sha, tmp_path,
                             config=_config(), session=storage)
        assert chemin.parent == tmp_path and chemin.read_bytes() == data
        appel = storage.appels[0]
        assert appel["url"] == ("https://fake-projet.supabase.co/storage/v1/object/authenticated/"
                                "bulk-exports/exp_1/output/export_plat.csv")
        assert appel["headers"] == {"apikey": FAKE_CLE, "Authorization": f"Bearer {FAKE_CLE}"}
        assert appel["stream"] is True
        assert storage.reponse.closed

    def test_un_octet_modifie_refuse_et_fichier_supprime(self, tmp_path):
        data = FAKE_PLAT.read_bytes()
        sha = hashlib.sha256(data).hexdigest()
        altere = data[:100] + bytes([data[100] ^ 1]) + data[101:]
        storage = FakeStorage(FakeResponse(blocs=_blocs(altere)))
        with pytest.raises(ExportSourceError, match="empreinte"):
            telecharger("p.csv", sha, tmp_path, config=_config(), session=storage)
        assert list(tmp_path.iterdir()) == []

    def test_coupure_reseau_refuse_et_fichier_supprime(self, tmp_path):
        data = FAKE_PLAT.read_bytes()
        storage = FakeStorage(FakeResponse(blocs=_blocs(data), casse_apres=2))
        with pytest.raises(ExportSourceError, match="interrompue"):
            telecharger("p.csv", hashlib.sha256(data).hexdigest(), tmp_path,
                        config=_config(), session=storage)
        assert list(tmp_path.iterdir()) == []

    def test_erreur_http_cle_masquee_et_conseil(self, tmp_path):
        corps = f'{{"message":"Invalid Compact JWS","cle":"{FAKE_CLE}"}}'.encode()
        storage = FakeStorage(FakeResponse(status_code=400, content=corps))
        with pytest.raises(ExportSourceError) as err:
            telecharger("p.csv", "0" * 64, tmp_path, config=_config(), session=storage)
        assert FAKE_CLE not in str(err.value)
        assert "HTTP 400" in str(err.value) and "Legacy" in str(err.value)
        assert list(tmp_path.iterdir()) == []

    def test_cle_non_jwt_sans_authorization(self, tmp_path):
        data = b"x"
        storage = FakeStorage(FakeResponse(blocs=[data]))
        config = StorageConfig("https://fake-projet.supabase.co", "sb_secret_fake", "b")
        telecharger("p.csv", hashlib.sha256(data).hexdigest(), tmp_path,
                    config=config, session=storage)
        assert storage.appels[0]["headers"] == {"apikey": "sb_secret_fake"}


class TestConfiguration:
    def test_cle_jamais_dans_repr(self):
        assert FAKE_CLE not in repr(_config())

    def test_absente_erreur_explicite(self, monkeypatch):
        monkeypatch.setattr("core.exports_source.load_dotenv", lambda: None)
        monkeypatch.delenv("SUPABASE_URL", raising=False)
        monkeypatch.delenv("SUPABASE_SERVICE_KEY", raising=False)
        with pytest.raises(ExportSourceError, match="SUPABASE_URL"):
            StorageConfig.from_env()

    def test_lue_depuis_environnement(self, monkeypatch):
        monkeypatch.setattr("core.exports_source.load_dotenv", lambda: None)
        monkeypatch.setenv("SUPABASE_URL", "https://fake-projet.supabase.co/")
        monkeypatch.setenv("SUPABASE_SERVICE_KEY", f"  {FAKE_CLE}\n")
        monkeypatch.delenv("BULK_EXPORT_BUCKET", raising=False)
        config = StorageConfig.from_env()
        assert config == StorageConfig("https://fake-projet.supabase.co", FAKE_CLE, "bulk-exports")


def test_nettoyer_dossier_temp_ne_supprime_que_les_vieux_exports(tmp_path):
    vieux, recent, autre = (tmp_path / n for n in ("export_a.csv", "export_b.csv", "autre.csv"))
    for f in (vieux, recent, autre):
        f.write_bytes(b"x")
    os.utime(vieux, (0, 0))
    os.utime(autre, (0, 0))
    assert nettoyer_dossier_temp(tmp_path) == 1
    assert not vieux.exists() and recent.exists() and autre.exists()


# ─── lister_exports() ─────────────────────────────────────────────────────────

class _CurseurSansSchema:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, *a):
        raise psycopg2.errors.UndefinedTable("relation \"bulk_export.exports\" does not exist")


class _ConnSansSchema:
    rolled_back = False

    def cursor(self):
        return _CurseurSansSchema()

    def rollback(self):
        self.rolled_back = True


def test_lister_exports_schema_absent_erreur_explicite():
    conn = _ConnSansSchema()
    with pytest.raises(ExportSourceError, match="bulk_export"):
        lister_exports(conn)
    assert conn.rolled_back


_TEST_DB_URL = os.getenv("COMPASS_TEST_DB_URL")

# Sous-ensemble des vraies tables (api-explorer 001/003, Compass schema.sql),
# limité aux colonnes utiles ici.
_DDL = """
CREATE SCHEMA bulk_export;
CREATE TABLE bulk_export.exports (
    export_id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL,
    finished_at TIMESTAMPTZ, files_expired_at TIMESTAMPTZ);
CREATE TABLE bulk_export.files (
    export_id TEXT NOT NULL REFERENCES bulk_export.exports(export_id), path TEXT NOT NULL,
    kind TEXT NOT NULL, rows_count BIGINT, sha256 TEXT, size_bytes BIGINT,
    PRIMARY KEY (export_id, path));
CREATE TABLE import_logs (
    id UUID PRIMARY KEY, file_hash VARCHAR(64) NOT NULL, filename VARCHAR(500) NOT NULL,
    import_type VARCHAR(20) NOT NULL, started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ, rows_total INTEGER DEFAULT 0, rows_inserted INTEGER DEFAULT 0,
    status VARCHAR(20) NOT NULL DEFAULT 'running');
"""


@pytest.fixture
def base_de_test():
    if not _TEST_DB_URL:
        pytest.skip("COMPASS_TEST_DB_URL non définie (base PostgreSQL de test)")
    conn = psycopg2.connect(_TEST_DB_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            if "test" not in cur.fetchone()[0]:
                pytest.skip("COMPASS_TEST_DB_URL ne désigne pas une base de test")
            cur.execute(_DDL)
        yield conn
    finally:
        conn.rollback()  # rien n'est conservé (DDL compris)
        conn.close()


def _sha(c: str) -> str:
    return c * 64


def test_lister_exports_base_reelle(base_de_test):
    with base_de_test.cursor() as cur:
        cur.execute("""
            INSERT INTO bulk_export.exports (export_id, name, status, finished_at, files_expired_at) VALUES
              ('exp_ancien',   'Ancien',   'COMPLETED',  '2026-10-01', NULL),
              ('exp_recent',   'Récent',   'COMPLETED',  '2026-10-20', NULL),
              ('exp_incomplet','Incomplet','INCOMPLETE', '2026-10-21', NULL),
              ('exp_sans_plat','Sans plat','COMPLETED',  '2026-10-21', NULL),
              ('exp_expire',   'Expiré',   'COMPLETED',  '2026-10-21', '2026-10-21');
            INSERT INTO bulk_export.files (export_id, path, kind, rows_count, sha256, size_bytes) VALUES
              ('exp_ancien',    'exp_ancien/output/export_plat.csv',    'csv_plat', 10, %(a)s, 100),
              ('exp_recent',    'exp_recent/output/export_plat.csv',    'csv_plat', 20, %(b)s, 200),
              ('exp_recent',    'exp_recent/output/export_complete.csv','csv_complete', 20, %(c)s, 300),
              ('exp_incomplet', 'exp_incomplet/output/export_plat.csv', 'csv_plat', 5, %(d)s, 50),
              ('exp_sans_plat', 'exp_sans_plat/output/export_complete.csv', 'csv_complete', 5, %(e)s, 50),
              ('exp_expire',    'exp_expire/output/export_plat.csv',    'csv_plat', 5, %(f)s, 50);
            INSERT INTO import_logs (id, file_hash, filename, import_type, status, rows_inserted, finished_at)
            VALUES ('00000000-0000-0000-0000-000000000001', %(a)s, 'Ancien (exp_ancien)',
                    'mensuel', 'success', 10, '2026-10-02');
        """, {k: _sha(k) for k in "abcdef"})

    exports = lister_exports(base_de_test)

    assert [e.export_id for e in exports] == ["exp_recent", "exp_ancien"]
    recent, ancien = exports
    assert recent.dernier_import is None
    assert (recent.path, recent.rows_count, recent.sha256) == (
        "exp_recent/output/export_plat.csv", 20, _sha("b"))
    assert recent.filename_compass == "Récent (exp_recent)"
    assert ancien.dernier_import["status"] == "success"
    assert ancien.dernier_import["filename"] == "Ancien (exp_ancien)"
