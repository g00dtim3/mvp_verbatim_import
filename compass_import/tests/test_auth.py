"""
Tests du module commun auth.py (à placer dans tests/ de chaque application).

Lancement : pytest tests/test_auth.py
Le test crée une petite page factice qui appelle exiger_mot_de_passe(), puis vérifie
qu'aucun contenu n'apparaît sans le bon mot de passe.
"""

import os
import shutil
import sys
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

RACINE = Path(__file__).resolve().parent.parent  # dossier qui contient auth.py
MDP = "mot-de-passe-de-test-assez-long"          # 31 caractères, factice

PAGE = '''
import streamlit as st
st.set_page_config(page_title="test")
from auth import exiger_mot_de_passe
exiger_mot_de_passe()
st.write("CONTENU PROTÉGÉ")
'''


@pytest.fixture
def page(tmp_path, monkeypatch):
    shutil.copy(RACINE / "auth.py", tmp_path / "auth.py")
    (tmp_path / "page.py").write_text(PAGE, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delenv("APP_PASSWORD", raising=False)
    sys.modules.pop("auth", None)

    def lancer(secret=MDP):
        at = AppTest.from_file(str(tmp_path / "page.py"))
        if secret is not None:
            at.secrets["APP_PASSWORD"] = secret
        return at.run()
    return lancer


def _textes(at):
    return [m.value for m in at.markdown] + [e.value for e in at.error]


def test_rien_sans_mot_de_passe(page):
    at = page()
    assert at.title[0].value == "Accès protégé"
    assert "CONTENU PROTÉGÉ" not in _textes(at)


def test_mauvais_puis_bon_mot_de_passe(page):
    at = page()
    at.text_input[0].input("faux")
    at.button[0].click().run()
    assert "Mot de passe incorrect." in _textes(at)
    assert "CONTENU PROTÉGÉ" not in _textes(at)
    at.text_input[0].input(MDP)
    at.button[0].click().run()
    assert "CONTENU PROTÉGÉ" in _textes(at)
    assert "mot_de_passe_saisi" not in at.session_state  # la saisie ne reste pas en mémoire


def test_secret_absent(page):
    assert "Configuration manquante : accès impossible." in _textes(page(secret=None))


def test_secret_trop_court(page):
    assert any("Configuration invalide" in t for t in _textes(page(secret="court")))


def test_variable_environnement(page, monkeypatch):
    monkeypatch.setenv("APP_PASSWORD", MDP)
    assert page(secret=None).title[0].value == "Accès protégé"


def test_blocage_apres_cinq_echecs(page):
    at = page()
    for i in range(5):
        at.text_input[0].input(f"faux{i}")
        at.button[0].click().run()
    at.run()
    assert "Trop d'essais. Réessayez dans quelques minutes." in _textes(at)
