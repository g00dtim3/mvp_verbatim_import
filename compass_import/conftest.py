"""
conftest.py — racine du projet Compass Import
Ajoute compass_import/ au sys.path pour que pytest trouve les modules core/.
"""

import sys
from pathlib import Path

# Permet d'importer core.*, compass_ui.* depuis les tests
sys.path.insert(0, str(Path(__file__).parent))
