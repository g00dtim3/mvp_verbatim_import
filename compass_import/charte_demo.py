"""
Page de démonstration de la charte Consumer Voice.

    streamlit run charte_demo.py

Montre chaque composant (maison et natif) avec la charte appliquée. Sert de référence visuelle :
après une modification de la charte ou une mise à jour de Streamlit, l'ouvrir en clair et en sombre
(menu ⋮ → Settings → Theme) et vérifier que rien n'a bougé. Aucune donnée réelle, aucune connexion.
"""

import pandas as pd
import streamlit as st

from charte import (appliquer_charte, badge_statut, bandeau, carte_verbatim, encadre_cle, etapes,
                    fil_ariane, libelle_statut, section, vide)

st.set_page_config(page_title="Charte — démonstration", page_icon=":material/palette:", layout="wide")
appliquer_charte()

with st.sidebar:
    st.header("Navigation", anchor=False)
    st.radio("Page", ["Import", "Matching", "Outils"], label_visibility="collapsed")
    st.selectbox("Environnement", ["Production", "Test"])

bandeau("Consumer Voice · Import Compass", "Importer des verbatims",
        sous_titre="Depuis un export validé ou un fichier CSV", etiquette="Import mensuel")
fil_ariane(["Compass", "Import", "Export du 18/10/2026"])
etapes(["Source", "Contrôle", "Import", "Matching"], courante=1)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Lignes du fichier", "189 824", border=True)
c2.metric("Insérées", "187 410", "+12 %", border=True)
c3.metric("Doublons", "2 101", border=True)
c4.metric("Écartées", "313", "-4 %", delta_color="inverse", border=True)

encadre_cle("187 410 verbatims ajoutés. 46 produits restent à rattacher à une catégorie avant la mise à "
            "disposition dans Tableau.", titre="À retenir")

with section("Exports validés", "Choisissez l'export à importer. Un export déjà importé est indiqué."):
    df = pd.DataFrame({
        "Export": ["Avis FR — octobre", "Avis UK — octobre", "Avis DE — septembre", "Avis ES — septembre"],
        "Fin": ["18/10/2026", "17/10/2026", "21/09/2026", "20/09/2026"],
        "Lignes": [189824, 52310, 40112, 0],
        "Statut": [libelle_statut(s) for s in ["COMPLETED", "COMPLETED", "INCOMPLETE", "FAILED"]],
    })
    st.dataframe(df, hide_index=True, width="stretch",
                 column_config={"Lignes": st.column_config.NumberColumn(format="localized")})
    with st.container(horizontal=True):
        st.button("Importer l'export", type="primary", icon=":material/upload:")
        st.button("Voir le journal", icon=":material/description:")

with section("Statuts", "Un seul vocabulaire pour les trois applications."):
    st.markdown("  ".join(badge_statut(s) for s in
                          ["COMPLETED", "RUNNING", "PENDING", "INCOMPLETE", "FAILED", "MATCH", "MISMATCH"]))

with section("Messages", "Les alertes natives de Streamlit, aux couleurs de la charte."):
    st.info("Le fichier est en cours de lecture (lot 12 sur 190).", icon=":material/info:")
    st.success("Import terminé : 187 410 lignes insérées.", icon=":material/check_circle:")
    st.warning("46 produits sans catégorie : le matching est à faire.", icon=":material/warning:")
    st.error("Empreinte différente : le fichier a changé depuis sa validation. Import arrêté.", icon=":material/error:")

tab1, tab2 = st.tabs(["Verbatims", "Paramètres"])
with tab1:
    carte_verbatim("Enfin un shampooing qui calme les démangeaisons dès la deuxième utilisation.",
                   meta="Amazon · FR · 12/10/2026 · ★ 5", sentiment="positive")
    carte_verbatim("It stings a bit and dries the lengths, but the flakes are gone.",
                   meta="Boots · UK · 03/10/2026 · ★ 3", sentiment="neutral",
                   detail="Ça pique un peu et ça assèche les longueurs, mais les pellicules ont disparu.")
    carte_verbatim("Aucun effet après trois semaines.", meta="Pharmacie Lafayette · FR · 28/09/2026 · ★ 1",
                   sentiment="negative")
    vide("Aucun verbatim pour ce filtre. Élargissez la période ou retirez un pays.")
with tab2:
    with st.form("parametres"):
        f1, f2 = st.columns(2)
        f1.text_input("Nom de l'export", "Avis FR — octobre")
        f2.date_input("Début de période")
        st.multiselect("Marques", ["Ducray", "Vichy", "Nizoral"], default=["Ducray"])
        st.checkbox("Conserver le brut en base", value=True)
        st.form_submit_button("Enregistrer", type="primary")

with section("Graphique", "Barres horizontales triées par valeur, une seule couleur (bleu de la charte) "
                         "quand il n'y a qu'une série."):
    st.bar_chart(pd.DataFrame({"Marque": ["Ducray", "Vichy", "Nizoral", "Klorane"],
                               "Avis": [420, 310, 280, 150]}),
                 x="Marque", y="Avis", horizontal=True, sort="-Avis", color="#065cb1", height=220)
