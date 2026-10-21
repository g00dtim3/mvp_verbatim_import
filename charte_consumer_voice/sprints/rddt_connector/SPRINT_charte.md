# Sprint charte — Collecte Reddit (rddt_connector)

**But :** l'application Reddit adopte la charte Consumer Voice (référence : `verbatims_run/outils/rr_gabarit.html`).
Lire d'abord `docs/charte/DESIGN_GUIDE.md`.
**Dépend de :** lot 0 (mot de passe). Peut se faire avant ou après le lot 2 (base principale) : indépendant.
**Décision :** validée par l'utilisateur le 21/10/2026. Elle répond à la section « Design » du `CLAUDE.md` :
la charte attendue dans `design-guidelines/` est désormais ce kit.

## Où vont les fichiers du kit
`.streamlit/config.toml` à la racine (à côté de `secrets.toml.example`) ; `charte/`, `charte_demo.py` à la racine ;
`tests/test_charte.py` ; `DESIGN_GUIDE.md` et `apercus/` dans `docs/charte/`. `requirements.txt` : `streamlit>=1.66`.

`design-guidelines/` : supprimer `style.css` et `design_preview.html` (copie de l'ancienne charte Compass v1.0,
jamais branchée sur l'application) ; remplacer `README.md` par trois lignes renvoyant à `docs/charte/DESIGN_GUIDE.md`.
Mettre à jour la section « Design » du `CLAUDE.md` dans le même sens.

## Tâches
1. `app.py` : `set_page_config(page_title="Collecte Reddit", page_icon=":material/forum:", layout="wide")`,
   `exiger_mot_de_passe()`, `appliquer_charte()`.
2. Remplacer `st.title("MVP Reddit — Brandwatch → PostgreSQL")` par
   `bandeau("Consumer Voice · Collecte Reddit", <écran courant>, sous_titre=<une phrase par écran>)`.
3. Navigation (`SCREENS`, `st.sidebar.radio("Étape", …)`) : garder les cinq écrans, libellés sans numéro
   (« Connexions », « Sélection et volume », « Acquisition », « Export », « Bibliothèque de datasets ») ;
   les écrans 1 à 3 forment un parcours : `etapes(["Sélection et volume", "Acquisition", "Export"], courante=…)`
   en haut de ces trois écrans.
4. Les `st.header("1. Sélection & Volume — …")` deviennent les titres du bandeau ; les blocs de chaque écran passent
   dans des `section(titre, intro)`.
5. Composants :
   - contrôle de volume UI / API : statuts `MATCH` / `WARNING` / `MISMATCH` via `badge_statut()` (Conforme,
     À vérifier, Écart) ; la fenêtre de 48 h exclue est expliquée dans un `encadre_cle()` ;
   - statuts d'acquisition et de datasets (`CREATED` … `CANCELLED`, `LOCAL` … `COMPLETE`) : `badge_statut()` /
     `libelle_statut()` ;
   - compteurs (reçues, insérées, déjà présentes) : `st.metric(border=True)` ;
   - aperçu de mentions : `carte_verbatim(texte, meta="r/<subreddit> · <date> · u/<auteur>")` (pas de sentiment
     disponible : liseré neutre) ;
   - journal en direct (mode verbose) : garder `st.code`, dans une `section("Journal")`.
6. `tests/test_charte.py` : `PAGES = ["app.py"]`.
7. Contrôle visuel, clair et sombre, des cinq écrans ; captures dans la pull request.

## Définition de terminé
- [ ] Bandeau, sections et statuts harmonisés sur les cinq écrans ; plus de `design-guidelines/style.css`.
- [ ] `pytest` vert.
- [ ] `docs/06-decisions-et-risques.md` : décision du 21/10/2026 ; captures avant / après dans la pull request.
