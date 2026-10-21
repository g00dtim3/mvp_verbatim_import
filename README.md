# mvp_verbatim_import

## Accès

- Toutes les pages de Compass demandent un mot de passe (`compass_import/auth.py`) ; il n'est saisi qu'une fois par session.
- Il se règle dans Streamlit Cloud → Settings → Secrets (`APP_PASSWORD = "…"`, en haut du fichier), ou via la variable d'environnement `APP_PASSWORD` (cf. `compass_import/.env.example`).
- Pour le changer : remplacer la valeur dans les Secrets (au moins 16 caractères, aléatoire, jamais dans Git) ; les sessions ouvertes devront le ressaisir après redémarrage de l'application.
