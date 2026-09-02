-- ═══════════════════════════════════════════════════════════════
-- Migration 0001 — Hash d'unicité + traçabilité des imports
-- Compass · Consumer Voice — Import Pipeline
--
-- Regroupe les changements de schéma des lots 1 et 2 de la refonte
-- import (cf. SPEC_import_hash_et_tracabilite_2.md) :
--   1. verbatims.guid          — stockage du guid Semantiweb (audit /
--                                 bascule ultérieure vers le scénario A
--                                 du hash sans nouvelle purge).
--   2. import_logs.error_detail passe de TEXT à JSONB (enveloppe
--                                 versionnée des lignes skippées).
--   3. import_logs.rows_duplicates — compteur des doublons ON CONFLICT,
--                                 désormais distinct de rows_skipped.
--
-- À appliquer AVANT la purge de `verbatims` et la reprise de l'import
-- initial (cf. spec §1.4, étape 1). Idempotente (IF NOT EXISTS / bloc
-- de conversion tolérant aux valeurs non-JSON déjà en base).
-- ═══════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. verbatims.guid ──────────────────────────────────────────
ALTER TABLE verbatims
    ADD COLUMN IF NOT EXISTS guid TEXT;

COMMENT ON COLUMN verbatims.guid IS
    'Identifiant Semantiweb de l''avis, tel que fourni dans le CSV source. '
    'Stocké pour audit et pour permettre un basculement du hash vers le '
    'scénario A (verbatim_hash(guid)) sans repurger la base.';

CREATE INDEX IF NOT EXISTS idx_verbatims_guid
    ON verbatims (guid);

-- ── 2. import_logs.rows_duplicates ─────────────────────────────
ALTER TABLE import_logs
    ADD COLUMN IF NOT EXISTS rows_duplicates INTEGER DEFAULT 0;

COMMENT ON COLUMN import_logs.rows_duplicates IS
    'Lignes rejetées par ON CONFLICT (id) DO NOTHING — déjà présentes en '
    'base OU doublon interne au fichier. Distinct de rows_skipped, qui ne '
    'compte que les lignes rejetées par la validation en amont de l''INSERT.';

-- import_type='reset' : ligne de log marquant une reprise à zéro de
-- `verbatims` (cf. scripts/reset_verbatims.py). filename porte le chemin
-- du dump pg_dump, rows_total le nombre de verbatims supprimés.
ALTER TABLE import_logs
    DROP CONSTRAINT IF EXISTS import_logs_import_type_check;
ALTER TABLE import_logs
    ADD CONSTRAINT import_logs_import_type_check
    CHECK (import_type IN ('initial', 'mensuel', 'reset'));

-- ── 3. import_logs.error_detail : TEXT → JSONB ─────────────────
-- Les valeurs historiques peuvent être : NULL, une chaîne JSON valide
-- (nouveau format ou ancien format {"skipped": [...], "batch_errors": [...]})
-- ou du texte libre (ex. message d'exception brut str(exc)). On convertit
-- ligne à ligne pour ne jamais faire échouer la migration sur une valeur
-- non-JSON : le texte libre est préservé tel quel sous une clé dédiée,
-- lisible par le lecteur "legacy" côté application (cf. ui/skip_table.py).
DO $$
DECLARE
    col_type TEXT;
    rec RECORD;
    parsed JSONB;
BEGIN
    SELECT data_type INTO col_type
      FROM information_schema.columns
     WHERE table_name = 'import_logs' AND column_name = 'error_detail';

    IF col_type IS DISTINCT FROM 'jsonb' THEN
        ALTER TABLE import_logs ADD COLUMN IF NOT EXISTS error_detail_jsonb JSONB;

        FOR rec IN SELECT id, error_detail FROM import_logs WHERE error_detail IS NOT NULL LOOP
            BEGIN
                parsed := rec.error_detail::jsonb;
            EXCEPTION WHEN OTHERS THEN
                parsed := jsonb_build_object('legacy_text', rec.error_detail);
            END;
            UPDATE import_logs SET error_detail_jsonb = parsed WHERE id = rec.id;
        END LOOP;

        ALTER TABLE import_logs DROP COLUMN error_detail;
        ALTER TABLE import_logs RENAME COLUMN error_detail_jsonb TO error_detail;
    END IF;
END $$;

COMMENT ON COLUMN import_logs.error_detail IS
    'Détail JSON versionné des lignes skippées et erreurs de lot. '
    'Enveloppe {"version": 1, "skips": [...], "skips_par_code": {...}, '
    '"skips_total": int, "skips_tronque": bool, "batch_errors": [...]}. '
    'Une valeur sans clé "version" (ou {"legacy_text": ...}) est un import '
    'antérieur à cette migration, à afficher en mode dégradé.';

COMMIT;
