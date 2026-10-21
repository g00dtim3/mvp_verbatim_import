-- ═══════════════════════════════════════════════════════════════
-- Migration 0002 — Export d'origine d'un import (lot 1B)
-- Compass · Consumer Voice — Import Pipeline
--
-- Un import lancé depuis un export validé d'api-explorer garde
-- l'identifiant de cet export. Un verbatim retrouve ainsi son export
-- d'origine : verbatims.import_batch_id → import_logs.source_export_id.
-- NULL pour un fichier déposé à la main.
--
-- À exécuter dans le SQL Editor de Supabase. Rejouable sans danger
-- (IF NOT EXISTS). Ne touche qu'au schéma public (Compass).
-- ═══════════════════════════════════════════════════════════════

BEGIN;

ALTER TABLE import_logs
    ADD COLUMN IF NOT EXISTS source_export_id TEXT NULL;

COMMENT ON COLUMN import_logs.source_export_id IS
    'export_id api-explorer (bulk_export.exports) dont provient le fichier importé ; '
    'NULL pour un fichier déposé manuellement.';

CREATE INDEX IF NOT EXISTS idx_import_logs_source_export_id
    ON import_logs (source_export_id)
    WHERE source_export_id IS NOT NULL;

COMMIT;
