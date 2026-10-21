-- ═══════════════════════════════════════════════════════════════
-- Migration 0003 — verbatims.guid n'est plus unique (lot 1B)
-- Compass · Consumer Voice — Import Pipeline
--
-- api-explorer a établi que l'id Semantiweb (colonne guid du CSV) est
-- régénéré à chaque export et se répète d'un export à l'autre
-- (api-explorer/docs/bulk_v2/decision_identifiant.md). L'unicité d'un
-- verbatim repose sur verbatims.id (empreinte SHA-256), pas sur guid.
--
-- Constaté le 21/10/2026 dans la base : contrainte verbatims_guid_key
-- présente. Avec elle, un guid déjà vu fait échouer TOUT le lot de
-- l'import (ON CONFLICT ne couvre que id), alors que la mention est
-- différente. On la remplace par un index simple (recherches d'audit).
--
-- À exécuter dans le SQL Editor de Supabase. Rejouable sans danger
-- (IF EXISTS / IF NOT EXISTS). Aucune donnée n'est modifiée.
-- ═══════════════════════════════════════════════════════════════

BEGIN;

ALTER TABLE verbatims
    DROP CONSTRAINT IF EXISTS verbatims_guid_key;

CREATE INDEX IF NOT EXISTS idx_verbatims_guid
    ON verbatims (guid);

COMMIT;

-- Vérification (doit ne plus renvoyer verbatims_guid_key) :
-- SELECT conname FROM pg_constraint
--  WHERE conrelid = 'public.verbatims'::regclass AND contype = 'u';
