-- ═══════════════════════════════════════════════════════════════
-- Compass · Consumer Voice — Import Pipeline
-- DDL Schema PostgreSQL / Supabase
-- v1.1
-- ═══════════════════════════════════════════════════════════════

-- ── Extensions ────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ── Table : verbatims ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS verbatims (
    -- Identifiant unique : SHA-256 de brand+date+product_name+verbatim_content
    id                      TEXT            PRIMARY KEY,

    -- Champs source CSV
    brand                   VARCHAR(255)    NOT NULL,
    country                 VARCHAR(10)     NOT NULL,
    date                    DATE            NOT NULL,
    opinion                 VARCHAR(20)     CHECK (opinion IN ('positive','negative','neutral')),
    product_name            VARCHAR(500)    NOT NULL,  -- renommé depuis product_name_SEMANTIWEB
    rating                  SMALLINT        CHECK (rating BETWEEN 1 AND 5),
    source                  VARCHAR(255),
    verbatim_content        TEXT            NOT NULL,
    sampling                BOOLEAN         DEFAULT FALSE,

    -- Attributs sentiment
    attribute_efficiency    VARCHAR(20)     CHECK (attribute_efficiency IN ('positive','negative') OR attribute_efficiency IS NULL),
    attribute_packaging     VARCHAR(20)     CHECK (attribute_packaging  IN ('positive','negative') OR attribute_packaging  IS NULL),
    attribute_price         VARCHAR(20)     CHECK (attribute_price      IN ('positive','negative') OR attribute_price      IS NULL),
    attribute_quality       VARCHAR(20)     CHECK (attribute_quality    IN ('positive','negative') OR attribute_quality    IS NULL),
    attribute_scent         VARCHAR(20)     CHECK (attribute_scent      IN ('positive','negative') OR attribute_scent      IS NULL),
    attribute_taste         VARCHAR(20)     CHECK (attribute_taste      IN ('positive','negative') OR attribute_taste      IS NULL),
    attribute_texture       VARCHAR(20)     CHECK (attribute_texture    IN ('positive','negative') OR attribute_texture    IS NULL),
    attribute_safety        VARCHAR(20)     CHECK (attribute_safety     IN ('positive','negative') OR attribute_safety     IS NULL),
    attribute_composition   VARCHAR(20)     CHECK (attribute_composition IN ('positive','negative') OR attribute_composition IS NULL),

    -- Champs enrichissement (NULL à l'import mensuel)
    categorie_interne       VARCHAR(255),
    sous_categorie_interne  VARCHAR(255),
    photo                   BOOLEAN,        -- NULL = non renseigné

    -- Métadonnées import
    imported_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    import_batch_id         UUID            REFERENCES import_logs(id) ON DELETE SET NULL
);

-- ── Table : categories_mapping ────────────────────────────────
-- Source de vérité unique catégories. 1 entrée par product_name.
CREATE TABLE IF NOT EXISTS categories_mapping (
    product_name            VARCHAR(500)    PRIMARY KEY,
    categorie_interne       VARCHAR(255)    NOT NULL,
    sous_categorie_interne  VARCHAR(255)    NOT NULL,
    photo                   BOOLEAN,
    matched_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    matched_by              VARCHAR(100)    DEFAULT 'system'
);

-- ── Table : import_logs ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS import_logs (
    id                      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    file_hash               VARCHAR(64)     NOT NULL,   -- SHA-256 contenu fichier
    filename                VARCHAR(500)    NOT NULL,
    import_type             VARCHAR(20)     NOT NULL CHECK (import_type IN ('initial','mensuel')),
    started_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at             TIMESTAMPTZ,
    rows_total              INTEGER         DEFAULT 0,
    rows_inserted           INTEGER         DEFAULT 0,
    rows_skipped            INTEGER         DEFAULT 0,
    rows_matched            INTEGER         DEFAULT 0,  -- catégorie connue à l'import
    rows_unmatched          INTEGER         DEFAULT 0,  -- catégorie inconnue
    status                  VARCHAR(20)     NOT NULL DEFAULT 'running'
                                            CHECK (status IN ('running','success','partial','error','duplicate')),
    error_detail            TEXT
);

-- Unicité du hash fichier pour le contrôle doublon
-- NOTE : on n'empêche pas l'INSERT (le contrôle est fait en Python),
-- mais l'index unique permet une recherche rapide.
CREATE UNIQUE INDEX IF NOT EXISTS idx_import_logs_file_hash
    ON import_logs (file_hash)
    WHERE status NOT IN ('error','duplicate');

-- ── Index verbatims ───────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_verbatims_product_name
    ON verbatims (product_name);

CREATE INDEX IF NOT EXISTS idx_verbatims_categorie
    ON verbatims (categorie_interne);

CREATE INDEX IF NOT EXISTS idx_verbatims_date
    ON verbatims (date DESC);

CREATE INDEX IF NOT EXISTS idx_verbatims_brand
    ON verbatims (brand);

CREATE INDEX IF NOT EXISTS idx_verbatims_source
    ON verbatims (source);

CREATE INDEX IF NOT EXISTS idx_verbatims_opinion
    ON verbatims (opinion);

-- Index composite pour les requêtes de matching fréquentes
CREATE INDEX IF NOT EXISTS idx_verbatims_product_categorie
    ON verbatims (product_name, categorie_interne);

-- ── Index categories_mapping ──────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_categories_mapping_categorie
    ON categories_mapping (categorie_interne);

-- ── Vue utilitaire : produits sans catégorie ──────────────────
CREATE OR REPLACE VIEW v_unmatched_products AS
SELECT
    v.product_name,
    COUNT(*)                AS nb_verbatims,
    MIN(v.date)             AS first_seen,
    MAX(v.date)             AS last_seen,
    MAX(v.imported_at)      AS last_imported
FROM verbatims v
LEFT JOIN categories_mapping cm ON v.product_name = cm.product_name
WHERE cm.product_name IS NULL
GROUP BY v.product_name
ORDER BY nb_verbatims DESC;

-- ── Vue utilitaire : résumé catégories ────────────────────────
CREATE OR REPLACE VIEW v_category_summary AS
SELECT
    cm.categorie_interne,
    cm.sous_categorie_interne,
    COUNT(DISTINCT cm.product_name)  AS nb_produits,
    COUNT(v.id)                      AS nb_verbatims,
    MIN(v.date)                      AS date_min,
    MAX(v.date)                      AS date_max
FROM categories_mapping cm
LEFT JOIN verbatims v ON v.product_name = cm.product_name
GROUP BY cm.categorie_interne, cm.sous_categorie_interne
ORDER BY nb_verbatims DESC;

-- ═══════════════════════════════════════════════════════════════
-- COMMENTAIRES
-- ═══════════════════════════════════════════════════════════════
COMMENT ON TABLE verbatims              IS 'Verbatims clients importés depuis l''API Semantiweb';
COMMENT ON TABLE categories_mapping     IS 'Correspondance product_name → catégorie interne. Source de vérité unique.';
COMMENT ON TABLE import_logs            IS 'Journal de tous les imports avec contrôle doublon par hash fichier';
COMMENT ON COLUMN verbatims.id          IS 'SHA-256(brand + date + product_name + verbatim_content) — garantit l''unicité et l''idempotence';
COMMENT ON COLUMN verbatims.product_name IS 'Valeur exacte du champ product_name_SEMANTIWEB du CSV, renommé à l''import';
COMMENT ON COLUMN verbatims.photo       IS 'NULL = non renseigné (import mensuel). true/false = renseigné (import initial ou matching)';
COMMENT ON COLUMN import_logs.file_hash IS 'SHA-256 du contenu binaire du fichier CSV — utilisé pour bloquer les imports en doublon';
COMMENT ON COLUMN import_logs.import_type IS 'initial = fichier historique complet / mensuel = export courant API';
