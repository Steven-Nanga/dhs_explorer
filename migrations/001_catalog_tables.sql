CREATE SCHEMA IF NOT EXISTS catalog;

-- ── Country ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.country (
    id          SERIAL PRIMARY KEY,
    iso2        CHAR(2) NOT NULL UNIQUE,
    name        VARCHAR(100) NOT NULL
);

INSERT INTO catalog.country (iso2, name) VALUES
    ('MW', 'Malawi')
ON CONFLICT (iso2) DO NOTHING;

-- ── Survey programme ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.survey_program (
    id          SERIAL PRIMARY KEY,
    code        VARCHAR(20) NOT NULL UNIQUE,
    name        VARCHAR(100) NOT NULL
);

INSERT INTO catalog.survey_program (code, name) VALUES
    ('DHS', 'Demographic and Health Survey'),
    ('MIS', 'Malaria Indicator Survey'),
    ('AIS', 'AIDS Indicator Survey'),
    ('SPA', 'Service Provision Assessment'),
    ('KAP', 'Knowledge, Attitudes, and Practices')
ON CONFLICT (code) DO NOTHING;

-- ── Survey wave (one per country + programme + year) ─────────────────
CREATE TABLE IF NOT EXISTS catalog.survey_wave (
    id          SERIAL PRIMARY KEY,
    country_id  INTEGER NOT NULL REFERENCES catalog.country(id),
    program_id  INTEGER NOT NULL REFERENCES catalog.survey_program(id),
    year_label  VARCHAR(20) NOT NULL,
    dhs_phase   VARCHAR(10),
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (country_id, program_id, year_label)
);

-- ── Individual recode file within a wave ─────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.survey_file (
    id              SERIAL PRIMARY KEY,
    survey_wave_id  INTEGER NOT NULL REFERENCES catalog.survey_wave(id),
    recode_type     VARCHAR(10) NOT NULL,
    file_stem       VARCHAR(50) NOT NULL,
    file_format     VARCHAR(10) NOT NULL,
    source_zip      VARCHAR(500),
    row_count       INTEGER,
    column_count    INTEGER,
    db_table_name   VARCHAR(200),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (survey_wave_id, file_stem)
);

-- ── Import batch (audit trail) ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS catalog.import_batch (
    id               SERIAL PRIMARY KEY,
    survey_wave_id   INTEGER REFERENCES catalog.survey_wave(id),
    started_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           VARCHAR(20) NOT NULL DEFAULT 'running',
    files_processed  INTEGER DEFAULT 0,
    rows_loaded      BIGINT DEFAULT 0,
    errors           TEXT
);
