CREATE SCHEMA IF NOT EXISTS microdata;

-- ── Generic row store for auxiliary / rare recode types ───────────────
CREATE TABLE IF NOT EXISTS microdata.observation (
    id               BIGSERIAL PRIMARY KEY,
    survey_wave_id   INTEGER NOT NULL REFERENCES catalog.survey_wave(id),
    recode_type      VARCHAR(10) NOT NULL,
    caseid           TEXT,
    source_row       INTEGER,
    import_batch_id  INTEGER REFERENCES catalog.import_batch(id),
    payload          JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_obs_wave_recode
    ON microdata.observation (survey_wave_id, recode_type);

CREATE INDEX IF NOT EXISTS idx_obs_caseid
    ON microdata.observation (caseid) WHERE caseid IS NOT NULL;

-- ── Variable metadata (populated from Stata .DTA headers) ────────────
CREATE TABLE IF NOT EXISTS microdata.variable_dictionary (
    id              SERIAL PRIMARY KEY,
    survey_file_id  INTEGER NOT NULL REFERENCES catalog.survey_file(id),
    var_name        VARCHAR(100) NOT NULL,
    var_label       TEXT,
    stata_type      VARCHAR(30),
    pg_type         VARCHAR(30),
    col_position    INTEGER,
    in_overflow     BOOLEAN DEFAULT FALSE,
    UNIQUE (survey_file_id, var_name)
);

-- ── Value labels (Stata value‑label mappings) ────────────────────────
CREATE TABLE IF NOT EXISTS microdata.value_labels (
    id              SERIAL PRIMARY KEY,
    survey_file_id  INTEGER NOT NULL REFERENCES catalog.survey_file(id),
    var_name        VARCHAR(100) NOT NULL,
    code            VARCHAR(50) NOT NULL,
    label           TEXT NOT NULL,
    UNIQUE (survey_file_id, var_name, code)
);

CREATE INDEX IF NOT EXISTS idx_vallbl_file_var
    ON microdata.value_labels (survey_file_id, var_name);
