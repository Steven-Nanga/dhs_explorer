-- User accounts for multi-user access control
CREATE TABLE IF NOT EXISTS catalog.app_user (
    id            SERIAL PRIMARY KEY,
    email         TEXT UNIQUE NOT NULL,
    display_name  TEXT NOT NULL DEFAULT '',
    role          TEXT NOT NULL DEFAULT 'viewer',       -- 'admin' or 'viewer'
    status        TEXT NOT NULL DEFAULT 'pending',      -- 'pending','approved','rejected','disabled'
    password_hash TEXT,                                 -- bcrypt hash (admin users)
    login_token   TEXT,                                 -- one-time magic link token
    token_expires TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_app_user_token
    ON catalog.app_user (login_token) WHERE login_token IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_app_user_status
    ON catalog.app_user (status);
