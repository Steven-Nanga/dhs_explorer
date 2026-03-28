-- Add log_text column to import_batch for persistent job tracking
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'catalog'
          AND table_name = 'import_batch'
          AND column_name = 'log_text'
    ) THEN
        ALTER TABLE catalog.import_batch ADD COLUMN log_text TEXT;
    END IF;
END $$;
